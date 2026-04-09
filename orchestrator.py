import json
import importlib
import schedule
import time
import logging
import sqlite3
import re
from pathlib import Path
from processor import run_processor
from settings_loader import load_json_config
try:
    import mysql.connector
except Exception:
    mysql = None
else:
    mysql = mysql.connector

# Setup logging
logging.basicConfig(
    filename='orchestrator_debug.log',
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s %(message)s'
)
from datetime import datetime, timezone

# Load local override config when available.
config = load_json_config()

def normalize_identifier(name):
    """Convert arbitrary names to SQL-safe identifiers."""
    cleaned = re.sub(r'[^a-zA-Z0-9_]', '_', name)
    cleaned = re.sub(r'_+', '_', cleaned).strip('_').lower()
    if not cleaned:
        cleaned = 'field'
    if cleaned[0].isdigit():
        cleaned = f'c_{cleaned}'
    return cleaned


def get_column_sql_type(column_name):
    return "TEXT"


def get_storage_type():
    return config.get('storage', {}).get('type', 'sqlite').lower()


def get_db_connection():
    storage = config.get('storage', {})
    storage_type = get_storage_type()

    if storage_type == 'mysql':
        if mysql is None:
            raise RuntimeError("mysql-connector-python is not installed. Install it before using MySQL storage.")
        mysql_cfg = storage.get('mysql', {})
        conn = mysql.connect(
            host=mysql_cfg.get('host', '127.0.0.1'),
            port=int(mysql_cfg.get('port', 3306)),
            database=mysql_cfg.get('database', 'public_data'),
            user=mysql_cfg.get('user', 'root'),
            password=mysql_cfg.get('password', ''),
            ssl_disabled=bool(mysql_cfg.get('ssl_disabled', True))
        )
        return conn

    sqlite_cfg = storage.get('sqlite', {})
    db_path = sqlite_cfg.get('db_path', storage.get('db_path', 'public_data.db'))
    db_file = Path(db_path)
    conn = sqlite3.connect(db_file)
    conn.execute('PRAGMA journal_mode=WAL;')
    conn.execute('PRAGMA synchronous=NORMAL;')
    return conn


def quote_ident(name):
    return f'`{name}`' if get_storage_type() == 'mysql' else f'"{name}"'


def param_placeholder(count):
    marker = '%s' if get_storage_type() == 'mysql' else '?'
    return ', '.join([marker] * count)


def _stream_min_interval_minutes(stream_config):
    try:
        value = int(stream_config.get('min_interval_minutes', 0) or 0)
    except (TypeError, ValueError):
        return 0
    return max(0, value)


def _parse_logged_timestamp(raw_value):
    if raw_value is None:
        return None
    try:
        return datetime.fromisoformat(str(raw_value).replace('Z', '+00:00')).replace(tzinfo=None)
    except Exception:
        return None


def should_run_stream(conn, stream_name, stream_config):
    min_interval = _stream_min_interval_minutes(stream_config)
    if min_interval <= 0:
        return True, 0

    sql = (
        'SELECT timestamp FROM master_log WHERE stream_name = %s '
        'ORDER BY id DESC LIMIT 1'
        if get_storage_type() == 'mysql'
        else 'SELECT timestamp FROM master_log WHERE stream_name = ? ORDER BY id DESC LIMIT 1'
    )
    cur = conn.cursor()
    try:
        cur.execute(sql, (stream_name,))
        row = cur.fetchone()
    finally:
        cur.close()

    last_attempt = _parse_logged_timestamp(row[0] if row else None)
    if last_attempt is None:
        return True, 0

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    elapsed_minutes = (now - last_attempt).total_seconds() / 60
    if elapsed_minutes >= min_interval:
        return True, 0

    remaining = max(1, int(min_interval - elapsed_minutes))
    return False, remaining

def ensure_master_log_table(conn):
    if get_storage_type() == 'mysql':
        conn.cursor().execute(
            """
            CREATE TABLE IF NOT EXISTS master_log (
                id BIGINT PRIMARY KEY AUTO_INCREMENT,
                timestamp VARCHAR(64) NOT NULL,
                stream_name VARCHAR(128) NOT NULL,
                rows_added INT NOT NULL,
                status TEXT NOT NULL
            )
            """
        )
    else:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS master_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                stream_name TEXT NOT NULL,
                rows_added INTEGER NOT NULL,
                status TEXT NOT NULL
            )
            """
        )
    conn.commit()


def ensure_stream_table(conn, stream_name, headers):
    table_name = f"stream_{normalize_identifier(stream_name)}"
    columns = [normalize_identifier(h) for h in headers]
    column_defs = [f'{quote_ident(c)} {get_column_sql_type(c)}' for c in columns]
    if get_storage_type() == 'mysql':
        sql = f'''
            CREATE TABLE IF NOT EXISTS {quote_ident(table_name)} (
                id BIGINT PRIMARY KEY AUTO_INCREMENT,
                ingested_at VARCHAR(64) NOT NULL,
                {', '.join(column_defs)}
            )
        '''
        conn.cursor().execute(sql)
    else:
        sql = f'''
            CREATE TABLE IF NOT EXISTS {quote_ident(table_name)} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ingested_at TEXT NOT NULL,
                {', '.join(column_defs)}
            )
        '''
        conn.execute(sql)
    conn.commit()
    return table_name, columns


def insert_stream_rows(conn, table_name, columns, rows):
    if not rows:
        return 0
    placeholders = param_placeholder(len(columns) + 1)
    quoted_columns = ', '.join([quote_ident(c) for c in columns])
    sql = f'INSERT INTO {quote_ident(table_name)} (ingested_at, {quoted_columns}) VALUES ({placeholders})'
    ingested_at = datetime.now(timezone.utc).isoformat()
    records = [(ingested_at, *row) for row in rows]
    if get_storage_type() == 'mysql':
        cur = conn.cursor()
        cur.executemany(sql, records)
    else:
        conn.executemany(sql, records)
    conn.commit()
    return len(records)


def safe_insert_master_log(conn, row, retries=3):
    logging.debug(f'Attempting to insert master log row: {row}')
    for attempt in range(retries):
        try:
            if get_storage_type() == 'mysql':
                sql = 'INSERT INTO master_log (timestamp, stream_name, rows_added, status) VALUES (%s, %s, %s, %s)'
                conn.cursor().execute(sql, row)
            else:
                conn.execute(
                    'INSERT INTO master_log (timestamp, stream_name, rows_added, status) VALUES (?, ?, ?, ?)',
                    row
                )
            conn.commit()
            logging.info(f'Successfully inserted master log row: {row}')
            return True
        except Exception as err:
            logging.error(f'Insert attempt {attempt + 1} failed: {err}')
            print(f"[!] Insert attempt {attempt + 1} failed: {err}")
            time.sleep(2 ** attempt)
    print(f"[!] All insert attempts failed for row: {row}")
    logging.error(f'All insert attempts failed for row: {row}')
    return False


conn = get_db_connection()
ensure_master_log_table(conn)
if get_storage_type() == 'mysql':
    mysql_cfg = config.get('storage', {}).get('mysql', {})
    print(f"MySQL database: {mysql_cfg.get('host', '127.0.0.1')}:{mysql_cfg.get('port', 3306)}/{mysql_cfg.get('database', 'public_data')}")
else:
    sqlite_cfg = config.get('storage', {}).get('sqlite', {})
    print('SQLite database:', sqlite_cfg.get('db_path', config.get('storage', {}).get('db_path', 'public_data.db')))


def run_orchestrator():
    # Iterate enabled streams
    for stream_name, stream_config in config['streams'].items():
        if stream_config.get('enabled'):
            should_run, retry_in_min = should_run_stream(conn, stream_name, stream_config)
            if not should_run:
                print(f"[SKIP] Stream '{stream_name}' throttled; next attempt in about {retry_in_min} min.")
                logging.info(
                    "Stream '%s' throttled by min_interval_minutes=%s; retry in %s min.",
                    stream_name,
                    stream_config.get('min_interval_minutes'),
                    retry_in_min,
                )
                continue
            try:
                module = importlib.import_module(f"modules.{stream_name}_module")
                fetch_func = getattr(module, f"fetch_{stream_name}")

                data_points = fetch_func(stream_config)

                headers = module.get_headers() if hasattr(module, "get_headers") else ["timestamp","location","metric","value","unit"]
                table_name, columns = ensure_stream_table(conn, stream_name, headers)

                # Build rows safely
                rows = [[dp.get(h, "") for h in headers] for dp in data_points]
                inserted_rows = insert_stream_rows(conn, table_name, columns, rows)

                print(f"[OK] Stream '{stream_name}' inserted {inserted_rows} rows into table '{table_name}'.")
                logging.info(f"Stream '{stream_name}' inserted {inserted_rows} rows into table '{table_name}'.")

                safe_insert_master_log(conn, (
                    datetime.now(timezone.utc).isoformat(),
                    stream_name,
                    inserted_rows,
                    "success"
                ))
            except Exception as e:
                print(f"[FAIL] Stream '{stream_name}' failed: {e}")
                logging.error(f"Stream '{stream_name}' failed: {e}")
                import traceback
                traceback.print_exc()
                logging.debug(traceback.format_exc())
                safe_insert_master_log(conn, (
                    datetime.now(timezone.utc).isoformat(),
                    stream_name,
                    0,
                    f"failed: {e}"
                ))

def run_collection_and_process():
    run_orchestrator()
    try:
        run_processor()
    except Exception as e:
        print(f"[FAIL] Processor error: {e}")
        logging.error(f"Processor error: {e}")

# --- Scheduler / Execution ---
logging.info('Starting orchestrator main loop')
run_collection_and_process()
schedule.every(30).minutes.do(run_collection_and_process)

while True:
    logging.debug('Main loop tick: running schedule.run_pending()')
    schedule.run_pending()
    time.sleep(1)
    logging.debug('Main loop tick: sleep complete')