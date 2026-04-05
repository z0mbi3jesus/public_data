import hashlib
import json
import secrets
from datetime import datetime

import mysql.connector
from fastapi import Cookie, Depends, FastAPI, Header, HTTPException, Response
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from settings_loader import load_json_config


VERTICAL_SIGNAL_MAP = {
    "restaurant": "demand_pressure_score",
    "logistics": "delivery_risk_score",
    "outdoor": "outdoor_safety_score",
}


def load_config():
    return load_json_config()


def get_conn():
    cfg = load_config()
    mysql_cfg = cfg["storage"]["mysql"]
    return mysql.connector.connect(
        host=mysql_cfg["host"],
        port=int(mysql_cfg["port"]),
        user=mysql_cfg["user"],
        password=mysql_cfg["password"],
        database=mysql_cfg["database"],
        ssl_disabled=True,
    )


def get_provider_settings():
    cfg = load_config()
    provider = cfg.get("provider", {})
    return {
        "owner_name": provider.get("owner_name", "Provider"),
        "key": provider.get("key", ""),
    }


def get_admin_bootstrap_settings():
    cfg = load_config()
    admin = cfg.get("admin", {})
    return {
        "email": admin.get("bootstrap_email", "admin@publicdata.local"),
        "password": admin.get("bootstrap_password", "ChangeThisAdmin123!"),
    }


def hash_password(password):
    salt = secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        bytes.fromhex(salt),
        100000,
    ).hex()
    return f"pbkdf2_sha256$100000${salt}${digest}"


def verify_password(password, encoded):
    try:
        algo, rounds, salt, digest = encoded.split("$", 3)
        if algo != "pbkdf2_sha256":
            return False
        actual = hashlib.pbkdf2_hmac(
            "sha256",
            password.encode("utf-8"),
            bytes.fromhex(salt),
            int(rounds),
        ).hex()
        return secrets.compare_digest(actual, digest)
    except Exception:
        return False


def hash_secret(raw):
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def generate_session_token(prefix):
    return f"{prefix}_{secrets.token_urlsafe(36)}"


def ensure_auth_tables():
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS admin_users (
                id INT NOT NULL AUTO_INCREMENT,
                email VARCHAR(255) NOT NULL,
                password_hash VARCHAR(255) NOT NULL,
                active TINYINT(1) NOT NULL DEFAULT 1,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (id),
                UNIQUE KEY uq_admin_email (email)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS admin_sessions (
                id BIGINT NOT NULL AUTO_INCREMENT,
                admin_user_id INT NOT NULL,
                session_hash VARCHAR(64) NOT NULL,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                expires_at DATETIME NOT NULL,
                last_seen_at DATETIME NULL,
                PRIMARY KEY (id),
                UNIQUE KEY uq_admin_session_hash (session_hash),
                KEY idx_admin_sessions_expires (expires_at),
                CONSTRAINT fk_admin_sessions_admin_user FOREIGN KEY (admin_user_id) REFERENCES admin_users(id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS client_sessions (
                id BIGINT NOT NULL AUTO_INCREMENT,
                user_id INT NOT NULL,
                tenant_id INT NOT NULL,
                session_hash VARCHAR(64) NOT NULL,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                expires_at DATETIME NOT NULL,
                last_seen_at DATETIME NULL,
                PRIMARY KEY (id),
                UNIQUE KEY uq_client_session_hash (session_hash),
                KEY idx_client_sessions_expires (expires_at),
                CONSTRAINT fk_client_sessions_user FOREIGN KEY (user_id) REFERENCES users(id),
                CONSTRAINT fk_client_sessions_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS purchase_tokens (
                id BIGINT NOT NULL AUTO_INCREMENT,
                tenant_id INT NOT NULL,
                token_hash VARCHAR(64) NOT NULL,
                plan_tier VARCHAR(32) NOT NULL,
                entitlements_json JSON NOT NULL,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                expires_at DATETIME NOT NULL,
                used_at DATETIME NULL,
                used_by_user_id INT NULL,
                is_used TINYINT(1) NOT NULL DEFAULT 0,
                PRIMARY KEY (id),
                UNIQUE KEY uq_purchase_token_hash (token_hash),
                KEY idx_purchase_token_tenant (tenant_id),
                KEY idx_purchase_token_expires (expires_at),
                CONSTRAINT fk_purchase_token_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id),
                CONSTRAINT fk_purchase_token_user FOREIGN KEY (used_by_user_id) REFERENCES users(id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def ensure_admin_bootstrap_user():
    cfg = get_admin_bootstrap_settings()
    email = str(cfg["email"]).lower().strip()
    password = str(cfg["password"])

    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT id FROM admin_users WHERE email = %s", (email,))
        row = cur.fetchone()
        if not row:
            cur.execute(
                "INSERT INTO admin_users (email, password_hash, active) VALUES (%s, %s, 1)",
                (email, hash_password(password)),
            )
            conn.commit()
    finally:
        cur.close()
        conn.close()


def ensure_processed_signal_columns():
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("SHOW COLUMNS FROM `processed_signals` LIKE 'details_json'")
        exists = cur.fetchone()
        if not exists:
            cur.execute("ALTER TABLE `processed_signals` ADD COLUMN `details_json` JSON NULL")
            conn.commit()
    finally:
        cur.close()
        conn.close()


def hash_api_key(raw_key):
    return hashlib.sha256(raw_key.encode("utf-8")).hexdigest()


def serialize_dt(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def get_request_context(x_api_key: str = Header(default=None)):
    if not x_api_key:
        raise HTTPException(status_code=401, detail="Missing X-API-Key header")

    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT
                ak.id AS api_key_id,
                ak.tenant_id,
                ak.active AS api_key_active,
                t.name AS tenant_name,
                t.plan_tier,
                t.active AS tenant_active
            FROM api_keys ak
            JOIN tenants t ON t.id = ak.tenant_id
            WHERE ak.key_hash = %s
            """,
            (hash_api_key(x_api_key),),
        )
        row = cur.fetchone()
        if not row or not row["api_key_active"] or not row["tenant_active"]:
            raise HTTPException(status_code=401, detail="Invalid or inactive API key")

        cur.execute(
            "UPDATE api_keys SET last_used_at = NOW() WHERE id = %s",
            (row["api_key_id"],),
        )
        conn.commit()
        return {
            "tenant_id": row["tenant_id"],
            "tenant_name": row["tenant_name"],
            "plan_tier": row["plan_tier"],
        }
    finally:
        cur.close()
        conn.close()


def get_provider_context(x_provider_key: str = Header(default=None)):
    provider = get_provider_settings()
    expected_key = provider.get("key", "")
    if not expected_key:
        raise HTTPException(status_code=500, detail="Provider key is not configured")
    if not x_provider_key or x_provider_key != expected_key:
        raise HTTPException(status_code=401, detail="Invalid provider key")
    return {
        "owner_name": provider.get("owner_name", "Provider"),
    }


def get_admin_context(admin_session: str = Cookie(default=None)):
    if not admin_session:
        raise HTTPException(status_code=401, detail="Missing admin session")

    session_hash = hash_secret(admin_session)
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT s.id AS session_id, s.admin_user_id, s.expires_at, a.email, a.active
            FROM admin_sessions s
            JOIN admin_users a ON a.id = s.admin_user_id
            WHERE s.session_hash = %s
            """,
            (session_hash,),
        )
        row = cur.fetchone()
        if not row or not row["active"]:
            raise HTTPException(status_code=401, detail="Invalid admin session")

        expires = row["expires_at"]
        if expires and isinstance(expires, datetime) and expires < datetime.utcnow():
            raise HTTPException(status_code=401, detail="Admin session expired")

        cur.execute("UPDATE admin_sessions SET last_seen_at = NOW() WHERE id = %s", (row["session_id"],))
        conn.commit()

        return {
            "admin_user_id": row["admin_user_id"],
            "email": row["email"],
        }
    finally:
        cur.close()
        conn.close()


def get_client_context(client_session: str = Cookie(default=None)):
    if not client_session:
        raise HTTPException(status_code=401, detail="Missing client session")

    session_hash = hash_secret(client_session)
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT s.id AS session_id, s.user_id, s.tenant_id, s.expires_at,
                   u.email, u.role, u.active AS user_active,
                   t.name AS tenant_name, t.plan_tier, t.active AS tenant_active
            FROM client_sessions s
            JOIN users u ON u.id = s.user_id
            JOIN tenants t ON t.id = s.tenant_id
            WHERE s.session_hash = %s
            """,
            (session_hash,),
        )
        row = cur.fetchone()
        if not row or not row["user_active"] or not row["tenant_active"]:
            raise HTTPException(status_code=401, detail="Invalid client session")

        expires = row["expires_at"]
        if expires and isinstance(expires, datetime) and expires < datetime.utcnow():
            raise HTTPException(status_code=401, detail="Client session expired")

        cur.execute("UPDATE client_sessions SET last_seen_at = NOW() WHERE id = %s", (row["session_id"],))
        conn.commit()

        return {
            "session_id": row["session_id"],
            "user_id": row["user_id"],
            "email": row["email"],
            "role": row["role"],
            "tenant_id": row["tenant_id"],
            "tenant_name": row["tenant_name"],
            "plan_tier": row["plan_tier"],
        }
    finally:
        cur.close()
        conn.close()


def require_vertical(vertical):
    def dependency(context=Depends(get_request_context)):
        conn = get_conn()
        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT 1 FROM entitlements WHERE tenant_id = %s AND vertical = %s",
                (context["tenant_id"], vertical),
            )
            if not cur.fetchone():
                raise HTTPException(status_code=403, detail=f"Tenant does not have {vertical} access")
            return context
        finally:
            cur.close()
            conn.close()

    return dependency


def fetch_entitlements(tenant_id):
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT vertical FROM entitlements WHERE tenant_id = %s ORDER BY vertical ASC",
            (tenant_id,),
        )
        return [row[0] for row in cur.fetchall()]
    finally:
        cur.close()
        conn.close()


def fetch_latest_signal(vertical):
    signal_key = VERTICAL_SIGNAL_MAP[vertical]
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT computed_at, window_start, window_end, vertical, signal_key,
                     signal_value, signal_label, contributing_streams, details_json
            FROM processed_signals
            WHERE vertical = %s AND signal_key = %s
            ORDER BY computed_at DESC, id DESC
            LIMIT 1
            """,
            (vertical, signal_key),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail=f"No signal available for {vertical}")
        details_raw = row.get("details_json")
        if isinstance(details_raw, str):
            try:
                details = json.loads(details_raw)
            except Exception:
                details = {}
        elif isinstance(details_raw, dict):
            details = details_raw
        else:
            details = {}
        return {
            "vertical": row["vertical"],
            "signal_key": row["signal_key"],
            "signal_value": row["signal_value"],
            "signal_label": row["signal_label"],
            "computed_at": serialize_dt(row["computed_at"]),
            "window_start": serialize_dt(row["window_start"]),
            "window_end": serialize_dt(row["window_end"]),
            "contributing_streams": row["contributing_streams"].split(",") if row["contributing_streams"] else [],
            "details": details,
        }
    finally:
        cur.close()
        conn.close()


def fetch_signal_trend(vertical, points=24):
    signal_key = VERTICAL_SIGNAL_MAP[vertical]
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT computed_at, signal_value, signal_label
            FROM processed_signals
            WHERE vertical = %s AND signal_key = %s
            ORDER BY computed_at DESC, id DESC
            LIMIT %s
            """,
            (vertical, signal_key, int(points)),
        )
        rows = cur.fetchall()
        rows.reverse()
        return [
            {
                "computed_at": serialize_dt(row["computed_at"]),
                "signal_value": row["signal_value"],
                "signal_label": row["signal_label"],
            }
            for row in rows
        ]
    finally:
        cur.close()
        conn.close()


def fetch_stream_health():
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT sh.stream_name, sh.status, sh.last_success_at, sh.rows_last_run, sh.checked_at
            FROM stream_health sh
            JOIN (
                SELECT stream_name, MAX(checked_at) AS max_checked_at
                FROM stream_health
                GROUP BY stream_name
            ) latest
              ON latest.stream_name = sh.stream_name
             AND latest.max_checked_at = sh.checked_at
            ORDER BY sh.stream_name ASC
            """
        )
        rows = cur.fetchall()
        return [
            {
                "stream_name": row["stream_name"],
                "status": row["status"],
                "last_success_at": serialize_dt(row["last_success_at"]),
                "rows_last_run": row["rows_last_run"],
                "checked_at": serialize_dt(row["checked_at"]),
            }
            for row in rows
        ]
    finally:
        cur.close()
        conn.close()


def fetch_platform_kpis():
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT COUNT(*) AS c FROM tenants WHERE active = 1")
        active_tenants = cur.fetchone()["c"]

        cur.execute("SELECT COUNT(*) AS c FROM api_keys WHERE active = 1")
        active_api_keys = cur.fetchone()["c"]

        cur.execute(
            """
            SELECT COUNT(*) AS c
            FROM stream_health sh
            JOIN (
                SELECT stream_name, MAX(checked_at) AS latest_checked
                FROM stream_health
                GROUP BY stream_name
            ) latest ON latest.stream_name = sh.stream_name AND latest.latest_checked = sh.checked_at
            WHERE sh.status = 'ok'
            """
        )
        healthy_streams = cur.fetchone()["c"]

        cur.execute("SELECT COUNT(DISTINCT stream_name) AS c FROM stream_health")
        total_streams = cur.fetchone()["c"]

        cur.execute(
            """
            SELECT vertical, signal_key, signal_value, signal_label, computed_at
            FROM processed_signals
            ORDER BY computed_at DESC, id DESC
            LIMIT 3
            """
        )
        latest_signals = [
            {
                "vertical": row["vertical"],
                "signal_key": row["signal_key"],
                "signal_value": row["signal_value"],
                "signal_label": row["signal_label"],
                "computed_at": serialize_dt(row["computed_at"]),
            }
            for row in cur.fetchall()
        ]

        return {
            "active_tenants": active_tenants,
            "active_api_keys": active_api_keys,
            "healthy_streams": healthy_streams,
            "total_streams": total_streams,
            "latest_signals": latest_signals,
        }
    finally:
        cur.close()
        conn.close()


def fetch_provider_tenants():
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT t.id, t.name, t.plan_tier, t.contact_email, t.created_at, t.active,
                   COALESCE(MAX(ak.last_used_at), NULL) AS last_api_use
            FROM tenants t
            LEFT JOIN api_keys ak ON ak.tenant_id = t.id AND ak.active = 1
            GROUP BY t.id, t.name, t.plan_tier, t.contact_email, t.created_at, t.active
            ORDER BY t.created_at DESC
            """
        )
        tenant_rows = cur.fetchall()

        cur.execute(
            """
            SELECT tenant_id, vertical
            FROM entitlements
            ORDER BY tenant_id ASC, vertical ASC
            """
        )
        ent_rows = cur.fetchall()
        ent_map = {}
        for row in ent_rows:
            ent_map.setdefault(row["tenant_id"], []).append(row["vertical"])

        cur.execute(
            """
            SELECT tenant_id, key_prefix, label, active, created_at, last_used_at
            FROM api_keys
            ORDER BY created_at DESC
            """
        )
        key_rows = cur.fetchall()
        key_map = {}
        for row in key_rows:
            key_map.setdefault(row["tenant_id"], []).append(
                {
                    "key_prefix": row["key_prefix"],
                    "label": row["label"],
                    "active": bool(row["active"]),
                    "created_at": serialize_dt(row["created_at"]),
                    "last_used_at": serialize_dt(row["last_used_at"]),
                }
            )

        return [
            {
                "id": row["id"],
                "name": row["name"],
                "plan_tier": row["plan_tier"],
                "contact_email": row["contact_email"],
                "created_at": serialize_dt(row["created_at"]),
                "active": bool(row["active"]),
                "last_api_use": serialize_dt(row["last_api_use"]),
                "entitlements": ent_map.get(row["id"], []),
                "api_keys": key_map.get(row["id"], []),
            }
            for row in tenant_rows
        ]
    finally:
        cur.close()
        conn.close()


class ProviderTenantCreate(BaseModel):
    name: str
    contact_email: str
    plan_tier: str = "trial"
    entitlements: list[str] = ["restaurant"]
    api_key_label: str = "default"


class LoginPayload(BaseModel):
    email: str
    password: str


class PurchaseTokenCreatePayload(BaseModel):
    tenant_id: int
    plan_tier: str = "trial"
    entitlements: list[str] = ["restaurant"]
    expires_hours: int = 72


class PurchaseTokenRedeemPayload(BaseModel):
    purchase_token: str
    api_key_label: str = "default"


class ClientKeyRotatePayload(BaseModel):
    api_key_label: str = "rotated-key"
    deactivate_existing: bool = True


class ClientKeyRevokePayload(BaseModel):
    key_prefix: str


class AdminTenantStatusPayload(BaseModel):
    active: bool


class AdminKeyRotatePayload(BaseModel):
    tenant_id: int
    api_key_label: str = "admin-rotated-key"
    deactivate_existing: bool = False


class AdminKeyRevokePayload(BaseModel):
    tenant_id: int
    key_prefix: str


def create_tenant(payload: ProviderTenantCreate):
    normalized_plan = payload.plan_tier.lower().strip()
    if normalized_plan not in {"trial", "basic", "pro", "enterprise"}:
        raise HTTPException(status_code=400, detail="plan_tier must be one of trial/basic/pro/enterprise")

    allowed_verticals = {"restaurant", "logistics", "outdoor"}
    normalized_ents = sorted({e.lower().strip() for e in payload.entitlements if e})
    if not normalized_ents:
        normalized_ents = ["restaurant"]
    for ent in normalized_ents:
        if ent not in allowed_verticals:
            raise HTTPException(status_code=400, detail=f"Invalid entitlement: {ent}")

    raw_api_key = f"pd_{secrets.token_urlsafe(24)}"
    key_hash = hash_api_key(raw_api_key)
    key_prefix = raw_api_key[:8]

    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO tenants (name, plan_tier, contact_email, active) VALUES (%s, %s, %s, 1)",
            (payload.name.strip(), normalized_plan, str(payload.contact_email).lower().strip()),
        )
        tenant_id = cur.lastrowid

        for ent in normalized_ents:
            cur.execute(
                "INSERT INTO entitlements (tenant_id, vertical) VALUES (%s, %s)",
                (tenant_id, ent),
            )

        cur.execute(
            """
            INSERT INTO api_keys (tenant_id, key_hash, key_prefix, label, active)
            VALUES (%s, %s, %s, %s, 1)
            """,
            (tenant_id, key_hash, key_prefix, payload.api_key_label.strip() or "default"),
        )

        conn.commit()
        return {
            "tenant_id": tenant_id,
            "tenant_name": payload.name.strip(),
            "contact_email": str(payload.contact_email).lower().strip(),
            "plan_tier": normalized_plan,
            "entitlements": normalized_ents,
            "api_key": raw_api_key,
            "api_key_prefix": key_prefix,
        }
    except mysql.connector.Error as err:
        conn.rollback()
        if getattr(err, "errno", None) == 1062:
            raise HTTPException(status_code=409, detail="Tenant with that contact email already exists")
        raise
    finally:
        cur.close()
        conn.close()


def login_admin(payload: LoginPayload):
    email = str(payload.email).lower().strip()
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT id, email, password_hash, active FROM admin_users WHERE email = %s", (email,))
        row = cur.fetchone()
        if not row or not row["active"] or not verify_password(payload.password, row["password_hash"]):
            raise HTTPException(status_code=401, detail="Invalid admin credentials")

        raw_session = generate_session_token("adm")
        session_hash = hash_secret(raw_session)
        cur.execute(
            """
            INSERT INTO admin_sessions (admin_user_id, session_hash, expires_at, last_seen_at)
            VALUES (%s, %s, DATE_ADD(NOW(), INTERVAL 12 HOUR), NOW())
            """,
            (row["id"], session_hash),
        )
        conn.commit()
        return {
            "session": raw_session,
            "admin_user_id": row["id"],
            "email": row["email"],
        }
    finally:
        cur.close()
        conn.close()


def login_client(payload: LoginPayload):
    email = str(payload.email).lower().strip()
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT u.id, u.tenant_id, u.email, u.password_hash, u.role, u.active,
                   t.name AS tenant_name, t.plan_tier, t.active AS tenant_active
            FROM users u
            JOIN tenants t ON t.id = u.tenant_id
            WHERE u.email = %s
            """,
            (email,),
        )
        row = cur.fetchone()
        if not row or not row["active"] or not row["tenant_active"] or not verify_password(payload.password, row["password_hash"]):
            raise HTTPException(status_code=401, detail="Invalid client credentials")

        raw_session = generate_session_token("cli")
        session_hash = hash_secret(raw_session)
        cur.execute(
            """
            INSERT INTO client_sessions (user_id, tenant_id, session_hash, expires_at, last_seen_at)
            VALUES (%s, %s, %s, DATE_ADD(NOW(), INTERVAL 12 HOUR), NOW())
            """,
            (row["id"], row["tenant_id"], session_hash),
        )
        conn.commit()
        return {
            "session": raw_session,
            "user_id": row["id"],
            "email": row["email"],
            "tenant_id": row["tenant_id"],
            "tenant_name": row["tenant_name"],
            "plan_tier": row["plan_tier"],
            "role": row["role"],
        }
    finally:
        cur.close()
        conn.close()


def revoke_session(raw_token, table_name):
    if not raw_token:
        return
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(f"DELETE FROM {table_name} WHERE session_hash = %s", (hash_secret(raw_token),))
        conn.commit()
    finally:
        cur.close()
        conn.close()


def create_purchase_token(payload: PurchaseTokenCreatePayload):
    allowed_verticals = {"restaurant", "logistics", "outdoor"}
    entitlements = sorted({e.lower().strip() for e in payload.entitlements if e})
    if not entitlements:
        entitlements = ["restaurant"]
    for ent in entitlements:
        if ent not in allowed_verticals:
            raise HTTPException(status_code=400, detail=f"Invalid entitlement: {ent}")

    plan_tier = payload.plan_tier.lower().strip()
    if plan_tier not in {"trial", "basic", "pro", "enterprise"}:
        raise HTTPException(status_code=400, detail="Invalid plan_tier")

    expires_hours = max(1, min(int(payload.expires_hours), 24 * 30))
    raw_token = f"pay_{secrets.token_urlsafe(26)}"
    token_hash = hash_secret(raw_token)

    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT id FROM tenants WHERE id = %s", (payload.tenant_id,))
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Tenant not found")

        cur.execute(
            """
            INSERT INTO purchase_tokens (tenant_id, token_hash, plan_tier, entitlements_json, expires_at, is_used)
            VALUES (%s, %s, %s, %s, DATE_ADD(NOW(), INTERVAL %s HOUR), 0)
            """,
            (payload.tenant_id, token_hash, plan_tier, json.dumps(entitlements), expires_hours),
        )
        token_id = cur.lastrowid
        conn.commit()
        return {
            "id": token_id,
            "tenant_id": payload.tenant_id,
            "plan_tier": plan_tier,
            "entitlements": entitlements,
            "expires_hours": expires_hours,
            "purchase_token": raw_token,
        }
    finally:
        cur.close()
        conn.close()


def fetch_purchase_tokens(limit=100):
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT pt.id, pt.tenant_id, t.name AS tenant_name, pt.plan_tier,
                   pt.entitlements_json, pt.created_at, pt.expires_at,
                   pt.used_at, pt.is_used
            FROM purchase_tokens pt
            JOIN tenants t ON t.id = pt.tenant_id
            ORDER BY pt.created_at DESC
            LIMIT %s
            """,
            (int(limit),),
        )
        rows = cur.fetchall()
        return [
            {
                "id": row["id"],
                "tenant_id": row["tenant_id"],
                "tenant_name": row["tenant_name"],
                "plan_tier": row["plan_tier"],
                "entitlements": json.loads(row["entitlements_json"]) if row.get("entitlements_json") else [],
                "created_at": serialize_dt(row["created_at"]),
                "expires_at": serialize_dt(row["expires_at"]),
                "used_at": serialize_dt(row["used_at"]),
                "is_used": bool(row["is_used"]),
            }
            for row in rows
        ]
    finally:
        cur.close()
        conn.close()


def redeem_purchase_token(context, payload: PurchaseTokenRedeemPayload):
    raw_token = payload.purchase_token.strip()
    if not raw_token:
        raise HTTPException(status_code=400, detail="purchase_token is required")

    token_hash = hash_secret(raw_token)
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT *
            FROM purchase_tokens
            WHERE token_hash = %s
            """,
            (token_hash,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Purchase token not found")
        if int(row["tenant_id"]) != int(context["tenant_id"]):
            raise HTTPException(status_code=403, detail="Purchase token belongs to another tenant")
        if row["is_used"]:
            raise HTTPException(status_code=409, detail="Purchase token already used")
        if isinstance(row["expires_at"], datetime) and row["expires_at"] < datetime.utcnow():
            raise HTTPException(status_code=410, detail="Purchase token expired")

        entitlements = json.loads(row["entitlements_json"]) if row.get("entitlements_json") else []

        cur.execute(
            "UPDATE tenants SET plan_tier = %s WHERE id = %s",
            (row["plan_tier"], context["tenant_id"]),
        )

        for ent in entitlements:
            cur.execute(
                """
                INSERT INTO entitlements (tenant_id, vertical)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE vertical = VALUES(vertical)
                """,
                (context["tenant_id"], ent),
            )

        raw_api_key = f"pd_{secrets.token_urlsafe(24)}"
        cur.execute(
            """
            INSERT INTO api_keys (tenant_id, key_hash, key_prefix, label, active)
            VALUES (%s, %s, %s, %s, 1)
            """,
            (context["tenant_id"], hash_secret(raw_api_key), raw_api_key[:8], payload.api_key_label.strip() or "purchased-key"),
        )

        cur.execute(
            """
            UPDATE purchase_tokens
            SET is_used = 1, used_at = NOW(), used_by_user_id = %s
            WHERE id = %s
            """,
            (context["user_id"], row["id"]),
        )

        conn.commit()
        return {
            "tenant_id": context["tenant_id"],
            "tenant_name": context["tenant_name"],
            "plan_tier": row["plan_tier"],
            "entitlements": entitlements,
            "api_key": raw_api_key,
            "api_key_prefix": raw_api_key[:8],
        }
    finally:
        cur.close()
        conn.close()


def fetch_client_api_keys(tenant_id):
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT key_prefix, label, active, created_at, last_used_at
            FROM api_keys
            WHERE tenant_id = %s
            ORDER BY created_at DESC
            """,
            (tenant_id,),
        )
        rows = cur.fetchall()
        return [
            {
                "key_prefix": row["key_prefix"],
                "label": row["label"],
                "active": bool(row["active"]),
                "created_at": serialize_dt(row["created_at"]),
                "last_used_at": serialize_dt(row["last_used_at"]),
            }
            for row in rows
        ]
    finally:
        cur.close()
        conn.close()


def create_api_key_for_tenant(tenant_id, label, deactivate_existing=False):
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT id, active FROM tenants WHERE id = %s", (tenant_id,))
        tenant_row = cur.fetchone()
        if not tenant_row:
            raise HTTPException(status_code=404, detail="Tenant not found")
        if not tenant_row[1]:
            raise HTTPException(status_code=409, detail="Tenant is inactive")

        if deactivate_existing:
            cur.execute("UPDATE api_keys SET active = 0 WHERE tenant_id = %s AND active = 1", (tenant_id,))

        raw_api_key = f"pd_{secrets.token_urlsafe(24)}"
        cur.execute(
            """
            INSERT INTO api_keys (tenant_id, key_hash, key_prefix, label, active)
            VALUES (%s, %s, %s, %s, 1)
            """,
            (tenant_id, hash_secret(raw_api_key), raw_api_key[:8], label.strip() or "rotated-key"),
        )
        conn.commit()
        return {
            "tenant_id": tenant_id,
            "api_key": raw_api_key,
            "api_key_prefix": raw_api_key[:8],
            "label": label.strip() or "rotated-key",
            "deactivated_existing": bool(deactivate_existing),
        }
    finally:
        cur.close()
        conn.close()


def revoke_api_key_by_prefix(tenant_id, key_prefix):
    prefix = (key_prefix or "").strip()
    if not prefix:
        raise HTTPException(status_code=400, detail="key_prefix is required")

    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE api_keys SET active = 0 WHERE tenant_id = %s AND key_prefix = %s",
            (tenant_id, prefix),
        )
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="No matching key for tenant")
        conn.commit()
        return {
            "tenant_id": tenant_id,
            "key_prefix": prefix,
            "revoked": True,
        }
    finally:
        cur.close()
        conn.close()


def set_tenant_active(tenant_id, active):
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("UPDATE tenants SET active = %s WHERE id = %s", (1 if active else 0, tenant_id))
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="Tenant not found")
        if not active:
            cur.execute("UPDATE api_keys SET active = 0 WHERE tenant_id = %s", (tenant_id,))
        conn.commit()
        return {
            "tenant_id": tenant_id,
            "active": bool(active),
        }
    finally:
        cur.close()
        conn.close()


def _classify_trend(points):
    if not points or len(points) < 2:
        return {"direction": "flat", "delta": 0.0}
    first = float(points[0].get("signal_value", 0.0))
    last = float(points[-1].get("signal_value", 0.0))
    delta = round(last - first, 1)
    if delta >= 5:
        direction = "rising"
    elif delta <= -5:
        direction = "falling"
    else:
        direction = "flat"
    return {"direction": direction, "delta": delta}


def _condition_risk_points(condition_text):
    condition = str(condition_text or "").lower()
    if any(w in condition for w in ("thunder", "storm", "blizzard", "freezing", "ice", "snow")):
        return 45
    if any(w in condition for w in ("heavy rain", "rain", "drizzle", "shower", "sleet")):
        return 30
    if any(w in condition for w in ("fog", "mist", "overcast", "cloudy")):
        return 10
    return 0


def _build_restaurant_recommendation(opportunity, risk, trend_direction):
    if opportunity >= 70 and risk <= 35:
        return "Increase prep 15-20% and schedule extra front-line staff for the next service block."
    if opportunity < 50 or risk > 60:
        return "Reduce prep 10-15%, tighten perishables, and prioritize delivery/takeout channels."
    if trend_direction == "rising":
        return "Maintain normal prep and preload quick-turn menu items ahead of the next 2-3 hours."
    return "Operate at baseline prep and monitor conditions hourly before changing staffing."


def fetch_restaurant_operations_insight(context):
    entitlements = fetch_entitlements(context["tenant_id"])
    if "restaurant" not in entitlements:
        raise HTTPException(status_code=403, detail="Tenant does not have restaurant access")

    demand = fetch_latest_signal("restaurant")
    trend_points = fetch_signal_trend("restaurant", points=6)
    trend = _classify_trend(trend_points)
    details = demand.get("details", {}) if isinstance(demand.get("details"), dict) else {}

    congestion_pct = float(details.get("traffic_congestion_pct") or 0.0)
    avg_aqi = float(details.get("avg_aqi") or 0.0)
    temp_f = details.get("temp_f")
    condition = details.get("condition")

    weather_risk = _condition_risk_points(condition)
    congestion_risk = min(35.0, round(congestion_pct * 0.35, 1))
    aqi_risk = 0.0 if avg_aqi <= 50 else min(20.0, round((avg_aqi - 50) * 0.2, 1))
    operating_risk_score = min(100.0, round(weather_risk + congestion_risk + aqi_risk, 1))

    comfort = 100.0
    if temp_f is not None:
        try:
            temp = float(temp_f)
            if temp < 40 or temp > 95:
                comfort -= 25
            elif temp < 50 or temp > 88:
                comfort -= 12
        except (TypeError, ValueError):
            pass
    if avg_aqi > 50:
        comfort -= min(35.0, round((avg_aqi - 50) * 0.35, 1))
    comfort -= min(20.0, float(weather_risk) * 0.45)
    outdoor_comfort_score = max(0.0, round(comfort, 1))

    opportunity = float(demand.get("signal_value") or 0.0)
    recommendation = _build_restaurant_recommendation(opportunity, operating_risk_score, trend["direction"])

    positive_drivers = []
    negative_drivers = []
    if congestion_pct >= 55:
        positive_drivers.append(f"High road activity supports walk-in potential ({congestion_pct:.1f}% congestion).")
    elif congestion_pct <= 20:
        negative_drivers.append("Road activity is light, which can reduce walk-in demand.")

    condition_l = str(condition or "").lower()
    if any(w in condition_l for w in ("sunny", "clear", "partly")):
        positive_drivers.append(f"Weather is favorable ({condition or 'clear'}).")
    elif _condition_risk_points(condition) >= 30:
        negative_drivers.append(f"Weather risk is elevated ({condition or 'adverse conditions'}).")

    if avg_aqi and avg_aqi <= 60:
        positive_drivers.append(f"Air quality is supportive for outdoor traffic (AQI {avg_aqi:.1f}).")
    elif avg_aqi > 100:
        negative_drivers.append(f"Air quality may suppress outdoor demand (AQI {avg_aqi:.1f}).")

    if not positive_drivers:
        positive_drivers.append("Demand conditions are stable without a dominant positive trigger.")
    if not negative_drivers:
        negative_drivers.append("No major risk flags at the moment.")

    stream_health = fetch_stream_health()
    considered = [row for row in stream_health if row.get("stream_name") in {"traffic", "weather", "air_quality", "airport"}]
    if considered:
        status_points = {"ok": 1.0, "degraded": 0.6, "down": 0.2}
        confidence = round(
            (sum(status_points.get(str(r.get("status", "")).lower(), 0.2) for r in considered) / len(considered)) * 100,
            1,
        )
    else:
        confidence = 40.0

    return {
        "tenant": {
            "tenant_id": context["tenant_id"],
            "tenant_name": context.get("tenant_name"),
            "plan_tier": context.get("plan_tier"),
        },
        "opportunity_score": round(opportunity, 1),
        "operating_risk_score": operating_risk_score,
        "outdoor_comfort_score": outdoor_comfort_score,
        "trend_3h": trend,
        "drivers": {
            "positive": positive_drivers[:3],
            "negative": negative_drivers[:3],
        },
        "recommendation": recommendation,
        "confidence_score": confidence,
        "source": {
            "computed_at": demand.get("computed_at"),
            "window_start": demand.get("window_start"),
            "window_end": demand.get("window_end"),
        },
    }


def build_dashboard_summary(context):
    entitlements = fetch_entitlements(context["tenant_id"])
    signals = {}
    trends = {}
    for vertical in entitlements:
        if vertical in VERTICAL_SIGNAL_MAP:
            signals[vertical] = fetch_latest_signal(vertical)
            trends[vertical] = fetch_signal_trend(vertical, points=24)
    return {
        "tenant": {
            **context,
            "entitlements": entitlements,
        },
        "signals": signals,
        "trends": trends,
        "stream_health": fetch_stream_health(),
    }


def render_dashboard_html():
    return """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Public Data Atlas</title>
    <style>
        :root {
            --sand-1: #f5efe4;
            --sand-2: #efe5d3;
            --panel: rgba(255, 250, 241, 0.88);
            --panel-strong: #fff8ee;
            --ink: #1e2a25;
            --muted: #5f6b64;
            --line: rgba(34, 43, 35, 0.12);
            --teal: #0f766e;
            --amber: #b8691c;
            --red: #b7412b;
            --green: #1f7a3d;
            --shadow: 0 18px 50px rgba(25, 38, 32, 0.16);
            --radius-xl: 32px;
            --radius-lg: 22px;
            --radius-md: 14px;
        }

        * {
            box-sizing: border-box;
        }

        body {
            margin: 0;
            min-height: 100vh;
            color: var(--ink);
            font-family: "Palatino Linotype", "Book Antiqua", Palatino, serif;
            background:
                radial-gradient(1200px 500px at -8% -12%, rgba(15, 118, 110, 0.28), transparent 62%),
                radial-gradient(900px 420px at 106% 8%, rgba(184, 105, 28, 0.22), transparent 60%),
                linear-gradient(130deg, var(--sand-1), var(--sand-2));
            overflow-x: hidden;
        }

        .bg-ribbon {
            position: fixed;
            inset: auto -20vw -24vh auto;
            width: 72vw;
            height: 56vh;
            border-radius: 50%;
            background: radial-gradient(circle, rgba(15, 118, 110, 0.14), rgba(15, 118, 110, 0));
            pointer-events: none;
            animation: drift 14s ease-in-out infinite alternate;
            z-index: 0;
        }

        @keyframes drift {
            from { transform: translateY(0) translateX(0); }
            to { transform: translateY(-16px) translateX(-12px); }
        }

        .shell {
            position: relative;
            z-index: 1;
            width: min(1240px, calc(100vw - 30px));
            margin: 26px auto 34px;
            display: grid;
            gap: 18px;
        }

        .glass {
            border: 1px solid var(--line);
            background: var(--panel);
            backdrop-filter: blur(10px);
            box-shadow: var(--shadow);
        }

        .hero {
            border-radius: var(--radius-xl);
            padding: 28px;
            display: grid;
            gap: 18px;
            overflow: hidden;
            position: relative;
        }

        .hero::before {
            content: "";
            position: absolute;
            inset: -40% auto auto -18%;
            width: 340px;
            height: 340px;
            border-radius: 50%;
            background: radial-gradient(circle, rgba(184, 105, 28, 0.24), transparent 62%);
            pointer-events: none;
        }

        .hero-top {
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            gap: 14px;
            flex-wrap: wrap;
        }

        .kicker {
            margin: 0 0 8px;
            text-transform: uppercase;
            letter-spacing: 0.2em;
            font-size: 12px;
            color: var(--teal);
        }

        h1 {
            margin: 0;
            font-size: clamp(38px, 6vw, 78px);
            line-height: 0.92;
            font-weight: 500;
            letter-spacing: -0.02em;
        }

        .subtitle {
            margin: 12px 0 0;
            max-width: 750px;
            color: var(--muted);
            font-size: 18px;
            line-height: 1.5;
        }

        .tenant-chip {
            border-radius: 999px;
            padding: 11px 14px;
            background: rgba(15, 118, 110, 0.12);
            color: var(--teal);
            font-size: 13px;
            letter-spacing: 0.06em;
            text-transform: uppercase;
            min-width: 260px;
            text-align: center;
        }

        .hero-stats {
            display: grid;
            grid-template-columns: repeat(3, minmax(0, 1fr));
            gap: 12px;
        }

        .stat {
            border: 1px solid var(--line);
            border-radius: var(--radius-md);
            background: var(--panel-strong);
            padding: 12px;
        }

        .stat-title {
            margin: 0 0 6px;
            font-size: 12px;
            letter-spacing: 0.14em;
            text-transform: uppercase;
            color: var(--muted);
        }

        .stat-value {
            margin: 0;
            font-size: 24px;
            line-height: 1;
        }

        .panel {
            border-radius: var(--radius-lg);
            padding: 18px;
        }

        .panel-head {
            display: flex;
            justify-content: space-between;
            align-items: center;
            gap: 12px;
            margin-bottom: 14px;
            flex-wrap: wrap;
        }

        .panel-title {
            margin: 0;
            font-size: 26px;
            letter-spacing: -0.02em;
        }

        .meta {
            color: var(--muted);
            font-size: 13px;
            line-height: 1.4;
        }

        .controls {
            display: grid;
            grid-template-columns: 1.2fr auto auto;
            gap: 10px;
            align-items: end;
        }

        label {
            display: block;
            margin: 0 0 7px;
            font-size: 11px;
            letter-spacing: 0.14em;
            text-transform: uppercase;
            color: var(--muted);
        }

        input {
            width: 100%;
            border: 1px solid var(--line);
            border-radius: var(--radius-md);
            padding: 13px 14px;
            background: rgba(255, 255, 255, 0.72);
            font-size: 15px;
            color: var(--ink);
        }

        button {
            border: 0;
            border-radius: var(--radius-md);
            padding: 13px 14px;
            font-weight: 700;
            font-size: 13px;
            cursor: pointer;
            transition: transform 120ms ease, opacity 140ms ease;
        }

        button:hover {
            transform: translateY(-1px);
        }

        .btn-primary {
            background: linear-gradient(130deg, #0f766e, #155e58);
            color: #fff;
        }

        .btn-muted {
            background: rgba(34, 43, 35, 0.1);
            color: var(--ink);
        }

        .toolbar {
            margin-top: 11px;
            display: flex;
            gap: 8px;
            align-items: center;
            flex-wrap: wrap;
        }

        .toggle {
            display: inline-flex;
            align-items: center;
            gap: 8px;
            padding: 8px 11px;
            border-radius: 999px;
            background: rgba(34, 43, 35, 0.08);
            font-size: 12px;
            text-transform: uppercase;
            letter-spacing: 0.11em;
            cursor: pointer;
            user-select: none;
        }

        .toggle.on {
            background: rgba(15, 118, 110, 0.16);
            color: var(--teal);
        }

        .range-group {
            display: inline-flex;
            border: 1px solid var(--line);
            border-radius: 999px;
            overflow: hidden;
            background: rgba(255, 255, 255, 0.65);
        }

        .range-btn {
            border: 0;
            border-right: 1px solid var(--line);
            background: transparent;
            color: var(--muted);
            padding: 8px 12px;
            border-radius: 0;
            font-size: 12px;
            letter-spacing: 0.08em;
            text-transform: uppercase;
        }

        .range-btn:last-child {
            border-right: 0;
        }

        .range-btn.active {
            background: rgba(15, 118, 110, 0.13);
            color: var(--teal);
        }

        .signals {
            display: grid;
            grid-template-columns: repeat(3, minmax(0, 1fr));
            gap: 14px;
        }

        .signal-card {
            border: 1px solid var(--line);
            border-radius: 18px;
            padding: 14px;
            background: var(--panel-strong);
            display: grid;
            gap: 10px;
            animation: cardIn 420ms ease both;
        }

        .signal-card:nth-child(2) { animation-delay: 70ms; }
        .signal-card:nth-child(3) { animation-delay: 140ms; }

        @keyframes cardIn {
            from { transform: translateY(8px); opacity: 0; }
            to { transform: translateY(0); opacity: 1; }
        }

        .signal-head {
            display: flex;
            justify-content: space-between;
            gap: 10px;
            align-items: flex-start;
        }

        .signal-name {
            margin: 0;
            font-size: 13px;
            letter-spacing: 0.14em;
            text-transform: uppercase;
            color: var(--muted);
        }

        .signal-score {
            margin: 5px 0 0;
            font-size: clamp(32px, 5vw, 52px);
            line-height: 0.92;
        }

        .badge {
            padding: 7px 10px;
            border-radius: 999px;
            font-size: 11px;
            letter-spacing: 0.13em;
            text-transform: uppercase;
            white-space: nowrap;
        }

        .badge.low { background: rgba(15, 118, 110, 0.11); color: var(--teal); }
        .badge.safe, .badge.ok { background: rgba(31, 122, 61, 0.12); color: var(--green); }
        .badge.moderate, .badge.degraded { background: rgba(184, 105, 28, 0.14); color: var(--amber); }
        .badge.high, .badge.poor, .badge.down { background: rgba(183, 65, 43, 0.14); color: var(--red); }

        .sparkline {
            border: 1px solid var(--line);
            border-radius: 12px;
            background: rgba(255, 255, 255, 0.55);
            padding: 6px;
        }

        .micro-grid {
            display: grid;
            grid-template-columns: repeat(3, minmax(0, 1fr));
            gap: 8px;
        }

        .micro {
            border: 1px solid var(--line);
            border-radius: 10px;
            padding: 7px;
            background: rgba(255, 255, 255, 0.58);
        }

        .micro-label {
            margin: 0 0 3px;
            font-size: 10px;
            text-transform: uppercase;
            letter-spacing: 0.12em;
            color: var(--muted);
        }

        .micro-value {
            margin: 0;
            font-size: 18px;
            line-height: 1;
        }

        .details {
            border-top: 1px solid var(--line);
            padding-top: 8px;
            display: grid;
            gap: 3px;
            color: var(--muted);
            font-size: 12px;
        }

        .details-row {
            display: flex;
            justify-content: space-between;
            gap: 8px;
        }

        .details-row strong {
            color: var(--ink);
            font-weight: 600;
        }

        .health-grid {
            display: grid;
            grid-template-columns: repeat(2, minmax(0, 1fr));
            gap: 12px;
        }

        .health-card {
            border: 1px solid var(--line);
            border-radius: 14px;
            background: var(--panel-strong);
            padding: 12px;
        }

        .health-head {
            display: flex;
            justify-content: space-between;
            align-items: center;
            gap: 8px;
            margin-bottom: 7px;
        }

        .health-name {
            margin: 0;
            font-size: 19px;
            text-transform: capitalize;
        }

        .health-meta {
            margin: 0;
            color: var(--muted);
            font-size: 13px;
            line-height: 1.45;
        }

        .error {
            margin-top: 8px;
            color: var(--red);
            font-size: 13px;
        }

        .hidden {
            display: none;
        }

        @media (max-width: 1080px) {
            .signals {
                grid-template-columns: 1fr 1fr;
            }
        }

        @media (max-width: 860px) {
            .controls {
                grid-template-columns: 1fr;
            }

            .hero-stats {
                grid-template-columns: 1fr;
            }

            .signals,
            .health-grid {
                grid-template-columns: 1fr;
            }
        }
    </style>
</head>
<body>
    <div class="bg-ribbon"></div>
    <main class="shell">
        <section class="hero glass">
            <div class="hero-top">
                <div>
                    <p class="kicker">Live Urban Intelligence</p>
                    <h1>Public Data Atlas</h1>
                    <p class="subtitle">Demand pressure, route risk, and outdoor safety in one operational cockpit. Authenticate with a tenant key to inspect live scores, trend lines, and stream health.</p>
                </div>
                <div class="tenant-chip" id="tenant-chip">No tenant connected</div>
            </div>
            <div class="hero-stats">
                <article class="stat">
                    <p class="stat-title">Latest Compute</p>
                    <p class="stat-value" id="latest-compute">n/a</p>
                </article>
                <article class="stat">
                    <p class="stat-title">Last Refresh</p>
                    <p class="stat-value" id="last-refresh">n/a</p>
                </article>
                <article class="stat">
                    <p class="stat-title">Streams Healthy</p>
                    <p class="stat-value" id="healthy-count">0/0</p>
                </article>
            </div>
        </section>

        <section class="panel glass">
            <div class="panel-head">
                <h2 class="panel-title">Access and Controls</h2>
                <p class="meta" id="tenant-meta">Use the seeded demo API key or your own tenant key.</p>
            </div>
            <div class="controls">
                <div>
                    <label for="api-key">API Key</label>
                    <input id="api-key" type="password" placeholder="pd_..." />
                </div>
                <button id="load-btn" class="btn-primary">Load Dashboard</button>
                <button id="clear-btn" class="btn-muted">Clear</button>
            </div>
            <div class="toolbar">
                <div class="toggle" id="auto-toggle">Auto refresh: off</div>
                <div class="range-group" id="range-group">
                    <button class="range-btn" data-hours="6">6h</button>
                    <button class="range-btn active" data-hours="24">24h</button>
                    <button class="range-btn" data-hours="168">7d</button>
                </div>
                <p class="meta" id="signal-meta">No signal data loaded</p>
            </div>
            <p class="error hidden" id="error-box"></p>
        </section>

        <section class="panel glass">
            <div class="panel-head">
                <h2 class="panel-title">Signals</h2>
                <p class="meta" id="signal-updated">Waiting for first load</p>
            </div>
            <div class="signals" id="signal-grid"></div>
        </section>

        <section class="panel glass">
            <div class="panel-head">
                <h2 class="panel-title">Stream Health</h2>
            </div>
            <div class="health-grid" id="health-grid"></div>
        </section>
    </main>

    <script>
        const apiInput = document.getElementById('api-key');
        const loadBtn = document.getElementById('load-btn');
        const clearBtn = document.getElementById('clear-btn');
        const autoToggle = document.getElementById('auto-toggle');
        const rangeGroup = document.getElementById('range-group');

        const tenantChip = document.getElementById('tenant-chip');
        const tenantMeta = document.getElementById('tenant-meta');
        const signalMeta = document.getElementById('signal-meta');
        const latestCompute = document.getElementById('latest-compute');
        const lastRefresh = document.getElementById('last-refresh');
        const healthyCount = document.getElementById('healthy-count');
        const signalUpdated = document.getElementById('signal-updated');

        const signalGrid = document.getElementById('signal-grid');
        const healthGrid = document.getElementById('health-grid');
        const errorBox = document.getElementById('error-box');

        const signalLabels = {
            restaurant: 'Restaurant Demand',
            logistics: 'Logistics Route Risk',
            outdoor: 'Outdoor Safety'
        };

        const storedKey = localStorage.getItem('public-data-api-key');
        const storedAutoRefresh = localStorage.getItem('public-data-auto-refresh') === 'on';
        const storedRange = Number(localStorage.getItem('public-data-range-hours') || '24');

        const REFRESH_INTERVAL_MS = 45000;
        let refreshTimer = null;
        let selectedRangeHours = [6, 24, 168].includes(storedRange) ? storedRange : 24;

        if (storedKey) {
            apiInput.value = storedKey;
        }

        function setError(message) {
            if (!message) {
                errorBox.classList.add('hidden');
                errorBox.textContent = '';
                return;
            }
            errorBox.classList.remove('hidden');
            errorBox.textContent = message;
        }

        function badgeClass(label) {
            return String(label || '').toLowerCase();
        }

        function formatDate(value) {
            if (!value) {
                return 'n/a';
            }
            const date = new Date(value);
            return Number.isNaN(date.getTime()) ? value : date.toLocaleString();
        }

        function formatTimeOnly(value) {
            if (!value) {
                return 'n/a';
            }
            const date = new Date(value);
            return Number.isNaN(date.getTime()) ? value : date.toLocaleTimeString();
        }

        function formatMetric(value) {
            const n = Number(value);
            if (Number.isNaN(n)) {
                return String(value);
            }
            return n.toFixed(1);
        }

        function getFilteredTrend(points, hours) {
            const now = Date.now();
            const windowMs = hours * 60 * 60 * 1000;
            return (points || []).filter((point) => {
                const ts = new Date(point.computed_at).getTime();
                return Number.isFinite(ts) && (now - ts) <= windowMs;
            });
        }

        function sparklineSvg(points) {
            const values = (points || []).map((p) => Number(p.signal_value)).filter((v) => !Number.isNaN(v));
            if (!values.length) {
                return '<svg viewBox="0 0 280 66" width="100%" height="66"><text x="10" y="36" fill="#5f6b64" font-size="12">No trend points in this range</text></svg>';
            }

            const min = Math.min(...values);
            const max = Math.max(...values);
            const span = Math.max(max - min, 1);
            const width = 280;
            const height = 66;
            const stepX = values.length > 1 ? width / (values.length - 1) : width;
            const coords = values.map((v, idx) => {
                const x = idx * stepX;
                const normalized = (v - min) / span;
                const y = height - normalized * (height - 12) - 6;
                return `${x.toFixed(1)},${y.toFixed(1)}`;
            });

            const line = coords.join(' ');
            const area = `0,${height} ${line} ${width},${height}`;

            return `
                <svg viewBox="0 0 ${width} ${height}" width="100%" height="66" preserveAspectRatio="none">
                    <polygon points="${area}" fill="rgba(15,118,110,0.12)"></polygon>
                    <polyline points="${line}" fill="none" stroke="#0f766e" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"></polyline>
                </svg>
            `;
        }

        function renderSignals(signals, trends) {
            const entries = Object.entries(signals || {});
            const detailsHtml = (details) => {
                const rows = Object.entries(details || {});
                if (!rows.length) {
                    return '<div class="details-row"><span>No breakdown available</span><strong>n/a</strong></div>';
                }
                return rows.slice(0, 7).map(([key, value]) => `
                    <div class="details-row">
                        <span>${key.replaceAll('_', ' ')}</span>
                        <strong>${formatMetric(value)}</strong>
                    </div>
                `).join('');
            };

            signalGrid.innerHTML = entries.map(([vertical, data]) => {
                const trend = getFilteredTrend((trends || {})[vertical] || [], selectedRangeHours);
                const trendValues = trend.map((point) => Number(point.signal_value)).filter((v) => !Number.isNaN(v));
                const min = trendValues.length ? Math.min(...trendValues).toFixed(1) : 'n/a';
                const max = trendValues.length ? Math.max(...trendValues).toFixed(1) : 'n/a';
                const delta = trendValues.length > 1 ? (trendValues[trendValues.length - 1] - trendValues[0]).toFixed(1) : '0.0';

                return `
                    <article class="signal-card">
                        <div class="signal-head">
                            <div>
                                <p class="signal-name">${signalLabels[vertical] || vertical}</p>
                                <p class="signal-score">${typeof data.signal_value === 'number' ? data.signal_value.toFixed(1) : 'n/a'}</p>
                            </div>
                            <span class="badge ${badgeClass(data.signal_label)}">${data.signal_label || 'unknown'}</span>
                        </div>
                        <div class="sparkline">${sparklineSvg(trend)}</div>
                        <div class="micro-grid">
                            <div class="micro">
                                <p class="micro-label">Range Min</p>
                                <p class="micro-value">${min}</p>
                            </div>
                            <div class="micro">
                                <p class="micro-label">Range Max</p>
                                <p class="micro-value">${max}</p>
                            </div>
                            <div class="micro">
                                <p class="micro-label">Delta</p>
                                <p class="micro-value">${delta}</p>
                            </div>
                        </div>
                        <p class="meta">Window: ${formatDate(data.window_start)} to ${formatDate(data.window_end)}</p>
                        <p class="meta">Streams: ${(data.contributing_streams || []).join(', ') || 'n/a'}</p>
                        <div class="details">${detailsHtml(data.details)}</div>
                    </article>
                `;
            }).join('');
        }

        function renderHealth(rows) {
            const total = (rows || []).length;
            const healthy = (rows || []).filter((row) => row.status === 'ok').length;
            healthyCount.textContent = `${healthy}/${total}`;

            healthGrid.innerHTML = (rows || []).map((row) => `
                <article class="health-card">
                    <div class="health-head">
                        <h3 class="health-name">${row.stream_name}</h3>
                        <span class="badge ${badgeClass(row.status)}">${row.status}</span>
                    </div>
                    <p class="health-meta">Last success: ${formatDate(row.last_success_at)}</p>
                    <p class="health-meta">Rows last run: ${row.rows_last_run}</p>
                    <p class="health-meta">Checked: ${formatDate(row.checked_at)}</p>
                </article>
            `).join('');
        }

        function setRange(hours) {
            selectedRangeHours = hours;
            localStorage.setItem('public-data-range-hours', String(hours));
            rangeGroup.querySelectorAll('.range-btn').forEach((btn) => {
                const btnHours = Number(btn.dataset.hours);
                btn.classList.toggle('active', btnHours === selectedRangeHours);
            });
        }

        function setAutoRefresh(enabled) {
            if (refreshTimer) {
                clearInterval(refreshTimer);
                refreshTimer = null;
            }

            if (enabled) {
                refreshTimer = setInterval(() => {
                    if (apiInput.value.trim()) {
                        loadDashboard(true);
                    }
                }, REFRESH_INTERVAL_MS);
                autoToggle.classList.add('on');
                autoToggle.textContent = `Auto refresh: on (${Math.floor(REFRESH_INTERVAL_MS / 1000)}s)`;
                localStorage.setItem('public-data-auto-refresh', 'on');
            } else {
                autoToggle.classList.remove('on');
                autoToggle.textContent = 'Auto refresh: off';
                localStorage.setItem('public-data-auto-refresh', 'off');
            }
        }

        async function loadDashboard(isAuto = false) {
            const apiKey = apiInput.value.trim();
            setError('');
            if (!apiKey) {
                setError('Enter an API key first.');
                return;
            }

            if (!isAuto) {
                loadBtn.disabled = true;
                loadBtn.textContent = 'Loading...';
            }

            try {
                const response = await fetch('/v1/dashboard/summary', {
                    headers: {
                        'X-API-Key': apiKey
                    }
                });
                const payload = await response.json();
                if (!response.ok) {
                    throw new Error(payload.detail || 'Request failed');
                }

                localStorage.setItem('public-data-api-key', apiKey);
                tenantChip.textContent = `${payload.tenant.tenant_name} | ${payload.tenant.plan_tier}`;
                tenantMeta.textContent = `Entitlements: ${payload.tenant.entitlements.join(', ') || 'none'}`;
                signalMeta.textContent = `Viewing trend range: ${selectedRangeHours === 168 ? '7 days' : `${selectedRangeHours} hours`}`;

                const dates = Object.values(payload.signals || {}).map((item) => item.computed_at).filter(Boolean).sort();
                const latest = dates.length ? dates[dates.length - 1] : null;
                latestCompute.textContent = formatTimeOnly(latest);
                lastRefresh.textContent = new Date().toLocaleTimeString();
                signalUpdated.textContent = latest ? `Signal compute timestamp: ${formatDate(latest)}` : 'No signal timestamps yet';

                renderSignals(payload.signals, payload.trends || {});
                renderHealth(payload.stream_health || []);
            } catch (error) {
                setError(error.message || String(error));
            } finally {
                if (!isAuto) {
                    loadBtn.disabled = false;
                    loadBtn.textContent = 'Load Dashboard';
                }
            }
        }

        loadBtn.addEventListener('click', () => loadDashboard(false));

        apiInput.addEventListener('keydown', (event) => {
            if (event.key === 'Enter') {
                loadDashboard(false);
            }
        });

        clearBtn.addEventListener('click', () => {
            localStorage.removeItem('public-data-api-key');
            apiInput.value = '';
            tenantChip.textContent = 'No tenant connected';
            tenantMeta.textContent = 'Use the seeded demo API key or your own tenant key.';
            signalMeta.textContent = 'No signal data loaded';
            latestCompute.textContent = 'n/a';
            lastRefresh.textContent = 'n/a';
            healthyCount.textContent = '0/0';
            signalUpdated.textContent = 'Waiting for first load';
            signalGrid.innerHTML = '';
            healthGrid.innerHTML = '';
            setAutoRefresh(false);
            setError('');
        });

        autoToggle.addEventListener('click', () => {
            const currentlyOn = localStorage.getItem('public-data-auto-refresh') === 'on';
            setAutoRefresh(!currentlyOn);
        });

        rangeGroup.addEventListener('click', (event) => {
            const button = event.target.closest('.range-btn');
            if (!button) {
                return;
            }
            const hours = Number(button.dataset.hours);
            if (!Number.isFinite(hours)) {
                return;
            }
            setRange(hours);
            if (apiInput.value.trim()) {
                loadDashboard(true);
            }
        });

        setRange(selectedRangeHours);
        setAutoRefresh(storedAutoRefresh);

        if (storedKey) {
            loadDashboard(false);
        }
    </script>
</body>
</html>
    """


def render_provider_html():
    return """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Provider Console</title>
    <style>
        :root {
            --bg-a: #f0efe9;
            --bg-b: #e6ecf3;
            --panel: rgba(255, 255, 255, 0.84);
            --ink: #1c2b34;
            --muted: #5b6e79;
            --line: rgba(28, 43, 52, 0.12);
            --blue: #0b5ad9;
            --cyan: #0f766e;
            --green: #1f7a3d;
            --amber: #b8691c;
            --red: #b7412b;
            --radius: 16px;
            --shadow: 0 16px 40px rgba(17, 30, 42, 0.14);
        }

        * { box-sizing: border-box; }

        body {
            margin: 0;
            min-height: 100vh;
            color: var(--ink);
            font-family: "Segoe UI", Tahoma, sans-serif;
            background:
                radial-gradient(900px 360px at -10% -20%, rgba(11, 90, 217, 0.22), transparent 65%),
                radial-gradient(700px 320px at 110% 0%, rgba(15, 118, 110, 0.16), transparent 60%),
                linear-gradient(130deg, var(--bg-a), var(--bg-b));
        }

        .shell {
            width: min(1260px, calc(100vw - 28px));
            margin: 20px auto 34px;
            display: grid;
            gap: 14px;
        }

        .panel {
            border: 1px solid var(--line);
            border-radius: var(--radius);
            background: var(--panel);
            box-shadow: var(--shadow);
            padding: 16px;
        }

        .hero {
            display: flex;
            justify-content: space-between;
            gap: 12px;
            flex-wrap: wrap;
            align-items: flex-start;
        }

        h1 {
            margin: 0;
            font-size: clamp(34px, 5vw, 56px);
            line-height: 0.95;
            letter-spacing: -0.02em;
        }

        .sub {
            margin: 8px 0 0;
            max-width: 680px;
            color: var(--muted);
            font-size: 16px;
        }

        .owner-chip {
            border-radius: 999px;
            background: rgba(11, 90, 217, 0.1);
            color: var(--blue);
            padding: 10px 14px;
            text-transform: uppercase;
            font-size: 12px;
            letter-spacing: 0.1em;
        }

        .controls {
            display: grid;
            grid-template-columns: 1.2fr auto auto;
            gap: 8px;
            align-items: end;
        }

        label {
            display: block;
            margin: 0 0 6px;
            text-transform: uppercase;
            letter-spacing: 0.11em;
            color: var(--muted);
            font-size: 11px;
        }

        input, select {
            width: 100%;
            border: 1px solid var(--line);
            border-radius: 10px;
            padding: 10px 11px;
            font-size: 14px;
            color: var(--ink);
            background: rgba(255, 255, 255, 0.8);
        }

        button {
            border: 0;
            border-radius: 10px;
            padding: 11px 13px;
            cursor: pointer;
            font-weight: 700;
            font-size: 13px;
        }

        .btn-main { background: linear-gradient(130deg, #0b5ad9, #0b3fc0); color: #fff; }
        .btn-muted { background: rgba(28, 43, 52, 0.1); color: var(--ink); }

        .meta { color: var(--muted); font-size: 13px; margin: 8px 0 0; }
        .error { color: var(--red); font-size: 13px; margin: 8px 0 0; }

        .kpis {
            display: grid;
            grid-template-columns: repeat(4, minmax(0, 1fr));
            gap: 10px;
        }

        .kpi {
            border: 1px solid var(--line);
            border-radius: 12px;
            padding: 10px;
            background: rgba(255, 255, 255, 0.7);
        }

        .kpi-title { margin: 0; font-size: 11px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.1em; }
        .kpi-value { margin: 6px 0 0; font-size: 30px; line-height: 1; }

        .grid-2 {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 12px;
        }

        .section-title { margin: 0 0 10px; font-size: 24px; letter-spacing: -0.01em; }

        .signal-list, .new-key-box {
            border: 1px solid var(--line);
            border-radius: 12px;
            background: rgba(255, 255, 255, 0.66);
            padding: 10px;
            display: grid;
            gap: 8px;
        }

        .signal-item {
            display: flex;
            justify-content: space-between;
            gap: 10px;
            align-items: center;
            border-bottom: 1px dashed var(--line);
            padding-bottom: 7px;
        }

        .signal-item:last-child { border-bottom: 0; padding-bottom: 0; }

        .badge {
            border-radius: 999px;
            padding: 6px 9px;
            font-size: 11px;
            text-transform: uppercase;
            letter-spacing: 0.1em;
        }

        .badge.ok { background: rgba(31, 122, 61, 0.14); color: var(--green); }
        .badge.degraded { background: rgba(184, 105, 28, 0.14); color: var(--amber); }
        .badge.down { background: rgba(183, 65, 43, 0.14); color: var(--red); }

        .tenant-form {
            display: grid;
            grid-template-columns: 1fr 1fr 1fr;
            gap: 8px;
        }

        .ent-checkboxes {
            grid-column: 1 / -1;
            display: flex;
            gap: 14px;
            flex-wrap: wrap;
        }

        .ent-checkboxes label {
            display: inline-flex;
            align-items: center;
            gap: 6px;
            margin: 0;
            text-transform: none;
            letter-spacing: 0;
            font-size: 13px;
            color: var(--ink);
        }

        table {
            width: 100%;
            border-collapse: collapse;
            font-size: 13px;
            overflow: hidden;
            border-radius: 10px;
        }

        thead th {
            text-align: left;
            padding: 8px;
            background: rgba(28, 43, 52, 0.08);
            color: var(--muted);
            text-transform: uppercase;
            letter-spacing: 0.08em;
            font-size: 11px;
        }

        tbody td {
            padding: 8px;
            border-top: 1px solid var(--line);
            vertical-align: top;
        }

        .mono { font-family: Consolas, Monaco, monospace; font-size: 12px; }
        .hidden { display: none; }

        @media (max-width: 1040px) {
            .kpis { grid-template-columns: 1fr 1fr; }
            .grid-2 { grid-template-columns: 1fr; }
            .tenant-form { grid-template-columns: 1fr 1fr; }
        }

        @media (max-width: 760px) {
            .controls { grid-template-columns: 1fr; }
            .tenant-form { grid-template-columns: 1fr; }
            .kpis { grid-template-columns: 1fr; }
        }
    </style>
</head>
<body>
    <main class="shell">
        <section class="panel hero">
            <div>
                <h1>Provider Console</h1>
                <p class="sub">Control center for the data platform: tenants, entitlements, stream health, and recent signal output.</p>
            </div>
            <div class="owner-chip" id="owner-chip">Provider Access</div>
        </section>

        <section class="panel">
            <div class="controls">
                <div>
                    <label for="provider-key">Session</label>
                    <input id="provider-key" type="password" placeholder="Session is managed via admin login" disabled />
                </div>
                <button id="load-btn" class="btn-main">Load Console</button>
                <button id="clear-btn" class="btn-muted">Clear</button>
            </div>
            <p class="meta" id="refresh-meta">Awaiting authentication.</p>
            <p class="error hidden" id="error-box"></p>
        </section>

        <section class="panel">
            <h2 class="section-title">Platform KPIs</h2>
            <div class="kpis">
                <article class="kpi"><p class="kpi-title">Active Tenants</p><p class="kpi-value" id="kpi-tenants">0</p></article>
                <article class="kpi"><p class="kpi-title">Active API Keys</p><p class="kpi-value" id="kpi-keys">0</p></article>
                <article class="kpi"><p class="kpi-title">Healthy Streams</p><p class="kpi-value" id="kpi-streams">0/0</p></article>
                <article class="kpi"><p class="kpi-title">Last Refresh</p><p class="kpi-value" id="kpi-refresh">n/a</p></article>
            </div>
        </section>

        <section class="panel grid-2">
            <div>
                <h2 class="section-title">Latest Signals</h2>
                <div class="signal-list" id="signal-list"></div>
            </div>
            <div>
                <h2 class="section-title">Create Tenant</h2>
                <form id="tenant-form" class="tenant-form">
                    <div>
                        <label for="tenant-name">Name</label>
                        <input id="tenant-name" required />
                    </div>
                    <div>
                        <label for="tenant-email">Contact Email</label>
                        <input id="tenant-email" type="email" required />
                    </div>
                    <div>
                        <label for="tenant-plan">Plan Tier</label>
                        <select id="tenant-plan">
                            <option value="trial">trial</option>
                            <option value="basic">basic</option>
                            <option value="pro">pro</option>
                            <option value="enterprise">enterprise</option>
                        </select>
                    </div>
                    <div>
                        <label for="tenant-key-label">API Key Label</label>
                        <input id="tenant-key-label" value="default" />
                    </div>
                    <div class="ent-checkboxes">
                        <label><input type="checkbox" value="restaurant" checked /> Restaurant</label>
                        <label><input type="checkbox" value="logistics" /> Logistics</label>
                        <label><input type="checkbox" value="outdoor" /> Outdoor</label>
                    </div>
                    <div>
                        <button class="btn-main" type="submit">Create Tenant</button>
                    </div>
                </form>
                <div id="new-key-box" class="new-key-box hidden"></div>
                <h2 class="section-title" style="margin-top:12px;">Create Purchase Token</h2>
                <form id="purchase-form" class="tenant-form">
                    <div>
                        <label for="purchase-tenant-id">Tenant ID</label>
                        <input id="purchase-tenant-id" type="number" min="1" required />
                    </div>
                    <div>
                        <label for="purchase-plan">Plan Tier</label>
                        <select id="purchase-plan">
                            <option value="trial">trial</option>
                            <option value="basic">basic</option>
                            <option value="pro">pro</option>
                            <option value="enterprise">enterprise</option>
                        </select>
                    </div>
                    <div>
                        <label for="purchase-expiry">Expiry (hours)</label>
                        <input id="purchase-expiry" type="number" min="1" max="720" value="72" />
                    </div>
                    <div class="ent-checkboxes" id="purchase-ent-checkboxes">
                        <label><input type="checkbox" value="restaurant" checked /> Restaurant</label>
                        <label><input type="checkbox" value="logistics" /> Logistics</label>
                        <label><input type="checkbox" value="outdoor" /> Outdoor</label>
                    </div>
                    <div>
                        <button class="btn-main" type="submit">Create Purchase Token</button>
                    </div>
                </form>
                <div id="purchase-result" class="new-key-box hidden"></div>
            </div>
        </section>

        <section class="panel">
            <h2 class="section-title">Tenants</h2>
            <table>
                <thead>
                    <tr>
                        <th>ID</th>
                        <th>Name</th>
                        <th>Plan</th>
                        <th>Status</th>
                        <th>Entitlements</th>
                        <th>API Keys</th>
                        <th>Last API Use</th>
                    </tr>
                </thead>
                <tbody id="tenant-rows"></tbody>
            </table>
        </section>

        <section class="panel grid-2">
            <div>
                <h2 class="section-title">Tenant Status Control</h2>
                <form id="tenant-status-form" class="tenant-form">
                    <div>
                        <label for="status-tenant-id">Tenant ID</label>
                        <input id="status-tenant-id" type="number" min="1" required />
                    </div>
                    <div>
                        <label for="status-active">Status</label>
                        <select id="status-active">
                            <option value="active">active</option>
                            <option value="inactive">inactive</option>
                        </select>
                    </div>
                    <div>
                        <button class="btn-main" type="submit">Apply Status</button>
                    </div>
                </form>
                <div id="tenant-status-result" class="new-key-box hidden"></div>
            </div>
            <div>
                <h2 class="section-title">Admin Key Actions</h2>
                <form id="admin-key-rotate-form" class="tenant-form">
                    <div>
                        <label for="admin-rotate-tenant-id">Tenant ID</label>
                        <input id="admin-rotate-tenant-id" type="number" min="1" required />
                    </div>
                    <div>
                        <label for="admin-rotate-label">Key Label</label>
                        <input id="admin-rotate-label" value="admin-rotated-key" />
                    </div>
                    <div>
                        <label><input id="admin-rotate-deactivate" type="checkbox" checked /> Deactivate existing keys</label>
                        <button class="btn-main" type="submit">Rotate Key</button>
                    </div>
                </form>
                <form id="admin-key-revoke-form" class="tenant-form" style="margin-top:8px;">
                    <div>
                        <label for="admin-revoke-tenant-id">Tenant ID</label>
                        <input id="admin-revoke-tenant-id" type="number" min="1" required />
                    </div>
                    <div>
                        <label for="admin-revoke-prefix">Key Prefix</label>
                        <input id="admin-revoke-prefix" placeholder="pd_ABC123" required />
                    </div>
                    <div>
                        <button class="btn-muted" type="submit">Revoke Key</button>
                    </div>
                </form>
                <div id="admin-key-action-result" class="new-key-box hidden"></div>
            </div>
        </section>

        <section class="panel">
            <h2 class="section-title">Recent Purchase Tokens</h2>
            <table>
                <thead>
                    <tr>
                        <th>ID</th>
                        <th>Tenant</th>
                        <th>Plan</th>
                        <th>Entitlements</th>
                        <th>Expires</th>
                        <th>Used</th>
                    </tr>
                </thead>
                <tbody id="purchase-token-rows"></tbody>
            </table>
        </section>
    </main>

    <script>
        const providerKeyInput = document.getElementById('provider-key');
        const loadBtn = document.getElementById('load-btn');
        const clearBtn = document.getElementById('clear-btn');
        const refreshMeta = document.getElementById('refresh-meta');
        const errorBox = document.getElementById('error-box');
        const ownerChip = document.getElementById('owner-chip');

        const kpiTenants = document.getElementById('kpi-tenants');
        const kpiKeys = document.getElementById('kpi-keys');
        const kpiStreams = document.getElementById('kpi-streams');
        const kpiRefresh = document.getElementById('kpi-refresh');
        const signalList = document.getElementById('signal-list');
        const tenantRows = document.getElementById('tenant-rows');
        const tenantForm = document.getElementById('tenant-form');
        const newKeyBox = document.getElementById('new-key-box');
        const purchaseForm = document.getElementById('purchase-form');
        const purchaseResult = document.getElementById('purchase-result');
        const purchaseTokenRows = document.getElementById('purchase-token-rows');
        const tenantStatusForm = document.getElementById('tenant-status-form');
        const tenantStatusResult = document.getElementById('tenant-status-result');
        const adminKeyRotateForm = document.getElementById('admin-key-rotate-form');
        const adminKeyRevokeForm = document.getElementById('admin-key-revoke-form');
        const adminKeyActionResult = document.getElementById('admin-key-action-result');

        providerKeyInput.value = 'Authenticated via cookie session';

        function setError(message) {
            if (!message) {
                errorBox.classList.add('hidden');
                errorBox.textContent = '';
                return;
            }
            errorBox.classList.remove('hidden');
            errorBox.textContent = message;
        }

        function formatDate(value) {
            if (!value) {
                return 'n/a';
            }
            const date = new Date(value);
            return Number.isNaN(date.getTime()) ? value : date.toLocaleString();
        }

        function providerHeaders() {
            return {
                'Content-Type': 'application/json'
            };
        }

        async function loadProviderDashboard() {
            setError('');
            loadBtn.disabled = true;
            loadBtn.textContent = 'Loading...';
            try {
                const [overviewResp, tenantsResp] = await Promise.all([
                    fetch('/v1/admin/overview', { headers: providerHeaders() }),
                    fetch('/v1/admin/tenants', { headers: providerHeaders() })
                ]);
                const tokensResp = await fetch('/v1/admin/purchase-tokens', { headers: providerHeaders() });

                const overview = await overviewResp.json();
                const tenants = await tenantsResp.json();
                const tokens = await tokensResp.json();

                if (!overviewResp.ok) {
                    if (overviewResp.status === 401) {
                        window.location.href = '/admin/login';
                        return;
                    }
                    throw new Error(overview.detail || 'Failed to load provider overview');
                }
                if (!tenantsResp.ok) {
                    throw new Error(tenants.detail || 'Failed to load tenants');
                }
                if (!tokensResp.ok) {
                    throw new Error(tokens.detail || 'Failed to load purchase tokens');
                }

                ownerChip.textContent = `${overview.provider.owner_name} | ${overview.admin.email}`;
                refreshMeta.textContent = 'Authenticated. Data is live.';
                kpiTenants.textContent = overview.kpis.active_tenants;
                kpiKeys.textContent = overview.kpis.active_api_keys;
                kpiStreams.textContent = `${overview.kpis.healthy_streams}/${overview.kpis.total_streams}`;
                kpiRefresh.textContent = new Date().toLocaleTimeString();

                signalList.innerHTML = (overview.kpis.latest_signals || []).map((item) => `
                    <div class="signal-item">
                        <div>
                            <div><strong>${item.vertical}</strong> · ${item.signal_key}</div>
                            <div class="meta">${formatDate(item.computed_at)}</div>
                        </div>
                        <div>
                            <span>${Number(item.signal_value).toFixed(1)}</span>
                            <span class="badge ${String(item.signal_label || '').toLowerCase()}">${item.signal_label || 'n/a'}</span>
                        </div>
                    </div>
                `).join('') || '<div class="meta">No signals yet.</div>';

                tenantRows.innerHTML = (tenants.tenants || []).map((tenant) => `
                    <tr>
                        <td>${tenant.id}</td>
                        <td>${tenant.name}<br><span class="meta">${tenant.contact_email}</span></td>
                        <td>${tenant.plan_tier}</td>
                        <td>${tenant.active ? 'active' : 'inactive'}</td>
                        <td>${(tenant.entitlements || []).join(', ') || 'none'}</td>
                        <td>${(tenant.api_keys || []).map((k) => `<span class="mono">${k.key_prefix}</span> (${k.label})`).join('<br>') || 'none'}</td>
                        <td>${formatDate(tenant.last_api_use)}</td>
                    </tr>
                `).join('');

                purchaseTokenRows.innerHTML = (tokens.purchase_tokens || []).map((token) => `
                    <tr>
                        <td>${token.id}</td>
                        <td>${token.tenant_name} (#${token.tenant_id})</td>
                        <td>${token.plan_tier}</td>
                        <td>${(token.entitlements || []).join(', ')}</td>
                        <td>${formatDate(token.expires_at)}</td>
                        <td>${token.is_used ? `yes (${formatDate(token.used_at)})` : 'no'}</td>
                    </tr>
                `).join('');
            } catch (err) {
                setError(err.message || String(err));
            } finally {
                loadBtn.disabled = false;
                loadBtn.textContent = 'Load Console';
            }
        }

        loadBtn.addEventListener('click', loadProviderDashboard);
        providerKeyInput.addEventListener('keydown', (event) => {
            if (event.key === 'Enter') {
                loadProviderDashboard();
            }
        });

        clearBtn.addEventListener('click', async () => {
            try {
                await fetch('/v1/admin/logout', { method: 'POST' });
            } catch (e) {
                // best effort logout
            }
            providerKeyInput.value = 'Authenticated via cookie session';
            ownerChip.textContent = 'Provider Access';
            refreshMeta.textContent = 'Awaiting authentication.';
            kpiTenants.textContent = '0';
            kpiKeys.textContent = '0';
            kpiStreams.textContent = '0/0';
            kpiRefresh.textContent = 'n/a';
            signalList.innerHTML = '';
            tenantRows.innerHTML = '';
            purchaseTokenRows.innerHTML = '';
            newKeyBox.classList.add('hidden');
            newKeyBox.innerHTML = '';
            purchaseResult.classList.add('hidden');
            purchaseResult.innerHTML = '';
            tenantStatusResult.classList.add('hidden');
            tenantStatusResult.innerHTML = '';
            adminKeyActionResult.classList.add('hidden');
            adminKeyActionResult.innerHTML = '';
            setError('');
            window.location.href = '/admin/login';
        });

        tenantForm.addEventListener('submit', async (event) => {
            event.preventDefault();
            setError('');

            const checkedEntitlements = Array.from(tenantForm.querySelectorAll('input[type="checkbox"]:checked')).map((i) => i.value);
            const payload = {
                name: document.getElementById('tenant-name').value.trim(),
                contact_email: document.getElementById('tenant-email').value.trim(),
                plan_tier: document.getElementById('tenant-plan').value,
                api_key_label: document.getElementById('tenant-key-label').value.trim() || 'default',
                entitlements: checkedEntitlements.length ? checkedEntitlements : ['restaurant']
            };

            try {
                const response = await fetch('/v1/admin/tenants', {
                    method: 'POST',
                    headers: providerHeaders(),
                    body: JSON.stringify(payload)
                });
                const data = await response.json();
                if (!response.ok) {
                    throw new Error(data.detail || 'Failed to create tenant');
                }

                newKeyBox.classList.remove('hidden');
                newKeyBox.innerHTML = `
                    <strong>Tenant created:</strong> ${data.tenant_name}<br>
                    <strong>Plan:</strong> ${data.plan_tier}<br>
                    <strong>Entitlements:</strong> ${(data.entitlements || []).join(', ')}<br>
                    <strong>API Key:</strong> <span class="mono">${data.api_key}</span>
                `;

                tenantForm.reset();
                tenantForm.querySelector('input[value="restaurant"]').checked = true;
                loadProviderDashboard();
            } catch (err) {
                setError(err.message || String(err));
            }
        });

        tenantStatusForm.addEventListener('submit', async (event) => {
            event.preventDefault();
            setError('');
            const tenantId = Number(document.getElementById('status-tenant-id').value);
            const active = document.getElementById('status-active').value === 'active';
            try {
                const resp = await fetch(`/v1/admin/tenants/${tenantId}/status`, {
                    method: 'POST',
                    headers: providerHeaders(),
                    body: JSON.stringify({ active })
                });
                const data = await resp.json();
                if (!resp.ok) throw new Error(data.detail || 'Failed to update tenant status');
                tenantStatusResult.classList.remove('hidden');
                tenantStatusResult.innerHTML = `<strong>Tenant #${data.tenant_id}</strong> set to <strong>${data.active ? 'active' : 'inactive'}</strong>.`;
                loadProviderDashboard();
            } catch (err) {
                setError(err.message || String(err));
            }
        });

        adminKeyRotateForm.addEventListener('submit', async (event) => {
            event.preventDefault();
            setError('');
            const payload = {
                tenant_id: Number(document.getElementById('admin-rotate-tenant-id').value),
                api_key_label: document.getElementById('admin-rotate-label').value.trim() || 'admin-rotated-key',
                deactivate_existing: document.getElementById('admin-rotate-deactivate').checked
            };
            try {
                const resp = await fetch('/v1/admin/api-keys/rotate', {
                    method: 'POST',
                    headers: providerHeaders(),
                    body: JSON.stringify(payload)
                });
                const data = await resp.json();
                if (!resp.ok) throw new Error(data.detail || 'Failed to rotate key');
                adminKeyActionResult.classList.remove('hidden');
                adminKeyActionResult.innerHTML = `
                    <strong>Rotated key for tenant #${data.tenant_id}</strong><br>
                    <strong>New key:</strong> <span class="mono">${data.api_key}</span><br>
                    <strong>Prefix:</strong> ${data.api_key_prefix}
                `;
                loadProviderDashboard();
            } catch (err) {
                setError(err.message || String(err));
            }
        });

        adminKeyRevokeForm.addEventListener('submit', async (event) => {
            event.preventDefault();
            setError('');
            const payload = {
                tenant_id: Number(document.getElementById('admin-revoke-tenant-id').value),
                key_prefix: document.getElementById('admin-revoke-prefix').value.trim()
            };
            try {
                const resp = await fetch('/v1/admin/api-keys/revoke', {
                    method: 'POST',
                    headers: providerHeaders(),
                    body: JSON.stringify(payload)
                });
                const data = await resp.json();
                if (!resp.ok) throw new Error(data.detail || 'Failed to revoke key');
                adminKeyActionResult.classList.remove('hidden');
                adminKeyActionResult.innerHTML = `<strong>Revoked key</strong> ${data.key_prefix} for tenant #${data.tenant_id}.`;
                loadProviderDashboard();
            } catch (err) {
                setError(err.message || String(err));
            }
        });

        purchaseForm.addEventListener('submit', async (event) => {
            event.preventDefault();
            setError('');

            const entitlements = Array.from(document.querySelectorAll('#purchase-ent-checkboxes input[type="checkbox"]:checked')).map((i) => i.value);
            const payload = {
                tenant_id: Number(document.getElementById('purchase-tenant-id').value),
                plan_tier: document.getElementById('purchase-plan').value,
                expires_hours: Number(document.getElementById('purchase-expiry').value || 72),
                entitlements: entitlements.length ? entitlements : ['restaurant']
            };

            try {
                const response = await fetch('/v1/admin/purchase-tokens', {
                    method: 'POST',
                    headers: providerHeaders(),
                    body: JSON.stringify(payload)
                });
                const data = await response.json();
                if (!response.ok) {
                    throw new Error(data.detail || 'Failed to create purchase token');
                }

                purchaseResult.classList.remove('hidden');
                purchaseResult.innerHTML = `
                    <strong>Purchase token created:</strong><br>
                    <span class="mono">${data.purchase_token}</span><br>
                    <strong>Tenant ID:</strong> ${data.tenant_id}<br>
                    <strong>Plan:</strong> ${data.plan_tier}<br>
                    <strong>Entitlements:</strong> ${(data.entitlements || []).join(', ')}<br>
                    <strong>Expiry:</strong> ${data.expires_hours}h
                `;
                purchaseForm.reset();
                document.querySelector('#purchase-ent-checkboxes input[value="restaurant"]').checked = true;
                document.getElementById('purchase-expiry').value = '72';
                loadProviderDashboard();
            } catch (err) {
                setError(err.message || String(err));
            }
        });

        loadProviderDashboard();
    </script>
</body>
</html>
    """


def render_admin_login_html():
        return """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Admin Login</title>
    <style>
        body { font-family: Segoe UI, sans-serif; margin: 0; background: linear-gradient(140deg, #eef2f7, #dfe7f2); min-height: 100vh; display: grid; place-items: center; }
        .card { width: min(460px, calc(100vw - 22px)); background: #fff; border-radius: 14px; border: 1px solid #d0d8e4; padding: 20px; box-shadow: 0 14px 30px rgba(24, 44, 70, 0.14); }
        h1 { margin: 0 0 8px; font-size: 34px; }
        p { margin: 0 0 14px; color: #566372; }
        label { display: block; margin-bottom: 4px; font-size: 12px; text-transform: uppercase; color: #566372; }
        input { width: 100%; padding: 10px 11px; border-radius: 9px; border: 1px solid #ccd6e2; margin-bottom: 12px; }
        button { width: 100%; border: 0; border-radius: 9px; padding: 11px; font-weight: 700; background: #0b5ad9; color: #fff; cursor: pointer; }
        .error { color: #b7412b; margin-top: 10px; font-size: 13px; }
    </style>
</head>
<body>
    <main class="card">
        <h1>Admin Login</h1>
        <p>Provider-only access.</p>
        <label for="email">Email</label>
        <input id="email" type="email" placeholder="admin@publicdata.local" />
        <label for="password">Password</label>
        <input id="password" type="password" placeholder="********" />
        <button id="login">Sign In</button>
        <div class="error" id="error"></div>
    </main>
    <script>
        const errorEl = document.getElementById('error');
        document.getElementById('login').addEventListener('click', async () => {
            errorEl.textContent = '';
            const email = document.getElementById('email').value.trim();
            const password = document.getElementById('password').value;
            try {
                const resp = await fetch('/v1/admin/login', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ email, password })
                });
                const payload = await resp.json();
                if (!resp.ok) throw new Error(payload.detail || 'Login failed');
                window.location.href = '/admin';
            } catch (e) {
                errorEl.textContent = e.message || String(e);
            }
        });
    </script>
</body>
</html>
        """


def render_client_login_html():
        return """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Client Login</title>
    <style>
        body { font-family: Segoe UI, sans-serif; margin: 0; background: linear-gradient(140deg, #f2eee7, #e8edf6); min-height: 100vh; display: grid; place-items: center; }
        .card { width: min(460px, calc(100vw - 22px)); background: #fff; border-radius: 14px; border: 1px solid #d8d4cd; padding: 20px; box-shadow: 0 14px 30px rgba(38, 46, 52, 0.14); }
        h1 { margin: 0 0 8px; font-size: 34px; }
        p { margin: 0 0 14px; color: #5b6770; }
        label { display: block; margin-bottom: 4px; font-size: 12px; text-transform: uppercase; color: #5b6770; }
        input { width: 100%; padding: 10px 11px; border-radius: 9px; border: 1px solid #d6dee5; margin-bottom: 12px; }
        button { width: 100%; border: 0; border-radius: 9px; padding: 11px; font-weight: 700; background: #0f766e; color: #fff; cursor: pointer; }
        .error { color: #b7412b; margin-top: 10px; font-size: 13px; }
    </style>
</head>
<body>
    <main class="card">
        <h1>Client Login</h1>
        <p>Sign in to redeem purchase tokens and manage API keys.</p>
        <label for="email">Email</label>
        <input id="email" type="email" placeholder="you@company.com" />
        <label for="password">Password</label>
        <input id="password" type="password" placeholder="********" />
        <button id="login">Sign In</button>
        <div class="error" id="error"></div>
    </main>
    <script>
        const errorEl = document.getElementById('error');
        document.getElementById('login').addEventListener('click', async () => {
            errorEl.textContent = '';
            const email = document.getElementById('email').value.trim();
            const password = document.getElementById('password').value;
            try {
                const resp = await fetch('/v1/client/login', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ email, password })
                });
                const payload = await resp.json();
                if (!resp.ok) throw new Error(payload.detail || 'Login failed');
                window.location.href = '/client';
            } catch (e) {
                errorEl.textContent = e.message || String(e);
            }
        });
    </script>
</body>
</html>
        """


def render_client_portal_html():
        return """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Client Portal</title>
    <style>
        body { margin: 0; font-family: Segoe UI, sans-serif; background: #f5f1e8; color: #1f2a2d; }
        .shell { width: min(1100px, calc(100vw - 24px)); margin: 20px auto 28px; display: grid; gap: 12px; }
        .panel { background: #fff; border: 1px solid #ded8cb; border-radius: 12px; padding: 14px; }
        .head { display: flex; justify-content: space-between; align-items: center; gap: 10px; flex-wrap: wrap; }
        h1,h2 { margin: 0; }
        .muted { color: #5f6b66; font-size: 13px; }
        input { width: 100%; border: 1px solid #d7dfd4; border-radius: 8px; padding: 10px; margin-top: 5px; }
        button { border: 0; border-radius: 8px; padding: 10px 12px; font-weight: 700; cursor: pointer; }
        .main { background: #0f766e; color: #fff; }
        .warn { background: #ece7dc; color: #1f2a2d; }
        table { width: 100%; border-collapse: collapse; font-size: 13px; }
        th, td { text-align: left; padding: 8px; border-top: 1px solid #ece8df; }
        th { text-transform: uppercase; letter-spacing: 0.08em; font-size: 11px; color: #5f6b66; }
        .mono { font-family: Consolas, Monaco, monospace; font-size: 12px; }
        .result { margin-top: 10px; padding: 10px; border-radius: 8px; background: #edf8f6; border: 1px solid #c8e7e1; }
        .error { color: #b7412b; font-size: 13px; margin-top: 8px; }
    </style>
</head>
<body>
    <main class="shell">
        <section class="panel head">
            <div>
                <h1>Client Portal</h1>
                <p class="muted" id="who">Loading account...</p>
            </div>
            <button id="logout" class="warn">Logout</button>
        </section>

        <section class="panel">
            <h2>Redeem Purchase Token</h2>
            <p class="muted">Redeem a payment token from your receipt to activate plan access and generate a new API key.</p>
            <label>Purchase Token<input id="purchase-token" placeholder="pay_..." /></label>
            <label>Key Label<input id="key-label" value="purchased-key" /></label>
            <button id="redeem" class="main">Redeem and Generate Key</button>
            <div id="redeem-result"></div>
            <div id="redeem-error" class="error"></div>
        </section>

        <section class="panel">
            <h2>Your API Keys</h2>
            <p class="muted">Rotate keys regularly. Revoke any key you no longer trust.</p>
            <p><a href="/client/restaurant">Open Restaurant Analytics</a></p>
            <label>Rotate Key Label<input id="rotate-label" value="rotated-key" /></label>
            <label><input id="rotate-deactivate" type="checkbox" checked /> Deactivate existing active keys when rotating</label>
            <button id="rotate" class="main">Rotate API Key</button>
            <div id="rotate-result"></div>
            <table>
                <thead><tr><th>Prefix</th><th>Label</th><th>Status</th><th>Created</th><th>Last Used</th><th>Action</th></tr></thead>
                <tbody id="key-rows"></tbody>
            </table>
        </section>
    </main>
    <script>
        const whoEl = document.getElementById('who');
        const keyRows = document.getElementById('key-rows');
        const redeemResult = document.getElementById('redeem-result');
        const redeemError = document.getElementById('redeem-error');
        const rotateResult = document.getElementById('rotate-result');

        function fmt(v) {
            if (!v) return 'n/a';
            const d = new Date(v);
            return Number.isNaN(d.getTime()) ? v : d.toLocaleString();
        }

        async function loadClientData() {
            const [meResp, keysResp] = await Promise.all([
                fetch('/v1/client/me'),
                fetch('/v1/client/api-keys')
            ]);
            const me = await meResp.json();
            const keys = await keysResp.json();
            if (!meResp.ok) { window.location.href = '/client/login'; return; }
            if (!keysResp.ok) { throw new Error(keys.detail || 'Failed to load keys'); }

            whoEl.textContent = `${me.email} | ${me.tenant_name} | plan=${me.plan_tier}`;
            keyRows.innerHTML = (keys.api_keys || []).map((k) => `
                <tr>
                    <td class="mono">${k.key_prefix}</td>
                    <td>${k.label}</td>
                    <td>${k.active ? 'active' : 'inactive'}</td>
                    <td>${fmt(k.created_at)}</td>
                    <td>${fmt(k.last_used_at)}</td>
                    <td>${k.active ? `<button data-prefix="${k.key_prefix}" class="revoke-btn warn">Revoke</button>` : 'n/a'}</td>
                </tr>
            `).join('');

            keyRows.querySelectorAll('.revoke-btn').forEach((btn) => {
                btn.addEventListener('click', async () => {
                    redeemError.textContent = '';
                    try {
                        const resp = await fetch('/v1/client/api-keys/revoke', {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({ key_prefix: btn.dataset.prefix })
                        });
                        const payload = await resp.json();
                        if (!resp.ok) throw new Error(payload.detail || 'Revoke failed');
                        rotateResult.innerHTML = `<div class="result"><strong>Revoked key:</strong> ${payload.key_prefix}</div>`;
                        await loadClientData();
                    } catch (e) {
                        redeemError.textContent = e.message || String(e);
                    }
                });
            });
        }

        document.getElementById('logout').addEventListener('click', async () => {
            await fetch('/v1/client/logout', { method: 'POST' });
            window.location.href = '/client/login';
        });

        document.getElementById('redeem').addEventListener('click', async () => {
            redeemResult.innerHTML = '';
            rotateResult.innerHTML = '';
            redeemError.textContent = '';
            const purchase_token = document.getElementById('purchase-token').value.trim();
            const api_key_label = document.getElementById('key-label').value.trim() || 'purchased-key';
            try {
                const resp = await fetch('/v1/client/redeem-token', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ purchase_token, api_key_label })
                });
                const payload = await resp.json();
                if (!resp.ok) throw new Error(payload.detail || 'Redeem failed');
                redeemResult.innerHTML = `<div class="result"><strong>New API key:</strong> <span class="mono">${payload.api_key}</span><br><strong>Plan:</strong> ${payload.plan_tier}<br><strong>Entitlements:</strong> ${(payload.entitlements || []).join(', ')}</div>`;
                document.getElementById('purchase-token').value = '';
                await loadClientData();
            } catch (e) {
                redeemError.textContent = e.message || String(e);
            }
        });

        document.getElementById('rotate').addEventListener('click', async () => {
            redeemResult.innerHTML = '';
            rotateResult.innerHTML = '';
            redeemError.textContent = '';
            const api_key_label = document.getElementById('rotate-label').value.trim() || 'rotated-key';
            const deactivate_existing = document.getElementById('rotate-deactivate').checked;
            try {
                const resp = await fetch('/v1/client/api-keys/rotate', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ api_key_label, deactivate_existing })
                });
                const payload = await resp.json();
                if (!resp.ok) throw new Error(payload.detail || 'Rotate failed');
                rotateResult.innerHTML = `<div class="result"><strong>New API key:</strong> <span class="mono">${payload.api_key}</span><br><strong>Prefix:</strong> ${payload.api_key_prefix}</div>`;
                await loadClientData();
            } catch (e) {
                redeemError.textContent = e.message || String(e);
            }
        });

        loadClientData().catch((e) => { redeemError.textContent = e.message || String(e); });
    </script>
</body>
</html>
        """


def render_restaurant_analytics_html():
        return """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Restaurant Analytics</title>
    <style>
        :root {
            --bg: #f5efe4;
            --panel: #fff8ee;
            --line: #e5dccf;
            --ink: #1f2a2d;
            --muted: #5f6b66;
            --accent: #0f766e;
            --good: #1f7a3d;
            --warn: #b8691c;
            --bad: #b7412b;
        }
        * { box-sizing: border-box; }
        body {
            margin: 0;
            font-family: "Palatino Linotype", "Book Antiqua", Palatino, serif;
            color: var(--ink);
            background: radial-gradient(circle at 10% -10%, rgba(15, 118, 110, 0.2), transparent 45%), var(--bg);
        }
        .shell { width: min(980px, calc(100vw - 28px)); margin: 24px auto 40px; display: grid; gap: 14px; }
        .panel { background: var(--panel); border: 1px solid var(--line); border-radius: 18px; padding: 16px; }
        .head { display: flex; justify-content: space-between; align-items: center; gap: 10px; flex-wrap: wrap; }
        h1 { margin: 0; font-size: clamp(32px, 5vw, 54px); line-height: 0.92; }
        p { margin: 6px 0; }
        .muted { color: var(--muted); }
        .grid { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 10px; }
        .metric { border: 1px solid var(--line); border-radius: 12px; padding: 12px; background: #fffdf7; }
        .metric h3 { margin: 0 0 8px; font-size: 12px; text-transform: uppercase; letter-spacing: 0.1em; color: var(--muted); }
        .metric p { margin: 0; font-size: 38px; line-height: 0.95; }
        .tag { display: inline-block; padding: 6px 10px; border-radius: 999px; font-size: 11px; text-transform: uppercase; letter-spacing: 0.08em; }
        .tag.good { background: rgba(31, 122, 61, 0.14); color: var(--good); }
        .tag.warn { background: rgba(184, 105, 28, 0.16); color: var(--warn); }
        .tag.bad { background: rgba(183, 65, 43, 0.14); color: var(--bad); }
        .lists { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; }
        ul { margin: 8px 0 0; padding-left: 18px; }
        li { margin: 4px 0; }
        .recommendation { font-size: 17px; line-height: 1.5; border-left: 4px solid var(--accent); padding-left: 12px; }
        .error { color: var(--bad); font-size: 13px; }
        .row { display: flex; justify-content: space-between; gap: 10px; flex-wrap: wrap; }
        @media (max-width: 820px) {
            .grid { grid-template-columns: 1fr; }
            .lists { grid-template-columns: 1fr; }
        }
    </style>
</head>
<body>
    <main class="shell">
        <section class="panel">
            <div class="head">
                <div>
                    <h1>Restaurant Analytics</h1>
                    <p class="muted">Simple live interpretation for food trucks/carts and restaurants.</p>
                </div>
                <div>
                    <a href="/client">Back to Client Portal</a>
                </div>
            </div>
            <p class="muted" id="tenant">Loading tenant...</p>
            <div class="row">
                <div><span class="tag" id="trend-tag">trend</span></div>
                <div class="muted">Updated: <span id="updated-at">n/a</span></div>
            </div>
        </section>

        <section class="panel">
            <div class="grid">
                <article class="metric">
                    <h3>Demand Opportunity</h3>
                    <p id="opportunity">0.0</p>
                </article>
                <article class="metric">
                    <h3>Operating Risk</h3>
                    <p id="risk">0.0</p>
                </article>
                <article class="metric">
                    <h3>Outdoor Comfort</h3>
                    <p id="comfort">0.0</p>
                </article>
            </div>
            <p class="muted">Confidence: <strong id="confidence">n/a</strong></p>
        </section>

        <section class="panel">
            <h2>Recommended Action</h2>
            <p class="recommendation" id="recommendation">Loading...</p>
            <p class="error" id="error"></p>
        </section>

        <section class="panel">
            <div class="lists">
                <article>
                    <h2>Positive Drivers</h2>
                    <ul id="positive-list"></ul>
                </article>
                <article>
                    <h2>Risk Drivers</h2>
                    <ul id="negative-list"></ul>
                </article>
            </div>
        </section>
    </main>

    <script>
        const tenantEl = document.getElementById('tenant');
        const opportunityEl = document.getElementById('opportunity');
        const riskEl = document.getElementById('risk');
        const comfortEl = document.getElementById('comfort');
        const recommendationEl = document.getElementById('recommendation');
        const positiveList = document.getElementById('positive-list');
        const negativeList = document.getElementById('negative-list');
        const trendTag = document.getElementById('trend-tag');
        const confidenceEl = document.getElementById('confidence');
        const updatedAtEl = document.getElementById('updated-at');
        const errorEl = document.getElementById('error');

        function fmtDate(v) {
            if (!v) return 'n/a';
            const d = new Date(v);
            return Number.isNaN(d.getTime()) ? v : d.toLocaleString();
        }

        function fmtNum(v) {
            const n = Number(v);
            return Number.isNaN(n) ? 'n/a' : n.toFixed(1);
        }

        function setTrendTag(direction, delta) {
            const d = (direction || 'flat').toLowerCase();
            trendTag.className = 'tag ' + (d === 'rising' ? 'good' : (d === 'falling' ? 'bad' : 'warn'));
            trendTag.textContent = `${d} (${fmtNum(delta)})`;
        }

        function listHtml(items) {
            if (!items || !items.length) return '<li>n/a</li>';
            return items.map((i) => `<li>${i}</li>`).join('');
        }

        async function loadInsights() {
            errorEl.textContent = '';
            try {
                const resp = await fetch('/v1/client/restaurant-insights');
                const payload = await resp.json();
                if (!resp.ok) {
                    if (resp.status === 401) {
                        window.location.href = '/client/login';
                        return;
                    }
                    throw new Error(payload.detail || 'Failed to load insights');
                }

                tenantEl.textContent = `${payload.tenant.tenant_name || 'Tenant'} | plan=${payload.tenant.plan_tier || 'n/a'}`;
                opportunityEl.textContent = fmtNum(payload.opportunity_score);
                riskEl.textContent = fmtNum(payload.operating_risk_score);
                comfortEl.textContent = fmtNum(payload.outdoor_comfort_score);
                confidenceEl.textContent = `${fmtNum(payload.confidence_score)}%`;
                recommendationEl.textContent = payload.recommendation || 'No recommendation available';
                positiveList.innerHTML = listHtml(payload.drivers && payload.drivers.positive);
                negativeList.innerHTML = listHtml(payload.drivers && payload.drivers.negative);

                const trend = payload.trend_3h || {};
                setTrendTag(trend.direction, trend.delta);
                updatedAtEl.textContent = fmtDate(payload.source && payload.source.computed_at);
            } catch (err) {
                errorEl.textContent = err.message || String(err);
            }
        }

        loadInsights();
        setInterval(loadInsights, 60000);
    </script>
</body>
</html>
        """


def render_admin_console_html():
        return render_provider_html()


app = FastAPI(title="Public Data API", version="0.1.0")


@app.on_event("startup")
def on_startup():
    ensure_processed_signal_columns()
    ensure_auth_tables()
    ensure_admin_bootstrap_user()


@app.get("/", response_class=HTMLResponse)
def dashboard_page():
    return render_dashboard_html()


@app.get("/provider", response_class=HTMLResponse)
def provider_page():
    return render_admin_console_html()


@app.get("/admin/login", response_class=HTMLResponse)
def admin_login_page():
    return render_admin_login_html()


@app.get("/admin", response_class=HTMLResponse)
def admin_page():
    return render_admin_console_html()


@app.get("/client/login", response_class=HTMLResponse)
def client_login_page():
    return render_client_login_html()


@app.get("/client", response_class=HTMLResponse)
def client_page():
    return render_client_portal_html()


@app.get("/client/restaurant", response_class=HTMLResponse)
def client_restaurant_page():
    return render_restaurant_analytics_html()


@app.get("/health")
def health_check():
    return {"status": "ok"}


@app.get("/v1/me")
def who_am_i(context=Depends(get_request_context)):
    return context


@app.get("/v1/dashboard/summary")
def dashboard_summary(context=Depends(get_request_context)):
    return build_dashboard_summary(context)


@app.get("/v1/dashboard/trends")
def dashboard_trends(context=Depends(get_request_context)):
    entitlements = fetch_entitlements(context["tenant_id"])
    trend_map = {}
    for vertical in entitlements:
        if vertical in VERTICAL_SIGNAL_MAP:
            trend_map[vertical] = fetch_signal_trend(vertical, points=24)
    return {
        "tenant": context,
        "trends": trend_map,
    }


@app.get("/v1/provider/overview")
def provider_overview(provider=Depends(get_provider_context)):
    return {
        "provider": provider,
        "kpis": fetch_platform_kpis(),
        "stream_health": fetch_stream_health(),
    }


@app.get("/v1/provider/tenants")
def provider_tenants(provider=Depends(get_provider_context)):
    return {
        "provider": provider,
        "tenants": fetch_provider_tenants(),
    }


@app.post("/v1/provider/tenants")
def provider_create_tenant(payload: ProviderTenantCreate, provider=Depends(get_provider_context)):
    _ = provider
    return create_tenant(payload)


@app.post("/v1/admin/login")
def admin_login(payload: LoginPayload, response: Response):
    result = login_admin(payload)
    response.set_cookie(
        key="admin_session",
        value=result["session"],
        httponly=True,
        samesite="lax",
        secure=False,
        max_age=60 * 60 * 12,
    )
    return {
        "admin_user_id": result["admin_user_id"],
        "email": result["email"],
    }


@app.post("/v1/admin/logout")
def admin_logout(response: Response, admin_session: str = Cookie(default=None)):
    revoke_session(admin_session, "admin_sessions")
    response.delete_cookie("admin_session")
    return {"ok": True}


@app.get("/v1/admin/me")
def admin_me(context=Depends(get_admin_context)):
    return context


@app.get("/v1/admin/overview")
def admin_overview(context=Depends(get_admin_context)):
    provider = get_provider_settings()
    return {
        "provider": {
            "owner_name": provider.get("owner_name", "Provider"),
        },
        "admin": context,
        "kpis": fetch_platform_kpis(),
        "stream_health": fetch_stream_health(),
    }


@app.get("/v1/admin/tenants")
def admin_tenants(context=Depends(get_admin_context)):
    _ = context
    return {"tenants": fetch_provider_tenants()}


@app.post("/v1/admin/tenants")
def admin_create_tenant(payload: ProviderTenantCreate, context=Depends(get_admin_context)):
    _ = context
    return create_tenant(payload)


@app.post("/v1/admin/tenants/{tenant_id}/status")
def admin_set_tenant_status(tenant_id: int, payload: AdminTenantStatusPayload, context=Depends(get_admin_context)):
    _ = context
    return set_tenant_active(tenant_id, payload.active)


@app.get("/v1/admin/purchase-tokens")
def admin_purchase_tokens(context=Depends(get_admin_context)):
    _ = context
    return {"purchase_tokens": fetch_purchase_tokens(limit=200)}


@app.post("/v1/admin/purchase-tokens")
def admin_create_purchase_token(payload: PurchaseTokenCreatePayload, context=Depends(get_admin_context)):
    _ = context
    return create_purchase_token(payload)


@app.post("/v1/admin/api-keys/rotate")
def admin_rotate_api_key(payload: AdminKeyRotatePayload, context=Depends(get_admin_context)):
    _ = context
    return create_api_key_for_tenant(payload.tenant_id, payload.api_key_label, payload.deactivate_existing)


@app.post("/v1/admin/api-keys/revoke")
def admin_revoke_api_key(payload: AdminKeyRevokePayload, context=Depends(get_admin_context)):
    _ = context
    return revoke_api_key_by_prefix(payload.tenant_id, payload.key_prefix)


@app.post("/v1/client/login")
def client_login(payload: LoginPayload, response: Response):
    result = login_client(payload)
    response.set_cookie(
        key="client_session",
        value=result["session"],
        httponly=True,
        samesite="lax",
        secure=False,
        max_age=60 * 60 * 12,
    )
    return {
        "user_id": result["user_id"],
        "email": result["email"],
        "tenant_id": result["tenant_id"],
        "tenant_name": result["tenant_name"],
        "plan_tier": result["plan_tier"],
        "role": result["role"],
    }


@app.post("/v1/client/logout")
def client_logout(response: Response, client_session: str = Cookie(default=None)):
    revoke_session(client_session, "client_sessions")
    response.delete_cookie("client_session")
    return {"ok": True}


@app.get("/v1/client/me")
def client_me(context=Depends(get_client_context)):
    return context


@app.get("/v1/client/restaurant-insights")
def client_restaurant_insights(context=Depends(get_client_context)):
    return fetch_restaurant_operations_insight(context)


@app.post("/v1/client/redeem-token")
def client_redeem_token(payload: PurchaseTokenRedeemPayload, context=Depends(get_client_context)):
    return redeem_purchase_token(context, payload)


@app.get("/v1/client/api-keys")
def client_api_keys(context=Depends(get_client_context)):
    return {
        "tenant_id": context["tenant_id"],
        "api_keys": fetch_client_api_keys(context["tenant_id"]),
    }


@app.post("/v1/client/api-keys/rotate")
def client_rotate_api_key(payload: ClientKeyRotatePayload, context=Depends(get_client_context)):
    return create_api_key_for_tenant(context["tenant_id"], payload.api_key_label, payload.deactivate_existing)


@app.post("/v1/client/api-keys/revoke")
def client_revoke_api_key(payload: ClientKeyRevokePayload, context=Depends(get_client_context)):
    return revoke_api_key_by_prefix(context["tenant_id"], payload.key_prefix)


@app.get("/v1/restaurant/demand-forecast")
def restaurant_demand(context=Depends(require_vertical("restaurant"))):
    return {
        "tenant": context,
        "data": fetch_latest_signal("restaurant"),
    }


@app.get("/v1/logistics/route-risk")
def logistics_route_risk(context=Depends(require_vertical("logistics"))):
    return {
        "tenant": context,
        "data": fetch_latest_signal("logistics"),
    }


@app.get("/v1/outdoor/safety-status")
def outdoor_safety_status(context=Depends(require_vertical("outdoor"))):
    return {
        "tenant": context,
        "data": fetch_latest_signal("outdoor"),
    }


@app.get("/v1/system/stream-health")
def system_stream_health(context=Depends(get_request_context)):
    return {
        "tenant": context,
        "data": fetch_stream_health(),
    }