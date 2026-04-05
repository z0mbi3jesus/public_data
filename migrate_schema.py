"""
migrate_schema.py
Idempotent schema migration — safe to run multiple times.
Creates tenant, auth, signal, and health tables in the public_data database.
"""

import mysql.connector
from settings_loader import load_json_config


def get_conn():
    cfg = load_json_config()
    m = cfg["storage"]["mysql"]
    return mysql.connector.connect(
        host=m["host"],
        port=int(m["port"]),
        user=m["user"],
        password=m["password"],
        database=m["database"],
        ssl_disabled=True,
    )


TABLES = {
    # ------------------------------------------------------------------
    # tenants — one row per paying customer / company
    # ------------------------------------------------------------------
    "tenants": """
        CREATE TABLE IF NOT EXISTS `tenants` (
            `id`            INT           NOT NULL AUTO_INCREMENT,
            `name`          VARCHAR(255)  NOT NULL,
            `plan_tier`     ENUM('trial','basic','pro','enterprise') NOT NULL DEFAULT 'trial',
            `contact_email` VARCHAR(255)  NOT NULL,
            `created_at`    DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
            `active`        TINYINT(1)    NOT NULL DEFAULT 1,
            PRIMARY KEY (`id`),
            UNIQUE KEY `uq_tenants_email` (`contact_email`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,

    # ------------------------------------------------------------------
    # users — people who log into the web dashboard
    # ------------------------------------------------------------------
    "users": """
        CREATE TABLE IF NOT EXISTS `users` (
            `id`            INT           NOT NULL AUTO_INCREMENT,
            `tenant_id`     INT           NOT NULL,
            `email`         VARCHAR(255)  NOT NULL,
            `password_hash` VARCHAR(255)  NOT NULL,
            `role`          ENUM('admin','viewer') NOT NULL DEFAULT 'viewer',
            `created_at`    DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
            `active`        TINYINT(1)    NOT NULL DEFAULT 1,
            PRIMARY KEY (`id`),
            UNIQUE KEY `uq_users_email` (`email`),
            CONSTRAINT `fk_users_tenant` FOREIGN KEY (`tenant_id`) REFERENCES `tenants` (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,

    # ------------------------------------------------------------------
    # api_keys — keys sent by tenant apps with every API request.
    # The raw key is never stored; only the SHA-256 hash + a display prefix.
    # ------------------------------------------------------------------
    "api_keys": """
        CREATE TABLE IF NOT EXISTS `api_keys` (
            `id`            INT           NOT NULL AUTO_INCREMENT,
            `tenant_id`     INT           NOT NULL,
            `key_hash`      VARCHAR(64)   NOT NULL COMMENT 'SHA-256 of raw key (hex)',
            `key_prefix`    VARCHAR(12)   NOT NULL COMMENT 'First 8 chars for display',
            `label`         VARCHAR(128)  NOT NULL DEFAULT 'default',
            `created_at`    DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
            `last_used_at`  DATETIME      NULL,
            `active`        TINYINT(1)    NOT NULL DEFAULT 1,
            PRIMARY KEY (`id`),
            UNIQUE KEY `uq_api_keys_hash` (`key_hash`),
            KEY `idx_api_keys_tenant` (`tenant_id`),
            CONSTRAINT `fk_api_keys_tenant` FOREIGN KEY (`tenant_id`) REFERENCES `tenants` (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,

    # ------------------------------------------------------------------
    # entitlements — which verticals each tenant can access
    # ------------------------------------------------------------------
    "entitlements": """
        CREATE TABLE IF NOT EXISTS `entitlements` (
            `id`            INT           NOT NULL AUTO_INCREMENT,
            `tenant_id`     INT           NOT NULL,
            `vertical`      ENUM('restaurant','logistics','outdoor') NOT NULL,
            `granted_at`    DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (`id`),
            UNIQUE KEY `uq_entitlements` (`tenant_id`, `vertical`),
            CONSTRAINT `fk_entitlements_tenant` FOREIGN KEY (`tenant_id`) REFERENCES `tenants` (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,

    # ------------------------------------------------------------------
    # raw_events — unified normalized record from all 4 streams.
    # The full source row is stored as JSON for flexibility.
    # processed=0 means the feature scorer hasn't touched it yet.
    # ------------------------------------------------------------------
    "raw_events": """
        CREATE TABLE IF NOT EXISTS `raw_events` (
            `id`            BIGINT        NOT NULL AUTO_INCREMENT,
            `stream_name`   VARCHAR(64)   NOT NULL,
            `event_ts`      DATETIME      NOT NULL,
            `ingested_at`   DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
            `location_label` VARCHAR(128) NOT NULL DEFAULT '',
            `payload`       JSON          NOT NULL,
            `processed`     TINYINT(1)    NOT NULL DEFAULT 0,
            PRIMARY KEY (`id`),
            KEY `idx_raw_events_stream` (`stream_name`),
            KEY `idx_raw_events_processed` (`processed`),
            KEY `idx_raw_events_event_ts` (`event_ts`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,

    # ------------------------------------------------------------------
    # processed_signals — feature scores computed by the processing layer.
    # One row per vertical × signal × time window.
    # ------------------------------------------------------------------
    "processed_signals": """
        CREATE TABLE IF NOT EXISTS `processed_signals` (
            `id`                    BIGINT        NOT NULL AUTO_INCREMENT,
            `computed_at`           DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
            `window_start`          DATETIME      NOT NULL,
            `window_end`            DATETIME      NOT NULL,
            `vertical`              VARCHAR(32)   NOT NULL,
            `signal_key`            VARCHAR(64)   NOT NULL,
            `signal_value`          FLOAT         NULL,
            `signal_label`          VARCHAR(32)   NULL COMMENT 'low / medium / high etc.',
            `contributing_streams`  VARCHAR(255)  NULL COMMENT 'comma-separated stream names',
            `details_json`          JSON          NULL COMMENT 'Signal breakdown details',
            PRIMARY KEY (`id`),
            KEY `idx_ps_vertical_key` (`vertical`, `signal_key`),
            KEY `idx_ps_window` (`window_start`, `window_end`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,

    # ------------------------------------------------------------------
    # stream_health — one row per stream per orchestrator run.
    # Feeds the /v1/system/stream-health API endpoint.
    # ------------------------------------------------------------------
    "stream_health": """
        CREATE TABLE IF NOT EXISTS `stream_health` (
            `id`              INT           NOT NULL AUTO_INCREMENT,
            `checked_at`      DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
            `stream_name`     VARCHAR(64)   NOT NULL,
            `status`          ENUM('ok','degraded','down') NOT NULL,
            `last_success_at` DATETIME      NULL,
            `rows_last_run`   INT           NOT NULL DEFAULT 0,
            `error_message`   TEXT          NULL,
            PRIMARY KEY (`id`),
            KEY `idx_sh_stream` (`stream_name`),
            KEY `idx_sh_checked_at` (`checked_at`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
}

# Tables must be created in this order to satisfy foreign key dependencies
CREATE_ORDER = [
    "tenants",
    "users",
    "api_keys",
    "entitlements",
    "raw_events",
    "processed_signals",
    "stream_health",
]


def run_migration():
    conn = get_conn()
    cur = conn.cursor()

    for table_name in CREATE_ORDER:
        cur.execute(TABLES[table_name])
        print(f"[OK] {table_name}")

    conn.commit()
    cur.close()
    conn.close()
    print("\nMigration complete.")


if __name__ == "__main__":
    run_migration()
