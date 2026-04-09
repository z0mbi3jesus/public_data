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

    # ------------------------------------------------------------------
    # audit_log — append-only record of security-relevant actions.
    # ------------------------------------------------------------------
    "audit_log": """
        CREATE TABLE IF NOT EXISTS `audit_log` (
            `id`          BIGINT        NOT NULL AUTO_INCREMENT,
            `created_at`  DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
            `action`      VARCHAR(64)   NOT NULL,
            `actor_type`  VARCHAR(16)   NOT NULL COMMENT 'admin | client | provider | system',
            `actor_ref`   VARCHAR(255)  NULL     COMMENT 'email or identifier of the actor',
            `tenant_id`   INT           NULL,
            `details_json` JSON         NULL,
            PRIMARY KEY (`id`),
            KEY `idx_audit_created` (`created_at`),
            KEY `idx_audit_action` (`action`),
            KEY `idx_audit_tenant` (`tenant_id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,

    # ------------------------------------------------------------------
    # invite_tokens — admin-issued one-time links to create client users.
    # ------------------------------------------------------------------
    "invite_tokens": """
        CREATE TABLE IF NOT EXISTS `invite_tokens` (
            `id`                  BIGINT        NOT NULL AUTO_INCREMENT,
            `created_at`          DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
            `expires_at`          DATETIME      NOT NULL,
            `email`               VARCHAR(255)  NOT NULL,
            `tenant_id`           INT           NOT NULL,
            `role`                ENUM('admin','viewer') NOT NULL DEFAULT 'viewer',
            `token_hash`          VARCHAR(64)   NOT NULL,
            `invited_by_admin_id` INT           NOT NULL,
            `used_at`             DATETIME      NULL,
            `is_used`             TINYINT(1)    NOT NULL DEFAULT 0,
            PRIMARY KEY (`id`),
            UNIQUE KEY `uq_invite_token_hash` (`token_hash`),
            KEY `idx_invite_tenant` (`tenant_id`),
            KEY `idx_invite_expires` (`expires_at`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,

    # ------------------------------------------------------------------
    # password_reset_tokens — admin-issued one-time password reset links.
    # ------------------------------------------------------------------
    "password_reset_tokens": """
        CREATE TABLE IF NOT EXISTS `password_reset_tokens` (
            `id`         BIGINT        NOT NULL AUTO_INCREMENT,
            `created_at` DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
            `expires_at` DATETIME      NOT NULL,
            `user_id`    INT           NOT NULL,
            `token_hash` VARCHAR(64)   NOT NULL,
            `used_at`    DATETIME      NULL,
            `is_used`    TINYINT(1)    NOT NULL DEFAULT 0,
            PRIMARY KEY (`id`),
            UNIQUE KEY `uq_pw_reset_token_hash` (`token_hash`),
            KEY `idx_pw_reset_user` (`user_id`),
            KEY `idx_pw_reset_expires` (`expires_at`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,

    # ------------------------------------------------------------------
    # nws_alerts — structured store for active NWS weather alerts.
    # Populated by the analytics processor from raw stream_weather rows.
    # ------------------------------------------------------------------
    "nws_alerts": """
        CREATE TABLE IF NOT EXISTS `nws_alerts` (
            `id`           BIGINT        NOT NULL AUTO_INCREMENT,
            `fetched_at`   DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
            `location_lat` DECIMAL(8,4)  NOT NULL,
            `location_lon` DECIMAL(8,4)  NOT NULL,
            `alert_id`     VARCHAR(255)  NULL     COMMENT 'NWS URN identifier',
            `event`        VARCHAR(128)  NOT NULL COMMENT 'e.g. Winter Storm Warning',
            `severity`     VARCHAR(32)   NOT NULL COMMENT 'Extreme/Severe/Moderate/Minor/Unknown',
            `urgency`      VARCHAR(32)   NOT NULL COMMENT 'Immediate/Expected/Future/Past/Unknown',
            `certainty`    VARCHAR(32)   NOT NULL COMMENT 'Observed/Likely/Possible/Unlikely/Unknown',
            `status`       VARCHAR(32)   NOT NULL COMMENT 'Actual/Exercise/System/Test/Draft',
            `message_type` VARCHAR(32)   NULL     COMMENT 'Alert/Update/Cancel',
            `onset`        DATETIME      NULL,
            `expires`      DATETIME      NULL,
            `headline`     TEXT          NULL,
            `description`  MEDIUMTEXT    NULL,
            `area_desc`    TEXT          NULL,
            PRIMARY KEY (`id`),
            KEY `idx_nws_alerts_fetched`  (`fetched_at`),
            KEY `idx_nws_alerts_event`    (`event`),
            KEY `idx_nws_alerts_severity` (`severity`),
            KEY `idx_nws_alerts_expires`  (`expires`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,

    # ------------------------------------------------------------------
    # nws_hourly_forecast — structured store for NWS hourly forecast periods.
    # Up to 12 periods per ingest run; enables the forecast endpoint.
    # ------------------------------------------------------------------
    "nws_hourly_forecast": """
        CREATE TABLE IF NOT EXISTS `nws_hourly_forecast` (
            `id`                  BIGINT        NOT NULL AUTO_INCREMENT,
            `fetched_at`          DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
            `location_lat`        DECIMAL(8,4)  NOT NULL,
            `location_lon`        DECIMAL(8,4)  NOT NULL,
            `period_number`       SMALLINT      NOT NULL,
            `start_time`          DATETIME      NOT NULL,
            `end_time`            DATETIME      NOT NULL,
            `is_daytime`          TINYINT(1)    NOT NULL DEFAULT 1,
            `temperature_f`       SMALLINT      NULL,
            `temperature_trend`   VARCHAR(16)   NULL     COMMENT 'rising/falling/null',
            `wind_speed`          VARCHAR(32)   NULL     COMMENT 'e.g. 5 mph or 5 to 10 mph',
            `wind_direction`      VARCHAR(8)    NULL,
            `short_forecast`      VARCHAR(255)  NULL,
            `detailed_forecast`   TEXT          NULL,
            `precip_probability`  TINYINT       NULL     COMMENT 'Percentage 0-100',
            PRIMARY KEY (`id`),
            KEY `idx_nws_hf_fetched`    (`fetched_at`),
            KEY `idx_nws_hf_start_time` (`start_time`),
            KEY `idx_nws_hf_location`   (`location_lat`, `location_lon`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,

    # ------------------------------------------------------------------
    # opensky_states — structured snapshot of aircraft state vectors.
    # ------------------------------------------------------------------
    "opensky_states": """
        CREATE TABLE IF NOT EXISTS `opensky_states` (
            `id`             BIGINT        NOT NULL AUTO_INCREMENT,
            `fetched_at`     DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
            `icao24`         VARCHAR(16)   NOT NULL,
            `callsign`       VARCHAR(32)   NULL,
            `origin_country` VARCHAR(128)  NULL,
            `longitude`      DECIMAL(9,6)  NULL,
            `latitude`       DECIMAL(9,6)  NULL,
            `baro_altitude`  FLOAT         NULL,
            `on_ground`      TINYINT(1)    NOT NULL DEFAULT 0,
            `velocity_ms`    FLOAT         NULL,
            `true_track`     FLOAT         NULL,
            `vertical_rate`  FLOAT         NULL,
            PRIMARY KEY (`id`),
            KEY `idx_os_fetched` (`fetched_at`),
            KEY `idx_os_icao24` (`icao24`),
            KEY `idx_os_lat_lon` (`latitude`, `longitude`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,

    # ------------------------------------------------------------------
    # purpleair_observations — structured PurpleAir sensor observations.
    # ------------------------------------------------------------------
    "purpleair_observations": """
        CREATE TABLE IF NOT EXISTS `purpleair_observations` (
            `id`             BIGINT        NOT NULL AUTO_INCREMENT,
            `fetched_at`     DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
            `sensor_name`    VARCHAR(255)  NULL,
            `latitude`       DECIMAL(9,6)  NULL,
            `longitude`      DECIMAL(9,6)  NULL,
            `pm25_ugm3`      FLOAT         NULL,
            `humidity_pct`   FLOAT         NULL,
            `temperature_f`  FLOAT         NULL,
            `confidence_pct` FLOAT         NULL,
            PRIMARY KEY (`id`),
            KEY `idx_pa_fetched` (`fetched_at`),
            KEY `idx_pa_lat_lon` (`latitude`, `longitude`),
            KEY `idx_pa_pm25` (`pm25_ugm3`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,

    # ------------------------------------------------------------------
    # road_incidents — normalized incidents from 511 / DOT feeds.
    # ------------------------------------------------------------------
    "road_incidents": """
        CREATE TABLE IF NOT EXISTS `road_incidents` (
            `id`            BIGINT        NOT NULL AUTO_INCREMENT,
            `fetched_at`    DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
            `source`        VARCHAR(64)   NOT NULL DEFAULT '511',
            `incident_id`   VARCHAR(128)  NULL,
            `incident_type` VARCHAR(64)   NULL,
            `severity`      VARCHAR(32)   NULL,
            `status`        VARCHAR(32)   NULL,
            `description`   TEXT          NULL,
            `start_time`    DATETIME      NULL,
            `end_time`      DATETIME      NULL,
            `lanes_blocked` INT           NULL,
            `latitude`      DECIMAL(9,6)  NULL,
            `longitude`     DECIMAL(9,6)  NULL,
            PRIMARY KEY (`id`),
            KEY `idx_ri_fetched` (`fetched_at`),
            KEY `idx_ri_type` (`incident_type`),
            KEY `idx_ri_severity` (`severity`),
            KEY `idx_ri_start` (`start_time`),
            KEY `idx_ri_lat_lon` (`latitude`, `longitude`)
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
    "audit_log",
    "invite_tokens",
    "password_reset_tokens",
    "nws_alerts",
    "nws_hourly_forecast",
    "opensky_states",
    "purpleair_observations",
    "road_incidents",
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
