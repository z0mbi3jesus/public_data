import hashlib
import json
import secrets

import mysql.connector
from settings_loader import load_json_config


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


def hash_api_key(raw_key):
    return hashlib.sha256(raw_key.encode("utf-8")).hexdigest()


def hash_password(password):
    salt = secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        bytes.fromhex(salt),
        100000,
    ).hex()
    return f"pbkdf2_sha256$100000${salt}${digest}"


def seed_demo_tenant():
    tenant_name = "Demo Restaurant Group"
    contact_email = "owner@demo-restaurant.local"
    user_email = "admin@demo-restaurant.local"
    user_password = "ChangeMeNow123!"
    raw_api_key = f"pd_{secrets.token_urlsafe(24)}"

    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT id FROM tenants WHERE contact_email = %s",
            (contact_email,),
        )
        tenant_row = cur.fetchone()

        if tenant_row:
            tenant_id = tenant_row[0]
        else:
            cur.execute(
                "INSERT INTO tenants (name, plan_tier, contact_email, active) VALUES (%s, %s, %s, 1)",
                (tenant_name, "trial", contact_email),
            )
            tenant_id = cur.lastrowid

        cur.execute(
            """
            INSERT INTO users (tenant_id, email, password_hash, role, active)
            VALUES (%s, %s, %s, %s, 1)
            ON DUPLICATE KEY UPDATE password_hash = VALUES(password_hash), role = VALUES(role), active = 1
            """,
            (tenant_id, user_email, hash_password(user_password), "admin"),
        )

        for vertical in ("restaurant", "logistics", "outdoor"):
            cur.execute(
                """
                INSERT INTO entitlements (tenant_id, vertical)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE vertical = VALUES(vertical)
                """,
                (tenant_id, vertical),
            )

        key_hash = hash_api_key(raw_api_key)
        cur.execute(
            "SELECT id FROM api_keys WHERE key_hash = %s",
            (key_hash,),
        )
        if not cur.fetchone():
            cur.execute(
                """
                INSERT INTO api_keys (tenant_id, key_hash, key_prefix, label, active)
                VALUES (%s, %s, %s, %s, 1)
                """,
                (tenant_id, key_hash, raw_api_key[:8], "demo-key"),
            )

        conn.commit()
        print("Demo tenant ready.")
        print(f"tenant_id={tenant_id}")
        print(f"admin_email={user_email}")
        print(f"admin_password={user_password}")
        print(f"api_key={raw_api_key}")
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    seed_demo_tenant()