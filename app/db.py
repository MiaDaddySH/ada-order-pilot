import sqlite3
from pathlib import Path

from app.llm_client import Settings
from app.product_seed import PRODUCT_ROWS


def ensure_parent_dir(db_path: str) -> None:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)


def get_connection(db_path: str) -> sqlite3.Connection:
    ensure_parent_dir(db_path)
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    return connection


def init_db(db_path: str) -> None:
    with get_connection(db_path) as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS product_catalog (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_name TEXT NOT NULL UNIQUE,
                simple_code TEXT NOT NULL,
                status INTEGER NOT NULL DEFAULT 1
            )
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_product_catalog_code
            ON product_catalog(simple_code)
            """
        )
        product_columns = connection.execute("PRAGMA table_info(product_catalog)").fetchall()
        product_column_names = {str(row["name"]) for row in product_columns}
        if "status" not in product_column_names:
            connection.execute("ALTER TABLE product_catalog ADD COLUMN status INTEGER DEFAULT 1")
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS recipients (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                phone TEXT NOT NULL,
                id_card_no TEXT,
                province TEXT,
                city TEXT,
                district TEXT,
                address_detail TEXT NOT NULL,
                raw_address TEXT NOT NULL,
                postcode TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(phone, name, address_detail)
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_no TEXT NOT NULL UNIQUE,
                recipient_id INTEGER NOT NULL,
                source_text TEXT NOT NULL,
                confidence REAL NOT NULL,
                needs_review INTEGER NOT NULL,
                idempotency_key TEXT NOT NULL UNIQUE,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(recipient_id) REFERENCES recipients(id)
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS order_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id INTEGER NOT NULL,
                simple_code TEXT NOT NULL,
                brand TEXT,
                product_name TEXT NOT NULL,
                stage TEXT,
                quantity INTEGER NOT NULL,
                unit TEXT NOT NULL,
                FOREIGN KEY(order_id) REFERENCES orders(id)
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS sender_profiles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                phone TEXT NOT NULL,
                street TEXT NOT NULL,
                house_no TEXT NOT NULL,
                postcode TEXT NOT NULL,
                city TEXT NOT NULL,
                country_code TEXT NOT NULL,
                is_default INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        columns = connection.execute("PRAGMA table_info(order_items)").fetchall()
        column_names = {str(row["name"]) for row in columns}
        if "simple_code" not in column_names:
            connection.execute("ALTER TABLE order_items ADD COLUMN simple_code TEXT")
        recipient_columns = connection.execute("PRAGMA table_info(recipients)").fetchall()
        recipient_column_names = {str(row["name"]) for row in recipient_columns}
        if "id_card_no" not in recipient_column_names:
            connection.execute("ALTER TABLE recipients ADD COLUMN id_card_no TEXT")
        for product_name, simple_code in PRODUCT_ROWS:
            connection.execute(
                """
                INSERT OR IGNORE INTO product_catalog (product_name, simple_code, status)
                VALUES (?, ?, 1)
                """,
                (product_name, simple_code),
            )
        settings = Settings()
        count_row = connection.execute("SELECT COUNT(1) AS cnt FROM sender_profiles").fetchone()
        profile_count = int(count_row["cnt"]) if count_row is not None else 0
        if profile_count == 0:
            connection.execute(
                """
                INSERT INTO sender_profiles
                (name, phone, street, house_no, postcode, city, country_code, is_default)
                VALUES (?, ?, ?, ?, ?, ?, ?, 1)
                """,
                (
                    settings.sender_name,
                    settings.sender_phone,
                    settings.sender_street,
                    settings.sender_house_no,
                    settings.sender_postcode,
                    settings.sender_city,
                    settings.sender_country_code,
                ),
            )
