import sqlite3
from pathlib import Path


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
            CREATE TABLE IF NOT EXISTS recipients (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                phone TEXT NOT NULL,
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
                brand TEXT,
                product_name TEXT NOT NULL,
                stage TEXT,
                quantity INTEGER NOT NULL,
                unit TEXT NOT NULL,
                FOREIGN KEY(order_id) REFERENCES orders(id)
            )
            """
        )
