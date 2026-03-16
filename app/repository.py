import hashlib
import uuid

from app.db import get_connection
from app.schemas import ParseOrderResponse


class OrderRepository:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path

    def upsert_recipient(self, parsed: ParseOrderResponse) -> tuple[int, bool]:
        recipient = parsed.recipient
        with get_connection(self.db_path) as connection:
            row = connection.execute(
                """
                SELECT id FROM recipients
                WHERE phone = ? AND name = ? AND address_detail = ?
                """,
                (recipient.phone, recipient.name, recipient.address_detail),
            ).fetchone()
            if row is not None:
                return int(row["id"]), False
            cursor = connection.execute(
                """
                INSERT INTO recipients
                (name, phone, province, city, district, address_detail, raw_address, postcode)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    recipient.name,
                    recipient.phone,
                    recipient.province,
                    recipient.city,
                    recipient.district,
                    recipient.address_detail,
                    recipient.raw_address,
                    recipient.postcode,
                ),
            )
            recipient_id = cursor.lastrowid
            if recipient_id is None:
                raise RuntimeError("failed to create recipient")
            return int(recipient_id), True

    def create_or_get_order(
        self,
        recipient_id: int,
        input_text: str,
        parsed: ParseOrderResponse,
    ) -> tuple[str, str, bool]:
        key = hashlib.sha256(input_text.encode("utf-8")).hexdigest()
        with get_connection(self.db_path) as connection:
            row = connection.execute(
                "SELECT id, order_no, status FROM orders WHERE idempotency_key = ?",
                (key,),
            ).fetchone()
            if row is not None:
                return str(row["order_no"]), str(row["status"]), False

            order_no = f"AO{uuid.uuid4().hex[:12].upper()}"
            status = "pending_review" if parsed.needs_review else "ready_to_upload"
            cursor = connection.execute(
                """
                INSERT INTO orders
                (order_no, recipient_id, source_text, confidence, needs_review, idempotency_key, status)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    order_no,
                    recipient_id,
                    input_text,
                    parsed.confidence,
                    1 if parsed.needs_review else 0,
                    key,
                    status,
                ),
            )
            order_id = cursor.lastrowid
            if order_id is None:
                raise RuntimeError("failed to create order")
            for item in parsed.products:
                connection.execute(
                    """
                    INSERT INTO order_items
                    (order_id, brand, product_name, stage, quantity, unit)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        int(order_id),
                        item.brand,
                        item.product_name,
                        item.stage,
                        item.quantity,
                        item.unit,
                    ),
                )
            return order_no, status, True
