import hashlib
import re
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
                    (order_id, simple_code, brand, product_name, stage, quantity, unit)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        int(order_id),
                        item.simple_code,
                        item.brand,
                        item.product_name,
                        item.stage,
                        item.quantity,
                        item.unit,
                    ),
                )
            return order_no, status, True

    def product_code_exists(self, simple_code: str) -> bool:
        with get_connection(self.db_path) as connection:
            row = connection.execute(
                "SELECT 1 FROM product_catalog WHERE upper(simple_code) = upper(?) LIMIT 1",
                (simple_code,),
            ).fetchone()
            return row is not None

    def resolve_product_code(
        self,
        source_text: str,
        product_name: str,
        brand: str | None,
        stage: str | None,
    ) -> str | None:
        with get_connection(self.db_path) as connection:
            code_rows = connection.execute("SELECT DISTINCT simple_code FROM product_catalog").fetchall()
            for row in code_rows:
                code = str(row["simple_code"])
                if re.search(rf"(?<![0-9A-Za-z]){re.escape(code)}(?![0-9A-Za-z])", source_text, re.IGNORECASE):
                    return code

            source = self._normalize(f"{source_text} {product_name} {brand or ''} {stage or ''}")
            rows = connection.execute("SELECT product_name, simple_code FROM product_catalog").fetchall()
            best_code: str | None = None
            best_score = 0
            for row in rows:
                candidate_name = str(row["product_name"])
                candidate_code = str(row["simple_code"])
                score = self._score_match(source, candidate_name, brand, stage)
                if score > best_score:
                    best_score = score
                    best_code = candidate_code
            if best_score >= 5:
                return best_code
            return None

    def _score_match(self, source: str, candidate_name: str, brand: str | None, stage: str | None) -> int:
        normalized_name = self._normalize(candidate_name)
        score = 0
        if normalized_name and normalized_name in source:
            score += 8
        if brand:
            normalized_brand = self._normalize(brand)
            if normalized_brand and normalized_brand in normalized_name:
                score += 2
        if stage:
            normalized_stage = self._normalize(stage)
            if normalized_stage and normalized_stage in normalized_name:
                score += 3
        if "羊" in source and "羊" in normalized_name:
            score += 1
        if "牛" in source and "牛" in normalized_name:
            score += 1
        return score

    def _normalize(self, value: str) -> str:
        compact = value.lower().replace(" ", "")
        compact = compact.replace("（", "(").replace("）", ")")
        compact = compact.replace("＋", "+")
        compact = compact.replace("克", "g")
        compact = compact.replace("段", "段")
        return re.sub(r"[^0-9a-z\u4e00-\u9fff\+]+", "", compact)
