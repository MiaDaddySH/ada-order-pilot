import hashlib
import re
import uuid
from typing import Any

from app.db import get_connection
from app.schemas import ParseOrderResponse, ProductCatalogItem


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
                """
                SELECT 1 FROM product_catalog
                WHERE upper(simple_code) = upper(?) AND status = 1
                LIMIT 1
                """,
                (simple_code,),
            ).fetchone()
            return row is not None

    def get_active_product_by_code(self, simple_code: str) -> tuple[str, str | None] | None:
        with get_connection(self.db_path) as connection:
            row = connection.execute(
                """
                SELECT product_name
                FROM product_catalog
                WHERE upper(simple_code) = upper(?) AND status = 1
                ORDER BY id ASC
                LIMIT 1
                """,
                (simple_code,),
            ).fetchone()
            if row is None:
                return None
            product_name = str(row["product_name"])
            return product_name, self._extract_brand_from_catalog_name(product_name)

    def resolve_product_code(
        self,
        source_text: str,
        product_name: str,
        brand: str | None,
        stage: str | None,
    ) -> str | None:
        with get_connection(self.db_path) as connection:
            code_rows = connection.execute(
                "SELECT DISTINCT simple_code FROM product_catalog WHERE status = 1"
            ).fetchall()
            for row in code_rows:
                code = str(row["simple_code"])
                if re.search(rf"(?<![0-9A-Za-z]){re.escape(code)}(?![0-9A-Za-z])", source_text, re.IGNORECASE):
                    return code

            source = self._normalize(f"{source_text} {product_name} {brand or ''} {stage or ''}")
            rows = connection.execute(
                "SELECT product_name, simple_code FROM product_catalog WHERE status = 1"
            ).fetchall()
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

    def list_products(self, keyword: str | None = None, include_inactive: bool = False) -> list[ProductCatalogItem]:
        with get_connection(self.db_path) as connection:
            sql = "SELECT id, product_name, simple_code, status FROM product_catalog WHERE 1=1"
            params: list[object] = []
            if not include_inactive:
                sql += " AND status = 1"
            if keyword:
                sql += " AND (product_name LIKE ? OR simple_code LIKE ?)"
                kw = f"%{keyword}%"
                params.extend([kw, kw])
            sql += " ORDER BY id ASC"
            rows = connection.execute(sql, params).fetchall()
            return [
                ProductCatalogItem(
                    id=int(row["id"]),
                    product_name=str(row["product_name"]),
                    simple_code=str(row["simple_code"]),
                    status=int(row["status"]),
                )
                for row in rows
            ]

    def create_product(self, product_name: str, simple_code: str) -> ProductCatalogItem:
        with get_connection(self.db_path) as connection:
            row = connection.execute(
                "SELECT id, product_name, simple_code, status FROM product_catalog WHERE product_name = ?",
                (product_name,),
            ).fetchone()
            if row is None:
                cursor = connection.execute(
                    """
                    INSERT INTO product_catalog (product_name, simple_code, status)
                    VALUES (?, ?, 1)
                    """,
                    (product_name, simple_code),
                )
                product_id = cursor.lastrowid
                if product_id is None:
                    raise RuntimeError("failed to create product")
                row = connection.execute(
                    """
                    SELECT id, product_name, simple_code, status
                    FROM product_catalog
                    WHERE id = ?
                    """,
                    (int(product_id),),
                ).fetchone()
            else:
                connection.execute(
                    """
                    UPDATE product_catalog
                    SET simple_code = ?, status = 1
                    WHERE id = ?
                    """,
                    (simple_code, int(row["id"])),
                )
                row = connection.execute(
                    """
                    SELECT id, product_name, simple_code, status
                    FROM product_catalog
                    WHERE id = ?
                    """,
                    (int(row["id"]),),
                ).fetchone()
            if row is None:
                raise RuntimeError("failed to upsert product")
            return ProductCatalogItem(
                id=int(row["id"]),
                product_name=str(row["product_name"]),
                simple_code=str(row["simple_code"]),
                status=int(row["status"]),
            )

    def batch_upsert_products(self, items: list[tuple[str, str]]) -> int:
        with get_connection(self.db_path) as connection:
            upserted = 0
            for product_name, simple_code in items:
                row = connection.execute(
                    "SELECT id FROM product_catalog WHERE product_name = ?",
                    (product_name,),
                ).fetchone()
                if row is None:
                    connection.execute(
                        """
                        INSERT INTO product_catalog (product_name, simple_code, status)
                        VALUES (?, ?, 1)
                        """,
                        (product_name, simple_code),
                    )
                else:
                    connection.execute(
                        """
                        UPDATE product_catalog
                        SET simple_code = ?, status = 1
                        WHERE id = ?
                        """,
                        (simple_code, int(row["id"])),
                    )
                upserted += 1
            return upserted

    def update_product_status(self, product_id: int, status: int) -> ProductCatalogItem | None:
        with get_connection(self.db_path) as connection:
            connection.execute(
                """
                UPDATE product_catalog
                SET status = ?
                WHERE id = ?
                """,
                (status, product_id),
            )
            row = connection.execute(
                "SELECT id, product_name, simple_code, status FROM product_catalog WHERE id = ?",
                (product_id,),
            ).fetchone()
            if row is None:
                return None
            return ProductCatalogItem(
                id=int(row["id"]),
                product_name=str(row["product_name"]),
                simple_code=str(row["simple_code"]),
                status=int(row["status"]),
            )

    def _score_match(self, source: str, candidate_name: str, brand: str | None, stage: str | None) -> int:
        normalized_name = self._normalize(candidate_name)
        score = 0
        if normalized_name and normalized_name in source:
            score += 8
        inferred_brand = self._extract_brand_from_catalog_name(candidate_name)
        if inferred_brand:
            normalized_inferred_brand = self._normalize(inferred_brand)
            if normalized_inferred_brand and normalized_inferred_brand in source:
                score += 4
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

    def _extract_brand_from_catalog_name(self, product_name: str) -> str | None:
        cleaned = product_name.strip()
        latin = re.match(r"^([A-Za-z][A-Za-z ]{1,30})", cleaned)
        if latin:
            return latin.group(1).strip()
        mixed = re.match(r"^([\u4e00-\u9fffA-Za-z]{2,20})", cleaned)
        if mixed:
            return mixed.group(1).strip()
        return None

    def list_recipients_for_export(self) -> list[dict[str, object]]:
        with get_connection(self.db_path) as connection:
            rows = connection.execute(
                """
                SELECT id, name, phone, province, city, district, address_detail, postcode
                FROM recipients
                ORDER BY id ASC
                """
            ).fetchall()
            return [dict(row) for row in rows]

    def list_orders_for_export(
        self,
        status: str | None = "ready_to_upload",
        recent_days: int | None = None,
        limit: int | None = None,
    ) -> list[dict[str, object]]:
        with get_connection(self.db_path) as connection:
            sql = """
                SELECT
                    o.id AS order_id,
                    o.order_no AS order_no,
                    o.status AS order_status,
                    o.created_at AS created_at,
                    r.name AS recipient_name,
                    r.phone AS recipient_phone,
                    r.province AS province,
                    r.city AS city,
                    r.district AS district,
                    r.address_detail AS address_detail,
                    oi.simple_code AS simple_code,
                    oi.quantity AS quantity,
                    oi.product_name AS product_name
                FROM orders o
                JOIN recipients r ON r.id = o.recipient_id
                JOIN order_items oi ON oi.order_id = o.id
            """
            params: list[object] = []
            clauses: list[str] = []
            if status is not None:
                clauses.append("o.status = ?")
                params.append(status)
            if recent_days is not None and recent_days > 0:
                clauses.append("o.created_at >= datetime('now', ?)")
                params.append(f"-{recent_days} days")
            if clauses:
                sql += " WHERE " + " AND ".join(clauses)
            sql += " ORDER BY o.created_at DESC, o.id DESC, oi.id ASC"
            if limit is not None and limit > 0:
                sql += " LIMIT ?"
                params.append(limit * 6)
            rows = connection.execute(sql, params).fetchall()
            grouped: dict[str, dict[str, Any]] = {}
            for row in rows:
                order_no = str(row["order_no"])
                if order_no not in grouped:
                    grouped[order_no] = {
                        "order_no": order_no,
                        "order_status": str(row["order_status"]),
                        "recipient_name": str(row["recipient_name"]),
                        "recipient_phone": str(row["recipient_phone"]),
                        "province": str(row["province"] or ""),
                        "city": str(row["city"] or ""),
                        "district": str(row["district"] or ""),
                        "address_detail": str(row["address_detail"]),
                        "created_at": str(row["created_at"] or ""),
                        "items": [],
                    }
                grouped[order_no]["items"].append(
                    {
                        "simple_code": str(row["simple_code"]),
                        "quantity": int(row["quantity"]),
                        "product_name": str(row["product_name"]),
                    }
                )
                if limit is not None and limit > 0 and len(grouped) >= limit:
                    break
            return list(grouped.values())
