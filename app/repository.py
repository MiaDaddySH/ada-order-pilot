import hashlib
import re
import uuid
from typing import Any

from app.db import get_connection
from app.schemas import ParseOrderResponse, ProductCatalogItem


class OrderRepository:
    _SOURCE_ALIASES: tuple[tuple[str, str], ...] = (
        ("小狮子", "乐温赞"),
        ("狮子", "乐温赞"),
    )

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
                if recipient.id_card_no:
                    connection.execute(
                        "UPDATE recipients SET id_card_no = COALESCE(id_card_no, ?) WHERE id = ?",
                        (recipient.id_card_no, int(row["id"])),
                    )
                return int(row["id"]), False
            cursor = connection.execute(
                """
                INSERT INTO recipients
                (name, phone, id_card_no, province, city, district, address_detail, raw_address, postcode)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    recipient.name,
                    recipient.phone,
                    recipient.id_card_no,
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

    def list_recipients(self) -> list[dict[str, object]]:
        with get_connection(self.db_path) as connection:
            rows = connection.execute(
                """
                SELECT id, name, phone, id_card_no, province, city, district, address_detail, raw_address, postcode
                FROM recipients
                ORDER BY id DESC
                """
            ).fetchall()
            return [dict(row) for row in rows]

    def create_recipient(self, payload: dict[str, Any]) -> dict[str, object]:
        with get_connection(self.db_path) as connection:
            cursor = connection.execute(
                """
                INSERT INTO recipients
                (name, phone, id_card_no, province, city, district, address_detail, raw_address, postcode)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    payload["name"],
                    payload["phone"],
                    payload.get("id_card_no"),
                    payload.get("province"),
                    payload.get("city"),
                    payload.get("district"),
                    payload["address_detail"],
                    payload["raw_address"],
                    payload.get("postcode"),
                ),
            )
            recipient_id = cursor.lastrowid
            if recipient_id is None:
                raise RuntimeError("failed to create recipient")
            row = connection.execute(
                """
                SELECT id, name, phone, id_card_no, province, city, district, address_detail, raw_address, postcode
                FROM recipients WHERE id = ?
                """,
                (int(recipient_id),),
            ).fetchone()
            if row is None:
                raise RuntimeError("failed to fetch recipient")
            return dict(row)

    def update_recipient(self, recipient_id: int, payload: dict[str, Any]) -> dict[str, object] | None:
        with get_connection(self.db_path) as connection:
            connection.execute(
                """
                UPDATE recipients
                SET name = ?, phone = ?, id_card_no = ?, province = ?, city = ?, district = ?, address_detail = ?, raw_address = ?, postcode = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (
                    payload["name"],
                    payload["phone"],
                    payload.get("id_card_no"),
                    payload.get("province"),
                    payload.get("city"),
                    payload.get("district"),
                    payload["address_detail"],
                    payload["raw_address"],
                    payload.get("postcode"),
                    recipient_id,
                ),
            )
            row = connection.execute(
                """
                SELECT id, name, phone, id_card_no, province, city, district, address_detail, raw_address, postcode
                FROM recipients WHERE id = ?
                """,
                (recipient_id,),
            ).fetchone()
            return dict(row) if row is not None else None

    def delete_recipient(self, recipient_id: int) -> bool:
        with get_connection(self.db_path) as connection:
            row = connection.execute("SELECT id FROM recipients WHERE id = ?", (recipient_id,)).fetchone()
            if row is None:
                return False
            connection.execute("DELETE FROM recipients WHERE id = ?", (recipient_id,))
            return True

    def batch_upsert_recipients(self, payloads: list[dict[str, Any]]) -> int:
        with get_connection(self.db_path) as connection:
            imported = 0
            for payload in payloads:
                row = connection.execute(
                    """
                    SELECT id FROM recipients
                    WHERE phone = ? AND name = ? AND address_detail = ?
                    LIMIT 1
                    """,
                    (payload["phone"], payload["name"], payload["address_detail"]),
                ).fetchone()
                if row is None:
                    connection.execute(
                        """
                        INSERT INTO recipients
                        (name, phone, id_card_no, province, city, district, address_detail, raw_address, postcode)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            payload["name"],
                            payload["phone"],
                            payload.get("id_card_no"),
                            payload.get("province"),
                            payload.get("city"),
                            payload.get("district"),
                            payload["address_detail"],
                            payload["raw_address"],
                            payload.get("postcode"),
                        ),
                    )
                else:
                    connection.execute(
                        """
                        UPDATE recipients
                        SET id_card_no = COALESCE(id_card_no, ?),
                            province = COALESCE(province, ?),
                            city = COALESCE(city, ?),
                            district = COALESCE(district, ?),
                            raw_address = COALESCE(raw_address, ?),
                            postcode = COALESCE(postcode, ?),
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                        """,
                        (
                            payload.get("id_card_no"),
                            payload.get("province"),
                            payload.get("city"),
                            payload.get("district"),
                            payload.get("raw_address"),
                            payload.get("postcode"),
                            int(row["id"]),
                        ),
                    )
                imported += 1
            return imported

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

    def list_orders(self) -> list[dict[str, object]]:
        with get_connection(self.db_path) as connection:
            order_rows = connection.execute(
                """
                SELECT
                    o.id, o.order_no, o.recipient_id, o.source_text, o.confidence, o.needs_review, o.status, o.created_at,
                    r.name AS recipient_name, r.phone AS recipient_phone
                FROM orders o
                JOIN recipients r ON r.id = o.recipient_id
                ORDER BY o.id DESC
                """
            ).fetchall()
            item_rows = connection.execute(
                """
                SELECT id, order_id, simple_code, brand, product_name, stage, quantity, unit
                FROM order_items
                ORDER BY id ASC
                """
            ).fetchall()
            items_by_order: dict[int, list[dict[str, object]]] = {}
            for row in item_rows:
                order_id = int(row["order_id"])
                items_by_order.setdefault(order_id, []).append(
                    {
                        "id": int(row["id"]),
                        "simple_code": str(row["simple_code"]),
                        "brand": str(row["brand"] or ""),
                        "product_name": str(row["product_name"]),
                        "stage": str(row["stage"] or ""),
                        "quantity": int(row["quantity"]),
                        "unit": str(row["unit"]),
                    }
                )
            results: list[dict[str, object]] = []
            for row in order_rows:
                order_id = int(row["id"])
                results.append(
                    {
                        "id": order_id,
                        "order_no": str(row["order_no"]),
                        "recipient_id": int(row["recipient_id"]),
                        "source_text": str(row["source_text"]),
                        "confidence": float(row["confidence"]),
                        "needs_review": bool(int(row["needs_review"])),
                        "status": str(row["status"]),
                        "created_at": str(row["created_at"]),
                        "recipient_name": str(row["recipient_name"]),
                        "recipient_phone": str(row["recipient_phone"]),
                        "items": items_by_order.get(order_id, []),
                    }
                )
            return results

    def create_order_manual(self, payload: dict[str, Any]) -> dict[str, object]:
        with get_connection(self.db_path) as connection:
            order_no = self._generate_order_no()
            cursor = connection.execute(
                """
                INSERT INTO orders
                (order_no, recipient_id, source_text, confidence, needs_review, idempotency_key, status)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    order_no,
                    int(payload["recipient_id"]),
                    str(payload.get("source_text") or ""),
                    float(payload.get("confidence") or 1.0),
                    1 if bool(payload.get("needs_review")) else 0,
                    uuid.uuid4().hex,
                    str(payload.get("status") or "ready_to_upload"),
                ),
            )
            order_id = cursor.lastrowid
            if order_id is None:
                raise RuntimeError("failed to create order")
            for item in payload["items"]:
                connection.execute(
                    """
                    INSERT INTO order_items
                    (order_id, simple_code, brand, product_name, stage, quantity, unit)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        int(order_id),
                        item["simple_code"],
                        item.get("brand"),
                        item["product_name"],
                        item.get("stage"),
                        int(item["quantity"]),
                        item["unit"],
                    ),
                )
            row = connection.execute(
                "SELECT id FROM orders WHERE id = ?",
                (int(order_id),),
            ).fetchone()
            if row is None:
                raise RuntimeError("failed to fetch order")
        orders = self.list_orders()
        for order in orders:
            if int(str(order["id"])) == int(order_id):
                return order
        raise RuntimeError("failed to load order")

    def update_order(self, order_id: int, payload: dict[str, Any]) -> dict[str, object] | None:
        with get_connection(self.db_path) as connection:
            existing = connection.execute("SELECT id FROM orders WHERE id = ?", (order_id,)).fetchone()
            if existing is None:
                return None
            current = connection.execute(
                "SELECT recipient_id, needs_review, status FROM orders WHERE id = ?",
                (order_id,),
            ).fetchone()
            if current is None:
                return None
            recipient_id = int(payload.get("recipient_id") or current["recipient_id"])
            needs_review = 1 if bool(payload.get("needs_review")) else int(current["needs_review"])
            if "needs_review" not in payload:
                needs_review = int(current["needs_review"])
            status = str(payload.get("status") or current["status"])
            connection.execute(
                """
                UPDATE orders
                SET recipient_id = ?, needs_review = ?, status = ?
                WHERE id = ?
                """,
                (recipient_id, needs_review, status, order_id),
            )
        orders = self.list_orders()
        for order in orders:
            if int(str(order["id"])) == order_id:
                return order
        return None

    def delete_order(self, order_id: int) -> bool:
        with get_connection(self.db_path) as connection:
            row = connection.execute("SELECT id FROM orders WHERE id = ?", (order_id,)).fetchone()
            if row is None:
                return False
            connection.execute("DELETE FROM order_items WHERE order_id = ?", (order_id,))
            connection.execute("DELETE FROM orders WHERE id = ?", (order_id,))
            return True

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

    def update_product(
        self,
        product_id: int,
        product_name: str | None,
        simple_code: str | None,
        status: int | None,
    ) -> ProductCatalogItem | None:
        with get_connection(self.db_path) as connection:
            row = connection.execute(
                "SELECT id, product_name, simple_code, status FROM product_catalog WHERE id = ?",
                (product_id,),
            ).fetchone()
            if row is None:
                return None
            next_name = product_name if product_name is not None else str(row["product_name"])
            next_code = simple_code if simple_code is not None else str(row["simple_code"])
            next_status = status if status is not None else int(row["status"])
            connection.execute(
                """
                UPDATE product_catalog
                SET product_name = ?, simple_code = ?, status = ?
                WHERE id = ?
                """,
                (next_name, next_code, next_status, product_id),
            )
            updated = connection.execute(
                "SELECT id, product_name, simple_code, status FROM product_catalog WHERE id = ?",
                (product_id,),
            ).fetchone()
            if updated is None:
                return None
            return ProductCatalogItem(
                id=int(updated["id"]),
                product_name=str(updated["product_name"]),
                simple_code=str(updated["simple_code"]),
                status=int(updated["status"]),
            )

    def delete_product(self, product_id: int) -> bool:
        with get_connection(self.db_path) as connection:
            row = connection.execute("SELECT id FROM product_catalog WHERE id = ?", (product_id,)).fetchone()
            if row is None:
                return False
            connection.execute("DELETE FROM product_catalog WHERE id = ?", (product_id,))
            return True

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

    def list_sender_profiles(self) -> list[dict[str, object]]:
        with get_connection(self.db_path) as connection:
            rows = connection.execute(
                """
                SELECT id, name, phone, street, house_no, postcode, city, country_code, is_default
                FROM sender_profiles
                ORDER BY is_default DESC, id DESC
                """
            ).fetchall()
            return [
                {
                    "id": int(row["id"]),
                    "name": str(row["name"]),
                    "phone": str(row["phone"]),
                    "street": str(row["street"]),
                    "house_no": str(row["house_no"]),
                    "postcode": str(row["postcode"]),
                    "city": str(row["city"]),
                    "country_code": str(row["country_code"]),
                    "is_default": bool(int(row["is_default"])),
                }
                for row in rows
            ]

    def get_default_sender_profile(self) -> dict[str, object] | None:
        with get_connection(self.db_path) as connection:
            row = connection.execute(
                """
                SELECT id, name, phone, street, house_no, postcode, city, country_code, is_default
                FROM sender_profiles
                ORDER BY is_default DESC, id ASC
                LIMIT 1
                """
            ).fetchone()
            if row is None:
                return None
            return {
                "id": int(row["id"]),
                "name": str(row["name"]),
                "phone": str(row["phone"]),
                "street": str(row["street"]),
                "house_no": str(row["house_no"]),
                "postcode": str(row["postcode"]),
                "city": str(row["city"]),
                "country_code": str(row["country_code"]),
                "is_default": bool(int(row["is_default"])),
            }

    def create_sender_profile(self, payload: dict[str, Any]) -> dict[str, object]:
        with get_connection(self.db_path) as connection:
            if bool(payload.get("is_default")):
                connection.execute("UPDATE sender_profiles SET is_default = 0")
            cursor = connection.execute(
                """
                INSERT INTO sender_profiles
                (name, phone, street, house_no, postcode, city, country_code, is_default)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    payload["name"],
                    payload["phone"],
                    payload["street"],
                    payload["house_no"],
                    payload["postcode"],
                    payload["city"],
                    payload["country_code"],
                    1 if bool(payload.get("is_default")) else 0,
                ),
            )
            sender_id = cursor.lastrowid
            if sender_id is None:
                raise RuntimeError("failed to create sender profile")
            row = connection.execute(
                """
                SELECT id, name, phone, street, house_no, postcode, city, country_code, is_default
                FROM sender_profiles WHERE id = ?
                """,
                (int(sender_id),),
            ).fetchone()
            if row is None:
                raise RuntimeError("failed to load sender profile")
            return {
                "id": int(row["id"]),
                "name": str(row["name"]),
                "phone": str(row["phone"]),
                "street": str(row["street"]),
                "house_no": str(row["house_no"]),
                "postcode": str(row["postcode"]),
                "city": str(row["city"]),
                "country_code": str(row["country_code"]),
                "is_default": bool(int(row["is_default"])),
            }

    def update_sender_profile(self, sender_id: int, payload: dict[str, Any]) -> dict[str, object] | None:
        with get_connection(self.db_path) as connection:
            row = connection.execute(
                """
                SELECT id, name, phone, street, house_no, postcode, city, country_code, is_default
                FROM sender_profiles WHERE id = ?
                """,
                (sender_id,),
            ).fetchone()
            if row is None:
                return None
            next_default = bool(payload.get("is_default"))
            if "is_default" not in payload:
                next_default = bool(int(row["is_default"]))
            if next_default:
                connection.execute("UPDATE sender_profiles SET is_default = 0")
            connection.execute(
                """
                UPDATE sender_profiles
                SET name = ?, phone = ?, street = ?, house_no = ?, postcode = ?, city = ?, country_code = ?, is_default = ?
                WHERE id = ?
                """,
                (
                    payload.get("name", row["name"]),
                    payload.get("phone", row["phone"]),
                    payload.get("street", row["street"]),
                    payload.get("house_no", row["house_no"]),
                    payload.get("postcode", row["postcode"]),
                    payload.get("city", row["city"]),
                    payload.get("country_code", row["country_code"]),
                    1 if next_default else 0,
                    sender_id,
                ),
            )
            updated = connection.execute(
                """
                SELECT id, name, phone, street, house_no, postcode, city, country_code, is_default
                FROM sender_profiles WHERE id = ?
                """,
                (sender_id,),
            ).fetchone()
            if updated is None:
                return None
            return {
                "id": int(updated["id"]),
                "name": str(updated["name"]),
                "phone": str(updated["phone"]),
                "street": str(updated["street"]),
                "house_no": str(updated["house_no"]),
                "postcode": str(updated["postcode"]),
                "city": str(updated["city"]),
                "country_code": str(updated["country_code"]),
                "is_default": bool(int(updated["is_default"])),
            }

    def delete_sender_profile(self, sender_id: int) -> bool:
        with get_connection(self.db_path) as connection:
            row = connection.execute(
                "SELECT id, is_default FROM sender_profiles WHERE id = ?",
                (sender_id,),
            ).fetchone()
            if row is None:
                return False
            connection.execute("DELETE FROM sender_profiles WHERE id = ?", (sender_id,))
            if bool(int(row["is_default"])):
                fallback = connection.execute(
                    "SELECT id FROM sender_profiles ORDER BY id ASC LIMIT 1"
                ).fetchone()
                if fallback is not None:
                    connection.execute(
                        "UPDATE sender_profiles SET is_default = 1 WHERE id = ?",
                        (int(fallback["id"]),),
                    )
            return True

    def batch_upsert_sender_profiles(self, payloads: list[dict[str, Any]]) -> int:
        with get_connection(self.db_path) as connection:
            imported = 0
            for payload in payloads:
                existing = connection.execute(
                    """
                    SELECT id FROM sender_profiles
                    WHERE name = ? AND phone = ?
                    LIMIT 1
                    """,
                    (payload["name"], payload["phone"]),
                ).fetchone()
                if bool(payload.get("is_default")):
                    connection.execute("UPDATE sender_profiles SET is_default = 0")
                if existing is None:
                    connection.execute(
                        """
                        INSERT INTO sender_profiles
                        (name, phone, street, house_no, postcode, city, country_code, is_default)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            payload["name"],
                            payload["phone"],
                            payload["street"],
                            payload["house_no"],
                            payload["postcode"],
                            payload["city"],
                            payload["country_code"],
                            1 if bool(payload.get("is_default")) else 0,
                        ),
                    )
                else:
                    connection.execute(
                        """
                        UPDATE sender_profiles
                        SET street = ?, house_no = ?, postcode = ?, city = ?, country_code = ?, is_default = ?
                        WHERE id = ?
                        """,
                        (
                            payload["street"],
                            payload["house_no"],
                            payload["postcode"],
                            payload["city"],
                            payload["country_code"],
                            1 if bool(payload.get("is_default")) else 0,
                            int(existing["id"]),
                        ),
                    )
                imported += 1
            return imported

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
        source_stage_match = re.search(r"(pre|\d+\+?)", source)
        if source_stage_match:
            inferred_stage = self._normalize(source_stage_match.group(1))
            if inferred_stage and inferred_stage in normalized_name:
                score += 3
        if "牛奶" in source:
            if "羊奶" in normalized_name:
                score -= 2
            if "全脂" in normalized_name or "牛" in normalized_name:
                score += 2
        if "牛" in source and "全脂" in normalized_name:
            score += 2
        if "羊" in source and "羊" in normalized_name:
            score += 1
        if "牛" in source and "牛" in normalized_name:
            score += 1
        return score

    def _normalize(self, value: str) -> str:
        compact = value.lower().replace(" ", "")
        for alias, canonical in self._SOURCE_ALIASES:
            compact = compact.replace(alias, canonical)
        compact = compact.replace("（", "(").replace("）", ")")
        compact = compact.replace("＋", "+")
        compact = compact.replace("➕", "+")
        compact = compact.replace("克", "g")
        compact = compact.replace("段", "段")
        return re.sub(r"[^0-9a-z\u4e00-\u9fff\+]+", "", compact)

    def _extract_brand_from_catalog_name(self, product_name: str) -> str | None:
        cleaned = product_name.strip()
        marker_positions = [
            cleaned.find(marker) for marker in ("婴幼儿", "奶粉", "有机", "全脂", "羊奶", "牛奶", "燕麦", "谷物")
        ]
        valid_positions = [pos for pos in marker_positions if pos > 0]
        if valid_positions:
            candidate = cleaned[: min(valid_positions)].strip()
            if 2 <= len(candidate) <= 12:
                return candidate
        latin = re.match(r"^([A-Za-z][A-Za-z ]{1,30})", cleaned)
        if latin:
            return latin.group(1).strip()
        mixed = re.match(r"^([\u4e00-\u9fffA-Za-z]{2,20})", cleaned)
        if mixed:
            return mixed.group(1).strip()
        return None

    def _generate_order_no(self) -> str:
        return f"AO{uuid.uuid4().hex[:12].upper()}"

    def list_recipients_for_export(self) -> list[dict[str, object]]:
        with get_connection(self.db_path) as connection:
            rows = connection.execute(
                """
                SELECT id, name, phone, id_card_no, province, city, district, address_detail, postcode
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
                    r.id_card_no AS id_card_no,
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
                        "id_card_no": str(row["id_card_no"] or ""),
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
