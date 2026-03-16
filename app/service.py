import re

from app.db import init_db
from app.llm_client import LLMOrderParser, Settings
from app.repository import OrderRepository
from app.schemas import (
    BatchUpsertProductsRequest,
    CreateOrderFromInputResponse,
    CreateProductRequest,
    OrderCreateRequest,
    OrderUpdateRequest,
    OrderView,
    ParseOrderResponse,
    ParsedProduct,
    ParsedRecipient,
    ProductCatalogItem,
    RecipientBatchUpsertRequest,
    RecipientImportPreviewResponse,
    RecipientImportImageResponse,
    RecipientItem,
    RecipientUpsertRequest,
    SenderBatchUpsertRequest,
    SenderImportImageResponse,
    SenderProfileItem,
    SenderProfileUpsertRequest,
    UpdateProductRequest,
    UpdateProductStatusRequest,
)
from app.template_export import TemplateExporter


class OrderParseService:
    def __init__(self) -> None:
        self.settings = Settings()
        self.parser = LLMOrderParser(self.settings)
        init_db(self.settings.db_path)
        self.repository = OrderRepository(self.settings.db_path)
        self.template_exporter = TemplateExporter(self.settings)

    def parse(self, input_text: str) -> ParseOrderResponse:
        result = self.parser.parse_order(input_text)
        recipient = ParsedRecipient.model_validate(result.recipient)
        products = [ParsedProduct.model_validate(item) for item in result.products]
        unresolved = any(item.simple_code is None for item in products)
        confidence = result.confidence
        if unresolved:
            confidence = min(confidence, 0.5)
        return ParseOrderResponse(
            recipient=recipient,
            products=products,
            confidence=confidence,
            needs_review=result.needs_review or unresolved,
            parse_source=result.parse_source,
        )

    def create_order_from_input(
        self,
        input_text: str,
        recipient_id_card_no: str | None = None,
    ) -> CreateOrderFromInputResponse:
        parsed = self.parse(input_text)
        self._enrich_recipient_from_existing(parsed, input_text)
        if recipient_id_card_no and recipient_id_card_no.strip():
            parsed.recipient.id_card_no = recipient_id_card_no.strip()
        missing_fields = self._collect_missing_recipient_fields(parsed.recipient)
        if missing_fields:
            raise ValueError(f"收件人信息不完整，缺少: {','.join(missing_fields)}")
        unresolved_items = [item.product_name for item in parsed.products if item.simple_code is None]
        if unresolved_items:
            names = ",".join(unresolved_items)
            raise ValueError(f"未匹配到商品简易代码: {names}")
        recipient_id, recipient_created = self.repository.upsert_recipient(parsed)
        order_no, order_status, order_created = self.repository.create_or_get_order(
            recipient_id=recipient_id,
            input_text=input_text,
            parsed=parsed,
        )
        return CreateOrderFromInputResponse(
            order_no=order_no,
            order_status=order_status,
            order_created=order_created,
            recipient_id=recipient_id,
            recipient_created=recipient_created,
            parse_result=parsed,
        )

    def _enrich_recipient_from_existing(self, parsed: ParseOrderResponse, input_text: str) -> None:
        name = (parsed.recipient.name or "").strip()
        extracted_name = self._extract_name_from_input(input_text)
        lookup_names: list[str] = []
        if name and name != "待确认":
            lookup_names.append(name)
        if extracted_name and extracted_name not in lookup_names:
            lookup_names.append(extracted_name)
        if not lookup_names:
            return
        existing: dict[str, object] | None = None
        matched_name = ""
        for candidate in lookup_names:
            candidates = self.repository.find_recipients_by_name(candidate)
            if candidates:
                existing = self._choose_best_recipient_candidate(candidates, parsed, input_text)
                matched_name = candidate
                break
        if existing is None:
            return
        if (not parsed.recipient.name or parsed.recipient.name == "待确认") and matched_name:
            parsed.recipient.name = matched_name
        should_hydrate = (
            not parsed.recipient.phone
            or parsed.recipient.phone == "00000000000"
            or not parsed.recipient.province
            or not parsed.recipient.city
            or not parsed.recipient.district
        )
        if should_hydrate:
            parsed.recipient.phone = str(existing.get("phone") or parsed.recipient.phone)
            parsed.recipient.province = (str(existing.get("province") or "").strip() or None)
            parsed.recipient.city = (str(existing.get("city") or "").strip() or None)
            parsed.recipient.district = (str(existing.get("district") or "").strip() or None)
            parsed.recipient.address_detail = str(existing.get("address_detail") or parsed.recipient.address_detail)
            parsed.recipient.raw_address = str(existing.get("raw_address") or parsed.recipient.raw_address)
            parsed.recipient.postcode = (str(existing.get("postcode") or "").strip() or None)
        if not parsed.recipient.id_card_no:
            parsed.recipient.id_card_no = (str(existing.get("id_card_no") or "").strip() or None)

    def _choose_best_recipient_candidate(
        self,
        candidates: list[dict[str, object]],
        parsed: ParseOrderResponse,
        input_text: str,
    ) -> dict[str, object]:
        best = candidates[0]
        best_score = self._recipient_match_score(best, parsed, input_text)
        for candidate in candidates[1:]:
            score = self._recipient_match_score(candidate, parsed, input_text)
            if score > best_score:
                best = candidate
                best_score = score
        return best

    def _recipient_match_score(
        self,
        candidate: dict[str, object],
        parsed: ParseOrderResponse,
        input_text: str,
    ) -> int:
        score = 0
        source = input_text.strip()
        phone = str(candidate.get("phone") or "").strip()
        if phone and phone in source:
            score += 10
        if phone and len(phone) >= 4 and phone[-4:] in source:
            score += 4
        id_card_no = str(candidate.get("id_card_no") or "").strip()
        if id_card_no and id_card_no in source:
            score += 8
        for key in ("province", "city", "district"):
            value = str(candidate.get(key) or "").strip()
            if value and value in source:
                score += 2
        address_detail = str(candidate.get("address_detail") or "").strip()
        if address_detail:
            tokens = [t for t in re.split(r"[，,。；;、\s\-—（）()]+", address_detail) if len(t) >= 2]
            for token in tokens[:6]:
                if token in source:
                    score += 1
        parsed_phone = (parsed.recipient.phone or "").strip()
        if parsed_phone and parsed_phone != "00000000000" and parsed_phone == phone:
            score += 6
        if parsed.recipient.province and parsed.recipient.province == str(candidate.get("province") or ""):
            score += 2
        if parsed.recipient.city and parsed.recipient.city == str(candidate.get("city") or ""):
            score += 2
        if parsed.recipient.district and parsed.recipient.district == str(candidate.get("district") or ""):
            score += 2
        return score

    def _extract_name_from_input(self, input_text: str) -> str | None:
        text = input_text.strip()
        if not text:
            return None
        first_chunk = re.split(r"[\s,，;；（(]", text, maxsplit=1)[0].strip()
        if re.fullmatch(r"[\u4e00-\u9fff]{2,6}", first_chunk):
            return first_chunk
        matched = re.search(r"[\u4e00-\u9fff]{2,6}", text)
        if matched:
            return matched.group(0)
        return None

    def _collect_missing_recipient_fields(self, recipient: ParsedRecipient) -> list[str]:
        missing: list[str] = []
        if not recipient.name or recipient.name.strip() == "待确认":
            missing.append("姓名")
        if not recipient.phone or recipient.phone == "00000000000":
            missing.append("手机号")
        if not recipient.id_card_no:
            missing.append("身份证号码")
        if not recipient.province:
            missing.append("省")
        if not recipient.city:
            missing.append("市")
        if not recipient.district:
            missing.append("区县")
        if not recipient.address_detail:
            missing.append("详细地址")
        if not recipient.raw_address:
            missing.append("原始地址")
        return missing

    def list_products(self, keyword: str | None = None, include_inactive: bool = False) -> list[ProductCatalogItem]:
        return self.repository.list_products(keyword=keyword, include_inactive=include_inactive)

    def create_product(self, payload: CreateProductRequest) -> ProductCatalogItem:
        return self.repository.create_product(
            product_name=payload.product_name.strip(),
            simple_code=payload.simple_code.strip(),
        )

    def batch_upsert_products(self, payload: BatchUpsertProductsRequest) -> int:
        items = [(item.product_name.strip(), item.simple_code.strip()) for item in payload.products]
        return self.repository.batch_upsert_products(items)

    def update_product_status(self, product_id: int, payload: UpdateProductStatusRequest) -> ProductCatalogItem:
        updated = self.repository.update_product_status(product_id=product_id, status=payload.status)
        if updated is None:
            raise ValueError("商品不存在")
        return updated

    def update_product(self, product_id: int, payload: UpdateProductRequest) -> ProductCatalogItem:
        updated = self.repository.update_product(
            product_id=product_id,
            product_name=payload.product_name.strip() if payload.product_name is not None else None,
            simple_code=payload.simple_code.strip() if payload.simple_code is not None else None,
            status=payload.status,
        )
        if updated is None:
            raise ValueError("商品不存在")
        return updated

    def delete_product(self, product_id: int) -> bool:
        return self.repository.delete_product(product_id)

    def list_recipients(self) -> list[RecipientItem]:
        rows = self.repository.list_recipients()
        return [RecipientItem.model_validate(row) for row in rows]

    def create_recipient(self, payload: RecipientUpsertRequest) -> RecipientItem:
        row = self.repository.create_recipient(payload.model_dump())
        return RecipientItem.model_validate(row)

    def update_recipient(self, recipient_id: int, payload: RecipientUpsertRequest) -> RecipientItem:
        row = self.repository.update_recipient(recipient_id, payload.model_dump())
        if row is None:
            raise ValueError("收件人不存在")
        return RecipientItem.model_validate(row)

    def delete_recipient(self, recipient_id: int) -> bool:
        return self.repository.delete_recipient(recipient_id)

    def batch_upsert_recipients(self, payload: RecipientBatchUpsertRequest) -> int:
        items = []
        for item in payload.recipients:
            data = item.model_dump()
            data["name"] = data["name"].strip()
            data["phone"] = data["phone"].strip()
            data["address_detail"] = data["address_detail"].strip()
            data["raw_address"] = data["raw_address"].strip()
            if data.get("id_card_no"):
                data["id_card_no"] = str(data["id_card_no"]).strip().upper()
            items.append(data)
        return self.repository.batch_upsert_recipients(items)

    def import_recipients_from_image(self, image_bytes: bytes, mime_type: str) -> RecipientImportImageResponse:
        parsed = self.parser.parse_recipients_from_image(image_bytes=image_bytes, mime_type=mime_type)
        if not parsed:
            return RecipientImportImageResponse(imported_count=0, recipients=[])
        imported_count = self.repository.batch_upsert_recipients(parsed)
        rows = self.repository.list_recipients()
        recipient_map = {(str(row["name"]), str(row["phone"]), str(row["address_detail"])): row for row in rows}
        imported_rows = [
            recipient_map[(str(item["name"]), str(item["phone"]), str(item["address_detail"]))]
            for item in parsed
            if (str(item["name"]), str(item["phone"]), str(item["address_detail"])) in recipient_map
        ]
        return RecipientImportImageResponse(
            imported_count=imported_count,
            recipients=[RecipientItem.model_validate(row) for row in imported_rows],
        )

    def preview_recipients_from_image(self, image_bytes: bytes, mime_type: str) -> RecipientImportPreviewResponse:
        parsed = self.parser.parse_recipients_from_image(image_bytes=image_bytes, mime_type=mime_type)
        return RecipientImportPreviewResponse(
            recipients=[RecipientUpsertRequest.model_validate(item) for item in parsed],
        )

    def list_orders(self) -> list[OrderView]:
        rows = self.repository.list_orders()
        return [OrderView.model_validate(row) for row in rows]

    def create_order(self, payload: OrderCreateRequest) -> OrderView:
        row = self.repository.create_order_manual(payload.model_dump())
        return OrderView.model_validate(row)

    def update_order(self, order_id: int, payload: OrderUpdateRequest) -> OrderView:
        row = self.repository.update_order(order_id, payload.model_dump(exclude_none=True))
        if row is None:
            raise ValueError("订单不存在")
        return OrderView.model_validate(row)

    def delete_order(self, order_id: int) -> bool:
        return self.repository.delete_order(order_id)

    def list_senders(self) -> list[SenderProfileItem]:
        rows = self.repository.list_sender_profiles()
        return [SenderProfileItem.model_validate(row) for row in rows]

    def create_sender(self, payload: SenderProfileUpsertRequest) -> SenderProfileItem:
        row = self.repository.create_sender_profile(payload.model_dump())
        return SenderProfileItem.model_validate(row)

    def update_sender(self, sender_id: int, payload: SenderProfileUpsertRequest) -> SenderProfileItem:
        row = self.repository.update_sender_profile(sender_id, payload.model_dump())
        if row is None:
            raise ValueError("寄件人不存在")
        return SenderProfileItem.model_validate(row)

    def delete_sender(self, sender_id: int) -> bool:
        return self.repository.delete_sender_profile(sender_id)

    def batch_upsert_senders(self, payload: SenderBatchUpsertRequest) -> int:
        items = []
        for item in payload.senders:
            data = item.model_dump()
            data["name"] = data["name"].strip()
            data["phone"] = data["phone"].strip()
            data["street"] = data["street"].strip()
            data["house_no"] = data["house_no"].strip()
            data["postcode"] = data["postcode"].strip()
            data["city"] = data["city"].strip()
            data["country_code"] = data["country_code"].strip().upper()
            items.append(data)
        return self.repository.batch_upsert_sender_profiles(items)

    def import_senders_from_image(self, image_bytes: bytes, mime_type: str) -> SenderImportImageResponse:
        parsed = self.parser.parse_senders_from_image(image_bytes=image_bytes, mime_type=mime_type)
        if not parsed:
            return SenderImportImageResponse(imported_count=0, senders=[])
        imported_count = self.repository.batch_upsert_sender_profiles(parsed)
        rows = self.repository.list_sender_profiles()
        sender_map = {(str(row["name"]), str(row["phone"])): row for row in rows}
        imported_rows = [sender_map[(str(item["name"]), str(item["phone"]))] for item in parsed if (str(item["name"]), str(item["phone"])) in sender_map]
        return SenderImportImageResponse(
            imported_count=imported_count,
            senders=[SenderProfileItem.model_validate(row) for row in imported_rows],
        )

    def export_recipients_template(self) -> str:
        recipients = self.repository.list_recipients_for_export()
        path = self.template_exporter.export_recipients(recipients)
        return str(path)

    def export_orders_template(
        self,
        status: str | None = "ready_to_upload",
        recent_days: int | None = None,
        limit: int | None = None,
    ) -> str:
        orders = self.repository.list_orders_for_export(
            status=status,
            recent_days=recent_days,
            limit=limit,
        )
        sender_profile = self.repository.get_default_sender_profile()
        path = self.template_exporter.export_orders(orders, sender_profile=sender_profile)
        return str(path)
