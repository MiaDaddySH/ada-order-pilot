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
    RecipientItem,
    RecipientUpsertRequest,
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
        if recipient_id_card_no and recipient_id_card_no.strip():
            parsed.recipient.id_card_no = recipient_id_card_no.strip()
        if not parsed.recipient.id_card_no:
            raise ValueError("缺少收件人身份证号码")
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
