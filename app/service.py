from app.db import init_db
from app.llm_client import LLMOrderParser, Settings
from app.repository import OrderRepository
from app.schemas import (
    BatchUpsertProductsRequest,
    CreateProductRequest,
    CreateOrderFromInputResponse,
    ParseOrderResponse,
    ParsedProduct,
    ParsedRecipient,
    ProductCatalogItem,
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
        unresolved = False
        for product in products:
            resolved = self._resolve_product_code(input_text, product)
            product.simple_code = resolved
            if resolved is None:
                unresolved = True
                continue
            catalog_product = self.repository.get_active_product_by_code(resolved)
            if catalog_product is not None:
                catalog_name, catalog_brand = catalog_product
                product.product_name = catalog_name
                if catalog_brand:
                    product.brand = catalog_brand
        confidence = result.confidence
        if unresolved:
            confidence = min(confidence, 0.5)
        return ParseOrderResponse(
            recipient=recipient,
            products=products,
            confidence=confidence,
            needs_review=result.needs_review or unresolved,
        )

    def create_order_from_input(self, input_text: str) -> CreateOrderFromInputResponse:
        parsed = self.parse(input_text)
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

    def _resolve_product_code(self, input_text: str, product: ParsedProduct) -> str | None:
        if product.simple_code and self.repository.product_code_exists(product.simple_code):
            return product.simple_code
        return self.repository.resolve_product_code(
            source_text=input_text,
            product_name=product.product_name,
            brand=product.brand,
            stage=product.stage,
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

    def export_recipients_template(self) -> str:
        recipients = self.repository.list_recipients_for_export()
        path = self.template_exporter.export_recipients(recipients)
        return str(path)

    def export_orders_template(self, status: str | None = "ready_to_upload") -> str:
        orders = self.repository.list_orders_for_export(status=status)
        path = self.template_exporter.export_orders(orders)
        return str(path)
