from app.db import init_db
from app.llm_client import LLMOrderParser, Settings
from app.repository import OrderRepository
from app.schemas import (
    CreateOrderFromInputResponse,
    ParseOrderResponse,
    ParsedProduct,
    ParsedRecipient,
)


class OrderParseService:
    def __init__(self) -> None:
        self.settings = Settings()
        self.parser = LLMOrderParser(self.settings)
        init_db(self.settings.db_path)
        self.repository = OrderRepository(self.settings.db_path)

    def parse(self, input_text: str) -> ParseOrderResponse:
        result = self.parser.parse_order(input_text)
        recipient = ParsedRecipient.model_validate(result.recipient)
        products = [ParsedProduct.model_validate(item) for item in result.products]
        return ParseOrderResponse(
            recipient=recipient,
            products=products,
            confidence=result.confidence,
            needs_review=result.needs_review,
        )

    def create_order_from_input(self, input_text: str) -> CreateOrderFromInputResponse:
        parsed = self.parse(input_text)
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
