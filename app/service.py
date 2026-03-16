from app.llm_client import LLMOrderParser, Settings
from app.schemas import ParseOrderResponse, ParsedProduct, ParsedRecipient


class OrderParseService:
    def __init__(self) -> None:
        self.parser = LLMOrderParser(Settings())

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
