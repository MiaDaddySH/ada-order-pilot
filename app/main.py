from fastapi import FastAPI

from app.schemas import ParseOrderRequest, ParseOrderResponse
from app.service import OrderParseService

app = FastAPI(title="ADA Order Pilot", version="0.1.0")
service = OrderParseService()


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/api/v1/parse-order-input", response_model=ParseOrderResponse)
def parse_order_input(payload: ParseOrderRequest) -> ParseOrderResponse:
    return service.parse(payload.input_text)
