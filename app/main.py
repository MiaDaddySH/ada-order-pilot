from fastapi import FastAPI

from app.schemas import CreateOrderFromInputResponse, ParseOrderRequest, ParseOrderResponse
from app.service import OrderParseService

app = FastAPI(title="ADA Order Pilot", version="0.1.0")


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/api/v1/parse-order-input", response_model=ParseOrderResponse)
def parse_order_input(payload: ParseOrderRequest) -> ParseOrderResponse:
    service = OrderParseService()
    return service.parse(payload.input_text)


@app.post("/api/v1/orders/from-input", response_model=CreateOrderFromInputResponse)
def create_order_from_input(payload: ParseOrderRequest) -> CreateOrderFromInputResponse:
    service = OrderParseService()
    return service.create_order_from_input(payload.input_text)
