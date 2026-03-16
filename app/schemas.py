from pydantic import BaseModel, Field


class ParseOrderRequest(BaseModel):
    input_text: str = Field(min_length=1, max_length=2000)


class ParsedProduct(BaseModel):
    brand: str | None = None
    product_name: str
    stage: str | None = None
    quantity: int = Field(ge=1, le=999)
    unit: str = "盒"


class ParsedRecipient(BaseModel):
    name: str
    phone: str
    province: str | None = None
    city: str | None = None
    district: str | None = None
    address_detail: str
    raw_address: str
    postcode: str | None = None


class ParseOrderResponse(BaseModel):
    recipient: ParsedRecipient
    products: list[ParsedProduct]
    confidence: float = Field(ge=0, le=1)
    needs_review: bool


class CreateOrderFromInputResponse(BaseModel):
    order_no: str
    order_status: str
    order_created: bool
    recipient_id: int
    recipient_created: bool
    parse_result: ParseOrderResponse
