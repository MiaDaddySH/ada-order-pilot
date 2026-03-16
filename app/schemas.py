from pydantic import BaseModel, Field


class ParseOrderRequest(BaseModel):
    input_text: str = Field(min_length=1, max_length=2000)


class ParsedProduct(BaseModel):
    brand: str | None = None
    product_name: str
    stage: str | None = None
    quantity: int = Field(ge=1, le=999)
    unit: str = "盒"
    simple_code: str | None = None


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
    parse_source: str


class CreateOrderFromInputResponse(BaseModel):
    order_no: str
    order_status: str
    order_created: bool
    recipient_id: int
    recipient_created: bool
    parse_result: ParseOrderResponse


class ProductCatalogItem(BaseModel):
    id: int
    product_name: str
    simple_code: str
    status: int


class CreateProductRequest(BaseModel):
    product_name: str = Field(min_length=1, max_length=200)
    simple_code: str = Field(min_length=1, max_length=64)


class BatchUpsertProductsRequest(BaseModel):
    products: list[CreateProductRequest] = Field(min_length=1, max_length=1000)


class UpdateProductStatusRequest(BaseModel):
    status: int = Field(ge=0, le=1)
