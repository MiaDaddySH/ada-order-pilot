from pydantic import BaseModel, Field


class ParseOrderRequest(BaseModel):
    input_text: str = Field(min_length=1, max_length=2000)
    recipient_id_card_no: str | None = Field(default=None, min_length=6, max_length=32)


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
    id_card_no: str | None = None
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


class RecipientItem(BaseModel):
    id: int
    name: str
    phone: str
    id_card_no: str | None = None
    province: str | None = None
    city: str | None = None
    district: str | None = None
    address_detail: str
    raw_address: str
    postcode: str | None = None


class RecipientUpsertRequest(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    phone: str = Field(min_length=1, max_length=30)
    id_card_no: str | None = Field(default=None, min_length=6, max_length=32)
    province: str | None = None
    city: str | None = None
    district: str | None = None
    address_detail: str = Field(min_length=1, max_length=500)
    raw_address: str = Field(min_length=1, max_length=1000)
    postcode: str | None = None


class OrderItemPayload(BaseModel):
    simple_code: str = Field(min_length=1, max_length=64)
    brand: str | None = None
    product_name: str = Field(min_length=1, max_length=200)
    stage: str | None = None
    quantity: int = Field(ge=1, le=999)
    unit: str = Field(min_length=1, max_length=20)


class OrderItemView(OrderItemPayload):
    id: int


class OrderCreateRequest(BaseModel):
    recipient_id: int = Field(ge=1)
    source_text: str = ""
    confidence: float = Field(default=1.0, ge=0, le=1)
    needs_review: bool = False
    status: str = Field(default="ready_to_upload", min_length=1, max_length=64)
    items: list[OrderItemPayload] = Field(min_length=1, max_length=6)


class OrderUpdateRequest(BaseModel):
    status: str | None = Field(default=None, min_length=1, max_length=64)
    needs_review: bool | None = None
    recipient_id: int | None = Field(default=None, ge=1)


class OrderView(BaseModel):
    id: int
    order_no: str
    recipient_id: int
    source_text: str
    confidence: float
    needs_review: bool
    status: str
    created_at: str
    recipient_name: str
    recipient_phone: str
    items: list[OrderItemView]


class SenderProfileItem(BaseModel):
    id: int
    name: str
    phone: str
    street: str
    house_no: str
    postcode: str
    city: str
    country_code: str
    is_default: bool


class SenderProfileUpsertRequest(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    phone: str = Field(min_length=1, max_length=30)
    street: str = Field(min_length=1, max_length=200)
    house_no: str = Field(min_length=1, max_length=30)
    postcode: str = Field(min_length=1, max_length=20)
    city: str = Field(min_length=1, max_length=100)
    country_code: str = Field(min_length=2, max_length=8)
    is_default: bool = False


class SenderBatchUpsertRequest(BaseModel):
    senders: list[SenderProfileUpsertRequest] = Field(min_length=1, max_length=500)


class SenderImportImageResponse(BaseModel):
    imported_count: int
    senders: list[SenderProfileItem]


class SenderImportImageRequest(BaseModel):
    image_base64: str = Field(min_length=16)
    mime_type: str = Field(min_length=5, max_length=50)


class CreateProductRequest(BaseModel):
    product_name: str = Field(min_length=1, max_length=200)
    simple_code: str = Field(min_length=1, max_length=64)


class UpdateProductRequest(BaseModel):
    product_name: str | None = Field(default=None, min_length=1, max_length=200)
    simple_code: str | None = Field(default=None, min_length=1, max_length=64)
    status: int | None = Field(default=None, ge=0, le=1)


class BatchUpsertProductsRequest(BaseModel):
    products: list[CreateProductRequest] = Field(min_length=1, max_length=1000)


class UpdateProductStatusRequest(BaseModel):
    status: int = Field(ge=0, le=1)
