from pathlib import Path
from threading import Lock

import base64

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from app.db import init_db
from app.llm_client import Settings
from app.schemas import (
    BatchUpsertProductsRequest,
    CreateOrderFromInputResponse,
    OrderCreateRequest,
    OrderUpdateRequest,
    OrderView,
    CreateProductRequest,
    ParseOrderRequest,
    ParseOrderResponse,
    ProductCatalogItem,
    RecipientBatchUpsertRequest,
    RecipientImportPreviewResponse,
    RecipientImportImageRequest,
    RecipientImportImageResponse,
    RecipientItem,
    RecipientUpsertRequest,
    SenderBatchUpsertRequest,
    SenderImportImageRequest,
    SenderImportImageResponse,
    SenderProfileItem,
    SenderProfileUpsertRequest,
    UpdateProductRequest,
    UpdateProductStatusRequest,
)
from app.service import OrderParseService

app = FastAPI(title="ADA Order Pilot", version="0.1.0")
INDEX_FILE = Path(__file__).parent / "static" / "index.html"
RECIPIENTS_FILE = Path(__file__).parent / "static" / "recipients.html"
ORDERS_FILE = Path(__file__).parent / "static" / "orders.html"
PRODUCTS_FILE = Path(__file__).parent / "static" / "products.html"
SENDERS_FILE = Path(__file__).parent / "static" / "senders.html"
app.mount("/static", StaticFiles(directory=Path(__file__).parent / "static"), name="static")
_service_cache: dict[tuple[tuple[str, object], ...], OrderParseService] = {}
_initialized_db_paths: set[str] = set()
_service_cache_lock = Lock()


def _settings_cache_key(settings: Settings) -> tuple[tuple[str, object], ...]:
    return tuple(sorted(settings.model_dump().items()))


def get_service() -> OrderParseService:
    settings = Settings()
    key = _settings_cache_key(settings)
    with _service_cache_lock:
        if settings.db_path not in _initialized_db_paths:
            init_db(settings.db_path)
            _initialized_db_paths.add(settings.db_path)
        if key not in _service_cache:
            _service_cache[key] = OrderParseService(settings=settings)
        return _service_cache[key]


@app.on_event("startup")
def startup() -> None:
    get_service()


@app.get("/")
def index() -> FileResponse:
    return FileResponse(INDEX_FILE)


@app.get("/recipients")
def recipients_page() -> FileResponse:
    return FileResponse(RECIPIENTS_FILE)


@app.get("/orders")
def orders_page() -> FileResponse:
    return FileResponse(ORDERS_FILE)


@app.get("/products")
def products_page() -> FileResponse:
    return FileResponse(PRODUCTS_FILE)


@app.get("/senders")
def senders_page() -> FileResponse:
    return FileResponse(SENDERS_FILE)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/api/v1/parse-order-input", response_model=ParseOrderResponse)
def parse_order_input(payload: ParseOrderRequest) -> ParseOrderResponse:
    service = get_service()
    try:
        return service.parse(payload.input_text)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.post("/api/v1/orders/from-input", response_model=CreateOrderFromInputResponse)
def create_order_from_input(payload: ParseOrderRequest) -> CreateOrderFromInputResponse:
    service = get_service()
    try:
        return service.create_order_from_input(
            payload.input_text,
            recipient_id_card_no=payload.recipient_id_card_no,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.get("/api/v1/orders", response_model=list[OrderView])
def list_orders() -> list[OrderView]:
    service = get_service()
    return service.list_orders()


@app.post("/api/v1/orders", response_model=OrderView)
def create_order(payload: OrderCreateRequest) -> OrderView:
    service = get_service()
    return service.create_order(payload)


@app.put("/api/v1/orders/{order_id}", response_model=OrderView)
def update_order(order_id: int, payload: OrderUpdateRequest) -> OrderView:
    service = get_service()
    try:
        return service.update_order(order_id=order_id, payload=payload)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.delete("/api/v1/orders/{order_id}")
def delete_order(order_id: int) -> dict[str, bool]:
    service = get_service()
    deleted = service.delete_order(order_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="订单不存在")
    return {"deleted": True}


@app.get("/api/v1/recipients", response_model=list[RecipientItem])
def list_recipients() -> list[RecipientItem]:
    service = get_service()
    return service.list_recipients()


@app.post("/api/v1/recipients", response_model=RecipientItem)
def create_recipient(payload: RecipientUpsertRequest) -> RecipientItem:
    service = get_service()
    return service.create_recipient(payload)


@app.post("/api/v1/recipients/batch-upsert")
def batch_upsert_recipients(payload: RecipientBatchUpsertRequest) -> dict[str, int]:
    service = get_service()
    imported_count = service.batch_upsert_recipients(payload)
    return {"imported_count": imported_count}


@app.post("/api/v1/recipients/import-image", response_model=RecipientImportImageResponse)
def import_recipients_by_image(payload: RecipientImportImageRequest) -> RecipientImportImageResponse:
    if not payload.mime_type.startswith("image/"):
        raise HTTPException(status_code=400, detail="仅支持图片文件")
    try:
        content = base64.b64decode(payload.image_base64)
    except Exception as exc:
        raise HTTPException(status_code=400, detail="图片base64格式不正确") from exc
    service = get_service()
    try:
        return service.import_recipients_from_image(image_bytes=content, mime_type=payload.mime_type)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.post("/api/v1/recipients/parse-image", response_model=RecipientImportPreviewResponse)
def parse_recipients_by_image(payload: RecipientImportImageRequest) -> RecipientImportPreviewResponse:
    if not payload.mime_type.startswith("image/"):
        raise HTTPException(status_code=400, detail="仅支持图片文件")
    try:
        content = base64.b64decode(payload.image_base64)
    except Exception as exc:
        raise HTTPException(status_code=400, detail="图片base64格式不正确") from exc
    service = get_service()
    try:
        return service.preview_recipients_from_image(image_bytes=content, mime_type=payload.mime_type)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.put("/api/v1/recipients/{recipient_id}", response_model=RecipientItem)
def update_recipient(recipient_id: int, payload: RecipientUpsertRequest) -> RecipientItem:
    service = get_service()
    try:
        return service.update_recipient(recipient_id=recipient_id, payload=payload)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.delete("/api/v1/recipients/{recipient_id}")
def delete_recipient(recipient_id: int) -> dict[str, bool]:
    service = get_service()
    deleted = service.delete_recipient(recipient_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="收件人不存在")
    return {"deleted": True}


@app.get("/api/v1/products", response_model=list[ProductCatalogItem])
def list_products(keyword: str | None = None, include_inactive: bool = False) -> list[ProductCatalogItem]:
    service = get_service()
    return service.list_products(keyword=keyword, include_inactive=include_inactive)


@app.post("/api/v1/products", response_model=ProductCatalogItem)
def create_product(payload: CreateProductRequest) -> ProductCatalogItem:
    service = get_service()
    return service.create_product(payload)


@app.put("/api/v1/products/{product_id}", response_model=ProductCatalogItem)
def update_product(product_id: int, payload: UpdateProductRequest) -> ProductCatalogItem:
    service = get_service()
    try:
        return service.update_product(product_id=product_id, payload=payload)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.delete("/api/v1/products/{product_id}")
def delete_product(product_id: int) -> dict[str, bool]:
    service = get_service()
    deleted = service.delete_product(product_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="商品不存在")
    return {"deleted": True}


@app.post("/api/v1/products/batch-upsert")
def batch_upsert_products(payload: BatchUpsertProductsRequest) -> dict[str, int]:
    service = get_service()
    upserted_count = service.batch_upsert_products(payload)
    return {"upserted_count": upserted_count}


@app.patch("/api/v1/products/{product_id}/status", response_model=ProductCatalogItem)
def update_product_status(product_id: int, payload: UpdateProductStatusRequest) -> ProductCatalogItem:
    service = get_service()
    try:
        return service.update_product_status(product_id=product_id, payload=payload)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get("/api/v1/senders", response_model=list[SenderProfileItem])
def list_senders() -> list[SenderProfileItem]:
    service = get_service()
    return service.list_senders()


@app.post("/api/v1/senders", response_model=SenderProfileItem)
def create_sender(payload: SenderProfileUpsertRequest) -> SenderProfileItem:
    service = get_service()
    return service.create_sender(payload)


@app.post("/api/v1/senders/batch-upsert")
def batch_upsert_senders(payload: SenderBatchUpsertRequest) -> dict[str, int]:
    service = get_service()
    imported_count = service.batch_upsert_senders(payload)
    return {"imported_count": imported_count}


@app.post("/api/v1/senders/import-image", response_model=SenderImportImageResponse)
def import_senders_by_image(payload: SenderImportImageRequest) -> SenderImportImageResponse:
    if not payload.mime_type.startswith("image/"):
        raise HTTPException(status_code=400, detail="仅支持图片文件")
    try:
        content = base64.b64decode(payload.image_base64)
    except Exception as exc:
        raise HTTPException(status_code=400, detail="图片base64格式不正确") from exc
    service = get_service()
    try:
        return service.import_senders_from_image(image_bytes=content, mime_type=payload.mime_type)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.put("/api/v1/senders/{sender_id}", response_model=SenderProfileItem)
def update_sender(sender_id: int, payload: SenderProfileUpsertRequest) -> SenderProfileItem:
    service = get_service()
    try:
        return service.update_sender(sender_id=sender_id, payload=payload)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.delete("/api/v1/senders/{sender_id}")
def delete_sender(sender_id: int) -> dict[str, bool]:
    service = get_service()
    deleted = service.delete_sender(sender_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="寄件人不存在")
    return {"deleted": True}


@app.get("/api/v1/export/recipients-template")
def export_recipients_template() -> FileResponse:
    service = get_service()
    path = service.export_recipients_template()
    return FileResponse(path=path, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


@app.get("/api/v1/export/orders-template")
def export_orders_template(
    status: str | None = None,
    recent_days: int | None = None,
    limit: int | None = None,
) -> FileResponse:
    service = get_service()
    path = service.export_orders_template(status=status, recent_days=recent_days, limit=limit)
    return FileResponse(path=path, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


@app.get("/api/v1/export/orders")
def export_orders(
    status: str | None = None,
    recent_days: int | None = None,
    limit: int | None = None,
) -> FileResponse:
    service = get_service()
    path = service.export_orders_template(status=status, recent_days=recent_days, limit=limit)
    return FileResponse(path=path, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
