from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse

from app.schemas import (
    BatchUpsertProductsRequest,
    CreateOrderFromInputResponse,
    CreateProductRequest,
    ParseOrderRequest,
    ParseOrderResponse,
    ProductCatalogItem,
    UpdateProductStatusRequest,
)
from app.service import OrderParseService

app = FastAPI(title="ADA Order Pilot", version="0.1.0")
INDEX_FILE = Path(__file__).parent / "static" / "index.html"


@app.get("/")
def index() -> FileResponse:
    return FileResponse(INDEX_FILE)


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
    try:
        return service.create_order_from_input(payload.input_text)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@app.get("/api/v1/products", response_model=list[ProductCatalogItem])
def list_products(keyword: str | None = None, include_inactive: bool = False) -> list[ProductCatalogItem]:
    service = OrderParseService()
    return service.list_products(keyword=keyword, include_inactive=include_inactive)


@app.post("/api/v1/products", response_model=ProductCatalogItem)
def create_product(payload: CreateProductRequest) -> ProductCatalogItem:
    service = OrderParseService()
    return service.create_product(payload)


@app.post("/api/v1/products/batch-upsert")
def batch_upsert_products(payload: BatchUpsertProductsRequest) -> dict[str, int]:
    service = OrderParseService()
    upserted_count = service.batch_upsert_products(payload)
    return {"upserted_count": upserted_count}


@app.patch("/api/v1/products/{product_id}/status", response_model=ProductCatalogItem)
def update_product_status(product_id: int, payload: UpdateProductStatusRequest) -> ProductCatalogItem:
    service = OrderParseService()
    try:
        return service.update_product_status(product_id=product_id, payload=payload)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get("/api/v1/export/recipients-template")
def export_recipients_template() -> FileResponse:
    service = OrderParseService()
    path = service.export_recipients_template()
    return FileResponse(path=path, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


@app.get("/api/v1/export/orders-template")
def export_orders_template(
    status: str | None = "ready_to_upload",
    recent_days: int | None = None,
    limit: int | None = None,
) -> FileResponse:
    service = OrderParseService()
    path = service.export_orders_template(status=status, recent_days=recent_days, limit=limit)
    return FileResponse(path=path, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


@app.get("/api/v1/export/orders")
def export_orders(
    status: str | None = "ready_to_upload",
    recent_days: int | None = None,
    limit: int | None = None,
) -> FileResponse:
    service = OrderParseService()
    path = service.export_orders_template(status=status, recent_days=recent_days, limit=limit)
    return FileResponse(path=path, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
