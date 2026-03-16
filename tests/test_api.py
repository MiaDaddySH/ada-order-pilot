import os
from io import BytesIO
from pathlib import Path

from fastapi.testclient import TestClient
from openpyxl import Workbook, load_workbook

from app.main import app


def test_health() -> None:
    client = TestClient(app)
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_index_page() -> None:
    client = TestClient(app)
    response = client.get("/")
    assert response.status_code == 200
    assert "text/html" in response.headers.get("content-type", "")
    assert "ADA 智能订单系统" in response.text


def test_parse_order_input(tmp_path: Path) -> None:
    os.environ["DB_PATH"] = str(tmp_path) + "/test_parse.db"
    os.environ["LLM_API_KEY"] = ""
    os.environ["LLM_BASE_URL"] = ""
    client = TestClient(app)
    payload = {
        "input_text": "广东省广州市花都区庙南巷42号嘉汇城西区4栋，游锦平13416101033（Holle 羊2段4盒）"
    }
    response = client.post("/api/v1/parse-order-input", json=payload)
    assert response.status_code == 200
    body = response.json()
    assert "recipient" in body
    assert "products" in body
    assert isinstance(body["products"], list)
    assert body["recipient"]["province"] == "广东省"
    assert body["recipient"]["city"] == "广州市"
    assert body["recipient"]["district"] == "花都区"
    assert "庙南巷42号" in body["recipient"]["address_detail"]
    assert body["products"][0]["simple_code"] is not None
    assert "holle" in (body["products"][0]["brand"] or "").lower()
    assert "holle" in body["products"][0]["product_name"].lower()


def test_create_order_from_input_idempotent(tmp_path: Path) -> None:
    os.environ["DB_PATH"] = str(tmp_path) + "/test_order.db"
    client = TestClient(app)
    payload = {
        "input_text": "广东省广州市花都区庙南巷42号嘉汇城西区4栋，游锦平13416101033（HO2 4盒）"
    }
    first = client.post("/api/v1/orders/from-input", json=payload)
    second = client.post("/api/v1/orders/from-input", json=payload)
    assert first.status_code == 200
    assert second.status_code == 200
    first_body = first.json()
    second_body = second.json()
    assert first_body["order_created"] is True
    assert second_body["order_created"] is False
    assert first_body["order_no"] == second_body["order_no"]
    assert first_body["parse_result"]["products"][0]["simple_code"] == "HO2"


def test_create_order_from_input_requires_simple_code(tmp_path: Path) -> None:
    os.environ["DB_PATH"] = str(tmp_path) + "/test_order_2.db"
    client = TestClient(app)
    payload = {"input_text": "上海市静安区南京西路100号，王明13800138000（未知奶粉 2盒）"}
    response = client.post("/api/v1/orders/from-input", json=payload)
    assert response.status_code == 422


def test_product_catalog_create_list_and_disable(tmp_path: Path) -> None:
    os.environ["DB_PATH"] = str(tmp_path) + "/test_products.db"
    client = TestClient(app)
    created = client.post(
        "/api/v1/products",
        json={"product_name": "测试奶粉1段800克", "simple_code": "TEST1"},
    )
    assert created.status_code == 200
    product_id = created.json()["id"]
    queried = client.get("/api/v1/products", params={"keyword": "TEST1"})
    assert queried.status_code == 200
    assert any(item["simple_code"] == "TEST1" for item in queried.json())
    disabled = client.patch(f"/api/v1/products/{product_id}/status", json={"status": 0})
    assert disabled.status_code == 200
    active_only = client.get("/api/v1/products", params={"keyword": "TEST1"})
    assert active_only.status_code == 200
    assert all(item["id"] != product_id for item in active_only.json())
    include_inactive = client.get(
        "/api/v1/products",
        params={"keyword": "TEST1", "include_inactive": True},
    )
    assert include_inactive.status_code == 200
    assert any(item["id"] == product_id and item["status"] == 0 for item in include_inactive.json())


def test_product_batch_upsert(tmp_path: Path) -> None:
    os.environ["DB_PATH"] = str(tmp_path) + "/test_products_batch.db"
    client = TestClient(app)
    response = client.post(
        "/api/v1/products/batch-upsert",
        json={
            "products": [
                {"product_name": "批量奶粉A", "simple_code": "BATCHA"},
                {"product_name": "批量奶粉B", "simple_code": "BATCHB"},
            ]
        },
    )
    assert response.status_code == 200
    assert response.json()["upserted_count"] == 2
    queried = client.get("/api/v1/products", params={"keyword": "BATCH"})
    assert queried.status_code == 200
    codes = {item["simple_code"] for item in queried.json()}
    assert "BATCHA" in codes
    assert "BATCHB" in codes


def test_export_templates(tmp_path: Path) -> None:
    recipient_template = tmp_path / "收件人模板.xlsx"
    order_template = tmp_path / "订单导入模板.xlsx"
    _create_recipient_template(recipient_template)
    _create_order_template(order_template)
    os.environ["DB_PATH"] = str(tmp_path / "test_export.db")
    os.environ["RECIPIENT_TEMPLATE_PATH"] = str(recipient_template)
    os.environ["ORDER_TEMPLATE_PATH"] = str(order_template)
    os.environ["EXPORT_DIR"] = str(tmp_path / "exports")
    client = TestClient(app)
    payload = {
        "input_text": "广东省广州市花都区庙南巷42号嘉汇城西区4栋，游锦平13416101033（HO2 4盒）"
    }
    created = client.post("/api/v1/orders/from-input", json=payload)
    assert created.status_code == 200
    recipients_file = client.get("/api/v1/export/recipients-template")
    orders_file = client.get("/api/v1/export/orders-template", params={"recent_days": 7, "limit": 10})
    orders_data_file = client.get("/api/v1/export/orders", params={"recent_days": 7, "limit": 10})
    assert recipients_file.status_code == 200
    assert orders_file.status_code == 200
    assert orders_data_file.status_code == 200
    assert recipients_file.content[:2] == b"PK"
    assert orders_file.content[:2] == b"PK"
    assert orders_data_file.content[:2] == b"PK"
    workbook = load_workbook(BytesIO(orders_data_file.content))
    worksheet = workbook[workbook.sheetnames[0]]
    assert worksheet.cell(row=2, column=9).value not in (None, "")


def _create_recipient_template(path: Path) -> None:
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "导入数据"
    sheet["A1"] = "中国地址模板导入"
    headers = ["*姓名", "*身份证号码", "*电话国际区号", "*电话号码", "*省", "*市", "*区", "*详细地址", "*邮编"]
    for idx, header in enumerate(headers, start=1):
        sheet.cell(row=2, column=idx).value = header
    sheet["C3"] = "86"
    workbook.save(path)


def _create_order_template(path: Path) -> None:
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "批量下单"
    headers = [
        "包裹备注",
        "寄件人姓名",
        "寄件人电话",
        "路名",
        "门牌号",
        "寄件人邮编",
        "寄件人城市",
        "寄件人国家简称",
        "收件人姓名",
        "身份证号",
        "手机号码",
        "收件人国家简称",
        "省",
        "市",
        "区/县",
        "详细地址（省市区/县请勿重复填）",
        "渠道代码",
        "货物用途",
        "商品代码1",
        "数量1",
        "商品代码2",
        "数量2",
        "商品代码3",
        "数量3",
        "商品代码4",
        "数量4",
        "商品代码5",
        "数量5",
        "商品代码6",
        "数量6",
    ]
    for idx, header in enumerate(headers, start=1):
        sheet.cell(row=1, column=idx).value = header
    workbook.save(path)
