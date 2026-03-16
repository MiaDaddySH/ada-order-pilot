import os
from pathlib import Path

from fastapi.testclient import TestClient

from app.main import app


def test_health() -> None:
    client = TestClient(app)
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_parse_order_input(tmp_path: Path) -> None:
    os.environ["DB_PATH"] = str(tmp_path) + "/test_parse.db"
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
