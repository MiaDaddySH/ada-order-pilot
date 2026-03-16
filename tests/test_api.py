from fastapi.testclient import TestClient

from app.main import app


def test_health() -> None:
    client = TestClient(app)
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_parse_order_input() -> None:
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
