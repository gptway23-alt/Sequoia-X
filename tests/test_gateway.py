"""No live BaoStock or SQLite is needed for security/regression tests."""
from fastapi.testclient import TestClient
from sequoia_x.gateway import app as gateway

client = TestClient(gateway.app)
KEY = "a-long-random-test-only-key-123456789abcdef"
HEADERS = {"Authorization": f"Bearer {KEY}"}


def test_no_database_routes(monkeypatch):
    monkeypatch.setenv("SEQUOIA_GATEWAY_API_KEY", KEY)
    paths = {r.path for r in gateway.app.routes}
    assert paths == {"/health", "/v1/stocks/{symbol}/daily"}
    for route in gateway.app.routes:
        assert route.methods == {"GET"}
    assert client.get("/database", headers=HEADERS).status_code == 404
    assert client.post("/v1/stocks/301363/daily", headers=HEADERS).status_code == 405


def test_auth_is_mandatory(monkeypatch):
    monkeypatch.setenv("SEQUOIA_GATEWAY_API_KEY", KEY)
    assert client.get("/health").status_code == 401
    assert client.get("/health", headers=HEADERS).json()["database_access"] is False


def test_reads_without_sqlite(monkeypatch):
    monkeypatch.setenv("SEQUOIA_GATEWAY_API_KEY", KEY)
    def fake_fetch(symbol, start, end):
        assert (symbol, start, end) == ("301363", "2026-07-01", "2026-10-09")
        return [(symbol, "2026-10-09", 10.0, 11.0, 9.0, 10.5, 123.0, 1300.0)]
    monkeypatch.setattr(gateway, "_query_raw", fake_fetch)
    r = client.get("/v1/stocks/301363/daily?start=2026-07-01&end=2026-10-09",headers=HEADERS)
    assert r.status_code == 200
    body = r.json()
    assert body["count"] == 1
    assert body["price_asof"] == "2026-10-09"
    assert body["bars"][0]["close"] == 10.5
    assert body["is_realtime"] is False


def test_rejects_unbounded_requests(monkeypatch):
    monkeypatch.setenv("SEQUOIA_GATEWAY_API_KEY", KEY)
    assert client.get("/v1/stocks/301363/daily?start=2024-01-01&end=2026-10-09",headers=HEADERS).status_code == 422
    assert client.get("/v1/stocks/301363/daily?start=2026-10-09&end=2026-07-01",headers=HEADERS).status_code == 422
    assert client.get("/v1/stocks/NOTSTOCK/daily?start=2026-07-01&end=2026-10-09",headers=HEADERS).status_code == 422
