from fastapi.testclient import TestClient
from search_engine.api import app, index


def test_api_search_and_frontend(monkeypatch):
    monkeypatch.setattr(index, "documents", {"a": {
        "title": "Solar system", "text": "The Sun and planets", "url": "https://example.test/a",
        "version": 1, "pagerank": 1.0, "doc_len": 6}})
    monkeypatch.setattr(index, "postings", {"solar": [("a", 2, 1)]})
    monkeypatch.setattr(index, "avgdl", 6.0)
    monkeypatch.setattr(index, "manifest", {"version": "unit-v1"})
    client = TestClient(app)
    res = client.get("/api/search", params={"q": "solar"})
    assert res.status_code == 200
    assert res.json()["results"][0]["doc_id"] == "a"
    assert res.json()["index_version"] == "unit-v1"
    assert "ATLAS" in client.get("/").text
    assert client.get("/api/search", params={"q": ""}).status_code == 422
    assert client.get("/static/../../README.md").status_code in {404, 405}
