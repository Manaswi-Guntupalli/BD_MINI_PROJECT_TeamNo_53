"""FastAPI search endpoint and single-page frontend, deploy only on Node 4."""
from __future__ import annotations
import os
from pathlib import Path
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from search_engine.serving import SearchIndex

PROJECT_ROOT = Path(__file__).resolve().parents[1]
app = FastAPI(title="Distributed Search Engine", version="0.1.0")
index = SearchIndex(os.getenv("SERVING_ROOT", "/srv/distributed-search"))


@app.get("/api/health")
def health():
    return index.status()


@app.get("/api/search")
def search(q: str = Query(min_length=1, max_length=256),
           limit: int = Query(default=10, ge=1, le=50)):
    try:
        results = index.search(q, limit)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return {"query": q, "count": len(results), "index_version": index.manifest["version"],
            "results": results}


@app.get("/", include_in_schema=False)
def frontend():
    return FileResponse(PROJECT_ROOT / "frontend" / "index.html")


@app.get("/static/{filename}", include_in_schema=False)
def static(filename: str):
    if filename not in {"app.css", "app.js"}:
        raise HTTPException(404)
    return FileResponse(PROJECT_ROOT / "frontend" / filename)
