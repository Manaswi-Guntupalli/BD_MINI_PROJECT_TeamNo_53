"""Node 4 standalone versioned Parquet reader (no HDFS/Spark runtime)."""
from __future__ import annotations
import json
import os
from pathlib import Path
import threading
import logging
from search_engine.ranking import rank_query

logger = logging.getLogger(__name__)


class SearchIndex:
    def __init__(self, root: str | Path):
        self.root = Path(root).resolve()
        self.current = self.root / "current"
        self._lock = threading.RLock()
        self._version_path = ""
        self.manifest: dict = {}
        self.documents: dict = {}
        self.postings: dict = {}
        self.avgdl: float = 0.0

    @staticmethod
    def read_release(path: Path):
        # Deliberately import pyarrow only at serving/deployment time.
        import pyarrow.dataset as ds
        manifest = json.loads((path / "manifest.json").read_text(encoding="utf-8"))
        if manifest.get("format_version") != 1:
            raise ValueError("Unsupported manifest format")
        if manifest.get("bm25_k1") != 1.2 or manifest.get("bm25_b") != 0.75:
            raise ValueError("Unexpected course-required BM25 settings")
        if manifest.get("pagerank_damping") != 0.85 or manifest.get("pagerank_iterations", 0) < 10:
            raise ValueError("Unexpected PageRank configuration")
        if manifest.get("bm25_weight") != 0.7 or manifest.get("pagerank_weight") != 0.3:
            raise ValueError("Unexpected course-required ranking weights")
        docs_rows = ds.dataset(str(path / "docs"), format="parquet").to_table().to_pylist()
        postings_rows = ds.dataset(str(path / "postings"), format="parquet").to_table().to_pylist()
        docs = {str(d["doc_id"]): d for d in docs_rows}
        if len(docs) != len(docs_rows) or len(docs) != manifest["documents"]:
            raise ValueError("Duplicate documents / document count mismatch")
        if len(postings_rows) != manifest["postings"]:
            raise ValueError("Posting count mismatch")
        rank_sum = sum(float(d["pagerank"]) for d in docs.values())
        if abs(rank_sum - 1.0) > 1e-4:
            raise ValueError("PageRank sum mismatch")
        posts: dict[str, list[tuple[str, int, int]]] = {}
        for p in postings_rows:
            doc_id = str(p["doc_id"])
            if doc_id not in docs or p["tf"] <= 0 or p["df"] <= 0:
                raise ValueError("Invalid posting")
            posts.setdefault(p["term"], []).append((doc_id, int(p["tf"]), int(p["df"])))
        avgdl = float(manifest["avgdl"])
        if avgdl <= 0:
            raise ValueError("Average document length must be > 0")
        return manifest, docs, posts, avgdl

    def reload_if_changed(self) -> bool:
        """Keep last good in-memory release if an invalid version appears."""
        resolved = str(self.current.resolve(strict=False))
        with self._lock:
            if resolved == self._version_path:
                return bool(self.documents)
            if not self.current.exists():
                return bool(self.documents)
            try:
                candidate = Path(resolved)
                if self.root / "versions" not in candidate.parents:
                    raise ValueError("Current pointer must target the versions directory")
                manifest, docs, posts, avgdl = self.read_release(candidate)
            except Exception as exc:  # PyArrow also raises ArrowInvalid/ArrowIOError on broken releases
                logger.exception("Rejected search index update: %s", exc)
                return bool(self.documents)
            self.manifest, self.documents, self.postings = manifest, docs, posts
            self.avgdl = avgdl
            self._version_path = resolved
            return True

    def search(self, query: str, limit: int = 10):
        self.reload_if_changed()
        with self._lock:
            if not self.documents:
                raise RuntimeError("No validated index deployed")
            return rank_query(query, self.documents, self.postings, self.avgdl, limit=limit)

    def status(self) -> dict:
        self.reload_if_changed()
        with self._lock:
            return {"ready": bool(self.documents), "version": self.manifest.get("version"),
                    "documents": len(self.documents),
                    "source_snapshot_id": self.manifest.get("source_snapshot_id")}
