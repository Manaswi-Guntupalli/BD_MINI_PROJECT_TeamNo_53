"""Deterministic BM25 + normalized PageRank ranking on Node 4."""
from __future__ import annotations
import math
from search_engine.records import tokenize


def bm25(tf: int, df: int, document_count: int, doc_len: int,
         avgdl: float, k1: float = 1.2, b: float = 0.75) -> float:
    if not tf or not df or not document_count or avgdl <= 0:
        return 0.0
    idf = math.log(1 + (document_count - df + 0.5) / (df + 0.5))
    return idf * (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * doc_len / avgdl))


def rank_query(query: str, documents: dict, postings: dict, avgdl: float,
               *, limit: int = 10) -> list[dict]:
    """Candidate-only lexical retrieval; PageRank is a secondary ranking signal.

    Pages with zero matching terms never appear in search results.
    Normalize BM25 by max among candidates, PageRank by global maximum.
    """
    terms = list(dict.fromkeys(tokenize(query)))
    if not terms or not documents:
        return []
    n = len(documents)
    raw = {}
    for term in terms:
        for doc_id, tf, df in postings.get(term, []):
            if doc_id in documents:
                document = documents[doc_id]
                raw[doc_id] = raw.get(doc_id, 0.0) + bm25(
                    tf, df, n, document["doc_len"], avgdl)
    if not raw:
        return []
    max_bm25 = max(raw.values()) or 1.0
    max_rank = max(float(d["pagerank"]) for d in documents.values()) or 1.0
    results = []
    for doc_id, score in raw.items():
        doc = documents[doc_id]
        rank = max(float(doc["pagerank"]), 0.0)
        total = 0.7 * score / max_bm25 + 0.3 * rank / max_rank
        result = {"doc_id": doc_id, "title": doc["title"], "url": doc["url"],
                  "snippet": doc["text"][:220], "score": round(total, 8),
                  "bm25": round(score, 8), "pagerank": round(rank, 8),
                  "version": doc["version"]}
        results.append(result)
    results.sort(key=lambda r: (-r["score"], r["doc_id"]))
    return results[:max(0, min(int(limit), 100))]
