from search_engine.ranking import bm25, rank_query


def test_bm25_term_frequency_saturation():
    assert bm25(2, 1, 3, 10, 10) > bm25(1, 1, 3, 10, 10)
    assert bm25(2, 1, 3, 10, 10) < 2 * bm25(1, 1, 3, 10, 10)


def test_bm25_short_document_advantage():
    assert bm25(2, 1, 4, 10, 20) > bm25(2, 1, 4, 40, 20)


def test_search_requires_keyword_match():
    docs = {
        "a": {"title": "Solar", "text": "star", "url": "", "version": 1, "doc_len": 5, "pagerank": .2},
        "b": {"title": "Earth", "text": "planet", "url": "", "version": 1, "doc_len": 8, "pagerank": .8},
    }
    postings = {"solar": [("a", 2, 1)], "planet": [("b", 1, 1)]}
    results = rank_query("SOLAR", docs, postings, 6.5)
    assert [r["doc_id"] for r in results] == ["a"]
    assert results[0]["score"] > 0
    assert rank_query("unknown", docs, postings, 6.5) == []


def test_tie_break_deterministic():
    docs = {x: {"title": x, "text": "y", "url": "", "version": 1,
                "doc_len": 1, "pagerank": .5} for x in ["b", "a"]}
    postings = {"y": [(x, 1, 2) for x in ["b", "a"]]}
    assert [r["doc_id"] for r in rank_query("y", docs, postings, 1)] == ["a", "b"]
