import json
import pytest
from search_engine.records import normalize_document, parse_line, tokenize


def test_parses_aliases_and_links():
    d = normalize_document({"id": 14, "title": "Tree", "content": "A green tree", "links":
                            [{"url": "https://example.test/15"}, {"id": 9}, 9], "revision": 4})
    assert d["doc_id"] == "14"
    assert d["version"] == 4
    assert d["raw_links"] == ["https://example.test/15", "9"]
    assert not d["is_deleted"]


def test_phase_fallback_and_idempotent_hash():
    line = '{"doc_id":"x","title":"X","text":"Text"}'
    one = normalize_document(line, 1)
    two = normalize_document(line, 1)
    assert one == two
    assert one["version"] == 1
    assert normalize_document(line, 0)["version"] == 0


def test_delete_tombstone_can_omit_body():
    d = normalize_document({"doc_id": "x", "version": 2, "event_type": "delete"})
    assert d["is_deleted"]
    assert d["title"] == ""


def test_invalid_document_is_rejected_without_crashing_batch():
    okay, error = parse_line("{broken", 0)
    assert not okay
    assert "JSON" in error["reason"]
    okay, error = parse_line(json.dumps({"id": "x", "title": "Only title"}), 0)
    assert not okay
    assert "text" in error["reason"]


def test_tokenizer():
    assert tokenize("Hello, PLANET-42! 🌙") == ["hello", "planet", "42"]


def test_replaying_canonical_kafka_event_keeps_links():
    initial = normalize_document({"doc_id": "x", "title": "X", "text": "Body", "out_links": ["y"]})
    again = normalize_document(initial)
    assert initial == again


def test_string_false_flag_not_a_delete():
    d = normalize_document({"id": "1", "title": "T", "text": "Text", "is_deleted": "false"})
    assert not d["is_deleted"]


def test_reject_fractional_version():
    with pytest.raises(ValueError, match="version"):
        normalize_document({"id": "1", "title": "T", "text": "Text", "version": 1.5})
