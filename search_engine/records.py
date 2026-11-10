"""Canonical document/event normalization shared by batch and Kafka ingestion.

The official Phase 0/1 corpus uses doc_id/url/title/text/optional links,
and Phase 1 uses op=DELETE or op=UPSERT without explicit version.
"""
from __future__ import annotations

import hashlib
import json
import re
from typing import Any

TOKEN_PATTERN = re.compile(r"[a-z0-9]+")


def tokenize(text: str) -> list[str]:
    return TOKEN_PATTERN.findall((text or "").lower())


def _pick(item: dict, keys: tuple[str, ...], default=None):
    for key in keys:
        if key in item and item[key] is not None:
            return item[key]
    return default


def _links(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        # A string can be a single URL/id, not a delimiter-separated list.
        value = [value]
    if not isinstance(value, list):
        raise ValueError("links must be a list or a single string")
    links = []
    for entry in value:
        if isinstance(entry, dict):
            entry = _pick(entry, ("target_id", "doc_id", "document_id", "id", "url", "href"))
        if entry is None:
            continue
        if not isinstance(entry, (str, int)):
            raise ValueError("link target must be a string, integer, or identifier object")
        target = str(entry).strip()
        if target and target not in links:
            links.append(target)
    return links


def _is_true(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes"}
    raise ValueError("invalid deleted flag")


def normalize_document(value: str | dict[str, Any], phase: int = 0) -> dict[str, Any]:
    """Return canonical JSON-safe record, or raise a descriptive ValueError.

    Supports the verified uploaded course dataset schema as well as aliases.
    Full cluster execution still requires Spark/HDFS integration tests.
    """
    try:
        item = json.loads(value) if isinstance(value, str) else value
    except json.JSONDecodeError as exc:
        raise ValueError("invalid JSON") from exc
    if not isinstance(item, dict):
        raise ValueError("document must be a JSON object")
    raw_id = _pick(item, ("doc_id", "document_id", "page_id", "id"))
    if raw_id is None or not str(raw_id).strip():
        raise ValueError("missing document ID (doc_id/id/page_id)")
    doc_id = str(raw_id).strip()
    raw_version = _pick(item, ("version", "revision", "revision_id"), phase)
    try:
        if isinstance(raw_version, bool) or str(raw_version).strip() != str(int(raw_version)):
            raise ValueError("version must be integer-valued")
        version = int(raw_version)
    except (TypeError, ValueError) as exc:
        raise ValueError("version must be an integer") from exc
    if version < 0:
        raise ValueError("version cannot be negative")
    operation = str(_pick(item, ("op", "operation", "event_type", "action"), "upsert")).lower()
    deleted = _is_true(item.get("is_deleted", item.get("deleted", False))) or operation in {"delete", "remove"}
    title = str(_pick(item, ("title", "name"), "") or "").strip()
    body = str(_pick(item, ("text", "content", "body", "plain_text"), "") or "").strip()
    url = str(_pick(item, ("url", "page_url", "link"), "") or "").strip()
    if not deleted and (not title or not body):
        raise ValueError("active document must include title and text/content")
    links = _links(_pick(item, ("raw_links", "out_links", "outlinks", "links", "outgoing_links"), []))
    # Hash excludes arrival time/event ID, so retries are idempotent.
    canonical = {"doc_id": doc_id, "version": version, "title": title, "text": body,
                 "url": url, "raw_links": links, "is_deleted": deleted}
    digest = hashlib.sha256(json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    canonical["checksum"] = digest
    return canonical


def parse_line(line: str, phase: int = 0) -> tuple[bool, dict[str, Any]]:
    try:
        return True, normalize_document(line, phase)
    except (ValueError, TypeError) as exc:
        return False, {"raw_line": line[:4000], "reason": str(exc)}
