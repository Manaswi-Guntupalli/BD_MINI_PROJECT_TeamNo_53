"""Offline reference verification for the *provided* two-phase JSONL corpus.

Uses no Spark, Hadoop or Kafka. Its expected numbers are independent of live
cluster execution and MUST be checked against the distributed Spark results.

Usage:
  PYTHONPATH=. python scripts/verify_official_dataset.py \
    data/official/pages.jsonl data/official/changes.json --output data/expected_results.json
"""
from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
from search_engine.records import parse_line


def read_jsonl(path: Path, phase: int) -> tuple[list[dict], dict]:
    records: list[dict] = []
    reasons: Counter[str] = Counter()
    lines = 0
    with path.open('r', encoding='utf-8') as handle:
        for line in handle:
            if not line.strip():
                continue
            lines += 1
            good, item = parse_line(line, phase)
            if good:
                records.append(item)
            else:
                reasons[item['reason']] += 1
    return records, {
        'input_lines': lines,
        'valid_records': len(records),
        'rejected_records': lines - len(records),
        'rejected_by_reason': dict(sorted(reasons.items())),
        'sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
    }


def latest(records: list[dict]) -> dict[str, dict]:
    docs: dict[str, dict] = {}
    for doc in records:
        key = doc['doc_id']
        prev = docs.get(key)
        if prev is None or (doc['version'], doc['checksum']) > (prev['version'], prev['checksum']):
            docs[key] = doc
    return docs


def count_links(documents: dict[str, dict]) -> tuple[int, int, int]:
    active = {key: rec for key, rec in documents.items() if not rec['is_deleted']}
    id_refs = set(active)
    url_refs = {rec['url'] for rec in active.values() if rec['url']}
    edges = set()
    source_link_count = 0
    for sid, rec in active.items():
        source_link_count += len(rec['raw_links'])
        for ref in rec['raw_links']:
            if ref in id_refs:
                edges.add((sid, ref))
            elif ref in url_refs:
                targets = sorted(k for k, value in active.items() if value['url'] == ref)
                edges.add((sid, targets[0]))
    return len(active), source_link_count, len(edges)


def audit(base_path: Path, changes_path: Path) -> dict:
    base_records, base_stats = read_jsonl(base_path, 0)
    phase1_records, change_stats = read_jsonl(changes_path, 1)
    original = latest(base_records)
    version0 = dict(original)
    base_stats['unique_documents'] = len(version0)
    base_stats['duplicates_removed'] = len(base_records) - len(version0)
    base_stats['active_documents'], base_stats['raw_link_references'], base_stats['valid_links'] = count_links(version0)

    updates = latest(phase1_records)
    change_stats['duplicates_removed'] = len(phase1_records) - len(updates)
    change_stats['upserts'] = sum(not rec['is_deleted'] for rec in updates.values())
    change_stats['deletes'] = sum(rec['is_deleted'] for rec in updates.values())
    change_stats['new_document_ids'] = sum(key not in original for key, rec in updates.items() if not rec['is_deleted'])
    change_stats['existing_document_upserts'] = sum(key in original for key, rec in updates.items() if not rec['is_deleted'])
    change_stats['deletes_of_existing_documents'] = sum(key in original for key, rec in updates.items() if rec['is_deleted'])
    applied = 0
    for key, doc in updates.items():
        previous = version0.get(key)
        if previous is None or (doc['version'], doc['checksum']) > (previous['version'], previous['checksum']):
            version0[key] = doc
            applied += 1
    change_stats['records_applied'] = applied
    active_count, raw_links, valid_links = count_links(version0)
    final = {
        'total_versioned_rows_including_tombstones': len(version0),
        'active_documents': active_count,
        'deleted_documents': len(version0) - active_count,
        'raw_link_references_from_active_documents': raw_links,
        'valid_link_edges': valid_links,
    }
    # Replay both phases without altering the final store, including old Phase 0.
    before = {key: doc['checksum'] for key, doc in version0.items()}
    replay_applied = 0
    for incoming in (base_records, phase1_records):
        for key, doc in latest(incoming).items():
            current = version0.get(key)
            if current is None or (doc['version'], doc['checksum']) > (current['version'], current['checksum']):
                version0[key] = doc
                replay_applied += 1
    final['replay_applied_changes'] = replay_applied
    final['replay_idempotent'] = before == {key: doc['checksum'] for key, doc in version0.items()}
    return {'phase0': base_stats, 'phase1': change_stats, 'expected_final': final}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('phase0', type=Path)
    parser.add_argument('phase1', type=Path)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    report = audit(args.phase0, args.phase1)
    text = json.dumps(report, indent=2, sort_keys=True) + '\n'
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text, encoding='utf-8')
    print(text)
    if not report['expected_final']['replay_idempotent']:
        raise SystemExit('ERROR: non-idempotent reference replay')


if __name__ == '__main__':
    main()
