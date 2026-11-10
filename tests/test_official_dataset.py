"""Optional ground-truth checks, run only when actual course data is present.

The public GitHub repository need not contain the copyrighted/restricted corpus.
"""
from pathlib import Path
import pytest
from scripts.verify_official_dataset import audit

ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / 'data' / 'official' / 'pages.jsonl'
CHANGES = ROOT / 'data' / 'official' / 'changes.json'

@pytest.mark.skipif(not BASE.exists() or not CHANGES.exists(), reason='Official corpus not downloaded')
def test_actual_course_phases_counts_and_replay():
    output = audit(BASE, CHANGES)
    phase0, phase1, final = output['phase0'], output['phase1'], output['expected_final']
    assert (phase0['input_lines'], phase0['valid_records'], phase0['rejected_records']) == (1040, 1020, 20)
    assert (phase0['duplicates_removed'], phase0['unique_documents'], phase0['valid_links']) == (20, 1000, 499)
    assert (phase1['upserts'], phase1['deletes']) == (50, 50)
    assert (phase1['new_document_ids'], phase1['existing_document_upserts']) == (25, 25)
    assert (final['active_documents'], final['deleted_documents'], final['valid_link_edges']) == (975, 50, 467)
    assert final['replay_applied_changes'] == 0
    assert final['replay_idempotent'] is True


@pytest.mark.skipif(not BASE.exists() or not CHANGES.exists(), reason='Official corpus not downloaded')
def test_official_phase1_tombstones_and_inserted_pages():
    from search_engine.records import parse_line
    base = {}
    for raw in BASE.open(encoding='utf-8'):
        valid, value = parse_line(raw, phase=0)
        if valid:
            base[value['doc_id']] = value
    assert not base['1']['is_deleted']
    assert 'phase1-new-001' not in base
    for raw in CHANGES.open(encoding='utf-8'):
        valid, value = parse_line(raw, phase=1)
        assert valid
        base[value['doc_id']] = value
    assert base['1']['is_deleted']
    assert base['101']['title'] == 'Biology (Phase 1)'
    assert base['phase1-new-001']['title'] == 'Phase 1 Added Page 1'
    assert not base['phase1-new-001']['is_deleted']
