from pathlib import Path
import pytest
from search_engine.deploy import activate
from search_engine.serving import SearchIndex


def test_atomic_activation(monkeypatch, tmp_path):
    incoming = tmp_path / "incoming" / "v1"
    incoming.mkdir(parents=True)
    (incoming / "manifest.json").write_text('{"version":"v1"}')
    monkeypatch.setattr(SearchIndex, "read_release", staticmethod(
        lambda path: ({"version": "v1"}, {}, {}, 1.0)))
    result = activate(tmp_path, "v1")
    assert result == tmp_path / "versions" / "v1"
    assert (tmp_path / "current").resolve() == result


def test_bad_release_cannot_replace_current(monkeypatch, tmp_path):
    incoming = tmp_path / "incoming" / "v1"
    incoming.mkdir(parents=True)
    monkeypatch.setattr(SearchIndex, "read_release", staticmethod(
        lambda path: (_ for _ in ()).throw(ValueError("broken parquet"))))
    with pytest.raises(ValueError, match="broken"):
        activate(tmp_path, "v1")
    assert not (tmp_path / "current").exists()
    assert incoming.exists()


def test_deploy_rejects_path_traversal(tmp_path):
    with pytest.raises(ValueError):
        activate(tmp_path, "../secrets")
