"""Node 4: atomically activate a release already copied into incoming/vN/."""
from __future__ import annotations
import argparse
import os
import shutil
import uuid
from pathlib import Path
from search_engine.serving import SearchIndex


def activate(root: Path, version: str) -> Path:
    if not version or not version.replace("-", "").replace("_", "").isalnum():
        raise ValueError("Unsafe release version")
    root = Path(root).resolve()
    incoming = root / "incoming" / version
    destination = root / "versions" / version
    if not incoming.is_dir() or destination.exists():
        raise ValueError("Incoming version missing or destination version already exists")
    # Verify before changing active pointer, including Parquet schema and counts.
    manifest, _, _, _ = SearchIndex.read_release(incoming)
    if manifest["version"] != version:
        raise ValueError("Manifest version mismatch")
    destination.parent.mkdir(parents=True, exist_ok=True)
    incoming.rename(destination)  # both are under the same serving root/filesystem
    pointer = root / "current"
    tmp_pointer = root / (".current-" + uuid.uuid4().hex)
    try:
        os.symlink(str(Path("versions") / version), tmp_pointer)
        os.replace(tmp_pointer, pointer)  # atomic switch on POSIX
    finally:
        tmp_pointer.unlink(missing_ok=True)
    return destination


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--root", default="/srv/distributed-search")
    p.add_argument("--version", required=True)
    args = p.parse_args()
    print(f"Activated {activate(Path(args.root), args.version)}")


if __name__ == "__main__":
    main()
