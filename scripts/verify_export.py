"""Local/Node4 verification of a copied export before or after deployment."""
import argparse
import json
from pathlib import Path
from search_engine.serving import SearchIndex
from search_engine.ranking import rank_query


def main():
    p = argparse.ArgumentParser()
    p.add_argument("path", help="directory containing docs/, postings/, manifest.json")
    p.add_argument("--query", default="solar system")
    a = p.parse_args()
    manifest, docs, postings, avgdl = SearchIndex.read_release(Path(a.path))
    results = rank_query(a.query, docs, postings, avgdl)
    print(json.dumps({"valid": True, "version": manifest["version"],
                      "source_snapshot_id": manifest["source_snapshot_id"],
                      "documents": len(docs), "terms": len(postings),
                      "sample_query": a.query, "sample_matches": len(results),
                      "top_result": results[0] if results else None}, indent=2))


if __name__ == "__main__":
    main()
