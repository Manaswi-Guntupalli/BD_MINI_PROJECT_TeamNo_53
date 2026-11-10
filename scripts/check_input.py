"""Local preflight: report actual JSONL keys and normalization success rate."""
import argparse
import collections
import json
from search_engine.records import parse_line


def main():
    p = argparse.ArgumentParser()
    p.add_argument("path")
    p.add_argument("--phase", type=int, default=0)
    a = p.parse_args()
    keys, rejects = collections.Counter(), collections.Counter()
    valid, total = 0, 0
    samples = []
    with open(a.path, encoding="utf-8") as fh:
        for line in fh:
            if not line.strip():
                continue
            total += 1
            try:
                obj = json.loads(line)
                if isinstance(obj, dict):
                    keys.update(obj.keys())
            except ValueError:
                pass
            good, doc = parse_line(line, a.phase)
            valid += int(good)
            if not good:
                rejects[doc["reason"]] += 1
                if len(samples) < 3:
                    samples.append(doc)
    print(json.dumps({"total": total, "valid": valid, "rejected": total-valid,
                      "keys": keys.most_common(), "rejection_reasons": rejects.most_common(),
                      "rejection_examples": samples}, indent=2))
    if valid == 0:
        raise SystemExit("ERROR: zero usable documents, review dataset schema aliases")


if __name__ == "__main__":
    main()
