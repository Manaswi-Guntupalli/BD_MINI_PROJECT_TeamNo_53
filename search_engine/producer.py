"""Node 1 optional Kafka producer for newline-delimited document updates."""
from __future__ import annotations
import argparse
import json
from kafka import KafkaProducer
from search_engine.records import normalize_document


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True)
    p.add_argument("--input", required=True)
    p.add_argument("--topic", default="document_updates")
    p.add_argument("--phase", type=int, default=2)
    a = p.parse_args()
    producer = KafkaProducer(bootstrap_servers=a.bootstrap.split(","), acks="all", retries=5,
                             key_serializer=lambda k: k.encode("utf-8"),
                             value_serializer=lambda v: json.dumps(v).encode("utf-8"))
    sent = 0
    with open(a.input, encoding="utf-8") as stream:
        for number, line in enumerate(stream, 1):
            if not line.strip():
                continue
            try:
                doc = normalize_document(line, phase=a.phase)
            except ValueError as exc:
                print(f"Line {number} rejected: {exc}")
                continue
            # Preserve explicit version and normalized content; Kafka key keeps
            # updates for the same document on the same partition.
            producer.send(a.topic, key=doc["doc_id"], value=doc)
            sent += 1
    producer.flush(timeout=60)
    print(f"Published {sent} document events to {a.topic}")
    producer.close()


if __name__ == "__main__":
    main()
