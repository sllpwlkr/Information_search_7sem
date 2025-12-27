import json
import sys
import os
from pymongo import MongoClient
import yaml


def main():
    config_path = sys.argv[1] if len(sys.argv) > 1 else "config.yaml"
    out_path = sys.argv[2] if len(sys.argv) > 2 else "data/corpus.jsonl"

    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    db_cfg = cfg["db"]
    client = MongoClient(
        host=db_cfg["host"],
        port=db_cfg["port"],
        username=db_cfg.get("username", "admin"),
        password=db_cfg.get("password", "admin123"),
        authSource=db_cfg.get("database", "search_engine"),
    )
    col = client[db_cfg["database"]][db_cfg["collection"]]

    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    processed_doc_ids = set()

    cur = col.find(
        {"clean_text": {"$exists": True, "$ne": ""}},
        {"normalized_url": 1, "clean_text": 1, "source_name": 1, "metadata.title": 1}
    )

    with open(out_path, "w", encoding="utf-8") as out:
        for doc in cur:
            doc_id = str(doc["_id"])

            if doc_id in processed_doc_ids:
                continue
            processed_doc_ids.add(doc_id)

            title = (doc.get("metadata", {}) or {}).get("title") or ""

            rec = {
                "doc_id": doc_id,
                "normalized_url": doc.get("normalized_url", ""),
                "source_name": doc.get("source_name", ""),
                "title": title,
                "clean_text": doc.get("clean_text", ""),
            }
            out.write(json.dumps(rec, ensure_ascii=False) + "\n")

    print(f"Exported to {out_path}")


if __name__ == "__main__":
    main()
