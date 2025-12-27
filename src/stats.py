import argparse
import json
import os
import time
import random
from datetime import datetime
from statistics import median
from typing import Dict, Tuple, List, Optional

import yaml
try:
    from pymongo import MongoClient
except Exception:
    MongoClient = None


def human_bytes(n: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    x = float(n)
    for u in units:
        if x < 1024.0:
            return f"{x:.2f} {u}"
        x /= 1024.0
    return f"{x:.2f} PB"


def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)


def percentile(sorted_values: List[int], p: float) -> int:
    if not sorted_values:
        return 0
    if p <= 0:
        return sorted_values[0]
    if p >= 100:
        return sorted_values[-1]
    k = (len(sorted_values) - 1) * (p / 100.0)
    i = int(k)
    j = min(i + 1, len(sorted_values) - 1)
    if i == j:
        return sorted_values[i]
    frac = k - i
    return int(sorted_values[i] * (1 - frac) + sorted_values[j] * frac)


def load_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def analyze_corpus_jsonl(corpus_path: str) -> dict:
    t0 = time.time()

    total_docs = 0
    total_clean_bytes = 0
    total_clean_chars = 0
    total_lines = 0
    total_words_est = 0

    by_source = {}
    sample_titles = []

    with open(corpus_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            total_lines += 1
            try:
                obj = json.loads(line)
            except Exception:
                continue

            total_docs += 1
            source = (obj.get("source_name") or "UNKNOWN").strip()
            by_source[source] = by_source.get(source, 0) + 1

            text = obj.get("clean_text") or ""
            # bytes/char
            total_clean_bytes += len(text.encode("utf-8", errors="ignore"))
            total_clean_chars += len(text)

            # очень грубо, но стабильно и дёшево
            total_words_est += len([w for w in text.split() if w])

            title = (obj.get("title") or "").strip()
            if title and len(sample_titles) < 10:
                sample_titles.append(title)

    dt = time.time() - t0
    avg_bytes = (total_clean_bytes / total_docs) if total_docs else 0
    avg_chars = (total_clean_chars / total_docs) if total_docs else 0
    avg_words = (total_words_est / total_docs) if total_docs else 0

    return {
        "corpus_jsonl_path": corpus_path,
        "documents": total_docs,
        "sources": by_source,
        "clean_text_bytes_total": total_clean_bytes,
        "clean_text_bytes_avg": avg_bytes,
        "clean_text_chars_avg": avg_chars,
        "clean_text_words_est_avg": avg_words,
        "sample_titles": sample_titles,
        "analysis_time_sec": dt,
        "analysis_speed_kb_per_sec": (total_clean_bytes / 1024.0) / dt if dt > 0 else 0.0
    }


def analyze_tokens(tokens_path: str, docs_idx_path: str, top_n: int = 30) -> dict:
    doc_count = 0
    tokens_per_doc = []
    total_tokens_from_idx = 0

    with open(docs_idx_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split("\t")
            if len(parts) != 3:
                continue
            try:
                c = int(parts[2])
            except Exception:
                continue
            doc_count += 1
            tokens_per_doc.append(c)
            total_tokens_from_idx += c

    tokens_per_doc_sorted = sorted(tokens_per_doc)
    avg_tokens_per_doc = (total_tokens_from_idx / doc_count) if doc_count else 0

    t0 = time.time()
    total_tokens = 0
    total_token_chars = 0
    total_bytes_read = 0

    freqs: Dict[str, int] = {}

    with open(tokens_path, "rb") as fb:
        for raw_line in fb:
            total_bytes_read += len(raw_line)
            try:
                line = raw_line.decode("utf-8", errors="ignore").rstrip("\n")
            except Exception:
                continue
            if not line:
                continue
            parts = line.split("\t")
            if len(parts) < 3:
                continue
            token = parts[2].strip()
            if not token:
                continue

            total_tokens += 1
            total_token_chars += len(token)

            freqs[token] = freqs.get(token, 0) + 1

    dt = time.time() - t0
    avg_len = (total_token_chars / total_tokens) if total_tokens else 0.0
    kb = total_bytes_read / 1024.0
    kb_per_sec = kb / dt if dt > 0 else 0.0

    # top-n
    top = sorted(freqs.items(), key=lambda x: (-x[1], x[0]))[:top_n]

    return {
        "tokens_path": tokens_path,
        "docs_idx_path": docs_idx_path,
        "documents": doc_count,
        "tokens_total": total_tokens,
        "tokens_total_from_docs_idx": total_tokens_from_idx,
        "avg_token_length_chars": avg_len,
        "tokens_file_bytes": total_bytes_read,
        "tokens_file_speed_kb_per_sec": kb_per_sec,
        "tokens_file_parse_time_sec": dt,
        "tokens_per_doc": {
            "avg": avg_tokens_per_doc,
            "median": median(tokens_per_doc_sorted) if tokens_per_doc_sorted else 0,
            "p95": percentile(tokens_per_doc_sorted, 95) if tokens_per_doc_sorted else 0,
            "min": tokens_per_doc_sorted[0] if tokens_per_doc_sorted else 0,
            "max": tokens_per_doc_sorted[-1] if tokens_per_doc_sorted else 0,
        },
        "top_tokens": [{"token": t, "freq": c} for t, c in top]
    }


def analyze_mongo_raw(config_path: str) -> Optional[dict]:
    if MongoClient is None:
        return None

    cfg = load_yaml(config_path)
    db_cfg = cfg.get("db", {})
    host = db_cfg.get("host", "mongodb")
    port = int(db_cfg.get("port", 27017))
    username = db_cfg.get("username", "admin")
    password = db_cfg.get("password", "admin123")
    database = db_cfg.get("database", "search_engine")
    collection = db_cfg.get("collection", "documents")

    client = MongoClient(
        host=host,
        port=port,
        username=username,
        password=password,
        authSource=database
    )
    col = client[database][collection]

    t0 = time.time()

    total_docs = col.count_documents({})

    cur = col.find(
        {},
        {"raw_html": 1, "clean_text": 1, "source_name": 1}
    )

    raw_bytes = 0
    clean_bytes = 0
    with_raw = 0
    with_clean = 0
    by_source = {}

    for doc in cur:
        src = (doc.get("source_name") or "UNKNOWN").strip()
        by_source[src] = by_source.get(src, 0) + 1

        rh = doc.get("raw_html")
        if isinstance(rh, str) and rh:
            with_raw += 1
            raw_bytes += len(rh.encode("utf-8", errors="ignore"))

        ct = doc.get("clean_text")
        if isinstance(ct, str) and ct:
            with_clean += 1
            clean_bytes += len(ct.encode("utf-8", errors="ignore"))

    dt = time.time() - t0
    client.close()

    return {
        "db": {
            "host": host,
            "port": port,
            "database": database,
            "collection": collection
        },
        "documents_total": total_docs,
        "documents_with_raw_html": with_raw,
        "documents_with_clean_text": with_clean,
        "sources": by_source,
        "raw_html_bytes_total": raw_bytes,
        "raw_html_bytes_avg": (raw_bytes / with_raw) if with_raw else 0,
        "clean_text_bytes_total": clean_bytes,
        "clean_text_bytes_avg": (clean_bytes / with_clean) if with_clean else 0,
        "analysis_time_sec": dt
    }


def write_report_txt(out_path: str, result: dict) -> None:
    lines = []
    lines.append("ИНФОРМАЦИЯ О КОРПУСЕ\n")

    corpus = result.get("corpus_from_export", {})
    tokens = result.get("tokens", {})
    mongo = result.get("mongo_raw", None)

    if corpus:
        lines.append("Корпус (из export_corpus.py / corpus.jsonl):")
        lines.append(f"  Путь: {corpus.get('corpus_jsonl_path')}")
        lines.append(f"  Количество документов: {corpus.get('documents')}")
        lines.append(f"  Источники: {corpus.get('sources')}")
        lines.append(f"  Общий размер clean_text: {human_bytes(int(corpus.get('clean_text_bytes_total', 0)))}")
        lines.append(f"  Средний размер clean_text: {human_bytes(int(corpus.get('clean_text_bytes_avg', 0)))}")
        lines.append(f"  Средняя длина текста (символов): {corpus.get('clean_text_chars_avg'):.2f}")
        lines.append(f"  Средний объём текста (слов, оценка): {corpus.get('clean_text_words_est_avg'):.2f}")
        lines.append(f"  Время анализа: {corpus.get('analysis_time_sec'):.3f} сек")
        lines.append(f"  Скорость анализа: {corpus.get('analysis_speed_kb_per_sec'):.2f} KB/s")
        lines.append("")

        samples = corpus.get("sample_titles") or []
        lines.append("Примеры документов в корпусе (title):")
        if samples:
            samples_list = list(samples)
            random.shuffle(samples_list)
            for i, t in enumerate(samples_list, 1):
                lines.append(f"  {i}. {t}")
        else:
            lines.append("  (нет заголовков в export)")
        lines.append("")

    lines.append("Токенизация (из tokens.tsv + docs.idx):")
    lines.append(f"  Путь tokens.tsv: {tokens.get('tokens_path')}")
    lines.append(f"  Путь docs.idx: {tokens.get('docs_idx_path')}")
    lines.append(f"  Количество документов: {tokens.get('documents')}")
    lines.append(f"  Количество токенов: {tokens.get('tokens_total')}")
    lines.append(f"  Средняя длина токена: {tokens.get('avg_token_length_chars'):.2f}")
    lines.append(f"  Размер tokens.tsv: {human_bytes(int(tokens.get('tokens_file_bytes', 0)))}")
    lines.append(f"  Время анализа tokens.tsv: {tokens.get('tokens_file_parse_time_sec'):.3f} сек")
    lines.append(f"  Скорость чтения tokens.tsv: {tokens.get('tokens_file_speed_kb_per_sec'):.2f} KB/s")
    tpd = tokens.get("tokens_per_doc", {})
    lines.append("  Токенов на документ:")
    lines.append(f"    avg={tpd.get('avg'):.2f}, median={tpd.get('median')}, p95={tpd.get('p95')}, min={tpd.get('min')}, max={tpd.get('max')}")
    lines.append("")

    top = tokens.get("top_tokens", [])
    lines.append("Топ токенов (для закона Ципфа / первичной проверки):")
    for i, it in enumerate(top, 1):
        lines.append(f"  {i:02d}. {it['token']}\t{it['freq']}")
    lines.append("")

    if mongo:
        lines.append("Сырые документы (из MongoDB, опционально):")
        lines.append(f"  DB: {mongo['db']}")
        lines.append(f"  Всего документов: {mongo.get('documents_total')}")
        lines.append(f"  Документов с raw_html: {mongo.get('documents_with_raw_html')}")
        lines.append(f"  Общий размер raw_html: {human_bytes(int(mongo.get('raw_html_bytes_total', 0)))}")
        lines.append(f"  Средний размер raw_html: {human_bytes(int(mongo.get('raw_html_bytes_avg', 0)))}")
        lines.append(f"  Документов с clean_text: {mongo.get('documents_with_clean_text')}")
        lines.append(f"  Общий размер clean_text (в БД): {human_bytes(int(mongo.get('clean_text_bytes_total', 0)))}")
        lines.append(f"  Средний размер clean_text (в БД): {human_bytes(int(mongo.get('clean_text_bytes_avg', 0)))}")
        lines.append(f"  Источники: {mongo.get('sources')}")
        lines.append(f"  Время анализа Mongo: {mongo.get('analysis_time_sec'):.3f} сек")
        lines.append("")

    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=None, help="config.yaml (опционально, для секции raw_html из Mongo)")
    ap.add_argument("--corpus", default="data/corpus.jsonl", help="Путь к data/corpus.jsonl")
    ap.add_argument("--tokens", default="data/tokens/tokens.tsv", help="Путь к data/tokens/tokens.tsv")
    ap.add_argument("--docs-idx", default="data/tokens/docs.idx", help="Путь к data/tokens/docs.idx")
    ap.add_argument("--out-dir", default="data/stats", help="Папка для отчётов")
    ap.add_argument("--top", type=int, default=30, help="Top-N токенов по частоте")
    args = ap.parse_args()

    if not os.path.exists(args.corpus):
        raise FileNotFoundError(f"corpus.jsonl not found: {args.corpus}")
    if not os.path.exists(args.tokens):
        raise FileNotFoundError(f"tokens.tsv not found: {args.tokens}")
    if not os.path.exists(args.docs_idx):
        raise FileNotFoundError(f"docs.idx not found: {args.docs_idx}")

    ensure_dir(args.out_dir)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    result = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "corpus_from_export": analyze_corpus_jsonl(args.corpus),
        "tokens": analyze_tokens(args.tokens, args.docs_idx, top_n=args.top),
        "mongo_raw": None
    }

    if args.config:
        try:
            result["mongo_raw"] = analyze_mongo_raw(args.config)
        except Exception as e:
            result["mongo_raw"] = {"error": str(e)}

    json_path = os.path.join(args.out_dir, f"statistics_{ts}.json")
    txt_path = os.path.join(args.out_dir, f"report_{ts}.txt")

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    write_report_txt(txt_path, result)

    print(f"Saved JSON: {json_path}")
    print(f"Saved TXT : {txt_path}")

    print("\n=== SUMMARY ===")
    print(f"Docs: {result['tokens']['documents']}")
    print(f"Tokens: {result['tokens']['tokens_total']}")
    print(f"Avg token length: {result['tokens']['avg_token_length_chars']:.2f}")
    print(f"tokens.tsv size: {human_bytes(int(result['tokens']['tokens_file_bytes']))}")


if __name__ == "__main__":
    main()
