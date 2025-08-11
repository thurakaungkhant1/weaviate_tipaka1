# make_wide_table.py
# Usage:
#   1) Raw text → full 3-level + sentences:
#      python make_wide_table.py -i "pali chunk.txt" -o wide_chunks.csv --main_limit 8000 --sub_limit 200
#   2) If you already have a CSV with (chunk_id,chunk_text) and want only step2+3:
#      python make_wide_table.py -i chunks_8000token_ids_with_meta.csv --from_csv -o wide_chunks.csv --sub_limit 200
#
# Requirements:
#   pip install tiktoken pandas

import argparse, csv, re
from pathlib import Path
from typing import List, Iterable, Tuple
import pandas as pd
import tiktoken

ENC = tiktoken.get_encoding("cl100k_base")

def enc(text: str) -> List[int]:
    return ENC.encode(text or "")

def dec(tokens: List[int]) -> str:
    return ENC.decode(tokens)

def split_by_tokens(text: str, limit: int) -> List[str]:
    toks = enc(text)
    parts = [toks[i:i+limit] for i in range(0, len(toks), limit)]
    return [dec(p) for p in parts]

# Keep sentence punctuation with the sentence; split on . ? ! ; : ၊ ။ or blank lines
SENT_SPLIT_RE = re.compile(r'(?<=[\.\?\!;:၊။])\s+|\n{2,}', re.UNICODE)

def split_into_sentences(text: str) -> List[str]:
    t = (text or "").strip()
    if not t:
        return []
    t = re.sub(r'[ \t]+', ' ', t)
    parts = re.split(SENT_SPLIT_RE, t)
    return [p.strip() for p in parts if p and p.strip()]

def from_raw_text(full_text: str, main_limit: int, sub_limit: int
                 ) -> Iterable[Tuple[int, str, int, str, int, str]]:
    main_chunks = split_by_tokens(full_text, main_limit)
    for chunk_id, chunk_text in enumerate(main_chunks, start=1):
        subchunks = split_by_tokens(chunk_text, sub_limit)
        for sub_id, sub_text in enumerate(subchunks, start=1):
            sentences = split_into_sentences(sub_text) or [sub_text]
            for sent_id, sent_text in enumerate(sentences, start=1):
                yield chunk_id, chunk_text, sub_id, sub_text, sent_id, sent_text

def from_chunk_csv(df: pd.DataFrame, sub_limit: int
                  ) -> Iterable[Tuple[int, str, int, str, int, str]]:
    cols = {c.lower(): c for c in df.columns}
    if "chunk_id" not in cols or "chunk_text" not in cols:
        raise ValueError("CSV must contain 'chunk_id' and 'chunk_text' columns.")
    for _, row in df.iterrows():
        chunk_id = int(row[cols["chunk_id"]])
        chunk_text = str(row[cols["chunk_text"]] or "")
        subchunks = split_by_tokens(chunk_text, sub_limit)
        for sub_id, sub_text in enumerate(subchunks, start=1):
            sentences = split_into_sentences(sub_text) or [sub_text]
            for sent_id, sent_text in enumerate(sentences, start=1):
                yield chunk_id, chunk_text, sub_id, sub_text, sent_id, sent_text

def write_csv(rows: Iterable[Tuple[int, str, int, str, int, str]], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["chunk_id","chunk_text","subchunk_id","subchunk_text","sentence_id","sentence_text"])
        for r in rows:
            w.writerow(r)

def main():
    ap = argparse.ArgumentParser(description="3-level token split → sentence split → 6-column CSV")
    ap.add_argument("-i","--input", required=True, help="Input .txt (raw) or .csv (with chunk_id,chunk_text)")
    ap.add_argument("-o","--output", default="wide_chunks.csv", help="Output CSV path")
    ap.add_argument("--main_limit", type=int, default=8000, help="Tokens per chunk (default 8000)")
    ap.add_argument("--sub_limit", type=int, default=200, help="Tokens per subchunk (default 200)")
    ap.add_argument("--from_csv", action="store_true", help="Treat input as CSV that has (chunk_id,chunk_text)")
    args = ap.parse_args()

    in_path = Path(args.input)
    out_path = Path(args.output)

    if args.from_csv or in_path.suffix.lower() == ".csv":
        df = pd.read_csv(in_path, encoding="utf-8")
        rows = from_chunk_csv(df, sub_limit=args.sub_limit)
    else:
        full_text = in_path.read_text(encoding="utf-8")
        rows = from_raw_text(full_text, main_limit=args.main_limit, sub_limit=args.sub_limit)

    write_csv(rows, out_path)
    print(f"✅ Done: {out_path}")

if __name__ == "__main__":
    main()
