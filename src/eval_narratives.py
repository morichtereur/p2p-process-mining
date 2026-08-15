"""Grounding eval for the narratives generated in src/explain_case.py.

Every citation is checked against the real event log, not judged by a human
or another LLM: a citation is grounded if and only if {activity, timestamp}
is an actual event on that case_id. This is possible only because the source
is a fully structured, exact-timestamped log -- if the source were a PDF or
a web page, "faithfulness" would need a judge model, and the judge itself
would need evaluating.

Run: .venv/bin/python src/eval_narratives.py (after src/explain_case.py)
"""

from __future__ import annotations

import json
from collections import defaultdict

import duckdb
import polars as pl

con = duckdb.connect()

rows = [json.loads(line) for line in open("output/case_narratives.jsonl")]

case_ids = {r["case_id"] for r in rows}
events = con.execute(f"""
    select case_id, activity, strftime(timestamp, '%Y-%m-%d %H:%M:%S') as ts
    from read_parquet('data/events.parquet')
    where case_id in ({", ".join(f"'{c}'" for c in case_ids)})
""").pl()

real_events = defaultdict(set)
for row in events.iter_rows(named=True):
    real_events[row["case_id"]].add((row["activity"], row["ts"]))

eval_rows = []
for r in rows:
    if r.get("parse_failed"):
        eval_rows.append({
            "case_id": r["case_id"], "category": r["category"], "model": r["model"],
            "parse_failed": True, "n_citations": 0, "n_grounded": 0, "grounding_rate": None,
        })
        continue
    valid_pairs = real_events[r["case_id"]]
    citations = r["citations"]
    n_grounded = sum(1 for c in citations if (c["activity"], c["timestamp"]) in valid_pairs)
    eval_rows.append({
        "case_id": r["case_id"], "category": r["category"], "model": r["model"],
        "parse_failed": False, "n_citations": len(citations), "n_grounded": n_grounded,
        "grounding_rate": n_grounded / len(citations) if citations else None,
    })

eval_df = pl.DataFrame(eval_rows)

print(f"Narratives evaluated: {len(eval_df)}")
n_parse_fail = eval_df["parse_failed"].sum()
print(f"Parse failures (structured output truncated before valid JSON): {n_parse_fail}")

clean = eval_df.filter(~pl.col("parse_failed"))
total_citations = clean["n_citations"].sum()
total_grounded = clean["n_grounded"].sum()
print(f"\nOverall: {total_grounded:,} / {total_citations:,} citations grounded "
      f"({total_grounded/total_citations:.1%})")

print("\nBy model:")
by_model = clean.group_by("model").agg(
    pl.col("n_citations").sum().alias("citations"),
    pl.col("n_grounded").sum().alias("grounded"),
    pl.col("grounding_rate").mean().alias("mean_per_narrative_rate"),
)
for row in by_model.iter_rows(named=True):
    rate = row["grounded"] / row["citations"]
    print(f"  {row['model']:<22} {row['grounded']:>4}/{row['citations']:<4} citations "
          f"({rate:.1%})  mean per-narrative rate={row['mean_per_narrative_rate']:.1%}")

print("\nBy category:")
by_cat = clean.group_by("category").agg(
    pl.col("n_citations").sum().alias("citations"),
    pl.col("n_grounded").sum().alias("grounded"),
)
for row in by_cat.iter_rows(named=True):
    rate = row["grounded"] / row["citations"]
    print(f"  {row['category']:<18} {row['grounded']:>4}/{row['citations']:<4} citations ({rate:.1%})")

print("\nUngrounded citations (hallucinated activity/timestamp pairs), if any:")
n_shown = 0
for r in rows:
    if r.get("parse_failed"):
        continue
    valid_pairs = real_events[r["case_id"]]
    for c in r["citations"]:
        if (c["activity"], c["timestamp"]) not in valid_pairs:
            print(f"  [{r['model']}] {r['case_id']}: claimed \"{c['activity']}\" @ {c['timestamp']} "
                  f"-- not a real event. Claim: {c['claim']!r}")
            n_shown += 1
if n_shown == 0:
    print("  (none)")

eval_df.write_parquet("output/narrative_eval.parquet")
print("\nWritten -> output/narrative_eval.parquet")
