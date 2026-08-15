"""Generate a grounded, plain-English narrative for a sample of cases, with
structured citations back to the raw event log -- and compare two model
tiers on how well those citations actually hold up.

Why this is a meaningful eval and not just a demo: every citation is checked
against data this project already computed exactly (steps 1-4), not against
human judgment. That's unusual for LLM output -- most "faithfulness" evals
need a human or a second LLM to judge, because the source text is free-form
prose (a PDF, a web page). Here the source is a fully structured event log,
so grounding is checkable with a plain lookup: either {activity, timestamp}
is a real event on that case, or it isn't.

Two models, same prompt, same 12 cases (4 touchless, 4 reworked, 4 maverick
buying): claude-haiku-4-5 and claude-sonnet-5. Structured outputs
(output_config.format) guarantee valid JSON on both, so the eval measures
grounding, not JSON-parsing luck.

Run: .venv/bin/python src/explain_case.py
Requires ANTHROPIC_API_KEY. Real API calls -- ~24 requests, small prompts.
"""

from __future__ import annotations

import json
from pathlib import Path

import anthropic
import duckdb
import polars as pl

MODELS = ["claude-haiku-4-5", "claude-sonnet-5"]
N_PER_CATEGORY = 4

SYSTEM_PROMPT = """You are a P2P (purchase-to-pay) process auditor. You are \
given the complete, ordered event log for a single case (one purchase order \
line item): every activity, its exact timestamp, and the resource who \
performed it.

Write a concise 2-4 sentence narrative in plain English, suitable for a \
controls report, describing what happened in this case.

Then list citations: for every factual claim in your narrative that \
references a specific event, give the exact activity name and exact \
timestamp string as they appear in the event log below. Copy both \
character-for-character -- do not reformat, abbreviate, or paraphrase them.

Only state facts directly visible in the event log below. Do not infer, \
assume, or add anything the log does not show."""

RESPONSE_SCHEMA = {
    "type": "object",
    "properties": {
        "narrative": {"type": "string"},
        "citations": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "claim": {"type": "string"},
                    "activity": {"type": "string"},
                    "timestamp": {"type": "string"},
                },
                "required": ["claim", "activity", "timestamp"],
                "additionalProperties": False,
            },
        },
    },
    "required": ["narrative", "citations"],
    "additionalProperties": False,
}

con = duckdb.connect()

# --- sample 4 cases each from three categories already established by steps 1-4 ---
touchless = con.execute("""
    select case_id from 'output/touchless_cases.parquet'
    where is_complete and not has_rework
    order by case_id limit ?
""", [N_PER_CATEGORY]).pl()["case_id"].to_list()

reworked = con.execute("""
    select f.case_id
    from 'output/rework_cases.parquet' f
    where f.case_id in (
        select case_id from read_parquet('data/events.parquet') where activity = 'Cancel Invoice Receipt'
    )
    order by f.case_id limit ?
""", [N_PER_CATEGORY]).pl()["case_id"].to_list()

maverick = con.execute("""
    select case_id from 'output/sequence_cases.parquet'
    where t_po is not null and t_inv_vendor is not null and not has_bad_ts
      and t_po > t_inv_vendor
    order by case_id limit ?
""", [N_PER_CATEGORY]).pl()["case_id"].to_list()

sample = [(c, "touchless") for c in touchless] + \
         [(c, "reworked") for c in reworked] + \
         [(c, "maverick_buying") for c in maverick]
print(f"Sampled {len(sample)} cases: {len(touchless)} touchless, {len(reworked)} reworked, "
      f"{len(maverick)} maverick buying")


def events_for(case_id: str) -> pl.DataFrame:
    return con.execute("""
        select activity, timestamp, resource
        from read_parquet('data/events.parquet')
        where case_id = ?
        order by event_index
    """, [case_id]).pl()


def format_log(events: pl.DataFrame) -> str:
    lines = []
    for row in events.iter_rows(named=True):
        ts = row["timestamp"].strftime("%Y-%m-%d %H:%M:%S")
        lines.append(f"- {row['activity']} | {ts} | {row['resource']}")
    return "\n".join(lines)


client = anthropic.Anthropic()
results = []

for case_id, category in sample:
    events = events_for(case_id)
    log_text = format_log(events)
    user_content = f"Case: {case_id}\n\nEvent log (activity | timestamp | resource):\n{log_text}"

    for model in MODELS:
        response = client.messages.create(
            model=model,
            max_tokens=4096,
            system=SYSTEM_PROMPT,
            output_config={"format": {"type": "json_schema", "schema": RESPONSE_SCHEMA}},
            messages=[{"role": "user", "content": user_content}],
        )
        text = next(b.text for b in response.content if b.type == "text")
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            print(f"  {case_id} ({category}) / {model}: PARSE FAILURE "
                  f"(stop_reason={response.stop_reason}, {len(events)} events)")
            results.append({
                "case_id": case_id, "category": category, "model": model,
                "narrative": None, "citations": [], "parse_failed": True,
                "input_tokens": response.usage.input_tokens,
                "output_tokens": response.usage.output_tokens,
            })
            continue
        results.append({
            "case_id": case_id,
            "category": category,
            "model": model,
            "narrative": parsed["narrative"],
            "citations": parsed["citations"],
            "parse_failed": False,
            "input_tokens": response.usage.input_tokens,
            "output_tokens": response.usage.output_tokens,
        })
        print(f"  {case_id} ({category}) / {model}: {len(parsed['citations'])} citations "
              f"({len(events)} events)")

Path("output").mkdir(exist_ok=True)
with open("output/case_narratives.jsonl", "w") as f:
    for r in results:
        f.write(json.dumps(r) + "\n")
print(f"\nWritten -> output/case_narratives.jsonl ({len(results)} narratives)")
