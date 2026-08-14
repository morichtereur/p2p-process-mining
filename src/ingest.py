"""Stream the BPI Challenge 2019 XES log into columnar Parquet.

The log is 111 MB of XML holding ~1.5M events. Parsing it with a DOM tree
would hold the whole thing in memory at once, so this walks it with
iterparse and clears each trace after reading it — memory stays flat
regardless of log size.

Output:
    data/cases.parquet   one row per purchase order line item
    data/events.parquet  one row per event, ordered within its case
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

LOG = Path("data/BPI_Challenge_2019.xes")
CASES_OUT = Path("data/cases.parquet")
EVENTS_OUT = Path("data/events.parquet")

# Trace-level attributes → column names. Anything not listed is ignored.
CASE_FIELDS = {
    "concept:name": "case_id",
    "Purchasing Document": "purchase_doc",
    "Item": "item",
    "Company": "company",
    "Vendor": "vendor",
    "Name": "vendor_name",
    "Document Type": "doc_type",
    "Item Type": "item_type",
    "Item Category": "item_category",
    "Spend area text": "spend_area",
    "Sub spend area text": "sub_spend_area",
    "Spend classification text": "spend_class",
    "Source": "source_system",
    "GR-Based Inv. Verif.": "gr_based_inv_verif",
    "Goods Receipt": "goods_receipt",
}

EVENT_FIELDS = {
    "concept:name": "activity",
    "time:timestamp": "timestamp",
    "org:resource": "resource",
    "User": "user",
    "Cumulative net worth (EUR)": "cumulative_net_worth_eur",
}

CASE_SCHEMA = pa.schema(
    [(name, pa.bool_() if name in {"gr_based_inv_verif", "goods_receipt"} else pa.string())
     for name in CASE_FIELDS.values()]
)

EVENT_SCHEMA = pa.schema([
    ("case_id", pa.string()),
    ("event_index", pa.int32()),
    ("activity", pa.string()),
    ("timestamp", pa.timestamp("us", tz="UTC")),
    ("resource", pa.string()),
    ("user", pa.string()),
    ("cumulative_net_worth_eur", pa.float64()),
])

BATCH = 50_000


def _local(tag: str) -> str:
    """Local tag name, with any XML namespace stripped.

    This log ships without a default namespace, but other XES exports carry
    one. Matching on the local name handles both.
    """
    return tag.rsplit("}", 1)[-1]


def _attrs(element) -> dict[str, str]:
    """Read the XES key/value children of a trace or event element."""
    out = {}
    for child in element:
        if _local(child.tag) == "event":
            continue
        key = child.get("key")
        if key is not None:
            out[key] = child.get("value")
    return out


def _to_bool(value: str | None) -> bool | None:
    if value is None:
        return None
    return value.lower() == "true"


def _to_datetime(value: str | None) -> datetime | None:
    """XES timestamps are ISO-8601 with a literal Z; fromisoformat wants +00:00."""
    if value is None:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _flush(writer, schema, rows: list[dict]):
    if not rows:
        return
    table = pa.Table.from_pylist(rows, schema=schema)
    writer.write_table(table)
    rows.clear()


def main() -> None:
    if not LOG.exists():
        raise SystemExit(
            f"{LOG} not found. Download BPI_Challenge_2019.xes from "
            "https://data.4tu.nl/articles/dataset/BPI_Challenge_2019/12715853/1 "
            "and place it there."
        )

    case_rows: list[dict] = []
    event_rows: list[dict] = []
    n_cases = n_events = 0

    case_writer = pq.ParquetWriter(CASES_OUT, CASE_SCHEMA, compression="zstd")
    event_writer = pq.ParquetWriter(EVENTS_OUT, EVENT_SCHEMA, compression="zstd")

    try:
        for _, element in ET.iterparse(LOG, events=("end",)):
            if _local(element.tag) != "trace":
                continue

            raw = _attrs(element)
            case = {col: raw.get(key) for key, col in CASE_FIELDS.items()}
            case["gr_based_inv_verif"] = _to_bool(raw.get("GR-Based Inv. Verif."))
            case["goods_receipt"] = _to_bool(raw.get("Goods Receipt"))
            case_id = case["case_id"]
            case_rows.append(case)

            for index, event_el in enumerate(element):
                if _local(event_el.tag) != "event":
                    continue
                ev_raw = _attrs(event_el)
                worth = ev_raw.get("Cumulative net worth (EUR)")
                event_rows.append({
                    "case_id": case_id,
                    "event_index": index,
                    "activity": ev_raw.get("concept:name"),
                    "timestamp": _to_datetime(ev_raw.get("time:timestamp")),
                    "resource": ev_raw.get("org:resource"),
                    "user": ev_raw.get("User"),
                    "cumulative_net_worth_eur": float(worth) if worth is not None else None,
                })
                n_events += 1

            n_cases += 1
            # Free the parsed subtree; without this the log accumulates in RAM.
            element.clear()

            if len(case_rows) >= BATCH:
                _flush(case_writer, CASE_SCHEMA, case_rows)
            if len(event_rows) >= BATCH:
                _flush(event_writer, EVENT_SCHEMA, event_rows)
            if n_cases % 50_000 == 0:
                print(f"  {n_cases:>7,} cases · {n_events:>9,} events", flush=True)

        _flush(case_writer, CASE_SCHEMA, case_rows)
        _flush(event_writer, EVENT_SCHEMA, event_rows)
    finally:
        case_writer.close()
        event_writer.close()

    print(f"\ncases  → {CASES_OUT}  ({n_cases:,} rows)")
    print(f"events → {EVENTS_OUT}  ({n_events:,} rows)")


if __name__ == "__main__":
    main()
