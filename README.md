# P2P Process Mining

Process mining on a real purchase-to-pay event log: reconstructing how the
process actually ran, and quantifying where cycle time and rework go.

## Data

[BPI Challenge 2019](https://data.4tu.nl/articles/dataset/BPI_Challenge_2019/12715853/1)
— SAP-derived purchase order handling from a multinational coatings company,
published for research. Not in git (695 MB); download the `.xes` into `data/`.

Verified on ingest against the published figures:

| | Expected | Parsed |
|---|---|---|
| Cases (PO line items) | 251,734 | 251,734 ✓ |
| Purchasing documents | 76,349 | 76,349 ✓ |
| Distinct activities | 42 | 42 ✓ |
| Events | >1.5M | 1,595,923 |

## What the log actually contains

Two things worth knowing before designing any analysis:

- **Effectively one company.** The dataset description mentions 60 subsidiaries,
  but the log holds 4 company IDs and 99.6% of cases sit under `companyID_0000`.
  Cross-subsidiary benchmarking is not available here.
- **Timestamps run 1948–2020** despite covering orders submitted in 2018. The
  outliers are data errors and need an explicit filter — their prevalence is
  itself a finding worth reporting rather than quietly dropping.

Dimensions that do carry signal:

- `item_category` — the match type (`3-way match, invoice before GR` is 87.8%)
- `user` — `batch_*` identifies automated steps, so the touchless rate is
  directly measurable rather than estimated (9.8% of events are batch)
- `Remove Payment Block` fires 57,136 times — a rework loop visible in the raw
  activity counts before any mining

## Setup

    python3 -m venv .venv
    .venv/bin/pip install duckdb polars pyarrow pytz
    .venv/bin/python src/ingest.py

Ingest streams the XES with `iterparse` (memory stays flat) into two Parquet
tables — `data/cases.parquet` and `data/events.parquet`. Takes ~22s.

## Planned analysis

1. **Variant analysis** — the happy path against the long tail; what share of
   cases follow the designed process at all.
2. **Touchless rate** — cases completing PO → GR → invoice → payment with no
   human rework, split by match type and vendor.
3. **Rework cost** — frequency of post-approval changes and payment blocks, and
   the cycle-time penalty attached to them.
4. **Sequence violations** — invoice before goods receipt, PO raised after the
   invoice arrived (maverick buying), and their control implications.

## Layout

    src/ingest.py     XES → Parquet
    notebooks/        exploratory analysis
    data/             raw log + derived tables (gitignored)
    output/           charts and exports (gitignored)
