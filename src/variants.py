"""Variant analysis: how many cases follow the dominant path, and how long is the tail.

A variant is the ordered sequence of activities within a case. We don't assume
a "designed" path up front — the most frequent variant empirically defines it,
and everything else is deviation. Run: .venv/bin/python src/variants.py
"""

from __future__ import annotations

import duckdb
import polars as pl

con = duckdb.connect()

# string_agg's own ORDER BY is required here, not an ORDER BY on the input
# subquery -- DuckDB parallelizes aggregation, so a preceding "order by
# case_id, event_index" is not guaranteed to survive into the aggregate.
# Without it this was silently non-deterministic: reran the exact same query
# three times and got 11,973 / 11,974 / 11,975 distinct variants.
variants = con.execute("""
    with seq as (
        select case_id, string_agg(activity, ' -> ' order by event_index) as variant
        from read_parquet('data/events.parquet')
        group by case_id
    )
    select variant, count(*) as n_cases
    from seq
    group by variant
    order by n_cases desc
""").pl()

total_cases = variants["n_cases"].sum()
variants = variants.with_columns(
    (pl.col("n_cases") / total_cases).alias("share"),
)
variants = variants.with_columns(pl.col("share").cum_sum().alias("cum_share"))

n_variants = variants.height
top = variants.row(0, named=True)

print(f"Total cases: {total_cases:,}")
print(f"Distinct variants: {n_variants:,}")
print()
print(f"Top variant covers {top['n_cases']:,} cases ({top['share']:.1%}):")
print(f"  {top['variant']}")
print()

for pct in (0.50, 0.80, 0.90, 0.95, 0.99):
    k = int((variants["cum_share"] >= pct).arg_max()) + 1
    print(f"  {pct:.0%} of cases covered by top {k:,} variants ({k/n_variants:.1%} of all variants)")

n_singletons = (variants["n_cases"] == 1).sum()
print()
print(f"Singleton variants (exactly 1 case): {n_singletons:,} ({n_singletons/n_variants:.1%} of variants, "
      f"{n_singletons/total_cases:.2%} of cases)")

print()
print("Top 10 variants:")
for row in variants.head(10).iter_rows(named=True):
    print(f"  {row['n_cases']:>7,} ({row['share']:>5.1%})  {row['variant']}")

variants.write_parquet("output/variants.parquet")
print("\nWritten -> output/variants.parquet")
