"""Rework cost: how often rework fires, and the cycle-time penalty it carries.

Two things this depends on that are already established elsewhere in this
codebase:

- The rework/exception activity list from `src/touchless.py` (payment blocks,
  cancellations, quantity/price/approval changes, subsequent invoices/debit
  memos, SRM exception states) -- reused here rather than redefined.
- The timestamp corruption noted in the README: 266 cases (0.1%) have at
  least one event stamped outside 2018-2019 (1948, 1993, 2001, 2008, 2020 --
  clearly a default/sentinel date, not real data). Cycle time is undefined
  for those cases, so they're dropped from the duration analysis only (not
  from the frequency counts, where the corrupt timestamp doesn't matter).

Cycle time = max(timestamp) - min(timestamp) across a case's events. Only
completed cases (reached Clear Invoice) are used, since an in-flight case's
duration is censored, not a real cycle time.

Run: .venv/bin/python src/rework.py
"""

from __future__ import annotations

import duckdb

REWORK_ACTIVITIES = [
    "Remove Payment Block",
    "Set Payment Block",
    "Block Purchase Order Item",
    "Reactivate Purchase Order Item",
    "Delete Purchase Order Item",
    "Change Quantity",
    "Change Price",
    "Change Currency",
    "Change Storage Location",
    "Change Delivery Indicator",
    "Change Final Invoice Indicator",
    "Change payment term",
    "Change Rejection Indicator",
    "Change Approval for Purchase Order",
    "Cancel Goods Receipt",
    "Cancel Invoice Receipt",
    "Cancel Subsequent Invoice",
    "Record Subsequent Invoice",
    "Vendor creates debit memo",
    "Update Order Confirmation",
    "SRM: Incomplete",
    "SRM: Held",
    "SRM: Deleted",
    "SRM: Transfer Failed (E.Sys.)",
]

con = duckdb.connect()
rework_list = ", ".join(f"'{a}'" for a in REWORK_ACTIVITIES)

con.execute(f"""
    create or replace view case_flags as
    select
        case_id,
        bool_or(activity in ({rework_list})) as has_rework,
        bool_or(activity = 'Clear Invoice') as is_complete,
        bool_or(extract(year from timestamp) not in (2018, 2019)) as has_bad_ts,
        max(timestamp) - min(timestamp) as duration
    from read_parquet('data/events.parquet')
    group by case_id
""")

# --- 1. Frequency of each rework activity, case-level ---
print("Rework activity frequency (share of all 251,734 cases):\n")
freq = con.execute(f"""
    select activity, count(distinct case_id) as n_cases
    from read_parquet('data/events.parquet')
    where activity in ({rework_list})
    group by activity
    order by n_cases desc
""").pl()
total_cases = 251_734
for row in freq.iter_rows(named=True):
    print(f"  {row['activity']:<38} {row['n_cases']:>7,}  ({row['n_cases']/total_cases:.1%})")

n_any_rework = con.execute("select count(*) from case_flags where has_rework").fetchone()[0]
print(f"\n  {'ANY rework activity':<38} {n_any_rework:>7,}  ({n_any_rework/total_cases:.1%})")

# --- 2. Cycle-time penalty: touchless vs reworked, completed + clean-timestamp cases ---
print("\nCycle time (days), completed cases with clean timestamps only:\n")
n_bad_ts_complete = con.execute(
    "select count(*) from case_flags where is_complete and has_bad_ts"
).fetchone()[0]
print(f"  ({n_bad_ts_complete:,} completed cases excluded for a corrupt timestamp)\n")

stats = con.execute("""
    select
        has_rework,
        count(*) as n,
        median(epoch(duration) / 86400.0) as median_days,
        avg(epoch(duration) / 86400.0) as mean_days,
        quantile_cont(epoch(duration) / 86400.0, 0.90) as p90_days
    from case_flags
    where is_complete and not has_bad_ts
    group by has_rework
    order by has_rework
""").pl()
for row in stats.iter_rows(named=True):
    label = "Reworked" if row["has_rework"] else "Touchless"
    print(f"  {label:<10} n={row['n']:>7,}  median={row['median_days']:>6.1f}d  "
          f"mean={row['mean_days']:>6.1f}d  p90={row['p90_days']:>6.1f}d")

# --- 3. Penalty by individual rework activity (cases with that activity vs cases without any rework) ---
# Every activity with >=30 comparable cases -- not just the headline five -- so
# charts.py can plot the full frequency-vs-cost picture, not a cherry-picked one.
print("\nMedian cycle time by specific rework activity (vs touchless baseline):\n")
touchless_median = stats.filter(stats["has_rework"] == False)["median_days"][0]
print(f"  {'(touchless baseline)':<38} {touchless_median:>6.1f}d")

penalty_rows = []
for activity in freq["activity"]:
    row = con.execute(f"""
        select median(epoch(duration) / 86400.0) as median_days, count(*) as n
        from case_flags f
        where f.is_complete and not f.has_bad_ts
          and f.case_id in (
              select case_id from read_parquet('data/events.parquet') where activity = '{activity}'
          )
    """).pl().row(0, named=True)
    if row["n"] < 30 or row["median_days"] is None:
        continue
    delta = row["median_days"] - touchless_median
    penalty_rows.append({"activity": activity, "median_days": row["median_days"],
                          "n": row["n"], "delta_days": delta})

import polars as pl
penalty_df = pl.DataFrame(penalty_rows).sort("delta_days", descending=True)
for row in penalty_df.iter_rows(named=True):
    print(f"  {row['activity']:<38} {row['median_days']:>6.1f}d  n={row['n']:>6,}  "
          f"(+{row['delta_days']:.1f}d vs touchless)")
penalty_df.write_parquet("output/rework_penalty_by_activity.parquet")

# --- 4. Rework rate and cycle-time penalty by vendor (top 15 by completed-case volume) ---
print("\nBy vendor (top 15 by completed-case volume):\n")
by_vendor = con.execute("""
    select
        c.vendor,
        count(*) as n,
        sum(case when f.has_rework then 1 else 0 end) * 1.0 / count(*) as rework_rate,
        median(epoch(f.duration) / 86400.0) filter (where not f.has_rework) as touchless_median_days,
        median(epoch(f.duration) / 86400.0) filter (where f.has_rework) as reworked_median_days
    from case_flags f
    join read_parquet('data/cases.parquet') c using (case_id)
    where f.is_complete and not f.has_bad_ts
    group by c.vendor
    having count(*) >= 200
    order by n desc
    limit 15
""").pl()
for row in by_vendor.iter_rows(named=True):
    penalty = (row["reworked_median_days"] or 0) - (row["touchless_median_days"] or 0)
    print(f"  {row['vendor']:<14} n={row['n']:>6,}  rework_rate={row['rework_rate']:>5.1%}  "
          f"penalty={penalty:>5.1f}d")
by_vendor.write_parquet("output/rework_by_vendor.parquet")

con.execute("""
    copy (select * from case_flags) to 'output/rework_cases.parquet' (format parquet)
""")
print("\nWritten -> output/rework_cases.parquet, output/rework_penalty_by_activity.parquet, "
      "output/rework_by_vendor.parquet")
