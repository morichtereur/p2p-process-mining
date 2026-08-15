"""Touches per reworked case: how many rework-activity events fire on a case
that has at least one, not just whether it has one.

This exists because the GBS business case (a downstream repo) needs an
effort-per-case figure to price rework, and `rework.py` only established
*that* a case had rework, not *how much*. Reuses the rework-activity list
from `src/touchless.py` rather than redefining it, same as `rework.py` does.

Only completed cases with at least one rework activity are counted — an
incomplete case's touch count is censored, and a touchless case has none by
definition.

Run: .venv/bin/python src/rework_touches.py
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
    select case_id,
           bool_or(activity in ({rework_list})) as has_rework,
           bool_or(activity = 'Clear Invoice') as is_complete
    from read_parquet('data/events.parquet')
    group by case_id
""")

con.execute(f"""
    create or replace view touches as
    select case_id, count(*) as touch_count
    from read_parquet('data/events.parquet')
    where activity in ({rework_list})
    group by case_id
""")

stats = con.execute("""
    select count(*) as n_cases,
           median(t.touch_count) as median_touches,
           avg(t.touch_count) as mean_touches,
           quantile_cont(t.touch_count, 0.9) as p90_touches,
           max(t.touch_count) as max_touches
    from case_flags f join touches t using (case_id)
    where f.is_complete and f.has_rework
""").pl().row(0, named=True)

print(f"Reworked, completed cases: {stats['n_cases']:,}")
print(f"  median touches/case: {stats['median_touches']:.1f}")
print(f"  mean touches/case:   {stats['mean_touches']:.3f}")
print(f"  p90 touches/case:    {stats['p90_touches']:.1f}")
print(f"  max touches/case:    {stats['max_touches']}")
print(
    "\nMean exceeds median because a long tail of cases (SRM transfer "
    "failures, repeated changes) carries many touches while most reworked "
    "cases carry exactly one. Total labour scales with the mean, not the "
    "median — use the mean when pricing effort."
)

con.execute("""
    copy (
        select f.case_id, t.touch_count
        from case_flags f join touches t using (case_id)
        where f.is_complete and f.has_rework
    ) to 'output/rework_touches.parquet' (format parquet)
""")
print("\nWritten -> output/rework_touches.parquet")
