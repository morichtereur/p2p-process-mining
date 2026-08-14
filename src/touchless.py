"""Touchless rate: cases that ran PO -> GR -> invoice -> payment without any
rework or exception handling, split by match type and vendor.

First attempt at a definition -- "no event has a human (user_*) resource" --
turned out degenerate: 92% of `Create Purchase Order Item` events alone carry
a human user, since a buyer has to click "create" even on a routine order.
That's normal operation, not rework, so a per-case "any human touch" rule
gives a touchless rate of 0.0% and measures nothing.

Definition used instead, derived from the activity list itself: split the 42
activities into the expected PO-to-pay flow (create/approve/receive/invoice/
clear -- including SRM's normal state transitions) and everything that only
fires when something deviates from that flow (payment blocks, cancellations,
quantity/price/approval changes, subsequent/debit-memo corrections, SRM
exception states). A case is touchless if it completes (reaches `Clear
Invoice`) and never fires an activity from the rework set.

Run: .venv/bin/python src/touchless.py
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
        bool_or(activity = 'Clear Invoice') as is_complete
    from read_parquet('data/events.parquet')
    group by case_id
""")

overall = con.execute("""
    select
        count(*) as n_cases,
        sum(case when is_complete then 1 else 0 end) as n_complete,
        sum(case when not has_rework then 1 else 0 end) as n_touchless_any,
        sum(case when is_complete and not has_rework then 1 else 0 end) as n_touchless_complete
    from case_flags
""").pl().row(0, named=True)

n_cases = overall["n_cases"]
n_complete = overall["n_complete"]
n_touchless_complete = overall["n_touchless_complete"]

print(f"Cases: {n_cases:,}")
print(f"Complete (reached Clear Invoice): {n_complete:,} ({n_complete/n_cases:.1%})")
print(f"Touchless of ALL cases: {overall['n_touchless_any']:,} ({overall['n_touchless_any']/n_cases:.1%})")
print(f"Touchless of COMPLETE cases (the STP rate): "
      f"{n_touchless_complete:,} ({n_touchless_complete/n_complete:.1%})")

print("\nBy match type (item_category), completed cases only:")
by_category = con.execute("""
    select
        c.item_category,
        count(*) as n_complete,
        sum(case when not f.has_rework then 1 else 0 end) as n_touchless,
        sum(case when not f.has_rework then 1 else 0 end) * 1.0 / count(*) as touchless_rate
    from case_flags f
    join read_parquet('data/cases.parquet') c using (case_id)
    where f.is_complete
    group by c.item_category
    order by n_complete desc
""").pl()
for row in by_category.iter_rows(named=True):
    print(f"  {row['item_category']:<32} n={row['n_complete']:>7,}  "
          f"touchless={row['n_touchless']:>7,}  rate={row['touchless_rate']:.1%}")

print("\nBy vendor (top 10 by completed-case volume):")
by_vendor = con.execute("""
    select
        c.vendor,
        c.vendor_name,
        count(*) as n_complete,
        sum(case when not f.has_rework then 1 else 0 end) as n_touchless,
        sum(case when not f.has_rework then 1 else 0 end) * 1.0 / count(*) as touchless_rate
    from case_flags f
    join read_parquet('data/cases.parquet') c using (case_id)
    where f.is_complete
    group by c.vendor, c.vendor_name
    order by n_complete desc
    limit 10
""").pl()
for row in by_vendor.iter_rows(named=True):
    print(f"  {row['vendor_name'] or row['vendor']:<28} n={row['n_complete']:>6,}  "
          f"touchless={row['n_touchless']:>6,}  rate={row['touchless_rate']:.1%}")

con.execute("""
    copy (
        select c.*, f.has_rework, f.is_complete
        from case_flags f
        join read_parquet('data/cases.parquet') c using (case_id)
    ) to 'output/touchless_cases.parquet' (format parquet)
""")
print("\nWritten -> output/touchless_cases.parquet")
