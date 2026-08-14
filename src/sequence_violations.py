"""Sequence violations: cases where a real control was breached, plus the
maverick-buying check (a PO raised after the vendor's invoice already
existed).

The first cut at this compared `item_category`'s label ("invoice before GR" /
"invoice after GR") to actual event order and called any mismatch a
violation -- 91.8% of "invoice before GR" cases came up "violating," which is
implausible for a field that's supposedly the norm. Checking
`item_category` against `gr_based_inv_verif` showed they're a 1:1 encoding of
the same underlying flag:

    invoice before GR  <=>  gr_based_inv_verif = false
    invoice after GR   <=>  gr_based_inv_verif = true

So the field is a *system control setting* -- whether invoice verification is
gated on goods receipt -- not a claim about when the vendor's invoice
happens to arrive. Only `gr_based_inv_verif = true` cases have an actual rule
that can be broken (invoice verification may not be recorded before GR).
`gr_based_inv_verif = false` cases have no such rule, so an invoice recorded
before GR there is normal operation, not a violation.

Two checks follow from that:

1. **Control violation**: for `gr_based_inv_verif = true` cases, does
   `Record Invoice Receipt` (the system's own verification step) ever
   precede `Record Goods Receipt`? If SAP enforces the setting, this should
   be ~0%.
2. **Maverick buying**: `Create Purchase Order Item` timestamp vs. the first
   `Vendor creates invoice` timestamp. No policy field permits this one --
   a PO created after the vendor's invoice already exists means the
   purchase happened before it was authorized.

A separate, purely descriptive stat -- how often the vendor's invoice
(`Vendor creates invoice`, an external event SAP doesn't control) arrives
before goods receipt across all 3-way-match cases, regardless of the GR-based
setting -- is reported too, since it cross-checks against the variant
analysis (the top two variants differ on exactly this order, ~20% vs ~12% of
all cases, i.e. roughly the same 60/40 split found here among the subset with
both events).

All checks exclude the 266 cases with a corrupt event timestamp (see
`src/rework.py`).

Run: .venv/bin/python src/sequence_violations.py
"""

from __future__ import annotations

import duckdb

con = duckdb.connect()

con.execute("""
    create or replace view per_case as
    select
        case_id,
        min(timestamp) filter (where activity = 'Create Purchase Order Item') as t_po,
        min(timestamp) filter (where activity = 'Record Goods Receipt') as t_gr,
        min(timestamp) filter (where activity = 'Record Invoice Receipt') as t_inv_recv,
        min(timestamp) filter (where activity = 'Vendor creates invoice') as t_inv_vendor,
        bool_or(extract(year from timestamp) not in (2018, 2019)) as has_bad_ts
    from read_parquet('data/events.parquet')
    group by case_id
""")

# --- 1. Control violation: GR-based invoice verification actually enforced? ---
print("1. Control check: Record Invoice Receipt before Record Goods Receipt,\n"
      "   where gr_based_inv_verif requires GR first\n")
ctrl = con.execute("""
    select
        count(*) as n_comparable,
        sum(case when t_inv_recv < t_gr then 1 else 0 end) as n_violation
    from per_case p
    join read_parquet('data/cases.parquet') c using (case_id)
    where p.t_gr is not null and p.t_inv_recv is not null and not p.has_bad_ts
      and c.gr_based_inv_verif = true
""").pl().row(0, named=True)
rate = ctrl["n_violation"] / ctrl["n_comparable"]
print(f"  n={ctrl['n_comparable']:,}  violations={ctrl['n_violation']:,}  rate={rate:.2%}")

# --- descriptive cross-check: raw vendor-invoice-vs-GR order, all 3-way-match cases ---
print("\n(descriptive, not a violation) Vendor invoice vs. GR order, both 3-way-match categories:\n")
desc = con.execute("""
    select
        count(*) as n_comparable,
        sum(case when t_inv_vendor < t_gr then 1 else 0 end) as n_inv_first,
        sum(case when t_gr < t_inv_vendor then 1 else 0 end) as n_gr_first
    from per_case p
    join read_parquet('data/cases.parquet') c using (case_id)
    where p.t_gr is not null and p.t_inv_vendor is not null and not p.has_bad_ts
      and c.item_category in ('3-way match, invoice before GR', '3-way match, invoice after GR')
""").pl().row(0, named=True)
print(f"  n={desc['n_comparable']:,}  invoice-first={desc['n_inv_first']:,} "
      f"({desc['n_inv_first']/desc['n_comparable']:.1%})  "
      f"GR-first={desc['n_gr_first']:,} ({desc['n_gr_first']/desc['n_comparable']:.1%})")

# --- 2. Maverick buying: PO raised after the vendor's invoice already exists ---
print("\n2. Maverick buying (PO created after the vendor invoice)\n")
mav = con.execute("""
    select
        count(*) as n_comparable,
        sum(case when t_po > t_inv_vendor then 1 else 0 end) as n_maverick
    from per_case
    where t_po is not null and t_inv_vendor is not null and not has_bad_ts
""").pl().row(0, named=True)
rate = mav["n_maverick"] / mav["n_comparable"]
print(f"  n={mav['n_comparable']:,}  maverick={mav['n_maverick']:,}  rate={rate:.1%}")

lag = con.execute("""
    select median(epoch(t_po - t_inv_vendor) / 86400.0)
    from per_case
    where t_po is not null and t_inv_vendor is not null and not has_bad_ts
      and t_po > t_inv_vendor
""").fetchone()[0]
print(f"  median delay: PO raised {lag:.1f} days after the invoice already existed")

con.execute("""
    copy (select * from per_case) to 'output/sequence_cases.parquet' (format parquet)
""")
print("\nWritten -> output/sequence_cases.parquet")
