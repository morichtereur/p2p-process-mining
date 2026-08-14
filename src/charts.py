"""Four charts for the README, built from the parquet outputs of the other
four scripts (run those first). Nothing here recomputes an analysis — it only
visualizes numbers already established and printed elsewhere.

Run: .venv/bin/python src/charts.py
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import networkx as nx
import polars as pl

ASSETS = Path("assets")
ASSETS.mkdir(exist_ok=True)

NAVY = "#1b2a4a"
TEAL = "#2a9d8f"
CORAL = "#e76f51"
GRAY = "#8a94a6"
GRID = "#e3e6ec"

plt.rcParams.update({
    "figure.facecolor": "white",
    "axes.facecolor": "white",
    "axes.edgecolor": GRAY,
    "axes.labelcolor": "#333333",
    "text.color": "#222222",
    "xtick.color": "#333333",
    "ytick.color": "#333333",
    "font.size": 11,
    "font.family": "sans-serif",
    "axes.grid": True,
    "grid.color": GRID,
    "grid.linewidth": 0.8,
    "axes.axisbelow": True,
})


def _clean(ax):
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color(GRAY)
    ax.spines["bottom"].set_color(GRAY)


con = duckdb.connect()

# ---------------------------------------------------------------- 1. variant coverage
variants = con.execute("select n_cases, share, cum_share from 'output/variants.parquet' "
                        "order by n_cases desc").pl()
rank = range(1, len(variants) + 1)

fig, ax = plt.subplots(figsize=(9, 5.5))
ax.plot(rank, variants["cum_share"] * 100, color=NAVY, linewidth=2.2)
ax.set_xscale("log")
ax.set_xlabel("Variant rank (log scale)")
ax.set_ylabel("Cumulative share of cases (%)")
ax.set_title(f"{len(variants):,} variants: coverage builds slowly after the top few",
             loc="left", fontsize=13, fontweight="bold")
ax.set_ylim(0, 100)

for pct, label_dx in [(0.50, 1.3), (0.90, 1.3), (0.99, 0.55)]:
    k = int((variants["cum_share"] >= pct).arg_max()) + 1
    ax.plot([k], [pct * 100], "o", color=CORAL, zorder=5, markersize=6)
    ax.annotate(f"{pct:.0%} @ rank {k:,}", xy=(k, pct * 100),
                xytext=(k * label_dx, pct * 100 - 6), fontsize=9.5, color=CORAL)

_clean(ax)
ax.yaxis.set_major_formatter(mticker.PercentFormatter())
fig.tight_layout()
fig.savefig(ASSETS / "variant_coverage.png", dpi=170)
plt.close(fig)
print("wrote assets/variant_coverage.png")

# ---------------------------------------------------------------- 2. rework: frequency vs. cost
penalty = con.execute("select activity, median_days, n, delta_days from "
                       "'output/rework_penalty_by_activity.parquet'").pl()
total_cases = 251_734
freq_share = (penalty["n"] / total_cases * 100).to_numpy()
delta = penalty["delta_days"].to_numpy()
size = 30 + (penalty["n"].to_numpy() ** 0.5) * 1.6

fig, ax = plt.subplots(figsize=(9.5, 6))
ax.scatter(freq_share, delta, s=size, color=TEAL, alpha=0.75, edgecolor=NAVY, linewidth=0.6, zorder=3)
ax.set_xscale("log")
ax.set_xlabel("Share of all cases (%, log scale)")
ax.set_ylabel("Cycle-time penalty vs. touchless baseline (days)")
ax.set_title("Rework's cost isn't proportional to how often it happens", loc="left", fontsize=13, fontweight="bold")
ax.axhline(0, color=GRAY, linewidth=0.8)

highlight = {"Remove Payment Block", "Change Quantity", "Change Price",
             "Cancel Invoice Receipt", "Change Approval for Purchase Order",
             "SRM: Transfer Failed (E.Sys.)"}
for a, x, y in zip(penalty["activity"], freq_share, delta):
    if a in highlight:
        ax.annotate(a, xy=(x, y), xytext=(6, 4), textcoords="offset points", fontsize=9)

_clean(ax)
fig.tight_layout()
fig.savefig(ASSETS / "rework_penalty.png", dpi=170)
plt.close(fig)
print("wrote assets/rework_penalty.png")

# ---------------------------------------------------------------- 3. touchless rate by vendor
by_vendor = con.execute("""
    select vendor, count(*) as n,
           sum(case when not has_rework then 1 else 0 end) * 1.0 / count(*) as touchless_rate
    from 'output/touchless_cases.parquet'
    where is_complete
    group by vendor
    order by n desc
    limit 15
""").pl().sort("touchless_rate")

fig, ax = plt.subplots(figsize=(9, 6.5))
colors = [CORAL if r < 0.5 else TEAL for r in by_vendor["touchless_rate"]]
ax.barh(by_vendor["vendor"], by_vendor["touchless_rate"] * 100, color=colors, height=0.62)
ax.set_xlabel("Touchless rate (%)")
ax.set_title("Touchless rate, top 15 vendors by volume — a 2x spread", loc="left", fontsize=13, fontweight="bold")
ax.set_xlim(0, 100)
for y, (rate, n) in enumerate(zip(by_vendor["touchless_rate"], by_vendor["n"])):
    ax.text(rate * 100 + 1.5, y, f"n={n:,}", va="center", fontsize=8.5, color=GRAY)

_clean(ax)
ax.spines["left"].set_visible(False)
fig.tight_layout()
fig.savefig(ASSETS / "touchless_by_vendor.png", dpi=170)
plt.close(fig)
print("wrote assets/touchless_by_vendor.png")

# ---------------------------------------------------------------- 4. process map (directly-follows graph)
REWORK_ACTIVITIES = {
    "Remove Payment Block", "Set Payment Block", "Block Purchase Order Item",
    "Reactivate Purchase Order Item", "Delete Purchase Order Item", "Change Quantity",
    "Change Price", "Change Currency", "Change Storage Location", "Change Delivery Indicator",
    "Change Final Invoice Indicator", "Change payment term", "Change Rejection Indicator",
    "Change Approval for Purchase Order", "Cancel Goods Receipt", "Cancel Invoice Receipt",
    "Cancel Subsequent Invoice", "Record Subsequent Invoice", "Vendor creates debit memo",
    "Update Order Confirmation", "SRM: Incomplete", "SRM: Held", "SRM: Deleted",
    "SRM: Transfer Failed (E.Sys.)",
}

edges = con.execute("""
    with ordered as (
        select case_id, event_index, activity,
               lead(activity) over (partition by case_id order by event_index) as next_activity
        from read_parquet('data/events.parquet')
    )
    select activity, next_activity, count(*) as n
    from ordered
    where next_activity is not null and activity != next_activity
    group by activity, next_activity
    order by n desc
    limit 18
""").pl()

# event_index is offset by each trace's XES attribute count (see src/ingest.py),
# so it isn't 0-based -- rank events within each case instead of dividing the
# raw index, or every activity's "position" comes out shifted and >1.
positions = con.execute("""
    with ranked as (
        select case_id, activity,
               (row_number() over (partition by case_id order by event_index) - 1) as rnk,
               count(*) over (partition by case_id) as n_events
        from read_parquet('data/events.parquet')
    )
    select activity, avg(rnk * 1.0 / nullif(n_events - 1, 0)) as mean_pos
    from ranked
    group by activity
""").pl()
pos_map = dict(zip(positions["activity"], positions["mean_pos"]))

G = nx.DiGraph()
node_freq = con.execute("""
    select activity, count(*) as n from read_parquet('data/events.parquet') group by activity
""").pl()
node_freq_map = dict(zip(node_freq["activity"], node_freq["n"]))

nodes = set(edges["activity"]) | set(edges["next_activity"])
for node in nodes:
    G.add_node(node)
for a, b, n in zip(edges["activity"], edges["next_activity"], edges["n"]):
    G.add_edge(a, b, weight=n)

SHORT_LABEL = {
    "Create Purchase Order Item": "Create PO Item",
    "Create Purchase Requisition Item": "Create PR Item",
    "Record Goods Receipt": "Record GR",
    "Record Invoice Receipt": "Record Invoice Rcpt",
    "Record Service Entry Sheet": "Record Service Entry",
    "Receive Order Confirmation": "Receive Order Conf.",
    "Vendor creates invoice": "Vendor Creates Invoice",
    "Vendor creates debit memo": "Vendor Creates Debit Memo",
    "Change Approval for Purchase Order": "Change PO Approval",
    "Delete Purchase Order Item": "Delete PO Item",
}


def short(name: str) -> str:
    return SHORT_LABEL.get(name, name)


# x from mean position in the trace; y staggered by rank within similar x to avoid overlap
xs = {n: pos_map.get(n, 0.5) for n in nodes}
order = sorted(nodes, key=lambda n: xs[n])
ys = {}
lane_last_x = {}
for n in order:
    lane = 0
    while lane in lane_last_x and xs[n] - lane_last_x[lane] < 0.22:
        lane += 1
    lane_last_x[lane] = xs[n]
    ys[n] = lane * 1.25 - 2.0

layout = {n: (xs[n], ys[n]) for n in nodes}

fig, ax = plt.subplots(figsize=(14, 9))
max_edge_n = edges["n"].max()
for a, b, n in zip(edges["activity"], edges["next_activity"], edges["n"]):
    is_rework = a in REWORK_ACTIVITIES or b in REWORK_ACTIVITIES
    color = CORAL if is_rework else NAVY
    width = 0.6 + (n / max_edge_n) * 5.5
    ax.annotate(
        "", xy=layout[b], xytext=layout[a],
        arrowprops=dict(arrowstyle="-|>", color=color, lw=width, alpha=0.45,
                         shrinkA=16, shrinkB=16, connectionstyle="arc3,rad=0.08"),
        zorder=2,
    )

max_node_n = max(node_freq_map.get(n, 1) for n in nodes)
min_node_n = min(node_freq_map.get(n, 1) for n in nodes)
for n in nodes:
    x, y = layout[n]
    frac = (node_freq_map.get(n, 0) - min_node_n) / max(1, (max_node_n - min_node_n))
    size = 260 + frac * 1400
    color = CORAL if n in REWORK_ACTIVITIES else TEAL
    ax.scatter([x], [y], s=size, color=color, edgecolor=NAVY, linewidth=1.1, zorder=5)
    ax.annotate(short(n), xy=(x, y), xytext=(0, -13), textcoords="offset points",
                ha="center", va="top", fontsize=8.8, color="#1a1a1a", fontweight="bold", zorder=6,
                bbox=dict(boxstyle="round,pad=0.18", facecolor="white", edgecolor="none", alpha=0.82))

ax.set_title("Directly-follows map — top 18 transitions (red = touches a rework activity)",
              loc="left", fontsize=13, fontweight="bold")
ax.set_xticks([])
ax.set_yticks([])
for spine in ax.spines.values():
    spine.set_visible(False)
ax.grid(False)
ax.set_xlim(-0.15, 1.15)
ys_all = list(ys.values())
ax.set_ylim(min(ys_all) - 0.9, max(ys_all) + 0.9)
fig.tight_layout()
fig.savefig(ASSETS / "process_map.png", dpi=170)
plt.close(fig)
print("wrote assets/process_map.png")
