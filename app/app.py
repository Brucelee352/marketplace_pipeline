"""
Florida Marketplace Mental Health Coverage Dashboard

Explores ACA individual market plan data across twelve FL counties to surface
coverage gaps, parity failures, and carrier-level value signals.
"""

import json
import os
from datetime import datetime
from pathlib import Path

import dash
import dash_bootstrap_components as dbc
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import dash_ag_grid as dag
from dash import Dash, Input, Output, State, dcc, html
from flask import send_from_directory


# ── Constants ─────────────────────────────────────────────────────────────────

ROOT    = Path(__file__).parents[1]
DB_PATH = ROOT / "marketplace.duckdb"

COUNTY_COORDS = {
    "12083": {"name": "Marion",       "lat": 29.21, "lon": -82.13},
    "12057": {"name": "Hillsborough", "lat": 27.86, "lon": -82.39},
    "12011": {"name": "Broward",      "lat": 26.19, "lon": -80.37},
    "12086": {"name": "Miami-Dade",   "lat": 25.76, "lon": -80.19},
    "12095": {"name": "Orange",       "lat": 28.54, "lon": -81.22},
    "12103": {"name": "Pinellas",     "lat": 27.88, "lon": -82.74},
    "12031": {"name": "Duval",        "lat": 30.34, "lon": -81.66},
    "12115": {"name": "Sarasota",     "lat": 27.18, "lon": -82.36},
    "12099": {"name": "Palm Beach",   "lat": 26.65, "lon": -80.44},
    "12069": {"name": "Lake",         "lat": 28.76, "lon": -81.71},
    "12001": {"name": "Alachua",      "lat": 29.67, "lon": -82.35},
    "12105": {"name": "Polk",         "lat": 27.95, "lon": -81.70},
}

METAL_ORDER  = ["Catastrophic", "Bronze", "Silver", "Gold", "Platinum"]
METAL_COLORS = {
    "Catastrophic": "#94A3B8",
    "Bronze":       "#B45309",
    "Silver":       "#9CA3AF",
    "Gold":         "#D97706",
    "Platinum":     "#6366F1",
}

# Assumed cost of one therapy session for coinsurance-based plans
SESSION_COST = 150

C = {
    "bg":           "#EEF2FF",
    "card":         "#FFFFFF",
    "border":       "#E0E7FF",
    "navy":         "#1E3A5F",
    "blue":         "#2563EB",
    "purple":       "#7C3AED",
    "light_purple": "#C4B5FD",
    "text":         "#1E293B",
    "subtext":      "#64748B",
    "grid":         "#F1F5F9",
    "axis":         "#E2E8F0",
}

# Cross-platform font stack: native UI font on Windows, macOS, iOS, Android, Linux
FONT = (
    "system-ui, -apple-system, 'Segoe UI', Roboto, 'Helvetica Neue', "
    "Arial, 'Noto Sans', sans-serif"
)


# ── Data preparation ──────────────────────────────────────────────────────────

def _load():
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        dim_plan = con.execute("SELECT * FROM main.dim_plan").df()
        benefits = con.execute("SELECT * FROM main.fct_plan_benefits").df()
        score    = con.execute("SELECT * FROM main.fct_plan_mh_coverage_score").df()
    finally:
        con.close()

    # Carrier display name: strip legal suffixes and parenthetical notes
    carrier_name = dim_plan["carrier_name"].fillna(dim_plan["plan_id"].str[:5])
    dim_plan["carrier"]       = carrier_name
    dim_plan["carrier_short"] = (
        carrier_name
        .str.replace(r"\s*\(.*?\)", "", regex=True)
        .str.replace(r",?\s*(Inc\.?|LLC\.?|Company of Florida)", "", regex=True)
        .str.strip()
    )

    # Per-plan minimum in-network copays from fct_plan_benefits
    def _best_in_net(btype, col_rename):
        return (
            benefits[
                (benefits["benefit_type"] == btype)
                & (benefits["network_tier"] == "In-Network")
            ]
            .groupby(["county_fips", "plan_id"])
            .agg(**{k: (v, "min") for k, v in col_rename.items()})
            .reset_index()
        )

    mh_in   = _best_in_net(
        "MENTAL_BEHAVIORAL_HEALTH_OUTPATIENT_SERVICES",
        {"mh_copay": "copay", "mh_coinsurance": "coinsurance_rate"},
    )
    spec_in = _best_in_net("SPECIALIST_VISIT", {"spec_copay": "copay"})
    pc_in   = _best_in_net(
        "PRIMARY_CARE_VISIT_TO_TREAT_AN_INJURY_OR_ILLNESS", {"pc_copay": "copay"}
    )

    # Individual in-network deductible already computed and COALESCE'd to $9,200 in the dbt model
    ded = score[["county_fips", "plan_id", "in_network_deductible", "coverage_score"]].rename(
        columns={"in_network_deductible": "deductible"}
    )

    master = (
        dim_plan[[
            "county_fips", "county_name", "plan_id", "plan_name",
            "metal_level", "plan_type", "premium",
            "hsa_eligible", "specialist_referral_required",
            "carrier", "carrier_short", "global_rating",
        ]]
        .merge(mh_in,   on=["county_fips", "plan_id"], how="left")
        .merge(spec_in, on=["county_fips", "plan_id"], how="left")
        .merge(pc_in,   on=["county_fips", "plan_id"], how="left")
        .merge(ded,     on=["county_fips", "plan_id"], how="left")
    )
    master["deductible"]        = master["deductible"].fillna(9200)
    master["mh_coinsurance"]    = master["mh_coinsurance"].fillna(0)
    master["effective_mh_cost"] = master["mh_copay"] + master["mh_coinsurance"] * SESSION_COST
    master["metal_level"] = pd.Categorical(
        master["metal_level"], categories=METAL_ORDER, ordered=True
    )

    return benefits, master


BENEFITS, PLANS = _load()


def _last_built() -> str:
    """Date the dbt models were last built, from the shipped manifest.

    Read once at import: the manifest is baked into the image, so this is a
    constant for the life of the container. Returns "" if unavailable, which
    drops the date from the footer rather than breaking the app.
    """
    candidates = [
        ROOT / "dbt_docs" / "manifest.json",                  # container layout
        ROOT / "marketplace_pipeline" / "target" / "manifest.json",  # local dbt target
        ROOT / "dbt_docs" / "target" / "manifest.json",
    ]
    for path in candidates:
        try:
            with path.open(encoding="utf-8") as fh:
                stamp = json.load(fh)["metadata"]["generated_at"]
            dt = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
            return f"{dt.day} {dt:%b %Y}"
        except (OSError, KeyError, ValueError):
            continue
    return ""


LAST_BUILT = _last_built()


# ── Helpers ───────────────────────────────────────────────────────────────────

def _filter(df: pd.DataFrame, county: str) -> pd.DataFrame:
    if county and county != "All Counties":
        return df[df["county_name"] == county].copy()
    return df.copy()


def _theme(fig: go.Figure, title: str = "") -> go.Figure:
    fig.update_layout(
        paper_bgcolor=C["card"],
        plot_bgcolor=C["grid"],
        font=dict(family=FONT, color=C["text"], size=12),
        title=dict(
            text=title, x=0.01, xanchor="left",
            font=dict(size=13, color=C["navy"]),
        ) if title else {},
        margin=dict(l=16, r=16, t=52 if title else 16, b=72),
        legend=dict(
            bgcolor="rgba(0,0,0,0)", borderwidth=0, font_size=11,
            orientation="h", yanchor="top", y=-0.12, xanchor="center", x=0.5,
        ),
    )
    fig.update_xaxes(gridcolor=C["axis"], linecolor=C["axis"], zeroline=False)
    fig.update_yaxes(gridcolor=C["axis"], linecolor=C["axis"], zeroline=False)
    return fig


def _pareto(df: pd.DataFrame, x: str, y: str) -> pd.DataFrame:
    """Pareto-optimal rows minimising both x and y."""
    df = df.dropna(subset=[x, y]).sort_values(x).reset_index(drop=True)
    rows, min_y = [], float("inf")
    for _, row in df.iterrows():
        if row[y] < min_y:
            min_y = row[y]
            rows.append(row)
    return pd.DataFrame(rows)


# ── Chart builders ────────────────────────────────────────────────────────────

def fig_map(county: str) -> go.Figure:
    stats = (
        PLANS.groupby("county_name")
        .agg(plan_count=("plan_id", "nunique"), avg_mh_copay=("mh_copay", "mean"))
        .reset_index()
    )
    coords = pd.DataFrame([
        {"county_name": v["name"], "lat": v["lat"], "lon": v["lon"]}
        for v in COUNTY_COORDS.values()
    ])
    stats = stats.merge(coords, on="county_name")

    fig = px.scatter_map(
        stats,
        lat="lat", lon="lon",
        size="plan_count",
        color="plan_count",
        color_continuous_scale="Tropic_r",
        hover_name="county_name",
        hover_data={"plan_count": True, "avg_mh_copay": ":.0f", "lat": False, "lon": False},
        labels={"plan_count": "Plans", "avg_mh_copay": "Avg MH Copay ($)"},
        size_max=42,
        zoom=5.8,
        center={"lat": 27.8, "lon": -81.5},
        map_style="carto-positron",
        custom_data=["county_name"],
    )

    if county and county != "All Counties":
        sel = stats[stats["county_name"] == county]
        fig.add_trace(go.Scattermap(
            lat=sel["lat"], lon=sel["lon"],
            mode="markers",
            marker=dict(size=64, color="rgba(37,128,135,0.22)"),
            hoverinfo="skip",
            showlegend=False,
        ))

    fig.update_layout(
        paper_bgcolor=C["card"],
        margin=dict(l=0, r=0, t=0, b=0),
        height=440,
        font=dict(family=FONT),
        coloraxis_colorbar=dict(
            title="Plans", thickness=10, len=0.55, tickfont_size=10,
        ),
    )
    return fig


def fig_q1_access(county: str) -> go.Figure:
    """Q1 -- What share of plans offer affordable MH outpatient coverage by metal tier?"""
    df = _filter(PLANS, county).dropna(subset=["mh_copay"])
    df["copay_tier"] = pd.cut(
        df["mh_copay"],
        bins=[-1, 0, 25, 50, 999],
        labels=["$0 (No Cost-Share)", "$1-$25", "$26-$50", "$51+"],
    )
    grouped = (
        df.groupby(["metal_level", "copay_tier"], observed=True)
        .size().reset_index(name="plans")
    )
    fig = px.bar(
        grouped, x="metal_level", y="plans", color="copay_tier",
        barmode="stack",
        color_discrete_sequence=["#2563EB", "#7C3AED", "#C4B5FD", "#DBEAFE"],
        labels={"metal_level": "", "plans": "N", "copay_tier": "MH Copay Tier"},
        category_orders={"metal_level": METAL_ORDER},
    )
    _theme(fig, "Outpatient Access: Plans by Copay Tier & Metal Level")
    fig.update_layout(legend_title_text="In-Network Copay", height=420)
    return fig


def fig_q2_plan_types(county: str) -> go.Figure:
    """Q2 -- Plan type availability heatmap. Counties x plan types, or metal tiers x plan types."""
    df = _filter(PLANS, county).copy()
    all_counties = not county or county == "All Counties"

    if all_counties:
        agg = (
            df.groupby(["county_name", "plan_type"])
            .agg(plan_count=("plan_id", "nunique"), avg_premium=("premium", "mean"))
            .reset_index()
        )
        pivot_z    = agg.pivot(index="county_name", columns="plan_type", values="plan_count").fillna(0)
        pivot_prem = agg.pivot(index="county_name", columns="plan_type", values="avg_premium")
        y_title = "County"
    else:
        df["metal_level"] = pd.Categorical(df["metal_level"], categories=METAL_ORDER, ordered=True)
        agg = (
            df.groupby(["metal_level", "plan_type"], observed=True)
            .agg(plan_count=("plan_id", "nunique"), avg_premium=("premium", "mean"))
            .reset_index()
            .sort_values("metal_level")
        )
        pivot_z    = agg.pivot(index="metal_level", columns="plan_type", values="plan_count").fillna(0)
        pivot_prem = agg.pivot(index="metal_level", columns="plan_type", values="avg_premium")
        y_title = "Metal Tier"

    z_vals    = pivot_z.values
    prem_vals = pivot_prem.values
    rows_idx  = pivot_z.index.tolist()
    cols_idx  = pivot_z.columns.tolist()
    hover = [
        [
            (
                f"<b>{cols_idx[j]} — {rows_idx[i]}</b><br>"
                f"Plans: {int(z_vals[i, j])}<br>"
                + (f"Avg Premium: ${prem_vals[i, j]:.0f}/mo" if pd.notna(prem_vals[i, j]) else "")
            )
            for j in range(len(cols_idx))
        ]
        for i in range(len(rows_idx))
    ]

    fig = go.Figure(go.Heatmap(
        z=pivot_z.values,
        x=pivot_z.columns.tolist(),
        y=pivot_z.index.tolist(),
        text=[[str(int(v)) if v > 0 else "" for v in row] for row in pivot_z.values],
        texttemplate="%{text}",
        textfont=dict(size=13, color="white"),
        hovertext=hover,
        hovertemplate="%{hovertext}<extra></extra>",
        colorscale=[[0, C["bg"]], [0.4, C["blue"]], [1, C["purple"]]],
        colorbar=dict(title="Plans", thickness=12, len=0.75, tickfont_size=10),
    ))

    _theme(fig, "Plan Type Availability by County")
    fig.update_layout(
        height=340 if all_counties else 300,
        xaxis_title="Plan Type",
        yaxis_title=y_title,
    )
    return fig


def fig_q3_parity(county: str) -> go.Figure:
    """Q3 -- Parity: copay distribution for therapy vs. comparable medical visits."""
    type_map = {
        "MENTAL_BEHAVIORAL_HEALTH_OUTPATIENT_SERVICES":     "Therapy",
        "SPECIALIST_VISIT":                                  "Specialist",
        "PRIMARY_CARE_VISIT_TO_TREAT_AN_INJURY_OR_ILLNESS": "Primary Care",
    }
    df = BENEFITS[
        BENEFITS["benefit_type"].isin(type_map)
        & (BENEFITS["network_tier"] == "In-Network")
    ].copy()
    df = _filter(df, county)
    df["visit_type"] = df["benefit_type"].map(type_map)

    if "metal_level" not in df.columns:
        df = df.merge(
            PLANS[["county_fips", "plan_id", "metal_level"]],
            on=["county_fips", "plan_id"],
            how="left",
        )

    df["metal_level"] = pd.Categorical(df["metal_level"], categories=METAL_ORDER, ordered=True)

    fig = px.violin(
        df, x="visit_type", y="copay",
        color="metal_level",
        color_discrete_map=METAL_COLORS,
        box=True,
        points=False,
        category_orders={
            "visit_type":  ["Therapy", "Specialist", "Primary Care"],
            "metal_level": METAL_ORDER,
        },
        labels={"visit_type": "", "copay": "In-Network Copay ($)", "metal_level": "Metal Tier"},
    )
    _theme(fig, "Therapy vs. Medical Visit Copay Distribution")
    fig.update_layout(height=420, violingap=0.2, violinmode="group")
    fig.update_xaxes(tickangle=0, tickfont_size=11)
    return fig


def fig_q4_carrier(county: str) -> go.Figure:
    """Q4 -- Which carriers offer the best MH value relative to their premium tier?"""
    df = _filter(PLANS, county).dropna(subset=["carrier_short", "mh_copay"])
    agg = (
        df.groupby("carrier_short").agg(
            avg_premium=("premium", "mean"),
            avg_mh_copay=("mh_copay", "mean"),
            plan_count=("plan_id", "nunique"),
        ).reset_index()
    )

    fig = px.scatter(
        agg,
        x="avg_premium", y="avg_mh_copay",
        size="plan_count", text="carrier_short",
        color="avg_mh_copay",
        color_continuous_scale=[[0, C["blue"]], [0.5, C["purple"]], [1, "#FDA4AF"]],
        labels={
            "avg_premium":  "Avg Monthly Premium ($)",
            "avg_mh_copay": "Avg MH Outpatient Copay ($)",
            "plan_count":   "Plans Offered",
        },
        size_max=48,
        hover_name="carrier_short",
        hover_data={
            "plan_count":   True,
            "avg_premium":  ":.0f",
            "avg_mh_copay": ":.0f",
            "carrier_short": False,
        },
    )
    fig.update_traces(textposition="top center", textfont=dict(size=10, color=C["text"]))
    fig.update_coloraxes(showscale=False)

    fig.add_vline(x=agg["avg_premium"].median(),  line_dash="dot", line_color="#CBD5E1", line_width=1.5)
    fig.add_hline(y=agg["avg_mh_copay"].median(), line_dash="dot", line_color="#CBD5E1", line_width=1.5)

    _theme(fig, "Carrier Value: Average Premium vs. Mental Health Access")
    fig.update_layout(height=420)
    return fig


def fig_q5_pareto(county: str, annual_visits: int = 12) -> go.Figure:
    """
    Optimization -- Plan Efficiency Frontier.

    Multi-objective: minimise monthly premium AND expected annual MH out-of-pocket.
    Expected annual MH OOP = effective_mh_cost x annual_visits.
    Plans on the Pareto frontier are optimal -- no other plan beats them on both axes.
    """
    df = _filter(PLANS, county).dropna(subset=["effective_mh_cost", "premium", "carrier_short"])
    df["annual_mh_oop"] = (df["effective_mh_cost"] * annual_visits).round(0)
    df["metal_level"] = pd.Categorical(df["metal_level"], categories=METAL_ORDER, ordered=True)

    pareto = _pareto(df, "premium", "annual_mh_oop")

    fig = px.scatter(
        df,
        x="premium", y="annual_mh_oop",
        color="metal_level",
        color_discrete_map=METAL_COLORS,
        hover_name="plan_name",
        hover_data={
            "carrier_short": True,
            "mh_copay":      ":.0f",
            "deductible":    ":.0f",
            "premium":       ":.2f",
            "annual_mh_oop": ":.0f",
        },
        opacity=0.55,
        labels={
            "premium":       "Monthly Premium ($)",
            "annual_mh_oop": f"Expected OOP, {annual_visits} visits ($)",
            "metal_level":   "Metal Tier",
        },
        category_orders={"metal_level": METAL_ORDER},
    )

    if not pareto.empty:
        fig.add_trace(go.Scatter(
            x=pareto["premium"],
            y=pareto["annual_mh_oop"],
            mode="lines+markers",
            name="Pareto Frontier",
            line=dict(color=C["navy"], width=2, dash="dash"),
            marker=dict(size=9, color=C["navy"], symbol="diamond"),
            hovertemplate=(
                "<b>%{text}</b><br>"
                "Premium: $%{x:.0f}/mo<br>"
                "Annual MH OOP: $%{y:.0f}"
                "<extra></extra>"
            ),
            text=pareto["plan_name"],
        ))

    fig.add_annotation(
        xref="paper", yref="paper", x=0.01, y=0.98,
        text="Frontier: no other plan beats<br>these on both cost dimensions.",
        showarrow=False, align="left",
        font=dict(size=10, color=C["subtext"]),
        bgcolor="rgba(255,255,255,0.85)",
        bordercolor=C["border"], borderwidth=1,
    )

    _theme(fig, f"Plan Efficiency Frontier (Visits per year: {annual_visits})")
    fig.update_layout(
        height=440,
        legend_title_text="Metal Tier",
        legend=dict(orientation="h", xanchor="center", x=0.5, yanchor="top", y=-0.22),
        margin=dict(r=16, b=100),
    )
    return fig


# ── Layout helpers ────────────────────────────────────────────────────────────

def _card(*children, **extra_style):
    return html.Div(
        list(children),
        style={
            "background":   C["card"],
            "borderRadius": "12px",
            "padding":      "16px",
            "boxShadow":    "0 1px 4px rgba(0,0,0,0.08)",
            "border":       f"1px solid {C['border']}",
            "marginBottom": "20px",
            **extra_style,
        },
    )


def _graph(graph_id: str) -> dcc.Graph:
    return dcc.Graph(id=graph_id, config={"displayModeBar": False, "responsive": True})


# AG Grid theme (Theming API), matched to the dashboard palette
GRID_THEME = (
    "themeQuartz.withParams({"
    f"fontFamily: \"{FONT}\", fontSize: 12, "
    f"headerBackgroundColor: '{C['navy']}', headerTextColor: '#FFFFFF', "
    f"oddRowBackgroundColor: '{C['bg']}', borderColor: '{C['border']}', "
    f"accentColor: '{C['blue']}', wrapperBorderRadius: 8"
    "})"
)

# Fixed box so <a> links and the <button> render identically in every browser
NAV_BTN = {
    "display":        "inline-flex",
    "alignItems":     "center",
    "justifyContent": "center",
    "boxSizing":      "border-box",
    "width":          "96px",
    "height":         "32px",
    "margin":         0,
    "padding":        "0 8px",
    "fontFamily":     "inherit",
    "fontSize":       "12px",
    "fontWeight":     600,
    "lineHeight":     1,
    "color":          C["navy"],
    "background":     C["card"],
    "textDecoration": "none",
    "border":         f"1px solid {C['navy']}",
    "borderRadius":   "6px",
    "whiteSpace":     "nowrap",
    "cursor":         "pointer",
}

ABOUT_MD = """
This dashboard compares mental health coverage across 2025 ACA individual
market plans in twelve Florida counties. I built it to find out which plans
give the best mental health coverage in the Florida metros I've lived in or
liked visiting.

#### Pipeline

1. `scripts/main.py` pages through the CMS Marketplace API
   (`POST /plans/search`) for each county and collects plans, benefits,
   deductibles, out-of-pocket maximums, issuers, and CMS quality ratings.
2. Each dataset is loaded into the `raw_data` schema of a DuckDB file.
   Timestamped CSV copies go to `data/snapshots/` for auditing.
3. dbt (`dbt deps`, then `dbt build`) builds the staging views, the
   `dim_plan` and `dim_issuer` marts, the fact tables, and an SCD Type 2
   snapshot of plan history, and runs the tests.
4. `fct_plan_mh_coverage_score` scores each plan from 0 to 100: 40 points
   for the outpatient therapy copay, 25 for the deductible, 20 for mental
   health benefit coverage, and 15 for the CMS star rating.
5. This app reads the dbt models from DuckDB in read-only mode. It runs on
   Dash, Plotly, and gunicorn in a container on Google Cloud Run.

#### Reading the charts

For plans that charge coinsurance instead of a copay, therapy cost assumes
a $150 session. Plans with no deductible on file are set to $9,200, the
highest deductible in this market. The efficiency frontier marks the plans
that no other plan beats on both monthly premium and expected yearly
therapy cost.

>Source: CMS Marketplace API, plan year 2025.
"""


# ── App ───────────────────────────────────────────────────────────────────────

app = Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    title="FL MH Coverage Dashboard",
    meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}],
)
server = app.server

DOCS_DIR = ROOT / "dbt_docs"

@server.route("/dbt-docs/")
@server.route("/dbt-docs/<path:filename>")
def dbt_docs(filename="index.html"):
    return send_from_directory(DOCS_DIR, filename)

COUNTY_OPTIONS = [{"label": "All Counties", "value": "All Counties"}] + [
    {"label": v["name"], "value": v["name"]} for v in COUNTY_COORDS.values()
]

about_modal = dbc.Modal(
    id="about-modal",
    is_open=False,
    size="lg",
    scrollable=True,
    centered=True,
    fullscreen="sm-down",
    children=[
        dbc.ModalHeader(dbc.ModalTitle("About this dashboard", style={"color": C["navy"]})),
        dbc.ModalBody(dcc.Markdown(ABOUT_MD, style={"fontSize": "14px", "color": C["text"]})),
    ],
)

app.layout = dbc.Container(
    fluid=True,
    className="p-0",
    style={"background": C["bg"], "minHeight": "100vh", "fontFamily": FONT},
    children=[
        about_modal,

        # Header
        html.Div(
            className="px-3 px-md-4 py-3",
            style={"background": C["navy"]},
            children=[
                html.H5(
                    "FL Marketplace: ACA Mental Health Coverage Tracker",
                    style={"color": "#FFFFFF", "margin": 0, "fontWeight": 600},
                ),
                html.P(
                    "Bruce A. Lee, 2026",
                    style={"color": "#FFFFFF", "margin": 0, "fontSize": "12px", 'textAlign': 'right'},
                ),
                html.Div(
                    className="d-flex flex-wrap gap-2 mt-2",
                    children=[
                        html.Button("About", id="about-btn", n_clicks=0,
                                    type="button", style=NAV_BTN),
                        html.A("Home", href="https://brucea-lee.com/",
                               target="_self", style=NAV_BTN),
                        html.A("dbt Docs ↗", href="https://brucea-lee.com/docs-aca/",
                               target="_blank", rel="noopener noreferrer", style=NAV_BTN),
                    ],
                ),
            ],
        ),

        # Body
        html.Div(
            className="p-2 p-md-4",
            children=[

                # County selector + Florida map
                dbc.Row([
                    dbc.Col(
                        _card(
                            html.Label(
                                "Filter by County",
                                style={"fontWeight": 600, "fontSize": "13px", "color": C["navy"]},
                            ),
                            dcc.Dropdown(
                                id="county-dd",
                                options=COUNTY_OPTIONS,
                                value="All Counties",
                                clearable=False,
                                style={"fontSize": "13px"},
                            ),
                            html.Hr(style={"margin": "14px 0"}),
                            html.P(
                                "Click a county bubble on the map or use the dropdown. "
                                "All charts and the data table update together.",
                                style={"fontSize": "12px", "color": C["subtext"], "margin": 0},
                            ),
                            html.Hr(style={"margin": "14px 0"}),
                            html.P(
                                "Source: CMS Marketplace API · dbt + DuckDB",
                                style={"fontSize": "11px", "color": C["purple"], "margin": 0},
                            ),
                        ),
                        xs=12, md=3,
                    ),
                    dbc.Col(_card(_graph("map-fig")), xs=12, md=9),
                ]),

                # Q1 + Q3
                dbc.Row([
                    dbc.Col(_card(_graph("q1-fig")), xs=12, md=6),
                    dbc.Col(_card(_graph("q3-fig")), xs=12, md=6),
                ]),

                # Q4 -- full width
                dbc.Row([dbc.Col(_card(_graph("q4-fig")), width=12)]),

                # Q2 -- full width
                dbc.Row([dbc.Col(_card(_graph("q2-fig")), width=12)]),

                # Q5 -- Optimization + sessions slider
                dbc.Row([
                    dbc.Col(
                        _card(
                            dbc.Row([
                                dbc.Col(
                                    html.Label(
                                        "Expected therapy sessions / year",
                                        style={"fontWeight": 600, "fontSize": "13px", "color": C["navy"]},
                                    ),
                                    xs=12, md=4,
                                ),
                                dbc.Col(
                                    dcc.Slider(
                                        id="sessions-slider",
                                        min=4, max=52, step=4, value=12,
                                        marks={4: "4", 12: "12", 24: "24", 36: "36", 52: "52"},
                                        tooltip={"placement": "bottom", "always_visible": True},
                                    ),
                                    xs=12, md=8,
                                ),
                            ], align="center", style={"marginBottom": "8px"}),
                            _graph("q5-fig"),
                        ),
                        width=12,
                    ),
                ]),

                # Data viewer
                dbc.Row([
                    dbc.Col(
                        _card(
                            html.Div(
                                className="d-flex flex-wrap align-items-center justify-content-between gap-2 mb-2",
                                children=[
                                    html.Label(
                                        "Plan Data Explorer",
                                        style={"fontWeight": 600, "fontSize": "13px",
                                               "color": C["navy"], "margin": 0},
                                    ),
                                    dbc.Button(
                                        "Export to .csv",
                                        id="export-btn",
                                        n_clicks=0,
                                        size="sm",
                                        style={"background": C["navy"], "borderColor": C["navy"],
                                               "fontSize": "12px", "fontWeight": 600},
                                    ),
                                ],
                            ),
                            dag.AgGrid(
                                id="data-table",
                                defaultColDef={
                                    "sortable":       True,
                                    "filter":         True,
                                    "floatingFilter": True,
                                    "resizable":      True,
                                    "minWidth":       120,
                                },
                                columnSize="responsiveSizeToFit",
                                dashGridOptions={
                                    "pagination":         True,
                                    "paginationPageSize": 12,
                                    "paginationPageSizeSelector": [12, 25, 50, 100],
                                    "domLayout":          "autoHeight",
                                    "theme": {"function": GRID_THEME},
                                },
                                style={"width": "100%"},
                            ),
                        ),
                        width=12,
                    ),
                ]),

                # Footer
                html.Div(
                    f"Data models last updated: {LAST_BUILT}" if LAST_BUILT else "",
                    style={
                        "fontSize":   "11px",
                        "color":      C["subtext"],
                        "textAlign":  "center",
                        "padding":    "4px 0 20px",
                    },
                ),

            ],
        ),
    ],
)


# ── Callbacks ─────────────────────────────────────────────────────────────────

@app.callback(
    Output("about-modal", "is_open"),
    Input("about-btn", "n_clicks"),
    State("about-modal", "is_open"),
    prevent_initial_call=True,
)
def _toggle_about(_, is_open):
    return not is_open


@app.callback(
    Output("data-table", "exportDataAsCsv"),
    Output("data-table", "csvExportParams"),
    Input("export-btn", "n_clicks"),
    State("county-dd", "value"),
    prevent_initial_call=True,
)
def _export_csv(_, county):
    # AG Grid exports the rows as currently filtered and sorted in the browser
    slug = (county or "All Counties").lower().replace(" ", "_")
    return True, {"fileName": f"fl_mh_plans_{slug}.csv"}


@app.callback(
    Output("county-dd", "value"),
    Input("map-fig", "clickData"),
    prevent_initial_call=True,
)
def _map_click(click_data):
    if click_data:
        pt = click_data["points"][0]
        if "customdata" in pt:
            return pt["customdata"][0]
    return dash.no_update


@app.callback(
    Output("map-fig",   "figure"),
    Output("q1-fig",    "figure"),
    Output("q2-fig",    "figure"),
    Output("q3-fig",    "figure"),
    Output("q4-fig",    "figure"),
    Output("q5-fig",    "figure"),
    Output("data-table", "rowData"),
    Output("data-table", "columnDefs"),
    Input("county-dd",       "value"),
    Input("sessions-slider", "value"),
)
def _update(county, sessions):
    df = _filter(PLANS, county)

    display_cols = {
        "county_name":   "County",
        "plan_name":     "Plan",
        "carrier_short": "Carrier",
        "metal_level":   "Metal Tier",
        "plan_type":     "Plan Type",
        "premium":       "Premium ($)",
        "mh_copay":      "MH Copay ($)",
        "spec_copay":    "Specialist Copay ($)",
        "pc_copay":      "Primary Care Copay ($)",
        "deductible":    "Deductible ($)",
        "hsa_eligible":   "HSA Eligible",
        "global_rating":  "CMS Rating",
        "coverage_score": "Coverage Score",
    }
    tbl = df[list(display_cols)].rename(columns=display_cols).round(2)
    tbl["Metal Tier"] = tbl["Metal Tier"].astype(str)

    num_ids = {"Premium ($)", "MH Copay ($)", "Specialist Copay ($)", "Primary Care Copay ($)", "Deductible ($)", "CMS Rating", "Coverage Score"}
    columns = [
        {
            "field": c,
            "filter": "agNumberColumnFilter" if c in num_ids else "agTextColumnFilter",
            **({"minWidth": 240} if c == "Plan" else {}),
        }
        for c in tbl.columns
    ]

    return (
        fig_map(county),
        fig_q1_access(county),
        fig_q2_plan_types(county),
        fig_q3_parity(county),
        fig_q4_carrier(county),
        fig_q5_pareto(county, sessions),
        tbl.to_dict("records"),
        columns,
    )


if __name__ == "__main__":
    # Local dev server; production runs under gunicorn (see Dockerfile)
    app.run(
        host=os.environ.get("HOST", "127.0.0.1"),
        debug=True, 
        port=int(os.environ.get("PORT", 8052)),
    )

