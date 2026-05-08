"""NYC Taxi Demand — Tesla-inspired interactive Streamlit dashboard.

Layout (top to bottom):
    [HERO HEADER]  Tesla-style centered branding with context
    [METRIC STRIP] 4 animated KPI cards with hover effects
    [TAB NAV]      Map View | Analytics | AI Assistant
    [MAP + ZONES]  Interactive choropleth + ranked top zones
    [ANALYTICS]    Charts, distributions, borough breakdown
    [CHAT]         LLM agent conversation
    [FOOTER]       Clean minimal footer

Sidebar: source / month / day_of_week / hour filters with Tesla styling.
All styling lives in `styles.py`.
"""
from __future__ import annotations

import folium
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from streamlit_folium import st_folium

from data_loader import (
    day_of_week_labels,
    fetch_predictions,
    fetch_zone_24h_profile,
    fetch_zone_metadata,
    format_freshness,
    get_data_freshness,
    list_filter_options,
    load_zone_geojson,
    month_labels,
)
from styles import ACCENT, BORDER, CSS, DEMAND_COLORS, SURFACE, TEXT, TEXT_MUTED

# ── Page setup ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="NYC Taxi Demand",
    page_icon="🚕",
    layout="wide",
    initial_sidebar_state="expanded",
)
st.markdown(CSS, unsafe_allow_html=True)


# ── Sidebar filters ──────────────────────────────────────────────────────────
def render_sidebar() -> dict:
    options = list_filter_options()

    st.sidebar.markdown(
        """
        <div style="padding: 16px 0 8px 0; text-align: center;">
            <div style="font-size: 20px; font-weight: 700; letter-spacing: 0.12em;
                        text-transform: uppercase; color: #171a20; margin-bottom: 4px;">
                NYC TAXI
            </div>
            <div style="font-size: 10px; letter-spacing: 0.2em; text-transform: uppercase;
                        color: #5c5e62; font-weight: 500;">
                Demand Intelligence
            </div>
        </div>
        <hr style="border: none; border-top: 1px solid #e0e0e0; margin: 16px 0 24px 0;">
        <div style="padding: 0 0 8px 0; font-size: 10px;
            letter-spacing: 0.2em; text-transform: uppercase; color: #5c5e62;
            font-weight: 600;">
            Filters
        </div>
        """,
        unsafe_allow_html=True,
    )

    source_choice = st.sidebar.radio(
        "Data Source",
        options=("Combined", "yellow", "hvfhv"),
        index=0,
        help="Yellow = traditional taxi, HVFHV = ride-share (Uber/Lyft)",
    )
    month_lbls = month_labels()
    month = st.sidebar.selectbox(
        "Month",
        options=options["month"],
        format_func=lambda m: month_lbls.get(m, str(m)),
        index=options["month"].index(6) if 6 in options["month"] else 0,
    )
    dow_lbls = day_of_week_labels()
    day_of_week = st.sidebar.selectbox(
        "Day of Week",
        options=options["day_of_week"],
        format_func=lambda d: dow_lbls.get(d, str(d)),
        index=options["day_of_week"].index(2) if 2 in options["day_of_week"] else 0,
    )
    hour = st.sidebar.slider(
        "Hour of Day",
        min_value=min(options["hour"]),
        max_value=max(options["hour"]),
        value=18,
        step=1,
        help="Slide to see demand predictions for different hours",
    )

    # Time display in sidebar
    hour_display = f"{hour:02d}:00"
    period = "AM" if hour < 12 else "PM"
    display_hour = hour if hour <= 12 else hour - 12
    if display_hour == 0:
        display_hour = 12
    st.sidebar.markdown(
        f"""
        <div style="text-align: center; margin: 16px 0 8px 0; padding: 16px;
                    background: #ffffff; border: 1px solid #e0e0e0; border-radius: 8px;">
            <div style="font-size: 11px; color: #5c5e62; letter-spacing: 0.12em;
                        text-transform: uppercase; font-weight: 600; margin-bottom: 8px;">
                Selected Time
            </div>
            <div style="font-size: 32px; font-weight: 300; color: #171a20;
                        letter-spacing: -0.02em;">
                {display_hour}:00 <span style="font-size: 14px;
                    color: #5c5e62;">{period}</span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    return {
        "source_name": None if source_choice == "Combined" else source_choice,
        "month": int(month),
        "day_of_week": int(day_of_week),
        "hour": int(hour),
    }


# ── Hero Header ──────────────────────────────────────────────────────────────
def render_header(context: str, freshness_label: str = "") -> None:
    freshness_html = (
        f'<div style="font-size:11px;color:{TEXT_MUTED};letter-spacing:0.08em;'
        f'text-transform:uppercase;margin-top:8px;">{freshness_label}</div>'
        if freshness_label
        else ""
    )
    st.markdown(
        f"""
        <div class="tp-hero tp-animate">
            <div class="tp-hero-title">NYC Taxi Demand</div>
            <div class="tp-hero-sub">{context}</div>
            {freshness_html}
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_setup_screen(error_message: str) -> None:
    """Friendly first-run screen when output/predictions.duckdb is missing."""
    st.markdown(
        f"""
        <div class="tp-hero tp-animate">
            <div class="tp-hero-title">NYC Taxi Demand</div>
            <div class="tp-hero-sub">Setup required</div>
        </div>
        <div style="max-width:680px;margin:24px auto;padding:24px;
                    background:{SURFACE};border:1px solid {BORDER};border-radius:8px;
                    color:{TEXT};font-size:14px;line-height:1.6;">
            <p style="margin-top:0;"><b>Predictions database not found.</b></p>
            <p style="color:{TEXT_MUTED};">{error_message}</p>
            <p>To fix this, run from the project root:</p>
            <pre style="background:#fff;border:1px solid {BORDER};padding:14px;
                        border-radius:4px;font-size:12px;overflow-x:auto;">
mkdir -p .tmp/parquet_extract
unzip -o output/taxi_demand_ml_parquet.zip -d .tmp/parquet_extract/
python streamlit_app/predictions.py</pre>
            <p style="color:{TEXT_MUTED};margin-bottom:0;">
                Then refresh this page.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )


# ── KPI metric cards ─────────────────────────────────────────────────────────
def _metric_card(
    label: str, value: str, sub: str = "", unit: str = "", delay: int = 1
) -> str:
    unit_html = f'<span class="tp-metric-unit">{unit}</span>' if unit else ""
    sub_html = f'<div class="tp-metric-sub">{sub}</div>' if sub else ""
    return f"""
        <div class="tp-metric tp-animate tp-animate-delay-{delay}">
            <div class="tp-metric-label">{label}</div>
            <div class="tp-metric-value">{value}{unit_html}</div>
            {sub_html}
        </div>
    """


def render_metrics(predictions: pd.DataFrame, source_label: str) -> None:
    if predictions.empty:
        return
    total_zones = predictions["PULocationID"].nunique()
    peak = predictions["predicted_trip_count"].max()
    avg = predictions["predicted_trip_count"].mean()
    total_trips = predictions["predicted_trip_count"].sum()
    top_borough = (
        predictions.groupby("borough")["predicted_trip_count"].sum().idxmax()
        if "borough" in predictions.columns
        else "—"
    )

    cards = [
        _metric_card(
            "Active Zones",
            f"{total_zones}",
            f"of 263 total NYC pickup zones",
            delay=1,
        ),
        _metric_card(
            "Peak Demand",
            f"{peak:,.0f}",
            "trips in the busiest zone",
            unit="trips",
            delay=2,
        ),
        _metric_card(
            "Total Predicted",
            f"{total_trips:,.0f}",
            "total trips this time slice",
            unit="trips",
            delay=3,
        ),
        _metric_card(
            "Top Borough",
            top_borough.upper() if isinstance(top_borough, str) else "—",
            f"highest aggregate predicted demand",
            delay=4,
        ),
    ]
    cols = st.columns(4, gap="small")
    for col, html in zip(cols, cards):
        with col:
            st.markdown(html, unsafe_allow_html=True)


# ── Map ──────────────────────────────────────────────────────────────────────
def _bucket(value: float, q1: float, q2: float, q3: float) -> str:
    if value <= q1:
        return "low"
    if value <= q2:
        return "medium"
    if value <= q3:
        return "high"
    return "very_high"


def render_map(predictions: pd.DataFrame) -> dict | None:
    """Render the choropleth and return st_folium's interaction state.

    The return value lets the caller react to clicks (drill-down panel).
    Returns None if predictions are empty.
    """
    geojson = load_zone_geojson()
    pred_by_zone = predictions.set_index("PULocationID")[
        "predicted_trip_count"
    ].to_dict()

    if predictions.empty:
        st.info("No predictions for this slice.")
        return None
    q1, q2, q3 = (
        predictions["predicted_trip_count"].quantile([0.25, 0.5, 0.75]).tolist()
    )

    m = folium.Map(
        location=[40.7549, -73.9840],
        zoom_start=11,
        tiles="cartodbpositron",
        zoom_control=True,
        attribution_control=False,
    )

    def style_fn(feature):
        loc = feature["properties"].get("LocationID")
        v = pred_by_zone.get(loc)
        if v is None:
            return {
                "fillColor": "#f0f0f0",
                "color": BORDER,
                "weight": 0.5,
                "fillOpacity": 0.3,
            }
        return {
            "fillColor": DEMAND_COLORS[_bucket(v, q1, q2, q3)],
            "color": "#ffffff",
            "weight": 0.8,
            "fillOpacity": 0.82,
        }

    def highlight_fn(feature):
        return {
            "weight": 2.5,
            "color": ACCENT,
            "fillOpacity": 0.92,
        }

    # Build tooltip with zone-level prediction data
    # Add predicted_trip_count to geojson properties for the tooltip
    zone_data = {}
    for _, row in predictions.iterrows():
        zone_data[row["PULocationID"]] = int(row["predicted_trip_count"])

    for feature in geojson.get("features", []):
        loc_id = feature["properties"].get("LocationID")
        feature["properties"]["predicted_trips"] = zone_data.get(loc_id, 0)

    folium.GeoJson(
        geojson,
        style_function=style_fn,
        highlight_function=highlight_fn,
        tooltip=folium.GeoJsonTooltip(
            fields=["zone", "borough", "LocationID", "predicted_trips"],
            aliases=["Zone", "Borough", "ID", "Predicted Trips"],
            sticky=True,
            style=(
                "background-color: #ffffff; color: #171a20; "
                "border: 1px solid #e0e0e0; padding: 10px 14px; "
                "font-family: Inter, sans-serif; font-size: 12px; "
                "letter-spacing: 0.01em; border-radius: 4px; "
                "box-shadow: 0 2px 8px rgba(0,0,0,0.08);"
            ),
        ),
    ).add_to(m)

    # `returned_objects` controls what triggers a Streamlit rerun when the
    # user interacts with the map. We watch last_active_drawing so a zone
    # click triggers a rerun and the drill-down panel updates.
    return st_folium(
        m,
        width=None,
        height=560,
        returned_objects=["last_active_drawing"],
        key="zone_map",
    )


def render_zone_drilldown(map_state: dict | None, selectors: dict) -> None:
    """24-hour profile for the zone the user clicked on the map."""
    if not map_state or not map_state.get("last_active_drawing"):
        st.markdown(
            f'<div style="color:{TEXT_MUTED}; font-size:12px; padding: 12px 0;">'
            "Click any zone on the map for its 24-hour demand profile."
            "</div>",
            unsafe_allow_html=True,
        )
        return

    props = (map_state["last_active_drawing"] or {}).get("properties", {}) or {}
    loc_id = props.get("LocationID")
    if loc_id is None:
        return

    meta = fetch_zone_metadata(int(loc_id)) or {"zone": "Unknown", "borough": ""}
    profile = fetch_zone_24h_profile(
        location_id=int(loc_id),
        source_name=selectors["source_name"],
        month=selectors["month"],
        day_of_week=selectors["day_of_week"],
    )
    if profile.empty:
        st.info(f"No 24-hour profile for {meta['zone']}.")
        return

    selected_hour = selectors["hour"]
    bar_colors = [
        ACCENT if int(h) == selected_hour else "#d4d4d4"
        for h in profile["pickup_hour"]
    ]

    st.markdown(
        f"""
        <div style="display:flex;align-items:baseline;justify-content:space-between;
                    margin: 12px 0 4px 0;">
            <div>
                <div style="font-size:18px;font-weight:500;color:{TEXT};">{meta['zone']}</div>
                <div style="font-size:11px;color:{TEXT_MUTED};letter-spacing:0.06em;
                            text-transform:uppercase;margin-top:2px;">
                    {meta['borough']} · ZONE {loc_id}
                </div>
            </div>
            <div style="font-size:11px;color:{TEXT_MUTED};letter-spacing:0.12em;
                        text-transform:uppercase;">24-hour profile</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    fig = go.Figure(
        go.Bar(
            x=profile["pickup_hour"],
            y=profile["predicted_trip_count"],
            marker=dict(color=bar_colors, line=dict(color="#ffffff", width=1)),
            hovertemplate="Hour %{x:02d}:00<br>%{y:,.0f} trips<extra></extra>",
        )
    )
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter, sans-serif", size=11, color=TEXT),
        margin=dict(l=30, r=10, t=10, b=30),
        height=200,
        showlegend=False,
        xaxis=dict(
            tickmode="array",
            tickvals=[0, 6, 12, 18, 23],
            ticktext=["00", "06", "12", "18", "23"],
            gridcolor="#f0f0f0",
        ),
        yaxis=dict(gridcolor="#f0f0f0", title=""),
    )
    st.plotly_chart(fig, use_container_width=True)


def render_legend() -> None:
    swatches = [
        (DEMAND_COLORS["low"], "Low"),
        (DEMAND_COLORS["medium"], "Medium"),
        (DEMAND_COLORS["high"], "High"),
        (DEMAND_COLORS["very_high"], "Very High"),
    ]
    items = "".join(
        f'<span><span class="tp-legend-swatch" style="background:{c}"></span>{lbl}</span>'
        for c, lbl in swatches
    )
    st.markdown(f'<div class="tp-legend">{items}</div>', unsafe_allow_html=True)


# ── Top zones panel ──────────────────────────────────────────────────────────
def render_top_zones(predictions: pd.DataFrame, n: int = 10) -> None:
    st.markdown(
        '<div class="tp-section">Top Demand Zones</div>', unsafe_allow_html=True
    )
    if predictions.empty:
        st.markdown(
            f'<div style="color:{TEXT_MUTED};font-size:13px;">No data for this slice.</div>',
            unsafe_allow_html=True,
        )
        return

    top = predictions.head(n).copy()
    top["predicted_trip_count"] = top["predicted_trip_count"].round(0).astype(int)
    max_v = top["predicted_trip_count"].max() or 1

    rows_html = []
    for rank, (_, row) in enumerate(top.iterrows(), 1):
        is_peak = row["predicted_trip_count"] >= 0.85 * max_v
        accent_cls = " tp-zone-value-accent" if is_peak else ""
        rank_cls = "tp-zone-rank-top" if rank <= 3 else "tp-zone-rank-normal"
        zone = row.get("zone", "Unknown")
        borough = row.get("borough", "")
        pct = (row["predicted_trip_count"] / max_v) * 100

        # Color for the bar fill
        if is_peak:
            bar_color = ACCENT
        elif pct > 60:
            bar_color = DEMAND_COLORS["high"]
        elif pct > 30:
            bar_color = DEMAND_COLORS["medium"]
        else:
            bar_color = DEMAND_COLORS["low"]

        rows_html.append(
            f"""
            <div class="tp-zone-row">
                <div style="display: flex; align-items: center;">
                    <span class="tp-zone-rank {rank_cls}">{rank}</span>
                    <div>
                        <div class="tp-zone-name">{zone}</div>
                        <div class="tp-zone-borough">{borough}</div>
                        <div class="tp-bar-bg">
                            <div class="tp-bar-fill" style="width: {pct:.0f}%; background: {bar_color};"></div>
                        </div>
                    </div>
                </div>
                <div class="tp-zone-value{accent_cls}">{row['predicted_trip_count']:,}</div>
            </div>
        """
        )
    st.markdown("".join(rows_html), unsafe_allow_html=True)


# ── Analytics panel ──────────────────────────────────────────────────────────
def render_analytics(predictions: pd.DataFrame, selectors: dict) -> None:
    if predictions.empty:
        st.info("No prediction data available for the selected filters.")
        return

    # Plotly theme consistent with Tesla white design
    plotly_layout = dict(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter, sans-serif", size=12, color=TEXT),
        margin=dict(l=40, r=20, t=40, b=40),
        hoverlabel=dict(
            bgcolor="#ffffff",
            bordercolor=BORDER,
            font=dict(family="Inter, sans-serif", size=12, color=TEXT),
        ),
    )

    col1, col2 = st.columns(2, gap="large")

    with col1:
        st.markdown(
            '<div class="tp-section">Demand Distribution</div>',
            unsafe_allow_html=True,
        )
        fig_hist = px.histogram(
            predictions,
            x="predicted_trip_count",
            nbins=30,
            color_discrete_sequence=[ACCENT],
        )
        fig_hist.update_layout(
            **plotly_layout,
            xaxis_title="Predicted Trip Count",
            yaxis_title="Number of Zones",
            showlegend=False,
            xaxis=dict(gridcolor="#f0f0f0", zerolinecolor="#e0e0e0"),
            yaxis=dict(gridcolor="#f0f0f0", zerolinecolor="#e0e0e0"),
        )
        fig_hist.update_traces(
            marker=dict(line=dict(color="#ffffff", width=1)),
            hovertemplate="Trips: %{x:.0f}<br>Zones: %{y}<extra></extra>",
        )
        st.plotly_chart(fig_hist, width="stretch")

    with col2:
        st.markdown(
            '<div class="tp-section">Borough Breakdown</div>',
            unsafe_allow_html=True,
        )
        if "borough" in predictions.columns:
            borough_agg = (
                predictions.groupby("borough")["predicted_trip_count"]
                .sum()
                .reset_index()
                .sort_values("predicted_trip_count", ascending=True)
            )
            colors = [
                "#fcd5d5",
                "#f5a3a3",
                "#e8734a",
                "#e31937",
                "#c41230",
                "#8b0d22",
                "#5c5e62",
                "#ababab",
            ]
            fig_bar = go.Figure(
                go.Bar(
                    x=borough_agg["predicted_trip_count"],
                    y=borough_agg["borough"],
                    orientation="h",
                    marker=dict(
                        color=colors[: len(borough_agg)],
                        line=dict(color="#ffffff", width=1),
                    ),
                    hovertemplate="%{y}<br>Predicted: %{x:,.0f} trips<extra></extra>",
                )
            )
            fig_bar.update_layout(
                **plotly_layout,
                xaxis_title="Total Predicted Trips",
                yaxis_title="",
                showlegend=False,
                xaxis=dict(gridcolor="#f0f0f0", zerolinecolor="#e0e0e0"),
                yaxis=dict(gridcolor="#f0f0f0", zerolinecolor="#e0e0e0"),
            )
            st.plotly_chart(fig_bar, width="stretch")

    # Second row of charts
    col3, col4 = st.columns(2, gap="large")

    with col3:
        st.markdown(
            '<div class="tp-section">Top 15 Zones</div>',
            unsafe_allow_html=True,
        )
        top15 = predictions.head(15).copy()
        top15["zone_label"] = top15["zone"].str[:20]
        fig_top = go.Figure(
            go.Bar(
                x=top15["predicted_trip_count"],
                y=top15["zone_label"],
                orientation="h",
                marker=dict(
                    color=top15["predicted_trip_count"],
                    colorscale=[[0, "#f5c06b"], [0.5, "#e8734a"], [1, "#e31937"]],
                    line=dict(color="#ffffff", width=1),
                ),
                hovertemplate="%{y}<br>%{x:,.0f} trips<extra></extra>",
            )
        )
        fig_top.update_layout(
            **plotly_layout,
            xaxis_title="Predicted Trips",
            yaxis_title="",
            showlegend=False,
            yaxis=dict(autorange="reversed", gridcolor="#f0f0f0"),
            xaxis=dict(gridcolor="#f0f0f0"),
            height=420,
        )
        st.plotly_chart(fig_top, width="stretch")

    with col4:
        st.markdown(
            '<div class="tp-section">Demand Levels</div>',
            unsafe_allow_html=True,
        )
        if "demand_level" in predictions.columns:
            demand_counts = (
                predictions.groupby("demand_level")["predicted_trip_count"]
                .count()
                .reset_index()
            )
            demand_counts.columns = ["Demand Level", "Zone Count"]
            level_colors = {
                "low": DEMAND_COLORS["low"],
                "medium": DEMAND_COLORS["medium"],
                "high": DEMAND_COLORS["high"],
                "very_high": DEMAND_COLORS["very_high"],
            }
            fig_pie = go.Figure(
                go.Pie(
                    labels=demand_counts["Demand Level"],
                    values=demand_counts["Zone Count"],
                    marker=dict(
                        colors=[
                            level_colors.get(l, "#e0e0e0")
                            for l in demand_counts["Demand Level"]
                        ],
                        line=dict(color="#ffffff", width=2),
                    ),
                    hole=0.55,
                    textinfo="label+percent",
                    textfont=dict(size=11),
                    hovertemplate="%{label}<br>%{value} zones (%{percent})<extra></extra>",
                )
            )
            fig_pie.update_layout(
                **plotly_layout,
                showlegend=False,
                height=420,
            )
            st.plotly_chart(fig_pie, width="stretch")
        else:
            # Fallback: compute demand levels on the fly
            q1, q2, q3 = predictions["predicted_trip_count"].quantile(
                [0.25, 0.5, 0.75]
            )
            predictions_copy = predictions.copy()
            predictions_copy["demand_level"] = predictions_copy[
                "predicted_trip_count"
            ].apply(lambda v: _bucket(v, q1, q2, q3))
            demand_counts = (
                predictions_copy.groupby("demand_level")["predicted_trip_count"]
                .count()
                .reset_index()
            )
            demand_counts.columns = ["Demand Level", "Zone Count"]
            level_colors = {
                "low": DEMAND_COLORS["low"],
                "medium": DEMAND_COLORS["medium"],
                "high": DEMAND_COLORS["high"],
                "very_high": DEMAND_COLORS["very_high"],
            }
            fig_pie = go.Figure(
                go.Pie(
                    labels=demand_counts["Demand Level"],
                    values=demand_counts["Zone Count"],
                    marker=dict(
                        colors=[
                            level_colors.get(l, "#e0e0e0")
                            for l in demand_counts["Demand Level"]
                        ],
                        line=dict(color="#ffffff", width=2),
                    ),
                    hole=0.55,
                    textinfo="label+percent",
                    textfont=dict(size=11),
                    hovertemplate="%{label}<br>%{value} zones (%{percent})<extra></extra>",
                )
            )
            fig_pie.update_layout(
                **plotly_layout,
                showlegend=False,
                height=420,
            )
            st.plotly_chart(fig_pie, width="stretch")


# ── LLM panel ────────────────────────────────────────────────────────────────
def render_llm_panel() -> None:
    st.markdown(
        '<div class="tp-section" style="margin-bottom:16px;">AI Assistant</div>',
        unsafe_allow_html=True,
    )
    st.markdown(
        f"""
        <div style="color:{TEXT_MUTED}; font-size:13px; margin-bottom:16px;
                    padding: 12px 16px; background: {SURFACE}; border-radius: 8px;
                    border: 1px solid {BORDER};">
            Ask natural-language questions about NYC taxi demand predictions.
            Powered by DuckDB + LLM.
        </div>
        """,
        unsafe_allow_html=True,
    )

    if "chat_history" not in st.session_state:
        st.session_state.chat_history = []

    for entry in st.session_state.chat_history:
        with st.chat_message(entry["role"]):
            st.markdown(entry["content"])

    user_q = st.chat_input("Ask about taxi demand... e.g. 'Top 5 zones in Manhattan at 6pm'")
    if not user_q:
        return

    st.session_state.chat_history.append({"role": "user", "content": user_q})
    with st.chat_message("user"):
        st.markdown(user_q)

    with st.chat_message("assistant"):
        with st.spinner("Analyzing..."):
            try:
                from llm_agent import ask

                # Pass prior turns so follow-ups like "now Brooklyn" make sense.
                # Exclude the just-appended user message — the agent re-adds it.
                prior_history = st.session_state.chat_history[:-1]
                answer, trace = ask(user_q, history=prior_history)
            except RuntimeError as exc:
                answer, trace = f"⚠ {exc}", []
            except Exception as exc:
                answer, trace = f"⚠ Agent error: {exc}", []
        st.markdown(answer)
        sql_steps = [t for t in trace if t["role"] == "tool"]
        if sql_steps:
            with st.expander("🔍 Query trace"):
                for step in sql_steps:
                    st.code(step["content"]["sql"], language="sql")
    st.session_state.chat_history.append({"role": "assistant", "content": answer})


# ── Data Explorer ────────────────────────────────────────────────────────────
def render_data_explorer(predictions: pd.DataFrame) -> None:
    st.markdown(
        '<div class="tp-section">Data Explorer</div>',
        unsafe_allow_html=True,
    )

    if predictions.empty:
        st.info("No data for the selected filters.")
        return

    # Show/hide toggle
    col1, col2 = st.columns([3, 1])
    with col1:
        search_term = st.text_input(
            "🔍 Search zones",
            placeholder="Type a zone or borough name...",
            label_visibility="collapsed",
        )
    with col2:
        sort_order = st.selectbox(
            "Sort",
            options=["Highest Demand", "Lowest Demand", "Zone Name"],
            label_visibility="collapsed",
        )

    filtered = predictions.copy()
    if search_term:
        mask = (
            filtered["zone"].str.contains(search_term, case=False, na=False)
            | filtered["borough"].str.contains(search_term, case=False, na=False)
        )
        filtered = filtered[mask]

    if sort_order == "Lowest Demand":
        filtered = filtered.sort_values("predicted_trip_count", ascending=True)
    elif sort_order == "Zone Name":
        filtered = filtered.sort_values("zone", ascending=True)
    # Default is already sorted by highest demand

    display_cols = [
        c
        for c in ["zone", "borough", "PULocationID", "predicted_trip_count"]
        if c in filtered.columns
    ]
    if "demand_level" in filtered.columns:
        display_cols.append("demand_level")

    display_df = filtered[display_cols].copy()
    display_df.columns = [
        c.replace("_", " ").title() for c in display_df.columns
    ]
    if "Predicted Trip Count" in display_df.columns:
        display_df["Predicted Trip Count"] = display_df[
            "Predicted Trip Count"
        ].round(0).astype(int)

    st.dataframe(
        display_df,
        width="stretch",
        height=400,
        hide_index=True,
    )

    st.markdown(
        f'<div style="font-size:12px; color:{TEXT_MUTED}; margin-top:8px;">'
        f"Showing {len(filtered)} of {len(predictions)} zones</div>",
        unsafe_allow_html=True,
    )


# ── Footer ───────────────────────────────────────────────────────────────────
def render_footer() -> None:
    st.markdown(
        """
        <div class="tp-footer">
            NYC Taxi Demand Prediction Dashboard &nbsp;·&nbsp;
            Built with <a href="https://streamlit.io" target="_blank">Streamlit</a>
            &nbsp;·&nbsp; Data228 Group Project
        </div>
        """,
        unsafe_allow_html=True,
    )


# ── Main ─────────────────────────────────────────────────────────────────────
def main() -> None:
    # Friendly first-run screen if the DuckDB file hasn't been built yet.
    try:
        freshness = get_data_freshness()
    except FileNotFoundError as exc:
        render_setup_screen(str(exc))
        return

    selectors = render_sidebar()
    predictions = fetch_predictions(**selectors)

    src = selectors["source_name"] or "Combined"
    dow = day_of_week_labels()[selectors["day_of_week"]]
    mon = month_labels()[selectors["month"]]
    context = f"{src} · {mon} · {dow} · {selectors['hour']:02d}:00"

    render_header(context, format_freshness(freshness))
    render_metrics(predictions, src)

    st.write("")  # vertical breath

    # Tab navigation — Tesla style
    tab_map, tab_analytics, tab_explorer, tab_ai = st.tabs(
        ["🗺️  Map View", "📊  Analytics", "🔎  Data Explorer", "🤖  AI Assistant"]
    )

    with tab_map:
        map_col, side_col = st.columns([2.6, 1], gap="large")
        with map_col:
            st.markdown(
                '<div class="tp-section">Demand Heatmap</div>',
                unsafe_allow_html=True,
            )
            map_state = render_map(predictions)
            render_legend()
            # Drill-down panel: 24-hour profile for the clicked zone.
            render_zone_drilldown(map_state, selectors)
        with side_col:
            render_top_zones(predictions)

    with tab_analytics:
        render_analytics(predictions, selectors)

    with tab_explorer:
        render_data_explorer(predictions)

    with tab_ai:
        render_llm_panel()

    render_footer()


if __name__ == "__main__":
    main()
