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
    fetch_reliability,
    fetch_zone_24h_profile,
    fetch_zone_metadata,
    format_freshness,
    get_data_freshness,
    list_filter_options,
    load_zone_geojson,
    month_labels,
)
from scoring import DEFAULT_WEIGHTS, Weights, annotate_recommendations
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

    source_choice = "Combined"
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

    if "chat_history" not in st.session_state:
        st.session_state.chat_history = []

    # Show welcome card only when the conversation is empty
    if not st.session_state.chat_history:
        st.markdown(
            f"""
            <div style="text-align:center; padding: 40px 20px 24px 20px; margin-bottom: 16px;">
                <div style="font-size: 40px; margin-bottom: 12px;">🚕</div>
                <div style="font-size: 18px; font-weight: 500; color: {TEXT};
                            margin-bottom: 6px;">What would you like to know?</div>
                <div style="font-size: 13px; color: {TEXT_MUTED}; max-width: 420px;
                            margin: 0 auto;">
                    Ask about NYC taxi demand — zones, boroughs, peak hours,
                    trends, or driver recommendations.
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        # Suggestion chips
        suggestions = [
            "Top 5 busiest zones in Manhattan at 6pm",
            "Where should I drive on a Friday night?",
            "Compare yellow taxi vs Uber in Brooklyn",
            "Which zones are growing fastest?",
        ]
        chip_cols = st.columns(len(suggestions))
        for col, suggestion in zip(chip_cols, suggestions):
            with col:
                if st.button(
                    suggestion,
                    key=f"suggest_{suggestion[:20]}",
                    use_container_width=True,
                ):
                    st.session_state.chat_history.append(
                        {"role": "user", "content": suggestion}
                    )
                    st.rerun()

    # Render conversation history
    for entry in st.session_state.chat_history:
        avatar = "👤" if entry["role"] == "user" else "🚕"
        with st.chat_message(entry["role"], avatar=avatar):
            st.markdown(entry["content"])

    # If the last message is from the user and has no assistant reply yet,
    # generate the response now (handles suggestion chip clicks)
    needs_response = (
        st.session_state.chat_history
        and st.session_state.chat_history[-1]["role"] == "user"
        and (
            len(st.session_state.chat_history) < 2
            or st.session_state.chat_history[-2]["role"] != "assistant"
            or st.session_state.chat_history[-1] != st.session_state.chat_history[-2]
        )
    )

    # Check if we truly need a response (no assistant reply follows the last user msg)
    if needs_response:
        last_user_idx = len(st.session_state.chat_history) - 1
        has_reply = (
            last_user_idx + 1 < len(st.session_state.chat_history)
            and st.session_state.chat_history[last_user_idx + 1]["role"] == "assistant"
        )
        if not has_reply:
            _generate_response(st.session_state.chat_history[-1]["content"])

    # Chat input
    user_q = st.chat_input(
        "Ask about taxi demand...",
        key="ai_chat_input",
    )
    if user_q:
        st.session_state.chat_history.append({"role": "user", "content": user_q})
        with st.chat_message("user", avatar="👤"):
            st.markdown(user_q)
        _generate_response(user_q)


def _generate_response(user_q: str) -> None:
    """Generate and display an AI assistant response — no logs, just the answer."""
    with st.chat_message("assistant", avatar="🚕"):
        with st.spinner("Thinking..."):
            try:
                from llm_agent import ask

                prior_history = st.session_state.chat_history[:-1]
                answer, _ = ask(user_q, history=prior_history)
            except RuntimeError as exc:
                err_msg = str(exc).lower()
                if "api_key" in err_msg or "not set" in err_msg:
                    answer = (
                        "The AI assistant isn't configured yet. "
                        "Please set your **ANTHROPIC_API_KEY** environment variable "
                        "or switch to a local Ollama model."
                    )
                elif "not installed" in err_msg:
                    answer = (
                        "A required library is missing. "
                        "Please run `pip install -r streamlit_app/requirements.txt`."
                    )
                else:
                    answer = (
                        "Something went wrong processing your request. "
                        "Please try rephrasing your question."
                    )
            except Exception:
                answer = (
                    "Sorry, I couldn't process that request. "
                    "Please try a different question."
                )
        st.markdown(answer)
    st.session_state.chat_history.append({"role": "assistant", "content": answer})




# ── Recommender (4-factor reliability) ──────────────────────────────────────
RECOMMENDER_BAND_COLORS = {
    "top_pick": ACCENT,
    "good":     "#e8734a",
    "okay":     "#f5c06b",
    "avoid":    "#e8e8e8",
}


def render_recommender_weights() -> Weights:
    """Driver-preference sliders. Returns the chosen weights (auto-normalized)."""
    st.markdown(
        '<div class="tp-section">Driver Preferences</div>',
        unsafe_allow_html=True,
    )
    st.markdown(
        f'<div style="color:{TEXT_MUTED};font-size:12px;margin-bottom:12px;">'
        "Adjust how much each factor matters to you. Weights are normalized."
        "</div>",
        unsafe_allow_html=True,
    )
    cols = st.columns(4, gap="medium")
    with cols[0]:
        demand = st.slider("Demand", 0.0, 1.0, DEFAULT_WEIGHTS.demand, 0.05,
                           help="How busy the zone is on average.")
    with cols[1]:
        reliability = st.slider("Reliability", 0.0, 1.0, DEFAULT_WEIGHTS.reliability, 0.05,
                                help="Demand consistency (low coefficient of variation).")
    with cols[2]:
        trend = st.slider("Growth", 0.0, 1.0, DEFAULT_WEIGHTS.trend, 0.05,
                          help="Multi-year demand trend (2023→2025 slope).")
    with cols[3]:
        ys = st.slider("Yellow Edge", 0.0, 1.0, DEFAULT_WEIGHTS.yellow_share, 0.05,
                       help="Strength of yellow taxi position vs HVFHV.")
    return Weights(demand=demand, reliability=reliability,
                   trend=trend, yellow_share=ys)


def render_recommender_map(scored: pd.DataFrame) -> dict | None:
    """Choropleth colored by recommendation_band (not raw demand)."""
    geojson = load_zone_geojson()
    if scored.empty:
        st.info("No reliability data for this slice.")
        return None

    band_by_zone = scored.set_index("PULocationID")["recommendation_band"].to_dict()
    score_by_zone = scored.set_index("PULocationID")["opportunity_score"].to_dict()

    m = folium.Map(
        location=[40.7549, -73.9840],
        zoom_start=11,
        tiles="cartodbpositron",
        zoom_control=True,
        attribution_control=False,
    )

    def style_fn(feature):
        loc = feature["properties"].get("LocationID")
        band = band_by_zone.get(loc)
        if band is None:
            return {"fillColor": "#f0f0f0", "color": BORDER, "weight": 0.5,
                    "fillOpacity": 0.3}
        return {
            "fillColor": RECOMMENDER_BAND_COLORS[band],
            "color": "#ffffff", "weight": 0.8, "fillOpacity": 0.82,
        }

    def highlight_fn(feature):
        return {"weight": 2.5, "color": ACCENT, "fillOpacity": 0.92}

    # Inject opportunity_score into properties for the tooltip
    for feature in geojson.get("features", []):
        loc_id = feature["properties"].get("LocationID")
        feature["properties"]["opportunity_score"] = round(
            float(score_by_zone.get(loc_id, 0)), 3
        )
        feature["properties"]["band"] = band_by_zone.get(loc_id, "—")

    folium.GeoJson(
        geojson,
        style_function=style_fn,
        highlight_function=highlight_fn,
        tooltip=folium.GeoJsonTooltip(
            fields=["zone", "borough", "opportunity_score", "band"],
            aliases=["Zone", "Borough", "Score", "Recommendation"],
            sticky=True,
            style=(
                "background-color: #ffffff; color: #171a20; "
                "border: 1px solid #e0e0e0; padding: 10px 14px; "
                "font-family: Inter, sans-serif; font-size: 12px; "
                "border-radius: 4px; box-shadow: 0 2px 8px rgba(0,0,0,0.08);"
            ),
        ),
    ).add_to(m)

    return st_folium(
        m, width=None, height=560,
        returned_objects=["last_active_drawing"],
        key="reco_map",
    )


def render_zone_radar(map_state: dict | None, scored: pd.DataFrame) -> None:
    """Radar chart showing the 4 sub-scores for the clicked zone."""
    if not map_state or not map_state.get("last_active_drawing"):
        st.markdown(
            f'<div style="color:{TEXT_MUTED};font-size:12px;padding:12px 0;">'
            "Click any zone on the map for its 4-factor breakdown."
            "</div>",
            unsafe_allow_html=True,
        )
        return

    props = (map_state["last_active_drawing"] or {}).get("properties", {}) or {}
    loc_id = props.get("LocationID")
    if loc_id is None:
        return

    row = scored[scored["PULocationID"] == int(loc_id)]
    if row.empty:
        st.info("No reliability data for that zone.")
        return
    r = row.iloc[0]

    categories = ["Demand", "Reliability", "Growth", "Yellow Edge"]
    values = [
        float(r.get("demand_score", 0)),
        float(r.get("reliability_score", 0)),
        float(r.get("trend_score", 0)),
        float(r.get("yellow_share_score", 0)),
    ]
    values_closed = values + values[:1]
    cats_closed = categories + categories[:1]

    fig = go.Figure()
    fig.add_trace(go.Scatterpolar(
        r=values_closed, theta=cats_closed, fill="toself",
        line=dict(color=ACCENT, width=2),
        fillcolor=f"rgba(227, 25, 55, 0.18)",
        hovertemplate="%{theta}: %{r:.2f}<extra></extra>",
    ))
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter, sans-serif", size=11, color=TEXT),
        polar=dict(
            bgcolor="rgba(0,0,0,0)",
            radialaxis=dict(visible=True, range=[0, 1], gridcolor="#e0e0e0",
                            tickfont=dict(size=9, color=TEXT_MUTED)),
            angularaxis=dict(gridcolor="#e0e0e0",
                             tickfont=dict(size=11, color=TEXT)),
        ),
        showlegend=False,
        margin=dict(l=40, r=40, t=20, b=20),
        height=320,
    )

    st.markdown(
        f"""
        <div style="display:flex;align-items:baseline;justify-content:space-between;
                    margin: 8px 0 4px 0;">
            <div>
                <div style="font-size:18px;font-weight:500;color:{TEXT};">{r['zone']}</div>
                <div style="font-size:11px;color:{TEXT_MUTED};letter-spacing:0.06em;
                            text-transform:uppercase;margin-top:2px;">
                    {r['borough']} · ZONE {loc_id} · {r['recommendation_band'].upper().replace('_', ' ')}
                </div>
            </div>
            <div style="font-size:28px;font-weight:300;color:{TEXT};letter-spacing:-0.02em;">
                {r['opportunity_score']:.2f}
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.plotly_chart(fig, use_container_width=True)

    # Raw values panel
    raw = pd.DataFrame({
        "Factor": ["Mean demand", "Volatility (CV)", "Trend slope", "Yellow share"],
        "Value": [
            f"{r['mean_demand']:.0f} trips/hr",
            f"{r['demand_cv']:.2%}",
            f"{r['trend_slope']:+.1f} trips/year",
            f"{r['yellow_share']:.0%}" if pd.notna(r['yellow_share']) else "—",
        ],
    })
    st.dataframe(raw, use_container_width=True, hide_index=True)


def render_recommender_top(scored: pd.DataFrame, n: int = 10) -> None:
    """Top N recommended zones for the current weights."""
    st.markdown(
        '<div class="tp-section">Top Recommendations</div>',
        unsafe_allow_html=True,
    )
    if scored.empty:
        st.markdown(
            f'<div style="color:{TEXT_MUTED};font-size:13px;">No data.</div>',
            unsafe_allow_html=True,
        )
        return

    top = scored.sort_values("opportunity_score", ascending=False).head(n)
    rows_html = []
    for rank, (_, row) in enumerate(top.iterrows(), 1):
        band = row["recommendation_band"]
        band_color = RECOMMENDER_BAND_COLORS.get(band, "#888")
        rows_html.append(f"""
            <div class="tp-zone-row">
                <div style="display: flex; align-items: center;">
                    <span class="tp-zone-rank tp-zone-rank-normal">{rank}</span>
                    <div>
                        <div class="tp-zone-name">{row['zone']}</div>
                        <div class="tp-zone-borough">{row['borough']} ·
                            <span style="color:{band_color};font-weight:500;">
                                {band.replace('_', ' ').upper()}
                            </span>
                        </div>
                    </div>
                </div>
                <div class="tp-zone-value">{row['opportunity_score']:.2f}</div>
            </div>
        """)
    st.markdown("".join(rows_html), unsafe_allow_html=True)


def render_recommender(selectors: dict) -> None:
    """The whole Reliable Income Recommender tab."""
    weights = render_recommender_weights()

    raw = fetch_reliability(**selectors)
    if raw.empty:
        st.info("No reliability data for this filter combination yet. "
                "Try a more common time slice (e.g. weekday 18:00).")
        return

    scored = annotate_recommendations(raw, weights)

    st.write("")
    map_col, side_col = st.columns([2.6, 1], gap="large")
    with map_col:
        st.markdown(
            '<div class="tp-section">Opportunity Map</div>',
            unsafe_allow_html=True,
        )
        map_state = render_recommender_map(scored)
        # Recommendation band legend
        legend_items = "".join(
            f'<span><span class="tp-legend-swatch" '
            f'style="background:{RECOMMENDER_BAND_COLORS[b]}"></span>'
            f'{b.replace("_", " ")}</span>'
            for b in ("top_pick", "good", "okay", "avoid")
        )
        st.markdown(f'<div class="tp-legend">{legend_items}</div>',
                    unsafe_allow_html=True)
        render_zone_radar(map_state, scored)
    with side_col:
        render_recommender_top(scored)


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
    tab_map, tab_reco, tab_analytics, tab_ai = st.tabs(
        ["🗺️  Map View", "🎯  Recommender", "📊  Analytics",
         "🤖  AI Assistant"]
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

    with tab_reco:
        render_recommender(selectors)

    with tab_analytics:
        render_analytics(predictions, selectors)

    with tab_ai:
        render_llm_panel()

    render_footer()


if __name__ == "__main__":
    main()
