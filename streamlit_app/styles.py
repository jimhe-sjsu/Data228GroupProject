"""Custom CSS for the Streamlit dashboard."""

BG = "#ffffff"
SURFACE = "#f5f5f5"
SURFACE_2 = "#ebebeb"
BORDER = "#e0e0e0"
TEXT = "#171a20"
TEXT_MUTED = "#5c5e62"
ACCENT = "#e31937"

DEMAND_COLORS = {
    "low": "#e8e8e8",
    "medium": "#f5c06b",
    "high": "#e8734a",
    "very_high": "#e31937",
}


CSS = f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@200;300;400;500;600;700&display=swap');

/* ── Reset Streamlit chrome ─────────────────────────────────────────── */
#MainMenu, footer {{ visibility: hidden; }}
header {{ visibility: hidden; height: 0; min-height: 0; padding: 0; }}
.stDeployButton {{ display: none !important; }}
[data-testid="stToolbar"] {{ display: none !important; }}
[data-testid="stDecoration"] {{ display: none !important; }}

/* Hide sidebar collapse button — prevent accidental collapse */
[data-testid="stSidebar"] button[data-testid="stBaseButton-headerNoPadding"] {{
    display: none !important;
}}

/* If sidebar does get collapsed, make the reopen control prominent */
[data-testid="collapsedControl"] {{
    visibility: visible !important;
    display: block !important;
    position: fixed !important;
    top: 8px !important;
    left: 0 !important;
    z-index: 999999 !important;
    background: {SURFACE} !important;
    border: 1px solid {BORDER} !important;
    border-left: none !important;
    border-radius: 0 8px 8px 0 !important;
    box-shadow: 2px 2px 12px rgba(0, 0, 0, 0.1) !important;
    padding: 12px 10px !important;
}}
[data-testid="collapsedControl"]:hover {{
    border-color: {ACCENT} !important;
    box-shadow: 2px 2px 16px rgba(227, 25, 55, 0.2) !important;
}}
[data-testid="collapsedControl"] svg {{
    width: 22px !important;
    height: 22px !important;
    color: {ACCENT} !important;
}}

/* ── Base typography ─────────────────────────────────────────────────── */
html, body, [class*="st-"], .stApp, .stMarkdown {{
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif !important;
    font-weight: 400;
    letter-spacing: -0.01em;
}}
h1, h2, h3, h4 {{ font-weight: 600; letter-spacing: -0.02em; color: {TEXT}; }}
.stApp {{ background: {BG}; color: {TEXT}; }}

/* ── Block container ─────────────────────────────────────────────────── */
.block-container {{
    padding-top: 1.2rem !important;
    padding-bottom: 2rem !important;
    max-width: 100% !important;
}}

/* ── Top header bar ──────────────────────────────────────────────────── */
.tp-header {{
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 0 0 1.5rem 0;
    border-bottom: 1px solid {BORDER};
    margin-bottom: 1.5rem;
}}
.tp-brand {{
    font-size: 20px;
    font-weight: 700;
    letter-spacing: 0.12em;
    color: {TEXT};
    text-transform: uppercase;
}}
.tp-brand-mark {{
    display: inline-block;
    width: 10px;
    height: 10px;
    background: {ACCENT};
    border-radius: 50%;
    margin-right: 14px;
    transform: translateY(-1px);
    box-shadow: 0 0 0 3px rgba(227, 25, 55, 0.15);
    animation: pulse-dot 2s infinite;
}}
@keyframes pulse-dot {{
    0%, 100% {{ box-shadow: 0 0 0 3px rgba(227, 25, 55, 0.15); }}
    50% {{ box-shadow: 0 0 0 8px rgba(227, 25, 55, 0.05); }}
}}
.tp-context {{
    font-size: 13px;
    color: {TEXT_MUTED};
    letter-spacing: 0.06em;
    text-transform: uppercase;
    font-weight: 500;
}}

/* ── Navigation pills ────────────────────────────────────────────────── */
.tp-nav {{
    display: flex;
    gap: 0;
    margin-bottom: 1.5rem;
    border-bottom: 1px solid {BORDER};
}}
.tp-nav-item {{
    padding: 12px 24px;
    font-size: 13px;
    font-weight: 500;
    letter-spacing: 0.05em;
    text-transform: uppercase;
    color: {TEXT_MUTED};
    cursor: pointer;
    border-bottom: 2px solid transparent;
    transition: all 0.3s ease;
    text-decoration: none;
}}
.tp-nav-item:hover {{
    color: {TEXT};
    border-bottom-color: {BORDER};
}}
.tp-nav-item.active {{
    color: {TEXT};
    border-bottom-color: {ACCENT};
}}

/* ── Metric cards (KPI strip) ────────────────────────────────────────── */
.tp-metric {{
    background: {BG};
    border: 1px solid {BORDER};
    border-radius: 4px;
    padding: 24px 28px;
    height: 100%;
    transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
    position: relative;
    overflow: hidden;
}}
.tp-metric:hover {{
    border-color: {ACCENT};
    transform: translateY(-2px);
    box-shadow: 0 8px 25px rgba(0, 0, 0, 0.06);
}}
.tp-metric::before {{
    content: '';
    position: absolute;
    top: 0;
    left: 0;
    width: 100%;
    height: 3px;
    background: linear-gradient(90deg, {ACCENT}, transparent);
    opacity: 0;
    transition: opacity 0.3s ease;
}}
.tp-metric:hover::before {{
    opacity: 1;
}}
.tp-metric-label {{
    font-size: 11px;
    color: {TEXT_MUTED};
    letter-spacing: 0.15em;
    text-transform: uppercase;
    margin-bottom: 14px;
    font-weight: 600;
}}
.tp-metric-value {{
    font-size: 42px;
    font-weight: 300;
    color: {TEXT};
    line-height: 1;
    letter-spacing: -0.03em;
}}
.tp-metric-unit {{ font-size: 14px; color: {TEXT_MUTED}; margin-left: 6px; font-weight: 400; }}
.tp-metric-sub {{
    font-size: 12px;
    color: {TEXT_MUTED};
    margin-top: 10px;
    letter-spacing: 0.01em;
    font-weight: 400;
}}

/* ── Section labels ──────────────────────────────────────────────────── */
.tp-section {{
    font-size: 12px;
    color: {TEXT_MUTED};
    letter-spacing: 0.15em;
    text-transform: uppercase;
    margin: 28px 0 14px 0;
    font-weight: 600;
}}

/* ── Top zones list ──────────────────────────────────────────────────── */
.tp-zone-row {{
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 16px 16px;
    border-bottom: 1px solid {BORDER};
    transition: all 0.25s ease;
    border-radius: 4px;
    margin-bottom: 2px;
    cursor: default;
}}
.tp-zone-row:hover {{
    background: {SURFACE};
    border-color: transparent;
    padding-left: 20px;
}}
.tp-zone-row:last-child {{ border-bottom: none; }}
.tp-zone-name {{ font-size: 14px; color: {TEXT}; font-weight: 500; }}
.tp-zone-borough {{ font-size: 11px; color: {TEXT_MUTED}; margin-top: 3px; font-weight: 400; }}
.tp-zone-value {{
    font-size: 20px;
    color: {TEXT};
    font-weight: 300;
    text-align: right;
    letter-spacing: -0.01em;
    font-variant-numeric: tabular-nums;
}}
.tp-zone-value-accent {{ color: {ACCENT}; font-weight: 400; }}

/* ── Zone rank badge ─────────────────────────────────────────────────── */
.tp-zone-rank {{
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 22px;
    height: 22px;
    font-size: 10px;
    font-weight: 600;
    border-radius: 50%;
    margin-right: 12px;
    flex-shrink: 0;
}}
.tp-zone-rank-top {{ background: {ACCENT}; color: #fff; }}
.tp-zone-rank-normal {{ background: {SURFACE}; color: {TEXT_MUTED}; }}

/* ── Demand bar sparkline ────────────────────────────────────────────── */
.tp-bar-bg {{
    width: 100%;
    height: 4px;
    background: {SURFACE};
    border-radius: 2px;
    margin-top: 6px;
    overflow: hidden;
}}
.tp-bar-fill {{
    height: 100%;
    border-radius: 2px;
    transition: width 0.6s cubic-bezier(0.4, 0, 0.2, 1);
}}

/* ── Sidebar (filters) ───────────────────────────────────────────────── */
[data-testid="stSidebar"] {{
    background: {SURFACE} !important;
    border-right: 1px solid {BORDER};
}}


[data-testid="stSidebar"] .stRadio > label,
[data-testid="stSidebar"] .stSelectbox > label,
[data-testid="stSidebar"] .stSlider > label {{
    font-size: 11px !important;
    color: {TEXT_MUTED} !important;
    letter-spacing: 0.14em;
    text-transform: uppercase;
    font-weight: 600;
}}
[data-testid="stSidebar"] [data-baseweb="radio"] label {{ font-size: 13px; }}

/* Slider color */
[data-baseweb="slider"] [role="slider"] {{ background: {ACCENT} !important; }}

/* ── Chat ────────────────────────────────────────────────────────────── */
[data-testid="stChatInput"] {{
    background: {BG} !important;
    border: 1px solid {BORDER} !important;
    border-radius: 8px !important;
}}
[data-testid="stChatMessage"] {{
    background: {SURFACE} !important;
    border: 1px solid {BORDER};
    border-radius: 8px !important;
    padding: 16px 20px !important;
    max-width: 100% !important;
    width: 100% !important;
    word-wrap: break-word;
    overflow-wrap: break-word;
}}
[data-testid="stChatMessage"] p {{
    font-size: 14px;
    line-height: 1.6;
    color: {TEXT};
}}
/* User messages get a slightly different look */
[data-testid="stChatMessage"][data-testid*="user"] {{
    background: {BG} !important;
}}

/* ── Map container ───────────────────────────────────────────────────── */
.folium-map {{
    border: 1px solid {BORDER};
    border-radius: 4px;
    overflow: hidden;
}}
iframe {{ border: none !important; border-radius: 4px; }}

/* ── Demand legend strip ─────────────────────────────────────────────── */
.tp-legend {{
    display: flex;
    gap: 20px;
    margin-top: 12px;
    font-size: 11px;
    color: {TEXT_MUTED};
    letter-spacing: 0.1em;
    text-transform: uppercase;
    font-weight: 500;
}}
.tp-legend-swatch {{
    display: inline-block;
    width: 12px;
    height: 12px;
    margin-right: 8px;
    border-radius: 2px;
    transform: translateY(2px);
}}

/* ── Buttons / dataframes ────────────────────────────────────────────── */
button[kind="primary"] {{
    background: {ACCENT} !important;
    border: none !important;
    border-radius: 4px !important;
    font-weight: 500 !important;
    letter-spacing: 0.05em;
    transition: all 0.2s ease;
}}
button[kind="primary"]:hover {{
    transform: translateY(-1px);
    box-shadow: 0 4px 12px rgba(227, 25, 55, 0.3);
}}
.stDataFrame {{ border: 1px solid {BORDER}; border-radius: 4px; }}

/* ── Expander ────────────────────────────────────────────────────────── */
.stExpander {{
    border: 1px solid {BORDER};
    border-radius: 8px;
}}

/* ── AI suggestion chips ─────────────────────────────────────────────── */
.stTabs [data-baseweb="tab-panel"] button[kind="secondary"] {{
    font-size: 11px !important;
    letter-spacing: 0.04em;
    color: {TEXT_MUTED} !important;
    border: 1px solid {BORDER} !important;
    border-radius: 20px !important;
    padding: 8px 16px !important;
    background: {BG} !important;
    transition: all 0.2s ease;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
}}
.stTabs [data-baseweb="tab-panel"] button[kind="secondary"]:hover {{
    border-color: {ACCENT} !important;
    color: {ACCENT} !important;
    background: rgba(227, 25, 55, 0.04) !important;
}}

/* ── Misc ────────────────────────────────────────────────────────────── */
hr {{ border-color: {BORDER}; opacity: 0.5; }}
code {{ background: {SURFACE}; color: {TEXT}; border-radius: 4px; padding: 2px 6px; }}

/* ── Scrollbar styling ───────────────────────────────────────────────── */
::-webkit-scrollbar {{ width: 6px; height: 6px; }}
::-webkit-scrollbar-track {{ background: transparent; }}
::-webkit-scrollbar-thumb {{ background: {BORDER}; border-radius: 3px; }}
::-webkit-scrollbar-thumb:hover {{ background: {TEXT_MUTED}; }}

/* ── Animated entrance (fade-in for key sections) ────────────────────── */
@keyframes fadeInUp {{
    from {{ opacity: 0; transform: translateY(12px); }}
    to {{ opacity: 1; transform: translateY(0); }}
}}
.tp-animate {{
    animation: fadeInUp 0.5s cubic-bezier(0.4, 0, 0.2, 1) forwards;
}}
.tp-animate-delay-1 {{ animation-delay: 0.1s; opacity: 0; }}
.tp-animate-delay-2 {{ animation-delay: 0.2s; opacity: 0; }}
.tp-animate-delay-3 {{ animation-delay: 0.3s; opacity: 0; }}
.tp-animate-delay-4 {{ animation-delay: 0.4s; opacity: 0; }}

/* ── Tooltip overlay for map info ────────────────────────────────────── */
.tp-tooltip {{
    background: {BG};
    border: 1px solid {BORDER};
    border-radius: 4px;
    padding: 12px 16px;
    box-shadow: 0 4px 16px rgba(0, 0, 0, 0.08);
    font-size: 12px;
    color: {TEXT};
}}

/* ── Stats comparison cards ──────────────────────────────────────────── */
.tp-stat-change {{
    display: inline-flex;
    align-items: center;
    gap: 4px;
    font-size: 11px;
    font-weight: 500;
    padding: 2px 8px;
    border-radius: 12px;
}}
.tp-stat-up {{ background: rgba(16, 185, 129, 0.1); color: #059669; }}
.tp-stat-down {{ background: rgba(239, 68, 68, 0.1); color: #dc2626; }}

/* ── Tab styling ─────────────────────────────────────────────────────── */
.stTabs [data-baseweb="tab-list"] {{
    gap: 0;
    border-bottom: 1px solid {BORDER};
}}
.stTabs [data-baseweb="tab"] {{
    font-size: 13px;
    font-weight: 500;
    letter-spacing: 0.05em;
    text-transform: uppercase;
    padding: 12px 24px;
    color: {TEXT_MUTED};
    border-bottom: 2px solid transparent;
}}
.stTabs [aria-selected="true"] {{
    color: {TEXT} !important;
    border-bottom-color: {ACCENT} !important;
}}

/* ── Selectbox / input styling ───────────────────────────────────────── */
[data-baseweb="select"] {{
    border-radius: 4px !important;
}}
[data-baseweb="input"] {{
    border-radius: 4px !important;
}}

/* ── Full-width hero section ─────────────────────────────────────────── */
.tp-hero {{
    text-align: center;
    padding: 32px 0 24px 0;
    margin-bottom: 1rem;
}}
.tp-hero-title {{
    font-size: 48px;
    font-weight: 700;
    letter-spacing: -0.03em;
    color: {TEXT};
    margin-bottom: 8px;
    line-height: 1.1;
}}
.tp-hero-sub {{
    font-size: 16px;
    color: {TEXT_MUTED};
    font-weight: 400;
    letter-spacing: 0.01em;
}}

/* ── Footer ──────────────────────────────────────────────────────────── */
.tp-footer {{
    text-align: center;
    padding: 32px 0 16px 0;
    margin-top: 40px;
    border-top: 1px solid {BORDER};
    font-size: 11px;
    color: {TEXT_MUTED};
    letter-spacing: 0.06em;
}}
.tp-footer a {{
    color: {TEXT};
    text-decoration: none;
    font-weight: 500;
}}
.tp-footer a:hover {{ color: {ACCENT}; }}
</style>
"""
