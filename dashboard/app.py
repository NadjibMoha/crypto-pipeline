"""
Crypto Data Pipeline & Analytics Dashboard
A premium, dark-themed analytics interface for cryptocurrency market data.
"""
import os
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
from datetime import datetime

# ── Page Config ──────────────────────────────────────────────
st.set_page_config(
    page_title="CryptoPulse — Analytics Dashboard",
    layout="wide",
    page_icon="⚡",
    initial_sidebar_state="expanded",
)

# ── Theme & Styling ──────────────────────────────────────────
COLORS = {
    "bg": "#0e1117",
    "card": "#1a1f2e",
    "accent": "#00d4aa",
    "accent2": "#7c3aed",
    "positive": "#22c55e",
    "negative": "#ef4444",
    "warning": "#f59e0b",
    "text": "#e2e8f0",
    "muted": "#94a3b8",
    "border": "#1e293b",
}

PLOTLY_TEMPLATE = {
    "layout": {
        "paper_bgcolor": "rgba(0,0,0,0)",
        "plot_bgcolor": "rgba(0,0,0,0)",
        "font": {"color": COLORS["text"], "family": "Inter, sans-serif"},
        "xaxis": {"gridcolor": "rgba(255,255,255,0.05)", "zerolinecolor": "rgba(255,255,255,0.05)"},
        "yaxis": {"gridcolor": "rgba(255,255,255,0.05)", "zerolinecolor": "rgba(255,255,255,0.05)"},
        "margin": {"l": 40, "r": 20, "t": 50, "b": 40},
    }
}

COIN_ICONS = {
    "bitcoin": "₿",
    "ethereum": "Ξ",
    "binancecoin": "🔶",
    "solana": "◎",
    "ripple": "✕",
}

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');

    /* Global */
    .stApp { font-family: 'Inter', sans-serif; }
    .block-container { padding-top: 1.5rem; }

    /* Header */
    .dashboard-header {
        background: linear-gradient(135deg, #0f172a 0%, #1e1b4b 50%, #0f172a 100%);
        border: 1px solid rgba(124, 58, 237, 0.2);
        border-radius: 16px;
        padding: 1.8rem 2.2rem;
        margin-bottom: 1.5rem;
        position: relative;
        overflow: hidden;
    }
    .dashboard-header::before {
        content: '';
        position: absolute;
        top: -50%; left: -50%;
        width: 200%; height: 200%;
        background: radial-gradient(circle at 30% 50%, rgba(0,212,170,0.06) 0%, transparent 50%),
                    radial-gradient(circle at 70% 50%, rgba(124,58,237,0.06) 0%, transparent 50%);
        animation: pulse-bg 8s ease-in-out infinite;
    }
    @keyframes pulse-bg {
        0%, 100% { opacity: 0.5; }
        50% { opacity: 1; }
    }
    .dashboard-header h1 {
        font-size: 1.8rem;
        font-weight: 800;
        background: linear-gradient(135deg, #00d4aa, #7c3aed);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin: 0; position: relative; z-index: 1;
    }
    .dashboard-header p {
        color: #94a3b8;
        font-size: 0.9rem;
        margin: 0.3rem 0 0 0;
        position: relative; z-index: 1;
    }

    /* Metric Cards */
    .metric-card {
        background: linear-gradient(145deg, #1a1f2e, #151929);
        border: 1px solid rgba(255,255,255,0.06);
        border-radius: 14px;
        padding: 1.2rem 1.4rem;
        transition: all 0.3s ease;
        position: relative;
        overflow: hidden;
    }
    .metric-card:hover {
        border-color: rgba(0,212,170,0.3);
        box-shadow: 0 8px 32px rgba(0,212,170,0.08);
        transform: translateY(-2px);
    }
    .metric-card .label {
        font-size: 0.75rem;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        color: #94a3b8;
        font-weight: 600;
        margin-bottom: 0.3rem;
    }
    .metric-card .value {
        font-size: 1.6rem;
        font-weight: 700;
        color: #e2e8f0;
    }
    .metric-card .sub {
        font-size: 0.8rem;
        margin-top: 0.2rem;
    }
    .metric-card .icon {
        position: absolute;
        top: 1rem; right: 1.2rem;
        font-size: 1.8rem;
        opacity: 0.15;
    }

    /* Coin Cards */
    .coin-card {
        background: linear-gradient(145deg, #1a1f2e, #151929);
        border: 1px solid rgba(255,255,255,0.06);
        border-radius: 12px;
        padding: 1rem 1.2rem;
        transition: all 0.3s ease;
    }
    .coin-card:hover {
        border-color: rgba(0,212,170,0.25);
        transform: translateY(-2px);
        box-shadow: 0 6px 24px rgba(0,0,0,0.3);
    }
    .coin-name {
        font-weight: 600;
        color: #e2e8f0;
        font-size: 1rem;
    }
    .coin-symbol {
        color: #94a3b8;
        font-size: 0.75rem;
        text-transform: uppercase;
        letter-spacing: 0.06em;
    }
    .coin-price {
        font-size: 1.3rem;
        font-weight: 700;
        color: #e2e8f0;
        margin: 0.4rem 0 0.2rem 0;
    }
    .positive { color: #22c55e !important; }
    .negative { color: #ef4444 !important; }

    /* Section Headers */
    .section-header {
        font-size: 1.1rem;
        font-weight: 700;
        color: #e2e8f0;
        display: flex;
        align-items: center;
        gap: 0.5rem;
        margin: 1.2rem 0 0.8rem 0;
        padding-bottom: 0.5rem;
        border-bottom: 1px solid rgba(255,255,255,0.06);
    }

    /* Fear & Greed Gauge */
    .fng-container {
        text-align: center;
        padding: 0.5rem;
    }
    .fng-score {
        font-size: 2.8rem;
        font-weight: 800;
    }
    .fng-label {
        font-size: 0.85rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        margin-top: 0.2rem;
    }

    /* Pipeline Status */
    .status-badge {
        display: inline-block;
        padding: 0.2rem 0.6rem;
        border-radius: 999px;
        font-size: 0.7rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    .status-success {
        background: rgba(34,197,94,0.15);
        color: #22c55e;
        border: 1px solid rgba(34,197,94,0.3);
    }
    .status-failed {
        background: rgba(239,68,68,0.15);
        color: #ef4444;
        border: 1px solid rgba(239,68,68,0.3);
    }

    /* Tab styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 0.5rem;
        background: rgba(0,0,0,0.2);
        padding: 0.3rem;
        border-radius: 12px;
    }
    .stTabs [data-baseweb="tab"] {
        border-radius: 10px;
        padding: 0.5rem 1.2rem;
        font-weight: 500;
    }

    /* Sidebar */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0f172a, #1a1f2e);
    }
    [data-testid="stSidebar"] .block-container {
        padding-top: 2rem;
    }

    /* Hide Streamlit branding */
    #MainMenu, footer, header { visibility: hidden; }

    /* Custom scrollbar */
    ::-webkit-scrollbar { width: 6px; }
    ::-webkit-scrollbar-track { background: #0e1117; }
    ::-webkit-scrollbar-thumb { background: #1e293b; border-radius: 3px; }
    ::-webkit-scrollbar-thumb:hover { background: #334155; }
</style>
""", unsafe_allow_html=True)


# ── Database Connection ──────────────────────────────────────
def get_connection():
    """Create a fresh psycopg2 connection to the crypto_db warehouse."""
    return psycopg2.connect(
        dbname="crypto_db",
        user="crypto_user",
        password="crypto_pass",
        host=os.getenv("DB_HOST", "postgres"),
        port="5432",
    )


# ── Data Fetching (cached) ──────────────────────────────────
@st.cache_data(ttl=300)
def fetch_market_summary() -> pd.DataFrame:
    query = "SELECT * FROM reports.rpt_market_summary ORDER BY calculated_at DESC LIMIT 1"
    try:
        with get_connection() as conn:
            return pd.read_sql(query, conn)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def fetch_top_movers() -> pd.DataFrame:
    query = "SELECT * FROM reports.rpt_top_movers ORDER BY price_change_24h_pct DESC LIMIT 10"
    try:
        with get_connection() as conn:
            return pd.read_sql(query, conn)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def fetch_coin_prices() -> pd.DataFrame:
    query = """
        SELECT DISTINCT ON (coin_id)
               coin_id, symbol, name, price_usd, market_cap_usd, 
               volume_24h_usd, price_change_24h_pct, circulating_supply
        FROM staging.stg_prices
        ORDER BY coin_id, fetched_at DESC
    """
    try:
        with get_connection() as conn:
            return pd.read_sql(query, conn)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def fetch_price_analysis(days=7) -> pd.DataFrame:
    query = f"""
        SELECT coin_id, close_price, volume, candle_ts 
        FROM staging.stg_ohlcv 
        WHERE candle_ts >= NOW() - INTERVAL '{days} days'
        ORDER BY candle_ts ASC
    """
    try:
        with get_connection() as conn:
            return pd.read_sql(query, conn)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def fetch_daily_indicators() -> pd.DataFrame:
    query = """
        SELECT * FROM analytics.agg_daily 
        WHERE date >= CURRENT_DATE - INTERVAL '30 days'
        ORDER BY date ASC
    """
    try:
        with get_connection() as conn:
            return pd.read_sql(query, conn)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def fetch_pipeline_runs() -> pd.DataFrame:
    query = "SELECT * FROM analytics.pipeline_audit ORDER BY run_at DESC LIMIT 20"
    try:
        with get_connection() as conn:
            return pd.read_sql(query, conn)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def fetch_coins_meta() -> pd.DataFrame:
    query = "SELECT * FROM analytics.dim_coins ORDER BY rank ASC NULLS LAST"
    try:
        with get_connection() as conn:
            return pd.read_sql(query, conn)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def fetch_sentiment() -> pd.DataFrame:
    query = "SELECT * FROM raw.raw_sentiment ORDER BY fetched_at DESC LIMIT 1"
    try:
        with get_connection() as conn:
            return pd.read_sql(query, conn)
    except Exception:
        return pd.DataFrame()


# ── Helper Functions ─────────────────────────────────────────
def format_large_number(num):
    """Format large numbers into human-readable strings (K, M, B, T)."""
    if num is None or pd.isna(num):
        return "N/A"
    abs_num = abs(num)
    if abs_num >= 1e12:
        return f"${num / 1e12:,.2f}T"
    elif abs_num >= 1e9:
        return f"${num / 1e9:,.2f}B"
    elif abs_num >= 1e6:
        return f"${num / 1e6:,.2f}M"
    elif abs_num >= 1e3:
        return f"${num / 1e3:,.1f}K"
    else:
        return f"${num:,.2f}"


def format_price(price):
    """Format price with appropriate decimal places."""
    if price is None or pd.isna(price):
        return "N/A"
    if price >= 1000:
        return f"${price:,.2f}"
    elif price >= 1:
        return f"${price:.2f}"
    else:
        return f"${price:.4f}"


def get_fng_color(score):
    """Return color based on Fear & Greed score."""
    if score <= 25:
        return COLORS["negative"]
    elif score <= 45:
        return COLORS["warning"]
    elif score <= 55:
        return COLORS["muted"]
    elif score <= 75:
        return COLORS["accent"]
    else:
        return COLORS["positive"]


def get_fng_label(score):
    """Return label based on Fear & Greed score."""
    if score <= 25:
        return "Extreme Fear"
    elif score <= 45:
        return "Fear"
    elif score <= 55:
        return "Neutral"
    elif score <= 75:
        return "Greed"
    else:
        return "Extreme Greed"


def make_plotly_dark(fig):
    """Apply dark theme to a Plotly figure."""
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color=COLORS["text"], family="Inter, sans-serif", size=12),
        xaxis=dict(gridcolor="rgba(255,255,255,0.04)", zerolinecolor="rgba(255,255,255,0.04)"),
        yaxis=dict(gridcolor="rgba(255,255,255,0.04)", zerolinecolor="rgba(255,255,255,0.04)"),
        margin=dict(l=40, r=20, t=50, b=40),
        legend=dict(
            bgcolor="rgba(0,0,0,0)",
            font=dict(size=11),
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
        ),
        hoverlabel=dict(
            bgcolor="#1a1f2e",
            font_size=12,
            font_family="Inter, sans-serif",
            bordercolor="rgba(255,255,255,0.1)",
        ),
    )
    return fig


# ── Sidebar ──────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style="text-align:center; margin-bottom:1.5rem;">
        <div style="font-size:2.5rem; margin-bottom:0.3rem;">⚡</div>
        <div style="font-size:1.2rem; font-weight:700; 
             background: linear-gradient(135deg, #00d4aa, #7c3aed);
             -webkit-background-clip: text; -webkit-text-fill-color: transparent;">
            CryptoPulse
        </div>
        <div style="color:#64748b; font-size:0.75rem; margin-top:0.2rem;">
            Real-Time Analytics Engine
        </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("---")

    if st.button("🔄  Refresh All Data", width="stretch", type="primary"):
        st.cache_data.clear()
        st.rerun()

    st.markdown("---")

    # Show live data stats
    st.markdown('<div style="color:#94a3b8; font-size:0.75rem; text-transform:uppercase; letter-spacing:0.1em; font-weight:600;">Data Sources</div>', unsafe_allow_html=True)

    meta = fetch_coins_meta()
    if not meta.empty:
        last_updated = meta['last_updated'].max()
        if pd.notnull(last_updated):
            ts = pd.Timestamp(last_updated)
            st.markdown(f"""
            <div style="background:#151929; border:1px solid rgba(255,255,255,0.06); border-radius:10px; padding:0.8rem; margin:0.5rem 0;">
                <div style="color:#94a3b8; font-size:0.7rem; text-transform:uppercase;">Last Refresh</div>
                <div style="color:#e2e8f0; font-size:0.9rem; font-weight:600;">{ts.strftime('%b %d, %H:%M UTC')}</div>
            </div>
            """, unsafe_allow_html=True)

    audit = fetch_pipeline_runs()
    if not audit.empty:
        success_count = len(audit[audit['status'] == 'SUCCESS'])
        total_count = len(audit)
        rate = (success_count / total_count) * 100 if total_count > 0 else 0
        rate_color = COLORS["positive"] if rate >= 80 else COLORS["warning"] if rate >= 50 else COLORS["negative"]
        st.markdown(f"""
        <div style="background:#151929; border:1px solid rgba(255,255,255,0.06); border-radius:10px; padding:0.8rem; margin:0.5rem 0;">
            <div style="color:#94a3b8; font-size:0.7rem; text-transform:uppercase;">Pipeline Health</div>
            <div style="color:{rate_color}; font-size:1.3rem; font-weight:700;">{rate:.0f}%</div>
            <div style="color:#64748b; font-size:0.7rem;">{success_count}/{total_count} runs passed</div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("---")
    st.markdown(f"""
    <div style="color:#475569; font-size:0.65rem; text-align:center;">
        Powered by Airflow • PostgreSQL • Streamlit<br>
        v1.0 — {datetime.now().strftime('%Y')}
    </div>
    """, unsafe_allow_html=True)


# ── Dashboard Header ─────────────────────────────────────────
st.markdown("""
<div class="dashboard-header">
    <h1>⚡ CryptoPulse Analytics</h1>
    <p>Real-time cryptocurrency market intelligence powered by your automated data pipeline</p>
</div>
""", unsafe_allow_html=True)


# ── Navigation Tabs ──────────────────────────────────────────
tab1, tab2, tab3, tab4 = st.tabs([
    "📊  Market Overview",
    "📈  Price Analysis",
    "🔬  Technical Signals",
    "⚙️  Pipeline Health",
])


# ═════════════════════════════════════════════════════════════
# TAB 1 — MARKET OVERVIEW
# ═════════════════════════════════════════════════════════════
with tab1:
    market_summary = fetch_market_summary()
    sentiment = fetch_sentiment()

    # ── KPI Row ──
    kpi1, kpi2, kpi3, kpi4 = st.columns(4)

    if not market_summary.empty:
        ms = market_summary.iloc[0]

        with kpi1:
            st.markdown(f"""
            <div class="metric-card">
                <div class="icon">💰</div>
                <div class="label">Total Market Cap</div>
                <div class="value">{format_large_number(ms.get('total_market_cap'))}</div>
            </div>
            """, unsafe_allow_html=True)

        with kpi2:
            btc_dom = ms.get('btc_dominance_pct')
            st.markdown(f"""
            <div class="metric-card">
                <div class="icon">₿</div>
                <div class="label">BTC Dominance</div>
                <div class="value">{btc_dom:.1f}%</div>
            </div>
            """, unsafe_allow_html=True)

        with kpi3:
            st.markdown(f"""
            <div class="metric-card">
                <div class="icon">📊</div>
                <div class="label">24h Trading Volume</div>
                <div class="value">{format_large_number(ms.get('total_volume_24h'))}</div>
            </div>
            """, unsafe_allow_html=True)

    # Fear & Greed in the 4th column
    with kpi4:
        if not sentiment.empty:
            fng_val = int(sentiment.iloc[0].get('value', 0))
            fng_class = sentiment.iloc[0].get('value_classification', '')
        elif not market_summary.empty:
            fng_val = int(ms.get('fear_greed_score', 0))
            fng_class = get_fng_label(fng_val)
        else:
            fng_val = 0
            fng_class = "N/A"

        fng_color = get_fng_color(fng_val)
        st.markdown(f"""
        <div class="metric-card">
            <div class="label">Fear & Greed Index</div>
            <div class="fng-container">
                <div class="fng-score" style="color:{fng_color}">{fng_val}</div>
                <div class="fng-label" style="color:{fng_color}">{fng_class}</div>
            </div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Coin Price Cards ──
    st.markdown('<div class="section-header">🪙 Coin Prices</div>', unsafe_allow_html=True)

    coin_prices = fetch_coin_prices()
    if not coin_prices.empty:
        cols = st.columns(len(coin_prices))
        for i, (_, row) in enumerate(coin_prices.iterrows()):
            with cols[i]:
                coin_id = row.get('coin_id', '')
                symbol = str(row.get('symbol', '')).upper()
                name = str(row.get('name', coin_id.capitalize()))
                price = row.get('price_usd')
                change = row.get('price_change_24h_pct')
                mcap = row.get('market_cap_usd')
                icon = COIN_ICONS.get(coin_id, "🔹")

                change_val = float(change) if change is not None and not pd.isna(change) else 0
                change_class = "positive" if change_val >= 0 else "negative"
                change_arrow = "▲" if change_val >= 0 else "▼"

                st.markdown(f"""
                <div class="coin-card">
                    <div style="display:flex; align-items:center; gap:0.5rem;">
                        <span style="font-size:1.4rem;">{icon}</span>
                        <div>
                            <div class="coin-name">{name}</div>
                            <div class="coin-symbol">{symbol}</div>
                        </div>
                    </div>
                    <div class="coin-price">{format_price(price)}</div>
                    <div class="{change_class}" style="font-size:0.85rem; font-weight:600;">
                        {change_arrow} {abs(change_val):.2f}%
                    </div>
                    <div style="color:#64748b; font-size:0.7rem; margin-top:0.3rem;">
                        MCap: {format_large_number(mcap)}
                    </div>
                </div>
                """, unsafe_allow_html=True)
    else:
        st.info("Coin price data not available yet. The pipeline needs to complete at least one full run.")

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Top Movers ──
    st.markdown('<div class="section-header">🚀 Top Movers (24h)</div>', unsafe_allow_html=True)
    top_movers = fetch_top_movers()
    if not top_movers.empty:
        cols = st.columns(len(top_movers))
        for i, (_, row) in enumerate(top_movers.iterrows()):
            with cols[i]:
                symbol = str(row.get('symbol', '')).upper()
                change = float(row.get('price_change_24h_pct', 0))
                vol_spike = row.get('volume_spike_ratio')
                change_class = "positive" if change >= 0 else "negative"
                change_arrow = "▲" if change >= 0 else "▼"

                vol_text = ""
                if vol_spike is not None and not pd.isna(vol_spike) and vol_spike > 1.5:
                    vol_text = f'<div style="color:#f59e0b; font-size:0.65rem; margin-top:0.2rem;">🔥 Vol {vol_spike:.1f}x</div>'

                st.markdown(f"""
                <div class="coin-card" style="text-align:center;">
                    <div class="coin-symbol" style="font-size:0.85rem;">{symbol}</div>
                    <div class="{change_class}" style="font-size:1.2rem; font-weight:700; margin:0.3rem 0;">
                        {change_arrow} {abs(change):.2f}%
                    </div>
                    {vol_text}
                </div>
                """, unsafe_allow_html=True)
    else:
        st.info("Top movers data not available yet.")


# ═════════════════════════════════════════════════════════════
# TAB 2 — PRICE ANALYSIS
# ═════════════════════════════════════════════════════════════
with tab2:
    price_data = fetch_price_analysis(days=7)
    indicators = fetch_daily_indicators()

    if not price_data.empty:
        # ── Coin Selector ──
        st.markdown('<div class="section-header">📈 Multi-Coin Price Trends</div>', unsafe_allow_html=True)
        coins_list = sorted(price_data['coin_id'].unique())
        selected_coins = st.multiselect(
            "Select coins to compare",
            coins_list,
            default=coins_list[:3],
            key="price_coins",
        )

        if selected_coins:
            filtered = price_data[price_data['coin_id'].isin(selected_coins)]

            # Color palette
            coin_colors = {
                "bitcoin": "#f7931a",
                "ethereum": "#627eea",
                "binancecoin": "#f0b90b",
                "solana": "#9945ff",
                "ripple": "#00aae4",
            }

            fig = go.Figure()
            for coin in selected_coins:
                coin_data = filtered[filtered['coin_id'] == coin]
                color = coin_colors.get(coin, "#00d4aa")
                fig.add_trace(go.Scatter(
                    x=coin_data['candle_ts'],
                    y=coin_data['close_price'],
                    name=coin.capitalize(),
                    line=dict(color=color, width=2.5),
                    hovertemplate=f"{coin.capitalize()}<br>Price: $%{{y:,.2f}}<br>%{{x}}<extra></extra>",
                ))
            fig.update_layout(
                title=dict(text="7-Day Hourly Prices", font=dict(size=16)),
                xaxis_title="",
                yaxis_title="Price (USD)",
                hovermode="x unified",
                height=450,
            )
            make_plotly_dark(fig)
            st.plotly_chart(fig, width="stretch")

        # ── Heatmap + Volume ──
        col1, col2 = st.columns(2)

        with col1:
            st.markdown('<div class="section-header">🗓️ Daily Change Heatmap</div>', unsafe_allow_html=True)
            if not indicators.empty and 'price_change_1d_pct' in indicators.columns:
                heatmap_data = indicators.pivot_table(
                    index='coin_id', columns='date', values='price_change_1d_pct'
                )
                if not heatmap_data.empty:
                    fig_hm = px.imshow(
                        heatmap_data,
                        color_continuous_scale=["#ef4444", "#1e293b", "#22c55e"],
                        labels=dict(x="Date", y="Coin", color="Change %"),
                        aspect="auto",
                    )
                    fig_hm.update_layout(height=300, title="")
                    make_plotly_dark(fig_hm)
                    st.plotly_chart(fig_hm, width="stretch")
                else:
                    st.info("Heatmap requires multiple days of data.")
            else:
                st.info("Indicator data not available yet.")

        with col2:
            st.markdown('<div class="section-header">📊 24h Volume by Coin</div>', unsafe_allow_html=True)
            coin_prices_vol = fetch_coin_prices()
            if not coin_prices_vol.empty and 'volume_24h_usd' in coin_prices_vol.columns:
                vol_df = coin_prices_vol[['coin_id', 'volume_24h_usd']].dropna()
                if not vol_df.empty:
                    fig_bar = px.bar(
                        vol_df,
                        x='coin_id',
                        y='volume_24h_usd',
                        color='coin_id',
                        color_discrete_map={
                            "bitcoin": "#f7931a", "ethereum": "#627eea",
                            "binancecoin": "#f0b90b", "solana": "#9945ff", "ripple": "#00aae4",
                        },
                    )
                    fig_bar.update_layout(
                        height=300,
                        showlegend=False,
                        title="",
                        xaxis_title="",
                        yaxis_title="Volume (USD)",
                    )
                    fig_bar.update_traces(
                        hovertemplate="%{x}<br>Volume: $%{y:,.0f}<extra></extra>"
                    )
                    make_plotly_dark(fig_bar)
                    st.plotly_chart(fig_bar, width="stretch")
            else:
                st.info("Volume data not available yet.")
    else:
        st.info("No OHLCV price data available. Wait for the pipeline to run.")


# ═════════════════════════════════════════════════════════════
# TAB 3 — TECHNICAL SIGNALS
# ═════════════════════════════════════════════════════════════
with tab3:
    indicators = fetch_daily_indicators()

    if not indicators.empty:
        coins_list = sorted(indicators['coin_id'].unique())
        tech_coin = st.selectbox(
            "Select Coin for Technical Analysis",
            coins_list,
            format_func=lambda x: f"{COIN_ICONS.get(x, '🔹')}  {x.capitalize()}",
            key="tech_coin",
        )

        tech_data = indicators[indicators['coin_id'] == tech_coin].sort_values('date')
        coin_color = {
            "bitcoin": "#f7931a", "ethereum": "#627eea",
            "binancecoin": "#f0b90b", "solana": "#9945ff", "ripple": "#00aae4",
        }.get(tech_coin, "#00d4aa")

        # ── Bollinger Bands ──
        st.markdown(f'<div class="section-header">📐 {tech_coin.capitalize()} — Bollinger Bands</div>', unsafe_allow_html=True)

        fig_bb = go.Figure()
        # Fill between upper and lower
        fig_bb.add_trace(go.Scatter(
            x=tech_data['date'], y=tech_data['bb_upper'],
            name='Upper Band', line=dict(color='rgba(124,58,237,0.4)', width=1, dash='dot'),
            showlegend=True,
        ))
        fig_bb.add_trace(go.Scatter(
            x=tech_data['date'], y=tech_data['bb_lower'],
            name='Lower Band', line=dict(color='rgba(124,58,237,0.4)', width=1, dash='dot'),
            fill='tonexty', fillcolor='rgba(124,58,237,0.06)',
            showlegend=True,
        ))
        fig_bb.add_trace(go.Scatter(
            x=tech_data['date'], y=tech_data['bb_middle'],
            name='SMA 20', line=dict(color='#7c3aed', width=1.5),
        ))
        fig_bb.add_trace(go.Scatter(
            x=tech_data['date'], y=tech_data['avg_price'],
            name='Price', line=dict(color=coin_color, width=2.5),
        ))
        fig_bb.update_layout(
            height=400,
            yaxis_title="Price (USD)",
            hovermode="x unified",
        )
        make_plotly_dark(fig_bb)
        st.plotly_chart(fig_bb, width="stretch")

        # ── RSI + Volume Spike ──
        col1, col2 = st.columns(2)

        with col1:
            st.markdown('<div class="section-header">💪 RSI (14-Period)</div>', unsafe_allow_html=True)
            fig_rsi = go.Figure()

            # Overbought/oversold zones
            fig_rsi.add_hrect(y0=70, y1=100, fillcolor="rgba(239,68,68,0.08)", line_width=0)
            fig_rsi.add_hrect(y0=0, y1=30, fillcolor="rgba(34,197,94,0.08)", line_width=0)

            fig_rsi.add_trace(go.Scatter(
                x=tech_data['date'], y=tech_data['rsi_14'],
                name='RSI', line=dict(color=coin_color, width=2.5),
                fill='tozeroy', fillcolor=f"rgba({int(coin_color[1:3],16)},{int(coin_color[3:5],16)},{int(coin_color[5:7],16)},0.08)",
            ))
            fig_rsi.add_hline(y=70, line_dash="dash", line_color="rgba(239,68,68,0.5)", line_width=1,
                              annotation_text="Overbought", annotation_font_color="#ef4444", annotation_font_size=10)
            fig_rsi.add_hline(y=30, line_dash="dash", line_color="rgba(34,197,94,0.5)", line_width=1,
                              annotation_text="Oversold", annotation_font_color="#22c55e", annotation_font_size=10)
            fig_rsi.update_yaxes(range=[0, 100])
            fig_rsi.update_layout(height=350, showlegend=False)
            make_plotly_dark(fig_rsi)
            st.plotly_chart(fig_rsi, width="stretch")

        with col2:
            st.markdown('<div class="section-header">🔥 Volume Spike Ratio</div>', unsafe_allow_html=True)
            fig_spike = go.Figure()

            colors = [COLORS["negative"] if v > 2 else COLORS["accent"] for v in tech_data['volume_spike']]
            fig_spike.add_trace(go.Bar(
                x=tech_data['date'], y=tech_data['volume_spike'],
                marker_color=colors,
                hovertemplate="Volume Spike: %{y:.2f}x<extra></extra>",
            ))
            fig_spike.add_hline(
                y=2, line_dash="dash", line_color="rgba(239,68,68,0.6)", line_width=1,
                annotation_text="2x Threshold", annotation_font_color="#ef4444", annotation_font_size=10,
            )
            fig_spike.update_layout(height=350, showlegend=False, yaxis_title="Spike Ratio")
            make_plotly_dark(fig_spike)
            st.plotly_chart(fig_spike, width="stretch")

        # ── Quick Signal Summary Cards ──
        st.markdown('<div class="section-header">📋 Signal Summary</div>', unsafe_allow_html=True)
        latest = tech_data.iloc[-1] if len(tech_data) > 0 else None
        if latest is not None:
            s1, s2, s3, s4 = st.columns(4)

            # RSI Signal
            rsi = latest.get('rsi_14', 50)
            rsi_signal = "OVERBOUGHT" if rsi > 70 else "OVERSOLD" if rsi < 30 else "NEUTRAL"
            rsi_color = COLORS["negative"] if rsi > 70 else COLORS["positive"] if rsi < 30 else COLORS["muted"]
            with s1:
                st.markdown(f"""
                <div class="metric-card">
                    <div class="label">RSI Signal</div>
                    <div class="value" style="font-size:1.2rem; color:{rsi_color}">{rsi_signal}</div>
                    <div class="sub" style="color:#64748b">RSI: {rsi:.1f}</div>
                </div>
                """, unsafe_allow_html=True)

            # Volatility
            vol = latest.get('volatility', 0)
            vol_level = "HIGH" if vol > 0.05 else "LOW" if vol < 0.02 else "MODERATE"
            vol_color = COLORS["negative"] if vol > 0.05 else COLORS["positive"] if vol < 0.02 else COLORS["warning"]
            with s2:
                st.markdown(f"""
                <div class="metric-card">
                    <div class="label">Volatility</div>
                    <div class="value" style="font-size:1.2rem; color:{vol_color}">{vol_level}</div>
                    <div class="sub" style="color:#64748b">σ: {vol:.4f}</div>
                </div>
                """, unsafe_allow_html=True)

            # Volume Spike
            spike = latest.get('volume_spike', 1)
            spike_alert = "SPIKE!" if spike > 2 else "ELEVATED" if spike > 1.5 else "NORMAL"
            spike_color = COLORS["negative"] if spike > 2 else COLORS["warning"] if spike > 1.5 else COLORS["positive"]
            with s3:
                st.markdown(f"""
                <div class="metric-card">
                    <div class="label">Volume Status</div>
                    <div class="value" style="font-size:1.2rem; color:{spike_color}">{spike_alert}</div>
                    <div class="sub" style="color:#64748b">{spike:.2f}x avg</div>
                </div>
                """, unsafe_allow_html=True)

            # Band Position
            price = latest.get('avg_price', 0)
            bb_upper = latest.get('bb_upper', 0)
            bb_lower = latest.get('bb_lower', 0)
            if bb_upper and bb_lower and (bb_upper - bb_lower) > 0:
                band_pos = (price - bb_lower) / (bb_upper - bb_lower) * 100
            else:
                band_pos = 50
            band_signal = "NEAR UPPER" if band_pos > 80 else "NEAR LOWER" if band_pos < 20 else "MID RANGE"
            band_color = COLORS["negative"] if band_pos > 80 else COLORS["positive"] if band_pos < 20 else COLORS["accent"]
            with s4:
                st.markdown(f"""
                <div class="metric-card">
                    <div class="label">Band Position</div>
                    <div class="value" style="font-size:1.2rem; color:{band_color}">{band_signal}</div>
                    <div class="sub" style="color:#64748b">{band_pos:.0f}% of range</div>
                </div>
                """, unsafe_allow_html=True)
    else:
        st.info("Technical indicator data not available yet. The pipeline needs to complete at least one full run.")


# ═════════════════════════════════════════════════════════════
# TAB 4 — PIPELINE HEALTH
# ═════════════════════════════════════════════════════════════
with tab4:
    st.markdown('<div class="section-header">⚙️ Pipeline Execution History</div>', unsafe_allow_html=True)
    audit_df = fetch_pipeline_runs()

    if not audit_df.empty:
        # ── KPI Row ──
        k1, k2, k3, k4 = st.columns(4)

        total_runs = len(audit_df)
        success_runs = len(audit_df[audit_df['status'] == 'SUCCESS'])
        success_rate = (success_runs / total_runs) * 100 if total_runs > 0 else 0
        avg_duration = audit_df['duration_seconds'].mean() if 'duration_seconds' in audit_df.columns else 0
        total_records = audit_df['records_fetched'].sum() if 'records_fetched' in audit_df.columns else 0

        rate_color = COLORS["positive"] if success_rate >= 80 else COLORS["warning"] if success_rate >= 50 else COLORS["negative"]

        with k1:
            st.markdown(f"""
            <div class="metric-card">
                <div class="label">Success Rate</div>
                <div class="value" style="color:{rate_color}">{success_rate:.0f}%</div>
                <div class="sub" style="color:#64748b">{success_runs}/{total_runs} runs</div>
            </div>
            """, unsafe_allow_html=True)

        with k2:
            st.markdown(f"""
            <div class="metric-card">
                <div class="label">Avg Duration</div>
                <div class="value">{avg_duration:.0f}s</div>
                <div class="sub" style="color:#64748b">per pipeline run</div>
            </div>
            """, unsafe_allow_html=True)

        with k3:
            st.markdown(f"""
            <div class="metric-card">
                <div class="label">Total Records</div>
                <div class="value">{total_records:,}</div>
                <div class="sub" style="color:#64748b">fetched across all runs</div>
            </div>
            """, unsafe_allow_html=True)

        with k4:
            last_run = audit_df.iloc[0]
            last_status = last_run.get('status', 'UNKNOWN')
            badge_class = "status-success" if last_status == "SUCCESS" else "status-failed"
            last_ts = pd.Timestamp(last_run.get('run_at'))
            st.markdown(f"""
            <div class="metric-card">
                <div class="label">Last Run</div>
                <div class="value" style="font-size:1rem;">{last_ts.strftime('%H:%M:%S') if pd.notnull(last_ts) else 'N/A'}</div>
                <div class="sub"><span class="status-badge {badge_class}">{last_status}</span></div>
            </div>
            """, unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)

        # ── Run History Table ──
        st.markdown('<div class="section-header">📋 Recent Runs</div>', unsafe_allow_html=True)

        display_df = audit_df.copy()
        if 'run_at' in display_df.columns:
            display_df['run_at'] = pd.to_datetime(display_df['run_at']).dt.strftime('%Y-%m-%d %H:%M:%S')
        if 'duration_seconds' in display_df.columns:
            display_df['duration_seconds'] = display_df['duration_seconds'].apply(lambda x: f"{x:.1f}s" if pd.notnull(x) else "N/A")

        st.dataframe(
            display_df,
            width="stretch",
            hide_index=True,
            column_config={
                "run_id": st.column_config.TextColumn("Run ID", width="medium"),
                "run_at": st.column_config.TextColumn("Timestamp", width="medium"),
                "status": st.column_config.TextColumn("Status", width="small"),
                "records_fetched": st.column_config.NumberColumn("Records", width="small"),
                "duration_seconds": st.column_config.TextColumn("Duration", width="small"),
            },
        )

        # ── Duration Chart ──
        if 'duration_seconds' in audit_df.columns:
            st.markdown('<div class="section-header">⏱️ Run Duration Trend</div>', unsafe_allow_html=True)
            dur_df = audit_df.copy()
            dur_df['run_at'] = pd.to_datetime(dur_df['run_at'])
            dur_df = dur_df.sort_values('run_at')

            colors = [COLORS["positive"] if s == 'SUCCESS' else COLORS["negative"] for s in dur_df['status']]
            fig_dur = go.Figure()
            fig_dur.add_trace(go.Bar(
                x=dur_df['run_at'], y=dur_df['duration_seconds'],
                marker_color=colors,
                hovertemplate="Duration: %{y:.1f}s<br>%{x}<extra></extra>",
            ))
            fig_dur.update_layout(
                height=300, showlegend=False,
                xaxis_title="", yaxis_title="Duration (seconds)",
            )
            make_plotly_dark(fig_dur)
            st.plotly_chart(fig_dur, width="stretch")
    else:
        st.info("No pipeline runs logged yet. Trigger the DAG from Airflow to generate data.")
