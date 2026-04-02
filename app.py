"""
OS v2.0 — Poker Operating System
Streamlit App — Interfaz limpia para análisis post-sesión
"""

import streamlit as st
import tempfile, os, sys, io, contextlib, json
from pathlib import Path

# ── Page config ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="OS v2.0 — Poker OS",
    page_icon="♠",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── CSS personalizado ────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700&family=Inter:wght@300;400;500;600&display=swap');

/* Base */
html, body, [class*="css"] {
    font-family: 'Inter', sans-serif;
}

/* Header */
.os-header {
    background: linear-gradient(135deg, #0a0a0a 0%, #1a1a2e 100%);
    border: 1px solid #2d2d4e;
    border-radius: 12px;
    padding: 24px 32px;
    margin-bottom: 24px;
    display: flex;
    align-items: center;
    gap: 16px;
}
.os-header h1 {
    font-family: 'JetBrains Mono', monospace;
    font-size: 1.8rem;
    font-weight: 700;
    color: #e8e8f0;
    margin: 0;
    letter-spacing: -0.5px;
}
.os-header .subtitle {
    font-size: 0.85rem;
    color: #6b6b8a;
    margin: 4px 0 0 0;
    font-weight: 400;
}
.os-badge {
    background: #1e3a5f;
    color: #60a5fa;
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.75rem;
    padding: 4px 10px;
    border-radius: 20px;
    border: 1px solid #2d5a8e;
    white-space: nowrap;
}

/* Metric cards */
.metric-card {
    background: #0f0f1a;
    border: 1px solid #1e1e3a;
    border-radius: 10px;
    padding: 16px 20px;
    transition: border-color 0.2s;
}
.metric-card:hover { border-color: #3d3d6e; }
.metric-label {
    font-size: 0.75rem;
    font-weight: 500;
    color: #6b6b8a;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    margin-bottom: 6px;
}
.metric-value {
    font-family: 'JetBrains Mono', monospace;
    font-size: 1.6rem;
    font-weight: 700;
    line-height: 1;
}
.metric-value.green  { color: #22c55e; }
.metric-value.red    { color: #ef4444; }
.metric-value.yellow { color: #f59e0b; }
.metric-value.neutral{ color: #e8e8f0; }
.metric-sub {
    font-size: 0.78rem;
    color: #4b4b6a;
    margin-top: 4px;
}

/* Section headers */
.section-title {
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.8rem;
    font-weight: 600;
    color: #4b4b8a;
    text-transform: uppercase;
    letter-spacing: 0.12em;
    border-bottom: 1px solid #1e1e3a;
    padding-bottom: 8px;
    margin: 24px 0 16px 0;
}

/* Leak / opportunity rows */
.leak-row {
    background: #0f0f1a;
    border-left: 3px solid #ef4444;
    border-radius: 0 8px 8px 0;
    padding: 12px 16px;
    margin-bottom: 8px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.82rem;
}
.opp-row {
    background: #0f1a0f;
    border-left: 3px solid #22c55e;
    border-radius: 0 8px 8px 0;
    padding: 12px 16px;
    margin-bottom: 8px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.82rem;
}
.leak-spot { color: #a78bfa; font-weight: 600; }
.leak-ev   { color: #ef4444; float: right; }
.opp-ev    { color: #22c55e; float: right; }
.leak-meta { color: #4b4b6a; font-size: 0.75rem; margin-top: 4px; }

/* Drill card */
.drill-card {
    background: linear-gradient(135deg, #0f1629 0%, #0a0f1e 100%);
    border: 1px solid #1e3a5f;
    border-radius: 12px;
    padding: 20px 24px;
}
.drill-title {
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.75rem;
    color: #60a5fa;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    margin-bottom: 8px;
}
.drill-spot {
    font-family: 'JetBrains Mono', monospace;
    font-size: 1rem;
    color: #e8e8f0;
    font-weight: 600;
    margin-bottom: 12px;
}
.drill-trigger {
    font-size: 0.85rem;
    color: #94a3b8;
    margin-bottom: 6px;
}
.drill-action {
    font-size: 0.85rem;
    color: #22c55e;
    font-weight: 500;
}

/* Progress bar custom */
.progress-container {
    background: #1e1e3a;
    border-radius: 4px;
    height: 6px;
    margin: 8px 0;
    overflow: hidden;
}
.progress-fill {
    height: 100%;
    border-radius: 4px;
    background: linear-gradient(90deg, #3b82f6, #8b5cf6);
    transition: width 0.3s ease;
}

/* Pool exploit rows */
.exploit-row {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 8px 12px;
    border-radius: 6px;
    margin-bottom: 4px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.8rem;
}
.exploit-red    { background: #1a0f0f; border-left: 2px solid #ef4444; }
.exploit-yellow { background: #1a160a; border-left: 2px solid #f59e0b; }
.exploit-green  { background: #0a1a0f; border-left: 2px solid #22c55e; }

/* Session table */
.session-row {
    display: flex;
    gap: 8px;
    align-items: center;
    padding: 6px 10px;
    border-radius: 6px;
    margin-bottom: 3px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.78rem;
    background: #0a0a14;
    border: 1px solid #15152a;
}

/* Uploader zone */
.upload-zone {
    border: 2px dashed #2d2d4e;
    border-radius: 12px;
    padding: 32px;
    text-align: center;
    background: #05050f;
    transition: border-color 0.2s;
}

/* Sidebar */
section[data-testid="stSidebar"] {
    background: #05050f;
    border-right: 1px solid #1e1e3a;
}

/* Remove Streamlit default padding excess */
.main .block-container { padding-top: 1rem; }
</style>
""", unsafe_allow_html=True)


# ── Cargar librería OS ───────────────────────────────────────────────────────
@st.cache_resource(show_spinner="Cargando OS v2.0...")
def load_os_library():
    """Carga todo el código del OS v2.0 en el namespace."""
    import importlib.util, types, unittest.mock as mock
    import pandas as pd, numpy as np, random
    from datetime import datetime, timedelta

    g = {
        'pd': pd, 'np': np, 'random': random,
        'datetime': datetime, 'timedelta': timedelta,
        '__builtins__': __builtins__,
    }

    # Mock Colab/Google
    import sys
    sys.modules['google'] = mock.MagicMock()
    sys.modules['google.colab'] = mock.MagicMock()

    lib_path = Path(__file__).parent / 'os_library.py'
    if not lib_path.exists():
        return None, "❌ os_library.py no encontrado."

    try:
        with open(lib_path, 'r', encoding='utf-8') as f:
            code = f.read()
        exec(code, g)
        return g, None
    except Exception as e:
        return None, f"❌ Error cargando librería: {e}"


def run_pipeline(hh_path: str, friccion_r: int, friccion_a: int, friccion_v: int,
                 num_tables: int, hero_name: str, g: dict):
    """Ejecuta el pipeline completo y devuelve los resultados."""
    import pandas as pd

    # Capture stdout
    output_buf = io.StringIO()
    results = {}

    parse_fn = g.get('parse_real_hand_history_file')
    if parse_fn is None:
        return None, "❌ Parser no disponible."

    # 1. Parse
    df = parse_fn(hh_path, hero=hero_name)
    if df.empty:
        return None, "❌ No se encontraron manos cash en el archivo."

    # 2. Board texture
    if 'enrich_df_with_board_texture' in g:
        df = g['enrich_df_with_board_texture'](df)

    # 3. Spot identifier
    if 'build_spot_identifier' in g:
        df = g['build_spot_identifier'](df)

    # 4. Inject friction
    current_session_id = df['session_id'].iloc[-1] if 'session_id' in df.columns else 'session_001'
    mask = df['session_id'] == current_session_id
    df.loc[mask, 'friccion_r'] = friccion_r
    df.loc[mask, 'friccion_a'] = friccion_a
    df.loc[mask, 'friccion_v'] = friccion_v
    df['num_tables'] = num_tables

    # 5. Pool classifier
    if 'classify_opponent_pool' in g:
        try:
            _, df = g['classify_opponent_pool'](df, hero=hero_name)
        except:
            df['opp_class'] = 'unknown'

    # 6. Core metrics
    overall_metrics, spot_results = {}, pd.DataFrame()
    if 'calculate_ev_metrics' in g:
        overall_metrics, spot_results = g['calculate_ev_metrics'](
            df, current_session_id=current_session_id
        )

    hand_count = len(df)

    # 7. Friction avg
    friccion_avg = 1.0
    if 'calculate_friccion_avg' in g:
        friccion_avg = g['calculate_friccion_avg'](df)

    # 8. Mode
    current_mode = 'M1'
    if 'determine_operating_mode' in g:
        current_mode = g['determine_operating_mode'](overall_metrics, friccion_avg, hand_count)

    # 9. ROI ranking
    roi_ranking = {}
    if 'build_roi_ranking' in g and not spot_results.empty:
        roi_ranking = g['build_roi_ranking'](spot_results, top_n=10)

    # 10. M5 Pool detector
    m5_result = {}
    if 'run_m5_pool_detector' in g:
        try:
            m5_result = g['run_m5_pool_detector'](df, hand_count=hand_count)
        except:
            pass

    # 11. Tilt
    tilt_result = {}
    if 'detect_tilt_sessions' in g:
        try:
            tilt_result = g['detect_tilt_sessions'](df)
        except:
            pass

    # 12. Session metrics
    sess_df = df[df['session_id'] == current_session_id]
    session_net = sess_df['net_won'].sum() if 'net_won' in sess_df.columns else 0
    session_hands = len(sess_df)

    # 13. Speed
    speed_result = {}
    if 'estimate_preflop_speed' in g:
        try:
            speed_result = g['estimate_preflop_speed'](df, num_tables=num_tables)
        except:
            pass

    return {
        'df': df,
        'overall_metrics': overall_metrics,
        'spot_results': spot_results,
        'roi_ranking': roi_ranking,
        'm5_result': m5_result,
        'tilt_result': tilt_result,
        'current_mode': current_mode,
        'current_session_id': current_session_id,
        'session_net': session_net,
        'session_hands': session_hands,
        'hand_count': hand_count,
        'friccion_avg': friccion_avg,
        'speed_result': speed_result,
        'g': g,
    }, None


# ── Helpers de display ───────────────────────────────────────────────────────
def color_class(val, positive_good=True):
    if val is None: return 'neutral'
    try: val = float(val)
    except: return 'neutral'
    if positive_good:
        return 'green' if val > 0 else ('red' if val < 0 else 'neutral')
    else:
        return 'red' if val > 0 else ('green' if val < 0 else 'neutral')


def fmt_bb100(val):
    try:
        v = float(val)
        sign = '+' if v > 0 else ''
        return f"{sign}{v:.1f}"
    except:
        return 'N/A'


def fmt_evh(val):
    try:
        v = float(val)
        sign = '+' if v > 0 else ''
        return f"{sign}{v:.2f}€/h"
    except:
        return 'N/A'


def semaforo_dot(val, positive_good=True):
    try:
        v = float(val)
        if positive_good:
            return '🟢' if v > 0 else ('🔴' if v < 0 else '🟡')
        else:
            return '🔴' if v > 0 else ('🟢' if v < 0 else '🟡')
    except:
        return '⚪'


# ════════════════════════════════════════════════════════════════════════════
# LAYOUT PRINCIPAL
# ════════════════════════════════════════════════════════════════════════════

# ── Header ───────────────────────────────────────────────────────────────────
st.markdown("""
<div class="os-header">
    <div>
        <h1>♠ OS v2.0</h1>
        <p class="subtitle">Poker Operating System · LaRuinaDeMago · NL2</p>
    </div>
    <span class="os-badge">v2.05</span>
</div>
""", unsafe_allow_html=True)

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### ⚙️ Configuración de sesión")

    hero_name = st.text_input("Nick PokerStars", value="LaRuinaDeMago")

    st.markdown("---")
    st.markdown("**🎚️ Fricción post-sesión** *(1=nada · 5=mucho)*")
    friccion_r = st.slider("🔴 Rabia",     1, 5, 2)
    friccion_a = st.slider("🟠 Ansiedad",  1, 5, 1)
    friccion_v = st.slider("🟡 Varianza",  1, 5, 2)

    fric_avg = round((friccion_r + friccion_a + friccion_v) / 3, 2)
    fric_color = "#22c55e" if fric_avg <= 2 else ("#f59e0b" if fric_avg <= 3 else "#ef4444")
    fric_label = "🟢 VERDE" if fric_avg <= 2 else ("🟡 AMARILLO" if fric_avg <= 3 else "🔴 STOP")
    st.markdown(f"""
    <div style="background:#0f0f1a;border-radius:8px;padding:10px 14px;margin-top:8px;
                border:1px solid #1e1e3a;font-family:'JetBrains Mono',monospace;font-size:0.85rem;">
        Promedio: <span style="color:{fric_color};font-weight:700;">{fric_avg:.2f}</span>
        <span style="color:#4b4b6a;margin-left:8px;">{fric_label}</span>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("---")
    num_tables = st.selectbox("🎮 Mesas simultáneas", [1, 2, 3, 4], index=1)

    st.markdown("---")
    st.markdown("**📂 Hand History**")
    uploaded_file = st.file_uploader(
        "Sube tu .txt de PokerStars",
        type=['txt'],
        help="Exporta desde PokerStars → Historial de manos"
    )

    run_btn = st.button("▶ Ejecutar análisis", type="primary", use_container_width=True,
                        disabled=uploaded_file is None)

# ── Estado de sesión ──────────────────────────────────────────────────────────
if 'results' not in st.session_state:
    st.session_state.results = None
if 'error' not in st.session_state:
    st.session_state.error = None

# ── Carga librería ────────────────────────────────────────────────────────────
g, lib_error = load_os_library()
if lib_error:
    st.error(lib_error)
    st.stop()

# ── Ejecutar pipeline ─────────────────────────────────────────────────────────
if run_btn and uploaded_file is not None:
    with tempfile.NamedTemporaryFile(suffix='.txt', delete=False, mode='wb') as tmp:
        tmp.write(uploaded_file.read())
        tmp_path = tmp.name

    with st.spinner("Analizando tus manos..."):
        results, err = run_pipeline(
            hh_path=tmp_path,
            friccion_r=friccion_r,
            friccion_a=friccion_a,
            friccion_v=friccion_v,
            num_tables=num_tables,
            hero_name=hero_name,
            g=g,
        )
    os.unlink(tmp_path)

    if err:
        st.session_state.error = err
        st.session_state.results = None
    else:
        st.session_state.results = results
        st.session_state.error = None

# ── Error state ───────────────────────────────────────────────────────────────
if st.session_state.error:
    st.error(st.session_state.error)

# ── Empty state ───────────────────────────────────────────────────────────────
if st.session_state.results is None:
    st.markdown("""
    <div style="text-align:center;padding:60px 20px;color:#3d3d6e;">
        <div style="font-size:3rem;margin-bottom:16px;">♠</div>
        <div style="font-family:'JetBrains Mono',monospace;font-size:1rem;color:#4b4b8a;">
            Sube tu .txt de PokerStars y pulsa Ejecutar análisis
        </div>
        <div style="font-size:0.8rem;color:#2d2d4e;margin-top:8px;">
            PokerStars → Historial de manos → Exportar
        </div>
    </div>
    """, unsafe_allow_html=True)
    st.stop()

# ════════════════════════════════════════════════════════════════════════════
# DASHBOARD — Resultados
# ════════════════════════════════════════════════════════════════════════════
R = st.session_state.results
om = R['overall_metrics']
df = R['df']
roi = R['roi_ranking']
m5 = R['m5_result']
hand_count = R['hand_count']

# ── KPIs principales ──────────────────────────────────────────────────────────
st.markdown('<div class="section-title">📊 Métricas globales</div>', unsafe_allow_html=True)

bb100      = om.get('bb_per_100_net', 0)
evh        = om.get('ev_h', 0)
total_net  = df['net_won'].sum() if 'net_won' in df.columns else 0
sess_net   = R['session_net']
sess_hands = R['session_hands']
n_sessions = df['session_id'].nunique() if 'session_id' in df.columns else 1
speed      = R['speed_result'].get('hands_per_hour', 0)
mode       = R['current_mode']

col1, col2, col3, col4, col5, col6 = st.columns(6)

with col1:
    c = color_class(bb100)
    st.markdown(f"""<div class="metric-card">
        <div class="metric-label">BB/100 global</div>
        <div class="metric-value {c}">{fmt_bb100(bb100)}</div>
        <div class="metric-sub">{hand_count:,} manos</div>
    </div>""", unsafe_allow_html=True)

with col2:
    c = color_class(evh)
    st.markdown(f"""<div class="metric-card">
        <div class="metric-label">EV €/hora</div>
        <div class="metric-value {c}">{fmt_evh(evh)}</div>
        <div class="metric-sub">{n_sessions} sesiones</div>
    </div>""", unsafe_allow_html=True)

with col3:
    c = color_class(total_net)
    sign = '+' if total_net > 0 else ''
    st.markdown(f"""<div class="metric-card">
        <div class="metric-label">Net total</div>
        <div class="metric-value {c}">{sign}{total_net:.2f}€</div>
        <div class="metric-sub">acumulado</div>
    </div>""", unsafe_allow_html=True)

with col4:
    c = color_class(sess_net)
    sign = '+' if sess_net > 0 else ''
    st.markdown(f"""<div class="metric-card">
        <div class="metric-label">Sesión actual</div>
        <div class="metric-value {c}">{sign}{sess_net:.2f}€</div>
        <div class="metric-sub">{sess_hands} manos</div>
    </div>""", unsafe_allow_html=True)

with col5:
    speed_c = 'green' if 70 <= speed <= 110 else ('yellow' if speed > 0 else 'neutral')
    st.markdown(f"""<div class="metric-card">
        <div class="metric-label">Velocidad</div>
        <div class="metric-value {speed_c}">{speed:.0f}</div>
        <div class="metric-sub">manos/hora</div>
    </div>""", unsafe_allow_html=True)

with col6:
    mode_c = {'M1': '#60a5fa', 'M2': '#a78bfa', 'M3': '#22c55e'}.get(mode, '#6b6b8a')
    st.markdown(f"""<div class="metric-card">
        <div class="metric-label">Modo OS</div>
        <div class="metric-value" style="color:{mode_c};">{mode}</div>
        <div class="metric-sub">fricción {R['friccion_avg']:.2f}</div>
    </div>""", unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ── Progreso hacia M2 ─────────────────────────────────────────────────────────
m2_gate = 30000
pct_m2 = min(hand_count / m2_gate * 100, 100)
st.markdown(f"""
<div style="background:#0a0a14;border:1px solid #1e1e3a;border-radius:10px;padding:14px 18px;margin-bottom:20px;">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;">
        <span style="font-family:'JetBrains Mono',monospace;font-size:0.75rem;color:#4b4b8a;text-transform:uppercase;letter-spacing:0.1em;">
            Progreso hacia M2
        </span>
        <span style="font-family:'JetBrains Mono',monospace;font-size:0.8rem;color:#60a5fa;">
            {hand_count:,} / {m2_gate:,} manos — {pct_m2:.1f}%
        </span>
    </div>
    <div class="progress-container">
        <div class="progress-fill" style="width:{pct_m2}%;"></div>
    </div>
    <div style="font-size:0.75rem;color:#3d3d6e;margin-top:4px;">
        Gate M2: ≥30.000 manos + BB/100 > 0 + EV/h > 0 + fricción ≤ 2
    </div>
</div>
""", unsafe_allow_html=True)

# ── Tabs principales ──────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "🎯 Drill activo",
    "📉 Leaks & ROI",
    "🌊 Pool (M5)",
    "📈 Sesiones",
    "🔢 Stats detalle",
])

# ════════════════════════════════════════════════════════════════════════════
# TAB 1 — DRILL ACTIVO
# ════════════════════════════════════════════════════════════════════════════
with tab1:
    # Get drill from ROI
    drill_activo = None
    drill_info = {}
    if roi and roi.get('leaks'):
        top_leak = roi['leaks'][0] if roi['leaks'] else None
        if top_leak:
            drill_activo = top_leak.get('spot_identifier', '')

    col_d1, col_d2 = st.columns([3, 2])

    with col_d1:
        st.markdown('<div class="section-title">🎯 Drill activo</div>', unsafe_allow_html=True)

        if drill_activo:
            # Parse drill info from registry if available
            registry = g.get('DRILL_REGISTRY', {})
            drill_data = registry.get(drill_activo, {}) if registry else {}

            trigger = drill_data.get('trigger', 'Detectado automáticamente por ROI ranking')
            action  = drill_data.get('action',  'Ver análisis de leaks para instrucción específica')
            level   = drill_data.get('level',   'level_1')

            st.markdown(f"""
            <div class="drill-card">
                <div class="drill-title">🎯 Drill primario — {level}</div>
                <div class="drill-spot">{drill_activo}</div>
                <div class="drill-trigger"><b>Trigger:</b> {trigger}</div>
                <div class="drill-action"><b>Acción:</b> {action}</div>
            </div>
            """, unsafe_allow_html=True)
        else:
            st.info("Sin drill activo — acumula más manos para señal estadística.")

        # Reglas paralelas
        st.markdown('<div class="section-title">📌 Reglas paralelas</div>', unsafe_allow_html=True)

        # Compute SB stats
        sb_df = df[df['player_position'] == 'SB'] if 'player_position' in df.columns else None
        bb_df = df[df['player_position'] == 'BB'] if 'player_position' in df.columns else None

        sb_vpip = sb_df['flg_vpip'].mean() * 100 if sb_df is not None and 'flg_vpip' in sb_df.columns and len(sb_df) > 0 else 0
        sb_limp  = sb_df['flg_p_limp'].mean() * 100 if sb_df is not None and 'flg_p_limp' in sb_df.columns and len(sb_df) > 0 else 0
        bb_vpip = bb_df['flg_vpip'].mean() * 100 if bb_df is not None and 'flg_vpip' in bb_df.columns and len(bb_df) > 0 else 0

        sb_ok = sb_vpip <= 40 and sb_limp < 5
        bb_ok = bb_vpip >= 45

        reglas = [
            ("SB: NUNCA limp. Solo raise o fold.",
             f"VPIP {sb_vpip:.1f}% · Limp {sb_limp:.1f}%",
             sb_ok),
            ("BB: Defender amplio. Suited siempre call.",
             f"VPIP {bb_vpip:.1f}% (ref ≥55%)",
             bb_ok),
        ]

        for regla, stat, ok in reglas:
            dot = '🟢' if ok else '🔴'
            st.markdown(f"""
            <div style="background:#0a0a14;border:1px solid #1e1e3a;border-radius:8px;
                        padding:10px 14px;margin-bottom:8px;font-size:0.85rem;">
                {dot} <b style="color:#e8e8f0;">{regla}</b><br>
                <span style="color:#4b4b6a;font-family:'JetBrains Mono',monospace;font-size:0.78rem;">
                    {stat}
                </span>
            </div>
            """, unsafe_allow_html=True)

    with col_d2:
        st.markdown('<div class="section-title">📚 Tareas de estudio (M1)</div>', unsafe_allow_html=True)

        study_tasks = []
        if 'develop_canalized_study_module_logic' in g and not R['spot_results'].empty:
            try:
                tasks_result = g['develop_canalized_study_module_logic'](
                    R['spot_results'], R['current_mode'], roi_ranking=roi
                )
                if tasks_result and 'tasks' in tasks_result:
                    study_tasks = tasks_result['tasks']
            except:
                pass

        if study_tasks:
            for i, task in enumerate(study_tasks[:3], 1):
                desc = task if isinstance(task, str) else task.get('description', str(task))
                st.markdown(f"""
                <div style="background:#0f0f1a;border:1px solid #1e1e3a;border-radius:8px;
                            padding:10px 14px;margin-bottom:8px;">
                    <span style="color:#60a5fa;font-family:'JetBrains Mono',monospace;
                                 font-size:0.75rem;font-weight:600;">DRILL #{i}</span><br>
                    <span style="color:#94a3b8;font-size:0.82rem;">{desc[:180]}</span>
                </div>
                """, unsafe_allow_html=True)
        else:
            for i, task in enumerate([
                "Rangos preflop: repasa aperturas por posición (BTN/CO/MP/UTG). Foco en SRP. (10-15 min)",
                "Equity tables: elige 2-3 situaciones de tu última sesión. Calcula equity vs rango estimado. (10 min)",
                "Evaluación de boards: clasifica los últimos 10 flops (favorable/neutro/peligroso). (10 min)",
            ], 1):
                st.markdown(f"""
                <div style="background:#0f0f1a;border:1px solid #1e1e3a;border-radius:8px;
                            padding:10px 14px;margin-bottom:8px;">
                    <span style="color:#60a5fa;font-family:'JetBrains Mono',monospace;
                                 font-size:0.75rem;font-weight:600;">DRILL #{i}</span><br>
                    <span style="color:#94a3b8;font-size:0.82rem;">{task}</span>
                </div>
                """, unsafe_allow_html=True)

        # Anti-tilt
        st.markdown('<div class="section-title">🧠 Anti-tilt</div>', unsafe_allow_html=True)
        tilt = R['tilt_result']
        n_tilt = tilt.get('n_tilt', 0) if tilt else 0
        n_sess = tilt.get('n_sessions', n_sessions) if tilt else n_sessions

        if n_tilt == 0:
            st.markdown("""
            <div style="background:#0a1a0a;border:1px solid #15381a;border-radius:8px;
                        padding:10px 14px;font-size:0.85rem;color:#4ade80;">
                🟢 Sin sesiones tilt detectadas ({} sesiones analizadas)
            </div>
            """.format(n_sess), unsafe_allow_html=True)
        else:
            st.markdown(f"""
            <div style="background:#1a0a0a;border:1px solid #381515;border-radius:8px;
                        padding:10px 14px;font-size:0.85rem;color:#f87171;">
                🔴 {n_tilt} sesión(es) con señal tilt de {n_sess} analizadas
            </div>
            """, unsafe_allow_html=True)


# ════════════════════════════════════════════════════════════════════════════
# TAB 2 — LEAKS & ROI
# ════════════════════════════════════════════════════════════════════════════
with tab2:
    col_l1, col_l2 = st.columns([3, 2])

    with col_l1:
        st.markdown('<div class="section-title">🔴 Top leaks (ROI ranking)</div>', unsafe_allow_html=True)

        leaks = roi.get('leaks', []) if roi else []
        opps  = roi.get('opportunities', []) if roi else []

        if leaks:
            for i, leak in enumerate(leaks[:8], 1):
                spot = leak.get('spot_identifier', '?')
                ev   = leak.get('ev_shrunk', leak.get('ev_raw', 0))
                n    = leak.get('n', 0)
                fam  = leak.get('family', '')
                st.markdown(f"""
                <div class="leak-row">
                    <span style="color:#4b4b6a;font-size:0.72rem;">#{i}</span>
                    <span class="leak-spot"> {spot}</span>
                    <span class="leak-ev">{ev:.3f}€</span>
                    <div class="leak-meta">{n} manos{f' · {fam}' if fam else ''}</div>
                </div>
                """, unsafe_allow_html=True)
        else:
            st.info("Sin leaks identificados aún — acumula más manos.")

        # Familias
        families = roi.get('families', []) if roi else []
        if families:
            st.markdown('<div class="section-title">📦 Familias de leaks</div>', unsafe_allow_html=True)
            for fam in families:
                name = fam.get('family', '?')
                ev   = fam.get('ev_total', 0)
                n    = fam.get('n_hands', 0)
                desc = fam.get('description', '')
                dot  = '🔴' if ev < -0.2 else ('🟡' if ev < 0 else '⚪')
                st.markdown(f"""
                <div style="background:#0a0a14;border:1px solid #1e1e3a;border-radius:8px;
                            padding:10px 14px;margin-bottom:6px;font-family:'JetBrains Mono',
                            monospace;font-size:0.8rem;">
                    {dot} <b style="color:#e8e8f0;">{name}</b>
                    <span style="float:right;color:#ef4444;">{ev:.3f}€</span>
                    <div style="color:#4b4b6a;font-size:0.75rem;margin-top:3px;">
                        {n} manos · {desc}
                    </div>
                </div>
                """, unsafe_allow_html=True)

    with col_l2:
        st.markdown('<div class="section-title">🟢 Oportunidades</div>', unsafe_allow_html=True)
        if opps:
            for opp in opps[:5]:
                spot = opp.get('spot_identifier', '?')
                ev   = opp.get('ev_shrunk', opp.get('ev_raw', 0))
                n    = opp.get('n', 0)
                st.markdown(f"""
                <div class="opp-row">
                    <span class="leak-spot" style="color:#4ade80;">{spot}</span>
                    <span class="opp-ev">+{ev:.3f}€</span>
                    <div class="leak-meta">{n} manos</div>
                </div>
                """, unsafe_allow_html=True)
        else:
            st.info("Sin oportunidades con señal suficiente aún.")

        # Rangos referencia
        st.markdown('<div class="section-title">📐 Hero vs referencia NL2</div>', unsafe_allow_html=True)

        ref_stats = []
        positions = ['BTN', 'CO', 'HJ', 'UTG', 'SB', 'BB']
        ref_vpip  = {'BTN': 45, 'CO': 35, 'HJ': 28, 'UTG': 20, 'SB': 40, 'BB': 55}

        for pos in positions:
            pos_df = df[df['player_position'] == pos] if 'player_position' in df.columns else None
            if pos_df is not None and len(pos_df) >= 20 and 'flg_vpip' in pos_df.columns:
                vpip = pos_df['flg_vpip'].mean() * 100
                ref  = ref_vpip.get(pos, 30)
                gap  = vpip - ref
                dot  = '✅' if abs(gap) <= 7 else ('⚠️' if gap < 0 else '🟡')
                ref_stats.append((pos, vpip, ref, gap, dot))

        if ref_stats:
            for pos, vpip, ref, gap, dot in ref_stats:
                sign = '+' if gap >= 0 else ''
                st.markdown(f"""
                <div style="display:flex;justify-content:space-between;align-items:center;
                            padding:5px 10px;border-radius:5px;margin-bottom:3px;
                            background:#0a0a14;font-family:'JetBrains Mono',monospace;font-size:0.78rem;">
                    <span style="color:#94a3b8;width:40px;">{pos}</span>
                    <span style="color:#e8e8f0;">{vpip:.1f}%</span>
                    <span style="color:#4b4b6a;">ref {ref}%</span>
                    <span style="color:{'#22c55e' if abs(float(gap))<=7 else '#ef4444'};">
                        {sign}{gap:.1f}pp {dot}
                    </span>
                </div>
                """, unsafe_allow_html=True)


# ════════════════════════════════════════════════════════════════════════════
# TAB 3 — POOL M5
# ════════════════════════════════════════════════════════════════════════════
with tab3:
    st.markdown('<div class="section-title">🌊 Pool fingerprint — NL2</div>', unsafe_allow_html=True)

    exploits = m5.get('exploits', []) if m5 else []
    signals  = m5.get('signals', []) if m5 else []

    if not exploits and not signals:
        # Compute manually from df for display
        st.info(f"M5 activo · {hand_count:,} manos analizadas")

        # Show key pool stats directly
        stats_display = []

        if 'flg_f_cbet_def_opp' in df.columns and 'flg_f_cbet_def' in df.columns:
            opp = df['flg_f_cbet_def_opp'].sum()
            folds = opp - df['flg_f_cbet_def'].sum()
            if opp > 50:
                pct = folds / opp * 100
                stats_display.append(("Fold vs cbet flop", pct, 40, "CALL más vs cbet — pool llama mucho"))

        if 'flg_f_cbet_opp' in df.columns and 'flg_f_cbet' in df.columns:
            opp = df['flg_f_cbet_opp'].sum()
            did = df['flg_f_cbet'].sum()
            if opp > 50:
                pct = did / opp * 100
                stats_display.append(("CBet flop IP", pct, 62, "Expandir cbet range — pool over-folds"))

        if 'flg_p_fold' in df.columns:
            pf = df['flg_p_fold'].mean() * 100
            stats_display.append(("Fold preflop (hero)", pf, None, ""))

        for label, val, ref, tip in stats_display:
            if ref:
                diff = val - ref
                color_cls = 'exploit-red' if abs(diff) > 10 else 'exploit-yellow'
                dot = '🔴' if abs(diff) > 10 else '🟡'
            else:
                color_cls = 'exploit-green'
                dot = '⚪'

            st.markdown(f"""
            <div class="exploit-row {color_cls}">
                <span style="color:#94a3b8;">{dot} {label}</span>
                <span style="color:#e8e8f0;font-weight:600;">{val:.1f}%</span>
                {f'<span style="color:#4b4b6a;">ref {ref}%</span>' if ref else ''}
            </div>
            """, unsafe_allow_html=True)
            if tip:
                st.markdown(f"<div style='font-size:0.75rem;color:#3d5a6e;margin:-2px 0 6px 12px;'>→ {tip}</div>",
                            unsafe_allow_html=True)

    else:
        # Display M5 results
        for item in (exploits or signals)[:15]:
            spot_name = item.get('spot', item.get('name', '?'))
            obs  = item.get('observed_pct', item.get('obs_pct', 0))
            base = item.get('baseline_pct', item.get('base_pct', 0))
            n    = item.get('n', 0)
            tip  = item.get('exploit_tip', item.get('tip', ''))
            diff = obs - base
            dot  = '🔴' if abs(diff) > 10 else ('🟡' if abs(diff) > 5 else '✅')
            cls  = 'exploit-red' if abs(diff) > 10 else ('exploit-yellow' if abs(diff) > 5 else 'exploit-green')

            st.markdown(f"""
            <div class="exploit-row {cls}">
                <span style="color:#94a3b8;">{dot} {spot_name}</span>
                <div>
                    <span style="color:#e8e8f0;">{obs:.1f}%</span>
                    <span style="color:#4b4b6a;margin-left:8px;">base {base:.0f}%</span>
                    <span style="color:#3d5a6e;margin-left:8px;">n={n}</span>
                </div>
            </div>
            """, unsafe_allow_html=True)
            if tip:
                st.markdown(f"<div style='font-size:0.75rem;color:#3d5a6e;margin:-2px 0 6px 12px;'>→ {tip}</div>",
                            unsafe_allow_html=True)

    # Pool composition
    st.markdown('<div class="section-title">👥 Composición del pool</div>', unsafe_allow_html=True)
    if 'opp_class' in df.columns:
        comp = df.drop_duplicates('opponent_names' if 'opponent_names' in df.columns else df.columns[0])
        if 'opp_class' in comp.columns:
            counts = df['opp_class'].value_counts()
            total_opp = counts.sum()
            icons = {'fish': '🐟', 'maniac': '🤪', 'reg': '🎯', 'unknown': '❓'}
            cols = st.columns(len(counts))
            for i, (cls, cnt) in enumerate(counts.items()):
                pct = cnt / total_opp * 100
                icon = icons.get(cls, '❓')
                with cols[i]:
                    st.markdown(f"""<div class="metric-card" style="text-align:center;">
                        <div style="font-size:1.5rem;">{icon}</div>
                        <div class="metric-label">{cls}</div>
                        <div class="metric-value neutral" style="font-size:1.2rem;">{pct:.0f}%</div>
                        <div class="metric-sub">{cnt} oponentes</div>
                    </div>""", unsafe_allow_html=True)


# ════════════════════════════════════════════════════════════════════════════
# TAB 4 — SESIONES
# ════════════════════════════════════════════════════════════════════════════
with tab4:
    st.markdown('<div class="section-title">📈 Sesiones</div>', unsafe_allow_html=True)

    if 'session_id' in df.columns and 'net_won' in df.columns:
        import pandas as pd

        sess_summary = df.groupby('session_id', sort=True).agg(
            date=('date', 'first'),
            hands=('hand_id', 'count'),
            net=('net_won', 'sum'),
        ).reset_index()

        # Running cumulative
        sess_summary['cumulative'] = sess_summary['net'].cumsum()

        # Chart
        import plotly.graph_objects as go

        fig = go.Figure()
        colors = ['#22c55e' if x >= 0 else '#ef4444' for x in sess_summary['net']]

        fig.add_trace(go.Bar(
            x=sess_summary['session_id'],
            y=sess_summary['net'],
            marker_color=colors,
            name='Net sesión',
            opacity=0.8,
        ))
        fig.add_trace(go.Scatter(
            x=sess_summary['session_id'],
            y=sess_summary['cumulative'],
            line=dict(color='#60a5fa', width=2),
            name='Acumulado',
            yaxis='y2',
        ))

        fig.update_layout(
            plot_bgcolor='#0a0a14',
            paper_bgcolor='#0a0a14',
            font=dict(family='JetBrains Mono', color='#6b6b8a', size=11),
            xaxis=dict(gridcolor='#1e1e3a', tickangle=45),
            yaxis=dict(gridcolor='#1e1e3a', title='Net €'),
            yaxis2=dict(overlaying='y', side='right', title='Acumulado €',
                       gridcolor='transparent'),
            legend=dict(bgcolor='#0f0f1a', bordercolor='#1e1e3a'),
            margin=dict(l=40, r=40, t=20, b=60),
            height=300,
        )
        st.plotly_chart(fig, use_container_width=True)

        # Table
        st.markdown('<div class="section-title">📋 Detalle por sesión</div>', unsafe_allow_html=True)
        for _, row in sess_summary.iterrows():
            net_val = row['net']
            cum_val = row['cumulative']
            net_c = '#22c55e' if net_val >= 0 else '#ef4444'
            cum_c = '#22c55e' if cum_val >= 0 else '#ef4444'
            date_str = row['date'].strftime('%Y-%m-%d') if hasattr(row['date'], 'strftime') else str(row['date'])[:10]
            sign = '+' if net_val >= 0 else ''
            sign_c = '+' if cum_val >= 0 else ''

            st.markdown(f"""
            <div class="session-row">
                <span style="color:#4b4b6a;width:100px;">{row['session_id']}</span>
                <span style="color:#94a3b8;width:90px;">{date_str}</span>
                <span style="color:#6b6b8a;width:70px;">{row['hands']} manos</span>
                <span style="color:{net_c};width:80px;font-weight:600;">{sign}{net_val:.2f}€</span>
                <span style="color:{cum_c};width:80px;">cum: {sign_c}{cum_val:.2f}€</span>
            </div>
            """, unsafe_allow_html=True)


# ════════════════════════════════════════════════════════════════════════════
# TAB 5 — STATS DETALLE
# ════════════════════════════════════════════════════════════════════════════
with tab5:
    col_s1, col_s2 = st.columns(2)

    with col_s1:
        st.markdown('<div class="section-title">🔢 Stats por posición</div>', unsafe_allow_html=True)

        positions = ['BTN', 'CO', 'HJ', 'UTG', 'SB', 'BB']
        for pos in positions:
            pos_df = df[df['player_position'] == pos] if 'player_position' in df.columns else None
            if pos_df is None or len(pos_df) < 5:
                continue

            vpip = pos_df['flg_vpip'].mean() * 100 if 'flg_vpip' in pos_df.columns else 0
            pfr  = pos_df['flg_p_first_raise'].mean() * 100 if 'flg_p_first_raise' in pos_df.columns else 0
            net  = pos_df['net_won'].sum() if 'net_won' in pos_df.columns else 0
            n    = len(pos_df)
            bb100_pos = net / n * 100 / 0.02 if n > 0 else 0  # approx
            c = '#22c55e' if net > 0 else '#ef4444'

            st.markdown(f"""
            <div style="background:#0a0a14;border:1px solid #1e1e3a;border-radius:8px;
                        padding:10px 14px;margin-bottom:6px;font-family:'JetBrains Mono',
                        monospace;font-size:0.8rem;">
                <span style="color:#a78bfa;font-weight:600;width:40px;display:inline-block;">{pos}</span>
                <span style="color:#94a3b8;">VPIP {vpip:.1f}%</span>
                <span style="color:#6b6b8a;margin-left:12px;">PFR {pfr:.1f}%</span>
                <span style="color:{c};float:right;">
                    {'+'if net>=0 else ''}{net:.2f}€ ({n}m)
                </span>
            </div>
            """, unsafe_allow_html=True)

    with col_s2:
        st.markdown('<div class="section-title">📊 KPIs globales</div>', unsafe_allow_html=True)

        kpis = [
            ("VPIP global",         df['flg_vpip'].mean()*100 if 'flg_vpip' in df.columns else None,        "%", (20, 35)),
            ("PFR global",          df['flg_p_first_raise'].mean()*100 if 'flg_p_first_raise' in df.columns else None, "%", (15, 28)),
            ("3-bet %",             df['flg_p_3bet'].sum()/max(df['flg_p_3bet_opp'].sum(),1)*100 if 'flg_p_3bet' in df.columns else None, "%", (5, 12)),
            ("CBet flop IP",        df['flg_f_cbet'].sum()/max(df['flg_f_cbet_opp'].sum(),1)*100 if 'flg_f_cbet' in df.columns else None, "%", (55, 70)),
            ("Fold to CBet",        (df['flg_f_cbet_def_opp'].sum()-df['flg_f_cbet_def'].sum())/max(df['flg_f_cbet_def_opp'].sum(),1)*100 if 'flg_f_cbet_def_opp' in df.columns else None, "%", (27, 45)),
            ("WTSD%",               df['flg_showdown'].sum()/max(df['flg_f_saw'].sum(),1)*100 if 'flg_showdown' in df.columns else None, "%", (25, 32)),
            ("W$SD%",               df[df['flg_showdown']==True]['flg_won_hand'].mean()*100 if 'flg_showdown' in df.columns and df['flg_showdown'].sum()>0 else None, "%", (48, 56)),
        ]

        for label, val, unit, ref_range in kpis:
            if val is None:
                continue
            lo, hi = ref_range
            in_range = lo <= val <= hi
            dot = '✅' if in_range else ('⬆️' if val < lo else '⬇️')
            c = '#22c55e' if in_range else '#f59e0b'

            st.markdown(f"""
            <div style="display:flex;justify-content:space-between;align-items:center;
                        padding:7px 12px;border-radius:6px;margin-bottom:4px;
                        background:#0a0a14;border:1px solid #15152a;
                        font-family:'JetBrains Mono',monospace;font-size:0.8rem;">
                <span style="color:#94a3b8;">{label}</span>
                <span style="color:{c};font-weight:600;">{val:.1f}{unit} {dot}</span>
                <span style="color:#3d3d5e;">ref {lo}-{hi}{unit}</span>
            </div>
            """, unsafe_allow_html=True)

        # Red/Blue line
        st.markdown('<div class="section-title">📉 Red line / Blue line</div>', unsafe_allow_html=True)

        if 'flg_showdown' in df.columns and 'net_won' in df.columns:
            sd_hands = df[df['flg_showdown'] == True]
            non_sd   = df[df['flg_showdown'] == False]
            n_total  = len(df)

            blue_net = sd_hands['net_won'].sum()
            red_net  = non_sd['net_won'].sum()
            blue_bb  = blue_net / n_total * 100 / 0.02 if n_total > 0 else 0
            red_bb   = red_net  / n_total * 100 / 0.02 if n_total > 0 else 0

            for line, val_bb, val_net, color in [
                ("🔵 Blue line (showdown)", blue_bb, blue_net, '#60a5fa'),
                ("🔴 Red line (no-SD)",     red_bb,  red_net,  '#f87171'),
            ]:
                sign = '+' if val_bb >= 0 else ''
                st.markdown(f"""
                <div style="background:#0a0a14;border:1px solid #1e1e3a;border-radius:8px;
                            padding:10px 14px;margin-bottom:6px;
                            font-family:'JetBrains Mono',monospace;font-size:0.82rem;">
                    <span style="color:{color};">{line}</span>
                    <span style="float:right;color:{color};font-weight:600;">
                        {sign}{val_bb:.1f} BB/100 ({sign}{val_net:.2f}€)
                    </span>
                </div>
                """, unsafe_allow_html=True)

# ── Footer ───────────────────────────────────────────────────────────────────
st.markdown("---")
st.markdown("""
<div style="text-align:center;font-family:'JetBrains Mono',monospace;font-size:0.72rem;color:#2d2d4e;padding:8px;">
    OS v2.0 · LaRuinaDeMago · NL2 → NL25+ · El sistema mide, tú decides.
</div>
""", unsafe_allow_html=True)
