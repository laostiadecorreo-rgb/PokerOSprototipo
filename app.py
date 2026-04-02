"""
OS v2.0 — Poker Operating System  |  Streamlit App
v2.05-fix2: Plotly 6 fix + M4 Coach + SunChat + Progresión + Drill Guiado + Hole Cards
"""

import streamlit as st
import tempfile, os, sys, io, json, math
from pathlib import Path

st.set_page_config(page_title="OS v2.0 — Poker OS", page_icon="♠",
                   layout="wide", initial_sidebar_state="expanded")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700&family=Inter:wght@300;400;500;600&display=swap');
html,body,[class*="css"]{font-family:'Inter',sans-serif;}
.os-header{background:linear-gradient(135deg,#0a0a0a 0%,#1a1a2e 100%);border:1px solid #2d2d4e;border-radius:12px;padding:24px 32px;margin-bottom:24px;display:flex;align-items:center;gap:16px;}
.os-header h1{font-family:'JetBrains Mono',monospace;font-size:1.8rem;font-weight:700;color:#e8e8f0;margin:0;letter-spacing:-0.5px;}
.os-header .subtitle{font-size:0.85rem;color:#6b6b8a;margin:4px 0 0 0;}
.os-badge{background:#1e3a5f;color:#60a5fa;font-family:'JetBrains Mono',monospace;font-size:0.75rem;padding:4px 10px;border-radius:20px;border:1px solid #2d5a8e;white-space:nowrap;}
.metric-card{background:#0f0f1a;border:1px solid #1e1e3a;border-radius:10px;padding:16px 20px;transition:border-color .2s;}
.metric-card:hover{border-color:#3d3d6e;}
.metric-label{font-size:.75rem;font-weight:500;color:#6b6b8a;text-transform:uppercase;letter-spacing:.08em;margin-bottom:6px;}
.metric-value{font-family:'JetBrains Mono',monospace;font-size:1.6rem;font-weight:700;line-height:1;}
.metric-value.green{color:#22c55e;}.metric-value.red{color:#ef4444;}.metric-value.yellow{color:#f59e0b;}.metric-value.neutral{color:#e8e8f0;}
.metric-sub{font-size:.78rem;color:#4b4b6a;margin-top:4px;}
.section-title{font-family:'JetBrains Mono',monospace;font-size:.8rem;font-weight:600;color:#4b4b8a;text-transform:uppercase;letter-spacing:.12em;border-bottom:1px solid #1e1e3a;padding-bottom:8px;margin:24px 0 16px 0;}
.leak-row{background:#0f0f1a;border-left:3px solid #ef4444;border-radius:0 8px 8px 0;padding:12px 16px;margin-bottom:8px;font-family:'JetBrains Mono',monospace;font-size:.82rem;}
.opp-row{background:#0f1a0f;border-left:3px solid #22c55e;border-radius:0 8px 8px 0;padding:12px 16px;margin-bottom:8px;font-family:'JetBrains Mono',monospace;font-size:.82rem;}
.leak-spot{color:#a78bfa;font-weight:600;}.leak-ev{color:#ef4444;float:right;}.opp-ev{color:#22c55e;float:right;}.leak-meta{color:#4b4b6a;font-size:.75rem;margin-top:4px;}
.drill-card{background:linear-gradient(135deg,#0f1629 0%,#0a0f1e 100%);border:1px solid #1e3a5f;border-radius:12px;padding:20px 24px;}
.drill-title{font-family:'JetBrains Mono',monospace;font-size:.75rem;color:#60a5fa;text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;}
.drill-spot{font-family:'JetBrains Mono',monospace;font-size:1rem;color:#e8e8f0;font-weight:600;margin-bottom:12px;}
.drill-trigger{font-size:.85rem;color:#94a3b8;margin-bottom:6px;}.drill-action{font-size:.85rem;color:#22c55e;font-weight:500;}
.progress-container{background:#1e1e3a;border-radius:4px;height:6px;margin:8px 0;overflow:hidden;}
.progress-fill{height:100%;border-radius:4px;background:linear-gradient(90deg,#3b82f6,#8b5cf6);}
.exploit-row{display:flex;justify-content:space-between;align-items:center;padding:8px 12px;border-radius:6px;margin-bottom:4px;font-family:'JetBrains Mono',monospace;font-size:.8rem;}
.exploit-red{background:#1a0f0f;border-left:2px solid #ef4444;}.exploit-yellow{background:#1a160a;border-left:2px solid #f59e0b;}.exploit-green{background:#0a1a0f;border-left:2px solid #22c55e;}
.session-row{display:flex;gap:8px;align-items:center;padding:6px 10px;border-radius:6px;margin-bottom:3px;font-family:'JetBrains Mono',monospace;font-size:.78rem;background:#0a0a14;border:1px solid #15152a;}
.coach-box{background:#0a0f1e;border:1px solid #1e3a5f;border-radius:10px;padding:16px 20px;font-size:.85rem;color:#94a3b8;line-height:1.6;}
.coach-box b{color:#60a5fa;}
section[data-testid="stSidebar"]{background:#05050f;border-right:1px solid #1e1e3a;}
.main .block-container{padding-top:1rem;}
</style>
""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# CARGA LIBRERÍA
# ══════════════════════════════════════════════════════════════════════════════
@st.cache_resource(show_spinner="Cargando OS v2.0...")
def load_os_library():
    import unittest.mock as mock
    import pandas as pd, numpy as np, random, sqlite3
    from datetime import datetime, timedelta
    g = {'pd':pd,'np':np,'random':random,'datetime':datetime,'timedelta':timedelta,
         'sqlite3':sqlite3,'__builtins__':__builtins__}
    sys.modules['google']       = mock.MagicMock()
    sys.modules['google.colab'] = mock.MagicMock()
    lib_path = Path(__file__).parent / 'os_library.py'
    if not lib_path.exists():
        return None, "❌ os_library.py no encontrado."
    try:
        exec(open(lib_path, encoding='utf-8').read(), g)
        return g, None
    except Exception as e:
        return None, f"❌ Error cargando librería: {e}"


# ══════════════════════════════════════════════════════════════════════════════
# PIPELINE
# ══════════════════════════════════════════════════════════════════════════════
def run_pipeline(hh_path, friccion_r, friccion_a, friccion_v, num_tables, hero_name, g):
    import pandas as pd

    parse_fn = g.get('parse_real_hand_history_file')
    if not parse_fn:
        return None, "❌ Parser no disponible."

    df = parse_fn(hh_path, hero=hero_name)
    if df.empty:
        return None, "❌ No se encontraron manos cash."

    for fn_name in ['enrich_df_with_board_texture', 'build_spot_identifier']:
        if fn_name in g:
            df = g[fn_name](df)

    current_session_id = df['session_id'].iloc[-1] if 'session_id' in df.columns else 'session_001'
    mask = df['session_id'] == current_session_id
    for col, val in [('friccion_r', friccion_r), ('friccion_a', friccion_a), ('friccion_v', friccion_v)]:
        df.loc[mask, col] = val
    df['num_tables'] = num_tables

    if 'classify_opponent_pool' in g:
        try: _, df = g['classify_opponent_pool'](df, hero=hero_name)
        except: df['opp_class'] = 'unknown'

    overall_metrics, spot_results = {}, pd.DataFrame()
    if 'calculate_ev_metrics' in g:
        overall_metrics, spot_results = g['calculate_ev_metrics'](df, current_session_id=current_session_id)

    hand_count = len(df)

    friccion_avg = round((friccion_r + friccion_a + friccion_v) / 3, 2)
    if 'calculate_friccion_avg' in g:
        try:
            fa = g['calculate_friccion_avg'](df)
            if fa is not None and not math.isnan(float(fa)):
                friccion_avg = float(fa)
        except: pass

    current_mode = 'M1'
    if 'determine_operating_mode' in g:
        current_mode = g['determine_operating_mode'](overall_metrics, friccion_avg, hand_count)

    roi_ranking = {}
    if 'build_roi_ranking' in g and not spot_results.empty:
        roi_ranking = g['build_roi_ranking'](spot_results, top_n=10)

    m5_result = {}
    if 'run_m5_pool_detector' in g:
        try: m5_result = g['run_m5_pool_detector'](df, hand_count=hand_count)
        except: pass

    tilt_result = {}
    if 'detect_tilt_sessions' in g:
        try: tilt_result = g['detect_tilt_sessions'](df)
        except: pass

    speed_result = {}
    if 'estimate_preflop_speed' in g:
        try: speed_result = g['estimate_preflop_speed'](df, num_tables=num_tables)
        except: pass

    # Progression metrics
    progression = []
    if 'calculate_progression_metrics' in g:
        try: progression = g['calculate_progression_metrics'](df)
        except: pass

    # Build leak object for M4/SunChat
    leak_object = None
    if 'build_leak_object_from_roi' in g and roi_ranking:
        try: leak_object = g['build_leak_object_from_roi'](roi_ranking, df, top_n=1)
        except: pass

    sess_df = df[df['session_id'] == current_session_id]

    return {
        'df': df, 'overall_metrics': overall_metrics,
        'spot_results': spot_results, 'roi_ranking': roi_ranking,
        'm5_result': m5_result, 'tilt_result': tilt_result,
        'current_mode': current_mode, 'current_session_id': current_session_id,
        'session_net': sess_df['net_won'].sum() if 'net_won' in sess_df.columns else 0,
        'session_hands': len(sess_df),
        'hand_count': hand_count, 'friccion_avg': friccion_avg,
        'speed_result': speed_result, 'progression': progression,
        'leak_object': leak_object, 'g': g,
    }, None


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════
def cc(val, pos=True):
    try:
        v = float(val)
        if pos: return 'green' if v>0 else ('red' if v<0 else 'neutral')
        else:   return 'red'   if v>0 else ('green' if v<0 else 'neutral')
    except: return 'neutral'

def fbb(v):
    try: v=float(v); return f"{'+' if v>0 else ''}{v:.1f}"
    except: return 'N/A'

def fevh(v):
    try: v=float(v); return f"{'+' if v>0 else ''}{v:.2f}€/h"
    except: return 'N/A'

def card(label, value, sub, color_cls, extra_style=""):
    return f"""<div class="metric-card">
        <div class="metric-label">{label}</div>
        <div class="metric-value {color_cls}" {extra_style}>{value}</div>
        <div class="metric-sub">{sub}</div>
    </div>"""

def _df_to_rows(df_or_none):
    """FIX: DataFrames from build_roi_ranking — never eval as bool. Normalize cols."""
    import pandas as pd
    if df_or_none is None or not isinstance(df_or_none, pd.DataFrame) or df_or_none.empty:
        return []
    rows = []
    for _, r in df_or_none.iterrows():
        rows.append({
            'spot_identifier': r.get('spot_identifier', '?'),
            'ev_shrunk':       float(r.get('impacto_ev_total_eur_shrunk', 0)),
            'n':               int(r.get('spot_hands_count', 0)),
            'ip_oop':          r.get('ip_oop', ''),
            'pot_type':        r.get('pot_type', ''),
            'stack_depth':     r.get('stack_depth', ''),
            'decision_street': r.get('decision_street', ''),
            'tipo':            r.get('tipo', ''),
            'prioridad':       int(r.get('prioridad', 99)),
        })
    return rows

def _fam_to_rows(fd):
    if not fd or not isinstance(fd, dict): return []
    rows = [{'family':k,'ev_total':float(v.get('ev_combined',0)),
             'n_hands':int(v.get('n_combined',0)),'description':v.get('descripcion',''),
             'icon':v.get('icon','⚪'),'n_spots':int(v.get('n_spots',0))}
            for k,v in fd.items()]
    rows.sort(key=lambda x: x['ev_total'])
    return rows

def _safe_capture(fn, *args, **kwargs):
    """Run OS display function capturing stdout → return as string."""
    buf = io.StringIO()
    try:
        old = sys.stdout; sys.stdout = buf
        fn(*args, **kwargs)
        sys.stdout = old
    except Exception as e:
        sys.stdout = old
        return f"⚠️ {e}"
    return buf.getvalue()


# ══════════════════════════════════════════════════════════════════════════════
# HEADER + SIDEBAR
# ══════════════════════════════════════════════════════════════════════════════
st.markdown("""<div class="os-header">
    <div><h1>♠ OS v2.0</h1>
    <p class="subtitle">Poker Operating System · LaRuinaDeMago · NL2</p></div>
    <span class="os-badge">v2.05</span>
</div>""", unsafe_allow_html=True)

with st.sidebar:
    st.markdown("### ⚙️ Sesión")
    hero_name = st.text_input("Nick PokerStars", value="LaRuinaDeMago")
    st.markdown("---")
    st.markdown("**🎚️ Fricción** *(1=nada · 5=mucho)*")
    friccion_r = st.slider("🔴 Rabia",    1, 5, 2)
    friccion_a = st.slider("🟠 Ansiedad", 1, 5, 1)
    friccion_v = st.slider("🟡 Varianza", 1, 5, 2)
    fa = round((friccion_r+friccion_a+friccion_v)/3, 2)
    fc = "#22c55e" if fa<=2 else ("#f59e0b" if fa<=3 else "#ef4444")
    fl = "🟢 VERDE" if fa<=2 else ("🟡 AMARILLO" if fa<=3 else "🔴 STOP")
    st.markdown(f"""<div style="background:#0f0f1a;border-radius:8px;padding:10px 14px;margin-top:8px;
        border:1px solid #1e1e3a;font-family:'JetBrains Mono',monospace;font-size:.85rem;">
        Promedio: <span style="color:{fc};font-weight:700;">{fa:.2f}</span>
        <span style="color:#4b4b6a;margin-left:8px;">{fl}</span></div>""", unsafe_allow_html=True)
    st.markdown("---")
    num_tables = st.selectbox("🎮 Mesas", [1, 2, 3, 4], index=1)
    st.markdown("---")

    # M4 / SunChat API keys
    with st.expander("🤖 IA Coach (opcional)"):
        gemini_key = st.text_input("GEMINI_API_KEY", type="password",
                                    help="Gemini 2.0 Flash — gratuito")
        groq_key   = st.text_input("GROQ_API_KEY",   type="password",
                                    help="Groq Llama-3.3-70B — gratuito")
        m4_enabled = st.checkbox("Activar M4.4 Coach", value=bool(gemini_key))
        sc_enabled = st.checkbox("Activar SunChat", value=bool(groq_key))

    st.markdown("---")
    st.markdown("**📂 Hand History**")
    uploaded_file = st.file_uploader("Sube tu .txt de PokerStars", type=['txt'])
    run_btn = st.button("▶ Ejecutar análisis", type="primary",
                        use_container_width=True, disabled=uploaded_file is None)

for k in ['results','error','m4_output','sunchat_msgs']:
    if k not in st.session_state:
        st.session_state[k] = None if k != 'sunchat_msgs' else []

g, lib_error = load_os_library()
if lib_error: st.error(lib_error); st.stop()

if run_btn and uploaded_file:
    with tempfile.NamedTemporaryFile(suffix='.txt', delete=False, mode='wb') as tmp:
        tmp.write(uploaded_file.read()); tmp_path = tmp.name
    with st.spinner("Analizando manos..."):
        results, err = run_pipeline(tmp_path, friccion_r, friccion_a, friccion_v,
                                    num_tables, hero_name, g)
    os.unlink(tmp_path)
    if err:
        st.session_state.error = err; st.session_state.results = None
    else:
        st.session_state.results = results
        st.session_state.error   = None
        st.session_state.m4_output    = None
        st.session_state.sunchat_msgs = []

if st.session_state.error: st.error(st.session_state.error)

if st.session_state.results is None:
    st.markdown("""<div style="text-align:center;padding:60px 20px;color:#3d3d6e;">
        <div style="font-size:3rem;margin-bottom:16px;">♠</div>
        <div style="font-family:'JetBrains Mono',monospace;font-size:1rem;color:#4b4b8a;">
            Sube tu .txt de PokerStars y pulsa Ejecutar análisis</div>
        <div style="font-size:.8rem;color:#2d2d4e;margin-top:8px;">
            PokerStars → Historial de manos → Exportar</div>
    </div>""", unsafe_allow_html=True)
    st.stop()

# ──────────────────────────────────────────────────────────────────────────────
R = st.session_state.results
om, df, roi, m5 = R['overall_metrics'], R['df'], R['roi_ranking'], R['m5_result']
hand_count = R['hand_count']

leaks_list    = _df_to_rows(roi.get('leaks')         if roi else None)
opps_list     = _df_to_rows(roi.get('oportunidades') if roi else None)
families_list = _fam_to_rows(roi.get('families', {}) if roi else {})

# ══════════════════════════════════════════════════════════════════════════════
# KPIs PRINCIPALES
# ══════════════════════════════════════════════════════════════════════════════
st.markdown('<div class="section-title">📊 Métricas globales</div>', unsafe_allow_html=True)

bb100     = om.get('bb_per_100_net', 0)
evh       = om.get('ev_euro_per_hour', 0)   # FIX: clave correcta
total_net = df['net_won'].sum() if 'net_won' in df.columns else 0
sess_net  = R['session_net']
n_sess    = df['session_id'].nunique() if 'session_id' in df.columns else 1
speed     = R['speed_result'].get('hands_per_hour', 0)
mode      = R['current_mode']

c1,c2,c3,c4,c5,c6 = st.columns(6)
with c1: st.markdown(card("BB/100 global",  fbb(bb100),              f"{hand_count:,} manos",   cc(bb100)), unsafe_allow_html=True)
with c2: st.markdown(card("EV €/hora",      fevh(evh),               f"{n_sess} sesiones",       cc(evh)),  unsafe_allow_html=True)
with c3: st.markdown(card("Net total",      f"{'+' if total_net>=0 else ''}{total_net:.2f}€", "acumulado", cc(total_net)), unsafe_allow_html=True)
with c4: st.markdown(card("Sesión actual",  f"{'+' if sess_net>=0 else ''}{sess_net:.2f}€",   f"{R['session_hands']} manos", cc(sess_net)), unsafe_allow_html=True)
with c5:
    sp_c = 'green' if 70<=speed<=110 else ('yellow' if speed>0 else 'neutral')
    st.markdown(card("Velocidad", f"{speed:.0f}", "manos/hora", sp_c), unsafe_allow_html=True)
with c6:
    mc = {'M1':'#60a5fa','M2':'#a78bfa','M3':'#22c55e'}.get(mode,'#6b6b8a')
    st.markdown(card("Modo OS", mode, f"fricción {R['friccion_avg']:.2f}", "neutral",
                     f'style="color:{mc};"'), unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

pct_m2 = min(hand_count/30000*100, 100)
st.markdown(f"""<div style="background:#0a0a14;border:1px solid #1e1e3a;border-radius:10px;
    padding:14px 18px;margin-bottom:20px;">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;">
        <span style="font-family:'JetBrains Mono',monospace;font-size:.75rem;color:#4b4b8a;
            text-transform:uppercase;letter-spacing:.1em;">Progreso → M2</span>
        <span style="font-family:'JetBrains Mono',monospace;font-size:.8rem;color:#60a5fa;">
            {hand_count:,} / 30,000 — {pct_m2:.1f}%</span>
    </div>
    <div class="progress-container"><div class="progress-fill" style="width:{pct_m2}%;"></div></div>
    <div style="font-size:.75rem;color:#3d3d6e;margin-top:4px;">Gate M2: ≥30k manos + BB/100 > 0 + EV/h > 0 + fricción ≤ 2</div>
</div>""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# TABS
# ══════════════════════════════════════════════════════════════════════════════
tab1,tab2,tab3,tab4,tab5,tab6,tab7,tab8 = st.tabs([
    "🎯 Drill", "📉 Leaks & ROI", "🌊 Pool M5",
    "📈 Sesiones", "📊 Progresión", "🔢 Stats",
    "🤖 M4 Coach", "💬 SunChat",
])

# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — DRILL ACTIVO
# ══════════════════════════════════════════════════════════════════════════════
with tab1:
    drill_activo = leaks_list[0]['spot_identifier'] if leaks_list else None
    col_d1, col_d2 = st.columns([3,2])

    with col_d1:
        st.markdown('<div class="section-title">🎯 Drill activo</div>', unsafe_allow_html=True)
        if drill_activo:
            registry   = g.get('DRILL_REGISTRY', {})
            drill_data = (registry or {}).get(drill_activo, {})
            trigger = drill_data.get('trigger','Detectado por ROI ranking')
            action  = drill_data.get('action', 'Ver leaks para instrucción')
            level   = drill_data.get('level',  'level_1')
            st.markdown(f"""<div class="drill-card">
                <div class="drill-title">🎯 Drill primario — {level}</div>
                <div class="drill-spot">{drill_activo}</div>
                <div class="drill-trigger"><b>Trigger:</b> {trigger}</div>
                <div class="drill-action"><b>Acción:</b> {action}</div>
            </div>""", unsafe_allow_html=True)
        else:
            st.info("Sin drill activo — acumula más manos para señal.")

        # Drill guiado — manos representativas
        if drill_activo and 'get_representative_hands' in g:
            st.markdown('<div class="section-title">📋 Manos representativas del spot</div>', unsafe_allow_html=True)
            try:
                rep_df = g['get_representative_hands'](df, drill_activo, top_n=5)
                if not rep_df.empty:
                    for _, hrow in rep_df.head(5).iterrows():
                        date_s  = str(hrow.get('date',''))[:10]
                        hole    = hrow.get('hole_cards','??')
                        flop    = hrow.get('board_cards_flop','') or '—'
                        pf_act  = hrow.get('preflop_action','?')
                        net_h   = float(hrow.get('net_won',0))
                        ev_h    = float(hrow.get('ev_won',0))
                        nc      = '#22c55e' if net_h>=0 else '#ef4444'
                        st.markdown(f"""<div style="background:#0a0a14;border:1px solid #1e1e3a;
                            border-radius:8px;padding:8px 12px;margin-bottom:4px;
                            font-family:'JetBrains Mono',monospace;font-size:.78rem;">
                            <span style="color:#a78bfa;">{hole}</span>
                            <span style="color:#6b6b8a;margin-left:8px;">Flop: {flop}</span>
                            <span style="color:#94a3b8;margin-left:8px;">PF: {pf_act}</span>
                            <span style="color:{nc};float:right;">
                                {'+' if net_h>=0 else ''}{net_h:.3f}€ (EV {'+' if ev_h>=0 else ''}{ev_h:.3f}€)
                            </span>
                            <div style="color:#3d3d5e;font-size:.72rem;">{date_s}</div>
                        </div>""", unsafe_allow_html=True)
            except Exception as e:
                st.caption(f"Drill hands: {e}")

        st.markdown('<div class="section-title">📌 Reglas paralelas</div>', unsafe_allow_html=True)
        sb_df = df[df['player_position']=='SB'] if 'player_position' in df.columns else None
        bb_df = df[df['player_position']=='BB'] if 'player_position' in df.columns else None
        sb_vpip = sb_df['flg_vpip'].mean()*100 if sb_df is not None and 'flg_vpip' in sb_df.columns and len(sb_df)>0 else 0
        sb_limp = sb_df['flg_p_limp'].mean()*100 if sb_df is not None and 'flg_p_limp' in sb_df.columns and len(sb_df)>0 else 0
        bb_vpip = bb_df['flg_vpip'].mean()*100 if bb_df is not None and 'flg_vpip' in bb_df.columns and len(bb_df)>0 else 0
        for regla, stat, ok in [
            ("SB: NUNCA limp. Solo raise o fold.", f"VPIP {sb_vpip:.1f}% · Limp {sb_limp:.1f}%", sb_vpip<=40 and sb_limp<5),
            ("BB: Defender amplio. Suited → call.", f"VPIP {bb_vpip:.1f}% (ref ≥55%)", bb_vpip>=45),
        ]:
            st.markdown(f"""<div style="background:#0a0a14;border:1px solid #1e1e3a;border-radius:8px;
                padding:10px 14px;margin-bottom:8px;font-size:.85rem;">
                {'🟢' if ok else '🔴'} <b style="color:#e8e8f0;">{regla}</b><br>
                <span style="color:#4b4b6a;font-family:'JetBrains Mono',monospace;font-size:.78rem;">{stat}</span>
            </div>""", unsafe_allow_html=True)

    with col_d2:
        st.markdown('<div class="section-title">📚 Plan de estudio M1</div>', unsafe_allow_html=True)
        study_tasks = []
        if 'develop_canalized_study_module_logic' in g and not R['spot_results'].empty:
            try:
                tr = g['develop_canalized_study_module_logic'](R['spot_results'], mode, roi_ranking=roi)
                if tr and 'tasks' in tr: study_tasks = tr['tasks']
            except: pass

        for i, task in enumerate((study_tasks or [
            "Rangos preflop: aperturas por posición BTN/CO/MP/UTG. Foco SRP. (10 min)",
            "2-3 situaciones de tu sesión: calcula equity vs rango estimado. (10 min)",
            "Clasifica últimos 10 flops: favorable/neutro/peligroso. (10 min)",
        ])[:3], 1):
            desc = task if isinstance(task,str) else task.get('description',str(task))
            st.markdown(f"""<div style="background:#0f0f1a;border:1px solid #1e1e3a;border-radius:8px;
                padding:10px 14px;margin-bottom:8px;">
                <span style="color:#60a5fa;font-family:'JetBrains Mono',monospace;
                    font-size:.75rem;font-weight:600;">DRILL #{i}</span><br>
                <span style="color:#94a3b8;font-size:.82rem;">{desc[:200]}</span>
            </div>""", unsafe_allow_html=True)

        # Recursos de estudio
        if 'display_study_resources' in g and drill_activo:
            st.markdown('<div class="section-title">📖 Recursos</div>', unsafe_allow_html=True)
            out = _safe_capture(g['display_study_resources'], drill_activo)
            if out.strip():
                st.code(out.strip(), language=None)

        st.markdown('<div class="section-title">🧠 Tilt</div>', unsafe_allow_html=True)
        tilt = R['tilt_result']
        n_tilt   = tilt.get('n_tilt', 0) if tilt else 0
        n_sess_t = tilt.get('n_sessions', n_sess) if tilt else n_sess
        if n_tilt == 0:
            st.markdown(f"""<div style="background:#0a1a0a;border:1px solid #15381a;border-radius:8px;
                padding:10px 14px;font-size:.85rem;color:#4ade80;">
                🟢 Sin sesiones tilt ({n_sess_t} analizadas)</div>""", unsafe_allow_html=True)
        else:
            cost = tilt.get('tilt_cost_bb100', 0) if tilt else 0
            st.markdown(f"""<div style="background:#1a0a0a;border:1px solid #381515;border-radius:8px;
                padding:10px 14px;font-size:.85rem;color:#f87171;">
                🔴 {n_tilt} sesión(es) tilt · coste {cost:+.1f} BB/100</div>""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — LEAKS & ROI
# ══════════════════════════════════════════════════════════════════════════════
with tab2:
    col_l1, col_l2 = st.columns([3,2])

    with col_l1:
        st.markdown('<div class="section-title">🔴 Top leaks</div>', unsafe_allow_html=True)
        if leaks_list:
            for i, lk in enumerate(leaks_list[:10], 1):
                ip   = lk['ip_oop'][:3] if lk['ip_oop'] else ''
                pt   = lk['pot_type'][:3] if lk['pot_type'] else ''
                tag  = f" · {ip} {pt}".strip() if ip or pt else ''
                st.markdown(f"""<div class="leak-row">
                    <span style="color:#4b4b6a;font-size:.72rem;">#{i}</span>
                    <span class="leak-spot"> {lk['spot_identifier']}</span>
                    <span class="leak-ev">{lk['ev_shrunk']:.3f}€</span>
                    <div class="leak-meta">{lk['n']} manos{tag}</div>
                </div>""", unsafe_allow_html=True)
        else:
            st.info("Sin leaks — acumula más manos.")

        if families_list:
            st.markdown('<div class="section-title">📦 Familias</div>', unsafe_allow_html=True)
            for fam in families_list:
                ev  = fam['ev_total']
                dot = '🔴' if ev<-0.2 else ('🟡' if ev<0 else '⚪')
                st.markdown(f"""<div style="background:#0a0a14;border:1px solid #1e1e3a;border-radius:8px;
                    padding:10px 14px;margin-bottom:6px;font-family:'JetBrains Mono',monospace;font-size:.8rem;">
                    {dot} {fam['icon']} <b style="color:#e8e8f0;">{fam['family']}</b>
                    <span style="float:right;color:#ef4444;">{ev:.3f}€</span>
                    <div style="color:#4b4b6a;font-size:.75rem;margin-top:3px;">
                        {fam['n_hands']} manos · {fam['description']}</div>
                </div>""", unsafe_allow_html=True)

        # Error pattern analysis
        if 'display_error_pattern_analysis' in g:
            st.markdown('<div class="section-title">🔍 Patrones de error</div>', unsafe_allow_html=True)
            out = _safe_capture(g['display_error_pattern_analysis'])
            if out.strip():
                st.code(out.strip(), language=None)

    with col_l2:
        st.markdown('<div class="section-title">🟢 Oportunidades</div>', unsafe_allow_html=True)
        if opps_list:
            for opp in opps_list[:5]:
                st.markdown(f"""<div class="opp-row">
                    <span class="leak-spot" style="color:#4ade80;">{opp['spot_identifier']}</span>
                    <span class="opp-ev">+{opp['ev_shrunk']:.3f}€</span>
                    <div class="leak-meta">{opp['n']} manos</div>
                </div>""", unsafe_allow_html=True)
        else:
            st.info("Sin oportunidades con señal.")

        st.markdown('<div class="section-title">📐 Hero vs referencia NL2</div>', unsafe_allow_html=True)
        ref_vpip = {'BTN':45,'CO':35,'HJ':28,'UTG':20,'SB':40,'BB':55}
        for pos in ['BTN','CO','HJ','UTG','SB','BB']:
            pos_df = df[df['player_position']==pos] if 'player_position' in df.columns else None
            if pos_df is None or len(pos_df)<20 or 'flg_vpip' not in pos_df.columns: continue
            vpip = pos_df['flg_vpip'].mean()*100
            ref  = ref_vpip.get(pos,30); gap = vpip-ref
            dot  = '✅' if abs(gap)<=7 else ('⬆️' if gap>0 else '⬇️')
            col_g= '#22c55e' if abs(gap)<=7 else '#ef4444'
            st.markdown(f"""<div style="display:flex;justify-content:space-between;align-items:center;
                padding:5px 10px;border-radius:5px;margin-bottom:3px;
                background:#0a0a14;font-family:'JetBrains Mono',monospace;font-size:.78rem;">
                <span style="color:#94a3b8;width:40px;">{pos}</span>
                <span style="color:#e8e8f0;">{vpip:.1f}%</span>
                <span style="color:#4b4b6a;">ref {ref}%</span>
                <span style="color:{col_g};">{'+' if gap>=0 else ''}{gap:.1f}pp {dot}</span>
            </div>""", unsafe_allow_html=True)

        # Velocity forecast
        if 'display_velocity_forecast' in g:
            st.markdown('<div class="section-title">⏱ Proyección</div>', unsafe_allow_html=True)
            out = _safe_capture(g['display_velocity_forecast'], df)
            if out.strip():
                st.code(out.strip()[:600], language=None)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — POOL M5
# ══════════════════════════════════════════════════════════════════════════════
with tab3:
    st.markdown('<div class="section-title">🌊 Pool fingerprint NL2</div>', unsafe_allow_html=True)
    m5_exploits = (m5 or {}).get('exploits', [])

    if not m5_exploits:
        st.info(f"M5 activo · {hand_count:,} manos")
        for opp_col, did_col, ref, label, tip in [
            ('flg_f_cbet_def_opp','flg_f_cbet_def', 40,"Fold vs cbet flop","CALL más — pool llama mucho"),
            ('flg_f_cbet_opp','flg_f_cbet', 62,"CBet flop IP","Expandir — pool over-folds"),
        ]:
            if opp_col in df.columns and did_col in df.columns:
                on = df[opp_col].sum()
                if on > 50:
                    pct = (on - df[did_col].sum())/on*100 if 'def' in opp_col else df[did_col].sum()/on*100
                    diff= pct-ref; cls='exploit-red' if abs(diff)>10 else 'exploit-yellow'
                    st.markdown(f"""<div class="exploit-row {cls}">
                        <span style="color:#94a3b8;">{'🔴' if abs(diff)>10 else '🟡'} {label}</span>
                        <span style="color:#e8e8f0;font-weight:600;">{pct:.1f}%</span>
                        <span style="color:#4b4b6a;">ref {ref}%</span>
                    </div>
                    <div style="font-size:.75rem;color:#3d5a6e;margin:-2px 0 6px 12px;">→ {tip}</div>""",
                    unsafe_allow_html=True)
    else:
        for item in m5_exploits[:20]:
            obs=item.get('observed_pct',0); base=item.get('baseline_pct',0)
            diff=obs-base; n=item.get('n',0); tip=item.get('exploit_tip','')
            dot='🔴' if abs(diff)>10 else('🟡' if abs(diff)>5 else '✅')
            cls='exploit-red' if abs(diff)>10 else('exploit-yellow' if abs(diff)>5 else 'exploit-green')
            st.markdown(f"""<div class="exploit-row {cls}">
                <span style="color:#94a3b8;">{dot} {item.get('spot','?')}</span>
                <div><span style="color:#e8e8f0;">{obs:.1f}%</span>
                <span style="color:#4b4b6a;margin-left:8px;">base {base:.0f}%</span>
                <span style="color:#3d5a6e;margin-left:8px;">n={n}</span></div>
            </div>""", unsafe_allow_html=True)
            if tip:
                st.markdown(f"<div style='font-size:.75rem;color:#3d5a6e;margin:-2px 0 6px 12px;'>→ {tip}</div>",
                            unsafe_allow_html=True)

    st.markdown('<div class="section-title">👥 Pool</div>', unsafe_allow_html=True)
    if 'opp_class' in df.columns:
        counts = df['opp_class'].value_counts(); tot=counts.sum()
        icons  = {'fish':'🐟','maniac':'🤪','reg':'🎯','unknown':'❓'}
        cols_p = st.columns(len(counts))
        for i,(cls,cnt) in enumerate(counts.items()):
            with cols_p[i]:
                st.markdown(f"""<div class="metric-card" style="text-align:center;">
                    <div style="font-size:1.5rem;">{icons.get(cls,'❓')}</div>
                    <div class="metric-label">{cls}</div>
                    <div class="metric-value neutral" style="font-size:1.2rem;">{cnt/tot*100:.0f}%</div>
                    <div class="metric-sub">{cnt} opp</div>
                </div>""", unsafe_allow_html=True)

    # Pool fingerprint pending
    if 'display_pool_fingerprint_pending' in g:
        st.markdown('<div class="section-title">📍 Pool fingerprint detalle</div>', unsafe_allow_html=True)
        out = _safe_capture(g['display_pool_fingerprint_pending'], m5)
        if out.strip():
            st.code(out.strip(), language=None)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 4 — SESIONES
# ══════════════════════════════════════════════════════════════════════════════
with tab4:
    st.markdown('<div class="section-title">📈 Sesiones</div>', unsafe_allow_html=True)

    if 'session_id' in df.columns and 'net_won' in df.columns:
        import pandas as pd
        sess = df.groupby('session_id',sort=True).agg(
            date=('date','first'),hands=('hand_id','count'),net=('net_won','sum')
        ).reset_index()
        sess['cumulative'] = sess['net'].cumsum()

        import plotly.graph_objects as go
        fig = go.Figure()
        fig.add_trace(go.Bar(x=sess['session_id'], y=sess['net'],
            marker_color=['#22c55e' if x>=0 else '#ef4444' for x in sess['net']],
            name='Net sesión', opacity=0.8))
        fig.add_trace(go.Scatter(x=sess['session_id'], y=sess['cumulative'],
            line=dict(color='#60a5fa',width=2), name='Acumulado', yaxis='y2'))
        fig.update_layout(
            plot_bgcolor='#0a0a14', paper_bgcolor='#0a0a14',
            font=dict(family='JetBrains Mono',color='#6b6b8a',size=11),
            xaxis=dict(gridcolor='#1e1e3a',tickangle=45),
            yaxis=dict(gridcolor='#1e1e3a',title='Net €'),
            yaxis2=dict(overlaying='y',side='right',title='Acumulado €',
                        gridcolor='rgba(0,0,0,0)'),
            legend=dict(bgcolor='#0f0f1a',bordercolor='#1e1e3a'),
            margin=dict(l=40,r=40,t=20,b=60), height=300)
        st.plotly_chart(fig, use_container_width=True)

        # Luck/skill analysis
        if 'display_luck_skill_analysis' in g and hand_count >= 5000:
            st.markdown('<div class="section-title">🎲 Luck vs Skill</div>', unsafe_allow_html=True)
            out = _safe_capture(g['display_luck_skill_analysis'], df)
            if out.strip():
                st.code(out.strip(), language=None)
        elif hand_count < 5000:
            st.caption(f"🔒 Luck/Skill analysis se activa a 5.000 manos ({hand_count:,}/{5000})")

        st.markdown('<div class="section-title">📋 Detalle sesiones</div>', unsafe_allow_html=True)
        for _, row in sess.iterrows():
            nv=row['net']; cv=row['cumulative']
            nc='#22c55e' if nv>=0 else '#ef4444'; cc2='#22c55e' if cv>=0 else '#ef4444'
            ds=row['date'].strftime('%Y-%m-%d') if hasattr(row['date'],'strftime') else str(row['date'])[:10]
            st.markdown(f"""<div class="session-row">
                <span style="color:#4b4b6a;width:100px;">{row['session_id']}</span>
                <span style="color:#94a3b8;width:90px;">{ds}</span>
                <span style="color:#6b6b8a;width:70px;">{row['hands']}m</span>
                <span style="color:{nc};width:80px;font-weight:600;">{'+' if nv>=0 else ''}{nv:.2f}€</span>
                <span style="color:{cc2};width:80px;">cum: {'+' if cv>=0 else ''}{cv:.2f}€</span>
            </div>""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 5 — PROGRESIÓN
# ══════════════════════════════════════════════════════════════════════════════
with tab5:
    col_p1, col_p2 = st.columns(2)

    with col_p1:
        st.markdown('<div class="section-title">📊 Progresión sesión a sesión</div>', unsafe_allow_html=True)
        progression = R.get('progression', [])
        if progression:
            for sess_data in progression[-10:]:
                sid      = sess_data.get('session_id','?')
                bb_vpip  = sess_data.get('bb_vpip', None)
                btn_vpip = sess_data.get('btn_vpip', None)
                cbet_ip  = sess_data.get('cbet_ip', None)
                net_s    = sess_data.get('net', 0)
                nc       = '#22c55e' if net_s>=0 else '#ef4444'
                parts = []
                if bb_vpip  is not None: parts.append(f"BB VPIP {bb_vpip:.0f}%")
                if btn_vpip is not None: parts.append(f"BTN {btn_vpip:.0f}%")
                if cbet_ip  is not None: parts.append(f"CBet IP {cbet_ip:.0f}%")
                st.markdown(f"""<div style="background:#0a0a14;border:1px solid #1e1e3a;border-radius:8px;
                    padding:8px 12px;margin-bottom:4px;font-family:'JetBrains Mono',monospace;font-size:.78rem;">
                    <span style="color:#a78bfa;">{sid}</span>
                    <span style="color:{nc};float:right;">{'+' if net_s>=0 else ''}{net_s:.2f}€</span>
                    <div style="color:#4b4b6a;font-size:.72rem;margin-top:2px;">{' · '.join(parts)}</div>
                </div>""", unsafe_allow_html=True)
        else:
            st.info("Progresión disponible con ≥2 sesiones.")

        # Features status
        if 'display_features_status' in g:
            st.markdown('<div class="section-title">🔓 Features activas</div>', unsafe_allow_html=True)
            out = _safe_capture(g['display_features_status'], hand_count)
            if out.strip():
                st.code(out.strip(), language=None)

    with col_p2:
        st.markdown('<div class="section-title">📐 KPI gaps</div>', unsafe_allow_html=True)
        if 'display_kpi_gaps' in g and not R['spot_results'].empty:
            out = _safe_capture(g['display_kpi_gaps'], df)
            if out.strip():
                st.code(out.strip(), language=None)

        # Hole card analysis
        if 'display_hole_card_analysis' in g:
            st.markdown('<div class="section-title">🃏 Hole cards analysis</div>', unsafe_allow_html=True)
            out = _safe_capture(g['display_hole_card_analysis'], df)
            if out.strip():
                st.code(out.strip()[:800], language=None)

        # Strengths
        if 'build_strength_ranking' in g and not R['spot_results'].empty:
            st.markdown('<div class="section-title">💪 Fortalezas</div>', unsafe_allow_html=True)
            try:
                str_ranking = g['build_strength_ranking'](R['spot_results'])
                if str_ranking is not None and hasattr(str_ranking,'head'):
                    for _, sr in str_ranking.head(3).iterrows():
                        spot = sr.get('spot_identifier','?')
                        ev   = float(sr.get('impacto_ev_total_eur_shrunk',0))
                        n    = int(sr.get('spot_hands_count',0))
                        st.markdown(f"""<div class="opp-row">
                            <span class="leak-spot" style="color:#4ade80;">{spot}</span>
                            <span class="opp-ev">+{ev:.3f}€</span>
                            <div class="leak-meta">{n} manos</div>
                        </div>""", unsafe_allow_html=True)
            except Exception as e:
                st.caption(f"Strengths: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 6 — STATS DETALLE
# ══════════════════════════════════════════════════════════════════════════════
with tab6:
    col_s1, col_s2 = st.columns(2)

    with col_s1:
        st.markdown('<div class="section-title">🔢 Stats por posición</div>', unsafe_allow_html=True)
        for pos in ['BTN','CO','HJ','UTG','SB','BB']:
            pos_df = df[df['player_position']==pos] if 'player_position' in df.columns else None
            if pos_df is None or len(pos_df)<5: continue
            vpip = pos_df['flg_vpip'].mean()*100 if 'flg_vpip' in pos_df.columns else 0
            pfr  = pos_df['flg_p_first_raise'].mean()*100 if 'flg_p_first_raise' in pos_df.columns else 0
            net  = pos_df['net_won'].sum() if 'net_won' in pos_df.columns else 0
            n    = len(pos_df); c='#22c55e' if net>0 else '#ef4444'
            st.markdown(f"""<div style="background:#0a0a14;border:1px solid #1e1e3a;border-radius:8px;
                padding:10px 14px;margin-bottom:6px;font-family:'JetBrains Mono',monospace;font-size:.8rem;">
                <span style="color:#a78bfa;font-weight:600;width:40px;display:inline-block;">{pos}</span>
                <span style="color:#94a3b8;">VPIP {vpip:.1f}%</span>
                <span style="color:#6b6b8a;margin-left:12px;">PFR {pfr:.1f}%</span>
                <span style="color:{c};float:right;">{'+' if net>=0 else ''}{net:.2f}€ ({n}m)</span>
            </div>""", unsafe_allow_html=True)

        # Session degradation
        if 'display_session_degradation' in g:
            st.markdown('<div class="section-title">⏳ Degradación de sesión</div>', unsafe_allow_html=True)
            out = _safe_capture(g['display_session_degradation'], df)
            if out.strip(): st.code(out.strip(), language=None)

    with col_s2:
        st.markdown('<div class="section-title">📊 KPIs globales</div>', unsafe_allow_html=True)
        kpis = [
            ("VPIP global",  df['flg_vpip'].mean()*100 if 'flg_vpip' in df.columns else None,"%",(20,35)),
            ("PFR global",   df['flg_p_first_raise'].mean()*100 if 'flg_p_first_raise' in df.columns else None,"%",(15,28)),
            ("3-bet %",      df['flg_p_3bet'].sum()/max(df['flg_p_3bet_opp'].sum(),1)*100 if 'flg_p_3bet' in df.columns else None,"%",(5,12)),
            ("CBet flop IP", df['flg_f_cbet'].sum()/max(df['flg_f_cbet_opp'].sum(),1)*100 if 'flg_f_cbet' in df.columns else None,"%",(55,70)),
            ("Fold to CBet", (df['flg_f_cbet_def_opp'].sum()-df['flg_f_cbet_def'].sum())/max(df['flg_f_cbet_def_opp'].sum(),1)*100 if 'flg_f_cbet_def_opp' in df.columns else None,"%",(27,45)),
            ("WTSD%",        df['flg_showdown'].sum()/max(df['flg_f_saw'].sum(),1)*100 if 'flg_showdown' in df.columns else None,"%",(25,32)),
            ("W$SD%",        df[df['flg_showdown']==True]['flg_won_hand'].mean()*100 if 'flg_showdown' in df.columns and df['flg_showdown'].sum()>0 else None,"%",(48,56)),
        ]
        for label, val, unit, (lo,hi) in kpis:
            if val is None: continue
            in_r = lo<=val<=hi; dot='✅' if in_r else ('⬆️' if val<lo else '⬇️')
            c='#22c55e' if in_r else '#f59e0b'
            st.markdown(f"""<div style="display:flex;justify-content:space-between;align-items:center;
                padding:7px 12px;border-radius:6px;margin-bottom:4px;
                background:#0a0a14;border:1px solid #15152a;
                font-family:'JetBrains Mono',monospace;font-size:.8rem;">
                <span style="color:#94a3b8;">{label}</span>
                <span style="color:{c};font-weight:600;">{val:.1f}{unit} {dot}</span>
                <span style="color:#3d3d5e;">ref {lo}-{hi}{unit}</span>
            </div>""", unsafe_allow_html=True)

        st.markdown('<div class="section-title">📉 Red / Blue line</div>', unsafe_allow_html=True)
        if 'flg_showdown' in df.columns and 'net_won' in df.columns:
            sd=df[df['flg_showdown']==True]; nsd=df[df['flg_showdown']==False]; nt=len(df)
            for line, vn, color in [("🔵 Blue (showdown)",sd['net_won'].sum(),'#60a5fa'),
                                     ("🔴 Red (no-SD)",   nsd['net_won'].sum(),'#f87171')]:
                vb=vn/nt*100/0.02 if nt>0 else 0; s='+' if vb>=0 else ''
                st.markdown(f"""<div style="background:#0a0a14;border:1px solid #1e1e3a;border-radius:8px;
                    padding:10px 14px;margin-bottom:6px;font-family:'JetBrains Mono',monospace;font-size:.82rem;">
                    <span style="color:{color};">{line}</span>
                    <span style="float:right;color:{color};font-weight:600;">
                        {s}{vb:.1f} BB/100 ({s}{vn:.2f}€)</span>
                </div>""", unsafe_allow_html=True)

        # Stack depth performance
        if 'display_stack_depth_performance' in g:
            st.markdown('<div class="section-title">📏 Stack depth</div>', unsafe_allow_html=True)
            out = _safe_capture(g['display_stack_depth_performance'], df)
            if out.strip(): st.code(out.strip(), language=None)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 7 — M4 COACH (GEMINI)
# ══════════════════════════════════════════════════════════════════════════════
with tab7:
    st.markdown('<div class="section-title">🤖 M4.4 Coach Analítico</div>', unsafe_allow_html=True)

    if not gemini_key:
        st.info("Introduce tu GEMINI_API_KEY en la barra lateral para activar el coach. Es gratuito: https://aistudio.google.com/apikey")
    elif not m4_enabled:
        st.info("Activa M4.4 Coach en la barra lateral.")
    else:
        col_m1, col_m2 = st.columns([2,1])
        with col_m1:
            if st.session_state.m4_output:
                out = st.session_state.m4_output
                try:
                    d = json.loads(out) if isinstance(out, str) else out
                    st.markdown(f"""<div class="coach-box">
                        <b>Acción concreta:</b> {d.get('accion_concreta','—')}<br><br>
                        <b>Concepto teórico:</b> {d.get('concepto_teorico','—')}<br><br>
                        <b>Contexto:</b> {d.get('contexto_spot',d.get('contexto_pool','—'))}<br><br>
                        <b>Impacto estimado:</b> {d.get('impacto_estimado','—')}<br><br>
                        <b>Patrón:</b> {d.get('patron_detectado','—')} ·
                        <b>Confianza:</b> {d.get('confianza','—')}<br><br>
                        <b>❓ Pregunta:</b> {d.get('pregunta_implementacion',d.get('pregunta_reflexion','—'))}
                    </div>""", unsafe_allow_html=True)
                except:
                    st.code(str(out), language=None)
            else:
                st.markdown("""<div class="coach-box" style="color:#3d3d6e;">
                    Pulsa "Consultar coach" para obtener análisis del top leak con el contexto de tu sesión actual.
                </div>""", unsafe_allow_html=True)

        with col_m2:
            if st.button("🤖 Consultar M4 Coach", type="primary", use_container_width=True):
                if 'run_m44_coach' in g:
                    import os as _os
                    _os.environ['GEMINI_API_KEY'] = gemini_key
                    with st.spinner("Consultando Gemini..."):
                        buf = io.StringIO()
                        try:
                            old = sys.stdout; sys.stdout = buf
                            result = g['run_m44_coach'](
                                R['overall_metrics'], R['spot_results'], mode,
                                full_df=df, m5_result=m5,
                                speed_result=R['speed_result'],
                                roi_ranking=roi, m4_enabled=True
                            )
                            sys.stdout = old
                            st.session_state.m4_output = result if result else buf.getvalue()
                        except Exception as e:
                            sys.stdout = old
                            st.session_state.m4_output = f"Error: {e}"
                    st.rerun()

            if 'run_m4_gemini_diagnosis' in g:
                st.markdown("---")
                if st.button("🔬 Diagnóstico Gemini", use_container_width=True):
                    import os as _os; _os.environ['GEMINI_API_KEY'] = gemini_key
                    with st.spinner("Diagnosticando..."):
                        out = _safe_capture(g['run_m4_gemini_diagnosis'],
                                           R['leak_object'] or {}, mode, api_key=gemini_key)
                    st.code(out[:1000] if out.strip() else "Sin output", language=None)

    # Coach history
    if 'display_study_progress' in g:
        st.markdown('<div class="section-title">📚 Progreso de estudio</div>', unsafe_allow_html=True)
        out = _safe_capture(g['display_study_progress'])
        if out.strip(): st.code(out.strip(), language=None)

    # Followup effectiveness
    if 'display_followup_effectiveness' in g:
        st.markdown('<div class="section-title">📈 Efectividad followup</div>', unsafe_allow_html=True)
        out = _safe_capture(g['display_followup_effectiveness'])
        if out.strip(): st.code(out.strip(), language=None)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 8 — SUNCHAT
# ══════════════════════════════════════════════════════════════════════════════
with tab8:
    st.markdown('<div class="section-title">💬 SunChat — Entrenamiento conversacional</div>', unsafe_allow_html=True)

    if not groq_key:
        st.info("Introduce tu GROQ_API_KEY en la barra lateral. Gratuito: https://console.groq.com/keys")
    elif not sc_enabled:
        st.info("Activa SunChat en la barra lateral.")
    else:
        leak_obj = R.get('leak_object')
        if leak_obj:
            col_sc1, col_sc2 = st.columns([3,1])
            with col_sc1:
                st.markdown(f"""<div style="background:#0a0f1e;border:1px solid #1e3a5f;border-radius:10px;
                    padding:14px 18px;margin-bottom:16px;font-family:'JetBrains Mono',monospace;font-size:.82rem;">
                    <b style="color:#60a5fa;">Leak activo:</b>
                    <span style="color:#a78bfa;"> {leak_obj.get('leak_id','?')}</span><br>
                    <span style="color:#4b4b6a;">
                        EV loss: {leak_obj.get('ev_loss_bb100',0):.0f} BB/100 ·
                        {leak_obj.get('sample',0)} manos ·
                        {leak_obj.get('pattern','—')[:80]}
                    </span>
                </div>""", unsafe_allow_html=True)
            with col_sc2:
                if st.button("🆕 Reset chat", use_container_width=True):
                    st.session_state.sunchat_msgs = []
                    st.rerun()

        # Chat history
        msgs = st.session_state.sunchat_msgs or []
        for msg in msgs:
            role = msg.get('role','user')
            txt  = msg.get('content','')
            bg   = '#0f1629' if role=='assistant' else '#0a0a14'
            bc   = '#1e3a5f' if role=='assistant' else '#1e1e3a'
            icon = '🤖' if role=='assistant' else '👤'
            st.markdown(f"""<div style="background:{bg};border:1px solid {bc};border-radius:8px;
                padding:10px 14px;margin-bottom:8px;font-size:.85rem;color:#94a3b8;">
                {icon} {txt}</div>""", unsafe_allow_html=True)

        # Input
        user_input = st.chat_input("Escribe tu respuesta o pregunta...")
        if user_input:
            msgs.append({'role':'user','content':user_input})
            if 'run_sunchat_session' in g and '_groq_call' in g:
                import os as _os; _os.environ['GROQ_API_KEY'] = groq_key
                try:
                    # Build conversation for groq
                    system_fn = g.get('_build_sunchat_system_prompt')
                    system_p  = system_fn(leak_obj or {}, mode) if system_fn else \
                                f"Eres SunChat, coach de poker. Leak: {(leak_obj or {}).get('leak_id','?')}. Modo: {mode}."
                    history_for_api = [{'role':m['role'],'content':m['content']} for m in msgs]
                    reply, err_sc = g['_groq_call'](history_for_api, system=system_p, api_key=groq_key)
                    if err_sc:
                        reply = f"⚠️ {err_sc}"
                    msgs.append({'role':'assistant','content':reply or '...'})
                except Exception as e:
                    msgs.append({'role':'assistant','content':f"⚠️ Error: {e}"})
            else:
                msgs.append({'role':'assistant','content':"⚠️ SunChat no disponible en esta versión de os_library."})
            st.session_state.sunchat_msgs = msgs
            st.rerun()

        if not msgs and leak_obj:
            if st.button("▶ Iniciar sesión SunChat", type="primary"):
                import os as _os; _os.environ['GROQ_API_KEY'] = groq_key
                try:
                    system_fn = g.get('_build_sunchat_system_prompt')
                    system_p  = system_fn(leak_obj, mode) if system_fn else \
                                f"Eres SunChat, coach de poker. Modo {mode}. Leak: {leak_obj.get('leak_id','?')}."
                    history_for_api = [{'role':'user','content':f"Empezamos. Mi leak activo es: {leak_obj.get('leak_id','?')}. Patrón: {leak_obj.get('pattern','')}. EV loss: {leak_obj.get('ev_loss_bb100',0):.0f} BB/100."}]
                    reply, _ = g['_groq_call'](history_for_api, system=system_p, api_key=groq_key)
                    st.session_state.sunchat_msgs = [
                        history_for_api[0],
                        {'role':'assistant','content': reply or 'Hola, empecemos.'}
                    ]
                except Exception as e:
                    st.error(f"Error iniciando SunChat: {e}")
                st.rerun()


# ── Footer ──────────────────────────────────────────────────────────────────
st.markdown("---")
st.markdown("""<div style="text-align:center;font-family:'JetBrains Mono',monospace;
    font-size:.72rem;color:#2d2d4e;padding:8px;">
    OS v2.0 · LaRuinaDeMago · NL2 → NL25+ · El sistema mide, tú decides.
</div>""", unsafe_allow_html=True)
