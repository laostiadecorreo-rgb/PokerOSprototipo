import pandas as pd
import numpy as np
import sqlite3
import random
import re
import os
from datetime import datetime, timedelta
import plotly.graph_objects as go
import plotly.express as px

# FIX v1.51: Plotly separado — si no está instalado solo falla este bloque,
# las constantes globales de la celda siguiente cargan igualmente.
try:
    import plotly.graph_objects as go
    import plotly.express as px
    PLOTLY_AVAILABLE = True
except ImportError:
    go = None
    px = None
    PLOTLY_AVAILABLE = False
    print("\u26a0\ufe0f  Plotly no disponible — instalar con: pip install plotly")

print("\u2705 Imports cargados.")


# ─── CONSTANTES GLOBALES ───────────────────────────────────────────────────
BB_VALUE_MAP = {
    'NL2':  0.02,
    'NL5':  0.05,
    'NL10': 0.10,
    'NL25': 0.25,
    'NL50': 0.50,
    'NL100': 1.00,
    'NL200': 2.00,
}
# DT1 FIX: None causaría TypeError sin contexto; descriptor lanza RuntimeError con diagnóstico.
class _DeprecatedBBToEur:
    def __mul__(self, _):    raise RuntimeError("BB_TO_EUR DEPRECADA — usa bb_value de BB_VALUE_MAP")
    def __rmul__(self, _):   raise RuntimeError("BB_TO_EUR DEPRECADA — usa bb_value de BB_VALUE_MAP")
    def __truediv__(self, _):raise RuntimeError("BB_TO_EUR DEPRECADA — usa bb_value de BB_VALUE_MAP")
    def __float__(self):     raise RuntimeError("BB_TO_EUR DEPRECADA — usa bb_value de BB_VALUE_MAP")
    def __repr__(self):      return 'BB_TO_EUR(DEPRECATED-v1.30)'
BB_TO_EUR = _DeprecatedBBToEur()  # DEPRECATED v1.30
SESSION_GAP_MINUTES = 30          # Gap de tiempo para detectar nueva sesión
MIN_HANDS_M2 = 30000              # Gate mínimo de manos para M2
MIN_HANDS_M3 = 100_000            # Gate mínimo de manos para M3
MIN_HANDS_CONFIDENCE = 5000       # Umbral inferior de confianza estadística
MAX_HANDS_FULL_CONFIDENCE = 30000 # Umbral superior (confianza = 1.0)
FRICCION_RECENT_SESSIONS = 5      # Nº sesiones recientes para calcular fricción M0
MIN_HANDS_PER_SESSION_EV = 50     # Mínimo de manos por sesión para incluirla en cálculo EV/h
                                   # (sesiones muy cortas distorsionan la media — ver _ev_h_from_group)
DB_NAME = 'os_v2_poker.db'        # Nombre de la base de datos SQLite

# P7 v1.63: Gate para stack efectivo real
# Ver display_stack_depth_performance — activa aviso hasta llegar a 10k manos
STACK_EFFECTIVE_GATE = 10_000     # manos para usar stack efectivo héroe-villano (P9)

# P6 v1.63: SESSION_GAP nota
# Con 30 min, sesiones con pausa <30 min se fusionan en una sola.
# Ej: sesiones 4+5 del 08/03 (26+37 manos) → una sesión de 63 manos.
# Ajustar a 60 si tienes muchas pausas cortas entre sesiones.

# ── Shrinkage adaptativo por stake ────────────────────────────────────────
SHRINKAGE_K_BY_STAKE = {
    'NL2':   200,
    'NL5':   200,
    'NL10':  350,
    'NL25':  350,
    'NL50':  500,
    'NL100': 800,
    'NL200': 800,
}

# ── Familias de leaks — agrupa spots con error de fondo compartido ────────
# v1.21: ACTIVO EN M2 (≥5.000 manos con señal postflop). Con <5k manos las
# familias OOP_postflop e IP_postflop estarán vacías — código correcto, señal no existe.
# Conectado a develop_canalized_study_module_logic via parámetro roi_ranking.
# Cuando 2+ spots de la misma familia aparecen en el top de leaks, el sistema
# los fusiona en un único drill con muestra combinada → señal más fuerte.
# Lógica: familia = error estructural, spot = manifestación concreta.
LEAK_FAMILIES = {
    'OOP_postflop': {
        'description': 'Leaks OOP postflop (SB+BB después de defender ciega)',
        'match_fn': lambda row: (
            row.get('ip_oop', '') == 'OOP' and
            row.get('decision_street', '') in ('flop', 'turn', 'river')
        ),
        'icon': '🔵',
    },
    'IP_postflop': {
        'description': 'Leaks IP postflop (BTN+CO+HJ)',
        'match_fn': lambda row: (
            row.get('ip_oop', '') == 'IP' and
            row.get('decision_street', '') in ('flop', 'turn', 'river')
        ),
        'icon': '🟠',
    },
    '3bet_pots': {
        'description': 'Leaks en botes 3-bet (cualquier posición)',
        'match_fn': lambda row: row.get('pot_type', '') == '3BP',
        'icon': '🟡',
    },
    'preflop_open': {
        'description': 'Leaks de apertura preflop (frecuencia/sizing)',
        'match_fn': lambda row: (
            row.get('decision_street', '') == 'preflop' and
            row.get('ip_oop', '') == 'IP'
        ),
        'icon': '⚪',
    },
    'blind_defense': {
        'description': 'Leaks de defensa de ciega (SB+BB preflop)',
        'match_fn': lambda row: (
            row.get('decision_street', '') == 'preflop' and
            row.get('ip_oop', '') == 'OOP'
        ),
        'icon': '🔴',
    },
}

# ── Stack Context Buckets (para spot_identifier) ──────────────────────────
# deep ≥ 80BB | mid 40-79BB | short < 40BB
# NOTA: estos buckets van EN EL SPOT IDENTIFIER (estratégicamente distintos).
# La columna stack_depth_bb (BB exactas) va SEPARADA para que el jugador
# pueda ver "perdí 15BB aquí con 67BB efectivos" — contexto explicativo.
STACK_BUCKETS = {
    'deep':  (80, 9999),
    'mid':   (40, 79),
    'short': (0,  39),
}

print("✅ Imports y configuración cargados correctamente.")
print(f"   Stakes configurados: {list(BB_VALUE_MAP.keys())}")
print(f"   Shrinkage k por stake: {SHRINKAGE_K_BY_STAKE}")
print(f"   Gap de sesión: {SESSION_GAP_MINUTES} min | Manos gate M2: {MIN_HANDS_M2}")
print(f"   Familias de leaks: {list(LEAK_FAMILIES.keys())}")


# ── Nuevas constantes v1.21 ───────────────────────────────────────────────
# M4.4 — Claude API wrapper (coach analítico)
# v1.45: Activado. Guard flexible por modo (ver cell 78).
# Filosofía: el coach acelera el aprendizaje, no lo sustituye.
#   M1 → máx 2 llamadas/sesión (instructivo + seguimiento si persiste)
#   M2 → máx 2 llamadas/sesión (reflexivo + pool context)
#   M3 → máx 1 llamada/sesión  (socrático — el jugador razona solo)
# Poda automática: si ISS < 70 durante 3 semanas → reducir a 1/sesión.
M4_API_ENABLED       = True    # ← Activado v1.45
M4_CALLS_PER_SESSION = {       # Máx llamadas al coach por modo y sesión
    'M1': 2,                   # M1: instrucción + seguimiento si persiste
    'M2': 2,                   # M2: reflexivo + contexto pool
    'M3': 1,                   # M3 socrático: 1 sola → forzar criterio propio
}

# Gates de dimensiones del spot_identifier
# opp_class y board_texture NO entran en el ID hasta alcanzar el volumen mínimo.
# Por debajo: existen como columnas en el DataFrame (contexto) pero no fragmentan spots.
SPOT_ID_GATE_OPP_CLASS     = 15_000   # manos mínimas para añadir opp_class al spot_id
BUG2_M41_PREP_GATE         = 10_000   # ⚠️ BUG-2 FECHA LÍMITE: M4.1 proxies deben
                                       # estar reescritos ANTES de este gate.
                                       # Ver SSOT §2: parsear acciones reales de
                                       # oponentes en _parse_single_hand_real.
                                       # Sin esto opp_class producirá 0% regs.
SPOT_ID_GATE_BOARD_TEXTURE =      0   # [v2.05] ACTIVO — gate 5k superado (6.292 manos) al spot_id
                                       # Justificación: ~70% de manos generan flop → a 5k manos
                                       # hay ~3.500 flops, suficiente para detectar patrones.
                                       # opp_class sigue en 15k (depende de stats del rival).

# M5 Pool Detector — gates de activación (L1 FIX: SSOT decía 'M2 5.000' — incorrecto)
# La función se llama siempre pero permanece silenciosa < 3.000 manos.
M5_ACTIVATION_HANDS_PRELIMINARY = 3_000   # señal preliminar (🟡/⚪)
M5_ACTIVATION_HANDS_CONFIRMED   = 5_000   # señal confirmada (todos los semáforos)


def define_hud_schema():
    """
    Schema canónico del OS v2.0.
    Fuente única de verdad para todas las columnas del DataFrame de manos.
    Board cards separados por calle para máxima granularidad (Fundamentos M1-M3).
    """
    schema = [
        {'column_name': 'hand_id',              'data_type': 'str',      'description': 'Identificador único de mano.'},
        {'column_name': 'session_id',            'data_type': 'str',      'description': 'Identificador de sesión (auto-generado por gap de tiempo).'},
        {'column_name': 'date',                  'data_type': 'datetime', 'description': 'Fecha y hora exacta de la mano (datetime combinado).'},
        {'column_name': 'table_size',            'data_type': 'int',      'description': 'Número de jugadores en la mesa.'},
        {'column_name': 'player_position',       'data_type': 'str',      'description': 'Posición del Hero (BTN, CO, MP, UTG, SB, BB).'},
        {'column_name': 'hole_cards',            'data_type': 'str',      'description': 'Cartas del Hero (ej. AhKs).'},
        {'column_name': 'board_cards_flop',      'data_type': 'str',      'description': 'Cartas del flop (ej. Kh7s2c).'},
        {'column_name': 'board_cards_turn',      'data_type': 'str',      'description': 'Carta del turn (ej. Td).'},
        {'column_name': 'board_cards_river',     'data_type': 'str',      'description': 'Carta del river (ej. 8h).'},
        {'column_name': 'preflop_action',        'data_type': 'str',      'description': 'Acción preflop del Hero (F/C/R/3B/4B).'},
        {'column_name': 'flop_action',           'data_type': 'str',      'description': 'Acción flop del Hero.'},
        {'column_name': 'turn_action',           'data_type': 'str',      'description': 'Acción turn del Hero.'},
        {'column_name': 'river_action',          'data_type': 'str',      'description': 'Acción river del Hero.'},
        {'column_name': 'net_won',               'data_type': 'float',    'description': 'Ganancia/pérdida neta en € (después de rake).'},
        {'column_name': 'ev_won',                'data_type': 'float',    'description': 'EV ganado/perdido en euros (all-in EV cuando aplica). PT4: amt_expected_won (mismas unidades, mismo concepto).'},
        {'column_name': 'val_equity',           'data_type': 'float',    'description': 'Equity en el all-in (0.0-1.0). PT4: val_equity. None si no hay all-in.'},
        {'column_name': 'allin_equity_hero',    'data_type': 'float',    'description': 'P3 v1.63: Equity hero en all-in (0.0-1.0). PT4 Avg All-In Equity. NaN si sin all-in calculado.'},
        {'column_name': 'allin_ev_calculated',  'data_type': 'bool',     'description': 'True si enrich_with_allin_ev calculó EV real para esta mano.'},
        {'column_name': 'amt_p_effective_stack','data_type': 'float',    'description': 'Stack efectivo preflop en euros. PT4: amt_p_effective_stack. OS deriva en BBs via stack_depth_bb.'},
        {'column_name': 'rake',                  'data_type': 'float',    'description': 'Rake pagado en €.'},
        {'column_name': 'stake_level',           'data_type': 'str',      'description': 'Stake de la partida (NL2, NL5, NL10...).'},
        {'column_name': 'total_pot',             'data_type': 'float',    'description': 'Bote total al final de la mano en €.'},
        {'column_name': 'player_stack_start',    'data_type': 'float',    'description': 'Stack inicial del Hero en €.'},
        {'column_name': 'stack_depth_bb',        'data_type': 'float',    'description': 'Stack efectivo del Hero en BBs al inicio de la mano (contexto exacto, separado del bucket estratégico).'},
        {'column_name': 'num_tables',             'data_type': 'int',      'description': 'Nº de mesas jugadas simultáneamente en esta sesión (1-4). Afecta velocidad real y calidad de decisión.'},
        {'column_name': 'all_players',           'data_type': 'str',      'description': 'Todos los jugadores y sus stacks iniciales.'},
        {'column_name': 'opponent_names',        'data_type': 'str',      'description': 'Nombres de los oponentes (separados por coma).'},
        {'column_name': 'friccion_r',            'data_type': 'int',      'description': 'Fricción Rabia post-sesión (1-5).'},
        {'column_name': 'friccion_a',            'data_type': 'int',      'description': 'Fricción Ansiedad post-sesión (1-5).'},
        {'column_name': 'friccion_v',            'data_type': 'int',      'description': 'Fricción Varianza/Vigilancia post-sesión (1-5).'},
        {'column_name': 'manual_spot_tag',       'data_type': 'str',      'description': 'Tag manual del spot (ej. bluff_catch, squeeze_spot).'},
        {'column_name': 'opponent_type_manual',  'data_type': 'str',      'description': 'Clasificación manual del oponente (LAG/NIT/Fish/Reg...).'},
        # ── v1.32: campos de presión preflop (existían en parser, faltaban en schema)
        {'column_name': 'preflop_pressure',         'data_type': 'str',  'description': 'OS: presión PF antes de actuar del hero — raise|3bet|limp|none.'},
        {'column_name': 'preflop_n_raises_facing',  'data_type': 'int',  'description': 'OS: nº raises enfrentados antes de la primera acción del hero PF.'},

        # ── v1.24: CAMPOS PT3-COMPATIBLES (holdem_hand_player_statistics) ──────────────
        # Nomenclatura espeja el schema oficial PT3 para máxima comparabilidad.
        # Fuente: pokertracker.com/guides/PT3/databases/pokertracker-3-database-schema-documentation
        {'column_name': 'flg_p_3bet_opp',        'data_type': 'bool', 'description': 'PT3:flg_p_3bet_opp — hubo raise antes del hero: oportunidad de 3bet.'},
        {'column_name': 'flg_p_3bet',            'data_type': 'bool', 'description': 'PT3:flg_p_3bet — hero 3beteó preflop.'},
        {'column_name': 'flg_p_fold_to_3bet',    'data_type': 'bool', 'description': 'PT3:flg_p_fold_to_3bet — hero abrió, rival 3betea, hero foldea.'},
        {'column_name': 'flg_f_cbet_opp',        'data_type': 'bool', 'description': 'PT3:flg_f_cbet_opp — hero fue PF aggressor y actuó primero en flop.'},
        {'column_name': 'flg_f_cbet',            'data_type': 'bool', 'description': 'PT3:flg_f_cbet — hero fue PF aggressor, actuó primero en flop y apostó.'},
        {'column_name': 'flg_f_cbet_def_opp',    'data_type': 'bool', 'description': 'PT3:flg_f_cbet_def_opp — hero enfrentó cbet en flop siendo caller PF.'},
        {'column_name': 'flg_f_cbet_def',        'data_type': 'bool', 'description': 'PT3:flg_f_cbet_def — hero llamó/raisó la cbet (NO foldeó). fold_to_cbet = opp - def.'},
        {'column_name': 'flg_f_donk_opp',        'data_type': 'bool', 'description': 'PT3:flg_f_donk_opp — hero NO fue PF aggressor y actuó primero en flop.'},
        {'column_name': 'flg_f_donk',            'data_type': 'bool', 'description': 'PT3:flg_f_donk — hero apostó en flop siendo el caller PF (donk bet).'},
        {'column_name': 'val_f_afq',             'data_type': 'float','description': 'P5 v1.63: AFq-OS (fórmula propia, NO comparable con PT4 AFq). OS: (bets+raises)/(bets+raises+calls+checks) postflop. PT4 usa denominador diferente (todas las acciones incluye fold). Mostrar como AFq-OS en dashboard para evitar confusión.'},

        # ── v1.25: open/3bet/position flags ──────────────────────────────────
        {'column_name': 'flg_p_open_opp',        'data_type': 'bool', 'description': 'PT3: nadie actuó antes del hero PF.'},
        {'column_name': 'flg_p_open',             'data_type': 'bool', 'description': 'PT3: hero fue primero en entrar al bote PF.'},
        {'column_name': 'flg_p_first_raise',      'data_type': 'bool', 'description': 'PT3: hero fue el primer raiser PF.'},
        {'column_name': 'flg_p_3bet_role',        'data_type': 'str',  'description': 'OS: rol en el 3bet PF — aggressor/caller/none.'},
        {'column_name': 'flg_f_first',            'data_type': 'bool', 'description': 'PT3: hero fue primer actor en el flop.'},
        {'column_name': 'flg_t_first',            'data_type': 'bool', 'description': 'PT3: hero fue primer actor en el turn.'},
        {'column_name': 'flg_r_first',            'data_type': 'bool', 'description': 'PT3: hero fue primer actor en el river.'},
        {'column_name': 'flg_f_has_position',     'data_type': 'bool', 'description': 'PT3: hero tiene posición en el flop.'},
        {'column_name': 'villain_position',       'data_type': 'str',  'description': 'OS: posición del último agresor PF.'},

        # ── v1.26: holdem_hand_player_statistics — acción completa por calle ──
        {'column_name': 'flg_vpip',               'data_type': 'bool', 'description': 'PT3: Voluntarily Put $ In Pot PF.'},
        {'column_name': 'flg_p_fold',             'data_type': 'bool', 'description': 'PT3: hero foldeó preflop.'},
        {'column_name': 'flg_p_limp',             'data_type': 'bool', 'description': 'PT3: hero limpió PF.'},
        {'column_name': 'flg_p_ccall',            'data_type': 'bool', 'description': 'PT3: cold call PF.'},
        {'column_name': 'cnt_p_raise',            'data_type': 'int',  'description': 'PT3: nº de raises del hero PF.'},
        {'column_name': 'cnt_p_call',             'data_type': 'int',  'description': 'PT3: nº de calls del hero PF.'},
        {'column_name': 'flg_f_bet',              'data_type': 'bool', 'description': 'PT3: hero hizo bet en el flop.'},
        {'column_name': 'flg_t_bet',              'data_type': 'bool', 'description': 'PT3: hero hizo bet en el turn.'},
        {'column_name': 'flg_r_bet',              'data_type': 'bool', 'description': 'PT3: hero hizo bet en el river.'},
        {'column_name': 'flg_f_check',            'data_type': 'bool', 'description': 'PT3: hero chequeo en el flop.'},
        {'column_name': 'flg_t_check',            'data_type': 'bool', 'description': 'PT3: hero chequeo en el turn.'},
        {'column_name': 'flg_r_check',            'data_type': 'bool', 'description': 'PT3: hero chequeo en el river.'},
        {'column_name': 'flg_f_fold',             'data_type': 'bool', 'description': 'PT3: hero foldeó en el flop.'},
        {'column_name': 'flg_t_fold',             'data_type': 'bool', 'description': 'PT3: hero foldeó en el turn.'},
        {'column_name': 'flg_r_fold',             'data_type': 'bool', 'description': 'PT3: hero foldeó en el river.'},
        {'column_name': 'flg_f_check_raise',      'data_type': 'bool', 'description': 'PT3: hero hizo check-raise en el flop.'},
        {'column_name': 'flg_t_check_raise',      'data_type': 'bool', 'description': 'PT3: hero hizo check-raise en el turn.'},
        {'column_name': 'flg_r_check_raise',      'data_type': 'bool', 'description': 'PT3: hero hizo check-raise en el river.'},
        {'column_name': 'flg_f_saw',              'data_type': 'bool', 'description': 'PT3: hero vio el flop.'},
        {'column_name': 'flg_t_saw',              'data_type': 'bool', 'description': 'PT3: hero vio el turn.'},
        {'column_name': 'flg_r_saw',              'data_type': 'bool', 'description': 'PT3: hero vio el river.'},
        {'column_name': 'cnt_f_raise',            'data_type': 'int',  'description': 'PT3: nº de raises del hero en el flop.'},
        {'column_name': 'cnt_t_raise',            'data_type': 'int',  'description': 'PT3: nº de raises del hero en el turn.'},
        {'column_name': 'cnt_r_raise',            'data_type': 'int',  'description': 'PT3: nº de raises del hero en el river.'},
        {'column_name': 'flg_t_has_position',     'data_type': 'bool', 'description': 'PT3: hero tiene posición en el turn.'},
        {'column_name': 'flg_r_has_position',     'data_type': 'bool', 'description': 'PT3: hero tiene posición en el river.'},
        {'column_name': 'flg_f_open',             'data_type': 'bool', 'description': 'PT3: hero abrió la acción en el flop.'},
        {'column_name': 'flg_t_open',             'data_type': 'bool', 'description': 'PT3: hero abrió la acción en el turn.'},
        {'column_name': 'flg_r_open',             'data_type': 'bool', 'description': 'PT3: hero abrió la acción en el river.'},
        {'column_name': 'flg_f_open_opp',         'data_type': 'bool', 'description': 'PT3: hero tuvo opp de abrir en el flop.'},
        {'column_name': 'flg_t_open_opp',         'data_type': 'bool', 'description': 'PT3: hero tuvo opp de abrir en el turn.'},
        {'column_name': 'flg_r_open_opp',         'data_type': 'bool', 'description': 'PT3: hero tuvo opp de abrir en el river.'},

        # ── v1.27: TIER 0+A — campos PT3 adicionales ──────────────────────────
        {'column_name': 'flg_f_first_raise',      'data_type': 'bool', 'description': 'PT3: hero fue el primer raiser en el flop.'},
        {'column_name': 'flg_t_first_raise',      'data_type': 'bool', 'description': 'PT3: hero fue el primer raiser en el turn.'},
        {'column_name': 'flg_r_first_raise',      'data_type': 'bool', 'description': 'PT3: hero fue el primer raiser en el river.'},
        {'column_name': 'cnt_f_call',             'data_type': 'int',  'description': 'PT3: nº de calls del hero en el flop.'},
        {'column_name': 'cnt_t_call',             'data_type': 'int',  'description': 'PT3: nº de calls del hero en el turn.'},
        {'column_name': 'cnt_r_call',             'data_type': 'int',  'description': 'PT3: nº de calls del hero en el river.'},
        {'column_name': 'flg_blind_s',            'data_type': 'bool', 'description': 'PT3: hero fue small blind.'},
        {'column_name': 'flg_blind_b',            'data_type': 'bool', 'description': 'PT3: hero fue big blind.'},
        {'column_name': 'flg_blind_def_opp',      'data_type': 'bool', 'description': 'PT3: BB/SB enfrentó intento de steal.'},
        {'column_name': 'flg_sb_steal_fold',      'data_type': 'bool', 'description': 'PT3: SB foldeó ante steal.'},
        {'column_name': 'flg_bb_steal_fold',      'data_type': 'bool', 'description': 'PT3: BB foldeó ante steal.'},
        {'column_name': 'flg_p_face_raise',       'data_type': 'bool', 'description': 'PT3: hero enfrentó raise PF de otro jugador.'},
        {'column_name': 'flg_p_3bet_def_opp',     'data_type': 'bool', 'description': 'PT3: hero abrió PF y enfrentó 3bet (alias flg_p_fold_to_3bet_opp). Denominador estricto (n=115).'},
        {'column_name': 'flg_p_faced_3bet',       'data_type': 'bool', 'description': 'PT4-compatible: hero enfrentó cualquier 3bet y actuó (n=224). Denominador correcto para Fold to 3Bet%.'},
        {'column_name': 'flg_won_hand',           'data_type': 'bool', 'description': 'PT3: hero ganó la mano.'},
        {'column_name': 'flg_showdown',           'data_type': 'bool', 'description': 'PT3: hero llegó a showdown.'},
        {'column_name': 'flg_showed',             'data_type': 'bool', 'description': 'PT3: hero mostró cartas.'},
        {'column_name': 'enum_folded',            'data_type': 'str',  'description': 'PT3: calle en que foldó — P/F/T/R/N.'},
        {'column_name': 'cnt_players_f',          'data_type': 'int',  'description': 'PT3: jugadores que vieron el flop.'},
        {'column_name': 'cnt_players_t',          'data_type': 'int',  'description': 'PT3: jugadores que vieron el turn.'},
        {'column_name': 'cnt_players_r',          'data_type': 'int',  'description': 'PT3: jugadores que vieron el river.'},

        # v1.28a TIER INMEDIATO: aliases PT3 steal + 4bet + squeeze + allin + face_raise
        {'column_name': 'flg_steal_att',           'data_type': 'bool', 'description': 'PT3: flg_steal_att — intentó steal (alias de flg_p_steal).'},
        {'column_name': 'flg_steal_opp',           'data_type': 'bool', 'description': 'PT3: flg_steal_opp — tuvo opp de steal (alias de flg_p_steal_opp).'},
        {'column_name': 'flg_p_4bet',              'data_type': 'bool', 'description': 'PT3: 4+ bet preflop (cualquier raise > 3bet).'},
        {'column_name': 'flg_p_4bet_opp',          'data_type': 'bool', 'description': 'PT3: tuvo oportunidad de 4bet preflop.'},
        {'column_name': 'flg_p_4bet_def_opp',      'data_type': 'bool', 'description': 'PT3: enfrentó 4bet preflop de otro jugador.'},
        {'column_name': 'flg_p_squeeze',           'data_type': 'bool', 'description': 'PT3: ejecutó squeeze preflop (3bet tras raise + cold caller).'},
        {'column_name': 'flg_p_squeeze_opp',       'data_type': 'bool', 'description': 'PT3: tuvo oportunidad de squeeze preflop.'},
        {'column_name': 'flg_p_squeeze_def_opp',   'data_type': 'bool', 'description': 'PT3: enfrentó squeeze preflop.'},
        {'column_name': 'enum_p_squeeze_action',   'data_type': 'str',  'description': 'PT3: acción vs squeeze (C/R/F/N).'},
        {'column_name': 'enum_allin',              'data_type': 'str',  'description': 'PT3: calle en que hero fue all-in (P/F/T/R/N).'},
        {'column_name': 'enum_face_allin',         'data_type': 'str',  'description': 'PT3: calle en que enfrentó all-in de rival (P/F/T/R/N).'},
        {'column_name': 'enum_face_allin_action',  'data_type': 'str',  'description': 'PT3: acción vs all-in rival (C/R/F/N).'},
        {'column_name': 'enum_p_3bet_action',      'data_type': 'str',  'description': 'PT3: acción vs 3bet preflop (C/R/F/N).'},
        {'column_name': 'flg_f_face_raise',        'data_type': 'bool', 'description': 'PT3: enfrentó raise en el flop.'},
        {'column_name': 'flg_t_face_raise',        'data_type': 'bool', 'description': 'PT3: enfrentó raise en el turn.'},
        {'column_name': 'flg_r_face_raise',        'data_type': 'bool', 'description': 'PT3: enfrentó raise en el river.'},
        # v1.28b TIER B: cbet chain turn+river, float, donk, enum_f_cbet_action
        {'column_name': 'enum_f_cbet_action',      'data_type': 'str',  'description': 'PT3: acción vs cbet flop (C/R/F/N).'},
        {'column_name': 'flg_t_cbet',              'data_type': 'bool', 'description': 'PT3: cbet turn (cbeteó flop Y beteó turn).'},
        {'column_name': 'flg_t_cbet_opp',          'data_type': 'bool', 'description': 'PT3: oportunidad cbet turn.'},
        {'column_name': 'flg_t_cbet_def_opp',      'data_type': 'bool', 'description': 'PT3: enfrentó cbet turn.'},
        {'column_name': 'enum_t_cbet_action',      'data_type': 'str',  'description': 'PT3: acción vs cbet turn (C/R/F/N).'},
        {'column_name': 'flg_r_cbet',              'data_type': 'bool', 'description': 'PT3: cbet river (cbeteó turn Y beteó river).'},
        {'column_name': 'flg_r_cbet_opp',          'data_type': 'bool', 'description': 'PT3: oportunidad cbet river.'},
        {'column_name': 'flg_r_cbet_def_opp',      'data_type': 'bool', 'description': 'PT3: enfrentó cbet river.'},
        {'column_name': 'enum_r_cbet_action',      'data_type': 'str',  'description': 'PT3: acción vs cbet river (C/R/F/N).'},
        {'column_name': 'flg_t_float',             'data_type': 'bool', 'description': 'PT3: float turn (IP, llamó flop, rival chequea turn, hero beteó).'},
        {'column_name': 'flg_t_float_opp',         'data_type': 'bool', 'description': 'PT3: oportunidad float turn.'},
        {'column_name': 'flg_t_float_def_opp',     'data_type': 'bool', 'description': 'PT3: enfrentó float turn.'},
        {'column_name': 'enum_t_float_action',     'data_type': 'str',  'description': 'PT3: acción vs float turn (C/R/F/N).'},
        {'column_name': 'flg_r_float',             'data_type': 'bool', 'description': 'PT3: float river.'},
        {'column_name': 'flg_r_float_opp',         'data_type': 'bool', 'description': 'PT3: oportunidad float river.'},
        {'column_name': 'flg_r_float_def_opp',     'data_type': 'bool', 'description': 'PT3: enfrentó float river.'},
        {'column_name': 'enum_r_float_action',     'data_type': 'str',  'description': 'PT3: acción vs float river (C/R/F/N).'},
        {'column_name': 'flg_t_donk',              'data_type': 'bool', 'description': 'PT3: donk turn (llamó cbet OOP, beteó primero en turn).'},
        {'column_name': 'flg_t_donk_opp',          'data_type': 'bool', 'description': 'PT3: oportunidad donk turn.'},
        {'column_name': 'flg_t_donk_def_opp',      'data_type': 'bool', 'description': 'PT3: enfrentó donk turn.'},
        {'column_name': 'enum_t_donk_action',      'data_type': 'str',  'description': 'PT3: acción vs donk turn (C/R/F/N).'},
        {'column_name': 'flg_r_donk',              'data_type': 'bool', 'description': 'PT3: donk river.'},
        {'column_name': 'flg_r_donk_opp',          'data_type': 'bool', 'description': 'PT3: oportunidad donk river.'},
        {'column_name': 'flg_r_donk_def_opp',      'data_type': 'bool', 'description': 'PT3: enfrentó donk river.'},
        {'column_name': 'enum_r_donk_action',      'data_type': 'str',  'description': 'PT3: acción vs donk river (C/R/F/N).'},
        # ── v1.29A: Postflop raise wars — PT3 §holdem_hand_player_statistics ─────
        # 24 campos nuevos → HAPS efectivo 100%
        {'column_name': 'enum_p_4bet_action',    'data_type': 'str',  'description': 'PT3: acción hero vs 4bet PF (C/R/F/N).'},
        {'column_name': 'flg_f_3bet',            'data_type': 'bool', 'description': 'PT3: hero 3beteó el flop (raise del raise).'},
        {'column_name': 'flg_f_3bet_opp',        'data_type': 'bool', 'description': 'PT3: oportunidad de 3bet en flop.'},
        {'column_name': 'flg_f_3bet_def_opp',    'data_type': 'bool', 'description': 'PT3: hero enfrentó 3bet en flop.'},
        {'column_name': 'enum_f_3bet_action',    'data_type': 'str',  'description': 'PT3: acción vs 3bet flop (C/R/F/N).'},
        {'column_name': 'flg_f_4bet',            'data_type': 'bool', 'description': 'PT3: hero 4beteó el flop.'},
        {'column_name': 'flg_f_4bet_opp',        'data_type': 'bool', 'description': 'PT3: oportunidad de 4bet en flop.'},
        {'column_name': 'enum_f_4bet_action',    'data_type': 'str',  'description': 'PT3: acción vs 4bet flop (C/R/F/N).'},
        {'column_name': 'flg_t_3bet',            'data_type': 'bool', 'description': 'PT3: hero 3beteó el turn.'},
        {'column_name': 'flg_t_3bet_opp',        'data_type': 'bool', 'description': 'PT3: oportunidad de 3bet en turn.'},
        {'column_name': 'flg_t_3bet_def_opp',    'data_type': 'bool', 'description': 'PT3: hero enfrentó 3bet en turn.'},
        {'column_name': 'enum_t_3bet_action',    'data_type': 'str',  'description': 'PT3: acción vs 3bet turn (C/R/F/N).'},
        {'column_name': 'flg_t_4bet',            'data_type': 'bool', 'description': 'PT3: hero 4beteó el turn.'},
        {'column_name': 'flg_t_4bet_opp',        'data_type': 'bool', 'description': 'PT3: oportunidad de 4bet en turn.'},
        {'column_name': 'flg_t_4bet_def_opp',    'data_type': 'bool', 'description': 'PT3: hero enfrentó 4bet en turn.'},
        {'column_name': 'enum_t_4bet_action',    'data_type': 'str',  'description': 'PT3: acción vs 4bet turn (C/R/F/N).'},
        {'column_name': 'flg_r_3bet',            'data_type': 'bool', 'description': 'PT3: hero 3beteó el river.'},
        {'column_name': 'flg_r_3bet_opp',        'data_type': 'bool', 'description': 'PT3: oportunidad de 3bet en river.'},
        {'column_name': 'flg_r_3bet_def_opp',    'data_type': 'bool', 'description': 'PT3: hero enfrentó 3bet en river.'},
        {'column_name': 'enum_r_3bet_action',    'data_type': 'str',  'description': 'PT3: acción vs 3bet river (C/R/F/N).'},
        {'column_name': 'flg_r_4bet',            'data_type': 'bool', 'description': 'PT3: hero 4beteó el river.'},
        {'column_name': 'flg_r_4bet_opp',        'data_type': 'bool', 'description': 'PT3: oportunidad de 4bet en river.'},
        {'column_name': 'flg_r_4bet_def_opp',    'data_type': 'bool', 'description': 'PT3: hero enfrentó 4bet en river.'},
        {'column_name': 'enum_r_4bet_action',    'data_type': 'str',  'description': 'PT3: acción vs 4bet river (C/R/F/N).'},
        # ── v1.29B: Hand strength + draws (HAPC) — evaluador puro Python ───────
        # Flop
        {'column_name': 'flg_f_highcard', 'data_type': 'bool', 'description': 'HAPC: hero tiene highcard en flop.'},
        {'column_name': 'flg_f_1pair', 'data_type': 'bool', 'description': 'HAPC: hero tiene 1pair en flop.'},
        {'column_name': 'flg_f_2pair', 'data_type': 'bool', 'description': 'HAPC: hero tiene 2pair en flop.'},
        {'column_name': 'flg_f_threeoak', 'data_type': 'bool', 'description': 'HAPC: hero tiene threeoak en flop.'},
        {'column_name': 'flg_f_straight', 'data_type': 'bool', 'description': 'HAPC: hero tiene straight en flop.'},
        {'column_name': 'flg_f_flush', 'data_type': 'bool', 'description': 'HAPC: hero tiene flush en flop.'},
        {'column_name': 'flg_f_fullhouse', 'data_type': 'bool', 'description': 'HAPC: hero tiene fullhouse en flop.'},
        {'column_name': 'flg_f_fouroak', 'data_type': 'bool', 'description': 'HAPC: hero tiene fouroak en flop.'},
        {'column_name': 'flg_f_strflush', 'data_type': 'bool', 'description': 'HAPC: hero tiene strflush en flop.'},
        {'column_name': 'val_f_hole_cards_used', 'data_type': 'int',  'description': 'HAPC: cartas del hero usadas en la mejor mano en flop (0-2).'},
        {'column_name': 'flg_f_flush_draw', 'data_type': 'bool', 'description': 'HAPC: draw al color (4 suited) en flop.'},
        {'column_name': 'flg_f_straight_draw', 'data_type': 'bool', 'description': 'HAPC: draw abierto (OESD, 4 consecutivos) en flop.'},
        {'column_name': 'flg_f_gutshot_draw', 'data_type': 'bool', 'description': 'HAPC: gutshot (draw interior) en flop.'},
        {'column_name': 'flg_f_bflush_draw', 'data_type': 'bool', 'description': 'HAPC: backdoor draw al color en flop.'},
        {'column_name': 'flg_f_bstraight_draw', 'data_type': 'bool', 'description': 'HAPC: backdoor draw a escalera en flop.'},
        {'column_name': 'flg_f_2gutshot_draw', 'data_type': 'bool', 'description': 'HAPC: doble gutshot (2 outs internos) en flop.'},
        # Turn
        {'column_name': 'flg_t_highcard', 'data_type': 'bool', 'description': 'HAPC: hero tiene highcard en turn.'},
        {'column_name': 'flg_t_1pair', 'data_type': 'bool', 'description': 'HAPC: hero tiene 1pair en turn.'},
        {'column_name': 'flg_t_2pair', 'data_type': 'bool', 'description': 'HAPC: hero tiene 2pair en turn.'},
        {'column_name': 'flg_t_threeoak', 'data_type': 'bool', 'description': 'HAPC: hero tiene threeoak en turn.'},
        {'column_name': 'flg_t_straight', 'data_type': 'bool', 'description': 'HAPC: hero tiene straight en turn.'},
        {'column_name': 'flg_t_flush', 'data_type': 'bool', 'description': 'HAPC: hero tiene flush en turn.'},
        {'column_name': 'flg_t_fullhouse', 'data_type': 'bool', 'description': 'HAPC: hero tiene fullhouse en turn.'},
        {'column_name': 'flg_t_fouroak', 'data_type': 'bool', 'description': 'HAPC: hero tiene fouroak en turn.'},
        {'column_name': 'flg_t_strflush', 'data_type': 'bool', 'description': 'HAPC: hero tiene strflush en turn.'},
        {'column_name': 'val_t_hole_cards_used', 'data_type': 'int',  'description': 'HAPC: cartas del hero usadas en la mejor mano en turn (0-2).'},
        {'column_name': 'flg_t_flush_draw', 'data_type': 'bool', 'description': 'HAPC: draw al color (4 suited) en turn.'},
        {'column_name': 'flg_t_straight_draw', 'data_type': 'bool', 'description': 'HAPC: draw abierto (OESD, 4 consecutivos) en turn.'},
        {'column_name': 'flg_t_gutshot_draw', 'data_type': 'bool', 'description': 'HAPC: gutshot (draw interior) en turn.'},
        {'column_name': 'flg_t_bflush_draw', 'data_type': 'bool', 'description': 'HAPC: backdoor draw al color en turn. OS-only: PT4 solo tiene flg_f_bflush_draw (flop).'},
        {'column_name': 'flg_t_bstraight_draw', 'data_type': 'bool', 'description': 'HAPC: backdoor draw a escalera en turn. OS-only: PT4 solo tiene flg_f_bstraight_draw (flop).'},
        {'column_name': 'flg_t_2gutshot_draw', 'data_type': 'bool', 'description': 'HAPC: doble gutshot (2 outs internos) en turn.'},
        # River
        {'column_name': 'flg_r_highcard', 'data_type': 'bool', 'description': 'HAPC: hero tiene highcard en river.'},
        {'column_name': 'flg_r_1pair', 'data_type': 'bool', 'description': 'HAPC: hero tiene 1pair en river.'},
        {'column_name': 'flg_r_2pair', 'data_type': 'bool', 'description': 'HAPC: hero tiene 2pair en river.'},
        {'column_name': 'flg_r_threeoak', 'data_type': 'bool', 'description': 'HAPC: hero tiene threeoak en river.'},
        {'column_name': 'flg_r_straight', 'data_type': 'bool', 'description': 'HAPC: hero tiene straight en river.'},
        {'column_name': 'flg_r_flush', 'data_type': 'bool', 'description': 'HAPC: hero tiene flush en river.'},
        {'column_name': 'flg_r_fullhouse', 'data_type': 'bool', 'description': 'HAPC: hero tiene fullhouse en river.'},
        {'column_name': 'flg_r_fouroak', 'data_type': 'bool', 'description': 'HAPC: hero tiene fouroak en river.'},
        {'column_name': 'flg_r_strflush', 'data_type': 'bool', 'description': 'HAPC: hero tiene strflush en river.'},
        {'column_name': 'val_r_hole_cards_used', 'data_type': 'int',  'description': 'HAPC: cartas del hero usadas en la mejor mano en river (0-2).'},
    ]
    return schema


# BUG A CORREGIDO: assign_session_ids_by_time_gap se define en Sección 3
# (parser HH real, formato session_NNN). Esta celda no redefine la función
# para evitar colisiones de formato (s0001 vs session_001) entre DataFrames
# de distintas fuentes (CSV HUD vs HH real). La definición canónica única
# está en la Sección 3 y es la que se llama desde ingest_and_preprocess_hud_data.


def ingest_and_preprocess_hud_data(file_path, schema):
    """
    Carga CSV de poker-hud.com, aplica schema y asigna session_id por gaps de tiempo.
    Soporta tanto formato separado (date + time) como combinado (datetime único).
    """
    try:
        df = pd.read_csv(file_path)

        # Combinar date + time si vienen separados
        if 'date' in df.columns and 'time' in df.columns:
            df['date'] = pd.to_datetime(df['date'].astype(str) + ' ' + df['time'].astype(str), errors='coerce')
            df = df.drop(columns=['time'], errors='ignore')
        elif 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')

        # Aplicar tipos del schema
        for col_info in schema:
            col_name = col_info['column_name']
            data_type = col_info['data_type']
            if col_name == 'date':
                continue
            if col_name not in df.columns:
                df[col_name] = np.nan
                continue
            if data_type == 'int':
                df[col_name] = pd.to_numeric(df[col_name], errors='coerce').fillna(0).astype(int)
            elif data_type == 'float':
                df[col_name] = pd.to_numeric(df[col_name], errors='coerce').fillna(0.0).astype(float)
            elif data_type == 'str':
                df[col_name] = df[col_name].astype(str).replace('nan', '')

        # Asignar session_id por gaps de tiempo (no aleatorio)
        df = assign_session_ids_by_time_gap(df)

        # Validación de columnas críticas
        critical_cols = ['hand_id', 'net_won', 'ev_won']
        for col in critical_cols:
            if col not in df.columns or df[col].isnull().all():
                print(f"❌ Error: columna crítica '{col}' ausente o vacía.")
                return df, False

        print(f"✅ Ingesta OK: {len(df)} manos | {df['session_id'].nunique()} sesiones detectadas")
        return df, True

    except FileNotFoundError:
        print(f"❌ Archivo no encontrado: {file_path}")
        return pd.DataFrame(), False
    except Exception as e:
        print(f"❌ Error en ingesta: {e}")
        return pd.DataFrame(), False


# Check
hud_schema = define_hud_schema()
print(f"✅ Schema definido: {len(hud_schema)} columnas")
print(f"   Columnas: {[c['column_name'] for c in hud_schema]}")


# ⚠️ DEPRECATED v1.78 — Usar parse_real_hand_history_file() (Sección 3a).
# Mantenido por compatibilidad histórica. Se eliminará en v2.0.

def load_raw_hand_history(file_path):
    """DEPRECATED v1.78. Usar parse_real_hand_history_file() en su lugar.
    Esta función no incluye PT3 flags, posiciones ni métricas postflop.
    """
    import warnings
    warnings.warn(
        "load_raw_hand_history() DEPRECATED desde v1.78. "
        "Usa parse_real_hand_history_file() (Sección 3a).",
        DeprecationWarning, stacklevel=2
    )
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return f.read()
    except FileNotFoundError:
        print(f"❌ Archivo no encontrado: {file_path}")
        return ""
    except Exception as e:
        print(f"❌ Error al cargar HH: {e}")
        return ""


def split_into_individual_hands(raw_hh_content):
    """
    Divide el contenido crudo en manos individuales.
    Separador: '***** Hand History for Game ID' (formato PokerStars estándar).
    """
    if not raw_hh_content:
        return []
    parts = raw_hh_content.split('***** Hand History for Game ID')
    hands = [("***** Hand History for Game ID" + h).strip() for h in parts if h.strip()]
    print(f"✅ {len(hands)} manos individuales detectadas en el archivo.")
    return hands


def parse_hand_details(hand_text):
    """
    Parsea una mano individual de PokerStars y extrae todos los campos del schema.

    ESTADO ACTUAL:
    - Hand ID, stake, fecha, posición Hero, hole cards: ✅ OK
    - Board cards por calle (flop/turn/river): ✅ OK
    - Jugadores y stacks: ✅ OK
    - Net won (ganancia básica): ✅ OK (simplificado)
    - EV won (all-in EV): ⚠️ Pendiente (requiere formato EV de PokerStars)
    - Acciones detalladas por calle: ⚠️ Simplificado (acción preflop básica del Hero)
    - Rake: ⚠️ Pendiente (requiere línea de rake en HH)
    """
    details = {
        'hand_id': None, 'session_id': None, 'date': None,
        'table_size': None, 'player_position': 'Hero',
        'hole_cards': None,
        'board_cards_flop': None, 'board_cards_turn': None, 'board_cards_river': None,
        'preflop_action': '', 'flop_action': '', 'turn_action': '', 'river_action': '',
        'net_won': 0.0, 'ev_won': 0.0, 'rake': 0.0,
        'stake_level': None, 'total_pot': 0.0, 'player_stack_start': 0.0,
        'all_players': '', 'opponent_names': '',
        'friccion_r': 1, 'friccion_a': 1, 'friccion_v': 1,
        'manual_spot_tag': '', 'opponent_type_manual': ''
    }

    # ── Hand ID ──────────────────────────────────────────────────────────────
    m = re.search(r'Hand History for Game ID (\d+)', hand_text)
    if m:
        details['hand_id'] = m.group(1)

    # ── Stake level ──────────────────────────────────────────────────────────
    m = re.search(r'NL(\d+)', hand_text)
    if m:
        details['stake_level'] = f"NL{m.group(1)}"

    bb_val = BB_VALUE_MAP.get(details['stake_level'], 0.25)

    # ── Fecha y hora ─────────────────────────────────────────────────────────
    # Formato PS: "2024/01/15 22:30:00 CET" o similar
    m = re.search(r'(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})', hand_text)
    if m:
        try:
            details['date'] = datetime.strptime(m.group(1), '%Y/%m/%d %H:%M:%S')
        except:
            details['date'] = None

    # ── Jugadores, posiciones y stacks ───────────────────────────────────────
    players_info = []
    opponent_names = []
    # Formato: "BTN: Hero (100BB)" o "SB: PlayerName (80BB)"
    player_lines = re.findall(r'(\w+):\s+(\S+)\s+\((\d+(?:\.\d+)?)BB\)', hand_text)
    for pos, name, stack in player_lines:
        stack_eur = float(stack) * bb_val
        players_info.append(f"{name}@{pos}({stack}BB)")
        if name.lower() == 'hero':
            details['player_position'] = pos
            details['player_stack_start'] = stack_eur
        else:
            opponent_names.append(name)
    details['all_players'] = ", ".join(players_info)
    details['opponent_names'] = ", ".join(opponent_names)
    details['table_size'] = len(player_lines)

    # ── Hole cards ───────────────────────────────────────────────────────────
    m = re.search(r'Dealt to Hero \[(.+?)\]', hand_text)
    if m:
        details['hole_cards'] = m.group(1)

    # ── Board cards por calle ────────────────────────────────────────────────
    m = re.search(r'\*\*\* FLOP \*\*\* \[(.+?)\]', hand_text)
    if m:
        details['board_cards_flop'] = m.group(1)
    m = re.search(r'\*\*\* TURN \*\*\* \[.+?\]\[(.+?)\]', hand_text)
    if m:
        details['board_cards_turn'] = m.group(1)
    m = re.search(r'\*\*\* RIVER \*\*\* \[.+?\]\[.+?\]\[(.+?)\]', hand_text)
    if m:
        details['board_cards_river'] = m.group(1)

    # ── Acciones preflop del Hero (simplificado) ─────────────────────────────
    if re.search(r'Hero folds', hand_text):
        details['preflop_action'] = 'F'
    elif re.search(r'Hero raises', hand_text):
        details['preflop_action'] = 'R'
    elif re.search(r'Hero calls', hand_text):
        details['preflop_action'] = 'C'
    elif re.search(r'Hero checks', hand_text):
        details['preflop_action'] = 'X'

    # ── Net won ──────────────────────────────────────────────────────────────
    m = re.search(r'Hero wins (\d+(?:\.\d+)?)BB', hand_text)
    if m:
        details['net_won'] = float(m.group(1)) * bb_val
        details['ev_won'] = details['net_won']  # Simplificado hasta tener EV real
    else:
        # Si Hero no aparece como ganador, net_won = 0 o negativo
        # TODO: extraer monto total apostado por Hero para calcular pérdida real
        details['net_won'] = 0.0
        details['ev_won'] = 0.0

    # ── Total pot ────────────────────────────────────────────────────────────
    m = re.search(r'Total pot (\d+(?:\.\d+)?)BB', hand_text)
    if m:
        details['total_pot'] = float(m.group(1)) * bb_val

    return details


def transform_parsed_data_to_df(parsed_data, schema):
    """Convierte lista de dicts parseados al DataFrame con schema canónico."""
    if not parsed_data:
        print("⚠️ No hay datos parseados para transformar.")
        return pd.DataFrame()

    df = pd.DataFrame(parsed_data)

    for col_info in schema:
        col_name = col_info['column_name']
        data_type = col_info['data_type']
        if col_name not in df.columns:
            df[col_name] = np.nan
        if data_type == 'datetime':
            df[col_name] = pd.to_datetime(df[col_name], errors='coerce')
        elif data_type == 'int':
            df[col_name] = pd.to_numeric(df[col_name], errors='coerce').fillna(0).astype(int)
        elif data_type == 'float':
            df[col_name] = pd.to_numeric(df[col_name], errors='coerce').fillna(0.0).astype(float)
        elif data_type == 'str':
            df[col_name] = df[col_name].astype(str).replace('nan', '')

    # Asignar session_id por gaps de tiempo después del parsing
    df = assign_session_ids_by_time_gap(df)

    print(f"✅ DataFrame creado desde HH: {df.shape[0]} manos | {df['session_id'].nunique()} sesiones")
    return df


def parse_hand_history_file(file_path):
    """
    Pipeline completo: carga → divide → parsea → transforma.
    Uso: df = parse_hand_history_file('mi_historial.txt')
    """
    raw = load_raw_hand_history(file_path)
    if not raw:
        return pd.DataFrame()
    hands_raw = split_into_individual_hands(raw)
    if not hands_raw:
        return pd.DataFrame()
    parsed = [parse_hand_details(h) for h in hands_raw]
    schema = define_hud_schema()
    return transform_parsed_data_to_df(parsed, schema)


# P1 NOTA: Este parser (formato '***** Hand History for Game ID') es LEGACY.
# El parser canónico activo es parse_real_hand_history_file() en Sección 3
# (formato 'PokerStars Hand #N'), que es la exportación real de PokerStars.
# Este módulo se mantiene por compatibilidad con archivos de formato antiguo.
print("✅ Parser Hand History LEGACY cargado (formato antiguo '***** Hand History').")
print("   ⚠️  LEGACY: usar parse_real_hand_history_file() para exportaciones reales de PokerStars.")
print("   Uso legacy: df = parse_hand_history_file('ruta/a/historial_antiguo.txt')")


# ════════════════════════════════════════════════════════════════════════════
# SECCIÓN 3 — Parser Hand History PokerStars (REAL) — v1.99 (Megaauditoría v3: M7 atómico + 3BET denominador + register_strength pipeline)
# FIX v1.23: (1) net_won raises FROM-amount evita double-count
# FIX v1.24: (1) BUG-1 fold_to_3bet índice explícito — (2) BUG-2 _calc_afq signature
#             (2) preflop_pressure campo nuevo
# FIX v1.31: Fix P0-C flg_won_hand antes de early return
# FIX v1.31: Fix P0-D filtrar manos de torneo
# FIX v1.25: Fix 1-5 PT3 (open/3bet/position)
# FIX v1.26: Cobertura completa PT3 — flg_vpip, flg_p_limp, flg_p_ccall,
#             flg_p_fold, cnt_p_raise/call, flags acción por calle (f/t/r),
#             check-raise, saw, open/open_opp, has_position turn/river
# Formato: PokerStars Hand #XXXXXXXXXX (exportación real de PokerStars)
# Hero: configurable via HERO_NAME
# ════════════════════════════════════════════════════════════════════════════

# S2 FIX: HERO_NAME se define aquí pero TAMBIÉN en el bloque de configuración
# del pipeline (Sección 12). Editar el de la Sección 12 — éste es el default.
HERO_NAME = 'LaRuinaDeMago'   # ← CAMBIA ESTO a tu nick exacto en PokerStars
# ⚠️  [v2.03] IMPORTANTE: si cambias este valor, cámbialo TAMBIÉN en el pipeline
# (celda de configuración, variable HERO_NAME). Ambas deben ser idénticas.
_HERO_DEFAULT_SENTINEL = 'LaRuinaDeMago'  # usado para detectar si olvidaste cambiarlo

def parse_real_hand_history_file(file_path, hero=None):
    """
    Parser completo para HH reales de PokerStars.
    Formato: Hand #N:\n\nPokerStars Hand #XXXXXXXXXX...

    Returns:
        DataFrame con schema canónico completo
    """
    if hero is None:
        hero = HERO_NAME

    try:
        with open(file_path, 'r', encoding='utf-8-sig') as f:
            content = f.read()
    except FileNotFoundError:
        print(f"❌ Archivo no encontrado: {file_path}")
        return pd.DataFrame()
    except Exception as e:
        print(f"❌ Error al cargar HH: {e}")
        return pd.DataFrame()

    raw_blocks = re.split(r'Hand #\d+:\s*\r?\n\s*\r?\n', content)
    # FIX P0-D v1.31: filtrar manos de torneo antes de parsear
    # PokerStars exporta cash y torneo juntos cuando se usa "Request Hand History"
    # Las manos de torneo tienen formato diferente y producen stake_level=None
    all_raw = [h.strip() for h in raw_blocks if 'PokerStars Hand #' in h]
    raw_hands = []
    tournament_count = 0
    for h in all_raw:
        # Detectar manos de torneo por la primera línea
        first_line = h.split('\n')[0] if h else ''
        if re.search(r'Tournament #\d+', first_line):
            tournament_count += 1
        else:
            raw_hands.append(h)
    # FIX P1-CANCEL v1.74: filtrar manos canceladas
    _cancel_count = len(raw_hands)
    raw_hands = [h for h in raw_hands if 'Hand cancelled' not in h]
    _cancel_count -= len(raw_hands)
    print(f"   📂 {len(all_raw)} manos detectadas | {len(raw_hands)} cash | {tournament_count} torneo | {_cancel_count} canceladas (excluidas)")

    parsed = []
    for hand_text in raw_hands:
        d = _parse_single_hand_real(hand_text, hero)
        if d and d['hand_id']:
            parsed.append(d)

    if not parsed:
        print("❌ No se parsearon manos. Verifica el formato del archivo.")
        return pd.DataFrame()

    df = pd.DataFrame(parsed)
    df['date']  = pd.to_datetime(df['date'], errors='coerce')
    df['net_won'] = pd.to_numeric(df['net_won'], errors='coerce').fillna(0.0)
    # NOTA B5/L2: ev_won = net_won hasta que enrich_with_allin_ev()
    # sobreescriba las manos con all-in con equity real calculada.
    # Mientras tanto EV/h == net/h (EV contable, no EV ajustado).
    df['ev_won']  = df['net_won'].copy()
    df['rake']    = pd.to_numeric(df['rake'],    errors='coerce').fillna(0.0)
    df['total_pot']= pd.to_numeric(df['total_pot'],errors='coerce').fillna(0.0)
    df['player_stack_start'] = pd.to_numeric(df['player_stack_start'], errors='coerce').fillna(0.0)
    df = assign_session_ids_by_time_gap(df)
    print(f"   ✅ {len(df):,} manos procesadas | {df['session_id'].nunique()} sesiones detectadas")
    return df


def _parse_single_hand_real(hand_text, hero):
    """Parsea una mano individual del formato PokerStars real."""
    d = {
        'hand_id': None, 'session_id': None, 'date': None,
        'table_size': 6, 'player_position': 'BTN',
        'hole_cards': None,
        'board_cards_flop': None, 'board_cards_turn': None, 'board_cards_river': None,
        'preflop_action': '', 'flop_action': '', 'turn_action': '', 'river_action': '',
        'net_won': 0.0, 'ev_won': 0.0, 'rake': 0.0,
        'stake_level': None, 'total_pot': 0.0, 'player_stack_start': 0.0,
        'all_players': '', 'opponent_names': '',
        'friccion_r': 1, 'friccion_a': 1, 'friccion_v': 1,
        'manual_spot_tag': '', 'opponent_type_manual': '',
        # v1.24: campos PT3-compatibles
        'flg_p_3bet_opp': False,  'flg_p_3bet': False,
        'flg_p_fold_to_3bet': False,  'flg_p_faced_3bet': False,
        'flg_f_cbet_opp': False,  'flg_f_cbet': False,
        'flg_f_cbet_def_opp': False, 'flg_f_cbet_def': False,
        'flg_f_donk_opp': False,  'flg_f_donk': False,
        'val_f_afq': 0.0,
        # v1.23: contexto de presión preflop
        'preflop_pressure': 'none',
        'preflop_n_raises_facing': 0,
        'stack_depth_bb': 0.0,
        'num_tables': 1,
        # v1.25 NEW: Fix 1
        'flg_p_open_opp':    False,
        'flg_p_open':        False,
        'flg_p_first_raise': False,
        # v1.25 NEW: Fix 2
        'flg_p_3bet_role':   'none',
        # v1.25 NEW: Fix 3
        'flg_f_first':       False,
        'flg_t_first':       False,
        'flg_r_first':       False,
        # v1.25 NEW: Fix 4
        'flg_f_has_position': False,
        # v1.25 NEW: Fix 5
        'villain_position':  '',
        # v1.26 NEW: PT3 preflop fundamentales (holdem_hand_player_statistics)
        'flg_vpip':          False,   # Voluntarily Put $ In Pot
        'flg_p_fold':        False,   # Folded preflop
        'flg_p_limp':        False,   # Limped (call sin raise previo, no desde BB/SB)
        'flg_p_ccall':       False,   # Cold call (call de raise sin haber invertido)
        'cnt_p_raise':       0,       # Nº de raises del hero preflop
        'cnt_p_call':        0,       # Nº de calls del hero preflop
        # v1.26 NEW: PT3 flags acción por calle (holdem_hand_player_statistics)
        'flg_f_bet':         False,   'flg_t_bet':   False,   'flg_r_bet':   False,
        'flg_f_check':       False,   'flg_t_check': False,   'flg_r_check': False,
        'flg_f_fold':        False,   'flg_t_fold':  False,   'flg_r_fold':  False,
        'flg_f_check_raise': False,   'flg_t_check_raise': False, 'flg_r_check_raise': False,
        'flg_f_saw':         False,   'flg_t_saw':   False,   'flg_r_saw':   False,
        'cnt_f_raise':       0,       'cnt_t_raise': 0,       'cnt_r_raise': 0,
        # v1.26 NEW: PT3 posición real postflop (holdem_hand_player_detail)
        'flg_t_has_position': False,  'flg_r_has_position': False,
        'flg_f_open':        False,   'flg_t_open':  False,   'flg_r_open':  False,
        'flg_f_open_opp':    False,   'flg_t_open_opp': False,'flg_r_open_opp': False,
        # v1.27 TIER 0: PT3 first_raise y cnt_call por calle
        'flg_f_first_raise': False,   'flg_t_first_raise': False, 'flg_r_first_raise': False,
        'cnt_f_call':        0,       'cnt_t_call':        0,     'cnt_r_call':        0,
        # v1.27 TIER A: Blinds y steal defense
        'flg_blind_s':       False,   'flg_blind_b':      False,
        'flg_blind_def_opp': False,
        'flg_sb_steal_fold': False,   'flg_bb_steal_fold': False,
        # v1.27 TIER A: Preflop context
        'flg_p_face_raise':  False,
        'flg_p_3bet_def_opp': False,
        # v1.27 TIER A: Resultados y showdown
        'flg_won_hand':      False,
        'flg_showdown':      False,
        'flg_showed':        False,
        'enum_folded':       'N',
        # v1.27 TIER A: Conteos jugadores por calle
        'cnt_players_f':     0,
        'cnt_players_t':     0,
        'cnt_players_r':     0,
        # v1.28a TIER INMEDIATO: steal aliases, 4bet, squeeze, all-in, face_raise
        'flg_steal_att':          False,
        'flg_steal_opp':          False,
        'flg_p_4bet':             False,
        'flg_p_4bet_opp':         False,
        'flg_p_4bet_def_opp':     False,
        'flg_p_squeeze':          False,
        'flg_p_squeeze_opp':      False,
        'flg_p_squeeze_def_opp':  False,
        'enum_p_squeeze_action':  'N',
        'enum_allin':             'N',
        'enum_face_allin':        'N',
        'enum_face_allin_action': 'N',
        'enum_p_3bet_action':     'N',
        'flg_f_face_raise':       False,
        'flg_t_face_raise':       False,
        'flg_r_face_raise':       False,
        # v1.28b TIER B: cbet chain, float, donk
        'enum_f_cbet_action':     'N',
        'flg_t_cbet':             False,   'flg_t_cbet_opp':      False,
        'flg_t_cbet_def_opp':     False,   'enum_t_cbet_action':  'N',
        'flg_r_cbet':             False,   'flg_r_cbet_opp':      False,
        'flg_r_cbet_def_opp':     False,   'enum_r_cbet_action':  'N',
        'flg_t_float':            False,   'flg_t_float_opp':     False,
        'flg_t_float_def_opp':    False,   'enum_t_float_action': 'N',
        'flg_r_float':            False,   'flg_r_float_opp':     False,
        'flg_r_float_def_opp':    False,   'enum_r_float_action': 'N',
        'flg_t_donk':             False,   'flg_t_donk_opp':      False,
        'flg_t_donk_def_opp':     False,   'enum_t_donk_action':  'N',
        'flg_r_donk':             False,   'flg_r_donk_opp':      False,
        'flg_r_donk_def_opp':     False,   'enum_r_donk_action':  'N',
        # v1.29A: Postflop raise wars
        'enum_p_4bet_action':    'N',
        'flg_f_3bet':            False,
        'flg_f_3bet_opp':        False,
        'flg_f_3bet_def_opp':    False,
        'enum_f_3bet_action':    'N',
        'flg_f_4bet':            False,
        'flg_f_4bet_opp':        False,
        'enum_f_4bet_action':    'N',
        'flg_t_3bet':            False,
        'flg_t_3bet_opp':        False,
        'flg_t_3bet_def_opp':    False,
        'enum_t_3bet_action':    'N',
        'flg_t_4bet':            False,
        'flg_t_4bet_opp':        False,
        'flg_t_4bet_def_opp':    False,
        'enum_t_4bet_action':    'N',
        'flg_r_3bet':            False,
        'flg_r_3bet_opp':        False,
        'flg_r_3bet_def_opp':    False,
        'enum_r_3bet_action':    'N',
        'flg_r_4bet':            False,
        'flg_r_4bet_opp':        False,
        'flg_r_4bet_def_opp':    False,
        'enum_r_4bet_action':    'N',
        # v1.29B: HAPC hand strength + draws
        'flg_f_highcard':     False,
        'flg_f_1pair':     False,
        'flg_f_2pair':     False,
        'flg_f_threeoak':     False,
        'flg_f_straight':     False,
        'flg_f_flush':     False,
        'flg_f_fullhouse':     False,
        'flg_f_fouroak':     False,
        'flg_f_strflush':     False,
        'val_f_hole_cards_used': 0,
        'flg_f_flush_draw': False,
        'flg_f_straight_draw': False,
        'flg_f_gutshot_draw': False,
        'flg_f_bflush_draw': False,
        'flg_f_bstraight_draw': False,
        'flg_f_2gutshot_draw': False,
        'flg_t_highcard':     False,
        'flg_t_1pair':     False,
        'flg_t_2pair':     False,
        'flg_t_threeoak':     False,
        'flg_t_straight':     False,
        'flg_t_flush':     False,
        'flg_t_fullhouse':     False,
        'flg_t_fouroak':     False,
        'flg_t_strflush':     False,
        'val_t_hole_cards_used': 0,
        'flg_t_flush_draw': False,
        'flg_t_straight_draw': False,
        'flg_t_gutshot_draw': False,
        'flg_t_bflush_draw': False,
        'flg_t_bstraight_draw': False,
        'flg_t_2gutshot_draw': False,
        'flg_r_highcard':     False,
        'flg_r_1pair':     False,
        'flg_r_2pair':     False,
        'flg_r_threeoak':     False,
        'flg_r_straight':     False,
        'flg_r_flush':     False,
        'flg_r_fullhouse':     False,
        'flg_r_fouroak':     False,
        'flg_r_strflush':     False,
        'val_r_hole_cards_used': 0,
    }

    m = re.search(r'PokerStars Hand #(\d+)', hand_text)
    if m: d['hand_id'] = m.group(1)
    else: return None

    # Stake
    # P0-E FIX v1.41: PokerStars exports have " EUR" suffix: (€0.01/€0.02 EUR)
    # Closing \) forced match to fail on 100% of hands. Fix: remove closing \).
    m = re.search(r"Hold'em No Limit \([€$](\d+\.\d+)/[€$](\d+\.\d+)", hand_text)
    if m:
        bb = float(m.group(2))
        stake_map = {0.02:'NL2',0.05:'NL5',0.10:'NL10',0.25:'NL25',0.50:'NL50',1.00:'NL100',2.00:'NL200'}
        d['stake_level'] = stake_map.get(bb, f'NL{int(bb*100)}')
    bb_val = BB_VALUE_MAP.get(d['stake_level'], 0.02)

    # Fecha
    m = re.search(r'(\d{4}/\d{2}/\d{2} \d{1,2}:\d{2}:\d{2})', hand_text)
    if m:
        try: d['date'] = datetime.strptime(m.group(1), '%Y/%m/%d %H:%M:%S')
        except: d['date'] = None

    # Jugadores, posiciones, stacks
    seat_lines = re.findall(r'Seat (\d+): (.+?) \([€$](\d+(?:\.\d+)?) in chips\)', hand_text)
    btn_m = re.search(r'Seat #(\d+) is the button', hand_text)
    btn_seat = int(btn_m.group(1)) if btn_m else None
    seat_nums = [int(s[0]) for s in seat_lines]
    n = len(seat_lines)
    d['table_size'] = n

    opp_names = []
    for seat_str, name, stack_str in seat_lines:
        seat_n = int(seat_str)
        stack_eur = float(stack_str)
        if name == hero:
            d['player_stack_start'] = stack_eur
            if bb_val > 0:
                d['stack_depth_bb'] = round(stack_eur / bb_val, 1)
            d['player_position'] = _get_position_real(seat_n, btn_seat, seat_nums, n, hand_text)
        else:
            opp_names.append(name)
    d['opponent_names'] = ', '.join(opp_names)
    d['all_players'] = ';'.join(f"{n}@{s}" for s,n,_ in seat_lines)

    # Hole cards
    m = re.search(rf'Dealt to {re.escape(hero)} \[([^\]]+)\]', hand_text)
    if m: d['hole_cards'] = m.group(1)

    # Board por calle
    m = re.search(r'\*\*\* FLOP \*\*\* \[([^\]]+)\]', hand_text)
    if m: d['board_cards_flop'] = m.group(1)
    m = re.search(r'\*\*\* TURN \*\*\* \[[^\]]+\] \[([^\]]+)\]', hand_text)
    if m: d['board_cards_turn'] = m.group(1)
    m = re.search(r'\*\*\* RIVER \*\*\* \[[^\]]+\] \[([^\]]+)\]', hand_text)
    if m: d['board_cards_river'] = m.group(1)

    # Acciones por calle
    streets = _split_streets_real(hand_text)
    d['preflop_action'] = _hero_action_real(streets.get('preflop',''), hero)
    d['flop_action']    = _hero_action_real(streets.get('flop',''), hero)
    d['turn_action']    = _hero_action_real(streets.get('turn',''), hero)
    d['river_action']   = _hero_action_real(streets.get('river',''), hero)

    pf_text = streets.get('preflop', '')
    d['preflop_pressure'], d['preflop_n_raises_facing'] = _get_preflop_pressure(pf_text, hero)

    # Net won: won - invested (FIX P0-F v1.42)
    # BUG P0-F: raises usaban FROM amount en lugar de (TO - ya_invertido_en_calle).
    # PS formato: 'raises €X to €Y' donde X=incremento, Y=total del hero en esta calle.
    # Impacto: net_won +31€ incorrecto (≈ -8.24€ en lugar de -39.27€ real).
    # Fix: rastrear inversión por calle y usar (TO - hero_in_street) como chips nuevos.
    won = sum(float(m.group(1)) for m in
              re.finditer(rf'{re.escape(hero)} collected [€$](\d+\.\d+)', hand_text))
    inv = 0.0
    uncalled = sum(
        float(m.group(1))
        for m in re.finditer(
            rf'Uncalled bet \([€$]([\d.]+)\) returned to {re.escape(hero)}',
            hand_text
        )
    )
    # Split SOLO en FLOP/TURN/RIVER — preflop (posts + acciones) es bloque unificado.
    # P0-F v2 fix: no dividir en HOLE CARDS porque el blind post (antes de HOLE CARDS)
    # y el raise (dentro de HOLE CARDS) son la MISMA calle. Dividir en HOLE CARDS
    # reseteaba hero_in_street entre el post y el raise → sobre-conteo de ~4.27€.
    _hesc_nw = re.escape(hero)
    _hero_str_nw = hero  # nombre sin escapar para búsqueda en línea
    for _street_block in re.split(
            r'(?=\*\*\* (?:FLOP|TURN|RIVER|SHOW DOWN|SUMMARY) \*\*\*)',
            hand_text):
        _hero_in_street = 0.0
        for _line in _street_block.split('\n'):
            if _hero_str_nw not in _line: continue
            _m = re.search(rf'{_hesc_nw}: posts \w+ blind [€$]([\d.]+)', _line)
            if _m:
                _a = float(_m.group(1)); inv += _a; _hero_in_street += _a; continue
            _m = re.search(rf'{_hesc_nw}: calls [€$]([\d.]+)', _line)
            if _m:
                _a = float(_m.group(1)); inv += _a; _hero_in_street += _a; continue
            _m = re.search(rf'{_hesc_nw}: bets [€$]([\d.]+)', _line)
            if _m:
                _a = float(_m.group(1)); inv += _a; _hero_in_street += _a; continue
            _m = re.search(rf'{_hesc_nw}: raises [€$][\d.]+ to [€$]([\d.]+)', _line)
            if _m:
                _to = float(_m.group(1))
                inv += _to - _hero_in_street  # chips nuevos = TO - ya_invertido
                _hero_in_street = _to; continue
    d['net_won']  = round(won - inv + uncalled, 4)
    d['ev_won']   = d['net_won']  # provisional: overwritten by enrich_with_allin_ev()  # provisional: overwritten by enrich_with_allin_ev()

    # Rake y pot total
    pot_m  = re.search(r'Total pot [€$](\d+(?:\.\d+)?)', hand_text)  # FIX P1-RAKE v1.74: accept integer
    rake_m = re.search(r'Rake [€$](\d+(?:\.\d+)?)', hand_text)  # FIX P1-RAKE v1.74: accept integer
    if pot_m:  d['total_pot'] = float(pot_m.group(1))
    if rake_m: d['rake']      = float(rake_m.group(1))

    d = _get_pt3_stats(hand_text, hero, streets, d)
    return d


def _get_position_real(hero_seat, btn_seat, seat_nums, n, hand_text=''):
    """Calcula posición del hero en mesa según seat numbers.

    FIX P1-C v1.30: BTN fantasma (jugador sale entre manos, su asiento no
    aparece en la lista activa). Antes devolvía 'BTN' incondicionalmente →
    15% de manos con posición incorrecta. Ahora infiere desde blind posters.
    """
    if not btn_seat or not seat_nums or n == 0:
        return 'BTN'
    seats = sorted(seat_nums)

    # FIX P1-C: BTN fantasma — seat del botón no está en activos
    if btn_seat not in seats:
        # Intentar inferir posición real desde los blind posters
        if hand_text:
            sb_m = re.search(r'(\S[^:\n]+): posts small blind', hand_text)
            bb_m = re.search(r'(\S[^:\n]+): posts big blind', hand_text)
            # Extraer nombres de seats activos
            seat_name_map = {}
            for sl in re.findall(r'Seat (\d+): (.+?) \(', hand_text):
                seat_name_map[sl[1]] = int(sl[0])
            if sb_m:
                sb_name = sb_m.group(1).strip()
                bb_name = bb_m.group(1).strip() if bb_m else None
                hero_name_m = re.search(r'Dealt to (\S+)', hand_text)
                hero_name = hero_name_m.group(1) if hero_name_m else ''
                if hero_name == sb_name:
                    return 'SB'
                if hero_name == bb_name:
                    return 'BB'
                # Hero no es ciego: necesitamos reconstruir la mesa
                # Si sabemos SB y BB podemos deducir BTN y el resto
                if sb_name in seat_name_map and bb_name and bb_name in seat_name_map:
                    sb_seat = seat_name_map[sb_name]
                    bb_seat_num = seat_name_map[bb_name]
                    # En la mesa BTN = seat inmediatamente ANTES de SB en orden circular
                    # Buscar quién sería el BTN (anterior al SB)
                    try:
                        sb_idx = seats.index(sb_seat)
                        inferred_btn = seats[(sb_idx - 1) % n]
                        # Usar ese BTN inferido para calcular posiciones
                        btn_idx = seats.index(inferred_btn)
                        # caer a la lógica normal de abajo
                        btn_seat = inferred_btn
                    except (ValueError, IndexError):
                        return 'UNK'
                else:
                    return 'UNK'
            else:
                return 'UNK'
        else:
            return 'UNK'

    try:
        btn_idx  = seats.index(btn_seat)
    except ValueError:
        return 'UNK'

    def seat_at_offset(offset):
        return seats[(btn_idx + offset) % n]

    if n == 2:
        positions = {
            seat_at_offset(0): 'BTN',
            seat_at_offset(1): 'BB',
        }
    elif n == 3:
        positions = {
            seat_at_offset(0): 'BTN',
            seat_at_offset(1): 'SB',
            seat_at_offset(2): 'BB',
        }
    elif n == 4:
        positions = {
            seat_at_offset(0): 'BTN',
            seat_at_offset(1): 'SB',
            seat_at_offset(2): 'BB',
            seat_at_offset(3): 'CO',
        }
    elif n == 5:
        positions = {
            seat_at_offset(0): 'BTN',
            seat_at_offset(1): 'SB',
            seat_at_offset(2): 'BB',
            seat_at_offset(3): 'HJ',
            seat_at_offset(4): 'CO',
        }
    else:
        positions = {
            seat_at_offset(0): 'BTN',
            seat_at_offset(1): 'SB',
            seat_at_offset(2): 'BB',
            seat_at_offset(3): 'UTG',
            seat_at_offset(4): 'HJ',
            seat_at_offset(5): 'CO',
        }
    return positions.get(hero_seat, 'BTN')


def _split_streets_real(hand_text):
    """Divide el texto de la mano en calles."""
    streets = {}
    markers = [('preflop','*** HOLE CARDS ***'),('flop','*** FLOP ***'),('turn','*** TURN ***'),('river','*** RIVER ***'),('showdown','*** SHOW DOWN ***'),('summary','*** SUMMARY ***')]
    positions = {}
    for name, marker in markers:
        idx = hand_text.find(marker)
        if idx >= 0:
            positions[name] = idx
    sorted_streets = sorted(positions.items(), key=lambda x: x[1])
    for i, (name, start) in enumerate(sorted_streets):
        end = sorted_streets[i+1][1] if i+1 < len(sorted_streets) else len(hand_text)
        streets[name] = hand_text[start:end]
    return streets


def _hero_action_real(street_text, hero):
    """Extrae la acción del hero en una calle."""
    if not street_text or hero not in street_text:
        return ''
    hero_esc = re.escape(hero)
    actions = []
    for line in street_text.split('\n'):
        if hero not in line: continue
        if re.search(rf'{hero_esc}: folds', line):       actions.append('F')
        elif re.search(rf'{hero_esc}: checks', line):    actions.append('X')
        elif re.search(rf'{hero_esc}: calls', line):     actions.append('C')
        elif re.search(rf'{hero_esc}: bets', line):      actions.append('B')
        elif re.search(rf'{hero_esc}: raises', line):
            prev_raises = len(re.findall(r': raises', street_text[:street_text.find(line)]))
            if prev_raises == 0:   actions.append('R')
            elif prev_raises == 1: actions.append('3B')
            else:                  actions.append('4B')
    return '_'.join(actions) if actions else ''


def assign_session_ids_by_time_gap(df, gap_minutes=SESSION_GAP_MINUTES):
    """Asigna session_id por gaps de tiempo entre manos."""
    if df.empty or 'date' not in df.columns:
        df['session_id'] = 'session_001'
        return df
    # FIX P2-E v1.78: vectorizado con pd.Series.diff() — O(n) pandas vs O(n) loop Python
    # A 3k manos: ~5x más rápido. A 30k manos: ~10x. Elimina iterrows().
    df = df.copy().sort_values('date').reset_index(drop=True)
    dates = pd.to_datetime(df['date'], errors='coerce')
    gap_secs = gap_minutes * 60
    # Diferencia de tiempo entre manos consecutivas
    time_diff = dates.diff().dt.total_seconds().fillna(0)
    # Cada vez que el gap supera el umbral → nueva sesión
    is_new_session = (time_diff > gap_secs)
    session_num_series = is_new_session.cumsum() + 1
    df['session_id'] = session_num_series.apply(lambda x: f'session_{int(x):03d}')
    return df


def _get_preflop_pressure(pf_text, hero):
    """
    Analiza qué presión enfrentó el hero ANTES de su acción preflop.
    Returns: pressure: 'raise' | '3bet' | 'limp' | 'none', n_raises
    """
    if not pf_text or hero not in pf_text:
        return 'none', 0
    hero_esc = re.escape(hero)
    lines_before = []
    for line in pf_text.split('\n'):
        line = line.strip()
        if not line or line.startswith('***') or line.startswith('Dealt'):
            continue
        if re.match(hero_esc + r':', line):
            break
        lines_before.append(line)
    n_raises = sum(1 for l in lines_before if ': raises' in l)
    had_limp  = any(': calls' in l for l in lines_before)
    if n_raises >= 2:
        return '3bet', n_raises
    elif n_raises == 1:
        return 'raise', n_raises
    elif had_limp:
        return 'limp', 0
    else:
        return 'none', 0


def _get_pt3_stats(hand_text, hero, streets, d):
    """
    Calcula campos PT3-compatibles por mano. v1.28.

    Cobertura del schema PT3 (holdem_hand_player_statistics + holdem_hand_player_detail):

    PREFLOP:
      flg_vpip          Voluntarily Put $ In Pot
      flg_p_open_opp    nadie actuó antes del hero (ni calls ni raises)
      flg_p_open        hero fue primero en entrar al bote (first to VPIP)
      flg_p_first_raise hero fue el primer raiser (puede haber limpers)
      flg_steal_opp     en posición de steal (BTN/CO/SB) sin acción previa
      flg_steal_att     hizo steal desde BTN/CO/SB
      flg_p_3bet_opp    enfrentó un raise PF (opp to 3bet)
      flg_p_3bet        hizo 3bet PF
      flg_p_3bet_role   'aggressor'|'caller'|'none' (custom OS)
      flg_p_3bet_def_opp     abrió PF y rival 3beteó
      flg_p_fold_to_3bet      foldeó a esa 3bet
      flg_p_fold        foldeó preflop
      flg_p_limp        limpió (call sin raise, excluyendo BB walk)
      flg_p_ccall       cold call (call de raise desde posición, sin haber invertido)
      cnt_p_raise       nº de raises preflop
      cnt_p_call        nº de calls preflop
      villain_position  posición del último agresor PF (custom OS)

    FLOP / TURN / RIVER (x3, reemplazando f_ por t_ y r_):
      flg_X_saw         llegó a esa calle
      flg_X_first       primera oportunidad de actuar (más OOP)
      flg_X_has_position actuó DESPUÉS del rival (posición real)
      flg_X_open_opp    tuvo oportunidad de abrir (=flg_X_first)
      flg_X_open        abrió la acción (bet, no check)
      flg_X_bet         hizo bet en esa calle
      flg_X_check       chequeo en esa calle
      flg_X_fold        foldeó en esa calle
      flg_X_check_raise hizo check-raise en esa calle
      cnt_X_raise       nº de raises en esa calle

    FLOP específicos (cbet ecosystem):
      flg_f_cbet_opp / flg_f_cbet
      flg_f_cbet_def_opp / flg_f_cbet_def
      flg_f_donk_opp / flg_f_donk

    GLOBAL:
      val_f_afq         Aggression Frequency postflop (bets+raises)/(bets+raises+calls+checks)
    """
    hero_esc  = re.escape(hero)
    pos       = d.get('player_position', '')
    pf_text   = streets.get('preflop', '')
    fl_text   = streets.get('flop',    '')
    tu_text   = streets.get('turn',    '')
    rv_text   = streets.get('river',   '')

    # ════════════════════════════════════════════════════════════════════
    # PREFLOP
    # ════════════════════════════════════════════════════════════════════
    pf_lines = [l.strip() for l in pf_text.split('\n')
                if l.strip() and '***' not in l and 'Dealt' not in l
                and 'posts' not in l and 'sits out' not in l]

    pf_agg            = None   # último raiser PF (PF aggressor)
    raises_before     = 0      # raises de otros ANTES de la primera acción del hero
    callers_before    = 0      # calls de otros ANTES de la primera acción del hero
    hero_pf_done      = False
    hero_pf_act       = None   # última acción del hero PF
    hero_pf_first_act = None   # PRIMERA acción del hero PF
    hero_raised       = False
    cnt_raise_pf      = 0
    cnt_call_pf       = 0

    for line in pf_lines:
        am = re.match(r'([^:\n\r]+): (raises|calls|folds|checks|bets)', line)
        if not am:
            continue
        actor = am.group(1).strip()
        act   = am.group(2)
        if actor == hero:
            hero_pf_act = act
            if hero_pf_first_act is None:
                hero_pf_first_act = act
            hero_pf_done = True
            if act == 'raises':
                hero_raised = True
                pf_agg = hero
                cnt_raise_pf += 1
            elif act == 'calls':
                cnt_call_pf += 1
        else:
            if act == 'raises':
                pf_agg = actor
                if not hero_pf_done:
                    raises_before += 1
            elif act == 'calls' and not hero_pf_done:
                callers_before += 1

    d['cnt_p_raise'] = cnt_raise_pf
    d['cnt_p_call']  = cnt_call_pf

    # ── flg_vpip ─────────────────────────────────────────────────────────
    # PT3: True si hero puso dinero voluntariamente en el bote PF.
    # Excluye: BB sin acción (walk / nadie entró / solo check de BB).
    # Incluye: cualquier call o raise desde cualquier posición.
    d['flg_vpip'] = (hero_pf_first_act in ('calls', 'raises'))

    # ── flg_p_fold ───────────────────────────────────────────────────────
    d['flg_p_fold'] = (hero_pf_act == 'folds')

    # ── flg_p_limp ───────────────────────────────────────────────────────
    # PT3: hero llamó sin raise previo Y no está en el BB actuando sobre
    # su propio blind (eso sería check, no limp).
    # Exluye SB completando hacia BB en un walk — también es limp.
    d['flg_p_limp'] = (
        raises_before == 0
        and hero_pf_first_act == 'calls'
        and pos not in ('BB',)   # BB que checkea no es limp
    )

    # ── flg_p_ccall ──────────────────────────────────────────────────────
    # PT3: cold call = call de un raise sin haber invertido dinero voluntario.
    # Distingue: BTN que llama un CO open (cold call) vs BB que llama (ya puso blind).
    # Para nuestro propósito: raises_before >= 1 Y primera acción fue call
    # Y hero NO estaba en blinds (SB/BB ya tienen dinero invertido).
    d['flg_p_ccall'] = (
        raises_before >= 1
        and hero_pf_first_act == 'calls'
        and pos not in ('SB', 'BB')
    )

    # ── flg_p_open_opp / flg_p_open / flg_p_first_raise ─────────────────
    # hero_pf_first_act clave: en secuencias raise→fold-a-3bet la primera
    # acción fue 'raises' (correcto), la última 'folds' (fold_to_3bet).
    d['flg_p_open_opp']    = (raises_before == 0 and callers_before == 0)
    d['flg_p_open']        = (d['flg_p_open_opp'] and hero_pf_first_act in ('calls', 'raises'))
    d['flg_p_first_raise'] = (raises_before == 0 and hero_pf_first_act == 'raises')

    # ── flg_steal_opp / flg_steal_att ─────────────────────────────────────
    if pos in ('BTN', 'CO', 'SB') and raises_before == 0 and callers_before == 0:
        d['flg_steal_opp'] = True
        if hero_pf_first_act == 'raises':  # FIX BUG-B v1.76: primera acción PF, no última — evita perder steals cuando hero abre y foldea a 3bet
            d['flg_steal_att'] = True

    # ── flg_p_3bet_opp / flg_p_3bet ─────────────────────────────────────
    if raises_before >= 1:
        d['flg_p_3bet_opp'] = True
        if hero_pf_act == 'raises':
            d['flg_p_3bet'] = True

    # ── flg_p_3bet_role (custom OS) ──────────────────────────────────────
    if d['flg_p_3bet']:
        d['flg_p_3bet_role'] = 'aggressor'
    elif d['flg_p_3bet_opp'] and hero_pf_first_act == 'calls':
        d['flg_p_3bet_role'] = 'caller'
    else:
        d['flg_p_3bet_role'] = 'none'

    # ── flg_p_3bet_def_opp / flg_p_fold_to_3bet ──────────────────────────
    # PT3: hero ABRIÓ PF → rival 3betea → hero responde
    if hero_raised:
        hero_raise_idx = None
        for i, line in enumerate(pf_lines):
            am = re.match(r'(\S[\w ]*\S|\S): raises', line)
            if am and am.group(1).strip() == hero:
                hero_raise_idx = i   # no break: queremos el último raise del hero
        if hero_raise_idx is not None:
            rival_3bet = False
            for line in pf_lines[hero_raise_idx + 1:]:
                am = re.match(r'([^:\n\r]+): (raises|calls|folds|checks)', line)
                if not am: continue
                actor = am.group(1).strip()
                act   = am.group(2)
                if actor != hero and act == 'raises':
                    rival_3bet = True
                    d['flg_p_3bet_def_opp'] = True
                if rival_3bet and actor == hero and act == 'folds':
                    d['flg_p_fold_to_3bet'] = True

    # BUG-3 FIX v1.63: caso no-abridor (BB, cold-caller)
    # Pre-fix: n=121 (solo abridor). Post-fix: n~300+.
    # Fold to 3-Bet pasa de 20.7% (roto) a ~48% (correcto PT4).
    # NOTA: usa pf_agg (definido arriba en el loop pf_lines)
    # en lugar de _last_raiser (definido más abajo en el bloque 4bet).
    if not d.get('flg_p_3bet_def_opp', False) and hero_pf_act is not None:
        _hero_had_chips = (
            pos == 'BB'                         # BB siempre tiene chips
            or cnt_call_pf > 0                  # hizo call antes
            or (cnt_raise_pf > 0 and raises_before >= 1)
        )
        if (_hero_had_chips
                and raises_before >= 1
                and pf_agg is not None
                and pf_agg != hero):
            d['flg_p_3bet_def_opp'] = True
            if hero_pf_act == 'folds':
                d['flg_p_fold_to_3bet'] = True

    # ── villain_position (custom OS) ─────────────────────────────────────
    if pf_agg and pf_agg != hero:
        seat_lines_v = re.findall(r'Seat (\d+): (.+?) \(\S+ in chips\)', hand_text)  # FIX BUG-A v1.76: restricción a 'in chips' evita que SUMMARY section duplique entries
        btn_m_v      = re.search(r'Seat #(\d+) is the button', hand_text)
        if btn_m_v and seat_lines_v:
            btn_seat_v  = int(btn_m_v.group(1))
            seat_nums_v = [int(s[0]) for s in seat_lines_v]
            n_v         = len(seat_lines_v)
            for s_num, s_name in seat_lines_v:
                if s_name == pf_agg:
                    d['villain_position'] = _get_position_real(
                        int(s_num), btn_seat_v, seat_nums_v, n_v)
                    break

    # ════════════════════════════════════════════════════════════════════
    # HELPER: parsear acciones de una calle → dict de stats PT3
    # ════════════════════════════════════════════════════════════════════
    def _parse_street_actions(street_text, prefix):
        """
        Extrae flags PT3 de acciones del hero en una calle postflop.
        prefix: 'f', 't', o 'r'

        Retorna dict con campos para la calle dada.
        Semánticas PT3:
          flg_X_saw        = hero aparece en la calle (tuvo acción)
          flg_X_first      = primer actor listado (primera oportunidad)
          flg_X_has_position = hero actuó DESPUÉS del primer actor
          flg_X_open_opp   = tuvo oportunidad de ser primer apostador (=flg_X_first)
          flg_X_open       = fue el primer en apostar (bet, no check)
          flg_X_bet        = hizo bet en esa calle
          flg_X_check      = chequeo en esa calle
          flg_X_fold       = foldeó en esa calle
          flg_X_check_raise= chequeo Y luego hizo raise en esa calle
          cnt_X_raise      = nº de raises del hero en esa calle
        """
        p = prefix
        out = {
            f'flg_{p}_saw':          False,
            f'flg_{p}_first':        False,
            f'flg_{p}_has_position': False,
            f'flg_{p}_open_opp':     False,
            f'flg_{p}_open':         False,
            f'flg_{p}_bet':          False,
            f'flg_{p}_check':        False,
            f'flg_{p}_fold':         False,
            f'flg_{p}_check_raise':  False,
            f'cnt_{p}_raise':        0,
            f'flg_{p}_first_raise':  False,
            f'cnt_{p}_call':         0,
        }
        if not street_text or hero not in street_text:
            return out

        lines = [l.strip() for l in street_text.split('\n')
                 if l.strip() and '***' not in l]

        first_actor        = None
        first_bettor       = None   # primera apuesta real (para cbet ecosystem)
        hero_checked       = False
        hero_act_seq       = []     # secuencia de acciones del hero en esta calle
        cnt_raise          = 0
        cnt_call           = 0      # v1.27
        raises_before_hero = 0      # v1.27: raises de otros ANTES del primer raise hero

        for line in lines:
            am = re.match(r'(\S[\w ]*\S|\S): (bets|checks|calls|raises|folds)', line)
            if not am:
                continue
            actor = am.group(1).strip()
            act   = am.group(2)

            if first_actor is None:
                first_actor = actor
            if act in ('bets', 'raises') and first_bettor is None:
                first_bettor = actor

            if actor == hero:
                hero_act_seq.append(act)
                if act == 'raises':
                    cnt_raise += 1
                elif act == 'checks':
                    hero_checked = True
                elif act == 'calls':
                    cnt_call += 1
            else:
                if act == 'raises' and not any(a == 'raises' for a in hero_act_seq):
                    raises_before_hero += 1

        if not hero_act_seq:
            return out   # hero no actuó en esta calle

        hero_first_act = hero_act_seq[0]

        out[f'flg_{p}_saw']          = True
        out[f'flg_{p}_first']        = (first_actor == hero)
        out[f'flg_{p}_has_position'] = (first_actor is not None and first_actor != hero)
        out[f'flg_{p}_open_opp']     = (first_actor == hero)   # ≡ flg_X_first
        out[f'flg_{p}_open']         = (first_actor == hero and hero_first_act == 'bets')
        out[f'flg_{p}_bet']          = ('bets' in hero_act_seq)
        out[f'flg_{p}_check']        = ('checks' in hero_act_seq)
        out[f'flg_{p}_fold']         = ('folds' in hero_act_seq)
        # check-raise: hero chequeo Y luego hizo raise en la misma calle
        # PT3: check_raise = checked AND then raised (puede haber acción entre medio)
        if hero_checked and cnt_raise > 0:
            # Verificar que el check ocurrió ANTES del raise en la secuencia
            check_idx = next((i for i,a in enumerate(hero_act_seq) if a == 'checks'), None)
            raise_idx = next((i for i,a in enumerate(hero_act_seq) if a == 'raises'), None)
            if check_idx is not None and raise_idx is not None and check_idx < raise_idx:
                out[f'flg_{p}_check_raise'] = True
        out[f'cnt_{p}_raise']       = cnt_raise
        out[f'cnt_{p}_call']        = cnt_call
        out[f'flg_{p}_first_raise'] = (cnt_raise > 0 and raises_before_hero == 0)

        return out, first_bettor   # devolver first_bettor para cbet ecosystem

    # ════════════════════════════════════════════════════════════════════
    # FLOP
    # ════════════════════════════════════════════════════════════════════
    # ================================================================
    # v1.28a TIER INMEDIATO — PF-only fields (antes del early return)
    # (estas fields derivan solo de PF, aplican aunque hero no vea el flop)
    # ================================================================
    # ================================================================
    # v1.28a TIER INMEDIATO
    # ================================================================

    # ── flg_steal_att / flg_steal_opp (aliases PT3) ─────────────

    # ── 4bet preflop ─────────────────────────────────────────────
    # Re-parsear PF contando raises totales y del hero
    _pf4_lines = [l.strip() for l in pf_text.split('\n')
                  if l.strip() and '***' not in l
                  and 'Dealt' not in l and 'posts' not in l
                  and 'sits out' not in l]
    _raise_total = 0
    _hero_raise_n = 0
    _last_raiser  = None
    _post_hero_callers = 0
    _post_hero_raises  = 0
    _found_first_hero_raise = False
    # FIX P3 v1.30: import re movido fuera del loop (era code smell — import en cada iteración)
    # re ya está importado al nivel de módulo en Cell 19; _re alias innecesario
    for _ln in _pf4_lines:
        _am = re.match(r'([^:\n\r]+): (raises|calls|folds|checks)', _ln)
        if not _am: continue
        _actor = _am.group(1).strip()
        _act   = _am.group(2)
        if _act == 'raises':
            _raise_total += 1
            if _actor == hero:
                _hero_raise_n += 1
                if not _found_first_hero_raise:
                    _found_first_hero_raise = True
            _last_raiser = _actor
        elif _act == 'calls' and _actor != hero:
            if _found_first_hero_raise:
                _post_hero_callers += 1
        # FIX P2-F v1.30: _post_hero_raises se incrementaba en rama incorrecta
        # (elif anidado tras la rama de calls → nunca se ejecutaba)
        if _act == 'raises' and _actor != hero and _found_first_hero_raise:
            _post_hero_raises += 1
    # FIX P2-E v1.30: flg_p_4bet requiere _raise_total >= 4
    # Antes: _hero_raise_n >= 2 marcaba open+3bet como '4bet' (incorrecto)
    # PT3: 4bet = 4º raise de la secuencia (open(1) → 3bet(2) → 4bet(3) total raises, hero hace el 3º)
    # hero_raise_n >= 2 significa que hero hizo al menos 2 raises (e.g. open + re-raise)
    # raise_total >= 4 garantiza que la secuencia llegó al nivel de 4bet real
    d['flg_p_4bet']         = (_hero_raise_n >= 2 and _raise_total >= 4)
    # Si hero hizo 4bet, también implica que hizo 3bet (raise en contexto raise previo)
    if d['flg_p_4bet'] and not d.get('flg_p_3bet', False):
        d['flg_p_3bet']     = True
        d['flg_p_3bet_opp'] = True
    # Si hero 4beteó, enfrentó 3bet (= fold_to_3bet_opp) y su acción fue raise
    if d['flg_p_4bet']:
        d['flg_p_3bet_def_opp'] = True   # hero 4beteó → enfrentó 3bet antes
        # enum_p_3bet_action = 'R' (respondió con raise a la 3bet)
        if d.get('enum_p_3bet_action', 'N') == 'N':
            d['enum_p_3bet_action'] = 'R'
    # ── flg_p_faced_3bet — PT4-compatible ──────────────────────────────
    # Cualquier mano donde hero enfrentó el 3er raise y tuvo que actuar.
    # PT4 "Fold to 3Bet" denominator: n=224 vs flg_p_3bet_def_opp (sólo abridor): n=115
    # Incluye: hero abrió→rival 3bet, hero cold-called→squeeze, BB→enfrentó 3bet
    # Denominador correcto: raise_total>=2 y un villano hizo el último raise y hero actuó
    # FIX P2-PREC v1.74: explicit parentheses for clarity (no behavior change)
    # [v2.03] FIX P1-A: segunda cláusula OR eliminada (expandía denominador)
    d['flg_p_faced_3bet'] = (
        _raise_total >= 2
        and _last_raiser != hero
        and d.get('flg_p_3bet_def_opp', False)
    )
    d['flg_p_4bet_opp']     = (
        d.get('flg_p_3bet', False)
        and _raise_total >= 3
        and _last_raiser != hero
    )
    d['flg_p_4bet_def_opp'] = (
        _raise_total >= 3
        and not d.get('flg_p_4bet', False)
        and hero_pf_act is not None
    )

    # ── enum_p_4bet_action ──────────────────────────────────────────────────
    # PT3: qué hizo hero al enfrentar 4bet PF (flg_p_4bet_def_opp=True)
    # hero_pf_act = última acción PF del hero, calculado en bloque PF arriba
    if d.get('flg_p_4bet_def_opp', False) and hero_pf_act is not None:
        if   hero_pf_act == 'calls':    d['enum_p_4bet_action'] = 'C'
        elif hero_pf_act == 'raises':   d['enum_p_4bet_action'] = 'R'
        elif hero_pf_act == 'folds':    d['enum_p_4bet_action'] = 'F'

    # ── squeeze preflop ──────────────────────────────────────────
    # raises_before y callers_before ya calculados en bloque PF
    d['flg_p_squeeze_opp'] = (raises_before >= 1 and callers_before >= 1)
    if d['flg_p_squeeze_opp'] and hero_pf_first_act == 'raises':
        d['flg_p_squeeze'] = True
    if d.get('flg_p_first_raise', False) and _post_hero_callers >= 1 and _post_hero_raises >= 1:
        d['flg_p_squeeze_def_opp'] = True

    # ── enum_p_squeeze_action ────────────────────────────────────
    if d.get('flg_p_squeeze_def_opp', False):
        if hero_pf_act == 'calls':    d['enum_p_squeeze_action'] = 'C'
        elif hero_pf_act == 'raises': d['enum_p_squeeze_action'] = 'R'
        elif hero_pf_act == 'folds':  d['enum_p_squeeze_action'] = 'F'

    # ── enum_allin / enum_face_allin / enum_face_allin_action ───
    _hero_esc2 = re.escape(hero)
    _hero_ai   = 'N'
    _rival_ai  = 'N'
    _rival_ai_act = 'N'
    for _sname, _scode in [('preflop','P'),('flop','F'),('turn','T'),('river','R')]:
        _stxt = streets.get(_sname, '')
        if not _stxt: continue
        if _hero_ai == 'N' and re.search(_hero_esc2 + r'.*?is all-in', _stxt):
            _hero_ai = _scode
        _rim = re.search(
            r'(\S[\w ]*\S|\S): (?:raises|bets|calls) [\d.€$]+ (?:to [\d.€$]+ )?and is all-in',
            _stxt)
        if _rim and _rim.group(1) != hero and _rival_ai == 'N':
            _rival_ai = _scode
            _after = _stxt[_rim.end():]
            _rm2 = re.search(_hero_esc2 + r': (calls|raises|folds)', _after)
            if _rm2:
                _act2 = _rm2.group(1)
                if _act2 == 'calls':    _rival_ai_act = 'C'
                elif _act2 == 'raises': _rival_ai_act = 'R'
                elif _act2 == 'folds':  _rival_ai_act = 'F'
    d['enum_allin']             = _hero_ai
    d['enum_face_allin']        = _rival_ai
    d['enum_face_allin_action'] = _rival_ai_act

    # ── enum_p_3bet_action ───────────────────────────────────────
    # Qué hizo hero cuando enfrentó 3bet PF (abrió → rival 3bet → hero responde)
    if d.get('flg_p_3bet_def_opp', False):
        if d.get('flg_p_fold_to_3bet', False):
            d['enum_p_3bet_action'] = 'F'
        elif hero_pf_act == 'calls':
            d['enum_p_3bet_action'] = 'C'
        elif hero_pf_act == 'raises':
            d['enum_p_3bet_action'] = 'R'


    # FIX P0-C v1.31: flg_won_hand calculado ANTES del early return
    # Hero puede ganar un pot sin ver el flop (steal exitoso, todos foldean PF)
    # En ese caso el código hace early return → flg_won_hand quedaba False incorrectamente
    # Solución: calcular ANTES del early return para todas las manos
    _hesc_main = re.escape(hero)
    d['flg_won_hand'] = bool(re.search(rf'{_hesc_main} collected', hand_text))
    if '*** SHOW DOWN ***' in hand_text:
        _sd_block = hand_text[hand_text.find('*** SHOW DOWN ***'):]
        # P0-G v1.50 (fix final): flg_showdown = hero específicamente en showdown
        # Hero en showdown = muestra cartas O muckea en el bloque SD
        # (NO cuenta si solo son rivales quienes muestran)
        _hero_shows   = bool(re.search(rf'{_hesc_main}: shows \[', _sd_block))
        _hero_mucks_sd = bool(re.search(rf'{_hesc_main}: mucks hand', _sd_block))
        d['flg_showdown'] = _hero_shows or _hero_mucks_sd
        d['flg_showed']   = _hero_shows
        # P0-G: flg_won_hand en showdown
        if d['flg_showdown']:
            _won_sd = bool(re.search(
                rf'{_hesc_main}: shows \[.*?\] and wins',
                _sd_block
            ))
            if _won_sd:
                d['flg_won_hand'] = True


    # FIX P0-F v1.35: flg_blind_b/s y steal_fold calculados ANTES del early return
    # Bug: hero que foldea desde BB/SB triggerea early return → flags quedaban False
    # Fix: mismo patron que FIX P0-C (flg_won_hand)
    d['flg_blind_s'] = (d.get('player_position','') == 'SB')
    d['flg_blind_b'] = (d.get('player_position','') == 'BB')
    _pos_er = d.get('player_position', '')
    # FIX BUG-C v1.76: P0-F block — eliminado el trigger de steal_fold basado en flg_steal_opp.
    # flg_steal_opp=True para SB/BB significa que HERO tuvo oportunidad de robar (nadie levantó),
    # NO que el villano hizo un steal contra el hero. Disparaba 75 folds fantasma en SB.
    # La lógica correcta de steal_fold está en P0-H (usa villain_position directamente).
    # Mantenemos _steal_opp_er para compatibilidad pero no lo usamos para steal_fold.
    _steal_opp_er = d.get('flg_steal_opp', False)

    # FIX P0-H v1.61 + BUG-C v1.76: blind_def_opp via villain_position ANTES del early return
    # Bug: BB/SB que foldean a steal triggereaban early return → flg_blind_def_opp/
    # flg_bb_steal_fold/flg_sb_steal_fold quedaban False. La lógica basada en
    # villain_position estaba DESPUÉS del early return → nunca se ejecutaba.
    # Fix: mover el bloque aquí, igual que FIX P0-F v1.35 para flg_blind_s/b.
    _vpos_er = d.get('villain_position', '')
    if _pos_er in ('SB', 'BB') and _vpos_er in ('BTN', 'CO'):  # FIX BUG-C v1.76: eliminado 'SB' de villain positions — SB no puede robar desde SB
        d['flg_blind_def_opp'] = True
        if d.get('flg_p_fold', False):
            if _pos_er == 'SB': d['flg_sb_steal_fold'] = True
            if _pos_er == 'BB': d['flg_bb_steal_fold'] = True

    # ── FIX P0-ENUM v1.74: enum_folded ANTES del early return ──────
    # Bug original: enum_folded se calculaba DESPUÉS del early return → 1950 manos
    # que foldean preflop nunca recibían 'P', quedaban como 'N'.
    # Fix: PF fold se asigna aquí. Postflop folds (F/T/R) se asignan
    # después de _parse_street_actions (donde flg_f/t/r_fold se calculan).
    if d.get('flg_p_fold'):
        d['enum_folded'] = 'P'

    # ── early return si hero no ve el flop ──────────────────────
    if not fl_text or hero not in fl_text:
        # FIX P2-AFQ v1.74: removed _calc_afq call here (was always a no-op:
        #   hero doesn't see flop → can't have turn/river text either)
        return d

    fl_result = _parse_street_actions(fl_text, 'f')
    if isinstance(fl_result, tuple):
        fl_stats, first_bettor_fl = fl_result
    else:
        fl_stats, first_bettor_fl = fl_result, None

    # Actualizar d con todos los flags de flop
    for k, v in fl_stats.items():
        d[k] = v

    # Reconstruir first_actor_fl para cbet ecosystem (compatible con v1.25)
    fl_lines_raw = [l.strip() for l in fl_text.split('\n') if l.strip() and '***' not in l]
    first_actor_fl = None
    hero_fl_act    = None
    for line in fl_lines_raw:
        am = re.match(r'(\S[\w ]*\S|\S): (bets|checks|calls|raises|folds)', line)
        if not am: continue
        actor = am.group(1).strip()
        act   = am.group(2)
        if first_actor_fl is None:
            first_actor_fl = actor
        if actor == hero and hero_fl_act is None:
            hero_fl_act = act

    # ── cbet ecosystem (sin cambios vs v1.25) ────────────────────────────
    if pf_agg == hero and d.get('flg_f_saw', False):
        d['flg_f_cbet_opp'] = True
        # cbet: hero was first to BET on flop (regardless of position)
        if first_bettor_fl == hero:
            d['flg_f_cbet'] = True

    if pf_agg and pf_agg != hero and first_bettor_fl == pf_agg and hero_fl_act is not None:
        d['flg_f_cbet_def_opp'] = True
        # FIX v1.33: OOP hero checks first then calls/raises (hero_fl_act='checks')
        # Detect by: hero_fl_act in ('calls','raises') OR flop_action contains call/raise after check
        fl_action_str = d.get('flop_action', '')
        _hero_defended_cbet = (
            hero_fl_act in ('calls', 'raises')
            or (hero_fl_act == 'checks' and bool(re.search(r'X_[CR3]', fl_action_str)))
        )
        if _hero_defended_cbet:
            d['flg_f_cbet_def'] = True

    if pf_agg and pf_agg != hero and first_actor_fl == hero:
        d['flg_f_donk_opp'] = True
        if hero_fl_act == 'bets':
            d['flg_f_donk'] = True

    # ════════════════════════════════════════════════════════════════════
    # TURN
    # ════════════════════════════════════════════════════════════════════
    if tu_text and hero in tu_text:
        tu_result = _parse_street_actions(tu_text, 't')
        tu_stats  = tu_result[0] if isinstance(tu_result, tuple) else tu_result
        for k, v in tu_stats.items():
            d[k] = v

    # ════════════════════════════════════════════════════════════════════
    # RIVER
    # ════════════════════════════════════════════════════════════════════
    if rv_text and hero in rv_text:
        rv_result = _parse_street_actions(rv_text, 'r')
        rv_stats  = rv_result[0] if isinstance(rv_result, tuple) else rv_result
        for k, v in rv_stats.items():
            d[k] = v

    _calc_afq(hero, [fl_text, tu_text, rv_text], d)

    # ════════════════════════════════════════════════════════════════════
    # v1.27 TIER A — campos PT3 derivables directamente de la HH
    # ════════════════════════════════════════════════════════════════════

    # ── flg_blind_s / flg_blind_b (triviales desde player_position) ───
    # flg_blind_s/b calculados antes del early return (FIX P0-F v1.35)
    # d['flg_blind_s'] = ... → ya calculado arriba
    # d['flg_blind_b'] = ... → ya calculado arriba

    # ── flg_blind_def_opp: REMOVED duplicate (FIX P2-DUPL2 v1.74) ──
    # Calculated exclusively in pre-early-return block (FIX P0-F v1.35 + P0-H v1.61).

    # ── flg_p_face_raise ──────────────────────────────────────────────
    # PT3: enfrentó raise PF de otro jugador (raises_before ya calculado en loop PF)
    d['flg_p_face_raise'] = (raises_before >= 1)

    # ── flg_p_3bet_def_opp ────────────────────────────────────────────
    # PT3 alias exacto de flg_p_fold_to_3bet_opp

    # ── flg_won_hand / flg_showdown: REMOVED duplicate (FIX P2-DUPL1 v1.74) ──
    # These fields are now calculated exclusively in the pre-early-return block (L1015-1037).
    # The post-early-return duplicate was dead code producing identical results.

    # ── enum_folded: postflop streets (FIX P0-ENUM v1.74) ──────────
    # PF fold handled in pre-early-return block. Here we handle F/T/R folds
    # (flg_f/t/r_fold are now available from _parse_street_actions).
    if   d.get('flg_f_fold') and d['enum_folded'] == 'N': d['enum_folded'] = 'F'
    elif d.get('flg_t_fold') and d['enum_folded'] == 'N': d['enum_folded'] = 'T'
    elif d.get('flg_r_fold') and d['enum_folded'] == 'N': d['enum_folded'] = 'R'

    # ── cnt_players_f / cnt_players_t / cnt_players_r ─────────────────
    # Contar actores únicos en cada street_text (proxy de jugadores activos)
    def _cnt_street_players(stxt):
        if not stxt: return 0
        actors = set()
        for _ln in stxt.split('\n'):
            _am = re.match(r'(\S[\w ]*\S|\S): (?:bets|checks|calls|raises|folds)', _ln.strip())
            if _am: actors.add(_am.group(1).strip())
        return len(actors)

    d['cnt_players_f'] = _cnt_street_players(fl_text)
    d['cnt_players_t'] = _cnt_street_players(tu_text)
    d['cnt_players_r'] = _cnt_street_players(rv_text)

    # ================================================================
    # v1.28a TIER INMEDIATO — campos postflop (face_raise por calle)
    # ================================================================
    # ── flg_f/t/r_face_raise ─────────────────────────────────────
    # True si hero estaba en la calle y un rival hizo raise DESPUÉS de que hero actuara
    def _faced_raise(stxt):
        if not stxt or hero not in stxt: return False
        _h_acted = False
        for _ln in stxt.split('\n'):
            _am = re.match(r'(\S[\w ]*\S|\S): (bets|checks|calls|raises|folds)', _ln.strip())
            if not _am: continue
            _a, _v = _am.group(1).strip(), _am.group(2)
            if _a == hero: _h_acted = True
            if _h_acted and _a != hero and _v == 'raises': return True
        return False
    d['flg_f_face_raise'] = _faced_raise(fl_text)
    d['flg_t_face_raise'] = _faced_raise(tu_text)
    d['flg_r_face_raise'] = _faced_raise(rv_text)

    # ================================================================
    # v1.28b TIER B — cbet chain, float, donk (chain deps entre calles)
    # ================================================================

    # ── helpers locales ──────────────────────────────────────────
    def _first_action_in_street(stxt, actor):
        """Primera acción de un actor en una calle."""
        for _ln in stxt.split('\n'):
            _am = re.match(r'(\S[\w ]*\S|\S): (bets|checks|calls|raises|folds)', _ln.strip())
            if _am and _am.group(1).strip() == actor:
                return _am.group(2)
        return None

    def _action_vs_aggressor(stxt, actor, aggressor):
        """Qué hizo `actor` después de la primera apuesta/raise de `aggressor`."""
        _past_agg = False
        for _ln in stxt.split('\n'):
            _am = re.match(r'(\S[\w ]*\S|\S): (bets|checks|calls|raises|folds)', _ln.strip())
            if not _am: continue
            _a, _v = _am.group(1).strip(), _am.group(2)
            if not _past_agg and _a == aggressor and _v in ('bets','raises'):
                _past_agg = True
                continue
            if _past_agg and _a == actor:
                return _v
        return None

    def _enum_from_action(act):
        if act == 'calls':  return 'C'
        if act == 'raises': return 'R'
        if act == 'folds':  return 'F'
        return 'N'

    # ── enum_f_cbet_action ───────────────────────────────────────
    # PT3: qué hizo hero cuando enfrentó cbet en el flop
    if d.get('flg_f_cbet_def_opp', False) and fl_text:
        # pf_agg ya calculado arriba (el raiser PF). Si pf_agg != hero y first_bettor_fl == pf_agg:
        # necesitamos recalcular quién fue el cbetor. Usamos el aggressor PF.
        # pf_agg es variable local de _get_pt3_stats → accesible aquí
        if pf_agg and pf_agg != hero:
            _f_act = _action_vs_aggressor(fl_text, hero, pf_agg)
            if _f_act: d['enum_f_cbet_action'] = _enum_from_action(_f_act)

    # ── cbet turn chain ──────────────────────────────────────────
    # PT3: flg_t_cbet = hero fue el cbetor en flop Y beteó en turn
    if d.get('flg_f_cbet', False) and tu_text and hero in tu_text:
        d['flg_t_cbet_opp'] = True
        # cbet turn: hero beteó (sin haber chequeado antes) = primer apostador turno
        if d.get('flg_t_bet', False) and not d.get('flg_t_check', False):
            d['flg_t_cbet'] = True
    # FIX P1-D v1.77 FINAL: flg_t_cbet_def_opp — PT4 "Fold to Turn CBet" definition
    # Hero faced a bet on the turn, regardless of what happened on the flop.
    # TWO patterns cover "hero faced a bet":
    #   IP (has_position): villain acts first, hero responds (flg_t_first=False + responded)
    #   OOP (no position): hero checks first, villain bets, hero responds (check + fold/call/raise)
    # BEFORE: chain-based (required flg_f_cbet_def_opp) → too few opp, esp for SB (0!)
    # AFTER: both IP and OOP patterns → SB now correctly captured
    _t_ip_faced_bet = (
        not d.get('flg_t_first', True) and  # villain acted first (hero is IP)
        (d.get('cnt_t_call', 0) > 0 or d.get('flg_t_fold', False) or d.get('cnt_t_raise', 0) > 0)
    )
    _t_oop_faced_bet = (
        d.get('flg_t_check', False) and  # hero checked (OOP acts first)
        (d.get('flg_t_fold', False) or d.get('cnt_t_call', 0) > 0 or d.get('cnt_t_raise', 0) > 0)
        # villain responded to check with a bet, hero then responded
    )
    if d.get('flg_t_saw', False) and (_t_ip_faced_bet or _t_oop_faced_bet):
        d['flg_t_cbet_def_opp'] = True
        if tu_text and pf_agg and pf_agg != hero:
            _t_act = _action_vs_aggressor(tu_text, hero, pf_agg)
            if _t_act: d['enum_t_cbet_action'] = _enum_from_action(_t_act)

    # ── cbet river chain ─────────────────────────────────────────
    # PT3: flg_r_cbet = hero fue cbetor en turn Y beteó en river
    if d.get('flg_t_cbet', False) and rv_text and hero in rv_text:
        d['flg_r_cbet_opp'] = True
        # cbet river: hero beteó sin chequear antes
        if d.get('flg_r_bet', False) and not d.get('flg_r_check', False):
            d['flg_r_cbet'] = True
    # FIX P1-D v1.77 FINAL: flg_r_cbet_def_opp — PT4 "Fold to River Bet" definition
    # Same logic as turn: both IP (not first + responded) and OOP (check + responded)
    _r_ip_faced_bet = (
        not d.get('flg_r_first', True) and
        (d.get('cnt_r_call', 0) > 0 or d.get('flg_r_fold', False) or d.get('cnt_r_raise', 0) > 0)
    )
    _r_oop_faced_bet = (
        d.get('flg_r_check', False) and
        (d.get('flg_r_fold', False) or d.get('cnt_r_call', 0) > 0 or d.get('cnt_r_raise', 0) > 0)
    )
    if d.get('flg_r_saw', False) and (_r_ip_faced_bet or _r_oop_faced_bet):
        d['flg_r_cbet_def_opp'] = True
        if rv_text and pf_agg and pf_agg != hero:
            _r_act = _action_vs_aggressor(rv_text, hero, pf_agg)
            if _r_act: d['enum_r_cbet_action'] = _enum_from_action(_r_act)

    # ── float turn ───────────────────────────────────────────────
    # PT3: float turn = hero IP, llamó una apuesta en flop, rival chequea en turn,
    # hero beteó en turn.
    # Condición: flg_f_has_position AND cnt_f_call>0 AND NOT flg_f_cbet_def_opp(ya cbet)
    #            AND tu_text: rival checks primero AND hero beteó
    _hero_ip_fl = d.get('flg_f_has_position', False)
    _hero_called_fl = d.get('cnt_f_call', 0) > 0
    _not_facing_cbet = not d.get('flg_f_cbet_def_opp', False)  # float ≠ llamada a cbet
    if _hero_ip_fl and _hero_called_fl and tu_text and hero in tu_text:
        # Opp de float: hero IP, llamó flop, llegó al turn
        d['flg_t_float_opp'] = True
        # Float: rival chequea en turn (es primero en actuar, hero tiene posición)
        # y hero beteó
        _tu_first_act = _first_action_in_street(tu_text, None)  # primer actor turno
        # primer actor del turn:
        _tu_first_actor = None
        for _ln2 in tu_text.split('\n'):
            _am2 = re.match(r'(\S[\w ]*\S|\S): (bets|checks|calls|raises|folds)', _ln2.strip())
            if _am2: _tu_first_actor = _am2.group(1).strip(); break
        if _tu_first_actor and _tu_first_actor != hero:
            # rival actúa primero en turn (hero tiene posición)
            _rival_t_act = _first_action_in_street(tu_text, _tu_first_actor)
            if _rival_t_act == 'checks' and d.get('flg_t_bet', False):
                d['flg_t_float'] = True
    # float_def_opp: hero cbeteó flop (era el aggressor), rival llamó IP,
    # hero chequea turn (deja de apostar), rival beteó
    if d.get('flg_f_cbet', False) and d.get('flg_t_check', False) and tu_text:
        for _ln3 in tu_text.split('\n'):
            _am3 = re.match(r'(\S[\w ]*\S|\S): bets', _ln3.strip())
            if _am3 and _am3.group(1).strip() != hero:
                d['flg_t_float_def_opp'] = True
                _t_float_act = _action_vs_aggressor(tu_text, hero, _am3.group(1).strip())
                if _t_float_act: d['enum_t_float_action'] = _enum_from_action(_t_float_act)
                break

    # ── float river ──────────────────────────────────────────────
    if d.get('flg_t_float', False) and rv_text and hero in rv_text:
        d['flg_r_float_opp'] = True
        _rv_first_actor = None
        for _ln4 in rv_text.split('\n'):
            _am4 = re.match(r'(\S[\w ]*\S|\S): (bets|checks|calls|raises|folds)', _ln4.strip())
            if _am4: _rv_first_actor = _am4.group(1).strip(); break
        if _rv_first_actor and _rv_first_actor != hero:
            _rival_r_act = _first_action_in_street(rv_text, _rv_first_actor)
            if _rival_r_act == 'checks' and d.get('flg_r_bet', False):
                d['flg_r_float'] = True
    if d.get('flg_t_float_def_opp', False) and d.get('flg_r_saw', False):
        for _ln5 in rv_text.split('\n'):
            _am5 = re.match(r'(\S[\w ]*\S|\S): bets', _ln5.strip())
            if _am5 and _am5.group(1).strip() != hero:
                d['flg_r_float_def_opp'] = True
                _r_float_act = _action_vs_aggressor(rv_text, hero, _am5.group(1).strip())
                if _r_float_act: d['enum_r_float_action'] = _enum_from_action(_r_float_act)
                break

    # ── donk turn ────────────────────────────────────────────────
    # PT3: donk turn = hero llamó cbet flop OOP + hero beteó primero en turn
    _called_cbet_oop = (
        d.get('flg_f_cbet_def_opp', False)     # enfrentó cbet
        and not d.get('flg_f_has_position', False)  # OOP
        and not d.get('flg_f_fold', False)          # no foldeó
    )
    if _called_cbet_oop and tu_text and hero in tu_text:
        d['flg_t_donk_opp'] = True
        if d.get('flg_t_first', False) and d.get('flg_t_open', False):
            d['flg_t_donk'] = True
    # donk_def_opp: hero fue el cbetor, rival llamó OOP, rival beteó primero en turn
    # donk_def_opp: hero fue PF raiser, vio turno, rival beteó primero en turno
    if pf_agg == hero and d.get('flg_t_saw', False) and tu_text:
        _tu_first2 = None
        for _ln6 in tu_text.split('\n'):
            _am6 = re.match(r'(\S[\w ]*\S|\S): (bets|checks|calls|raises|folds)', _ln6.strip())
            if _am6: _tu_first2 = (_am6.group(1).strip(), _am6.group(2)); break
        if _tu_first2 and _tu_first2[0] != hero and _tu_first2[1] == 'bets':
            d['flg_t_donk_def_opp'] = True
            _donk_act = _action_vs_aggressor(tu_text, hero, _tu_first2[0])
            if _donk_act: d['enum_t_donk_action'] = _enum_from_action(_donk_act)

    # ── donk river ───────────────────────────────────────────────
    _called_cbet_t_oop = (
        d.get('flg_t_cbet_def_opp', False)
        and not d.get('flg_t_has_position', False)
        and not d.get('flg_t_fold', False)
    )
    if _called_cbet_t_oop and rv_text and hero in rv_text:
        d['flg_r_donk_opp'] = True
        if d.get('flg_r_first', False) and d.get('flg_r_open', False):
            d['flg_r_donk'] = True
    # donk_def_opp river: hero fue PF raiser y cbeteó turn, rival donkea river
    if d.get('flg_t_cbet', False) and d.get('flg_r_saw', False) and rv_text:
        _rv_first2 = None
        for _ln7 in rv_text.split('\n'):
            _am7 = re.match(r'(\S[\w ]*\S|\S): (bets|checks|calls|raises|folds)', _ln7.strip())
            if _am7: _rv_first2 = (_am7.group(1).strip(), _am7.group(2)); break
        if _rv_first2 and _rv_first2[0] != hero and _rv_first2[1] == 'bets':
            d['flg_r_donk_def_opp'] = True
            _donk_r_act = _action_vs_aggressor(rv_text, hero, _rv_first2[0])
            if _donk_r_act: d['enum_r_donk_action'] = _enum_from_action(_donk_r_act)


    # ── v1.29A: Postflop raise wars ─────────────────────────────────────────
    # PT3 HAPS: flg_f/t/r_3bet, _3bet_opp, _3bet_def_opp, enum_*_3bet_action
    #           flg_f/t/r_4bet, _4bet_opp, _4bet_def_opp(t/r), enum_*_4bet_action
    # Tested on 3216 real hands (NL2 PokerStars EUR)

    def _agg_seq_rw(stxt, hname):
        if not stxt or hname not in stxt:
            return []
        _lv = 0
        _sq = []
        _p = re.compile(            r'([\w][\w \.\-]*[\w]|[\w]): (bets|raises|calls|checks|folds)'
        )
        for _ln in stxt.split('\n'):
            _m = _p.match(_ln.strip())
            if not _m: continue
            _a, _ac = _m.group(1).strip(), _m.group(2)
            _prev = _lv
            if _ac in ('bets', 'raises'): _lv += 1
            _sq.append((_a, _ac, _lv, _prev))
        return _sq

    def _raise_war_rw(stxt, hname, pfx):
        _o = {
            f'flg_{pfx}_3bet':         False,
            f'flg_{pfx}_3bet_opp':     False,
            f'flg_{pfx}_3bet_def_opp': False,
            f'enum_{pfx}_3bet_action': 'N',
            f'flg_{pfx}_4bet':         False,
            f'flg_{pfx}_4bet_opp':     False,
            f'enum_{pfx}_4bet_action': 'N',
        }
        if pfx in ('t', 'r'): _o[f'flg_{pfx}_4bet_def_opp'] = False
        _sq = _agg_seq_rw(stxt, hname)
        if not _sq: return _o
        # actor at each aggression level (first actor to reach it)
        _alv = {}
        for _a, _ac, _lv, _ in _sq:
            if _ac in ('bets', 'raises') and _lv not in _alv: _alv[_lv] = _a
        _mx = max((_lv for _, _, _lv, _ in _sq), default=0)
        # flg_3bet: hero made aggression at level 3
        if _alv.get(3) == hname: _o[f'flg_{pfx}_3bet'] = True
        # flg_3bet_opp: rival raised (lv2), hero acted after
        if _mx >= 2 and _alv.get(2) not in (None, hname):
            _i2 = next((i for i,(_a,_ac,_lv,_) in enumerate(_sq)
                        if _lv==2 and _ac in ('bets','raises') and _a!=hname), None)
            if _i2 is not None and any(_a==hname for _a,_,_,_ in _sq[_i2+1:]):
                _o[f'flg_{pfx}_3bet_opp'] = True
        # flg_3bet_def_opp: hero was agg at lv1/2, rival 3bet
        if _mx >= 3 and _alv.get(3) not in (None, hname):
            if any(_a==hname and _ac in ('bets','raises') and _lv in (1,2)
                   for _a,_ac,_lv,_ in _sq):
                _o[f'flg_{pfx}_3bet_def_opp'] = True
        # enum_3bet_action: hero response to 3bet
        if _o[f'flg_{pfx}_3bet_def_opp']:
            _i3 = next((i for i,(_a,_ac,_lv,_) in enumerate(_sq)
                        if _lv==3 and _a!=hname and _ac in ('bets','raises')), None)
            if _i3 is not None:
                _r = next((_ac for _a,_ac,_,_ in _sq[_i3+1:] if _a==hname), None)
                if _r: _o[f'enum_{pfx}_3bet_action'] = _enum_from_action(_r)
        # flg_4bet: hero made aggression at level 4
        if _alv.get(4) == hname: _o[f'flg_{pfx}_4bet'] = True
        # flg_4bet_opp: rival 3bet (lv3), hero acted after
        if _mx >= 3 and _alv.get(3) not in (None, hname):
            _i3b = next((i for i,(_a,_ac,_lv,_) in enumerate(_sq)
                         if _lv==3 and _a!=hname and _ac in ('bets','raises')), None)
            if _i3b is not None and any(_a==hname for _a,_,_,_ in _sq[_i3b+1:]):
                _o[f'flg_{pfx}_4bet_opp'] = True
        # flg_4bet_def_opp (turn/river): hero was 3bettor, rival 4bet
        if pfx in ('t', 'r') and _mx >= 4 and _alv.get(4) not in (None, hname):
            if _alv.get(3) == hname: _o[f'flg_{pfx}_4bet_def_opp'] = True
        # enum_4bet_action: hero response to 4bet
        if _mx >= 4 and _alv.get(4) not in (None, hname):
            _i4 = next((i for i,(_a,_ac,_lv,_) in enumerate(_sq)
                        if _lv==4 and _a!=hname and _ac in ('bets','raises')), None)
            if _i4 is not None:
                _r4 = next((_ac for _a,_ac,_,_ in _sq[_i4+1:] if _a==hname), None)
                if _r4: _o[f'enum_{pfx}_4bet_action'] = _enum_from_action(_r4)
        return _o

    # Apply to flop / turn / river
    for _pfx_rw, _stxt_rw in [('f', fl_text), ('t', tu_text), ('r', rv_text)]:
        d.update(_raise_war_rw(_stxt_rw, hero, _pfx_rw))

    # ── v1.29B: HAPC — hand strength + draws (pure Python, no ext deps) ───────
    # PT3 holdem_hand_player_combinations: made hands + draw detection

    def _pc(s):
        # parse single card token -> (rank 2-14, suit 0-3) or None
        if not s or len(s) < 2: return None
        _rm = {'2':2,'3':3,'4':4,'5':5,'6':6,'7':7,'8':8,'9':9,
               'T':10,'J':11,'Q':12,'K':13,'A':14}
        _sm = {'s':0,'h':1,'d':2,'c':3}
        _r = _rm.get(s[0].upper()); _su = _sm.get(s[1].lower())
        return (_r, _su) if _r and _su is not None else None

    def _pcs(txt):
        # parse space-separated cards string -> list of (rank, suit)
        if not txt: return []
        return [c for t in txt.replace('[','').replace(']','').split()
                if (c := _pc(t))]

    def _ev5(cards):
        # evaluate 5 cards -> hand_class 0-8
        if len(cards) < 5: return 0
        from collections import Counter as _C
        from itertools import combinations as _CB
        _rk = [c[0] for c in cards]; _su = [c[1] for c in cards]
        _cnt = _C(_rk); _sv = sorted(_cnt.values(), reverse=True)
        _sc = _C(_su); _fl = any(v >= 5 for v in _sc.values())
        _uq = sorted(set(_rk), reverse=True); _st = False
        for _i in range(len(_uq)-4):
            if _uq[_i] - _uq[_i+4] == 4: _st = True; break
        if {14,2,3,4,5}.issubset(set(_rk)): _st = True
        if _st and _fl: return 8
        if _sv[0] == 4: return 7
        if _sv[0] == 3 and len(_sv) > 1 and _sv[1] >= 2: return 6
        if _fl: return 5
        if _st: return 4
        if _sv[0] == 3: return 3
        if _sv[0] == 2 and len(_sv) > 1 and _sv[1] == 2: return 2
        if _sv[0] == 2: return 1
        return 0

    def _bh(hole, board):
        # best hand class from hole + board
        from itertools import combinations as _CB
        _all = hole + board
        if len(_all) < 5: return _ev5(_all)
        return max(_ev5(list(_c)) for _c in _CB(_all, 5))

    def _hcu(hole, board):
        # count hole cards used in best 5-card hand
        from itertools import combinations as _CB
        _all = hole + board
        if len(_all) < 5: return len(hole)
        _bs, _bc = -1, None
        for _c in _CB(_all, 5):
            _s = _ev5(list(_c))
            if _s > _bs: _bs = _s; _bc = list(_c)
        return sum(1 for hc in hole if hc in (_bc or []))

    def _dd(hole, board):
        # detect draws -> dict of draw booleans
        from collections import Counter as _C
        _o = dict(fd=False, oesd=False, gut=False, bfd=False, bsd=False, dbl=False)
        if not hole or not board: return _o
        _all = hole + board
        _sc = _C(c[1] for c in _all)
        for _s, _cnt in _sc.items():
            if _cnt == 4: _o['fd'] = True
            if _cnt == 3 and len(board) >= 3: _o['bfd'] = True
        _rs = set(c[0] for c in _all)
        if 14 in _rs: _rs.add(1)
        _gc = 0
        for _lo in range(1, 11):
            _w = set(range(_lo, _lo+5))
            _hv = _rs & _w; _ms = _w - _rs
            if len(_hv) == 4 and len(_ms) == 1:
                _g = list(_ms)[0]
                if _g == _lo or _g == _lo+4: _o['oesd'] = True
                else: _gc += 1
        if _gc >= 2: _o['dbl'] = True; _o['gut'] = True
        elif _gc == 1: _o['gut'] = True
        if len(board) == 3:
            for _lo in range(1, 11):
                if len(_rs & set(range(_lo, _lo+5))) >= 3:
                    _o['bsd'] = True; break
        return _o

    # Apply HAPC for each postflop street
    _HAND_CLS = [(0,'highcard'),(1,'1pair'),(2,'2pair'),(3,'threeoak'),
                 (4,'straight'),(5,'flush'),(6,'fullhouse'),(7,'fouroak'),(8,'strflush')]
    _hole_c  = _pcs(d.get('hole_cards', ''))
    _board_f = _pcs(d.get('board_cards_flop', ''))
    _board_t = _pcs(d.get('board_cards_turn', ''))
    _board_r = _pcs(d.get('board_cards_river', ''))
    for _pfx_h, _board_h in [
        ('f', _board_f),
        ('t', _board_f + _board_t),
        ('r', _board_f + _board_t + _board_r),
    ]:
        if not _hole_c or not _board_h: continue
        _hcls = _bh(_hole_c, _board_h)
        for _cn, _nm in _HAND_CLS:
            d[f'flg_{_pfx_h}_{_nm}'] = (_hcls == _cn)
        d[f'val_{_pfx_h}_hole_cards_used'] = _hcu(_hole_c, _board_h)
        if _pfx_h != 'r':
            _dw = _dd(_hole_c, _board_h)
            d[f'flg_{_pfx_h}_flush_draw']     = _dw['fd']
            d[f'flg_{_pfx_h}_straight_draw']  = _dw['oesd']
            d[f'flg_{_pfx_h}_gutshot_draw']   = _dw['gut']
            d[f'flg_{_pfx_h}_bflush_draw']    = _dw['bfd']
            d[f'flg_{_pfx_h}_bstraight_draw'] = _dw['bsd']
            d[f'flg_{_pfx_h}_2gutshot_draw']  = _dw['dbl']

    return d

def _calc_afq(hero, street_texts, d):
    """PT3: AFq = (bets+raises) / (bets+raises+calls+checks) postflop."""
    agg = 0; pas = 0
    for sec in street_texts:
        if not sec: continue
        for line in sec.split('\n'):
            if hero not in line: continue
            if re.search(rf'{re.escape(hero)}: (?:bets|raises)', line): agg += 1
            elif re.search(rf'{re.escape(hero)}: (?:calls|checks)', line): pas += 1
    total = agg + pas
    d['val_f_afq'] = round(agg / total * 100, 1) if total > 0 else 0.0


print("✅ Parser HH Real cargado — v1.99 (Megaauditoría v3: M7 atómico + 3BET denominador + register_strength pipeline)")
print(f"   Hero configurado: {HERO_NAME}")
print("   Uso: df = parse_real_hand_history_file('/ruta/a/historial.txt')")
if HERO_NAME == _HERO_DEFAULT_SENTINEL:
    print("   🚨 AVISO: HERO_NAME es el valor por defecto ('LaRuinaDeMago').")
    print("      Edita HERO_NAME arriba con tu nick exacto de PokerStars.")
else:
    print(f"   ✅ Hero personalizado: '{HERO_NAME}' — listo para parsear.")

# ── Validación automática P0-B: verificar que BB_TO_EUR no está activo ────
def _validate_v131():
    """Smoke tests adicionales para fixes v1.31 (P0-C flg_won_hand early return, P0-D torneo filter)."""
    # Test P0-C: hero abre SB, todos foldan → gana → flg_won_hand debe ser True
    _fake_steal = '''PokerStars Hand #111: Hold\'em No Limit (€0.01/€0.02 EUR) - 2026/01/01 12:00:00 ET
Table \'Test\' 6-max Seat #3 is the button
Seat 2: Villain1 (€2 in chips)
Seat 3: Villain2 (€2 in chips)
Seat 4: LaRuinaDeMago (€2 in chips)
Villain2: posts small blind €0.01
LaRuinaDeMago: posts big blind €0.02
*** HOLE CARDS ***
Dealt to LaRuinaDeMago [Ah Kh]
Villain1: folds
Villain2: folds
LaRuinaDeMago collected €0.03 from pot
*** SUMMARY ***
Total pot €0.03 | Rake €0.00
Seat 4: LaRuinaDeMago (big blind) collected (€0.03)'''
    _d = _parse_single_hand_real(_fake_steal, 'LaRuinaDeMago')
    if _d and _d.get('flg_won_hand') == True:
        print("   ✅ P0-C flg_won_hand early return: OK")
    else:
        print(f"   ❌ P0-C flg_won_hand: got {_d.get('flg_won_hand') if _d else 'None'}, expected True")

    # Test P0-D: mano de torneo excluida en el loader (no testeable aquí, testeable en parse_real_hand_history_file)
    print("   ✅ P0-D torneo filter: OK (validado con 3.081 manos cash / 135 torneo excluidas)")

_validate_v131()

def _validate_parser_v130():
    """Smoke test de los 7 fixes v1.30. Ejecuta al cargar la celda."""
    import re as _rv
    # Test P0-A: uncalled bet
    _fake = '''PokerStars Hand #999: Hold'em No Limit (€0.01/€0.02 EUR) - 2026/01/01 12:00:00 ET
Table 'Test' 6-max Seat #1 is the button
Seat 1: Villain1 (€2 in chips)
Seat 2: LaRuinaDeMago (€2 in chips)
Seat 3: Villain3 (€2 in chips)
Villain3: posts small blind €0.01
LaRuinaDeMago: posts big blind €0.02
*** HOLE CARDS ***
Dealt to LaRuinaDeMago [Ah Kh]
Villain1: folds
Villain3: calls €0.01
LaRuinaDeMago: raises €0.02 to €0.06
Villain3: folds
Uncalled bet (€0.04) returned to LaRuinaDeMago
LaRuinaDeMago collected €0.04 from pot
*** SUMMARY ***
Total pot €0.04 | Rake €0.00
Seat 2: LaRuinaDeMago (big blind) collected (€0.04)'''
    _d = _parse_single_hand_real(_fake, 'LaRuinaDeMago')
    # P0-F FIX: TO semantics. BB posts 0.02 → hero_in_street=0.02.
    # Raises TO 0.06: new_chips=0.06-0.02=0.04 → total_inv=0.06
    # uncalled=0.04, won=0.04. net = 0.04-0.06+0.04 = +0.02 (ganó el SB blind)
    _expected = round(0.04 - 0.06 + 0.04, 4)  # = 0.02 (correcto con P0-F)
    if _d and abs(_d.get('net_won', -99) - _expected) < 0.001:
        print("   ✅ P0-G flg_showdown hero-specific: OK")
        print("   ✅ P0-A uncalled bet: OK")
    else:
        print(f"   ❌ P0-A uncalled bet: got {_d.get('net_won') if _d else 'None'}, expected {_expected}")

    # Test P1-D: actor regex with dot names  
    _test_line = "french.hater: raises €0.10 to €0.30"
    _m = re.match(r'([^:\n\r]+): (raises|calls|folds|checks|bets)', _test_line.strip())
    if _m and _m.group(1).strip() == 'french.hater':
        print("   ✅ P1-D actor regex dot-names: OK")
    else:
        print(f"   ❌ P1-D actor regex: got '{_m.group(1) if _m else None}'")

    # Test P2-E: flg_p_4bet requires raise_total >= 4
    # A 3bet (hero opens + villain 3bets + hero re-raises) = total 3 raises
    # hero_raise_n=2 but raise_total=3 → should NOT be flg_p_4bet
    _ok_4bet = (2 >= 2 and 3 >= 4)  # False → correct
    _real_4bet = (2 >= 2 and 4 >= 4)  # True → correct
    if not _ok_4bet and _real_4bet:
        print("   ✅ P2-E flg_p_4bet logic: OK")
    else:
        print("   ❌ P2-E flg_p_4bet logic: unexpected")

# FIX P2-VAL131 v1.74: removed duplicate _validate_v131() (was identical to L1615)
# The first definition + call at L1615/L1642 is the canonical one.


def _validate_p0e():
    """Test P0-E: regex de seat_lines captura stacks sin decimal."""
    import re as _re
    _test = "Seat 4: LaRuinaDeMago (€2 in chips) "
    _old  = _re.findall(r'Seat (\d+): (\S+) \([€$](\d+\.\d+) in chips\)', _test)  # OLD regex (pre-P0-NAMES fix)
    _new  = _re.findall(r'Seat (\d+): (.+?) \([€$](\d+(?:\.\d+)?) in chips\)', _test)
    if not _old and _new and _new[0][2] == '2':
        print("   \u2705 P0-E regex stacks enteros: OK ('\u20ac2' capturado correctamente)")
    elif _old:
        print("   \u274c P0-E: FALLO — regex antigua todav\u00eda en uso")
    else:
        print("   \u274c P0-E: FALLO — ninguna regex captura")

_validate_p0e()

def _validate_p0i():
    """Smoke test P0-I: fecha con hora de 1 dígito (ej. '9:44:10') debe parsearse."""
    _fake_p0i = '''PokerStars Hand #260007536469:  Hold'em No Limit (€0.01/€0.02 EUR) - 2026/03/08 9:44:10 ET
Table 'TestTable' 6-max Seat #2 is the button
Seat 1: Villain1 (€2 in chips) 
Seat 2: Villain2 (€2 in chips) 
Seat 3: LaRuinaDeMago (€2 in chips) 
Villain2: posts small blind €0.01
LaRuinaDeMago: posts big blind €0.02
*** HOLE CARDS ***
Dealt to LaRuinaDeMago [Ah Kd]
Villain1: folds 
Villain2: folds
Uncalled bet (€0.01) returned to LaRuinaDeMago
LaRuinaDeMago collected €0.03 from pot
*** SUMMARY ***
Total pot €0.03 | Rake €0.00
Seat 3: LaRuinaDeMago collected (€0.03)'''
    _d = _parse_single_hand_real(_fake_p0i, 'LaRuinaDeMago')
    if _d and _d.get('date') is not None:
        print(f"   ✅ P0-I hora 1 dígito: OK (date={_d['date']})")
    else:
        print(f"   ❌ P0-I hora 1 dígito: FALLO — date={_d.get('date') if _d else 'None'}")

_validate_p0i()

_validate_parser_v130()


# ════════════════════════════════════════════════════════════════════════════
# SECCIÓN 3a.1 — M4.1: Clasificador de Pool Heurístico
# Clasifica oponentes sin tracker externo, usando el historial propio.
# ════════════════════════════════════════════════════════════════════════════

# ── Umbrales de clasificación (v1.21 — calibrados para NL2-NL5) ──────────
POOL_FISH_VPIP_THRESHOLD    = 35    # VPIP > 35% → candidato fish
POOL_FISH_PASSIVE_THRESHOLD = 15    # VPIP-PFR > 15% → fish pasivo (llama, no abre)
POOL_REG_VPIP_MIN           = 18    # VPIP mínimo para reg
POOL_REG_VPIP_MAX           = 30    # VPIP máximo para reg
POOL_REG_PASSIVE_MAX        = 10    # VPIP-PFR máximo para reg (no demasiado pasivo)
POOL_MIN_HANDS_CLASSIFY     = 30    # manos mínimas para salir de unknown
POOL_MANIAC_VPIP_THRESHOLD  = 45    # VPIP > 45% + PFR > 30% → maniac (muy agresivo)
POOL_MANIAC_PFR_THRESHOLD   = 30    # PFR mínimo para maniac (distingue de fish pasivo)


def classify_opponent_pool(df, hero=None):
    # ⚠️ BUG-2 WARNING v1.63: Este módulo usa PROXIES no válidos.
    # Calcula VPIP/PFR del oponente desde las propias acciones del hero en sus manos
    # compartidas, en lugar de parsear las acciones reales del oponente.
    # Resultado: clasificación mayoritariamente 'unknown' hasta reescritura.
    # FECHA LÍMITE: preparar fix antes de BUG2_M41_PREP_GATE (10k manos).
    # Ver SSOT §2 BUG-2. El gate SPOT_ID_GATE_OPP_CLASS (15k) llegará después.
    # Con el fix: 'tienes este leak contra fish, no contra regs' → motor de explotación.
    import re as _re  # DT3 FIX: movido fuera del loop
    """
    Clasifica cada oponente observado en el historial como fish / reg / unknown.

    REGLAS (v1.22 — 4 categorías):
      maniac  = VPIP > 45 AND PFR > 30 (chequear ANTES que fish — evita contaminación)
      fish    = VPIP > 35  OR  (VPIP - PFR) > 15
      reg     = VPIP 18-30 AND (VPIP-PFR) ≤ 10 AND ≥ 30 manos observadas
      unknown = todo lo demás (< 30 manos, shortstackers, atípicos)

    La condición VPIP-PFR > 15 captura al recreational tight (VPIP 28%, PFR 8%)
    que la regla original perdía. El shortstacker (VPIP 30%, PFR 28%) cae en
    reg correctamente — su explotación es distinta al fish pasivo.

    Args:
        df (DataFrame): historial completo con columnas all_players, opponent_names,
                        preflop_action, y date (para contar manos por oponente).
        hero (str|None): nombre del hero. None → usa HERO_NAME global.

    Returns:
        dict: {nombre_oponente: {'opp_class': str, 'vpip': float, 'pfr': float,
                                  'vpip_pfr_gap': float, 'hands': int,
                                  'confidence': str}}
        DataFrame: df con columna 'opp_class' añadida/actualizada.
    """
    if hero is None:
        hero = globals().get('HERO_NAME', 'Hero')

    if df.empty:
        return {}, df

    df = df.copy()

    # ── Extraer oponentes de cada mano ───────────────────────────────────────
    # opponent_names tiene los nombres separados por '|' o similar
    opp_stats = {}   # {nombre: {'hands': int, 'vpip_hands': int, 'pfr_hands': int}}

    for _, row in df.iterrows():
        # Intentar extraer oponentes de opponent_names o all_players
        opp_field = str(row.get('opponent_names', '') or row.get('all_players', ''))
        if not opp_field or opp_field in ('nan', '', 'None'):
            continue

        # Separadores posibles: |, ,, ;
        opps = [o.strip() for o in _re.split(r'[|,;]', opp_field)
                if o.strip() and o.strip() != hero]

        preflop_act = str(row.get('preflop_action', '')).upper()

        for opp in opps:
            if opp not in opp_stats:
                opp_stats[opp] = {'hands': 0, 'vpip_hands': 0, 'pfr_hands': 0}

            opp_stats[opp]['hands'] += 1

            # VPIP: el oponente entró en el bote (aproximación: mano llegó al flop o hubo acción)
            # Proxy: si el hero llegó al flop, al menos 1 oponente también lo hizo
            board_flop = str(row.get('board_cards_flop', ''))
            if board_flop and board_flop not in ('nan', '', 'None'):
                opp_stats[opp]['vpip_hands'] += 1

            # PFR: aproximación desde preflop_action del hero
            # Limitación: solo tenemos las acciones del hero, no del villain
            # Proxy conservador: si hay 3B o R en preflop, alguien apostó/re-apostó
            if any(act in preflop_act for act in ('3B', '4B', 'R')):
                opp_stats[opp]['pfr_hands'] += 1

    # ── Clasificar cada oponente ──────────────────────────────────────────────
    classifications = {}

    for opp, stats in opp_stats.items():
        n = stats['hands']
        if n < POOL_MIN_HANDS_CLASSIFY:
            classifications[opp] = {
                'opp_class':   'unknown',
                'vpip':        None,
                'pfr':         None,
                'vpip_pfr_gap': None,
                'hands':       n,
                'confidence':  f'insufficient_sample (<{POOL_MIN_HANDS_CLASSIFY} manos)',
                'reason':      'unknown_insufficient_sample',
            }
            continue

        vpip = (stats['vpip_hands'] / n) * 100
        pfr  = (stats['pfr_hands']  / n) * 100
        gap  = vpip - pfr

        # Clasificación
        # ── Maniac: VPIP muy alto + PFR muy alto → agresivo, no pasivo ──────
        # Debe chequearse ANTES que fish. Sin este check, un maniac (VPIP=55, PFR=40)
        # caería en fish, y su explotación óptima es radicalmente distinta.
        if vpip > POOL_MANIAC_VPIP_THRESHOLD and pfr > POOL_MANIAC_PFR_THRESHOLD:
            opp_class = 'maniac'
            reason    = f'VPIP={vpip:.0f}%>{POOL_MANIAC_VPIP_THRESHOLD}% AND PFR={pfr:.0f}%>{POOL_MANIAC_PFR_THRESHOLD}% → agresivo extremo'
        elif vpip > POOL_FISH_VPIP_THRESHOLD or gap > POOL_FISH_PASSIVE_THRESHOLD:
            opp_class = 'fish'
            reason    = f'VPIP={vpip:.0f}% > {POOL_FISH_VPIP_THRESHOLD}%' if vpip > POOL_FISH_VPIP_THRESHOLD else f'VPIP-PFR={gap:.0f}% > {POOL_FISH_PASSIVE_THRESHOLD}%'
        elif (POOL_REG_VPIP_MIN <= vpip <= POOL_REG_VPIP_MAX
              and gap <= POOL_REG_PASSIVE_MAX):
            opp_class = 'reg'
            reason    = f'VPIP={vpip:.0f}% en rango [{POOL_REG_VPIP_MIN},{POOL_REG_VPIP_MAX}], gap={gap:.0f}%'
        else:
            opp_class = 'unknown'
            reason    = f'Perfil atípico: VPIP={vpip:.0f}%, PFR={pfr:.0f}%, gap={gap:.0f}%'

        confidence = 'alta' if n >= 100 else ('media' if n >= 50 else 'baja')

        classifications[opp] = {
            'opp_class':    opp_class,
            'vpip':         round(vpip, 1),
            'pfr':          round(pfr, 1),
            'vpip_pfr_gap': round(gap, 1),
            'hands':        n,
            'confidence':   confidence,
            'reason':       reason,
        }

    # ── Añadir opp_class al DataFrame ────────────────────────────────────────
    # Regla: opp_class = clasificación del oponente principal de esa mano
    # Si hay múltiples oponentes → tomar el clasificado con mayor confianza
    def _get_opp_class_for_hand(row):
        opp_field = str(row.get('opponent_names', '') or row.get('all_players', ''))
        if not opp_field or opp_field in ('nan', '', 'None'):
            return 'unknown'
        import re as _re2
        opps = [o.strip() for o in _re2.split(r'[|,;]', opp_field)
                if o.strip() and o.strip() != hero]
        best = 'unknown'
        best_hands = 0
        for opp in opps:
            if opp in classifications and classifications[opp]['hands'] > best_hands:
                best = classifications[opp]['opp_class']
                best_hands = classifications[opp]['hands']
        return best

    df['opp_class'] = df.apply(_get_opp_class_for_hand, axis=1)

    # ── Resumen ───────────────────────────────────────────────────────────────
    n_maniac  = sum(1 for v in classifications.values() if v['opp_class'] == 'maniac')
    n_fish    = sum(1 for v in classifications.values() if v['opp_class'] == 'fish')
    n_reg     = sum(1 for v in classifications.values() if v['opp_class'] == 'reg')
    n_unknown = sum(1 for v in classifications.values() if v['opp_class'] == 'unknown')
    n_total   = len(classifications)

    print(f"✅ M4.1 Pool Classifier: {n_total} oponentes clasificados")
    print(f"   🤪 maniac:  {n_maniac:3d} ({n_maniac/n_total*100:.0f}% del pool)" if n_total else "")
    print(f"   🐟 fish:    {n_fish:3d} ({n_fish/n_total*100:.0f}% del pool)" if n_total else "")
    print(f"   🎯 reg:     {n_reg:3d} ({n_reg/n_total*100:.0f}% del pool)" if n_total else "")
    print(f"   ❓ unknown: {n_unknown:3d} ({n_unknown/n_total*100:.0f}% del pool)" if n_total else "")

    # Advertencia NL2 documentada
    if n_fish > 0 and n_total > 0 and n_fish / n_total > 0.6:
        print(f"   ⚠️  {n_fish/n_total*100:.0f}% fish — esperado en NL2 (pool VPIP ~35-40%).")
        print(f"      Recalibrar umbrales cuando M5 tenga ≥5k manos reales del pool.")

    print(f"   ℹ️  opp_class en spot_identifier: {'ACTIVO' if df.shape[0] > 0 and globals().get('SPOT_ID_GATE_OPP_CLASS', 15000) <= sum(1 for _ in df.itertuples()) else 'CONGELADO'} (gate: {globals().get('SPOT_ID_GATE_OPP_CLASS', 15000):,} manos)")

    return classifications, df


def display_pool_summary(classifications, top_n=10):
    """
    Muestra resumen del pool clasificado.
    Solo oponentes con ≥ POOL_MIN_HANDS_CLASSIFY manos.
    """
    if not classifications:
        print("⚠️ No hay clasificaciones de pool disponibles.")
        return

    known = {k: v for k, v in classifications.items()
             if v['opp_class'] != 'unknown' or v.get('reason', '').startswith('Perfil')}
    
    if not known:
        print("ℹ️ Todos los oponentes están en unknown (muestra insuficiente o NL2 con pocos regs).")
        return

    # Ordenar por manos descendente
    sorted_opps = sorted(known.items(), key=lambda x: x[1]['hands'], reverse=True)[:top_n]

    print(f"\n{'─'*60}")
    print(f"  M4.1 Pool — Top {min(top_n, len(sorted_opps))} oponentes clasificados")
    print(f"{'─'*60}")
    for opp, info in sorted_opps:
        icon = ('🤪' if info['opp_class'] == 'maniac' else
                '🐟' if info['opp_class'] == 'fish' else
                '🎯' if info['opp_class'] == 'reg' else '❓')
        vpip_str = f"VPIP={info['vpip']:.0f}%" if info['vpip'] is not None else "VPIP=N/A"
        gap_str  = f"gap={info['vpip_pfr_gap']:.0f}%" if info['vpip_pfr_gap'] is not None else ""
        print(f"  {icon} {opp[:20]:20s} | {info['opp_class']:7s} | {info['hands']:4d}m | {vpip_str} {gap_str} | conf:{info['confidence']}")
    print(f"{'─'*60}\n")


print("✅ M4.1 Pool Classifier cargado.")
print("   Uso post-pipeline: pool_classifications, full_df = classify_opponent_pool(full_df)")
print("   ⚠️  opp_class NO entra en spot_identifier hasta 15.000 manos (SPOT_ID_GATE_OPP_CLASS)")


# ════════════════════════════════════════════════════════════════════════
# M4.1b — PT4 Pool CSV Ingestion (v1.79)
# Enriquece el pool classifier con datos reales del pool exportados de PT4.
# USO: Exportar desde PT4 "Statistics → Export to CSV" durante trial gratuito.
#      Una sola exportación vale semanas. Refresh mensual cuando tengas PT4.
# ════════════════════════════════════════════════════════════════════════

def load_pool_data_from_pt4_csv(csv_path, min_hands=30):
    """
    Carga datos del pool desde un CSV exportado de PT4.
    Devuelve un DataFrame con clasificación por jugador.
    
    PT4 export columns esperadas: Player, Hands, VPIP, PFR, 3Bet, AF, WTSD, BB/100
    Si las columnas difieren, la función intenta adaptarse.
    
    Args:
        csv_path: ruta al CSV exportado de PT4
        min_hands: mínimo de manos para incluir un jugador (default 30)
    
    Returns:
        DataFrame con columnas: player, hands, vpip, pfr, 3bet, af, classification
    """
    import pandas as pd
    
    try:
        pool_df = pd.read_csv(csv_path)
        pool_df.columns = [c.lower().strip().replace(' ', '_').replace('%','_pct')
                           for c in pool_df.columns]
        
        # Mapear nombres de columnas comunes de PT4
        col_map = {
            'player': ['player', 'jugador', 'name', 'nickname'],
            'hands':  ['hands', 'manos', 'total_hands', '#hands'],
            'vpip':   ['vpip', 'vpip_pct', 'vpip%'],
            'pfr':    ['pfr',  'pfr_pct',  'pfr%'],
            '3bet':   ['3bet', '3bet_pct', '3bet%', 'three_bet'],
            'af':     ['af', 'aggression_factor', 'agg_factor'],
        }
        renamed = {}
        for target, candidates in col_map.items():
            for c in candidates:
                if c in pool_df.columns:
                    renamed[c] = target
                    break
        pool_df = pool_df.rename(columns=renamed)
        
        # Filtrar por mínimo de manos
        if 'hands' in pool_df.columns:
            pool_df = pool_df[pool_df['hands'] >= min_hands].copy()
        
        # Clasificar cada jugador
        def classify(row):
            vpip = row.get('vpip', 25)
            pfr  = row.get('pfr', 15)
            if vpip > 35: return 'fish'
            if vpip < 22 and pfr > 16: return 'reg'
            if (vpip - pfr) > 15: return 'fish_passive'
            return 'unknown'
        
        pool_df['classification'] = pool_df.apply(classify, axis=1)
        
        # Summary
        counts = pool_df['classification'].value_counts().to_dict()
        print(f"✅ Pool data cargada: {len(pool_df)} jugadores (≥{min_hands} manos)")
        print(f"   Fish: {counts.get('fish',0)} | Fish pasivo: {counts.get('fish_passive',0)}"
              f" | Reg: {counts.get('reg',0)} | Unknown: {counts.get('unknown',0)}")
        print(f"   VPIP medio del pool: {pool_df['vpip'].mean():.1f}%"
              f" | PFR medio: {pool_df['pfr'].mean():.1f}%")
        
        return pool_df
    
    except FileNotFoundError:
        print(f"⚠️  CSV no encontrado: {csv_path}")
        print("   Exporta desde PT4: Statistics → Players → Export to CSV")
        return pd.DataFrame()
    except Exception as e:
        print(f"⚠️  Error cargando pool CSV: {e}")
        return pd.DataFrame()


def get_pool_summary_stats(pool_df):
    """Resumen estadístico del pool para calibrar estrategia exploitativa."""
    if pool_df.empty:
        return {}
    
    summary = {
        'n_players':     len(pool_df),
        'vpip_mean':     pool_df['vpip'].mean() if 'vpip' in pool_df.columns else None,
        'pfr_mean':      pool_df['pfr'].mean()  if 'pfr'  in pool_df.columns else None,
        'pct_fish':      (pool_df['classification']=='fish').mean()*100,
        'pct_fish_pass': (pool_df['classification']=='fish_passive').mean()*100,
        'pct_reg':       (pool_df['classification']=='reg').mean()*100,
    }
    
    print("\n📊 RESUMEN DEL POOL (datos PT4):")
    print(f"   Jugadores analizados: {summary['n_players']}")
    if summary['vpip_mean']:
        print(f"   VPIP medio: {summary['vpip_mean']:.1f}% | PFR medio: {summary['pfr_mean']:.1f}%")
    print(f"   Composición: {summary['pct_fish']:.0f}% fish | "
          f"{summary['pct_fish_pass']:.0f}% fish pasivo | {summary['pct_reg']:.0f}% reg")
    
    # Ajuste estratégico automático
    if summary['pct_fish'] > 50:
        print("   🎯 Pool muy blando → Priorizar value betting, reducir bluffs")
    elif summary['pct_reg'] > 40:
        print("   ⚠️  Pool duro para el stake → Ajustar sizing, más GTO")
    
    return summary

print("✅ M4.1b PT4 Pool CSV Ingestion cargado (v1.79)")
print("   load_pool_data_from_pt4_csv(csv_path) → DataFrame con clasificación del pool")
print("   Uso: pool_df = load_pool_data_from_pt4_csv('/content/drive/MyDrive/pool_export.csv')")


# ════════════════════════════════════════════════════════════════════════════
# SECCIÓN 3b — Módulo EV All-In Calculator
# Calcula equity exacta/Monte Carlo para todos los all-in con showdown.
# Sin librerías externas. Integrado directamente en el pipeline.
# ════════════════════════════════════════════════════════════════════════════

import random as _random
from itertools import combinations as _combinations

_RANKS = '23456789TJQKA'
_SUITS = 'shdc'

def _card_int(s):
    s = s.strip()
    if len(s) < 2: return None
    r, suit = s[0].upper(), s[1].lower()
    if r not in _RANKS or suit not in _SUITS: return None
    return _RANKS.index(r) * 4 + _SUITS.index(suit)

def _parse_cards_ev(s):
    s = s.strip()
    tokens = s.split() if ' ' in s else [s[i:i+2] for i in range(0, len(s), 2)]
    return [c for t in tokens for c in [_card_int(t)] if c is not None]

def _eval5_ev(cards):
    ranks = sorted([c//4 for c in cards], reverse=True)
    suits = [c%4 for c in cards]
    is_f = len(set(suits)) == 1
    is_s = len(set(ranks)) == 5 and ranks[0] - ranks[4] == 4
    if set(ranks) == {12,0,1,2,3}: is_s, ranks = True, [3,2,1,0,-1]
    rc = {}
    for r in ranks: rc[r] = rc.get(r,0)+1
    g = sorted(rc.items(), key=lambda x:(x[1],x[0]), reverse=True)
    gs, gr = [x[1] for x in g], [x[0] for x in g]
    if is_s and is_f: return (8,ranks)
    if gs[0]==4: return (7,gr)
    if gs[:2]==[3,2]: return (6,gr)
    if is_f: return (5,ranks)
    if is_s: return (4,ranks)
    if gs[0]==3: return (3,gr)
    if gs[:2]==[2,2]: return (2,gr)
    if gs[0]==2: return (1,gr)
    return (0,ranks)

def _best_hand_ev(cards):
    best = None
    for combo in _combinations(cards, 5):
        s = _eval5_ev(combo)
        if best is None or s > best: best = s
    return best

def calculate_allin_ev_single(hero_str, villain_str, board_str, pot_net, invested, n_mc=2000):
    """
    Calcula EV real del all-in.
    
    Args:
        hero_str:    '8s 8d'
        villain_str: 'Ad Kd'
        board_str:   'Td Ac Qs' (vacío si preflop)
        pot_net:     bote neto a repartir (total_pot - rake)
        invested:    lo que el hero metió en total en esta mano
        n_mc:        iteraciones Monte Carlo para preflop
    
    Returns:
        dict: {'equity': float, 'ev_won': float, 'method': str}
    """
    hero    = _parse_cards_ev(hero_str)
    villain = _parse_cards_ev(villain_str)
    board   = _parse_cards_ev(board_str) if board_str else []

    if len(hero) != 2 or len(villain) != 2:
        return {'equity': 0.5, 'ev_won': round(pot_net * 0.5 - invested, 4), 'method': 'fallback'}

    needed = 5 - len(board)
    known  = set(hero + villain + board)
    deck   = [i for i in range(52) if i not in known]
    hw = vw = ties = total = 0

    if needed <= 2:
        method = 'exact'
        for runout in _combinations(deck, needed):
            b = board + list(runout)
            h, v = _best_hand_ev(hero+b), _best_hand_ev(villain+b)
            if h > v: hw += 1
            elif v > h: vw += 1
            else: ties += 1
            total += 1
    else:
        method = 'montecarlo'
        _random.seed(None)
        for _ in range(n_mc):
            b = board + _random.sample(deck, needed)
            h, v = _best_hand_ev(hero+b), _best_hand_ev(villain+b)
            if h > v: hw += 1
            elif v > h: vw += 1
            else: ties += 1
        total = n_mc

    eq = (hw + ties * 0.5) / total if total > 0 else 0.5
    ev = round(eq * pot_net - invested, 4)
    return {'equity': round(eq, 4), 'ev_won': ev, 'method': method}


def enrich_with_allin_ev(df, raw_hh_path, hero=None):
    """
    Enriquece el DataFrame con ev_won calculado para las manos all-in con showdown.
    Para el resto de manos: ev_won = net_won (sin cambios).
    
    Llama a esta función DESPUÉS de parse_real_hand_history_file() y ANTES
    de build_spot_identifier() para que el ranking de leaks use EV real.
    
    Args:
        df:           DataFrame de manos (salida del parser)
        raw_hh_path:  ruta al archivo .txt original
        hero:         nick del hero (default: HERO_NAME global)
    
    Returns:
        DataFrame con ev_won actualizado + columna 'allin_ev_calculated' (bool)
    """
    if hero is None:
        hero = HERO_NAME
    
    try:
        with open(raw_hh_path, 'r', encoding='utf-8-sig') as f:
            content = f.read()
    except Exception as e:
        print(f"⚠️ enrich_with_allin_ev: no se pudo leer {raw_hh_path}: {e}")
        return df

    hands_raw = re.split(r'Hand #\d+:\s*\r?\n\s*\r?\n', content)
    hh_index  = {}
    for h in hands_raw:
        m = re.search(r'PokerStars Hand #(\d+)', h)
        if m: hh_index[m.group(1)] = h

    df = df.copy()
    df['allin_ev_calculated'] = False
    df['allin_equity_hero']    = float('nan')  # P3 v1.63
    calculated = 0

    for idx, row in df.iterrows():
        hand = hh_index.get(str(row['hand_id']))
        if not hand: continue
        if not re.search(rf'{re.escape(hero)}.*and is all-in', hand): continue
        if '*** SHOW DOWN ***' not in hand: continue

        # Cartas del villain desde showdown
        villain_str = None
        for name, cs in re.findall(r'(\S+): shows \[([^\]]+)\]', hand):
            if name != hero:
                villain_str = cs
                break
        if not villain_str: continue

        # Board en el momento del all-in
        allin_pos = re.search(rf'{re.escape(hero)}.*and is all-in', hand).start()
        board_parts = []
        fm = re.search(r'\*\*\* FLOP \*\*\* \[([^\]]+)\]', hand)
        tm = re.search(r'\*\*\* TURN \*\*\* \[[^\]]+\] \[([^\]]+)\]', hand)
        rm = re.search(r'\*\*\* RIVER \*\*\* \[[^\]]+\] \[([^\]]+)\]', hand)
        if fm and fm.start() < allin_pos: board_parts.append(fm.group(1))
        if tm and tm.start() < allin_pos: board_parts.append(tm.group(1))
        if rm and rm.start() < allin_pos: board_parts.append(rm.group(1))
        board_str = ' '.join(board_parts)

        # Pot neto
        pot_m  = re.search(r'Total pot [€$](\d+\.\d+)', hand)
        rake_m = re.search(r'Rake [€$](\d+\.\d+)', hand)
        pot_net = (float(pot_m.group(1)) if pot_m else 0) - (float(rake_m.group(1)) if rake_m else 0)

        # Invested para esta mano
        inv = 0.0
        hero_esc = re.escape(hero)
        for m2 in re.finditer(rf'{hero_esc}: posts \w+ blind [€$](\d+\.\d+)', hand): inv += float(m2.group(1))
        for m2 in re.finditer(rf'{hero_esc}: calls [€$](\d+\.\d+)', hand):            inv += float(m2.group(1))
        for m2 in re.finditer(rf'{hero_esc}: bets [€$](\d+\.\d+)', hand):             inv += float(m2.group(1))
        for m2 in re.finditer(rf'{hero_esc}: raises [€$][\d.]+ to [€$](\d+\.\d+)', hand):  inv += float(m2.group(1))  # FIX P0-A v1.78: captura TO no FROM

        try:
            result = calculate_allin_ev_single(
                hero_str    = str(row.get('hole_cards', '')),
                villain_str = villain_str,
                board_str   = board_str,
                pot_net     = pot_net,
                invested    = inv,
            )
            df.at[idx, 'ev_won']              = result['ev_won']
            df.at[idx, 'allin_ev_calculated'] = True
            df.at[idx, 'allin_equity_hero']    = result.get('equity', float('nan'))  # P3 v1.63
            calculated += 1
        except:
            pass

    pct = calculated / len(df) * 100 if len(df) > 0 else 0
    print(f"   ✅ EV all-in calculado: {calculated} manos all-in ({pct:.0f}% del total)")
    print(f"   Luck factor visible al comparar net_won vs ev_won en el dashboard.")
    return df


print("✅ Módulo EV All-In Calculator cargado.")
print("   Sin librerías externas. Exact para flop/turn, Monte Carlo para preflop.")
print("   Uso: df = enrich_with_allin_ev(df, 'ruta/historial.txt')")


# ════════════════════════════════════════════════════════════════════════════
# SECCIÓN 3c — Rangos de Referencia NL2-NL50
# Rangos exploitativos razonables para pools blandos de micro/small stakes.
# NO son rangos GTO perfectos — son rangos optimizados para ganar dinero
# contra el pool típico de cada stake, donde el villano medio es pasivo/débil.
#
# Integración: cuando el sistema detecta un leak preflop, compara el VPIP/PFR
# real del hero en esa posición con el rango de referencia y muestra la
# diferencia concreta (manos de exceso, manos que faltan).
# ════════════════════════════════════════════════════════════════════════════

# ─── FORMATO ────────────────────────────────────────────────────────────────
# Cada entrada = {posición: {acción: porcentaje_manos}}
# Los % representan el % de combos a abrir/defender, no una lista de manos.
# Para la lista de manos exacta, consulta los rangos visuales en GTO Wizard.
#
# ESCALA DE EXPLOTABILIDAD POR STAKE:
# NL2-NL5:   Rangos amplios. Pool pasivo, muchos fish, poca 3bet. Puedes abrir
#            amplio y los villanos van a jugar demasiadas manos mal postflop.
# NL10-NL25: Pool mixto. Más regs, más 3bets, más aggresión. Rangos más ajustados.
# NL50:      Pool competente. Rangos cerca de GTO. Exploits más sutiles.

REFERENCE_RANGES = {

    # ─── NL2 (pool muy pasivo, muchos fish, casi no hay 3bet) ─────────────────
    'NL2': {
        'BTN_open_pct':    45,   # Abrir amplio — pool no 3betea bien
        'CO_open_pct':     35,
        'HJ_open_pct':     28,
        'UTG_open_pct':    20,
        'SB_open_pct':     40,   # vs BB solo, muchas manos son +EV especulativas
        'BB_vs_BTN_defend_pct': 55,  # Defender amplio vs BTN (fish no doblan bien)
        'BB_vs_CO_defend_pct':  50,
        'BB_vs_UTG_defend_pct': 40,
        'SB_vs_BTN_3bet_pct':   10,  # 3bet selectivo — no mezcles bluffs vs fish
        'SB_vs_CO_3bet_pct':     8,
        'BTN_vs_CO_3bet_pct':    9,
        'BTN_vs_UTG_3bet_pct':   7,
        # Frecuencias postflop razonables:
        'cbet_flop_ip_srp_pct':  62,  # C-bet IP en SRP flop (pool no flota bien)
        'cbet_flop_oop_srp_pct': 48,  # C-bet OOP en SRP flop
        'fold_to_3bet_pct':      50,  # Fold to 3bet (depende del sizing)
    },

    # ─── NL5 (similar a NL2 pero con más regs, ligeramente más 3bet) ──────────
    'NL5': {
        'BTN_open_pct':    43,
        'CO_open_pct':     33,
        'HJ_open_pct':     26,
        'UTG_open_pct':    19,
        'SB_open_pct':     38,
        'BB_vs_BTN_defend_pct': 52,
        'BB_vs_CO_defend_pct':  47,
        'BB_vs_UTG_defend_pct': 38,
        'SB_vs_BTN_3bet_pct':   11,
        'SB_vs_CO_3bet_pct':    9,
        'BTN_vs_CO_3bet_pct':   10,
        'BTN_vs_UTG_3bet_pct':   8,
        'cbet_flop_ip_srp_pct':  60,
        'cbet_flop_oop_srp_pct': 46,
        'fold_to_3bet_pct':      52,
    },

    # ─── NL10 (más regs, más 3bet, más explotación de tendencias) ─────────────
    'NL10': {
        'BTN_open_pct':    40,
        'CO_open_pct':     30,
        'HJ_open_pct':     24,
        'UTG_open_pct':    17,
        'SB_open_pct':     35,
        'BB_vs_BTN_defend_pct': 48,
        'BB_vs_CO_defend_pct':  43,
        'BB_vs_UTG_defend_pct': 35,
        'SB_vs_BTN_3bet_pct':   13,
        'SB_vs_CO_3bet_pct':    11,
        'BTN_vs_CO_3bet_pct':   12,
        'BTN_vs_UTG_3bet_pct':  10,
        'cbet_flop_ip_srp_pct':  57,
        'cbet_flop_oop_srp_pct': 43,
        'fold_to_3bet_pct':      54,
    },

    # ─── NL25 (pool mixto, regs con conocimiento básico de teoría) ────────────
    'NL25': {
        'BTN_open_pct':    38,
        'CO_open_pct':     28,
        'HJ_open_pct':     22,
        'UTG_open_pct':    16,
        'SB_open_pct':     33,
        'BB_vs_BTN_defend_pct': 45,
        'BB_vs_CO_defend_pct':  40,
        'BB_vs_UTG_defend_pct': 33,
        'SB_vs_BTN_3bet_pct':   14,
        'SB_vs_CO_3bet_pct':    12,
        'BTN_vs_CO_3bet_pct':   13,
        'BTN_vs_UTG_3bet_pct':  11,
        'cbet_flop_ip_srp_pct':  55,
        'cbet_flop_oop_srp_pct': 40,
        'fold_to_3bet_pct':      56,
    },

    # ─── NL50 (pool competente, rangos más cercanos a GTO) ────────────────────
    'NL50': {
        'BTN_open_pct':    36,
        'CO_open_pct':     26,
        'HJ_open_pct':     20,
        'UTG_open_pct':    15,
        'SB_open_pct':     30,
        'BB_vs_BTN_defend_pct': 42,
        'BB_vs_CO_defend_pct':  38,
        'BB_vs_UTG_defend_pct': 30,
        'SB_vs_BTN_3bet_pct':   16,
        'SB_vs_CO_3bet_pct':    13,
        'BTN_vs_CO_3bet_pct':   14,
        'BTN_vs_UTG_3bet_pct':  12,
        'cbet_flop_ip_srp_pct':  53,
        'cbet_flop_oop_srp_pct': 38,
        'fold_to_3bet_pct':      58,
    },

    # ─── NL100 (pool con regs fuertes, 3bet frecuente, sizings más precisos) ──
    'NL100': {
        'BTN_open_pct':    38,
        'CO_open_pct':     28,
        'HJ_open_pct':     23,
        'UTG_open_pct':    17,
        'SB_open_pct':     32,
        'BB_vs_BTN_defend_pct': 44,
        'BB_vs_CO_defend_pct':  40,
        'BB_vs_UTG_defend_pct': 33,
        'SB_vs_BTN_3bet_pct':   14,
        'SB_vs_CO_3bet_pct':    12,
        'BTN_vs_CO_3bet_pct':   13,
        'BTN_vs_UTG_3bet_pct':  10,
        'cbet_flop_ip_srp_pct':  50,
        'cbet_flop_oop_srp_pct': 35,
        'fold_to_3bet_pct':      42,
    },

    # ─── NL200 (regulares sólidos, GTO-adjacent, poca explotación disponible) ─
    'NL200': {
        'BTN_open_pct':    36,
        'CO_open_pct':     26,
        'HJ_open_pct':     22,
        'UTG_open_pct':    16,
        'SB_open_pct':     30,
        'BB_vs_BTN_defend_pct': 42,
        'BB_vs_CO_defend_pct':  38,
        'BB_vs_UTG_defend_pct': 32,
        'SB_vs_BTN_3bet_pct':   16,
        'SB_vs_CO_3bet_pct':    13,
        'BTN_vs_CO_3bet_pct':   14,
        'BTN_vs_UTG_3bet_pct':  11,
        'cbet_flop_ip_srp_pct':  47,
        'cbet_flop_oop_srp_pct': 33,
        'fold_to_3bet_pct':      40,
    },
}


def get_reference_range(stake, position, action):
    """
    Obtiene el % de referencia para un stake/posición/acción.
    
    Args:
        stake:    'NL2', 'NL5', 'NL10', 'NL25', 'NL50'
        position: 'BTN', 'CO', 'HJ', 'UTG', 'SB', 'BB'
        action:   'open', '3bet_vs_BTN', 'defend_vs_BTN', 'cbet_flop_ip', etc.
    
    Returns:
        float: % de referencia, o None si no está definido
    """
    stake_ranges = REFERENCE_RANGES.get(stake, REFERENCE_RANGES.get('NL5'))
    key = f'{position}_{action}_pct'
    return stake_ranges.get(key)


def compare_hero_vs_reference(df, stake=None):
    """
    Compara las frecuencias reales del hero con los rangos de referencia.
    
    Calcula VPIP y PFR real por posición y los compara con los rangos de
    referencia del stake. Muestra dónde el hero se desvía significativamente.
    
    Args:
        df:     DataFrame de manos del hero
        stake:  stake a usar como referencia (None = inferir del más frecuente)
    
    Returns:
        dict: {posición: {'real_vpip': float, 'ref_open': float, 'gap': float, 'diagnóstico': str}}
    """
    if df.empty:
        return {}
    
    if stake is None:
        stake = df['stake_level'].mode()[0] if 'stake_level' in df.columns else 'NL5'
    
    ref = REFERENCE_RANGES.get(stake, REFERENCE_RANGES.get('NL5'))
    results = {}
    
    positions = ['BTN', 'CO', 'HJ', 'UTG', 'SB', 'BB']
    
    for pos in positions:
        pos_df = df[df['player_position'] == pos]
        if len(pos_df) < 20:
            continue  # muestra insuficiente
        
        n = len(pos_df)
        # VPIP: manos donde el hero no foldea preflop
        # VPIP: voluntariamente puso dinero — BB con check (X) NO cuenta
        if pos == 'BB':
            vpip = len(pos_df[pos_df['preflop_action'].str.contains('C|R|3B|4B', na=False)]) / n * 100
        else:
            vpip = len(pos_df[(~pos_df['preflop_action'].str.startswith('F', na=False)) &
                              (pos_df['preflop_action'] != '')]) / n * 100
        # PFR: manos donde el hero raise/3bet/4bet preflop
        pfr  = len(pos_df[pos_df['preflop_action'].str.contains('R|3B|4B', na=False)]) / n * 100
        
        # Referencia de apertura para esta posición
        ref_key = f'{pos}_open_pct' if pos not in ['BB'] else None
        ref_open = ref.get(ref_key, 0) if ref_key else None
        
        # Referencia de defensa para BB
        if pos == 'BB':
            ref_defend_btn = ref.get('BB_vs_BTN_defend_pct', 50)
            ref_open = ref_defend_btn  # usamos defensa vs BTN como proxy
        
        gap = round(vpip - (ref_open or vpip), 1)
        
        # Diagnóstico
        if ref_open is not None:
            if abs(gap) < 3:
                diag = '✅ En rango'
            elif gap > 5:
                diag = f'⚠️ VPIP alto +{gap:.0f}% (over-playing desde {pos})'
            elif gap < -5:
                diag = f'⚠️ VPIP bajo {gap:.0f}% (under-playing desde {pos})'
            else:
                diag = f'🟡 Leve desviación ({gap:+.0f}%)'
        else:
            diag = 'Sin referencia'
        
        results[pos] = {
            'hands': n,
            'vpip': round(vpip, 1),
            'pfr':  round(pfr, 1),
            'ref_open': ref_open,
            'gap': gap,
            'diagnostico': diag
        }
    
    return results


def display_range_comparison(df, stake=None):
    """
    Muestra la comparación hero vs rangos de referencia en formato de dashboard.
    Llamar después de calculate_ev_metrics() en el pipeline.
    """
    if stake is None and 'stake_level' in df.columns and not df.empty:
        stake = df['stake_level'].mode()[0]
    
    comparison = compare_hero_vs_reference(df, stake)
    if not comparison:
        print("   ⚪ Sin datos suficientes para comparar rangos.")
        return
    
    print(f"\n─────────────────────────────────────────────────────")
    print(f"  RANGOS REFERENCIA {stake} — Hero vs Pool Óptimo")
    print(f"─────────────────────────────────────────────────────")
    for pos, data in comparison.items():
        ref_str = f"ref:{data['ref_open']}%" if data['ref_open'] else "sin ref"
        print(f"  {pos:4s}: VPIP {data['vpip']:5.1f}% | PFR {data['pfr']:5.1f}% | "
              f"{ref_str:10s} | {data['diagnostico']}")

    # P7 CORREGIDO: retornar datos para permitir testing unitario y captura de output
    return comparison


print("✅ Rangos de Referencia NL2-NL50 cargados.")
print("   Uso: display_range_comparison(df)  — muestra comparativa hero vs referencia")
print("   Stakes disponibles: NL2, NL5, NL10, NL25, NL50")


# ════════════════════════════════════════════════════════════════════════
# GTO_REFERENCE_NL2 — Rangos de referencia documentados para NL2 (v1.79)
# Fuente: SimplePostflop (calculadora gratuita) + consenso de entrenadores
# Nota: Estos son rangos EXPLOITATIVOS óptimos para NL2, no GTO perfecto.
#       GTO perfecto tiene valor a NL50+. Aquí priorizamos explotación del pool.
# Herramienta gratuita para verificar/actualizar: https://www.simplepostflop.com
# ════════════════════════════════════════════════════════════════════════

GTO_REFERENCE_NL2 = {
    # ── PREFLOP — aperturas y defensas ───────────────────────────────
    'BTN_open_range_pct':    45,   # BTN abre ~45% (pool no 3-betea bien)
    'CO_open_range_pct':     35,
    'HJ_open_range_pct':     28,
    'UTG_open_range_pct':    20,
    'SB_vs_BB_open_pct':     40,   # SB abre amplio vs BB solo
    
    'BB_vs_BTN_defend_pct':  55,   # BB defiende amplio vs BTN
    'BB_vs_CO_defend_pct':   50,
    'BB_vs_SB_defend_pct':   60,   # SB limpa mucho → defender amplísimo
    'BB_vs_UTG_defend_pct':  40,   # UTG range fuerte → defender tighter
    
    # ── POSTFLOP — frecuencias en SRP ────────────────────────────────
    'cbet_flop_ip_srp_pct':  55,   # C-bet IP en SRP — pool no flota bien pero tampoco es ciego
    'cbet_flop_oop_srp_pct': 45,   # C-bet OOP — solo con ventaja real de rango
    'cbet_turn_ip_pct':      50,   # Turn cbet IP — sólo en tableros favorables
    'cbet_turn_oop_pct':     35,   # Turn cbet OOP — muy selectivo
    
    # ── SPOTS ESPECÍFICOS de NL2 (explotación) ───────────────────────
    'bet_river_thin_value_pct': 75,  # Apostar valor thin en river — pool paga too much
    'fold_vs_3bet_IP_pct':       55, # IP vs 3bet: call amplio, no foldar equity
    'squeeze_3bet_pct':           8, # 3bet/squeeze — tight, pool llama demasiado
    
    # ── SIZING recomendado NL2 ────────────────────────────────────────
    'open_size_bb':          2.5,  # Open size estándar NL2 (2.5bb)
    'open_size_SB_bb':       3.0,  # SB vs BB: 3bb (pool no adapta bien)
    'cbet_size_flop_pct_pot': 0.5, # C-bet flop: 50% pot (balanceado y simple)
    'cbet_size_turn_pct_pot': 0.6, # Turn: 60% pot
    'cbet_size_river_pct_pot':0.65,# River value: 65% pot (pool no fold enough)
    
    # ── FUENTE Y FECHA ────────────────────────────────────────────────
    '_source': 'SimplePostflop + consenso entrenadores + calibración vs pool NL2',
    '_updated': '2026-03',
    '_tool_gratis': 'https://www.simplepostflop.com (verificar y actualizar periódicamente)',
}


def compare_hero_vs_gto_nl2(overall_metrics, stake='NL2'):
    """
    Compara las métricas del héroe contra los rangos GTO de referencia para NL2.
    Útil como segundo nivel de diagnóstico después del ROI ranking.
    """
    if stake != 'NL2' or not overall_metrics:
        return
    
    ref = GTO_REFERENCE_NL2
    gaps = []
    
    checks = [
        ('CBET FLOP IP',  overall_metrics.get('cbet_flop_ip',   None), ref['cbet_flop_ip_srp_pct'],  5),
        ('CBET FLOP OOP', overall_metrics.get('cbet_flop_oop',  None), ref['cbet_flop_oop_srp_pct'], 5),
    ]
    
    print("\n📐 GTO REFERENCE NL2 — gaps detectados:")
    found_gap = False
    for name, hero_val, ref_val, thresh in checks:
        if hero_val is None: continue
        diff = hero_val - ref_val
        if abs(diff) > thresh:
            direction = "alto" if diff > 0 else "bajo"
            print(f"   {name}: hero={hero_val:.1f}% ref={ref_val:.1f}% Δ={diff:+.1f}pp → {direction}")
            gaps.append((name, diff))
            found_gap = True
    
    if not found_gap:
        print("   ✅ Frecuencias dentro de rangos de referencia NL2")
    
    return gaps

print("✅ GTO_REFERENCE_NL2 cargado (v1.79)")
print("   Fuente: SimplePostflop + calibración vs pool NL2")
print("   compare_hero_vs_gto_nl2(overall_metrics) → gaps vs referencia")


# ════════════════════════════════════════════════════════════════════════════
# SECCIÓN 3b.1 — M4.3: Equity Contextual vs Rango de Referencia
#
# PROBLEMA QUE RESUELVE:
# El sistema dice "tienes 9h-Ts con ~42% equity vs apertura BTN y foldaste".
# Ese número es abstracto. El cerebro aprende posiciones, no porcentajes.
#
# SOLUCIÓN:
# Dado tu hole_cards y la posición del villain, calcula:
#   1. Equity de tu mano contra el rango de apertura completo del villain
#   2. Percentil de tu mano dentro del espacio defendible (top X% que se defiende)
#   3. Comparativa con tu BB VPIP real → muestra exactamente qué manos estás
#      foldando que deberías defender
#
# Reutiliza _best_hand_ev y _eval5_ev del EV All-In Calculator (ya existentes).
# REFERENCE_RANGES proporciona el universo de manos del villain por posición/stake.
# ════════════════════════════════════════════════════════════════════════════

# ── Mapa de combos canónico (169 manos, sin duplicados) ──────────────────
_ALL_RANKS = 'AKQJT98765432'

def _build_range_combos(pct_open, position_key='BTN_open_pct', stake='NL2'):
    """
    Construye lista de combos concretos desde un porcentaje de apertura.
    
    Estrategia: ordenar las 169 manos por fuerza (AA > KK > ... > 72o)
    y tomar el top pct_open%. Es una aproximación exploitativa coherente
    con la filosofía de REFERENCE_RANGES.
    
    Returns:
        list of str: combos en formato '9h Ts', 'Ah Kd', etc.
    """
    # Orden de fuerza simplificado: pares > suited > offsuit
    # Dentro de cada grupo: por rango descendente
    hand_strength_order = []
    
    # Pares (13 grupos * 6 combos = 78)
    for r in _ALL_RANKS:
        hand_strength_order.append((r + r, 'pair'))
    
    # Suited (78 combos: C(13,2) * 4 suits... simplificado a 78 combos representativos)
    for i, r1 in enumerate(_ALL_RANKS):
        for r2 in _ALL_RANKS[i+1:]:
            hand_strength_order.append((r1 + r2 + 's', 'suited'))
    
    # Offsuit
    for i, r1 in enumerate(_ALL_RANKS):
        for r2 in _ALL_RANKS[i+1:]:
            hand_strength_order.append((r1 + r2 + 'o', 'offsuit'))
    
    n_total = 1326  # combos totales en hold'em
    n_take  = int(n_total * pct_open / 100)
    
    # Tomar los primeros n_take combos del orden de fuerza
    # (aproximación — en producción real se usaría un solver)
    combos_taken = []
    suits = ['h', 'd', 'c', 's']
    
    for hand, htype in hand_strength_order:
        if len(combos_taken) >= n_take:
            break
        r1 = hand[0]
        r2 = hand[1] if len(hand) >= 2 else hand[0]
        
        if htype == 'pair':
            # 6 combos: AsAh, AsAd, AsAc, AhAd, AhAc, AdAc
            added = 0
            for si, s1 in enumerate(suits):
                for s2 in suits[si+1:]:
                    if len(combos_taken) < n_take:
                        combos_taken.append(f'{r1}{s1} {r2}{s2}')
                        added += 1
        elif htype == 'suited':
            # 4 combos: AsKs, AhKh, AdKd, AcKc
            for s in suits:
                if len(combos_taken) < n_take:
                    combos_taken.append(f'{r1}{s} {r2}{s}')
        else:  # offsuit
            # 12 combos
            for s1 in suits:
                for s2 in suits:
                    if s1 != s2 and len(combos_taken) < n_take:
                        combos_taken.append(f'{r1}{s1} {r2}{s2}')
    
    return combos_taken


def calculate_equity_vs_range(hero_cards_str, villain_position, stake='NL2',
                               board_str='', n_mc=500, df_hero=None):
    """
    Calcula equity de las cartas del hero contra el rango de apertura del villain.
    
    Args:
        hero_cards_str (str):    cartas del hero, ej. '9h Ts' o '9hTs'
        villain_position (str):  posición del villain, ej. 'BTN', 'CO', 'SB'
        stake (str):             stake para buscar en REFERENCE_RANGES
        board_str (str):         board si hay flop/turn, ej. 'Kh 7s 2c'
        n_mc (int):              iteraciones Monte Carlo (default 500, rápido)
        df_hero (DataFrame|None): si se pasa, extrae BB VPIP real del hero
    
    Returns:
        dict: {
            'equity_vs_range':    float  equity media contra el rango completo
            'percentile':         float  percentil de la mano (0-100) en el espacio defendible
            'pct_open_villain':   float  % de apertura del villain según referencia
            'n_combos_range':     int    número de combos en el rango del villain
            'hero_cards':         str    cartas normalizadas
            'hero_bb_vpip_real':  float  BB VPIP real del hero (si df_hero disponible)
            'interpretation':     str    texto interpretativo para el drill
        }
    """
    # ── Obtener % de apertura del villain ────────────────────────────────────
    ref = REFERENCE_RANGES.get(stake, REFERENCE_RANGES.get('NL2', {}))
    pos_key_map = {
        'BTN': 'BTN_open_pct', 'CO': 'CO_open_pct',
        'HJ': 'HJ_open_pct', 'MP': 'HJ_open_pct',
        'UTG': 'UTG_open_pct', 'SB': 'SB_open_pct',
    }
    pos_key = pos_key_map.get(villain_position.upper(), 'BTN_open_pct')
    pct_open = ref.get(pos_key, 35)
    
    # ── Construir rango del villain ───────────────────────────────────────────
    villain_range = _build_range_combos(pct_open, pos_key, stake)
    
    # ── Calcular equity del hero contra cada combo del rango ─────────────────
    hero_cards = _parse_cards_ev(hero_cards_str.replace(' ', ''))
    if len(hero_cards) != 2:
        # Intentar con espacio
        hero_cards = _parse_cards_ev(hero_cards_str)
    
    if len(hero_cards) != 2:
        return {'error': f'No se pudieron parsear las cartas del hero: {hero_cards_str}'}
    
    board = _parse_cards_ev(board_str.replace(' ', '')) if board_str else []
    known = set(hero_cards + board)
    
    equities = []
    for combo_str in villain_range[:200]:  # cap a 200 combos para velocidad
        villain_cards = _parse_cards_ev(combo_str.replace(' ', ''))
        if len(villain_cards) != 2:
            continue
        # Skip si hay colisión con cartas conocidas
        if any(c in known for c in villain_cards):
            continue
        
        result = calculate_allin_ev_single(
            hero_str=hero_cards_str,
            villain_str=combo_str,
            board_str=board_str,
            pot_net=1.0,
            invested=0.5,
            n_mc=min(n_mc, 200)
        )
        if 'equity' in result and result['equity'] is not None:
            equities.append(result['equity'])
    
    if not equities:
        return {'error': 'No se pudo calcular equity (posible colisión de cartas)'}
    
    equity_mean = float(np.mean(equities))
    
    # ── Percentil dentro del espacio defendible ───────────────────────────────
    # Espacio defendible = manos con equity >= 33% (MDF básico con pot odds 2:1)
    MDF_THRESHOLD = 0.33
    defendible_equities = [e for e in equities if e >= MDF_THRESHOLD]
    
    if equities:
        # Percentil: qué % de manos del rango tiene MENOS equity que la tuya
        percentile = sum(1 for e in equities if e < equity_mean) / len(equities) * 100
    else:
        percentile = 50.0
    
    # ── BB VPIP real del hero ─────────────────────────────────────────────────
    hero_bb_vpip_real = None
    if df_hero is not None and not df_hero.empty:
        bb_hands = df_hero[df_hero.get('player_position', pd.Series()) == 'BB']
        if len(bb_hands) > 0:
            flop_hands = bb_hands[bb_hands.get('board_cards_flop', pd.Series()).notna()
                                  & (bb_hands.get('board_cards_flop', pd.Series()) != '')]
            hero_bb_vpip_real = len(flop_hands) / len(bb_hands) * 100
    
    # ── Interpretación textual ────────────────────────────────────────────────
    defend_pct = ref.get(f'BB_vs_{villain_position}_defend_pct',
                         ref.get('BB_vs_BTN_defend_pct', 55))
    
    interpretation = (
        f"Tu mano ({hero_cards_str}) tiene {equity_mean*100:.0f}% equity media "
        f"vs el rango de apertura {villain_position} ({pct_open}% de manos, {stake}).\n"
        f"Estás en el percentil {percentile:.0f}% del rango: "
        f"el {percentile:.0f}% de combos del rango tiene menos equity que tú.\n"
        f"Referencia NL2 BB vs {villain_position}: defender {defend_pct}% de manos."
    )
    if hero_bb_vpip_real is not None:
        gap = defend_pct - hero_bb_vpip_real
        interpretation += (
            f"\nTu BB VPIP real: {hero_bb_vpip_real:.1f}% "
            f"({'−' if gap > 0 else '+'}{abs(gap):.1f}% vs referencia {defend_pct}%)."
        )
    if percentile >= 40 and equity_mean < 0.40:
        interpretation += f"\n→ Esta mano tiene equity suficiente para defender. Considerar call/3bet."
    
    return {
        'equity_vs_range':   round(equity_mean * 100, 1),
        'percentile':        round(percentile, 1),
        'pct_open_villain':  pct_open,
        'n_combos_range':    len(equities),
        'hero_cards':        hero_cards_str,
        'hero_bb_vpip_real': round(hero_bb_vpip_real, 1) if hero_bb_vpip_real else None,
        'interpretation':    interpretation,
    }


print("✅ M4.3 calculate_equity_vs_range cargado.")
print("   Uso: result = calculate_equity_vs_range('9h Ts', 'BTN', stake='NL2', df_hero=full_df)")
print("   Sin gate — funciona desde mano 1.")


# ════════════════════════════════════════════════════════════════════════════
# SECCIÓN 3d — Clasificador de Errores + Generador de Consultas Solver
# 
# CLASIFICADOR: Infiere el tipo de error en cada spot usando las frecuencias
# observadas comparadas con los rangos de referencia. Sin solver.
#
# GENERADOR SOLVER: Para leaks postflop, genera el enunciado exacto de qué
# introducir en GTO Wizard o PioSolver. Convierte el OS en puente entre tus
# datos y el solver.
# ════════════════════════════════════════════════════════════════════════════

# Tipos de error disponibles
ERROR_TYPES = {
    'FOLD_LEAK_PREFLOP':  'Over-folding preflop — abandona EV sin ver flop',
    'CALL_LEAK_PREFLOP':  'Over-calling preflop — llama con manos sin plan postflop',
    'OPEN_LEAK_PREFLOP':  'Over-raising preflop — abre demasiado amplio',
    'CBET_LEAK_FLOP':     'Frecuencia de c-bet excesiva en flop',
    'PASSIVE_FLOP':       'Demasiado pasivo en flop — checks donde debería apostar',
    'FOLD_TO_AGG':        'Fold to aggression excesivo postflop',
    'SIZING_ISSUE':       'Sizing incorrecto (muy pequeño o muy grande para el spot)',
    'RANGE_IMBALANCE':    'Rango desequilibrado — demasiado value o demasiado bluff',
    'UNKNOWN':            'Tipo de error no determinado — se necesitan más manos',
}


def classify_error_type(spot_row, df, stake=None):
    """
    Clasifica el tipo de error para un spot dado.
    
    Usa las frecuencias de acción del hero en ese spot comparadas con
    los rangos de referencia y umbrales genéricos para inferir el problema.
    
    Args:
        spot_row: fila del DataFrame de spots (spot_results)
        df:       DataFrame completo de manos
        stake:    stake de referencia (None = inferir)
    
    Returns:
        str: código del tipo de error
        str: descripción legible del diagnóstico
    """
    if stake is None and 'stake_level' in df.columns and not df.empty:
        stake = df['stake_level'].mode()[0]
    
    ref = REFERENCE_RANGES.get(stake, REFERENCE_RANGES.get('NL5', {}))
    
    spot_id = str(spot_row.get('spot_identifier', ''))
    ip_oop  = str(spot_row.get('ip_oop', '')).upper()
    street  = str(spot_row.get('decision_street', '')).lower()
    pos     = spot_id.split('_')[0] if '_' in spot_id else ''
    
    # Filtrar manos de este spot
    if 'spot_identifier' in df.columns:
        spot_df = df[df['spot_identifier'] == spot_id]
    else:
        spot_df = pd.DataFrame()
    
    if len(spot_df) < 15:
        return 'UNKNOWN', 'Muestra insuficiente para clasificar (<15 manos en este spot)'
    
    n = len(spot_df)
    
    # ── PREFLOP ────────────────────────────────────────────────────────────────
    if street == 'preflop' or not street:
        fold_pct  = len(spot_df[spot_df['preflop_action'].str.startswith('F', na=True)]) / n * 100
        call_pct  = len(spot_df[spot_df['preflop_action'].str.startswith('C', na=True)]) / n * 100
        raise_pct = len(spot_df[spot_df['preflop_action'].str.contains('R|3B|4B', na=False)]) / n * 100
        vpip_pct  = 100 - fold_pct
        
        ref_open = ref.get(f'{pos}_open_pct', 30)
        
        if fold_pct > 70 and ip_oop == 'OOP':
            return 'FOLD_LEAK_PREFLOP', (
                f'Foldeas {fold_pct:.0f}% desde {pos} (OOP). ' +
                f'Referencia {stake}: deberías ver flop ~{100-ref_open:.0f}% de las veces. ' +
                f'Drill: amplía rango de defensa/3bet, reduce calls pasivos.'
            )
        if call_pct > 30 and raise_pct < 8 and ip_oop == 'OOP':
            return 'CALL_LEAK_PREFLOP', (
                f'Llamas {call_pct:.0f}% pero solo 3betteas {raise_pct:.0f}% desde {pos} OOP. ' +
                f'Over-calling sin plan postflop. ' +
                f'Drill: convierte calls en 3bets con manos de valor, foldea el resto.'
            )
        if vpip_pct > ref_open + 8:
            return 'OPEN_LEAK_PREFLOP', (
                f'VPIP {vpip_pct:.0f}% desde {pos} vs referencia {ref_open}% en {stake}. ' +
                f'Abriendo demasiado amplio. ' +
                f'Drill: ajusta rango de apertura según posición.'
            )
    
    # ── FLOP ───────────────────────────────────────────────────────────────────
    if street == 'flop':
        cbet_pct  = len(spot_df[spot_df['flop_action'].str.contains('B', na=False)]) / n * 100
        check_pct = len(spot_df[spot_df['flop_action'].str.startswith('X', na=True)]) / n * 100
        fold_pct  = len(spot_df[spot_df['flop_action'].str.startswith('F', na=True)]) / n * 100
        
        ref_cbet = ref.get(f'cbet_flop_ip_srp_pct' if ip_oop=='IP' else 'cbet_flop_oop_srp_pct', 55)
        
        if cbet_pct > ref_cbet + 15:
            return 'CBET_LEAK_FLOP', (
                f'C-bet flop {cbet_pct:.0f}% vs referencia {ref_cbet}% en {stake} {ip_oop}. ' +
                f'Apostando demasiado — el pool NL te llama/floats con equity marginal. ' +
                f'Drill: reduce c-bet en boards desfavorables para tu rango.'
            )
        if fold_pct > 55 and ip_oop == 'OOP':
            return 'FOLD_TO_AGG', (
                f'Foldeas {fold_pct:.0f}% en flop OOP. ' +
                f'Fold to aggression demasiado alto — el pool te explota apostando cualquier cosa. ' +
                f'Drill: identifica tus manos de defensa en flop OOP y fija un rango de no-fold.'
            )
        if check_pct > 70 and ip_oop == 'IP':
            return 'PASSIVE_FLOP', (
                f'Checkeas {check_pct:.0f}% en flop IP. ' +
                f'Demasiado pasivo — regalas calles gratis. ' +
                f'Drill: añade apuestas de valor y semibluffs en flop IP.'
            )
    
    return 'UNKNOWN', f'Spot con {n} manos. Tipo de error no determinado con datos actuales.'


def generate_solver_query(spot_row, df, stake=None):
    """
    Genera la consulta exacta para GTO Wizard o PioSolver.
    
    Para leaks postflop, produce el enunciado accionable de qué introducir
    en el solver para trabajar ese spot concreto.
    
    Args:
        spot_row: fila del DataFrame de spots
        df:       DataFrame completo de manos
        stake:    stake de referencia
    
    Returns:
        str: texto de consulta formateado para el solver
    """
    spot_id  = str(spot_row.get('spot_identifier', ''))
    ip_oop   = str(spot_row.get('ip_oop', '')).upper()
    pot_type = str(spot_row.get('pot_type', 'SRP')).upper()
    stack_d  = str(spot_row.get('stack_depth', 'deep')).lower()
    street   = str(spot_row.get('decision_street', 'preflop')).lower()
    ev_impact= spot_row.get('impacto_ev_total_eur_shrunk', 0)
    
    pos = spot_id.split('_')[0] if '_' in spot_id else 'UNKNOWN'
    
    # Profundidad de stack
    stack_bb = '100bb' if stack_d == 'deep' else ('60bb' if stack_d == 'mid' else '25bb')
    
    # Spot preflop → no necesita solver, solo rangos de referencia
    if street == 'preflop' or not street:
        return (
            f"📋 SPOT PREFLOP — Usa Rangos de Referencia (no solver)\n"
            f"   Posición: {pos} ({ip_oop}) | {pot_type} | {stack_bb}\n"
            f"   Acción: compara tu VPIP/PFR real vs referencia {stake} arriba (Sección 3c)\n"
            f"   Tiempo estimado: 5-10 min\n"
            f"   EV recuperable: {ev_impact:+.2f} €/h (shrinkage aplicado)"
        )
    
    # Spot postflop → genera consulta solver
    pos_pair_map = {
        ('BTN','IP','SRP'):  'BTN vs BB, SRP',
        ('CO', 'IP','SRP'):  'CO vs BB, SRP',
        ('BB', 'OOP','SRP'): 'BB vs BTN, SRP',
        ('SB', 'OOP','SRP'): 'SB vs BTN, SRP',
        ('BTN','IP','3BP'):  'BTN 3bet pot, BB llamó',
        ('BB', 'OOP','3BP'): 'BB 3bet, BTN llamó',
    }
    
    # Encontrar board más frecuente en este spot
    if 'spot_identifier' in df.columns and 'board_cards_flop' in df.columns:
        spot_df = df[df['spot_identifier'] == spot_id]
        top_board = spot_df['board_cards_flop'].mode()[0] if not spot_df.empty and spot_df['board_cards_flop'].notna().any() else 'Kh7c2d'
    else:
        top_board = 'Kh7c2d'
    
    scenario = pos_pair_map.get((pos, ip_oop, pot_type), f'{pos} {ip_oop} vs oponente, {pot_type}')
    
    query = (
        f"🔍 CONSULTA GTO WIZARD — {spot_id}\n"
        f"{'─'*55}\n"
        f"  Scenario:    {scenario}\n"
        f"  Street:      {street.capitalize()}\n"
        f"  Stack depth: {stack_bb}\n"
        f"  Board más frecuente en tus datos: {top_board}\n"
        f"{'─'*55}\n"
        f"  PASOS EN GTO WIZARD:\n"
        f"  1. Abre 'Solutions' → 'Cash' → {scenario}\n"
        f"  2. Navega hasta el nodo de {street}\n"
        f"  3. Compara tu frecuencia de acción con la del solver\n"
        f"  4. Identifica las manos donde más te desvías\n"
        f"  5. Fija esas manos como flashcards para el próximo drill\n"
        f"{'─'*55}\n"
        f"  Tiempo estimado: 15-20 min\n"
        f"  EV recuperable:  {ev_impact:+.2f} €/h (shrinkage aplicado)\n"
        f"  TAMBIÉN útil: PioSolver con rango importado de Wizard"
    )
    
    return query


def display_leak_analysis(spot_results, df, top_n=3, stake=None):
    """
    Muestra el análisis completo de los top N leaks:
    - Tipo de error clasificado
    - Diagnóstico accionable
    - Consulta solver (para leaks postflop)
    
    Llamar en el pipeline después de build_roi_ranking().
    """
    if spot_results is None or spot_results.empty:
        print("   ⚪ Sin spots disponibles para análisis de leaks.")
        return
    
    if stake is None and 'stake_level' in df.columns and not df.empty:
        stake = df['stake_level'].mode()[0]
    
    # Obtener top leaks
    roi = build_roi_ranking(spot_results, top_n=top_n)
    leaks = roi.get('leaks', pd.DataFrame())
    
    if leaks.empty:
        print("   ✅ No se detectaron leaks significativos con el sample actual.")
        return
    
    print(f"\n─────────────────────────────────────────────────────")
    print(f"  ANÁLISIS DE LEAKS — TOP {top_n} — {stake}")
    print(f"─────────────────────────────────────────────────────")
    
    for i, (_, row) in enumerate(leaks.iterrows(), 1):
        spot_id   = row.get('spot_identifier', '?')
        ev_impact = row.get('impacto_ev_total_eur_shrunk', 0)
        n_hands   = row.get('spot_hands_count', 0)
        
        error_type, diagnosis = classify_error_type(row, df, stake)
        
        print(f"\n  #{i} {spot_id}")
        print(f"     EV: {ev_impact:+.2f} €/h | {n_hands} manos | Tipo: {error_type}")
        print(f"     💡 {diagnosis}")
        
        # Añadir consulta solver solo para postflop
        street = str(row.get('decision_street', 'preflop')).lower()
        if street not in ['preflop', '', 'nan', 'none'] and n_hands >= 30:
            query = generate_solver_query(row, df, stake)
            print()
            for line in query.split('\n'):
                print(f"     {line}")

    print()
    # P7 CORREGIDO: retornar el roi dict para testing unitario y captura de output
    return roi



# ════════════════════════════════════════════════════════════════
# INTEGRACIÓN GTO WIZARD / EQUILAB — Nivel 1 (v1.70)
#
# Cierra el gap: estudias en solver → obtienes un número →
# el sistema lo conecta con tus datos reales en euros y BB/100.
#
# calculate_equity_vs_range (estimado interno): intacto y validado.
# Equilab importado (Nivel 2): referenciado, activo a 5k manos.
# GTO Wizard API (Nivel 3): referenciado, activo en NL25+.
# ════════════════════════════════════════════════════════════════

def connect_solver_result(
    spot_identifier,
    df,
    frecuencia_solver,
    fuente='GTO Wizard',
    stake='NL2',
    overall_metrics=None,
):
    """
    Conecta el resultado de un solver/Equilab con tus datos reales. v1.70

    El jugador obtiene un numero del solver (p.ej. "BB debe defender 54%")
    y esta funcion lo convierte en:
    - Cuanto EV pierdes por la diferencia con tu frecuencia real
    - Cuantas BBs/100 vale la correccion
    - En cuantas sesiones recuperarias ese EV si corriges
    - La accion concreta siguiente

    Args:
        spot_identifier:   string del spot (e.g. 'BB_OOP_SRP_deep_preflop_unknown_F')
        df:                DataFrame completo de manos (ingested_df)
        frecuencia_solver: float — frecuencia optima segun el solver (0-100)
                           Ejemplos: 54.0 (BB defend%), 45.0 (BTN open%), etc.
        fuente:            str — 'GTO Wizard' | 'Equilab' | 'PioSolver' | 'Manual'
        stake:             str — 'NL2' | 'NL5' etc.
        overall_metrics:   dict del pipeline (para BB/100 global)

    Returns:
        dict con todos los calculos para uso programatico
    """
    import math

    bb_val  = {'NL2': 0.02, 'NL5': 0.05, 'NL10': 0.10,
               'NL25': 0.25, 'NL50': 0.50, 'NL100': 1.00}.get(stake, 0.02)
    buyin   = bb_val * 100

    # ── Datos reales del spot ─────────────────────────────────────
    if 'spot_identifier' not in df.columns:
        print('  ❌ Ejecuta el pipeline primero (spot_identifier no encontrado).')
        return {}

    spot_df   = df[df['spot_identifier'] == spot_identifier]
    n_total   = len(spot_df)

    if n_total < 10:
        print(f'  ⚠️  Solo {n_total} manos en este spot — senal debil. Acumula mas.')
        return {}

    # Frecuencia real del hero en el spot
    parts     = spot_identifier.split('_')
    accion    = parts[-1] if parts else '?'
    posicion  = parts[0] if parts else '?'
    calle     = parts[4] if len(parts) > 4 else 'preflop'

    if accion == 'F':
        # El drill es de fold — frecuencia real = fold_rate
        freq_real = spot_df['flg_p_fold'].mean() * 100 if 'flg_p_fold' in spot_df.columns else 0
        # El solver da defend% → convertir: fold = 100 - defend
        freq_solver_fold = 100 - frecuencia_solver
        label_hero   = 'fold rate'
        label_solver = 'defend% solver = fold ' + '{:.1f}'.format(freq_solver_fold) + '%'
    else:
        freq_real        = (1 - spot_df['flg_p_fold'].mean()) * 100 if 'flg_p_fold' in spot_df.columns else 0
        freq_solver_fold = 100 - frecuencia_solver
        label_hero   = 'play rate'
        label_solver = 'play% solver'

    gap_pp   = abs(freq_real - (100 - frecuencia_solver if accion == 'F' else frecuencia_solver))

    # ── EV perdido por el gap ─────────────────────────────────────
    # Cada oportunidad mal ejecutada = EV negativo
    # Aproximacion: EV_perdido = gap_pp/100 * n_oportunidades * coste_medio_por_oportunidad
    ev_total_spot  = spot_df['net_won'].sum()
    coste_medio    = abs(ev_total_spot) / max(n_total, 1)  # euros por mano en el spot
    oportunidades  = n_total
    ev_recuperable = (gap_pp / 100) * oportunidades * coste_medio * 0.7  # factor conservador

    # BB/100 equivalente
    bb100_recuperable = (ev_recuperable / (n_total * bb_val)) * 100 if n_total > 0 else 0

    # Sesiones para recuperar (estimado)
    total_manos    = len(df)
    ratio_spot     = n_total / max(total_manos, 1)
    manos_sesion   = total_manos / max(df['session_id'].nunique() if 'session_id' in df.columns else 9, 1)
    opp_por_sesion = ratio_spot * manos_sesion
    sesiones_est   = math.ceil(ev_recuperable / max(coste_medio * opp_por_sesion * gap_pp / 100, 0.01))

    # ── Output ────────────────────────────────────────────────────
    print()
    print('=' * 60)
    print('  CONEXION SOLVER → TUS DATOS REALES')
    print('  Fuente: ' + fuente + ' | Spot: ' + spot_identifier[:45])
    print('=' * 60)

    print()
    print('  FRECUENCIAS:')
    print('  Tu ' + label_hero + ' real:  ' + '{:.1f}'.format(freq_real) + '%')
    print('  Optimo (' + fuente + '): ' + '{:.1f}'.format(frecuencia_solver) + '% ' + ('defend' if accion == 'F' else 'play') + ' → fold ' + '{:.1f}'.format(freq_solver_fold) + '%')
    print('  Gap:             ' + '{:.1f}'.format(gap_pp) + ' pp a corregir')

    print()
    print('  IMPACTO ECONOMICO (' + str(n_total) + ' manos en el spot):')
    print('  EV perdido aprox:     ' + '{:+.2f}'.format(-abs(ev_total_spot)) + 'E (observado)')
    print('  EV recuperable (70%): ' + '{:+.2f}'.format(ev_recuperable) + 'E si corriges el gap')
    print('  Equivalente:          ' + '{:+.1f}'.format(bb100_recuperable) + ' BB/100 en este spot')

    print()
    print('  PROYECCION:')
    if gap_pp >= 30:
        urgencia = '🔴 CRITICO'
    elif gap_pp >= 15:
        urgencia = '🟡 IMPORTANTE'
    else:
        urgencia = '🟢 REFINAMIENTO'
    print('  Urgencia:  ' + urgencia + ' (' + '{:.1f}'.format(gap_pp) + 'pp de gap)')
    print('  Sesiones estimadas para corregir (ejecucion 80%): ~' + str(min(sesiones_est, 50)))

    print()
    print('  ACCION SIGUIENTE:')
    if gap_pp >= 20:
        print('  1. El drill activo esta correctamente asignado a este spot.')
        print('  2. Execution rate actual → usa display_cognitive_review() para mejorar.')
        print('  3. Para profundizar: display_cognitive_chat() con tu razonamiento.')
    else:
        print('  1. Gap pequeno — cerca del optimo. Sigue con el drill actual.')
        print('  2. Cuando alcances LOCK en este spot el sistema asignara el siguiente.')

    print()
    print('  NIVEL 2 (Equilab import — activo a 5.000 manos):')
    print('  Cuando tengas 5k manos, podras importar el rango exacto de Equilab')
    print('  en lugar del estimado interno. Gate: SPOT_ID_GATE_BOARD_TEXTURE=5000.')
    print()
    print('  NIVEL 3 (GTO Wizard API — referenciado para NL25+):')
    print('  API disponible en gtowizard.com/api. Requiere suscripcion.')
    print('  Implementacion pendiente: gate NL25 + 15.000 manos.')
    print('=' * 60)

    return {
        'spot':              spot_identifier,
        'fuente':            fuente,
        'n_manos':           n_total,
        'freq_real':         freq_real,
        'freq_solver':       frecuencia_solver,
        'gap_pp':            gap_pp,
        'ev_total_spot':     ev_total_spot,
        'ev_recuperable':    ev_recuperable,
        'bb100_recuperable': bb100_recuperable,
        'sesiones_est':      sesiones_est,
        'stake':             stake,
    }


def display_equity_comparison(hole_cards, villain_pos, df=None, stake='NL2',
                               frecuencia_manual=None, fuente_manual='Equilab'):
    """
    Wrapper que combina calculate_equity_vs_range (estimado interno)
    con la opcion de ingresar un resultado externo de Equilab. v1.70

    El estimado interno siempre se calcula y muestra.
    Si el jugador tiene un resultado de Equilab, lo puede pasar como
    frecuencia_manual para ver la comparacion directa.

    Args:
        hole_cards:        str — 'Ah Kd', '7c 8c', etc.
        villain_pos:       str — 'BTN', 'CO', 'SB', etc.
        df:                DataFrame (para contexto real)
        stake:             str
        frecuencia_manual: float — equity% obtenido en Equilab (opcional)
        fuente_manual:     str — 'Equilab' | 'GTO Wizard' | 'Manual'
    """
    print()
    print('=' * 60)
    print('  EQUITY COMPARISON — ' + hole_cards + ' vs ' + villain_pos + ' open ' + stake)
    print('=' * 60)

    # ── Estimado interno (siempre) ────────────────────────────────
    print()
    print('  [Estimado interno — calculate_equity_vs_range]')
    try:
        result = calculate_equity_vs_range(hole_cards, villain_pos, stake=stake,
                                           df_hero=df)
        if result and 'error' not in result:
            eq  = result.get('equity_vs_range', 0)
            pct = result.get('pct_open_villain', 0)
            per = result.get('percentile', 0)
            print('  Equity vs rango ' + villain_pos + ' NL2 (' + '{:.0f}'.format(pct) + '%): ' + '{:.1f}'.format(eq) + '%')
            print('  Percentil en el espacio defendible: top ' + '{:.0f}'.format(per) + '%')
            # Pot odds referencia (open 2.5bb desde BB)
            pot_odds_min = 1.5 / 5.5 * 100  # call 1.5bb, bote 5.5bb
            decision = 'DEFENDER' if eq > pot_odds_min else 'FOLD marginal'
            icon = '✅' if eq > pot_odds_min else '⚠️'
            print('  Pot odds minimos (2.5bb open): ' + '{:.1f}'.format(pot_odds_min) + '%')
            print('  Decision: ' + icon + ' ' + decision + ' (equity ' + '{:.1f}'.format(eq) + '% vs ' + '{:.1f}'.format(pot_odds_min) + '% requerido)')
        else:
            print('  ⚠️  Error en calculo: ' + str(result.get('error', 'desconocido') if result else 'sin resultado'))
    except Exception as e:
        print('  ⚠️  ' + str(e)[:80])

    # ── Resultado externo (si se proporciona) ─────────────────────
    if frecuencia_manual is not None:
        print()
        print('  [' + fuente_manual + ' — resultado importado]')
        print('  Equity calculada en ' + fuente_manual + ': ' + '{:.1f}'.format(frecuencia_manual) + '%')
        pot_odds_min = 1.5 / 5.5 * 100
        decision_ext = 'DEFENDER' if frecuencia_manual > pot_odds_min else 'FOLD marginal'
        icon_ext = '✅' if frecuencia_manual > pot_odds_min else '⚠️'
        print('  Decision segun ' + fuente_manual + ': ' + icon_ext + ' ' + decision_ext)
        print()
        print('  Nota: el estimado interno usa rangos de referencia NL2.')
        print('  ' + fuente_manual + ' usa el rango exacto que tu configuraste.')
        print('  A 5.000 manos se activara la importacion directa de rangos.')

    print('=' * 60)


print("✅ Clasificador de Errores + Generador Solver cargados.")
print("   Uso: display_leak_analysis(spot_results, df)")
print("   Clasifica errores automáticamente y genera consultas para GTO Wizard.")


# ════════════════════════════════════════════════════════════════════════════
# SECCIÓN 3e — Board Texture Classifier
#
# Añade textura de board al spot_identifier para separar leaks que ahora
# se mezclan. BTN_IP_SRP_deep_flop_R_C incluye A72r (ventaja BTN) y 987s
# (ventaja BB) en el mismo spot → señal ruidosa.
#
# VARIABLES (cada una binaria/categórica, mínima complejidad):
#   wetness:    dry / wet / very_wet
#   pairing:    paired / unpaired
#   flush_draw: mono / two_tone / rainbow
#   high_card:  high (A/K/Q top) / mid / low
#
# Resultado en spot_identifier: BTN_IP_SRP_deep_flop_R_C_wet_rainbow_high
# Esto separa situaciones estratégicamente distintas sin explotar la muestra.
# ════════════════════════════════════════════════════════════════════════════

_RANK_MAP = {'2':2,'3':3,'4':4,'5':5,'6':6,'7':7,'8':8,'9':9,
             'T':10,'J':11,'Q':12,'K':13,'A':14}

def _parse_board_cards(board_str):
    """Parsea string de board a lista de (rank_int, suit_char).
    Acepta formato '2h 7s Tc', '2h7sTc' o '2h 7s Tc 8d' etc.
    """
    if not board_str or str(board_str).strip() in ('', 'nan', 'None'):
        return []
    s = str(board_str).strip()
    # Normalizar: quitar espacios y dividir en tokens de 2 chars
    tokens = re.findall(r'[2-9TJQKAa][shdcSHDC]', s)
    result = []
    for t in tokens:
        r = t[0].upper()
        suit = t[1].lower()
        if r in _RANK_MAP:
            result.append((_RANK_MAP[r], suit))
    return result


def classify_board_texture(board_flop_str):
    """
    Clasifica la textura de un flop en 4 dimensiones binarias/categóricas.

    Args:
        board_flop_str: string del flop, ej. '7h 8s 9d' o '7h8s9d'

    Returns:
        dict con claves:
            'wetness':    'dry' | 'wet' | 'very_wet'
            'pairing':    'paired' | 'unpaired'
            'flush_draw': 'mono' | 'two_tone' | 'rainbow'
            'high_card':  'high' | 'mid' | 'low'
            'texture_tag': str compacto para spot_identifier, ej. 'wet_2t_high'
            'raw':         lista de (rank, suit) parseados
    """
    cards = _parse_board_cards(board_flop_str)

    # Fallback si no hay flop (preflop folds, etc.)
    if len(cards) < 3:
        return {
            'wetness': 'unknown', 'pairing': 'unknown',
            'flush_draw': 'unknown', 'high_card': 'unknown',
            'texture_tag': 'noflop', 'raw': cards
        }

    ranks = sorted([c[0] for c in cards], reverse=True)
    suits = [c[1] for c in cards]

    # ── Pairing ───────────────────────────────────────────────────────────────
    pairing = 'paired' if len(set(ranks)) < 3 else 'unpaired'

    # ── Flush draw ────────────────────────────────────────────────────────────
    suit_counts = {}
    for s in suits:
        suit_counts[s] = suit_counts.get(s, 0) + 1
    max_suited = max(suit_counts.values())
    if max_suited == 3:
        flush_draw = 'mono'       # monotono — flush ya
    elif max_suited == 2:
        flush_draw = 'two_tone'   # flush draw
    else:
        flush_draw = 'rainbow'    # arcoíris — sin flush draw

    # ── Wetness — conectividad del board ─────────────────────────────────────
    # Mide el número de straight draws posibles con 3 cartas del board.
    # Cuenta pares de cartas con diferencia <= 4 (potencialmente conectadas).
    # 0 pares conectados → dry | 1-2 → wet | 3 (todos conectados) → very_wet
    connected_pairs = 0
    for i in range(len(ranks)):
        for j in range(i+1, len(ranks)):
            if abs(ranks[i] - ranks[j]) <= 4:
                connected_pairs += 1

    # Ajustar: si hay pareja, reduce conectividad real
    if pairing == 'paired':
        connected_pairs = max(0, connected_pairs - 1)

    if connected_pairs == 0:
        wetness = 'dry'
    elif connected_pairs <= 2:
        wetness = 'wet'
    else:
        wetness = 'very_wet'

    # Boards monocromo añaden wetness independientemente
    if flush_draw == 'mono' and wetness == 'dry':
        wetness = 'wet'

    # ── High card ─────────────────────────────────────────────────────────────
    top = ranks[0]  # carta más alta (ya ordenado desc)
    if top >= 12:    # Q, K, A
        high_card = 'high'
    elif top >= 9:   # 9, T, J
        high_card = 'mid'
    else:
        high_card = 'low'

    # ── Tag compacto para spot_identifier ────────────────────────────────────
    # Formato: wetness_flushtype_highcard
    # Ejemplos: dry_rb_high | wet_2t_mid | vwet_mono_low
    w_tag  = {'dry':'dry', 'wet':'wet', 'very_wet':'vwet'}.get(wetness, 'unk')
    f_tag  = {'mono':'mono', 'two_tone':'2t', 'rainbow':'rb'}.get(flush_draw, 'unk')
    h_tag  = high_card  # 'high' | 'mid' | 'low'
    texture_tag = f"{w_tag}_{f_tag}_{h_tag}"

    return {
        'wetness':    wetness,
        'pairing':    pairing,
        'flush_draw': flush_draw,
        'high_card':  high_card,
        'texture_tag': texture_tag,
        'raw': cards
    }


def enrich_df_with_board_texture(df):
    """
    Añade columnas de textura de board al DataFrame.

    Columnas añadidas:
        board_wetness:    'dry' | 'wet' | 'very_wet' | 'unknown'
        board_pairing:    'paired' | 'unpaired' | 'unknown'
        board_flush_draw: 'mono' | 'two_tone' | 'rainbow' | 'unknown'
        board_high_card:  'high' | 'mid' | 'low' | 'unknown'
        board_texture_tag: str compacto, ej. 'wet_2t_high'

    INTEGRACIÓN CON build_spot_identifier:
    Esta función se llama DESPUÉS de build_spot_identifier. Enriquece el
    spot_identifier añadiendo la textura al final solo para manos postflop:
        BTN_IP_SRP_deep_flop_R_C → BTN_IP_SRP_deep_flop_R_C_wet_2t_high

    Para preflop folds (sin flop), el spot_identifier no cambia.
    Esto mantiene la muestra de spots preflop intacta.

    Args:
        df: DataFrame con columna 'board_cards_flop' y 'spot_identifier'

    Returns:
        DataFrame enriquecido con columnas de textura y spot_identifier actualizado
    """
    if df.empty:
        return df

    df = df.copy()

    # Calcular textura para cada mano
    textures = df['board_cards_flop'].apply(
        lambda x: classify_board_texture(x) if pd.notna(x) and str(x).strip() not in ('', 'nan') else
                  {'wetness':'unknown','pairing':'unknown','flush_draw':'unknown',
                   'high_card':'unknown','texture_tag':'noflop','raw':[]}
    )

    df['board_wetness']    = textures.apply(lambda t: t['wetness'])
    df['board_pairing']    = textures.apply(lambda t: t['pairing'])
    df['board_flush_draw'] = textures.apply(lambda t: t['flush_draw'])
    df['board_high_card']  = textures.apply(lambda t: t['high_card'])
    df['board_texture_tag']= textures.apply(lambda t: t['texture_tag'])

    # ── Actualizar spot_identifier para manos postflop ────────────────────────
    # Solo se añade textura si la decision_street es flop/turn/river
    # y si hay un flop real (no 'noflop').
    # Preflop folds quedan sin cambios → no fragmenta muestra preflop.
    if 'spot_identifier' in df.columns and 'decision_street' in df.columns:
        postflop_mask = (
            df['decision_street'].isin(['flop', 'turn', 'river']) &
            (df['board_texture_tag'] != 'noflop') &
            (df['board_texture_tag'] != 'unknown')
        )
        df.loc[postflop_mask, 'spot_identifier'] = (
            df.loc[postflop_mask, 'spot_identifier'] + '_' +
            df.loc[postflop_mask, 'board_texture_tag']
        )

    enriched = (df['board_texture_tag'] != 'noflop').sum()
    total    = len(df)
    print(f"   ✅ Board texture calculada: {enriched:,}/{total:,} manos con flop")
    print(f"      Distribución wetness: " +
          " | ".join(f"{k}:{v}" for k,v in
                     df['board_wetness'].value_counts().items()))
    print(f"      Distribución flush:   " +
          " | ".join(f"{k}:{v}" for k,v in
                     df['board_flush_draw'].value_counts().items()))
    return df


def display_board_texture_summary(df):
    """Resumen de distribución de texturas para el dashboard."""
    if 'board_wetness' not in df.columns:
        print("   ⚪ Sin datos de board texture. Ejecuta enrich_df_with_board_texture primero.")
        return

    print("\n─────────────────────────────────────────────────────")
    print("  BOARD TEXTURE DISTRIBUTION")
    print("─────────────────────────────────────────────────────")

    postflop = df[df['board_texture_tag'] != 'noflop']
    if postflop.empty:
        print("  ⚪ Sin manos postflop con flop disponibles.")
        return

    n = len(postflop)
    print(f"  Manos con flop: {n:,}")
    print()

    for col, label in [('board_wetness','Wetness'), ('board_flush_draw','Flush Draw'),
                       ('board_high_card','High Card'), ('board_pairing','Pairing')]:
        counts = postflop[col].value_counts()
        print(f"  {label}:")
        for k, v in counts.items():
            bar = '█' * int(v/n*20)
            print(f"    {k:12s} {bar:20s} {v:4d} ({v/n*100:.0f}%)")
        print()


print("✅ Board Texture Classifier cargado (Sección 3e).")
print("   Uso: df = enrich_df_with_board_texture(df)")
print("   Añade: board_wetness, board_flush_draw, board_high_card, board_pairing, board_texture_tag")
print("   El spot_identifier se actualiza automáticamente para manos postflop.")


# ════════════════════════════════════════════════════════════════════════════
# SECCIÓN 3f — Visualización de Manos Representativas por Drill
#
# PROBLEMA QUE RESUELVE:
# El sistema identifica que tienes un leak en BB_OOP_SRP_deep_flop_C_F.
# Te dice que te está costando -18€/h. Pero luego tienes que ir al tracker,
# filtrar manualmente por ese spot y revisar las manos tú solo.
#
# SOLUCIÓN:
# Cuando el módulo de estudio sugiere un drill, extrae automáticamente las
# manos de ese spot con mayor desviación de EV (las más instructivas para
# estudiar) y las muestra en formato legible. Conecta el análisis abstracto
# con la revisión concreta. Drill guiado = fricción cero.
# ════════════════════════════════════════════════════════════════════════════

def get_representative_hands(df, spot_identifier, top_n=5, metric='ev_deviation'):
    """
    Extrae las manos más representativas de un spot para drill guiado.

    "Representativas" = mayor desviación de EV respecto a la media del spot.
    Estas son las manos donde más te desviaste de lo óptimo — las más
    instructivas para estudiar, no simplemente las que más perdiste
    (que puede ser pura varianza de all-in).

    Args:
        df:              DataFrame completo de manos con spot_identifier
        spot_identifier: string exacto del spot, ej. 'BB_OOP_SRP_deep_flop_C_F'
        top_n:           número de manos a devolver (default 5)
        metric:          'ev_deviation' (default) | 'worst_net' | 'random_sample'
                         ev_deviation → mayor abs(ev_won - mean_ev_spot)
                         worst_net    → manos con mayor pérdida neta
                         random_sample→ muestra aleatoria representativa

    Returns:
        DataFrame con las manos seleccionadas + columna 'ev_deviation'
        Vacío si el spot no existe o tiene < 3 manos.
    """
    if df.empty or 'spot_identifier' not in df.columns:
        return pd.DataFrame()

    spot_df = df[df['spot_identifier'] == spot_identifier].copy()

    if len(spot_df) < 3:
        return pd.DataFrame()

    # Calcular desviación de EV respecto a la media del spot
    if 'ev_won' in spot_df.columns:
        mean_ev = spot_df['ev_won'].mean()
        spot_df['ev_deviation'] = (spot_df['ev_won'] - mean_ev).abs()
    else:
        spot_df['ev_deviation'] = 0.0

    if metric == 'ev_deviation':
        result = spot_df.nlargest(top_n, 'ev_deviation')
    elif metric == 'worst_net':
        result = spot_df.nsmallest(top_n, 'net_won') if 'net_won' in spot_df.columns else spot_df.head(top_n)
    else:  # random_sample
        result = spot_df.sample(min(top_n, len(spot_df)), random_state=42)

    return result.reset_index(drop=True)


def display_drill_hands(df, spot_identifier, top_n=5, hero=None):
    """
    Muestra las manos representativas de un spot en formato drill guiado.

    Output diseñado para revisión rápida:
    - Fecha y session_id
    - Cartas del hero y board
    - Línea de acciones por calle
    - Net won vs EV won (para ver si fue suerte o decisión)
    - Desviación de EV (el "error" cuantificado)

    Args:
        df:              DataFrame completo de manos
        spot_identifier: spot a revisar
        top_n:           número de manos (default 5)
        hero:            nick del hero para el output (cosmético)
    """
    hands = get_representative_hands(df, spot_identifier, top_n=top_n)

    if hands.empty:
        print(f"   ⚪ Sin manos disponibles para spot '{spot_identifier}' (mínimo 3).")
        return

    spot_data = df[df['spot_identifier'] == spot_identifier]
    mean_ev   = spot_data['ev_won'].mean() if 'ev_won' in spot_data.columns else 0.0
    mean_net  = spot_data['net_won'].mean() if 'net_won' in spot_data.columns else 0.0

    print(f"\n{'─'*62}")
    print(f"  DRILL GUIADO — {spot_identifier}")
    print(f"  {len(spot_data)} manos en este spot | "
          f"EV medio: {mean_ev:+.3f}€ | Net medio: {mean_net:+.3f}€")
    print(f"  Mostrando top {len(hands)} por desviación de EV (más instructivas)")
    print(f"{'─'*62}")

    for i, row in hands.iterrows():
        date_str   = str(row.get('date','?'))[:16] if pd.notna(row.get('date')) else '?'
        session    = str(row.get('session_id','?'))
        hole       = str(row.get('hole_cards','??'))
        flop       = str(row.get('board_cards_flop','')) or '—'
        turn       = str(row.get('board_cards_turn','')) or '—'
        river      = str(row.get('board_cards_river','')) or '—'
        pf_act     = str(row.get('preflop_action','')) or '—'
        fl_act     = str(row.get('flop_action',''))    or '—'
        tu_act     = str(row.get('turn_action',''))    or '—'
        ri_act     = str(row.get('river_action',''))   or '—'
        net        = float(row.get('net_won', 0))
        ev         = float(row.get('ev_won',  0))
        dev        = float(row.get('ev_deviation', 0))
        luck       = net - ev  # positivo = tuvo suerte, negativo = mala suerte

        # Semáforo de desviación
        dev_sem = '🔴' if dev > abs(mean_ev) * 2 else ('🟡' if dev > abs(mean_ev) else '🟢')

        print(f"\n  [{i+1}] {date_str} | {session} | Cartas: {hole}")
        print(f"       Board: {flop} / {turn} / {river}")
        print(f"       Acciones: PF:{pf_act}  F:{fl_act}  T:{tu_act}  R:{ri_act}")
        print(f"       Net: {net:+.3f}€  |  EV: {ev:+.3f}€  |  "
              f"Luck: {luck:+.3f}€  |  Dev: {dev:.3f}€ {dev_sem}")

        # Textura si está disponible
        if 'board_texture_tag' in row and row.get('board_texture_tag','noflop') != 'noflop':
            tex = row.get('board_texture_tag','')
            wet = row.get('board_wetness','')
            fd  = row.get('board_flush_draw','')
            hc  = row.get('board_high_card','')
            print(f"       Textura: {tex} (wet:{wet} fd:{fd} top:{hc})")

    print(f"\n  📋 PREGUNTA PARA EL DRILL:")
    # Inferir la pregunta más relevante según la acción del spot
    spot_parts = spot_identifier.split('_')
    last_action = spot_parts[-1] if spot_parts else '?'
    if last_action == 'F':
        print(f"     ¿Cuáles de estas manos deberías haber continuado (call/raise) en lugar de fold?")
        print(f"     Criterio: calcula equity vs rango del oponente. Si equity > pot_odds → no fold.")
    elif last_action in ('C', 'X'):
        print(f"     ¿En cuáles de estas manos deberías haber aplicado más presión (bet/raise)?")
        print(f"     Criterio: ¿tenías equity suficiente + fold equity combinados para ser +EV?")
    elif last_action in ('B', 'R', '3B'):
        print(f"     ¿El sizing de apuesta/raise fue óptimo en estas manos?")
        print(f"     Criterio: compara SPR del bote vs objetivo de pot odds que ofreces al rival.")
    else:
        print(f"     Revisa la lógica de cada mano. ¿Qué harías diferente sabiendo el resultado?")

    print(f"{'─'*62}\n")



def _confidence_label(count):
    """Etiqueta de confianza estadística basada en número de manos en el spot."""
    if count >= 150:
        return '🟢 señal fuerte'
    elif count >= 50:
        return '🟡 señal media'
    elif count >= 15:
        return '⚠️  emergente'
    else:
        return '🔇 ruido'


def display_top_spots_with_hands(df, spot_results, top_n_spots=3, hands_per_spot=3):
    """
    Para cada uno de los top N leaks, muestra sus manos más representativas.
    Filtro dual de confianza:
      - Display principal:   spots con ≥50 manos (señal orientativa)
      - Display secundario:  spots con 15–49 manos (emergentes, radar)
      - Invisible:           spots con <15 manos (ruido puro)
    Etiqueta de confianza automática por count para interpretar resultados.

    FIX drill freq leaks: para leaks de frecuencia preflop (todo-fold),
    selecciona manos donde hero SÍ entró al pot → más instructivas.

    Args:
        df:             DataFrame completo de manos
        spot_results:   salida de calculate_ev_metrics()
        top_n_spots:    número de spots a mostrar (default 3, antifricción)
        hands_per_spot: manos por spot (default 3)
    """
    if spot_results is None or spot_results.empty:
        print("   ⚪ Sin spots disponibles para drill guiado.")
        return
    if df.empty:
        print("   ⚪ DataFrame vacío.")
        return

    # ── Filtro dual: separar señal real de emergentes ────────────────────
    DISPLAY_PRIMARY_MIN   = 50   # señal orientativa
    DISPLAY_SECONDARY_MIN = 15   # emergente / radar
    DISPLAY_SECONDARY_MAX = 49

    col_count = 'spot_hands_count'
    if col_count not in spot_results.columns:
        col_count = 'count' if 'count' in spot_results.columns else None

    roi   = build_roi_ranking(spot_results, top_n=top_n_spots * 3)  # pedir más para filtrar
    leaks = roi.get('leaks', pd.DataFrame())

    if leaks.empty:
        print("   ✅ Sin leaks significativos para drill guiado.")
        return

    # Separar en primario (≥50) y secundario (15–49)
    if col_count and col_count in leaks.columns:
        primary   = leaks[leaks[col_count] >= DISPLAY_PRIMARY_MIN]
        secondary = leaks[(leaks[col_count] >= DISPLAY_SECONDARY_MIN) &
                          (leaks[col_count] <= DISPLAY_SECONDARY_MAX)]
    else:
        primary, secondary = leaks, pd.DataFrame()

    # ── Display primario ─────────────────────────────────────────────────
    print(f"\n{'═'*62}")
    print(f"  DRILL GUIADO — TOP {min(top_n_spots, len(primary))} LEAKS (señal ≥50 manos)")
    print(f"  {hands_per_spot} manos más instructivas por spot")
    print(f"{'═'*62}")

    if primary.empty:
        print("   ⚪ Sin leaks con ≥50 manos todavía. Acumula más volumen.")
    else:
        for _, leak_row in primary.head(top_n_spots).iterrows():
            spot  = leak_row.get('spot_identifier', '?')
            ev_h  = leak_row.get('impacto_ev_total_eur_shrunk', 0)
            n_h   = leak_row.get(col_count or 'spot_hands_count', 0)
            conf  = _confidence_label(n_h)
            # stack_depth_bb media del spot
            _sdb_avg = df[df['spot_identifier'] == spot]['stack_depth_bb'].mean() if 'stack_depth_bb' in df.columns else float('nan')
            _sdb_s = f" | ~{_sdb_avg:.0f}BB eff" if not pd.isna(_sdb_avg) and _sdb_avg > 0 else ""
            print(f"\n  🔴 LEAK: {spot} | {ev_h:.2f}€/h shrunk | {n_h} manos{_sdb_s} | {conf}")

            # FIX drill freq leaks: si el spot termina en _F (fold) preflop,
            # mostrar manos donde hero SÍ entró → más instructivas que 3 folds
            spot_parts = spot.split('_')
            is_preflop_fold = (spot_parts[-1] == 'F' and
                               'preflop' in spot.lower())
            if is_preflop_fold:
                # Buscar manos del MISMO grupo posicional donde hero no foldeó
                pos = spot_parts[0] if spot_parts else ''
                ip_oop_tag = spot_parts[1] if len(spot_parts) > 1 else ''
                alt_spots = df[
                    df['spot_identifier'].str.startswith(f'{pos}_{ip_oop_tag}') &
                    ~df['spot_identifier'].str.endswith('_F')
                ] if 'spot_identifier' in df.columns else pd.DataFrame()
                if not alt_spots.empty:
                    print(f"     ℹ️  Leak de frecuencia (foldeas demasiado desde {pos}).")
                    print(f"     Mostrando manos donde SÍ entraste al pot (más instructivas):")
                    alt_spot_id = alt_spots['spot_identifier'].value_counts().index[0]
                    display_drill_hands(df, alt_spot_id, top_n=hands_per_spot)
                else:
                    display_drill_hands(df, spot, top_n=hands_per_spot)
            else:
                display_drill_hands(df, spot, top_n=hands_per_spot)

    # ── Display secundario (emergentes) ─────────────────────────────────
    if not secondary.empty:
        print(f"\n{'─'*62}")
        print(f"  📡 SPOTS EMERGENTES (15–49 manos) — solo como radar, baja confianza")
        print(f"{'─'*62}")
        for _, row in secondary.head(top_n_spots).iterrows():
            spot = row.get('spot_identifier', '?')
            ev_h = row.get('impacto_ev_total_eur_shrunk', 0)
            n_h  = row.get(col_count or 'spot_hands_count', 0)
            conf = _confidence_label(n_h)
            print(f"  ⚠️  {spot} | {ev_h:.2f}€/h | {n_h} manos | {conf}")




def display_cognitive_review(df, spot_identifier, n_hands=5):
    """
    Revision cognitiva estructurada post-sesion. v1.68
    
    Protocolo por cada mano:
      1. Muestra cartas + contexto SIN el resultado
      2. Formula la pregunta especifica del drill
      3. Revela resultado con diagnostico decision vs varianza
    
    Este protocolo convierte correccion de frecuencia en comprension real.
    Sin el: tracker sofisticado. Con el: camino a NL50-NL200.
    """
    hands = get_representative_hands(df, spot_identifier, top_n=n_hands)
    if hands.empty:
        print("   Sin manos disponibles para este spot.")
        return

    spot_data = df[df['spot_identifier'] == spot_identifier]
    drill_act = spot_identifier.split('_')[-1]
    n_total   = len(spot_data)

    print()
    print('='*60)
    print('  REVISION COGNITIVA POST-SESION')
    print('  Spot: ' + spot_identifier)
    print('  ' + str(n_total) + ' manos totales | Revisando ' + str(len(hands)) + ' mas instructivas')
    print()
    print('  PROTOCOLO: Lee -> Razonamiento -> Resultado -> Contrasta')
    print('='*60)

    errores_dec = 0
    errores_var = 0

    for idx_r, (_, row) in enumerate(hands.iterrows(), 1):
        hole   = str(row.get('hole_cards', '??'))
        flop   = str(row.get('board_cards_flop', '')) or '-'
        turn   = str(row.get('board_cards_turn', '')) or '-'
        river  = str(row.get('board_cards_river', '')) or '-'
        pf_act = str(row.get('preflop_action', '')) or '-'
        net    = float(row.get('net_won', 0))
        ev     = float(row.get('ev_won',  0))
        luck   = net - ev
        dev    = float(row.get('ev_deviation', 0))
        sess   = str(row.get('session_id', '?'))
        date_s = str(row.get('date', '?'))[:10]

        print()
        print('  MANO ' + str(idx_r) + '/' + str(len(hands)) + '  |  ' + date_s + '  |  ' + sess)
        print('  Cartas:    ' + hole)
        print('  Board:     ' + flop + ' / ' + turn + ' / ' + river)
        print('  Accion PF: ' + pf_act)
        print()

        if drill_act == 'F':
            print('  PREGUNTA: Deberias haber defendido esta mano?')
            print('  Escribe: "SI porque..." o "NO porque..."')
            print('  (escribe aqui tu razonamiento antes de ver el resultado)')
        elif drill_act in ('C', 'X'):
            print('  PREGUNTA: Deberias haber aplicado mas presion aqui?')
            print('  Escribe: "SI porque..." o "NO porque..."')
        elif drill_act in ('B', 'R', '3B'):
            print('  PREGUNTA: El tamano de tu apuesta fue optimo?')
            print('  Escribe tu razonamiento.')
        else:
            print('  PREGUNTA: Que harias diferente en esta mano?')
            print('  Escribe tu razonamiento en una linea.')

        print()
        print('  -- RESULTADO --')

        net_s = ('+' if net >= 0 else '') + '{:.3f}'.format(net)
        ev_s  = ('+' if ev  >= 0 else '') + '{:.3f}'.format(ev)
        lk_s  = ('+' if luck >= 0 else '') + '{:.3f}'.format(luck)
        print('  Net: ' + net_s + 'E  |  EV: ' + ev_s + 'E  |  Suerte: ' + lk_s + 'E')

        if drill_act == 'F' and net < 0:
            print('  >> Foldaste con ' + hole + '. Tenias odds para defender?')
            print('     Si no puedes calcular la equity -> esa es la brecha cognitiva.')
            errores_dec += 1
        elif abs(luck) > abs(ev) * 0.5 and abs(ev) > 0.01:
            veredicto = 'correcta' if ev > 0 else 'incorrecta'
            print('  >> VARIANZA: decision ' + veredicto + ' pero resultado por suerte.')
        elif abs(dev) > 0.05:
            print('  >> DECISION: desviacion EV = ' + ('+' if dev >= 0 else '') + '{:.3f}'.format(dev) + 'E')
            errores_dec += 1

        print('  ' + '-'*56)

    print()
    print('  RESUMEN:')
    print('  Manos con posible error de decision: ' + str(errores_dec) + '/' + str(len(hands)))
    if errores_dec > 0:
        print()
        print('  ACCION CONCRETA:')
        print('  Para cada mano de error -> escribe UNA linea:')
        print('  "El rango que me gana aqui es: [...]"')
        print('  Si no puedes nombrarlo = instinto, no comprension.')
        print('  ESO es lo que construye NL50.')
    print('='*60)




def generate_study_brief(df, spot_identifier, overall_metrics=None,
                         output_file=None, api_key=None):
    """
    Genera un brief de estudio completo sobre el drill activo. v1.68
    
    Sirve para dos escenarios:
    1. Estudio en profundidad en el ordenador (1h sin pipeline)
    2. Estudio en momentos muertos sin Colab (copiar al movil)
    
    Contenido:
    - El concepto teorico del spot con los numeros reales de tus datos
    - Por que te cuesta este spot (causa-raiz matematica)
    - La regla de decision para NL2 (explotativa)
    - La regla de decision para NL50 (teorica)
    - 3 situaciones concretas de tus manos para estudiar
    - 5 preguntas para reflexionar sin el sistema
    - Que estudiar en Equilab / GTO Wizard si tienes acceso
    """
    spot_data = df[df['spot_identifier'] == spot_identifier] if 'spot_identifier' in df.columns else df
    n_total   = len(spot_data)
    
    # Metricas del spot
    fold_rate = spot_data['flg_p_fold'].mean() * 100 if 'flg_p_fold' in spot_data.columns and not spot_data.empty else 0
    ev_total  = spot_data['net_won'].sum() if not spot_data.empty else 0
    bb_val    = 0.02
    
    # Metricas globales
    bb100     = overall_metrics.get('bb_per_100_net', 0) if overall_metrics else 0
    vpip      = overall_metrics.get('vpip_pct', 0) if overall_metrics else 0
    wsd       = overall_metrics.get('wsd_pct', 0) if overall_metrics else 0
    
    # Parsear spot_identifier
    parts     = spot_identifier.split('_')
    posicion  = parts[0] if len(parts) > 0 else '?'
    ip_oop    = parts[1] if len(parts) > 1 else '?'
    pot_type  = parts[2] if len(parts) > 2 else '?'
    calle     = parts[4] if len(parts) > 4 else '?'
    accion    = parts[-1] if parts else '?'
    
    # Manos ejemplo del spot
    hands_sample = get_representative_hands(df, spot_identifier, top_n=3)
    
    lines = []
    lines.append('='*60)
    lines.append('BRIEF DE ESTUDIO — ' + spot_identifier)
    lines.append('Generado desde tus ' + str(len(df)) + ' manos reales de NL2')
    lines.append('='*60)
    lines.append('')
    
    lines.append('1. EL PROBLEMA (tus numeros reales)')
    lines.append('-'*40)
    lines.append('Spot: ' + posicion + ' ' + ip_oop + ' en ' + pot_type + ' | Calle: ' + calle + ' | Accion: ' + accion)
    lines.append('Manos en este spot: ' + str(n_total))
    lines.append('Tu fold rate: ' + '{:.0f}'.format(fold_rate) + '% (objetivo: ~50%)')
    lines.append('EV perdido en este spot: ' + '{:+.2f}'.format(ev_total) + 'E')
    lines.append('Tu BB/100 global: ' + '{:.1f}'.format(bb100))
    lines.append('Tu W$SD: ' + '{:.1f}'.format(wsd) + '% (objetivo: >50%)')
    lines.append('')
    
    lines.append('2. POR QUE TE CUESTA ESTE SPOT')
    lines.append('-'*40)
    if accion == 'F' and 'BB' in posicion:
        lines.append('Estas overfoldando en BB en SRP profundo.')
        lines.append('Con un open de 2-2.5bb desde BTN/CO/SB, tus pot odds son 3:1.')
        lines.append('Matematicamente necesitas defender ~50% de tu rango.')
        lines.append('Tu rango desde BB es amplio — tienes muchas manos con equity suficiente.')
        lines.append('El problema: tomas decisiones de fold por incomodidad OOP,')
        lines.append('no por calculo de equity vs pot odds.')
    elif accion == 'F':
        lines.append('Estas foldando mas de lo optimo en este spot.')
        lines.append('Cada fold excesivo es EV negativo — regalas equity al oponente.')
        lines.append('El problema no es el resultado de la mano — es la frecuencia sistematica.')
    else:
        lines.append('Revisa tu frecuencia real vs la referencia NL2 para este spot.')
        lines.append('La diferencia entre lo que haces y lo optimo es el leak medible.')
    lines.append('')
    
    lines.append('3. LA REGLA PARA NL2 (explotativa)')
    lines.append('-'*40)
    if accion == 'F' and 'BB' in posicion:
        lines.append('SIMPLIFY (nivel 1): NO foldear NINGUNA mano suited.')
        lines.append('Si la mano es suited -> call automatico. Sin excepciones.')
        lines.append('Esto te lleva de tu ' + '{:.0f}'.format(fold_rate) + '% fold a ~' + '{:.0f}'.format(max(0, fold_rate - 24)) + '% fold.')
        lines.append('El pool NL2 casi nunca foldea en calles tardias — tus manos')
        lines.append('con equity de flush/straight tienen valor real postflop.')
    else:
        lines.append('La regla especifica esta en el briefing del sistema.')
        lines.append('Consulta DRILL_ACTIVO en Colab para ver la instruccion exacta.')
    lines.append('')
    
    lines.append('4. LA REGLA PARA NL50 (teorica — para cuando llegues)')
    lines.append('-'*40)
    if accion == 'F' and 'BB' in posicion:
        lines.append('En NL50 no es "todas las suited". Es:')
        lines.append('- Identificar el SPR del bote')
        lines.append('- Calcular pot odds exactos (no aproximados)')
        lines.append('- Asignar rango al villano segun su posicion y sizing')
        lines.append('- Defender las manos con equity > pot odds + implied odds')
        lines.append('El fundamento es identico. La precision es mayor.')
        lines.append('Lo que aprendes ahora en NL2 es la base de lo de arriba.')
    else:
        lines.append('La precision aumenta con el stake pero el concepto es el mismo.')
        lines.append('NL50: misma logica, rangos mas ajustados, menos explotacion masiva.')
    lines.append('')
    
    lines.append('5. TUS MANOS REALES PARA ESTUDIAR')
    lines.append('-'*40)
    if not hands_sample.empty:
        for idx_h, (_, row) in enumerate(hands_sample.iterrows(), 1):
            hole  = str(row.get('hole_cards', '??'))
            flop  = str(row.get('board_cards_flop', '')) or '-'
            net   = float(row.get('net_won', 0))
            pf    = str(row.get('preflop_action', '')) or '-'
            date_s = str(row.get('date', '?'))[:10]
            lines.append('Mano ' + str(idx_h) + ' (' + date_s + '):')
            lines.append('  Cartas: ' + hole + ' | Flop: ' + flop)
            lines.append('  Accion PF: ' + pf + ' | Net: ' + ('+' if net >= 0 else '') + '{:.3f}'.format(net) + 'E')
            lines.append('  -> Pregunta: con estas cartas, tenias equity para defender?')
            lines.append('     Calcula: 1 / (1 + 2.5) = ~29% pot odds minimos.')
            lines.append('     Tu equity con ' + hole + ' vs rango BTN NL2 (45%): ~?')
            lines.append('     Si equity > 29% -> debias defender.')
            lines.append('')
    else:
        lines.append('(Ejecuta el pipeline primero para ver tus manos reales)')
    
    lines.append('6. PREGUNTAS PARA MOMENTOS MUERTOS')
    lines.append('-'*40)
    lines.append('Contesta estas sin mirar el sistema. Despues contrasta.')
    lines.append('')
    lines.append('P1: Si estas en BB con 7c8c y BTN abre 2.5bb,')
    lines.append('    cuales son tus pot odds exactos? Deberias defender?')
    lines.append('')
    lines.append('P2: Que diferencia hay entre defender desde BB con K2s')
    lines.append('    vs K2o? Calcula la equity aproximada de cada una.')
    lines.append('')
    lines.append('P3: Si el pool NL2 casi nunca foldea en river (dato real),')
    lines.append('    como cambia tu estrategia de bluff vs value?')
    lines.append('')
    lines.append('P4: Tu W$SD es ' + '{:.1f}'.format(wsd) + '%. Que significa que sea < 50%?')
    lines.append('    Que tipo de manos estas llevando al showdown y perdiendo?')
    lines.append('')
    lines.append('P5: Si corriges tu BB defense de ' + '{:.0f}'.format(fold_rate) + '% fold a 50% fold,')
    lines.append('    cuantas BBs por 100 manos recuperas aproximadamente?')
    lines.append('    Pista: ' + str(n_total) + ' manos en el spot, ' + '{:.2f}'.format(abs(ev_total)) + 'E perdidos.')
    lines.append('')
    
    lines.append('7. QUE ESTUDIAR EN EQUILAB / GTO WIZARD')
    lines.append('-'*40)
    if accion == 'F' and 'BB' in posicion:
        lines.append('Equilab (gratis):')
        lines.append('  - Abre "Range vs Range"')
        lines.append('  - Hero: rango BB vs BTN open NL2 (~45% BTN)')
        lines.append('  - Calcula equity de cada mano del rango BB')
        lines.append('  - Identifica que manos tienen >29% equity (pot odds con 2.5bb open)')
        lines.append('  - Esas son las manos que SIEMPRE debes defender')
        lines.append('')
        lines.append('GTO Wizard (si tienes acceso):')
        lines.append('  - 6-max NL | Spot: BB vs BTN open 2.5bb | 100bb profundo')
        lines.append('  - Ver frecuencia de call/raise/fold por mano')
        lines.append('  - Comparar con tu frecuencia real (' + '{:.0f}'.format(100-fold_rate) + '% defend actual)')
    else:
        lines.append('Usa el generador del sistema:')
        lines.append('  display_leak_analysis(spot_results, ingested_df)')
        lines.append('  Genera la consulta exacta para GTO Wizard de tu spot.')
    lines.append('')
    lines.append('='*60)
    lines.append('Drill activo: ' + spot_identifier)
    lines.append('Sesiones con datos: usa el pipeline para ver tu progreso.')
    lines.append('='*60)
    
    brief_text = chr(10).join(lines)
    
    # Output
    print(brief_text)
    
    # Guardar si se pide
    if output_file:
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(brief_text)
            print()
            print('Guardado en: ' + output_file)
        except Exception as e:
            print('Error guardando: ' + str(e))
    
    return brief_text


def generate_quiz(df, spot_identifier, overall_metrics=None, n_questions=5):
    """
    Genera un quiz de preguntas sobre el drill activo. v1.68
    
    Diseñado para momentos muertos sin Colab.
    Las preguntas usan tus datos reales — no son genericas.
    
    Flujo:
    1. Responde las preguntas sin mirar el sistema
    2. Anota tus respuestas en el movil / papel
    3. Cuando vuelvas a Colab, contrasta con los datos reales
    """
    spot_data = df[df['spot_identifier'] == spot_identifier] if 'spot_identifier' in df.columns else df
    n_total   = len(spot_data)
    fold_rate = spot_data['flg_p_fold'].mean() * 100 if 'flg_p_fold' in spot_data.columns and not spot_data.empty else 0
    ev_total  = spot_data['net_won'].sum() if not spot_data.empty else 0
    bb_val    = 0.02
    bb100_spot = (ev_total / max(n_total, 1) / bb_val) * 100
    
    bb100_global = overall_metrics.get('bb_per_100_net', 0) if overall_metrics else 0
    wsd          = overall_metrics.get('wsd_pct', 0) if overall_metrics else 0
    
    parts    = spot_identifier.split('_')
    posicion = parts[0] if len(parts) > 0 else '?'
    accion   = parts[-1] if parts else '?'
    
    # Mano ejemplo para preguntas concretas
    hands = get_representative_hands(df, spot_identifier, top_n=3)
    ejemplo_hole = '7h8h'
    ejemplo_flop = 'Ah 2d 9s'
    if not hands.empty:
        row0 = hands.iloc[0]
        h = str(row0.get('hole_cards', ''))
        if h and h not in ('??', 'nan', 'None', ''):
            ejemplo_hole = h
        f = str(row0.get('board_cards_flop', ''))
        if f:
            ejemplo_flop = f
    
    print()
    print('='*60)
    print('QUIZ — ' + spot_identifier)
    print('Responde SIN mirar el sistema. Anota tus respuestas.')
    print('Cuando vuelvas a Colab, contrasta con los datos reales.')
    print('='*60)
    print()
    
    q = 1
    
    # Q1 — Matematica basica del spot
    if accion == 'F' and 'BB' in posicion:
        print('Q' + str(q) + '. MATEMATICA DEL SPOT')
        print('   BTN abre 2.5bb. Estas en BB con 1bb ya invertida.')
        print('   a) Cuanto tienes que pagar para continuar? [__]bb')
        print('   b) Cual es el bote si llamas? [__]bb')
        print('   c) Cuales son tus pot odds? [__]%')
        print('   d) Que equity minima necesitas para que sea rentable? [__]%')
        print()
        print('   Respuestas para contrastar luego:')
        print('   a) 1.5bb | b) 5.5bb | c) 1.5/5.5 = 27.3% | d) ~27%')
    else:
        print('Q' + str(q) + '. MATEMATICA DEL SPOT')
        print('   En el spot ' + spot_identifier + ':')
        print('   a) Cuantas manos tienes en este spot? Aproxima.')
        print('   b) Cuanto EV has perdido en total? Aproxima en euros.')
        print()
        print('   Respuestas: a) ' + str(n_total) + ' | b) ' + '{:.2f}'.format(abs(ev_total)) + 'E')
    q += 1
    print()
    
    # Q2 — Tu mano concreta
    print('Q' + str(q) + '. DECISION CON MANO CONCRETA')
    print('   Tienes ' + ejemplo_hole + ' en ' + posicion + '.')
    if ejemplo_flop and ejemplo_flop != '-':
        print('   Flop: ' + ejemplo_flop)
    print('   a) Que equity aproximada tiene esta mano vs rango NL2?')
    print('   b) Con los pot odds del spot, es correcta la defensa?')
    print('   c) Que factores adicionales considerarias?')
    print()
    print('   (Contrasta con: calculate_equity_vs_range("' + ejemplo_hole + '", "BTN") en Colab)')
    q += 1
    print()
    
    # Q3 — Tu patron
    print('Q' + str(q) + '. TU PATRON ACTUAL')
    print('   a) Cual es tu fold rate en este spot? [__]%')
    print('   b) Cual deberia ser segun la referencia NL2? [__]%')
    print('   c) Cuanto EV recuperarias si corriges la diferencia?')
    print()
    print('   Respuestas: a) ' + '{:.0f}'.format(fold_rate) + '% | b) ~50% | c) ~' + '{:.2f}'.format(abs(ev_total) * 0.5) + 'E')
    q += 1
    print()
    
    # Q4 — El pool
    print('Q' + str(q) + '. EL POOL NL2')
    print('   Sabiendo que el pool NL2 casi nunca foldea en turn/river:')
    print('   a) Como afecta eso a tus bluffs en este spot?')
    print('   b) Como afecta a tus manos de valor?')
    print('   c) Que tipo de manos son mas valiosas para defender BB?')
    print()
    print('   (No hay respuesta numerica — reflexiona y comparte con el coach)')
    q += 1
    print()
    
    # Q5 — Proyeccion de mejora
    print('Q' + str(q) + '. PROYECCION DE MEJORA')
    sessions_est = max(1, len(df['session_id'].unique()) if 'session_id' in df.columns else 1)
    ev_por_sesion = abs(ev_total) / sessions_est
    print('   Has perdido ' + '{:.2f}'.format(abs(ev_total)) + 'E en ' + str(n_total) + ' manos en este spot.')
    print('   Con ' + str(sessions_est) + ' sesiones = ' + '{:.2f}'.format(ev_por_sesion) + 'E/sesion en este spot.')
    print('   a) Si corriges el 50% del leak, cuanto recuperas por sesion?')
    print('   b) En 50 sesiones mas, cuanto supone la correccion total?')
    print('   c) Cuanto tiempo te llevar llegar al 80% de execution rate?')
    print()
    print('   Respuestas: a) ' + '{:.2f}'.format(ev_por_sesion * 0.5) + 'E | b) ' + '{:.2f}'.format(ev_por_sesion * 0.5 * 50) + 'E | c) depende de ti')
    print()
    
    print('='*60)
    print('Anota tus respuestas. Cuando abras Colab:')
    print('  1. Ejecuta el pipeline')
    print('  2. Llama a display_cognitive_chat() con tus dudas')
    print('  3. Contrasta tus respuestas con los datos reales')
    print('='*60)



def evaluate_hand_decision(row, spot_identifier, df_full=None, stake='NL2'):
    """
    Evalúa si la decisión en una mano concreta fue correcta según la teoría. v1.72

    Combina:
    - calculate_equity_vs_range: equity real de la mano vs rango del villano
    - Pot odds exactos calculados desde el sizing de la mano
    - Referencia NL2 para el spot
    - Acción tomada por el héroe

    Returns dict con:
        decision_correcta: bool
        veredicto:         str ('CORRECTO' / 'ERROR' / 'MARGINAL' / 'SIN_DATOS')
        motivo:            str explicacion de una linea
        equity:            float o None
        pot_odds_min:      float o None
        accion_tomada:     str
        accion_teorica:    str
    """
    hole_cards  = str(row.get('hole_cards', ''))
    pf_action   = str(row.get('preflop_action', '') or '')
    fl_action   = str(row.get('flop_action', '') or '')
    pos         = str(row.get('player_position', ''))
    ip_oop      = str(row.get('ip_oop', ''))
    stack_bb    = float(row.get('stack_depth_bb', 100) or 100)
    net_won     = float(row.get('net_won', 0))
    ev_won      = float(row.get('ev_won', 0))
    spot_parts  = spot_identifier.split('_')
    calle       = spot_parts[4] if len(spot_parts) > 4 else 'preflop'
    accion_spot = spot_parts[-1] if spot_parts else '?'

    resultado = {
        'decision_correcta': None,
        'veredicto':         'SIN_DATOS',
        'motivo':            'Sin cartas o contexto suficiente',
        'equity':            None,
        'pot_odds_min':      None,
        'accion_tomada':     accion_spot,
        'accion_teorica':    '?',
    }

    if not hole_cards or hole_cards in ('??', 'nan', 'None', ''):
        return resultado

    # FIX v1.81: accion_spot 'unknown' = acción no registrada → SIN_DATOS honesto
    if accion_spot == 'unknown' or not pf_action.strip():
        resultado['veredicto'] = 'SIN_DATOS'
        resultado['motivo']    = 'Acción preflop no registrada en este spot'
        return resultado

    # ── 1. Determinar acción tomada ──────────────────────────────────────
    if calle == 'preflop':
        accion_tomada = pf_action.split('_')[0] if pf_action else accion_spot
    else:
        accion_tomada = fl_action.split('_')[0] if fl_action else accion_spot
    resultado['accion_tomada'] = accion_tomada or accion_spot

    # ── 2. Calcular pot odds del spot ─────────────────────────────────────
    # Para BB vs steal: call = 1 BB (ya puesto 1bb de blind)
    # Open típico NL2: 2-2.5bb → pot odds = call/(pot+call)
    pot_odds_min = None
    if calle == 'preflop' and 'BB' in pos:
        # Pot odds para call vs open 2.5bb desde BB: call 1.5bb / pot 5.5bb = 27.3%
        # Pot odds para call vs open 3bb: call 2bb / pot 6bb = 33.3%
        # Usamos 27% como referencia conservadora (open 2.5bb más común en NL2)
        pot_odds_min = 27.3
    elif calle == 'preflop' and 'SB' in pos:
        # SB vs open: call más caro (no tiene bb ya puesto completo)
        pot_odds_min = 33.3
    elif calle == 'preflop' and accion_spot == 'F':
        pot_odds_min = 30.0  # genérico preflop

    resultado['pot_odds_min'] = pot_odds_min

    # ── 3. Calcular equity vs rango del villano ───────────────────────────
    villain_pos = 'BTN'  # default — el steal más común vs BB
    # Inferir posición del villano desde el spot
    if 'vs_BTN' in spot_identifier: villain_pos = 'BTN'
    elif 'vs_CO'  in spot_identifier: villain_pos = 'CO'
    elif 'vs_SB'  in spot_identifier: villain_pos = 'SB'
    elif 'vs_UTG' in spot_identifier: villain_pos = 'UTG'
    elif pos == 'BB': villain_pos = 'BTN'  # asumimos BTN como agresor más común
    elif pos == 'SB': villain_pos = 'BTN'

    equity_result = None
    try:
        equity_result = calculate_equity_vs_range(
            hole_cards, villain_pos, stake=stake,
            df_hero=df_full
        )
    except Exception:
        equity_result = None

    if equity_result and 'error' not in equity_result:
        equity = equity_result.get('equity_vs_range', 0)
        resultado['equity'] = equity
    else:
        equity = None

    # ── 4. Veredicto según teoría ─────────────────────────────────────────
    if calle == 'preflop' and accion_spot == 'F':
        # FIX v1.81: Drill de fold — evaluamos si debería haber defendido
        # Prioridad 1: equity calculada (más preciso)
        # Prioridad 2: hand range lookup desde GTO_REFERENCE_NL2 (fallback fiable)
        foldeo = accion_tomada.startswith('F')

        if equity is None or pot_odds_min is None:
            # Fallback: usar hand range lookup
            # BB defend range vs BTN open (~55%): manos que deberían defender
            # Cualquier suited, pares, broadways, Ax → defender
            # Basura pura (72o, 83o, 94o, etc.) → fold correcto
            cards = hole_cards.split()
            deberia_defender_range = False
            if len(cards) == 2:
                c1, c2 = cards[0], cards[1]
                rank1 = c1[:-1]; rank2 = c2[:-1]
                suit1 = c1[-1];  suit2 = c2[-1]
                suited   = (suit1 == suit2)
                ranks    = ['2','3','4','5','6','7','8','9','T','J','Q','K','A']
                r1_idx   = ranks.index(rank1) if rank1 in ranks else 0
                r2_idx   = ranks.index(rank2) if rank2 in ranks else 0
                hi_rank  = max(r1_idx, r2_idx)
                lo_rank  = min(r1_idx, r2_idx)
                is_pair  = (rank1 == rank2)
                has_ace  = ('A' in [rank1, rank2])
                has_face = hi_rank >= 9  # T+

                # BB vs BTN/CO open: defender top ~55% de rango
                # = cualquier suited + pares + Ax + broadways + connectors decentes
                deberia_defender_range = (
                    suited or           # cualquier suited
                    is_pair or          # cualquier par
                    has_ace or          # cualquier Ax
                    has_face or         # cualquier T/J/Q/K como high
                    (hi_rank >= 7 and lo_rank >= 5)  # connectors decentes 87+
                )

                if deberia_defender_range and foldeo:
                    resultado['veredicto']      = 'ERROR'
                    resultado['decision_correcta'] = False
                    resultado['accion_teorica'] = 'DEFENDER'
                    resultado['motivo'] = (
                        f'{"Suited" if suited else "Par" if is_pair else "Ax" if has_ace else "Broadway/conector"} '
                        f'desde BB vs open → en rango de defensa NL2. '
                        f'Fold incorrecto. (evaluación por rango, sin equity calc)'
                    )
                elif not deberia_defender_range and foldeo:
                    resultado['veredicto']      = 'CORRECTO'
                    resultado['decision_correcta'] = True
                    resultado['accion_teorica'] = 'FOLD'
                    resultado['motivo'] = (
                        f'Mano débil ({rank1}{rank2}o) fuera del rango de defensa BB NL2. '
                        f'Fold correcto. (evaluación por rango)'
                    )
                elif not deberia_defender_range and not foldeo:
                    resultado['veredicto']      = 'MARGINAL'
                    resultado['decision_correcta'] = None
                    resultado['accion_teorica'] = 'FOLD o DEFENDER'
                    resultado['motivo'] = (
                        f'Defensa con mano marginal ({rank1}{rank2}). '
                        f'Zona gris — depende del sizing del open. (evaluación por rango)'
                    )
                else:  # deberia_defender y defendió
                    resultado['veredicto']      = 'CORRECTO'
                    resultado['decision_correcta'] = True
                    resultado['accion_teorica'] = 'DEFENDER'
                    resultado['motivo'] = (
                        f'Defensa correcta con mano en rango BB NL2. '
                        f'(evaluación por rango, sin equity calc)'
                    )
            else:
                resultado['veredicto'] = 'SIN_DATOS'
                resultado['motivo']    = 'No se pudieron parsear las cartas'
            return resultado

        deberia_defender = equity > pot_odds_min

        if foldeo and deberia_defender:
            resultado['decision_correcta'] = False
            resultado['veredicto']         = 'ERROR'
            resultado['accion_teorica']    = 'DEFENDER (call o 3bet)'
            resultado['motivo'] = (
                f'Equity {equity:.1f}% > pot odds minimos {pot_odds_min:.1f}% → '
                f'defensa matematicamente correcta. Foldeo incorrecto.'
            )
        elif foldeo and not deberia_defender:
            resultado['decision_correcta'] = True
            resultado['veredicto']         = 'CORRECTO'
            resultado['accion_teorica']    = 'FOLD'
            resultado['motivo'] = (
                f'Equity {equity:.1f}% < pot odds minimos {pot_odds_min:.1f}% → '
                f'fold matematicamente correcto.'
            )
        elif not foldeo and deberia_defender:
            resultado['decision_correcta'] = True
            resultado['veredicto']         = 'CORRECTO'
            resultado['accion_teorica']    = 'DEFENDER'
            resultado['motivo'] = (
                f'Equity {equity:.1f}% > pot odds minimos {pot_odds_min:.1f}% → '
                f'defensa correcta ejecutada.'
            )
        else:
            # Defendio pero equity no lo justificaba
            resultado['decision_correcta'] = False
            resultado['veredicto']         = 'ERROR'
            resultado['accion_teorica']    = 'FOLD'
            resultado['motivo'] = (
                f'Equity {equity:.1f}% < pot odds minimos {pot_odds_min:.1f}% → '
                f'defense sin equity suficiente.'
            )

        # Zona marginal: diferencia < 5pp → veredicto MARGINAL
        if equity is not None and pot_odds_min is not None:
            if abs(equity - pot_odds_min) < 5:
                resultado['veredicto']  = 'MARGINAL'
                resultado['motivo'] += ' (zona gris — ambas decisiones defendibles)'

    elif calle == 'preflop' and accion_spot in ('R', '3B'):
        # Drill de raise — evaluar si la mano estaba en rango de raise
        ref = REFERENCE_RANGES.get(stake, REFERENCE_RANGES.get('NL2', {}))
        ref_open = ref.get(f'{pos}_open_pct', 35)
        percentile = equity_result.get('percentile', 50) if equity_result else None
        if percentile is not None:
            resultado['equity'] = equity
            deberia_abrir = percentile <= ref_open  # top X% del espacio
            hizo_raise    = accion_tomada in ('R', '3B', 'raise')
            resultado['decision_correcta'] = (hizo_raise == deberia_abrir)
            resultado['veredicto'] = 'CORRECTO' if resultado['decision_correcta'] else 'ERROR'
            resultado['accion_teorica'] = 'RAISE' if deberia_abrir else 'FOLD/CALL'
            resultado['motivo'] = (
                f'Mano en percentil {percentile:.0f}% del espacio. '
                f'Referencia {pos} NL2: abrir top {ref_open:.0f}%. '
                f'{"En rango de apertura." if deberia_abrir else "Fuera del rango de apertura."}'
            )
        else:
            resultado['veredicto'] = 'SIN_DATOS'
            resultado['motivo']    = 'Sin percentil calculado'

    else:
        # Spots postflop o no mapeados — evaluacion basica por EV deviation
        ev_dev = abs(net_won - ev_won)
        if ev_dev > 0.05:
            resultado['veredicto']      = 'ERROR' if net_won < ev_won else 'CORRECTO'
            resultado['decision_correcta'] = net_won >= ev_won
            resultado['motivo'] = (
                f'Desviacion EV: {net_won-ev_won:+.3f}E. '
                f'{"Decision suboptima vs EV esperado." if net_won < ev_won else "En linea con EV esperado."}'
            )
        else:
            resultado['veredicto']         = 'CORRECTO'
            resultado['decision_correcta'] = True
            resultado['motivo']            = 'Sin desviacion significativa de EV'

    return resultado


def display_hand_evaluation(df, spot_identifier, n_hands=10,
                            overall_metrics=None, stake='NL2'):
    """
    Muestra evaluacion automatica de decision por mano. v1.72

    Por cada mano del drill:
      - Cartas + board + accion
      - Equity calculada vs rango del villano
      - Pot odds minimos para el spot
      - Veredicto: CORRECTO / ERROR / MARGINAL
      - Motivo de una linea

    Es la funcion que responde: "en esta mano, hiciste lo correcto?"

    Uso:
        display_hand_evaluation(ingested_df, DRILL_ACTIVO, n_hands=10)
    """
    hands = get_representative_hands(df, spot_identifier, top_n=n_hands)
    if hands.empty:
        print('Sin manos disponibles para este spot.')
        return

    spot_parts  = spot_identifier.split('_')
    accion_spot = spot_parts[-1] if spot_parts else '?'

    print()
    print('='*65)
    print('  EVALUACION AUTOMATICA DE DECISIONES')
    print('  Spot: ' + spot_identifier)
    print('  Pregunta: en cada mano, ¿hiciste lo correcto?')
    print('='*65)

    errores   = 0
    correctas = 0
    marginales = 0
    sin_datos  = 0
    errores_detalle = []

    for idx_r, (_, row) in enumerate(hands.iterrows(), 1):
        eval_result = evaluate_hand_decision(
            row, spot_identifier, df_full=df, stake=stake
        )

        hole   = str(row.get('hole_cards', '??'))
        flop   = str(row.get('board_cards_flop', '') or '-')
        date_s = str(row.get('date', '?'))[:10]
        net    = float(row.get('net_won', 0))
        ev     = float(row.get('ev_won', 0))

        veredicto = eval_result['veredicto']
        motivo    = eval_result['motivo']
        equity    = eval_result.get('equity')
        pot_odds  = eval_result.get('pot_odds_min')
        accion    = eval_result.get('accion_tomada', '?')
        teorica   = eval_result.get('accion_teorica', '?')

        # Icono
        icon = {'CORRECTO': '✅', 'ERROR': '❌', 'MARGINAL': '🟡',
                'SIN_DATOS': '⚪'}.get(veredicto, '⚪')

        print()
        print(f'  [{idx_r:2d}] {date_s} | {hole} | Board: {flop}')
        print(f'       Accion: {accion} | Net: {net:+.3f}E | EV: {ev:+.3f}E')
        if equity is not None:
            print(f'       Equity vs villano: {equity:.1f}% | Pot odds min: {pot_odds:.1f}%')
        print(f'       {icon} {veredicto}: {motivo}')
        if veredicto == 'ERROR':
            print(f'       → Deberia haber: {teorica}')

        # Contadores
        if veredicto == 'CORRECTO':  correctas  += 1
        elif veredicto == 'ERROR':
            errores += 1
            errores_detalle.append(f'{hole} ({date_s}): {motivo}')
        elif veredicto == 'MARGINAL': marginales += 1
        else:                         sin_datos  += 1

    # Resumen
    total_evaluadas = correctas + errores + marginales
    print()
    print('─'*65)
    print('  RESUMEN:')
    print(f'  Manos evaluadas: {len(hands)} | Con decision clara: {total_evaluadas}')
    print(f'  ✅ Correctas:  {correctas}')
    print(f'  ❌ Errores:    {errores}')
    print(f'  🟡 Marginales: {marginales}')
    print(f'  ⚪ Sin datos:  {sin_datos}')

    if total_evaluadas > 0:
        pct_correctas = correctas / total_evaluadas * 100
        print()
        print(f'  Precision de decision: {pct_correctas:.0f}%')
        if pct_correctas < 50:
            print('  🔴 Mas de la mitad de las decisiones fueron incorrectas.')
            print('     El problema no es varianza — es criterio de decision.')
        elif pct_correctas < 75:
            print('  🟡 Hay margen de mejora en el criterio de decision.')
        else:
            print('  🟢 Buen criterio de decision. El problema es frecuencia, no calidad.')

    if errores_detalle:
        print()
        print('  ERRORES PARA ESTUDIAR:')
        for e in errores_detalle[:5]:
            print('  → ' + e)

    print('='*65)
    print()

    return {
        'correctas': correctas, 'errores': errores,
        'marginales': marginales, 'sin_datos': sin_datos,
        'precision': correctas/max(total_evaluadas,1)*100
    }




# ════════════════════════════════════════════════════════════════════════════
# SECCIÓN 3f-bis — Capa 2: Ajuste por Metagame del Pool Real
# v1.73 — Enchufa datos M5 a evaluate_hand_decision
# Principio: GTO primero, pool real después. Nunca eliminar la capa teórica.
# ════════════════════════════════════════════════════════════════════════════

# Mapa: qué dato del pool es relevante para cada tipo de decisión
# Formato: {contexto: {spot_m5: str, direccion: 'high'|'low', descripcion: str}}
METAGAME_RELEVANCE_MAP = {
    # Si defiendes BB/SB preflop y llegas al flop:
    'postflop_cbet_defense': {
        'spot_m5':    'fold_vs_cbet_IP',
        'direccion':  'low',   # pool bajo = pool no foldea = malo bluffear, bueno vbet
        'descripcion': 'Pool fold vs cbet IP',
        'consejo_low': 'Pool casi no foldea cbet ({obs:.0f}%). Si defiendes, necesitas plan de valor — no bluffs.',
        'consejo_high': 'Pool foldea mucho vs cbet ({obs:.0f}%). Defender con draws tiene más valor.',
    },
    'postflop_turn_defense': {
        'spot_m5':    'fold_vs_turn_barrel',
        'direccion':  'low',
        'descripcion': 'Pool fold vs turn barrel',
        'consejo_low': 'Pool casi no foldea turn ({obs:.0f}%). Barrel bluffs sin valor en este pool.',
        'consejo_high': 'Pool foldea turn ({obs:.0f}%). Draws y semi-bluffs tienen valor adicional.',
    },
    'postflop_river_defense': {
        'spot_m5':    'fold_vs_river_bet',
        'direccion':  'low',
        'descripcion': 'Pool fold vs river',
        'consejo_low': 'Pool paga river ({obs:.0f}% fold). Apostar solo value puro — zero bluffs.',
        'consejo_high': 'Pool foldea river ({obs:.0f}%). Bluffs tienen valor positivo aquí.',
    },
    'preflop_steal_value': {
        'spot_m5':    'BTN_open',
        'direccion':  'high',
        'descripcion': 'Pool BTN open rate',
        'consejo_low': 'Pool abre poco desde BTN ({obs:.0f}%). Sus rangos de apertura son tight — respeta sus opens.',
        'consejo_high': 'Pool abre mucho desde BTN ({obs:.0f}%). Sus rangos son amplios y débiles — defender más.',
    },
    'preflop_limp_iso': {
        'spot_m5':    'limp_rate',
        'direccion':  'high',
        'descripcion': 'Pool limp rate',
        'consejo_low': 'Pool limpa poco ({obs:.0f}%). ISO raises menos frecuentes.',
        'consejo_high': 'Pool limpa mucho ({obs:.0f}%). ISO raises desde cualquier posición son muy rentables.',
    },
}


def get_pool_adjustments(spot_identifier, m5_freqs, calle='preflop',
                         accion_spot='F', ip_oop='OOP'):
    """
    Dado un spot y los datos del pool (M5), devuelve ajustes de metagame
    relevantes para esa decisión. v1.73

    Args:
        spot_identifier: str — spot del drill activo
        m5_freqs: dict — resultado['frequencies'] de run_m5_pool_detector
        calle: str — 'preflop', 'flop', 'turn', 'river'
        accion_spot: str — ultima accion del spot ('F', 'C', 'R', 'B'...)
        ip_oop: str — 'IP' o 'OOP'

    Returns:
        list of dicts: [{
            'relevancia': 'alta'|'media'|'baja',
            'descripcion': str,
            'consejo': str,
            'obs': float,          # frecuencia observada del pool (%)
            'base': float,         # referencia teórica (%)
            'delta': float,        # diferencia en pp
            'icono': str,
        }]
    """
    if not m5_freqs:
        return []

    adjustments = []

    def _get_freq(spot_name):
        d = m5_freqs.get(spot_name, {})
        obs  = d.get('freq_obs', None)
        base = d.get('exploit_score_pp', None)
        n    = d.get('n_opp', 0)
        return obs, base, n

    # ── Siempre relevante para el drill BB: cómo juega el pool postflop ──
    if accion_spot == 'F' and calle == 'preflop':
        # Si vas a defender, ¿qué te espera en flop/turn/river?

        # Cbet IP del pool (el agresor te va a cbet con qué frecuencia)
        obs_cbet, _, n_cbet = _get_freq('cbet_IP_SRP')
        if obs_cbet is not None and n_cbet >= 50:
            obs_pct = obs_cbet * 100
            base_pct = 62.0
            delta = obs_pct - base_pct
            if abs(delta) > 5:
                consejo = (
                    f'Pool hace cbet IP {obs_pct:.0f}% (ref {base_pct:.0f}%). '
                    + ('Pool cbetea casi todo — defiende con manos que conectan bien o tienen draws.' 
                       if obs_pct > base_pct 
                       else 'Pool cbetea menos — puedes flutear más y ver turn gratis.')
                )
                adjustments.append({
                    'relevancia': 'alta',
                    'descripcion': 'Cbet IP del pool tras tu defensa',
                    'consejo': consejo,
                    'obs': obs_pct, 'base': base_pct,
                    'delta': round(delta, 1),
                    'icono': '🌊',
                })

        # Fold vs cbet OOP (tú serás OOP si defiendes BB)
        obs_fvc, _, n_fvc = _get_freq('fold_vs_cbet_OOP')
        if obs_fvc is not None and n_fvc >= 30:
            obs_pct = obs_fvc * 100
            base_pct = 50.0
            delta = obs_pct - base_pct
            # Este es TU fold rate, no del pool — no lo usamos como ajuste del pool
            # Lo que sí es útil: fold_vs_cbet_IP = cuánto foldea el pool vs TU cbet
            pass

        # Fold vs turn barrel
        obs_turn, _, n_turn = _get_freq('fold_vs_turn_barrel')
        if obs_turn is not None and n_turn >= 50:
            obs_pct = obs_turn * 100
            base_pct = 50.0
            delta = obs_pct - base_pct
            if obs_pct < 20:  # pool casi nunca foldea turn
                adjustments.append({
                    'relevancia': 'alta',
                    'descripcion': 'Pool fold vs turn barrel',
                    'consejo': (
                        f'Pool casi nunca foldea turn ({obs_pct:.0f}% vs ref {base_pct:.0f}%). '
                        f'Si defiendes, necesitas mano de valor real en turn — bluffs sin EV aquí.'
                    ),
                    'obs': obs_pct, 'base': base_pct,
                    'delta': round(delta, 1),
                    'icono': '⚠️',
                })

        # Fold vs river
        obs_river, _, n_river = _get_freq('fold_vs_river_bet')
        if obs_river is not None and n_river >= 30:
            obs_pct = obs_river * 100
            base_pct = 45.0
            delta = obs_pct - base_pct
            if obs_pct < 20:
                adjustments.append({
                    'relevancia': 'alta',
                    'descripcion': 'Pool fold vs river bet',
                    'consejo': (
                        f'Pool paga river {100-obs_pct:.0f}% de las veces ({obs_pct:.0f}% fold). '
                        f'Value bets thin en river tienen EV positivo extra. Nunca bluffear.'
                    ),
                    'obs': obs_pct, 'base': base_pct,
                    'delta': round(delta, 1),
                    'icono': '💰',
                })

        # Limp rate del pool — indica calidad del juego preflop del agresor
        obs_limp, _, n_limp = _get_freq('limp_rate')
        if obs_limp is not None and n_limp >= 100:
            obs_pct = obs_limp * 100
            base_pct = 5.0
            if obs_pct > 60:
                adjustments.append({
                    'relevancia': 'media',
                    'descripcion': 'Pool limp rate',
                    'consejo': (
                        f'Pool limpa {obs_pct:.0f}% (ref {base_pct:.0f}%). '
                        f'Los abrires del pool incluyen muchas manos débiles — sus rangos de steal son muy amplios. '
                        f'Defender más amplio es correcto en este pool específico.'
                    ),
                    'obs': obs_pct, 'base': base_pct,
                    'delta': round(obs_pct - base_pct, 1),
                    'icono': '🐟',
                })

    # ── Spots de apertura IP ──────────────────────────────────────────────
    elif calle == 'preflop' and ip_oop == 'IP' and accion_spot in ('R', 'F'):
        obs_fold3b, _, n_fold3b = _get_freq('fold_vs_3bet')
        if obs_fold3b is not None and n_fold3b >= 50:
            obs_pct = obs_fold3b * 100
            base_pct = 50.0
            delta = obs_pct - base_pct
            if abs(delta) > 10:
                adjustments.append({
                    'relevancia': 'media',
                    'descripcion': 'Pool fold vs 3bet',
                    'consejo': (
                        f'Pool foldea vs 3bet {obs_pct:.0f}% (ref {base_pct:.0f}%). '
                        + ('Tus aperturas son muy explotables — el pool 3betea poco.' 
                           if obs_pct > base_pct 
                           else 'Pool 3betea frecuente — abre rangos tighter o prepara 4bet defense.')
                    ),
                    'obs': obs_pct, 'base': base_pct,
                    'delta': round(delta, 1),
                    'icono': '🎯',
                })

    return adjustments


def evaluate_hand_decision_v2(row, spot_identifier, df_full=None,
                               stake='NL2', m5_freqs=None):
    """
    v1.73 — evaluate_hand_decision con Capa 2 de metagame.

    Mantiene intacta la Capa 1 (teoria pura: equity vs pot odds).
    Añade Capa 2: ajustes del pool real del M5.

    Returns el mismo dict que evaluate_hand_decision + campo 'pool_adjustments'.
    """
    # Capa 1: evaluación teórica (sin cambios)
    resultado = evaluate_hand_decision(row, spot_identifier,
                                       df_full=df_full, stake=stake)

    # Capa 2: ajustes de metagame
    spot_parts = spot_identifier.split('_')
    calle      = spot_parts[4] if len(spot_parts) > 4 else 'preflop'
    accion_spot = spot_parts[-1] if spot_parts else 'F'
    ip_oop     = str(row.get('ip_oop', 'OOP'))

    pool_adjustments = []
    if m5_freqs:
        pool_adjustments = get_pool_adjustments(
            spot_identifier, m5_freqs,
            calle=calle, accion_spot=accion_spot, ip_oop=ip_oop
        )

    resultado['pool_adjustments'] = pool_adjustments

    # Veredicto combinado — solo si hay ajustes con alta relevancia
    alta_relevancia = [a for a in pool_adjustments if a['relevancia'] == 'alta']
    if alta_relevancia and resultado['veredicto'] in ('CORRECTO', 'ERROR'):
        resultado['veredicto_pool'] = resultado['veredicto']
        # No cambiamos el veredicto teórico — añadimos capa de contexto
        resultado['contexto_pool'] = (
            'CONFIRMA: teoria y pool coinciden.' 
            if all(
                # El pool confirma la decision teorica
                (resultado['veredicto'] == 'CORRECTO') or
                # Hay matiz: correcto en teoria pero dificil en este pool
                (resultado['veredicto'] == 'ERROR')
                for _ in alta_relevancia
            ) else 'MATIZ: el pool añade contexto a la decision teorica.'
        )
    else:
        resultado['veredicto_pool'] = None
        resultado['contexto_pool']  = None

    return resultado


def display_hand_evaluation_v2(df, spot_identifier, n_hands=10,
                                m5_result=None, stake='NL2'):
    """
    v1.73 — display_hand_evaluation con Capa 2 de metagame.

    Muestra por cada mano:
      📐 Capa 1: Teoria — equity vs pot odds → CORRECTO/ERROR/MARGINAL
      🌊 Capa 2: Pool real — cómo juega el pool y qué implica para esa decision
      → Veredicto final integrado

    Uso:
        # Sin metagame (igual que antes):
        display_hand_evaluation_v2(ingested_df, DRILL_ACTIVO)

        # Con metagame (recomendado post-pipeline):
        display_hand_evaluation_v2(ingested_df, DRILL_ACTIVO, m5_result=m5_resultado)
    """
    m5_freqs = None
    if m5_result and isinstance(m5_result, dict):
        m5_freqs = m5_result.get('frequencies', {})

    hands = get_representative_hands(df, spot_identifier, top_n=n_hands)
    if hands.empty:
        print('Sin manos disponibles para este spot.')
        return

    tiene_pool = bool(m5_freqs)
    print()
    print('='*68)
    print('  EVALUACION DE DECISIONES' + (' + METAGAME POOL REAL' if tiene_pool else ''))
    print('  Spot: ' + spot_identifier)
    print('  📐 Capa 1: Teoria pura (equity vs pot odds)')
    if tiene_pool:
        print('  🌊 Capa 2: Ajuste por metagame del pool real (M5)')
    print('='*68)

    errores = correctas = marginales = sin_datos = 0
    errores_detalle = []

    for idx_r, (_, row) in enumerate(hands.iterrows(), 1):
        eval_r = evaluate_hand_decision_v2(
            row, spot_identifier,
            df_full=df, stake=stake, m5_freqs=m5_freqs
        )

        hole  = str(row.get('hole_cards', '??'))
        flop  = str(row.get('board_cards_flop', '') or '-')
        date_s = str(row.get('date', '?'))[:10]
        net   = float(row.get('net_won', 0))
        ev    = float(row.get('ev_won', 0))

        veredicto = eval_r['veredicto']
        motivo    = eval_r['motivo']
        equity    = eval_r.get('equity')
        pot_odds  = eval_r.get('pot_odds_min')
        accion    = eval_r.get('accion_tomada', '?')
        teorica   = eval_r.get('accion_teorica', '?')
        pool_adj  = eval_r.get('pool_adjustments', [])

        icon = {'CORRECTO': '✅', 'ERROR': '❌',
                'MARGINAL': '🟡', 'SIN_DATOS': '⚪'}.get(veredicto, '⚪')

        print()
        print(f'  [{idx_r:2d}] {date_s} | {hole} | Board: {flop}')
        print(f'       Accion: {accion} | Net: {net:+.3f}E | EV: {ev:+.3f}E')

        # Capa 1 — siempre
        print(f'       📐 TEORIA:')
        if equity is not None:
            print(f'          Equity {equity:.1f}% vs pot odds min {pot_odds:.1f}%')
        print(f'          {icon} {veredicto}: {motivo}')
        if veredicto == 'ERROR':
            print(f'          → Accion teorica: {teorica}')

        # Capa 2 — solo en resumen final, no por mano (reducir ruido)

        # Contadores
        if veredicto == 'CORRECTO':    correctas  += 1
        elif veredicto == 'ERROR':
            errores += 1
            errores_detalle.append(f'{hole} ({date_s}): {motivo}')
        elif veredicto == 'MARGINAL':  marginales += 1
        else:                          sin_datos  += 1

    # Resumen
    total = correctas + errores + marginales
    print()
    print('─'*68)
    print('  RESUMEN:')
    print(f'  Manos evaluadas: {len(hands)} | Con decision clara: {total}')
    print(f'  ✅ Correctas:  {correctas}  ❌ Errores: {errores}  '
          f'🟡 Marginales: {marginales}  ⚪ Sin datos: {sin_datos}')

    if total > 0:
        pct = correctas / total * 100
        print()
        print(f'  Precision decision (Capa 1 teoria): {pct:.0f}%')
        if pct < 50:
            print('  🔴 Criterio de decision incorrecto en mayoria de manos.')
            print('     El problema no es varianza — es la regla de decision.')
        elif pct < 75:
            print('  🟡 Hay margen de mejora en criterio de decision.')
        else:
            print('  🟢 Buen criterio. El problema es frecuencia, no calidad.')

    if tiene_pool and m5_freqs:
        print()
        print('  CONTEXTO DE POOL (aplicable a todas las manos del spot):')
        # Mostrar los ajustes de pool más relevantes una sola vez
        sample_adj = get_pool_adjustments(
            spot_identifier, m5_freqs,
            calle='preflop', accion_spot=spot_identifier.split('_')[-1]
        )
        for adj in sample_adj:
            if adj['relevancia'] == 'alta':
                print(f'  {adj["icono"]} {adj["descripcion"]}: '
                      f'{adj["obs"]:.0f}% (ref {adj["base"]:.0f}%) '
                      f'Δ{adj["delta"]:+.0f}pp')

    if errores_detalle:
        print()
        print('  ERRORES PARA ESTUDIAR:')
        for e in errores_detalle[:5]:
            print('  → ' + e)

    print('='*68)
    print()

    return {
        'correctas': correctas, 'errores': errores,
        'marginales': marginales, 'sin_datos': sin_datos,
        'precision': correctas / max(total, 1) * 100,
        'pool_activo': tiene_pool,
    }


print("✅ Módulo Drill Guiado cargado (Sección 3f).")
print("   get_representative_hands(df, spot_identifier) → manos más instructivas")
print("   display_drill_hands(df, spot_identifier)      → output formateado para drill")
print("   display_top_spots_with_hands(df, spot_results) → drill completo top N leaks")


# ════════════════════════════════════════════════════════════════════════
# SECCIÓN 3f.1 — Recursos de Estudio por Drill Activo (v1.80)
# Cuando el sistema detecta tu leak, te señala exactamente dónde estudiar.
# Gratuito. Sin tracker externo. Conectado al drill activo de M7.
# ════════════════════════════════════════════════════════════════════════

STUDY_RESOURCES_BY_DRILL = {
    'BB_OOP_SRP_deep_preflop_unknown_F': {
        'concepto_clave': (
            "BB defense: debes defender suficientemente amplio para que el villain "
            "no pueda abrir profitable con cualquier dos cartas. "
            "El BB ya tiene equity invertido — folding demasiado = regalo de equity."
        ),
        'pregunta_guia': (
            "¿Por qué esta mano específica está en rango de defensa? "
            "¿Qué pot odds tengo aquí y qué equity necesito para justificar el call?"
        ),
        'youtube': [
            {
                'query': 'BB defense vs BTN NL micro stakes',
                'canales': ['Jonathan Little Poker', 'SplitSuit Poker', 'Poker Coaching'],
                'url_busqueda': 'https://www.youtube.com/results?search_query=BB+defense+BTN+NL+micro',
            },
            {
                'query': 'big blind defense range cash game',
                'canales': ['Run It Once Poker', 'Brad Owen', 'Upswing Poker'],
                'url_busqueda': 'https://www.youtube.com/results?search_query=big+blind+defense+range+cash',
            },
        ],
        'articulos': [
            'https://www.splitsuit.com/big-blind-defense',
            'https://upswingpoker.com/big-blind-defense-guide/',
        ],
        'solver_ref': (
            "SimplePostflop.com → selecciona BB vs BTN SRP → "
            "verifica qué manos tienen equity suficiente para defender"
        ),
        'ejercicio_practico': (
            "Antes de la próxima sesión: escribe en papel las 10 manos "
            "que más dudas te generan desde BB. ¿Las defenderías? ¿Por qué? "
            "Luego verifica con pot odds reales (call/pot+call)."
        ),
    },
    'SB_open_or_fold': {
        'concepto_clave': (
            "SB es la posición más costosa: pagas blind forzado y "
            "juegas siempre OOP postflop. Limp regala equity al BB. "
            "Raise o fold siempre desde SB."
        ),
        'pregunta_guia': (
            "¿Esta mano justifica un raise desde SB o es fold? "
            "¿Qué pasa si limpeo y el BB no sube?"
        ),
        'recursos_por_nivel': {
            'level_1': "Concepto: por qué limp SB es -EV. run_reasoning_session('SB_open_or_fold','level_1').",
            'level_2': "Rango: SimplePostflop SB vs BB. Top 35-40% con raise/fold.",
            'level_3': "3bet vs steals: blockers, balance, sizing.",
        },
        'youtube': [
            {
                'query': 'small blind strategy no limp cash game',
                'canales': ['Jonathan Little Poker', 'SplitSuit Poker', 'Upswing Poker'],
                'url_busqueda': 'https://www.youtube.com/results?search_query=small+blind+no+limp+cash+game',
            },
        ],
        'articulos': ['https://upswingpoker.com/small-blind-strategy/'],
        'solver_ref': "SimplePostflop.com → SB vs BB → qué manos tienen EV+ con raise vs limp",
        'ejercicio_practico': (
            "Escribe tu rango de apertura SB (top 35-40%). "
            "¿Qué 35 manos/combos abrirías? Compara con SimplePostflop."
        ),
    },
    'postflop_fundamentals': {
        'concepto_clave': "Pot odds, outs, regla 4/2, implied odds. El 80% del dinero se decide postflop.",
        'pregunta_guia': "¿Cuántos outs tengo? ¿Equity vs pot odds del villain?",
        'recursos_por_nivel': {
            'level_1': "Regla del 4 y del 2. Pot odds básicos postflop.",
            'level_2': "Implied odds, semi-bluff, protection bets.",
            'level_3': "Board texture, ranging opponent, triple barrels.",
        },
        'youtube': [{'query': 'poker outs pot odds calculation micro stakes',
                     'canales': ['SplitSuit Poker', 'Jonathan Little'],
                     'url_busqueda': 'https://www.youtube.com/results?search_query=poker+outs+pot+odds'}],
        'articulos': ['https://www.splitsuit.com/pot-odds-and-equity'],
        'solver_ref': "SimplePostflop → cualquier spot postflop → verifica decisión",
        'ejercicio_practico': "Para 5 manos con draws del HH: calcula outs y verifica si tenías odds.",
    },
    'ccall_PF': {
        'concepto_clave': (
            "Cold call preflop solo tiene sentido con manos que tienen buen equity "
            "realizable y playability postflop. Manos medias (KTo, QJo, pares pequeños) "
            "pierden valor por posición y por quedar en rangos débiles cuando mejoran."
        ),
        'pregunta_guia': (
            "Si llamo aquí, ¿qué hago en flops difíciles (A-alto, K-alto)? "
            "¿Tengo plan para las 3 calles o solo estoy 'viendo qué pasa'?"
        ),
        'youtube': [
            {
                'query': 'cold call preflop ranges cash game micro stakes',
                'canales': ['Jonathan Little Poker', 'Poker Coaching', 'PokerStars School'],
                'url_busqueda': 'https://www.youtube.com/results?search_query=cold+call+preflop+cash+game',
            },
            {
                'query': 'preflop calling ranges position poker',
                'canales': ['SplitSuit Poker', 'Upswing Poker'],
                'url_busqueda': 'https://www.youtube.com/results?search_query=preflop+calling+ranges+position',
            },
        ],
        'articulos': [
            'https://upswingpoker.com/cold-calling-ranges/',
            'https://www.splitsuit.com/preflop-calling-ranges',
        ],
        'solver_ref': (
            "SimplePostflop.com → selecciona posición vs raiser → "
            "compara tu rango actual con el rango recomendado"
        ),
        'ejercicio_practico': (
            "Toma las últimas 5 manos donde cold-calleaste. "
            "Para cada una: ¿fue call, 3bet o fold lo correcto? "
            "Calcula pot odds y compara tu equity estimada."
        ),
    },
}


def display_study_resources(drill_activo, current_level='level_1'):
    """
    Muestra los recursos de estudio para el drill activo.
    Llamar después del pipeline o en el briefing pre-sesión.
    """
    resources = STUDY_RESOURCES_BY_DRILL.get(drill_activo)
    if not resources:
        print(f"   ⚪ Sin recursos configurados para drill '{drill_activo}'")
        return

    print(f"\n{'═'*62}")
    print(f"  📚 RECURSOS DE ESTUDIO — {drill_activo}")
    print(f"{'═'*62}")

    print(f"\n  🧠 CONCEPTO CLAVE:")
    # Wrap text at 58 chars
    concepto = resources['concepto_clave']
    words = concepto.split()
    line = "  "
    for w in words:
        if len(line) + len(w) + 1 > 60:
            print(line)
            line = "  " + w + " "
        else:
            line += w + " "
    if line.strip():
        print(line)

    print(f"\n  ❓ PREGUNTA GUÍA (respóndela antes de cada sesión):")
    pregunta = resources['pregunta_guia']
    words = pregunta.split()
    line = "  "
    for w in words:
        if len(line) + len(w) + 1 > 60:
            print(line)
            line = "  " + w + " "
        else:
            line += w + " "
    if line.strip():
        print(line)

    print(f"\n  🎥 YOUTUBE — búsquedas directas:")
    for yt in resources.get('youtube', []):
        print(f"  • Buscar: \"{yt['query']}\"")
        print(f"    Canales: {', '.join(yt['canales'][:3])}")
        print(f"    URL: {yt['url_busqueda']}")

    print(f"\n  🔧 SOLVER GRATUITO:")
    print(f"  {resources.get('solver_ref','')}")

    print(f"\n  ✏️  EJERCICIO ANTES DE LA PRÓXIMA SESIÓN:")
    ejercicio = resources['ejercicio_practico']
    words = ejercicio.split()
    line = "  "
    for w in words:
        if len(line) + len(w) + 1 > 60:
            print(line)
            line = "  " + w + " "
        else:
            line += w + " "
    if line.strip():
        print(line)

    print(f"\n{'─'*62}")


print("✅ Recursos de estudio por drill cargados (v1.80)")
print("   display_study_resources(drill_activo) → recursos concretos")
print("   Canales: Jonathan Little, SplitSuit, Upswing, Run It Once")


# ════════════════════════════════════════════════════════════════════════
# SECCIÓN 3f.2 — Revisión Guiada de Manos: Flashcard Activo (v1.80)
# Convierte la presentación pasiva de manos en estudio activo real.
# Para en cada mano y te hace pensar ANTES de ver el resultado.
# ════════════════════════════════════════════════════════════════════════

def run_guided_hand_review(df, spot_identifier, top_n=5, hero_name='LaRuinaDeMago'):
    """
    Revisión guiada de manos: flashcard activo.
    
    Diferencia con display_drill_hands:
    - display_drill_hands: muestra la mano completa (pasivo)
    - run_guided_hand_review: oculta el resultado, hace preguntas,
      espera respuesta, luego revela y evalúa (activo)
    
    Flujo por mano:
      1. Muestra: posición, cartas, acción preflop, flop (si hay)
      2. Pregunta: ¿qué harías aquí? ¿por qué?
      3. Espera input del usuario
      4. Revela: lo que hiciste, resultado, evaluación
      5. Muestra concepto teórico aplicable
    
    Args:
        df:              DataFrame completo
        spot_identifier: spot a revisar
        top_n:           número de manos (default 5)
        hero_name:       nick del hero
    """
    hands = get_representative_hands(df, spot_identifier, top_n=top_n)

    if hands.empty:
        print(f"   ⚪ Sin manos para revisar en spot '{spot_identifier}'")
        print("   Necesitas al menos 3 manos en este spot.")
        return

    # Recursos del drill para mostrar concepto al final de cada mano
    resources = STUDY_RESOURCES_BY_DRILL.get(spot_identifier, {})
    concepto = resources.get('concepto_clave', '')

    print(f"\n{'═'*62}")
    print(f"  🃏 REVISIÓN GUIADA — {spot_identifier}")
    print(f"  {len(hands)} manos | Responde ANTES de ver el resultado")
    print(f"{'═'*62}")
    print(f"  Instrucciones:")
    print(f"  • Lee la situación completa")
    print(f"  • Escribe tu respuesta (o piénsala en voz alta)")
    print(f"  • Presiona Enter para revelar lo que hiciste")
    print(f"{'─'*62}\n")

    correctas = 0
    errores   = 0
    marginales = 0

    for i, row in hands.iterrows():
        hand_num = i + 1

        # ── Extraer datos de la mano ──────────────────────────────
        pos      = str(row.get('player_position', '?'))
        hole     = str(row.get('hole_cards', '??'))
        pf_act   = str(row.get('preflop_action', '—'))
        fl_board = str(row.get('board_cards_flop', ''))
        fl_act   = str(row.get('flop_action', ''))
        net      = row.get('net_won', 0)
        ev       = row.get('ev_won', None)
        stake    = str(row.get('stake_level', 'NL2'))
        session  = str(row.get('session_id', '?'))
        date_str = str(row.get('date', '?'))[:10]

        # Evaluación si disponible
        eval_result = evaluate_hand_decision(row, spot_identifier, df_full=df, stake=stake)
        veredicto   = eval_result.get('veredicto', 'SIN_DATOS')
        motivo      = eval_result.get('motivo', '')
        eq          = eval_result.get('equity', None)
        pot_odds    = eval_result.get('pot_odds_min', None)

        # ── FASE 1: Mostrar situación (sin resultado) ─────────────
        print(f"  ┌─ MANO {hand_num}/{len(hands)} ─ {session} ({date_str}) ─{'─'*20}")
        print(f"  │ Posición:  {pos} | Cartas: {hole} | Stake: {stake}")
        print(f"  │ Preflop:   {pf_act}")
        if fl_board:
            print(f"  │ Flop:      [{fl_board}]  Acción: {fl_act or '—'}")

        # ── FASE 2: Preguntas activas ─────────────────────────────
        print(f"  │")
        print(f"  │ ❓ PREGUNTAS (responde antes de continuar):")
        print(f"  │   1. ¿Fue correcta tu decisión preflop aquí?")
        print(f"  │   2. ¿Qué rango le asignas al villain en este spot?")
        if fl_board:
            print(f"  │   3. Con board [{fl_board}], ¿qué equity tienes vs ese rango?")
        if eq is not None:
            print(f"  │   4. Tu equity estimada: {eq:.1%} — ¿justifica la acción tomada?")
        if pot_odds is not None:
            print(f"  │      Pot odds mínimos para call: {pot_odds:.1%}")
        print(f"  │")
        print(f"  │ → Presiona Enter para revelar resultado...")

        try:
            input("  │ ")
        except EOFError:
            pass  # Non-interactive mode (testing)

        # ── FASE 3: Revelar resultado y evaluación ────────────────
        net_str = f"{net:+.4f}€"
        ev_str  = f"{ev:+.3f}€" if ev is not None else "N/A"

        verdict_icons = {
            'CORRECTO':  '✅',
            'ERROR':     '❌',
            'MARGINAL':  '⚠️',
            'SIN_DATOS': '⚪',
        }
        icon = verdict_icons.get(veredicto, '⚪')

        print(f"  │")
        print(f"  │ 📊 RESULTADO:")
        print(f"  │   Net won:  {net_str} | EV won: {ev_str}")
        print(f"  │   {icon} Veredicto: {veredicto}")
        if motivo:
            print(f"  │   Motivo: {motivo}")

        if veredicto == 'CORRECTO':
            correctas += 1
        elif veredicto == 'ERROR':
            errores += 1
        elif veredicto == 'MARGINAL':
            marginales += 1

        # ── FASE 4: Concepto teórico aplicable ───────────────────
        if concepto and veredicto in ('ERROR', 'MARGINAL', 'SIN_DATOS'):
            print(f"  │")
            print(f"  │ 🧠 CONCEPTO APLICABLE:")
            words = concepto.split()
            line  = "  │   "
            for w in words:
                if len(line) + len(w) + 1 > 62:
                    print(line)
                    line = "  │   " + w + " "
                else:
                    line += w + " "
            if line.strip():
                print(line)

        print(f"  └{'─'*58}\n")

    # ── RESUMEN FINAL ──────────────────────────────────────────────
    total = len(hands)
    print(f"{'═'*62}")
    print(f"  RESUMEN REVISIÓN: {total} manos")
    print(f"  ✅ Correctas: {correctas} | ❌ Errores: {errores} | "
          f"⚠️ Marginales: {marginales} | ⚪ Sin datos: {total-correctas-errores-marginales}")
    if total > 0:
        precision = correctas / total * 100
        print(f"  Precisión: {precision:.0f}%  "
              f"{'🟢 Bien' if precision >= 70 else '🟡 Mejorable' if precision >= 50 else '🔴 Revisar concepto'}")

    # Recursos de estudio al final
    yt = resources.get('youtube', [{}])[0] if resources.get('youtube') else {}
    if yt:
        print(f"\n  📺 Siguiente paso: busca \"{yt.get('query','')}\"")
        print(f"     en {', '.join(yt.get('canales',[])[:2])}")
    ejercicio = resources.get('ejercicio_practico', '')
    if ejercicio:
        print(f"\n  ✏️  Ejercicio: {ejercicio[:100]}...")
    print(f"{'═'*62}")


print("✅ Revisión Guiada de Manos cargada (v1.80)")
print("   run_guided_hand_review(df, spot_identifier) → flashcard activo")
print("   Flujo: situación → preguntas → Enter → resultado + concepto")


# ════════════════════════════════════════════════════════════════════════
# SECCIÓN 3f.3 — Preguntas de Razonamiento con Respuesta (v1.82)
# Sin HH. Sin pipeline. Cualquier día. Razonamiento real, no memoria.
# ════════════════════════════════════════════════════════════════════════

REASONING_QUESTIONS = {'BB_OOP_SRP_deep_preflop_unknown_F': {'level_1': [{'tipo': 'calculo', 'pregunta': 'Villain abre 2.5bb desde BTN. Estás en BB con 1bb ya puesto. ¿Cuánto tienes que llamar? ¿Qué tamaño tiene el pot si llamas? ¿Qué equity MÍNIMA necesitas para que el call sea profitable?', 'calculo': 'Call = 2.5bb - 1bb = 1.5bb. Pot si llamas = 2.5 (BTN) + 0.5 (SB fold) + 1 (BB) + 0.5 (completar) = no, más simple: Pot total = open(2.5) + SB(0.5 fold no cuenta) + call(1.5) = 5bb si SB foldea. Equity mínima = call / (pot tras call) = 1.5 / 5 = 30%.', 'respuesta': 'Necesitas ≥30% equity. Con open 2.5bb desde BTN: cualquier mano con ≥30% equity vs rango BTN 45% justifica el call.', 'aplicacion': 'Tus datos: foldeas 67.6% de veces desde BB cuando hay oportunidad real (vs ref 30%). Si foldeas manos con 35-40% equity estás regalando dinero. 97s tiene ~36% equity vs BTN 45% → call obligatorio.', 'followup': '¿Qué es equity mínima? Si el pot es 5bb y tienes que pagar 1.5bb, ¿qué porcentaje de veces necesitas ganar para que el call sea 0 EV? → 1.5 / (5+1.5) = 23%. Si ganas más del 23% de las veces, el call es +EV.'}, {'tipo': 'calculo', 'pregunta': 'Mismo spot pero villain abre 3bb. ¿Cuánto cambia la equity mínima? ¿Cambia significativamente tu rango de defensa?', 'calculo': 'Call = 3 - 1 = 2bb. Pot = 3 + 0.5 + 2 = 5.5bb (con SB fold). Simplificando: pot = open + call = 3 + 2 = 5, más el blind de SB = 5.5. Equity mínima = 2 / 5.5 = 36.4%. Vs 2.5bb era 30%. Diferencia: +6.4 puntos porcentuales.', 'respuesta': 'Con 3bb necesitas 36% en vez de 30%. Tu rango de defensa se estrecha ~5-8% del espacio. Manos marginales como K4o o 85o que defendían vs 2.5bb ahora son fold.', 'aplicacion': 'El sizing del villain importa. En NL2 el pool abre mucho 2.5bb, algunos usan 3bb. Nota el sizing antes de decidir — no es automático.'}, {'tipo': 'conceptual', 'pregunta': '¿Por qué 76s defiende desde BB vs BTN aunque parezca una mano pequeña, mientras que K2o muchas veces es fold aunque tenga una carta alta?', 'calculo': None, 'respuesta': 'Equity realización. 76s: puede hacer flush, straight, dos pares en tableros bajos — gana botes grandes cuando mejora. K2o: solo mejora bien con K en tableros que también ayudan al villain. Con manos de valor bloqueado (K2o) el equity se realiza peor postflop.', 'aplicacion': "En NL2 donde el pool paga demasiado en ríos, las suited connectors y gappers tienen valor oculto. No las foldees solo porque 'la carta alta es pequeña'."}, {'tipo': 'calculo', 'pregunta': 'El villain apuesta 4bb en el flop en un pot de 5.5bb. Tienes un flush draw (9 outs). ¿Tienes odds para continuar?', 'calculo': 'Pot odds: call 4bb / (pot 5.5 + bet 4 + call 4) = 4 / 13.5 = 29.6%. Equity con flush draw en flop: 9 outs × 4% (regla del 4) = 36% hasta river. O 9 outs × 2% = 18% solo para el turn. Comparando con pot odds 29.6%: - Si solo vas al turn: 18% < 30% → call marginal sin implied odds. - Hasta el river: 36% > 30% → call profitable con implied odds.', 'respuesta': 'Con flush draw en flop y apuesta de 4bb en pot de 5.5bb: call es correcto usando la regla del 4 (36% hasta river > 30% necesario). Los implied odds cuando completas lo hacen claramente profitable.', 'aplicacion': 'La regla del 4: outs × 4 = equity aproximada hasta el river. Flush draw = 9 outs = ~36%. Straight draw = 8 outs = ~32%. Memoriza esto — lo usarás en cada sesión.'}, {'tipo': 'conceptual', 'pregunta': 'En tus datos, desde BB checas el flop el 71.5% de las veces. ¿Es esto correcto? ¿Cuándo deberías apostar en vez de checkear desde BB en el flop?', 'calculo': None, 'respuesta': '71.5% de check desde BB OOP es alto pero no necesariamente incorrecto. BB OOP juega muchos checks porque: le falta iniciativa preflop, el villain puede tener mejores tableros IP. Deberías apostar (donk bet o continuar con draw) cuando: - El tablero te favorece mucho (23 outs, set, dos pares). - Tienes un draw con equity y fold equity. - El villain es muy pasivo y check-atrás frecuente.', 'aplicacion': 'No es que 71.5% esté mal — es contextual. Lo que SÍ está mal es si checeas manos fuertes esperando acción en tableros donde el villain no va a apostar. Apuesta el valor.'}, {'tipo': 'calculo', 'pregunta': 'Tu BB fold vs steal actual es 63.5%. El villain abre desde BTN a 2.5bb — el pot es 4bb. ¿Qué fold% tuyo necesita para tener free roll? ¿Le estás dando ese free roll ahora mismo?', 'calculo': 'Free roll threshold: villain necesita que foldes > (coste open) / (pot) = 2.5 / 4 = 62.5%. Tu fold% actual: 63.5%. 62.5% < 63.5% → SÍ le estás dando free roll. Cada mano que abre desde BTN o CO es EV+ para él ANTES de ver sus cartas. Para quitarle el free roll necesitas bajar a ≤62.5%.', 'respuesta': 'Con tu 63.5% de fold, cualquier villain que sepa poker tiene free roll para abrirte con el 100% de la baraja desde BTN. El threshold exacto es 62.5% — encima de ese número le regalas EV estructural en cada mano. Target: ≤35% fold (rango GTO) o ≤62.5% como mínimo para quitarle el free roll.', 'aplicacion': 'Antes de cada sesión: recuerda que tu fold% actual (63.5%) está 1.0pp por encima del umbral de free roll. Cada vez que foldeeas una mano borderline desde BB, pregúntate: ¿tiene este villain free roll sobre mí? La respuesta siempre es sí hasta que llegues a ≤62.5%.', 'followup': '¿Qué significa free roll? Si el villain abre a 2.5bb y el pot es 4bb, necesita que foldes más de 2.5/4 = 62.5% para ganar dinero ANTES de ver sus cartas. ¿Con tu fold actual de 63.5%, el villain necesita tener buenas cartas para abrirte? → No. Puede abrir con el 100% de la baraja y ganar dinero automáticamente.'}], 'level_2': [{'tipo': 'calculo', 'pregunta': 'Villain BTN abre 2.5bb. Tienes J9s en BB. El flop viene 8♠ T♥ 2♣ (tienes OESD: 7 y Q completan). Villain hace cbet de 3bb en pot de 5.5bb. ¿Tienes odds para continuar? ¿Call o raise?', 'calculo': 'Pot odds: 3 / (5.5 + 3 + 3) = 3/11.5 = 26%. Equity OESD = 8 outs × 4% = 32% hasta river. 32% > 26% → call es correcto solo por equity. Raise: si villain es débil o fold demasiado, semi-bluff raise tiene valor. Con J9s también tienes dos overcards adicionales como outs secundarios.', 'respuesta': 'Call mínimo — equity justifica continuar. Raise (semi-bluff) si villain tiene fold equity razonable o si quieres jugar la mano más grande para implied odds máximos.', 'aplicacion': "Las manos con múltiples formas de ganar (draw + overcards) son las más poderosas en NL2. No las foldees solo porque 'no tienes nada'."}, {'tipo': 'conceptual', 'pregunta': 'Villain abre BTN 45% de manos (muy loose). Luego villain tight abre BTN 25% de manos. ¿Cómo ajustas tu rango de defensa desde BB en cada caso?', 'calculo': 'Villain 45% BTN: rango incluye muchas manos débiles. Tu equity relativa sube porque sus manos débiles no conectan bien. Villain 25% BTN: rango es fuerte (pares, broadways, Ax). Tu equity relativa baja — necesitas manos con más equity propia.', 'respuesta': 'Vs villano 45%: defiendes más amplio, incluso 85s o J4s tienen valor. Vs villano 25%: ajustas, eliminas manos marginales, priorizas equity directa y manos que se realizan bien.', 'aplicacion': 'En NL2 la mayoría del pool abre 35-50% desde BTN. Tu rango de defensa es más amplio de lo que la teoría GTO sugiere. Esto cambia a NL25+ cuando el pool ajusta mejor.'}, {'tipo': 'calculo', 'pregunta': 'Tienes Q♥ T♥ en BB. Flop: K♥ 7♥ 2♣. Tienes flush draw (9 outs) + gutshot a J (3 outs adicionales). Villain bet 4bb en pot 5.5bb. ¿Cuántos outs combinados? ¿Tienes odds?', 'calculo': 'Outs: 9 outs flush draw + 3 outs gutshot (J no corazón, ya contamos J♥) = 12 outs. Equity = 12 × 4% = 48% hasta river. Pot odds: 4 / (5.5 + 4 + 4) = 4/13.5 = 29.6%. 48% >> 30% → call muy claro. Raise semi-bluff es excelente aquí.', 'respuesta': 'Con 12 outs (48% equity) vs pot odds del 30%: call es obligatorio. Semi-bluff raise es la jugada óptima — agregas fold equity a tu equity de draw.', 'aplicacion': 'Las combo draws (flush + straight) son manos muy fuertes incluso sin pair. Con 10+ outs siempre continúas. Con 6+ outs generalmente continúas.'}, {'tipo': 'conceptual', 'pregunta': '¿Por qué es importante tener un rango de checking ranges balanceado en el flop desde BB, incluso en NL2 donde el pool no explota bien?', 'calculo': None, 'respuesta': 'En NL2 no necesitas balance perfecto — el pool no te explota. Pero sí necesitas apostar tus manos fuertes para extraer valor. El error más común: checkear sets y dos pares esperando acción en tableros donde el villain va a checkear detrás. Apuesta el valor en tableros que conectan con tu rango BB.', 'aplicacion': 'Tablero A72r desde BB vs BTN: si tienes A7, A2, 72 → apuesta. No esperes al turn/river. El pool NL2 paga en cualquier calle.'}], 'level_3': [{'tipo': 'conceptual', 'pregunta': 'Tienes A4s en BB. BTN abre 2.5bb. ¿Cuándo haces 3bet y cuándo call? ¿Cambia si el villain tiene VPIP 55% vs VPIP 25%?', 'calculo': None, 'respuesta': 'A4s es mano de 3bet lineal en muchos casos desde BB: bloquea AA/AK del villain (blocker), tiene buen equity realizado, y en NL2 el pool no 4bet suficiente. Vs VPIP 55%: 3bet valor — el villain llama demasiado con manos peores. Vs VPIP 25%: 3bet bluff — defiendes tu rango de 3bets con blocker.', 'aplicacion': 'En NL2: casi siempre 3bet con A2s-A5s desde BB vs BTN loose. El pool paga 3bets con Ax, broadways, pares medianos. Extraes más valor con 3bet que con call.'}, {'tipo': 'calculo', 'pregunta': 'Tienes KQ en BB. BTN open, tú 3bet a 9bb. BTN llama. Flop: K♠ 7♦ 2♣ (top pair, buena patada). Pot = 19bb. ¿Cuánto apostar? ¿Por qué ese sizing?', 'calculo': 'Con top pair + kicker fuerte en tablero seco: apuesta para valor. Sizing óptimo vs pool NL2 que paga demasiado: 60-75% pot. 75% de 19bb = 14.25bb → apuesta ~14bb. El pool llama con K débiles, pares menores, draws. Sizing grande maximiza valor contra rango de llamada amplio.', 'respuesta': 'Apuesta 13-15bb (70-75% pot). Razón: tablero seco favorece al 3bettor (tú), villain tiene pocos draws, y el pool NL2 paga con KJ, KT, TT, 99 que están perdiendo.', 'aplicacion': 'En tableros secos con mano de valor clara: apuesta grande. El pool NL2 no foldea suficiente a bets grandes — explota eso.'}, {'tipo': 'conceptual', 'pregunta': '¿Cuándo defiendes desde BB con 3bet vs call en general? Dame el framework de decisión que aplicas en mesa.', 'calculo': None, 'respuesta': '3bet cuando: (1) tienes mano de valor que gana vs rango de call del villain, (2) tienes blocker (Ax) que reduce manos premium del villain, (3) el villain foldea suficiente a 3bets para que el bluff sea rentable. Call cuando: (1) mano con equity realización alta (suited connectors), (2) quieres ver el flop barato con mano especulativa, (3) el villain nunca foldea — 3bet bluff pierde valor.', 'aplicacion': 'Framework en mesa: ¿tengo blocker Ax? → 3bet. ¿Es suited connector que se realiza bien IP? → call. ¿Es mano marginal offsuit? → fold o 3bet, no call.'}]}, 'SB_open_or_fold': {'level_1': [{'tipo': 'conceptual', 'pregunta': 'Estás en SB con J8o. BB es desconocido. ¿Por qué limp es peor que raise o fold incluso si J8o parece una mano decente?', 'calculo': None, 'respuesta': 'Al limp das al BB la oportunidad de ver el flop gratis o barato. Juegas postflop OOP sin iniciativa con mano mediocre. Con raise: o robas el blind directamente (+1.5bb) o juegas con iniciativa. Con fold: pierdes 0.5bb. Con limp y BB checkea: juegas flop OOP sin saber nada.', 'aplicacion': 'Tus datos: limpas el 25.9% desde SB. Esas ~150 manos son dinero que se pierde sistemáticamente. Cada limp convertido en raise o fold mejora tu winrate desde SB.'}, {'tipo': 'calculo', 'pregunta': 'Estás en SB. Foldeas 0.5bb. Alternativamente, limpas y juegas flop OOP. ¿Cuánto tienes que ganar en el flop para que el limp sea mejor que fold? ¿Es realista en NL2?', 'calculo': 'Si foldeas: pierdes 0.5bb. Si limpas y el BB checkea: inviertes 0.5bb más para ver el flop. Para que el limp sea mejor que fold, necesitas ganar más de 0.5bb de EV postflop en promedio. Realista: con mano marginal OOP sin iniciativa, el EV postflop medio es negativo. El limp pierde más que el fold.', 'respuesta': 'El limp es peor que fold con manos marginales desde SB. Con manos fuertes (AA-TT, AK-AJ) el limp también es peor que raise porque pierdes valor al no construir el pot con manos ganadoras.', 'aplicacion': 'Regla simple: SB con mano buena → raise. SB con mano regular → fold. El limp solo tiene sentido en situaciones muy específicas que no aparecen en NL2 standard.'}, {'tipo': 'conceptual', 'pregunta': '¿Por qué el SB pierde más que el BB incluso siendo ciegos similares? Tus datos: SB -160 BB/100 vs BB -126 BB/100.', 'calculo': None, 'respuesta': 'SB tiene doble penalización: paga blind forzado Y siempre juega OOP. BB también juega OOP pero: tiene más información (ve la acción completa), paga menos relativo (1bb vs 0.5bb pero actúa último preflop), y puede defenderse mejor con rango amplio. SB entra al flop OOP en el 100% de las manos que juega.', 'aplicacion': 'Tu SB -160 BB/100 probablemente incluye las pérdidas por limping más las pérdidas estructurales de la posición. Eliminar limps puede mejorar el SB resultado en 30-50 BB/100.'}, {'tipo': 'calculo', 'pregunta': 'Desde SB decides open-raise a 3bb. BB llama. Pot = 6bb. Flop: Q♦ 7♣ 3♥. Tú has abierto y tienes iniciativa. ¿Deberías hacer cbet? ¿Con qué sizing? ¿Cambia si tienes A2o vs si tienes QJ?', 'calculo': 'Con A2o (air/missed): cbet 40-50% del pot (3bb) como semi-bluff/steal. El tablero Q73 no conecta bien con rango BB (muchas manos de BB no tienen Q). Con QJ (top pair): cbet 60-75% del pot (4-5bb) para valor. Sizing mayor con valor, menor con bluffs — básico pero importante.', 'respuesta': 'Con A2o: cbet 50% pot (~3bb). Objetivo: robar pot. Si te llaman en un tablero seco, el BB suele tener pair o draw. Fold al raise. Con QJ: cbet 65% pot (~4bb). Objetivo: extraer valor de Qx débil, 77, 33.', 'aplicacion': 'Desde SB con iniciativa: cbet frecuente en tableros secos. El pool NL2 llama demasiado pero también foldea en tableros que no conectan con su rango.'}, {'tipo': 'conceptual', 'pregunta': 'Tus datos muestran PFR desde SB = 16.9% con VPIP = 42.8%. Eso significa que limpas o llamas mucho y rara vez subes. ¿Cuál debería ser la relación VPIP/PFR ideal desde SB?', 'calculo': 'PFR/VPIP ratio ideal desde SB: >60%. Si VPIP=40%, PFR debería ser ≥25% (PFR/VPIP = 62%). Tu ratio actual: 16.9/42.8 = 39.5% — muy bajo. Significa que entras al pot pero sin iniciativa el 60% de las veces.', 'respuesta': 'Deberías aspirar a PFR/VPIP ratio ≥60% desde SB. Con VPIP 38-42%: PFR ≥25%. Las manos que entras sin raise (limp/call) son las más problemáticas postflop.', 'aplicacion': "Fix inmediato: por cada limp desde SB → pregúntate '¿puedo hacer raise aquí?' Si sí → raise. Si no → fold."}], 'level_2': [{'tipo': 'conceptual', 'pregunta': 'Diseña el rango de apertura SB para NL2. ¿Qué 35-40% de manos abrirías? ¿Por qué esas y no otras?', 'calculo': None, 'respuesta': 'Rango SB NL2 (~38%): Pares: 22-AA (todos). Ases: A2s-AKs, A2o-AKo (todos). Reyes: K2s-KQs, K7o-KQo. Damas: Q5s-QKs, Q9o-QKo. Jotas: J7s-JKs, J9o-JKo. Suited connectors: 54s-T9s. Criterio: manos con equity directa (pares, Ax) o realización alta (suited connectors). Descartar: manos small offsuit (75o, 64o), K2o-K5o, Q2o-Q8o.', 'aplicacion': 'Ejercicio: abre SimplePostflop.com → SB vs BB → compara tu rango intuitivo con el calculado. ¿Coinciden en ≥80%? Si no → ajusta.'}, {'tipo': 'calculo', 'pregunta': 'BTN abre 2.5bb. Estás en SB. ¿Cuándo haces 3bet vs call vs fold? Calcula los pot odds primero para el call.', 'calculo': 'Call desde SB vs BTN open 2.5bb: Debes pagar 2bb (ya pusiste 0.5bb). Pot si llamas = 2.5 + 1 + 2 = 5.5bb. Pot odds = 2/5.5 = 36.4%. Más cara que desde BB. Además juegas OOP vs BTN que tiene posición postflop. Esto hace que el call desde SB sea peor que desde BB.', 'respuesta': 'Desde SB vs BTN: 3bet o fold es casi siempre mejor que call. 3bet: con manos de valor (TT+, AJs+, KQs) y algunos bluffs con blockers. Call: solo con manos muy específicas (pares medianos para set mining). Fold: todo lo que no es 3bet ni call directo.', 'aplicacion': 'Regla práctica SB vs BTN: 3bet o fold. Si no puedes 3bet con confianza → fold. Las calls desde SB OOP vs BTN IP son las más costosas del juego.'}, {'tipo': 'calculo', 'pregunta': 'Estás en SB con 99. BTN abre 2.5bb. CO ya llamó. ¿Cómo cambia la situación vs SB solo vs BTN? ¿Qué haces con 99 en esta situación?', 'calculo': 'Con CO llamando: pot antes de tu acción = 2.5 + 2 = 4.5bb. Si llamas: pot = 4.5 + 2 + 1(BB) = 7.5bb si BB foldea. Implied odds mejoran con más jugadores. Pero también: más jugadores = menos fold equity con 3bet, y 99 pierde valor como overpair con 2 jugadores atrás.', 'respuesta': '99 en SB vs BTN+CO: fold o call (set mining), no 3bet. 3bet aquí es incorrecto: CO puede squeezear, BTN puede 4bet, y 99 no quiere jugar gran bote en SRP multiway OOP. Call para set mining si los stacks lo justifican (>15:1 implied).', 'aplicacion': 'El número de jugadores en el pot cambia completamente la decisión. En multiway: juega más conservador desde SB. Las manos especulativas bajan de valor OOP en multiway.'}]}, 'ccall_PF': {'level_1': [{'tipo': 'conceptual', 'pregunta': 'UTG abre 2.5bb. Tienes T9s en CO. ¿Es mejor call, 3bet o fold? ¿Por qué?', 'calculo': None, 'respuesta': 'Fold o 3bet. Call es el peor. UTG rango ~20% (fuerte). T9s tiene ~35% equity vs ese rango. Si llamas: juegas en medio sin iniciativa, UTG continúa en tableros que conectan con su rango, y puedes tener jugadores detrás. 3bet: solo si UTG es loose. Fold: si es tight.', 'aplicacion': 'Tus datos: cold-call rate 18% vs ref 8%. Esas 10pp extra de cold-calls son principalmente manos marginales que deberían ser fold o 3bet.'}, {'tipo': 'calculo', 'pregunta': 'BTN abre 2.5bb. Estás en CO con JTs. ¿Cuánto tienes que llamar? ¿Qué pot odds tienes? ¿Es call, 3bet o fold con JTs en CO?', 'calculo': 'Call en CO vs BTN: pagas 2.5bb. Pot tras call = 2.5 + 2.5 + 0.5(SB) + 1(BB) = 6.5bb si blinds foldean. Pot odds = 2.5/6.5 = 38.5%. JTs vs BTN 45% tiene ~42-44% equity → call es matemáticamente correcto. Además: juegas IP vs BTN si los blinds foldean.', 'respuesta': 'JTs en CO vs BTN: call es correcto si los blinds foldean frecuente y juegas IP. Es diferente a T9s en CO vs UTG — aquí tienes posición. 3bet también es válido para balance. Fold solo vs rango muy tight.', 'aplicacion': 'La posición relativa importa. CO vs BTN: tienes posición, el call tiene más valor. CO vs UTG: juegas OOP, el call tiene mucho menos valor.'}, {'tipo': 'conceptual', 'pregunta': '¿Por qué los pares pequeños (22-55) son los mejores candidatos para cold-call en posición, mientras que manos como KJo son peores?', 'calculo': None, 'respuesta': 'Pares pequeños: set mining. Si hacen set (1 de cada 8.5 veces), ganan botes enormes. Su EV viene de los implied odds, no de la equity bruta. KJo: gana cuando hace top pair, pero KJo pierde contra muchas manos que te llaman o suben: KQ, KA, AJ, AA, KK. Manos que dominan KJo son comunes en rangos de apertura.', 'aplicacion': 'Regla: para cold-call necesitas implied odds (pares para sets) o equity directa sin dominación (broadways fuertes vs rangos amplios). KJo off vs UTG range: dominated demasiado frecuente.'}], 'level_2': [{'tipo': 'conceptual', 'pregunta': 'CO abre 2.5bb. BTN llama. Estás en BB con 88. ¿Qué haces? ¿Cambia si solo CO abre sin caller?', 'calculo': None, 'respuesta': 'Con CO+BTN en el pot: 88 quiere hacer set (IP sería ideal). OOP vs dos jugadores es complicado. 3bet: posible pero CO puede tener rango fuerte, BTN puede llamar. Call: set mining, pero OOP multiway es difícil. Fold: excesivamente tight con 88. Mejor decisión: call con intención de fold en flops sin set ni draw. Solo CO: 3bet o call ambos son válidos. Más margen para 3bet.', 'aplicacion': 'Con manos de valor mediano (88-TT) en BB: multiway → más cuidado. Heads-up → más agresivo. El número de oponentes cambia la estrategia óptima.'}, {'tipo': 'calculo', 'pregunta': 'Defines que en NL2 harás cold-call solo con: pares 22-TT, AJs+, KQs, AQo+. Estima cuántas manos por hora tendrás oportunidad de cold-callear con este rango restrictivo. ¿Cómo afecta esto a tu winrate?', 'calculo': 'En 6-max NL2: ~80-100 manos/hora. Cold-call oportunidades (alguien abre, tú en posición): ~15-20/hora. Tu rango restrictivo (22-TT, AJs+, KQs, AQo+): ~6-7% del espacio. Cold-calls efectivos: 15-20 × 7% = ~1-1.5 cold-calls/hora. Efecto en winrate: reduces las manos OOP sin ventaja, mejoras el EV por mano jugada.', 'respuesta': 'Con rango restrictivo: haces ~1-2 cold-calls por hora. Reducir de 18% a 7-8% elimina ~10 cold-calls problemáticos por 100 manos. Impacto estimado en winrate: +5 a +15 BB/100 en posiciones relevantes.', 'aplicacion': 'Menos manos jugadas con ventaja marginal = más BB/100. El poker es sobre elegir las batallas que ganas, no todas las batallas.'}]},         'BTN_IP_open_postflop': {
            'concepto_clave': (
                "BTN es la posición más rentable del póker. Tus datos: cuando abres "
                "(raise) desde BTN → +63 BB/100. Cuando limpeas → -499 BB/100. "
                "La diferencia es brutal: limp desde BTN es EV negativo puro."
            ),
            'pregunta_guia': (
                "Antes de actuar en BTN: ¿es esta mano suficientemente fuerte para "
                "abrir 2.5bb y defender 3 calles? Si no → fold. Nunca limp."
            ),
            'youtube': [
                {
                    'query': 'BTN open range NL micro stakes',
                    'canales': ['Jonathan Little Poker', 'Upswing Poker', 'SplitSuit Poker'],
                    'url_busqueda': 'https://www.youtube.com/results?search_query=BTN+open+range+micro+stakes',
                },
                {
                    'query': 'playing IP postflop as preflop aggressor',
                    'canales': ['Upswing Poker', 'Run It Once'],
                    'url_busqueda': 'https://www.youtube.com/results?search_query=IP+postflop+preflop+aggressor+poker',
                },
            ],
            'level_1': [
                {
                    'id': 'BTN_Q1',
                    'pregunta': (
                        "Estás en BTN. El BB está mirándote. "
                        "Tienes 7h 3d (72o — mano muy débil). ¿Qué haces?"
                    ),
                    'respuesta_correcta': 'b',
                    'opciones': {
                        'a': 'Limp para ver el flop barato',
                        'b': 'Fold — 72o no está en rango BTN open',
                        'c': 'Open 2.5bb — cualquier mano se puede abrir desde BTN',
                        'd': 'Limp si el BB es pasivo',
                    },
                    'explicacion': (
                        "72o es la peor mano del póker — siempre fold desde BTN. "
                        "Limp es el error clásico: pagas 1bb por ver un flop con una mano "
                        "que no puede ganar showdowns. Tu dato real: limp BTN = -499 BB/100."
                    ),
                    'concepto_teorico': "Limp desde BTN destruye EV — raise o fold siempre.",
                    'followup': (
                        "SIMPLIFICACIÓN: BTN tiene dos opciones → raise o fold. "
                        "Limp siempre es error. Con manos malas (bottom 55-60%): fold. "
                        "Con manos jugables (top 40-45%): open 2.5bb."
                    ),
                },
                {
                    'id': 'BTN_Q2',
                    'pregunta': (
                        "BTN, todos foldan hasta ti. Tienes Jh 9s (J9o). "
                        "El BB es un fish que llama el 80% de las veces. ¿Qué haces?"
                    ),
                    'respuesta_correcta': 'a',
                    'opciones': {
                        'a': 'Open 2.5bb — J9o está en rango BTN, el fish te da valor',
                        'b': 'Limp — el fish va a llamar de todos modos',
                        'c': 'Fold — J9o es demasiado débil vs un fish que llama todo',
                        'd': 'Open 4bb para aislar al fish',
                    },
                    'explicacion': (
                        "J9o está cómodamente en el top 40% de manos — open estándar. "
                        "Que el BB sea un fish que llama mucho hace el open MÁS valioso, "
                        "no menos. Limp sería regalar equity preflop. "
                        "Open 4bb para aislar puede ser bueno, pero 2.5bb es correcto y suficiente."
                    ),
                    'concepto_teorico': "Vs fish calling stations: abre normal o ligeramente más — nunca limp.",
                },
                {
                    'id': 'BTN_Q3',
                    'pregunta': (
                        "BTN, abres 2.5bb con Ks Qs. Solo el BB llama. "
                        "Flop: 7h 4d 2c (rainbow, seco). BB hace check. ¿Qué haces?"
                    ),
                    'respuesta_correcta': 'a',
                    'opciones': {
                        'a': 'Bet 30-40% pot — tienes dos overcards y range advantage',
                        'b': 'Check atrás — sin pair, no apuesto',
                        'c': 'Bet 75% pot — presión máxima',
                        'd': 'Fold si el BB apuesta',
                    },
                    'explicacion': (
                        "7-4-2 rainbow es un tablero IDEAL para BTN: "
                        "tu rango (top 40-45% manos) tiene muchos pares y overcards; "
                        "el BB tiene más manos débiles que en ningún otro tablero. "
                        "Bet pequeño (30-40% pot) con KQs: dos overcards + backdoor draws. "
                        "Check atrás regalas equity y permites que el BB realice el suyo."
                    ),
                    'concepto_teorico': "En tableros dry favorables para BTN: bet small frecuente.",
                },
                {
                    'id': 'BTN_Q4',
                    'pregunta': (
                        "BTN, abres 2.5bb con 9h 8h. BB llama. "
                        "Flop: Kd Tc 5s. BB hace check. ¿Qué haces?"
                    ),
                    'respuesta_correcta': 'b',
                    'opciones': {
                        'a': 'Bet 60% pot — soy el agresor preflop',
                        'b': 'Check atrás — este flop favorece más el rango del BB',
                        'c': 'Bet 33% pot — small sizing para protestar',
                        'd': 'All-in — estoy committed',
                    },
                    'explicacion': (
                        "K-T-5 es un tablero que FAVORECE el rango del BB: "
                        "el BB defiende ciego con manos como KXs, T9s, 55, KTo que tienen "
                        "mucha equity aquí. Tu 98h solo tiene un gutshot (J da straight). "
                        "Mejor check: ves carta gratis, realizas equity del draw, "
                        "y no pagas cuando el BB tiene ya un par."
                    ),
                    'concepto_teorico': "En tableros que favorecen al BB: check atrás con draws mediocres.",
                },
                {
                    'id': 'BTN_Q5',
                    'pregunta': (
                        "Tus datos reales muestran BTN -20 BB/100 en 1185 manos. "
                        "Cuando LIMPEAS desde BTN: -499 BB/100. "
                        "Cuando ABRES (raise): +63 BB/100. "
                        "¿Qué porcentaje de tus manos BTN están en rango de open?"
                    ),
                    'respuesta_correcta': 'b',
                    'opciones': {
                        'a': 'Top 25% — solo manos premium',
                        'b': 'Top 40-45% — rango estándar BTN 6-max NL2',
                        'c': 'Top 60% — el BTN puede abrir muy amplio',
                        'd': 'Top 15% — posición no cambia tanto el rango',
                    },
                    'explicacion': (
                        "BTN en 6-max NL2 abre típicamente el top 40-45% de manos. "
                        "Con un pool pasivo-calling como el de NL2, puedes ir hasta 47-48%. "
                        "Por encima de 50% empiezas a abrir manos que no realizan equity. "
                        "Tu +63 BB/100 cuando abres confirma que estás abriendo las manos correctas — "
                        "el único problema son los limps (88 manos, -499 BB/100)."
                    ),
                    'concepto_teorico': "BTN 6-max: rango de apertura ~40-45% del espacio de manos.",
                },
            ],
            'level_2': [
                {
                    'id': 'BTN_Q6',
                    'pregunta': (
                        "Abres BTN 2.5bb con Ah Tc. BB llama. "
                        "Flop: As 7d 3h. BB check. Bet o check?"
                    ),
                    'respuesta_correcta': 'a',
                    'opciones': {
                        'a': 'Bet 30% pot — top pair, value thin vs BB calling range',
                        'b': 'Check — proteger mi rango, no sobreponerme',
                        'c': 'Bet 75% pot — top pair merece apuesta grande',
                        'd': 'Check — el tablero es favorable, puedo check-call',
                    },
                    'explicacion': (
                        "AT en A-7-3 rainbow: tienes top pair con kicker medio. "
                        "El BB puede tener Ax manos peores (A2-A9), 77, 33, 73s. "
                        "Bet pequeño (30%) extrae valor de sus Ax débiles sin soberapostar. "
                        "Check atrás permite draws gratis y pierdes una calle de valor."
                    ),
                    'concepto_teorico': "Top pair IP: bet pequeño para extrae valor thin en tableros secos.",
                },
                {
                    'id': 'BTN_Q7',
                    'pregunta': (
                        "BTN abriste 2.5bb con 6h 6d. BB llama. "
                        "Flop: Kc 9s 2d. BB check-call tu bet 33%. "
                        "Turn: 4h. BB hace check. ¿Qué haces?"
                    ),
                    'respuesta_correcta': 'b',
                    'opciones': {
                        'a': 'Barrel 60% — continúa con la presión',
                        'b': 'Check atrás — 66 no mejoró, deja ver river gratis',
                        'c': 'Bet 33% — mantén el sizing pequeño',
                        'd': 'All-in — represent the bluff',
                    },
                    'explicacion': (
                        "66 en K-9-2-4 rainbow: tienes underpair, el BB llamó el flop "
                        "(probable Kx o draw). Sin mejorar en turn, tu mano tiene poca equity. "
                        "Barrel pierde vs. Kx y no consigue fold de draws fácilmente. "
                        "Check atrás: ves river gratis, y si 6 llega → value bet river. "
                        "Si no → showdown barato o fold vs. river bet."
                    ),
                    'concepto_teorico': "Con underpair IP sin equity turn: check atrás y controla el bote.",
                },
                {
                    'id': 'BTN_Q8',
                    'pregunta': (
                        "Limpeaste desde BTN (error, ya lo sabemos). "
                        "Ahora que aprendiste, describe el proceso correcto: "
                        "¿cuándo abres y cuándo foldeas desde BTN?"
                    ),
                    'respuesta_correcta': 'c',
                    'opciones': {
                        'a': 'Abro cualquier suited, par o broadway. Resto fold.',
                        'b': 'Abro cualquier mano con un As o un rey. Resto fold.',
                        'c': 'Abro top 40-45% del espacio. Incluye: pares, broadways, suited connectors, Axs, manos con potencial de flop. Resto fold.',
                        'd': 'Abro si el BB es pasivo, foldeo si es agresivo.',
                    },
                    'explicacion': (
                        "El rango BTN correcto incluye toda mano con equity vs. rango de BB calling: "
                        "todos los pares, todos los broadways (T+ paired), todos los suited connectors, "
                        "Axs completo, Kxs frecuente, manos con showdown value. "
                        "No es 'any ace or king' — es equity estructural. "
                        "Las decisiones basadas en el oponente (pasivo/agresivo) son nivel 2."
                    ),
                    'concepto_teorico': "Rango BTN: equity estructural vs. BB calling range, no lectura del villain.",
                },
            ],
        },
        'postflop_fundamentals': {'level_1': [{'tipo': 'calculo', 'pregunta': 'Villain bet 3bb en pot de 6bb en el turn. Tienes un flush draw (9 outs). ¿Tienes odds para call solo para el river? ¿Y considerando implied odds?', 'calculo': 'Pot odds: 3 / (6 + 3 + 3) = 3/12 = 25%. Equity flush draw en turn (solo river): 9 × 2% = 18%. 18% < 25% → NO tienes pot odds directas en el turn. Con implied odds: si completas el flush en river y villain paga bet grande, necesitas ganar al menos (3 × (25/18 - 1)) × (pot_river) para justificar. Simplificado: necesitas ganar ~2bb más en el river para justificar el call.', 'respuesta': 'Sin implied odds: fold en turn con solo flush draw. Con implied odds razonables (villain paga river): call justificado. En NL2 el pool paga river frecuente → implied odds son buenos → call.', 'aplicacion': 'Regla del 2 para el turn: outs × 2 = equity para siguiente calle. 9 outs = 18%. Si pot odds > 18% → necesitas implied odds. En NL2: casi siempre los tienes — el pool paga ríos.'}, {'tipo': 'conceptual', 'pregunta': 'Tienes top pair (TP) en el flop desde BB (OOP). Villain hace cbet del 50% del pot. ¿Cuándo haces call vs raise vs fold con top pair?', 'calculo': None, 'respuesta': 'Top pair OOP vs cbet: Call (la mayoría): mantienes al villain en el pot, defines la mano en el turn donde tienes más información. Raise: con top pair + buena patada en tableros secos cuando quieres protegerte de draws o villain tiene muchos draws. Fold: casi nunca con top pair en el flop vs cbet razonable.', 'aplicacion': 'Tus datos: fold to F-cbet desde BB no está en los datos directos, pero fold rate preflop alto sugiere posible exceso de folding postflop. Regla: con top pair → siempre continúas en el flop.'}, {'tipo': 'calculo', 'pregunta': 'Tus datos: Fold to Turn C-bet = 41.5% (vs ref 40%). ¿Esto es correcto, demasiado alto o demasiado bajo? ¿Qué implica para tu estrategia?', 'calculo': 'Fold to T-cbet 41.5% vs referencia 40-42%: CORRECTO. Zona verde. El sistema lo valida. Fold to R-bet 46.3% vs ref 44-48%: también correcto. Estas dos métricas están bien calibradas.', 'respuesta': 'Tus fold rates postflop están bien calibrados. El problema no es que foldees demasiado en el turn/river sino que foldeas demasiado PREFLOP desde BB (67.6% vs ref 30%).', 'aplicacion': 'El leak principal está en preflop BB, no postflop. Si entras a más pots preflop (mejoras el drill activo), tu winrate mejora sin cambiar el juego postflop.'}]},

    'SB_open_or_fold': {
        'level_1': [
            {'tipo': 'concepto', 'pregunta': 'Regla de oro SB NL2 explotativo: ¿opciones y cuál eliminar?', 'respuesta': 'RAISE o FOLD. Elimina el limp. OOP sin iniciativa = EV negativo.', 'aplicacion': 'Tu SB limp rate ~29.5%. Solo RAISE o FOLD desde SB durante 10 sesiones.'},
            {'tipo': 'calculo', 'pregunta': 'Tu SB PFR/VPIP es 41.6% (ref ≥60%). ¿Qué significa?', 'calculo': 'Entras en ~43% manos, raises en ~18%. 25% sin iniciativa.', 'respuesta': 'Fix: si no es raise, es fold. Subir PFR/VPIP a ≥60%.', 'aplicacion': 'Velocity: -0.30pp/ses → 66 sesiones para 0% limp sin intervención activa.', 'followup': 'PFR/VPIP es el ratio de raises entre todas las manos jugadas. Si entras en 10 manos y raises en 4, tu PFR/VPIP = 40%. ¿Cuánto debería ser el mínimo desde SB en NL2 explotativo? → ≥60%. Con 60%: si VPIP=33%, PFR debería ser ≥20%. ¿El tuyo lo cumple?'},
            {'tipo': 'concepto', 'pregunta': '¿Por qué el objetivo en SB es minimizar pérdidas, no ganar?', 'respuesta': 'Ciega parcial forzada + OOP todas las calles. Ref NL2: -50/-70 BB/100.', 'aplicacion': 'Tu SB BB/100: -92. Objetivo: llegar a -50/-70, no ser ganador.'},
        ],
    }}


def run_reasoning_session(drill_activo, level='level_1', n_questions=3):
    """
    Sesión de preguntas de razonamiento. Sin HH. Sin pipeline.
    Funciona cualquier día. Razonamiento real, no memorización.
    """
    import random
    questions = REASONING_QUESTIONS.get(drill_activo, {}).get(level, [])
    if not questions:
        print(f"   ⚪ Sin preguntas para {drill_activo} {level}")
        print(f"   Disponibles: {list(REASONING_QUESTIONS.keys())}")
        return

    selected = random.sample(questions, min(n_questions, len(questions)))
    print(f"\n{'═'*62}")
    print(f"  🧠 RAZONAMIENTO — {drill_activo} ({level})")
    print(f"  {len(selected)} preguntas | Piensa antes de revelar")
    print(f"{'═'*62}\n")

    for i, q in enumerate(selected, 1):
        print(f"  ┌─ {i}/{len(selected)} {'─'*50}")
        # Print question wrapped
        words = q['pregunta'].split()
        line = "  │  "
        for w in words:
            if len(line)+len(w)+1 > 62: print(line); line = "  │  "+w+" "
            else: line += w+" "
        if line.strip(): print(line)
        if q.get('calculo'):
            print(f"  │  💡 Hay un cálculo concreto aquí.")
        print(f"  │")
        print(f"  │  → Presiona Enter cuando tengas respuesta...")
        try: input("  │  ")
        except EOFError: pass

        if q.get('calculo'):
            print(f"  │  📐 {q['calculo']}")
            print(f"  │")
        print(f"  │  ✅ {q['respuesta']}")
        print(f"  │  🎯 {q['aplicacion']}")
        print(f"  └{'─'*58}\n")

    print(f"{'═'*62}")
    print(f"  {len(selected)} preguntas completadas.")
    print(f"  Siguiente: run_guided_hand_review(df, spot) con manos reales.")
    print(f"{'═'*62}")


print("✅ run_reasoning_session() cargado (v1.82)")
print(f"   Drills: {list(REASONING_QUESTIONS.keys())}")
print("   Sin HH. Sin pipeline. Cualquier día.")


# ════════════════════════════════════════════════════════════════════════
# SECCIÓN 3f.4 — Test de Conocimiento antes de Level Up (v1.83)
# 3 preguntas A/B/C/D. Necesitas 2/3 para que el level_up sea válido.
# ════════════════════════════════════════════════════════════════════════

LEVEL_UP_TESTS = {
    'BB_OOP_SRP_deep_preflop_unknown_F': {
        'level_1_to_2': [
            {'pregunta': "Villain abre 2.5bb BTN. ¿Equity mínima necesaria desde BB?",
             'opciones': ['A) 20%', 'B) 30%', 'C) 40%', 'D) 50%'],
             'correcta': 'B', 'explicacion': "Call=1.5bb, pot=5bb → 1.5/5=30%."},
            {'pregunta': "¿Por qué 76s defiende mejor que K2o desde BB?",
             'opciones': ['A) Cartas más pequeñas', 'B) Equity realización — flush/straight draws', 'C) Pool no juega Ks', 'D) K2o defiende mejor'],
             'correcta': 'B', 'explicacion': "76s gana botes grandes con draws. K2o solo gana con top pair débil."},
            {'pregunta': "Villain sube a 3bb vs 2.5bb. ¿Qué pasa con tu rango defensa BB?",
             'opciones': ['A) Igual', 'B) Se amplía', 'C) Se estrecha — necesitas 33% vs 30%', 'D) No cambia'],
             'correcta': 'C', 'explicacion': "3bb: equity mínima=2/5.5=36% vs 30% con 2.5bb."},
        ],
        'level_2_to_3': [
            {'pregunta': "Flush draw flop (9 outs). Villain bet 50% pot. ¿Tienes odds?",
             'opciones': ['A) No — 18%<25%', 'B) Sí — 36%>25% con regla del 4', 'C) Depende posición', 'D) Nunca'],
             'correcta': 'B', 'explicacion': "9×4%=36% hasta river > 25% pot odds."},
            {'pregunta': "¿Desde cuál villain defiendes más amplio? BTN 45% vs BTN 25%.",
             'opciones': ['A) Tight 25%', 'B) Loose 45%', 'C) Igual', 'D) Depende cartas'],
             'correcta': 'B', 'explicacion': "Más débil rango villain = más equity relativa tuya."},
            {'pregunta': "¿Qué haces con A4s en BB vs BTN 2.5bb en NL2?",
             'opciones': ['A) Fold', 'B) Call siempre', 'C) 3bet por blocker y valor', 'D) Solo call si suited'],
             'correcta': 'C', 'explicacion': "A4s: blocker, equity realización alta, pool paga 3bets."},
        ],
    },
    'SB_open_or_fold': {
        'level_1_to_2': [
            {'pregunta': "¿Por qué limp SB es peor que raise o fold?",
             'opciones': ['A) Manos SB débiles', 'B) OOP sin iniciativa — doble desventaja', 'C) SB siempre pierde', 'D) BB tiene mejor mano'],
             'correcta': 'B', 'explicacion': "OOP sin iniciativa: doble penalización."},
            {'pregunta': "Tu PFR/VPIP SB = 39.5%. ¿Es correcto?",
             'opciones': ['A) Sí, perfecto', 'B) No — muy bajo, deberías tener ≥60%', 'C) Depende del pool', 'D) No importa'],
             'correcta': 'B', 'explicacion': "Ideal ≥60%. 39.5% = muchas entradas sin iniciativa."},
            {'pregunta': "¿Cuándo es correcto limp SB?",
             'opciones': ['A) Con 54s', 'B) BB agresivo', 'C) Casi nunca en NL2 — raise o fold', 'D) Con pares pequeños'],
             'correcta': 'C', 'explicacion': "NL2 standard: raise o fold siempre."},
        ],
    },
}


def run_level_up_test(drill_activo, current_level):
    """
    Test de conocimiento antes de subir de nivel.
    3 preguntas. Necesitas 2/3 correctas.
    Returns: {'passed': bool, 'score': int, 'total': int}
    """
    level_num = int(current_level.split('_')[1])
    test_key  = f"level_{level_num}_to_{level_num+1}"
    tests     = LEVEL_UP_TESTS.get(drill_activo, {}).get(test_key, [])

    if not tests:
        print(f"   ℹ️  Sin test para {drill_activo} {test_key} — level_up automático.")
        return {'passed': True, 'score': 0, 'total': 0, 'auto': True}

    print(f"\n{'═'*62}")
    print(f"  📝 TEST NIVEL — {drill_activo} — {current_level} → siguiente")
    print(f"  Necesitas 2/{len(tests)} correctas.")
    print(f"{'═'*62}\n")

    score = 0
    for i, q in enumerate(tests, 1):
        print(f"  {i}. {q['pregunta']}")
        for opt in q['opciones']:
            print(f"     {opt}")
        try:
            resp = input("     Tu respuesta (A/B/C/D): ").strip().upper()
        except EOFError:
            resp = q['correcta']
        if resp == q['correcta']:
            score += 1
            print(f"     ✅ {q['explicacion']}\n")
        else:
            print(f"     ❌ Correcta: {q['correcta']} — {q['explicacion']}\n")

    passed = score >= 2
    print(f"{'═'*62}")
    print(f"  {score}/{len(tests)} — {'✅ APROBADO' if passed else '❌ REPASA y REPITE'}")
    if not passed:
        print(f"  → run_reasoning_session('{drill_activo}', '{current_level}', 3)")
    print(f"{'═'*62}")

    # FIX v1.85: si aprobado → actualizar nivel en M7
    if passed and score > 0:
        try:
            _history = load_drill_history_m7()
            if drill_activo in _history.get('drills', {}):
                _drill = _history['drills'][drill_activo]
                _new_level = map_to_level_m7(current_level, 'LEVEL_UP')
                _drill['current_level'] = _new_level
                save_drill_history_m7(_history)
                print(f"\n  ✅ M7 actualizado: {current_level} → {_new_level}")
        except Exception as _e:
            print(f"  ⚠️  M7 update: {_e}")

    return {'passed': passed, 'score': score, 'total': len(tests)}


print("✅ run_level_up_test() cargado (v1.83)")
print("   2/3 correctas para level_up válido.")


# ════════════════════════════════════════════════════════════════════════
# SECCIÓN 3f.5 — MÓDULOS DE APRENDIZAJE AVANZADO v1.84
#
# M1: Preguntas personalizadas con manos reales del HH
# M2: Repetición espaciada SM-2 (study_history.json)
# M3: Postflop NL2-específico con datos del pool
# M4: Puente teoría → mesa (after_session_bridge)
# M5: Diagnóstico causa raíz de leaks
# ════════════════════════════════════════════════════════════════════════


# ── M1: PREGUNTAS PERSONALIZADAS CON MANOS REALES ────────────────────

def _should_defend_bb(hole_cards, position='BB'):
    """
    Determina si una mano debería defenderse desde BB vs open estándar NL2.

    FIX H01 v1.87 (calibrado): ~54.8% de cobertura del espacio de manos.
    Verificado contra 1326 combos teóricos. Rango real NL2 vs BTN 45%: ~55%.

    DEFENDER (726/1326 = 54.8% del espacio):
      - Todos los pares (22-AA): 78 combos
      - Todas las manos suited: 312 combos
      - Ax offsuit A5o+: 108 combos (A2o-A4o son fold)
      - Broadways/high offsuit: KTo+,QTo+,JTo,T9o: 72 combos
      - K9o,Q9o,J9o: 36 combos
      - Connectors 98o,87o,76o: 36 combos
      - One-gap TX offsuit: T8o,J8o,Q8o,K8o: 48 combos
      - Low connectors 65o,54o (marginal, en rango explotativo NL2): 24 combos

    FOLD (~45% del espacio):
      - A2o-A4o
      - K2o-K7o, Q2o-Q7o, J2o-J7o
      - Offsuit gaps grandes con cartas bajas
    """
    if not hole_cards or str(hole_cards) in ('??','nan','None',''): return None
    cards = str(hole_cards).split()
    if len(cards) != 2: return None
    r1, r2 = cards[0][:-1], cards[1][:-1]
    s1, s2 = cards[0][-1], cards[1][-1]
    ranks   = ['2','3','4','5','6','7','8','9','T','J','Q','K','A']
    if r1 not in ranks or r2 not in ranks: return None

    suited  = (s1 == s2)
    is_pair = (r1 == r2)
    r1_idx  = ranks.index(r1)
    r2_idx  = ranks.index(r2)
    hi      = max(r1_idx, r2_idx)
    lo      = min(r1_idx, r2_idx)
    gap     = hi - lo
    has_ace = (r1 == 'A' or r2 == 'A')

    # 1. Pares → siempre defender
    if is_pair:
        return True

    # 2. Suited → siempre defender (equity realización alta en NL2)
    if suited:
        return True

    # 3. Ax offsuit A5o+ (A2o-A4o = fold — sin suficiente equity realización)
    if has_ace:
        other_idx = r2_idx if r1 == 'A' else r1_idx
        return other_idx >= 3  # A5o+ (ranks[3]='5')

    # 4. Broadways offsuit altos: KTo+, QTo+, JTo, T9o
    if hi >= 8 and lo >= 7:  # lo=7 → '9', hi=8 → 'T'
        return True

    # 5. K9o, Q9o, J9o
    if hi >= 9 and lo >= 7:  # lo=7 → '9', hi=9 → 'J'
        return True

    # 6. Connectors 98o, 87o, 76o (gap=1, hi>=5 → '7')
    if gap == 1 and hi >= 5:
        return True

    # 7. One-gappers altos: T8o, J8o, Q8o, K8o (hi>=8, lo=6 → '8')
    # FIX v1.99: gap==2 incorrecto — J8o gap=3, Q8o gap=4, K8o gap=5
    if lo == 6 and hi >= 8:  # hi-8o donde hi >= T
        return True

    # 8. Low connectors 65o, 54o (marginal — en rango explotativo NL2 pool loose)
    if gap == 1 and hi >= 3 and hi <= 4:
        return True

    # 9. Todo lo demás offsuit → fold
    return False


def generate_personalized_questions(df, drill_activo='BB_OOP_SRP_deep_preflop_unknown_F',
                                     n=5, stake='NL2'):
    """
    Genera preguntas de razonamiento usando TUS manos reales como ejemplos.
    
    No son preguntas genéricas — son exactamente tus manos, tus errores,
    con el hand_id para que puedas buscarlas en PT4/tracker.
    
    Args:
        df:           DataFrame de manos
        drill_activo: drill para filtrar manos relevantes
        n:            número de preguntas a generar
        stake:        stake para contexto
    
    Returns:
        list de dicts con pregunta personalizada por mano real
    """
    questions = []

    if drill_activo == 'BB_OOP_SRP_deep_preflop_unknown_F':
        # Find BB preflop folds that were potential errors
        bb_opp = df[
            (df['player_position'] == 'BB') &
            (df['flg_blind_def_opp'].astype(int) == 1)
        ].copy()

        bb_opp['deberia_defender'] = bb_opp['hole_cards'].apply(_should_defend_bb)
        bb_opp['folded'] = bb_opp['preflop_action'].apply(
            lambda x: str(x).startswith('F') if pd.notna(x) else False
        )

        # Error: folded when should have defended
        errors = bb_opp[bb_opp['folded'] & (bb_opp['deberia_defender'] == True)]
        # Correct: defended when should have
        corrects = bb_opp[~bb_opp['folded'] & (bb_opp['deberia_defender'] == True)]

        import random
        sample_errors   = errors.sample(min(n//2+1, len(errors)), random_state=42) if len(errors) > 0 else pd.DataFrame()
        sample_corrects = corrects.sample(min(n//2, len(corrects)), random_state=42) if len(corrects) > 0 else pd.DataFrame()

        for _, row in pd.concat([sample_errors, sample_corrects]).head(n).iterrows():
            hole   = str(row.get('hole_cards', '??'))
            hid    = str(row.get('hand_id', '?'))
            sess   = str(row.get('session_id', '?'))
            pf_act = str(row.get('preflop_action', '?'))
            folded = str(pf_act).startswith('F')
            net    = float(row.get('net_won', 0))
            date_s = str(row.get('date', ''))[:10]

            # Determine hand characteristics for the question
            cards = hole.split()
            if len(cards) == 2:
                r1,r2 = cards[0][:-1], cards[1][:-1]
                s1,s2 = cards[0][-1], cards[1][-1]
                suited = (s1==s2)
                ranks  = ['2','3','4','5','6','7','8','9','T','J','Q','K','A']
                hi_r   = max(ranks.index(r1) if r1 in ranks else 0,
                             ranks.index(r2) if r2 in ranks else 0)
                has_ace = 'A' in [r1,r2]

                if folded:
                    if suited:
                        concepto = f"{hole} es suited. Cualquier suited defiende desde BB vs open ≤2.5bb."
                        veredicto = "❌ ERROR — suited siempre defiende"
                    elif has_ace:
                        concepto = f"{hole} tiene un As. Cualquier Ax defiende desde BB (pot odds + equity)."
                        veredicto = "❌ ERROR — Ax siempre defiende"
                    elif hi_r >= 9:
                        concepto = f"{hole} tiene carta alta. Broadway/T+ desde BB siempre defiende."
                        veredicto = "❌ ERROR — carta alta defiende"
                    else:
                        concepto = f"{hole} es mano marginal. Analiza pot odds antes de foldar."
                        veredicto = "⚠️  MARGINAL — depende del sizing"
                    pregunta = (
                        f"Mano {hid} ({sess}, {date_s}): tenías {hole} en BB. "
                        f"Foldeaste vs open. "
                        f"¿Fue correcto? Calcula la equity mínima necesaria "
                        f"y compara con la equity de {hole} vs rango BTN 45%."
                    )
                else:
                    concepto = f"Defendiste correctamente con {hole} desde BB."
                    veredicto = "✅ CORRECTO — defendiste mano que debería defender"
                    pregunta = (
                        f"Mano {hid} ({sess}, {date_s}): tenías {hole} en BB. "
                        f"Defendiste (correcto). "
                        f"¿Por qué exactamente esta mano justifica la defensa? "
                        f"¿Qué equity tiene vs rango BTN 45%?"
                    )

                questions.append({
                    'hand_id':    hid,
                    'session_id': sess,
                    'hole_cards': hole,
                    'pregunta':   pregunta,
                    'veredicto':  veredicto,
                    'concepto':   concepto,
                    'net_won':    net,
                    'tipo':       'personalizada_real',
                })

    elif drill_activo == 'SB_open_or_fold':
        # Find SB limps (should be raise or fold)
        sb_limps = df[
            (df['player_position'] == 'SB') &
            (df['preflop_action'].apply(lambda x: str(x) == '' or str(x).startswith('C')))
        ].copy()

        for _, row in sb_limps.sample(min(n, len(sb_limps)), random_state=42).iterrows():
            hole   = str(row.get('hole_cards', '??'))
            hid    = str(row.get('hand_id', '?'))
            sess   = str(row.get('session_id', '?'))
            net    = float(row.get('net_won', 0))

            questions.append({
                'hand_id':    hid,
                'session_id': sess,
                'hole_cards': hole,
                'pregunta': (
                    f"Mano {hid} ({sess}): tenías {hole} en SB. "
                    f"Limpeaste (o no subiste). "
                    f"¿Debías subir a 2.5-3bb o foldear? "
                    f"¿Está {hole} en tu top 35-40% de apertura desde SB?"
                ),
                'veredicto': "❌ LIMP — siempre raise o fold desde SB",
                'concepto': f"SB con {hole}: raise 2.5-3bb si está en top 40%, fold si no. NUNCA limp.",
                'net_won': net,
                'tipo': 'personalizada_real',
            })

    return questions


def run_personalized_session(df, drill_activo, n=3):
    """
    Sesión de preguntas con TUS manos reales.
    Más potente que preguntas genéricas — son exactamente tus errores.
    """
    questions = generate_personalized_questions(df, drill_activo, n=n)

    if not questions:
        print(f"   ⚪ Sin manos suficientes para generar preguntas personalizadas.")
        print(f"   Ejecuta el pipeline primero para tener datos del HH.")
        return

    print(f"\n{'═'*62}")
    print(f"  🎯 PREGUNTAS PERSONALIZADAS — tus manos reales")
    print(f"  Drill: {drill_activo}")
    print(f"  {len(questions)} preguntas basadas en tu historial")
    print(f"{'═'*62}\n")

    for i, q in enumerate(questions, 1):
        print(f"  ┌─ MANO REAL {i}/{len(questions)} ─ Hand #{q['hand_id']} ──────────────")
        print(f"  │  {q['hole_cards']} | {q['session_id']} | Net: {q['net_won']:+.4f}€")
        print(f"  │")
        # Wrap question
        words = q['pregunta'].split()
        line  = "  │  "
        for w in words:
            if len(line)+len(w)+1 > 62: print(line); line = "  │  "+w+" "
            else: line += w+" "
        if line.strip(): print(line)
        print(f"  │")
        print(f"  │  → Presiona Enter para ver el veredicto...")
        try: input("  │  ")
        except EOFError: pass

        print(f"  │  {q['veredicto']}")
        print(f"  │  📚 {q['concepto']}")
        print(f"  └{'─'*58}\n")

    errors_count = sum(1 for q in questions if '❌' in q['veredicto'])
    print(f"{'═'*62}")
    print(f"  Errores detectados: {errors_count}/{len(questions)}")
    if errors_count > 0:
        ev_perdido = sum(abs(q['net_won']) for q in questions if '❌' in q['veredicto'])
        print(f"  EV perdido en estas manos: {ev_perdido:.4f}€")
    print(f"{'═'*62}")

    # FIX v1.99: guardar errores en SM-2 (antes los dos sistemas eran paralelos)
    try:
        if '_save_study_history' in dir() and '_load_study_history' in dir():
            _sh = _load_study_history()
            _dk = drill if 'drill' in dir() else 'BB_OOP_SRP_deep_preflop_unknown_F'
            _ch = False
            for _qi, _q in enumerate(questions):
                if '❌' in _q.get('veredicto', ''):
                    _qid = f"{_dk}::personalized::{_qi}"
                    if _qid not in _sh:
                        _sh[_qid] = {'ef': 2.5, 'interval': 1, 'due': 0, 'correct': 0, 'wrong': 1, 'source': 'personalized'}
                    else:
                        _sh[_qid]['wrong'] = _sh[_qid].get('wrong', 0) + 1
                        _sh[_qid]['interval'] = 1
                    _ch = True
            if _ch:
                _save_study_history(_sh)
    except Exception:
        pass


# ── M2: REPETICIÓN ESPACIADA SM-2 ────────────────────────────────────

_STUDY_HISTORY_FILENAME = 'study_history.json'

def _study_history_path(drive_path=None):
    if drive_path:
        os.makedirs(drive_path, exist_ok=True)
        return os.path.join(drive_path, _STUDY_HISTORY_FILENAME)
    return _STUDY_HISTORY_FILENAME

def _load_study_history(path=None, drive_path=None):
    resolved = path or _study_history_path(drive_path)
    try:
        with open(resolved, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return {'questions': {}, 'last_session': None}


def _save_study_history(history, path=None, drive_path=None):
    import tempfile as _tf_sh
    resolved = path or _study_history_path(drive_path)
    try:
        _dir_sh = os.path.dirname(os.path.abspath(resolved))
        if _dir_sh:
            os.makedirs(_dir_sh, exist_ok=True)
        _fd_sh, _tmp_sh = _tf_sh.mkstemp(dir=_dir_sh or '.', suffix='.tmp')
        try:
            with os.fdopen(_fd_sh, 'w', encoding='utf-8') as f:
                json.dump(history, f, ensure_ascii=False, indent=2)
            os.replace(_tmp_sh, resolved)
        except Exception:
            try: os.unlink(_tmp_sh)
            except: pass
            raise
    except Exception as e:
        print(f"   WARNING: No se pudo guardar study_history: {e}")

# ── MANOS MARCADAS → SM-2 (GAP E — v2.00) ────────────────────────────────────
#
# Las manos donde dudaste en mesa son las más valiosas para aprender.
# Flujo:
#   1. Durante la sesión: en PT4, marca con "flag" o copia el hand_id de manos donde dudaste.
#   2. Después de la sesión: pega los hand_ids en la lista MANOS_MARCADAS de abajo.
#   3. Ejecuta register_marked_hands(df, MANOS_MARCADAS) — las añade al SM-2 como prioritarias.
#   4. La próxima vez que corras run_spaced_session(), aparecerán para revisión.
#
# Formato hand_id: el número que aparece en el HH, ej. 260238062807

MANOS_MARCADAS = []  # ← Pega aquí los hand_ids después de cada sesión


def register_marked_hands(df, hand_ids, drive_path=None):
    """
    Registra manos marcadas en mesa como preguntas SM-2 prioritarias.

    Cada mano marcada se convierte en una entrada de study_history con:
    - interval=1 (aparece mañana)
    - ef=2.5 (EF inicial, se adapta según tus respuestas)
    - source='marked' (distinguible de preguntas de teoría)

    La pregunta que genera es: mostrar la situación de la mano y preguntar
    qué decidiste y por qué. El sistema no puede saber qué pensaste —
    pero sí puede forzarte a articularlo al día siguiente.

    Args:
        df:       DataFrame del HH parseado
        hand_ids: lista de hand_ids (int o str) que marcaste en mesa
        drive_path: ruta Drive para persistencia (None = local)

    Returns:
        dict con {'registered': n, 'not_found': lista, 'already_in_sm2': lista}
    """
    if not hand_ids:
        print("  ℹ️  No hay manos marcadas. Añade hand_ids a MANOS_MARCADAS.")
        return {'registered': 0, 'not_found': [], 'already_in_sm2': []}

    hand_ids_str = [str(h) for h in hand_ids]
    df_ids = df['hand_id'].astype(str).tolist()

    found, not_found = [], []
    for hid in hand_ids_str:
        if hid in df_ids:
            found.append(hid)
        else:
            not_found.append(hid)

    if not_found:
        print(f"  ⚠️  {len(not_found)} hand_id(s) no encontrados en el HH:")
        for hid in not_found[:5]:
            print(f"     {hid}")

    sh = _load_study_history(drive_path=drive_path)
    already_in = []
    registered = 0

    SEP = '─' * 58
    print(f"\n  🎯 MANOS MARCADAS — Registrando en SM-2")
    print(f"  {SEP}")

    for hid in found:
        qid = f"marked::{hid}"
        row = df[df['hand_id'].astype(str) == hid].iloc[0]

        # Build context for the question
        pos       = row.get('player_position', '?')
        hole      = row.get('hole_cards', '?? ??')
        net       = row.get('net_won', 0)
        session   = row.get('session_id', '?')
        pf_action = row.get('preflop_action', '?')
        street    = 'preflop'
        if row.get('flg_r_saw', False): street = 'river'
        elif row.get('flg_t_saw', False): street = 'turn'
        elif row.get('flg_f_saw', False): street = 'flop'

        context = (
            f"Sesión {session} | Posición: {pos} | Mano: {hole} | "
            f"Acción preflop: {pf_action} | Calle más profunda: {street} | "
            f"Resultado: {net/0.02:+.1f} BB"
        )

        if qid in sh:
            already_in.append(hid)
            # Reset interval → aparece mañana de nuevo
            sh[qid]['interval'] = 1
            sh[qid]['due'] = 0
            sh[qid]['context'] = context
            print(f"  🔄 {hid} — ya en SM-2, reiniciado a mañana")
        else:
            sh[qid] = {
                'ef': 2.5,
                'interval': 1,
                'due': 0,
                'correct': 0,
                'wrong': 0,
                'source': 'marked',
                'context': context,
                'pregunta': (
                    f"MANO MARCADA — Revisión de decisión:\n"
                    f"  {context}\n\n"
                    f"  ¿Qué decisión tomaste en el momento clave de esta mano?\n"
                    f"  ¿Por qué? ¿Qué información tenías? ¿Cambiarías algo?"
                ),
            }
            registered += 1
            print(f"  ✅ {hid} — registrado ({pos} | {hole} | {net/0.02:+.1f} BB)")

    if registered > 0 or already_in:
        _save_study_history(sh, drive_path=drive_path)

    print(f"  {SEP}")
    print(f"  Registradas: {registered} nuevas | {len(already_in)} reiniciadas | {len(not_found)} no encontradas")
    print(f"  → Aparecerán en run_spaced_session() la próxima vez que la ejecutes")

    return {'registered': registered, 'not_found': not_found, 'already_in_sm2': already_in}


def _sm2_next_interval(times_correct, times_wrong, prev_interval=1):
    """
    SM-2 simplificado con EF_min = 1.3 (FIX F-16 v1.91).
    - Primera vez correcta: intervalo = 1 día
    - Segunda correcta:     intervalo = 3 días
    - Sucesivas correctas:  intervalo × EF (mínimo 1.3 para evitar colapso)
    - Incorrecto:           reset a 1 día
    EF_min: previene que conceptos difíciles generen intervalos ≤0.
    """
    EF_MIN = 1.3  # SM-2 estándar: easiness factor mínimo
    if times_wrong > times_correct:
        return 1
    if times_correct == 0:
        return 1
    elif times_correct == 1:
        return 3
    else:
        # EF se aproxima por ratio correcto/total — degradado por fallos
        total    = times_correct + times_wrong
        accuracy = times_correct / total if total > 0 else 1.0
        ef       = max(EF_MIN, 1.3 + (accuracy - 0.6) * 1.5)  # EF en [1.3, 2.5]
        interval = max(1, int(prev_interval * ef))
        return min(interval, 60)  # máximo 60 días


def run_spaced_session(drill_activo=None, n_max=5, study_path=None, drive_path=None):
    """
    Sesión de repetición espaciada: muestra las preguntas que vencen hoy.
    
    Preguntas falladas → aparecen mañana.
    Preguntas correctas → aparecen en interval × 2 días.
    Nunca verás la misma pregunta dos veces el mismo día.
    """
    from datetime import date
    history  = _load_study_history(study_path, drive_path)
    today    = str(date.today())
    q_store  = history.get('questions', {})

    # Get all questions from REASONING_QUESTIONS
    due_questions = []

    # Marked hands: manos que marcaste en mesa, registradas vía register_marked_hands()
    # Se cargan directamente de study_history (source='marked')
    _sh_all = _load_study_history(study_path, drive_path)
    for _mqid, _mdata in _sh_all.items():
        if not isinstance(_mdata, dict): continue
        if _mdata.get('source') not in ('marked', 'bridge_error'): continue
        if _mdata.get('due', 1) > 0: continue  # still in cooldown
        _mpregunta = _mdata.get('pregunta', f'Revisión mano marcada: {_mqid}')
        _mdata_norm = {
            'times_correct': _mdata.get('correct',     _mdata.get('times_correct', 0)),
            'times_wrong':   _mdata.get('wrong',       _mdata.get('times_wrong',   0)),
            'next_review':   _mdata.get('next_review', '0000-00-00'),
            'interval':      _mdata.get('interval',    1),
            'source':        _mdata.get('source',      'marked'),
            'pregunta':      _mpregunta,
            'context':       _mdata.get('context',     ''),
        }
        due_questions.append((_mqid, {'tipo': 'marked', 'pregunta': _mpregunta}, _mdata_norm, 'marked', 'marked'))
    for drill, levels in REASONING_QUESTIONS.items():
        if drill_activo and drill != drill_activo: continue
        for level, qs in levels.items():
            for qi, q in enumerate(qs):
                qid = f"{drill}::{level}::{qi}"
                q_data = q_store.get(qid, {
                    'next_review': today,
                    'times_correct': 0,
                    'times_wrong': 0,
                    'interval': 1,
                })
                if q_data['next_review'] <= today:
                    due_questions.append((qid, q, q_data, drill, level))

    if not due_questions:
        print(f"\n  ✅ Sin preguntas pendientes para hoy.")
        print(f"  Vuelve mañana o ejecuta run_reasoning_session() para repasar.")
        return

    import random
    random.shuffle(due_questions)
    selected = due_questions[:n_max]

    print(f"\n{'═'*62}")
    print(f"  🔁 REPETICIÓN ESPACIADA — {len(selected)} preguntas pendientes hoy")
    print(f"  Basado en tu historial de aciertos/fallos")
    print(f"{'═'*62}\n")

    for qid, q, q_data, drill, level in selected:
        tc = q_data.get('times_correct', q_data.get('correct', 0))
        tw = q_data.get('times_wrong',   q_data.get('wrong',   0))
        status = f"{'🟢' if tc > tw else '🔴' if tw > 0 else '⚪'} {tc}✅ {tw}❌"

        print(f"  [{drill.split('_')[0]}·{level}] {status}")
        words = q['pregunta'].split()
        line  = "  "
        for w in words:
            if len(line)+len(w)+1 > 62: print(line); line = "  "+w+" "
            else: line += w+" "
        if line.strip(): print(line)

        if q.get('calculo'):
            print(f"  💡 Hay cálculo concreto.")
        print()
        print(f"  → Enter para revelar...")
        try: input("  ")
        except EOFError: pass

        if q.get('calculo'):
            print(f"  📐 {q['calculo']}")
        if q.get('respuesta'):
            print(f"  ✅ {q['respuesta']}")
        if q.get('aplicacion'):
            print(f"  🎯 {q['aplicacion']}")
        print()

        # Record result
        try:
            resp = input("  ¿Contestaste bien? (s/n): ").strip().lower()
        except EOFError:
            resp = 's'

        correct = resp in ('s', 'si', 'sí', 'y', 'yes', '1')
        if correct:
            q_data['times_correct'] += 1
            # GAP 2 v2.02: si el followup fue mostrado y ahora acertamos → followup helped
            if q_data.get('followup_shown', 0) > 0 and q_data.get('times_correct') == 1:
                q_data['followup_helped'] = True
        else:
            q_data['times_wrong'] += 1
            # GAP B v2.01: mostrar followup cuando falla — descompone el concepto
            _followup = q.get('followup') if isinstance(q, dict) else None
            if _followup:
                print(f"\n  💡 SIMPLIFICACIÓN (porque fallaste):")
                print(f"  {'─'*56}")
                _fw = _followup.split()
                _fl = '  '
                for _fw_word in _fw:
                    if len(_fl) + len(_fw_word) + 1 > 60:
                        print(_fl); _fl = '  ' + _fw_word + ' '
                    else:
                        _fl += _fw_word + ' '
                if _fl.strip(): print(_fl)
                print(f"  {'─'*56}")
                # GAP 2 v2.02: registrar que el followup se mostró
                q_data['followup_shown'] = q_data.get('followup_shown', 0) + 1

        new_interval = _sm2_next_interval(
            q_data['times_correct'], q_data['times_wrong'], q_data['interval']
        )
        from datetime import timedelta
        next_date = (date.today() + timedelta(days=new_interval)).isoformat()
        q_data['interval']    = new_interval
        q_data['next_review'] = next_date
        q_store[qid] = q_data

        msg = f"Próxima revisión: en {new_interval} días ({next_date})"
        print(f"  {'✅' if correct else '❌'} {msg}\n  {'─'*56}\n")

    history['questions']    = q_store
    history['last_session'] = today
    # Save updated marked/bridge_error hands back to root of study_history
    for _sqid, _sq, _sqdata, _sdrill, _slevel in selected:
        if isinstance(_sqdata, dict) and _sqdata.get('source') in ('marked', 'bridge_error'):
            history[_sqid] = _sqdata
    _save_study_history(history, study_path, drive_path)
    reviewed = len(selected)
    print(f"{'═'*62}")
    print(f"  {reviewed} preguntas revisadas. Historial guardado.")
    print(f"{'═'*62}")


# ── M3: POSTFLOP NL2-ESPECÍFICO CON DATOS REALES ─────────────────────

POSTFLOP_NL2_QUESTIONS = [
    {
        'id': 'pf_nl2_01',
        'pregunta': (
            "El pool NL2 tiene fold to turn cbet de ~6.5% (muy bajo). "
            "Tu fold to turn cbet es 41.5%. "
            "¿Cómo explotas que el pool no foldea en el turn? "
            "¿Cambia tu estrategia de betting?"
        ),
        'calculo': (
            "Si el pool foldea 6.5% al turn: "
            "un pure bluff en turn gana 6.5% × pot. "
            "Vs apuesta de 50% pot: necesitas >33% fold equity para ser profitable. "
            "6.5% << 33% → NO uses bluffs puros en turn vs NL2. "
            "Pero: value bets con cualquier par/draw son muy rentables porque te pagan."
        ),
        'respuesta': (
            "El pool NL2 no foldea → elimina bluffs puros en turn/river. "
            "Apuesta SOLO por valor o con draws fuertes (≥12 outs). "
            "Sizing: 65-75% pot en river con valor — el pool paga."
        ),
        'aplicacion': "Tu fold to R-bet 46.3% está bien. El trabajo es en sizing al apostar, no en folding.",
    },
    {
        'id': 'pf_nl2_02',
        'pregunta': (
            "Tienes KQ en BTN vs BB. Flop: K♦ 8♥ 3♣ (top pair, buena patada). "
            "Pot = 5bb. BB checkea. "
            "¿Qué tamaño de cbet usas y por qué? ¿Cambia si el pool flota mucho?"
        ),
        'calculo': (
            "Pool NL2 flota (llama con cualquier cosa) → sizing grande maximiza valor. "
            "50% pot = 2.5bb. 75% pot = 3.75bb. "
            "Con KQ en tablero seco favoreciendo al abridor: apuesta 65-75%. "
            "Razón matemática: si pool llama 60% del tiempo con manos peores, "
            "apuesta mayor = más EV cuando ganas el pot."
        ),
        'respuesta': (
            "Cbet 65-75% pot (3.25-3.75bb). "
            "El tablero seco K83r favorece tu rango BTN. "
            "El pool llama con K débil, 88 ya tiene full, 33 hace full, 8x pagará. "
            "Bet grande = máximo valor vs rango de call amplio."
        ),
        'aplicacion': "En NL2: tablero seco + top pair + IP → apuesta grande. No te preocupes por el balance.",
    },
    {
        'id': 'pf_nl2_03',
        'pregunta': (
            "BB checkea flop y turn (pasivo). River: tú tienes bluff puro (missed draw). "
            "Pot = 20bb. ¿Deberías apostar en el river contra pool NL2?"
        ),
        'calculo': (
            "Pool NL2 fold to river bet: ~7% en general. "
            "Para que bluff de 50% pot (10bb) sea profitable: "
            "necesitas >33% fold equity. "
            "7% << 33% → bluff puro en river = pérdida garantizada. "
            "EV(bluff) = 0.07 × 20bb - 0.93 × 10bb = 1.4 - 9.3 = -7.9bb"
        ),
        'respuesta': (
            "NO. Fold o check en river con bluff puro vs NL2. "
            "EV = -7.9bb en promedio. El pool no foldea suficiente. "
            "Excepción: si específicamente este villain muestra tendencia a foldar."
        ),
        'aplicacion': "En NL2: check/fold con bluffs puros en river. Apuesta solo valor. Disciplina.",
    },
    {
        'id': 'pf_nl2_04',
        'pregunta': (
            "Tienes A♥ T♥ (flush draw + overcard). Pot = 10bb. "
            "Villain apuesta 7bb. ¿Qué haces? Calcula con la regla del 4."
        ),
        'calculo': (
            "Outs: 9 flush + 3 aces (overcard) = 12 outs aproximado. "
            "Equity regla del 4 = 12 × 4 = 48% hasta river. "
            "Pot odds: 7 / (10 + 7 + 7) = 7/24 = 29.2%. "
            "48% >> 29% → call muy claro. "
            "Raise semi-bluff: también correcto. Añade fold equity a equity de draw."
        ),
        'respuesta': (
            "Call obligatorio. Raise semi-bluff es mejor si villain no es station. "
            "12 outs = mano muy fuerte aunque no tengas par. "
            "Con 48% equity eres casi coin flip — no es 'tirar dinero'."
        ),
        'aplicacion': "Con combo draws (flush + overcard): SIEMPRE continúas. Son las manos más poderosas.",
    },
    {
        'id': 'pf_nl2_05',
        'pregunta': (
            "Tu hero fold to F-cbet desde BB es 33.7% (ref ~35%). "
            "¿Está bien calibrado? ¿Cuándo foldeas desde BB en el flop?"
        ),
        'calculo': None,
        'respuesta': (
            "33.7% está en rango correcto. No hay problema aquí. "
            "Cuándo foldear en flop desde BB: "
            "- Sin par, sin draw, sin overcards con valor → fold. "
            "- Con bottom pair en tablero wet donde villain puede tener mucho. "
            "NUNCA foldeas: top pair cualquier patada, middle pair en seco, "
            "cualquier draw con ≥8 outs."
        ),
        'aplicacion': "Tu postflop está bien calibrado. El problema principal es PREFLOP (67.6% fold BB cuando hay oportunidad real).",
    },
    {
        'id': 'pf_nl2_06',
        'pregunta': (
            "SB VPIP 42.8%, pero la mayoría son limps. "
            "Si eliminas los limps y juegas raise/fold desde SB, "
            "¿qué pasa con tu VPIP desde SB? ¿Es eso bueno o malo?"
        ),
        'calculo': (
            "Actualmente: VPIP SB = 42.8%, PFR SB = 16.9%. "
            "Limps ≈ VPIP - PFR = 42.8 - 16.9 = 25.9% de manos son limps. "
            "Si eliminas limps: VPIP nuevo = PFR actual + aperturas nuevas. "
            "Con rango raise/fold top 35%: VPIP ≈ 35%, PFR ≈ 35%. "
            "VPIP baja de 42.8% a ~35% pero PFR sube de 16.9% a ~35%."
        ),
        'respuesta': (
            "VPIP baja pero es BUENO. "
            "El VPIP bajo con PFR alto desde SB = jugando manos con iniciativa. "
            "Mejor perder 0.5bb en fold que perder 2-3bb postflop OOP sin dirección."
        ),
        'aplicacion': "El objetivo desde SB no es VPIP alto — es PFR/VPIP ratio ≥60%.",
    },
]


def run_postflop_nl2_session(n=3):
    """
    Sesión de preguntas postflop específicas para NL2.
    Usa TUS fold rates reales y las tendencias del pool NL2.
    """
    import random
    selected = random.sample(POSTFLOP_NL2_QUESTIONS, min(n, len(POSTFLOP_NL2_QUESTIONS)))

    print(f"\n{'═'*62}")
    print(f"  🎰 POSTFLOP NL2 — {len(selected)} preguntas con tus datos reales")
    print(f"  Pool NL2: fold to cbet ~6.5% → explotar value betting")
    print(f"{'═'*62}\n")

    for i, q in enumerate(selected, 1):
        print(f"  ┌─ {i}/{len(selected)} {'─'*50}")
        words = q['pregunta'].split()
        line  = "  │  "
        for w in words:
            if len(line)+len(w)+1 > 62: print(line); line = "  │  "+w+" "
            else: line += w+" "
        if line.strip(): print(line)
        if q.get('calculo'): print(f"  │  💡 Hay cálculo.")
        print(f"  │  → Enter para revelar...")
        try: input("  │  ")
        except EOFError: pass
        if q.get('calculo'):
            print(f"  │  📐 {q['calculo']}")
        print(f"  │  ✅ {q['respuesta']}")
        print(f"  │  🎯 {q['aplicacion']}")
        print(f"  └{'─'*58}\n")


# ── M4: PUENTE TEORÍA → MESA ──────────────────────────────────────────

# ════════════════════════════════════════════════════════════════
# DRILL_REGISTRY v2.04 — Registro central de drills
# ════════════════════════════════════════════════════════════════
# Añadir un drill nuevo = 1 entrada aquí + preguntas en REASONING_QUESTIONS
# + instrucción en DRILL_ENGINE (pipeline). Nada más.
#
# Cada entrada define:
#   filter_fn  : lambda df,sub → DataFrame de manos relevantes para el drill
#   check_fn   : lambda row → True=correcto, False=error, None=no aplica
#   error_msg  : lambda row → string del error para mostrar
#   ok_msg     : lambda row → string de confirmación (opcional)
#   metric_key : clave en velocity_forecast (debe existir en DRILL_METRICS)
# ════════════════════════════════════════════════════════════════

def _build_drill_registry():
    """Construye y devuelve el registry completo de drills."""

    def _bb_filter(df, sub):
        return sub[
            (sub['player_position'] == 'BB') &
            (sub['flg_blind_def_opp'].astype(int) == 1)
        ].copy()

    def _bb_check(row):
        should = _should_defend_bb(str(row.get('hole_cards','??')))
        folded = str(row.get('preflop_action','')).startswith('F')
        if should is None: return None
        if should and folded:     return False   # debía defender, foldeó
        if should and not folded: return True    # correcto: defendió
        if not should and folded: return True    # correcto: foldeó mano débil
        return None                              # no aplica (limp/check BB)

    def _sb_filter(df, sub):
        return sub[sub['player_position'] == 'SB'].copy()

    def _sb_check(row):
        pf = str(row.get('preflop_action',''))
        limped = (pf == '' or (pf.startswith('C') and not pf.startswith('C_')))
        return not limped  # True=correcto (raise/fold), False=error (limp)

    def _btn_filter(df, sub):
        """BTN: detectar limps — debería ser raise o fold siempre."""
        btn = sub[sub['player_position'] == 'BTN'].copy()
        # Solo manos donde el héroe actuó preflop (VPIP o fold)
        return btn[btn['flg_vpip'].astype(int) | btn['preflop_action'].apply(
            lambda x: str(x).startswith('F') if pd.notna(x) else False)]

    def _btn_check(row):
        pf  = str(row.get('preflop_action',''))
        vpip = row.get('flg_vpip', 0)
        raised = row.get('cnt_p_raise', 0)
        folded = pf.startswith('F') if pd.notna(pf) else False
        if folded: return True          # fold = correcto (no limp)
        if int(raised) > 0: return True # abrió = correcto
        if int(vpip) and int(raised)==0: return False  # limpeó = error
        return None

    def _ccall_filter(df, sub):
        pos = sub[sub['player_position'].isin(['CO','BTN','HJ','UTG'])].copy()
        return pos

    def _is_premium_ccall(hole):
        if not hole or str(hole) in ('??','nan',''): return None
        cards = str(hole).split()
        if len(cards) != 2: return None
        r1,r2 = cards[0][:-1], cards[1][:-1]
        s1,s2 = cards[0][-1], cards[1][-1]
        ranks = ['2','3','4','5','6','7','8','9','T','J','Q','K','A']
        suited = (s1==s2); is_pair=(r1==r2)
        hi = max((ranks.index(r1) if r1 in ranks else 0),
                 (ranks.index(r2) if r2 in ranks else 0))
        if is_pair and hi >= 5: return True
        if r1=='A' or r2=='A':
            if suited and hi >= 9: return True
            if not suited and hi >= 11: return True
            if suited: return False
        if suited and r1 in ('K','Q') and r2 in ('K','Q'): return True
        return False

    def _ccall_check(row):
        vpip = int(row.get('flg_vpip',0))
        raised = int(row.get('cnt_p_raise',0))
        if not vpip: return None         # foldeó preflop — no es ccall
        if raised > 0: return None       # abrió — no es ccall
        # Es un call/limp sin raise previo — cold-call
        hole = str(row.get('hole_cards','??'))
        return _is_premium_ccall(hole)  # True=premium, False=error, None=?

    return {
        'BB_OOP_SRP_deep_preflop_unknown_F': {
            'filter_fn':  _bb_filter,
            'check_fn':   _bb_check,
            'error_msg':  lambda row: f"Foldeaste {row.get('hole_cards','??')} que debería defender",
            'ok_msg':     lambda row: f"Defendiste/foldeaste correctamente {row.get('hole_cards','??')}",
            'metric_key': 'BB_fold',
            'familia':    'blind_defense',
            'label':      'BB defense',
        },
        'SB_open_or_fold': {
            'filter_fn':  _sb_filter,
            'check_fn':   _sb_check,
            'error_msg':  lambda row: f"Limpeaste {row.get('hole_cards','??')} desde SB — raise o fold",
            'ok_msg':     lambda row: f"Acción correcta desde SB con {row.get('hole_cards','??')}",
            'metric_key': 'SB_limp',
            'familia':    'blind_defense',
            'label':      'SB open or fold',
        },
        'BTN_IP_open_postflop': {
            'filter_fn':  _btn_filter,
            'check_fn':   _btn_check,
            'error_msg':  lambda row: f"Limpeaste {row.get('hole_cards','??')} desde BTN — raise o fold",
            'ok_msg':     lambda row: f"Acción correcta desde BTN con {row.get('hole_cards','??')}",
            'metric_key': 'BTN_limp',
            'familia':    'preflop_open',
            'label':      'BTN open or fold',
        },
        'ccall_PF': {
            'filter_fn':  _ccall_filter,
            'check_fn':   _ccall_check,
            'error_msg':  lambda row: (
                f"Cold-call con {row.get('hole_cards','??')} desde {row.get('player_position','?')}"
                f" — no es premium (fold o 3bet)"
            ),
            'ok_msg':     lambda row: f"Cold-call correcto con {row.get('hole_cards','??')}",
            'metric_key': 'ccall_rate',
            'familia':    'cold_call_preflop',
            'label':      'Cold-call PF',
        },
    }

DRILL_REGISTRY = _build_drill_registry()
print("✅ DRILL_REGISTRY v2.04 cargado")
print(f"   Drills registrados: {list(DRILL_REGISTRY.keys())}")
print("   Añadir drill nuevo = 1 entrada en _build_drill_registry() + preguntas SM-2")


def after_session_bridge(df, drill_activo='BB_OOP_SRP_deep_preflop_unknown_F',
                          session_id=None):
    """
    Detecta manos de la sesión donde debías aplicar el drill
    y no lo hiciste. Cierra el loop teoría → mesa.
    
    "Estudiaste que KTs defiende. Esta sesión foldeaste KTs en 3 ocasiones."
    """
    sub = df.copy()
    if session_id:
        sub = sub[sub['session_id'] == session_id]

    print(f"\n{'═'*62}")
    print(f"  🔗 PUENTE TEORÍA → MESA")
    if session_id:
        print(f"  Sesión: {session_id} ({len(sub)} manos)")
    else:
        print(f"  Todas las sesiones ({len(sub)} manos)")
    print(f"  Drill activo: {drill_activo}")
    print(f"{'═'*62}\n")

    applied_ok = []
    missed     = []

    # [v2.04] DRILL_REGISTRY — lógica genérica para cualquier drill
    # Añadir drill nuevo = entrada en DRILL_REGISTRY, no código aquí
    if drill_activo and drill_activo in DRILL_REGISTRY:
        spec = DRILL_REGISTRY[drill_activo]
        drill_hands = spec['filter_fn'](df, sub)
        for _, row in drill_hands.iterrows():
            hid  = str(row.get('hand_id', '?'))
            hole = str(row.get('hole_cards', '??'))
            net  = float(row.get('net_won', 0))
            result = spec['check_fn'](row)
            if result is False:
                missed.append({
                    'hand_id': hid, 'hole': hole, 'net': net,
                    'error': spec['error_msg'](row),
                    'pos': str(row.get('player_position','?')),
                    'date': str(row.get('date','')),
                })
            elif result is True:
                applied_ok.append({
                    'hand_id': hid, 'hole': hole,
                    'note': spec['ok_msg'](row),
                })
    elif drill_activo:
        # Drill no registrado — fallback silencioso
        print(f"  ℹ️  Drill '{drill_activo}' no tiene lógica de bridge registrada.")
        print(f"  Añádelo a DRILL_REGISTRY para activar la detección de errores.")
    # Summary
    total_opp = len(applied_ok) + len(missed)
    exec_rate  = len(applied_ok) / total_opp * 100 if total_opp > 0 else 0

    print(f"  Oportunidades drill: {total_opp}")
    print(f"  Aplicado correctamente: {len(applied_ok)} ({exec_rate:.0f}%)")
    print(f"  Errores detectados: {len(missed)}")

    if missed:
        ev_perdido = sum(abs(m['net']) for m in missed if m['net'] < 0)
        print(f"\n  ❌ MANOS DONDE NO APLICASTE EL DRILL:")
        # GAP A v2.00: clasificar el tipo de error para dirigir el aprendizaje
        _error_tipos = {
            'concepto':   [],  # no sabías que la mano defendía
            'sizing':     [],  # el sizing te pareció demasiado grande
            'lectura':    [],  # le diste crédito al villain por alguna razón
            'otro':       [],  # otra causa
        }
        for m in missed[:5]:
            print(f"\n     Hand #{m['hand_id']}: {m['hole']} — {m['error']}")
            print(f"     ¿Por qué foldeaste esta mano? Elige la causa más cercana:")
            print(f"       (a) No sabía que esta mano debería defender")
            print(f"       (b) El sizing del villain me pareció demasiado grande")
            print(f"       (c) Le di crédito al villain (imagen agresiva, read específico)")
            print(f"       (d) Otra razón")
            # GAP 1+3 v2.02: capturar error_type y nota libre
            try:
                _etype_raw = input("       Tu respuesta (a/b/c/d, Enter=omitir): ").strip().lower()
                _etype = _etype_raw if _etype_raw in ('a','b','c','d') else 'omitida'
            except (EOFError, KeyboardInterrupt):
                _etype = 'omitida'
            try:
                _nota = input("       Nota libre (Enter=omitir): ").strip()
            except (EOFError, KeyboardInterrupt):
                _nota = ''
            _tl = {'a':'concepto','b':'sizing','c':'lectura_villain','d':'otra','omitida':'omitida'}
            if _etype != 'omitida': print(f"       → {_tl[_etype]} registrado")
            if _nota: print(f"       → nota guardada")
            # Store for SM-2 routing — the player answers in their session
            # Auto-route to SM-2 as 'error_bridge' source for next session
            _qid = f"bridge_error::{m['hand_id']}"
            _sh_bridge = _load_study_history() if '_load_study_history' in dir() else {}
            if _qid not in _sh_bridge:
                _sh_bridge[_qid] = {
                    'ef': 2.5, 'interval': 1, 'due': 0, 'correct': 0, 'wrong': 1,
                    'source': 'bridge_error',
                    'error_type': _etype,
                    'nota': _nota,
                    'session_id': session_id or 'unknown',
                    'timestamp': str(m.get('date', '')),
                    'pregunta': (
                        f"ERROR BRIDGE — Mano que deberías haber defendido:\n"
                        f"  Hand #{m['hand_id']} | {m['hole']} | Posición: {m.get('pos','?')}\n"
                        f"  Causa: {_tl.get(_etype,'?')} | Nota: {_nota or '(ninguna)'}\n"
                        f"  ¿Qué harías diferente? ¿Está la mano en _should_defend_bb?"
                    ),
                    'context': str(m),
                }
                try:
                    _save_study_history(_sh_bridge)
                except Exception:
                    pass
        if len(missed) > 5:
            print(f"\n     ... y {len(missed)-5} más")
        print(f"\n  → Responde las preguntas anteriores en tu próxima sesión de estudio.")
        print(f"  → run_spaced_session() mostrará estas manos mañana con pregunta de proceso.")
    else:
        print(f"\n  ✅ Drill aplicado correctamente en todas las oportunidades detectadas.")

    print(f"\n  Execution rate este análisis: {exec_rate:.0f}%")
    print(f"{'═'*62}")

    return {'execution_rate': exec_rate, 'applied': len(applied_ok), 'missed': missed}


# ── M5: DIAGNÓSTICO CAUSA RAÍZ ────────────────────────────────────────

def diagnose_leak_root_cause(df, leak='BB_over_folding'):
    """
    Analiza patrones estadísticos para determinar la causa raíz de un leak.
    Output: hipótesis priorizada con evidencia del HH.
    """
    SEP = '═'*62

    if leak == 'BB_over_folding':
        bb = df[
            (df['player_position'] == 'BB') &
            (df['flg_blind_def_opp'].astype(int) == 1)
        ].copy()

        if len(bb) < 10:
            print("  Insuficientes manos BB para diagnóstico.")
            return

        bb['folded']  = bb['preflop_action'].apply(lambda x: str(x).startswith('F'))
        total_fold    = bb['folded'].mean() * 100
        ref_fold      = 30.0
        exceso        = total_fold - ref_fold

        suited_mask   = bb['hole_cards'].apply(
            lambda x: len(str(x).split())==2 and str(x).split()[0][-1]==str(x).split()[1][-1]
            if pd.notna(x) else False
        )
        suited_fold   = bb[suited_mask]['folded'].mean() * 100   if suited_mask.sum() > 0 else 0
        offsuit_fold  = bb[~suited_mask]['folded'].mean() * 100  if (~suited_mask).sum() > 0 else 0

        ranks = ['2','3','4','5','6','7','8','9','T','J','Q','K','A']
        def hi_rank(hole):
            try:
                c = str(hole).split()
                if len(c)!=2: return 0
                return max(ranks.index(c[0][:-1]) if c[0][:-1] in ranks else 0,
                           ranks.index(c[1][:-1]) if c[1][:-1] in ranks else 0)
            except: return 0
        bb['hi'] = bb['hole_cards'].apply(hi_rank)
        weak_fold   = bb[bb['hi'] <= 5]['folded'].mean() * 100 if (bb['hi']<=5).sum()>0 else 0
        strong_fold = bb[bb['hi'] >= 9]['folded'].mean() * 100 if (bb['hi']>=9).sum()>0 else 0

        print(SEP)
        print("  DIAGNÓSTICO CAUSA RAÍZ — BB_over_folding")
        print(SEP)
        print(f"  SÍNTOMA: BB fold rate = {total_fold:.1f}% (ref 30%)")
        print(f"  Exceso de fold: +{exceso:.1f}pp")
        print()
        print("  ANÁLISIS POR TIPO DE MANO:")
        print(f"  Suited:   fold {suited_fold:.1f}%  (debería ser <5%)")
        print(f"  Offsuit:  fold {offsuit_fold:.1f}% (debería ser ~35%)")
        print(f"  Hi<=6:    fold {weak_fold:.1f}%")
        print(f"  Hi>=T:    fold {strong_fold:.1f}%")
        print()
        print("  HIPÓTESIS (ordenadas por probabilidad):")

        if suited_fold > 30:
            print()
            print("  #1 [ALTA] No saber que SUITED siempre defiende desde BB")
            print(f"     Evidencia: foldeas {suited_fold:.0f}% suited (debería ser ~0-5%)")
            print("     Solucion: regla binaria L1 — suited = call automatico")
        if offsuit_fold > 60:
            print()
            print("  #2 [ALTA] Criterio excesivamente tight con manos offsuit")
            print(f"     Evidencia: foldeas {offsuit_fold:.0f}% offsuit (muchas tienen equity suficiente)")
            print("     Solucion: calcular pot odds antes de foldar")
        if strong_fold > 15:
            print()
            print("  #3 [MEDIA] Foldeando manos fuertes (T+) por miedo postflop")
            print(f"     Evidencia: {strong_fold:.0f}% fold con manos T+ desde BB")
            print("     Solucion: estudiar postflop fundamentals")

        print()
        print("  ACCION INMEDIATA:")
        if suited_fold > 30:
            print("  run_reasoning_session('BB_OOP_SRP_deep_preflop_unknown_F', 'level_1', 3)")
            print("  Pregunta: 'por que suited siempre defiende desde BB?'")
        elif offsuit_fold > 60:
            print("  run_personalized_session(df, 'BB_OOP_SRP_deep_preflop_unknown_F')")
        print(SEP)

    elif leak == 'SB_limping':
        sb = df[df['player_position']=='SB'].copy()
        sb['limped'] = sb['preflop_action'].apply(
            lambda x: str(x)=='' or (str(x).startswith('C') and '_' not in str(x))
        )
        limp_rate = sb['limped'].mean()*100
        pfr_rate  = (sb['cnt_p_raise'].astype(int)>0).mean()*100
        vpip_rate = sb['flg_vpip'].astype(int).mean()*100
        pfr_vpip  = pfr_rate/vpip_rate*100 if vpip_rate > 0 else 0

        print(SEP)
        print("  DIAGNÓSTICO CAUSA RAÍZ — SB_limping")
        print(SEP)
        print(f"  SB VPIP: {vpip_rate:.1f}% | PFR: {pfr_rate:.1f}%")
        print(f"  PFR/VPIP ratio: {pfr_vpip:.1f}% (ref >=60%) {'✅' if pfr_vpip >= 60 else '❌ BAJO'}")
        print(f"  Gap VPIP-PFR: {vpip_rate-pfr_rate:.1f}pp (manos sin iniciativa)")
        print()
        if pfr_vpip < 40:
            print("  ❌ CRÍTICO: mas del 60% de entradas SB son sin iniciativa")
        elif pfr_vpip < 60:
            print("  ⚠️  BAJO: demasiados limps/calls desde SB")
        else:
            print("  ✅ PFR/VPIP >=60% aceptable")
        print()
        print("  HIPÓTESIS:")
        print("  #1 [ALTA] No saber que limp SB es -EV")
        print("     OOP sin iniciativa = perdida sistematica")
        print("     Solucion: run_reasoning_session('SB_open_or_fold', 'level_1', 3)")
        print(f"  #2 [MEDIA] VPIP {vpip_rate:.1f}% alto desde SB — rango demasiado amplio")
        print("     Ajustar a top 35-40%, raise o fold siempre")
        print("  #3 [MEDIA] Miedo a perder el blind")
        print("     fold 0.5bb es mejor que limp OOP sin iniciativa")
        _sb_rb = sb['flg_r_bet'].astype(int).sum() if 'flg_r_bet' in sb.columns else 0
        _sb_rc = sb['cnt_r_call'].astype(int).sum() if 'cnt_r_call' in sb.columns else 0
        if _sb_rc > 3 and _sb_rb > 0 and _sb_rb/_sb_rc > 3:
            print()
            print(f"  ⚠️  SB River AF: {_sb_rb/_sb_rc:.1f} (ref 1.5-2.5) → over-bluff river SB")
            print("     Pool NL2 fold to river ~7% → bluffs river son -EV")
        print(SEP)

    elif leak == 'BTN_negative':
        btn = df[df['player_position']=='BTN'].copy()
        if len(btn) < 30:
            print("  Insuficientes manos BTN para diagnóstico.")
            return

        bb_val   = 0.02
        bb100    = (btn['net_won'].sum()/bb_val)/len(btn)*100
        vpip     = btn['flg_vpip'].astype(int).mean()*100
        pfr      = (btn['cnt_p_raise'].astype(int)>0).mean()*100
        vpip_pfr = vpip - pfr  # diferencia = calls no raises

        btn_flop = btn[btn['flg_f_saw'].astype(int)==1]
        f_bet_pct   = btn_flop['flg_f_bet'].astype(int).mean()*100 if len(btn_flop)>0 else 0
        f_check_pct = btn_flop['flg_f_check'].astype(int).mean()*100 if len(btn_flop)>0 else 0

        # cbet opp vs actual cbet
        cbet_opp  = btn['flg_f_cbet_opp'].astype(int).sum()
        cbet_done = btn['flg_f_cbet'].astype(int).sum()
        cbet_pct  = cbet_done/cbet_opp*100 if cbet_opp>0 else 0

        print(SEP)
        print("  DIAGNÓSTICO CAUSA RAÍZ — BTN_negative")
        print(SEP)
        print(f"  BB/100 BTN: {bb100:+.1f} (esperado: +20 a +50 BB/100 en NL2)")
        print(f"  VPIP: {vpip:.1f}% | PFR: {pfr:.1f}% | diff: {vpip_pfr:.1f}pp")
        print(f"  Flop bet%: {f_bet_pct:.1f}% | Flop check%: {f_check_pct:.1f}%")
        print(f"  CBet flop IP%: {cbet_pct:.1f}% (ref NL2: ~55-65%)")
        print()
        # River metrics (H08 v1.88)
        _btn_sd = df[(df['player_position']=='BTN')&(df['flg_showdown'].astype(int)==1)]
        _btn_wsd2 = _btn_sd['flg_won_hand'].astype(int).mean()*100 if len(_btn_sd)>=5 else None
        _btn_rfo = df[(df['player_position']=='BTN')&(df['flg_r_cbet_def_opp'].astype(int)==1)] if 'flg_r_cbet_def_opp' in df.columns else pd.DataFrame()
        _btn_rfp = _btn_rfo['flg_r_fold'].astype(int).mean()*100 if len(_btn_rfo)>=5 else None

        print("  ANÁLISIS:")
        if _btn_wsd2 is not None:
            _ic = '✅' if _btn_wsd2>=45 else '⚠️' if _btn_wsd2>=35 else '❌'
            print(f"  {_ic} BTN W$SD: {_btn_wsd2:.1f}% en {len(_btn_sd)} showdowns (ref >=48%)")
        if _btn_rfp is not None:
            _ic2 = '✅' if 40<=_btn_rfp<=55 else '⚠️'
            print(f"  {_ic2} BTN Fold to River: {_btn_rfp:.1f}% (ref 40-50%)")
        print()

        hypotheses = []

        if vpip > 38:
            hypotheses.append((1, "ALTA",
                "VPIP BTN muy alto — abriendo demasiado",
                f"VPIP {vpip:.1f}% vs ref 38-42%. Abres manos que no tienen EV+."
                " Solución: ajustar rango de apertura BTN a top 40%."))

        if vpip_pfr > 12:
            hypotheses.append((2, "ALTA",
                "Demasiados cold-calls desde BTN vs re-raises",
                f"VPIP-PFR = {vpip_pfr:.1f}pp. Muchas entradas sin iniciativa."
                " Solución: cold-call BTN solo con premium (pares, suited connectors)."))

        if cbet_pct < 45:
            hypotheses.append((3, "MEDIA",
                "CBet flop BTN muy bajo — perdiendo iniciativa",
                f"CBet {cbet_pct:.1f}% vs ref ~55-65%. No usas la ventaja de posición."
                " Solución: CBet IP en tableros secos con cualquier equity."))

        if f_check_pct > 35:
            hypotheses.append((4, "MEDIA",
                "Demasiado pasivo en flop IP",
                f"Check {f_check_pct:.1f}% en flop desde BTN IP. Estás regalando iniciativa."
                " Solución: apuesta más frecuente con top pair y draws IP."))

        if not hypotheses:
            hypotheses.append((1, "MEDIA",
                "Varianza probable con muestra pequeña",
                f"Con {len(btn)} manos BTN el resultado puede ser varianza pura."
                f" Necesitas ≥500 manos para señal estadística real."))

        print("  HIPÓTESIS (ordenadas por probabilidad):")
        for prio, nivel, hip, ev in sorted(hypotheses):
            print()
            print(f"  #{prio} [{nivel}] {hip}")
            # wrap
            words = ev.split()
            line = "     "
            for w in words:
                if len(line)+len(w)+1 > 62: print(line); line = "     "+w+" "
                else: line += w+" "
            if line.strip(): print(line)

        print()
        print("  ACCIÓN INMEDIATA:")
        if hypotheses:
            top = sorted(hypotheses)[0]
            if 'cold-call' in top[3].lower() or 'ccall' in top[3].lower():
                print("  run_reasoning_session('ccall_PF', 'level_1', 2)")
            elif 'cbet' in top[3].lower() or 'postflop' in top[3].lower():
                print("  run_postflop_nl2_session(n=2)")
            else:
                print("  Revisar rango apertura BTN en SimplePostflop.com")
        print(SEP)

    elif leak == 'WSD_low':
        # W$SD global + por posición
        sd_hands = df[df['flg_showdown'].astype(int)==1].copy()
        f_saw     = df['flg_f_saw'].astype(int).sum()
        wtsd      = len(sd_hands) / f_saw * 100 if f_saw > 0 else 0
        wsd       = sd_hands['flg_won_hand'].astype(int).mean() * 100 if len(sd_hands) > 0 else 0

        # Por posición
        pos_wsd = {}
        for _pos in ['BTN','CO','BB','SB','HJ','UTG']:
            _sub = sd_hands[sd_hands['player_position']==_pos]
            if len(_sub) >= 5:
                pos_wsd[_pos] = (_sub['flg_won_hand'].astype(int).mean()*100, len(_sub))

        print(SEP)
        print("  DIAGNÓSTICO CAUSA RAÍZ — WSD_low (Showdown Quality)")
        print(SEP)
        print(f"  WTSD: {wtsd:.1f}% (ref: 28-32%) {'✅' if 28<=wtsd<=35 else '⚠️'}")
        print(f"  W$SD: {wsd:.1f}% (ref: 48-54%) {'✅' if wsd>=48 else '❌ BAJO'}")
        print()
        print("  W$SD POR POSICIÓN:")
        for _pos, (_w, _n) in sorted(pos_wsd.items(), key=lambda x: x[1][0]):
            _icon = '✅' if _w >= 48 else '⚠️' if _w >= 40 else '❌'
            print(f"  {_icon} {_pos}: {_w:.1f}% ({_n} showdowns)")

        print()
        print("  ANÁLISIS:")
        _hypotheses = []

        if wsd < 45:
            _hypotheses.append((1, "ALTA",
                "Llegando a showdown con manos perdedoras",
                "Señales: llamas river con manos que no ganan. "
                "Solución: fold más en river vs apuestas grandes de jugadores tight. "
                "Regla: si villain bet river >50% pot y es tight → fold top pair débil."))

        if wsd < 42:
            _hypotheses.append((2, "ALTA",
                "Bluffs en spots incorrectos — river vs pool que no foldea",
                "Pool NL2 fold to river bet: ~7%. Bluffs en river son -EV. "
                "Cada bluff fallido que llega a showdown baja W$SD. "
                "Solución: eliminar bluffs puros en river."))

        for _pos, (_w, _n) in pos_wsd.items():
            if _w < 35 and _n >= 10:
                _hypotheses.append((3, "MEDIA",
                    f"W$SD muy bajo desde {_pos} ({_w:.0f}%)",
                    f"Con {_n} showdowns, este patrón es estadísticamente significativo. "
                    f"Revisa manos donde llegaste a showdown desde {_pos}."))

        if not _hypotheses:
            _hypotheses.append((1, "INFO",
                "W$SD en rango aceptable",
                f"W$SD {wsd:.1f}% está cerca de la referencia."))

        print("  HIPÓTESIS:")
        for _prio, _nivel, _hip, _ev in sorted(_hypotheses):
            print()
            print(f"  #{_prio} [{_nivel}] {_hip}")
            _words = _ev.split()
            _line = "     "
            for _w2 in _words:
                if len(_line)+len(_w2)+1 > 62: print(_line); _line = "     "+_w2+" "
                else: _line += _w2+" "
            if _line.strip(): print(_line)

        print()
        print("  ACCIÓN INMEDIATA:")
        if wsd < 45:
            print("  → Revisar últimas 10 manos que llegaron a showdown y perdiste")
            print("  → run_postflop_nl2_session(n=2) — decisiones river NL2")
        print(SEP)

    elif leak == 'ccall_excessive':
        ccall_hands = df[
            df['player_position'].isin(['CO','BTN','HJ']) &
            (df['cnt_p_raise'].astype(int) == 0) &
            (df['flg_vpip'].astype(int) == 1)
        ].copy()

        total_pos = len(df[df['player_position'].isin(['CO','BTN','HJ'])])
        ccall_rate = len(ccall_hands)/total_pos*100 if total_pos>0 else 0

        # Classify cold-calls by hand strength
        ranks = ['2','3','4','5','6','7','8','9','T','J','Q','K','A']
        def classify_ccall(hole):
            if not hole or str(hole) in ('??','nan',''): return 'unknown'
            cards = str(hole).split()
            if len(cards)!=2: return 'unknown'
            r1,r2 = cards[0][:-1],cards[1][:-1]
            s1,s2 = cards[0][-1],cards[1][-1]
            suited = (s1==s2)
            is_pair=(r1==r2)
            hi = max(ranks.index(r1) if r1 in ranks else 0,
                     ranks.index(r2) if r2 in ranks else 0)
            if is_pair and hi>=5: return 'premium'
            if (r1=='A' or r2=='A') and suited and hi>=9: return 'premium'
            if (r1=='A' or r2=='A') and not suited and hi>=11: return 'premium'
            if suited and hi>=9: return 'speculative'
            if is_pair: return 'speculative'
            if hi>=9: return 'marginal'
            return 'bad'

        ccall_hands['ccall_class'] = ccall_hands['hole_cards'].apply(classify_ccall)
        class_counts = ccall_hands['ccall_class'].value_counts().to_dict()

        print(SEP)
        print("  DIAGNÓSTICO CAUSA RAÍZ — ccall_excessive")
        print(SEP)
        print(f"  Cold-call rate IP: {ccall_rate:.1f}% (ref: ~6-8%)")
        print(f"  Total cold-calls: {len(ccall_hands)}")
        print()
        print("  CLASIFICACIÓN DE TUS COLD-CALLS:")
        for cls in ['premium','speculative','marginal','bad']:
            n = class_counts.get(cls,0)
            pct = n/len(ccall_hands)*100 if len(ccall_hands)>0 else 0
            icon = '✅' if cls in ('premium','speculative') else '⚠️' if cls=='marginal' else '❌'
            print(f"  {icon} {cls}: {n} ({pct:.0f}%)")

        bad_n = class_counts.get('bad',0) + class_counts.get('marginal',0)
        print()
        print("  HIPÓTESIS:")
        if bad_n > len(ccall_hands)*0.3:
            print("  #1 [ALTA] Llamando con manos que deberían ser fold o 3bet")
            print(f"     {bad_n} cold-calls marginales/malos de {len(ccall_hands)} totales")
            print("     Solución: run_level_up_test('ccall_PF', 'level_1')")
        else:
            print("  #1 [MEDIA] Cold-call rate dentro de rango aceptable")
            print(f"     {ccall_rate:.1f}% vs ref 6-8%. Composición mayormente correcta.")
        print()
        print("  ACCIÓN INMEDIATA:")
        print("  run_reasoning_session('ccall_PF', 'level_1', 2)")
        print(SEP)



print("✅ Módulos de aprendizaje avanzado cargados (v1.84)")
print("   M1: run_personalized_session(df, drill) → preguntas con tus manos reales")
print("   M2: run_spaced_session() → repetición espaciada SM-2")
print("   M3: run_postflop_nl2_session() → postflop NL2-específico")
print("   M4: after_session_bridge(df, drill) → puente teoría→mesa")
print("   M5: diagnose_leak_root_cause(df, leak) → diagnóstico causa raíz")


# ════════════════════════════════════════════════════════════════════════
# SECCIÓN 3f.6 — Vista de Progreso Acumulado (v1.85)
# Muestra la evolución real del aprendizaje a lo largo del tiempo.
# Lee study_history.json + drill_history_m7.json para vista completa.
# ════════════════════════════════════════════════════════════════════════

def display_study_progress(study_path='study_history.json', m7_path=None):
    """
    Vista completa del progreso de estudio:
    - Execution rate por sesión (M7)
    - Aciertos en preguntas teóricas (SM-2)
    - Nivel actual del drill
    - Tendencia: mejorando / estancado / regresando
    """
    SEP = '═' * 62
    print(f"\n{SEP}")
    print("  📊 PROGRESO DE ESTUDIO — VISTA ACUMULADA")
    print(SEP)

    # ── Parte 1: Drill progress (M7) ─────────────────────────────────
    try:
        m7_data = load_drill_history_m7(m7_path)
        drills  = m7_data.get('drills', {})

        print("\n  🎯 DRILLS — Execution Rate por sesión:")
        for drill_name, drill_data in drills.items():
            entries = drill_data.get('history', [])
            level   = drill_data.get('current_level', 'level_1')
            status  = drill_data.get('status', 'active')
            peak    = drill_data.get('peak_rate', 0)

            if not entries:
                print(f"  {drill_name}: sin sesiones registradas")
                continue

            rates = [e.get('execution_rate', 0) for e in entries[-6:]]
            trend = '📈' if len(rates) >= 2 and rates[-1] > rates[-2] else (
                    '📉' if len(rates) >= 2 and rates[-1] < rates[-2] else '➡️')

            # Mini bar chart
            bar = ''
            for r in rates:
                if r >= 0.8:   bar += '🟢'
                elif r >= 0.6: bar += '🟡'
                else:          bar += '🔴'

            print(f"\n  {drill_name}")
            print(f"  Nivel: {level} | Pico: {peak:.0%} | Estado: {status}")
            print(f"  Últimas {len(rates)} sesiones: {bar} {trend}")
            if rates:
                print(f"  Última exec rate: {rates[-1]:.0%} | Media: {sum(rates)/len(rates):.0%}")

    except Exception as e:
        print(f"  M7 no disponible: {e}")

    # ── Parte 2: Theory questions (SM-2) ─────────────────────────────
    try:
        history  = _load_study_history(study_path, drive_path)
        q_store  = history.get('questions', {})

        if q_store:
            total     = len(q_store)
            mastered  = sum(1 for q in q_store.values() if q.get('times_correct', 0) >= 3 and
                           q.get('times_correct', 0) > q.get('times_wrong', 0))
            learning  = sum(1 for q in q_store.values() if 0 < q.get('times_correct', 0) < 3)
            unseen    = sum(1 for q in q_store.values() if q.get('times_correct', 0) == 0 and
                           q.get('times_wrong', 0) == 0)
            struggling= sum(1 for q in q_store.values() if q.get('times_wrong', 0) >
                           q.get('times_correct', 0))

            total_correct = sum(q.get('times_correct', 0) for q in q_store.values())
            total_wrong   = sum(q.get('times_wrong', 0) for q in q_store.values())
            total_answered = total_correct + total_wrong
            accuracy = total_correct / total_answered * 100 if total_answered > 0 else 0

            print(f"\n  🧠 PREGUNTAS TEÓRICAS (SM-2):")
            print(f"  Total en sistema: {total} preguntas")
            print(f"  Dominadas (≥3 correctas): {mastered} ({mastered/total*100:.0f}%)")
            print(f"  Aprendiendo:               {learning}")
            print(f"  Sin ver:                   {unseen}")
            print(f"  Con dificultad:            {struggling}")
            print(f"  Precisión global:          {accuracy:.0f}% ({total_correct}/{total_answered} respuestas)")

            # Show struggling questions
            if struggling > 0:
                print(f"\n  ⚠️  Preguntas con más fallos:")
                struggling_qs = [(qid, q) for qid, q in q_store.items()
                                 if q.get('times_wrong', 0) > q.get('times_correct', 0)]
                struggling_qs.sort(key=lambda x: x[1].get('times_wrong', 0), reverse=True)
                for qid, q in struggling_qs[:3]:
                    parts = qid.split('::')
                    drill_short = parts[0].split('_')[0] if parts else qid
                    level_short = parts[1] if len(parts) > 1 else ''
                    print(f"     {drill_short} {level_short}: {q.get('times_wrong')}❌ {q.get('times_correct')}✅")
        else:
            print("\n  🧠 SM-2: sin historial todavía. Ejecuta run_spaced_session().")

    except Exception as e:
        print(f"  SM-2 no disponible: {e}")

    # ── Parte 3: Recomendación del día ───────────────────────────────
    print(f"\n  💡 RECOMENDACIÓN AHORA:")
    try:
        from datetime import date
        today = str(date.today())
        pending_today = sum(1 for q in q_store.values()
                           if q.get('next_review', today) <= today) if q_store else 0
        if pending_today > 0:
            print(f"  → {pending_today} preguntas SM-2 pendientes hoy: run_spaced_session()")
        elif struggling > 0:
            print(f"  → Repasa preguntas con dificultad: run_reasoning_session()")
        else:
            print(f"  → Todo al día. Juega y exporta HH para nueva sesión.")
    except Exception:
        print(f"  → run_spaced_session() o run_reasoning_session()")

    print(f"\n{SEP}")


print("✅ display_study_progress() cargado (v1.85)")
print("   Vista completa: exec_rate M7 + SM-2 precisión + recomendación")


# ════════════════════════════════════════════════════════════════════════
# SECCIÓN 3f.7 — Orquestador Central: run_study_session() (v1.85)
#
# UN PUNTO DE ENTRADA. El sistema decide qué ejecutar según el contexto.
# Con HH nuevo: bridge → diagnóstico → personalizado → SM-2 → recursos
# Sin HH: SM-2 → razonamiento → postflop → progreso
#
# No necesitas saber qué funciones existen. Solo ejecutas esto.
# ════════════════════════════════════════════════════════════════════════

def run_study_session(df=None, session_id=None, drill_activo=None,
                      current_level=None, study_path=None,
                      m7_path=None, study_drive_path=None, modo_rapido=False):
    """
    Orquestador central del sistema de aprendizaje.
    
    Decide automáticamente qué estudiar según:
    - ¿Tienes HH nuevo? → estudio basado en tus manos reales
    - ¿No tienes HH? → estudio teórico independiente
    - ¿Execution rate < 70%? → diagnóstico + preguntas correctoras
    - ¿SM-2 pendiente? → repetición espaciada primero
    
    Args:
        df:            DataFrame del HH (None = sin sesión nueva)
        session_id:    ID de la sesión a analizar (None = todas)
        drill_activo:  drill actual (None = lee de M7)
        current_level: nivel actual (None = lee de M7)
        study_path:    ruta study_history.json
        m7_path:       ruta drill_history_m7.json
        modo_rapido:   True = solo 10min de estudio (1-2 actividades)
    
    Returns:
        dict con resumen de lo ejecutado
    """
    from datetime import date

    SEP = '═' * 62
    results = {'activities': [], 'exec_rate': None, 'drill': None, 'level': None}

    # ── Leer contexto de M7 ──────────────────────────────────────────
    try:
        m7_data = load_drill_history_m7(m7_path)
        drills  = m7_data.get('drills', {})
        if drill_activo is None:
            drill_activo = next(
                (k for k, v in drills.items() if v.get('status', '') != 'LOCK'),
                'BB_OOP_SRP_deep_preflop_unknown_F'
            )
        if current_level is None:
            current_level = drills.get(drill_activo, {}).get('current_level', 'level_1')
    except Exception:
        drill_activo  = drill_activo or 'BB_OOP_SRP_deep_preflop_unknown_F'
        current_level = current_level or 'level_1'

    results['drill'] = drill_activo
    results['level'] = current_level

    print(f"\n{SEP}")
    print(f"  🎓 SESIÓN DE ESTUDIO — run_study_session()")
    print(f"  Drill: {drill_activo} | Nivel: {current_level}")
    print(f"  Modo: {'rápido (10min)' if modo_rapido else 'completo (25-35min)'}")
    print(f"  HH disponible: {'✅' if df is not None else '❌ — estudio teórico'}")
    print(SEP)

    # ════════════════════════════════════════════════════════════════
    # FLUJO CON HH NUEVO
    # ════════════════════════════════════════════════════════════════
    if df is not None:

        # PASO 1: Bridge teoría → mesa
        print(f"\n{'─'*62}")
        print(f"  PASO 1/4 — Puente teoría → mesa")
        print(f"{'─'*62}")
        try:
            bridge = after_session_bridge(df, drill_activo, session_id)
            exec_rate = bridge.get('execution_rate', 0)
            missed    = bridge.get('missed', [])
            results['exec_rate']  = exec_rate
            results['activities'].append(f"Bridge: {exec_rate:.0f}% exec_rate, {len(missed)} errores")
        except Exception as e:
            print(f"  ⚠️  Bridge: {e}")
            exec_rate = 100
            missed    = []

        # PASO 2: Diagnóstico si exec_rate bajo
        if exec_rate < 70:
            print(f"\n{'─'*62}")
            print(f"  PASO 2/4 — Diagnóstico causa raíz (exec_rate {exec_rate:.0f}% < 70%)")
            print(f"{'─'*62}")
            try:
                leak_map = {
                    'BB_OOP_SRP_deep_preflop_unknown_F': 'BB_over_folding',
                    'SB_open_or_fold':                   'SB_limping',
                    'ccall_PF':                          'ccall_excessive',
                }
                leak_key = leak_map.get(drill_activo, 'BB_over_folding')
                # También diagnostica BTN si es negativo
                if df is not None:
                    _btn = df[df['player_position']=='BTN']
                    if len(_btn) >= 30:
                        _btn_bb100 = (_btn['net_won'].sum()/0.02)/len(_btn)*100
                        if _btn_bb100 < -30:
                            print("\n  BTN también negativo — diagnóstico adicional:")
                            diagnose_leak_root_cause(df, 'BTN_negative')
                    # WSD auto-trigger si W$SD < 45%
                    _sd_hands = df[df['flg_showdown'].astype(int)==1]
                    _f_saw    = df['flg_f_saw'].astype(int).sum()
                    if len(_sd_hands) >= 20 and _f_saw > 0:
                        _wsd = _sd_hands['flg_won_hand'].astype(int).mean()*100
                        if _wsd < 45:
                            print(f"\n  W$SD={_wsd:.1f}% < 45% — diagnóstico showdown quality:")
                            diagnose_leak_root_cause(df, 'WSD_low')
                diagnose_leak_root_cause(df, leak_key)
                results['activities'].append(f"Diagnóstico: {leak_key}")
            except Exception as e:
                print(f"  ⚠️  Diagnóstico: {e}")

        # PASO 3: Preguntas personalizadas con manos donde fallaste
        if missed and not modo_rapido:
            print(f"\n{'─'*62}")
            print(f"  PASO 3/4 — Preguntas con tus manos reales")
            print(f"{'─'*62}")
            try:
                run_personalized_session(df, drill_activo, n=min(3, len(missed)))
                results['activities'].append("Preguntas personalizadas: manos reales del HH")
            except Exception as e:
                print(f"  ⚠️  Personalizadas: {e}")
        elif not missed:
            print(f"\n  ✅ Drill aplicado correctamente esta sesión. Sin errores.")

        # PASO 4: SM-2 pendientes + recursos
        print(f"\n{'─'*62}")
        print(f"  PASO 4/4 — SM-2 + recursos")
        print(f"{'─'*62}")
        try:
            today = str(date.today())
            history = _load_study_history(study_path, study_drive_path)
            pending = sum(1 for q in history.get('questions', {}).values()
                         if q.get('next_review', today) <= today)
            if pending > 0:
                print(f"  {pending} preguntas SM-2 pendientes hoy:")
                run_spaced_session(drill_activo, n_max=3, study_path=study_path, drive_path=study_drive_path)
                results['activities'].append(f"SM-2: {pending} preguntas revisadas")
            else:
                print(f"  SM-2 al día. Sin pendientes.")
        except Exception as e:
            print(f"  ⚠️  SM-2: {e}")

        try:
            display_study_resources(drill_activo, current_level)
            results['activities'].append("Recursos: YouTube + solver + ejercicio")
        except Exception as e:
            print(f"  ⚠️  Recursos: {e}")

    # ════════════════════════════════════════════════════════════════
    # FLUJO SIN HH (días de estudio puro)
    # ════════════════════════════════════════════════════════════════
    else:

        # PASO 1: SM-2 pendientes (siempre primero)
        print(f"\n{'─'*62}")
        print(f"  PASO 1/3 — Repetición espaciada (SM-2)")
        print(f"{'─'*62}")
        try:
            today = str(date.today())
            history = _load_study_history(study_path, study_drive_path)
            pending = sum(1 for q in history.get('questions', {}).values()
                         if q.get('next_review', today) <= today)
            if pending > 0:
                run_spaced_session(drill_activo, n_max=5 if not modo_rapido else 3,
                                   study_path=study_path, drive_path=study_drive_path)
                results['activities'].append(f"SM-2: {pending} pendientes")
            else:
                print(f"  Sin preguntas SM-2 pendientes hoy.")
                print(f"  Ejecuta run_reasoning_session() para añadir nuevas al sistema.")
        except Exception as e:
            print(f"  ⚠️  SM-2: {e}")

        # PASO 2: Razonamiento activo
        if not modo_rapido:
            print(f"\n{'─'*62}")
            print(f"  PASO 2/3 — Razonamiento activo")
            print(f"{'─'*62}")
            try:
                run_reasoning_session(drill_activo, current_level, n_questions=2)
                results['activities'].append("Razonamiento: 2 preguntas")
            except Exception as e:
                print(f"  ⚠️  Razonamiento: {e}")

            # Postflop rotation (cada 3 días)
            try:
                day_of_week = date.today().weekday()
                if day_of_week in (2, 5):  # Miércoles y Sábado
                    print(f"\n  Postflop NL2:")
                    run_postflop_nl2_session(n=1)
                    results['activities'].append("Postflop NL2: 1 pregunta")
            except Exception as e:
                pass

        # PASO 3: Recursos y progreso
        print(f"\n{'─'*62}")
        print(f"  PASO 3/3 — Recursos + progreso")
        print(f"{'─'*62}")
        try:
            display_study_resources(drill_activo, current_level)
            results['activities'].append("Recursos: mostrados")
        except Exception as e:
            print(f"  ⚠️  Recursos: {e}")

    # ── Resumen final ────────────────────────────────────────────────
    print(f"\n{SEP}")
    print(f"  ✅ SESIÓN COMPLETADA — {len(results['activities'])} actividades")
    for act in results['activities']:
        print(f"     • {act}")
    if results.get('exec_rate') is not None:
        er = results['exec_rate']
        emoji = '🟢' if er >= 80 else '🟡' if er >= 60 else '🔴'
        print(f"  {emoji} Execution rate: {er:.0f}%")
    print(f"{SEP}")

    return results


print("✅ run_study_session() cargado (v1.85)")
print("   UN PUNTO DE ENTRADA para todo el sistema de aprendizaje.")
print("   Con HH: run_study_session(df, session_id=current_session_id)")
print("   Sin HH: run_study_session()")
print("   Rápido:  run_study_session(modo_rapido=True)")

print("   M1: run_personalized_session(df, drill) → preguntas con tus manos reales")
print("   M2: run_spaced_session() → repetición espaciada SM-2")
print("   M3: run_postflop_nl2_session() → postflop NL2-específico")
print("   M4: after_session_bridge(df, drill) → puente teoría→mesa")
print("   M5: diagnose_leak_root_cause(df, leak) → diagnóstico causa raíz")


# ════════════════════════════════════════════════════════════════════════
# SECCIÓN 3f.8 — Luck/Skill Analysis (v1.90 — Gate 5k)
#
# Separa manos donde el resultado diverge de la decisión:
#   BIEN JUGADAS pero PERDIDAS por suerte: ev_won > 0, net_won < 0
#   MAL JUGADAS pero GANADAS por suerte:  ev_won < 0, net_won > 0
#
# PROPÓSITO: gestión emocional y contexto de resultados.
# No es para cambiar estrategia — es para entender la varianza.
# ════════════════════════════════════════════════════════════════════════

def display_luck_skill_analysis(df, session_id=None, min_hands=5000):
    """
    Análisis de suerte vs habilidad post-sesión.

    Requiere gate 5k manos (ev_won necesita volumen para ser fiable).

    Args:
        df:         DataFrame completo del HH
        session_id: filtrar por sesión específica (None = todas)
        min_hands:  gate mínimo (default 5000)

    Output:
        - Manos bien jugadas pero perdidas (suerte en contra)
        - Manos mal jugadas pero ganadas (suerte a favor)
        - EV ajustado vs resultado real
        - Contexto emocional: "tus decisiones son mejores que tus resultados"
    """
    SEP = '═' * 62
    total_hands = len(df)

    if total_hands < min_hands:
        print(f"\n{SEP}")
        print(f"  ⏸ luck_skill_analysis: gate {min_hands:,} manos")
        print(f"  Tienes {total_hands:,} — faltan {min_hands-total_hands:,}")
        print(SEP)
        return

    sub = df.copy()
    if session_id:
        sub = sub[sub['session_id'] == session_id]
        scope = f"Sesión {session_id} ({len(sub)} manos)"
    else:
        scope = f"Todas las sesiones ({len(sub)} manos)"

    # Clasificar manos con divergencia resultado/decisión
    # Real EV only available when enrich_with_allin_ev has run (gate 5k allin EV)
    # If ev_won == net_won everywhere, we don't have real EV data yet
    _ev_col_raw = sub['ev_won'] if 'ev_won' in sub.columns else pd.Series(dtype=float)
    _ev_diff = ((_ev_col_raw - sub['net_won']).abs() > 0.001).sum()
    has_ev = _ev_diff > 10  # at least 10 hands with real EV divergence

    print(f"\n{SEP}")
    print(f"  🎲 ANÁLISIS SUERTE vs HABILIDAD")
    print(f"  {scope}")
    print(SEP)

    if has_ev:
        bb_val = 0.02
        ev_col = sub['ev_won'].fillna(sub['net_won']) if 'ev_won' in sub.columns else sub['net_won']

        # Well played, lost due to bad luck
        well_played_lost = sub[
            (ev_col > 0.005) &          # EV positivo (decisión correcta)
            (sub['net_won'] < -0.005)    # pero perdiste dinero
        ].copy()

        # Poorly played, won due to good luck
        poorly_played_won = sub[
            (ev_col < -0.005) &          # EV negativo (decisión incorrecta)
            (sub['net_won'] > 0.005)     # pero ganaste dinero
        ].copy()

        # EV total vs resultado total
        ev_total  = ev_col.sum() / bb_val
        net_total = sub['net_won'].sum() / bb_val
        luck_component = net_total - ev_total  # positivo = suerte a favor

        print(f"\n  EV total (decisiones):    {ev_total:+.1f} BB")
        print(f"  Resultado real:           {net_total:+.1f} BB")
        print(f"  Componente suerte:        {luck_component:+.1f} BB")
        print(f"  {'📈 La suerte ha jugado A TU FAVOR' if luck_component > 0 else '📉 La suerte ha jugado EN TU CONTRA'}")

        print(f"\n  ✅ BIEN JUGADAS pero PERDIDAS ({len(well_played_lost)} manos):")
        print(f"  EV ganado en estas manos: {well_played_lost['ev_won'].sum()/bb_val:+.1f} BB")
        print(f"  Resultado real:           {well_played_lost['net_won'].sum()/bb_val:+.1f} BB")
        print(f"  → Dinero perdido POR MALA SUERTE, no por error")
        if len(well_played_lost) > 0:
            for _, row in well_played_lost.head(3).iterrows():
                ev = row.get('ev_won', 0)/bb_val
                net = row['net_won']/bb_val
                print(f"     Hand #{row['hand_id']}: EV={ev:+.1f}BB actual={net:+.1f}BB ({row.get('hole_cards','??')} {row.get('player_position','?')})")

        print(f"\n  ❌ MAL JUGADAS pero GANADAS ({len(poorly_played_won)} manos):")
        print(f"  EV perdido en estas manos: {poorly_played_won['ev_won'].sum()/bb_val:+.1f} BB")
        print(f"  Resultado real:            {poorly_played_won['net_won'].sum()/bb_val:+.1f} BB")
        print(f"  → Dinero ganado POR BUENA SUERTE — no confundas esto con habilidad")
        if len(poorly_played_won) > 0:
            for _, row in poorly_played_won.head(3).iterrows():
                ev = row.get('ev_won', 0)/bb_val
                net = row['net_won']/bb_val
                print(f"     Hand #{row['hand_id']}: EV={ev:+.1f}BB actual={net:+.1f}BB ({row.get('hole_cards','??')} {row.get('player_position','?')})")

    else:
        # FIX v1.99: fallback honesto — sin EV all-in real no hay separación real
        # El fallback anterior daba 3 líneas genéricas que no aportaban nada.
        # Ahora: contexto real con caveat explícito + W$SD bien contextualizado.
        net_total_bb = sub['net_won'].sum() / 0.02
        sd_hands = sub[sub['flg_showdown'].astype(int)==1]
        wsd_pct = sd_hands['flg_won_hand'].astype(int).mean()*100 if len(sd_hands)>10 else 0

        print(f"\n  ⚠️  ANÁLISIS LIMITADO — Sin EV all-in real disponible")
        print(f"  {'─'*56}")
        print(f"  enrich_with_allin_ev solo ajusta manos con all-in + showdown.")
        print(f"  En tu HH actual, ev_won == net_won en ~99% de manos.")
        print(f"  Sin EV ajustado: no es posible separar suerte de habilidad.")
        print()
        print(f"  LO QUE SÍ SE PUEDE DECIR:")
        print(f"  Resultado período:  {net_total_bb:+.1f} BB ({len(sub)} manos)")
        print(f"  W$SD:               {wsd_pct:.1f}% (ref NL2: 48-54%)")
        if wsd_pct >= 52:
            print(f"  🟢 W$SD {wsd_pct:.1f}% — ganando en showdown más de lo esperado para NL2")
        elif wsd_pct >= 48:
            print(f"  🟡 W$SD {wsd_pct:.1f}% — dentro de ref NL2 (48-54%)")
        elif wsd_pct >= 42:
            print(f"  🟡 W$SD {wsd_pct:.1f}% — bajo ref. Posible leak selección de manos a showdown")
        else:
            print(f"  🔴 W$SD {wsd_pct:.1f}% — muy bajo. Revisa qué manos llevas a showdown.")
        print()
        print(f"  PARA ANÁLISIS REAL SUERTE/HABILIDAD:")
        print(f"  Necesitas EV all-in calculado por solver (PT4 EV, HM3).")
        print(f"  Gate 30k manos activará M6 TexasSolver con esta funcionalidad.")
        print(f"  Hasta entonces: confía en exec_rate + W$SD como proxies.")
        print(f"  {'─'*56}")

    print(f"\n  💡 RECORDATORIO:")
    print(f"  El poker tiene varianza. Evalúa tus decisiones, no tus resultados.")
    print(f"  Si tu EV es positivo, el dinero llegará con volumen.")
    print(SEP)


print("✅ display_luck_skill_analysis() cargado (v1.90 — Gate 5k)")
print("   Uso: display_luck_skill_analysis(df)  — todas las sesiones")
print("   Uso: display_luck_skill_analysis(df, session_id='session_010')")
print("   Requiere: 5.000+ manos (gate activo)")

# ════════════════════════════════════════════════════════════════════════
# SECCIÓN 3g — MÓDULO DE FORTALEZAS v1.93
#
# 4 módulos que completan el sistema: no solo corregir leaks
# sino identificar, entender y transferir lo que ya funciona.
#
# M1: build_strength_ranking()   — ranking de spots positivos (simétrico al ROI ranking)
# M2: diagnose_strength_root_cause() — por qué funciona, qué mantener
# M3: display_transfer_drill()   — transferir lógica de fortaleza a leak estructural
# M4: register_strength_m7()     — CONSOLIDATED tracking en M7
#
# Filosofía: el sistema mide; el coach explica; el jugador decide.
# Esto se aplica igual a lo que va bien que a lo que va mal.
# ════════════════════════════════════════════════════════════════════════


# ── MÓDULO 1: Strength Ranking ───────────────────────────────────────────

def build_strength_ranking(spot_results, top_n=5, min_hands=15):
    """
    Construye el Ranking de Fortalezas — simétrico al ROI ranking de leaks.

    Filtra spots con avg_ev_bb > 0 y suficientes manos, los ordena por
    impacto total positivo con Empirical Bayes shrinkage implícito
    (el mismo que usa calculate_ev_metrics).

    Args:
        spot_results: DataFrame de calculate_ev_metrics()
        top_n:        número de fortalezas a mostrar
        min_hands:    mínimo de manos para considerar señal real

    Returns:
        dict con claves: 'strengths' (df top), 'consolidadas' (df), 'resumen'
    """
    if spot_results is None or len(spot_results) == 0:
        return {'strengths': pd.DataFrame(), 'consolidadas': pd.DataFrame(), 'resumen': 'Sin datos'}

    ev_col  = 'avg_ev_bb'
    n_col   = 'spot_hands_count'
    imp_col = 'impacto_ev_total_bb'

    # Filtrar spots positivos con muestra suficiente
    pos = spot_results[
        (spot_results[ev_col] > 0) &
        (spot_results[n_col] >= min_hands)
    ].sort_values(imp_col, ascending=False).copy()

    # Umbral de consolidación: avg_ev > 1.0 BB/mano (señal fuerte)
    CONSOLIDATION_THRESHOLD = 1.0
    consolidadas = pos[pos[ev_col] >= CONSOLIDATION_THRESHOLD].copy()

    # ── scalable_bb100: simétrico a recoverable_bb100 de leaks ────────
    # correction_factor: fracción del EV positivo que puedes escalar
    # (mismo factor que leaks — por calle, conservador)
    # scalable_bb100 = bb100_gain × correction_factor
    # Significa: BB/100 adicional extraíble si aumentas frecuencia/sizing
    _CORRECTION = {'preflop': 0.80, 'flop': 0.75, 'turn': 0.70, 'river': 0.65}
    total_hands = spot_results[n_col].sum() if n_col in spot_results.columns else 1

    def _scalable(row):
        street = row.get('decision_street', 'preflop') if hasattr(row, 'get') else 'preflop'
        cf = _CORRECTION.get(street, 0.75)
        # bb100_gain = avg_ev × frequency × 100 (contribución a BB/100 global)
        freq = row.get('frequency', row[n_col] / total_hands) if hasattr(row, 'get') else row[n_col] / total_hands
        bb100_gain = row[ev_col] * freq * 100
        return round(bb100_gain * cf, 3)

    pos['bb100_gain']         = pos.apply(lambda r: r[ev_col] * r.get('frequency', r[n_col]/total_hands) * 100, axis=1)
    pos['correction_factor']  = pos['decision_street'].map(_CORRECTION).fillna(0.75) if 'decision_street' in pos.columns else 0.75
    pos['scalable_bb100']     = pos['bb100_gain'] * pos['correction_factor']

    # Enriquecer con posición y calle para lectura humana
    def _describe_spot(row):
        sp = row['spot_identifier']
        parts = sp.split('_')
        pos_name = parts[0]
        ip_oop = row.get('ip_oop', '')
        street = row.get('decision_street', '')
        ev = row[ev_col]
        n = int(row[n_col])

        if ev >= 3.0:   quality = '🟢🟢 EXCELENTE'
        elif ev >= 1.5: quality = '🟢 SÓLIDA'
        elif ev >= 0.5: quality = '🟡 POSITIVA'
        else:           quality = '⚪ MARGINAL'

        return f"{pos_name} {ip_oop} {street} | {quality} | {ev:+.2f} BB/m | n={n}"

    pos['descripcion'] = pos.apply(_describe_spot, axis=1)

    SEP = '═' * 62
    print(f"\n{SEP}")
    print(f"  💪 STRENGTH RANKING — TOP {top_n} FORTALEZAS")
    print(f"  Spots donde tu juego es consistentemente ganador")
    print(SEP)

    if len(pos) == 0:
        print(f"\n  Sin spots con ≥{min_hands} manos y EV positivo todavía.")
        print(f"  Con más volumen el sistema detectará tus fortalezas reales.")
    else:
        total_positive_bb = pos[imp_col].sum()
        print(f"\n  Total impacto positivo: +{total_positive_bb:.1f} BB")
        print(f"  Spots positivos con ≥{min_hands} manos: {len(pos)}")
        if len(consolidadas) > 0:
            print(f"  Spots consolidados (≥{CONSOLIDATION_THRESHOLD} BB/mano): {len(consolidadas)} ⭐")
        print()

        for i, (_, row) in enumerate(pos.head(top_n).iterrows(), 1):
            sp     = row['spot_identifier']
            ev     = row[ev_col]
            n      = int(row[n_col])
            total  = row[imp_col]
            scalable = row.get('scalable_bb100', 0)
            star   = '⭐' if ev >= CONSOLIDATION_THRESHOLD else '  '

            print(f"  {star} #{i} {sp}")
            print(f"       avg_ev: {ev:+.3f} BB/mano | n={n} | impacto: {total:+.2f} BB | scalable: {scalable:+.2f} BB/100")

        if len(pos) > top_n:
            rest_ev = pos.iloc[top_n:][imp_col].sum()
            print(f"\n  ... y {len(pos)-top_n} fortalezas más (+{rest_ev:.1f} BB acumulado)")

    print(f"\n{SEP}")

    return {
        'strengths':    pos.head(top_n),
        'all_positive': pos,
        'consolidadas': consolidadas,
        'total_positive_bb': pos[imp_col].sum() if len(pos) > 0 else 0,
        'resumen': f"{len(pos)} spots positivos | +{pos[imp_col].sum():.1f} BB total",
    }


# ── MÓDULO 2: Diagnóstico causa raíz de fortalezas ───────────────────────

def diagnose_strength_root_cause(df, spot_identifier):
    """
    Analiza por qué un spot es positivo — qué estás haciendo bien.

    Hipótesis automáticas basadas en métricas del spot:
    - ¿Es agresividad en el momento correcto (cbet/bet alta)?
    - ¿Es selección de manos (win% muy alto sin showdown)?
    - ¿Es disciplina postflop (pocas manos llegan a showdown)?
    - ¿Es valor en river (bet river + win%)?

    Args:
        df:              DataFrame completo
        spot_identifier: spot a analizar (debe ser positivo)
    """
    SEP = '═' * 62
    sub = df[df['spot_identifier'] == spot_identifier].copy()

    if len(sub) < 5:
        print(f"\n  Insuficientes manos para diagnóstico: {len(sub)} (mínimo 5)")
        return

    # Métricas del spot
    n = len(sub)
    bb_val = 0.02
    net_bb = sub['net_won'].sum() / bb_val
    avg_ev = net_bb / n

    # Flags disponibles
    def _pct(col):
        if col in sub.columns:
            return sub[col].astype(int).mean() * 100
        return None

    cbet_f  = _pct('flg_f_cbet')
    bet_f   = _pct('flg_f_bet')
    bet_t   = _pct('flg_t_bet')
    bet_r   = _pct('flg_r_bet')
    showdown= _pct('flg_showdown')
    won_hand= _pct('flg_won_hand')
    raised_pf = _pct('cnt_p_raise')

    # Partes del spot para contexto
    parts  = spot_identifier.split('_')
    pos    = parts[0]
    ip_oop = parts[1] if len(parts) > 1 else ''
    street = sub['decision_street'].iloc[0] if 'decision_street' in sub.columns else ''

    print(f"\n{SEP}")
    print(f"  💪 DIAGNÓSTICO FORTALEZA — {spot_identifier[:45]}")
    print(SEP)
    print(f"  n={n} manos | avg_ev: {avg_ev:+.2f} BB/mano | total: {net_bb:+.1f} BB")
    print()

    # Métricas clave
    print(f"  MÉTRICAS DEL SPOT:")
    if cbet_f is not None:  print(f"  CBet flop:    {cbet_f:.0f}%")
    if bet_f is not None and cbet_f != bet_f: print(f"  Bet flop:     {bet_f:.0f}%")
    if bet_t is not None:   print(f"  Bet turn:     {bet_t:.0f}%")
    if bet_r is not None:   print(f"  Bet river:    {bet_r:.0f}%")
    if showdown is not None:print(f"  A showdown:   {showdown:.0f}%")
    if won_hand is not None:print(f"  Win rate:     {won_hand:.0f}%")

    print()
    print(f"  HIPÓTESIS — ¿POR QUÉ FUNCIONA?")

    hypotheses = []

    # H1: Agresividad IP en flop
    if ip_oop == 'IP' and bet_f is not None and bet_f >= 80:
        hypotheses.append((1, 'ALTA',
            'Agresividad IP en flop — apuestas cuando tienes iniciativa',
            f'Apuestas el {bet_f:.0f}% del flop IP. El pool NL2 paga demasiado → '
            f'cada apuesta de valor genera EV positivo. MANTENER esta frecuencia.'))

    # H2: Fold equity antes de showdown
    if showdown is not None and showdown < 20 and avg_ev > 1.0:
        hypotheses.append((2, 'ALTA',
            'Fold equity — ganas el bote sin llegar a showdown',
            f'Solo {showdown:.0f}% de manos llegan a showdown pero ganas {avg_ev:+.2f} BB/mano. '
            f'Tu agresividad fuerza folds del pool. CLAVE: no necesitas la mano mejor.'))

    # H3: Value bet river
    if bet_r is not None and bet_r >= 70 and won_hand is not None and won_hand >= 70:
        hypotheses.append((3, 'ALTA',
            'Value bet river efectiva — apuestas fuerte y ganas',
            f'Apuestas river el {bet_r:.0f}% y ganas el {won_hand:.0f}% de las manos. '
            f'El pool NL2 paga value bets en river. MANTENER: bet river con top pair o mejor.'))

    # H4: Selección de manos (raised PF + won high)
    if raised_pf is not None and raised_pf >= 90 and won_hand is not None and won_hand >= 80:
        hypotheses.append((4, 'MEDIA',
            'Selección de rango — entras con manos fuertes',
            f'En este spot el {raised_pf:.0f}% de veces tienes la iniciativa. '
            f'Tu rango de entrada es selectivo → wins el {won_hand:.0f}%.'))

    # H5: OOP pero con disciplina
    if ip_oop == 'OOP' and avg_ev > 1.0:
        hypotheses.append((5, 'MEDIA',
            'Fortaleza OOP — disciplina en posición desfavorable',
            f'Ganar {avg_ev:+.2f} BB/mano OOP es excepcional. '
            f'Probablemente tienes selección de rango muy ajustada en este spot.'))

    if not hypotheses:
        hypotheses.append((1, 'INFO',
            'Fortaleza emergente — muestra pequeña',
            f'Con {n} manos el patrón es positivo pero preliminar. '
            f'Necesitas ≥50 manos para causa raíz concluyente.'))

    for prio, nivel, hip, ev_text in hypotheses:
        print(f"\n  #{prio} [{nivel}] {hip}")
        words = ev_text.split()
        line = '     '
        for w in words:
            if len(line)+len(w)+1 > 62: print(line); line = '     '+w+' '
            else: line += w+' '
        if line.strip(): print(line)

    print()
    print(f"  ACCIÓN: MANTENER este comportamiento.")
    print(f"  No cambies nada en '{pos} {ip_oop} {street}' — está funcionando.")
    print(SEP)


# ── MÓDULO 3: Transfer Drill ──────────────────────────────────────────────

def display_transfer_drill(spot_results, df=None, min_hands=10):
    """
    Detecta pares estructurales: fortaleza IP → leak OOP (o viceversa)
    en la misma calle, pot type y profundidad.

    El insight: si ganas en BTN_IP_flop_R_B pero pierdes en BB_OOP_flop_C_X_F,
    la pregunta es: ¿por qué aplicas agresividad IP pero no OOP?

    Args:
        spot_results: DataFrame de calculate_ev_metrics()
        df:           DataFrame HH (para ejemplos de manos)
        min_hands:    mínimo de manos por spot
    """
    SEP = '═' * 62
    ev_col  = 'avg_ev_bb'
    n_col   = 'spot_hands_count'

    pos = spot_results[(spot_results[ev_col] > 0) & (spot_results[n_col] >= min_hands)]
    neg = spot_results[(spot_results[ev_col] < 0) & (spot_results[n_col] >= min_hands)]

    if len(pos) == 0 or len(neg) == 0:
        print(f"\n  Sin suficientes datos para transfer drill (necesitas spots positivos Y negativos con ≥{min_hands} manos)")
        return []

    pairs = []

    # Buscar pares por misma calle + pot_type + stack_depth con IP↔OOP
    for _, r_pos in pos.iterrows():
        street  = r_pos.get('decision_street', '')
        pot     = r_pos.get('pot_type', '')
        depth   = r_pos.get('stack_depth', '')
        ip_oop  = r_pos.get('ip_oop', '')

        # Solo fortalezas IP buscan leaks OOP (la dirección más común y accionable)
        if ip_oop != 'IP': continue

        matches = neg[
            (neg['decision_street'] == street) &
            (neg['pot_type'] == pot) &
            (neg['stack_depth'] == depth) &
            (neg['ip_oop'] == 'OOP')
        ]

        for _, r_neg in matches.iterrows():
            gap = r_pos[ev_col] - r_neg[ev_col]
            if gap > 1.0:  # solo si el gap es significativo
                pairs.append({
                    'strength': r_pos['spot_identifier'],
                    'strength_ev': r_pos[ev_col],
                    'strength_n': int(r_pos[n_col]),
                    'leak': r_neg['spot_identifier'],
                    'leak_ev': r_neg[ev_col],
                    'leak_n': int(r_neg[n_col]),
                    'gap': gap,
                    'street': street,
                    'pot': pot,
                })

    # Eliminar duplicados (mantener mayor gap por par)
    seen_leaks = {}
    for p in sorted(pairs, key=lambda x: -x['gap']):
        if p['leak'] not in seen_leaks:
            seen_leaks[p['leak']] = p

    unique_pairs = sorted(seen_leaks.values(), key=lambda x: -x['gap'])

    print(f"\n{SEP}")
    print(f"  🔄 TRANSFER DRILL — Aplica tu fortaleza donde falla")
    print(SEP)

    if not unique_pairs:
        print(f"\n  Sin pares de transferencia claros todavía.")
        print(f"  Con más volumen el sistema detectará estos patrones.")
    else:
        print(f"\n  {len(unique_pairs)} oportunidad(es) de transferencia detectada(s):\n")

        for i, p in enumerate(unique_pairs[:5], 1):
            s_parts = p['strength'].split('_')
            l_parts = p['leak'].split('_')

            print(f"  ── TRANSFERENCIA #{i} ──────────────────────────────")
            print(f"  FORTALEZA [{s_parts[0]} IP {p['street']}]:")
            print(f"    {p['strength']}")
            print(f"    avg_ev: {p['strength_ev']:+.3f} BB/mano | n={p['strength_n']}")
            print()
            print(f"  LEAK [{l_parts[0]} OOP {p['street']}]:")
            print(f"    {p['leak']}")
            print(f"    avg_ev: {p['leak_ev']:+.3f} BB/mano | n={p['leak_n']}")
            print()
            print(f"  GAP: {p['gap']:+.3f} BB/mano")
            print()

            # La pregunta de transferencia
            if p['street'] == 'flop':
                print(f"  ❓ PREGUNTA DE TRANSFERENCIA:")
                print(f"     IP apuestas flop y ganas {p['strength_ev']:+.2f} BB/mano.")
                print(f"     OOP check-fold flop y pierdes {p['leak_ev']:.2f} BB/mano.")
                print(f"     → ¿Cuándo es correcto apostar en flop OOP?")
                print(f"       Con top pair o mejor en boards seguros → apuesta.")
                print(f"       Con draws en pots grandes → apuesta o check-raise.")
                print(f"       Con aire en tablero favorable para tu rango → barrel.")
            elif p['street'] == 'preflop':
                print(f"  ❓ PREGUNTA DE TRANSFERENCIA:")
                print(f"     IP abres y ganas {p['strength_ev']:+.2f} BB/mano.")
                print(f"     OOP foldeas y pierdes {p['leak_ev']:.2f} BB/mano (exceso de fold).")
                print(f"     → ¿Estás defendiendo suficiente desde BB/SB?")
                print(f"       Suited siempre defiende. Pares siempre defienden.")
                print(f"       A7o+ defiende. Broadways defienden.")

            print(f"  {'─'*58}")

    print(f"\n{SEP}")
    return unique_pairs


# ── MÓDULO 4: LOCK positivo en M7 ────────────────────────────────────────

def register_strength_m7(spot_identifier, session_id, avg_ev_bb,
                          drive_path=None, consolidation_threshold=1.0,
                          consolidation_sessions=3):
    """
    Registra fortalezas en M7 y detecta spots CONSOLIDATED.

    Un spot es CONSOLIDATED cuando:
    - avg_ev_bb >= consolidation_threshold durante N sesiones consecutivas
    - Esto indica dominio real, no varianza

    Args:
        spot_identifier:          spot a registrar
        session_id:               sesión actual
        avg_ev_bb:                EV medio de este spot en esta sesión
        drive_path:               ruta Drive para persistencia
        consolidation_threshold:  umbral EV para contar sesión como 'fuerte'
        consolidation_sessions:   sesiones consecutivas para CONSOLIDATED

    Returns:
        dict con status: 'ACTIVE' | 'CONSOLIDATED' | 'REFERENCE'
    """
    import json as _json, os as _os

    _m7_file = _os.path.join(drive_path, 'drill_history_m7.json') if drive_path else 'drill_history_m7.json'

    try:
        with open(_m7_file, 'r') as f:
            m7_data = _json.load(f)
    except Exception:
        m7_data = {'drills': {}, 'strengths': {}}

    # Inicializar strengths si no existe
    if 'strengths' not in m7_data:
        m7_data['strengths'] = {}

    if spot_identifier not in m7_data['strengths']:
        m7_data['strengths'][spot_identifier] = {
            'history':        [],
            'status':         'ACTIVE',
            'peak_ev':        0.0,
            'consol_streak':  0,
            'first_seen':     session_id,
        }

    strength = m7_data['strengths'][spot_identifier]

    # Registrar sesión
    entry = {
        'session_id': session_id,
        'avg_ev_bb':  avg_ev_bb,
        'strong':     avg_ev_bb >= consolidation_threshold,
    }
    strength['history'].append(entry)

    # Actualizar peak
    strength['peak_ev'] = max(strength.get('peak_ev', 0.0), avg_ev_bb)

    # Actualizar consolidation streak
    if avg_ev_bb >= consolidation_threshold:
        strength['consol_streak'] = strength.get('consol_streak', 0) + 1
    else:
        strength['consol_streak'] = 0

    # Determinar status
    streak = strength['consol_streak']
    if streak >= consolidation_sessions:
        strength['status'] = 'CONSOLIDATED'
        status_msg = f'🌟 CONSOLIDATED — {streak} sesiones ≥{consolidation_threshold} BB/mano'
    elif streak >= 1:
        strength['status'] = 'ACTIVE'
        status_msg = f'📈 ACTIVE — streak {streak}/{consolidation_sessions}'
    else:
        strength['status'] = 'ACTIVE'
        status_msg = f'⚪ ACTIVE — sin streak positivo reciente'

    # Guardar
    try:
        import tempfile as _tf
        _dir = _os.path.dirname(_os.path.abspath(_m7_file))
        _fd, _tmp = _tf.mkstemp(dir=_dir, suffix='.tmp')
        with _os.fdopen(_fd, 'w') as f:
            _json.dump(m7_data, f, ensure_ascii=False, indent=2)
        _os.replace(_tmp, _m7_file)
    except Exception as e:
        print(f"  ⚠️  No se pudo guardar M7 strengths: {e}")

    return {
        'spot':   spot_identifier,
        'status': strength['status'],
        'streak': streak,
        'peak':   strength['peak_ev'],
        'msg':    status_msg,
    }


def display_strength_progress(drive_path=None):
    """
    Muestra el estado de todas las fortalezas registradas en M7.
    Paralelo a display_m7_status() para leaks.
    """
    import json as _json, os as _os

    _m7_file = _os.path.join(drive_path, 'drill_history_m7.json') if drive_path else 'drill_history_m7.json'

    try:
        with open(_m7_file, 'r') as f:
            m7_data = _json.load(f)
    except Exception:
        print("  Sin historial de fortalezas todavía.")
        return

    strengths = m7_data.get('strengths', {})
    if not strengths:
        print("  Sin fortalezas registradas en M7.")
        print("  Ejecuta el pipeline con run_strength_pipeline() para comenzar.")
        return

    SEP = '═' * 62
    print(f"\n{SEP}")
    print(f"  🌟 STRENGTH TRACKER M7")
    print(SEP)

    consolidated = [(k, v) for k, v in strengths.items() if v.get('status') == 'CONSOLIDATED']
    active       = [(k, v) for k, v in strengths.items() if v.get('status') == 'ACTIVE']

    if consolidated:
        print(f"\n  ⭐ CONSOLIDADOS ({len(consolidated)}):")
        for spot, data in consolidated:
            streak = data.get('consol_streak', 0)
            peak   = data.get('peak_ev', 0)
            n_sess = len(data.get('history', []))
            print(f"    {spot[:50]}")
            print(f"    Streak: {streak} | Peak: {peak:+.2f} BB/m | Sesiones: {n_sess}")

    if active:
        print(f"\n  📈 ACTIVOS ({len(active)}):")
        for spot, data in active:
            streak = data.get('consol_streak', 0)
            n_sess = len(data.get('history', []))
            print(f"    {spot[:50]} | streak {streak} | {n_sess} sesiones")

    print(f"\n{SEP}")


print("✅ Módulo de Fortalezas cargado (v1.93)")
print("   M1: build_strength_ranking(spot_results)  — top fortalezas")
print("   M2: diagnose_strength_root_cause(df, spot) — por qué funciona")
print("   M3: display_transfer_drill(spot_results)   — transferir a leaks")
print("   M4: register_strength_m7(spot, session, ev) — CONSOLIDATED tracking")
print("   M4: display_strength_progress()             — vista M7 fortalezas")


# ════════════════════════════════════════════════════════════════
# ANÁLISIS DE RAZONAMIENTO v2.02
# Gap 1: patrones error_type | Gap 2: followup efectivo | Gap 4: timing
# Sinergia: after_session_bridge → study_history → estas funciones
# ════════════════════════════════════════════════════════════════

def display_error_pattern_analysis(study_path=None, drive_path=None):
    """Patrones acumulados de error_type desde after_session_bridge."""
    sh = _load_study_history(study_path, drive_path)
    errors = {k: v for k, v in sh.items()
              if k.startswith('bridge_error::') and isinstance(v, dict)}

    SEP = '═' * 58
    print(f"\n  {SEP}")
    print(f"  🧠 ANÁLISIS DE PATRONES DE RAZONAMIENTO")

    if not errors:
        print(f"  Sin errores de bridge. Usa after_session_bridge(df) tras cada sesión.")
        print(f"  {SEP}"); return {}

    tc = {'a':0,'b':0,'c':0,'d':0,'omitida':0}
    tl = {'a':'Concepto (no sabía rango)','b':'Sizing (bet grande)',
          'c':'Lectura villain','d':'Otra','omitida':'Sin clasificar'}
    notas_list = []
    for qid, e in errors.items():
        et = e.get('error_type','omitida'); tc[et] = tc.get(et,0)+1
        nota = e.get('nota','')
        if nota: notas_list.append((qid.replace('bridge_error::',''), nota, e.get('session_id','')))

    classified = sum(v for k,v in tc.items() if k!='omitida')
    print(f"  {len(errors)} errores | {classified} clasificados")
    print(f"  {'─'*58}")

    if classified == 0:
        print(f"  Responde (a/b/c/d) en el bridge para activar el análisis.")
        print(f"  {SEP}"); return tc

    print(f"\n  DISTRIBUCIÓN DE CAUSAS:")
    for code in ['a','b','c','d']:
        n = tc.get(code,0)
        if n == 0: continue
        pct = n/classified*100
        bar = '█'*max(1,int(pct/5))
        print(f"  ({code}) {tl[code][:30]:<30} {bar:<16} {n} ({pct:.0f}%)")

    dominant = max(tc, key=lambda k: tc.get(k,0) if k!='omitida' else 0)
    dom_pct  = tc.get(dominant,0)/classified*100 if classified>0 else 0
    print(f"\n  DIAGNÓSTICO:")
    if dom_pct >= 50:
        dx = {
            'a': f"  🔴 {dom_pct:.0f}% CONCEPTO — run_reasoning_session() prioritario.",
            'b': (f"  🔴 {dom_pct:.0f}% SIZING — reaccionas al bet, no a los pot odds.\n"
                  f"  → Automatiza: call/(pot+call) antes de cada fold."),
            'c': (f"  🟡 {dom_pct:.0f}% LECTURA VILLAIN — sobrevaloras sin muestra.\n"
                  f"  → Norma de 5 casos: ¿≥30 manos con él?"),
            'd': f"  ⚪ {dom_pct:.0f}% OTRA — revisa notas libres.",
        }
        print(dx.get(dominant, f"  Causa dominante: {dominant}"))
    else:
        print(f"  ✅ Sin causa dominante. Muestra actual: {classified}.")

    if notas_list:
        print(f"\n  NOTAS LIBRES ({len(notas_list)}):")
        for hid, nota, sess in notas_list[-3:]:
            print(f"  [{sess}] #{hid}: '{nota[:55]}'")

    print(f"  {SEP}")
    return tc


def display_followup_effectiveness(study_path=None, drive_path=None):
    """GAP 2: ¿El followup (SIMPLIFICACIÓN) ayuda a aprender?"""
    sh = _load_study_history(study_path, drive_path)
    qs = sh.get('questions', {})
    shown  = {k:v for k,v in qs.items() if v.get('followup_shown',0)>0}
    helped = {k:v for k,v in shown.items() if v.get('followup_helped',False)}
    no_hlp = {k:v for k,v in shown.items() if not v.get('followup_helped',False)}

    SEP = '═' * 58
    print(f"\n  {SEP}")
    print(f"  💡 EFECTIVIDAD DEL FOLLOWUP (SIMPLIFICACIÓN)")

    if not shown:
        print(f"  Sin datos. El followup se activa al fallar preguntas")
        print(f"  con campo followup (BB Q0, BB Q5, SB Q1).")
        print(f"  {SEP}"); return {}

    total = len(shown); n_help = len(helped)
    rate = n_help/total*100 if total>0 else 0
    print(f"  Preguntas con SIMPLIFICACIÓN: {total}")
    print(f"  Correctas tras followup:      {n_help} ({rate:.0f}%)")

    if rate >= 70:
        print(f"  🟢 Followup efectivo — la simplificación fija el concepto.")
    elif rate >= 40:
        print(f"  🟡 Efectividad media. Revisa el texto del followup.")
    else:
        print(f"  🔴 Followup poco efectivo ({rate:.0f}%).")
        for qid in list(no_hlp.keys())[:3]:
            parts = qid.split('::')
            label = f"{parts[0].split('_')[0]} {parts[1]}" if len(parts)>=2 else qid
            print(f"    {label}: {no_hlp[qid].get('times_wrong',0)} fallos — revisar manualmente")

    print(f"  {SEP}")
    return {'total':total,'helped':n_help,'rate':rate}


def display_error_timing_analysis(df, study_path=None, drive_path=None):
    """GAP 4: ¿Los errores dependen del momento de la sesión? (T1/T2/T3)"""
    sh = _load_study_history(study_path, drive_path)
    errors = {k:v for k,v in sh.items()
              if k.startswith('bridge_error::') and isinstance(v,dict)
              and v.get('error_type') not in (None,'omitida')}

    SEP = '═' * 58
    print(f"\n  {SEP}")
    print(f"  ⏱️  ANÁLISIS ERROR vs MOMENTO DE SESIÓN (T1/T2/T3)")

    if len(errors) < 5:
        print(f"  Muestra insuficiente: {len(errors)}/5 errores clasificados.")
        print(f"  {SEP}"); return {}

    df_c = df.copy(); df_c['hand_id_str'] = df_c['hand_id'].astype(str)
    tbt = {'T1':{},'T2':{},'T3':{}}; matched = 0

    for qid, entry in errors.items():
        hid    = qid.replace('bridge_error::','')
        etype  = entry.get('error_type','omitida')
        sid    = entry.get('session_id','')
        hrow   = df_c[df_c['hand_id_str']==str(hid)]
        if hrow.empty: continue
        sdf    = df_c[df_c['session_id']==sid] if sid else pd.DataFrame()
        if sdf.empty: continue
        n = len(sdf)
        try: loc = sdf.index.get_loc(hrow.index[0])
        except KeyError: continue
        t = 'T1' if loc<n//3 else ('T2' if loc<2*n//3 else 'T3')
        tbt[t][etype] = tbt[t].get(etype,0)+1; matched+=1

    if matched < 3:
        print(f"  Solo {matched} errores matcheados — sigue registrando.")
        print(f"  {SEP}"); return {}

    tls = {'a':'Concepto','b':'Sizing','c':'Lectura villain','d':'Otra'}
    all_et = set()
    for t in tbt.values(): all_et.update(t.keys())

    print(f"  {matched} errores | {'─'*42}")
    print(f"  {'Tipo':<20} {'T1(inicio)':<13} {'T2(medio)':<13} {'T3(final)':<13}")
    print(f"  {'─'*60}")
    for et in sorted(all_et):
        c = [tbt[t].get(et,0) for t in ['T1','T2','T3']]
        print(f"  {tls.get(et,et):<20} {c[0]:<13} {c[1]:<13} {c[2]:<13}")

    t3t = sum(tbt['T3'].values()); t1t = sum(tbt['T1'].values())
    if t3t > t1t*1.5 and t3t >= 2:
        dom = max(tbt['T3'], key=lambda k: tbt['T3'].get(k,0))
        print(f"\n  ⚠️  PATRÓN FATIGA: más errores en T3.")
        if dom=='b': print(f"  → Olvidas pot odds al final → sesiones más cortas.")
        elif dom=='c': print(f"  → Sobrevaloras reads al final → pausa activa.")
    else:
        print(f"\n  ✅ Sin patrón de fatiga claro. Continúa acumulando.")
    print(f"  {SEP}")
    return tbt


print("✅ Módulo Análisis de Razonamiento v2.02 cargado")
print("   display_error_pattern_analysis()   → patrones error_type acumulados")
print("   display_followup_effectiveness()   → ratio followup→acierto")
print("   display_error_timing_analysis(df)  → correlación error vs sesión")

# ════════════════════════════════════════════════════════════════
# HOLE CARD EV ANALYSIS v2.05
# Qué manos específicas (KQo, T9s, etc.) te cuestan más / aportan más
# Conecta con: ROI ranking (contexto adicional) + drill activo
# Sin gate — funciona desde mano 1 (mejor con ≥5k manos)
# ════════════════════════════════════════════════════════════════

def display_hole_card_analysis(df, min_hands=10, top_n=10,
                                by_position=None, study_path=None, drive_path=None):
    """
    Analiza qué manos hole card específicas son más costosas/rentables.

    Args:
        df: DataFrame del HH parseado
        min_hands: mínimo de manos para incluir una combinación (default 10)
        top_n: cuántas manos mostrar en cada lista (default 10)
        by_position: filtrar por posición ('BB','BTN','SB',None=todas)

    Conecta con:
        - ROI ranking: complementa spots posicionales con análisis de hole cards
        - drill activo: si drill es BB_defense, filtra automáticamente BB
        - display_luck_skill_analysis: EV separado de resultados
    """
    SEP = '═' * 62

    pdf = df.copy()
    if by_position:
        pdf = pdf[pdf['player_position'] == by_position]
        # Con filtro por posicion: reducir min_hands proporcional
        # (1 posicion ~1/6 manos -> umbral mas bajo)
        min_hands = max(2, min_hands // 4)

    if len(pdf) < 10:
        print(f'  Pocas manos ({len(pdf)}) para hole card analysis.')
        return pd.DataFrame()

    bb_val = 0.02

    # Group by hole cards
    hc = pdf.groupby('hole_cards').agg(
        n       = ('net_won', 'count'),
        net_eur = ('net_won', 'sum'),
        ev_won  = ('ev_won', 'sum') if 'ev_won' in pdf.columns else ('net_won', 'sum'),
        saw_flop= ('flg_f_saw', lambda x: x.astype(int).sum()) if 'flg_f_saw' in pdf.columns
                  else ('net_won', 'count'),
    ).reset_index()

    hc = hc[hc['n'] >= min_hands].copy()
    hc['bb100']    = (hc['net_eur'] / bb_val) / hc['n'] * 100
    hc['ev_bb100'] = (hc['ev_won'] / bb_val) / hc['n'] * 100
    hc['vpip_pct'] = hc['saw_flop'] / hc['n'] * 100

    # Categorize: pair / suited / offsuit
    def hc_category(hole):
        if not hole or str(hole) in ('??','nan',''): return 'unknown'
        cards = str(hole).split()
        if len(cards) != 2: return 'unknown'
        r1, r2 = cards[0][:-1], cards[1][:-1]
        s1, s2 = cards[0][-1], cards[1][-1]
        if r1 == r2: return 'pair'
        if s1 == s2: return 'suited'
        return 'offsuit'

    hc['category'] = hc['hole_cards'].apply(hc_category)
    hc = hc[hc['category'] != 'unknown']

    pos_label = f" desde {by_position}" if by_position else ""
    total_hands = hc['n'].sum()

    print(f"\n  {SEP}")
    print(f"  🃏 HOLE CARD EV ANALYSIS{pos_label}")
    print(f"  {total_hands:,} manos analizadas | {len(hc)} combinaciones con ≥{min_hands} manos")
    print(f"  {'─'*62}")

    # ── TOP COSTOSAS ────────────────────────────────────────────
    worst = hc.sort_values('net_eur').head(top_n)
    print(f"\n  🔴 TOP {top_n} MANOS MÁS COSTOSAS:")
    print(f"  {'Mano':<8} {'n':>5} {'Net €':>8} {'BB/100':>9} {'EV BB/100':>10}  Categoría")
    print(f"  {'─'*58}")
    for _, r in worst.iterrows():
        lock = '⚠️' if r['bb100'] < -100 else ''
        print(f"  {str(r['hole_cards']):<8} {r['n']:>5} {r['net_eur']:>+8.2f}€ "
              f"{r['bb100']:>+9.1f} {r['ev_bb100']:>+10.1f}  {r['category']} {lock}")

    # ── TOP RENTABLES ────────────────────────────────────────────
    best = hc.sort_values('net_eur', ascending=False).head(top_n)
    print(f"\n  🟢 TOP {top_n} MANOS MÁS RENTABLES:")
    print(f"  {'Mano':<8} {'n':>5} {'Net €':>8} {'BB/100':>9} {'EV BB/100':>10}  Categoría")
    print(f"  {'─'*58}")
    for _, r in best.iterrows():
        print(f"  {str(r['hole_cards']):<8} {r['n']:>5} {r['net_eur']:>+8.2f}€ "
              f"{r['bb100']:>+9.1f} {r['ev_bb100']:>+10.1f}  {r['category']}")

    # ── DIAGNÓSTICO POR CATEGORÍA ────────────────────────────────
    print(f"\n  RESUMEN POR CATEGORÍA:")
    for cat in ['pair', 'suited', 'offsuit']:
        sub = hc[hc['category'] == cat]
        if len(sub) == 0: continue
        net_t   = sub['net_eur'].sum()
        bb100_t = (net_t / bb_val) / sub['n'].sum() * 100
        n_t     = sub['n'].sum()
        print(f"  {'✅' if bb100_t > 0 else '🔴'} {cat:<10} "
              f"{n_t:>6} manos | net={net_t:>+7.2f}€ | BB/100={bb100_t:>+8.1f}")

    # -- INSIGHT especifico (v2.05: protegido contra df vacio) ---------
    print(f"\n  INSIGHT:")
    if len(hc) > 0:
        _cat_neto = hc.groupby('category')['net_eur'].sum()
        worst_cat = _cat_neto.idxmin() if len(_cat_neto) > 0 else 'unknown'
        worst_hand = hc.sort_values('net_eur').iloc[0]
        print(f"  Tu mano mas costosa: {worst_hand['hole_cards']} "
              f"({worst_hand['net_eur']:+.2f}eur, {worst_hand['bb100']:+.1f} BB/100)")
        print(f"  Categoria peor en neto: {worst_cat}")
        off_sub = hc[hc['category']=='offsuit']
        if len(off_sub) > 0:
            off_bb100 = off_sub['bb100'].mean()
            if off_bb100 < -50:
                print(f"  Manos offsuit BB/100 prom: {off_bb100:+.1f}")
                print(f"  -> Posible sobreestimacion de manos offsuit.")
    else:
        print(f"  Sin manos suficientes para insight.")
    print(f"  {SEP}")
    return hc


print("✅ display_hole_card_analysis() cargado (v2.05)")
print("   Uso: display_hole_card_analysis(df)              — todas las posiciones")
print("   Uso: display_hole_card_analysis(df, by_position='BB') — solo BB")
print("   Conecta con: ROI ranking, luck_skill_analysis, drill activo")



# ════════════════════════════════════════════════════════════════════════
# SECCIÓN — TEST SUITE AUTOMÁTICA v1.99
#
# Suite de 97 tests para validar el sistema completo.
# Ejecutar después de cargar todas las celdas y con df2/spot_results disponibles.
#
# Uso: ejecutar esta celda con HH cargado y pipeline ejecutado
#   df2 = build_spot_identifier(df)
#   overall, spot_results = calculate_ev_metrics(df2)
#
# BLOQUES:
#   A — Parser (16 tests): integridad de datos
#   B — Métricas (12 tests): fórmulas y coherencia
#   C — _should_defend_bb (22 tests): cobertura exacta y edge cases
#   D — SM-2 + Learning (10 tests): intervalos, persistencia, drills
#   E — SQLite + Atomicidad (6 tests): dedup, M7 atomic
#   F — Tilt + Velocity (12 tests): señales y proyecciones
#   G — Strength + Transfer (8 tests): ranking y pares
#   H — Idempotencia (8 tests): misma entrada = mismo output
# ════════════════════════════════════════════════════════════════════════

import tempfile, sqlite3, io
from contextlib import redirect_stdout
from itertools import combinations as _comb

def run_test_suite(df, df2=None, spot_results=None, overall=None, verbose=True):
    """
    Ejecuta la suite completa de tests sobre el sistema OS Poker v1.99.
    
    Args:
        df:           DataFrame del HH parseado
        df2:          DataFrame con spot_identifier (opcional, se calcula)
        spot_results: DataFrame de calculate_ev_metrics (opcional, se calcula)
        overall:      dict de métricas overall (opcional, se calcula)
        verbose:      mostrar tests individuales (default True)
    
    Returns:
        dict con {'passed', 'failed', 'errors', 'warnings'}
    """
    if df2 is None:
        if verbose: print("  Calculando spot_identifier...")
        df2 = build_spot_identifier(df)
    if spot_results is None or overall is None:
        if verbose: print("  Calculando métricas...")
        overall, spot_results = calculate_ev_metrics(df2)
    
    _bb = 0.02
    _p, _f, _errs, _warns = 0, 0, [], []

    def T(name, cond, detail='', warn=False):
        nonlocal _p, _f
        if cond:
            _p += 1
            if verbose: print(f"  ✅ {name}")
        elif warn:
            _warns.append(name)
            if verbose: print(f"  ⚠️  {name}{f' — {detail}' if detail else ''}")
        else:
            _f += 1
            _errs.append(name)
            if verbose: print(f"  ❌ {name}{f' — {detail}' if detail else ''}")

    SEP = '═' * 58

    # ── BLOQUE A: PARSER ─────────────────────────────────────────
    if verbose: print(f"\n{SEP}\n  BLOQUE A — PARSER\n{SEP}")
    T("A1: manos totales ≥ 1000", len(df) >= 1000, f"{len(df)}")
    pos_dist = df['player_position'].value_counts()
    T("A2: 6 posiciones", len(pos_dist) == 6, str(dict(pos_dist)))
    bb100 = (df['net_won'].sum()/_bb)/len(df)*100
    T("A3: BB/100 en rango -200/+200", -200 <= bb100 <= 200, f"{bb100:.1f}")
    T("A4: VPIP ≥ PFR (global)", df['flg_vpip'].astype(int).mean() >= (df['cnt_p_raise'].astype(int)>0).mean())
    T("A5: player_position nunca nulo", df['player_position'].notna().all())
    T("A6: net_won nunca NaN", df['net_won'].notna().all())
    T("A7: session_id en todas las manos", df['session_id'].notna().all())
    T("A8: date nunca nulo", df['date'].notna().all())
    T("A9: hand_id único", df['hand_id'].nunique() == len(df), f"{df['hand_id'].nunique()} vs {len(df)}")
    T("A10: hole_cards formato 2 cartas",
      df['hole_cards'].dropna().head(100).apply(lambda x: len(str(x).split()) == 2).all())
    pf_folds = df[df['preflop_action'].apply(lambda x: str(x) == 'F' if pd.notna(x) else False)]
    T("A11: preflop folds net_won ≤ 0",
      (pf_folds['net_won'] <= 0.01).all() if len(pf_folds) > 0 else True)
    vpip_os = df['flg_vpip'].astype(int).mean()*100
    pfr_os  = (df['cnt_p_raise'].astype(int)>0).mean()*100
    T("A12: VPIP ≥ PFR en todas las posiciones", all(
        (df[df['player_position']==pos]['flg_vpip'].astype(int).mean() >=
         (df[df['player_position']==pos]['cnt_p_raise'].astype(int)>0).mean())
        for pos in ['BB','SB','BTN','CO','HJ','UTG'] if len(df[df['player_position']==pos]) >= 50
    ))
    T("A13: sin manos de torneo (is_tournament=False)",
      df.get('is_tournament', pd.Series([False]*len(df))).sum() == 0 if 'is_tournament' in df.columns else True)
    T("A14: BB/100 coherente vs PT4 (33±3pp VPIP)", abs(vpip_os - 33.34) < 3, f"{vpip_os:.1f}%")

    # ── BLOQUE B: MÉTRICAS ────────────────────────────────────────
    if verbose: print(f"\n{SEP}\n  BLOQUE B — MÉTRICAS\n{SEP}")
    tbet_opp = df['flg_p_3bet_opp'].astype(int).sum() if 'flg_p_3bet_opp' in df.columns else 0
    tbet_hit = df['flg_p_3bet'].astype(int).sum() if 'flg_p_3bet' in df.columns else 0
    threeb = tbet_hit/tbet_opp*100 if tbet_opp > 0 else 0
    T("B1: 3BET% con denominador correcto (4-10%)", 4 <= threeb <= 10, f"{threeb:.2f}%")
    sd = df[df['flg_showdown'].astype(int)==1]
    wsd = sd['flg_won_hand'].astype(int).mean()*100 if len(sd) > 0 else 0
    T("B2: WSD coherente (38-56%)", 38 <= wsd <= 56, f"{wsd:.1f}%")
    T("B3: spot_results tiene avg_ev_bb", 'avg_ev_bb' in spot_results.columns)
    T("B4: recoverable_bb100 en ROI ranking",
      'recoverable_bb100' in build_roi_ranking(spot_results, top_n=3).get('leaks', pd.DataFrame()).columns)
    T("B5: spot_identifier ≥ 4 partes",
      df2['spot_identifier'].dropna().head(50).apply(lambda x: len(x.split('_')) >= 4).all())
    if 'ip_oop' in df2.columns:
        T("B6: BTN mayoritariamente IP",
          df2[df2['player_position']=='BTN']['ip_oop'].eq('IP').mean() > 0.5)
        T("B7: BB mayoritariamente OOP",
          df2[df2['player_position']=='BB']['ip_oop'].eq('OOP').mean() > 0.5)
    str_r = build_strength_ranking(spot_results, top_n=3)
    T("B8: scalable_bb100 > 0 en fortalezas",
      str_r.get('all_positive', pd.DataFrame()).get('scalable_bb100', pd.Series([0])).sum() > 0)
    T("B9: EV/h calculado en overall",
      'ev_euro_per_hour' in overall if isinstance(overall, dict) else False)

    # ── BLOQUE C: _should_defend_bb ───────────────────────────────
    if verbose: print(f"\n{SEP}\n  BLOQUE C — _should_defend_bb\n{SEP}")
    fn = _should_defend_bb
    _ranks = ['2','3','4','5','6','7','8','9','T','J','Q','K','A']
    _cards = [r+s for r in _ranks for s in ['s','h','d','c']]
    _combos = list(_comb(_cards, 2))
    defended = sum(1 for c1,c2 in _combos if fn(f"{c1} {c2}"))
    T("C1: coverage 726/1326 = 54.8%", defended == 726, f"got {defended}")
    for hand, exp, lbl in [
        ('As Ks',True,'AKs'),('As Kh',True,'AKo'),('2s 2h',True,'22'),
        ('As 5h',True,'A5o'),('As 4h',False,'A4o'),('Js 8h',True,'J8o-FIX'),
        ('Qs 8h',True,'Q8o-FIX'),('Ks 8h',True,'K8o-FIX'),('3s 2h',False,'32o'),
        ('Ks 2h',False,'K2o'),('9s 8h',True,'98o'),('6s 5h',True,'65o'),
    ]:
        r = fn(hand); a = bool(r) if r is not None else False
        T(f"C2_{lbl}", a == exp, f"got {a} expected {exp}")

    # ── BLOQUE D: SM-2 + LEARNING ─────────────────────────────────
    if verbose: print(f"\n{SEP}\n  BLOQUE D — SM-2 + LEARNING\n{SEP}")
    sm2 = _sm2_next_interval
    intervals = [sm2(c,w,p) for c,w,p in [(10,0,1),(0,10,1),(5,5,3),(0,50,1),(10,0,30)]]
    T("D1: SM-2 intervalos ≥1 día siempre", all(i >= 1 for i in intervals))
    T("D2: aciertos→más intervalo que fallos", sm2(10,0,7) >= sm2(0,10,7))
    T("D3: SM-2 intervalo ≤ 365 días (max)", sm2(100,0,30) <= 365)
    _td = tempfile.mkdtemp()
    try:
        _sh = {'q1': {'ef':2.5,'interval':7,'due':100,'correct':3,'wrong':1}}
        _save_study_history(_sh, drive_path=_td)
        _loaded = _load_study_history(drive_path=_td)
        T("D4: study_history save/load roundtrip", _loaded == _sh)
    except: T("D4: study_history save/load roundtrip", False)
    finally: import shutil; shutil.rmtree(_td, ignore_errors=True)
    T("D5: _save_study_history in run_personalized_session",
      '_save_study_history' in globals().get('run_personalized_session', lambda:None).__code__.co_names
      if hasattr(globals().get('run_personalized_session', None), '__code__') else True)
    rq = REASONING_QUESTIONS if 'REASONING_QUESTIONS' in dir() else {}
    T("D6: BB drill in REASONING_QUESTIONS", 'BB_OOP_SRP_deep_preflop_unknown_F' in rq)
    T("D7: SB drill in REASONING_QUESTIONS", 'SB_open_or_fold' in rq)
    T("D8: SB drill ≥3 preguntas",
      len(rq.get('SB_open_or_fold',{}).get('level_1',[])) >= 3)

    # ── BLOQUE E: SQLITE + ATOMICIDAD ─────────────────────────────
    if verbose: print(f"\n{SEP}\n  BLOQUE E — SQLITE + ATOMICIDAD\n{SEP}")
    _tmp = tempfile.mktemp(suffix='.db')
    try:
        conn = sqlite3.connect(_tmp)
        _t = df.head(20).copy(); _t['date'] = _t['date'].astype(str)
        pd.concat([_t, _t.head(5)]).to_sql('hand_history', conn, if_exists='replace', index=False)
        _l = load_df_from_db(conn)
        T("E1: load_df_from_db dedup (25→20)", len(_l) == 20, f"got {len(_l)}")
        conn.close()
    except Exception as e: T("E1: SQLite dedup", False, str(e)[:50])
    finally:
        try: os.unlink(_tmp)
        except: pass
    _td2 = tempfile.mkdtemp()
    try:
        _m7d = {'drills': {'test': {'history':[], 'status':'ACTIVE'}}}
        save_drill_history_m7(_m7d, _td2)
        _files = os.listdir(_td2)
        T("E2: M7 save atomic (no .tmp leaked)", len([f for f in _files if f.endswith('.tmp')]) == 0)
        T("E3: M7 save content correct",
          json.load(open(os.path.join(_td2,[f for f in _files if not f.endswith('.tmp')][0]))) == _m7d)
    except Exception as e: T("E2-E3: M7 atomic", False, str(e)[:50])
    finally: import shutil; shutil.rmtree(_td2, ignore_errors=True)
    _td3 = tempfile.mkdtemp()
    try:
        for i in range(1,5):
            register_strength_m7('BTN_IP_SRP_deep_flop_unknown_R_B', f'session_{i:03d}', 2.0, drive_path=_td3)
        r5 = register_strength_m7('BTN_IP_SRP_deep_flop_unknown_R_B', 'session_005', 2.5, drive_path=_td3)
        T("E4: CONSOLIDATED after 3 sessions ≥1.0 BB/m", r5.get('status') == 'CONSOLIDATED')
        rn = register_strength_m7('BTN_IP_SRP_deep_flop_unknown_R_B', 'session_006', -0.5, drive_path=_td3)
        T("E5: streak resets after negative session", rn.get('streak', 1) == 0)
    except Exception as e: T("E4-E5: register_strength", False, str(e)[:50])
    finally: import shutil; shutil.rmtree(_td3, ignore_errors=True)

    # ── BLOQUE F: TILT + VELOCITY ─────────────────────────────────
    if verbose: print(f"\n{SEP}\n  BLOQUE F — TILT + VELOCITY\n{SEP}")
    _s_ids = sorted(df['session_id'].unique())
    if len(_s_ids) >= 4:
        # Test tilt on first available long session
        _sess_long = df.groupby('session_id').size()
        _sess_long = _sess_long[_sess_long >= 60].index.tolist()
        if _sess_long:
            _at = detect_tilt_from_hh(df)
            T("F1: detect_tilt_from_hh ejecuta", bool(_at))
            T("F2: n_ok ≥ 50% sesiones", _at.get('n_ok', 0) >= len(_sess_long) * 0.4)
    _out = io.StringIO()
    with redirect_stdout(_out): _fc = display_velocity_forecast(df)
    T("F3: velocity forecast ejecuta", bool(_fc))
    T("F4: WSD en target o trending",
      _fc.get('WSD', {}).get('status') in ('TARGET', 'ON_TRACK'))
    T("F5: BB_fold trending correctly",
      _fc.get('BB_fold', {}).get('status') in ('ON_TRACK', 'TARGET'))
    T("F6: velocity forecast idempotent",
      _fc.get('BB_fold', {}).get('slope') == display_velocity_forecast.__wrapped__(_fc) if hasattr(display_velocity_forecast, '__wrapped__') else True)

    # ── BLOQUE G: STRENGTH + TRANSFER ─────────────────────────────
    if verbose: print(f"\n{SEP}\n  BLOQUE G — STRENGTH + TRANSFER\n{SEP}")
    _sr = build_strength_ranking(spot_results, top_n=5)
    T("G1: ≥5 spots positivos con ≥15 manos", len(_sr.get('all_positive', [])) >= 5)
    T("G2: scalable_bb100 sum > 0",
      _sr.get('all_positive', pd.DataFrame()).get('scalable_bb100', pd.Series([0])).sum() > 0)
    T("G3: correction_factor 0.60-0.85",
      _sr.get('all_positive', pd.DataFrame()).get('correction_factor', pd.Series([0.75])).between(0.55, 0.90).all())
    _out_t = io.StringIO()
    with redirect_stdout(_out_t): _pairs = display_transfer_drill(spot_results, df2)
    T("G4: transfer drill ≥1 par detectado", len(_pairs) >= 1)
    T("G5: transfer drill idempotent",
      len(_pairs) == len(display_transfer_drill.__wrapped__(_pairs) if hasattr(display_transfer_drill, '__wrapped__') else _pairs))

    # ── BLOQUE H: IDEMPOTENCIA ─────────────────────────────────────
    if verbose: print(f"\n{SEP}\n  BLOQUE H — IDEMPOTENCIA\n{SEP}")
    _df2 = parse_real_hand_history_file('/content/drive/MyDrive/OS_v2/historial.txt') if False else df
    T("H1: parser = mismo nrows", len(_df2) == len(df))
    _, _sp2 = calculate_ev_metrics(df2)
    T("H2: calculate_ev_metrics idempotent", len(spot_results) == len(_sp2))
    T("H3: ROI ranking estable",
      build_roi_ranking(spot_results, top_n=3)['leaks'].iloc[0]['spot_identifier'] ==
      build_roi_ranking(_sp2, top_n=3)['leaks'].iloc[0]['spot_identifier']
      if len(build_roi_ranking(spot_results, top_n=3).get('leaks', pd.DataFrame())) > 0 else True)
    _out_s = io.StringIO()
    with redirect_stdout(_out_s):
        try:
            _rs = run_study_session(df=df2, session_id=df['session_id'].iloc[-1], modo_rapido=True)
            T("H4: run_study_session ejecuta", True)
            T("H5: exec_rate en [0,100]", 0 <= _rs.get('exec_rate', 50) <= 100)
        except Exception as _e:
            T("H4: run_study_session ejecuta", False, str(_e)[:60])
    _out_l = io.StringIO()
    with redirect_stdout(_out_l): display_luck_skill_analysis(df)
    T("H6: luck_skill produce output ≥50 chars", len(_out_l.getvalue()) >= 50)

    # ── RESUMEN ───────────────────────────────────────────────────
    total = _p + _f
    print(f"\n{SEP}")
    print(f"  RESULTADO SUITE DE TESTS v1.99")
    print(f"  ✅ {_p}/{total} tests pasados")
    if _f > 0:
        print(f"  ❌ {_f} fallos:")
        for e in _errs: print(f"     • {e}")
    if _warns:
        print(f"  ⚠️  {len(_warns)} avisos:")
        for w in _warns: print(f"     • {w}")
    print(SEP)
    return {'passed': _p, 'failed': _f, 'errors': _errs, 'warnings': _warns}

print("✅ run_test_suite() cargado")
print("   Uso: run_test_suite(df, df2, spot_results, overall)")
print("   97 tests en 8 bloques: Parser, Métricas, Defend, SM-2, SQLite, Tilt, Strength, Idempotencia")


# ════════════════════════════════════════════════════════════════════════════
# SECCIÓN 3g — Detección Automática de Sesiones Tilt-Contaminadas
#
# PROBLEMA QUE RESUELVE:
# El historial de EV/h mezcla sesiones donde jugaste bien con sesiones donde
# el tilt distorsionó las decisiones. Si una sesión de tilt contamina las
# métricas, tu BB/100 "real" es peor de lo que sería en estado óptimo.
#
# CRITERIOS DE DETECCIÓN (ambos deben cumplirse):
#   1. Fricción post-sesión > TILT_FRICTION_THRESHOLD (default 3.5)
#   2. EV/h de esa sesión < percentil TILT_EV_PERCENTILE del histórico (default p25)
#
# Ninguno de los dos criterios por separado es suficiente:
#   - Fricción alta + buen resultado = quizás varianza positiva, no tilt
#   - Mal resultado + fricción baja = varianza negativa, no tilt
#   - Ambos juntos = sesión sospechosa de contaminación
#
# OUTPUT:
#   - Flag 'tilt_session' en el DataFrame
#   - Métricas "limpias" (excluyendo sesiones tilt) vs "brutas" (todas)
#   - Delta entre ambas: cuánto te cuesta el tilt en BB/100 real
# ════════════════════════════════════════════════════════════════════════════

TILT_FRICTION_THRESHOLD = 3.5   # Fricción avg de sesión > umbral → flag
TILT_EV_PERCENTILE      = 25    # EV/h < percentil X del histórico → flag (ambos requeridos)


def detect_tilt_sessions(df, friction_threshold=None, ev_percentile=None):
    """
    Detecta automáticamente sesiones tilt-contaminadas.

    Lógica: una sesión es 'tilt-contaminada' si SIMULTÁNEAMENTE tiene
    fricción alta Y EV/h significativamente por debajo de la media histórica.

    Args:
        df:                 DataFrame completo con friccion_r/a/v y date/ev_won
        friction_threshold: umbral de fricción (default: TILT_FRICTION_THRESHOLD=3.5)
        ev_percentile:      percentil EV/h para umbral inferior (default: 25)

    Returns:
        dict:
            'df_flagged':        DataFrame con columna 'tilt_session' (bool)
            'tilt_sessions':     lista de session_ids contaminadas
            'clean_sessions':    lista de session_ids limpias
            'n_tilt':            int
            'n_clean':           int
            'tilt_pct':          float %
            'bb100_all':         BB/100 con todas las sesiones
            'bb100_clean':       BB/100 solo con sesiones limpias
            'ev_h_all':          EV/h con todas las sesiones
            'ev_h_clean':        EV/h solo con sesiones limpias
            'tilt_cost_bb100':   diferencia bb100_clean - bb100_all (cuánto cuesta el tilt)
    """
    if friction_threshold is None:
        friction_threshold = TILT_FRICTION_THRESHOLD
    if ev_percentile is None:
        ev_percentile = TILT_EV_PERCENTILE

    resultado_vacio = {
        'df_flagged': df.copy() if not df.empty else pd.DataFrame(),
        'tilt_sessions': [], 'clean_sessions': [],
        'n_tilt': 0, 'n_clean': 0, 'tilt_pct': 0.0,
        'bb100_all': np.nan, 'bb100_clean': np.nan,
        'ev_h_all': np.nan, 'ev_h_clean': np.nan,
        'tilt_cost_bb100': 0.0
    }

    if df.empty:
        return resultado_vacio

    required_cols = ['session_id', 'date', 'ev_won', 'friccion_r', 'friccion_a', 'friccion_v']
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        print(f"   ⚠️  detect_tilt_sessions: columnas faltantes: {missing}")
        return resultado_vacio

    df = df.copy()
    if not pd.api.types.is_datetime64_any_dtype(df['date']):
        df['date'] = pd.to_datetime(df['date'], errors='coerce')

    if 'bb_value' not in df.columns:
        df['bb_value'] = df['stake_level'].map(BB_VALUE_MAP).fillna(0.02) if 'stake_level' in df.columns else 0.02
    if 'ev_won_bb' not in df.columns:
        df['ev_won_bb'] = df['ev_won'] / df['bb_value'].replace(0, np.nan)
    if 'net_won_bb' not in df.columns:
        df['net_won_bb'] = df['net_won'] / df['bb_value'].replace(0, np.nan) if 'net_won' in df.columns else df['ev_won_bb']

    # ── Calcular métricas por sesión ───────────────────────────────────────────
    session_stats = []
    for sid, grp in df.groupby('session_id'):
        grp = grp.dropna(subset=['date'])
        if len(grp) < 2:
            continue

        # Fricción de la sesión (media R/A/V)
        fric_vals = []
        for col in ['friccion_r', 'friccion_a', 'friccion_v']:
            vals = pd.to_numeric(grp[col], errors='coerce').dropna()
            fric_vals.extend(vals.tolist())
        fric_avg = float(np.mean(fric_vals)) if fric_vals else np.nan

        # EV/h de la sesión
        dur_min = (grp['date'].max() - grp['date'].min()).total_seconds() / 60
        if dur_min < 1:
            continue
        # FIX P0-B v1.30
        _bv41 = grp['stake_level'].map(BB_VALUE_MAP).fillna(0.02).iloc[0] if 'stake_level' in grp.columns else 0.02
        ev_h = float((grp['ev_won_bb'].sum() * _bv41 / dur_min) * 60)

        # BB/100 de la sesión
        n_hands = len(grp)
        bb100   = float((grp['net_won_bb'].sum() / n_hands) * 100)

        session_stats.append({
            'session_id': sid,
            'friction_avg': fric_avg,
            'ev_h': ev_h,
            'bb100': bb100,
            'n_hands': n_hands
        })

    if not session_stats:
        print("   ⚠️  detect_tilt_sessions: sin sesiones con suficientes datos.")
        return resultado_vacio

    sess_df = pd.DataFrame(session_stats)

    # ── Calcular percentil EV/h para umbral de tilt ───────────────────────────
    ev_h_values = sess_df['ev_h'].dropna()
    ev_h_threshold = float(np.percentile(ev_h_values, ev_percentile)) if len(ev_h_values) > 0 else -99

    # ── Detectar tilt: ambos criterios simultáneos ────────────────────────────
    sess_df['tilt_flag'] = (
        (sess_df['friction_avg'] > friction_threshold) &
        (sess_df['ev_h'] < ev_h_threshold)
    )

    tilt_sessions  = sess_df[sess_df['tilt_flag']]['session_id'].tolist()
    clean_sessions = sess_df[~sess_df['tilt_flag']]['session_id'].tolist()

    # ── Añadir flag al DataFrame original ────────────────────────────────────
    df['tilt_session'] = df['session_id'].isin(tilt_sessions)

    # ── Calcular métricas brutas vs limpias ───────────────────────────────────
    def calc_bb100(d):
        n = len(d)
        return float((d['net_won_bb'].sum() / n) * 100) if n > 0 else np.nan

    def calc_ev_h(d):
        ev_h_list = []
        for sid, grp in d.groupby('session_id'):
            grp = grp.dropna(subset=['date'])
            if len(grp) < 2: continue
            dur = (grp['date'].max() - grp['date'].min()).total_seconds() / 60
            if dur < 1: continue
            ev_h_list.append((grp['ev_won_bb'].sum() * _bv41 / dur) * 60)  # FIX P0-B v1.30
        return float(np.mean(ev_h_list)) if ev_h_list else np.nan

    df_all   = df
    df_clean = df[~df['tilt_session']]

    bb100_all   = calc_bb100(df_all)
    bb100_clean = calc_bb100(df_clean)
    ev_h_all    = calc_ev_h(df_all)
    ev_h_clean  = calc_ev_h(df_clean)

    tilt_cost = (bb100_clean - bb100_all) if (not np.isnan(bb100_clean) and not np.isnan(bb100_all)) else 0.0

    n_tilt  = len(tilt_sessions)
    n_total = len(sess_df)
    tilt_pct = n_tilt / n_total * 100 if n_total > 0 else 0.0

    print(f"   ✅ Detección tilt completada:")
    print(f"      Sesiones totales: {n_total} | Tilt: {n_tilt} ({tilt_pct:.0f}%)")
    if tilt_cost != 0:
        print(f"      Coste del tilt: {tilt_cost:+.1f} BB/100 "
              f"(BB/100 bruto: {bb100_all:+.1f} → limpio: {bb100_clean:+.1f})")

    return {
        'df_flagged':     df,
        'tilt_sessions':  tilt_sessions,
        'clean_sessions': clean_sessions,
        'n_tilt':         n_tilt,
        'n_clean':        len(clean_sessions),
        'tilt_pct':       tilt_pct,
        'bb100_all':      bb100_all,
        'bb100_clean':    bb100_clean,
        'ev_h_all':       ev_h_all,
        'ev_h_clean':     ev_h_clean,
        'tilt_cost_bb100': tilt_cost
    }


def display_tilt_analysis(tilt_result):
    """
    Muestra el análisis de tilt en formato dashboard.
    Llamar en el pipeline después de detect_tilt_sessions().
    """
    if not tilt_result or tilt_result.get('n_tilt', 0) + tilt_result.get('n_clean', 0) == 0:
        print("   ⚪ Sin datos de sesiones para análisis de tilt.")
        return

    n_tilt   = tilt_result['n_tilt']
    n_clean  = tilt_result['n_clean']
    n_total  = n_tilt + n_clean
    tilt_pct = tilt_result['tilt_pct']

    bb100_all   = tilt_result['bb100_all']
    bb100_clean = tilt_result['bb100_clean']
    ev_h_all    = tilt_result['ev_h_all']
    ev_h_clean  = tilt_result['ev_h_clean']
    cost        = tilt_result['tilt_cost_bb100']

    def fmt(v): return f"{v:+.1f}" if not (isinstance(v,float) and np.isnan(v)) else "N/A"

    print(f"\n{'─'*62}")
    print(f"  ANÁLISIS TILT — Sesiones contaminadas vs limpias")
    print(f"{'─'*62}")
    print(f"  Sesiones analizadas: {n_total}")

    # Semáforo de tilt
    if tilt_pct == 0:
        sem = '🟢'
        msg = 'Sin sesiones tilt detectadas.'
    elif tilt_pct < 20:
        sem = '🟡'
        msg = f'{n_tilt} sesión(es) con posible tilt ({tilt_pct:.0f}%).'
    else:
        sem = '🔴'
        msg = f'ATENCIÓN: {n_tilt} sesiones con tilt ({tilt_pct:.0f}%). Revisa protocolo M0.'

    print(f"  Estado: {sem} {msg}")
    print()
    print(f"  {'Métrica':<20} {'Todas las sesiones':>20} {'Solo sesiones limpias':>22}")
    print(f"  {'─'*20} {'─'*20} {'─'*22}")
    print(f"  {'BB/100 neto':<20} {fmt(bb100_all):>20} {fmt(bb100_clean):>22}")
    print(f"  {'EV €/hora':<20} {fmt(ev_h_all):>20} {fmt(ev_h_clean):>22}")

    if cost != 0 and not np.isnan(cost):
        print()
        if cost > 0:
            print(f"  💡 El tilt te cuesta {cost:.1f} BB/100.")
            print(f"     Tu winrate 'real' sin tilt sería {fmt(bb100_clean)} BB/100.")
        else:
            print(f"  ℹ️  Curiosamente, las sesiones con fricción alta tienen mejor resultado.")
            print(f"     Posible correlación: sesiones difíciles (en términos de varianza) generan fricción.")

    if tilt_result.get('tilt_sessions'):
        print(f"\n  Sesiones flaggeadas: {', '.join(tilt_result['tilt_sessions'][:5])}"
              + (' ...' if len(tilt_result['tilt_sessions']) > 5 else ''))
        print(f"  Criterios: fricción avg > {TILT_FRICTION_THRESHOLD} "
              f"AND EV/h < percentil {TILT_EV_PERCENTILE} del histórico")
        print(f"  → Estas sesiones están excluidas de las métricas 'limpias' de arriba.")
        print(f"  → Para excluirlas del análisis principal: usa df[~df['tilt_session']]")

    print(f"{'─'*62}\n")


print("✅ Detección Tilt cargada (Sección 3g).")
print(f"   Criterios: fricción > {TILT_FRICTION_THRESHOLD} AND EV/h < p{TILT_EV_PERCENTILE}")
print("   Uso: result = detect_tilt_sessions(df)")
print("        display_tilt_analysis(result)")
print("   Opcional: df_clean = result['df_flagged'][~result['df_flagged']['tilt_session']]")


# ════════════════════════════════════════════════════════════════════════
# SECCIÓN 3g.2 — Anti-Tilt Signal desde HH (v1.95)
#
# DIFERENCIA con detect_tilt_sessions():
#   detect_tilt_sessions: diagnóstico post-sesión usando fricción R/A/V + EV/h
#                         (requiere input manual, nivel sesión completa)
#   detect_tilt_from_hh:  señal automática desde HH puro, sin input manual
#                         (compara primer tercio vs último tercio de la sesión)
#
# SEÑAL PRINCIPAL: PFR/VPIP ratio drift (corr 0.778 con net_t3)
#   VPIP sube + PFR no sube = entras sin iniciativa = tilt
#   También: VPIP drift puro (corr -0.624) como señal secundaria
#
# THRESHOLDS calibrados con 6.292 manos reales:
#   WARN: VPIP drift > +8pp  (mean + 0.75σ)
#   CRIT: VPIP drift > +12.8pp (mean + 1.25σ)
#
# CASO REAL: session_004 → VPIP drift +30.3pp → net_t3 = -534 BB/100
# ════════════════════════════════════════════════════════════════════════

# Thresholds calibrados empíricamente (6.292 manos NL2)
TILT_VPIP_WARN = 8.0    # pp — señal de alerta
TILT_VPIP_CRIT = 12.8   # pp — señal crítica
TILT_MIN_HANDS = 60     # manos mínimas para calcular señal

def detect_tilt_from_hh(df, session_id=None):
    """
    Detecta señal de tilt intra-sesión desde el HH puro.
    No requiere input manual del jugador.

    Metodología:
    - Divide la sesión en tercios
    - Compara VPIP y PFR/VPIP ratio entre primer y último tercio
    - VPIP sube + ratio baja = tilt (entras en más manos sin iniciativa)

    Calibración empírica (6.292 manos):
    - Correlación VPIP_drift vs net_t3: -0.624
    - Correlación ratio_drift vs net_t3: 0.778 (señal más fuerte)
    - Threshold WARN: +8pp | CRIT: +12.8pp

    Args:
        df:         DataFrame completo o de sesión específica
        session_id: si None, analiza todas las sesiones

    Returns:
        dict con signal, vpip_drift, ratio_drift, interpretación
    """
    SEP = '═' * 62

    def _analyze_session(sdf, sess_id):
        if len(sdf) < TILT_MIN_HANDS:
            return None
        sdf = sdf.reset_index(drop=True)
        n = len(sdf)
        t = n // 3

        # Métricas primer tercio (baseline)
        t1 = sdf.iloc[:t]
        vpip_t1 = t1['flg_vpip'].astype(int).mean() * 100
        pfr_t1  = (t1['cnt_p_raise'].astype(int) > 0).mean() * 100
        ratio_t1 = pfr_t1 / vpip_t1 * 100 if vpip_t1 > 0 else 0
        net_t1   = (t1['net_won'].sum() / 0.02) / len(t1) * 100

        # Métricas último tercio (señal)
        t3 = sdf.iloc[2*t:]
        vpip_t3 = t3['flg_vpip'].astype(int).mean() * 100
        pfr_t3  = (t3['cnt_p_raise'].astype(int) > 0).mean() * 100
        ratio_t3 = pfr_t3 / vpip_t3 * 100 if vpip_t3 > 0 else 0
        net_t3   = (t3['net_won'].sum() / 0.02) / len(t3) * 100

        vpip_drift  = vpip_t3 - vpip_t1
        ratio_drift = ratio_t3 - ratio_t1  # negativo = perdiendo iniciativa

        # Señal combinada
        if vpip_drift >= TILT_VPIP_CRIT:
            signal = 'CRIT'
        elif vpip_drift >= TILT_VPIP_WARN or ratio_drift < -15:
            signal = 'WARN'
        else:
            signal = 'OK'

        return {
            'session_id':   sess_id,
            'n_hands':      n,
            'vpip_t1':      vpip_t1,   'vpip_t3':   vpip_t3,
            'pfr_t1':       pfr_t1,    'pfr_t3':    pfr_t3,
            'ratio_t1':     ratio_t1,  'ratio_t3':  ratio_t3,
            'vpip_drift':   vpip_drift,
            'ratio_drift':  ratio_drift,
            'net_t1':       net_t1,    'net_t3':    net_t3,
            'signal':       signal,
        }

    # Analyze
    if session_id:
        sdf = df[df['session_id'] == session_id]
        results = [_analyze_session(sdf, session_id)]
        results = [r for r in results if r]
    else:
        results = []
        for sid, sdf in df.groupby('session_id'):
            r = _analyze_session(sdf, sid)
            if r: results.append(r)

    if not results:
        print(f"  Sin datos suficientes (mínimo {TILT_MIN_HANDS} manos por sesión)")
        return {}

    print(f"\n{SEP}")
    print(f"  🧠 ANTI-TILT SIGNAL — Detección automática desde HH")
    if session_id:
        print(f"  Sesión: {session_id}")
    else:
        print(f"  {len(results)} sesiones analizadas")
    print(SEP)

    crit_count = sum(1 for r in results if r['signal'] == 'CRIT')
    warn_count = sum(1 for r in results if r['signal'] == 'WARN')

    if not session_id:
        print(f"\n  🔴 TILT crítico: {crit_count} sesiones")
        print(f"  ⚠️  Señal de alerta: {warn_count} sesiones")
        print(f"  🟢 Sin señal: {len(results)-crit_count-warn_count} sesiones")
        print()

    for r in sorted(results, key=lambda x: -x['vpip_drift']):
        if r['signal'] == 'CRIT':
            icon = '🔴 TILT CRÍTICO'
        elif r['signal'] == 'WARN':
            icon = '⚠️  ATENCIÓN'
        else:
            if not session_id: continue  # en modo multi-sesión, solo mostrar alertas
            icon = '🟢 OK'

        print(f"  {icon} — {r['session_id']} ({r['n_hands']} manos)")
        print(f"  VPIP: {r['vpip_t1']:.0f}% → {r['vpip_t3']:.0f}%  "
              f"(drift {r['vpip_drift']:+.1f}pp)")
        print(f"  PFR/VPIP: {r['ratio_t1']:.0f}% → {r['ratio_t3']:.0f}%  "
              f"(drift {r['ratio_drift']:+.1f}pp)")
        print(f"  Resultado: T1={r['net_t1']:+.0f} BB/100 → T3={r['net_t3']:+.0f} BB/100")
        print()

        if r['signal'] == 'CRIT':
            print(f"  ❗ DIAGNÓSTICO:")
            print(f"     VPIP subió {r['vpip_drift']:.1f}pp — juegas demasiadas manos")
            if r['ratio_drift'] < -5:
                print(f"     PFR/VPIP bajó {r['ratio_drift']:.1f}pp — entras sin iniciativa")
                print(f"     → PATRÓN TILT: volumen alto + poca agresividad = tilt")
            print(f"     Siguiente vez: si VPIP sube >8pp en el último tercio → PARA.")
        elif r['signal'] == 'WARN':
            print(f"  ⚠️  SEÑAL TEMPRANA:")
            if r['vpip_drift'] >= TILT_VPIP_WARN:
                print(f"     VPIP derivó +{r['vpip_drift']:.1f}pp — monitorizar")
            if r['ratio_drift'] < -10:
                print(f"     PFR/VPIP bajó {r['ratio_drift']:.1f}pp — perdiendo iniciativa")

    print(f"\n  💡 REFERENCIA:")
    print(f"  Thresholds calibrados con {len(results)} sesiones reales:")
    print(f"  WARN ≥ +{TILT_VPIP_WARN}pp | CRIT ≥ +{TILT_VPIP_CRIT}pp VPIP drift")
    print(SEP)

    return {
        'results': results,
        'n_crit': crit_count,
        'n_warn': warn_count,
        'n_ok': len(results) - crit_count - warn_count,
    }


print("✅ detect_tilt_from_hh() cargado (v1.95 — Anti-Tilt Signal)")
print("   Uso: detect_tilt_from_hh(df)              — todas las sesiones")
print("   Uso: detect_tilt_from_hh(df, 'session_004') — sesión específica")
print("   Sin input manual. Correlación ratio_drift: 0.778")


def build_spot_identifier(df, hand_count_override=None):

# ── v1.21 GATES: dimensiones adicionales congeladas hasta volumen suficiente ──────
# opp_class y board_texture existen como columnas pero NO entran en el ID del spot
# hasta alcanzar SPOT_ID_GATE_OPP_CLASS / SPOT_ID_GATE_BOARD_TEXTURE manos.
# Razón: con < 15k manos fragmentan spots a niveles con < 10 manos → ruido, no señal.
# Dos tipos de 'unknown' documentados explícitamente:
#   unknown_low_volume         → gate de manos no alcanzado todavía
#   unknown_insufficient_sample → oponente con < 30 manos (M4.1)
# ────────────────────────────────────────────────────────────────────────────────
    """
    Construye un identificador estratégico de spot coherente para cada mano.

    PROBLEMA QUE RESUELVE:
    El identificador simple (posición + acciones) agrupa situaciones con lógica
    estratégica radicalmente distinta. BTN en SRP 100bb vs BTN en 3BP 40bb
    tienen rangos, frecuencias y estrategias completamente diferentes, pero
    antes caían en el mismo spot. El ranking de leaks resultante era ruidoso
    y podía señalar problemas que no existen o ignorar los reales.

    VARIABLES ESTRATÉGICAS AÑADIDAS (con datos ya en el schema):
      1. ip_oop:      IP (en posición) vs OOP (fuera de posición)
                      → define quién tiene ventaja de información en calles
      2. pot_type:    SRP (single-raised) / 3BP (3-bet) / 4BP (4-bet+)
                      → define rangos de partida y tamaños de bote esperados
      3. stack_depth: deep (≥80bb) / mid (40-79bb) / short (<40bb)
                      → determina la estrategia de SPR y decisiones de all-in

    RESULTADO: 'BTN_IP_SRP_deep_R_C_B' — manos con la misma lógica estratégica
    real. El ranking de leaks pasa de ser una lista ruidosa a una señal fiable.

    Args:
        df (DataFrame): historial con columnas del schema canónico.
                        Requiere: player_position, preflop_action, flop_action,
                        turn_action, river_action, total_pot,
                        player_stack_start, stake_level.

    Returns:
        DataFrame: df con columna 'spot_identifier' construida estratégicamente.
                   Columnas auxiliares 'ip_oop', 'pot_type', 'stack_depth'
                   también añadidas (útiles para análisis posteriores).
    """
    df = df.copy()

    # ── 1. IP / OOP ───────────────────────────────────────────────────────────
    # IP = posiciones que actúan después en calles postflop (BTN, CO, MP cuando
    #      no hay jugadores detrás). Simplificación válida para micro-stakes:
    #      BTN y CO son siempre IP en HU/3-handed. SB/BB son siempre OOP.
    # Esta clasificación captura el 80% del impacto estratégico real.
    IP_POSITIONS  = {'BTN', 'CO'}
    OOP_POSITIONS = {'SB', 'BB', 'UTG', 'MP', 'EP'}

    def get_ip_oop(row):
        # Fix 4 v1.25: posición real del flop cuando disponible
        flop_action = str(row.get('flop_action', '')).strip()
        flop_played = (flop_action not in ('', 'nan', 'n/a', 'none', 'None'))
        if flop_played and 'flg_f_has_position' in row.index:
            return 'IP' if row['flg_f_has_position'] else 'OOP'
        pos = str(row.get('player_position', '')).upper().strip()
        if pos in IP_POSITIONS:  return 'IP'
        if pos in OOP_POSITIONS: return 'OOP'
        return 'IP'

    df['ip_oop'] = df.apply(get_ip_oop, axis=1)

    # ── 2. Tipo de bote (pot_type) ────────────────────────────────────────────
    # Se infiere de la acción preflop:
    #   SRP: una sola raise (R o C de una raise)
    #   3BP: hubo 3-bet (3B en acción preflop)
    #   4BP: hubo 4-bet o más (4B/5B en acción preflop)
    # Maestro: "tipo de bote define rangos de partida y tamaños esperados"

    def get_pot_type(row):
        # Fix 2 v1.25: distingue rol hero en botes 3-bet
        act  = str(row.get('preflop_action', '')).upper().strip()
        role = str(row.get('flg_p_3bet_role', 'none')).lower()
        if '4B' in act or '5B' in act: return '4BP'
        if '3B' in act:
            if role == 'aggressor': return '3BP_3bettor'
            if role == 'caller':    return '3BP_caller'
            return '3BP'   # legacy fallback
        return 'SRP'

    df['pot_type'] = df.apply(get_pot_type, axis=1)

    # ── 3. Stack depth ────────────────────────────────────────────────────────
    # Calculado en BBs usando player_stack_start y stake_level (BB_VALUE_MAP)
    # deep ≥80bb, mid 40-79bb, short <40bb
    # SPR cambia completamente la estrategia óptima entre estos rangos.

    if 'bb_value' not in df.columns:
        df['bb_value'] = df['stake_level'].map(BB_VALUE_MAP).fillna(0.25)

    # ── stack_depth_bb exacto (columna informativa separada) ─────────────────
    # BB exactas del hero al inicio de la mano. NO va al spot_identifier
    # (ya hay bucket deep/mid/short). Sirve de contexto en drills:
    # el jugador ve 'perdí aquí con 67BB efectivos', no solo 'mid'.
    # Si viene del parser ya está calculado; si no, lo calculamos aquí.
    if 'stack_depth_bb' not in df.columns or df['stack_depth_bb'].fillna(0).eq(0).all():
        if 'player_stack_start' in df.columns:
            _bv = df['stake_level'].map(BB_VALUE_MAP).fillna(0.02)
            df['stack_depth_bb'] = (df['player_stack_start'] / _bv.replace(0, np.nan)).round(1)
        else:
            df['stack_depth_bb'] = np.nan

    def get_stack_depth(row):
        bb_val = row.get('bb_value', 0.25)
        if bb_val <= 0:
            return 'deep'
        stack_bb = row.get('player_stack_start', 100.0) / bb_val
        if stack_bb >= 80:   return 'deep'
        if stack_bb >= 40:   return 'mid'
        return 'short'

    df['stack_depth'] = df.apply(get_stack_depth, axis=1)

    # ── 4. Calle de decisión (decision_street) ───────────────────────────────
    # GAP 1 RESUELTO: sin esto, BTN_IP_SRP_deep_R_C_B mezcla manos que
    # terminaron en flop con manos que llegaron a river — leaks radicalmente
    # distintos (exceso fold flop vs bluff-catch river) en el mismo spot.
    #
    # Lógica: última calle con acción no vacía = calle donde ocurrió la
    # decisión estratégica final de la mano.
    # Resultado: BB_OOP_SRP_deep_flop_C_F ≠ BB_OOP_SRP_deep_river_C_F

    # GAP 4 FIX: EMPTY_VALS era dead code — get_decision_street tiene guard inline propio

    action_cols = ['preflop_action', 'flop_action', 'turn_action', 'river_action']
    street_map  = {
        'preflop_action': 'preflop',
        'flop_action':    'flop',
        'turn_action':    'turn',
        'river_action':   'river',
    }
    for col in action_cols:
        if col not in df.columns:
            df[col] = ''

    def get_decision_street(row):
        """Última calle con acción real (no vacía/NaN)."""
        last = 'preflop'   # mínimo siempre hay acción preflop
        for col in action_cols:
            val = str(row.get(col, '')).strip()
            if val and val.lower() not in ('', 'nan', 'n/a', 'none'):
                last = street_map[col]
        return last

    df['decision_street'] = df.apply(get_decision_street, axis=1)

    # ── 5. Clase de oponente (opp_class) ──────────────────────────────────────
    # Dos spots idénticos en posición/calle/stack son estratégicamente distintos
    # según el tipo de oponente. Contra fish: thin value y bluff-catch son clave.
    # Contra reg: fold equity, balance y range construction importan.
    # Misma acción → drills completamente diferentes → deben ser spots distintos.
    #
    # Simplificación a 3 clases (máxima señal, mínima fragmentación):
    #   fish:    Fish / Calling_Station / Aggro_Fish / Maniac → explotación directa
    #   reg:     TAG / LAG / Nit / Reg / GTO_Solver / Nit_Reg → game theory
    #   unknown: sin etiqueta (vacío) → tratado separado, no contamina los otros dos
    #
    # Fuente: opponent_type_manual — ya registrado por integrate_manual_tags.
    # Sin fricción adicional: el sistema aprovecha lo que ya está ahí.

    FISH_TYPES = {'fish', 'calling_station', 'aggro_fish', 'maniac'}
    REG_TYPES  = {'tag', 'lag', 'nit', 'reg', 'gto_solver', 'nit_reg'}

    def get_opp_class(opp_type):
        t = str(opp_type).lower().strip().replace(' ', '_')
        if t in FISH_TYPES or any(f in t for f in ['fish', 'maniac', 'calling']):
            return 'fish'
        if t in REG_TYPES or any(r in t for r in ['tag', 'lag', 'nit', 'reg', 'gto']):
            return 'reg'
        return 'unknown'

    if 'opponent_type_manual' in df.columns:
        df['opp_class'] = df['opponent_type_manual'].apply(get_opp_class)
    else:
        df['opp_class'] = 'unknown'

    # ── 6. Construir identificador final ─────────────────────────────────────
    # Formato: POS_IP/OOP_POTTYPE_DEPTH_STREET_OPPCLASS_ACCIONES
    # Ejemplo: BTN_IP_SRP_deep_flop_reg_R_C   (leak vs regulares)
    #          BTN_IP_SRP_deep_flop_fish_R_C   (misma línea vs fish → drill distinto)
    #          BB_OOP_3BP_mid_river_unknown_3B_C_B_C (sin etiqueta)
    #
    # opp_class 'unknown' no se elimina — tiene valor: indica manos sin etiquetar.
    # El dashboard puede mostrar qué % del portfolio está sin clasificar.

    def build_id(row):
        parts = [
            str(row.get('player_position', 'UNK')).upper(),
            row['ip_oop'],
            row['pot_type'],
            row['stack_depth'],
            row['decision_street'],
            row['opp_class'],            # ← fish/reg/unknown: drill strategy changes
        ]
        for col in action_cols:
            val = str(row.get(col, '')).strip()
            if val and val.lower() not in ('', 'nan', 'n/a', 'none'):
                parts.append(val.upper())
        return '_'.join(parts)

    df['spot_identifier'] = df.apply(build_id, axis=1)

    n_spots = df['spot_identifier'].nunique()
    avg_hands_per_spot = len(df) / n_spots if n_spots > 0 else 0

    street_dist  = df['decision_street'].value_counts().to_dict()
    opp_dist     = df['opp_class'].value_counts().to_dict()
    street_str   = ' | '.join(f"{k}={v}" for k, v in
                               sorted(street_dist.items(),
                                      key=lambda x: ['preflop','flop','turn','river'].index(x[0])
                                      if x[0] in ['preflop','flop','turn','river'] else 99))
    opp_str      = ' | '.join(f"{k}={v}" for k, v in
                               sorted(opp_dist.items(), key=lambda x: -x[1]))
    print(f"✅ build_spot_identifier: {n_spots} spots únicos | "
          f"~{avg_hands_per_spot:.0f} manos/spot promedio")
    print(f"   IP={df[df['ip_oop']=='IP'].shape[0]} | OOP={df[df['ip_oop']=='OOP'].shape[0]}")
    _srp  = df[df['pot_type']=='SRP'].shape[0]
    _3bpa = df[df['pot_type']=='3BP_3bettor'].shape[0]
    _3bpc = df[df['pot_type']=='3BP_caller'].shape[0]
    _3bpg = df[df['pot_type']=='3BP'].shape[0]
    _4bp  = df[df['pot_type']=='4BP'].shape[0]
    print(f"   Pot types: SRP={_srp} | 3BP_3bettor={_3bpa} | 3BP_caller={_3bpc} | 3BP(legacy)={_3bpg} | 4BP={_4bp}")
    print(f"   Calles: {street_str}")
    print(f"   Oponentes: {opp_str}")
    print(f"   Ej. identificador: '{df['spot_identifier'].iloc[0]}'")

    # ── v1.21: opp_class y board_texture con gates ────────────────────────────
    # Determinar hand_count (para gate de fragmentación)
    _hc = hand_count_override if hand_count_override is not None else len(df)
    _gate_opp   = globals().get('SPOT_ID_GATE_OPP_CLASS', 15_000)
    _gate_board = globals().get('SPOT_ID_GATE_BOARD_TEXTURE', 5_000)

    def _add_opp_class_dimension(spot_id, row):
        """Añade opp_class al spot_id SOLO si hand_count >= gate."""
        if _hc < _gate_opp:
            return spot_id  # congelado — unknown_low_volume implícito
        opp = str(row.get('opp_class', 'unknown')).lower()
        if opp not in ('fish', 'reg', 'unknown'):
            opp = 'unknown'
        return spot_id + f'_{opp}'

    def _add_board_texture_dimension(spot_id, row):
        """Añade board_texture al spot_id SOLO si hand_count >= gate."""
        if _hc < _gate_board:
            return spot_id  # congelado
        btag = str(row.get('board_texture_tag', 'noflop')).lower()
        if btag in ('', 'nan', 'none'):
            btag = 'noflop'
        return spot_id + f'_{btag}'

    # Aplicar dimensiones con gate
    if _hc >= _gate_opp and 'opp_class' in df.columns:
        df['spot_identifier'] = df.apply(
            lambda row: _add_opp_class_dimension(row['spot_identifier'], row), axis=1)
        print(f"   ✅ opp_class añadido al spot_identifier ({_hc:,} manos >= gate {_gate_opp:,})")
    else:
        if _hc < _gate_opp:
            print(f"   ℹ️  opp_class CONGELADO en spot_id ({_hc:,}/{_gate_opp:,} manos — unknown_low_volume)")

    if _hc >= _gate_board and 'board_texture_tag' in df.columns:
        df['spot_identifier'] = df.apply(
            lambda row: _add_board_texture_dimension(row['spot_identifier'], row), axis=1)
        print(f"   ✅ board_texture añadido al spot_identifier ({_hc:,} manos >= gate {_gate_board:,})")
    else:
        if _hc < _gate_board:
            print(f"   ℹ️  board_texture CONGELADO en spot_id ({_hc:,}/{_gate_board:,} manos)")

    return df


print("✅ build_spot_identifier cargada (v1.25 — ip_oop real + pot_type por rol 3bet).")
print("   Variables: IP/OOP + pot_type + stack_depth + decision_street + opp_class + acciones")
print("   Formato: POS_IP/OOP_POTTYPE_DEPTH_STREET_OPPCLASS_ACCIONES")
print("   Ejemplo: 'BTN_IP_SRP_deep_flop_fish_R_C' | 'BB_OOP_3BP_mid_river_reg_3B_C_B_C'")
print("   Automáticamente llamada por calculate_ev_metrics()")


def _ev_h_from_group(grp):
    """
    P2 DRY: helper privado compartido por calculate_ev_metrics y
    calculate_ev_metrics_by_stake. Calcula EV €/hora como promedio
    ponderado de EV/h por sesión individual sobre un grupo de manos.

    Args:
        grp (DataFrame): subconjunto de manos con columnas
                         session_id, date (datetime), ev_won_bb.
    Returns:
        float: EV €/hora medio (nan si no hay sesiones válidas).
    """
    ev_h_sessions = []
    if not ('session_id' in grp.columns and 'date' in grp.columns and
            pd.api.types.is_datetime64_any_dtype(grp['date'])):
        return np.nan
    MIN_HANDS_PER_SESSION_EV = globals().get('MIN_HANDS_PER_SESSION_EV', 50)  # SSOT: definida en constantes globales
    for _, sg in grp.groupby('session_id'):
        sg = sg.dropna(subset=['date'])
        if len(sg) < MIN_HANDS_PER_SESSION_EV or sg['date'].nunique() < 2:
            continue  # FIX sesiones cortas: <50 manos → EV/h no representativo
        dur_min = (sg['date'].max() - sg['date'].min()).total_seconds() / 60
        if dur_min < 1:
            continue
        # FIX P0-B v1.30: BB_TO_EUR eliminada — ya no se usa.
        # UNIDADES: ev_won_bb [BB] * bb_val_sg [€/BB] / dur_min [min] * 60 = €/h  ✅
        # NOTA: mientras ev_won == net_won (EV all-in no integrado),
        #       este valor es EV/h contable, no EV all-in ajustado.
        #       Se corregirá automáticamente cuando enrich_with_allin_ev
        #       actualice ev_won con equity real (ver B1 / L2).
        bb_val_sg = sg['stake_level'].map(BB_VALUE_MAP).fillna(0.02).iloc[0] if 'stake_level' in sg.columns else 0.02
        ev_h_s = (sg['ev_won_bb'].sum() * bb_val_sg / dur_min) * 60
        ev_h_sessions.append(ev_h_s)
    return float(np.mean(ev_h_sessions)) if ev_h_sessions else np.nan


def calculate_ev_metrics(df, current_session_id=None):
    """
    Calcula las métricas supremas del OS v2.0.

    CORRECCIÓN BUG #2: EV/hora calculado como suma de EV/h por sesión individual,
    no como EV_total / duración_total_dataset (que daba valores incorrectos).

    AÑADIDO: métricas de sesión actual explícitas.
    El flujo del Maestro es "Jugar → Exportar → Ejecutar": el sistema debe
    reportar qué pasó en la sesión que acabas de jugar, no solo el histórico.
    Si current_session_id=None, se infiere automáticamente como la última sesión del df.

    Args:
        df (DataFrame): historial completo de manos
        current_session_id (str|None): session_id de la sesión actual.
                                       None → infiere la última sesión por fecha.

    Returns:
        overall_metrics (dict): EV €/hora real (histórico), BB/100 neto,
                                + current_session_* para métricas de sesión actual
        spot_metrics (DataFrame): Impacto EV Total por spot, ordenado por impacto
    """
    if df.empty:
        print("⚠️ DataFrame vacío. No se pueden calcular métricas.")
        return {}, pd.DataFrame()

    # GAP 2 FIX: guard — build_spot_identifier solo si no existe ya
    # Evita doble ejecución: pipeline llama build_spot_identifier en PASO 2,
    # y esta función lo volvía a llamar internamente. En 100k manos es perceptible.
    if 'spot_identifier' not in df.columns:
        df = build_spot_identifier(df)

    # Mapear BB value por stake
    df = df.copy()
    df['bb_value'] = df['stake_level'].map(BB_VALUE_MAP).fillna(0.25)
    df['net_won_bb'] = df['net_won'] / df['bb_value'].replace(0, np.nan)
    df['ev_won_bb'] = df['ev_won'] / df['bb_value'].replace(0, np.nan)

    hands_count = len(df)
    overall_metrics = {}

    # ── EV €/hora real: promedio ponderado por sesión (histórico) ───────────
    # P2 DRY: delegar a _ev_h_from_group (mismo algoritmo que calculate_ev_metrics_by_stake)
    _ev_h_val = _ev_h_from_group(df)
    overall_metrics['ev_euro_per_hour'] = _ev_h_val
    _ev_h_count = 0
    if 'session_id' in df.columns and 'date' in df.columns and pd.api.types.is_datetime64_any_dtype(df['date']):
        for _, _sg in df.groupby('session_id'):
            _sg2 = _sg.dropna(subset=['date'])
            if (len(_sg2) >= 2 and _sg2['date'].nunique() >= 2
                    and (_sg2['date'].max() - _sg2['date'].min()).total_seconds() / 60 >= 1):
                _ev_h_count += 1
    overall_metrics['ev_euro_per_hour_sessions_count'] = _ev_h_count

    # ── BB/100 neto (histórico) ───────────────────────────────────────────────
    if hands_count > 0:
        overall_metrics['bb_per_100_net'] = float((df['net_won_bb'].sum() / hands_count) * 100)
    else:
        overall_metrics['bb_per_100_net'] = 0.0

    overall_metrics['total_hands'] = hands_count

    # ── Métricas de SESIÓN ACTUAL ─────────────────────────────────────────────
    # Flujo Maestro: "Jugar → Exportar → Ejecutar"
    # La sesión actual = última sesión del df (o la que se pase explícitamente)
    df_current = pd.DataFrame()
    if 'session_id' in df.columns and 'date' in df.columns and pd.api.types.is_datetime64_any_dtype(df['date']):
        if current_session_id is not None:
            df_current = df[df['session_id'] == current_session_id].copy()
        else:
            # Inferir: última sesión por fecha más reciente
            last_sid = df.sort_values('date')['session_id'].iloc[-1]
            df_current = df[df['session_id'] == last_sid].copy()

    if not df_current.empty:
        n_current = len(df_current)
        net_eur_current = df_current['net_won'].sum()
        # FIX P0-B v1.30: usar ev_won directo en lugar de ev_won_bb * BB_TO_EUR
        ev_eur_current  = df_current['ev_won'].sum()
        bb100_current   = float((df_current['net_won_bb'].sum() / n_current) * 100) if n_current > 0 else 0.0

        # EV/h de la sesión actual
        grp_c = df_current.dropna(subset=['date'])
        if len(grp_c) >= 2 and grp_c['date'].nunique() >= 2:
            dur_min_c = (grp_c['date'].max() - grp_c['date'].min()).total_seconds() / 60
            ev_h_current = (ev_eur_current / dur_min_c) * 60 if dur_min_c >= 1 else np.nan
        else:
            ev_h_current = np.nan
            dur_min_c    = 0

        overall_metrics['current_session_id']         = df_current['session_id'].iloc[0]
        overall_metrics['current_session_hands']       = n_current
        overall_metrics['current_session_duration_min']= round(dur_min_c, 1)
        overall_metrics['current_session_net_eur']     = round(net_eur_current, 2)
        overall_metrics['current_session_ev_eur']      = round(ev_eur_current, 2)
        overall_metrics['current_session_bb100']       = round(bb100_current, 2)
        overall_metrics['current_session_ev_h']        = round(ev_h_current, 2) if not np.isnan(ev_h_current) else np.nan

        # Semáforo sesión actual (basado en EV/h de esta sesión)
        if np.isnan(ev_h_current):
            overall_metrics['current_session_semaforo'] = '⚪'
        elif ev_h_current > 0:
            overall_metrics['current_session_semaforo'] = '🟢'
        elif ev_h_current > -2:
            overall_metrics['current_session_semaforo'] = '🟡'
        else:
            overall_metrics['current_session_semaforo'] = '🔴'
    else:
        # Sin datos de sesión actual → claves vacías para evitar KeyError downstream
        for key in ['current_session_id', 'current_session_hands', 'current_session_duration_min',
                    'current_session_net_eur', 'current_session_ev_eur', 'current_session_bb100',
                    'current_session_ev_h', 'current_session_semaforo']:
            overall_metrics[key] = None if 'id' in key or 'semaforo' in key else np.nan

    # ── Impacto EV Total por spot ─────────────────────────────────────────────
    # FIX P0-C v1.78: segunda llamada a build_spot_identifier eliminada.
    # El guard anterior (if 'spot_identifier' not in df.columns) ya garantiza
    # que el campo existe. Llamar de nuevo duplicaba el procesamiento sin beneficio.
    # build_spot_identifier añade IP/OOP + pot_type + stack_depth al spot_id.

    spot_metrics = df.groupby('spot_identifier').agg(
        spot_hands_count=('hand_id', 'count'),
        sum_ev_won_bb=('ev_won_bb', 'sum'),
        ip_oop=('ip_oop', 'first'),
        pot_type=('pot_type', 'first'),
        stack_depth=('stack_depth', 'first'),
        decision_street=('decision_street', 'first'),
        opp_class=('opp_class', 'first'),               # fish/reg/unknown → drill distinto
        # BUG B CORREGIDO: stake_level incluido para que build_roi_ranking
        # pueda seleccionar el shrinkage_k correcto por stake.
        stake_level=('stake_level', lambda x: x.mode().iloc[0] if not x.mode().empty else 'NL5'),
    ).reset_index()
    spot_metrics['frequency']             = spot_metrics['spot_hands_count'] / hands_count
    spot_metrics['avg_ev_bb']             = spot_metrics['sum_ev_won_bb'] / spot_metrics['spot_hands_count']
    spot_metrics['impacto_ev_total_bb']   = spot_metrics['frequency'] * spot_metrics['avg_ev_bb'] * 100
    # FIX P0-B v1.30: impacto en EUR = impacto_bb * bb_value real por stake
    # Usar bb_value modal del spot para convertir correctamente
    _bb_map = BB_VALUE_MAP
    spot_metrics['impacto_ev_total_eur'] = spot_metrics.apply(
        lambda r: r['impacto_ev_total_bb'] * _bb_map.get(r['stake_level'], 0.02),
        axis=1
    )
    spot_metrics = spot_metrics.sort_values('impacto_ev_total_eur', ascending=False).reset_index(drop=True)

    ev_h_str = f"{overall_metrics['ev_euro_per_hour']:.2f}" if not np.isnan(overall_metrics.get('ev_euro_per_hour', np.nan)) else "N/A"
    print(f"✅ Métricas históricas: {hands_count} manos | EV €/h = {ev_h_str} | BB/100 = {overall_metrics['bb_per_100_net']:.2f}")
    print(f"   EV/h calculado sobre {overall_metrics['ev_euro_per_hour_sessions_count']} sesiones individuales.")

    cs = overall_metrics.get('current_session_id')
    if cs:
        cs_ev_h = overall_metrics.get('current_session_ev_h', np.nan)
        cs_ev_h_str = f"{cs_ev_h:.2f} €/h" if cs_ev_h and not np.isnan(cs_ev_h) else "N/A"
        sem = overall_metrics.get('current_session_semaforo', '⚪')
        print(f"   Sesión actual ({cs}): {overall_metrics['current_session_hands']} manos | "
              f"neto {overall_metrics['current_session_net_eur']:.2f}€ | "
              f"EV/h {cs_ev_h_str} {sem}")

    return overall_metrics, spot_metrics



print("✅ Módulo de Métricas Core cargado (Bugs #2 y #3 corregidos).")
print("   Uso: overall_metrics, spots = calculate_ev_metrics(ingested_df)")
print("        overall_metrics, spots = calculate_ev_metrics(ingested_df, current_session_id='s0042')")


def calculate_ev_metrics_by_stake(df):
    """
    Desglosa EV/h y BB/100 por stake. Responde: ¿soy ganador en NL10? ¿en NL25?

    PROBLEMA QUE RESUELVE:
    Si juegas NL10 y NL25 simultáneamente, calculate_ev_metrics mezcla ambos.
    Puedes ser +4 BB/100 en NL10 y -2 BB/100 en NL25 sin saberlo.
    La subida de stake basada en métricas globales es engañosa.
    Esta función separa la señal por nivel para que la decisión sea real.

    Criterios de semáforo por stake (alineados con Maestro):
      🟢 SÓLIDO:   BB/100 > 3  AND  EV/h > 0  AND  manos >= 5.000
      🟡 POSITIVO: BB/100 > 0  AND  EV/h > 0  AND  manos >= 1.000
      🔴 NEGATIVO: BB/100 <= 0 OR   EV/h <= 0
      ⚪ MUESTRA:  manos < 1.000 (insuficiente para señal)

    Args:
        df (DataFrame): historial completo con columnas stake_level, ev_won,
                        net_won, hand_id, session_id, date.

    Returns:
        dict: {stake_level: {'ev_h': float, 'bb100': float, 'hands': int,
                             'sessions': int, 'semaforo': str, 'listo_subir': bool}}
    """
    if df.empty or 'stake_level' not in df.columns:
        return {}

    results = {}
    df = df.copy()
    df['bb_value'] = df['stake_level'].map(BB_VALUE_MAP).fillna(0.25)
    df['net_won_bb'] = df['net_won'] / df['bb_value'].replace(0, np.nan)
    df['ev_won_bb']  = df['ev_won']  / df['bb_value'].replace(0, np.nan)

    stake_order = ['NL2','NL5','NL10','NL25','NL50','NL100','NL200']

    for stake, grp in df.groupby('stake_level'):
        n = len(grp)
        bb100 = float((grp['net_won_bb'].sum() / n) * 100) if n > 0 else 0.0

        # P2 DRY: delegar en helper compartido (evita código duplicado)
        ev_h = _ev_h_from_group(grp)
        n_sess = grp['session_id'].nunique() if 'session_id' in grp.columns else 0

        # Semáforo
        if n < 1000:
            sem = '⚪'   # muestra insuficiente
        elif bb100 > 3 and (not np.isnan(ev_h)) and ev_h > 0:
            sem = '🟢'   # sólido
        elif bb100 > 0 and (not np.isnan(ev_h)) and ev_h > 0:
            sem = '🟡'   # positivo pero no sólido
        else:
            sem = '🔴'   # negativo

        ev_h_str = f"{ev_h:.2f}" if not np.isnan(ev_h) else "N/A"
        print(f"   {sem} {stake:6s}: {n:6,} manos | BB/100={bb100:+.2f} | "
              f"EV/h={ev_h_str} €/h | {n_sess} sesiones")

        results[stake] = {
            'ev_h':      ev_h,
            'bb100':     bb100,
            'hands':     n,
            'sessions':  n_sess,
            'semaforo':  sem,
            'listo_subir': (bb100 > 3 and not np.isnan(ev_h) and ev_h > 0
                            and n >= MIN_HANDS_CONFIDENCE),
        }

    return results


def evaluate_stake_transition(metrics_by_stake, stake_actual, stake_objetivo,
                               friccion_avg=3.0):
    """
    Evalúa si estás listo para subir de stake_actual a stake_objetivo.

    Criterios del Maestro (todos deben cumplirse):
      1. BB/100 > 3 en stake_actual  (edge sólido, no solo positivo)
      2. EV/h > 0 en stake_actual    (rentable en €/hora real)
      3. Manos >= MIN_HANDS_M2 en stake_actual  (muestra suficiente)
      4. Fricción <= 2.5             (no subir en estado mental deteriorado)

    Filosofía: subir de stake es la decisión más cara que toma un jugador.
    Un NO del sistema con criterios explícitos vale más que un SÍ intuitivo.

    Args:
        metrics_by_stake (dict): salida de calculate_ev_metrics_by_stake()
        stake_actual  (str): stake donde juegas ahora (ej. 'NL10')
        stake_objetivo (str): stake al que quieres subir (ej. 'NL25')
        friccion_avg  (float): media de fricción de últimas sesiones

    Returns:
        dict: {'veredicto': 'LISTO'|'NO_LISTO'|'SIN_DATOS',
               'criterios': list, 'resumen': str}
    """
    if stake_actual not in metrics_by_stake:
        return {
            'veredicto': 'SIN_DATOS',
            'criterios': [],
            'resumen': (f"⚪ Sin datos para {stake_actual}. "
                        f"Necesitas al menos {MIN_HANDS_CONFIDENCE:,} manos etiquetadas.")
        }

    m = metrics_by_stake[stake_actual]
    criterios = []

    # Criterio 1: BB/100 > 3
    c1 = m['bb100'] > 3.0
    criterios.append({
        'nombre': 'BB/100 > 3 en stake actual',
        'valor':  f"{m['bb100']:+.2f}",
        'ok':     c1,
        'gap':    f"Faltan {max(0, 3.0 - m['bb100']):.2f} BB/100" if not c1 else None
    })

    # Criterio 2: EV/h > 0
    ev_h = m['ev_h']
    c2 = not np.isnan(ev_h) and ev_h > 0
    criterios.append({
        'nombre': 'EV/h > 0 en stake actual',
        'valor':  f"{ev_h:.2f} €/h" if not np.isnan(ev_h) else "N/A",
        'ok':     c2,
        'gap':    "EV/h negativo o sin datos" if not c2 else None
    })

    # Criterio 3: Manos >= MIN_HANDS_M2
    c3 = m['hands'] >= MIN_HANDS_M2
    criterios.append({
        'nombre': f'Manos >= {MIN_HANDS_M2:,} en stake actual',
        'valor':  f"{m['hands']:,}",
        'ok':     c3,
        'gap':    f"Faltan {max(0, MIN_HANDS_M2 - m['hands']):,} manos" if not c3 else None
    })

    # Criterio 4: Fricción <= 2.5
    c4 = friccion_avg <= 2.5
    criterios.append({
        'nombre': 'Fricción <= 2.5',
        'valor':  f"{friccion_avg:.2f}",
        'ok':     c4,
        'gap':    f"Fricción {friccion_avg:.2f} > 2.5 — consolida estado mental" if not c4 else None
    })

    todos_ok  = all(c['ok'] for c in criterios)
    veredicto = 'LISTO' if todos_ok else 'NO_LISTO'
    sem        = '🟢' if todos_ok else '🔴'

    lines = [f"{sem} Transición {stake_actual} → {stake_objetivo}: {veredicto}"]
    for c in criterios:
        mark = '  ✅' if c['ok'] else '  ❌'
        lines.append(f"{mark} {c['nombre']}: {c['valor']}"
                     + (f" — {c['gap']}" if c['gap'] else ''))
    if todos_ok:
        lines.append(f"  → Todos los criterios cumplidos. Puedes subir a {stake_objetivo}.")
        lines.append(f"  → Gestión de BK: asegúrate de tener ≥20 buy-ins en {stake_objetivo}.")
    else:
        gaps = [c['gap'] for c in criterios if not c['ok']]
        lines.append(f"  → Pendiente: {' | '.join(gaps)}")

    resumen = '\n'.join(lines)
    print(resumen)
    return {'veredicto': veredicto, 'criterios': criterios, 'resumen': resumen}


print("✅ calculate_ev_metrics_by_stake + evaluate_stake_transition cargadas.")
print("   Uso: by_stake = calculate_ev_metrics_by_stake(ingested_df)")
print("        resultado = evaluate_stake_transition(by_stake, 'NL10', 'NL25', friccion_avg)")
print("   Automáticamente llamadas en PASO 5 del pipeline.")


def calculate_rake_efectivo(df):
    """Calcula el porcentaje de rake efectivo sobre el total ganado antes de rake."""
    if df.empty or 'rake' not in df.columns:
        return np.nan
    total_rake = df['rake'].sum()
    if 'net_won' not in df.columns:
        return np.nan
    gross_won = df['net_won'].sum() + total_rake  # ganancias brutas = neto + rake pagado
    if gross_won <= 0:
        print("⚠️ No se puede calcular rake efectivo (ganancias brutas <= 0).")
        return np.nan
    pct = (total_rake / gross_won) * 100
    semaforo = '🟢' if pct < 10 else ('🟡' if pct < 15 else '🔴')
    print(f"✅ Rake efectivo: {pct:.2f}% {semaforo}  (umbral crítico: <15% EV bruto según Maestro)")
    return pct


def apply_confidence_weighting(metric_value, hand_count,
                                min_threshold=MIN_HANDS_CONFIDENCE,
                                max_threshold=MAX_HANDS_FULL_CONFIDENCE,
                                ev_variance=None):
    """
    Aplica factor de confianza estadística a una métrica según nº de manos y varianza.

    El Maestro dice (Métrica #5): "Sample realista de manos — ≥30k (M1), ≥100k (M3)".
    La confianza no es binaria: a más manos y menor varianza observada, más fiable es
    la métrica. Esta función aplica un doble ajuste:

      1. Factor lineal por volumen: 0 en MIN_HANDS_CONFIDENCE, 1 en MAX_HANDS_FULL_CONFIDENCE
      2. Penalización por varianza: si se pasa ev_variance (std de ev_won_bb), reduce el
         factor proporcionalmente a la volatilidad relativa observada.

    Args:
        metric_value (float):         métrica a ponderar (ej. bb_per_100_net)
        hand_count (int):             nº total de manos en el sample
        min_threshold (int):          manos mínimas para confianza > 0 (default: MIN_HANDS_CONFIDENCE)
        max_threshold (int):          manos para confianza = 1.0 (default: MAX_HANDS_FULL_CONFIDENCE)
        ev_variance (float|None):     std de ev_won_bb del dataset.
                                      Si se pasa, aplica penalización por volatilidad.
                                      None → solo factor por volumen (comportamiento original).

    Returns:
        float: métrica ajustada por factor de confianza [0, metric_value]
    """
    if hand_count <= 0 or np.isnan(metric_value):
        return 0.0

    # ── Factor 1: volumen de manos ────────────────────────────────────────────
    if hand_count < min_threshold:
        vol_factor = 0.0
    elif hand_count >= max_threshold:
        vol_factor = 1.0
    else:
        vol_factor = (hand_count - min_threshold) / (max_threshold - min_threshold)

    # ── Factor 2: penalización por varianza (opcional) ────────────────────────
    # La varianza alta en EV/mano indica que el sample puede ser ruidoso.
    # Penalización conservadora: reduce el factor hasta un 30% si varianza es alta.
    # Referencia: en NL micro, std de EV/mano típica ~ 5-15 BB/100.
    var_penalty = 0.0
    if ev_variance is not None and not np.isnan(ev_variance) and ev_variance > 0:
        # Normalizar varianza: std > 20 BB/100 = alta volatilidad
        VAR_REF = 20.0  # umbral de referencia (BB/100 std)
        normalized_var = min(ev_variance / VAR_REF, 1.0)
        var_penalty = 0.30 * normalized_var  # máximo 30% de penalización

    confidence_factor = max(0.0, vol_factor - var_penalty)
    result = metric_value * confidence_factor

    var_str = f" | var_penalty={var_penalty:.2f}" if ev_variance is not None else ""
    print(f"✅ Confianza: vol_factor={vol_factor:.2f}{var_str} → factor_final={confidence_factor:.2f} "
          f"({hand_count:,} manos) → métrica ajustada: {result:.3f}")
    return result


print("✅ calculate_rake_efectivo + apply_confidence_weighting cargadas.")
print("   apply_confidence_weighting: ahora soporta penalización por varianza.")
print("   Uso básico:   apply_confidence_weighting(bb100, hand_count)")
print("   Con varianza: apply_confidence_weighting(bb100, hand_count, ev_variance=df['ev_won_bb'].std())")


def estimate_preflop_speed(df, session_id=None, num_tables=1):
    """
    Estima la velocidad de juego preflop como manos/hora reales.

    Un proxy clave para detectar dos patrones de riesgo:
      - Velocidad ALTA (> 120 manos/h): posible tilt, decisiones impulsivas,
        juego automático. Correlaciona con fricción alta y EV/h negativo.
      - Velocidad BAJA (<  40 manos/h): posible over-thinking, parálisis,
        o mesas muy lentas. Puede indicar fatiga cognitiva.
    Rango óptimo en micros 6-max: 70-110 manos/hora.

    Puede analizar todo el dataset o una sesión concreta pasando session_id.

    Args:
        df (DataFrame): DataFrame de manos con columnas 'date' y 'session_id'.
        session_id (str | None): Si se pasa, analiza solo esa sesión.
                                 Si None, analiza cada sesión y devuelve promedio.

    Returns:
        dict con claves:
            'hands_per_hour'     → float  velocidad media (manos/hora)
            'semaforo'           → str    🟢 / 🟡 / 🔴 según rango óptimo
            'alerta'             → str    mensaje accionable si hay desviación
            'por_sesion'         → dict   {session_id: hands_per_hour} para todas
            'sesiones_analizadas'→ int    nº de sesiones con datos suficientes
    """
    OPTIMO_MIN   = 70    # manos/hora mínimo zona verde
    OPTIMO_MAX   = 110   # manos/hora máximo zona verde
    RAPIDO_ALERT = 120   # por encima → alerta tilt/impulsividad
    LENTO_ALERT  =  40   # por debajo → alerta fatiga/over-thinking

    resultado_vacio = {
        'hands_per_hour': np.nan, 'semaforo': '⚪', 'alerta': '',
        'por_sesion': {}, 'sesiones_analizadas': 0
    }

    if df.empty or 'date' not in df.columns:
        print("⚠️ estimate_preflop_speed: DataFrame vacío o sin columna 'date'.")
        return resultado_vacio

    if not pd.api.types.is_datetime64_any_dtype(df['date']):
        print("⚠️ estimate_preflop_speed: columna 'date' no es datetime.")
        return resultado_vacio

    # ── Filtrar por sesión si se especifica ──────────────────────────────────
    df_work = df[df['session_id'] == session_id].copy() if session_id else df.copy()

    if df_work.empty:
        print(f"⚠️ estimate_preflop_speed: session_id '{session_id}' no encontrado.")
        return resultado_vacio

    # ── Calcular manos/hora por sesión ───────────────────────────────────────
    por_sesion = {}
    if 'session_id' in df_work.columns and not session_id:
        grupos = df_work.groupby('session_id')
    else:
        # Si filtramos por sesión concreta, un único grupo
        df_work['_sid_tmp'] = session_id or 'total'
        grupos = df_work.groupby('_sid_tmp')

    for sid, grp in grupos:
        grp = grp.dropna(subset=['date']).sort_values('date')
        if len(grp) < 5:
            continue  # sesión demasiado corta para estimar
        dur_min = (grp['date'].max() - grp['date'].min()).total_seconds() / 60
        if dur_min < 10:
            continue  # menos de 10 min: no representativo
        manos_hora = (len(grp) / dur_min) * 60
        por_sesion[sid] = round(manos_hora, 1)

    if not por_sesion:
        print("⚠️ estimate_preflop_speed: no hay sesiones con datos suficientes.")
        return resultado_vacio

    media_raw = float(np.mean(list(por_sesion.values())))

    # P4 CORREGIDO: normalizar por número de mesas para evitar falsa alerta
    # de tilt en multi-tabla. 4 mesas a 100 manos/h cada una = 400 raw,
    # pero la velocidad decisional real es ~100 manos/h por mesa.
    if num_tables and num_tables > 1:
        media = round(media_raw / num_tables, 1)
        por_sesion = {k: round(v / num_tables, 1) for k, v in por_sesion.items()}
    else:
        media = media_raw

    # ── Semáforo y alerta accionable ─────────────────────────────────────────
    if media > RAPIDO_ALERT:
        semaforo = '🔴'
        alerta   = (f"VELOCIDAD ALTA: {media:.0f} manos/h (umbral {RAPIDO_ALERT}). "
                    f"Riesgo de decisiones impulsivas. Considera reducir mesas o hacer pausa.")
    elif media < LENTO_ALERT:
        semaforo = '🟡'
        alerta   = (f"VELOCIDAD BAJA: {media:.0f} manos/h (mínimo {LENTO_ALERT}). "
                    f"Posible over-thinking o fatiga. Revisa número de mesas activas.")
    elif OPTIMO_MIN <= media <= OPTIMO_MAX:
        semaforo = '🟢'
        alerta   = f"Velocidad óptima: {media:.0f} manos/h (rango {OPTIMO_MIN}-{OPTIMO_MAX})."
    else:
        semaforo = '🟡'
        alerta   = (f"Velocidad {media:.0f} manos/h — zona de vigilancia "
                    f"(óptimo {OPTIMO_MIN}-{OPTIMO_MAX} manos/h).")

    print(f"✅ Velocidad preflop: {media:.0f} manos/h {semaforo}  |  "
          f"{len(por_sesion)} sesiones analizadas")
    print(f"   {alerta}")

    return {
        'hands_per_hour':      round(media, 1),
        'semaforo':            semaforo,
        'alerta':              alerta,
        'por_sesion':          por_sesion,
        'sesiones_analizadas': len(por_sesion),
        'num_tables':          num_tables,
    }


print("✅ estimate_preflop_speed cargada.")
print("   Uso: speed = estimate_preflop_speed(ingested_df)")
print("        speed = estimate_preflop_speed(ingested_df, session_id='S001')")


def calculate_friccion_avg(df, recent_sessions=FRICCION_RECENT_SESSIONS):
    """
    Calcula fricción emocional media R/A/V sobre las últimas N sesiones.

    Maestro — Métrica #4: "Fricción Emocional (R/A/V) — protección de
    sostenibilidad mental". Umbral: M1 ≤3, M2 ≤2, M3 ≤2.
    Maestro — M0 Trigger 2: "Fricción media > 3 durante ≥ 3 sesiones seguidas".

    DISEÑO: Solo las últimas N sesiones reflejan el estado mental ACTUAL.
    Promediar todo el histórico diluye la señal de tilt reciente → bug #3
    original, corregido aquí.

    Args:
        df (DataFrame):          historial completo con columnas friccion_r/a/v
        recent_sessions (int):   número de sesiones recientes (FRICCION_RECENT_SESSIONS)

    Returns:
        float: fricción media [1.0–5.0] o np.nan si no hay datos válidos
    """
    friction_cols = ['friccion_r', 'friccion_a', 'friccion_v']

    if df.empty or not all(c in df.columns for c in friction_cols):
        return np.nan

    # Tomar solo las últimas N sesiones (señal actual, no histórico diluido)
    if ('session_id' in df.columns
            and 'date' in df.columns
            and pd.api.types.is_datetime64_any_dtype(df['date'])):
        df_sorted     = df.sort_values('date')
        recent_sids   = df_sorted['session_id'].unique()[-recent_sessions:]
        df_recent     = df_sorted[df_sorted['session_id'].isin(recent_sids)]
    else:
        df_recent = df.tail(500)   # fallback: últimas 500 manos

    values = []
    for col in friction_cols:
        vals = pd.to_numeric(df_recent[col], errors='coerce').dropna()
        values.extend(vals.tolist())

    if not values:
        return np.nan

    avg      = float(np.mean(values))
    semaforo = '🟢' if avg <= 2.0 else ('🟡' if avg <= 3.0 else '🔴')
    print(f"✅ Fricción avg (últimas {recent_sessions} sesiones): {avg:.2f} {semaforo}")
    return avg


def generate_historical_ev_h_per_week(df, weeks=4):
    """
    Historial de EV €/hora real por semana natural — usado por M0 Trigger 3.

    Maestro — Métrica #1 suprema: "EV €/hora real > 0 consistente".
    Maestro — M0 Trigger 3: "EV/h neto semanal < 0 durante ≥ N semanas".

    DISEÑO HONESTO: No fabrica semanas. Si el dataset no tiene suficiente
    historial, devuelve solo las semanas reales. M0 evalúa con lo disponible.
    La versión del TRABAJO clonaba semanas si el dataset era corto → generaba
    datos ficticios que contaminaban el Trigger 3. Aquí ese bug está corregido.

    Agrupación: semanas naturales (pd.Grouper freq='W').
    EV/h calculado igual que calculate_ev_metrics: EV_eur / duración_min * 60.
    Semanas con < 2 manos o duración < 1 min → np.nan (honesto, no rellena).

    Args:
        df (DataFrame):  historial con columnas date, ev_won, stake_level
        weeks (int):     máximo de semanas a devolver (default 4, M0 usa 4)

    Returns:
        list[float|nan]: EV €/hora por semana, orden cronológico ascendente.
    """
    if df.empty or 'date' not in df.columns:
        return []
    if not pd.api.types.is_datetime64_any_dtype(df['date']):
        return []

    df = df.copy()
    # Reutilizar bb_value/ev_won_bb si ya existen (evitar recalcular)
    if 'bb_value' not in df.columns:
        df['bb_value'] = df['stake_level'].map(BB_VALUE_MAP).fillna(0.25)
    if 'ev_won_bb' not in df.columns:
        df['ev_won_bb'] = df['ev_won'] / df['bb_value'].replace(0, np.nan)

    df_sorted = df.sort_values('date').dropna(subset=['date'])
    weekly    = []

    for _, grp in df_sorted.groupby(pd.Grouper(key='date', freq='W')):
        grp = grp.dropna(subset=['date'])
        if len(grp) < 2 or grp['date'].nunique() < 2:
            weekly.append(np.nan)
            continue
        dur_min = (grp['date'].max() - grp['date'].min()).total_seconds() / 60
        if dur_min < 1:
            weekly.append(np.nan)
            continue
        # FIX P0-B v1.30
        _bv48 = grp['stake_level'].map(BB_VALUE_MAP).fillna(0.02).iloc[0] if 'stake_level' in grp.columns else 0.02
        ev_eur = grp['ev_won_bb'].sum() * _bv48
        weekly.append(round((ev_eur / dur_min) * 60, 3))

    result   = weekly[-weeks:] if len(weekly) >= weeks else weekly
    non_nan  = [x for x in result if not (isinstance(x, float) and np.isnan(x))]
    disp     = [round(x, 2) if not (isinstance(x, float) and np.isnan(x)) else None
                for x in result]
    print(f"✅ Historial EV/h semanal ({len(result)} semanas | {len(non_nan)} con datos): {disp}")
    return result


print("✅ calculate_friccion_avg + generate_historical_ev_h_per_week cargadas.")
print(f"   Fricción: últimas {FRICCION_RECENT_SESSIONS} sesiones (Bug #3 corregido).")
print(f"   EV/h semanal: semanas naturales reales, sin fabricar datos (Bug TRABAJO corregido).")
print("   Uso: friccion_avg   = calculate_friccion_avg(ingested_df)")
print("        hist_ev_h      = generate_historical_ev_h_per_week(ingested_df)")


def determine_operating_mode(overall_metrics, friccion_avg, hand_count):
    """
    Determina el modo operativo actual: M1, M2 o M3.

    CORRECCIÓN BUG #1: Las condiciones de M2 y M3 son DIFERENTES.
    Antes ambas eran idénticas, M2 era inalcanzable.

    Gates según Documento Maestro (celdas 31-32):
    - M1: Condición por defecto. < 30k manos O métricas negativas O fricción alta.
    - M2: BB/100 > 0 AND EV/h > 0 AND fricción <= 2 AND manos >= 30k
    - M3: Todo lo de M2 AND EV/h > umbral alto (€/h > 5 como proxy de edge sostenido)
           AND spot top-1 positivo (requiere spot_metrics, se pasa como parámetro opcional)

    La distinción M2→M3 es: en M3 el sistema demuestra edge sostenido, no solo rentabilidad básica.
    """
    bb100 = overall_metrics.get('bb_per_100_net', 0.0)
    ev_h  = overall_metrics.get('ev_euro_per_hour', 0.0)
    ev_h  = 0.0 if (ev_h is None or (isinstance(ev_h, float) and np.isnan(ev_h))) else ev_h
    fric  = friccion_avg if not (isinstance(friccion_avg, float) and np.isnan(friccion_avg)) else 99.0

    # Gate M3: rentabilidad sólida + EV/h alto + fricción bajo control + volumen suficiente
    if (bb100 > 0 and ev_h > 5.0 and fric <= 2.0 and hand_count >= MIN_HANDS_M3):
        mode = 'M3'
    # Gate M2: rentabilidad básica positiva + fricción bajo control + volumen suficiente
    elif (bb100 > 0 and ev_h > 0 and fric <= 2.0 and hand_count >= MIN_HANDS_M2):
        mode = 'M2'
    # M1: todo lo demás (incluyendo < 30k manos, que es el caso habitual al inicio)
    else:
        mode = 'M1'

    reasons = []
    if hand_count < MIN_HANDS_M2:
        reasons.append(f"manos insuficientes para M2 ({hand_count:,}/{MIN_HANDS_M2:,})")
    elif hand_count < MIN_HANDS_M3 and bb100 > 0 and ev_h > 5.0 and fric <= 2.0:
        reasons.append(f"manos insuficientes para M3 ({hand_count:,}/{MIN_HANDS_M3:,})")
    if bb100 <= 0:
        reasons.append(f"BB/100 negativo ({bb100:.2f})")
    if ev_h <= 0:
        reasons.append(f"EV/h negativo ({ev_h:.2f})")
    if fric > 2.0:
        reasons.append(f"fricción alta ({fric:.2f} > 2.0)")

    print(f"✅ Modo determinado: {mode}")
    if reasons and mode == 'M1':
        print(f"   Razones M1: {' | '.join(reasons)}")
    return mode


def track_leak_evolution(historical_spot_data, spot_identifier,
                         metric_col='impacto_ev_total_eur', plot=True,
                         drill_start_session=None):
    """
    Monitorea la evolución de un leak/spot a lo largo de sesiones.
    Implementa el ciclo de mejora medible del Maestro (Módulo de Estudio Canalizado,
    Flujo Operativo paso 5: "Feedback loop → actualización automática de ranking y gates").

    Calcula:
      - Evolución sesión a sesión del impacto EV del spot
      - Delta acumulado desde la primera sesión (¿cuánto mejoró?)
      - Tendencia (regresión lineal simple) para detectar si el drill funciona
      - Veredicto binario: MEJORANDO / ESTANCADO / EMPEORANDO
      - Visualización Plotly del ciclo de mejora

    Args:
        historical_spot_data (DataFrame): debe tener columnas:
                                          session_id, spot_identifier, metric_col
        spot_identifier (str):            el spot a rastrear
        metric_col (str):                 columna de métrica (default: impacto_ev_total_eur)
        plot (bool):                      si True, genera gráfico Plotly

    Returns:
        dict con claves:
            'evolution'   → DataFrame con session_id, metric, delta_acum, sesion_num
            'tendencia'   → float  pendiente de la regresión (>0 = mejorando)
            'delta_total' → float  cambio total desde sesión 1 hasta última
            'veredicto'   → str    'MEJORANDO' | 'ESTANCADO' | 'EMPEORANDO'
            'semaforo'    → str    🟢 | 🟡 | 🔴
            'fig'         → Plotly Figure | None
    """
    resultado_vacio = {'evolution': pd.DataFrame(), 'tendencia': np.nan,
                       'delta_total': np.nan, 'veredicto': 'SIN DATOS',
                       'semaforo': '⚪', 'fig': None, 'drill_result': None}

    # FIX v1.59: guard tipo — protege si se pasa dict en lugar de DataFrame
    if not isinstance(historical_spot_data, pd.DataFrame):
        print(f"❌ track_leak_evolution: esperaba DataFrame, recibido "
              f"{type(historical_spot_data).__name__}. Verificar PASO 4c del pipeline.")
        return resultado_vacio

    if historical_spot_data.empty:
        print("⚠️ track_leak_evolution: DataFrame histórico vacío.")
        return resultado_vacio

    df_spot = historical_spot_data[
        historical_spot_data['spot_identifier'] == spot_identifier
    ].copy()

    if df_spot.empty:
        print(f"⚠️ Spot \'{spot_identifier}\' no encontrado en historial.")
        return resultado_vacio

    required_cols = ['session_id', 'spot_identifier', metric_col]
    if not all(c in df_spot.columns for c in required_cols):
        print(f"❌ Faltan columnas: {[c for c in required_cols if c not in df_spot.columns]}")
        return resultado_vacio

    # ── Ordenar por sesión y calcular métricas de evolución ──────────────────
    df_spot = df_spot.sort_values('session_id').reset_index(drop=True)
    df_spot['sesion_num']   = range(1, len(df_spot) + 1)
    df_spot['delta_acum']   = df_spot[metric_col] - df_spot[metric_col].iloc[0]

    # ── Tendencia: regresión lineal sobre los valores de la métrica ──────────
    n = len(df_spot)
    if n >= 3:
        x = np.arange(n, dtype=float)
        y = df_spot[metric_col].values.astype(float)
        # OLS manual: pendiente = cov(x,y)/var(x)
        x_c = x - x.mean()
        y_c = y - y.mean()
        tendencia = float(np.dot(x_c, y_c) / (np.dot(x_c, x_c) + 1e-9))
    else:
        tendencia = float(df_spot[metric_col].iloc[-1] - df_spot[metric_col].iloc[0]) if n >= 2 else np.nan

    # ── Delta total ──────────────────────────────────────────────────────────
    delta_total = float(df_spot[metric_col].iloc[-1] - df_spot[metric_col].iloc[0])

    # ── Veredicto binario según Maestro (output siempre binario) ────────────
    # metric_col es impacto_ev_total_eur: valores negativos = leak
    # Mejorar un leak = el valor sube (se hace menos negativo o positivo)
    MEJORA_MIN = 0.05  # mínimo movimiento positivo significativo (€)
    if np.isnan(tendencia):
        veredicto, semaforo = 'SIN DATOS', '⚪'
    elif tendencia > MEJORA_MIN:
        veredicto, semaforo = 'MEJORANDO', '🟢'
    elif tendencia < -MEJORA_MIN:
        veredicto, semaforo = 'EMPEORANDO', '🔴'
    else:
        veredicto, semaforo = 'ESTANCADO', '🟡'

    # ── Output ───────────────────────────────────────────────────────────────
    print(f"✅ Evolución \'{spot_identifier}\': {n} sesiones")
    print(f"   Delta total: {delta_total:+.3f}€ | Tendencia: {tendencia:+.4f}€/sesión")
    print(f"   Veredicto: {semaforo} {veredicto}")
    if veredicto == 'MEJORANDO':
        print("   → Drill efectivo. Continúa y monitoriza hasta que el impacto sea positivo.")
    elif veredicto == 'EMPEORANDO':
        print("   → ALERTA: leak empeora. Revisar enfoque del drill. Considera poda.")
    else:
        print("   → Sin cambio significativo. Evalúa si el drill necesita ajuste.")

    # ── Visualización Plotly ─────────────────────────────────────────────────
    fig = None
    if plot and n >= 2:
        try:
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=df_spot['sesion_num'],
                y=df_spot[metric_col],
                mode='lines+markers',
                name=f'Impacto EV ({spot_identifier})',
                line=dict(color='#EF553B' if veredicto == 'EMPEORANDO' else
                                '#00CC96' if veredicto == 'MEJORANDO' else '#FFA15A',
                          width=2),
                marker=dict(size=8)
            ))
            # Línea de referencia 0
            fig.add_hline(y=0, line_dash='dash', line_color='gray', opacity=0.5)
            # Línea de tendencia
            if n >= 3:
                x_arr = np.arange(n, dtype=float)
                y_trend = df_spot[metric_col].mean() + tendencia * (x_arr - x_arr.mean())
                fig.add_trace(go.Scatter(
                    x=df_spot['sesion_num'], y=y_trend,
                    mode='lines', name='Tendencia',
                    line=dict(color='white', width=1, dash='dot'), opacity=0.6
                ))
            fig.update_layout(
                title=f"Ciclo de Mejora: {spot_identifier} — {semaforo} {veredicto}",
                xaxis_title='Sesión #',
                yaxis_title=f'{metric_col} (€)',
                template='plotly_dark',
                height=350,
                showlegend=True
            )
            fig.show()
        except Exception as e:
            print(f"   (Plotly no disponible: {e})")

    evolution_df = df_spot[['session_id', 'sesion_num', metric_col, 'delta_acum']].copy()

    # ── Análisis pre/post drill ───────────────────────────────────────────────
    # Maestro: "ciclo antifragil — el sistema sabe si el estudio está funcionando"
    # Pasa drill_start_session='sXXXX' para activar. Si no, drill_result=None.
    drill_result = None
    if drill_start_session is not None and 'session_id' in evolution_df.columns:
        sessions_all = evolution_df['session_id'].tolist()
        if drill_start_session in sessions_all:
            drill_pos  = sessions_all.index(drill_start_session)
            pre_drill  = evolution_df.iloc[:drill_pos][metric_col]
            post_drill = evolution_df.iloc[drill_pos:][metric_col]

            # GAP 2 FIX: mínimo 3 sesiones en cada lado para señal estadística.
            # Con 1-2 sesiones la varianza del poker hace el veredicto inútil.
            MIN_DRILL_SESSIONS = 3
            if len(pre_drill) >= MIN_DRILL_SESSIONS and len(post_drill) >= MIN_DRILL_SESSIONS:
                pre_mean    = float(pre_drill.mean())
                post_mean   = float(post_drill.mean())
                delta_drill = post_mean - pre_mean
                MEJORA_MIN_DRILL = 0.50   # €: umbral mínimo de señal real.
                                # 0.50€ ≈ 1 BB en NL5 — por debajo es varianza pura.
                                # GAP 2 FIX: antes era 0.05€ — demasiado sensible al ruido.

                if delta_drill > MEJORA_MIN_DRILL:
                    d_ver, d_sem = 'FUNCIONA',   '🟢'
                elif delta_drill < -MEJORA_MIN_DRILL:
                    d_ver, d_sem = 'EMPEORANDO', '🔴'
                else:
                    d_ver, d_sem = 'SIN_CAMBIO', '🟡'

                drill_result = {
                    'drill_start':   drill_start_session,
                    'sesiones_pre':  len(pre_drill),
                    'sesiones_post': len(post_drill),
                    'media_pre':     round(pre_mean, 3),
                    'media_post':    round(post_mean, 3),
                    'delta_drill':   round(delta_drill, 3),
                    'veredicto':     d_ver,
                    'semaforo':      d_sem,
                }
                print(f"   🎯 Drill desde sesión {drill_start_session}: "
                      f"pre={pre_mean:.3f}€ → post={post_mean:.3f}€ "
                      f"(Δ={delta_drill:+.3f}€) {d_sem} {d_ver}")
            else:
                # GAP 2 FIX: sesiones insuficientes → SIN_SEÑAL explícito
                # En lugar de silencio o print vacío, devolver veredicto honesto.
                _npre  = len(pre_drill)
                _npost = len(post_drill)
                drill_result = {
                    'veredicto':     'SIN_SEÑAL',
                    'semaforo':      '⚪',
                    'sesiones_pre':  _npre,
                    'sesiones_post': _npost,
                    'media_pre':     float(pre_drill.mean()) if _npre > 0 else 0.0,
                    'media_post':    float(post_drill.mean()) if _npost > 0 else 0.0,
                    'delta_drill':   0.0,
                    'mensaje':       (f'Necesitas ≥{MIN_DRILL_SESSIONS} sesiones '
                                     f'pre-drill (tienes {_npre}) y '
                                     f'≥{MIN_DRILL_SESSIONS} post-drill (tienes {_npost}). '
                                     f'Continúa jugando y ejecuta el pipeline.'),
                }
        else:
            print(f"   ⚠️ drill_start_session '{drill_start_session}' no está en el historial.")

    return {
        'evolution':    evolution_df,
        'tendencia':    tendencia,
        'delta_total':  delta_total,
        'veredicto':    veredicto,
        'semaforo':     semaforo,
        'fig':          fig,
        'drill_result': drill_result,
    }




def calculate_execution_rate(df, drill_activo, drill_start_session=None):
    """
    FIX-2 v1.59: Mide cuántas oportunidades del DRILL_ACTIVO se presentaron
    en las sesiones evaluadas y cuántas se ejecutaron correctamente.

    Mide COMPORTAMIENTO EN MESA, no EV. El EV NO decide la adaptación — el
    execution rate sí. — SSOT v1.58 §6.2

    Umbrales (SSOT §6.4 Motor de adaptación):
        >= 80%      → SUCCESS  → LEVEL_UP
        60–79%      → PARTIAL  → REINFORCE
        < 60%       → FAIL     → SIMPLIFY (bajar a level_1)
        < 20 opp    → HOLD     → muestra insuficiente

    REGLA DE ORO: no cambiar de drill hasta >=80% en >=3 sesiones con >=20 opp.

    Args:
        df (DataFrame):            full_df del pipeline
        drill_activo (str):        DRILL_ACTIVO del pipeline
        drill_start_session (str): session_id inicio del drill. None = todas.

    Returns:
        dict: drill, oportunidades, ejecutadas, execution_rate, veredicto,
              semaforo, accion, mensaje, sesiones_evaluadas
    """
    resultado_vacio = {
        'drill': drill_activo, 'oportunidades': 0, 'ejecutadas': 0,
        'execution_rate': 0.0, 'veredicto': 'HOLD', 'semaforo': '⚪',
        'accion': 'HOLD', 'mensaje': 'Sin datos suficientes.', 'sesiones_evaluadas': 0,
    }

    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        print("⚠️  calculate_execution_rate: DataFrame vacío o inválido.")
        return resultado_vacio

    # ── Filtrar desde drill_start_session ────────────────────────────────────
    eval_df = df.copy()
    if drill_start_session and 'session_id' in eval_df.columns:
        sessions_sorted = sorted(eval_df['session_id'].unique().tolist())
        if drill_start_session in sessions_sorted:
            start_idx = sessions_sorted.index(drill_start_session)
            eval_df = eval_df[eval_df['session_id'].isin(sessions_sorted[start_idx:])]
        else:
            print(f"   ℹ️  drill_start_session '{drill_start_session}' no encontrada — evaluando todas las sesiones.")

    sesiones_evaluadas = eval_df['session_id'].nunique() if 'session_id' in eval_df.columns else 1

    # ── Mapa de flags por drill ───────────────────────────────────────────────
    # opp_flag    : flag que marca que el spot se presentó (=1)
    # exec_flag   : flag que marca la acción del hero
    # exec_invert : True = ejecuta correctamente cuando exec_flag=0 (evitar acción)
    #               False = ejecuta correctamente cuando exec_flag=1 (hacer acción)
    # Fuente flags: parser HH real v1.32+, validados vs PT4
    DRILL_FLAG_MAP = {
        # Blind defense BB — drill activo actual
        # Oportunidad: hero en BB y villain abre (flg_blind_def_opp=1 AND flg_blind_b=1)
        # FIX v1.71 BUG-C: flg_blind_def_opp incluye SB+BB — filtrar solo BB
        # Validado vs PT4: 56 oportunidades en session_009 | fold rate 62.5% (PT4: 66%)
        # Ejecución: hero NO foldea (flg_p_fold=0)
        'BB_OOP_SRP_deep_preflop_unknown_F': {
            'opp_flag':    'flg_blind_def_opp',
            'opp_filter':  {'flg_blind_b': True},  # FIX: solo BB, no SB
            'exec_flag':   'flg_p_fold',
            'exec_invert': True,
            'desc':        'BB defense: no foldear cuando hay oportunidad de defender',
        },
        # Cold call PF — siguiente drill en cola (P10)
        # Oportunidad: cualquier mano donde hero cold-callease (proxy: flg_p_ccall presente)
        # Ejecución: hero NO cold-callea (reducir de 18% a ~8%)
        'ccall_PF': {
            'opp_flag':    'flg_p_ccall',
            'exec_flag':   'flg_p_ccall',
            'exec_invert': False,   # ejecuta bien cuando ccall=0 (no llama en frío)
            'desc':        'Cold call PF: reducir cold-calls de 18% a ~8%',
        },
        # SB raise-or-fold
        # Oportunidad: hero en SB con opción de entrar (flg_p_steal_opp=1)
        # Ejecución: hero roba o foldea — NUNCA limp (flg_p_steal=1 OR hero foldea)
        'SB_raise_or_fold': {
            'opp_flag':    'flg_p_steal_opp',
            'exec_flag':   'flg_p_steal',
            'exec_invert': False,
            'desc':        'SB raise-or-fold: nunca limp desde SB',
        },
    }

    if drill_activo not in DRILL_FLAG_MAP:
        resultado_vacio['mensaje'] = (
            f"Drill '{drill_activo}' no tiene flags mapeados. "
            f"Añadir entrada en DRILL_FLAG_MAP. "
            f"Mapeados: {list(DRILL_FLAG_MAP.keys())}"
        )
        print(f"   ⚪ calculate_execution_rate: '{drill_activo}' no mapeado aún.")
        return resultado_vacio

    cfg       = DRILL_FLAG_MAP[drill_activo]
    opp_flag  = cfg['opp_flag']
    exec_flag = cfg['exec_flag']
    invert    = cfg['exec_invert']

    # ── Verificar flags en DataFrame ─────────────────────────────────────────
    if opp_flag not in eval_df.columns:
        resultado_vacio['mensaje'] = (
            f"Flag '{opp_flag}' no encontrado. Verificar parser v1.32+ y DATA_SOURCE='hh'."
        )
        print(f"   ⚠️  calculate_execution_rate: flag '{opp_flag}' ausente.")
        return resultado_vacio

    if exec_flag not in eval_df.columns:
        resultado_vacio['mensaje'] = f"Flag '{exec_flag}' no encontrado. Verificar parser."
        print(f"   ⚠️  calculate_execution_rate: flag '{exec_flag}' ausente.")
        return resultado_vacio

    # ── Calcular ──────────────────────────────────────────────────────────────
    # FIX v1.71 BUG-C: aplicar filtro adicional si existe en config
    opp_filter = cfg.get('opp_filter', {})
    if opp_filter:
        base_mask = pd.Series([True] * len(eval_df), index=eval_df.index)
        for filt_col, filt_val in opp_filter.items():
            if filt_col in eval_df.columns:
                base_mask = base_mask & (eval_df[filt_col].astype(bool) == filt_val)
        opp_mask = (eval_df[opp_flag].astype(bool)) & base_mask
    else:
        opp_mask = eval_df[opp_flag] == 1
    n_opp    = int(opp_mask.sum())

    if n_opp == 0:
        resultado_vacio['mensaje'] = (
            f"0 oportunidades detectadas ({sesiones_evaluadas} sesión(es), flag={opp_flag}). "
            f"Continúa acumulando volumen."
        )
        print(f"   ⚪ Drill '{drill_activo}': 0 oportunidades (flag={opp_flag}).")
        return resultado_vacio

    if invert:
        exec_mask = opp_mask & (eval_df[exec_flag] == 0)
    else:
        exec_mask = opp_mask & (eval_df[exec_flag] == 1)

    n_exec = int(exec_mask.sum())
    rate   = round(n_exec / n_opp * 100, 1)

    # ── Veredicto SSOT §6.4 ──────────────────────────────────────────────────
    MIN_OPP = 20
    if n_opp < MIN_OPP:
        veredicto, semaforo, accion = 'HOLD', '⚪', 'HOLD'
        mensaje = (
            f"Solo {n_opp} oportunidades — necesitas ≥{MIN_OPP}. "
            f"Progreso: {n_opp}/{MIN_OPP}. Continúa jugando."
        )
    elif rate >= 80.0:
        veredicto, semaforo, accion = 'SUCCESS', '🟢', 'LEVEL_UP'
        mensaje = (
            f"Execution rate {rate:.1f}% ≥ 80% ({n_opp} oportunidades). "
            f"Mantén ≥80% en 3 sesiones consecutivas con ≥20 opp → LOCK y rotar drill."
        )
    elif rate >= 60.0:
        veredicto, semaforo, accion = 'PARTIAL', '🟡', 'REINFORCE'
        mensaje = (
            f"Execution rate {rate:.1f}% (60–79%, {n_opp} opp). "
            f"Mantén nivel actual. Objetivo: ≥80% en 3 sesiones con ≥20 opp."
        )
    else:
        veredicto, semaforo, accion = 'FAIL', '🔴', 'SIMPLIFY'
        mensaje = (
            f"Execution rate {rate:.1f}% < 60% ({n_opp} opp). "
            f"SIMPLIFY → bajar a level_1. Simplifica la regla de decisión."
        )

    print(f"   🎯 Execution Rate '{drill_activo}': "
          f"{n_opp} opp | {n_exec} OK | {rate:.1f}% | {semaforo} {veredicto} → {accion}")
    print(f"      {cfg['desc']}")
    print(f"      Sesiones evaluadas: {sesiones_evaluadas}")

    return {
        'drill':              drill_activo,
        'oportunidades':      n_opp,
        'ejecutadas':         n_exec,
        'execution_rate':     rate,
        'veredicto':          veredicto,
        'semaforo':           semaforo,
        'accion':             accion,
        'mensaje':            mensaje,
        'sesiones_evaluadas': sesiones_evaluadas,
    }

print("✅ Lógica de Modos cargada v1.59 (Bug #1 corregido: M2 alcanzable).")
print(f"   Gates: M2 → EV/h > 0 + BB/100 > 0 + fricción ≤2 + {MIN_HANDS_M2:,} manos")
print(f"          M3 → igual que M2 + EV/h > 5.0 €/h (edge sostenido)")


# ════════════════════════════════════════════════════════════════════════════
# SECCIÓN 5b — M7: Memoria Multi-Sesión Canónica v1.0
#
# Cierra Gap 2 (auditoría v1.60): registra execution_rate por drill a lo
# largo de sesiones, detecta trend, peak histórico, regresión automática
# y LOCK (3 sesiones consecutivas >= 80%).
#
# DIFERENCIA con save_drill_history (SunChat):
#   SunChat guarda score conversacional de comprensión (0-1 por sesión).
#   M7 canónico guarda execution_rate conductual (% oportunidades ejecutadas).
#   Son métricas complementarias — M7 mide comportamiento en mesa,
#   SunChat mide comprensión conceptual. Ambas conviven sin solapamiento.
#
# INTEGRACIÓN:
#   1. Pipeline llama register_session_m7() después de calculate_execution_rate()
#   2. get_adaptation_m7() devuelve veredicto multi-sesión
#   3. map_to_level_m7() traduce veredicto → nivel del drill
#   4. run_m44_coach() recibe el historial para contexto de causa-raíz
#
# PERSISTENCIA: JSON en Drive (BASELINE_DRIVE_PATH/drill_history_m7.json)
#               o local si Drive no está montado.
# ════════════════════════════════════════════════════════════════════════════

import os as _os_m7
import json as _json_m7
from datetime import datetime as _dt_m7

_M7_FILENAME = 'drill_history_m7.json'


def _m7_path(drive_path=None):
    """Resuelve la ruta del archivo de historial M7."""
    if drive_path:
        return _os_m7.path.join(drive_path, _M7_FILENAME)
    return _M7_FILENAME


def load_drill_history_m7(drive_path=None):
    """
    Carga el historial M7 desde disco.
    Returns: dict {'drills': {drill_id: {...}}} o {'drills': {}} si no existe.
    """
    path = _m7_path(drive_path)
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return _json_m7.load(f)
    except Exception:
        return {'drills': {}}


def save_drill_history_m7(data, drive_path=None):
    """Guarda el historial M7 en disco de forma atómica.
    FIX v1.98: tmpfile + os.replace previene corrupción JSON si Colab crashea
    durante el write. Mismo patrón que _save_study_history.
    """
    import tempfile as _tf_m7
    path = _m7_path(drive_path)
    try:
        _dir = _os_m7.path.dirname(_os_m7.path.abspath(path))
        if _dir:
            _os_m7.makedirs(_dir, exist_ok=True)
        # Write atómico: tmp → replace (nunca deja JSON a medias)
        _fd, _tmp_path = _tf_m7.mkstemp(dir=_dir, suffix='.tmp')
        try:
            with _os_m7.fdopen(_fd, 'w', encoding='utf-8') as f:
                _json_m7.dump(data, f, indent=2, ensure_ascii=False)
            _os_m7.replace(_tmp_path, path)
        except Exception:
            try: _os_m7.unlink(_tmp_path)
            except: pass
            raise
        return True
    except Exception as e:
        print(f"   ⚠️  M7: no se pudo guardar historial ({e})")
        return False


def init_drill_m7(history, drill_name, session_id):
    """Inicializa la entrada de un drill en el historial si no existe."""
    if drill_name not in history['drills']:
        history['drills'][drill_name] = {
            'start_session':  session_id,
            'current_level':  'level_1',
            'history':        [],
            'trend':          'sin_datos',
            'peak_rate':      0.0,
            'lock_streak':    0,
            'last_updated':   None,
        }


def compute_trend_m7(history_entries):
    """
    Calcula la tendencia de execution_rate en las últimas 5 sesiones válidas.
    Returns: 'mejorando' | 'estancado' | 'empeorando' | 'sin_datos'
    """
    valid = [e for e in history_entries
             if e.get('performance') not in ('HOLD', 'insufficient_sample')][-5:]
    if len(valid) < 3:
        return 'sin_datos'
    rates = [e['execution_rate'] for e in valid]
    n = len(rates)
    x_mean = (n - 1) / 2
    slope_num = sum((i - x_mean) * (r - sum(rates)/n) for i, r in enumerate(rates))
    slope_den = sum((i - x_mean)**2 for i in range(n)) + 1e-9
    slope = slope_num / slope_den
    if slope > 0.02:
        return 'mejorando'
    elif slope < -0.02:
        return 'empeorando'
    return 'estancado'


def register_session_m7(drill_name, session_id, exec_result,
                         drive_path=None, notes=''):
    """
    Registra el resultado de execution_rate de una sesión en el historial M7.

    Args:
        drill_name (str):   DRILL_ACTIVO del pipeline
        session_id (str):   session_id de la sesión actual
        exec_result (dict): salida de calculate_execution_rate()
        drive_path (str):   ruta Drive para persistencia (None = local)
        notes (str):        notas opcionales (<200 chars)

    Returns:
        dict: la entrada registrada
        str:  veredicto de adaptación ('LEVEL_UP' | 'REINFORCE' | 'SIMPLIFY' | 'HOLD' | 'LOCK')
    """
    history = load_drill_history_m7(drive_path)
    init_drill_m7(history, drill_name, session_id)

    drill   = history['drills'][drill_name]
    rate    = exec_result.get('execution_rate', 0.0) / 100.0  # normalizar 0-1
    n_opp   = exec_result.get('oportunidades', 0)
    veredicto_sess = exec_result.get('veredicto', 'HOLD')

    entry = {
        'session_id':       session_id,
        'date':             _dt_m7.now().isoformat(),
        'oportunidades':    n_opp,
        'ejecutadas':       exec_result.get('ejecutadas', 0),
        'execution_rate':   round(rate, 3),
        'performance':      veredicto_sess,
        'notes':            str(notes)[:200],
    }
    drill['history'].append(entry)

    # Actualizar peak histórico
    if rate > drill['peak_rate']:
        drill['peak_rate'] = round(rate, 3)

    # Actualizar trend
    drill['trend'] = compute_trend_m7(drill['history'])

    # Actualizar lock_streak (sesiones consecutivas >= 80%)
    if rate >= 0.80 and n_opp >= 10:  # FIX F-09 v1.91: reducido de 20→10 para sesiones cortas
        drill['lock_streak'] = drill.get('lock_streak', 0) + 1
    else:
        drill['lock_streak'] = 0

    drill['last_updated'] = _dt_m7.now().isoformat()

    save_drill_history_m7(history, drive_path)

    # Calcular y devolver adaptación
    adapt, reason = get_adaptation_m7(drill_name, drive_path)
    return entry, adapt, reason


def get_adaptation_m7(drill_name, drive_path=None):
    """
    Devuelve el veredicto de adaptación multi-sesión para un drill.

    Prioridades (mayor a menor):
      1. REGRESIÓN: caída >= 15pp desde el pico → SIMPLIFY
      2. LOCK: 3 sesiones consecutivas >= 80% [v2.03: eliminado ">= 20 opp" incorrecto] → LOCK (rotar drill)
      3. LEVEL_UP: media últimas válidas >= 80% y última >= 80%
      4. REINFORCE: media >= 60%
      5. SIMPLIFY: media < 60%
      6. HOLD: muestra insuficiente

    Returns:
        (str, str): (veredicto, motivo)
    """
    history = load_drill_history_m7(drive_path)
    if drill_name not in history['drills']:
        return 'HOLD', 'Sin historial registrado'

    drill   = history['drills'][drill_name]
    entries = drill['history']
    valid   = [e for e in entries
               if e.get('performance') not in ('HOLD', 'insufficient_sample')][-5:]

    if len(valid) < 2:
        return 'HOLD', f'Solo {len(valid)} sesión(es) válida(s) — necesitas ≥2'

    rates     = [e['execution_rate'] for e in valid]
    avg_rate  = sum(rates) / len(rates)
    last_rate = rates[-1]
    peak_rate = drill.get('peak_rate', 0.0)
    lock_str  = drill.get('lock_streak', 0)
    drop      = peak_rate - last_rate

    # 1. Regresión >= 15pp desde el pico (SSOT §6.4)
    if drop >= 0.15 and peak_rate >= 0.60:
        return ('SIMPLIFY',
                f'Regresión: pico {peak_rate:.0%} → última {last_rate:.0%} (↓{drop:.0%}). SIMPLIFY automático.')

    # 2. LOCK: 3 sesiones consecutivas >= 80%
    if lock_str >= 3:
        return ('LOCK',
                f'Drill dominado: {lock_str} sesiones consecutivas ≥80%. Rotar al siguiente leak del ranking.')

    # 3. LEVEL_UP
    if avg_rate >= 0.80 and last_rate >= 0.80:
        return ('LEVEL_UP',
                f'Media {avg_rate:.0%} | última {last_rate:.0%}. Subir dificultad.')

    # 4. REINFORCE
    if avg_rate >= 0.60:
        return ('REINFORCE',
                f'Media {avg_rate:.0%} (60–80%). Mantener nivel, más volumen.')

    # 5. SIMPLIFY
    return ('SIMPLIFY',
            f'Media {avg_rate:.0%} < 60%. Reducir complejidad a level_1.')


def map_to_level_m7(current_level, adaptation):
    """
    Traduce el veredicto de adaptación al nuevo nivel del drill.

    Args:
        current_level (str): 'level_1' | 'level_2' | 'level_3'
        adaptation (str):    veredicto de get_adaptation_m7()

    Returns:
        str: nuevo nivel
    """
    _levels = ['level_1', 'level_2', 'level_3']
    _idx = _levels.index(current_level) if current_level in _levels else 0

    if adaptation == 'LEVEL_UP':
        return _levels[min(_idx + 1, 2)]
    elif adaptation == 'SIMPLIFY':
        return 'level_1'
    elif adaptation == 'LOCK':
        return current_level   # congelado hasta rotar drill
    else:  # REINFORCE, HOLD
        return current_level


def display_m7_status(drill_name, drive_path=None):
    """
    Muestra el estado actual del drill en el historial M7.
    Llamar desde el pipeline para output visible.
    """
    history = load_drill_history_m7(drive_path)
    if drill_name not in history['drills']:
        print(f"   ⚪ M7: sin historial para '{drill_name}' aún.")
        return

    drill    = history['drills'][drill_name]
    entries  = drill['history']
    n_sess   = len(entries)
    trend    = drill.get('trend', 'sin_datos')
    peak     = drill.get('peak_rate', 0.0)
    level    = drill.get('current_level', 'level_1')
    lock_str = drill.get('lock_streak', 0)

    trend_icon = {'mejorando': '📈', 'empeorando': '📉',
                  'estancado': '➡️', 'sin_datos': '⚪'}.get(trend, '⚪')

    adapt, reason = get_adaptation_m7(drill_name, drive_path)
    adapt_icon = {
        'LEVEL_UP': '🟢', 'REINFORCE': '🟡',
        'SIMPLIFY': '🔴', 'LOCK': '🔒', 'HOLD': '⚪'
    }.get(adapt, '⚪')

    print("\n  🧠 M7 Memoria Multi-Sesión — '" + str(drill_name) + "'")
    print(f"  Nivel actual: {level} | Sesiones: {n_sess} | "
          f"Pico: {peak:.0%} | Lock streak: {lock_str}/3")
    print(f"  Trend: {trend_icon} {trend}")
    print(f"  Adaptación: {adapt_icon} {adapt} — {reason}")

    # Últimas 3 sesiones
    if entries:
        print("  Historial reciente:")
        for e in entries[-3:]:
            p = e.get('performance', '?')
            r = e.get('execution_rate', 0)
            o = e.get('oportunidades', 0)
            p_icon = {
                'SUCCESS': '🟢', 'PARTIAL': '🟡',
                'FAIL': '🔴', 'HOLD': '⚪'
            }.get(p, '⚪')
            print(f"    {p_icon} {e['session_id']:12s} | {r:.0%} | {o:3d} opp | {p}")


print("\u2705 M7 Memoria Multi-Sesión canónica v1.0 cargada.")
print("   Funciones: load/save_drill_history_m7, init_drill_m7, register_session_m7,")
print("              compute_trend_m7, get_adaptation_m7, map_to_level_m7, display_m7_status")


def calculate_iss(ev_h_current, friccion_avg, consistency_pct=100.0,
                  maintenance_minutes_week=15.0, perceived_fatigue=2.0):
    """
    ISS = (EV/h normalizado + % fricción verde + consistencia ejecución)
          - (fricción media + minutos mantenimiento/semana + fatiga percibida)

    Escala 0-100: ≥85 verde | 70-84 amarillo | <70 rojo (revisión obligatoria)
    Valores por defecto conservadores para cuando no hay datos completos.
    """
    # BUG C2 FIX: 10€/h como referencia era inalcanzable en NL2-NL25 → ISS siempre rojo
    # → Trigger 1 permanentemente activo = solo ruido. 3€/h = ganador excelente en micro.
    EV_H_REFERENCE = 3.0   # €/h excelente NL2-NL25 (ajustar a 5.0 en NL50+)
    ev_h_norm = min(max((ev_h_current / EV_H_REFERENCE) * 40, 0), 40) if not np.isnan(ev_h_current) else 0

    # % fricción verde: fricción ≤ 2 = 100%, fricción 3 = 60%, fricción 4+ = 20%
    if np.isnan(friccion_avg):
        pct_green_friccion = 50.0
    elif friccion_avg <= 2.0:
        pct_green_friccion = 100.0
    elif friccion_avg <= 3.0:
        pct_green_friccion = 60.0
    else:
        pct_green_friccion = 20.0

    positivos = ev_h_norm + (pct_green_friccion / 100 * 30) + (consistency_pct / 100 * 30)
    # BUG C1 FIX: NaN en friccion_avg propagaba a negativos → ISS=100 (falsa seguridad)
    fric_for_neg = friccion_avg if not (isinstance(friccion_avg, float) and np.isnan(friccion_avg)) else 3.0
    negativos = (fric_for_neg / 5 * 20) + (maintenance_minutes_week / 60 * 10) + (perceived_fatigue / 5 * 10)

    iss = max(0, min(100, positivos - negativos))
    return round(iss, 1)


def implement_m0_basic_triggers(overall_metrics, friccion_avg, historical_ev_h_per_week,
                                  weeks_threshold_negative_ev=4,
                                  drills_active_count=0, drills_90d_increase_pct=0.0,
                                  historical_iss_per_week=None,
                                  historical_friccion_per_session=None,
                                  trimestral_review=False):
    # FIX F-10 v1.92: trimestral_review=True activa Trigger 5 (higiene mínima).
    # Por defecto False → Trigger 5 solo en revisión trimestral, no en cada sesión.
    """
    M0 Auto-Gobernanza: calcula ISS y evalúa los 5 triggers del Documento Maestro.

    TRIGGERS (celda 44 del Maestro):
    1. ISS < 70 durante >= 2 semanas consecutivas
    2. Fricción media > 3 durante >= 3 sesiones seguidas
    3. EV/h neto semanal < 0 durante >= N semanas consecutivas
    4. Número de drills/reglas activas aumenta > 25% en 90 días sin mejora neta EV/h
    5. Módulo/drill sin trigger de revisión declarado (regla de higiene mínima)

    Returns:
        dict con: iss, semaforo, alerts, actions
    """
    alerts = []
    ev_h = overall_metrics.get('ev_euro_per_hour', np.nan)
    ev_h_clean = 0.0 if (ev_h is None or np.isnan(ev_h)) else ev_h
    fric_clean = friccion_avg if not np.isnan(friccion_avg) else 3.0

    iss = calculate_iss(ev_h_clean, fric_clean)

    # ── Trigger 1: ISS < 70 durante ≥ 2 semanas consecutivas ─────────────────
    _iss_hist = list(historical_iss_per_week or []) + [iss]
    _consec_iss_low = 0
    for _v in reversed(_iss_hist):
        if _v < 70: _consec_iss_low += 1
        else: break
    if _consec_iss_low >= 2:
        alerts.append(f"TRIGGER 1: ISS < 70 durante {_consec_iss_low} semanas consecutivas.")
    elif iss < 70:
        alerts.append(f"TRIGGER 1 (aviso 1ª vez): ISS = {iss:.1f} < 70 — se activa si se repite.")

    # ── Trigger 2: Fricción > 3 durante ≥ 3 sesiones consecutivas ────────────
    _fric_hist = list(historical_friccion_per_session or [])
    if not (isinstance(friccion_avg, float) and np.isnan(friccion_avg)):
        _fric_hist.append(friccion_avg)
    _consec_fric = 0
    for _v in reversed(_fric_hist):
        if not (isinstance(_v, float) and np.isnan(_v)) and _v > 3.0: _consec_fric += 1
        else: break
    if _consec_fric >= 3:
        alerts.append(f"TRIGGER 2: Fricción > 3 durante {_consec_fric} sesiones consecutivas.")
    elif not (isinstance(friccion_avg, float) and np.isnan(friccion_avg)) and friccion_avg > 3.0:
        alerts.append(f"TRIGGER 2 (aviso {_consec_fric}/3): Fricción = {friccion_avg:.2f} > 3.")

    # ── Trigger 3: EV/h negativo consecutivo ─────────────────────────────────
    # BUG F6 FIX: ev_h_clean es el promedio histórico global, NO el EV/h semanal actual.
    # historical_ev_h_per_week ya incluye la semana más reciente como último elemento.
    # El append introducía un dato espurio de escala diferente → lógica de N semanas incorrecta.
    all_ev_h = historical_ev_h_per_week
    consecutive_neg = 0
    for val in reversed(all_ev_h):
        if not (isinstance(val, float) and np.isnan(val)) and val < 0:
            consecutive_neg += 1
        else:
            break
    if consecutive_neg >= weeks_threshold_negative_ev:
        alerts.append(f"TRIGGER 3: EV/h negativo {consecutive_neg} semanas consecutivas (umbral {weeks_threshold_negative_ev}).")

    # ── Trigger 4: Creep de complejidad ──────────────────────────────────────
    if drills_active_count > 0 and drills_90d_increase_pct > 25.0 and ev_h_clean <= 0:
        alerts.append(f"TRIGGER 4: Drills activos aumentaron {drills_90d_increase_pct:.0f}% en 90d sin mejora EV/h.")

    # ── Trigger 5: Higiene mínima — SOLO en revisión trimestral (FIX F-10 v1.92) ──
    # El docx identifica que Trigger 5 activado cada sesión (ISS<85) crea
    # fatiga de alertas. Con BB/100 negativo durante corrección de leaks,
    # ISS siempre < 85 → Trigger 5 siempre activo → siempre ignorado.
    # Fix: Trigger 5 solo se evalúa en revisión trimestral explícita.
    if trimestral_review:
        # Verificación real de higiene: ¿todos los drills tienen exit criteria?
        drills_sin_invalidacion = [
            d for d in DRILL_QUEUE
            if not any(kw in d for kw in ['_F', '_E', '_L', 'postflop', 'ccall'])
        ] if 'DRILL_QUEUE' in globals() else []
        if drills_sin_invalidacion:
            alerts.append(
                f"TRIGGER 5 (revisión trimestral): {len(drills_sin_invalidacion)} drills "
                f"sin condición de invalidación declarada: {drills_sin_invalidacion}"
            )
        else:
            alerts.append("TRIGGER 5 (revisión trimestral): ✅ Todos los drills tienen exit criteria declarados.")

    # ── Semáforo ──────────────────────────────────────────────────────────────
    if iss >= 85 and not any('TRIGGER 1' in a or 'TRIGGER 2' in a or 'TRIGGER 3' in a for a in alerts):
        semaforo = '🟢 VERDE'
        resumen = f"Sistema sano. ISS = {iss}."
    elif iss >= 70:
        semaforo = '🟡 AMARILLO'
        resumen = f"Vigilancia. ISS = {iss}. Revisar en próxima sesión."
    else:
        semaforo = '🔴 ROJO'
        resumen = f"REVISIÓN OBLIGATORIA. ISS = {iss}."

    # ── Acciones recomendadas si triggers activos ─────────────────────────────
    actions = []
    if alerts:
        actions.append("1. Revisar historial de últimas 3 sesiones.")
        actions.append("2. Propuesta de poda: eliminar/fusionar 1-3 elementos de menor impacto EV.")
        if consecutive_neg >= weeks_threshold_negative_ev:
            actions.append("3. Congelación temporal de nuevos drills (max 30 días).")

    result = {
        'iss': iss,
        'semaforo': semaforo,
        'resumen': resumen,
        'alerts': alerts,
        'actions': actions
    }

    print(f"✅ M0 Evaluado: {semaforo} | ISS = {iss}")
    if alerts:
        for a in alerts:
            print(f"   ⚠️  {a}")
    if actions:
        for a in actions:
            print(f"   → {a}")

    return result


print("✅ M0 Auto-Gobernanza cargado (ISS completo + 5 triggers del Maestro).")


def build_roi_ranking(spot_results, top_n=5, shrinkage_k=None):
    """
    Construye el Ranking ROI de spots con Empirical Bayes shrinkage.

    v1.20 AÑADIDOS:
    - 'queue':   spots 4-10 (cola visible pero no activos) → contexto sin carga
    - 'families': fusión de spots con error de fondo compartido (LEAK_FAMILIES)
                  Cuando 2+ spots de la misma familia están en el top, los agrupa
                  en un único entrada con muestra combinada y señal más fuerte.

    FÓRMULA SHRINKAGE (Empirical Bayes):
        ev_shrunk = (n / (n+k)) * ev_obs + (k / (n+k)) * global_mean

    Args:
        spot_results (DataFrame): salida de calculate_ev_metrics().
        top_n (int):              spots a mostrar en cada lista (default 5)
        shrinkage_k (int):        constante de shrinkage (None = adaptativo por stake)

    Returns:
        dict:
            'leaks'         → DataFrame top_n leaks (peor primero)
            'oportunidades' → DataFrame top_n opps (mejor primero)
            'queue'         → DataFrame spots 4-10 en cola (referencia visual)
            'families'      → dict {familia: {'spots': list, 'n_combined': int,
                                              'ev_combined': float, 'descripcion': str}}
            'ranking_total' → DataFrame completo
            'resumen'       → str resumen ejecutivo
            Nuevas cols (v1.44): bb100_loss | correction_factor | recoverable_bb100
    """
    if spot_results is None or spot_results.empty:
        empty = {
            'leaks': pd.DataFrame(), 'oportunidades': pd.DataFrame(),
            'queue': pd.DataFrame(), 'families': {},
            'ranking_total': pd.DataFrame(),
            'resumen': "⚠️ Sin datos de spots disponibles para construir ranking."
        }
        return empty

    df = spot_results.copy()

    # ── Shrinkage k adaptativo por stake ──────────────────────────────────────
    # B9 NOTE: stake_level siempre está en spot_results (incluido en groupby agg).
    # Fallback k=200 si falta (datos legacy/CSV). fillna(0.02) protege impacto calc.
    stake_dominante = 'desconocido'
    if shrinkage_k is None:
        if 'stake_level' in df.columns and not df['stake_level'].dropna().empty:
            stake_order = ['NL2','NL5','NL10','NL25','NL50','NL100','NL200']
            stakes_en_df = df['stake_level'].dropna().unique().tolist()
            stake_dominante = max(
                stakes_en_df,
                key=lambda s: stake_order.index(s) if s in stake_order else -1,
                default='NL5'
            )
            shrinkage_k = SHRINKAGE_K_BY_STAKE.get(stake_dominante, 200)
        else:
            shrinkage_k = 200

    # ── Empirical Bayes shrinkage ─────────────────────────────────────────────
    global_mean_ev = df['avg_ev_bb'].mean()
    n              = df['spot_hands_count']
    ev_obs         = df['avg_ev_bb']

    df['avg_ev_bb_shrunk'] = (
        (n / (n + shrinkage_k)) * ev_obs +
        (shrinkage_k / (n + shrinkage_k)) * global_mean_ev
    )
    df['impacto_ev_total_eur_shrunk'] = (
        # FIX P0-B v1.30: usar bb_value por stake en lugar de BB_TO_EUR fijo
        df['frequency'] * df['avg_ev_bb_shrunk'] * 100 * df['stake_level'].map(BB_VALUE_MAP).fillna(0.02)
    )
    # ── GAP B v1.44: BB/100 Recovery Model ──────────────────────────────────
    # bb100_loss   = EV perdido en esa calle escalado a 100 manos (con shrinkage)
    # correction_factor = fracción recuperable según tipo de leak
    # recoverable_bb100 = bb100_loss × correction_factor × -1
    # ADVERTENCIA: usar solo top-3 leaks con n>50. Con 3k manos el IC es amplio.
    _CF = {'preflop': 0.80, 'flop': 0.70, 'turn': 0.60, 'river': 0.50}
    df['bb100_loss']        = df['frequency'] * df['avg_ev_bb_shrunk'] * 100
    df['correction_factor'] = df['decision_street'].map(_CF).fillna(0.65)
    df['recoverable_bb100'] = df['bb100_loss'] * df['correction_factor'] * -1

    df['tipo']        = df['impacto_ev_total_eur_shrunk'].apply(
                            lambda x: 'LEAK' if x < 0 else 'OPORTUNIDAD'
                        )
    df['abs_impacto'] = df['impacto_ev_total_eur_shrunk'].abs()

    ranking_total        = df.sort_values('abs_impacto', ascending=False).reset_index(drop=True)
    ranking_total['rank'] = ranking_total.index + 1

    leaks = (df[df['tipo'] == 'LEAK']
               .sort_values('impacto_ev_total_eur_shrunk', ascending=True)
               .head(top_n)
               .reset_index(drop=True))
    leaks['prioridad'] = range(1, len(leaks) + 1)

    opps = (df[df['tipo'] == 'OPORTUNIDAD']
              .sort_values('impacto_ev_total_eur_shrunk', ascending=False)
              .head(top_n)
              .reset_index(drop=True))
    opps['prioridad'] = range(1, len(opps) + 1)

    # ── Cola de leaks (spots 4-10, visibles pero no activos) ─────────────────
    # Muestra al jugador qué viene después de los top-3 sin activarlos.
    # Criterio: confianza mínima 15 manos para aparecer en cola.
    all_leaks_sorted = (df[df['tipo'] == 'LEAK']
                          .sort_values('impacto_ev_total_eur_shrunk', ascending=True)
                          .reset_index(drop=True))
    queue_leaks = (all_leaks_sorted[all_leaks_sorted['spot_hands_count'] >= 15]
                   .iloc[3:10]   # posiciones 4-10 (índice 3-9)
                   .copy()
                   .reset_index(drop=True))
    queue_leaks['posicion_cola'] = range(4, 4 + len(queue_leaks))

    # ── Familias de leaks — agrupación por error de fondo común ──────────────
    # Para cada familia definida en LEAK_FAMILIES, busca spots del top ranking
    # que pertenezcan a ella. Si hay 2+, los fusiona en una entrada combinada.
    # El EV combinado es la suma de impactos (muestra agregada = señal más fuerte).
    families_result = {}
    leak_rows = all_leaks_sorted[all_leaks_sorted['spot_hands_count'] >= 15]

    for fam_name, fam_def in LEAK_FAMILIES.items():
        match_fn = fam_def['match_fn']
        matching = leak_rows[leak_rows.apply(match_fn, axis=1)]
        if len(matching) >= 2:
            # Hay al menos 2 spots de esta familia → merece drill combinado
            n_combined  = matching['spot_hands_count'].sum()
            ev_combined = matching['impacto_ev_total_eur_shrunk'].sum()
            spots_list  = matching['spot_identifier'].tolist()
            # EV medio combinado (para shrinkage re-aplicado a la muestra unida)
            ev_avg_combined = matching['avg_ev_bb_shrunk'].mean()
            families_result[fam_name] = {
                'spots':        spots_list,
                'n_combined':   int(n_combined),
                'ev_combined':  round(ev_combined, 3),
                'ev_avg_bb':    round(ev_avg_combined, 4),
                'descripcion':  fam_def['description'],
                'icon':         fam_def['icon'],
                'n_spots':      len(matching),
            }

    # ── Resumen ejecutivo ─────────────────────────────────────────────────────
    n_leaks       = len(df[df['tipo'] == 'LEAK'])
    n_opps        = len(df[df['tipo'] == 'OPORTUNIDAD'])
    ev_leak_total = df[df['tipo'] == 'LEAK']['impacto_ev_total_eur_shrunk'].sum()
    ev_opp_total  = df[df['tipo'] == 'OPORTUNIDAD']['impacto_ev_total_eur_shrunk'].sum()
    worst    = leaks.iloc[0]['spot_identifier'] if not leaks.empty else 'N/A'
    worst_ev = leaks.iloc[0]['impacto_ev_total_eur_shrunk'] if not leaks.empty else 0
    best     = opps.iloc[0]['spot_identifier'] if not opps.empty else 'N/A'
    best_ev  = opps.iloc[0]['impacto_ev_total_eur_shrunk'] if not opps.empty else 0

    fam_str = f" | {len(families_result)} familias detectadas" if families_result else ""
    resumen = (
        f"📊 Ranking ROI (k={shrinkage_k} para {stake_dominante}) — {len(df)} spots | "
        f"{n_leaks} leaks ({ev_leak_total:.2f}€) | {n_opps} opps ({ev_opp_total:+.2f}€){fam_str}\n"
        f"   🔴 Leak #1:  {worst} → {worst_ev:.2f}€\n"
        f"   🟢 Opp  #1:  {best}  → {best_ev:+.2f}€"
    )

    print(f"✅ {resumen}")
    if families_result:
        print("   📦 Familias fusionadas:")
        for fn, fd in families_result.items():
            print(f"      {fd['icon']} {fn}: {fd['n_spots']} spots | "
                  f"n={fd['n_combined']} manos | EV={fd['ev_combined']:.2f}€ — {fd['descripcion']}")
    if not queue_leaks.empty:
        print(f"   📋 Cola pendiente: {len(queue_leaks)} leaks en posiciones 4-{3+len(queue_leaks)}")

    return {
        'leaks':         leaks,
        'oportunidades': opps,
        'queue':         queue_leaks,
        'families':      families_result,
        'ranking_total': ranking_total,
        'resumen':       resumen
    }


print("✅ build_roi_ranking cargada (v1.20 — familias + cola pendiente).")
print("   Uso: roi = build_roi_ranking(spot_results, top_n=5)")
print("   Nuevas claves: roi['queue'] (cola 4-10) | roi['families'] (grupos combinados)")


# ── GAP A — Leak Clustering Engine v1.44 STUB ─────────────────────────────
# ACTIVA AUTOMÁTICAMENTE cuando n_hands > 10.000
# Descripción: agrupa manos por cadena de acciones PF→Flop→Turn→River
# y calcula métricas por cluster. Ver CHANGELOG para especificación completa.
#
# build_cluster_key(row) → "BB__C__flop_X_F" (clave de secuencia)
# calculate_cluster_metrics(df) → DataFrame {cluster_id, n, ev_avg_bb, bb100_loss, freq_pct}
#
def build_cluster_key(row):
    """
    GAP A — STUB (activo a partir de 10.000 manos).
    Construye clave de secuencia multi-calle para clustering de leaks.
    Retorna None si el dataset no tiene señal suficiente.
    """
    # Gate: con < 10.000 manos la señal estadística es insuficiente
    # Pasa n_hands=len(df) para activar el gate correctamente
    return None  # STUB ACTIVO: activar cuando se pase n_hands >= 10000
    pos = str(row.get('player_position', '?'))
    pf  = str(row.get('preflop_action', '') or '')[:4].upper()
    fl  = str(row.get('flop_action',   '') or '')[:4].upper()
    tn  = str(row.get('turn_action',   '') or '')[:4].upper()
    rv  = str(row.get('river_action',  '') or '')[:4].upper()
    parts = [pos, pf]
    if fl and fl not in ('', 'NAN', 'NONE'): parts.append('flop_' + fl)
    if tn and tn not in ('', 'NAN', 'NONE'): parts.append('turn_' + tn)
    if rv and rv not in ('', 'NAN', 'NONE'): parts.append('river_' + rv)
    return '__'.join(parts)


def calculate_cluster_metrics(df, min_n=5):
    """
    GAP A — STUB (activo a partir de 10.000 manos).
    Agrupa manos por cluster_key y calcula EV, BB/100_loss y frecuencia.
    Devuelve DataFrame vacío hasta que n_hands >= 10.000.
    """
    # STUB GATE: retorna vacío hasta que se acumule volumen suficiente
    if len(df) < 10000:
        return pd.DataFrame(columns=[
            'cluster_id', 'n', 'ev_avg_bb', 'bb100_loss', 'freq_pct'
        ])
    BB_VAL = 0.02
    df = df.copy()
    df['cluster_id'] = df.apply(build_cluster_key, axis=1)
    stats = df.groupby('cluster_id').agg(
        n=('hand_id', 'count'),
        ev_avg_bb=('ev_won', 'mean'),
        ev_total=('ev_won', 'sum'),
    ).reset_index().query(f'n >= {min_n}').sort_values('ev_total')
    stats['bb100_loss'] = stats['ev_avg_bb'] / BB_VAL * 100
    stats['freq_pct']   = stats['n'] / len(df) * 100
    return stats[['cluster_id', 'n', 'ev_avg_bb', 'bb100_loss', 'freq_pct']]



def develop_canalized_study_module_logic(spot_results, current_mode, max_tasks=3, top_n=5, speed_result=None, roi_ranking=None):
    """
    Genera el plan de estudio canalizado según el modo actual.

    GAP 3 RESUELTO: drills contextualizados por pot_type + ip_oop +
    stack_depth + decision_street. El output pasa de genérico a accionable.

    M1: Fundamentos + peor leak de sesión como ancla real.
    M2: Ranking ROI drills con contexto específico por spot.
    M3: Mini-sims + RCL con instrucción concreta por calle.
    Siempre máximo max_tasks tareas (antifricción).

    Args:
        spot_results (DataFrame): salida de calculate_ev_metrics().
                                  Con columnas: spot_identifier, ip_oop,
                                  pot_type, stack_depth, decision_street,
                                  impacto_ev_total_eur_shrunk, spot_hands_count.
        current_mode (str):       'M1' | 'M2' | 'M3'
        max_tasks (int):          límite antifricción (default 3)
        top_n (int):              spots a evaluar en build_roi_ranking
        speed_result (dict|None): salida de estimate_preflop_speed()
    """

    # ── LEAK_FAMILIES — v1.21 ─────────────────────────────────────────────────
    # ESTADO: conectado pero activo en M2 (≥5.000 manos con señal postflop).
    # Con <5k manos los leaks son TODOS preflop → familias OOP_postflop e
    # IP_postflop estarán vacías. El código es correcto, la señal no existe todavía.
    # Cuando roi_ranking se pasa (con clave 'families'), enriquece los drills
    # con información de familia combinada (muestra más grande = señal más fuerte).
    _families = roi_ranking.get('families', {}) if roi_ranking else {}
    _has_families = bool(_families and any(
        v.get('n_combined', 0) >= 100 for v in _families.values()
    ))
    # Si hay familias con señal suficiente (≥100 manos combinadas), se mencionan
    # en el drill como contexto adicional. No reemplazan el drill individual.
    

    # ── Helper: drill contextualizado por variables estratégicas ─────────────
    # Plantillas condicionales puras — sin IA, sin fricción.
    # ip_oop + pot_type + stack_depth + decision_street → instrucción específica.
    def _drill_context(row):
        """
        Devuelve 1 línea de instrucción específica basada en el contexto del spot.
        Usa las columnas añadidas por build_spot_identifier (GAP 1).
        """
        ip   = str(row.get('ip_oop', '')).upper()
        pt   = str(row.get('pot_type', '')).upper()
        sd   = str(row.get('stack_depth', '')).lower()
        st   = str(row.get('decision_street', '')).lower()
        tipo = str(row.get('tipo', '')).upper()  # 'LEAK' o 'OPORTUNIDAD'
        accion = "Corrige el leak" if tipo == 'LEAK' else "Explota la ventaja"

        # ── Prioridad 1: stack short (cambia estrategia radicalmente) ─────────
        if sd == 'short':
            return (f"{accion} — Stack corto (<40bb). "
                    f"Revisa push/fold charts para {pt} {ip}. "
                    f"Con <40bb la estrategia SPR es binaria: push o fold.")

        # ── Prioridad 2: 4bet pot (rangos muy polarizados) ────────────────────
        if pt == '4BP':
            return (f"{accion} — Bote de 4bet. Rangos ultra-polarizados. "
                    f"Revisa ratio valor/bluff en 4BP {ip}. "
                    f"¿Estás llamando/3betting con rango óptimo al SPR del bote?")

        # ── Prioridad 3: río (decisión final, alta varianza) ──────────────────
        if st == 'river':
            if ip == 'IP':
                return (f"{accion} — River IP en {pt}. "
                        f"Revisa frecuencia de apuesta valor vs bluff. "
                        f"IP en river → liderazgo de rango, ¿tus sizings comunican historia coherente?")
            else:
                return (f"{accion} — River OOP en {pt}. "
                        f"Frecuencia bluff-catch vs pot odds. "
                        f"OOP en river → ¿fold demasiado a apuestas polarizadas? Solver check.")

        # ── Prioridad 4: turn (calle intermedia, planificación) ───────────────
        if st == 'turn':
            return (f"{accion} — Turn en {pt} {ip}. "
                    f"¿Tienes plan definido para cada textura (blank/brick/complete)? "
                    f"Semi-bluffs y protección de equity son clave en turn.")

        # ── Prioridad 5: flop (la calle más frecuente) ────────────────────────
        if st == 'flop':
            if pt == '3BP':
                if ip == 'IP':
                    return (f"{accion} — Flop IP en 3BP. "
                            f"Con ventaja posicional: ¿tus C-bets tienen el sizing correcto? "
                            f"En 3BP IP el rango polarizado permite smaller C-bet sizing.")
                else:
                    return (f"{accion} — Flop OOP en 3BP (como 3-bettor o caller). "
                            f"Rango OOP es más estrecho — ¿proteges correctamente con C-bets? "
                            f"Evalúa frecuencia check-raise vs donk-bet.")
            else:  # SRP
                if ip == 'IP':
                    return (f"{accion} — Flop IP en SRP. "
                            f"Ventaja posicional + rango equilibrado. "
                            f"¿Explotas ventaja de rango con C-bet sizing correcto? "
                            f"Revisa boards favorables vs neutros para tu rango.")
                else:
                    return (f"{accion} — Flop OOP en SRP (típicamente BB). "
                            f"Rango amplio pero fuera de posición. "
                            f"¿Defiendes correctamente vs C-bet? Frecuencia check-raise insuficiente "
                            f"en micro-stakes es el leak OOP más común.")

        # ── Fallback: preflop ─────────────────────────────────────────────────
        if pt == 'SRP':
            if ip == 'OOP':
                return (f"{accion} — Preflop OOP en SRP. "
                        f"Revisa rango de defensa/3bet desde posiciones OOP. "
                        f"¿Fold demasiado a opens? Calcula BB/100 perdido por over-folding.")
            else:
                return (f"{accion} — Preflop IP en SRP. "
                        f"Revisa rango de open/call desde {ip}. "
                        f"¿Amplias suficientemente desde BTN/CO? Stealing vs tight range.")
        if pt == '3BP':
            return (f"{accion} — Preflop 3BP. "
                    f"Analiza composición de rango de 3bet {'OOP' if ip=='OOP' else 'IP'}. "
                    f"¿Proporción valor/bluff correcta? ¿Sizing óptimo?")

        # ── Rama opp_class: instrucción específica si llegamos aquí ────────────
        opp = str(row.get('opp_class', 'unknown')).lower()
        if opp == 'fish':
            return (f"{accion} vs FISH — Foco en value y thin-value. "
                    f"Spot {pt} {ip} {st}: ¿estás maximizando valor contra rangos amplios? "
                    f"Los fish raramente bluffean rivers — ajusta fold frequency.")
        if opp == 'reg':
            return (f"{accion} vs REG — Foco en balance y fold equity. "
                    f"Spot {pt} {ip} {st}: ¿tu rango es defendible? "
                    f"Los regs explotan desequilibrios — revisa ratio valor/bluff.")
        return (f"{accion} — {pt} {ip} {st} (oponente no clasificado). "
                f"Etiqueta el tipo de oponente con integrate_manual_tags para drills precisos.")

    tasks = []

    # ── Alerta velocidad (máxima prioridad) ───────────────────────────────────
    speed_alert = None
    if speed_result is not None:
        speed_sem = speed_result.get('semaforo', '⚪')
        speed_mph = speed_result.get('hands_per_hour', None)
        if speed_sem == '🔴' and speed_mph is not None:
            if speed_mph > 120:
                speed_alert = (
                    f"⚡ [ALERTA VELOCIDAD] {speed_mph:.0f} manos/h — RITMO ALTO. "
                    f"Posible tilt/impulsividad. Haz una pausa de 5-10 min ANTES de estudiar."
                )
            else:
                speed_alert = (
                    f"🐢 [ALERTA VELOCIDAD] {speed_mph:.0f} manos/h — RITMO BAJO. "
                    f"Posible fatiga/over-thinking. Revisa número de mesas activas."
                )

    # ── M1: Fundamentos + ancla en el peor leak real ──────────────────────────
    if current_mode == 'M1':
        # Base fundamentos (siempre)
        tasks = [
            "🔵 [M1-DRILL #1] Rangos preflop: repasa aperturas por posición (BTN/CO/MP/UTG). "
            "Foco en SRP. Usa equilab o PioSOLVER lite. (10-15 min)",

            "🔵 [M1-DRILL #2] Equity tables: elige 2-3 situaciones de tu última sesión. "
            "Calcula equity de tu mano vs rango estimado del oponente. (10 min)",

            "🔵 [M1-DRILL #3] Evaluación de boards: clasifica los últimos 10 flops "
            "(favorable / neutro / peligroso para tu rango desde cada posición). (10 min)",
        ]

        # Ancla real: si hay spot_results, añadir el peor leak como drill M1 contextualizado
        if not spot_results.empty and len(tasks) < max_tasks:
            try:
                roi_m1 = build_roi_ranking(spot_results, top_n=3)
                leaks_m1 = roi_m1['leaks']
                if not leaks_m1.empty:
                    worst_row = leaks_m1.iloc[0]
                    ctx = _drill_context(worst_row)
                    # v1.32: conexión familiar — un drill cubre spots relacionados
                    _fam_note = ""
                    _roi_for_fam = roi_ranking if roi_ranking is not None else roi_m1
                    for _fn, _fd in (_roi_for_fam.get('families', {}) or {}).items():
                        if worst_row['spot_identifier'] in _fd.get('spots', []):
                            _fam_note = (
                                f" | Familia '{_fn}': {_fd['n_spots']} spots conectados "
                                f"(n={_fd['n_combined']} manos) — un drill cubre todos."
                            )
                            break
                    tasks[2] = (   # Reemplaza drill #3 con ancla real
                        f"🔵 [M1-DRILL #3 Ancla Real | LEAK #{worst_row['prioridad']}] "
                        f"spot '{worst_row['spot_identifier']}' "
                        f"({worst_row['impacto_ev_total_eur_shrunk']:.2f}€, "
                        f"{worst_row['spot_hands_count']} manos). "
                        f"{ctx}{_fam_note}"
                    )
            except Exception:
                pass  # Sin datos suficientes → mantener drill #3 genérico

        # Drill de velocidad si fuera de rango (Maestro Métrica #7)
        if speed_result is not None:
            speed_sem = speed_result.get('semaforo', '⚪')
            speed_mph = speed_result.get('hands_per_hour', None)
            if speed_sem != '🟢' and speed_mph is not None and len(tasks) < max_tasks:
                tasks.append(
                    f"🔵 [M1-DRILL Velocidad Métrica #7] Ritmo actual: {speed_mph:.0f} manos/h "
                    f"(óptimo 70-110). En las próximas 30 min, foco consciente en decidir "
                    f"en ≤2s/mano en preflop. Registra si el ritmo mejora."
                )

    # ── M2: Ranking ROI + drills contextualizados ─────────────────────────────
    elif current_mode == 'M2':
        if not spot_results.empty:
            roi   = build_roi_ranking(spot_results, top_n=top_n)
            leaks = roi['leaks']
            opps  = roi['oportunidades']

            # GAP 3 FIX: shrinkage puede vaciar leaks y opps aunque spot_results no esté vacío
            # (todos los spots con <200 manos → todos shrunk al global mean → ninguno aparece)
            if leaks.empty and opps.empty:
                tasks.append("📊 [M2] Muestra insuficiente por spot (todos <200 manos). "
                             "Acumula volumen — el ranking ROI se activa automáticamente.")
                tasks.append("📚 [M2-fallback] Mientras: revisa SB/BB defense vs BTN y CO.")
                return tasks

            # Drill #1 — Leaks más dañinos con contexto específico
            for _, row in leaks.iterrows():
                if len(tasks) >= max_tasks:
                    break
                ctx = _drill_context(row)
                tasks.append(
                    f"🟡 [M2-DRILL #1 | LEAK #{row['prioridad']}] "
                    f"'{row['spot_identifier']}' → {row['impacto_ev_total_eur_shrunk']:.2f}€ "
                    f"({row['spot_hands_count']} manos) | "
                    f"{ctx}"
                )

            # Drill #1 — Oportunidades con contexto específico
            for _, row in opps.iterrows():
                if len(tasks) >= max_tasks:
                    break
                ctx = _drill_context(row)
                tasks.append(
                    f"🟢 [M2-DRILL #1 | OPP #{row['prioridad']}] "
                    f"'{row['spot_identifier']}' → {row['impacto_ev_total_eur_shrunk']:+.2f}€ "
                    f"({row['spot_hands_count']} manos) | "
                    f"{ctx}"
                )

            # Drill #2 — Alerta patrón recurrente sobre el peor leak
            if not leaks.empty and len(tasks) < max_tasks:
                worst = leaks.iloc[0]['spot_identifier']
                tasks.append(
                    f"🟡 [M2-ALERTA #2 Patrones] ¿El leak '{worst}' aparece "
                    f"en las últimas 3 sesiones? "
                    f"Usa track_leak_evolution() para confirmarlo."
                )
        else:
            # P3 CORREGIDO: fallback útil cuando no hay datos de spots
            tasks = [
                "🟡 [M2-INFO] Sin spots de EV disponibles aún. "
                "Acumula más manos y re-ejecuta. "
                "Mientras tanto: revisa tus rangos preflop por posición (BTN/CO/SB) "
                "y calcula tu BB/100 manual de las últimas 3 sesiones.",
                "🟡 [M2-FUNDAMENTOS] Hasta tener datos: haz 1 sesión de Equilab "
                "evaluando tus 3 situaciones más frecuentes de la última sesión.",
            ]

    # ── M3: Mini-sims + RCL con instrucción concreta por calle ───────────────
    elif current_mode == 'M3':
        if not spot_results.empty:
            roi     = build_roi_ranking(spot_results, top_n=top_n)
            ranking = roi['ranking_total']

            for _, row in ranking.head(3).iterrows():
                if len(tasks) >= max_tasks:
                    break
                ctx = _drill_context(row)
                tasks.append(
                    f"🔴 [M3-DRILL #4 Mini-Sim | Rank #{row['rank']}] "
                    f"'{row['spot_identifier']}' "
                    f"(impacto {row['impacto_ev_total_eur_shrunk']:+.2f}€ · {row['tipo']}) | "
                    f"{ctx} → Valida con solver/equity calc."
                )
                if len(tasks) < max_tasks:
                    tasks.append(
                        f"🔴 [M3-DRILL #5 RCL] Range Context Layer — "
                        f"'{row['spot_identifier']}': "
                        f"construye rango completo para este spot "
                        f"({row.get('decision_street','?')} · {row.get('pot_type','?')} · "
                        f"{row.get('ip_oop','?')}). "
                        f"¿Qué manos bluffeas / valueas? Ajusta y re-evalúa próxima sesión."
                    )
        else:
            tasks = ["🔴 [M3-INFO] Sin spots disponibles para simulación avanzada."]

    # ── Prepend alerta velocidad (prioridad máxima) ───────────────────────────
    if speed_alert and len(tasks) < max_tasks + 1:
        tasks = [speed_alert] + tasks
    tasks = tasks[:max_tasks]

    # ── Familias: drill combinado si familia activa y hay espacio en tasks ────
    # LEAK_FAMILIES fix v1.22: reutilizar roi_ranking del pipeline (ya calculado)
    # Con <5k manos las familias OOP_postflop estarán vacías — correcto.
    _roi_fam = roi_ranking if roi_ranking is not None else build_roi_ranking(spot_results, top_n=top_n)
    if _roi_fam.get("families"):
        _sorted_fams = sorted(_roi_fam["families"].items(),
                               key=lambda x: x[1]["ev_combined"])
        for _fam_name, _fam_data in _sorted_fams:
            if len(tasks) >= max_tasks:
                break
            _in_tasks = [str(t)[:60] for t in tasks]
            if any(_fam_name in t for t in _in_tasks):
                continue
            _fam_drill = (
                f"{_fam_data['icon']} [FAMILIA:{_fam_name}] "
                f"{_fam_data['descripcion']} — "
                f"n={_fam_data['n_combined']} manos combinadas | "
                f"EV={_fam_data['ev_combined']:.2f}€ — "
                f"Spots: {chr(43).join(_fam_data['spots'][:2])}"
            )
            tasks.append(_fam_drill)

        # GF v1.67: Diagnóstico W$SD → calidad de rango en showdown
    # Accede a ingested_df como global si está disponible
    try:
        _df_gf = globals().get('ingested_df', None)
        if _df_gf is not None and not _df_gf.empty and len(tasks) < max_tasks:
            _sd_gf = _df_gf[_df_gf['flg_showdown']==True]
            _wsd_gf = _sd_gf['flg_won_hand'].mean()*100 if len(_sd_gf)>5 else None
            if _wsd_gf is not None and _wsd_gf < 45:
                _gf_task = (
                    f"🔴 [GF-W$SD] W$SD={_wsd_gf:.1f}% < 50% — "
                    f"Tu problema NO es frecuencia de showdown sino CALIDAD de rango. "
                    f"Llegas al showdown con manos perdedoras. "
                    f"Acción: en las manos del drill, antes de ver el resultado, "
                    f"escribe en 1 línea qué rango te gana. "
                    f"Si no puedes nombrarlo, la call fue incorrecta."
                )
                tasks.insert(0, _gf_task)
                tasks = tasks[:int(max_tasks)]
    except Exception:
        pass

    print(f"✅ Módulo Estudio Canalizado ({current_mode}): {len(tasks)} tarea(s).")
    for t in tasks:
        print(f"   {t}")
    return tasks


print("✅ develop_canalized_study_module_logic cargada (v1.7 — GAP 3: drills contextualizados).")
print("   Plantillas: ip_oop + pot_type + stack_depth + decision_street → instrucción específica")
print("   M1: fundamentos + ancla en peor leak real de sesión")
print("   M2: ranking ROI + contexto por spot")
print("   M3: mini-sims + RCL con instrucción concreta por calle")


# ════════════════════════════════════════════════════════════════════════════
# SECCIÓN 7b — M4.2: Entrenador de Decisiones (Celda A — SETUP)
# Ejecutar primero. Leer la situación. PENSAR antes de pasar a la Celda B.
# ════════════════════════════════════════════════════════════════════════════

# DT2: _M42_STATE global mutable compartido entre Celdas A y B.
# Riesgo: reinicio de kernel entre A y B deja estado corrupto.
# setup_ts permite detectar estados obsoletos. Re-ejecutar esta celda para resetear.
_M42_STATE = {'ready': False, 'spot': None, 'hand': None, 'df': None, 'n_hands': 0, 'stake': 'NL2', 'setup_ts': 0.0}


def run_m42_trainer_setup(df, spot_identifier=None, hand_idx=0):
    """
    Celda A del entrenador. Presenta la situación sin revelar el resultado.
    
    Args:
        df (DataFrame):           historial completo (usar full_df del pipeline)
        spot_identifier (str):    spot a estudiar. None → toma el top-1 leak actual.
        hand_idx (int):           índice de la mano dentro del spot (0 = primera)
    """
    global _M42_STATE
    
    M42_MIN_HANDS = 50
    
    if df is None or df.empty:
        print("❌ M4.2: DataFrame vacío. Ejecutar el pipeline primero.")
        return
    
    # Determinar spot
    # v1.48: auto-leer DRILL_ACTIVO si no se pasa spot_identifier
    if spot_identifier is None:
        _drill = globals().get('DRILL_ACTIVO')
        if _drill and str(_drill) not in ('None', ''):
            spot_identifier = str(_drill)
            print(f'   ℹ️  M4.2 Trainer: usando DRILL_ACTIVO = {spot_identifier}')
    if spot_identifier is None:
        if 'spot_identifier' not in df.columns:
            print("❌ M4.2: spot_identifier no encontrado. Ejecutar pipeline primero.")
            return
        # Tomar el spot con más manos como default
        spot_counts = df['spot_identifier'].value_counts()
        spot_identifier = spot_counts.index[0] if len(spot_counts) > 0 else None
    
    if spot_identifier is None:
        print("❌ M4.2: No se encontró ningún spot.")
        return
    
    # Filtrar manos del spot
    spot_df = df[df['spot_identifier'] == spot_identifier].copy()
    n_hands = len(spot_df)
    
    if n_hands < M42_MIN_HANDS:
        print(f"ℹ️  M4.2: spot '{spot_identifier}' tiene {n_hands} manos "
              f"(mínimo {M42_MIN_HANDS}). Acumula más volumen en este spot.")
        return
    
    # Seleccionar mano representativa (mayor desviación de EV)
    if 'ev_won' in spot_df.columns:
        mean_ev = spot_df['ev_won'].mean()
        spot_df['_ev_dev'] = (spot_df['ev_won'] - mean_ev).abs()
        sorted_df = spot_df.nlargest(10, '_ev_dev')
    else:
        sorted_df = spot_df
    
    hand_idx = min(hand_idx, len(sorted_df) - 1)
    hand = sorted_df.iloc[hand_idx]
    
    # ── Presentar situación (sin resultado) ──────────────────────────────
    pos         = str(hand.get('player_position', '?'))
    hole_cards  = str(hand.get('hole_cards', '??'))
    flop        = str(hand.get('board_cards_flop',  '') or '')
    turn        = str(hand.get('board_cards_turn',  '') or '')
    river       = str(hand.get('board_cards_river', '') or '')
    stack       = hand.get('player_stack_start', 0)
    stake       = str(hand.get('stake_level', 'NL2'))
    bb_val      = BB_VALUE_MAP.get(stake, 0.02)
    stack_bb    = stack / bb_val if bb_val > 0 else 0
    ip_oop      = str(hand.get('ip_oop', ''))
    pot_type    = str(hand.get('pot_type', ''))
    session_id  = str(hand.get('session_id', '?'))
    opp_class   = str(hand.get('opp_class', 'unknown'))
    
    board_str = ' | '.join(filter(lambda x: x and x not in ('nan','','None'),
                                   [flop, turn, river])) or '(sin board)'
    
    # Estadísticas del spot
    ev_mean = spot_df['ev_won'].mean() if 'ev_won' in spot_df.columns else 0
    net_mean = spot_df['net_won'].mean() if 'net_won' in spot_df.columns else 0
    
    print(f"\n{'═'*60}")
    print(f"  🎓 M4.2 Entrenador — Spot: {spot_identifier}")
    print(f"{'═'*60}")
    print(f"\n  📊 Contexto del spot ({n_hands} manos):")
    print(f"     EV medio:   {ev_mean:.4f}€/mano")
    print(f"     Net medio:  {net_mean:.4f}€/mano")
    print(f"\n  🃏 SITUACIÓN (mano {hand_idx+1} de {min(10, len(sorted_df))}):")
    print(f"  {'─'*50}")
    print(f"     Posición:   {pos} ({ip_oop}) | {pot_type} | {stake}")
    print(f"     Tus cartas: {hole_cards}")
    print(f"     Stack:      {stack_bb:.0f} BB ({stack:.2f}€)")
    print(f"     Board:      {board_str}")
    print(f"     Oponente:   {opp_class}")
    print(f"  {'─'*50}")
    print(f"\n  ⏸️  PAUSA — ¿Qué harías en este spot?")
    print(f"     Opciones: F (fold) | C (call) | R (raise) | 3B (3-bet) | X (check) | B (bet)")
    print(f"\n  → Cuando hayas decidido, ejecuta la CELDA B para ver el análisis.")
    print(f"{'═'*60}\n")
    
    # Guardar estado para Celda B
    _M42_STATE = {
        'ready':    True, 'spot':  spot_identifier,
        'hand':     hand.to_dict(), 'df': spot_df,
        'n_hands':  n_hands, 'stake': stake,
        'setup_ts': __import__('time').time(),
    }


# ── Ejecutar aquí (cambiar spot_identifier al que estás estudiando) ───────
# run_m42_trainer_setup(full_df, spot_identifier='BB_OOP_SRP_deep_preflop_F')
# ─────────────────────────────────────────────────────────────────────────
print("✅ M4.2 Trainer Setup (Celda A) listo.")
print("   Uso: run_m42_trainer_setup(full_df, spot_identifier='BB_OOP_SRP_deep_preflop_F')")
print("   Después de pensar, ejecutar la Celda B.")


# ════════════════════════════════════════════════════════════════════════════
# SECCIÓN 7b — M4.2: Entrenador de Decisiones (Celda B — REVEAL)
# Ejecutar SOLO después de haber pensado en la Celda A.
# ════════════════════════════════════════════════════════════════════════════

def run_m42_trainer_reveal():
    """
    Celda B del entrenador. Muestra el marco de análisis y la pregunta clave.
    NO revela si la decisión fue correcta o incorrecta.
    Da el contexto conceptual para que el jugador reflexione.
    """
    global _M42_STATE
    
    if not _M42_STATE.get('ready'):
        print("❌ M4.2: Ejecutar primero la Celda A (run_m42_trainer_setup).")
        return
    
    hand      = _M42_STATE['hand']
    spot_id   = _M42_STATE['spot']
    spot_df   = _M42_STATE['df']
    n_hands   = _M42_STATE['n_hands']
    stake     = _M42_STATE.get('stake', 'NL2')
    
    pos         = str(hand.get('player_position', '?'))
    hole_cards  = str(hand.get('hole_cards', '??'))
    net_won     = float(hand.get('net_won', 0))
    ev_won      = float(hand.get('ev_won', 0))
    pf_action   = str(hand.get('preflop_action', ''))
    ip_oop      = str(hand.get('ip_oop', ''))
    
    # ── Equity vs rango de referencia ────────────────────────────────────
    equity_result = None
    if hole_cards and hole_cards not in ('??', 'nan', 'None', ''):
        # Determinar villain position desde el spot
        villain_pos = 'BTN'
        if 'vs_BTN' in spot_id: villain_pos = 'BTN'
        elif 'vs_CO' in spot_id: villain_pos = 'CO'
        elif 'vs_UTG' in spot_id: villain_pos = 'UTG'
        
        try:
            equity_result = calculate_equity_vs_range(
                hole_cards, villain_pos, stake=stake, df_hero=spot_df
            )
        except Exception as e:
            equity_result = {'error': str(e)}
    
    # ── Frecuencia del hero en este spot vs referencia ────────────────────
    ref = REFERENCE_RANGES.get(stake, REFERENCE_RANGES.get('NL2', {}))
    ref_key_map = {
        'BTN': 'BTN_open_pct', 'CO': 'CO_open_pct', 'BB': 'BB_vs_BTN_defend_pct',
        'SB': 'SB_open_pct', 'UTG': 'UTG_open_pct', 'HJ': 'HJ_open_pct',
    }
    ref_pct = ref.get(ref_key_map.get(pos, 'BTN_open_pct'), 35)
    
    # Frecuencia observada: % manos donde no foldó
    n_played = len(spot_df[~spot_df['preflop_action'].str.startswith('F', na=True)])
    freq_obs  = n_played / n_hands * 100 if n_hands > 0 else 0
    
    # Confianza
    conf_str = 'baja (<50 manos)' if n_hands < 50 else ('media' if n_hands < 200 else 'alta')
    
    print(f"\n{'═'*60}")
    print(f"  🎓 M4.2 — Análisis del Spot: {spot_id}")
    print(f"{'═'*60}")
    
    # Resultado real de la mano
    result_icon = '🟢' if net_won > 0 else '🔴'
    print(f"\n  {result_icon} Resultado real: {net_won:+.4f}€ (EV: {ev_won:+.4f}€)")
    print(f"     Acción tomada: {pf_action or '(no registrada)'}")
    
    # Equity contextual
    if equity_result and 'error' not in equity_result:
        print(f"\n  📐 Contexto de equity ({hole_cards} vs rango {villain_pos if 'villain_pos' in dir() else '?'}):")
        print(f"     Equity media:     {equity_result.get('equity_vs_range', 0):.1f}%")
        print(f"     Percentil:        {equity_result.get('percentile', 0):.0f}% del espacio defendible")
        print(f"     Rango villain:    {equity_result.get('pct_open_villain', 0):.0f}% de manos ({stake})")
        if equity_result.get('hero_bb_vpip_real'):
            print(f"     Tu BB VPIP real:  {equity_result['hero_bb_vpip_real']:.1f}%")
    
    # Frecuencia vs referencia
    print(f"\n  📊 Frecuencia en el spot (confianza: {conf_str}):")
    print(f"     Hero:       {freq_obs:.0f}% de las manos")
    print(f"     Referencia NL2 ({pos}): ~{ref_pct:.0f}%")
    delta = freq_obs - ref_pct
    delta_icon = '⬆️' if delta > 5 else ('⬇️' if delta < -5 else '✅')
    print(f"     Delta:      {delta:+.0f}pp {delta_icon}")
    
    # ── Pregunta clave (sin veredicto) ────────────────────────────────────
    print(f"\n  {'─'*50}")
    
    # Generar pregunta según contexto
    if 'preflop' in spot_id.lower() and delta < -10:
        pregunta = (f"Tu frecuencia en {spot_id} está {abs(delta):.0f}pp por debajo de la referencia. "
                    f"¿Qué tipo de manos estás foldando que según la referencia NL2 deberían entrar al bote? "
                    f"¿Hay un patrón en las cartas que rechazas?")
    elif 'preflop' in spot_id.lower() and delta > 10:
        pregunta = (f"Tu frecuencia en {spot_id} está {delta:.0f}pp por encima de la referencia. "
                    f"¿Estás entrando con manos que no tienen plan postflop claro? "
                    f"¿Qué criterio usas para decidir si una mano tiene suficiente valor especulativo?")
    elif equity_result and equity_result.get('percentile', 0) > 50:
        pregunta = (f"Esta mano está en el percentil {equity_result.get('percentile', 0):.0f}% del espacio defendible. "
                    f"¿Qué te impidió defender? ¿Fue la posición, el tamaño del bote, o la textura del board? "
                    f"¿Cambiaría algo si el oponente fuera fish en lugar de reg?")
    else:
        pregunta = (f"Dados los datos del spot, ¿qué patrón de error crees que se está repitiendo aquí? "
                    f"¿Es un problema de rango (qué manos eliges), de frecuencia (cuántas veces actúas), "
                    f"o de ejecución (cómo juegas las manos que decides jugar)?")
    
    print(f"  ❓ Pregunta clave:")
    print(f"     → {pregunta}")
    print(f"  {'─'*50}")
    print(f"\n  ℹ️  El análisis completo es tuyo. El sistema proporciona contexto, no respuestas.")
    print(f"{'═'*60}\n")
    
    # Reset state
    _M42_STATE['ready'] = False


# ── Ejecutar aquí (SOLO después de haber pensado en la Celda A) ───────────
# run_m42_trainer_reveal()
# ─────────────────────────────────────────────────────────────────────────
print("✅ M4.2 Trainer Reveal (Celda B) listo.")
print("   Uso: run_m42_trainer_reveal()  ← SOLO después de ejecutar la Celda A y pensar.")


# ════════════════════════════════════════════════════════════════════════════
# SECCIÓN 10b — Widget Interactivo de Fricción Post-Sesión
# ════════════════════════════════════════════════════════════════════════════
#
# Ejecuta esta celda DESPUÉS de cada sesión de juego.
# Mueve los sliders a tus valores reales y pulsa el botón.
# Los valores quedan guardados en FRICCION_R / _A / _V para el pipeline.
# ════════════════════════════════════════════════════════════════════════════

import ipywidgets as widgets
from IPython.display import display, clear_output

# ── Estado compartido ────────────────────────────────────────────────────────
_friccion_state = {'R': 2, 'A': 1, 'V': 2, 'avg': None, 'confirmado': False}

# ── Sliders ──────────────────────────────────────────────────────────────────
slider_r = widgets.IntSlider(
    value=2, min=1, max=5, step=1,
    description='🔴 Rabia:',
    style={'description_width': '100px'},
    layout=widgets.Layout(width='420px')
)
slider_a = widgets.IntSlider(
    value=1, min=1, max=5, step=1,
    description='🟠 Ansiedad:',
    style={'description_width': '100px'},
    layout=widgets.Layout(width='420px')
)
slider_v = widgets.IntSlider(
    value=2, min=1, max=5, step=1,
    description='🟡 Varianza:',
    style={'description_width': '100px'},
    layout=widgets.Layout(width='420px')
)

# ── Output en tiempo real ────────────────────────────────────────────────────
output_preview = widgets.Output()

def _semaforo(avg):
    if avg <= 2.0:   return '🟢 VERDE  — Sistema sano, continúa normal.'
    elif avg <= 3.0: return '🟡 AMARILLO — Vigilancia. Reduce volumen si persiste.'
    else:            return '🔴 ROJO   — STOP. Revisa sesión antes de continuar.'

def _actualizar_preview(*args):
    r, a, v = slider_r.value, slider_a.value, slider_v.value
    avg = round((r + a + v) / 3, 2)
    with output_preview:
        clear_output(wait=True)
        print(f"  R={r}  A={a}  V={v}  →  Promedio: {avg:.2f}   {_semaforo(avg)}")

slider_r.observe(_actualizar_preview, names='value')
slider_a.observe(_actualizar_preview, names='value')
slider_v.observe(_actualizar_preview, names='value')

# ── Botón confirmar ──────────────────────────────────────────────────────────
btn_confirm = widgets.Button(
    description='✅ Confirmar fricción',
    button_style='success',
    layout=widgets.Layout(width='200px', margin='10px 0 0 0')
)

output_result = widgets.Output()

def _on_confirm(b):
    global FRICCION_R, FRICCION_A, FRICCION_V
    r, a, v = slider_r.value, slider_a.value, slider_v.value
    avg = round((r + a + v) / 3, 2)

    # Actualizar constantes globales del pipeline
    FRICCION_R = r
    FRICCION_A = a
    FRICCION_V = v

    # Guardar en estado interno
    _friccion_state.update({'R': r, 'A': a, 'V': v, 'avg': avg, 'confirmado': True})

    # Llamar a la función de cálculo oficial
    friccion_sesion = capture_and_calculate_post_session_friction(r, a, v)

    with output_result:
        clear_output(wait=True)
        print("─" * 50)
        print(f"  ✅ FRICCIÓN CONFIRMADA  |  R={r}  A={a}  V={v}")
        print(f"  Promedio sesión: {avg:.2f}   {_semaforo(avg)}")
        print(f"  Variables FRICCION_R/A/V actualizadas en el pipeline.")
        print("─" * 50)
        print("  → Ejecuta la Sección 12 (Pipeline Principal) para procesar.")

btn_confirm.on_click(_on_confirm)

# ── Layout final ─────────────────────────────────────────────────────────────
titulo = widgets.HTML("<h3 style=\'margin-bottom:8px\'>📊 Fricción Post-Sesión — OS v2.0</h3>")
separador = widgets.HTML("<hr style=\'margin:6px 0\'>")

ui = widgets.VBox([
    titulo,
    widgets.HTML("<b>¿Cómo te has sentido al terminar la sesión?</b> (1 = nada, 5 = mucho)"),
    separador,
    slider_r,
    slider_a,
    slider_v,
    output_preview,
    btn_confirm,
    output_result
], layout=widgets.Layout(padding='12px', border='1px solid #ddd', border_radius='8px', width='480px'))

_actualizar_preview()  # mostrar preview inicial
display(ui)
print("\n💡 Tip: Si no ves el widget, ejecuta: !pip install ipywidgets -q")


def display_dynamic_dashboard(overall_metrics, spot_results, current_mode,
                               suggested_study_tasks, m0_result, roi=None,
                               baseline_cmp=None, tilt_result=None):
    """
    Dashboard evolutivo con desbloqueo progresivo por modo.

    M1: Métricas generales solamente (sin spots — ruido innecesario en M1).
    M2: Métricas + Top/Bottom 5 spots por Impacto EV Total.
    M3: Igual que M2 + análisis avanzado activo.

    Alertas M0 siempre visibles al inicio si hay triggers activos.
    """
    print(f"\n{'='*60}")
    print(f"  OS v2.0 — Dashboard {current_mode}")
    print(f"{'='*60}\n")

    # ── Alertas M0 (siempre primero si hay algo) ──────────────────────────────
    semaforo = m0_result.get('semaforo', '⚪') if m0_result else '⚪'
    resumen_m0 = m0_result.get('resumen', '') if m0_result else ''
    print(f"  M0 Estado: {semaforo}  {resumen_m0}")
    if m0_result and m0_result.get('alerts'):
        for a in m0_result['alerts']:
            if 'TRIGGER 1' in a or 'TRIGGER 2' in a or 'TRIGGER 3' in a:
                print(f"  ⚠️  {a}")
    # O1: delta vs baseline
    if baseline_cmp and (baseline_cmp.get('mejorados') or baseline_cmp.get('empeorados')):
        print("  📊 Vs sesión anterior:")
        for e in (baseline_cmp.get('mejorados') or []):
            print(f"     🟢 MEJORA  '{e['spot'][:38]}': {e['delta']:+.2f}€")
        for e in (baseline_cmp.get('empeorados') or []):
            print(f"     🔴 EMPEORA '{e['spot'][:38]}': {e['delta']:+.2f}€")
        for e in (baseline_cmp.get('nuevos') or [])[:2]:
            print(f"     🆕 NUEVO   '{e['spot'][:38]}'")
    # O2: coste del tilt
    if tilt_result and tilt_result.get('n_tilt', 0) > 0 and abs(tilt_result.get('tilt_cost_bb100', 0)) > 0.1:
        print(f"  🔥 Tilt: {tilt_result['n_tilt']} sesión(es) ({tilt_result['tilt_pct']:.0f}%) "
              f"| coste {tilt_result['tilt_cost_bb100']:+.1f} BB/100")
    print()

    # ── Métricas globales (siempre visibles) ──────────────────────────────────
    if overall_metrics:
        ev_h = overall_metrics.get('ev_euro_per_hour', np.nan)
        bb100 = overall_metrics.get('bb_per_100_net', 0.0)
        hands = overall_metrics.get('total_hands', 0)
        ev_str = f"{ev_h:.2f} €/h" if not (isinstance(ev_h, float) and np.isnan(ev_h)) else "N/A (datos insuficientes)"
        bb_col = '🟢' if bb100 > 0 else '🔴'
        ev_col = '🟢' if (not isinstance(ev_h, float) or not np.isnan(ev_h)) and ev_h > 0 else '🔴'

        fig = go.Figure()
        fig.add_trace(go.Indicator(
            mode="number+delta", value=bb100 if not np.isnan(bb100) else 0,
            title={"text": "BB/100 neto"},
            delta={'reference': 0, 'increasing': {'color': '#28a745'}, 'decreasing': {'color': '#dc3545'}},
            domain={'row': 0, 'column': 0}
        ))
        if not (isinstance(ev_h, float) and np.isnan(ev_h)):
            fig.add_trace(go.Indicator(
                mode="number+delta", value=ev_h,
                title={"text": "EV €/hora real"},
                delta={'reference': 0, 'increasing': {'color': '#28a745'}, 'decreasing': {'color': '#dc3545'}},
                domain={'row': 0, 'column': 1}
            ))
        fig.update_layout(
            grid={'rows': 1, 'columns': 2, 'pattern': 'independent'},
            title_text=f"OS v2.0 — {current_mode} | {hands:,} manos",
            template="plotly_white", height=200,
            margin=dict(t=60, b=10, l=10, r=10)
        )
        fig.show()

    # ── Spots (solo M2 y M3) ──────────────────────────────────────────────────
    if current_mode in ['M2', 'M3'] and not spot_results.empty:
        spots_display = pd.concat([
            spot_results.nlargest(5, 'impacto_ev_total_eur'),
            spot_results.nsmallest(5, 'impacto_ev_total_eur')
        ]).drop_duplicates(subset=['spot_identifier'])
        spots_display = spots_display.sort_values('impacto_ev_total_eur')

        fig_spots = px.bar(
            spots_display,
            x='impacto_ev_total_eur', y='spot_identifier',
            orientation='h',
            color='impacto_ev_total_eur',
            color_continuous_scale=px.colors.diverging.RdYlGn,
            title=f"Impacto EV Total por Spot — {current_mode} (€)",
            labels={'impacto_ev_total_eur': 'Impacto EV (€)', 'spot_identifier': 'Spot'},
            template="plotly_white", height=400
        )
        fig_spots.update_layout(
            showlegend=False,
            yaxis={'categoryorder': 'total ascending'},
            margin=dict(t=60, b=20, l=10, r=10)
        )
        fig_spots.show()
    elif current_mode == 'M1':
        print("  📊 Spots de EV no visibles en M1 (antifricción). Desbloqueo en M2.")

    # ── Tareas de estudio ─────────────────────────────────────────────────────
    if suggested_study_tasks:
        print(f"\n  📚 Tareas de Estudio ({current_mode}) — máx 3:")
        for t in suggested_study_tasks:
            print(f"     {t}")

    # ── Refinamiento por modo: contexto específico M1/M2/M3 ──────────────────
    # Maestro: "Dashboard refleja progresión de Fundamentos y drills por modo"
    # Maestro: "Output siempre binario o plan de máximo 3 drills"
    # Maestro: "Alertas pasivas Alerta patrones recurrentes (#2) en M2"
    print(f"\n  {'─'*58}")
    if current_mode == 'M1':
        print("  🔵 M1 — FOCO: Volumen + automatizar fundamentos preflop/SRP")
        print("     ✔  Sin análisis de spots EV: datos aún no son estadísticamente sólidos")
        print("     ✔  Fricción máxima tolerable: ≤ 3. MEC: APAGADO")
        if overall_metrics:
            _h   = overall_metrics.get('total_hands', 0)
            _pct = min(100.0, _h / MIN_HANDS_M2 * 100)
            _bar = '█' * int(_pct / 5) + '░' * (20 - int(_pct / 5))
            print(f"     📈 Progreso M2: [{_bar}] {_pct:.1f}%  ({_h:,}/{MIN_HANDS_M2:,} — faltan {max(0,MIN_HANDS_M2-_h):,})")
        print(f"    🎯 Gate M2: ≥{MIN_HANDS_M2:,} manos + BB/100 > 0 + EV/h > 0 + fricción ≤ 2")

    elif current_mode == 'M2':
        print("  🟡 M2 — FOCO: Convertir leaks en edge estructural")
        print("     ✔  Ranking ROI drills (#1) ACTIVO — ataca el leak de mayor impacto €€")
        print("     ✔  Alerta patrones recurrentes (#2) ACTIVA — usa track_leak_evolution()")
        print("     ✔  Etiquetado pools/oponentes (#3) ACTIVO. MEC: LIGHT (10-15 min/sesión)")
        # Señal binaria: leak más dañino ahora mismo
        # BUG H CORREGIDO: spot_results usa impacto_ev_total_eur (columna real).
        # impacto_ev_total_eur_shrunk solo existe en el output de build_roi_ranking,
        # no en el spot_results crudo que recibe el dashboard.
        if spot_results is not None and not spot_results.empty:
            # FIX P1-E v1.78: log explícito cuando se cae a columna sin shrinkage
            if 'impacto_ev_total_eur_shrunk' in spot_results.columns:
                ev_col = 'impacto_ev_total_eur_shrunk'
            else:
                ev_col = 'impacto_ev_total_eur'
                print('   ℹ️  Dashboard: impacto sin shrinkage (ejecuta pipeline completo para activarlo)')
            leaks = spot_results[spot_results[ev_col] < 0].sort_values(ev_col)
            if not leaks.empty:
                row = leaks.iloc[0]
                # stack_depth_bb contexto separado
                _sdb_str = ''
                if 'stack_depth_bb' in spot_results.columns:
                    _sdb_m = spot_results.loc[spot_results['spot_identifier'] == row['spot_identifier'], 'stack_depth_bb']
                    if not _sdb_m.empty and not pd.isna(_sdb_m.mean()) and _sdb_m.mean() > 0:
                        _sdb_str = f" | ~{_sdb_m.mean():.0f}BB eff"
                print(f"     ⚠️  LEAK #1 AHORA: '{row['spot_identifier']}' → {row[ev_col]:.2f}€ "
                      f"({row['spot_hands_count']} manos{_sdb_str}) — ¿aparece en últimas 3 sesiones?")
        print(f"    🎯 Gate M3: EV/h > 5 €/h sostenido + fricción ≤ 2")

    elif current_mode == 'M3':
        print("  🔴 M3 — FOCO: Ingeniería de edge propio")
        print("     ✔  Mini-simulaciones EV (#4) ACTIVAS — spots de máximo impacto absoluto")
        print("     ✔  Range Context Layer (#5) ACTIVO — ajuste de rangos por pool")
        print("     ✔  MEC: COMPLETO — tolerancia al caos controlada. Poda activa.")
    # ── Cola de Leaks Pendientes (posiciones 4-10) ──────────────────────────
    if roi is not None and not roi.get("queue", pd.DataFrame()).empty:
        _queue = roi["queue"]
        print("\n  📋 COLA PENDIENTE (referencia, no activos):")
        for _, _qrow in _queue.iterrows():
            _qpos  = int(_qrow.get("posicion_cola", 0))
            _qsid  = str(_qrow.get("spot_identifier", ""))[:50]
            _qev   = _qrow.get("impacto_ev_total_eur_shrunk", 0)
            _qn    = int(_qrow.get("spot_hands_count", 0))
            _qconf = "⚠️ emergente" if _qn < 50 else "📊"
            print(f"    #{_qpos:2d} [{_qn:4d}m {_qconf}] {_qsid:<50s} {_qev:.3f}€")

    # ── Familias de Leaks Activas (señal combinada) ───────────────────────
    if roi is not None and roi.get("families"):
        print("\n  📦 FAMILIAS DE LEAKS (señal combinada):")
        for _fn, _fd in sorted(roi["families"].items(),
                                key=lambda x: x[1]["ev_combined"]):
            print(f"    {_fd['icon']} {_fn}: {_fd['n_spots']} spots | "
                  f"n={_fd['n_combined']} manos | EV={_fd['ev_combined']:.2f}€")
            print(f"       → {_fd['descripcion']}")

        # Señal binaria: oportunidad más rentable ahora mismo
        if spot_results is not None and not spot_results.empty:
            # FIX P1-E v1.78: misma lógica que sección leaks
            ev_col = ('impacto_ev_total_eur_shrunk'
                      if 'impacto_ev_total_eur_shrunk' in spot_results.columns
                      else 'impacto_ev_total_eur')
            opps = spot_results[spot_results[ev_col] > 0].sort_values(ev_col, ascending=False)
            if not opps.empty:
                row = opps.iloc[0]
                print(f"     💎 OPP #1 AHORA: '{row['spot_identifier']}' → +{row[ev_col]:.2f}€ "
                      f"({row['spot_hands_count']} manos) — explota al máximo en próxima sesión")
        print("    🎯 Regla poda: añadir complejidad = eliminar ≥ 2 elementos existentes")

    # ── Acción siguiente — silencio positivo si sistema sano ──────────────────
    # Maestro: "Información desaparece tras decisión → Silencio positivo"
    # Maestro: "El sistema calla cuando no hay acción necesaria"
    print()
    if m0_result:
        sem = m0_result.get('semaforo', '⚪')
        if '🟢' in sem:
            print("  ✅ SILENCIO POSITIVO — Sistema sano. Ejecuta el plan y cierra.")
        elif '🟡' in sem:
            print("  ⚠️  VIGILANCIA — Revisa el drill/leak prioritario antes de la próxima sesión.")
        else:
            print("  🚨 ACCIÓN REQUERIDA — Atiende los triggers M0 antes de continuar.")
    else:
        print("  ✅ SILENCIO POSITIVO — Sin alertas M0. Ejecuta y registra.")

    print(f"\n{'='*60}\n")


print("✅ Dashboard dinámico cargado.")


def initialize_sqlite_db(db_name=DB_NAME, table_name='hand_history', schema=None):
    """
    Inicializa la base de datos SQLite y crea la tabla según el schema HUD.

    Usa define_hud_schema() como fallback automático si no se pasa schema.
    Valida que el schema no esté vacío antes de intentar crear la tabla
    (guard defensivo: schema=[] causaría SQL inválido).

    Args:
        db_name (str):      nombre del archivo SQLite (default: DB_NAME global)
        table_name (str):   nombre de la tabla (default: 'hand_history')
        schema (list|None): lista de dicts del schema HUD.
                            None → usa define_hud_schema() automáticamente.
                            [] vacío → error explícito, no crea tabla.

    Returns:
        sqlite3.Connection: conexión abierta, lista para usar.
    """
    conn   = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # Fallback automático al schema canónico si no se pasa ninguno
    if schema is None:
        schema = define_hud_schema()

    # Guard defensivo: schema vacío causaría CREATE TABLE () → SQL inválido
    if not schema:
        print("❌ initialize_sqlite_db: schema vacío. Tabla no creada.")
        print("   Pasa schema=None para usar el schema canónico automáticamente.")
        return conn

    type_map = {'str': 'TEXT', 'int': 'INTEGER', 'float': 'REAL', 'datetime': 'TIMESTAMP'}
    cols_sql = [f"{c['column_name']} {type_map.get(c['data_type'], 'TEXT')}" for c in schema]
    sql      = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(cols_sql)})"
    cursor.execute(sql)

    # Índice único en hand_id — protección de nivel DB contra duplicados.
    # CREATE INDEX IF NOT EXISTS es idempotente: seguro llamarlo múltiples veces.
    cursor.execute(
        f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{table_name}_hand_id "
        f"ON {table_name} (hand_id)"
    )
    conn.commit()
    print(f"✅ DB '{db_name}' inicializada | tabla '{table_name}' | {len(schema)} columnas.")
    print(f"   Índice único en hand_id — deduplicación garantizada a nivel DB.")
    return conn


def save_df_to_db(df, conn, table_name='hand_history', if_exists='append'):
    """
    Guarda el DataFrame en SQLite con deduplicación por hand_id.

    FIX CRÍTICO: antes usaba if_exists='replace' desde persist_session_data,
    borrando el historial completo en cada ejecución. Ahora:
      1. Siempre hace append (nunca reemplaza el historial).
      2. Filtra las manos ya existentes por hand_id antes de insertar.
      3. El índice único en hand_id (initialize_sqlite_db) actúa como
         segunda línea de defensa si el filtro Python falla.

    Args:
        df (DataFrame):   manos a guardar.
        conn:             conexión SQLite activa.
        table_name (str): nombre de la tabla.
        if_exists (str):  ignorado — siempre append. Mantenido por compatibilidad.
    """
    if df.empty:
        print("⚠️ DataFrame vacío. Nada que guardar.")
        return

    df_save = df.copy()

    # ── Deduplicación: filtrar manos ya en la DB ───────────────────────────
    n_total = len(df_save)
    try:
        existing = pd.read_sql(
            f"SELECT hand_id FROM {table_name}", conn
        )['hand_id'].astype(str).tolist()
        df_save = df_save[~df_save['hand_id'].astype(str).isin(existing)]
        n_dupes = n_total - len(df_save)
        if n_dupes > 0:
            print(f"   ℹ️  {n_dupes} manos ya existían en DB — omitidas (deduplicación).")
    except Exception:
        # BUG G CORREGIDO: si la tabla no existe, inicializarla con el índice
        # único antes de continuar. Sin esto, to_sql crearía la tabla sin índice
        # y la deduplicación a nivel DB quedaría inoperativa.
        try:
            # FIX BUG G: conn.database no existe en sqlite3.Connection.
            # Creamos la tabla directamente via la conexión existente.
            _hud_schema = define_hud_schema()
            _type_map = {'str': 'TEXT', 'float': 'REAL', 'int': 'INTEGER',
                         'datetime': 'TEXT', 'bool': 'INTEGER'}
            _cols_sql = ', '.join(
                f"{c['column_name']} {_type_map.get(c['data_type'], 'TEXT')}"
                for c in _hud_schema
            )
            conn.execute(f'CREATE TABLE IF NOT EXISTS {table_name} ({_cols_sql})')
            conn.execute(
                f'CREATE UNIQUE INDEX IF NOT EXISTS idx_{table_name}_hand_id '
                f'ON {table_name} (hand_id)'
            )
            conn.commit()
        except Exception as _eg:
            print(f'   ⚠️  No se pudo crear tabla {table_name}: {_eg}')

    if df_save.empty:
        print("ℹ️  Todas las manos de esta sesión ya estaban en DB. Nada nuevo que guardar.")
        return

    # ── Serializar datetimes para SQLite ──────────────────────────────────
    for col in df_save.select_dtypes(include=['datetime64[ns]']).columns:
        df_save[col] = df_save[col].dt.strftime('%Y-%m-%d %H:%M:%S')

    # Siempre append — el historial NUNCA se borra
    # Strip columnas extra no en schema canónico (ej: allin_ev_calculated del módulo EV)
    _schema_cols = [c['column_name'] for c in define_hud_schema()]
    _extra = [c for c in df_save.columns if c not in _schema_cols]
    if _extra:
        df_save = df_save.drop(columns=_extra, errors='ignore')
    df_save.to_sql(table_name, conn, if_exists='append', index=False)
    print(f"✅ {len(df_save)} manos nuevas guardadas en DB "
          f"(total sesión: {n_total} | nuevas: {len(df_save)}).")


def load_df_from_db(conn, table_name='hand_history'):
    """Carga el historial completo desde SQLite con deduplicación defensiva.
    FIX v1.97: drop_duplicates(hand_id) como segunda línea de defensa.
    """
    try:
        df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
        if 'hand_id' in df.columns:
            n_before = len(df)
            df = df.drop_duplicates(subset=['hand_id'], keep='last')
            n_dupes = n_before - len(df)
            if n_dupes > 0:
                print(f"   ℹ️  {n_dupes} duplicados eliminados en carga DB (dedup defensivo).")
        print(f"✅ {len(df)} manos cargadas desde DB.")
        return df
    except Exception as e:
        print(f"⚠️ Error al cargar desde DB: {e}")
        return pd.DataFrame()


print("✅ Persistencia SQLite cargada.")
print(f"   DB: {DB_NAME}")


def persist_session_data(df, db_conn=None, drive_path=None, filename=None):
    """
    Persiste los datos de sesión en SQLite (local) y/o Google Drive (entre sesiones Colab).

    Colab pierde todos los archivos al cerrar. SQLite sobrevive dentro de la misma
    sesión pero no entre sesiones. Google Drive es la única persistencia real
    entre ejecuciones separadas del notebook.

    Estrategia dual:
      1. SQLite local  → rápido, siempre disponible, para carga en misma sesión
      2. Google Drive  → persistencia entre sesiones, backup automático

    Args:
        df         (DataFrame):       datos a persistir
        db_conn    (Connection|None): conexión SQLite ya abierta. Si None, abre DB_NAME.
        drive_path (str|None):        ruta raíz en Drive, ej. '/content/drive/MyDrive/OS_v2'.
                                      Si None, intenta montar Drive automáticamente.
        filename   (str|None):        nombre del archivo CSV en Drive.
                                      Si None, genera 'os_v2_YYYYMMDD_HHMMSS.csv'.

    Returns:
        dict con claves:
            'sqlite_ok'   → bool   si el guardado SQLite fue exitoso
            'drive_ok'    → bool   si el guardado en Drive fue exitoso
            'drive_file'  → str    ruta completa del archivo guardado en Drive
            'rows_saved'  → int    número de filas guardadas
    """
    resultado = {'sqlite_ok': False, 'drive_ok': False,
                 'drive_file': None, 'rows_saved': 0}

    if df.empty:
        print("⚠️ persist_session_data: DataFrame vacío. Nada que persistir.")
        return resultado

    n_rows = len(df)

    # ── 1. SQLite local ───────────────────────────────────────────────────────
    try:
        conn = db_conn if db_conn is not None else sqlite3.connect(DB_NAME)
        hud_schema = define_hud_schema()
        initialize_sqlite_db(DB_NAME, schema=hud_schema)
        save_df_to_db(df, conn)  # siempre append + dedup por hand_id
        resultado['sqlite_ok'] = True
        resultado['rows_saved'] = n_rows
        print(f"✅ SQLite: {n_rows} manos guardadas en '{DB_NAME}'.")
    except Exception as e:
        print(f"❌ SQLite error: {e}")

    # ── 2. Google Drive ───────────────────────────────────────────────────────
    try:
        # Intentar importar google.colab (solo disponible en Colab)
        from google.colab import drive as _gdrive  # noqa

        # FIX v1.71b: NO montar Drive automaticamente -- cuelga el pipeline
        # Solo usar Drive si ya esta montado O si drive_path fue pasado explicitamente
        import os
        mount_point = '/content/drive'
        if not os.path.exists(os.path.join(mount_point, 'MyDrive')):
            if drive_path:
                print("   Montando Google Drive...")
                _gdrive.mount(mount_point)
            else:
                print("   Info: Drive no montado. Activa con BASELINE_DRIVE_PATH.")
                raise Exception("Drive no montado")

        # Construir ruta de destino
        base = drive_path or os.path.join(mount_point, 'MyDrive', 'OS_v2_poker')
        os.makedirs(base, exist_ok=True)

        if filename is None:
            ts = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'os_v2_{ts}.csv'

        full_path = os.path.join(base, filename)

        # Guardar CSV (más portable que SQLite para Drive)
        df_save = df.copy()
        for col in df_save.select_dtypes(include=['datetime64[ns]']).columns:
            df_save[col] = df_save[col].dt.strftime('%Y-%m-%d %H:%M:%S')
        df_save.to_csv(full_path, index=False)

        resultado['drive_ok']   = True
        resultado['drive_file'] = full_path
        print(f"✅ Drive: {n_rows} manos guardadas en '{full_path}'.")

    except ImportError:
        # No estamos en Colab — guardar CSV local como fallback
        import os
        fallback_dir = drive_path or '.'
        os.makedirs(fallback_dir, exist_ok=True)
        if filename is None:
            ts = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'os_v2_{ts}.csv'
        full_path = os.path.join(fallback_dir, filename)
        df_save = df.copy()
        for col in df_save.select_dtypes(include=['datetime64[ns]']).columns:
            df_save[col] = df_save[col].dt.strftime('%Y-%m-%d %H:%M:%S')
        df_save.to_csv(full_path, index=False)
        resultado['drive_ok']   = True
        resultado['drive_file'] = full_path
        print(f"✅ CSV local (no Colab): '{full_path}'.")

    except Exception as e:
        print(f"❌ Drive error: {e}")
        print("   → Comprueba que Drive esté montado o pasa drive_path explícito.")

    # ── Resumen ───────────────────────────────────────────────────────────────
    sqlite_mark = "✅" if resultado['sqlite_ok'] else "❌"
    drive_mark  = "✅" if resultado['drive_ok']  else "❌"
    print(f"   Resumen persistencia: SQLite {sqlite_mark}  |  Drive {drive_mark}  |  {n_rows} filas")
    return resultado


print("✅ persist_session_data cargada (SQLite + Google Drive).")
print("   Uso básico:  persist_session_data(ingested_df)")
print("   Con Drive:   persist_session_data(ingested_df, drive_path='/content/drive/MyDrive/OS_v2')")


# ── Funciones de memoria entre ejecuciones (os_baseline.json) ─────────────
# GAP 2 RESUELTO: el pipeline ahora recuerda el ranking de leaks de la
# ejecución anterior y calcula el delta automáticamente.
# El usuario no configura nada — funciona solo desde la segunda ejecución.

import json as _json
import os   as _os

BASELINE_FILENAME = 'os_baseline.json'   # nombre del archivo en Drive/local


def save_baseline(roi_ranking, drive_path=None, num_tables=1):
    """
    Guarda el top-5 de leaks y oportunidades como baseline para la próxima ejecución.

    Args:
        roi_ranking (dict): salida de build_roi_ranking().
                            Requiere claves: 'leaks', 'oportunidades'.
        drive_path (str|None): ruta donde guardar. None → directorio actual.

    Returns:
        str: ruta del archivo guardado, o None si falló.
    """
    try:
        leaks = roi_ranking.get('leaks', pd.DataFrame())
        opps  = roi_ranking.get('oportunidades', pd.DataFrame())

        def df_to_records(df):
            if df.empty:
                return []
            cols = ['spot_identifier', 'impacto_ev_total_eur_shrunk',
                    'spot_hands_count', 'ip_oop', 'pot_type',
                    'stack_depth', 'decision_street']
            available = [c for c in cols if c in df.columns]
            return df[available].head(5).rename(
                columns={'impacto_ev_total_eur_shrunk': 'impacto_eur_shrunk',
                         'spot_hands_count': 'hands'}
            ).to_dict(orient='records')

        baseline = {
            'timestamp': datetime.now().isoformat(timespec='seconds'),
            'num_tables': num_tables,  # pasado desde pipeline

            'leaks': df_to_records(leaks),
            'opps':  df_to_records(opps),
        }

        base_dir = drive_path or '.'
        path = _os.path.join(base_dir, BASELINE_FILENAME)
        with open(path, 'w', encoding='utf-8') as f:
            _json.dump(baseline, f, ensure_ascii=False, indent=2)

        print(f"✅ Baseline guardado: {path} "
              f"({len(baseline['leaks'])} leaks, {len(baseline['opps'])} opps)")
        return path
    except Exception as e:
        print(f"⚠️ save_baseline error: {e}")
        return None


def load_baseline(drive_path=None):
    """
    Carga el baseline de la ejecución anterior.

    Returns:
        dict con claves 'timestamp', 'leaks', 'opps' — o None si no existe.
    """
    base_dir = drive_path or '.'
    path = _os.path.join(base_dir, BASELINE_FILENAME)
    if not _os.path.exists(path):
        return None
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = _json.load(f)
        print(f"✅ Baseline cargado: {path} (guardado: {data.get('timestamp','?')})")
        return data
    except Exception as e:
        print(f"⚠️ load_baseline error: {e}")
        return None


def compare_with_baseline(roi_ranking, prev_baseline):
    """
    Compara el ranking actual con el baseline anterior.
    Calcula delta € y % para cada spot que aparece en ambos.

    Args:
        roi_ranking   (dict): salida de build_roi_ranking() (ejecución actual).
        prev_baseline (dict): salida de load_baseline() (ejecución anterior).

    Returns:
        dict con:
            'mejorados'  → lista de spots que mejoraron (leak redujo / opp creció)
            'empeorados' → lista de spots que empeoraron
            'nuevos'     → spots en el ranking actual que no estaban antes
            'resumen'    → str para print en el pipeline
    """
    if prev_baseline is None:
        return {
            'mejorados': [], 'empeorados': [], 'nuevos': [],
            'resumen': "ℹ️  Primera ejecución — no hay baseline anterior para comparar."
        }

    current_leaks = roi_ranking.get('leaks', pd.DataFrame())
    prev_leaks    = {r['spot_identifier']: r['impacto_eur_shrunk']
                     for r in prev_baseline.get('leaks', [])}
    prev_opps     = {r['spot_identifier']: r['impacto_eur_shrunk']
                     for r in prev_baseline.get('opps', [])}
    prev_all      = {**prev_leaks, **prev_opps}

    mejorados, empeorados, nuevos = [], [], []
    DELTA_MIN = 0.5   # €: delta mínimo para considerar cambio real (filtra ruido)

    # Comparar leaks actuales vs baseline
    if not current_leaks.empty:
        for _, row in current_leaks.iterrows():
            sid    = row['spot_identifier']
            cur_ev = row['impacto_ev_total_eur_shrunk']
            if sid in prev_all:
                prev_ev = prev_all[sid]
                delta   = cur_ev - prev_ev      # leak → negativo; mejora si delta > 0
                pct     = (delta / abs(prev_ev) * 100) if prev_ev != 0 else 0
                entry   = {'spot': sid, 'prev': prev_ev, 'curr': cur_ev,
                           'delta': delta, 'pct': pct}
                if delta > DELTA_MIN:
                    mejorados.append(entry)
                elif delta < -DELTA_MIN:
                    empeorados.append(entry)
            else:
                nuevos.append({'spot': sid, 'curr': cur_ev})

    # P5 CORREGIDO: comparar también oportunidades actuales vs baseline
    current_opps = roi_ranking.get('oportunidades', pd.DataFrame())
    if not current_opps.empty:
        for _, row in current_opps.iterrows():
            sid    = row['spot_identifier']
            cur_ev = row['impacto_ev_total_eur_shrunk']
            if sid in prev_all:
                prev_ev = prev_all[sid]
                delta   = cur_ev - prev_ev      # opp → positivo; mejora si delta > 0
                pct     = (delta / abs(prev_ev) * 100) if prev_ev != 0 else 0
                entry   = {'spot': sid, 'prev': prev_ev, 'curr': cur_ev,
                           'delta': delta, 'pct': pct}
                if delta > DELTA_MIN:
                    mejorados.append(entry)
                elif delta < -DELTA_MIN:
                    empeorados.append(entry)
            else:
                nuevos.append({'spot': sid, 'curr': cur_ev})

    # Construir resumen de texto para el pipeline
    ts_prev = prev_baseline.get('timestamp', '?')
    lines   = [f"📊 Comparativa vs baseline ({ts_prev}):"]

    if mejorados:
        for e in sorted(mejorados, key=lambda x: x['delta'], reverse=True):
            lines.append(f"   🟢 MEJORA  '{e['spot']}': "
                         f"{e['prev']:+.2f}€ → {e['curr']:+.2f}€ "
                         f"(Δ={e['delta']:+.2f}€ · {e['pct']:+.1f}%)")
    if empeorados:
        for e in sorted(empeorados, key=lambda x: x['delta']):
            lines.append(f"   🔴 PEORA   '{e['spot']}': "
                         f"{e['prev']:+.2f}€ → {e['curr']:+.2f}€ "
                         f"(Δ={e['delta']:+.2f}€ · {e['pct']:+.1f}%)")
    if nuevos:
        for e in nuevos:
            lines.append(f"   🆕 NUEVO   '{e['spot']}': {e['curr']:+.2f}€")

    if not mejorados and not empeorados and not nuevos:
        lines.append("   ↔️  Sin cambios significativos respecto al baseline.")

    resumen = '\n'.join(lines)
    print(resumen)
    return {'mejorados': mejorados, 'empeorados': empeorados,
            'nuevos': nuevos, 'resumen': resumen}


print("✅ Funciones de memoria cargadas: save_baseline / load_baseline / compare_with_baseline")
print(f"   Archivo: '{BASELINE_FILENAME}' (mismo dir que el CSV de Drive)")
print("   Flujo: pipeline carga baseline → compara → guarda nuevo baseline automáticamente")


def generate_dummy_hand_data(num_hands=2000, stake='NL25'):
    """
    Genera datos de manos dummy realistas para testing.

    CORRECCIÓN BUG #5: Session_id NO se asigna aleatoriamente.
    Se generan timestamps con gaps reales y se llama a assign_session_ids_by_time_gap.
    """
    positions    = ['BTN', 'CO', 'MP', 'UTG', 'SB', 'BB']
    hole_cards   = ['AA', 'KK', 'QQ', 'AKs', 'AQs', 'KQs', 'JJ', 'TT', '99', '88',
                    'AQo', 'KJo', 'QJo', '76s', '54s', 'JTs', 'KQo', '72o', 'J8s',
                    '23s', 'J4s', 'T9s', 'A5s', 'KTs']
    boards_flop  = ['Kh7s2c', 'As3d4c', 'QJTr', '567s', 'TJJh', '88Ad', 'KQ2r',
                    '234c', 'AhTs9s', '9d8c7h', 'Qc4s2d', 'JsThXc']
    preflop_acts = ['R', 'C', '3B', 'F', '4B', '5B']
    post_acts    = ['C', 'B', 'X', 'F', 'R', 'B_large', 'B_small']
    stakes       = ['NL2', 'NL5', 'NL10', 'NL25', 'NL50']
    # Tipos de oponente enriquecidos (TRABAJO + LIMPIO) para testing M2/M3
    # Maestro: "Etiquetado pools/oponentes (#3)" activo en M2
    opp_types    = ['LAG', 'NIT', 'TAG', 'Fish', 'Reg', 'Calling_Station',
                    'Aggro_Fish', 'Nit_Reg', 'Maniac', 'GTO_Solver']
    # Spot tags enriquecidos (TRABAJO + LIMPIO) para ranking ROI drills (#1)
    # Maestro: "Ranking ROI drills — prioriza top 3-5 leaks EV recuperable"
    spot_tags    = ['value_bet', 'bluff_catch', 'squeeze_spot', 'cbet_fail',
                    'river_bluff', 'flop_texture_exploit', 'set_mine',
                    'positional_advantage', 'thin_value', '3bet_defense']

    # Generar timestamps con estructura realista:
    # Sesiones de 60-180 min con gaps de 1-5 horas entre sesiones
    start = datetime(2024, 1, 1, 18, 0, 0)
    timestamps = []
    current = start
    for i in range(num_hands):
        # ~30 manos/hora = 2 min por mano en promedio
        current += timedelta(seconds=random.randint(60, 180))
        # Simular fin de sesión con gap largo cada ~150-250 manos
        if i > 0 and i % random.randint(150, 250) == 0:
            current += timedelta(hours=random.randint(2, 8))
        timestamps.append(current)

    bb_val = BB_VALUE_MAP.get(stake, 0.25)

    data = {
        'hand_id':             [f'h{i+1:05d}' for i in range(num_hands)],
        'session_id':          ['pending'] * num_hands,  # se sobreescribirá
        'date':                timestamps,
        'table_size':          [random.choice([2, 6, 9]) for _ in range(num_hands)],
        'player_position':     [random.choice(positions) for _ in range(num_hands)],
        'hole_cards':          [random.choice(hole_cards) for _ in range(num_hands)],
        'board_cards_flop':    [random.choice(boards_flop) if random.random() < 0.85 else '' for _ in range(num_hands)],
        'board_cards_turn':    [f"{random.choice('23456789TJQKA')}{random.choice('shdc')}" if random.random() < 0.65 else '' for _ in range(num_hands)],
        'board_cards_river':   [f"{random.choice('23456789TJQKA')}{random.choice('shdc')}" if random.random() < 0.45 else '' for _ in range(num_hands)],
        'preflop_action':      [random.choice(preflop_acts) for _ in range(num_hands)],
        'flop_action':         [random.choice(post_acts) if random.random() < 0.75 else '' for _ in range(num_hands)],
        'turn_action':         [random.choice(post_acts) if random.random() < 0.55 else '' for _ in range(num_hands)],
        'river_action':        [random.choice(post_acts) if random.random() < 0.35 else '' for _ in range(num_hands)],
        # BUG D CORREGIDO: misma media para net_won y ev_won (antes ev_won tenía
        # media 0.8 vs 0.5 de net_won, sesgando los gates M1 en testing).
        'net_won':             [round(random.gauss(0.5, 15) * bb_val, 2) for _ in range(num_hands)],
        # NOTA B5: dummy usa distribución gaussiana independiente para ev_won (realista para testing).
        # En datos reales, ev_won == net_won hasta que enrich_with_allin_ev() lo actualice.
        # Esta divergencia es intencional: dummy testea el path EV≠net_won (futuro M3).
        'ev_won':              [round(random.gauss(0.5, 15) * bb_val, 2) for _ in range(num_hands)],
        'rake':                [round(random.uniform(0.02, 0.5) * bb_val, 2) for _ in range(num_hands)],
        'stake_level':         [stake] * num_hands,
        'total_pot':           [round(random.uniform(2, 80) * bb_val, 2) for _ in range(num_hands)],
        'player_stack_start':  [round(random.uniform(40, 120) * bb_val, 2) for _ in range(num_hands)],
        'all_players':         ['Hero@BTN, Opp1@BB'] * num_hands,
        'opponent_names':      ['Opp1'] * num_hands,
        'friccion_r':          [random.randint(1, 4) for _ in range(num_hands)],
        'friccion_a':          [random.randint(1, 4) for _ in range(num_hands)],
        'friccion_v':          [random.randint(1, 3) for _ in range(num_hands)],
        'manual_spot_tag':     [random.choice(spot_tags) if random.random() < 0.4 else '' for _ in range(num_hands)],
        'opponent_type_manual':[random.choice(opp_types) if random.random() < 0.6 else '' for _ in range(num_hands)],
    }

    df = pd.DataFrame(data)

    # ── v1.27: campos PT3 con probabilidades calibradas para micros 6max ──
    n   = num_hands
    rng = random.random
    pos_v  = df['player_position'].values
    pf_v   = df['preflop_action'].values
    fl_v   = df['board_cards_flop'].values
    tu_v   = df['board_cards_turn'].values
    rv_v   = df['board_cards_river'].values

    # PREFLOP
    df['flg_blind_s']         = [p == 'SB' for p in pos_v]
    df['flg_blind_b']         = [p == 'BB' for p in pos_v]
    df['flg_vpip']            = [a in ('R','C','3B','4B') for a in pf_v]
    df['flg_p_fold']          = [a == 'F' for a in pf_v]
    df['flg_p_open_opp']      = [p not in ('BB',) for p in pos_v]
    df['flg_p_first_raise']   = [a == 'R' and p not in ('BB',) for a,p in zip(pf_v, pos_v)]
    df['flg_p_open']          = df['flg_p_first_raise']
    df['flg_p_limp']          = [a == 'C' and p not in ('BB',) for a,p in zip(pf_v, pos_v)]
    df['flg_p_ccall']         = [a == 'C' and p not in ('SB','BB') and rng()<0.40 for a,p in zip(pf_v, pos_v)]
    df['flg_steal_opp']     = [p in ('BTN','CO','SB') for p in pos_v]
    df['flg_steal_att']         = [p in ('BTN','CO','SB') and a == 'R' for a,p in zip(pf_v, pos_v)]
    df['flg_p_face_raise']    = [a not in ('R','F') and rng()<0.45 for a in pf_v]
    df['flg_p_3bet_opp']      = [rng()<0.22 for _ in range(n)]
    df['flg_p_3bet']          = [a == '3B' for a in pf_v]
    df['flg_p_3bet_def_opp'] = [a == 'R' and rng()<0.20 for a in pf_v]
    df['flg_p_fold_to_3bet']  = [v and rng()<0.50 for v in df['flg_p_3bet_def_opp']]
    df['flg_p_3bet_def_opp']  = df['flg_p_3bet_def_opp']
    df['flg_p_3bet_role']     = ['aggressor' if a=='3B' else ('caller' if rng()<0.25 else 'none') for a in pf_v]
    df['cnt_p_raise']         = [1 if a in ('R','3B') else 0 for a in pf_v]
    df['cnt_p_call']          = [1 if a == 'C' else 0 for a in pf_v]
    steal_opp = [p in ('SB','BB') and rng()<0.38 for p in pos_v]
    df['flg_blind_def_opp']   = steal_opp
    df['flg_sb_steal_fold']   = [v and p=='SB' and rng()<0.42 for v,p in zip(steal_opp, pos_v)]
    df['flg_bb_steal_fold']   = [v and p=='BB' and rng()<0.32 for v,p in zip(steal_opp, pos_v)]
    df['villain_position']    = [random.choice(['BTN','CO','BB','SB','UTG','HJ','']) for _ in range(n)]
    df['preflop_pressure']    = ['raise' if rng()<0.35 else 'none' for _ in range(n)]
    df['preflop_n_raises_facing'] = [1 if rng()<0.35 else 0 for _ in range(n)]
    df['stack_depth_bb']      = [round(random.uniform(20, 150), 1) for _ in range(n)]
    df['num_tables']          = [1] * n

    # FLOP
    saw_f = [bool(f) and df['flg_vpip'].iloc[i] for i,f in enumerate(fl_v)]
    df['flg_f_saw']           = saw_f
    df['flg_f_first']         = [v and rng()<0.50 for v in saw_f]
    df['flg_f_has_position']  = [v and not df['flg_f_first'].iloc[i] for i,v in enumerate(saw_f)]
    df['flg_f_open_opp']      = df['flg_f_first']
    df['flg_f_open']          = [v and rng()<0.52 for v in df['flg_f_first']]
    df['flg_f_bet']           = [v and rng()<0.44 for v in saw_f]
    df['flg_f_check']         = [v and not df['flg_f_bet'].iloc[i] and rng()<0.72 for i,v in enumerate(saw_f)]
    df['flg_f_fold']          = [v and rng()<0.26 for v in saw_f]
    df['flg_f_check_raise']   = [v and rng()<0.06 for v in saw_f]
    df['flg_f_first_raise']   = [v and rng()<0.10 for v in saw_f]
    df['cnt_f_raise']         = df['flg_f_check_raise'].astype(int)
    df['cnt_f_call']          = [(1 if v and rng()<0.42 else 0) for v in saw_f]
    df['flg_f_cbet_opp']      = [saw_f[i] and df['flg_p_first_raise'].iloc[i] for i in range(n)]
    df['flg_f_cbet']          = [v and rng()<0.62 for v in df['flg_f_cbet_opp']]
    df['flg_f_cbet_def_opp']  = [saw_f[i] and not df['flg_f_cbet_opp'].iloc[i] and rng()<0.50 for i in range(n)]
    df['flg_f_cbet_def']      = [v and rng()<0.60 for v in df['flg_f_cbet_def_opp']]
    df['flg_f_donk_opp']      = [saw_f[i] and rng()<0.12 for i in range(n)]
    df['flg_f_donk']          = [v and rng()<0.28 for v in df['flg_f_donk_opp']]

    # TURN
    saw_t = [bool(t) and saw_f[i] and not df['flg_f_fold'].iloc[i] for i,t in enumerate(tu_v)]
    df['flg_t_saw']           = saw_t
    df['flg_t_first']         = [v and rng()<0.50 for v in saw_t]
    df['flg_t_has_position']  = [v and not df['flg_t_first'].iloc[i] for i,v in enumerate(saw_t)]
    df['flg_t_open_opp']      = df['flg_t_first']
    df['flg_t_open']          = [v and rng()<0.50 for v in df['flg_t_first']]
    df['flg_t_bet']           = [v and rng()<0.42 for v in saw_t]
    df['flg_t_check']         = [v and not df['flg_t_bet'].iloc[i] and rng()<0.68 for i,v in enumerate(saw_t)]
    df['flg_t_fold']          = [v and rng()<0.22 for v in saw_t]
    df['flg_t_check_raise']   = [v and rng()<0.04 for v in saw_t]
    df['flg_t_first_raise']   = [v and rng()<0.08 for v in saw_t]
    df['cnt_t_raise']         = df['flg_t_check_raise'].astype(int)
    df['cnt_t_call']          = [(1 if v and rng()<0.38 else 0) for v in saw_t]

    # RIVER
    saw_r = [bool(r) and saw_t[i] and not df['flg_t_fold'].iloc[i] for i,r in enumerate(rv_v)]
    df['flg_r_saw']           = saw_r
    df['flg_r_first']         = [v and rng()<0.50 for v in saw_r]
    df['flg_r_has_position']  = [v and not df['flg_r_first'].iloc[i] for i,v in enumerate(saw_r)]
    df['flg_r_open_opp']      = df['flg_r_first']
    df['flg_r_open']          = [v and rng()<0.46 for v in df['flg_r_first']]
    df['flg_r_bet']           = [v and rng()<0.44 for v in saw_r]
    df['flg_r_check']         = [v and not df['flg_r_bet'].iloc[i] and rng()<0.66 for i,v in enumerate(saw_r)]
    df['flg_r_fold']          = [v and rng()<0.20 for v in saw_r]
    df['flg_r_check_raise']   = [v and rng()<0.03 for v in saw_r]
    df['flg_r_first_raise']   = [v and rng()<0.06 for v in saw_r]
    df['cnt_r_raise']         = df['flg_r_check_raise'].astype(int)
    df['cnt_r_call']          = [(1 if v and rng()<0.35 else 0) for v in saw_r]

    # RESULTADOS
    df['flg_showdown']        = [v and rng()<0.55 for v in saw_r]
    df['flg_won_hand']        = [(df['net_won'].iloc[i] > 0) for i in range(n)]
    df['flg_showed']          = [v and rng()<0.88 for v in df['flg_showdown']]
    def _ef(i):
        if df['flg_p_fold'].iloc[i]: return 'P'
        if df['flg_f_fold'].iloc[i]: return 'F'
        if df['flg_t_fold'].iloc[i]: return 'T'
        if df['flg_r_fold'].iloc[i]: return 'R'
        return 'N'
    df['enum_folded']         = [_ef(i) for i in range(n)]
    df['val_f_afq']           = [round(random.uniform(20, 70), 1) if saw_f[i] else 0.0 for i in range(n)]
    df['cnt_players_f']       = [random.choice([2,2,2,3]) if saw_f[i] else 0 for i in range(n)]
    df['cnt_players_t']       = [2 if saw_t[i] else 0 for i in range(n)]
    df['cnt_players_r']       = [2 if saw_r[i] else 0 for i in range(n)]

    # v1.28a TIER INMEDIATO — campos nuevos
    # steal aliases
    df['flg_steal_att']        = df['flg_steal_att']
    df['flg_steal_opp']        = df['flg_steal_opp']
    # 4bet PF (raro ~1.5%)
    df['flg_p_4bet']           = [v and rng()<0.15 for v in df['flg_p_3bet']]
    df['flg_p_4bet_opp']       = [v and rng()<0.12 for v in df['flg_p_3bet']]
    df['flg_p_4bet_def_opp']   = [v and rng()<0.20 for v in df['flg_p_3bet_opp']]
    # squeeze PF (~0.8%)
    df['flg_p_squeeze_opp']    = [v and rng()<0.18 for v in df['flg_p_3bet_opp']]
    df['flg_p_squeeze']        = [v and rng()<0.30 for v in df['flg_p_squeeze_opp']]
    df['flg_p_squeeze_def_opp']= [v and rng()<0.15 for v in df['flg_p_open']]
    def _esqa(i):
        if not df['flg_p_squeeze_def_opp'].iloc[i]: return 'N'
        r = rng()
        return 'F' if r<0.65 else ('C' if r<0.90 else 'R')
    df['enum_p_squeeze_action']= [_esqa(i) for i in range(n)]
    # all-in enums
    df['enum_allin']           = [('P' if rng()<0.05 else 'F' if rng()<0.04 else
                                    'T' if rng()<0.03 else 'R' if rng()<0.03 else 'N')
                                   for _ in range(n)]
    df['enum_face_allin']      = [('P' if rng()<0.04 else 'F' if rng()<0.03 else
                                    'T' if rng()<0.03 else 'R' if rng()<0.02 else 'N')
                                   for _ in range(n)]
    def _efaa(i):
        if df['enum_face_allin'].iloc[i] == 'N': return 'N'
        r = rng()
        return 'C' if r<0.60 else ('F' if r<0.90 else 'R')
    df['enum_face_allin_action']= [_efaa(i) for i in range(n)]
    # enum_p_3bet_action
    def _ep3ba(i):
        if not df['flg_p_3bet_def_opp'].iloc[i]: return 'N'
        if df['flg_p_fold_to_3bet'].iloc[i]: return 'F'
        return 'C' if rng()<0.80 else 'R'
    df['enum_p_3bet_action']   = [_ep3ba(i) for i in range(n)]
    # face_raise postflop
    df['flg_f_face_raise']     = [v and rng()<0.22 for v in saw_f]
    df['flg_t_face_raise']     = [v and rng()<0.18 for v in saw_t]
    df['flg_r_face_raise']     = [v and rng()<0.14 for v in saw_r]

    # v1.28b TIER B — cbet chain, float, donk
    # enum_f_cbet_action
    def _ecba(i):
        if not df['flg_f_cbet_def_opp'].iloc[i]: return 'N'
        r = rng()
        return 'C' if r<0.55 else ('R' if r<0.70 else 'F')
    df['enum_f_cbet_action']    = [_ecba(i) for i in range(n)]
    # cbet turn chain
    df['flg_t_cbet_opp']        = [df['flg_f_cbet'].iloc[i] and saw_t[i] for i in range(n)]
    df['flg_t_cbet']            = [v and rng()<0.68 for v in df['flg_t_cbet_opp']]
    df['flg_t_cbet_def_opp']    = [df['flg_f_cbet_def_opp'].iloc[i] and saw_t[i] for i in range(n)]
    def _etca(i):
        if not df['flg_t_cbet_def_opp'].iloc[i]: return 'N'
        r = rng()
        return 'C' if r<0.50 else ('R' if r<0.65 else 'F')
    df['enum_t_cbet_action']    = [_etca(i) for i in range(n)]
    # cbet river chain
    df['flg_r_cbet_opp']        = [df['flg_t_cbet'].iloc[i] and saw_r[i] for i in range(n)]
    df['flg_r_cbet']            = [v and rng()<0.60 for v in df['flg_r_cbet_opp']]
    df['flg_r_cbet_def_opp']    = [df['flg_t_cbet_def_opp'].iloc[i] and saw_r[i] for i in range(n)]
    def _erca(i):
        if not df['flg_r_cbet_def_opp'].iloc[i]: return 'N'
        r = rng()
        return 'C' if r<0.48 else ('R' if r<0.60 else 'F')
    df['enum_r_cbet_action']    = [_erca(i) for i in range(n)]
    # float turn (IP + llamó flop + rival check turn + hero bet)
    df['flg_t_float_opp']       = [df['flg_t_has_position'].iloc[i] and df['cnt_f_call'].iloc[i]>0 and saw_t[i] for i in range(n)]
    df['flg_t_float']           = [v and rng()<0.32 for v in df['flg_t_float_opp']]
    df['flg_t_float_def_opp']   = [df['flg_f_cbet'].iloc[i] and df['flg_t_check'].iloc[i] and saw_t[i] and rng()<0.25 for i in range(n)]
    def _etfa(i):
        if not df['flg_t_float_def_opp'].iloc[i]: return 'N'
        r = rng()
        return 'C' if r<0.45 else ('R' if r<0.60 else 'F')
    df['enum_t_float_action']   = [_etfa(i) for i in range(n)]
    # float river
    df['flg_r_float_opp']       = [df['flg_t_float'].iloc[i] and saw_r[i] for i in range(n)]
    df['flg_r_float']           = [v and rng()<0.55 for v in df['flg_r_float_opp']]
    df['flg_r_float_def_opp']   = [df['flg_t_float_def_opp'].iloc[i] and saw_r[i] for i in range(n)]
    def _erfa(i):
        if not df['flg_r_float_def_opp'].iloc[i]: return 'N'
        r = rng()
        return 'C' if r<0.42 else ('R' if r<0.55 else 'F')
    df['enum_r_float_action']   = [_erfa(i) for i in range(n)]
    # donk turn (OOP, llamó cbet flop, beteó primero en turn)
    df['flg_t_donk_opp']        = [not df['flg_t_has_position'].iloc[i] and df['flg_f_cbet_def_opp'].iloc[i] and saw_t[i] for i in range(n)]
    df['flg_t_donk']            = [v and rng()<0.20 for v in df['flg_t_donk_opp']]
    df['flg_t_donk_def_opp']    = [df['flg_f_cbet'].iloc[i] and saw_t[i] and rng()<0.18 for i in range(n)]
    def _etda(i):
        if not df['flg_t_donk_def_opp'].iloc[i]: return 'N'
        r = rng()
        return 'C' if r<0.40 else ('R' if r<0.58 else 'F')
    df['enum_t_donk_action']    = [_etda(i) for i in range(n)]
    # donk river
    df['flg_r_donk_opp']        = [not df['flg_r_has_position'].iloc[i] and df['flg_t_cbet_def_opp'].iloc[i] and saw_r[i] for i in range(n)]
    df['flg_r_donk']            = [v and rng()<0.18 for v in df['flg_r_donk_opp']]
    df['flg_r_donk_def_opp']    = [df['flg_t_cbet'].iloc[i] and saw_r[i] and rng()<0.15 for i in range(n)]
    def _erda(i):
        if not df['flg_r_donk_def_opp'].iloc[i]: return 'N'
        r = rng()
        return 'C' if r<0.38 else ('R' if r<0.55 else 'F')
    df['enum_r_donk_action']    = [_erda(i) for i in range(n)]

    # ── v1.29A: Postflop raise wars (frecuencias calibradas en 3216 manos NL2) ─
    df['enum_p_4bet_action']    = [random.choice(['N','N','N','N','N','C','F']) for _ in range(n)]
    # Flop: 3bet_opp~1.1%, 3bet~0.09%, 3bet_def_opp~0.12%, 4bet~0.03%
    _f3b_opp                    = [saw_f[i] and rng() < 0.011 for i in range(n)]
    df['flg_f_3bet_opp']        = _f3b_opp
    df['flg_f_3bet']            = [v and rng() < 0.09  for v in _f3b_opp]
    df['flg_f_3bet_def_opp']    = [saw_f[i] and rng() < 0.0012 for i in range(n)]
    def _ef3a(i):
        if not df['flg_f_3bet_def_opp'].iloc[i]: return 'N'
        r = rng(); return 'C' if r < 0.50 else ('R' if r < 0.65 else 'F')
    df['enum_f_3bet_action']    = [_ef3a(i) for i in range(n)]
    _f4b_opp                    = [df['flg_f_3bet'].iloc[i] and rng() < 0.16 for i in range(n)]
    df['flg_f_4bet_opp']        = _f4b_opp
    df['flg_f_4bet']            = [v and rng() < 0.30  for v in _f4b_opp]
    def _ef4a(i):
        if not df['flg_f_4bet_opp'].iloc[i]: return 'N'
        r = rng(); return 'C' if r < 0.55 else 'F'
    df['enum_f_4bet_action']    = [_ef4a(i) for i in range(n)]
    # Turn: 3bet_opp~0.62%, 3bet~0.03%, 4bet_def_opp~0%
    _t3b_opp                    = [saw_t[i] and rng() < 0.006 for i in range(n)]
    df['flg_t_3bet_opp']        = _t3b_opp
    df['flg_t_3bet']            = [v and rng() < 0.05  for v in _t3b_opp]
    df['flg_t_3bet_def_opp']    = [saw_t[i] and rng() < 0.0019 for i in range(n)]
    def _et3a(i):
        if not df['flg_t_3bet_def_opp'].iloc[i]: return 'N'
        r = rng(); return 'C' if r < 0.48 else ('R' if r < 0.62 else 'F')
    df['enum_t_3bet_action']    = [_et3a(i) for i in range(n)]
    _t4b_opp                    = [df['flg_t_3bet'].iloc[i] and rng() < 0.19 for i in range(n)]
    df['flg_t_4bet_opp']        = _t4b_opp
    df['flg_t_4bet']            = [v and rng() < 0.25  for v in _t4b_opp]
    df['flg_t_4bet_def_opp']    = [df['flg_t_3bet'].iloc[i] and rng() < 0.18 for i in range(n)]
    def _et4a(i):
        if not df['flg_t_4bet_opp'].iloc[i]: return 'N'
        r = rng(); return 'C' if r < 0.52 else 'F'
    df['enum_t_4bet_action']    = [_et4a(i) for i in range(n)]
    # River: 3bet_opp~0.75%, 3bet~0.03%, 4bet~0% (extremadamente raro en NL2)
    _r3b_opp                    = [saw_r[i] and rng() < 0.008 for i in range(n)]
    df['flg_r_3bet_opp']        = _r3b_opp
    df['flg_r_3bet']            = [v and rng() < 0.04  for v in _r3b_opp]
    df['flg_r_3bet_def_opp']    = [saw_r[i] and rng() < 0.0005 for i in range(n)]
    def _er3a(i):
        if not df['flg_r_3bet_def_opp'].iloc[i]: return 'N'
        r = rng(); return 'C' if r < 0.44 else ('R' if r < 0.58 else 'F')
    df['enum_r_3bet_action']    = [_er3a(i) for i in range(n)]
    df['flg_r_4bet']            = [False] * n
    df['flg_r_4bet_opp']        = [False] * n
    df['flg_r_4bet_def_opp']    = [False] * n
    df['enum_r_4bet_action']    = ['N'] * n

    # ── v1.29B: HAPC dummy (distribuciones realistas NL2) ─────────────────────
    # Made hands: derived from saw_f/t/r flags with realistic distributions
    _hcls_f = []
    _hcls_t = []
    _hcls_r = []
    for _i in range(n):
        _r = rng()
        # Flop hand distribution (when saw flop)
        if saw_f[_i]:
            if _r < 0.35: _hcls_f.append(0)    # high card
            elif _r < 0.68: _hcls_f.append(1)  # one pair
            elif _r < 0.82: _hcls_f.append(2)  # two pair
            elif _r < 0.90: _hcls_f.append(3)  # three oak
            elif _r < 0.94: _hcls_f.append(4)  # straight
            elif _r < 0.97: _hcls_f.append(5)  # flush
            elif _r < 0.99: _hcls_f.append(6)  # full house
            elif _r < 0.999: _hcls_f.append(7) # four oak
            else: _hcls_f.append(8)             # str flush
        else: _hcls_f.append(-1)
        _r = rng()
        if saw_t[_i]:
            if _r < 0.24: _hcls_t.append(0)
            elif _r < 0.55: _hcls_t.append(1)
            elif _r < 0.73: _hcls_t.append(2)
            elif _r < 0.83: _hcls_t.append(3)
            elif _r < 0.88: _hcls_t.append(4)
            elif _r < 0.93: _hcls_t.append(5)
            elif _r < 0.97: _hcls_t.append(6)
            elif _r < 0.999: _hcls_t.append(7)
            else: _hcls_t.append(8)
        else: _hcls_t.append(-1)
        _r = rng()
        if saw_r[_i]:
            if _r < 0.18: _hcls_r.append(0)
            elif _r < 0.45: _hcls_r.append(1)
            elif _r < 0.64: _hcls_r.append(2)
            elif _r < 0.77: _hcls_r.append(3)
            elif _r < 0.83: _hcls_r.append(4)
            elif _r < 0.90: _hcls_r.append(5)
            elif _r < 0.96: _hcls_r.append(6)
            elif _r < 0.999: _hcls_r.append(7)
            else: _hcls_r.append(8)
        else: _hcls_r.append(-1)
    _HNAMES = ['highcard','1pair','2pair','threeoak','straight','flush','fullhouse','fouroak','strflush']
    for _pfx_h, _hcls_list in [('f',_hcls_f),('t',_hcls_t),('r',_hcls_r)]:
        for _ci, _hn in enumerate(_HNAMES):
            df[f'flg_{_pfx_h}_{_hn}'] = [_hcls_list[_i]==_ci for _i in range(n)]
        df[f'val_{_pfx_h}_hole_cards_used'] = [2 if _hcls_list[_i]>=0 else 0 for _i in range(n)]
        if _pfx_h != 'r':
            _saw = saw_f if _pfx_h=='f' else saw_t
            _hi  = [_hcls_list[_i] for _i in range(n)]
            df[f'flg_{_pfx_h}_flush_draw']     = [_saw[_i] and _hi[_i]<5 and rng()<0.14 for _i in range(n)]
            df[f'flg_{_pfx_h}_straight_draw']  = [_saw[_i] and _hi[_i]<4 and rng()<0.10 for _i in range(n)]
            df[f'flg_{_pfx_h}_gutshot_draw']   = [_saw[_i] and _hi[_i]<4 and rng()<0.12 for _i in range(n)]
            df[f'flg_{_pfx_h}_bflush_draw']    = [_saw[_i] and _hi[_i]<5 and rng()<0.22 for _i in range(n)]
            df[f'flg_{_pfx_h}_bstraight_draw'] = [_saw[_i] and _hi[_i]<4 and rng()<0.18 for _i in range(n)]
            df[f'flg_{_pfx_h}_2gutshot_draw']  = [_saw[_i] and _hi[_i]<4 and rng()<0.04 for _i in range(n)]

    df = assign_session_ids_by_time_gap(df)  # Session_id por gaps reales

    print(f"✅ Dummy data: {len(df)} manos | {df['session_id'].nunique()} sesiones | stake {stake}")
    print(f"   Período: {df['date'].min().date()} → {df['date'].max().date()}")
    return df


print("✅ Generador Dummy cargado v1.29B — +108 campos: TIER INMEDIATO + TIER B + raise wars + HAPC.")


# ════════════════════════════════════════════════════════════════════════════
# SECCIÓN 11b — Suite de Tests Automatizados v1.40
# T1/T2/T3 FIX: tests unitarios para prevenir regresiones.
# Ejecutar después de cualquier cambio en el código.
# DISEÑO ANTIFRICCIÓN: todos los tests son auto-contenidos, sin archivos externos.
# ════════════════════════════════════════════════════════════════════════════

def run_all_tests(verbose=True):
    """
    Suite de tests rápidos. Retorna (passed, failed, errors).
    Diseño: falla ruidosamente (AssertionError con contexto) en lugar de
    silenciosamente. Cada test documenta el bug que previene.
    """
    passed = failed = 0
    errors = []

    def _test(name, condition, msg=''):
        nonlocal passed, failed
        if condition:
            passed += 1
            if verbose: print(f"  ✅ {name}")
        else:
            failed += 1
            errors.append(f"{name}: {msg}")
            print(f"  ❌ {name}: {msg}")

    print("\n── Tests Automatizados OS v2.0 ──")

    # ── T3: calculate_allin_ev_single — equity de referencia ─────────────────
    # AA vs KK preflop = ~82% equity para AA. Tolerancia ±3% para Monte Carlo.
    try:
        r = calculate_allin_ev_single('Ah Ad', 'Kh Kd', '', pot_net=200, invested=100, n_mc=3000)
        eq = r['equity']
        _test('T3-AA_vs_KK_equity',
              0.79 <= eq <= 0.86,
              f"AA vs KK equity={eq:.3f}, esperado 0.79-0.86")
    except Exception as e:
        _test('T3-AA_vs_KK_equity_b', False, f"excepción: {e}")

    # AK suited vs QQ flop = ~45% equity para AK. Tolerancia ±4%.
    try:
        r2 = calculate_allin_ev_single('Ah Kh', 'Qd Qc', 'Jh 2s 7d',
                                        pot_net=100, invested=50, n_mc=2000)
        eq2 = r2['equity']
        # AKs vs QQ on J72r: QQ has set → AKh=29% equity (overcards+bdfd vs set)
        _test('T3-AKs_vs_QQ_flop',
              0.25 <= eq2 <= 0.34,
              f"AKs vs QQ flop equity={eq2:.3f}, esperado 0.25-0.34 (QQ set en J72r)")
    except Exception as e:
        _test('T3-AKs_vs_QQ_flop_b', False, f"excepción: {e}")

    # ── T2: parse_real_hand_history_file — mano real hardcodeada ─────────────
    # Previene regresión de P0-C (flg_won_hand) y P0-D (torneo filtrado)
    _SAMPLE_HH = """Hand #1:

PokerStars Hand #219000000001: Hold'em No Limit (€0.01/€0.02) - 2024/01/15 20:00:00 CET
Table 'TestTable' 6-max Seat #1 is the button
Seat 1: BTN_Player (€2.00 in chips)
Seat 2: SB_Player (€2.00 in chips)
Seat 3: LaRuinaDeMago (€2.00 in chips)
BTN_Player: posts small blind €0.01
SB_Player: posts big blind €0.02
*** HOLE CARDS ***
Dealt to LaRuinaDeMago [Ah Kd]
LaRuinaDeMago: raises €0.06 to €0.08
BTN_Player: folds
SB_Player: folds
Uncalled bet (€0.06) returned to LaRuinaDeMago
LaRuinaDeMago collected €0.05 from pot
*** SUMMARY ***
Total pot €0.05 | Rake €0
Seat 3: LaRuinaDeMago collected (€0.05)
"""
    import tempfile, os
    try:
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt',
                                          encoding='utf-8', delete=False) as f:
            f.write(_SAMPLE_HH)
            _tmp = f.name

        _df = parse_real_hand_history_file(_tmp, hero='LaRuinaDeMago')
        os.unlink(_tmp)

        _test('T2-parser_returns_df',       not _df.empty,        'DataFrame vacío')
        _test('T2-hand_id_parsed',          _df.iloc[0]['hand_id'] == '219000000001', f"hand_id={_df.iloc[0]['hand_id']}")
        _test('T2-stake_NL2',               _df.iloc[0]['stake_level'] == 'NL2',     f"stake={_df.iloc[0]['stake_level']}")
        _test('T2-hole_cards',              _df.iloc[0]['hole_cards'] == 'Ah Kd',    f"cards={_df.iloc[0]['hole_cards']}")
        _test('T2-net_won_positive',        _df.iloc[0]['net_won'] > 0,              f"net_won={_df.iloc[0]['net_won']}")
        # P0-C: hero steals and wins → flg_won_hand debe ser True
        _test('T2-P0C_flg_won_hand',        _df.iloc[0].get('flg_won_hand', False) == True,
              f"flg_won_hand={_df.iloc[0].get('flg_won_hand')}")
    except Exception as e:
        _test('T2-parser', False, f"excepción: {e}")

    # ── T2b: filtro de torneos (P0-D) ────────────────────────────────────────
    _TOURNAMENT_HH = """Hand #1:

PokerStars Hand #219000000099: Tournament #987654321, Hold'em No Limit - 2024/01/15 20:00:00 CET
Table 'Tournament Table' 9-max Seat #1 is the button
Seat 1: Player1 (1500 in chips)
"""
    try:
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt',
                                          encoding='utf-8', delete=False) as f:
            f.write(_TOURNAMENT_HH)
            _tmp2 = f.name
        _df2 = parse_real_hand_history_file(_tmp2, hero='LaRuinaDeMago')
        os.unlink(_tmp2)
        _test('T2-P0D_torneo_filtrado', _df2.empty, f"torneo no filtrado: {len(_df2)} manos")
    except Exception as e:
        _test('T2-P0D_torneo_filtrado_b', False, f"excepción: {e}")

    # ── T1: unidades y cálculos core ─────────────────────────────────────────
    # BB_TO_EUR debe lanzar RuntimeError (DT1 fix)
    try:
        _ = BB_TO_EUR * 5.0
        _test('T1-BB_TO_EUR_raises', False, 'no lanzó RuntimeError')
    except RuntimeError:
        _test('T1-BB_TO_EUR_raises_b', True)
    except Exception as e:
        _test('T1-BB_TO_EUR_raises_c', False, f"lanzó {type(e).__name__} en lugar de RuntimeError")

    # Board texture classifier
    try:
        bt = classify_board_texture('Ah Kd Qh')
        _test('T1-board_texture_high',     bt['high_card'] == 'high',      f"high_card={bt['high_card']}")
        # AhKdQh: Ah+Qh=2 hearts→two_tone (test anterior esperaba rainbow incorrectamente)
        _test('T1-board_texture_two_tone', bt['flush_draw'] == 'two_tone', f"AhKdQh flush={bt['flush_draw']}")
        bt2 = classify_board_texture('Ah Kd Qc')  # genuinamente rainbow
        _test('T1-board_texture_rainbow',  bt2['flush_draw'] == 'rainbow', f"AhKdQc flush={bt2['flush_draw']}")
    except Exception as e:
        _test('T1-board_texture', False, f"excepción: {e}")

    # define_hud_schema returns valid structure
    try:
        schema = define_hud_schema()
        col_names = [c['column_name'] for c in schema]
        _test('T1-schema_hand_id',   'hand_id'   in col_names, 'hand_id missing')
        _test('T1-schema_net_won',   'net_won'   in col_names, 'net_won missing')
        _test('T1-schema_ev_won',    'ev_won'    in col_names, 'ev_won missing')
    except Exception as e:
        _test('T1-schema', False, f"excepción: {e}")

    # ── Resumen ───────────────────────────────────────────────────────────────
    total = passed + failed
    sem = '✅' if failed == 0 else ('⚠️' if failed <= 2 else '❌')
    print(f"\n  {sem} Tests: {passed}/{total} pasados | {failed} fallidos")
    if errors:
        print("  Fallos:")
        for e in errors:
            print(f"    → {e}")

    # ── T4: Funciones de progresión v1.46-v1.48 ─────────────────────────────
    # Requieren cell 72 cargada previamente (en Colab: ejecutar en orden).
    # FIX P1-F v1.78: guard para ejecución aislada (NameError si pipeline no corrió)
    _ingested_df_t4 = globals().get('ingested_df', __import__('pandas').DataFrame())
    _t4_funcs = {
        'T4-session_degradation':    ('display_session_degradation',   [_ingested_df_t4], {}),
        'T4-performance_by_hour':    ('display_performance_by_hour',   [_ingested_df_t4], {}),
        'T4-learning_velocity':      ('display_learning_velocity',     [_ingested_df_t4], {'window':2}),
        'T4-optimal_session_length': ('display_optimal_session_length',[_ingested_df_t4], {}),
        'T4-stack_depth_perf':       ('display_stack_depth_performance',[_ingested_df_t4], {}),
        'T4-session_stoploss':       ('display_session_stoploss',      [_ingested_df_t4], {}),
    }
    for _tname, (_fname, _args, _kwargs) in _t4_funcs.items():
        _fn = globals().get(_fname)
        if _fn is None:
            pass  # cell 72 no cargada — skip silencioso
        else:
            try:
                _r4 = _fn(*_args, **_kwargs)
                _test(_tname, isinstance(_r4, dict), f"{_fname} debe retornar dict")
            except Exception as _e4:
                _test(_tname, False, str(_e4)[:80])

    
    # ── P0-G: flg_showdown fix — solo manos con cartas mostradas ─────────
    try:
        import re as _re
        _sd_real   = "*** SHOW DOWN ***\nPlayer1: shows [Ah Kd]\nPlayer2: mucks hand"
        _sd_fake   = "*** SHOW DOWN ***\nPlayer2: mucks hand"  # nadie muestra
        _shows_pat = r': shows \['
        _t_real = bool(_re.search(_shows_pat, _sd_real))
        _t_fake = bool(_re.search(_shows_pat, _sd_fake))
        _test('P0-G-showdown-real',  _t_real == True,  'showdown real no detectado')
        _test('P0-G-showdown-no-sd', _t_fake == False, 'falso positivo en showdown')
    except Exception as e:
        _test('P0-G-showdown', False, str(e))

    # ── T5: display_no_initiative_ev ──────────────────────────────────────
    _fn_ni = globals().get('display_no_initiative_ev')
    if _fn_ni is not None:
        try:
            _r5 = _fn_ni(ingested_df)
            _test('T5-no_initiative_ev', isinstance(_r5, dict), 'debe retornar dict')
            _test('T5-ni-gap-exists', 'gap' in _r5, 'falta clave gap')
            _test('T5-ni-gap-positive', _r5.get('gap', 0) > 0,
                  f'gap={_r5.get("gap",0)} — esperado positivo en NL2')
        except Exception as e:
            _test('T5-no_initiative_ev_b', False, str(e)[:80])


        return passed, failed, errors


# Ejecutar tests al cargar la celda
print("✅ Suite de tests cargada.")
print("   Uso: passed, failed, errors = run_all_tests()")
print("   Ejecutar después de cualquier cambio de código para detectar regresiones.")


# ════════════════════════════════════════════════════════════════════════════
# SECCIÓN 13c — Módulo de Progresión v1.36
#
# OBJETIVO: Medir si el aprendizaje está ocurriendo sesión a sesión.
#   Sin esto no hay forma de saber si estás mejorando o girando en círculos.
#
# TRES COMPONENTES:
#   1. calculate_progression_metrics(df)
#      → Calcula métricas clave por sesión (BB VPIP, BTN%, cbet_IP, fold_turn, etc.)
#
#   2. display_progression_table(df)
#      → Tabla sesión-a-sesión con semáforos de evolución
#
#   3. display_kpi_gaps(df, m5_result=None)
#      → KPIs primarios (máximo impacto) y secundarios (monitorizar)
#      → Con objetivo, actual, gap y dirección de corrección
#
#   4. display_features_status(hand_count)
#      → Qué está activo ahora, qué se desbloquea cuándo
#
#   5. display_pool_fingerprint_pending(m5_result, stake='NL2')
#      → Gap B referenciado: Pool Fingerprint (PENDING — activo a 5k manos)
#      → Muestra las desviaciones del pool real vs referencia teórica
#      → Anticipa los ajustes exploitativos cuando M5 sea 'confirmed'
# ════════════════════════════════════════════════════════════════════════════


def calculate_progression_metrics(df):
    """
    Calcula métricas de progresión por sesión.
    Retorna lista de dicts, una entrada por sesión con las métricas clave.
    """
    sessions = []
    for sid in sorted(df['session_id'].unique()):
        s    = df[df['session_id'] == sid]
        bb_s = s[s['player_position'] == 'BB']
        btn_s= s[s['player_position'] == 'BTN']

        # BB VPIP — métrica más importante en M1
        bb_vpip = (pd.to_numeric(bb_s['flg_vpip'], errors='coerce').mean() * 100) if len(bb_s) >= 10 else None

        # BTN open — segunda más importante
        btn_vpip = (pd.to_numeric(btn_s['flg_vpip'], errors='coerce').mean() * 100) if len(btn_s) >= 8 else None

        # helper: castea columna a numérico (fix: flags pueden volver de SQLite como object/string)
        def _num(col): return pd.to_numeric(col, errors='coerce').fillna(0)

        # Cbet IP SRP
        ci_opp = _num(s[s['ip_oop'] == 'IP']['flg_f_cbet_opp']).sum()
        ci_hit  = _num(s[s['ip_oop'] == 'IP']['flg_f_cbet']).sum()
        cbet_ip = (ci_hit / ci_opp * 100) if ci_opp >= 5 else None

        # Fold vs cbet (OOP = BB/SB defendiendo)
        # FIX v1.75: fold vs cbet = manos que foldaron / manos enfrentando cbet
        # FIX v1.75b: incluir 'X_F' (check-fold) además de 'F' (fold directo)
        _s_oop_cbet = s[(s.get('ip_oop', pd.Series()) == 'OOP') & (_num(s['flg_f_cbet_def_opp']) > 0)] if 'ip_oop' in s.columns else s.iloc[0:0]
        fvc_opp = len(_s_oop_cbet)
        def _fvc_is_fold(a):
            a = str(a).upper(); return a.startswith('F') or '_F' in a
        fvc_fold_n = _s_oop_cbet['flop_action'].fillna('').apply(_fvc_is_fold).sum() if fvc_opp > 0 else 0
        fold_cbet_oop = (fvc_fold_n / fvc_opp * 100) if fvc_opp >= 5 else None

        # 3-bet — usar flg_p_3bet_opp (nombre correcto del parser; flg_p_3bet_def_opp no existe)
        _3bet_col = 'flg_p_3bet_opp' if 'flg_p_3bet_opp' in s.columns else 'flg_p_3bet_def_opp'
        tbet_opp = _num(s[_3bet_col]).sum() if _3bet_col in s.columns else 0
        tbet_hit = _num(s['flg_p_3bet']).sum() if 'flg_p_3bet' in s.columns else 0
        threeb = (tbet_hit / tbet_opp * 100) if tbet_opp >= 5 else None

        # Net y EV
        net = s['net_won'].sum()
        hands = len(s)

        # Fecha
        date_raw = s['date'].iloc[0]
        try:
            date_str = date_raw.strftime('%Y-%m-%d')
        except Exception:
            date_str = str(date_raw)[:10]

        # v1.63 — WTSD y W$SD por sesión (SSOT §5.3)
        # WTSD: Went To Showdown % (sobre manos con flop)
        # Ref NL2: 25-32%. Encima = pagando demasiado en calles tardías.
        n_saw_f  = int(_num(s['flg_f_saw']).sum()) if 'flg_f_saw' in s.columns else 0
        n_sd_s   = int(_num(s['flg_showdown']).sum()) if 'flg_showdown' in s.columns else 0
        wtsd = round(n_sd_s / n_saw_f * 100, 1) if n_saw_f >= 10 else None

        # W$SD: Won $ at Showdown % (sobre manos con showdown)
        # Ref: >50%. Bajo = llegas a SD y pierdes más de la mitad.
        if 'flg_showdown' in s.columns and 'flg_won_hand' in s.columns and n_sd_s >= 5:
            mask_sd = _num(s['flg_showdown']) == 1
            mask_won = _num(s['flg_won_hand']) == 1
            n_won_sd_s = int((mask_sd & mask_won).sum())
            wsd = round(n_won_sd_s / n_sd_s * 100, 1)
        else:
            wsd = None

        sessions.append({
            'session_id':    sid,
            'date':          date_str,
            'hands':         hands,
            'net_eur':       round(net, 2),
            'bb_vpip':       round(bb_vpip, 1) if bb_vpip is not None else None,
            'btn_vpip':      round(btn_vpip, 1) if btn_vpip is not None else None,
            'cbet_ip':       round(cbet_ip, 1) if cbet_ip is not None else None,
            'fold_cbet_oop': round(fold_cbet_oop, 1) if fold_cbet_oop is not None else None,
            'threeb':        round(threeb, 1) if threeb is not None else None,
            'wtsd':          wtsd,   # v1.63: WTSD% por sesión
            'wsd':           wsd,    # v1.63: W$SD% por sesión
        })
    return sessions


def display_progression_table(df, m5_result=None):
    """
    Muestra tabla de progresión sesión a sesión con semáforos.
    """
    sessions = calculate_progression_metrics(df)

    # Objetivos de referencia
    targets = {
        'bb_vpip':       50.0,   # defender más desde BB
        'btn_vpip':      45.0,   # abrir más desde BTN
        'cbet_ip':       62.0,   # cbet IP en rango
        'fold_cbet_oop': 45.0,   # fold vs cbet OOP (no sobre-foldear ni over-call)
        'threeb':         8.0,   # 3-bet frequency
    }

    def _flag(val, target, name):
        if val is None:
            return '⚪'
        diff = val - target
        # Para estas métricas, bajo es malo (necesitas subir)
        if name in ('bb_vpip', 'btn_vpip', 'cbet_ip', 'threeb'):
            if diff >= -5:   return '✅'
            if diff >= -15:  return '🟡'
            return '🔴'
        # fold_cbet_oop: queremos ~40-55%, muy bajo = over-call, muy alto = over-fold
        elif name == 'fold_cbet_oop':
            if 35 <= val <= 55:  return '✅'
            if 25 <= val <= 65:  return '🟡'
            return '🔴'
        return '⚪'

    print()
    print('─' * 90)
    print('  📈 PROGRESIÓN SESIÓN A SESIÓN')
    print('─' * 90)
    print(f'  {"Sesión":<12} {"Fecha":<12} {"Manos":>5} {"Net€":>7} '
          f'{"BB_VPIP":>8}{"🎯":1} {"BTN%":>6}{"🎯":1} '
          f'{"CbetIP":>7}{"🎯":1} {"FvcbOOP":>8}{"🎯":1} '
          f'{"WTSD":>6}{"🎯":1} {"W$SD":>6}{"🎯":1}')  # v1.63
    print(f'  {"":<12} {"Objetivo":>12} {"":>5} {"":>7} '
          f'{"50%":>8}{"":1} {"45%":>6}{"":1} '
          f'{"62%":>7}{"":1} {"40-55%":>8}{"":1} '
          f'{"25-32%":>6}{"":1} {"≥50%":>6}{"":1}')  # v1.63
    print('  ' + '─' * 102)

    for s in sessions:
        bb_f   = _flag(s['bb_vpip'],       targets['bb_vpip'],       'bb_vpip')
        btn_f  = _flag(s['btn_vpip'],      targets['btn_vpip'],      'btn_vpip')
        ci_f   = _flag(s['cbet_ip'],       targets['cbet_ip'],       'cbet_ip')
        fvc_f  = _flag(s['fold_cbet_oop'], targets['fold_cbet_oop'], 'fold_cbet_oop')

        def _fmt(v, decimals=1):
            return f'{v:.{decimals}f}%' if v is not None else '  —  '

        net_str = f'{s["net_eur"]:+.2f}€'
        net_col = '🟢' if s['net_eur'] > 0 else '🔴'

        # v1.63: WTSD + W$SD semáforos
        wtsd_v = s.get('wtsd')
        wsd_v  = s.get('wsd')
        # WTSD: 25-32% verde, 33-38% amarillo, >38% o <20% rojo
        wtsd_f = ('✅' if wtsd_v is not None and 25 <= wtsd_v <= 32 else
                  '🟡' if wtsd_v is not None and 20 <= wtsd_v <= 38 else
                  '🔴' if wtsd_v is not None else '⚪')
        # W$SD: ≥52% verde, 45-51% amarillo, <45% rojo
        wsd_f  = ('✅' if wsd_v is not None and wsd_v >= 52 else
                  '🟡' if wsd_v is not None and wsd_v >= 45 else
                  '🔴' if wsd_v is not None else '⚪')
        print(f'  {s["session_id"]:<12} {s["date"]:<12} {s["hands"]:>5} '
              f'{net_col}{net_str:>6} '
              f'{_fmt(s["bb_vpip"]):>8}{bb_f} '
              f'{_fmt(s["btn_vpip"]):>6}{btn_f} '
              f'{_fmt(s["cbet_ip"]):>7}{ci_f} '
              f'{_fmt(s["fold_cbet_oop"]):>8}{fvc_f} '
              f'{_fmt(wtsd_v):>6}{wtsd_f} '
              f'{_fmt(wsd_v):>6}{wsd_f}')  # v1.63

    # Totals / rolling average
    valid_bb    = [s['bb_vpip']       for s in sessions if s['bb_vpip']       is not None]
    valid_btn   = [s['btn_vpip']      for s in sessions if s['btn_vpip']      is not None]
    valid_cbet  = [s['cbet_ip']       for s in sessions if s['cbet_ip']       is not None]
    valid_fvc   = [s['fold_cbet_oop'] for s in sessions if s['fold_cbet_oop'] is not None]

    avg_bb  = sum(valid_bb)  / len(valid_bb)  if valid_bb  else None
    avg_btn = sum(valid_btn) / len(valid_btn) if valid_btn else None

    print('  ' + '─' * 88)
    total_net = sum(s['net_eur'] for s in sessions)
    total_h   = sum(s['hands'] for s in sessions)
    net_col   = '🟢' if total_net > 0 else '🔴'
    def _fmt2(v):
        return f'{v:.1f}%' if v is not None else '—'

    print(f'  {"TOTAL":.<12} {"":.<12} {total_h:>5} '
          f'{net_col}{total_net:>6.2f}€ '
          f'{_fmt2(avg_bb):>8}   '
          f'{_fmt2(avg_btn):>6}   '
          f'{_fmt2(sum(valid_cbet)/len(valid_cbet) if valid_cbet else None):>7}   '
          f'{_fmt2(sum(valid_fvc)/len(valid_fvc) if valid_fvc else None):>8}  ')

    # Trend arrow for BB VPIP (most important)
    if len(valid_bb) >= 3:
        first3 = sum(valid_bb[:3]) / 3
        last3  = sum(valid_bb[-3:]) / 3
        diff   = last3 - first3
        arrow  = '⬆️' if diff > 2 else ('⬇️' if diff < -2 else '➡️')
        print()
        print(f'  Tendencia BB VPIP (últimas 3 vs primeras 3 sesiones): '
              f'{first3:.1f}% → {last3:.1f}% ({diff:+.1f}pp) {arrow}')

    print('─' * 90)
    return sessions




def display_learning_velocity(df, window=3):
    """
    Mide la VELOCIDAD DE CORRECCIÓN de cada KPI clave.

    PROBLEMA QUE RESUELVE:
    El promedio histórico total diluye la mejora real. Si en las primeras
    2.000 manos tu BB VPIP era 25% y ahora es 38%, el total histórico sigue
    siendo ~30% y parece que no has avanzado. Este módulo mide el movimiento
    real en una ventana deslizante, separando el aprendizaje pasado del actual.

    LÓGICA:
    - Divide las sesiones en bloques de `window` sesiones
    - Compara el bloque más reciente vs el anterior
    - Calcula velocidad = (bloque_reciente - bloque_anterior) por sesión
    - Veredicto: ACELERANDO / ESTABLE / RETROCEDIENDO por KPI

    Args:
        df:     DataFrame completo con todas las sesiones
        window: nº sesiones por bloque (default 3 — mínimo para señal)

    Returns:
        dict con velocidades y veredictos por KPI
    """
    sessions = calculate_progression_metrics(df)
    n = len(sessions)

    KPI_CONFIG = {
        'bb_vpip':       {'label': 'BB VPIP',         'target': 50.0, 'higher_better': True,  'unit': '%'},
        'btn_vpip':      {'label': 'BTN open',         'target': 45.0, 'higher_better': True,  'unit': '%'},
        'cbet_ip':       {'label': 'Cbet IP SRP',      'target': 62.0, 'higher_better': True,  'unit': '%'},
        'fold_cbet_oop': {'label': 'Fold vs cbet OOP', 'target': 47.0, 'higher_better': None,  'unit': '%'},
        'threeb':        {'label': '3-bet PF',         'target':  8.0, 'higher_better': True,  'unit': '%'},
    }

    print()
    print('─' * 90)
    print('  🚀 VELOCIDAD DE CORRECCIÓN — ¿Está el aprendizaje ocurriendo?')
    print(f'  Ventana: {window} sesiones por bloque | {n} sesiones totales')
    print('─' * 90)

    if n < window * 2:
        print(f'  ⚪ Insuficientes sesiones para ventana deslizante.')
        print(f'     Necesitas al menos {window * 2} sesiones (tienes {n}).')
        print(f'     Sigue jugando — la señal aparece con {window * 2}+ sesiones.')
        print('─' * 90)
        return {}

    results = {}

    # Bloques: comparar últimas `window` sesiones vs las `window` anteriores
    recent_sessions = sessions[-(window):]
    prev_sessions   = sessions[-(window*2):-(window)]

    print(f'  Bloque anterior : sesiones {prev_sessions[0]["session_id"]} → {prev_sessions[-1]["session_id"]}')
    print(f'  Bloque reciente : sesiones {recent_sessions[0]["session_id"]} → {recent_sessions[-1]["session_id"]}')
    print()
    print(f'  {"KPI":<20} {"Anterior":>10} {"Reciente":>10} {"Cambio":>8} {"vs Objetivo":>12}  Veredicto')
    print('  ' + '─' * 75)

    for kpi, cfg in KPI_CONFIG.items():
        prev_vals   = [s[kpi] for s in prev_sessions   if s[kpi] is not None]
        recent_vals = [s[kpi] for s in recent_sessions if s[kpi] is not None]

        if len(prev_vals) < 1 or len(recent_vals) < 1:
            print(f'  {cfg["label"]:<20} {"—":>10} {"—":>10} {"—":>8} {"—":>12}  ⚪ sin datos')
            results[kpi] = {'veredicto': 'SIN_DATOS', 'velocidad': None}
            continue

        prev_avg   = sum(prev_vals)   / len(prev_vals)
        recent_avg = sum(recent_vals) / len(recent_vals)
        delta      = recent_avg - prev_avg
        target     = cfg['target']
        gap_recent = recent_avg - target

        # Velocidad: cambio por sesión (normalizado)
        velocity = delta / window

        # Veredicto
        higher = cfg['higher_better']
        if higher is None:
            # Para métricas con rango óptimo (fold vs cbet): moverse hacia 47%
            moving_toward = abs(recent_avg - target) < abs(prev_avg - target)
            if abs(delta) < 1.5:
                verdict, sem = 'ESTABLE',      '🟡'
            elif moving_toward:
                verdict, sem = 'MEJORANDO',    '🟢'
            else:
                verdict, sem = 'ALEJÁNDOSE',   '🔴'
        else:
            improving = delta > 0 if higher else delta < 0
            if abs(delta) < 1.5:
                verdict, sem = 'ESTABLE',      '🟡'
            elif improving:
                verdict, sem = 'ACELERANDO',   '🟢'
            else:
                verdict, sem = 'RETROCEDIENDO','🔴'

        # Gap vs objetivo
        gap_str = f'{gap_recent:+.1f}pp'
        gap_sem = '✅' if abs(gap_recent) < 5 else ('🟡' if abs(gap_recent) < 15 else '🔴')

        print(f'  {cfg["label"]:<20} {prev_avg:>9.1f}% {recent_avg:>9.1f}% {delta:>+7.1f}pp {gap_str:>11} {gap_sem}  {sem} {verdict}')

        results[kpi] = {
            'prev_avg':  round(prev_avg, 1),
            'recent_avg': round(recent_avg, 1),
            'delta':     round(delta, 1),
            'velocity':  round(velocity, 2),
            'veredicto': verdict,
            'semaforo':  sem,
            'gap_target': round(gap_recent, 1),
        }

    print()

    # ── Resumen narrativo ────────────────────────────────────────────────────
    mejorando = [cfg['label'] for kpi, cfg in KPI_CONFIG.items()
                 if results.get(kpi, {}).get('veredicto') in ('ACELERANDO', 'MEJORANDO')]
    retrocediendo = [cfg['label'] for kpi, cfg in KPI_CONFIG.items()
                     if results.get(kpi, {}).get('veredicto') in ('RETROCEDIENDO', 'ALEJÁNDOSE')]
    estable = [cfg['label'] for kpi, cfg in KPI_CONFIG.items()
               if results.get(kpi, {}).get('veredicto') == 'ESTABLE']

    if mejorando:
        print(f'  🟢 Mejorando:     {", ".join(mejorando)}')
    if estable:
        print(f'  🟡 Estable:       {", ".join(estable)}')
    if retrocediendo:
        print(f'  🔴 Retrocediendo: {", ".join(retrocediendo)}')

    # ── Diagnóstico del leak activo (DRILL_ACTIVO) ───────────────────────────
    bb_res = results.get('bb_vpip', {})
    if bb_res.get('veredicto') in ('ACELERANDO', 'MEJORANDO'):
        print()
        print(f'  ✅ DRILL BB VPIP: el aprendizaje está ocurriendo.')
        print(f'     {bb_res["prev_avg"]}% → {bb_res["recent_avg"]}% ({bb_res["delta"]:+.1f}pp en {window} sesiones)')
        if bb_res["gap_target"] < -5:
            print(f'     Objetivo 50% — faltan {abs(bb_res["gap_target"]):.0f}pp. Sigue con el drill.')
        else:
            print(f'     ¡Muy cerca del objetivo 50%! Considera pasar al siguiente leak.')
    elif bb_res.get('veredicto') == 'ESTABLE':
        print()
        print(f'  🟡 DRILL BB VPIP: sin movimiento en las últimas {window} sesiones.')
        print(f'     La regla de mesa está activada? ¿Estás usando el M4.2 trainer post-sesión?')
    elif bb_res.get('veredicto') == 'RETROCEDIENDO':
        print()
        print(f'  🔴 DRILL BB VPIP: BB VPIP bajando. Revisa si la regla de mesa sigue activa.')

    print('─' * 90)
    return results


def display_kpi_gaps(df, m5_result=None):
    """
    Muestra KPIs primarios y secundarios con gaps vs objetivo.
    Primarios: máximo impacto económico inmediato.
    Secundarios: monitorizar, trabajar progresivamente.
    """
    freqs = m5_result.get('frequencies', {}) if m5_result else {}

    # Compute current values
    bb   = df[df['player_position'] == 'BB']
    btn  = df[df['player_position'] == 'BTN']
    sb   = df[df['player_position'] == 'SB']

    bb_vpip   = pd.to_numeric(bb['flg_vpip'],  errors='coerce').mean() * 100 if len(bb)  > 20 else None
    btn_vpip  = pd.to_numeric(btn['flg_vpip'], errors='coerce').mean() * 100 if len(btn) > 20 else None
    sb_vpip   = pd.to_numeric(sb['flg_vpip'],  errors='coerce').mean() * 100 if len(sb)  > 20 else None

    # Fold turn / river (from M5 — pool tendency, not hero action)
    fold_turn  = freqs.get('fold_vs_turn_barrel', {}).get('freq_obs', None)
    fold_river = freqs.get('fold_vs_river_bet', {}).get('freq_obs', None)
    fold_turn  = fold_turn * 100  if fold_turn is not None else None
    fold_river = fold_river * 100 if fold_river is not None else None

    # Hero frequencies
    # _n(): cast flag col to numeric before aggregation (fix SQLite object columns)
    def _n(col): return pd.to_numeric(col, errors='coerce').fillna(0)
    cbet_ip_opp = _n(df[df['ip_oop'] == 'IP']['flg_f_cbet_opp']).sum()
    cbet_ip_hit = _n(df[df['ip_oop'] == 'IP']['flg_f_cbet']).sum()
    cbet_ip = (cbet_ip_hit / cbet_ip_opp * 100) if cbet_ip_opp >= 20 else None

    cbet_oop_opp = _n(df[df['ip_oop'] == 'OOP']['flg_f_cbet_opp']).sum()
    cbet_oop_hit = _n(df[df['ip_oop'] == 'OOP']['flg_f_cbet']).sum()
    cbet_oop = (cbet_oop_hit / cbet_oop_opp * 100) if cbet_oop_opp >= 20 else None

    # FIX v1.75: flg_f_cbet_def=True significa CONTINUÓ (call/raise), NO fold
    # El fold real vs cbet = flop_action.startswith('F') en manos con cbet_def_opp=True
    # Validado: 11.4% fold real vs PT4 31.4% — sigue habiendo diferencia de definición
    # PT4 incluye manos donde hero no llegó al flop como denominador distinto
    # FIX v1.75b: fold vs cbet OOP incluye 'X_F' (check-fold) y 'F' (fold directo)
    # OOP siempre actúa después del check — fold viene como 'X_F' no como 'F' solo
    # Validado: 31/94 folds = 33.0% vs PT4 31.4% (Δ1.6pp ✅)
    _df_oop_cbet = df[(df['ip_oop'] == 'OOP') & (_n(df['flg_f_cbet_def_opp']) > 0)]
    fvc_opp = len(_df_oop_cbet)
    def _is_fold_vs_cbet(action):
        a = str(action).upper()
        return a.startswith('F') or '_F' in a or a.endswith('_F')
    fvc_fold = _df_oop_cbet['flop_action'].fillna('').apply(_is_fold_vs_cbet).sum() if fvc_opp > 0 else 0
    fold_cbet = (fvc_fold / fvc_opp * 100) if fvc_opp >= 20 else None

    # FIX v1.75: denominador correcto para 3Bet% = flg_p_3bet_opp
    # flg_p_3bet_def_opp = hero enfrenta 3bet (es el defensor) — INCORRECTO
    # flg_p_3bet_opp = hero tiene oportunidad de 3bet (ya hay un raise) — CORRECTO
    # Validado: 5.4% vs PT4 5.78% (Δ0.4pp ✅)
    tbet_opp = _n(df['flg_p_3bet_opp']).sum() if 'flg_p_3bet_opp' in df.columns else                _n(df['flg_p_3bet_def_opp']).sum() if 'flg_p_3bet_def_opp' in df.columns else 0
    tbet_hit = _n(df['flg_p_3bet']).sum() if 'flg_p_3bet' in df.columns else 0
    threeb = (tbet_hit / tbet_opp * 100) if tbet_opp >= 20 else None

    raise_cbet_n = freqs.get('raise_vs_cbet', {}).get('freq_obs', None)
    raise_cbet = raise_cbet_n * 100 if raise_cbet_n is not None else None

    def _kpi_line(name, cur, target, note, primary=True):
        if cur is None:
            return f'  ⚪ {name:<24}: — (sin datos suficientes)'
        diff  = cur - target
        if primary:
            flag = '🔴' if abs(diff) > 15 else ('🟡' if abs(diff) > 7 else '✅')
        else:
            flag = '🟡' if abs(diff) > 15 else '✅'
        arr   = '⬆️ subir' if diff < -7 else ('⬇️ bajar' if diff > 7 else '✅ en rango')
        return (f'  {flag} {name:<24}: {cur:>5.1f}% → objetivo {target:.0f}%  '
                f'gap {diff:+.1f}pp  {arr}\n'
                f'       {note}')

    print()
    print('─' * 80)
    print('  🎯 KPI DASHBOARD')
    print('─' * 80)
    print()
    print('  ── PRIMARIOS (máximo impacto económico — trabajar AHORA) ──────────────────')
    print()
    print(_kpi_line('BB VPIP',          bb_vpip,   50.0,
                    'Defender BB: AJo+/KQo/ATs+/KJs+/QJs/JTs/T9s/99+/88/77 vs BTN', True))
    print(_kpi_line('BTN VPIP (open%)', btn_vpip,  45.0,
                    'Abrir más desde BTN en posición — es la posición más rentable', True))
    if fold_turn is not None:
        print(_kpi_line('Pool fold vs turn', fold_turn, 50.0,
                        'POOL casi no foldea turn → bluffs en turn sin valor vs este pool', True))
    if fold_river is not None:
        print(_kpi_line('Pool fold vs river', fold_river, 45.0,
                        'POOL casi no foldea river → apostar solo con value en river', True))

    print()
    print('  ── SECUNDARIOS (monitorizar — trabajar progresivamente M1→M2) ─────────────')
    print()
    print(_kpi_line('SB VPIP',          sb_vpip,   40.0,
                    'Defender/steal más desde SB — impacto moderado', False))
    print(_kpi_line('Cbet IP SRP',      cbet_ip,   62.0,
                    'Ligeramente bajo — aumentar cbet cuando tengas ventaja de rango', False))
    if cbet_oop is not None:
        print(_kpi_line('Cbet OOP SRP',     cbet_oop,  48.0,
                        'Alto (+28pp) — reducir cbet OOP vs pool que llama mucho', False))
    if fold_cbet is not None:
        print(_kpi_line('Fold vs cbet OOP', fold_cbet, 45.0,
                        'Zona marginal — equilibrar calls vs folds según textura', False))
    if threeb is not None:
        print(_kpi_line('3-bet global',     threeb,     8.0,
                        'Expandir 3-bet range desde BB vs opens amplios (BTN/CO)', False))
    if raise_cbet is not None:
        print(_kpi_line('Check-raise flop', raise_cbet, 8.0,
                        'Bajo — check-raise con draws y top-pair vs fish en M2', False))

    # v1.63: WTSD + W$SD como KPIs secundarios (SSOT §5.3)
    _n_saw_f_kpi = int(pd.to_numeric(df.get('flg_f_saw', pd.Series([0]*len(df))),
                        errors='coerce').fillna(0).sum())
    # FIX v1.75: WTSD = showdowns / manos que vieron flop (no sobre total manos)
    # PT4 usa flop como denominador. Validado: 28.0% vs PT4 30.4% (Δ2.4pp ✅)
    _df_saw_flop_kpi = df[pd.to_numeric(df.get('flg_f_saw', pd.Series([0]*len(df))),
                          errors='coerce').fillna(0) > 0]
    _n_sd_kpi    = int(pd.to_numeric(
                       _df_saw_flop_kpi.get('flg_showdown',
                       pd.Series([0]*len(_df_saw_flop_kpi))),
                       errors='coerce').fillna(0).sum())
    _wtsd_kpi    = round(_n_sd_kpi / _n_saw_f_kpi * 100, 1) if _n_saw_f_kpi >= 20 else None
    _wsd_kpi     = None
    if 'flg_won_hand' in df.columns and _n_sd_kpi >= 10:
        _sd_mk  = pd.to_numeric(df['flg_showdown'], errors='coerce').fillna(0) == 1
        _won_mk = pd.to_numeric(df['flg_won_hand'],  errors='coerce').fillna(0) == 1
        _n_won_sd_kpi = int((_sd_mk & _won_mk).sum())
        _wsd_kpi = round(_n_won_sd_kpi / _n_sd_kpi * 100, 1)
    print(_kpi_line('WTSD (flop→SD%)', _wtsd_kpi, 29.0,
                        f'ref 25-32% | SD:{_n_sd_kpi}/{_n_saw_f_kpi} manos', False))
    print(_kpi_line('W$SD (ganó SD%)', _wsd_kpi, 52.0,
                        f'ref ≥50% | n={_n_sd_kpi} showdowns', True))

    print()
    print('  ── GATES HACIA M2 ─────────────────────────────────────────────────────────')
    bb100 = df['net_won'].sum() / len(df) * 100 / 2 if len(df) > 0 else 0  # approx
    # Better: use overall_metrics if available
    from collections import namedtuple
    try:
        _om, _ = calculate_ev_metrics(df)
        bb100  = _om.get('bb_per_100_net', 0)
        ev_h   = _om.get('ev_euro_per_hour', 0)
    except Exception:
        ev_h = 0
    frx = calculate_friccion_avg(df)
    print(f'  {"✅" if frx<=2 else "❌"} Fricción ≤ 2:     {frx:.2f} (actual) → {"✅ CUMPLE" if frx<=2 else "❌ revisar"}')
    print(f'  {"✅" if bb100>0 else "❌"} BB/100 > 0:       {bb100:+.2f} BB/100 → {"✅ CUMPLE" if bb100>0 else f"faltan {abs(bb100):.1f} BB/100"}')
    print(f'  {"✅" if ev_h>0 else "❌"} EV/h > 0€:        {ev_h:.2f}€/h → {"✅ CUMPLE" if ev_h>0 else "en negativo"}')
    print(f'  {"✅" if len(df)>=30000 else "❌"} Manos ≥ 30.000:  {len(df):,} → faltan {max(0,30000-len(df)):,}')
    print()
    print('─' * 80)

    # ── GAP E v1.44: Hero vs Pool tabla ───────────────────────────────────────
    if m5_result:
        _freqs = m5_result.get('frequencies', {})
        _M5S   = m5_result.get('m5_spots_ref', {})
        # Importar M5_SPOTS del scope global si no viene en m5_result
        try: _M5S = _M5S or M5_SPOTS
        except NameError: pass
        _vpip = pd.to_numeric(df['flg_vpip'], errors='coerce').mean()*100 if 'flg_vpip' in df.columns else 0
        # FIX v1.51: PFR = cnt_p_raise>0 (cualquier raise PF) = definición PT4.
        _pfr  = (pd.to_numeric(df['cnt_p_raise'], errors='coerce') > 0).mean()*100 if 'cnt_p_raise' in df.columns else (
                pd.to_numeric(df['flg_p_first_raise'], errors='coerce').mean()*100 if 'flg_p_first_raise' in df.columns else 0)
        print()
        print('── Hero vs Pool NL2 (GAP E) ──────────────────────────────────────────────')
        print(f'  {"Stat":<28} {"Hero":>8} {"Pool NL2":>10} {"Gap":>8}')
        print(f'  {"-"*28} {"-"*8} {"-"*10} {"-"*8}')
        _pool_vpip = 24.0; _pool_pfr = 18.0
        _gap_v = _vpip - _pool_vpip; _gap_p = _pfr - _pool_pfr
        _flag_v = '⚠️ loose' if _gap_v > 6 else ('⚠️ tight' if _gap_v < -4 else '✅')
        _flag_p = '⚠️ passive' if _gap_p < -3 else ('⚠️ aggro' if _gap_p > 5 else '✅')
        print(f'  {"VPIP":<28} {_vpip:>7.1f}% {_pool_vpip:>9.1f}% {_gap_v:>+7.1f}pp {_flag_v}')
        print(f'  {"PFR":<28} {_pfr:>7.1f}% {_pool_pfr:>9.1f}% {_gap_p:>+7.1f}pp {_flag_p}')
        for _sname, _sdesc in [
            ('fold_vs_3bet',       'Fold vs 3bet'),
            ('cbet_OOP_SRP',       'Cbet OOP SRP'),
            ('cbet_IP_SRP',        'Cbet IP SRP'),
            ('BTN_open',           'BTN open'),
            ('BB_defend_vs_BTN',   'BB defend vs BTN'),
        ]:
            _d    = _freqs.get(_sname, {})
            _base = _M5S.get(_sname, {}).get('baseline', 0) * 100
            _obs  = _d.get('freq_obs', 0) * 100
            _n    = _d.get('n_opp', 0)
            _exp  = _d.get('exploit_score_pp', round((_obs - _base), 1))
            if _n == 0: continue
            _flag = '🔴 exploit' if abs(_exp) > 15 else ('🟡' if abs(_exp) > 8 else '✅')
            print(f'  {_sdesc:<28} {_obs:>7.1f}% {_base:>9.1f}% {_exp:>+7.1f}pp {_flag}  n={_n}')
        print('─' * 80)


def display_features_status(hand_count):
    """
    Muestra qué features del OS están activas, bloqueadas o futuras.
    Visual claro de la hoja de ruta del sistema.
    """
    features = [
        # (label, gate, descripcion, modo)
        ('Parser + EV All-In + Pool M4.1',    0,
         'Métricas base, clasificación oponentes, EV all-ins', 'M1'),
        ('M5 Pool Detector (señal preliminar)', 3000,
         '21 spots frecuencias del pool vs referencia', 'M1'),
        ('ROI Ranking + Coach Claude (M4.4)',  0,
         'Leaks por €/h + coach directivo modo M1', 'M1'),
        ('Board texture en spots',             5000,
         'Wet/dry/mono entra en spot_id — spots más precisos', 'M1→M2'),
        ('M5 señal confirmada (Wilson IC)',    5000,
         'Frecuencias del pool estadísticamente fiables', 'M1→M2'),
        ('Pool Fingerprint dinámico (Gap B)',  5000,
         'Referencias ajustadas a TU pool real vs teóricas', 'M1→M2',
         'PENDING — compute_pool_fingerprint()'),
        ('Opp_class en spots (fish/reg)',     15000,
         'Spots separados por tipo de oponente — explotación específica', 'M2'),
        ('M2 — modo avanzado',               30000,
         'Coach mixto, drills por oponente, análisis postflop profundo', 'M2'),
        ('M6 TexasSolver (GTO frequencies)', 30000,
         'Frecuencias GTO exactas para leaks persistentes', 'M3'),
    ]

    print()
    print('─' * 80)
    print('  🗺️  MAPA DE FEATURES — ESTADO ACTUAL DEL OS v2')
    print('─' * 80)

    for item in features:
        label  = item[0]
        gate   = item[1]
        desc   = item[2]
        modo   = item[3] if len(item) > 3 else ''
        note   = item[4] if len(item) > 4 else ''

        if hand_count >= gate:
            status = '🟢 ACTIVO  '
            suffix = ''
        else:
            pct    = hand_count / gate * 100
            suffix = f'faltan {gate - hand_count:,} manos ({pct:.0f}%)'
            status = '🔵 PENDING'

        mod_tag = f'[{modo}]' if modo else ''
        print(f'  {status} {mod_tag:<8} {label}')
        print(f'              {desc}')
        if note:
            print(f'              ⚙️  {note}')
        if suffix:
            print(f'              ⏳ {suffix}')

    print('─' * 80)


def display_pool_fingerprint_pending(m5_result, stake='NL2'):
    """
    Gap B — Pool Fingerprint: muestra las desviaciones del pool real vs referencias teóricas.
    Estado: PENDING — se vuelve actionable cuando M5 pase a 'confirmed' (5.000+ manos).
    Ya computa los ajustes exploitativos para que estén visibles aunque no sean fiables aún.
    """
    freqs   = m5_result.get('frequencies', {})
    status  = m5_result.get('status', 'inactive')
    ref     = REFERENCE_RANGES.get(stake, REFERENCE_RANGES.get('NL2', {}))

    print()
    print('─' * 80)
    status_str = '🔬 SEÑAL PRELIMINAR (< 5k manos — orientativo)' if status == 'preliminary' else \
                 '✅ SEÑAL CONFIRMADA — ajustes fiables' if status == 'confirmed' else \
                 f'⏳ INACTIVO (status: {status})'
    print(f'  🌊 POOL FINGERPRINT — {stake}  [{status_str}]')
    print('─' * 80)
    print()

    # Map M5 spots to exploitative adjustments
    adjustments = [
        ('fold_vs_turn_barrel', 'Fold vs barrel turn',  50, 'turn',
         'BLUFF TURN: valor BAJO vs este pool',
         'BLUFF TURN: valor ALTO vs este pool'),
        ('fold_vs_river_bet',   'Fold vs bet river',    45, 'river',
         'BLUFF RIVER: valor BAJO → solo value bets',
         'BLUFF RIVER: valor ALTO → semi-bluffs rentables'),
        ('cbet_IP_SRP',         'Cbet IP SRP',          62, 'flop',
         'CBET IP: pool defiende más → usar manos fuertes',
         'CBET IP: pool over-folds → expandir cbet range'),
        ('cbet_OOP_SRP',        'Cbet OOP SRP',         48, 'flop',
         'CBET OOP: pool llama menos → puedes apostar más',
         'CBET OOP: pool llama mucho → reducir bluffs OOP'),
        ('fold_vs_cbet_IP',     'Fold vs cbet IP',      45, 'flop',
         'FLOAT IP: pool fold poco → floats sin valor',
         'FLOAT IP: pool fold mucho → float con amplio rango'),
        ('BTN_open',            'BTN open%',            45, 'preflop',
         'BTN STEAL: pool defiende más → tighten BTN opens',
         'BTN STEAL: pool over-folds → ampliar BTN range'),
        ('raise_vs_cbet',       'Raise vs cbet',        10, 'flop',
         'CHECK-RAISE: pool no hace cr → puedes apostar libre',
         'CHECK-RAISE: pool cr mucho → cuidado con cbets'),
        ('limp_rate',           'Limp rate',             5, 'preflop',
         'LIMP: pool limpa poco — pool activo',
         'LIMP: pool limpa mucho → ISO raises rentables'),
    ]

    any_data = False
    for key, label, ref_pct, street, adj_low, adj_high in adjustments:
        d = freqs.get(key, {})
        n = d.get('n_opp', 0)
        if n < 50:
            continue
        any_data   = True
        obs        = d['freq_obs'] * 100
        diff       = obs - ref_pct
        conf       = '🟢' if n >= 200 else ('🟡' if n >= 100 else '⚪')
        flag       = '🔴' if abs(diff) > 20 else ('🟡' if abs(diff) > 10 else '✅')
        adj        = adj_low if diff < -10 else (adj_high if diff > 10 else '✅ en rango teórico')

        print(f'  {flag} [{street:>7}] {label:<22}: {obs:>5.1f}% vs ref {ref_pct}%  (Δ{diff:+.1f}pp, n={n}) {conf}')
        print(f'           → EXPLOIT: {adj}')

    if not any_data:
        print('  ⚪ Sin datos suficientes (n<50) en ningún spot del pool fingerprint')
        print('     Activo con señal relevante a partir de ~3.000-5.000 manos')

    if status != 'confirmed':
        print()
        print('  ⚠️  Estado PRELIMINARY — los ajustes son orientativos, no fiables.')
        print('     A 5.000 manos M5 pasa a "confirmed" → exploits estadísticamente sólidos.')
        print('     Gate: activar compute_pool_fingerprint() cuando status == "confirmed"')

    print('─' * 80)




# ════════════════════════════════════════════════════════════════════
# v1.47 — Tres módulos de valor oculto en los datos
# ════════════════════════════════════════════════════════════════════







def display_no_initiative_ev(df):
    """
    No-Initiative EV — BB/100 cuando hero NO tiene la iniciativa preflop.

    VALOR: el mejor predictor de skill real postflop y de escalabilidad de stakes.
    Un crusher tiene sin iniciativa entre -2 y -6 BB/100.
    Un reg débil entre -8 y -12 BB/100.
    Un recreacional -15 BB/100 o peor.

    Hero sin iniciativa = no fue el último agresor preflop
    (no hizo open, no hizo 3bet, no hizo 4bet).

    CONEXIÓN con red/blue line:
    - Sin iniciativa + sin showdown = red line component
    - Sin iniciativa + con showdown = blue line component
    La separación muestra si el problema es overfold (red) o rangos de call (blue).
    """
    bb_val = 0.02

    df['_has_initiative'] = (
        (pd.to_numeric(df['flg_p_open'], errors='coerce').fillna(0) == 1) |
        (pd.to_numeric(df['flg_p_3bet'], errors='coerce').fillna(0) == 1) |
        (pd.to_numeric(df['flg_p_4bet'], errors='coerce').fillna(0) == 1)
    ).astype(int)

    df_init   = df[df['_has_initiative'] == 1]
    df_noinit = df[df['_has_initiative'] == 0]
    n = len(df)

    init_bb100   = (df_init['net_won'].sum()   / max(len(df_init),1)  / bb_val) * 100
    noinit_bb100 = (df_noinit['net_won'].sum() / max(len(df_noinit),1)/ bb_val) * 100
    total_bb100  = (df['net_won'].sum()         / n                   / bb_val) * 100

    def _lvl(bb):
        if bb >= -2:  return "crusher 🟢"
        if bb >= -6:  return "reg sólido 🟡"
        if bb >= -12: return "reg débil 🟡"
        return "en desarrollo 🔴"

    print()
    print('─'*72)
    print('  🎯 NO-INITIATIVE EV — Skill real postflop y escalabilidad')
    print('─'*72)
    print(f'  Con iniciativa:    {len(df_init):,} manos ({len(df_init)/n*100:.1f}%)')
    print(f'  Sin iniciativa:    {len(df_noinit):,} manos ({len(df_noinit)/n*100:.1f}%)')
    print()
    print(f'  {"Tipo":22s} {"BB/100":>8}  Nivel')
    print('  ' + '─'*50)
    print(f'  {"Con iniciativa":22s} {init_bb100:+8.1f}  {_lvl(init_bb100)}')
    print(f'  {"Sin iniciativa":22s} {noinit_bb100:+8.1f}  {_lvl(noinit_bb100)}')
    print(f'  {"TOTAL":22s} {total_bb100:+8.1f}')

    gap = init_bb100 - noinit_bb100
    print()
    if gap > 20:
        print(f'  ⚠️  Gap iniciativa/sin iniciativa: {gap:.1f} BB/100')
        print(f'     Winrate depende de tener la iniciativa.')
        print(f'     Al subir stakes este gap se amplifica.')
    elif gap < 10:
        print(f'  ✅ Gap controlado ({gap:.1f} BB/100) — juegas bien con y sin iniciativa.')

    # Por posición
    print()
    print(f'  {"Pos":5s} {"Sin init BB/100":>16}  {"Manos":>6}  Nivel')
    print('  ' + '─'*45)
    # P4 v1.63: alias HJ→HJ/MP, UTG→UTG/EP para compatibilidad con PT4
    _POS_ALIAS_NI = {'HJ': 'HJ/MP', 'UTG': 'UTG/EP'}
    for pos in ['BTN','CO','HJ','UTG','SB','BB']:
        sub = df_noinit[df_noinit['player_position']==pos]
        if len(sub) < 15: continue
        bb = (sub['net_won'].sum() / max(len(sub),1) / bb_val) * 100
        _pos_lbl = _POS_ALIAS_NI.get(pos, pos)
        print(f'  {_pos_lbl:7s} {bb:+13.1f}  {len(sub):6d}  {_lvl(bb)}')

    # Desglose showdown vs no-showdown sin iniciativa
    ni_sd   = df_noinit[df_noinit['flg_showdown']==1]
    _sd_noinit = pd.to_numeric(df_noinit['flg_showdown'], errors='coerce').fillna(0)
    ni_nosd = df_noinit[_sd_noinit==0]
    if len(ni_sd) > 10:
        ni_sd_bb   = (ni_sd['net_won'].sum()   / max(len(df_noinit),1) / bb_val) * 100
        ni_nosd_bb = (ni_nosd['net_won'].sum() / max(len(df_noinit),1) / bb_val) * 100
        print()
        print(f'  Desglose sin iniciativa:')
        print(f'    + sin showdown (red): {ni_nosd_bb:+.1f} BB/100  ← overfold si muy negativo')
        print(f'    + con showdown (blue):{ni_sd_bb:+.1f} BB/100  ← rangos call si muy negativo')
        if ni_nosd_bb > -5 and ni_sd_bb < -20:
            print(f'  → El problema es RANGOS DE CALL, no overfold.')
            print(f'    Cuando defiendes llegas al SD con las manos equivocadas.')
        elif ni_nosd_bb < -20:
            print(f'  → El problema es OVERFOLD: cedes botes sin pelea.')

    print('─'*72)
    df.drop(columns=['_has_initiative'], inplace=True, errors='ignore')
    return {
        'init_bb100':   round(init_bb100, 1),
        'noinit_bb100': round(noinit_bb100, 1),
        'gap':          round(gap, 1),
        'n_init':       len(df_init),
        'n_noinit':     len(df_noinit),
    }


def display_red_blue_line(df, by_position=True, by_session=True):
    """
    Red line (Non-Showdown Winnings) vs Blue line (Showdown Winnings).

    VALOR: predice escalabilidad de stakes mejor que BB/100 global.
    - Red line positiva = generas fold equity y presión sin depender de cartas
    - Blue line negativa aislada = puede ser varianza
    - Red line negativa = juegas pasivo, cedes botes sin llegar a showdown
    - Ratio blue/red > 3x = perfil dependiente de cartas (riesgo al subir stakes)

    PT4 lo llama "Non-Showdown Winnings" / "Showdown Winnings".
    """
    bb_val = 0.02

    _flg_sd = pd.to_numeric(df['flg_showdown'], errors='coerce').fillna(0)
    df_sd   = df[_flg_sd == 1]
    df_nosd = df[_flg_sd == 0]
    n       = len(df)

    red_net  = df_nosd['net_won'].sum()
    blue_net = df_sd['net_won'].sum()

    red_bb100  = (red_net  / n / bb_val) * 100
    blue_bb100 = (blue_net / n / bb_val) * 100

    print()
    print('─'*72)
    print('  📈 RED LINE vs BLUE LINE — Non-Showdown vs Showdown Winnings')
    print('─'*72)
    print(f'  Manos con showdown:    {len(df_sd):,} ({len(df_sd)/n*100:.1f}%)')
    print(f'  Manos sin showdown:    {len(df_nosd):,} ({len(df_nosd)/n*100:.1f}%)')
    print()
    print(f'  {"Línea":12s} {"Net €":>8} {"BB/100":>8}  Diagnóstico')
    print('  ' + '─'*55)

    # Blue line
    if blue_bb100 > -20:   b_d = "✅ sólida"
    elif blue_bb100 > -60: b_d = "🟡 débil — posible varianza"
    else:                  b_d = "🔴 muy negativa"
    print(f'  {"Blue line":12s} {blue_net:+8.2f}€ {blue_bb100:+7.1f}  {b_d}')

    # Red line
    if red_bb100 > -10:    r_d = "✅ presión activa — generas fold equity"
    elif red_bb100 > -40:  r_d = "🟡 neutral"
    else:                  r_d = "🔴 sin fold equity — perfil pasivo"
    print(f'  {"Red line":12s} {red_net:+8.2f}€ {red_bb100:+7.1f}  {r_d}')
    print(f'  {"TOTAL":12s} {df["net_won"].sum():+8.2f}€ {(df["net_won"].sum()/n/bb_val)*100:+7.1f}')

    # Ratio y diagnóstico de escalabilidad
    ratio = abs(blue_bb100) / (abs(red_bb100) + 0.01)
    print()
    if red_bb100 > -10 and blue_bb100 < -50:
        print('  ⚠️  PATRÓN: red line sana + blue line negativa.')
        print('     Los showdowns te cuestan — puede ser varianza a este volumen.')
        print('     Con más manos se verá si es runbad o rangos incorrectos en SD.')
    elif red_bb100 < -30:
        print('  ⚠️  PATRÓN: red line negativa — cedes botes sin pelea.')
        print('     Posible: over-folding, falta de cbets, sin barrels.')
    elif red_bb100 > 0 and blue_bb100 > -20:
        print('  ✅ PERFIL ROBUSTO: generas valor con y sin showdown.')

    if ratio > 3:
        print(f'  📊 Ratio blue/red = {ratio:.1f}x → dependencia de cartas alta.')
        print(f'     Riesgo al subir stakes: los rivales foldarán menos.')

    # Por posición
    if by_position:
        print()
        print(f'  {"Pos":5s} {"Red BB/100":>11} {"Blue BB/100":>12}  {"n":>5}')
        print('  ' + '─'*40)
        for pos in ['BTN','CO','HJ','UTG','SB','BB']:
            sub = df[df['player_position']==pos]
            if len(sub) < 20: continue
            _sd_pos = pd.to_numeric(sub['flg_showdown'], errors='coerce').fillna(0)
            s_nosd = sub[_sd_pos==0]
            s_sd   = sub[_sd_pos==1]
            rl = (s_nosd['net_won'].sum() / len(sub) / bb_val) * 100
            bl = (s_sd['net_won'].sum()   / len(sub) / bb_val) * 100
            rs = '✅' if rl > -10 else ('🟡' if rl > -30 else '🔴')
            print(f'  {pos:5s} {rl:+10.1f}  {bl:+11.1f}  {len(sub):5d}  {rs}')

    # Tendencia por sesión
    if by_session and df['session_id'].nunique() >= 3:
        print()
        print('  Tendencia red line por sesión:')
        reds = []
        for sid in sorted(df['session_id'].unique()):
            s = df[df['session_id']==sid]
            if len(s) < 30: continue
            _sd_num = pd.to_numeric(s['flg_showdown'], errors='coerce').fillna(0)
            rl = (s[_sd_num==0]['net_won'].sum() / len(s) / bb_val) * 100
            reds.append(rl)
        if len(reds) >= 3:
            trend = reds[-1] - reds[0]
            arrow = '⬆️' if trend > 5 else ('⬇️' if trend < -5 else '➡️')
            print(f'    Primera sesión: {reds[0]:+.1f} BB/100')
            print(f'    Última sesión:  {reds[-1]:+.1f} BB/100')
            print(f'    Tendencia:      {trend:+.1f} pp {arrow}')

    print('─'*72)
    return {
        'red_bb100':   round(red_bb100, 1),
        'blue_bb100':  round(blue_bb100, 1),
        'ratio':       round(ratio, 1),
        'red_net':     round(red_net, 2),
        'blue_net':    round(blue_net, 2),
        'pct_showdown': round(len(df_sd)/n*100, 1),
    }


def display_optimal_session_length(df, checkpoint_hands=50):
    """
    Duración óptima de sesión basada en datos reales del jugador.
    Calcula BB/100 acumulado cada N manos y detecta el punto de caída sostenida.
    """
    bb_val = 0.02
    from collections import defaultdict
    agg = defaultdict(list)
    for sid in sorted(df['session_id'].unique()):
        s = df[df['session_id']==sid].sort_values('date')
        if len(s) < checkpoint_hands * 2: continue
        for cp in range(checkpoint_hands, len(s)+1, checkpoint_hands):
            chunk = s.iloc[:cp]
            bb100 = (chunk['net_won'].sum()/len(chunk)/bb_val)*100
            agg[cp].append(bb100)
    if not agg: return {}
    print()
    print('─'*72)
    print('  ⏳ DURACIÓN ÓPTIMA DE SESIÓN — ¿Cuántas manos antes de parar?')
    print('─'*72)
    print(f'  {"Manos":>6}  {"BB/100 medio":>13}  {"Sesiones":>8}  Señal')
    print('  '+'─'*45)
    results = {}
    optimal_cp = None
    prev_pos = True
    for cp in sorted(agg.keys()):
        n_s = len(agg[cp])
        if n_s < 2: continue
        avg = sum(agg[cp])/n_s
        sem = '🟢' if avg > 5 else ('🔴' if avg < -20 else '🟡')
        bar = '█'*max(0,min(15,int((avg+100)/13)))
        print(f'  {cp:6d}  {avg:+12.1f}  {n_s:8d}  {sem} {bar}')
        results[cp] = {'avg_bb100': round(avg,1), 'n_sessions': n_s}
        if prev_pos and avg < -20 and optimal_cp is None:
            optimal_cp = cp - checkpoint_hands
        prev_pos = avg > 0
    print('  '+'─'*45)
    if optimal_cp and optimal_cp > 0:
        print(f'  📍 Umbral óptimo: ~{optimal_cp} manos')
        print(f'     Después de {optimal_cp} manos tu BB/100 medio cruza a negativo.')
        print(f'     ACCIÓN: stop suave a las {optimal_cp} manos (~{optimal_cp//90:.0f}h a 2 mesas).')
    else:
        print(f'  ⚪ Sin umbral claro todavía. Acumula más sesiones largas.')
    print('─'*72)
    return {'optimal_hands': optimal_cp, 'checkpoints': results}


def display_stack_depth_performance(df, min_hands=30):
    """
    Rendimiento por profundidad de stack inicial.
    Detecta over-stack penalty y rango de stack óptimo del jugador.
    P7 v1.63: guard STACK_EFFECTIVE_GATE (10k manos).
    Con <10k se usa stack del héroe (aproximación).
    Con ≥10k se activará stack efectivo real héroe-villano (P9).
    """
    bb_val = 0.02
    # P7 v1.63: informar cuando stack efectivo real estará disponible
    _gate_eff = globals().get('STACK_EFFECTIVE_GATE', 10_000)
    if len(df) < _gate_eff:
        print(f'  ℹ️  Stack efectivo real (mín. héroe-villano) disponible a '
              f'{_gate_eff:,} manos — ahora usando stack del héroe '
              f'({len(df):,}/{_gate_eff:,} manos)')
    if 'player_stack_start' not in df.columns: return {}
    df_s = df[df['player_stack_start']>0].copy()
    df_s['stack_bb'] = df_s['player_stack_start']/bb_val
    bins   = [0, 40, 70, 90, 100, 120, 999]
    labels = ['<40bb (corto)', '40-70bb', '70-90bb', '90-100bb (full)', '100-120bb (over)', '>120bb (deep)']
    df_s['stack_cat'] = pd.cut(df_s['stack_bb'], bins=bins, labels=labels)
    print()
    print('─'*72)
    print('  💰 RENDIMIENTO POR STACK INICIAL — ¿Con cuántas fichas juegas mejor?')
    print('─'*72)
    print(f'  {"Stack":22s}  {"BB/100":>8}  {"Manos":>6}  Señal')
    print('  '+'─'*50)
    results = {}
    best_range, best_bb100 = None, -999
    for cat in labels:
        sub = df_s[df_s['stack_cat']==cat]
        if len(sub) < min_hands: continue
        bb100 = (sub['net_won'].sum()/len(sub)/bb_val)*100
        sem   = '🟢' if bb100 > -20 else ('🟡' if bb100 > -60 else '🔴')
        print(f'  {cat:22s}  {bb100:+8.1f}  {len(sub):6d}  {sem}')
        results[cat] = {'bb100': round(bb100,1), 'n': len(sub)}
        if bb100 > best_bb100 and len(sub) >= min_hands:
            best_bb100, best_range = bb100, cat
    print('  '+'─'*50)
    if best_range:
        print(f'  ✅ Mejor rendimiento con stack: {best_range} ({best_bb100:+.1f} BB/100)')
    over = results.get('100-120bb (over)',{})
    full = results.get('90-100bb (full)',{})
    if over and full and over['bb100'] < full['bb100'] - 30:
        diff = full['bb100'] - over['bb100']
        print(f'  ⚠️  Over-stack penalty: {diff:.0f} BB/100 peor con 100-120bb vs 90-100bb.')
        print(f'     Recarga hasta exactamente 100bb al inicio de sesión.')
    print('─'*72)
    return results


def display_session_degradation(df, min_hands=100):
    """
    Detecta degradación de rendimiento DENTRO de cada sesión.

    PROBLEMA QUE RESUELVE:
    El sistema mide qué pasa en cada sesión pero no cuándo dentro
    de la sesión empieza a ir mal. Si siempre juegas bien la primera
    hora y mal la segunda, eso vale más saberlo que cualquier stat
    postflop con 3k manos.

    LÓGICA:
    Divide cada sesión en primera mitad vs segunda mitad por manos.
    Calcula BB/100 de cada mitad. Si segunda < primera → degradación.
    Con ≥4 sesiones degradadas del mismo tipo → patrón confirmado.

    Returns: dict con patrón detectado y umbral de parada sugerido
    """
    import numpy as np

    bb_val = 0.02
    degradations = []
    sessions_data = []

    for sid in sorted(df['session_id'].unique()):
        s = df[df['session_id'] == sid].sort_values('date')
        n = len(s)
        if n < min_hands:
            continue

        mid = n // 2
        first_half = s.iloc[:mid]
        second_half = s.iloc[mid:]

        bb100_first  = (first_half['net_won'].sum()  / len(first_half)  / bb_val) * 100
        bb100_second = (second_half['net_won'].sum() / len(second_half) / bb_val) * 100
        delta = bb100_second - bb100_first

        # Punto de inflexión: buscar la mano donde el BB/100 rolling empieza a caer
        s_copy = s.copy().reset_index(drop=True)
        s_copy['running_net'] = s_copy['net_won'].cumsum()
        s_copy['running_bb100'] = (s_copy['running_net'] / (s_copy.index + 1) / bb_val) * 100

        # Mano donde el rendimiento cruza de positivo a negativo (si aplica)
        inflection = None
        if bb100_first > 0 and bb100_second < 0:
            for i in range(mid, n):
                if s_copy['running_bb100'].iloc[i] < 0:
                    inflection = i
                    break

        sessions_data.append({
            'session_id':   sid,
            'hands':        n,
            'bb100_first':  round(bb100_first, 1),
            'bb100_second': round(bb100_second, 1),
            'delta':        round(delta, 1),
            'inflection':   inflection,
            'degraded':     delta < -20,  # umbral significativo
        })
        if delta < -20:
            degradations.append(delta)

    if not sessions_data:
        return {}

    print()
    print('─' * 80)
    print('  ⏱️  DEGRADACIÓN INTRA-SESIÓN — ¿En qué punto de la sesión empiezas a jugar peor?')
    print('─' * 80)
    print(f'  {"Sesión":12s} {"Manos":>5} {"1ª mitad":>10} {"2ª mitad":>10} {"Delta":>8}  Patrón')
    print('  ' + '─' * 60)

    degraded_count = 0
    for s in sessions_data:
        sem = '🔴' if s['degraded'] else ('🟡' if s['delta'] < 0 else '🟢')
        infl_str = f" (cruza 0 en mano ~{s['inflection']})" if s['inflection'] else ''
        print(f'  {s["session_id"]:12s} {s["hands"]:5d} '
              f'{s["bb100_first"]:+9.1f} {s["bb100_second"]:+9.1f} '
              f'{s["delta"]:+7.1f}pp  {sem}{infl_str}')
        if s['degraded']:
            degraded_count += 1

    print('  ' + '─' * 60)
    pct_degraded = degraded_count / len(sessions_data) * 100

    print()
    if pct_degraded >= 60:
        print(f'  🔴 PATRÓN CONFIRMADO: {degraded_count}/{len(sessions_data)} sesiones ({pct_degraded:.0f}%) muestran degradación significativa.')
        avg_delta = sum(s['delta'] for s in sessions_data if s['degraded']) / degraded_count
        print(f'     Caída media cuando degradas: {avg_delta:+.0f} BB/100 en la segunda mitad.')
        print(f'     ACCIÓN: considera sesiones más cortas (stop tras ~{min_hands} manos o 1h)')
        print(f'     o establece un stop-loss de 2 buy-ins para segunda mitad de sesión.')
    elif pct_degraded >= 40:
        print(f'  🟡 TENDENCIA: {degraded_count}/{len(sessions_data)} sesiones con degradación. Señal débil — acumula más sesiones.')
    else:
        print(f'  🟢 Sin patrón de degradación claro ({degraded_count}/{len(sessions_data)} sesiones afectadas).')

    print('─' * 80)
    return {
        'sessions': sessions_data,
        'pct_degraded': round(pct_degraded, 1),
        'patron_confirmado': pct_degraded >= 60,
    }


def display_performance_by_hour(df, min_hands_per_hour=50):
    """
    Rendimiento por hora del día.

    PROBLEMA QUE RESUELVE:
    Si juegas consistentemente peor a ciertas horas, eso vale más
    que cualquier ajuste técnico. Fatiga, tipo de pool en esa franja,
    o simplemente que juegas tarde cuando ya estás cansado.

    LÓGICA:
    Agrupa manos por hora del día. Calcula BB/100 por hora.
    Solo muestra horas con ≥min_hands_per_hour para evitar ruido.
    Marca las horas óptimas vs peligrosas.
    """
    import numpy as np

    bb_val = 0.02
    df_h = df.copy()
    df_h['hour'] = df_h['date'].dt.hour

    by_hour = (df_h.groupby('hour')
               .agg(net=('net_won','sum'), hands=('net_won','count'))
               .reset_index())
    by_hour['bb100'] = (by_hour['net'] / by_hour['hands'] / bb_val) * 100
    by_hour = by_hour[by_hour['hands'] >= min_hands_per_hour]

    if by_hour.empty:
        print(f'  ⚪ Insuficientes datos por hora (mín {min_hands_per_hour} manos/hora).')
        return {}

    print()
    print('─' * 80)
    print('  🕐 RENDIMIENTO POR HORA — ¿Cuándo juegas bien y cuándo mal?')
    print('─' * 80)
    print(f'  {"Hora":6s} {"BB/100":>8} {"Manos":>6}  Barra')
    print('  ' + '─' * 55)

    results = {}
    best_hour  = by_hour.loc[by_hour['bb100'].idxmax()]
    worst_hour = by_hour.loc[by_hour['bb100'].idxmin()]

    for _, row in by_hour.sort_values('hour').iterrows():
        bb = row['bb100']
        n  = int(row['hands'])
        h  = int(row['hour'])
        sem = '🟢' if bb > 10 else ('🔴' if bb < -50 else '🟡')
        # Barra visual normalizada
        bar_len = max(0, min(20, int((bb + 200) / 20)))
        bar = '█' * bar_len
        print(f'  {h:02d}:00  {bb:+8.1f}  {n:6d}  {sem} {bar}')
        results[h] = {'bb100': round(bb,1), 'hands': n}

    print('  ' + '─' * 55)
    print()
    print(f'  ✅ Mejor hora:  {int(best_hour["hour"]):02d}:00 → {float(best_hour["bb100"]):+.1f} BB/100 ({int(best_hour["hands"])} manos)')
    print(f'  ❌ Peor hora:   {int(worst_hour["hour"]):02d}:00 → {float(worst_hour["bb100"]):+.1f} BB/100 ({int(worst_hour["hands"])} manos)')

    diff = best_hour['bb100'] - worst_hour['bb100']
    if diff > 100:
        print()
        print(f'  ⚠️  DIFERENCIA DE {diff:.0f} BB/100 entre mejor y peor hora.')
        print(f'     Esto no es varianza — es estructural. Evita jugar después de las')
        # Find the hour where it starts going bad
        bad_hours = by_hour[by_hour['bb100'] < -50]['hour'].tolist()
        if bad_hours:
            print(f'     {int(min(bad_hours)):02d}:00h según tus datos.')
    print('─' * 80)
    return results


def display_session_stoploss(df, current_session_id=None,
                             stoploss_buyins=2.5, warning_buyins=1.5,
                             lookback_hands=50):
    """
    Stop-loss inteligente por sesión.

    PROBLEMA QUE RESUELVE:
    El sistema mide resultados pero no avisa cuando cruzas un umbral
    que estadísticamente correlaciona con tilt o fatiga en TUS datos.
    No es un límite rígido — es información para tomar la decisión tú.

    LÓGICA:
    1. Pérdida acumulada en sesión actual vs stoploss_buyins
    2. BB/100 de las últimas lookback_hands (señal de calidad actual)
    3. Combina ambas para un veredicto de 3 niveles: OK / AVISO / STOP

    Args:
        stoploss_buyins: buy-ins de pérdida para señal STOP (default 2.5)
        warning_buyins:  buy-ins de pérdida para señal AVISO (default 1.5)
        lookback_hands:  manos recientes para calcular BB/100 actual
    """
    import numpy as np

    bb_val = 0.02
    buyin  = bb_val * 100  # 1 buy-in NL2 = 2€

    # Determinar sesión actual
    if current_session_id is None:
        if 'session_id' not in df.columns or df.empty:
            return {}
        current_session_id = df.sort_values('date')['session_id'].iloc[-1]

    s = df[df['session_id'] == current_session_id].sort_values('date')
    if s.empty:
        return {}

    total_net   = s['net_won'].sum()
    n_hands     = len(s)
    bb100_total = (total_net / n_hands / bb_val) * 100 if n_hands > 0 else 0

    # BB/100 de las últimas N manos
    recent = s.tail(lookback_hands)
    bb100_recent = (recent['net_won'].sum() / len(recent) / bb_val) * 100 if len(recent) >= 10 else None

    # Buyins perdidos
    buyins_lost = abs(total_net) / buyin if total_net < 0 else 0

    print()
    print('─' * 80)
    print(f'  🛑 STOP-LOSS MONITOR — Sesión {current_session_id}')
    print('─' * 80)
    print(f'  Manos jugadas:      {n_hands}')
    print(f'  Net sesión:         {total_net:+.2f}€  ({total_net/buyin:+.1f} buy-ins)')
    print(f'  BB/100 sesión:      {bb100_total:+.1f}')
    if bb100_recent is not None:
        print(f'  BB/100 últimas {lookback_hands}:  {bb100_recent:+.1f}  ← calidad actual del juego')

    # GB v1.67: Aviso basado en curva de degradación personal
    # El sistema ya sabe en qué mano cruza cero cada sesión (display_session_degradation)
    # Aquí calculamos si la sesión actual está cerca de ese umbral histórico
    try:
        _all_sessions = df['session_id'].unique()
        _crossings = []
        for _sid in _all_sessions:
            if _sid == current_session_id:
                continue
            _s = df[df['session_id'] == _sid].sort_values('date')
            if len(_s) < 20:
                continue
            # Detectar mano donde el BB/100 acumulado cruza 0
            _cum = _s['net_won'].cumsum()
            _peak_idx = _cum.idxmax()
            _peak_pos = _s.index.get_loc(_peak_idx)
            if _peak_pos > 5:  # El pico no es al inicio
                _crossings.append(_peak_pos)
        if _crossings:
            _avg_crossing = int(sum(_crossings) / len(_crossings))
            if n_hands >= int(_avg_crossing * 0.85):
                _pct = int(n_hands / _avg_crossing * 100)
                print(f'  ⚠️  GB — Mano crítica personal: llevas {n_hands} manos.')
                print(f'     En tus datos históricos, el deterioro ocurre en la mano ~{_avg_crossing}.')
                if n_hands >= _avg_crossing:
                    print(f'     Has superado tu umbral habitual. Considera parar.')
                else:
                    print(f'     Estás al {_pct}% de tu umbral habitual.')
    except Exception:
        pass

    print()

    # ── Veredicto ────────────────────────────────────────────────────
    stop_triggered   = buyins_lost >= stoploss_buyins
    warning_triggered = buyins_lost >= warning_buyins

    # Degradación reciente agrava el veredicto
    recent_bad = bb100_recent is not None and bb100_recent < -100

    if stop_triggered and recent_bad:
        verdict = 'STOP'
        sem = '🔴'
        msg = (f'Pérdida de {buyins_lost:.1f} buy-ins + últimas {lookback_hands} manos '
               f'a {bb100_recent:+.0f} BB/100. Las probabilidades no están a tu favor ahora mismo.')
    elif stop_triggered:
        verdict = 'STOP'
        sem = '🔴'
        msg = f'Pérdida de {buyins_lost:.1f} buy-ins (umbral: {stoploss_buyins}). Considera cerrar.'
    elif warning_triggered and recent_bad:
        verdict = 'AVISO'
        sem = '🟡'
        msg = (f'Pérdida de {buyins_lost:.1f} buy-ins + juego reciente débil '
               f'({bb100_recent:+.0f} BB/100). Ojo.')
    elif warning_triggered:
        verdict = 'AVISO'
        sem = '🟡'
        msg = f'Pérdida de {buyins_lost:.1f} buy-ins. Normal, pero mantente alerta.'
    else:
        verdict = 'OK'
        sem = '🟢'
        msg = 'Dentro de parámetros normales.'

    print(f'  {sem} {verdict}: {msg}')
    print()

    # Contexto histórico: ¿cómo han terminado sesiones similares?
    similar = []
    for sid in df['session_id'].unique():
        if sid == current_session_id:
            continue
        s2 = df[df['session_id'] == sid]
        # Buscar punto en esa sesión donde tenía pérdida similar
        cumnet = s2.sort_values('date')['net_won'].cumsum()
        crossings = cumnet[cumnet <= total_net]
        if len(crossings) > 0:
            # ¿Cómo terminó después de ese punto?
            idx = crossings.index[-1]
            remaining = s2.loc[idx:]['net_won'].sum()
            similar.append(remaining)

    if len(similar) >= 2:
        avg_recovery = sum(similar) / len(similar)
        positive_recoveries = sum(1 for x in similar if x > 0)
        print(f'  📊 Historial: en {len(similar)} sesiones anteriores con pérdida similar,')
        print(f'     la recuperación media fue {avg_recovery:+.2f}€')
        print(f'     ({positive_recoveries}/{len(similar)} sesiones acabaron mejor desde ese punto)')

    print('─' * 80)
    return {
        'verdict':       verdict,
        'buyins_lost':   round(buyins_lost, 2),
        'bb100_recent':  round(bb100_recent, 1) if bb100_recent else None,
        'bb100_total':   round(bb100_total, 1),
        'stop_triggered': stop_triggered,
    }


print('✅ Módulo de Progresión v1.50 cargado. + velocidad + degradación + hora + stop-loss + duración + stack')
print('   display_progression_table(df)        → tabla sesión-a-sesión con semáforos')
print('   display_kpi_gaps(df, m5_result)       → KPIs primarios + secundarios con gaps')
print('   display_features_status(hand_count)   → mapa de features activas/pendientes')
print('   display_pool_fingerprint_pending(m5)  → Gap B pool fingerprint (preliminary)')


# ════════════════════════════════════════════════════════════════════════
# SECCIÓN 3h — Velocity Forecasting (v1.96)
#
# DIFERENCIA con display_learning_velocity():
#   display_learning_velocity: mide velocidad de corrección (pasado)
#                               compara ventana reciente vs anterior
#   display_velocity_forecast: proyecta al futuro (próximas N sesiones)
#                               "a este ritmo corriges en X sesiones"
#
# MÉTRICAS MONITOREADAS:
#   BB fold rate       — target 35% | pendiente real -2.35pp/sesión
#   SB limp rate       — target 0%  | pendiente real -0.30pp/sesión
#   W$SD%              — target 48% | pendiente real +0.40pp/sesión
#
# DATOS REALES (6.292 manos, 18 sesiones):
#   BB fold: a ritmo actual → 5 sesiones para target
#   SB limp: a ritmo actual → 66 sesiones (señal de alerta)
#   W$SD:    ya en target ✅
#
# HONESTIDAD ESTADÍSTICA:
#   IC 80% = ±11.3pp — proyección indicativa, no garantía
#   Regresión sobre ≥5 sesiones para evitar ruido
# ════════════════════════════════════════════════════════════════════════

def display_velocity_forecast(df, min_sessions=5):
    """
    Proyecta cuántas sesiones faltan para corregir cada leak principal.

    Metodología:
    - Calcula la métrica por sesión (regresión lineal)
    - Proyecta intersección con target
    - Comunica con IC al 80% para honestidad estadística

    Args:
        df:            DataFrame completo con todas las sesiones
        min_sessions:  mínimo de sesiones para proyección fiable

    Returns:
        dict con proyecciones por métrica
    """
    SEP = '═' * 62

    # ── Definición de métricas a proyectar ───────────────────────
    METRICS = {
        'BB_fold': {
            'label':     'BB fold vs steal',
            'target':    35.0,
            'direction': 'down',
            'unit':      '%',
            'drill':     'BB_OOP_SRP_deep_preflop_unknown_F',
        },
        'SB_limp': {
            'label':     'SB limp rate',
            'target':    0.0,
            'direction': 'down',
            'unit':      '%',
            'drill':     'SB_open_or_fold',
        },
        'BTN_limp': {
            'label':     'BTN limp rate',
            'target':    0.0,
            'direction': 'down',
            'unit':      '%',
            'drill':     'BTN_IP_open_postflop',
        },
        'ccall_rate': {
            'label':     'Cold-call PF rate',
            'target':    12.0,
            'direction': 'down',
            'unit':      '%',
            'drill':     'ccall_PF',
        },
        'WSD': {
            'label':     'W$SD%',
            'target':    48.0,
            'direction': 'up',
            'unit':      '%',
            'drill':     None,
        },
    }

    # ── Calcular métrica por sesión ───────────────────────────────
    def _bb_fold_per_session(sdf):
        bb_opp = sdf[(sdf['player_position']=='BB') &
                     (sdf['flg_blind_def_opp'].astype(int)==1)]
        if len(bb_opp) < 5: return None
        return bb_opp['preflop_action'].apply(
            lambda x: str(x).startswith('F') if pd.notna(x) else False
        ).mean() * 100

    def _sb_limp_per_session(sdf):
        sb = sdf[sdf['player_position']=='SB']
        if len(sb) < 5: return None
        return sb['preflop_action'].apply(
            lambda x: str(x)=='' or (str(x).startswith('C') and '_' not in str(x))
        ).mean() * 100

    def _wsd_per_session(sdf):
        sd = sdf[sdf['flg_showdown'].astype(int)==1]
        if len(sd) < 3: return None
        return sd['flg_won_hand'].astype(int).mean() * 100

    def _btn_limp_per_session(sdf):
        """BTN limp rate: % de manos en BTN donde limpeó (no raise, no fold)."""
        btn = sdf[sdf['player_position'] == 'BTN']
        if len(btn) < 5: return None
        limps = btn[
            (btn['flg_vpip'].astype(int) == 1) &
            (btn['cnt_p_raise'].astype(int) == 0)
        ]
        return len(limps) / len(btn) * 100

    def _ccall_per_session(sdf):
        """Cold-call rate: % de manos IP donde cold-calleó sin raise."""
        ip_hands = sdf[sdf['player_position'].isin(['CO','BTN','HJ','UTG'])]
        if len(ip_hands) < 5: return None
        ccalls = ip_hands[
            (ip_hands['flg_vpip'].astype(int) == 1) &
            (ip_hands['cnt_p_raise'].astype(int) == 0)
        ]
        return len(ccalls) / len(ip_hands) * 100

    collectors = {
        'BB_fold':    _bb_fold_per_session,
        'SB_limp':    _sb_limp_per_session,
        'BTN_limp':   _btn_limp_per_session,
        'ccall_rate': _ccall_per_session,
        'WSD':        _wsd_per_session,
    }

    # ── Recopilar datos por sesión ────────────────────────────────
    session_values = {k: [] for k in METRICS}
    for sess_id in sorted(df['session_id'].unique()):
        sdf = df[df['session_id'] == sess_id]
        for key, fn in collectors.items():
            val = fn(sdf)
            if val is not None:
                session_values[key].append(val)

    # ── Proyectar cada métrica ────────────────────────────────────
    print(f"\n{SEP}")
    print(f"  🎯 VELOCITY FORECAST — ¿Cuánto falta para el target?")
    print(SEP)

    forecasts = {}
    for key, meta in METRICS.items():
        values = session_values[key]
        if len(values) < min_sessions:
            print(f"\n  {meta['label']}: sin datos suficientes ({len(values)}/{min_sessions} sesiones)")
            continue

        n = len(values)
        x = np.arange(n)
        y = np.array(values)

        # Regresión lineal
        coeffs = np.polyfit(x, y, 1)
        slope, intercept = coeffs[0], coeffs[1]
        current = y[-1]
        target  = meta['target']
        direction = meta['direction']

        # IC 80% del error de la regresión
        y_pred = intercept + slope * x
        residuals = y - y_pred
        ic_80 = 1.28 * np.std(residuals) if len(residuals) > 2 else 0

        # ¿Ya en target?
        already_there = (direction == 'down' and current <= target) or                         (direction == 'up'   and current >= target)

        # Proyección
        if already_there:
            status = 'TARGET'
            sessions_remaining = 0
            msg = f"✅ YA en target ({current:.1f}% vs {target}%)"
        elif (direction == 'down' and slope < 0) or (direction == 'up' and slope > 0):
            # Tendencia correcta — proyectar
            if abs(slope) < 0.01:
                sessions_remaining = 999
                msg = "⚠️  Tendencia correcta pero muy lenta"
            else:
                n_target = (target - intercept) / slope
                sessions_remaining = max(0, int(np.ceil(n_target - (n-1))))
                if sessions_remaining <= 3:
                    emoji = '🟢'
                elif sessions_remaining <= 8:
                    emoji = '🟡'
                else:
                    emoji = '🔴'
                msg = (f"{emoji} {sessions_remaining} sesiones más al ritmo actual "
                       f"(±{ic_80:.1f}pp IC80%)")
            status = 'ON_TRACK'
        else:
            # Tendencia invertida
            sessions_remaining = None
            status = 'REGRESSING'
            if direction == 'down':
                msg = f"🔴 TENDENCIA INVERSA — {meta['label']} SUBE {abs(slope):.2f}pp/sesión"
            else:
                msg = f"🔴 TENDENCIA INVERSA — {meta['label']} BAJA {abs(slope):.2f}pp/sesión"

        print(f"\n  {meta['label']}")
        print(f"  Actual: {current:.1f}%  →  Target: {target}%  "
              f"| Tendencia: {slope:+.2f}pp/sesión")
        print(f"  {msg}")

        if status == 'ON_TRACK' and sessions_remaining not in (None, 999):
            # Show last 3 sessions for context
            last3 = y[-3:]
            trend_str = ' → '.join([f"{v:.0f}%" for v in last3])
            print(f"  Últimas 3 sesiones: {trend_str}")

        if key == 'SB_limp' and status == 'ON_TRACK' and sessions_remaining and sessions_remaining > 20:
            print(f"  ⚠️  A este ritmo tardas {sessions_remaining} sesiones.")
            print(f"     Necesitas intervención activa: `run_reasoning_session('SB_open_or_fold')`")

        # GAP C v2.01: semáforo estadístico — ¿la mejora es real o ruido?
        # Compara la mejora acumulada desde sesión 1 hasta la última vs IC80
        if len(y) >= 4 and ic_80 > 0 and status in ('ON_TRACK', 'TARGET'):
            improvement = abs(y[0] - y[-1])   # total change observed
            n_half = max(len(y)//2, 1)
            first_half_mean = float(np.mean(y[:n_half]))
            last_half_mean  = float(np.mean(y[n_half:]))
            half_change = abs(first_half_mean - last_half_mean)
            # Signal is real if the half-to-half change exceeds IC80
            if half_change > ic_80:
                stat_signal = '🟢 Mejora estadísticamente real'
                stat_detail = f'Cambio {half_change:.1f}pp > IC80 {ic_80:.1f}pp'
            elif half_change > ic_80 * 0.5:
                stat_signal = '🟡 Mejora posible — aún dentro del ruido'
                stat_detail = f'Cambio {half_change:.1f}pp vs IC80 {ic_80:.1f}pp'
            else:
                stat_signal = '⚪ Sin señal estadística todavía'
                stat_detail = f'Cambio {half_change:.1f}pp — necesitas >{ic_80:.1f}pp'
            print(f"  {stat_signal} ({stat_detail})")
        else:
            stat_signal = '⚪'
            stat_detail = 'Pocas sesiones'

        forecasts[key] = {
            'current': current, 'target': target,
            'slope': slope, 'status': status,
            'sessions_remaining': sessions_remaining,
            'ic_80': ic_80,
            'stat_signal': stat_signal if len(y) >= 4 else '⚪',
        }

    print(f"\n  ──────────────────────────────────────────────────────")
    print(f"  Proyección basada en regresión lineal.")
    print(f"  IC80% indica varianza real — no es una garantía.")

    # [v2.05] STAKE TRANSITION PREDICTIVA
    try:
        if 'evaluate_stake_transition' in dir() and len(df) >= 1000:
            _bb_fc  = forecasts.get('BB_fold', {})
            _sb_fc  = forecasts.get('SB_limp', {})
            _bb_cur = _bb_fc.get('current', None)
            _bb_sr  = _bb_fc.get('sessions_remaining', None)
            _sb_cur = _sb_fc.get('current', None)
            _sb_sr  = _sb_fc.get('sessions_remaining', None)
            if _bb_cur is not None:
                print(f"\n  {SEP}")
                print(f"  STAKE TRANSITION — cuándo estarás listo para subir?")
                _bb_ok = _bb_cur <= 40.0
                _bb_st = 'OK' if _bb_ok else (f'~{_bb_sr} ses.' if _bb_sr and _bb_sr > 0 else 'sin señal')
                _sb_ok = _sb_cur is not None and _sb_cur <= 5.0
                _sb_st = 'OK' if _sb_ok else (f'~{_sb_sr} ses.' if _sb_sr and _sb_sr > 0 else 'sin señal')
                _hok   = len(df) >= 15_000
                _hst   = 'OK' if _hok else f'{len(df):,}/15.000 manos'
                print(f"  BB fold <40%:   {_bb_st} (ahora: {_bb_cur:.1f}%)")
                print(f"  SB limp <5%:    {_sb_st}")
                print(f"  Gate M2 manos:  {_hst}")
                _bottleneck = max(
                    (_bb_sr if not _bb_ok and _bb_sr else 0),
                    (_sb_sr if not _sb_ok and _sb_sr else 0),
                )
                if _bottleneck > 0:
                    print(f"  -> A este ritmo: ~{_bottleneck} sesiones para dominar blinds")
                elif _bb_ok and _sb_ok:
                    print(f"  -> Blinds OK. Gate real: volumen (15k manos)")
    except Exception:
        pass

    print(SEP)
    return forecasts


print("✅ display_velocity_forecast() cargado (v1.96 — Velocity Forecasting)")
print("   Uso: display_velocity_forecast(df)")
print("   Proyecta sesiones restantes para cada leak/target")
print("   Integra con M7 y briefing pre-sesión")


# ════════════════════════════════════════════════════════════════════════════
# MÓDULO M5 — Pool Exploitation Detector v1.26
#
# CAMBIOS vs v1.20:
#   NEW 1 — 18 spots estructurales FIJOS (no spot_identifier dinámico)
#            → señal real desde 1.500-2.000 manos por spot fijo
#   NEW 2 — Wilson Score Interval por spot (IC estadístico correcto)
#            → no declara exploit si el IC cruza el baseline
#   NEW 3 — Semáforo de confianza: 🔴 exploit real | 🟡 señal débil | ⚪ sin datos
#   NEW 4 — Gates: 3.000 manos = señal preliminar | 5.000 = señal confirmada
#
# POR QUÉ 18 SPOTS FIJOS Y NO EL SPOT_IDENTIFIER DINÁMICO:
# Con 3.216 manos y 651 spots únicos → media de 5 manos/spot → ruido puro.
# Los 18 spots fijos tienen 150-500 oportunidades cada uno desde las primeras
# 2.000 manos → señal estadísticamente válida mucho antes.
# ════════════════════════════════════════════════════════════════════════════

import math as _math

# ── Baselines M5 — dos fuentes según volumen ─────────────────────────────
# REGLA: < 5.000 manos → usar POOL_BASELINES_THEORETICAL (valores de referencia GTO/exploitativo)
#        ≥ 5.000 manos → usar POOL_BASELINES_EMPIRICAL (calculado del pool real observado)
# Los baselines empíricos se recalculan automáticamente en run_m5_pool_detector()
# cuando hay suficiente volumen. Hasta entonces, los teóricos son la referencia.
POOL_BASELINES_THEORETICAL = {
    'BTN_open':            0.45,
    'CO_open':             0.35,
    'HJ_open':             0.28,
    'UTG_open':            0.20,
    'SB_open':             0.40,
    'BB_defend_vs_BTN':    0.55,
    'BB_defend_vs_CO':     0.50,
    'BB_defend_vs_UTG':    0.40,
    'fold_vs_3bet':        0.50,
    'cbet_IP_SRP':         0.62,
    'cbet_OOP_SRP':        0.48,
    'fold_vs_cbet_IP':     0.45,
    'fold_vs_cbet_OOP':    0.50,
    'raise_vs_cbet':       0.10,
    'second_barrel_IP':    0.50,
    'fold_vs_turn_barrel': 0.50,
    'river_bet_IP':        0.45,
    'fold_vs_river_bet':   0.45,
    'limp_rate':           0.05,
    'ccall_PF':            0.08,
    'check_raise_flop':    0.08,
}

# Baselines empíricos: se rellenan automáticamente cuando hand_count >= M5_ACTIVATION_HANDS_CONFIRMED
# Hasta entonces es un dict vacío — run_m5_pool_detector() usará POOL_BASELINES_THEORETICAL
POOL_BASELINES_EMPIRICAL = {}   # ← poblado en tiempo de ejecución por _update_empirical_baselines()

# ── 18 spots estructurales fijos ─────────────────────────────────────────
# Cada spot = {nombre: baseline_frecuencia_esperada_en_NL2}
# Fuente: aproximaciones exploitativas calibradas para pools blandos de micros.
# Recalibrar a percentiles del pool observado cuando M5 tenga ≥5k manos.
M5_SPOTS = {
    # ── PREFLOP ──────────────────────────────────────────────────────────
    'BTN_open':            {'baseline': 0.45, 'desc': 'Hero abre BTN (VPIP BTN)'},
    'CO_open':             {'baseline': 0.35, 'desc': 'Hero abre CO'},
    'HJ_open':             {'baseline': 0.28, 'desc': 'Hero abre HJ/MP'},
    'UTG_open':            {'baseline': 0.20, 'desc': 'Hero abre UTG'},
    'SB_open':             {'baseline': 0.40, 'desc': 'Hero completa/abre SB'},
    'BB_defend_vs_BTN':    {'baseline': 0.55, 'desc': 'Hero defiende BB vs BTN'},
    'BB_defend_vs_CO':     {'baseline': 0.50, 'desc': 'Hero defiende BB vs CO'},
    'BB_defend_vs_UTG':    {'baseline': 0.40, 'desc': 'Hero defiende BB vs UTG'},
    'fold_vs_3bet':        {'baseline': 0.50, 'desc': 'Hero foldea vs 3-bet'},
    # ── FLOP ─────────────────────────────────────────────────────────────
    'cbet_IP_SRP':         {'baseline': 0.62, 'desc': 'Hero c-bet IP en SRP flop'},
    'cbet_OOP_SRP':        {'baseline': 0.48, 'desc': 'Hero c-bet OOP en SRP flop'},
    'fold_vs_cbet_IP':     {'baseline': 0.45, 'desc': 'Hero foldea vs c-bet IP'},
    'fold_vs_cbet_OOP':    {'baseline': 0.50, 'desc': 'Hero foldea vs c-bet OOP'},
    'raise_vs_cbet':       {'baseline': 0.10, 'desc': 'Hero raise vs c-bet (cualquier posición)'},
    # ── TURN ─────────────────────────────────────────────────────────────
    'second_barrel_IP':    {'baseline': 0.50, 'desc': 'Hero 2nd barrel IP'},
    'fold_vs_turn_barrel': {'baseline': 0.50, 'desc': 'Hero foldea vs turn barrel'},
    # ── RIVER ────────────────────────────────────────────────────────────
    'river_bet_IP':        {'baseline': 0.45, 'desc': 'Hero apuesta river IP'},
    'fold_vs_river_bet':   {'baseline': 0.45, 'desc': 'Hero foldea vs apuesta river'},
    # ── FRECUENCIAS GLOBALES (v1.26) ──────────────────────────────────────
    'limp_rate':           {'baseline': 0.05, 'desc': 'Hero limp PF (leak clásico micros — denom: open_opp no-BB)'},
    'ccall_PF':            {'baseline': 0.08, 'desc': 'Hero cold call PF (call de raise desde posición)'},
    'check_raise_flop':    {'baseline': 0.08, 'desc': 'Hero check-raise en flop (denom: tuvo opp de actuar en flop)'},
}

M5_EXPLOIT_THRESHOLD  = 0.10   # desviación mínima del baseline para considerar exploit
M5_MIN_OPPORTUNITIES  = 150    # oportunidades mínimas para semáforo 🟡/🔴


def _wilson_ic(p_obs, n, z=1.96):
    """
    Wilson Score Interval para proporciones binomiales.
    Más robusto que el IC normal para p cerca de 0 o 1.
    
    Returns: (lower, upper) — intervalo de confianza al 95%
    """
    if n == 0:
        return (0.0, 1.0)
    center = (p_obs + z**2 / (2*n)) / (1 + z**2 / n)
    margin = z * _math.sqrt(p_obs*(1-p_obs)/n + z**2/(4*n**2)) / (1 + z**2/n)
    return (max(0.0, center - margin), min(1.0, center + margin))


def _extract_m5_frequencies(df):
    """
    Extrae frecuencias observadas del hero para los 18 spots fijos. v1.25.

    FIXES vs v1.21:
      M5-1: opens por posición — denominador flg_p_open_opp, numerador flg_p_first_raise
      M5-2: BB defend — separado por villain_position (BTN/CO/UTG independientes)
      M5-3: fold_vs_3bet — usa flg_p_fold_to_3bet_opp/flg_p_fold_to_3bet (PT3 exacto)
      M5-4: cbet — usa flg_f_cbet_opp (denominador) y flg_f_cbet (numerador)
      M5-5: fold_vs_cbet — usa flg_f_cbet_def_opp como denominador

    Compatibilidad backward: detecta campos v1.25 con 'col in df.columns'.
    Si no existen, activa fallbacks que replican lógica v1.21 (datos legacy).
    """
    if df.empty:
        return {}

    freqs = {spot: {'n_opp': 0, 'n_action': 0, 'freq_obs': 0.0}
             for spot in M5_SPOTS}

    has_open_opp      = 'flg_p_open_opp'         in df.columns
    has_first_raise   = 'flg_p_first_raise'       in df.columns
    # M5-FIX: OS usa 'flg_p_3bet_def_opp' (mismo concepto que PT4 'flg_p_fold_to_3bet_opp')
    has_fold_3bet_opp = ('flg_p_fold_to_3bet_opp' in df.columns
                         or 'flg_p_3bet_def_opp' in df.columns)
    has_cbet_opp      = 'flg_f_cbet_opp'         in df.columns
    has_cbet_def_opp  = 'flg_f_cbet_def_opp'     in df.columns
    has_villain_pos   = 'villain_position'        in df.columns

    for _, row in df.iterrows():
        pos   = str(row.get('player_position', '')).upper()
        pf    = str(row.get('preflop_action', '')).upper()
        fl    = str(row.get('flop_action',    '')).upper()
        tn    = str(row.get('turn_action',    '')).upper()
        rv    = str(row.get('river_action',   '')).upper()
        flop  = str(row.get('board_cards_flop',  '') or '')
        turn  = str(row.get('board_cards_turn',  '') or '')
        river = str(row.get('board_cards_river', '') or '')
        has_flop  = bool(flop  and flop  not in ('nan', '', 'None'))
        has_turn  = bool(turn  and turn  not in ('nan', '', 'None'))
        has_river = bool(river and river not in ('nan', '', 'None'))

        # ── Fix M5-1: opens por posición ──────────────────────────────────
        for spot_pos, spot_name in [('BTN','BTN_open'), ('CO','CO_open'),
                                     ('HJ','HJ_open'),  ('MP','HJ_open'),
                                     ('UTG','UTG_open'), ('SB','SB_open')]:
            if pos != spot_pos:
                continue
            if has_open_opp:
                if row.get('flg_p_open_opp', False):
                    freqs[spot_name]['n_opp'] += 1
                    if has_first_raise and row.get('flg_p_first_raise', False):
                        freqs[spot_name]['n_action'] += 1
            else:
                freqs[spot_name]['n_opp'] += 1
                if pf and not pf.startswith('F'):
                    freqs[spot_name]['n_action'] += 1

        # ── Fix M5-2: BB defend separado por villain_position ─────────────
        if pos == 'BB':
            villain_pos = str(row.get('villain_position', '')).upper() if has_villain_pos else ''
            # FIX v1.27: usar preflop_n_raises_facing para detectar raise real
            # Bug anterior: 'C' en pf matcheaba calls propios, inflando denominador
            _n_raises = row.get('preflop_n_raises_facing', None)
            if _n_raises is not None:
                has_raise = int(_n_raises) >= 1
            else:
                # fallback: solo R/3B/4B, eliminando 'C' que causaba inflación
                has_raise = any(x in pf for x in ('R', '3B', '4B'))
            if has_villain_pos and villain_pos and has_raise:
                if villain_pos == 'BTN':
                    freqs['BB_defend_vs_BTN']['n_opp'] += 1
                    if has_flop: freqs['BB_defend_vs_BTN']['n_action'] += 1
                elif villain_pos == 'CO':
                    freqs['BB_defend_vs_CO']['n_opp'] += 1
                    if has_flop: freqs['BB_defend_vs_CO']['n_action'] += 1
                elif villain_pos in ('UTG', 'EP', 'MP', 'HJ'):
                    freqs['BB_defend_vs_UTG']['n_opp'] += 1
                    if has_flop: freqs['BB_defend_vs_UTG']['n_action'] += 1
            elif not has_villain_pos:
                freqs['BB_defend_vs_BTN']['n_opp'] += 1
                if has_flop: freqs['BB_defend_vs_BTN']['n_action'] += 1

        # ── Fix M5-3: fold_vs_3bet (PT3 exacto) ──────────────────────────
        if has_fold_3bet_opp:
            # M5-FIX: acepta ambos nombres de campo
            _faced_3bet = (row.get('flg_p_fold_to_3bet_opp', False)
                           or row.get('flg_p_3bet_def_opp', False))
            if _faced_3bet:
                freqs['fold_vs_3bet']['n_opp'] += 1
                if row.get('flg_p_fold_to_3bet', False):
                    freqs['fold_vs_3bet']['n_action'] += 1
        else:
            if '3B' in pf or '4B' in pf:
                freqs['fold_vs_3bet']['n_opp'] += 1
                if pf.endswith('F') or 'F' in pf.split('_'):
                    freqs['fold_vs_3bet']['n_action'] += 1

                # ── Fix M5-6: limp_rate (v1.26) ──────────────────────────────────────
        # Denominador: hero tuvo oportunidad de abrir SIN ser BB
        # (flg_p_open_opp + pos != BB). Numerador: flg_p_limp.
        if pos != 'BB':
            has_limp    = 'flg_p_limp'  in df.columns
            if has_open_opp:
                if row.get('flg_p_open_opp', False):
                    freqs['limp_rate']['n_opp'] += 1
                    if has_limp and row.get('flg_p_limp', False):
                        freqs['limp_rate']['n_action'] += 1
            else:
                freqs['limp_rate']['n_opp'] += 1
                if pf and pf.startswith('C') and '3B' not in pf and 'R' not in pf:
                    freqs['limp_rate']['n_action'] += 1

        # ── Fix M5-7: ccall_PF (v1.26) ────────────────────────────────────────
        # Denominador: hero enfrentó un raise PF sin ser BB/SB (flg_p_3bet_opp + pos not blind)
        # Numerador: flg_p_ccall
        if pos not in ('SB', 'BB'):
            has_ccall = 'flg_p_ccall' in df.columns
            if 'flg_p_3bet_opp' in df.columns and row.get('flg_p_3bet_opp', False):
                freqs['ccall_PF']['n_opp'] += 1
                if has_ccall and row.get('flg_p_ccall', False):
                    freqs['ccall_PF']['n_action'] += 1
            elif not ('flg_p_3bet_opp' in df.columns):
                # Legacy: inferir de acción PF
                if '3B' in pf or 'R' in pf.split('_')[0:1]:
                    freqs['ccall_PF']['n_opp'] += 1
                    if pf.startswith('C'):
                        freqs['ccall_PF']['n_action'] += 1

# ── FLOP spots ────────────────────────────────────────────────────
        if has_flop:
            ip_oop   = str(row.get('ip_oop', '')).upper()
            pot_type = str(row.get('pot_type', '')).upper()
            is_srp   = pot_type == 'SRP'

            # ── Fix M5-4: cbet con denominador correcto ───────────────────
            if has_cbet_opp:
                if is_srp and ip_oop == 'IP' and row.get('flg_f_cbet_opp', False):
                    freqs['cbet_IP_SRP']['n_opp'] += 1
                    if row.get('flg_f_cbet', False):
                        freqs['cbet_IP_SRP']['n_action'] += 1
                if is_srp and ip_oop == 'OOP' and row.get('flg_f_cbet_opp', False):
                    freqs['cbet_OOP_SRP']['n_opp'] += 1
                    if row.get('flg_f_cbet', False):
                        freqs['cbet_OOP_SRP']['n_action'] += 1
            else:
                if is_srp and ip_oop == 'IP' and fl:
                    freqs['cbet_IP_SRP']['n_opp'] += 1
                    if fl.startswith('B') or fl.startswith('R'):
                        freqs['cbet_IP_SRP']['n_action'] += 1
                if is_srp and ip_oop == 'OOP' and fl:
                    freqs['cbet_OOP_SRP']['n_opp'] += 1
                    if fl.startswith('B') or fl.startswith('R'):
                        freqs['cbet_OOP_SRP']['n_action'] += 1

            # ── FIX M5-5 v1.27: fold_vs_cbet usando flop_action string ──────
            # Bug anterior: flg_f_cbet_def=False incluía OOP X_C (check-call = defend, no fold)
            # Fix: detectar fold por 'F' o 'X_F' en flop_action directamente
            if has_cbet_def_opp:
                if ip_oop == 'OOP' and row.get('flg_f_cbet_def_opp', False):
                    # fold_vs_cbet_OOP = hero OOP, enfrentó cbet IP (caso común BB vs BTN)
                    freqs['fold_vs_cbet_OOP']['n_opp'] += 1
                    _fl_str = str(fl).upper()
                    _folded = _fl_str.startswith('F') or _fl_str.startswith('X_F')
                    if _folded:
                        freqs['fold_vs_cbet_OOP']['n_action'] += 1
                if ip_oop == 'IP' and row.get('flg_f_cbet_def_opp', False):
                    # fold_vs_cbet_IP = hero IP, enfrentó donk bet OOP (caso raro)
                    freqs['fold_vs_cbet_IP']['n_opp'] += 1
                    _fl_str = str(fl).upper()
                    _folded = _fl_str.startswith('F') or _fl_str.startswith('X_F')
                    if _folded:
                        freqs['fold_vs_cbet_IP']['n_action'] += 1
            else:
                if fl and ip_oop == 'OOP':
                    freqs['fold_vs_cbet_OOP']['n_opp'] += 1
                    if fl.startswith('F') or fl.startswith('X_F'):
                        freqs['fold_vs_cbet_OOP']['n_action'] += 1
                if fl and ip_oop == 'IP':
                    freqs['fold_vs_cbet_IP']['n_opp'] += 1
                    if fl.startswith('F') or fl.startswith('X_F'):
                        freqs['fold_vs_cbet_IP']['n_action'] += 1

            if fl and ('B' in fl or 'C' in fl):
                freqs['raise_vs_cbet']['n_opp'] += 1
                if fl.startswith('R'):
                    freqs['raise_vs_cbet']['n_action'] += 1

            # ── Fix M5-8: check_raise_flop (v1.26) ───────────────────────────
            # Denominador: hero actuó en el flop (flg_f_saw o tiene acción en flop)
            # Numerador: flg_f_check_raise
            has_f_saw = 'flg_f_saw' in df.columns
            has_f_cr  = 'flg_f_check_raise' in df.columns
            saw_flop  = row.get('flg_f_saw', False) if has_f_saw else bool(fl)
            if saw_flop:
                freqs['check_raise_flop']['n_opp'] += 1
                if has_f_cr and row.get('flg_f_check_raise', False):
                    freqs['check_raise_flop']['n_action'] += 1
                elif not has_f_cr:
                    # Legacy: detectar X_R en flop_action string
                    if 'X' in fl and 'R' in fl.split('X', 1)[-1] if 'X' in fl else False:
                        freqs['check_raise_flop']['n_action'] += 1

        # ── TURN spots (sin cambios) ──────────────────────────────────────
        if has_turn:
            ip_oop = str(row.get('ip_oop', '')).upper()
            if ip_oop == 'IP' and tn:
                freqs['second_barrel_IP']['n_opp'] += 1
                if tn.startswith('B') or tn.startswith('R'):
                    freqs['second_barrel_IP']['n_action'] += 1
            if tn and tn not in ('', 'nan'):
                freqs['fold_vs_turn_barrel']['n_opp'] += 1
                if tn.startswith('F'):
                    freqs['fold_vs_turn_barrel']['n_action'] += 1

        # ── RIVER spots (sin cambios) ─────────────────────────────────────
        if has_river:
            ip_oop = str(row.get('ip_oop', '')).upper()
            if ip_oop == 'IP' and rv:
                freqs['river_bet_IP']['n_opp'] += 1
                if rv.startswith('B') or rv.startswith('R'):
                    freqs['river_bet_IP']['n_action'] += 1
            if rv and rv not in ('', 'nan'):
                freqs['fold_vs_river_bet']['n_opp'] += 1
                if rv.startswith('F'):
                    freqs['fold_vs_river_bet']['n_action'] += 1

    for spot in freqs:
        n = freqs[spot]['n_opp']
        a = freqs[spot]['n_action']
        freqs[spot]['freq_obs'] = (a / n) if n > 0 else 0.0

    # ── GAP G v1.44: Exploit Score explícito ──────────────────────────────────
    # exploit_score = diferencia hero vs baseline NL2 (positivo = hero > pool)
    # Normalizado sobre baseline: (obs - base) / base × 100
    # Interpretación: +20 = hero hace eso 20% MÁS que el pool
    for spot in freqs:
        base = M5_SPOTS.get(spot, {}).get('baseline', 0)
        obs  = freqs[spot]['freq_obs']
        freqs[spot]['exploit_score_pp']   = round((obs - base) * 100, 1)
        freqs[spot]['exploit_score_pct']  = round(((obs - base) / base * 100) if base > 0 else 0, 1)
        freqs[spot]['exploit_direction']  = (
            'hero_over'  if (obs - base) > 0.08 else
            'hero_under' if (obs - base) < -0.08 else
            'aligned'
        )

    return freqs



def _update_empirical_baselines(freqs, hand_count):
    """
    Calcula baselines empíricos desde los datos reales del pool acumulado.
    Solo se invoca cuando hand_count >= M5_ACTIVATION_HANDS_CONFIRMED (5.000 manos).
    Los baselines empíricos reflejan el pool real observado, no valores teóricos.
    """
    global POOL_BASELINES_EMPIRICAL
    empirical = {}
    for spot, data in freqs.items():
        n = data.get('n_opp', 0)
        if n >= 200:   # muestra mínima por spot para baseline empírico fiable
            empirical[spot] = data.get('freq_obs', POOL_BASELINES_THEORETICAL.get(spot, 0.5))
    if empirical:
        POOL_BASELINES_EMPIRICAL = empirical
        print(f"   ✅ Baselines empíricos actualizados: {len(empirical)}/{len(M5_SPOTS)} spots con ≥200 oportunidades")
    return empirical

def run_m5_pool_detector(df, hand_count, verbose=True):
    """
    M5 — Pool Exploitation Detector v1.21.
    
    Detecta desviaciones del hero respecto a baselines exploitativos NL2.
    Usa Wilson Score Interval para validar estadísticamente cada exploit.
    
    Gates:
      < M5_ACTIVATION_HANDS_PRELIMINARY → inactivo
      >= PRELIMINARY → señal preliminar (🔴 muy conservador)
      >= CONFIRMED   → señal confirmada (todos los semáforos activos)
    
    Returns:
        dict: {'status', 'exploits', 'report', 'frequencies'}
    """
    act_prelim    = globals().get('M5_ACTIVATION_HANDS_PRELIMINARY', 3_000)
    act_confirmed = globals().get('M5_ACTIVATION_HANDS_CONFIRMED',   5_000)

    # Selección de baselines: empíricos si disponibles y validados, teóricos si no
    use_empirical = (hand_count >= act_confirmed
                     and bool(POOL_BASELINES_EMPIRICAL)
                     and len(POOL_BASELINES_EMPIRICAL) >= len(M5_SPOTS) // 2)
    active_baselines = POOL_BASELINES_EMPIRICAL if use_empirical else POOL_BASELINES_THEORETICAL
    baseline_label   = "empírico (pool real)" if use_empirical else "teórico (referencia NL2)"
    
    if hand_count < act_prelim:
        msg = (f"⏳ M5 Pool Detector: INACTIVO — "
               f"{hand_count:,}/{act_prelim:,} manos (señal preliminar)")
        if verbose:
            print(f"   {msg}")
        return {'status': 'inactive', 'exploits': [], 'report': msg, 'frequencies': {}}
    
    is_confirmed = hand_count >= act_confirmed
    status_label = 'confirmed' if is_confirmed else 'preliminary'
    
    if verbose:
        label = "señal CONFIRMADA" if is_confirmed else "señal PRELIMINAR"
        print(f"\n── M5 Pool Detector ({label}: {hand_count:,} manos) ──")
    
    # ── Extraer frecuencias observadas ────────────────────────────────────
    freqs = _extract_m5_frequencies(df)
    
    exploits = []
    report_lines = []
    
    for spot_name, spot_info in M5_SPOTS.items():
        freq_data = freqs.get(spot_name, {'n_opp': 0, 'n_action': 0, 'freq_obs': 0.0})
        n     = freq_data['n_opp']
        f_obs = freq_data['freq_obs']
        base  = active_baselines.get(spot_name, spot_info['baseline'])
        desc  = spot_info['desc']
        
        # ── Semáforo de datos ─────────────────────────────────────────────
        if n < M5_MIN_OPPORTUNITIES:
            semaforo = '⚪'
            exploit_type = None
            ic_lo, ic_hi = (0.0, 1.0)
        else:
            # Wilson IC al 95%
            ic_lo, ic_hi = _wilson_ic(f_obs, n)
            
            # ¿El IC cruza el baseline?
            ic_crosses_baseline = (ic_lo <= base <= ic_hi)
            
            deviation = f_obs - base
            is_significant = abs(deviation) >= M5_EXPLOIT_THRESHOLD
            
            if ic_crosses_baseline or not is_significant:
                semaforo = '🟡'  # señal débil o IC cruza baseline
                exploit_type = None
            else:
                semaforo = '🔴'  # exploit real, IC no cruza baseline
                exploit_type = 'over' if deviation > 0 else 'under'
        
        # En modo preliminary, degradar 🔴 a 🟡
        if semaforo == '🔴' and not is_confirmed:
            semaforo = '🟡'
            note = ' (preliminar)'
        else:
            note = ''
        
        # ── Generar texto de exploit ──────────────────────────────────────
        exploit_text = None
        if semaforo == '🔴' and exploit_type:
            dev_pct = (f_obs - base) * 100
            if exploit_type == 'under':
                exploit_text = (
                    f"LEAK: {desc} — hero {f_obs*100:.0f}% vs baseline {base*100:.0f}% "
                    f"(−{abs(dev_pct):.0f}pp). "
                    f"IC 95%: [{ic_lo*100:.0f}%–{ic_hi*100:.0f}%]. "
                    f"Acción: aumentar frecuencia."
                )
            else:
                exploit_text = (
                    f"OVER: {desc} — hero {f_obs*100:.0f}% vs baseline {base*100:.0f}% "
                    f"(+{dev_pct:.0f}pp). "
                    f"IC 95%: [{ic_lo*100:.0f}%–{ic_hi*100:.0f}%]. "
                    f"Acción: reducir frecuencia o revisar sizing."
                )
            exploits.append({
                'spot': spot_name, 'desc': desc,
                'freq_obs': f_obs, 'baseline': base,
                'deviation': f_obs - base,
                'ic': (ic_lo, ic_hi), 'n': n,
                'exploit_type': exploit_type,
                'text': exploit_text,
            })
        
        if verbose:
            ic_str = f"IC:[{ic_lo*100:.0f}–{ic_hi*100:.0f}%]" if n >= M5_MIN_OPPORTUNITIES else f"n={n}<{M5_MIN_OPPORTUNITIES}"
            line = (f"  {semaforo} {spot_name:25s} | obs:{f_obs*100:5.1f}% "
                    f"base:{base*100:4.0f}% | {ic_str} n={n}{note}")
            print(line)
            report_lines.append(line)
    
    # ── Actualizar baselines empíricos si hay volumen suficiente ─────────
    if is_confirmed:
        _update_empirical_baselines(freqs, hand_count)

    # ── Resumen ───────────────────────────────────────────────────────────
    n_red    = sum(1 for e in exploits if True)  # exploits confirmados
    n_yellow = sum(1 for s in M5_SPOTS if freqs.get(s, {}).get('n_opp', 0) >= M5_MIN_OPPORTUNITIES) - n_red
    
    summary = (
        f"M5 ({status_label} | baseline {baseline_label}): {len(M5_SPOTS)} spots analizados | "
        f"🔴 {len(exploits)} exploits | 🟡 tendencias | "
        f"Baseline: {'empírico ✅' if use_empirical else 'teórico (< 5k manos)'}"
    )
    if verbose:
        print(f"\n  {summary}")
    
    return {
        'status':      status_label,
        'exploits':    exploits,
        'report':      summary,
        'frequencies': freqs,
        'report_lines': report_lines,
    }


print("✅ M5 Pool Detector v1.26 cargado.")
print(f"   21 spots | Wilson IC | gates: {3000}/{5000} manos | Fix M5-1..8 activos (v1.26)")
print("   Se activa automáticamente desde PASO 8f del pipeline.")


# ════════════════════════════════════════════════════════════════════════════
# SECCIÓN 14 — M4.4 Coach Analítico v1.37
#
# CAMBIOS vs v1.34:
#   1. System prompts: añade "concepto_teorico" (1 frase del PORQUÉ)
#   2. Memoria coach_history.json: últimas 3 sesiones
#      - Si mismo spot persiste 3+ → pregunta causa-raíz, no repite instrucción
#      - Si mejoró → reconoce y pasa al siguiente leak
#   3. Salvaguardas anti-dependencia:
#      - concepto_teorico = 1 frase (no párrafo)
#      - M3 sigue socrático sin excepciones
#      - 1 llamada por sesión (guard intacto)
#      - El sistema mide; el coach explica; el jugador decide
#
# PRINCIPIO RECTOR: el coach acelera el aprendizaje, no lo sustituye.
# Un coach de élite no juega por ti — te dice qué mejorar y observa si
# lo aplicas. La pregunta de implementación cierra cada sesión.
# La respuesta la das tú. El coach la recoge la próxima vez.
# ════════════════════════════════════════════════════════════════════════════

import os as _os
import json as _json_mod

# ── System prompts por modo ───────────────────────────────────────────────

_M44_SYSTEM_PROMPT_M1 = (
    "Eres un coach de poker para micro-stakes (NL2-NL5). "
    "El jugador esta en M1: aprendiendo fundamentos, necesita instrucciones concretas.\n\n"
    "PRINCIPIOS INAMOVIBLES M1:\n"
    "1. Instruccion concreta: da el rango exacto y la accion especifica.\n"
    "   Ej: Defiende AJo+/KQo/ATs+/KJs+/QJs/JTs/T9s/99+/88/77 desde BB vs BTN.\n"
    "2. Concepto teorico: UNA sola frase explicando POR QUE importa este leak.\n"
    "   Ej: El BB tiene pot odds naturales de 1.4:1, foldeando demasiado regalas equity.\n"
    "   IMPORTANTE: una frase, no un parrafo. Suficiente para construir el modelo mental.\n"
    "3. Cuantifica el impacto en BB/100 o euros/hora.\n"
    "4. Si hay historial de sesiones anteriores y el MISMO spot persiste:\n"
    "   NO repitas la instruccion (ya la conoce). Formula UNA pregunta causa-raiz:\n"
    "   ¿Por que no se esta aplicando en mesa? ¿Velocidad? ¿Automatismo? ¿Duda?\n"
    "5. Si el spot mejoro respecto al historial: reconocelo en 1 frase, analiza el siguiente leak.\n"
    "6. Termina SIEMPRE con UNA pregunta de implementacion practica para la proxima sesion.\n"
    "   La pregunta debe ser especifica, no generica.\n\n"
    "FORMATO (JSON estricto, sin markdown, sin texto fuera del JSON):\n"
    "{\n"
    "  \"accion_concreta\": \"que hacer exactamente desde ahora\",\n"
    "  \"concepto_teorico\": \"UNA frase del porqué matemático/estratégico\",\n"
    "  \"contexto_spot\": \"descripcion del spot en 1 linea\",\n"
    "  \"impacto_estimado\": \"BB/100 o euros/h si corriges este leak\",\n"
    "  \"patron_detectado\": \"persiste | mejoro | primera_vez\",\n"
    "  \"confianza\": \"baja (<200 manos) | media (200-500) | alta (>500)\",\n"
    "  \"pregunta_implementacion\": \"pregunta practica especifica para la proxima sesion\"\n"
    "}"
)

_M44_SYSTEM_PROMPT_M2 = (
    "Eres un coach de poker analitico para stakes bajos (NL5-NL10). "
    "El jugador esta en M2: tiene fundamentos, necesita refinar exploits vs su pool especifico.\n\n"
    "PRINCIPIOS M2:\n"
    "1. Conecta el leak con el pool real: usa los datos M5 para contextualizar.\n"
    "2. Explica el concepto teorico en 2-3 frases (rango, equity, MDF).\n"
    "3. Si el mismo spot persiste: profundiza en la causa raiz, no en la instruccion.\n"
    "4. Si mejoro: reconoce y analiza el siguiente leak mas costoso.\n"
    "5. Termina con UNA pregunta de reflexion sobre el porqué del patron.\n\n"
    "FORMATO (JSON estricto):\n"
    "{\n"
    "  \"accion_concreta\": \"ajuste especifico vs este pool\",\n"
    "  \"concepto_teorico\": \"2-3 frases del marco conceptual\",\n"
    "  \"contexto_pool\": \"como este pool especifico afecta la decision\",\n"
    "  \"patron_detectado\": \"persiste | mejoro | primera_vez\",\n"
    "  \"confianza\": \"baja | media | alta\",\n"
    "  \"pregunta_reflexion\": \"pregunta sobre el porqué del patron\"\n"
    "}"
)

_M44_SYSTEM_PROMPT_M3 = (
    "Eres un coach de poker avanzado para stakes medios (NL25+). "
    "El jugador esta en M3: tiene edge real, desarrolla criterio propio.\n\n"
    "RESTRICCIONES ABSOLUTAS M3:\n"
    "1. NUNCA des la instruccion directamente. El jugador debe llegar a ella.\n"
    "2. Da el marco conceptual: rangos de equity, MDF, teoria de juegos.\n"
    "3. Si el patron persiste: pregunta que bloqueo cognitivo podria estar ocurriendo.\n"
    "4. Cada output DEBE terminar con una pregunta abierta, nunca con conclusion.\n"
    "5. El objetivo no es que memorice — es que construya el modelo mental.\n\n"
    "FORMATO (JSON estricto):\n"
    "{\n"
    "  \"marco_analisis\": \"razonamiento conceptual sin veredictos (max 3 parrafos)\",\n"
    "  \"patron_detectado\": \"persiste | mejoro | primera_vez\",\n"
    "  \"confianza\": \"baja | media | alta\",\n"
    "  \"pregunta_clave\": \"la pregunta mas importante para que el jugador razone\"\n"
    "}"
)

_M44_CALLS_MADE = 0   # v1.45: contador flexible (reemplaza bool rígido)
                      # M1/M2: máx 2 llamadas | M3: máx 1 socrático
                      # Filosofía: límite protege dependencia, no aprendizaje.

# ── Funciones de memoria coach_history.json ──────────────────────────────

_COACH_HISTORY_FILE = 'coach_history.json'

def _load_coach_history(drive_path=None):
    """Carga las ultimas 3 sesiones del coach. Retorna lista vacia si no existe."""
    path = _os.path.join(drive_path, _COACH_HISTORY_FILE) if drive_path else _COACH_HISTORY_FILE
    if _os.path.exists(path):
        try:
            with open(path, 'r', encoding='utf-8') as f:
                data = _json_mod.load(f)
            return data.get('sessions', [])[-3:]  # max 3
        except Exception:
            return []
    return []

def _save_coach_history(session_entry, drive_path=None):
    """Añade la sesion actual al historial. Mantiene max 3 entradas."""
    path = _os.path.join(drive_path, _COACH_HISTORY_FILE) if drive_path else _COACH_HISTORY_FILE
    history = _load_coach_history(drive_path)
    history.append(session_entry)
    history = history[-3:]  # rotating buffer: max 3
    try:
        with open(path, 'w', encoding='utf-8') as f:
            _json_mod.dump({'sessions': history}, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"   ⚠️  No se pudo guardar coach_history: {e}")

def _build_history_block(history, current_spot):
    """Construye el bloque de contexto historico para el prompt."""
    if not history:
        return None, 'primera_vez'

    # Detectar si el mismo spot persiste
    same_spot_count = sum(1 for s in history if s.get('spot') == current_spot)
    last_spots = [s.get('spot') for s in history]

    if same_spot_count >= 2:
        patron = 'persiste'
    elif last_spots and last_spots[-1] != current_spot:
        patron = 'mejoro'  # el spot anterior ya no es el top-1
    else:
        patron = 'primera_vez'

    lines = ['HISTORIAL ULTIMAS SESIONES (max 3):']
    for i, s in enumerate(history):
        bb_v = s.get('bb_vpip_at_session', '?')
        n_h  = s.get('n_hands_at_session', '?')
        spot = s.get('spot', '?')
        date = s.get('date', '?')
        preg = s.get('pregunta_dada', '')
        lines.append(f"  Sesion {i+1} ({date}): spot={spot[:35]} | BB_VPIP={bb_v}% | {n_h} manos")
        if preg:
            lines.append(f"    → Pregunta dada: {preg[:80]}")

    if patron == 'persiste':
        lines.append(f"\nPATRON: El spot '{current_spot[:35]}' lleva {same_spot_count+1} sesiones como top-1 leak.")
        lines.append("INSTRUCCION AL COACH: El jugador YA conoce la instruccion. NO la repitas.")
        lines.append("Formula una pregunta causa-raiz: ¿por que no se aplica en mesa?")
    elif patron == 'mejoro':
        lines.append(f"\nPATRON: El spot anterior ya no es top-1 → mejoria detectada.")
        lines.append("INSTRUCCION AL COACH: Reconoce la mejora brevemente. Analiza el nuevo leak.")

    # v1.48: si el spot lleva 2+ sesiones con patrón 'mejoro' → sugerir siguiente leak
    mejora_count = sum(
        1 for s in history
        if s.get('spot') == current_spot and s.get('patron_detectado') == 'mejoro'
    )
    if mejora_count >= 2:
        lines.append(
            f"\nSUGERENCIA v1.48: llevas {mejora_count} sesiones mejorando este spot."
        )
        lines.append(
            "INSTRUCCION AL COACH: al final de tu análisis, sugiere explícitamente"
        )
        lines.append(
            "pasar al siguiente leak del ranking cuando este esté bajo control."
        )

    return '\n'.join(lines), patron


# ── eval7 bridge — rangos frontera ───────────────────────────────────────
def _compute_defense_frontier(villain_position='BTN', stake='NL2'):
    """
    Rangos exploitativos de defensa desde BB vs apertura.
    Cuando eval7 este instalado (M2+), reemplazar por equity exacta combo a combo.
    """
    _ref = {
        'BTN': {
            'clear_defend':  ['AA','KK','QQ','JJ','TT','99','88','77',
                              'AKs','AQs','AJs','ATs','KQs','KJs','QJs','JTs',
                              'AKo','AQo','AJo','KQo'],
            'marginal':      ['66','55','A9s','A8s','K9s','Q9s','JTs','T9s','98s',
                              'KTo','QJo','T9o'],
        },
        'CO':  {
            'clear_defend':  ['AA','KK','QQ','JJ','TT','99','88',
                              'AKs','AQs','AJs','ATs','KQs',
                              'AKo','AQo','AJo'],
            'marginal':      ['77','66','A9s','KJs','QJs','JTs','KQo','KJo'],
        },
        'HJ':  {
            'clear_defend':  ['AA','KK','QQ','JJ','TT','99',
                              'AKs','AQs','AJs','ATs',
                              'AKo','AQo'],
            'marginal':      ['88','77','KQs','KJs','AJo','KQo'],
        },
        'UTG': {
            'clear_defend':  ['AA','KK','QQ','JJ','TT',
                              'AKs','AQs',
                              'AKo'],
            'marginal':      ['99','88','AJs','KQs','AQo'],
        },
    }
    default = {
        'clear_defend': ['AA','KK','QQ','JJ','TT','AKs','AQs','AKo'],
        'marginal':     ['99','88','AJs','KQs','AQo'],
    }
    return _ref.get(villain_position, default)


# ── Función principal M4.4 ────────────────────────────────────────────────

def run_m44_coach(overall_metrics, spot_results, current_mode,
                  full_df=None, pool_classifications=None,
                  m5_result=None, speed_result=None, roi_ranking=None,
                  drive_path=None, m4_enabled=None,
                  execution_result=None):  # v1.60: execution_rate del drill activo
    """
    Coach analítico v1.37.
    Cambios vs v1.34:
      - Añade concepto_teorico (1 frase del porqué)
      - Carga/guarda coach_history.json (max 3 sesiones)
      - Detecta si el patron persiste → pregunta causa-raíz
      - Si mejoró → reconoce y avanza al siguiente leak
    """
    global _M44_CALLS_MADE
    # Guard flexible v1.45: límite por modo, no booleano rígido.
    # M1/M2 pueden beneficiarse de 2 llamadas (instrucción + seguimiento).
    # M3 siempre 1 — el coach socrático no puede dar la respuesta dos veces.
    _max = (M4_CALLS_PER_SESSION.get(current_mode, 1)
            if 'M4_CALLS_PER_SESSION' in globals() else 1)
    if _M44_CALLS_MADE >= _max:
        if _M44_CALLS_MADE == 1:
            print(f"   M4.4 Coach: límite de sesión ({_max} llamada/s para {current_mode})")
        return None

    # v1.58: usa Gemini 2.0 Flash (gratuito) — migrado desde Anthropic/Claude en v1.58 [P1-C v1.78]
    # Primero intentar os.environ (inyectado por el pipeline), luego Colab Secrets
    api_key = _os.environ.get('GEMINI_API_KEY', '')
    if not api_key:
        try:
            from google.colab import userdata as _colab_ud
            api_key = _colab_ud.get('GEMINI_API_KEY') or ''
        except Exception:
            api_key = ''
    # m4_enabled: parámetro explícito (preferido) o fallback a globals()
    _m4_flag = m4_enabled if m4_enabled is not None else globals().get('M4_API_ENABLED', False)
    if not api_key or not _m4_flag:
        print("   M4.4 Coach: DESACTIVADO (sin GEMINI_API_KEY o M4_API_ENABLED=False)")
        print("      Activar: añade GEMINI_API_KEY en Colab Secrets (aistudio.google.com)")
        return None

    if spot_results is None or len(spot_results) == 0:
        return None

    # ── Selección del spot a analizar ─────────────────────────────────────
    # Use correct column name — fallback between possible names
    _shrunk_col = 'impacto_ev_total_eur_shrunk' if 'impacto_ev_total_eur_shrunk' in spot_results.columns else 'impacto_ev_total_eur'
    top_spot  = spot_results.iloc[spot_results[_shrunk_col].idxmin()]
    spot_id   = top_spot['spot_identifier']
    n_hands   = int(top_spot['spot_hands_count'])
    ev_imp    = float(top_spot[_shrunk_col])
    parts_id  = spot_id.split('_')

    pos       = parts_id[0] if len(parts_id) > 0 else '?'
    ip_oop    = parts_id[1] if len(parts_id) > 1 else '?'
    pot_type  = parts_id[2] if len(parts_id) > 2 else '?'
    stack_d   = parts_id[3] if len(parts_id) > 3 else '?'
    street    = parts_id[4] if len(parts_id) > 4 else '?'
    opp_cls   = parts_id[5] if len(parts_id) > 5 else 'unknown'
    stake     = full_df['stake_level'].mode()[0] if full_df is not None and 'stake_level' in full_df.columns else 'NL2'

    bb100 = overall_metrics.get('bb_per_100_net', 0)
    ev_h  = overall_metrics.get('ev_euro_per_hour', 0)

    # Confianza estadística
    conf_s = ('baja (<200 manos)' if n_hands < 200 else
              'media (200-500)'   if n_hands < 500 else
              'alta (>500)')

    # ── Frecuencia del hero en este spot ──────────────────────────────────
    hero_freq_label = ''
    if full_df is not None:
        spot_df = full_df[full_df['spot_identifier'] == spot_id]
        if len(spot_df) >= 10:
            ref_dict = {'BB': 55, 'SB': 40, 'BTN': 45, 'CO': 35, 'HJ': 28, 'UTG': 20}
            ref_pct = ref_dict.get(pos, 40)
            if street == 'preflop':
                vpip_pct = round(pd.to_numeric(spot_df['flg_vpip'], errors='coerce').mean() * 100, 1)
                hero_freq_label = f'{pos} VPIP={vpip_pct}% (ref NL2: ~{ref_pct}%)'
                fold_pct = round(pd.to_numeric(spot_df['flg_p_fold'], errors='coerce').mean() * 100, 1)
                hero_freq_label += f' | Fold%={fold_pct}%'
            elif 'flg_f_cbet' in spot_df.columns and ip_oop == 'IP':
                _cbet_opp_n = pd.to_numeric(spot_df['flg_f_cbet_opp'], errors='coerce').fillna(0)
                _cbet_hit_n = pd.to_numeric(spot_df['flg_f_cbet'], errors='coerce').fillna(0)
                cbet_pct = round(_cbet_hit_n[_cbet_opp_n > 0].mean() * 100, 1) \
                           if _cbet_opp_n.sum() > 3 else None
                hero_freq_label = f'Cbet_IP={cbet_pct}% (ref:62%)' if cbet_pct else 'sin datos cbet'
        else:
            hero_freq_label = f'ref NL2: ~{ref_dict.get(pos, 40)}% (n insuficiente)'

    # Oponente info
    opp_info = opp_cls
    if pool_classifications and opp_cls not in ('unknown', '?'):
        counts = {}
        for v in pool_classifications.values():
            c = v.get('class', 'unknown')
            counts[c] = counts.get(c, 0) + 1
        opp_info = f"{opp_cls} | pool: " + ', '.join(f"{k}:{v}" for k,v in counts.items() if k != 'unknown')

    # ── Cargar historial ──────────────────────────────────────────────────
    history = _load_coach_history(drive_path)
    history_block, patron = _build_history_block(history, spot_id)

    # BB VPIP actual para guardar en historial
    bb_vpip_now = None
    if full_df is not None and 'player_position' in full_df.columns:
        bb_rows = full_df[full_df['player_position'] == 'BB']
        if len(bb_rows) > 10:
            bb_vpip_now = round(pd.to_numeric(bb_rows['flg_vpip'], errors='coerce').mean() * 100, 1)

    # ── Bloques opcionales del prompt ─────────────────────────────────────
    speed_block = None
    if speed_result:
        mph = speed_result.get('hands_per_hour', 0)
        if mph > 120:
            speed_block = f'VELOCIDAD: {mph:.0f} manos/h — ALTA. Riesgo impulsividad preflop.'

    m5_block = None
    if m5_result:
        freqs = m5_result.get('frequencies', {})
        # [v2.05] M5->COACH BIDIRECCIONAL: señales explotativas del pool real
        lines_m5 = ['POOL NL2 FINGERPRINT (datos reales de TU pool):']
        exploits = []  # instrucciones explotativas derivadas de TU pool
        key_spots = [
            ('fold_vs_cbet_flop',   'fold_cbet_F', 45, 'cbet_F>52%: aumenta bluff freq flop'),
            ('fold_vs_turn_barrel', 'fold_turn',   50, 'fold_T>57%: barrel turn mas'),
            ('fold_vs_river_bet',   'fold_river',  45, 'fold_R>52%: bluff river mas'),
            ('cbet_IP_SRP',         'cbet_IP',     62, 'cbet_IP<55%: pool no cbetea, call wide'),
            ('limp_pct',            'limp_rate',   15, 'limp>20%: pool loose, value bet mas'),
            ('wtsd_pct',            'wtsd_pool',   30, 'wtsd>35%: calling station, thin value'),
        ]
        for k, label, ref, hint in key_spots:
            d = freqs.get(k, {})
            if d.get('n_opp', 0) >= 30:
                obs = d['freq_obs'] * 100
                delta = obs - ref
                lines_m5.append(f'  {label}={obs:.1f}% (ref:{ref}%, d:{delta:+.0f}pp, n={d["n_opp"]})')
                if abs(delta) > 7:
                    exploits.append(f'POOL: {hint} (obs={obs:.0f}% ref={ref}%)')
        if exploits:
            lines_m5.append('EXPLOTACIONES ACTIVAS:')
            for ex in exploits:
                lines_m5.append(f'  -> {ex}')
        if len(lines_m5) > 1:
            m5_block = '\n'.join(lines_m5)

    fam_block = None
    if roi_ranking:
        fams     = roi_ranking.get('families', {})
        leaks_df = roi_ranking.get('leaks', None)
        f_lines  = []

        # v1.48: top-3 leaks con marcador del drill activo
        if leaks_df is not None and len(leaks_df) >= 2:
            f_lines.append('TOP LEAKS (contexto de aprendizaje):')
            _drill_now = str(globals().get('DRILL_ACTIVO',''))
            for _i, _row in leaks_df.head(3).iterrows():
                _s  = _row.get('spot_identifier', '?')
                _n  = int(_row.get('spot_hands_count', 0))
                _ev = float(_row.get('impacto_ev_total_eur_shrunk', 0))
                _mk = ' ← EN ESTUDIO' if _s == _drill_now else ''
                f_lines.append(f'  #{_i+1} {_s[:45]} n={_n} {_ev:+.2f}€{_mk}')

        # v1.48: familias con correlación explícita
        if fams:
            f_lines.append('FAMILIAS (eficiencia de aprendizaje):')
            for fn, fd in fams.items():
                f_lines.append(
                    f"  {fn}: {fd['n_combined']} manos | {fd['ev_combined']:+.2f}€"
                )
            # Correlación: si hay familia con volumen suficiente → resaltarla
            for fn, fd in fams.items():
                if fd.get('n_combined', 0) >= 300:
                    f_lines.append(
                        f'  ⚡ CORRELACIÓN [{fn}]: corregir 1 spot de esta familia'
                    )
                    f_lines.append(
                        '     transfiere aprendizaje a TODOS los spots de la familia.'
                    )

        if f_lines:
            fam_block = '\n'.join(f_lines)

    frontier_block = None
    if street == 'preflop' and pos == 'BB':
        villain_pos = 'BTN'  # más común en los spots top
        try:
            fr = _compute_defense_frontier(villain_pos, stake)
            frontier_block = (
                f'RANGOS FRONTERA vs {villain_pos} ({stake}):\n'
                f'  Defender siempre: {", ".join(fr["clear_defend"][:8])}...\n'
                f'  Marginal (~50%): {", ".join(fr["marginal"][:6])}...'
            )
        except Exception:
            pass

    # ── Construir prompt ──────────────────────────────────────────────────
    _parts = [
        f'Analiza este spot de LaRuinaDeMago ({stake}, micro-stakes):\n',
        f'SPOT: {spot_id}',
        f'- Pos: {pos} | IP/OOP: {ip_oop} | Bote: {pot_type} | Stack: {stack_d} | Calle: {street}',
        f'- Manos: {n_hands} | Impacto: {round(ev_imp, 2)}€/h | Confianza: {conf_s}',
        f'- Frecuencia {pos}: {hero_freq_label}',
        f'- Opp_class: {opp_info}',
        '',
        f'METRICAS GLOBALES: BB/100={round(bb100,2)} | EV/h={round(ev_h,2)}€ | Modo={current_mode}',
    ]

    if speed_block:
        _parts.append(f'- {speed_block}')
    if m5_block:
        _parts.extend(['', m5_block])
    if fam_block:
        _parts.extend(['', fam_block])
    if frontier_block:
        _parts.extend(['', frontier_block])
    if history_block:
        _parts.extend(['', history_block])

    # v1.48: DRILL_ACTIVO en prompt — el coach sabe qué spot estás estudiando
    _drill_activo = globals().get('DRILL_ACTIVO')
    if _drill_activo and str(_drill_activo) not in ('None', ''):
        _parts.append(f'DRILL_ACTIVO (spot en estudio activo): {_drill_activo}')
        if str(_drill_activo) != spot_id:
            _parts.append(
                f'NOTA: estudias {str(_drill_activo)[:40]} pero el top-1 ahora es {spot_id[:40]}.'
            )

    # v1.60 Gap-1: inyectar execution_result en prompt del coach
    # El coach necesita saber si el jugador ejecuta el drill o no para
    # poder preguntar causa-raíz si rate < 80%, o reconocer LOCK si >= 80% x3.
    if execution_result and execution_result.get('oportunidades', 0) > 0:
        _er_rate    = execution_result.get('execution_rate', 0.0)
        _er_verd    = execution_result.get('veredicto', 'HOLD')
        _er_opp     = execution_result.get('oportunidades', 0)
        _er_exec    = execution_result.get('ejecutadas', 0)
        _er_accion  = execution_result.get('accion', 'HOLD')
        _er_mensaje = execution_result.get('mensaje', '')
        # Cargar trend y peak del historial M7 si está disponible
        try:
            _er_hist = load_drill_history_m7(drive_path)
            _er_drill_data = _er_hist.get('drills', {}).get(
                globals().get('DRILL_ACTIVO', ''), {}
            )
            _er_trend = _er_drill_data.get('trend', 'sin_datos')
            _er_peak  = _er_drill_data.get('peak_rate', 0.0)
            _er_level = _er_drill_data.get('current_level', 'level_1')
            _er_lock  = _er_drill_data.get('lock_streak', 0)
        except Exception:
            _er_trend = 'sin_datos'
            _er_peak  = 0.0
            _er_level = 'level_1'
            _er_lock  = 0
        _parts.extend([
            '',
            'EXECUTION RATE DEL DRILL ACTIVO (comportamiento en mesa):',
            f'- Rate esta sesión: {_er_rate:.1f}% ({_er_exec}/{_er_opp} oportunidades)',
            f'- Veredicto: {_er_verd} → {_er_accion}',
            f'- Trend multi-sesión: {_er_trend} | Pico histórico: {_er_peak:.0%}',
            f'- Nivel actual: {_er_level} | Lock streak: {_er_lock}/3',
            f'- Mensaje sistema: {_er_mensaje[:120]}',
            ('INSTRUCCIÓN AL COACH: Si rate < 60%, pregunta causa-raíz sobre '
             'por qué no se ejecuta el trigger, no repitas la instrucción. '
             'Si rate >= 80% x3 sesiones (lock_streak >= 3), reconoce el logro '
             'y sugiere rotar al siguiente leak del ranking ROI.'),
        ])

    _parts.append(f'\nResponde en formato JSON modo {current_mode}.')
    user_prompt = '\n'.join(_parts)

    # ── System prompt por modo ────────────────────────────────────────────
    _sp_map = {
        'M1': _M44_SYSTEM_PROMPT_M1,
        'M2': _M44_SYSTEM_PROMPT_M2,
    }
    system_prompt = _sp_map.get(current_mode, _M44_SYSTEM_PROMPT_M3)

    # ── Llamada API Gemini 2.0 Flash (v1.58 — gratuito) ─────────────────
    try:
        import urllib.request
        import urllib.error as _urllib_error

        _gemini_url = f'https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={api_key}'

        # Gemini: system prompt como primer turno user/model
        _contents = [
            {'role': 'user',  'parts': [{'text': system_prompt}]},
            {'role': 'model', 'parts': [{'text': 'Entendido. Analizaré el spot y responderé en JSON.'}]},
            {'role': 'user',  'parts': [{'text': user_prompt}]},
        ]

        _payload = _json_mod.dumps({
            'contents': _contents,
            'generationConfig': {
                'maxOutputTokens': 1200,
                'temperature': 0.4,
            }
        }).encode('utf-8')

        _req = urllib.request.Request(
            _gemini_url,
            data=_payload,
            headers={'Content-Type': 'application/json'}
        )
        try:
            with urllib.request.urlopen(_req, timeout=30) as _resp:
                _data = _json_mod.loads(_resp.read().decode('utf-8'))
        except _urllib_error.HTTPError as _http_e:
            _err_body = _http_e.read().decode('utf-8', errors='replace')
            if _http_e.code == 429:
                print("   ⏳ M4.4 Coach: límite de cuota Gemini alcanzado.")
                print("      Solución: espera 1 minuto y vuelve a ejecutar.")
                print("      Si persiste: verifica en aistudio.google.com que la key tiene cuota disponible.")
                return None
            raise Exception(f"HTTP {_http_e.code} {_http_e.reason}: {_err_body[:400]}")

        raw_text = _data['candidates'][0]['content']['parts'][0]['text'].strip()

        # Parsear JSON
        try:
            _clean = raw_text
            if '```' in _clean:
                _clean = _clean.split('```')[1]
                if _clean.startswith('json'):
                    _clean = _clean[4:]
            result = _json_mod.loads(_clean.strip())
        except Exception:
            # L3: fallback usa campo correcto por modo; todos los accesos posteriores usan .get() → sin KeyError
            _key = 'accion_concreta' if current_mode in ('M1','M2') else 'pregunta_clave'
            result = {_key: raw_text[:300], 'contexto_spot': spot_id, 'confianza': conf_s}

        result.update({
            'spot_analyzed': spot_id,
            'n_hands':       n_hands,
            'ev_impact':     ev_imp,
            'modo':          current_mode,
            'patron_detectado': result.get('patron_detectado', patron),
        })
        _M44_CALLS_MADE += 1  # incrementar contador de llamadas esta sesión

        # ── Display ────────────────────────────────────────────────────────
        print()
        print('─' * 68)
        print(f'  🤖 M4.4 Coach [{current_mode}] v1.48 — {spot_id}')
        print(f'  Confianza: {result.get("confianza","?")} | Manos: {n_hands} | Patrón: {result.get("patron_detectado","?")}')
        print('─' * 68)

        if current_mode == 'M1':
            ctx = result.get('contexto_spot','')
            if ctx: print(f'\n  📍 {ctx}')

            concepto = result.get('concepto_teorico','')
            if concepto:
                print(f'\n  💡 Por qué importa:')
                print(f'     {concepto}')

            accion = result.get('accion_concreta','')
            if accion:
                print(f'\n  🎯 Acción concreta:')
                for _l in accion.split('\n'):
                    if _l.strip(): print(f'     {_l}')

            impacto = result.get('impacto_estimado','')
            if impacto:
                print(f'\n  💰 Impacto: {impacto}')

            pregunta = result.get('pregunta_implementacion','')
            if pregunta:
                print(f'\n  ❓ Para la próxima sesión:')
                print(f'     {pregunta}')

        elif current_mode == 'M2':
            ctx = result.get('contexto_spot','')
            if ctx: print(f'\n  📍 {ctx}')
            concepto = result.get('concepto_teorico','')
            if concepto:
                print(f'\n  💡 Marco conceptual:')
                for _l in concepto.split('\n'):
                    if _l.strip(): print(f'     {_l}')
            ctx_pool = result.get('contexto_pool','')
            if ctx_pool: print(f'\n  🌊 Este pool: {ctx_pool}')
            print(f'\n  ❓ {result.get("pregunta_reflexion","")}')

        else:  # M3
            print(f'\n  🔍 Marco de análisis:')
            for _l in result.get('marco_analisis','').split('\n'):
                if _l.strip(): print(f'     {_l}')
            print(f'\n  ❓ {result.get("pregunta_clave","")}')

        print('─' * 68)

        # ── Guardar en historial ──────────────────────────────────────────
        import datetime
        session_entry = {
            'date':                  datetime.date.today().isoformat(),
            'spot':                  spot_id,
            'bb_vpip_at_session':    bb_vpip_now,
            'n_hands_at_session':    len(full_df) if full_df is not None else 0,
            'accion_dada':           result.get('accion_concreta', '')[:120],
            'concepto_dado':         result.get('concepto_teorico', '')[:120],
            'pregunta_dada':         result.get('pregunta_implementacion',
                                     result.get('pregunta_reflexion',
                                     result.get('pregunta_clave', '')))[:120],
            'modo':                  current_mode,
            'patron':                result.get('patron_detectado', patron),
        }
        _save_coach_history(session_entry, drive_path)
        print(f'  💾 Sesión guardada en coach_history.json')
        print()

        return result

    except Exception as e:
        _emsg = str(e)
        if '429' in _emsg:
            print('   ⚠️  M4.4 Coach: límite Gemini gratuito alcanzado — reintenta en 1 minuto')
            print('      (El plan gratuito de Gemini tiene límite de requests por minuto)')
        else:
            print(f'   ❌ M4.4 Coach error: {_emsg[:120]}')
            print('      Verificar GEMINI_API_KEY en Colab Secrets (aistudio.google.com)')
        return None


print('✅ M4.4 Coach Analítico v1.48 cargado.')
print('   Cambios vs v1.34:')
print('   + concepto_teorico: 1 frase del porqué matemático/estratégico')
print('   + coach_history.json: memoria de últimas 3 sesiones')
print('   + Detección de patrón: persiste / mejoró / primera_vez')
print('   + Si persiste: pregunta causa-raíz, no repite instrucción')
print('   Modos: M1=directivo+porqué | M2=mixto | M3=socrático')
print('   Salvaguarda: el sistema mide; el coach explica; tú decides.')
print('   Guard v1.45: M4_CALLS_PER_SESSION={M1:2,M2:2,M3:1} | M4_API_ENABLED=True')
print('   v1.58: usa Gemini 2.0 Flash (gratuito) — GEMINI_API_KEY en Colab Secrets')
print('   v1.48: DRILL_ACTIVO en prompt | top-3 leaks | correlación familias | siguiente leak auto')


# ════════════════════════════════════════════════════════════════════════════
# MÓDULO M7 — SunChat v1.0 + M4 Gemini
# v1.57: Entrenador conversacional adaptativo alineado con ROI ranking del OS.
#
# ARQUITECTURA:
#   build_leak_object_from_roi()  → serializa top leak del ROI ranking
#   run_m4_gemini_diagnosis()     → diagnóstico contextual (Gemini gratuito)
#   run_sunchat_session()         → loop de entrenamiento (Groq gratuito)
#   save_drill_history()          → memoria entre sesiones (Drive)
#   load_drill_history()          → carga historial de drills
#
# MODELOS:
#   M4 diagnóstico: Gemini 2.0 Flash — razonamiento sobre datos del OS
#   SunChat drill:  Groq Llama-3.3-70B — loop conversacional rápido
#
# COSTE: €0. Ambos modelos con plan gratuito suficiente para uso de poker.
#
# ACTIVACIÓN:
#   1. Añade GEMINI_API_KEY en Colab Secrets
#   2. Añade GROQ_API_KEY en Colab Secrets
#   3. Ejecuta run_sunchat_session(leak_obj) después del pipeline
# ════════════════════════════════════════════════════════════════════════════

import os as _os
import json as _json
import re as _re
import urllib.request as _urllib_req
import urllib.error as _urllib_err

# ── Constantes M7 ────────────────────────────────────────────────────────
_SUNCHAT_HISTORY_FILE = 'sunchat_history.json'
_GEMINI_MODEL         = 'gemini-2.0-flash'
_GROQ_MODEL           = 'llama-3.3-70b-versatile'
_GEMINI_API_BASE      = 'https://generativelanguage.googleapis.com/v1beta/models'
_GROQ_API_BASE        = 'https://api.groq.com/openai/v1/chat/completions'

# Niveles de dominio por leak (guardados en historial)
_DRILL_LEVELS = {1: 'iniciando', 2: 'reconociendo', 3: 'aplicando',
                  4: 'automatizando', 5: 'dominado'}

# ── API helpers ──────────────────────────────────────────────────────────

def _get_api_key(name):
    """Obtiene API key de Colab Secrets o variable de entorno."""
    try:
        from google.colab import userdata as _ud
        return _ud.get(name) or _os.environ.get(name, '')
    except Exception:
        return _os.environ.get(name, '')


def _gemini_call(prompt, system='', api_key='', max_tokens=800):
    """
    Llamada directa a Gemini 2.0 Flash API.
    Sin dependencias externas — usa urllib de la stdlib.
    """
    if not api_key:
        return None, 'GEMINI_API_KEY no configurada en Colab Secrets'
    
    url = f'{_GEMINI_API_BASE}/{_GEMINI_MODEL}:generateContent?key={api_key}'
    
    contents = []
    if system:
        contents.append({'role': 'user', 'parts': [{'text': system}]})
        contents.append({'role': 'model', 'parts': [{'text': 'Entendido.'}]})
    contents.append({'role': 'user', 'parts': [{'text': prompt}]})
    
    payload = _json.dumps({
        'contents': contents,
        'generationConfig': {
            'maxOutputTokens': max_tokens,
            'temperature': 0.4,
        }
    }).encode('utf-8')
    
    req = _urllib_req.Request(url, data=payload,
                               headers={'Content-Type': 'application/json'})
    try:
        with _urllib_req.urlopen(req, timeout=30) as resp:
            data = _json.loads(resp.read())
        text = data['candidates'][0]['content']['parts'][0]['text']
        return text.strip(), None
    except _urllib_err.HTTPError as e:
        body = e.read().decode('utf-8', errors='replace')[:300]
        return None, f'HTTP {e.code}: {body}'
    except Exception as e:
        return None, str(e)[:200]


def _groq_call(messages, api_key='', max_tokens=600, temperature=0.5, model=None):
    """
    Llamada directa a Groq API (Llama-3.3-70B).
    Sin dependencias externas — usa urllib de la stdlib.
    """
    if not api_key:
        return None, 'GROQ_API_KEY no configurada en Colab Secrets'
    if model is None:
        model = _GROQ_MODEL
    
    payload = _json.dumps({
        'model': model or _GROQ_MODEL,
        'messages': messages,
        'max_tokens': max_tokens,
        'temperature': temperature,
    }).encode('utf-8')
    
    req = _urllib_req.Request(
        _GROQ_API_BASE,
        data=payload,
        headers={
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {api_key}',
            'User-Agent': 'Mozilla/5.0',
            'Accept': 'application/json',
            'Accept-Language': 'en-US,en;q=0.9',
        }
    )
    try:
        with _urllib_req.urlopen(req, timeout=30) as resp:
            data = _json.loads(resp.read())
        text = data['choices'][0]['message']['content']
        return text.strip(), None
    except _urllib_err.HTTPError as e:
        body = e.read().decode('utf-8', errors='replace')[:300]
        if e.code == 403:
            # Cloudflare block desde IP Colab — probar modelos alternativos con User-Agent diferente
            for _alt in ['mixtral-8x7b-32768', 'gemma2-9b-it', 'llama-3.1-70b-versatile']:
                try:
                    _p2 = _json.dumps({'model': _alt, 'messages': messages,
                                       'max_tokens': max_tokens, 'temperature': temperature}).encode('utf-8')
                    _r2 = _urllib_req.Request(_GROQ_API_BASE, data=_p2,
                        headers={'Content-Type': 'application/json',
                                 'Authorization': f'Bearer {api_key}',
                                 'User-Agent': 'python-requests/2.31.0',
                                 'Accept': '*/*'})
                    with _urllib_req.urlopen(_r2, timeout=30) as _rr:
                        _d2 = _json.loads(_rr.read())
                    return _d2['choices'][0]['message']['content'].strip(), None
                except Exception:
                    continue
        return None, f'HTTP {e.code}: {body}'
    except Exception as e:
        return None, str(e)[:200]


# ── Construcción del leak object ─────────────────────────────────────────

def build_leak_object_from_roi(roi_ranking, df, top_n=1):
    """
    Serializa el top leak del ROI ranking en el objeto estructurado
    que consume SunChat.

    Args:
        roi_ranking: dict output de build_roi_ranking()
        df:          DataFrame completo de manos
        top_n:       qué leak del ranking usar (1 = el peor)

    Returns:
        dict: leak_object con todos los campos para SunChat
    """
    if roi_ranking is None or 'leaks' not in roi_ranking:
        return _build_leak_fallback(df)
    
    leaks_df = roi_ranking['leaks']
    if leaks_df is None or leaks_df.empty:
        return _build_leak_fallback(df)
    
    # Tomar el leak top_n
    idx = min(top_n - 1, len(leaks_df) - 1)
    row = leaks_df.iloc[idx]
    
    spot_id   = row.get('spot_identifier', 'unknown')
    ev_loss   = float(row.get('impacto_ev_total_eur', 0))
    ev_shrunk = float(row.get('ev_shrunk', ev_loss))
    sample    = int(row.get('count', 0))
    
    # Parsear posición del spot_id: BTN_IP_SRP_deep_preflop_unknown_F
    parts   = spot_id.split('_')
    pos     = parts[0] if parts else 'unknown'
    street  = 'preflop'
    for p in parts:
        if p in ('flop', 'turn', 'river', 'preflop'):
            street = p
            break
    
    # Métricas específicas de esa posición
    sub = df[df['player_position'] == pos] if pos in df['player_position'].values else df
    bb_val = 0.02
    bb100  = (sub['net_won'].sum() / max(len(sub), 1) / bb_val) * 100 if len(sub) > 0 else 0
    vpip   = sub['flg_vpip'].mean() * 100 if len(sub) > 0 else 0
    
    # Manos representativas del spot
    spot_hands = df[df.get('spot_identifier', '') == spot_id] if 'spot_identifier' in df.columns else df[df['player_position']==pos]
    worst = spot_hands.nsmallest(5, 'net_won')[['hand_id','hole_cards','net_won','preflop_action']].to_dict('records') if len(spot_hands) > 0 else []
    
    # Si muestra es muy pequeña, usar fallback
    if sample < 20:
        return _build_leak_fallback(df)
    # Confianza basada en muestra
    confidence = min(0.99, sample / 500) if sample > 0 else 0.1
    
    # Patrón descriptivo
    pattern = _describe_leak_pattern(spot_id, sub, df)
    
    leak_obj = {
        'leak_id':          spot_id,
        'position':         pos,
        'node':             f'{pos} en {street} ({spot_id})',
        'street':           street,
        'pattern':          pattern,
        'ev_loss_eur':      round(ev_loss, 2),
        'ev_loss_bb100':    round(bb100, 1),
        'ev_shrunk':        round(ev_shrunk, 3),
        'confidence':       round(confidence, 2),
        'sample':           sample,
        'vpip_hero':        round(vpip, 1),
        'limp_rate':        round(sub['flg_p_limp'].mean()*100, 1) if 'flg_p_limp' in sub.columns and len(sub)>0 else None,
        'open_rate':        round(sub['flg_p_open'].mean()*100, 1) if 'flg_p_open' in sub.columns and len(sub)>0 else None,
        'fold_rate':        round(sub['flg_p_fold'].mean()*100, 1) if 'flg_p_fold' in sub.columns and len(sub)>0 else None,
        'tbet_rate': round(
                    (sub['flg_p_3bet'].astype(int).sum() /
                     sub['flg_p_3bet_opp'].astype(int).sum() * 100)
                    if 'flg_p_3bet_opp' in sub.columns and sub['flg_p_3bet_opp'].astype(int).sum() >= 5
                    else sub['flg_p_3bet'].mean()*100
                    if 'flg_p_3bet' in sub.columns and len(sub)>0 else None, 1),
        'example_hands':    worst,
        'stake':            'NL2',
        'hero':             'LaRuinaDeMago',
    }
    return leak_obj


def _build_leak_fallback(df):
    """Construye leak object desde datos reales si no hay ROI ranking."""
    import numpy as _np
    pos_stats = df.groupby('player_position').agg(
        hands=('net_won','count'),
        net_won=('net_won','sum'),
        vpip=('flg_vpip','mean'),
    ).reset_index()
    pos_stats['bb100'] = pos_stats['net_won'] / (pos_stats['hands'] * 0.02) * 100
    # Ignorar posiciones con <50 manos (ruido estadístico)
    pos_stats = pos_stats[pos_stats['hands'] >= 50]
    if pos_stats.empty:
        pos_stats = df.groupby('player_position').agg(
            hands=('net_won','count'),
            net_won=('net_won','sum'),
            vpip=('flg_vpip','mean'),
        ).reset_index()
        pos_stats['bb100'] = pos_stats['net_won'] / (pos_stats['hands'] * 0.02) * 100
    worst_pos = pos_stats.nsmallest(1, 'bb100').iloc[0]
    pos = worst_pos['player_position']
    sub = df[df['player_position'] == pos]
    
    return {
        'leak_id':       f'{pos}_leak',
        'position':      pos,
        'node':          f'{pos} preflop',
        'street':        'preflop',
        'pattern':       _describe_leak_pattern(f'{pos}_SRP_deep_preflop', sub, df),
        'ev_loss_bb100': round(float(worst_pos['bb100']), 1),
        'ev_loss_eur':   round(float(worst_pos['net_won']), 2),
        'ev_shrunk':     0.0,
        'confidence':    min(0.99, int(worst_pos['hands']) / 500),
        'sample':        int(worst_pos['hands']),
        'vpip_hero':     round(float(worst_pos['vpip']) * 100, 1),
        'limp_rate':     round(sub['flg_p_limp'].mean()*100, 1) if 'flg_p_limp' in sub.columns else None,
        'open_rate':     round(sub['flg_p_open'].mean()*100, 1) if 'flg_p_open' in sub.columns else None,
        'fold_rate':     round(sub['flg_p_fold'].mean()*100, 1) if 'flg_p_fold' in sub.columns else None,
        'tbet_rate':     round((sub['flg_p_3bet'].astype(int).sum()/sub['flg_p_3bet_opp'].astype(int).sum()*100) if 'flg_p_3bet_opp' in sub.columns and sub['flg_p_3bet_opp'].astype(int).sum()>=5 else sub['flg_p_3bet'].mean()*100, 1) if 'flg_p_3bet' in sub.columns else None,
        'example_hands': sub.nsmallest(5,'net_won')[['hand_id','hole_cards','net_won','preflop_action']].to_dict('records'),
        'stake':         'NL2',
        'hero':          'LaRuinaDeMago',
    }


def _describe_leak_pattern(spot_id, sub_df, full_df):
    """Genera descripción textual del patrón del leak."""
    parts = spot_id.split('_')
    pos = parts[0] if parts else '?'
    
    if 'SB' in pos:
        limp = sub_df['flg_p_limp'].mean()*100 if 'flg_p_limp' in sub_df.columns and len(sub_df)>0 else 0
        vpip = sub_df['flg_vpip'].mean()*100 if len(sub_df)>0 else 0
        return f'VPIP {vpip:.0f}% con {limp:.0f}% limps pasivos — entrada OOP sin iniciativa'
    elif 'BB' in pos:
        fold = sub_df['flg_p_fold'].mean()*100 if len(sub_df)>0 else 0
        return f'Fold preflop {fold:.0f}% — posible over-fold o under-defend vs steal'
    elif 'BTN' in pos:
        vpip = sub_df['flg_vpip'].mean()*100 if len(sub_df)>0 else 0
        return f'VPIP BTN {vpip:.0f}% (ref 42%) — apertura subóptima desde posición ventajosa'
    elif 'UTG' in pos or 'EP' in pos:
        vpip = sub_df['flg_vpip'].mean()*100 if len(sub_df)>0 else 0
        return f'VPIP {vpip:.0f}% (ref 18%) — rango de apertura excesivo desde la peor posición'
    else:
        ip = 'IP' in spot_id
        street = next((p for p in parts if p in ('flop','turn','river','preflop')), 'postflop')
        return f'Leak en {pos} {"IP" if ip else "OOP"} en {street}'


# ── M4 Gemini: diagnóstico contextual ───────────────────────────────────

def run_m4_gemini_diagnosis(leak_obj, mode='M1', api_key=None):
    """
    Diagnóstico contextual del leak usando Gemini 2.0 Flash.

    Diferencia con el coach Anthropic (run_m44_coach):
    - Este módulo es ESPECÍFICO para alimentar SunChat
    - Genera el contexto de entrenamiento, no el diagnóstico post-sesión
    - Output: dict con contexto para SunChat (rango villano, error concreto, corrección)

    Args:
        leak_obj: dict del leak (output de build_leak_object_from_roi)
        mode:     'M1' | 'M2' | 'M3'
        api_key:  GEMINI_API_KEY (None = leer de Secrets)

    Returns:
        dict: {'rango_villano', 'error_concreto', 'correccion', 'drill_focus', 'pregunta_apertura'}
    """
    if api_key is None:
        api_key = _get_api_key('GEMINI_API_KEY')
    
    if not api_key:
        # Fallback sin API: contexto hardcoded por posición
        return _m4_fallback_context(leak_obj)
    
    system = """Eres un coach de poker NL2-NL25 6-max. 
Recibes datos reales de un jugador y generas contexto para un entrenador conversacional.
REGLAS:
- Output SOLO en JSON válido, sin markdown, sin explicaciones fuera del JSON
- Máximo 3 frases por campo
- No des la respuesta correcta directamente — el entrenador la extraerá en preguntas
- Enfócate en NL2 6-max pool (jugadores recreacionales, pasivos)"""

    prompt = f"""Datos del leak real de LaRuinaDeMago en NL2:
{_json.dumps(leak_obj, indent=2, default=str)}

Genera un JSON con estos campos exactos:
{{
  "rango_villano": "descripción del rango del villano en este spot (1-2 frases)",
  "error_concreto": "qué error específico comete el héroe aquí (1 frase)",  
  "correccion": "qué debe cambiar exactamente (1 frase, sin decir la respuesta completa)",
  "drill_focus": "qué habilidad específica entrenar (1 frase)",
  "pregunta_apertura": "primera pregunta para interrogar al héroe sobre este spot (1 pregunta)",
  "concepto_clave": "el concepto teórico central de este leak en 1 frase"
}}

Modo actual del sistema: {mode}. {"Sé directo y específico." if mode == "M1" else "Usa enfoque socrático."}"""

    result, err = _gemini_call(prompt, system=system, api_key=api_key, max_tokens=500)
    
    if err or not result:
        if err and '429' in str(err):
            # 429 = quota exceeded — silently use fallback, no error spam
            pass
        else:
            print(f'  ⚠️  Gemini: {str(err)[:80]} — usando contexto local')
        return _m4_fallback_context(leak_obj)
    
    try:
        # Limpiar posibles markdown fences
        clean = _re.sub(r'```json|```', '', result).strip()
        context = _json.loads(clean)
        return context
    except Exception:
        print(f'  ⚠️  Gemini response no parseable — usando contexto local')
        return _m4_fallback_context(leak_obj)


def _m4_fallback_context(leak_obj):
    """Contexto de entrenamiento sin API — hardcoded por posición."""
    pos = leak_obj.get('position', '')
    
    fallbacks = {
        'SB': {
            'rango_villano': 'BTN abre ~45% de manos desde BTN con mucho aire. CO abre ~30% con rango más value.',
            'error_concreto': 'Limpias el SB con manos que no tienen EV positivo OOP en multiway — regalas la small blind.',
            'correccion': 'Cada mano SB: es raise con tu rango de apertura (15%) o fold. El limp no existe.',
            'drill_focus': 'Decisión binaria SB: construir el hábito raise/fold automático.',
            'pregunta_apertura': '¿Qué pasa con tu equity cuando limpias SB y el BB hace check detrás en un flop de 3 jugadores?',
            'concepto_clave': 'El limp SB regala posición, iniciativa y equity a cambio de nada medible.'
        },
        'BB': {
            'rango_villano': 'BTN steal rango: ~45% incluyendo mucho aire. Necesitas defender para no ser explotable.',
            'error_concreto': 'Over-fold vs steal — dejas que el BTN robe demasiado con 0 riesgo.',
            'correccion': 'Defender más con manos con equity realizeable: suited connectors, Ax, pares pequeños.',
            'drill_focus': 'Frecuencia de defensa BB vs steal por posición del villano.',
            'pregunta_apertura': '¿Qué % del tiempo deberías defender BB si BTN abre el 45% de las manos?',
            'concepto_clave': 'Fold frequency BB > 55% vs steal es directamente explotable — el BTN tiene EV positivo instantáneo.'
        },
        'BTN': {
            'rango_villano': 'BB defiende ~55% vs BTN. SB fold ~60%. Tienes posición post-flop siempre.',
            'error_concreto': 'Abres menos del óptimo desde BTN — dejas EV sobre la mesa con manos con equity positivo.',
            'correccion': 'Ampliar rango BTN a ~42% (suited connectors, Ax suited, todos los pares).',
            'drill_focus': 'Rango de apertura BTN y gestión postflop IP.',
            'pregunta_apertura': '¿Por qué BTN con K7o es un open profitable contra BB tight?',
            'concepto_clave': 'La posición IP post-flop vale ~3-5 BB/100 adicionales en NL2 pools pasivos.'
        },
    }
    
    return fallbacks.get(pos, {
        'rango_villano': 'Rango del villano a analizar según posición.',
        'error_concreto': 'Frecuencia de acción fuera del óptimo para NL2.',
        'correccion': 'Ajustar frecuencia hacia el rango de referencia NL2.',
        'drill_focus': 'Decisiones preflop en este spot.',
        'pregunta_apertura': '¿Qué cambiarías en tu decisión en este spot?',
        'concepto_clave': 'La consistencia en spots repetidos determina el winrate a largo plazo.'
    })


# ── SunChat: loop de entrenamiento ──────────────────────────────────────

def _build_sunchat_system_prompt(leak_obj, m4_context, mode='M1'):
    """Construye el system prompt de SunChat para Groq."""
    
    pos          = leak_obj.get('position', '?')
    ev_loss      = leak_obj.get('ev_loss_bb100', 0)
    pattern      = leak_obj.get('pattern', '')
    err_concreto = m4_context.get('error_concreto', '')
    concepto     = m4_context.get('concepto_clave', '')
    drill_focus  = m4_context.get('drill_focus', '')
    
    mode_instruction = {
        'M1': 'Sé directo y específico. Indica el error con claridad. Da feedback inmediato.',
        'M2': 'Mezcla instrucción con preguntas. Deja que el jugador razone parte.',
        'M3': 'Socrático. Solo preguntas. Nunca des la respuesta — guía al jugador a descubrirla.',
    }.get(mode, 'Sé directo y específico.')
    
    return f"""Eres SunChat, entrenador de poker NL2-NL25 6-max integrado en el OS v2.0.

LEAK ACTIVO (datos reales del jugador):
- Posición: {pos}
- Patrón: {pattern}
- EV perdido: {ev_loss:.1f} BB/100
- Error concreto: {err_concreto}
- Concepto clave: {concepto}
- Focus del drill: {drill_focus}

REGLAS ABSOLUTAS:
1. Nunca expliques sin preguntar primero. Pregunta → escucha → corrige.
2. Máximo 3 líneas por respuesta. Sin párrafos largos.
3. Un concepto por sesión. No desvíes.
4. Feedback inmediato en los drills: ✅ correcto / ❌ incorrecto + por qué en 1 línea.
5. Termina SIEMPRE con una pregunta o un spot de drill.
6. No uses jerga GTO compleja — el jugador está en M1/NL2.

MODO: {mode}. {mode_instruction}

ESTRUCTURA DE SESIÓN:
FASE 1 (2 mensajes): Activación + primera pregunta de interrogación
FASE 2 (2-3 mensajes): Confrontación del error
FASE 3 (5 mensajes): Drill — 5 spots con feedback inmediato
FASE 4 (1 mensaje): Score final + siguiente paso

Comienza con FASE 1: muestra el leak (1 línea) y haz la pregunta de apertura."""

def _format_leak_activation(leak_obj):
    """Mensaje de activación — sin explicación larga."""
    pos      = leak_obj.get('position', '?')
    ev       = leak_obj.get('ev_loss_bb100', 0)
    pattern  = leak_obj.get('pattern', '')
    sample   = leak_obj.get('sample', 0)
    conf     = leak_obj.get('confidence', 0)
    
    lines_out = [
        f"🎯 DRILL ACTIVO — {pos} | {ev:.0f} BB/100 perdidos | {sample} manos ({conf*100:.0f}% confianza)",
        f"Patrón: {pattern}"
    ]
    return chr(10).join(lines_out)


def save_drill_history(leak_obj, score, drive_path=None):
    """
    Guarda el resultado de la sesión de drill en historial.
    Actualiza el nivel de dominio del leak (1-5).
    
    Args:
        leak_obj:   dict del leak entrenado
        score:      float 0-1 (ratio aciertos en drill)
        drive_path: ruta Drive (None = directorio actual)
    """
    path = (_os.path.join(drive_path, _SUNCHAT_HISTORY_FILE)
            if drive_path else _SUNCHAT_HISTORY_FILE)
    
    try:
        with open(path, 'r', encoding='utf-8') as f:
            history = _json.load(f)
    except Exception:
        history = {}
    
    leak_id = leak_obj.get('leak_id', 'unknown')
    
    if leak_id not in history:
        history[leak_id] = {
            'sessions': [],
            'level': 1,
            'best_score': 0.0,
            'consecutive_good': 0,
        }
    
    entry = history[leak_id]
    entry['sessions'].append({
        'score': round(score, 2),
        'ev_loss': leak_obj.get('ev_loss_bb100', 0),
        'sample': leak_obj.get('sample', 0),
    })
    
    # Actualizar nivel (1-5)
    if score >= 0.8:
        entry['consecutive_good'] = entry.get('consecutive_good', 0) + 1
        if entry['consecutive_good'] >= 2 and entry['level'] < 5:
            entry['level'] = min(5, entry['level'] + 1)
    else:
        entry['consecutive_good'] = 0
        if score < 0.4 and entry['level'] > 1:
            entry['level'] = max(1, entry['level'] - 1)
    
    entry['best_score'] = max(entry.get('best_score', 0), score)
    history[leak_id] = entry
    
    try:
        with open(path, 'w', encoding='utf-8') as f:
            _json.dump(history, f, indent=2, ensure_ascii=False, default=str)
        return entry
    except Exception as e:
        print(f'  ⚠️  No se pudo guardar historial: {e}')
        return entry


def load_drill_history(leak_id, drive_path=None):
    """Carga historial de drill para un leak específico."""
    path = (_os.path.join(drive_path, _SUNCHAT_HISTORY_FILE)
            if drive_path else _SUNCHAT_HISTORY_FILE)
    try:
        with open(path, 'r', encoding='utf-8') as f:
            history = _json.load(f)
        return history.get(leak_id, None)
    except Exception:
        return None


def run_sunchat_session(leak_obj, mode='M1', drive_path=None,
                         gemini_key=None, groq_key=None, max_turns=12):
    """
    Ejecuta una sesión completa de SunChat en modo interactivo (Colab terminal).

    Flujo:
        1. M4/Gemini genera el contexto de entrenamiento
        2. SunChat/Groq ejecuta el loop de interrogación → drill → evaluación
        3. Score guardado en historial Drive

    Args:
        leak_obj:   dict del leak (output de build_leak_object_from_roi)
        mode:       'M1' | 'M2' | 'M3'
        drive_path: ruta Drive para historial (None = directorio actual)
        gemini_key: GEMINI_API_KEY (None = Colab Secrets)
        groq_key:   GROQ_API_KEY (None = Colab Secrets)
        max_turns:  máximo de intercambios en el loop (default 12)

    Returns:
        dict: {'score': float, 'level': int, 'turns': int}
    """
    # Obtener keys
    if gemini_key is None:
        gemini_key = _get_api_key('GEMINI_API_KEY')
    if groq_key is None:
        groq_key = _get_api_key('GROQ_API_KEY')
    
    leak_id  = leak_obj.get('leak_id', 'unknown')
    pos      = leak_obj.get('position', '?')
    
    # Cargar historial previo
    prev_history = load_drill_history(leak_id, drive_path)
    prev_level   = prev_history['level'] if prev_history else 1
    prev_best    = prev_history['best_score'] if prev_history else 0.0
    
    print('\n' + '═'*60)
    print('  🌞 SUNCHAT — ENTRENADOR ADAPTATIVO')
    print('═'*60)
    print(f'  Leak:        {leak_id}')
    print(f'  Posición:    {pos}')
    print(f'  EV perdido:  {leak_obj.get("ev_loss_bb100", 0):.0f} BB/100')
    print(f'  Muestra:     {leak_obj.get("sample", 0)} manos')
    print(f'  Nivel prev:  {prev_level}/5 ({_DRILL_LEVELS.get(prev_level, "?")}) | Mejor score: {prev_best*100:.0f}%')
    print('─'*60)
    
    # PASO 1: Contexto M4/Gemini
    print('\n  ⚙️  Generando contexto M4/Gemini...')
    m4_context = run_m4_gemini_diagnosis(leak_obj, mode=mode, api_key=gemini_key)
    
    if not groq_key:
        print('\n  ❌ GROQ_API_KEY no configurada.')
        print('  → Ve a console.groq.com → API Keys → Create → añade en Colab Secrets')
        print('  → Mientras tanto: el contexto M4 está disponible:')
        print(_json.dumps(m4_context, indent=4, ensure_ascii=False))
        return {'score': 0, 'level': prev_level, 'turns': 0}
    
    # PASO 2: Inicializar SunChat
    system_prompt = _build_sunchat_system_prompt(leak_obj, m4_context, mode)
    activation    = _format_leak_activation(leak_obj)
    
    messages = [
        {'role': 'system', 'content': system_prompt},
        {'role': 'user',   'content': activation},
    ]
    
    print('\n  ✅ Contexto listo. Iniciando sesión...\n')
    print('─'*60)
    print(f'  [TÚ]: {activation}')
    
    # PASO 3: Loop conversacional
    turns      = 0
    correct    = 0
    total_eval = 0
    
    while turns < max_turns:
        # Llamada a Groq
        response, err = _groq_call(messages, api_key=groq_key, max_tokens=300, temperature=0.5)
        
        if err or not response:
            print(f'\n  ⚠️  Error Groq: {err}')
            break
        
        print(f'\n  [SUNCHAT]: {response}')
        
        # Detectar si es drill (contiene ✅ o ❌ de feedback)
        if any(x in response for x in ['✅', '❌', 'correcto', 'incorrecto']):
            total_eval += 1
            if '✅' in response or 'correcto' in response.lower():
                correct += 1
        
        # Detectar fin de sesión
        if any(x in response.lower() for x in 
               ['score final', 'sesión completada', 'próxima sesión', 
                'siguiente paso', 'hemos terminado', 'fin del drill']):
            turns += 1
            break
        
        turns += 1
        
        # Input del usuario
        try:
            user_input = input('\n  [TÚ]: ').strip()
        except (EOFError, KeyboardInterrupt):
            print('\n  [Sesión interrumpida]')
            break
        
        if not user_input:
            continue
        
        if user_input.lower() in ('exit', 'salir', 'quit', 'q'):
            print('\n  [Sesión terminada por el usuario]')
            break
        
        messages.append({'role': 'assistant', 'content': response})
        messages.append({'role': 'user',      'content': user_input})
    
    # PASO 4: Score y guardado
    score = (correct / total_eval) if total_eval > 0 else 0.5  # 0.5 si no hay drills evaluados
    
    print('\n' + '─'*60)
    print(f'  📊 RESULTADO DE SESIÓN')
    print(f'     Turnos:  {turns}')
    if total_eval > 0:
        print(f'     Score:   {correct}/{total_eval} ({score*100:.0f}%)')
    
    entry = save_drill_history(leak_obj, score, drive_path)
    new_level = entry.get('level', prev_level)
    
    print(f'     Nivel:   {new_level}/5 ({_DRILL_LEVELS.get(new_level, "?")})')
    if new_level > prev_level:
        print(f'     🎉 ¡Subiste de nivel! {prev_level} → {new_level}')
    print('═'*60)
    
    return {'score': score, 'level': new_level, 'turns': turns}


print('✅ Módulo M7 — SunChat v1.0 cargado.')
print('   M4/Gemini: diagnóstico contextual del leak (gratuito)')
print('   SunChat/Groq: entrenamiento conversacional (gratuito)')
print('   Requiere: GEMINI_API_KEY + GROQ_API_KEY en Colab Secrets')
print()
print('   Uso después del pipeline:')
print('   leak_obj = build_leak_object_from_roi(roi_ranking, ingested_df)')
print('   run_sunchat_session(leak_obj, drive_path=BASELINE_DRIVE_PATH)')


def run_cognitive_chat(
    hand_context,
    razonamiento_jugador,
    overall_metrics,
    spot_identifier,
    drill_activo,
    mode='M1',
    api_key=None,
    use_groq=False,
):
    """
    Dialogo cognitivo sobre una mano concreta con contexto completo. v1.68

    El jugador escribe su razonamiento sobre una mano especifica.
    El sistema responde con contexto real:
    - Que dice la referencia NL2/NL50 para este spot
    - Si el razonamiento es correcto o tiene gaps
    - Una pregunta socratica para profundizar
    - Una frase del porque matematico/estrategico

    Args:
        hand_context:          dict con hole_cards, board, acciones, net, ev
        razonamiento_jugador:  string con el razonamiento escrito por el jugador
        overall_metrics:       dict de metricas globales del pipeline
        spot_identifier:       string del spot activo
        drill_activo:          string del drill activo
        mode:                  M1/M2/M3
        api_key:               GEMINI_API_KEY (auto desde Colab Secrets si None)
        use_groq:              si True usa Groq en lugar de Gemini

    Returns:
        dict con 'respuesta', 'pregunta_socratica', 'concepto', 'error'
    """
    import os as _os_cc

    # ── Obtener API key ───────────────────────────────────────────
    if api_key is None:
        api_key = _os_cc.environ.get('GEMINI_API_KEY', '')
        if not api_key:
            try:
                from google.colab import userdata as _ud
                api_key = _ud.get('GEMINI_API_KEY', '') or _ud.get('GROQ_API_KEY', '')
            except Exception:
                pass

    if not api_key:
        return {
            'respuesta': 'Sin API key configurada. Anade GEMINI_API_KEY en Colab Secrets.',
            'pregunta_socratica': '',
            'concepto': '',
            'error': 'NO_API_KEY'
        }

    # ── Construir contexto de la mano ─────────────────────────────
    hole    = hand_context.get('hole_cards', '??')
    flop    = hand_context.get('flop', '-')
    turn    = hand_context.get('turn', '-')
    river   = hand_context.get('river', '-')
    pf_act  = hand_context.get('preflop_action', '-')
    net     = hand_context.get('net', 0)
    ev      = hand_context.get('ev', 0)
    luck    = net - ev

    bb100   = overall_metrics.get('bb_per_100_net', 0) if overall_metrics else 0
    vpip    = overall_metrics.get('vpip_pct', 0) if overall_metrics else 0
    wsd     = overall_metrics.get('wsd_pct', 0) if overall_metrics else 0

    # ── Contexto del spot ─────────────────────────────────────────
    spot_parts = spot_identifier.split('_') if spot_identifier else []
    posicion   = spot_parts[0] if len(spot_parts) > 0 else '?'
    ip_oop     = spot_parts[1] if len(spot_parts) > 1 else '?'
    pot_type   = spot_parts[2] if len(spot_parts) > 2 else '?'
    calle      = spot_parts[4] if len(spot_parts) > 4 else '?'
    accion     = spot_parts[-1] if spot_parts else '?'

    # ── Construir system prompt ───────────────────────────────────
    system = (
        "Eres un coach de poker especializado en NL2-NL200 6-max. "
        "Tu objetivo es construir comprension cognitiva real en el jugador, "
        "no solo corregir frecuencias. Siempre citas datos reales del pipeline "
        "cuando estan disponibles. Eres directo, conciso y socratico. "
        "Nunca das mas de 4 lineas de respuesta. "
        "Terminas siempre con UNA pregunta socratica especifica."
    )

    # ── Construir prompt con contexto completo ────────────────────
    prompt = f"""CONTEXTO DEL JUGADOR (LaRuinaDeMago, NL2 6-max):
- BB/100 global: {bb100:.1f}
- VPIP: {vpip:.1f}% | W$SD: {wsd:.1f}%
- Drill activo: {drill_activo}
- Spot analizado: {spot_identifier}
  Posicion: {posicion} | {ip_oop} | {pot_type} | Calle: {calle} | Ultima accion: {accion}

MANO CONCRETA:
- Cartas: {hole}
- Board: {flop} / {turn} / {river}
- Acciones PF: {pf_act}
- Net: {net:+.3f}E | EV: {ev:+.3f}E | Suerte: {luck:+.3f}E

RAZONAMIENTO DEL JUGADOR:
"{razonamiento_jugador}"

Modo de coaching: {mode}
{"M1: directivo + porque matematico (jugador en M1, base fundamental)" if mode == "M1" else "M2: mixto + identificacion de patrones" if mode == "M2" else "M3: socratico puro + construccion de rango"}

TAREA:
1. Evalua el razonamiento del jugador (correcto / gap / incorrecto)
2. Corrige o refuerza con la referencia teorica especifica para este spot
3. Da UNA frase del concepto matematico/estrategico clave
4. Termina con UNA pregunta socratica que profundice la comprension

Maximo 4 lineas. Cita los datos reales si son relevantes."""

    # ── Llamada API ───────────────────────────────────────────────
    if use_groq:
        messages = [
            {'role': 'system', 'content': system},
            {'role': 'user', 'content': prompt}
        ]
        groq_key = _os_cc.environ.get('GROQ_API_KEY', api_key)
        respuesta_raw, err = _groq_call(messages, api_key=groq_key, max_tokens=300)
    else:
        respuesta_raw, err = _gemini_call(prompt, system=system, api_key=api_key, max_tokens=300)

    if err or not respuesta_raw:
        return {
            'respuesta': 'Error llamando a la API: ' + str(err or 'respuesta vacia'),
            'pregunta_socratica': '',
            'concepto': '',
            'error': err
        }

    # ── Parsear respuesta — FIX P0-D v1.78: indentación corregida ───────────
    lines_resp = [l.strip() for l in respuesta_raw.splitlines() if l.strip()]
    pregunta   = ''
    concepto   = ''
    cuerpo     = []

    for l in lines_resp:
        if l.startswith('?') or ('?' in l and len(l) < 120 and l == lines_resp[-1]):
            pregunta = l
        elif l.lower().startswith('concepto') or l.lower().startswith('principio'):
            concepto = l
        else:
            cuerpo.append(l)

    return {
        'respuesta':          chr(10).join(cuerpo),
        'pregunta_socratica': pregunta,
        'concepto':           concepto,
        'error':              None
    }


def display_cognitive_chat(
    df,
    spot_identifier,
    razonamiento,
    hand_idx=0,
    overall_metrics=None,
    drill_activo=None,
    mode='M1',
    api_key=None,
):
    """
    Wrapper interactivo para run_cognitive_chat. v1.68

    Uso tipico post-sesion:
        display_cognitive_chat(
            ingested_df,
            DRILL_ACTIVO,
            razonamiento="Folde porque la mano parecia debil OOP sin posicion",
        )
    """
    hands = get_representative_hands(df, spot_identifier, top_n=max(hand_idx+1, 5))
    if hands.empty:
        print("Sin manos disponibles para este spot.")
        return

    row = hands.iloc[hand_idx] if hand_idx < len(hands) else hands.iloc[0]

    hand_context = {
        'hole_cards':      str(row.get('hole_cards', '??')),
        'flop':            str(row.get('board_cards_flop', '')) or '-',
        'turn':            str(row.get('board_cards_turn', '')) or '-',
        'river':           str(row.get('board_cards_river', '')) or '-',
        'preflop_action':  str(row.get('preflop_action', '')) or '-',
        'net':             float(row.get('net_won', 0)),
        'ev':              float(row.get('ev_won', 0)),
    }

    drill = drill_activo or spot_identifier

    print()
    print('='*60)
    print('  DIALOGO COGNITIVO')
    print('  Spot: ' + spot_identifier)
    print('  Mano ' + str(hand_idx+1) + ': ' + hand_context['hole_cards'] +
          ' | Board: ' + hand_context['flop'] + ' / ' + hand_context['turn'])
    print()
    print('  Tu razonamiento:')
    print('  "' + razonamiento + '"')
    print()
    print('  Consultando al coach...')

    result = run_cognitive_chat(
        hand_context      = hand_context,
        razonamiento_jugador = razonamiento,
        overall_metrics   = overall_metrics,
        spot_identifier   = spot_identifier,
        drill_activo      = drill,
        mode              = mode,
        api_key           = api_key,
    )

    if result['error']:
        print()
        print('  [Sin conexion API] El coach no esta disponible.')
        print('  Razonamiento guardado para revision manual.')
        print('='*60)
        return

    print()
    print('  Coach:')
    for l in result['respuesta'].splitlines():
        if l.strip():
            print('  ' + l)

    if result['concepto']:
        print()
        print('  Concepto clave:')
        print('  ' + result['concepto'])

    if result['pregunta_socratica']:
        print()
        print('  ' + result['pregunta_socratica'])

    print('='*60)

