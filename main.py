#!/usr/bin/env python3
"""
Roulette Telegram Signal Bot - Sistema AMX V20
VERSION 2.0:
  - Intento 1/2/3: al perder analiza color opuesto si condiciones dadas
  - Si ningún color cumple condiciones → espera indefinida con gráfico
  - Cuando sale 0 en espera → salta UN giro antes de analizar
  - Verificación por game_id (sin temporizador de resultado)
  - Markov Chain: ventana de últimos 100 giros (señales recientes)
  - ML Pattern: aprende del historial COMPLETO (todos los patrones)
  - En cada intento se evalúan AMBOS colores
"""

import asyncio
import io
import json
import logging
import os
import threading
import time
import urllib.request
from collections import deque, defaultdict
from typing import Optional, Literal

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import telebot
import websockets
from flask import Flask, jsonify

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ─── LOGGING ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] %(levelname)s %(message)s'
)
logger = logging.getLogger("RouletteBotAMX")

# ─── TELEGRAM ─────────────────────────────────────────────────────────────────
TOKEN = "8308452662:AAGZFIZyYsmVR39SvIOSlKD3OY_YNMOsEQU"

_session = requests.Session()
_retry = Retry(
    total=5,
    backoff_factor=1.5,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "POST"],
    raise_on_status=False,
)
_adapter = HTTPAdapter(max_retries=_retry, pool_connections=10, pool_maxsize=20)
_session.mount("https://", _adapter)
_session.mount("http://",  _adapter)

bot = telebot.TeleBot(TOKEN, threaded=False)
bot.session = _session

# ─── ROULETTE COLOR MAPS ──────────────────────────────────────────────────────
REAL_COLOR_MAP = {
    0:"VERDE",1:"ROJO",2:"NEGRO",3:"ROJO",4:"NEGRO",5:"ROJO",6:"NEGRO",
    7:"ROJO",8:"NEGRO",9:"ROJO",10:"NEGRO",11:"NEGRO",12:"ROJO",13:"NEGRO",
    14:"ROJO",15:"NEGRO",16:"ROJO",17:"NEGRO",18:"ROJO",19:"ROJO",20:"NEGRO",
    21:"ROJO",22:"NEGRO",23:"ROJO",24:"NEGRO",25:"ROJO",26:"NEGRO",27:"ROJO",
    28:"NEGRO",29:"NEGRO",30:"ROJO",31:"NEGRO",32:"ROJO",33:"NEGRO",34:"ROJO",
    35:"NEGRO",36:"ROJO"
}

COLOR_DATA_AZURE = [
    {"id":0,"rojo":0.52,"negro":0.44,"senal":"ROJO"},
    {"id":1,"rojo":0.52,"negro":0.48,"senal":"ROJO"},
    {"id":2,"rojo":0.60,"negro":0.40,"senal":"ROJO"},
    {"id":3,"rojo":0.56,"negro":0.40,"senal":"ROJO"},
    {"id":4,"rojo":0.56,"negro":0.40,"senal":"ROJO"},
    {"id":5,"rojo":0.52,"negro":0.48,"senal":"ROJO"},
    {"id":6,"rojo":0.56,"negro":0.40,"senal":"ROJO"},
    {"id":7,"rojo":0.52,"negro":0.44,"senal":"ROJO"},
    {"id":8,"rojo":0.52,"negro":0.44,"senal":"ROJO"},
    {"id":9,"rojo":0.52,"negro":0.44,"senal":"ROJO"},
    {"id":10,"rojo":0.48,"negro":0.48,"senal":"NO APOSTAR"},
    {"id":11,"rojo":0.52,"negro":0.44,"senal":"ROJO"},
    {"id":12,"rojo":0.56,"negro":0.44,"senal":"ROJO"},
    {"id":13,"rojo":0.56,"negro":0.40,"senal":"ROJO"},
    {"id":14,"rojo":0.52,"negro":0.44,"senal":"ROJO"},
    {"id":15,"rojo":0.56,"negro":0.44,"senal":"ROJO"},
    {"id":16,"rojo":0.52,"negro":0.44,"senal":"ROJO"},
    {"id":17,"rojo":0.60,"negro":0.36,"senal":"ROJO"},
    {"id":18,"rojo":0.52,"negro":0.48,"senal":"ROJO"},
    {"id":19,"rojo":0.40,"negro":0.56,"senal":"NEGRO"},
    {"id":20,"rojo":0.44,"negro":0.52,"senal":"NEGRO"},
    {"id":21,"rojo":0.40,"negro":0.56,"senal":"NEGRO"},
    {"id":22,"rojo":0.48,"negro":0.52,"senal":"NEGRO"},
    {"id":23,"rojo":0.48,"negro":0.52,"senal":"NEGRO"},
    {"id":24,"rojo":0.44,"negro":0.52,"senal":"NEGRO"},
    {"id":25,"rojo":0.36,"negro":0.60,"senal":"NEGRO"},
    {"id":26,"rojo":0.44,"negro":0.56,"senal":"NEGRO"},
    {"id":27,"rojo":0.40,"negro":0.56,"senal":"NEGRO"},
    {"id":28,"rojo":0.44,"negro":0.52,"senal":"NEGRO"},
    {"id":29,"rojo":0.44,"negro":0.52,"senal":"NEGRO"},
    {"id":30,"rojo":0.44,"negro":0.52,"senal":"NEGRO"},
    {"id":31,"rojo":0.44,"negro":0.52,"senal":"NEGRO"},
    {"id":32,"rojo":0.44,"negro":0.52,"senal":"NEGRO"},
    {"id":33,"rojo":0.48,"negro":0.52,"senal":"NEGRO"},
    {"id":34,"rojo":0.40,"negro":0.60,"senal":"NEGRO"},
    {"id":35,"rojo":0.40,"negro":0.56,"senal":"NEGRO"},
    {"id":36,"rojo":0.44,"negro":0.56,"senal":"NEGRO"},
]

# ─── ROULETTE CONFIGS ─────────────────────────────────────────────────────────
ROULETTE_CONFIGS = {
    "Azure Roulette 1": {
        "ws_key": 227,
        "chat_id": -1003835197023,
        "thread_id": 6,
        "color_data": COLOR_DATA_AZURE,
        "betting_system": "dalembert",
        "min_prob_threshold": 0.49,
    },
}

WS_URL    = "wss://dga.pragmaticplaylive.net/ws"
CASINO_ID = "ppcjd00000007254"
MAX_ATTEMPTS = 3
BASE_BET  = 0.10
VISIBLE   = 50

# ─── D'ALEMBERT ───────────────────────────────────────────────────────────────
class D_Alembert:
    def __init__(self, base: float):
        self.base     = base
        self.step     = 0
        self.bankroll = 0.0
        self.max_step = 20

    def current_bet(self) -> float:
        return round(self.base * (self.step + 1), 2)

    def win(self) -> float:
        bet = self.current_bet()
        self.bankroll = round(self.bankroll + bet, 2)
        if self.step > 0:
            self.step -= 1
        return bet

    def loss(self) -> float:
        bet = self.current_bet()
        self.bankroll = round(self.bankroll - bet, 2)
        if self.step >= self.max_step - 1:
            self.step = 0
        else:
            self.step += 1
        return bet

# ─── MARKOV CHAIN (ventana 100 giros) ─────────────────────────────────────────
class MarkovChainPredictor:
    """
    Cadena de Markov de orden 2 sobre los últimos 100 giros no-verde.
    Optimizada para capturar señales recientes.
    """
    def __init__(self, window: int = 100, order: int = 2):
        self.window = window
        self.order  = order
        self.transition_counts: dict = {}

    def update(self, spin_history: list):
        """Recalcula las transiciones con la ventana de 100 giros más recientes."""
        self.transition_counts = defaultdict(lambda: defaultdict(int))
        recent = [s["real"] for s in spin_history[-self.window:] if s["real"] != "VERDE"]
        if len(recent) < self.order + 1:
            return
        for i in range(len(recent) - self.order):
            state   = tuple(recent[i : i + self.order])
            next_c  = recent[i + self.order]
            if next_c in ("ROJO", "NEGRO"):
                self.transition_counts[state][next_c] += 1

    def predict(self, spin_history: list) -> Optional[dict]:
        """
        Retorna {'ROJO': p, 'NEGRO': p, 'total': n} o None si datos insuficientes.
        """
        recent = [s["real"] for s in spin_history if s["real"] != "VERDE"]
        if len(recent) < self.order:
            return None
        state  = tuple(recent[-self.order:])
        counts = dict(self.transition_counts.get(state, {}))
        total  = sum(counts.values())
        if total < 5:  # mínimo estadístico
            return None
        return {
            "ROJO":  counts.get("ROJO", 0)  / total,
            "NEGRO": counts.get("NEGRO", 0) / total,
            "total": total,
        }

# ─── ML PATTERN PREDICTOR (historial completo) ────────────────────────────────
class MLPatternPredictor:
    """
    Predictor basado en patrones de longitud N.
    Aprende TODOS los patrones del historial completo de forma incremental (O(1) por giro).
    """
    def __init__(self, pattern_length: int = 4):
        self.pattern_length = pattern_length
        self.pattern_counts: dict = defaultdict(lambda: defaultdict(int))
        self._known_len: int = 0  # número de giros no-verde ya procesados

    def add_spin(self, spin_history: list):
        """Actualización incremental: agrega únicamente el patrón más reciente."""
        non_verde = [s["real"] for s in spin_history if s["real"] != "VERDE"]
        current_len = len(non_verde)
        if current_len <= self._known_len:
            return
        self._known_len = current_len
        if current_len < self.pattern_length + 1:
            return
        # El patrón que acaba de completarse
        i       = current_len - self.pattern_length - 1
        pattern = tuple(non_verde[i : i + self.pattern_length])
        next_c  = non_verde[i + self.pattern_length]
        if next_c in ("ROJO", "NEGRO"):
            self.pattern_counts[pattern][next_c] += 1

    def predict(self, spin_history: list) -> Optional[dict]:
        """
        Retorna {'ROJO': p, 'NEGRO': p, 'total': n} o None si patrón desconocido.
        """
        non_verde = [s["real"] for s in spin_history if s["real"] != "VERDE"]
        if len(non_verde) < self.pattern_length:
            return None
        pattern = tuple(non_verde[-self.pattern_length:])
        counts  = dict(self.pattern_counts.get(pattern, {}))
        total   = sum(counts.values())
        if total < 3:
            return None
        return {
            "ROJO":  counts.get("ROJO", 0)  / total,
            "NEGRO": counts.get("NEGRO", 0) / total,
            "total": total,
        }

# ─── SISTEMA AMX V20 ──────────────────────────────────────────────────────────
class AMXSignalSystem:
    def __init__(self, mode: Literal["tendencia", "moderado"] = "moderado"):
        self.mode = mode
        self.last_signal_time: float = 0
        self.cooldown_seconds: int = 8
        self.so_cooldown: Optional[float] = None
        self.momentum_consecutivo: int = 0
        self.direccion_momentum: int = 0
        self.prev_ema4_above_ema8: bool = True
        self.ultimos_puntos: list = []
        self.last_two_expected = deque(maxlen=2)
        self.last_two_colors   = deque(maxlen=2)

    def update_streak(self, real_color: str, expected_color: Optional[str]):
        if expected_color:
            self.last_two_expected.append(real_color == expected_color)
        self.last_two_colors.append(real_color)

    def calculate_ema(self, data: list, period: int) -> list:
        if len(data) < period:
            return [None] * len(data)
        mult = 2 / (period + 1)
        ema  = [None] * (period - 1)
        prev = sum(data[:period]) / period
        ema.append(prev)
        for i in range(period, len(data)):
            prev = (data[i] * mult) + (prev * (1 - mult))
            ema.append(prev)
        return ema

    def check_signal_tendencia(self, positions, color_data, current_number,
                               expected_color, prob_threshold) -> Optional[dict]:
        if len(positions) < 20:
            return None
        ahora = time.time()
        if ahora - self.last_signal_time < self.cooldown_seconds:
            return None
        if self.so_cooldown and ahora - self.so_cooldown < 8:
            return None

        ema4  = self.calculate_ema(positions, 4)
        ema8  = self.calculate_ema(positions, 8)
        ema20 = self.calculate_ema(positions, 20)

        if any(v is None for v in [ema4[-1], ema8[-1], ema20[-1],
                                    ema4[-2], ema8[-2], ema20[-2]]):
            return None

        current_pos = positions[-1]
        cruce_alcista    = ema4[-2] <= ema20[-2] and ema4[-1] > ema20[-1]
        sobre_tres_emas  = current_pos > ema4[-1] and current_pos > ema8[-1] and current_pos > ema20[-1]
        cruce_ema8       = ema8[-2] <= ema20[-2] and ema8[-1] > ema20[-1]
        cerca_ema4       = abs(current_pos - ema4[-1]) <= 0.5
        dos_ultimos_ok   = len(self.last_two_expected) >= 2 and all(self.last_two_expected)

        cond1 = cruce_alcista or sobre_tres_emas
        cond2 = cruce_ema8
        cond3 = sobre_tres_emas and dos_ultimos_ok
        cond4 = sobre_tres_emas and cerca_ema4

        if not (cond1 or cond2 or cond3 or cond4):
            return None

        entry = next((e for e in color_data if e["id"] == current_number), None)
        if not entry or entry["senal"] == "NO APOSTAR":
            return None

        prob = entry["rojo"] if expected_color == "ROJO" else entry["negro"]
        if entry["senal"] != expected_color or prob < prob_threshold:
            return None

        strength = "strong" if (cruce_alcista or cruce_ema8) else "moderate"
        return {
            "type": "SKRILL_2.0", "mode": "tendencia",
            "expected_color": expected_color,
            "probability": prob, "trigger_number": current_number, "strength": strength,
        }

    def check_signal_moderado(self, positions, color_data, current_number,
                              expected_color, prob_threshold) -> Optional[dict]:
        if len(positions) < 20:
            return None
        ahora = time.time()
        if ahora - self.last_signal_time < self.cooldown_seconds:
            return None
        if self.so_cooldown and ahora - self.so_cooldown < 8:
            return None

        ema4  = self.calculate_ema(positions, 4)
        ema8  = self.calculate_ema(positions, 8)
        ema20 = self.calculate_ema(positions, 20)

        if any(v is None for v in [ema4[-1], ema8[-1], ema20[-1], ema8[-2], ema20[-2]]):
            return None

        cruce_ema8   = ema8[-2] <= ema20[-2] and ema8[-1] > ema20[-1]
        sobre_emas   = positions[-1] > ema4[-1] and positions[-1] > ema8[-1]
        patron_v     = False
        if len(positions) >= 3:
            a, b, c  = positions[-3], positions[-2], positions[-1]
            patron_v = b < a and b < c and abs(a - c) <= 1 and c > a

        dos_ultimos_ok   = len(self.last_two_expected) >= 2 and all(self.last_two_expected)
        emas_alcistas    = ema4[-1] > ema8[-1] > ema20[-1]
        cond_racha       = dos_ultimos_ok and emas_alcistas and sobre_emas

        if not (cruce_ema8 or patron_v or cond_racha):
            return None

        entry = next((e for e in color_data if e["id"] == current_number), None)
        if not entry or entry["senal"] == "NO APOSTAR":
            return None

        prob = entry["rojo"] if expected_color == "ROJO" else entry["negro"]
        if entry["senal"] != expected_color or prob < prob_threshold:
            return None

        return {
            "type": "ALERTA_2.0", "mode": "moderado",
            "expected_color": expected_color,
            "probability": prob, "trigger_number": current_number,
            "pattern": "V" if patron_v else "EMA_CROSS",
        }

    def register_signal_sent(self):
        self.last_signal_time = time.time()

    def register_so_failed(self):
        self.so_cooldown = time.time()

# ─── STATISTICS ───────────────────────────────────────────────────────────────
class Stats:
    def __init__(self):
        self.total      = 0
        self.wins       = 0
        self.losses     = 0
        self.last_stats_at = 0
        self._h24: deque = deque()
        self.batch_start_bankroll = None
        self._wins_at_last_batch  = 0

    def record(self, is_win: bool, bankroll: float):
        self.total += 1
        if is_win:
            self.wins += 1
        else:
            self.losses += 1
        self._h24.append((time.time(), is_win, bankroll))
        self._trim24()

    def _trim24(self):
        cutoff = time.time() - 86400
        while self._h24 and self._h24[0][0] < cutoff:
            self._h24.popleft()

    def should_send_stats(self) -> bool:
        return (self.total - self.last_stats_at) >= 20

    def mark_stats_sent(self, bankroll: float):
        self.last_stats_at           = self.total
        self.batch_start_bankroll    = bankroll
        self._wins_at_last_batch     = self.wins

    def batch_stats(self, current_bankroll: float):
        n = self.total - self.last_stats_at
        w = self.wins  - self._wins_at_last_batch
        l = n - w
        e = round(w / n * 100, 1) if n else 0.0
        batch_bankroll = round(current_bankroll - self.batch_start_bankroll, 2) \
                         if self.batch_start_bankroll is not None else 0.0
        return w, l, n, e, batch_bankroll

    def stats_24h(self, current_bankroll: float):
        self._trim24()
        t = len(self._h24)
        w = sum(1 for _, iw, _ in self._h24 if iw)
        l = t - w
        e = round(w / t * 100, 1) if t else 0.0
        if t >= 2:
            bk24 = round(self._h24[-1][2] - self._h24[0][2], 2)
        else:
            bk24 = 0.0
        return w, l, t, e, bk24

# ─── SOPORTE Y RESISTENCIA ────────────────────────────────────────────────────
def find_support_resistance(levels: list, lookback: int = 30) -> dict:
    if len(levels) < lookback:
        return {'support': None, 'resistance': None}
    recent = levels[-lookback:]
    support_candidates    = []
    resistance_candidates = []
    for i in range(2, len(recent) - 2):
        if recent[i] < recent[i-1] and recent[i] < recent[i-2] and \
           recent[i] < recent[i+1] and recent[i] < recent[i+2]:
            support_candidates.append(recent[i])
        if recent[i] > recent[i-1] and recent[i] > recent[i-2] and \
           recent[i] > recent[i+1] and recent[i] > recent[i+2]:
            resistance_candidates.append(recent[i])
    return {
        'support':    support_candidates[-1]    if support_candidates    else None,
        'resistance': resistance_candidates[-1] if resistance_candidates else None,
    }

# ─── CHART GENERATION ─────────────────────────────────────────────────────────
def generate_chart(levels: list, spin_history: list, bet_color: str,
                   visible: int = VISIBLE,
                   markov_pred: Optional[dict] = None,
                   ml_pred: Optional[dict] = None) -> io.BytesIO:
    arr = np.array(levels, dtype=float)
    n   = len(arr)

    def calc_ema(data, period):
        if len(data) < period:
            return np.full(len(data), np.nan)
        mult = 2 / (period + 1)
        out  = np.full(len(data), np.nan)
        out[period - 1] = np.mean(data[:period])
        for i in range(period, len(data)):
            out[i] = (data[i] - out[i-1]) * mult + out[i-1]
        return out

    ema4  = calc_ema(arr, 4)
    ema8  = calc_ema(arr, 8)
    ema20 = calc_ema(arr, 20)

    start   = max(0, n - visible)
    sl      = slice(start, n)
    x       = np.arange(len(arr[sl]))
    hist_sl = spin_history[start:]

    is_rojo    = bet_color == "ROJO"
    bg         = "#0b101f"
    ax_bg      = "#0f1a2a"
    grid_c     = "#1e2e48"
    line_c     = "#e84040" if is_rojo else "#9090bb"
    ema4_c     = "#ff9f43"
    ema8_c     = "#48dbfb"
    ema20_c    = "#1dd1a1"
    title_c    = "#ff8080" if is_rojo else "#b0b8d0"

    fig, ax = plt.subplots(figsize=(8, 3.8), facecolor=bg)
    ax.set_facecolor(ax_bg)

    y   = arr[sl]
    e4  = ema4[sl]
    e8  = ema8[sl]
    e20 = ema20[sl]

    ax.fill_between(x, y, alpha=0.10, color=line_c)
    ax.plot(x, y,   color=line_c,  linewidth=0.8, zorder=3)
    ax.plot(x, e4,  color=ema4_c,  linewidth=0.7, linestyle="--", label="EMA 4",  zorder=4)
    ax.plot(x, e8,  color=ema8_c,  linewidth=0.7, linestyle="--", label="EMA 8",  zorder=4)
    ax.plot(x, e20, color=ema20_c, linewidth=1.0, label="EMA 20", zorder=4)

    dot_colors = {"ROJO": "#e84040", "NEGRO": "#aaaacc", "VERDE": "#2ecc71"}
    for i, spin in enumerate(hist_sl):
        c = dot_colors.get(spin["real"], "#ffffff")
        ax.scatter(i, y[i], color=c, s=22, zorder=5, edgecolors="white", linewidths=0.3)

    # Soporte y resistencia
    sr             = find_support_resistance(levels, lookback=30)
    support_val    = sr['support']
    resistance_val = sr['resistance']
    resistance_color = "#e84040" if is_rojo else "#888888"
    support_color    = "#888888" if is_rojo else "#e84040"

    if support_val is not None:
        ax.axhline(y=support_val, color=support_color, linestyle='--', linewidth=1.5, alpha=0.7)
        ax.text(x[-1], support_val, f' S {support_val:.1f}', color=support_color,
                fontsize=7, va='bottom', ha='right')
    if resistance_val is not None:
        ax.axhline(y=resistance_val, color=resistance_color, linestyle='--', linewidth=1.5, alpha=0.7)
        ax.text(x[-1], resistance_val, f' R {resistance_val:.1f}', color=resistance_color,
                fontsize=7, va='top', ha='right')

    tick_step = max(1, len(x) // 8)
    tick_x    = list(range(0, len(x), tick_step))
    tick_lbs  = [str(hist_sl[i]["number"]) if i < len(hist_sl) else "" for i in tick_x]
    ax.set_xticks(tick_x);    ax.set_xticklabels(tick_lbs, color="#8899bb", fontsize=7)
    ax.tick_params(axis='y', colors="#8899bb", labelsize=7)
    ax.tick_params(axis='x', colors="#8899bb", labelsize=7)
    ax.spines['bottom'].set_color(grid_c); ax.spines['left'].set_color(grid_c)
    ax.spines['top'].set_visible(False);   ax.spines['right'].set_visible(False)
    ax.grid(axis='y', color=grid_c, linewidth=0.4, alpha=0.5)

    # Título con info de predictores
    pred_info = ""
    if markov_pred:
        mv = markov_pred.get(bet_color, 0)
        pred_info += f" | Markov:{mv*100:.0f}%"
    if ml_pred:
        mlv = ml_pred.get(bet_color, 0)
        pred_info += f" | ML:{mlv*100:.0f}%"

    emoji = "🔴" if is_rojo else "⚫️"
    ax.set_title(
        f"{emoji} {'ROJO' if is_rojo else 'NEGRO'} — últimos {visible} giros · EMA 4/8/20{pred_info}",
        color=title_c, fontsize=8.5, pad=6
    )

    from matplotlib.lines import Line2D
    legend_els = [
        Line2D([0],[0], color=line_c,  linewidth=0.8, label="Nivel"),
        Line2D([0],[0], color=ema4_c,  linewidth=0.7, linestyle="--", label="EMA 4"),
        Line2D([0],[0], color=ema8_c,  linewidth=0.7, linestyle="--", label="EMA 8"),
        Line2D([0],[0], color=ema20_c, linewidth=1.0, label="EMA 20"),
        Line2D([0],[0], marker='o', color='w', markerfacecolor='#e84040', markersize=5, label="Rojo"),
        Line2D([0],[0], marker='o', color='w', markerfacecolor='#aaaacc', markersize=5, label="Negro"),
        Line2D([0],[0], marker='o', color='w', markerfacecolor='#2ecc71', markersize=5, label="Verde"),
    ]
    if support_val is not None:
        legend_els.append(Line2D([0],[0], color=support_color, linestyle='--', linewidth=1.5, label='Soporte'))
    if resistance_val is not None:
        legend_els.append(Line2D([0],[0], color=resistance_color, linestyle='--', linewidth=1.5, label='Resistencia'))

    ax.legend(handles=legend_els, loc="upper left", fontsize=6.5,
              facecolor="#0b101f", edgecolor=grid_c, labelcolor="white",
              framealpha=0.8, ncol=2)

    plt.tight_layout(pad=0.8)
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=120, facecolor=bg)
    plt.close(fig)
    buf.seek(0)
    return buf

# ─── TELEGRAM HELPERS ─────────────────────────────────────────────────────────
_TG_MAX_RETRIES = 5

def _tg_call(fn, *args, **kwargs):
    delay = 2.0
    for attempt in range(1, _TG_MAX_RETRIES + 1):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            err = str(e)
            if "retry after" in err.lower():
                try:
                    wait = int(''.join(filter(str.isdigit, err))) + 1
                except Exception:
                    wait = 30
                logger.warning(f"Telegram flood-wait {wait}s")
                time.sleep(wait)
                continue
            logger.warning(f"Telegram error (attempt {attempt}/{_TG_MAX_RETRIES}): {e}")
            if attempt < _TG_MAX_RETRIES:
                time.sleep(delay)
                delay = min(delay * 2, 60)
            else:
                logger.error(f"Telegram call failed after {_TG_MAX_RETRIES} attempts: {e}")
                return None

def tg_send_photo(chat_id, thread_id, photo_buf, caption) -> Optional[int]:
    photo_buf.seek(0)
    msg = _tg_call(bot.send_photo, chat_id=chat_id, photo=photo_buf,
                   caption=caption, parse_mode="HTML", message_thread_id=thread_id)
    return msg.message_id if msg else None

def tg_send_text(chat_id, thread_id, text) -> Optional[int]:
    msg = _tg_call(bot.send_message, chat_id=chat_id, text=text,
                   parse_mode="HTML", message_thread_id=thread_id)
    return msg.message_id if msg else None

def tg_delete(chat_id, msg_id):
    _tg_call(bot.delete_message, chat_id=chat_id, message_id=msg_id)

# ─── ROULETTE ENGINE ──────────────────────────────────────────────────────────
class RouletteEngine:
    def __init__(self, name: str, cfg: dict):
        self.name       = name
        self.ws_key     = cfg["ws_key"]
        self.chat_id    = cfg["chat_id"]
        self.thread_id  = cfg["thread_id"]
        self.color_data = cfg["color_data"]

        self.spin_history:      list = []
        self.original_levels:   list = []
        self.inverted_levels:   list = []
        self.last_nonzero_color: Optional[str] = None
        self.anti_block: set  = set()

        # ── Estado de la señal ─────────────────────────────────
        # signal_active: hay una apuesta en curso esperando resultado
        self.signal_active:   bool  = False
        # waiting_for_attempt: perdimos intento N, buscando condiciones para N+1
        self.waiting_for_attempt: bool = False
        self.waiting_attempt_number: int  = 0
        # skip_one_after_zero: cuando sale 0 en estado de espera, saltamos 1 giro
        self.skip_one_after_zero: bool = False

        self.expected_color:   Optional[str] = None
        self.bet_color:        Optional[str] = None
        self.attempts_left:    int  = 0
        self.total_attempts:   int  = 0
        self.trigger_number:   Optional[int] = None

        # IDs de mensajes Telegram en vuelo
        self.signal_msg_ids: list    = []
        self.waiting_msg_id: Optional[int] = None

        # ── D'Alembert ────────────────────────────────────────
        self.bet_sys = D_Alembert(BASE_BET)

        # ── Recuperación ──────────────────────────────────────
        self.consec_losses:   int   = 0
        self.recovery_active: bool  = False
        self.recovery_target: float = 0.0
        self.level1_bankroll: float = 0.0
        self.signal_is_level1: bool = False

        # ── AMX V20 ───────────────────────────────────────────
        self.amx_system = AMXSignalSystem(mode="moderado")
        self.min_prob_threshold = cfg.get("min_prob_threshold", 0.48)

        # ── Predictores ───────────────────────────────────────
        self.markov = MarkovChainPredictor(window=100, order=2)
        self.ml_predictor = MLPatternPredictor(pattern_length=4)

        self.stats = Stats()
        self.ws = None
        self.running = True

    # ── Helpers de configuración ──────────────────────────────────────────────
    def set_mode(self, mode: Literal["tendencia", "moderado"]):
        self.amx_system = AMXSignalSystem(mode=mode)
        logger.info(f"[{self.name}] Modo AMX V20 → {mode}")

    @staticmethod
    def calculate_ema(data: list, period: int) -> list:
        if len(data) < period:
            return [None] * len(data)
        mult = 2 / (period + 1)
        out  = [None] * (period - 1)
        prev = sum(data[:period]) / period
        out.append(prev)
        for i in range(period, len(data)):
            prev = (data[i] - prev) * mult + prev
            out.append(prev)
        return out

    def get_entry(self, number: int) -> Optional[dict]:
        for e in self.color_data:
            if e["id"] == number:
                return e
        return None

    def get_signal(self, number: int) -> Optional[str]:
        e = self.get_entry(number)
        return e["senal"] if e else None

    def get_prob(self, number: int, color: str) -> float:
        e = self.get_entry(number)
        if not e:
            return 0.0
        return e["rojo"] if color == "ROJO" else e["negro"]

    def _opposite_color(self, color: str) -> str:
        return "NEGRO" if color == "ROJO" else "ROJO"

    def _get_predictor_votes(self, color: str) -> int:
        """Retorna cuántos predictores (Markov, ML) votan a favor de este color (0-2)."""
        votes = 0
        mp = self.markov.predict(self.spin_history)
        if mp and mp.get(color, 0) > 0.50:
            votes += 1
        ml = self.ml_predictor.predict(self.spin_history)
        if ml and ml.get(color, 0) > 0.50:
            votes += 1
        return votes

    # ─── VERIFICACIÓN DE CONDICIONES PARA INTENTO 2/3 ────────────────────────
    def _check_retry_conditions(self, color: str, trigger_number: int) -> bool:
        """
        Evalúa si hay condiciones para apostar a `color` en un reintento.
        Condiciones:
          1. Entrada de tabla dice apostar a ese color y supera umbral de prob.
          2. El nivel del color está por encima de su EMA20 (tendencia alcista).
          3. Markov o ML confirman (al menos 1 voto) → si ambos predicen el
             color opuesto con >65% se filtra la señal.
        """
        # 1. Tabla
        entry = self.get_entry(trigger_number)
        if not entry or entry["senal"] == "NO APOSTAR":
            return False
        if entry["senal"] != color:
            return False
        prob = entry["rojo"] if color == "ROJO" else entry["negro"]
        if prob < self.min_prob_threshold:
            return False

        # 2. EMA20 uptrend
        levels = self.original_levels if color == "ROJO" else self.inverted_levels
        if len(levels) < 20:
            return False
        ema20 = self.calculate_ema(levels, 20)
        li    = len(levels) - 1
        if ema20[li] is None or levels[li] <= ema20[li]:
            return False

        # 3. Filtro de predictores: si ambos predicen el opuesto con alta
        #    confianza (>65%), descartamos la señal.
        opp = self._opposite_color(color)
        mp  = self.markov.predict(self.spin_history)
        ml  = self.ml_predictor.predict(self.spin_history)
        markov_contra = mp and mp.get(opp, 0) > 0.65
        ml_contra     = ml and ml.get(opp, 0) > 0.65
        if markov_contra and ml_contra:
            logger.info(f"[{self.name}] Condiciones {color} bloqueadas: Markov+ML contra")
            return False

        return True

    def _best_retry_color(self, trigger_number: int) -> Optional[str]:
        """
        Evalúa ambos colores para el reintento.
        Retorna el mejor color disponible o None si ninguno cumple condiciones.
        Orden de prioridad: mismo color → color opuesto.
        Desempate por votos Markov+ML.
        """
        same_ok = self._check_retry_conditions(self.bet_color, trigger_number)
        opp     = self._opposite_color(self.bet_color)
        opp_ok  = self._check_retry_conditions(opp, trigger_number)

        if same_ok and opp_ok:
            # Desempate por predictores
            same_votes = self._get_predictor_votes(self.bet_color)
            opp_votes  = self._get_predictor_votes(opp)
            chosen = self.bet_color if same_votes >= opp_votes else opp
            logger.info(f"[{self.name}] Ambos colores OK: {self.bet_color}({same_votes}) vs {opp}({opp_votes}) → {chosen}")
            return chosen
        if same_ok:
            return self.bet_color
        if opp_ok:
            logger.info(f"[{self.name}] Color opuesto seleccionado: {opp}")
            return opp
        return None

    # ─── DETECCIÓN DE SEÑAL AMX (intento 1) ──────────────────────────────────
    def _detect_amx_signal(self) -> Optional[dict]:
        if len(self.amx_system.ultimos_puntos) < 20:
            return None
        current_number = self.spin_history[-1]["number"] if self.spin_history else 0
        entry = self.get_entry(current_number)
        if not entry or entry["senal"] == "NO APOSTAR":
            return None
        expected_color = entry["senal"]

        recent_colors  = [s["real"] for s in self.spin_history[-5:]]
        momentum_count = 0
        for c in reversed(recent_colors):
            if c == expected_color:
                momentum_count += 1
            elif c != "VERDE":
                break
        if momentum_count < 2:
            return None

        try:
            if self.amx_system.mode == "tendencia":
                signal = self.amx_system.check_signal_tendencia(
                    self.amx_system.ultimos_puntos, self.color_data,
                    current_number, expected_color, self.min_prob_threshold)
            else:
                signal = self.amx_system.check_signal_moderado(
                    self.amx_system.ultimos_puntos, self.color_data,
                    current_number, expected_color, self.min_prob_threshold)
        except Exception as e:
            logger.warning(f"[{self.name}] Error AMX: {e}")
            return None

        if signal:
            # Confirmar con predictores (no obligatorio, pero registrar votos)
            votes = self._get_predictor_votes(expected_color)
            signal["predictor_votes"] = votes
        return signal

    def should_activate(self) -> Optional[str]:
        losses   = self.consec_losses
        min_spin = 22 + losses * 2
        if len(self.spin_history) < min_spin:
            return None

        last_num = self.spin_history[-1]["number"]
        entry    = self.get_entry(last_num)
        if not entry or entry["senal"] == "NO APOSTAR":
            return None
        expected = entry["senal"]

        if len(self.original_levels) < 20 or len(self.inverted_levels) < 20:
            return None

        ema4o  = self.calculate_ema(self.original_levels,  4)
        ema8o  = self.calculate_ema(self.original_levels,  8)
        ema20o = self.calculate_ema(self.original_levels, 20)
        ema4i  = self.calculate_ema(self.inverted_levels,  4)
        ema8i  = self.calculate_ema(self.inverted_levels,  8)
        ema20i = self.calculate_ema(self.inverted_levels, 20)

        req = min(3 + losses, 13)
        li  = len(self.original_levels) - 1

        def check(levels, e20, e8, e4, idx):
            for off in range(req):
                i = idx - (req - 1) + off
                if i < 0 or i >= len(levels) or i >= len(e20):
                    return False
                if e20[i] is None or levels[i] <= e20[i]:
                    return False
                if losses >= 2:
                    if i >= len(e8) or e8[i] is None or levels[i] <= e8[i]:
                        return False
                if losses >= 4:
                    if i >= len(e4) or e4[i] is None or levels[i] <= e4[i]:
                        return False
            return True

        if expected == "ROJO"  and check(self.original_levels, ema20o, ema8o, ema4o, li):
            return "ROJO"
        if expected == "NEGRO" and check(self.inverted_levels, ema20i, ema8i, ema4i, li):
            return "NEGRO"
        return None

    def determine_bet_color(self, expected: str) -> str:
        if len(self.spin_history) < 20:
            return expected
        ema20o = self.calculate_ema(self.original_levels, 20)
        ema20i = self.calculate_ema(self.inverted_levels, 20)
        li = len(self.original_levels) - 1
        if li < 0 or li >= len(ema20o) or li >= len(ema20i):
            return expected
        if ema20o[li] is None or ema20i[li] is None:
            return expected
        last_sig = self.get_signal(self.spin_history[-1]["number"])
        if expected == "ROJO":
            if self.original_levels[li] < ema20o[li]:
                return "NEGRO" if last_sig == "NEGRO" else "ROJO"
            return "ROJO"
        else:
            if self.inverted_levels[li] < ema20i[li]:
                return "ROJO" if last_sig == "ROJO" else "NEGRO"
            return "NEGRO"

    # ─── RECUPERACIÓN ─────────────────────────────────────────────────────────
    def _check_recovery(self):
        if not self.recovery_active:
            return
        if self.bet_sys.bankroll >= self.recovery_target:
            logger.info(f"[{self.name}] Recuperación completada! "
                        f"bankroll={self.bet_sys.bankroll:.2f} >= objetivo={self.recovery_target:.2f}")
            self.consec_losses   = 0
            self.recovery_active = False
            self.recovery_target = 0.0
            self.bet_sys.step    = 0

    # ─── AMX POSITIONS ────────────────────────────────────────────────────────
    def _update_amx_positions(self, color: str):
        last_pos = self.amx_system.ultimos_puntos[-1] if self.amx_system.ultimos_puntos else 0
        if color == "ROJO":
            new_pos = last_pos + 1
        elif color == "NEGRO":
            new_pos = last_pos - 1
        else:
            new_pos = last_pos
        self.amx_system.ultimos_puntos.append(new_pos)
        if len(self.amx_system.ultimos_puntos) > 300:
            self.amx_system.ultimos_puntos = self.amx_system.ultimos_puntos[-200:]

    # ─── ENVÍO DE MENSAJES ────────────────────────────────────────────────────
    def _send_signal(self, trigger: int, attempt: int, amx_signal: Optional[dict] = None):
        bet        = self.bet_sys.current_bet()
        prob       = int(self.get_prob(trigger, self.bet_color) * 100)
        color_icon = "🔴" if self.bet_color == "ROJO" else "⚫️"
        step       = self.bet_sys.step + 1

        self.signal_is_level1 = (self.bet_sys.step == 0 and not self.recovery_active)
        if self.signal_is_level1:
            self.level1_bankroll = self.bet_sys.bankroll

        sys_line = f"🌀 <i>D'Alembert paso {step} de 20</i>\n"
        amx_line = ""
        pred_line = ""
        if amx_signal:
            mode_icon = "📈" if amx_signal["mode"] == "tendencia" else "📊"
            amx_line  = f"{mode_icon} <i>AMX V20 • {amx_signal['mode'].upper()}</i>\n"
            votes = amx_signal.get("predictor_votes", 0)
            if votes > 0:
                pred_line = f"🤖 <i>Markov+ML confirman: {votes}/2 votos</i>\n"

        caption = (
            f"✅☑️ <b>SEÑAL CONFIRMADA</b> ☑️✅\n\n"
            f"🎰 <b>Juego: {self.name}</b>\n"
            f"👉 <b>Después de: {trigger}</b>\n"
            f"🎯 <b>Apostar a: {self.bet_color}</b> {color_icon}\n\n"
            f"💡 <i>Probabilidad tabla: {prob}%</i>\n"
            f"{sys_line}"
            f"{amx_line}"
            f"{pred_line}"
            f"📍 <i>Apuesta: {bet:.2f} usd</i>\n\n"
            f"♻️ <i>Intento {attempt}/{MAX_ATTEMPTS}</i>\n"
        )
        levels = self.original_levels[:] if self.bet_color == "ROJO" else self.inverted_levels[:]
        mp     = self.markov.predict(self.spin_history)
        ml     = self.ml_predictor.predict(self.spin_history)
        chart  = generate_chart(levels, self.spin_history[:], self.bet_color,
                                markov_pred=mp, ml_pred=ml)
        msg_id = tg_send_photo(self.chat_id, self.thread_id, chart, caption)
        if msg_id:
            self.signal_msg_ids.append(msg_id)
        logger.info(f"[{self.name}] Signal sent: {self.bet_color} after {trigger}, "
                    f"bet={bet:.2f}, step={step}, recovery={self.recovery_active}")

    def _send_retry_signal(self, trigger: int, new_bet: float, attempt_number: int):
        prob       = int(self.get_prob(trigger, self.bet_color) * 100)
        color_icon = "🔴" if self.bet_color == "ROJO" else "⚫️"
        step       = self.bet_sys.step + 1
        sys_line   = f"🌀 <i>D'Alembert paso {step} de 20</i>\n"
        recovery   = " 🔄 (modo recuperación)" if self.recovery_active else ""
        votes      = self._get_predictor_votes(self.bet_color)
        pred_line  = f"🤖 <i>Markov+ML: {votes}/2 votos</i>\n" if votes > 0 else ""

        caption = (
            f"✅☑️ <b>SEÑAL CONFIRMADA</b> ☑️✅\n\n"
            f"🎰 <b>Juego: {self.name}</b>\n"
            f"👉🏼 <b>Después de: {trigger}</b>\n"
            f"🎯 <b>Apostar a: {self.bet_color}</b> {color_icon}{recovery}\n\n"
            f"💡 <i>Probabilidad tabla: {prob}%</i>\n"
            f"{sys_line}"
            f"{pred_line}"
            f"📍 <i>Apuesta: {new_bet:.2f} usd</i>\n\n"
            f"♻️ <i>Intento {attempt_number}/{MAX_ATTEMPTS}</i>\n"
        )
        levels = self.original_levels[:] if self.bet_color == "ROJO" else self.inverted_levels[:]
        mp     = self.markov.predict(self.spin_history)
        ml     = self.ml_predictor.predict(self.spin_history)
        chart  = generate_chart(levels, self.spin_history[:], self.bet_color,
                                markov_pred=mp, ml_pred=ml)
        msg_id = tg_send_photo(self.chat_id, self.thread_id, chart, caption)
        if msg_id:
            self.signal_msg_ids.append(msg_id)
        logger.info(f"[{self.name}] Retry {attempt_number}/{MAX_ATTEMPTS}: "
                    f"{self.bet_color} after {trigger}, bet={new_bet:.2f}")

    def _send_waiting_message(self, attempt_number: int):
        """
        Envía el mensaje de espera con el gráfico actualizado.
        Elimina mensajes anteriores de la señal antes de enviar.
        """
        # Borrar mensajes anteriores de la señal
        for msg_id in self.signal_msg_ids:
            tg_delete(self.chat_id, msg_id)
        self.signal_msg_ids = []

        # Si ya hay un waiting_msg, borrarlo también
        if self.waiting_msg_id:
            tg_delete(self.chat_id, self.waiting_msg_id)
            self.waiting_msg_id = None

        ord_str = "2°" if attempt_number == 2 else "3°"
        caption = (
            f"⚠️ <b>Esperando condiciones para el {ord_str} intento</b>\n\n"
            f"🎰 <b>{self.name}</b>\n"
            f"🔍 <i>Analizando ROJO 🔴 y NEGRO ⚫️ cada giro...</i>\n"
        )
        # Gráfico con el color de bet actual (el que aún seguimos monitoreando)
        levels = self.original_levels[:] if self.bet_color == "ROJO" else self.inverted_levels[:]
        mp     = self.markov.predict(self.spin_history)
        ml     = self.ml_predictor.predict(self.spin_history)
        chart  = generate_chart(levels, self.spin_history[:], self.bet_color,
                                markov_pred=mp, ml_pred=ml)
        msg_id = tg_send_photo(self.chat_id, self.thread_id, chart, caption)
        if msg_id:
            self.waiting_msg_id = msg_id
        logger.info(f"[{self.name}] Waiting for attempt {attempt_number}")

    def _send_result(self, number: int, real: str, won: bool, bet: float):
        bankroll = self.bet_sys.bankroll
        icon = "🔴" if real == "ROJO" else ("⚫️" if real == "NEGRO" else "🟢")
        if won:
            text = f"💎 <b>RESULTADO: {number}</b> {icon}\n💰 <i>Bankroll Actual: {bankroll:.2f} usd</i>\n"
        else:
            text = f"❌ <b>RESULTADO: {number}</b> {icon}\n💰 <i>Bankroll Actual: {bankroll:.2f} usd</i>\n"
        tg_send_text(self.chat_id, self.thread_id, text)
        logger.info(f"[{self.name}] Result: {'WIN' if won else 'LOSS'} #{number}, bankroll={bankroll:.2f}")

    def _check_stats(self):
        if not self.stats.should_send_stats():
            return
        current_bankroll = self.bet_sys.bankroll
        w20, l20, t20, e20, batch_bankroll = self.stats.batch_stats(current_bankroll)
        self.stats.mark_stats_sent(current_bankroll)
        w24, l24, t24, e24, bk24 = self.stats.stats_24h(current_bankroll)
        text = (
            f"👉🏼 <b>ESTADÍSTICAS {t20} SEÑALES</b>\n"
            f"🈯️ <b>W: {w20}</b> 🈲 <b>L: {l20}</b> 🈺 <b>T: {t20}</b> 📈 <b>E: {e20}%</b>\n"
            f"💰 <i>Bankroll acumulado: {batch_bankroll:.2f} usd</i>\n\n"
            f"👉🏼 <b>ESTADÍSTICAS 24 HORAS</b>\n"
            f"🈯️ <b>W: {w24}</b> 🈲 <b>L: {l24}</b> 🈺 <b>T: {t24}</b> 📈 <b>E: {e24}%</b>\n"
            f"💰 <i>Bankroll acumulado: {bk24:.2f} usd</i>\n"
        )
        tg_send_text(self.chat_id, self.thread_id, text)

    # ─── PROCESO PRINCIPAL DE CADA NÚMERO ────────────────────────────────────
    def process_number(self, number: int):
        real = REAL_COLOR_MAP.get(number, "VERDE")

        # Actualizar historial de giros
        self.spin_history.append({"number": number, "real": real})
        if len(self.spin_history) > 300:
            self.spin_history.pop(0)

        # Actualizar niveles originales e invertidos
        last_o = self.original_levels[-1] if self.original_levels else 0
        last_i = self.inverted_levels[-1] if self.inverted_levels else 0

        if number == 0:
            if self.last_nonzero_color:
                self.original_levels.append(last_o + (1 if self.last_nonzero_color == "ROJO" else -1))
                self.inverted_levels.append(last_i + (1 if self.last_nonzero_color == "NEGRO" else -1))
            else:
                self.original_levels.append(last_o)
                self.inverted_levels.append(last_i)
        else:
            self.original_levels.append(last_o + (1 if real == "ROJO" else -1))
            self.inverted_levels.append(last_i + (1 if real == "NEGRO" else -1))
            self.last_nonzero_color = real

        while len(self.original_levels) > len(self.spin_history):
            self.original_levels.pop(0)
        while len(self.inverted_levels) > len(self.spin_history):
            self.inverted_levels.pop(0)
        min_len = min(len(self.original_levels), len(self.inverted_levels))
        self.original_levels = self.original_levels[-min_len:]
        self.inverted_levels = self.inverted_levels[-min_len:]

        # Actualizar AMX positions y streak
        self._update_amx_positions(real)
        expected_signal = self.get_signal(number)
        self.amx_system.update_streak(real, expected_signal)

        # Actualizar predictores (Markov: 100 giros; ML: historial completo)
        self.markov.update(self.spin_history)          # O(100)
        self.ml_predictor.add_spin(self.spin_history)  # O(1)

        # ══════════════════════════════════════════════════════════════════════
        #  MÁQUINA DE ESTADOS
        # ══════════════════════════════════════════════════════════════════════

        # ── ESTADO 1: Señal activa, esperando resultado ────────────────────
        if self.signal_active:
            is_win = (
                (self.bet_color == "ROJO"  and real == "ROJO") or
                (self.bet_color == "NEGRO" and real == "NEGRO")
            )

            if is_win:
                # ── GANAMOS ─────────────────────────────────────────────────
                bet = self.bet_sys.win()
                self.stats.record(True, self.bet_sys.bankroll)

                # Borrar mensajes de intentos anteriores (excepto el último)
                if len(self.signal_msg_ids) > 1:
                    for msg_id in self.signal_msg_ids[:-1]:
                        tg_delete(self.chat_id, msg_id)
                    self.signal_msg_ids = [self.signal_msg_ids[-1]]

                self.signal_active = False
                self._check_recovery()
                self._send_result(number, real, True, bet)
                self._check_stats()
                self.signal_msg_ids = []

            else:
                # ── PERDEMOS ─────────────────────────────────────────────────
                self.attempts_left -= 1
                bet = self.bet_sys.loss()

                if self.attempts_left <= 0:
                    # ── PÉRDIDA FINAL (intento 3 fallido) ───────────────────
                    self.consec_losses += 1
                    if self.consec_losses >= 10:
                        self.consec_losses   = 0
                        self.recovery_active = False
                        self.recovery_target = 0.0
                    else:
                        self.recovery_active = True
                        self.recovery_target = self.level1_bankroll + BASE_BET
                    self.stats.record(False, self.bet_sys.bankroll)
                    self.signal_active = False
                    self._send_result(number, real, False, bet)
                    self._check_stats()
                    self.signal_msg_ids = []

                else:
                    # ── INTENTAMOS CONTINUAR (intento 2 ó 3) ───────────────
                    attempt_number = MAX_ATTEMPTS - self.attempts_left + 1

                    # Borrar el mensaje del intento anterior
                    if self.signal_msg_ids:
                        last_id = self.signal_msg_ids.pop()
                        tg_delete(self.chat_id, last_id)

                    # Si salió CERO → entrar en espera y saltar 1 giro
                    if real == "VERDE":
                        self.signal_active = False
                        self.waiting_for_attempt  = True
                        self.waiting_attempt_number = attempt_number
                        self.skip_one_after_zero  = True
                        self._send_waiting_message(attempt_number)
                        return

                    # Buscar el mejor color (mismo u opuesto)
                    chosen = self._best_retry_color(number)

                    if chosen is not None:
                        self.bet_color      = chosen
                        self.trigger_number = number
                        self._send_retry_signal(number, self.bet_sys.current_bet(), attempt_number)
                    else:
                        # Sin condiciones para ningún color → esperar
                        self.signal_active          = False
                        self.waiting_for_attempt    = True
                        self.waiting_attempt_number = attempt_number
                        self._send_waiting_message(attempt_number)

        # ── ESTADO 2: Esperando condiciones para intento 2 ó 3 ───────────────
        elif self.waiting_for_attempt:
            # Si salió CERO → saltamos el próximo giro
            if real == "VERDE":
                self.skip_one_after_zero = True
                return

            # El giro inmediatamente posterior a un cero → lo saltamos
            if self.skip_one_after_zero:
                self.skip_one_after_zero = False
                return

            attempt_number = self.waiting_attempt_number
            chosen = self._best_retry_color(number)

            if chosen is not None:
                # ¡Condiciones dadas! Borrar mensaje de espera y lanzar intento
                if self.waiting_msg_id:
                    tg_delete(self.chat_id, self.waiting_msg_id)
                    self.waiting_msg_id = None

                self.bet_color          = chosen
                self.trigger_number     = number
                self.signal_active      = True
                self.waiting_for_attempt = False
                self._send_retry_signal(number, self.bet_sys.current_bet(), attempt_number)
            # else: seguir esperando (sin mensaje duplicado)

        # ── ESTADO 3: Idle – buscar señal para intento 1 ─────────────────────
        else:
            self.signal_msg_ids = []

            signal = self._detect_amx_signal()

            if signal:
                self.signal_active   = True
                self.expected_color  = signal["expected_color"]
                self.bet_color       = signal["expected_color"]
                self.attempts_left   = MAX_ATTEMPTS
                self.total_attempts  = MAX_ATTEMPTS
                self.trigger_number  = signal["trigger_number"]
                self._send_signal(signal["trigger_number"], 1, amx_signal=signal)
                self.amx_system.register_signal_sent()
            else:
                expected = self.should_activate()
                if expected:
                    self.signal_active  = True
                    self.expected_color = expected
                    self.bet_color      = self.determine_bet_color(expected)
                    self.attempts_left  = MAX_ATTEMPTS
                    self.total_attempts = MAX_ATTEMPTS
                    self.trigger_number = number
                    self._send_signal(number, 1)

    # ─── WEBSOCKET ────────────────────────────────────────────────────────────
    async def run_ws(self):
        reconnect_delay = 5
        while self.running:
            try:
                async with websockets.connect(
                    WS_URL, ping_interval=30, ping_timeout=60, close_timeout=10
                ) as ws:
                    self.ws = ws
                    reconnect_delay = 5
                    logger.info(f"[{self.name}] WS connected")
                    await ws.send(json.dumps({
                        "type": "subscribe",
                        "casinoId": CASINO_ID,
                        "currency": "USD",
                        "key": [self.ws_key],
                    }))
                    async for message in ws:
                        if not self.running:
                            break
                        try:
                            data = json.loads(message)
                        except Exception:
                            continue

                        # Carga inicial: últimos 20 resultados
                        if "last20Results" in data and isinstance(data["last20Results"], list):
                            tmp = []
                            for r in data["last20Results"]:
                                gid = r.get("gameId")
                                num = r.get("result")
                                if gid and num is not None:
                                    try:
                                        n = int(num)
                                    except Exception:
                                        continue
                                    if 0 <= n <= 36 and gid not in self.anti_block:
                                        tmp.append((gid, n))
                                        if len(self.anti_block) > 1000:
                                            self.anti_block.clear()
                                        self.anti_block.add(gid)
                            for gid, n in reversed(tmp):
                                self.process_number(n)

                        # Nuevo resultado en tiempo real — verificado por game_id
                        gid = data.get("gameId")
                        res = data.get("result")
                        if gid and res is not None:
                            try:
                                n = int(res)
                            except Exception:
                                continue
                            if 0 <= n <= 36 and gid not in self.anti_block:
                                if len(self.anti_block) > 1000:
                                    self.anti_block.clear()
                                self.anti_block.add(gid)
                                self.process_number(n)

            except Exception as e:
                logger.warning(f"[{self.name}] WS error: {e}. Reconnecting in {reconnect_delay}s")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 60)

# ─── FLASK KEEPALIVE ──────────────────────────────────────────────────────────
app = Flask(__name__)

@app.route("/")
def index():
    return jsonify({"status": "ok", "bot": "Roulette Signal Bot AMX V20.2", "ts": time.time()})

@app.route("/ping")
def ping():
    return jsonify({"pong": True, "ts": time.time()})

@app.route("/health")
def health():
    return jsonify({"healthy": True})

# ─── SELF-PING ────────────────────────────────────────────────────────────────
async def self_ping_loop():
    port     = int(os.environ.get("PORT", 10000))
    url      = os.environ.get("RENDER_EXTERNAL_URL", f"http://localhost:{port}")
    ping_url = f"{url}/ping"
    while True:
        await asyncio.sleep(300)
        try:
            with urllib.request.urlopen(ping_url, timeout=10) as r:
                logger.info(f"Self-ping OK: {r.status}")
        except Exception as e:
            logger.warning(f"Self-ping failed: {e}")

# ─── COMANDOS TELEGRAM ────────────────────────────────────────────────────────
engines: dict[str, RouletteEngine] = {}

@bot.message_handler(commands=['start', 'help'])
def cmd_start(message):
    help_text = """
<b>🎰 Roulette Bot - Sistema AMX V20.2</b>

<b>Novedades V2:</b>
• Intento 2/3 evalúa color opuesto si el original no cumple condiciones
• Espera indefinida con gráfico hasta que algún color esté listo
• Markov Chain (últimos 100 giros) + ML (historial completo)
• Verificación por game_id — sin temporizadores

Comandos:
/moderado - Activa modo MODERADO
/tendencia - Activa modo TENDENCIA
/status - Estado de todas las ruletas
/reset - Resetea estadísticas
/help - Muestra esta ayuda
    """
    bot.reply_to(message, help_text, parse_mode="HTML")

@bot.message_handler(commands=['moderado'])
def cmd_moderado(message):
    changed = []
    for name, engine in engines.items():
        old = engine.amx_system.mode
        engine.set_mode("moderado")
        if old != "moderado":
            changed.append(name)
    text = (f"✅ <b>Modo MODERADO activado</b>\n\nRuletas: {', '.join(changed)}"
            if changed else "📊 <b>Todas las ruletas en modo MODERADO</b>")
    bot.reply_to(message, text, parse_mode="HTML")

@bot.message_handler(commands=['tendencia'])
def cmd_tendencia(message):
    changed = []
    for name, engine in engines.items():
        old = engine.amx_system.mode
        engine.set_mode("tendencia")
        if old != "tendencia":
            changed.append(name)
    text = (f"📈 <b>Modo TENDENCIA activado</b>\n\nRuletas: {', '.join(changed)}"
            if changed else "📈 <b>Todas las ruletas en modo TENDENCIA</b>")
    bot.reply_to(message, text, parse_mode="HTML")

@bot.message_handler(commands=['status'])
def cmd_status(message):
    lines = ["<b>📊 ESTADO</b>\n"]
    for name, engine in engines.items():
        mode_icon = "📈" if engine.amx_system.mode == "tendencia" else "📊"
        if engine.signal_active:
            st = f"🟢 Activo ({engine.bet_color}, intento {MAX_ATTEMPTS - engine.attempts_left + 1}/{MAX_ATTEMPTS})"
        elif engine.waiting_for_attempt:
            st = f"⏳ Esperando intento {engine.waiting_attempt_number}/{MAX_ATTEMPTS}"
        else:
            st = "⚪ Idle"
        lines.append(f"<b>{name}</b>: {mode_icon} {engine.amx_system.mode} — {st}")
    bot.reply_to(message, "\n".join(lines), parse_mode="HTML")

@bot.message_handler(commands=['reset'])
def cmd_reset(message):
    for engine in engines.values():
        engine.stats = Stats()
    bot.reply_to(message, "🔄 <b>Estadísticas reseteadas</b>", parse_mode="HTML")

# ─── MAIN ─────────────────────────────────────────────────────────────────────
def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)

async def main():
    global engines
    engines = {name: RouletteEngine(name, cfg) for name, cfg in ROULETTE_CONFIGS.items()}
    tasks   = [asyncio.create_task(e.run_ws()) for e in engines.values()]
    tasks.append(asyncio.create_task(self_ping_loop()))

    def telegram_polling():
        logger.info("Iniciando polling de Telegram...")
        bot.polling(none_stop=True, interval=1, timeout=30)

    tg_thread = threading.Thread(target=telegram_polling, daemon=True)
    tg_thread.start()

    logger.info("🎰 Roulette Bot AMX V20.2 iniciado")
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    logger.info("Flask server started.")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped.")
