#!/usr/bin/env python3
"""
Roulette Telegram Signal Bot - Sistema AMX V20 + ML + Markov Chain
Incluye re‑evaluación de inversión en cada intento (2 y 3).
"""

import asyncio
import io
import json
import logging
import threading
import time
from collections import deque
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
from telebot import apihelper

# ─── FUNCIÓN DE ESCAPE HTML PERSONALIZADA ────────────────────────────────────
def escape_html(text: str) -> str:
    """Escapa caracteres especiales para Telegram HTML."""
    if text is None:
        return ""
    return (str(text)
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;"))

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
    total=5, backoff_factor=1.5,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "POST"], raise_on_status=False,
)
_adapter = HTTPAdapter(max_retries=_retry, pool_connections=10, pool_maxsize=20)
_session.mount("https://", _adapter)
_session.mount("http://",  _adapter)

bot = telebot.TeleBot(TOKEN, threaded=False)
bot.session = _session

import telebot.apihelper as apihelper
apihelper.CONNECT_TIMEOUT = 10
apihelper.READ_TIMEOUT = 60

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
MAX_WAIT_SPINS = 10

# ══════════════════════════════════════════════════════════════════════════════
# ─── MARKOV CHAIN (ORDEN 2) ────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════
class MarkovChain:
    COLORS = ("ROJO", "NEGRO")

    def __init__(self, order: int = 2, laplace_alpha: float = 1.0):
        self.order = order
        self.alpha = laplace_alpha
        self.transitions: dict = {}
        self._history: deque = deque(maxlen=500)

    def update(self, color: str):
        if color not in self.COLORS:
            return
        self._history.append(color)
        if len(self._history) < self.order + 1:
            return
        recent = list(self._history)
        state = tuple(recent[-(self.order + 1):-1])
        next_c = recent[-1]
        if state not in self.transitions:
            self.transitions[state] = {c: 0 for c in self.COLORS}
        self.transitions[state][next_c] += 1

    def predict(self) -> dict:
        if len(self._history) < self.order:
            return {c: 0.5 for c in self.COLORS}
        state = tuple(list(self._history)[-self.order:])
        counts = self.transitions.get(state, {c: 0 for c in self.COLORS})
        total = sum(counts.values()) + self.alpha * len(self.COLORS)
        return {c: (counts.get(c, 0) + self.alpha) / total for c in self.COLORS}

    def confidence(self) -> float:
        if len(self._history) < self.order:
            return 0.0
        state = tuple(list(self._history)[-self.order:])
        if state not in self.transitions:
            return 0.0
        return float(sum(self.transitions[state].values()))

    def state_info(self) -> str:
        if len(self._history) < self.order:
            return "Sin datos suficientes"
        state = tuple(list(self._history)[-self.order:])
        probs = self.predict()
        conf = self.confidence()
        return (f"Estado: {state} | "
                f"R={probs['ROJO']:.2f} N={probs['NEGRO']:.2f} | "
                f"Obs={conf:.0f}")


# ══════════════════════════════════════════════════════════════════════════════
# ─── ONLINE LOGISTIC REGRESSION (SGD) ────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════
class OnlineLogisticRegression:
    N_FEATURES = 12

    def __init__(self, lr: float = 0.05, reg: float = 0.005, min_samples: int = 30):
        self.weights = np.zeros(self.N_FEATURES)
        self.bias    = 0.0
        self.lr      = lr
        self.reg     = reg
        self.min_samples = min_samples
        self.n_samples   = 0
        self._feat_mean = np.zeros(self.N_FEATURES)
        self._feat_var  = np.ones(self.N_FEATURES)
        self._feat_n    = 0

    def _update_stats(self, x: np.ndarray):
        self._feat_n += 1
        delta = x - self._feat_mean
        self._feat_mean += delta / self._feat_n
        delta2 = x - self._feat_mean
        self._feat_var += delta * delta2

    def _normalize(self, x: np.ndarray) -> np.ndarray:
        if self._feat_n < 2:
            return x
        std = np.sqrt(self._feat_var / max(self._feat_n - 1, 1))
        std = np.where(std < 1e-8, 1.0, std)
        return (x - self._feat_mean) / std

    @staticmethod
    def _sigmoid(z: float) -> float:
        return 1.0 / (1.0 + np.exp(-np.clip(z, -15, 15)))

    def predict_proba(self, raw_features: np.ndarray) -> float:
        x = self._normalize(raw_features)
        return self._sigmoid(float(np.dot(self.weights, x)) + self.bias)

    def update(self, raw_features: np.ndarray, label: int):
        self._update_stats(raw_features)
        x = self._normalize(raw_features)
        pred  = self._sigmoid(float(np.dot(self.weights, x)) + self.bias)
        error = label - pred
        self.weights += self.lr * (error * x - self.reg * self.weights)
        self.bias    += self.lr * error
        self.n_samples += 1

    @property
    def ready(self) -> bool:
        return self.n_samples >= self.min_samples

    def summary(self) -> str:
        return f"LR={self.lr} | n={self.n_samples} | ready={self.ready}"


# ══════════════════════════════════════════════════════════════════════════════
# ─── ML SIGNAL FILTER  (integra Markov + LR) ─────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════
class MLSignalFilter:
    N_FEATURES = 12

    def __init__(
        self,
        markov_order: int = 2,
        markov_threshold: float = 0.52,
        ml_threshold: float = 0.55,
        ml_threshold_retry: float = 0.48,
        ml_min_samples: int = 30,
    ):
        self.markov = MarkovChain(order=markov_order)
        self.model  = OnlineLogisticRegression(min_samples=ml_min_samples)
        self.markov_threshold   = markov_threshold
        self.ml_threshold       = ml_threshold
        self.ml_threshold_retry = ml_threshold_retry
        self._last_features: Optional[np.ndarray] = None

    def extract_features(
        self,
        bet_color: str,
        tabla_prob: float,
        ema4: list, ema8: list, ema20: list,
        positions: list,
        momentum_count: int,
        consec_losses: int,
        bet_step: int,
        last_two_expected: deque,
        recovery_active: bool,
    ) -> np.ndarray:
        li = len(ema4) - 1
        safe = (li >= 0
                and ema4[li] is not None
                and ema8[li] is not None
                and ema20[li] is not None)

        f0 = tabla_prob
        f1 = self.markov.predict().get(bet_color, 0.5)
        f2 = min(self.markov.confidence(), 50.0) / 50.0
        f3 = float(ema4[li] > ema20[li]) if safe else 0.5
        f4 = float(ema8[li] > ema20[li]) if safe else 0.5
        f5 = float(ema4[li] > ema8[li])  if safe else 0.5

        above5 = 0.0
        if safe and len(positions) >= 5:
            cutoff = max(0, len(positions) - 5)
            e20_cut = max(0, li - 4)
            for k in range(5):
                pi = cutoff + k
                ei = e20_cut + k
                if (pi < len(positions)
                        and ei < len(ema20)
                        and ema20[ei] is not None
                        and positions[pi] > ema20[ei]):
                    above5 += 1.0
        f7 = above5 / 5.0

        f6  = min(momentum_count, 5) / 5.0
        f8  = min(consec_losses, 10) / 10.0
        f9  = min(bet_step, 20) / 20.0
        f10 = (sum(1 for v in last_two_expected if v) / max(len(last_two_expected), 1)
               if last_two_expected else 0.5)
        f11 = float(recovery_active)

        feats = np.array([f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11],
                         dtype=np.float32)
        self._last_features = feats
        return feats

    def should_emit_signal(
        self,
        features: np.ndarray,
        bet_color: str,
        is_retry: bool = False,
    ) -> tuple[bool, float, float, str]:
        markov_prob = self.markov.predict().get(bet_color, 0.5)
        if markov_prob < self.markov_threshold:
            return False, markov_prob, 0.0, (
                f"Markov bloqueó: {markov_prob:.2f} < {self.markov_threshold:.2f}"
            )
        if not self.model.ready:
            return True, markov_prob, 0.0, (
                f"Markov OK ({markov_prob:.2f}), ML en warm-up "
                f"({self.model.n_samples}/{self.model.min_samples})"
            )
        ml_prob  = self.model.predict_proba(features)
        threshold = self.ml_threshold_retry if is_retry else self.ml_threshold
        if ml_prob < threshold:
            return False, markov_prob, ml_prob, (
                f"ML bloqueó: {ml_prob:.2f} < {threshold:.2f}"
            )
        return True, markov_prob, ml_prob, (
            f"Aprobado — Markov={markov_prob:.2f} ML={ml_prob:.2f}"
        )

    def update_result(self, won: bool):
        if self._last_features is not None:
            self.model.update(self._last_features, int(won))

    def observe_color(self, color: str):
        self.markov.update(color)

    def info(self) -> str:
        return (f"Markov: {self.markov.state_info()} | "
                f"ML: {self.model.summary()}")


# ─── D'ALEMBERT ──────────────────────────────────────────────────────────────
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
        self.last_two_expected: deque = deque(maxlen=2)
        self.last_two_colors: deque = deque(maxlen=2)

    def update_streak(self, real_color: str, expected_color: Optional[str]):
        if expected_color:
            self.last_two_expected.append(real_color == expected_color)
        self.last_two_colors.append(real_color)

    def calculate_ema(self, data: list, period: int) -> list:
        if len(data) < period:
            return [None] * len(data)
        mult = 2 / (period + 1)
        ema = [None] * (period - 1)
        prev = sum(data[:period]) / period
        ema.append(prev)
        for i in range(period, len(data)):
            prev = (data[i] * mult) + (prev * (1 - mult))
            ema.append(prev)
        return ema

    def check_signal_tendencia(self, positions, color_data, current_number,
                               expected_color, prob_threshold):
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

        if (ema4[-1] is None or ema8[-1] is None or ema20[-1] is None or
                ema4[-2] is None or ema8[-2] is None or ema20[-2] is None):
            return None

        current_pos = positions[-1]
        cruce_alcista  = ema4[-2] <= ema20[-2] and ema4[-1] > ema20[-1]
        sobre_tres_emas = (current_pos > ema4[-1] and current_pos > ema8[-1] and
                           current_pos > ema20[-1])
        cruce_ema8 = ema8[-2] <= ema20[-2] and ema8[-1] > ema20[-1]
        cerca_ema4 = abs(current_pos - ema4[-1]) <= 0.5
        dos_ultimos = len(self.last_two_expected) >= 2 and all(self.last_two_expected)

        if not (cruce_alcista or sobre_tres_emas or cruce_ema8 or
                (sobre_tres_emas and dos_ultimos) or (sobre_tres_emas and cerca_ema4)):
            return None

        entry = next((e for e in color_data if e["id"] == current_number), None)
        if not entry or entry["senal"] == "NO APOSTAR":
            return None
        prob = entry["rojo"] if expected_color == "ROJO" else entry["negro"]
        if entry["senal"] != expected_color or prob < prob_threshold:
            return None

        strength = "strong" if (cruce_alcista or cruce_ema8) else "moderate"
        return {"type": "SKRILL_2.0", "mode": "tendencia",
                "expected_color": expected_color, "probability": prob,
                "trigger_number": current_number, "strength": strength}

    def check_signal_moderado(self, positions, color_data, current_number,
                              expected_color, prob_threshold):
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

        if (ema4[-1] is None or ema8[-1] is None or ema20[-1] is None or
                ema8[-2] is None or ema20[-2] is None):
            return None

        cruce_ema8 = ema8[-2] <= ema20[-2] and ema8[-1] > ema20[-1]
        sobre_emas = positions[-1] > ema4[-1] and positions[-1] > ema8[-1]

        patron_v = False
        if len(positions) >= 3:
            a, b, c = positions[-3], positions[-2], positions[-1]
            patron_v = b < a and b < c and abs(a - c) <= 1 and c > a

        dos_ultimos = len(self.last_two_expected) >= 2 and all(self.last_two_expected)
        emas_alcistas = ema4[-1] > ema8[-1] > ema20[-1]
        cond_racha = dos_ultimos and emas_alcistas and sobre_emas

        if not (cruce_ema8 or patron_v or cond_racha):
            return None

        entry = next((e for e in color_data if e["id"] == current_number), None)
        if not entry or entry["senal"] == "NO APOSTAR":
            return None
        prob = entry["rojo"] if expected_color == "ROJO" else entry["negro"]
        if entry["senal"] != expected_color or prob < prob_threshold:
            return None

        return {"type": "ALERTA_2.0", "mode": "moderado",
                "expected_color": expected_color, "probability": prob,
                "trigger_number": current_number,
                "pattern": "V" if patron_v else "EMA_CROSS"}

    def register_signal_sent(self):
        self.last_signal_time = time.time()

    def register_so_failed(self):
        self.so_cooldown = time.time()


# ─── STATISTICS ───────────────────────────────────────────────────────────────
class Stats:
    def __init__(self):
        self.total   = 0
        self.wins    = 0
        self.losses  = 0
        self.last_stats_at = 0
        self._h24: deque = deque()
        self.batch_start_bankroll = None
        self._wins_at_last_batch  = 0

    def record(self, is_win: bool, bankroll: float):
        self.total += 1
        if is_win: self.wins += 1
        else:       self.losses += 1
        self._h24.append((time.time(), is_win, bankroll))
        self._trim24()

    def _trim24(self):
        cutoff = time.time() - 86400
        while self._h24 and self._h24[0][0] < cutoff:
            self._h24.popleft()

    def should_send_stats(self) -> bool:
        return (self.total - self.last_stats_at) >= 20

    def mark_stats_sent(self, bankroll: float):
        self.last_stats_at        = self.total
        self.batch_start_bankroll = bankroll
        self._wins_at_last_batch  = self.wins

    def batch_stats(self, current_bankroll: float):
        n = self.total - self.last_stats_at
        w = self.wins  - self._wins_at_last_batch
        l = n - w
        e = round(w / n * 100, 1) if n else 0.0
        bk = round(current_bankroll - (self.batch_start_bankroll or 0), 2)
        return w, l, n, e, bk

    def stats_24h(self, current_bankroll: float):
        self._trim24()
        t = len(self._h24)
        w = sum(1 for _, iw, _ in self._h24 if iw)
        l = t - w
        e = round(w / t * 100, 1) if t else 0.0
        bk24 = (round(self._h24[-1][2] - self._h24[0][2], 2)
                if t >= 2 else 0.0)
        return w, l, t, e, bk24


# ─── SOPORTE / RESISTENCIA ────────────────────────────────────────────────────
def find_support_resistance(levels: list, lookback: int = 30) -> dict:
    if len(levels) < lookback:
        return {'support': None, 'resistance': None}
    recent = levels[-lookback:]
    support_c, resistance_c = [], []
    for i in range(2, len(recent) - 2):
        if all(recent[i] < recent[j] for j in (i-1, i-2, i+1, i+2)):
            support_c.append(recent[i])
        if all(recent[i] > recent[j] for j in (i-1, i-2, i+1, i+2)):
            resistance_c.append(recent[i])
    return {
        'support':    support_c[-1]    if support_c    else None,
        'resistance': resistance_c[-1] if resistance_c else None,
    }


# ─── CHART GENERATION ────────────────────────────────────────────────────────
def generate_chart(levels: list, spin_history: list, bet_color: str,
                   markov_prob: float = 0.0, ml_prob: float = 0.0,
                   visible: int = VISIBLE) -> io.BytesIO:
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

    start = max(0, n - visible)
    sl    = slice(start, n)
    x     = np.arange(len(arr[sl]))
    hist_sl = spin_history[start:]

    is_rojo  = bet_color == "ROJO"
    bg       = "#0b101f"
    ax_bg    = "#0f1a2a"
    grid_c   = "#1e2e48"
    line_c   = "#e84040" if is_rojo else "#9090bb"
    ema4_c   = "#ff9f43"
    ema8_c   = "#48dbfb"
    ema20_c  = "#1dd1a1"
    title_c  = "#ff8080" if is_rojo else "#b0b8d0"

    fig, ax = plt.subplots(figsize=(8, 3.6), facecolor=bg)
    ax.set_facecolor(ax_bg)

    y, e4, e8, e20 = arr[sl], ema4[sl], ema8[sl], ema20[sl]

    ax.fill_between(x, y, alpha=0.10, color=line_c)
    ax.plot(x, y,   color=line_c,  linewidth=0.8, zorder=3)
    ax.plot(x, e4,  color=ema4_c,  linewidth=0.7, linestyle="--", label="EMA 4",  zorder=4)
    ax.plot(x, e8,  color=ema8_c,  linewidth=0.7, linestyle="--", label="EMA 8",  zorder=4)
    ax.plot(x, e20, color=ema20_c, linewidth=1.0, label="EMA 20", zorder=4)

    dot_colors = {"ROJO": "#e84040", "NEGRO": "#aaaacc", "VERDE": "#2ecc71"}
    for i, spin in enumerate(hist_sl):
        c = dot_colors.get(spin["real"], "#ffffff")
        ax.scatter(i, y[i], color=c, s=22, zorder=5,
                   edgecolors="white", linewidths=0.3)

    sr = find_support_resistance(levels, lookback=30)
    res_color = "#e84040" if is_rojo else "#888888"
    sup_color = "#888888" if is_rojo else "#e84040"
    if sr['support'] is not None:
        ax.axhline(y=sr['support'], color=sup_color, linestyle='--',
                   linewidth=1.5, alpha=0.7, label='Soporte')
        ax.text(x[-1], sr['support'], f' S {sr["support"]:.1f}',
                color=sup_color, fontsize=8, va='bottom', ha='right')
    if sr['resistance'] is not None:
        ax.axhline(y=sr['resistance'], color=res_color, linestyle='--',
                   linewidth=1.5, alpha=0.7, label='Resistencia')
        ax.text(x[-1], sr['resistance'], f' R {sr["resistance"]:.1f}',
                color=res_color, fontsize=8, va='top', ha='right')

    if ml_prob > 0 or markov_prob > 0:
        label_txt = (f"Markov {markov_prob*100:.0f}%"
                     + (f"  |  ML {ml_prob*100:.0f}%" if ml_prob > 0 else ""))
        ax.text(0.01, 0.97, label_txt, transform=ax.transAxes,
                color="#f0e040", fontsize=7.5, va='top', ha='left',
                bbox=dict(boxstyle='round,pad=0.2', facecolor='#0b101f', alpha=0.7))

    tick_step = max(1, len(x) // 8)
    tick_x  = list(range(0, len(x), tick_step))
    tick_lbs = [str(hist_sl[i]["number"]) if i < len(hist_sl) else "" for i in tick_x]
    ax.set_xticks(tick_x)
    ax.set_xticklabels(tick_lbs, color="#8899bb", fontsize=7)
    ax.tick_params(axis='y', colors="#8899bb", labelsize=7)
    ax.tick_params(axis='x', colors="#8899bb", labelsize=7)
    for spine in ('bottom', 'left'):
        ax.spines[spine].set_color(grid_c)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.grid(axis='y', color=grid_c, linewidth=0.4, alpha=0.5)

    emoji = "🔴" if is_rojo else "⚫️"
    ax.set_title(f"{emoji} Señal {'ROJO' if is_rojo else 'NEGRO'} — últimos {visible} giros · EMA 4/8/20",
                 color=title_c, fontsize=9, pad=6)

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
    if sr['support']    is not None: legend_els.append(Line2D([0],[0], color=sup_color, linestyle='--', linewidth=1.5, label='Soporte'))
    if sr['resistance'] is not None: legend_els.append(Line2D([0],[0], color=res_color, linestyle='--', linewidth=1.5, label='Resistencia'))

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
                try:    wait = int(''.join(filter(str.isdigit, err))) + 1
                except: wait = 30
                logger.warning(f"Telegram flood-wait {wait}s")
                time.sleep(wait)
                continue
            logger.warning(f"Telegram error ({attempt}/{_TG_MAX_RETRIES}): {e}")
            if attempt < _TG_MAX_RETRIES:
                time.sleep(delay)
                delay = min(delay * 2, 60)
            else:
                logger.error(f"Telegram failed after {_TG_MAX_RETRIES} attempts: {e}")
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


# ══════════════════════════════════════════════════════════════════════════════
# ─── ROULETTE ENGINE (CON RE‑EVALUACIÓN DE INVERSIÓN POR INTENTO) ────────────
# ══════════════════════════════════════════════════════════════════════════════
class RouletteEngine:
    def __init__(self, name: str, cfg: dict):
        self.name      = name
        self.ws_key    = cfg["ws_key"]
        self.chat_id   = cfg["chat_id"]
        self.thread_id = cfg["thread_id"]
        self.color_data: list = cfg["color_data"]

        self.spin_history:    list = []
        self.original_levels: list = []
        self.inverted_levels: list = []
        self.last_nonzero_color: Optional[str] = None
        self.anti_block: set = set()

        self.signal_active:  bool = False
        self.expected_color: Optional[str] = None   # color base original de la señal
        self.bet_color:      Optional[str] = None   # color real de apuesta para el intento actual
        self.attempts_left:  int = 0
        self.total_attempts: int = 0
        self.trigger_number: Optional[int] = None

        self.result_until:    float = 0.0
        self.consec_losses:   int   = 0
        self.recovery_active: bool  = False
        self.recovery_target: float = 0.0
        self.level1_bankroll: float = 0.0
        self.signal_is_level1: bool = False

        self.betting_system_name = cfg.get("betting_system", "dalembert")
        self.bet_sys = D_Alembert(BASE_BET)

        self.stats = Stats()
        self.signal_msg_ids: list = []
        self.ws = None
        self.running = True

        self.amx_system = AMXSignalSystem(mode="moderado")
        self.min_prob_threshold = cfg.get("min_prob_threshold", 0.48)

        self.ml_filter = MLSignalFilter(
            markov_order=2,
            markov_threshold=0.52,
            ml_threshold=0.55,
            ml_threshold_retry=0.48,
            ml_min_samples=30,
        )
        self._pending_features: Optional[np.ndarray] = None
        self._last_markov_prob: float = 0.0
        self._last_ml_prob:     float = 0.0
        self.signal_sequence_colors: list = []
        self.signal_history: list = []

        # Estado de espera para reintentos
        self.waiting_for_retry = False
        self.waiting_attempt_number = 0
        self.waiting_message_id = None
        self.waiting_spins_remaining = 0

    def set_mode(self, mode: Literal["tendencia", "moderado"]):
        self.amx_system = AMXSignalSystem(mode=mode)
        logger.info(f"[{self.name}] Modo AMX V20: {mode}")
        return mode

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
        return next((e for e in self.color_data if e["id"] == number), None)

    def get_signal(self, number: int) -> Optional[str]:
        e = self.get_entry(number)
        return e["senal"] if e else None

    def get_prob(self, number: int, color: str) -> float:
        e = self.get_entry(number)
        if not e: return 0.0
        return e["rojo"] if color == "ROJO" else e["negro"]

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
            return ("NEGRO" if self.original_levels[li] < ema20o[li]
                             and last_sig == "NEGRO" else "ROJO")
        else:
            return ("ROJO" if self.inverted_levels[li] < ema20i[li]
                            and last_sig == "ROJO" else "NEGRO")

    def should_activate(self) -> Optional[str]:
        losses = self.consec_losses
        min_spin = 22 + losses * 2
        if len(self.spin_history) < min_spin:
            return None

        last_num = self.spin_history[-1]["number"]
        entry = self.get_entry(last_num)
        if not entry or entry["senal"] == "NO APOSTAR":
            return None
        expected = entry["senal"]

        if len(self.original_levels) < 20 or len(self.inverted_levels) < 20:
            return None

        ema4o  = self.calculate_ema(self.original_levels, 4)
        ema8o  = self.calculate_ema(self.original_levels, 8)
        ema20o = self.calculate_ema(self.original_levels, 20)
        ema4i  = self.calculate_ema(self.inverted_levels, 4)
        ema8i  = self.calculate_ema(self.inverted_levels, 8)
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

        if expected == "ROJO" and check(self.original_levels, ema20o, ema8o, ema4o, li):
            return "ROJO"
        if expected == "NEGRO" and check(self.inverted_levels, ema20i, ema8i, ema4i, li):
            return "NEGRO"
        return None

    def _check_recovery(self):
        if not self.recovery_active:
            return
        if self.bet_sys.bankroll >= self.recovery_target:
            logger.info(f"[{self.name}] Recuperación completada — bankroll={self.bet_sys.bankroll:.2f}")
            self.consec_losses   = 0
            self.recovery_active = False
            self.recovery_target = 0.0
            self.bet_sys.step    = 0

    def _update_amx_positions(self, color: str):
        last = self.amx_system.ultimos_puntos[-1] if self.amx_system.ultimos_puntos else 0
        delta = 1 if color == "ROJO" else (-1 if color == "NEGRO" else 0)
        self.amx_system.ultimos_puntos.append(last + delta)
        if len(self.amx_system.ultimos_puntos) > 300:
            self.amx_system.ultimos_puntos = self.amx_system.ultimos_puntos[-200:]

    # --------------------------------------------------------------------------
    # EVALUACIÓN DE INVERSIÓN (SE PUEDE LLAMAR EN CADA INTENTO)
    # --------------------------------------------------------------------------
    def _evaluate_inversion(self, expected_color: str, trigger_number: int) -> tuple[str, float, str]:
        opposite = "NEGRO" if expected_color == "ROJO" else "ROJO"

        feats_opp = self._build_features(opposite)
        prob_ml_opp = self.ml_filter.model.predict_proba(feats_opp) if self.ml_filter.model.ready else 0.5
        prob_mk_opp = self.ml_filter.markov.predict().get(opposite, 0.5)

        feats_orig = self._build_features(expected_color)
        prob_ml_orig = self.ml_filter.model.predict_proba(feats_orig) if self.ml_filter.model.ready else 0.5
        prob_mk_orig = self.ml_filter.markov.predict().get(expected_color, 0.5)

        levels_opp = self.inverted_levels if expected_color == "ROJO" else self.original_levels
        ema4_opp = self.calculate_ema(levels_opp, 4)
        ema8_opp = self.calculate_ema(levels_opp, 8)
        ema20_opp = self.calculate_ema(levels_opp, 20)
        idx = len(levels_opp) - 1
        tech_opp_strong = False
        if idx >= 0 and ema20_opp[idx] is not None:
            tech_opp_strong = (levels_opp[idx] > ema20_opp[idx] and
                               (ema4_opp[idx] > ema20_opp[idx] or ema8_opp[idx] > ema20_opp[idx]))

        last_expected_signals = [s for s in self.signal_history[-3:] if s["expected"] == expected_color]
        losses_in_expected = sum(1 for s in last_expected_signals if not s["won"])

        invert = False
        reason = ""

        if losses_in_expected >= 2:
            invert = True
            reason = f"2 pérdidas consecutivas en {expected_color}"
        elif prob_ml_opp > 0.55 and prob_ml_orig < 0.48:
            invert = True
            reason = f"ML opuesto {prob_ml_opp:.2f} > 0.55 y original {prob_ml_orig:.2f} < 0.48"
        elif prob_mk_opp > 0.55 and prob_mk_orig < 0.48:
            invert = True
            reason = f"Markov opuesto {prob_mk_opp:.2f} > 0.55 y original {prob_mk_orig:.2f} < 0.48"
        elif tech_opp_strong and prob_ml_orig < 0.50:
            invert = True
            reason = "Fuerza técnica del color opuesto + ML original débil"

        if invert:
            logger.info(f"[{self.name}] INVIRTIENDO señal de {expected_color} a {opposite}. Razón: {reason}")
            return opposite, prob_ml_opp, reason
        else:
            return expected_color, prob_ml_orig, "Sin inversión"

    def _send_waiting_message(self, trigger: int, attempt_number: int):
        icon = "🔴" if self.bet_color == "ROJO" else "⚫️"
        if attempt_number == 2:
            text = "⚠️ No cumplen las condiciones para emitir señal para segundo intento -> Esperar a que mejoren las condiciones ⚠️"
        else:
            text = "⚠️ No cumplen las condiciones para emitir señal para tercer intento -> Esperar a que mejoren las condiciones ⚠️"
        caption = f"{icon} {text}"
        levels = (self.original_levels[:] if self.bet_color == "ROJO" else self.inverted_levels[:])
        chart = generate_chart(levels, self.spin_history[:], self.bet_color,
                               markov_prob=0.0, ml_prob=0.0)
        msg_id = tg_send_photo(self.chat_id, self.thread_id, chart, caption)
        if msg_id:
            self.waiting_message_id = msg_id
        logger.info(f"[{self.name}] Enviado mensaje de espera para intento {attempt_number}.")

    # --------------------------------------------------------------------------
    # PROCESAMIENTO DE NÚMEROS (CON RE‑EVALUACIÓN DE COLOR EN CADA INTENTO)
    # --------------------------------------------------------------------------
    def process_number(self, number: int):
        real = REAL_COLOR_MAP.get(number, "VERDE")
        self.spin_history.append({"number": number, "real": real})
        if len(self.spin_history) > 300:
            self.spin_history.pop(0)

        last_o = self.original_levels[-1] if self.original_levels else 0
        last_i = self.inverted_levels[-1] if self.inverted_levels else 0

        if number == 0:
            ref = self.last_nonzero_color
            self.original_levels.append(last_o + (1 if ref == "ROJO"  else (-1 if ref else 0)))
            self.inverted_levels.append(last_i + (1 if ref == "NEGRO" else (-1 if ref else 0)))
        else:
            self.original_levels.append(last_o + (1 if real == "ROJO"  else -1))
            self.inverted_levels.append(last_i + (1 if real == "NEGRO" else -1))
            self.last_nonzero_color = real

        min_len = min(len(self.original_levels), len(self.inverted_levels),
                      len(self.spin_history))
        self.original_levels = self.original_levels[-min_len:]
        self.inverted_levels = self.inverted_levels[-min_len:]

        self.ml_filter.observe_color(real)
        self._update_amx_positions(real)
        expected_signal = self.get_signal(number)
        self.amx_system.update_streak(real, expected_signal)

        if self.signal_active:
            self.signal_sequence_colors.append(real)

        # ─── PRIORIDAD: ESPERA DE REINTENTO ───────────────────────────────────
        if self.waiting_for_retry and self.signal_active:
            self.waiting_spins_remaining -= 1
            if self.waiting_spins_remaining <= 0:
                logger.info(f"[{self.name}] Tiempo de espera agotado para reintento. Finalizando señal como pérdida.")
                self.waiting_for_retry = False
                if self.waiting_message_id:
                    tg_delete(self.chat_id, self.waiting_message_id)
                    self.waiting_message_id = None
                self.stats.record(False, self.bet_sys.bankroll)
                self._finalize_signal(won=False, number=number, real=real, bet=0)
                return
            else:
                # Re‑evaluar color para este reintento
                new_color, _, inv_reason = self._evaluate_inversion(self.expected_color, number)
                if new_color != self.bet_color:
                    logger.info(f"[{self.name}] Reintento en espera: cambia color de {self.bet_color} a {new_color}. Razón: {inv_reason}")
                    self.bet_color = new_color
                feats = self._build_features(self.bet_color)
                emit, mp, mlp, reason = self.ml_filter.should_emit_signal(feats, self.bet_color, is_retry=True)
                if emit:
                    logger.info(f"[{self.name}] Condiciones cumplidas para reintento tras espera, enviando intento {MAX_ATTEMPTS - self.attempts_left + 1} (color {self.bet_color})")
                    self.waiting_for_retry = False
                    if self.waiting_message_id:
                        tg_delete(self.chat_id, self.waiting_message_id)
                        self.waiting_message_id = None
                    new_bet = self.bet_sys.current_bet()
                    attempt_number = MAX_ATTEMPTS - self.attempts_left + 1
                    self._send_retry_signal(number, new_bet, attempt_number)
                    self.result_until = time.time() + 30
                else:
                    logger.info(f"[{self.name}] Aún no se cumplen condiciones para reintento. Giros restantes: {self.waiting_spins_remaining}")
                return

        # ─── RESULTADO DE APUESTA ACTIVA ──────────────────────────────────────
        if self.signal_active and time.time() > self.result_until:
            is_win = ((self.bet_color == "ROJO"  and real == "ROJO") or
                      (self.bet_color == "NEGRO" and real == "NEGRO"))

            self.signal_history.append({"expected": self.expected_color, "won": is_win})
            if len(self.signal_history) > 50:
                self.signal_history.pop(0)

            self.ml_filter.update_result(is_win)
            logger.info(f"[{self.name}] ML update: won={is_win} | {self.ml_filter.info()}")

            if is_win:
                bet = self.bet_sys.win()
                self.stats.record(True, self.bet_sys.bankroll)
                self._finalize_signal(won=True, number=number, real=real, bet=bet)
            else:
                self.attempts_left -= 1
                bet = self.bet_sys.loss()

                if self.attempts_left <= 0:
                    self.consec_losses += 1
                    if self.consec_losses >= 10:
                        self.consec_losses   = 0
                        self.recovery_active = False
                        self.recovery_target = 0.0
                    else:
                        self.recovery_active = True
                        self.recovery_target = self.level1_bankroll + BASE_BET
                    self.stats.record(False, self.bet_sys.bankroll)
                    self._finalize_signal(won=False, number=number, real=real, bet=bet)
                else:
                    # Reintento inmediato: re‑evaluar color primero
                    if self.signal_msg_ids:
                        tg_delete(self.chat_id, self.signal_msg_ids.pop())

                    self.trigger_number = number
                    new_bet = self.bet_sys.current_bet()
                    attempt_number = MAX_ATTEMPTS - self.attempts_left + 1

                    # Re‑evaluar inversión para este nuevo intento
                    new_color, _, inv_reason = self._evaluate_inversion(self.expected_color, number)
                    if new_color != self.bet_color:
                        logger.info(f"[{self.name}] Reintento inmediato: cambia color de {self.bet_color} a {new_color}. Razón: {inv_reason}")
                        self.bet_color = new_color

                    feats = self._build_features(self.bet_color)
                    emit, mp, mlp, reason = self.ml_filter.should_emit_signal(feats, self.bet_color, is_retry=True)
                    logger.info(f"[{self.name}] Reintento inmediato ML check: {reason} (color {self.bet_color})")
                    if emit:
                        self._last_markov_prob = mp
                        self._last_ml_prob     = mlp
                        self._send_retry_signal(number, new_bet, attempt_number)
                        self.result_until = time.time() + 30
                    else:
                        logger.info(f"[{self.name}] Reintento bloqueado, entrando en espera para intento {attempt_number}")
                        self.waiting_for_retry = True
                        self.waiting_attempt_number = attempt_number
                        self.waiting_spins_remaining = MAX_WAIT_SPINS
                        self._send_waiting_message(number, attempt_number)
                        self.result_until = time.time() + 1000  # desactivar resultado
            return

        # ─── NUEVA SEÑAL ──────────────────────────────────────────────────────
        if not self.signal_active and time.time() > self.result_until:
            self.signal_msg_ids.clear()
            self.signal_sequence_colors.clear()
            self.waiting_for_retry = False
            if self.waiting_message_id:
                tg_delete(self.chat_id, self.waiting_message_id)
                self.waiting_message_id = None

            signal = self._detect_amx_signal()
            if not signal:
                expected_classic = self.should_activate()
                if expected_classic:
                    bet_color_base = self.determine_bet_color(expected_classic)
                    signal = {
                        "type": "CLASSIC",
                        "expected_color": expected_classic,
                        "probability": self.get_prob(self.spin_history[-1]["number"], expected_classic),
                        "trigger_number": self.spin_history[-1]["number"]
                    }
                else:
                    return

            if signal:
                base_color = signal["expected_color"]
                trigger_num = signal["trigger_number"]

                final_color, _, inv_reason = self._evaluate_inversion(base_color, trigger_num)

                feats = self._build_features(final_color)
                emit, mp, mlp, reason = self.ml_filter.should_emit_signal(feats, final_color, is_retry=False)
                logger.info(f"[{self.name}] Señal detectada: base={base_color}, final={final_color}, ML check: {reason} (inversión: {inv_reason})")

                if emit:
                    self._last_markov_prob = mp
                    self._last_ml_prob     = mlp
                    self._pending_features = feats
                    self.signal_active   = True
                    self.expected_color  = base_color
                    self.bet_color       = final_color
                    self.attempts_left   = MAX_ATTEMPTS
                    self.total_attempts  = MAX_ATTEMPTS
                    self.trigger_number  = trigger_num
                    self._send_signal(trigger_num, 1, amx_signal=signal if signal.get("type") != "CLASSIC" else None)
                else:
                    logger.info(f"[{self.name}] Señal bloqueada por ML/Markov: {reason}")

    def _build_features(self, bet_color: str, tabla_prob: Optional[float] = None) -> np.ndarray:
        if tabla_prob is None:
            last_num = self.spin_history[-1]["number"] if self.spin_history else 0
            tabla_prob = self.get_prob(last_num, bet_color)

        positions = (self.original_levels if bet_color == "ROJO"
                     else self.inverted_levels)
        ema4  = self.calculate_ema(positions, 4)
        ema8  = self.calculate_ema(positions, 8)
        ema20 = self.calculate_ema(positions, 20)

        recent = [s["real"] for s in self.spin_history[-5:]]
        momentum = sum(1 for c in reversed(recent)
                       if c == bet_color) if recent else 0

        return self.ml_filter.extract_features(
            bet_color       = bet_color,
            tabla_prob      = tabla_prob,
            ema4            = ema4,
            ema8            = ema8,
            ema20           = ema20,
            positions       = positions,
            momentum_count  = momentum,
            consec_losses   = self.consec_losses,
            bet_step        = self.bet_sys.step,
            last_two_expected = self.amx_system.last_two_expected,
            recovery_active = self.recovery_active,
        )

    def _detect_amx_signal(self) -> Optional[dict]:
        if len(self.amx_system.ultimos_puntos) < 20:
            return None
        current_number = self.spin_history[-1]["number"] if self.spin_history else 0
        entry = self.get_entry(current_number)
        if not entry or entry["senal"] == "NO APOSTAR":
            return None
        expected_color = entry["senal"]

        recent_colors = [s["real"] for s in self.spin_history[-5:]]
        momentum = 0
        for c in reversed(recent_colors):
            if c == expected_color: momentum += 1
            elif c != "VERDE":      break
        if momentum < 2:
            return None

        try:
            if self.amx_system.mode == "tendencia":
                return self.amx_system.check_signal_tendencia(
                    self.amx_system.ultimos_puntos, self.color_data,
                    current_number, expected_color, self.min_prob_threshold)
            else:
                return self.amx_system.check_signal_moderado(
                    self.amx_system.ultimos_puntos, self.color_data,
                    current_number, expected_color, self.min_prob_threshold)
        except Exception as e:
            logger.warning(f"[{self.name}] Error AMX: {e}")
            return None

    def _send_signal(self, trigger: int, attempt: int, amx_signal: Optional[dict] = None):
        bet  = self.bet_sys.current_bet()
        prob = int(self.get_prob(trigger, self.bet_color) * 100)
        icon = "🔴" if self.bet_color == "ROJO" else "⚫️"
        step = self.bet_sys.step + 1
        mk   = self._last_markov_prob * 100

        self.signal_is_level1 = (self.bet_sys.step == 0 and not self.recovery_active)
        if self.signal_is_level1:
            self.level1_bankroll = self.bet_sys.bankroll

        safe_name = escape_html(self.name)
        safe_trigger = escape_html(str(trigger))
        safe_bet_color = escape_html(self.bet_color)

        caption = (
            f"✅☑️ <b>SEÑAL CONFIRMADA</b> ☑️✅\n\n"
            f"🎰 <b>Juego: {safe_name}</b>\n"
            f"👉🏼 <b>Después de: {safe_trigger}</b>\n"
            f"🎯 <b>Apostar a: {safe_bet_color}</b> {icon}\n\n"
            f"💡 <i>Probabilidad tabla: {prob}%</i>\n"
            f"💠 <i>Probabilidad Markov: {mk:.0f}%</i>\n"
            f"🌀 <i>D'Alembert paso {step} de 20</i>\n"
            f"📍 <i>Apuesta: {bet:.2f} usd</i>\n\n"
            f"♻️ <i>Intento {attempt}/{MAX_ATTEMPTS}</i>\n"
        )

        levels = (self.original_levels[:] if self.bet_color == "ROJO"
                  else self.inverted_levels[:])
        chart  = generate_chart(levels, self.spin_history[:], self.bet_color,
                                markov_prob=mk/100, ml_prob=0)
        msg_id = tg_send_photo(self.chat_id, self.thread_id, chart, caption)
        if msg_id:
            self.signal_msg_ids.append(msg_id)
        logger.info(f"[{self.name}] Signal: {self.bet_color} after {trigger}, bet={bet:.2f}, step={step}, Markov={mk:.0f}%")
        self.result_until = time.time() + 30

    def _send_retry_signal(self, trigger: int, new_bet: float, attempt_number: int):
        prob = int(self.get_prob(trigger, self.bet_color) * 100)
        icon = "🔴" if self.bet_color == "ROJO" else "⚫️"
        step = self.bet_sys.step + 1
        mk   = self._last_markov_prob * 100

        safe_name = escape_html(self.name)
        safe_trigger = escape_html(str(trigger))
        safe_bet_color = escape_html(self.bet_color)

        caption = (
            f"✅☑️ <b>SEÑAL CONFIRMADA</b> ☑️✅\n\n"
            f"🎰 <b>Juego: {safe_name}</b>\n"
            f"👉🏼 <b>Después de: {safe_trigger}</b>\n"
            f"🎯 <b>Apostar a: {safe_bet_color}</b> {icon}\n\n"
            f"💡 <i>Probabilidad tabla: {prob}%</i>\n"
            f"💠 <i>Probabilidad Markov: {mk:.0f}%</i>\n"
            f"🌀 <i>D'Alembert paso {step} de 20</i>\n"
            f"📍 <i>Apuesta: {new_bet:.2f} usd</i>\n\n"
            f"♻️ <i>Intento {attempt_number}/{MAX_ATTEMPTS}</i>\n"
        )

        levels = (self.original_levels[:] if self.bet_color == "ROJO"
                  else self.inverted_levels[:])
        chart  = generate_chart(levels, self.spin_history[:], self.bet_color,
                                markov_prob=mk/100, ml_prob=0)
        msg_id = tg_send_photo(self.chat_id, self.thread_id, chart, caption)
        if msg_id:
            self.signal_msg_ids.append(msg_id)
        logger.info(f"[{self.name}] Retry #{attempt_number}: {self.bet_color}, bet={new_bet:.2f}")
        self.result_until = time.time() + 30

    def _finalize_signal(self, won: bool, number: int, real: str, bet: float):
        for msg_id in self.signal_msg_ids:
            tg_delete(self.chat_id, msg_id)
        self.signal_msg_ids.clear()
        if self.waiting_message_id:
            tg_delete(self.chat_id, self.waiting_message_id)
            self.waiting_message_id = None

        emoji_map = {"ROJO": "🔴", "NEGRO": "⚫️", "VERDE": "🟢"}
        seq_str = " -> ".join(emoji_map.get(c, "⚪") for c in self.signal_sequence_colors)

        bankroll = self.bet_sys.bankroll
        icon = emoji_map.get(real, "⚪")
        prefix = "✅" if won else "❌"
        caption = (
            f"🆔 <i>Secuencia:</i> {seq_str}\n\n"
            f"{prefix} <i>Resultado: {number} {real}</i>\n"
            f"💰 <i>Bankroll Actual: {bankroll:.2f} usd</i>"
        )

        levels = (self.original_levels[:] if self.bet_color == "ROJO"
                  else self.inverted_levels[:])
        chart = generate_chart(levels, self.spin_history[:], self.bet_color,
                               markov_prob=0.0, ml_prob=0.0)

        tg_send_photo(self.chat_id, self.thread_id, chart, caption)

        self.signal_active = False
        self.signal_sequence_colors.clear()
        self.waiting_for_retry = False
        self._check_recovery()
        self._check_stats()
        self.result_until = time.time() + 7.0

        logger.info(f"[{self.name}] Signal finalized: {'WIN' if won else 'LOSS'} #{number}, bankroll={bankroll:.2f}")

    def _check_stats(self):
        if not self.stats.should_send_stats():
            return
        bk = self.bet_sys.bankroll
        w20, l20, t20, e20, bk20 = self.stats.batch_stats(bk)
        self.stats.mark_stats_sent(bk)
        w24, l24, t24, e24, bk24 = self.stats.stats_24h(bk)
        text = (
            f"👉🏼 <b>ESTADÍSTICAS {t20} SEÑALES</b>\n"
            f"🈯️ <b>W: {w20}</b> 🈲 <b>L: {l20}</b> 🈺 <b>T: {t20}</b> 📈 <b>E: {e20}%</b>\n"
            f"💰 <i>Bankroll acumulado: {bk20:.2f} usd</i>\n\n"
            f"👉🏼 <b>ESTADÍSTICAS 24 HORAS</b>\n"
            f"🈯️ <b>W: {w24}</b> 🈲 <b>L: {l24}</b> 🈺 <b>T: {t24}</b> 📈 <b>E: {e24}%</b>\n"
            f"💰 <i>Bankroll acumulado: {bk24:.2f} usd</i>\n"
        )
        tg_send_text(self.chat_id, self.thread_id, text)

    async def run_ws(self):
        reconnect_delay = 5
        while self.running:
            try:
                async with websockets.connect(WS_URL, ping_interval=30,
                                              ping_timeout=60, close_timeout=10) as ws:
                    self.ws = ws
                    reconnect_delay = 5
                    logger.info(f"[{self.name}] WS connected")
                    await ws.send(json.dumps({
                        "type": "subscribe", "casinoId": CASINO_ID,
                        "currency": "USD", "key": [self.ws_key]
                    }))
                    async for message in ws:
                        if not self.running:
                            break
                        try:
                            data = json.loads(message)
                        except Exception:
                            continue
                        if "last20Results" in data and isinstance(data["last20Results"], list):
                            tmp = []
                            for r in data["last20Results"]:
                                gid = r.get("gameId")
                                num = r.get("result")
                                if gid and num is not None:
                                    try: n = int(num)
                                    except: continue
                                    if 0 <= n <= 36 and gid not in self.anti_block:
                                        tmp.append((gid, n))
                                        self.anti_block.add(gid)
                                        if len(self.anti_block) > 1000:
                                            self.anti_block.clear()
                            for gid, n in reversed(tmp):
                                self.process_number(n)
                        gid = data.get("gameId")
                        res = data.get("result")
                        if gid and res is not None:
                            try: n = int(res)
                            except: continue
                            if 0 <= n <= 36 and gid not in self.anti_block:
                                self.anti_block.add(gid)
                                if len(self.anti_block) > 1000:
                                    self.anti_block.clear()
                                self.process_number(n)
            except Exception as e:
                logger.warning(f"[{self.name}] WS error: {e}. Retry in {reconnect_delay}s")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 60)


# ─── FLASK KEEPALIVE ──────────────────────────────────────────────────────────
app = Flask(__name__)

@app.route("/")
def index():
    return jsonify({"status": "ok", "bot": "Roulette Signal Bot AMX V20+ML", "ts": time.time()})

@app.route("/ping")
def ping():
    return jsonify({"pong": True, "ts": time.time()})

@app.route("/health")
def health():
    return jsonify({"healthy": True})

# ─── SELF-PING ────────────────────────────────────────────────────────────────
import os, urllib.request

async def self_ping_loop():
    port = int(os.environ.get("PORT", 10000))
    url  = os.environ.get("RENDER_EXTERNAL_URL", f"http://localhost:{port}")
    while True:
        await asyncio.sleep(300)
        try:
            with urllib.request.urlopen(f"{url}/ping", timeout=10) as r:
                logger.info(f"Self-ping OK: {r.status}")
        except Exception as e:
            logger.warning(f"Self-ping failed: {e}")


# ─── COMANDOS TELEGRAM ────────────────────────────────────────────────────────
engines: dict[str, RouletteEngine] = {}

@bot.message_handler(commands=['start', 'help'])
def cmd_start(message):
    bot.reply_to(message, """
<b>🎰 Roulette Bot - Sistema AMX V20 + ML + Markov</b>

/moderado  — Modo MODERADO (EMA8/EMA20 + patrón V)
/tendencia — Modo TENDENCIA (EMA4/EMA20 + momentum)
/mlstatus  — Estado del modelo ML y Markov Chain
/mlreset   — Resetea modelo ML (mantiene Markov)
/status    — Estado de ruletas
/reset     — Resetea estadísticas
/help      — Esta ayuda
""", parse_mode="HTML")

@bot.message_handler(commands=['mlstatus'])
def cmd_mlstatus(message):
    lines = ["<b>🧠 ESTADO ML / MARKOV</b>\n"]
    for name, engine in engines.items():
        ml   = engine.ml_filter.model
        mk   = engine.ml_filter.markov
        info = (
            f"<b>{name}</b>\n"
            f"  Markov: {mk.state_info()}\n"
            f"  ML ready: {ml.ready} | muestras: {ml.n_samples}/{ml.min_samples}\n"
            f"  Umbral señal: {engine.ml_filter.ml_threshold:.2f} | "
            f"retry: {engine.ml_filter.ml_threshold_retry:.2f}\n"
        )
        lines.append(info)
    bot.reply_to(message, "\n".join(lines), parse_mode="HTML")

@bot.message_handler(commands=['mlreset'])
def cmd_mlreset(message):
    for engine in engines.values():
        engine.ml_filter.model = OnlineLogisticRegression(min_samples=30)
    bot.reply_to(message, "🔄 <b>Modelos ML reseteados</b> (Markov conservado)",
                 parse_mode="HTML")

@bot.message_handler(commands=['moderado'])
def cmd_moderado(message):
    for n, e in engines.items():
        e.set_mode("moderado")
    bot.reply_to(message, "📊 <b>Modo MODERADO activado</b>", parse_mode="HTML")

@bot.message_handler(commands=['tendencia'])
def cmd_tendencia(message):
    for n, e in engines.items():
        e.set_mode("tendencia")
    bot.reply_to(message, "📈 <b>Modo TENDENCIA activado</b>", parse_mode="HTML")

@bot.message_handler(commands=['status'])
def cmd_status(message):
    lines = ["<b>📊 ESTADO</b>\n"]
    for name, engine in engines.items():
        icon = "📈" if engine.amx_system.mode == "tendencia" else "📊"
        sig  = "🟢" if engine.signal_active else "⚪"
        lines.append(f"<b>{name}</b>: {icon} {engine.amx_system.mode} {sig}")
    bot.reply_to(message, "\n".join(lines), parse_mode="HTML")

@bot.message_handler(commands=['reset'])
def cmd_reset(message):
    for e in engines.values():
        e.stats = Stats()
    bot.reply_to(message, "🔄 <b>Estadísticas reseteadas</b>", parse_mode="HTML")


# ─── MAIN ────────────────────────────────────────────────────────────────────
def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)

async def main():
    global engines
    engines = {name: RouletteEngine(name, cfg) for name, cfg in ROULETTE_CONFIGS.items()}
    tasks = [asyncio.create_task(e.run_ws()) for e in engines.values()]
    tasks.append(asyncio.create_task(self_ping_loop()))

    def telegram_polling():
        logger.info("Iniciando polling Telegram...")
        while True:
            try:
                bot.polling(none_stop=False, interval=1, timeout=20, long_polling_timeout=30)
            except requests.exceptions.ReadTimeout:
                logger.warning("Telegram read timeout. Reiniciando polling en 5 segundos...")
                time.sleep(5)
            except telebot.apihelper.ApiTelegramException as e:
                err_str = str(e)
                if "retry after" in err_str.lower():
                    try:
                        wait = int(''.join(filter(str.isdigit, err_str))) + 1
                    except:
                        wait = 30
                    logger.warning(f"Telegram API flood-wait {wait}s")
                    time.sleep(wait)
                else:
                    logger.error(f"ApiTelegramException: {e}. Reiniciando en 15 segundos...")
                    time.sleep(15)
            except Exception as e:
                logger.error(f"Error crítico en polling de Telegram: {e}. Reiniciando en 15 segundos...")
                time.sleep(15)

    threading.Thread(target=telegram_polling, daemon=True).start()
    logger.info("🎰 Roulette Bot AMX V20 + ML + Markov iniciado")
    await asyncio.gather(*tasks)
