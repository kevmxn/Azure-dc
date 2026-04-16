#!/usr/bin/env python3
"""
Roulette Telegram Signal Bot - Sistema AMX UNIFIED (v2.0 - Combinado)
  - Dos ruletas en un solo proceso: Russian Roulette + Azure Roulette
  - Pre-entrenamiento con russian-azure.db (~16k giros por tabla)
  - 5 intentos por señal (Labouchère)
  - Calentamiento WS: 21 giros silenciosos antes de emitir señales
  - Predicción basada en combinaciones (COLOR+PARIDAD+RANGO) con Markov orden 3
  - Selección automática de la categoría con mayor probabilidad marginal
"""

import asyncio
import io
import json
import logging
import os
import re
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
TOKEN_RUSSIAN = "8714149875:AAFJugWY0E5A4C0lrxn2bMcKsQEieqo_t5M"
TOKEN_AZURE   = "8308452662:AAGZFIZyYsmVR39SvIOSlKD3OY_YNMOsEQU"

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

def _make_bot(token: str) -> telebot.TeleBot:
    b = telebot.TeleBot(token, threaded=False)
    b.session = _session
    return b

bot_russian = _make_bot(TOKEN_RUSSIAN)
bot_azure   = _make_bot(TOKEN_AZURE)

# ─── DB CONFIG ────────────────────────────────────────────────────────────────
DB_PATH = "russian-azure.db"

# ─── ROULETTE COLOR MAPS ──────────────────────────────────────────────────────
REAL_COLOR_MAP = {
    0:"VERDE",1:"ROJO",2:"NEGRO",3:"ROJO",4:"NEGRO",5:"ROJO",6:"NEGRO",
    7:"ROJO",8:"NEGRO",9:"ROJO",10:"NEGRO",11:"NEGRO",12:"ROJO",13:"NEGRO",
    14:"ROJO",15:"NEGRO",16:"ROJO",17:"NEGRO",18:"ROJO",19:"ROJO",20:"NEGRO",
    21:"ROJO",22:"NEGRO",23:"ROJO",24:"NEGRO",25:"ROJO",26:"NEGRO",27:"ROJO",
    28:"NEGRO",29:"NEGRO",30:"ROJO",31:"NEGRO",32:"ROJO",33:"NEGRO",34:"ROJO",
    35:"NEGRO",36:"ROJO"
}

# ─── CATEGORÍAS HELPER ────────────────────────────────────────────────────────
def get_paridad(number: int) -> Optional[str]:
    if number == 0: return None
    return "PAR" if number % 2 == 0 else "IMPAR"

def get_rango(number: int) -> Optional[str]:
    if number == 0: return None
    return "BAJO" if 1 <= number <= 18 else "ALTO"

CATEGORY_ICONS = {
    "ROJO": "🔴", "NEGRO": "⚫️",
    "PAR":  "🟣", "IMPAR": "🟡",
    "BAJO": "🟤", "ALTO":  "🔵",
    "VERDE": "🟢",
}

# ─── COMBINACIONES (COLOR + PARIDAD + RANGO) ─────────────────────────────────
def get_combined_class(number: int, real: str) -> Optional[str]:
    """Retorna el código de combinación (ej. 'R-P-B') o None si es 0."""
    if number == 0:
        return None
    par = get_paridad(number)
    rang = get_rango(number)
    color_code = "R" if real == "ROJO" else "N"
    par_code = "P" if par == "PAR" else "I"
    rang_code = "B" if rang == "BAJO" else "A"
    return f"{color_code}-{par_code}-{rang_code}"

def combined_to_values(comb: str) -> dict:
    """Convierte 'R-P-B' en {'color':'ROJO','paridad':'PAR','rango':'BAJO'}"""
    c, p, r = comb.split("-")
    return {
        "color": "ROJO" if c == "R" else "NEGRO",
        "paridad": "PAR" if p == "P" else "IMPAR",
        "rango": "BAJO" if r == "B" else "ALTO"
    }

# ─── PREDICTOR DE COMBINACIONES (Markov orden 3) ────────────────────────────
class CombinedMarkovPredictor:
    def __init__(self, order: int = 3):
        self.order = order
        self.transition_counts = defaultdict(lambda: defaultdict(int))

    def update(self, combined_seq: list):
        """Entrena con la secuencia completa de combinaciones (ignora ceros)."""
        self.transition_counts.clear()
        if len(combined_seq) < self.order + 1:
            return
        for i in range(len(combined_seq) - self.order):
            state = tuple(combined_seq[i:i+self.order])
            nxt = combined_seq[i+self.order]
            self.transition_counts[state][nxt] += 1

    def predict(self, combined_seq: list) -> Optional[dict]:
        if len(combined_seq) < self.order:
            return None
        state = tuple(combined_seq[-self.order:])
        counts = dict(self.transition_counts.get(state, {}))
        total = sum(counts.values())
        if total < 5:
            return None
        probs = {k: v/total for k, v in counts.items()}
        probs["total"] = total
        return probs

# ─── COLOR DATA — RUSSIAN ROULETTE ────────────────────────────────────────────
RUSSIAN_COLOR_DATA = [
    {"id": 0,  "rojo": 0.56, "negro": 0.40, "senal": "ROJO"},
    {"id": 1,  "rojo": 0.44, "negro": 0.52, "senal": "NEGRO"},
    {"id": 2,  "rojo": 0.56, "negro": 0.40, "senal": "ROJO"},
    {"id": 3,  "rojo": 0.56, "negro": 0.40, "senal": "ROJO"},
    {"id": 4,  "rojo": 0.56, "negro": 0.40, "senal": "ROJO"},
    {"id": 5,  "rojo": 0.44, "negro": 0.56, "senal": "NEGRO"},
    {"id": 6,  "rojo": 0.52, "negro": 0.44, "senal": "ROJO"},
    {"id": 7,  "rojo": 0.52, "negro": 0.48, "senal": "ROJO"},
    {"id": 8,  "rojo": 0.56, "negro": 0.40, "senal": "ROJO"},
    {"id": 9,  "rojo": 0.48, "negro": 0.52, "senal": "NEGRO"},
    {"id": 10, "rojo": 0.56, "negro": 0.40, "senal": "ROJO"},
    {"id": 11, "rojo": 0.56, "negro": 0.40, "senal": "ROJO"},
    {"id": 12, "rojo": 0.56, "negro": 0.44, "senal": "ROJO"},
    {"id": 13, "rojo": 0.56, "negro": 0.44, "senal": "ROJO"},
    {"id": 14, "rojo": 0.48, "negro": 0.52, "senal": "NEGRO"},
    {"id": 15, "rojo": 0.56, "negro": 0.40, "senal": "ROJO"},
    {"id": 16, "rojo": 0.44, "negro": 0.52, "senal": "NEGRO"},
    {"id": 17, "rojo": 0.56, "negro": 0.44, "senal": "ROJO"},
    {"id": 18, "rojo": 0.48, "negro": 0.52, "senal": "NEGRO"},
    {"id": 19, "rojo": 0.40, "negro": 0.56, "senal": "NEGRO"},
    {"id": 20, "rojo": 0.52, "negro": 0.44, "senal": "ROJO"},
    {"id": 21, "rojo": 0.40, "negro": 0.56, "senal": "NEGRO"},
    {"id": 22, "rojo": 0.52, "negro": 0.48, "senal": "ROJO"},
    {"id": 23, "rojo": 0.40, "negro": 0.56, "senal": "NEGRO"},
    {"id": 24, "rojo": 0.52, "negro": 0.44, "senal": "ROJO"},
    {"id": 25, "rojo": 0.44, "negro": 0.56, "senal": "NEGRO"},
    {"id": 26, "rojo": 0.40, "negro": 0.56, "senal": "NEGRO"},
    {"id": 27, "rojo": 0.44, "negro": 0.52, "senal": "NEGRO"},
    {"id": 28, "rojo": 0.44, "negro": 0.52, "senal": "NEGRO"},
    {"id": 29, "rojo": 0.48, "negro": 0.48, "senal": "NO APOSTAR"},
    {"id": 30, "rojo": 0.40, "negro": 0.56, "senal": "NEGRO"},
    {"id": 31, "rojo": 0.52, "negro": 0.48, "senal": "ROJO"},
    {"id": 32, "rojo": 0.40, "negro": 0.56, "senal": "NEGRO"},
    {"id": 33, "rojo": 0.52, "negro": 0.44, "senal": "ROJO"},
    {"id": 34, "rojo": 0.44, "negro": 0.56, "senal": "NEGRO"},
    {"id": 35, "rojo": 0.40, "negro": 0.56, "senal": "NEGRO"},
    {"id": 36, "rojo": 0.40, "negro": 0.56, "senal": "NEGRO"},
]

# ─── COLOR DATA — AZURE ROULETTE ──────────────────────────────────────────────
AZURE_COLOR_DATA = [
    {"id":0, "rojo":0.52,"negro":0.44,"senal":"ROJO"},
    {"id":1, "rojo":0.52,"negro":0.48,"senal":"ROJO"},
    {"id":2, "rojo":0.60,"negro":0.40,"senal":"ROJO"},
    {"id":3, "rojo":0.56,"negro":0.40,"senal":"ROJO"},
    {"id":4, "rojo":0.56,"negro":0.40,"senal":"ROJO"},
    {"id":5, "rojo":0.52,"negro":0.48,"senal":"ROJO"},
    {"id":6, "rojo":0.56,"negro":0.40,"senal":"ROJO"},
    {"id":7, "rojo":0.52,"negro":0.44,"senal":"ROJO"},
    {"id":8, "rojo":0.52,"negro":0.44,"senal":"ROJO"},
    {"id":9, "rojo":0.52,"negro":0.44,"senal":"ROJO"},
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
    "Russian Roulette": {
        "bot":       bot_russian,
        "ws_key":    221,
        "chat_id":   -1003835197023,
        "thread_id": 8344,
        "db_table":  "russian_roulette",
        "color_data": RUSSIAN_COLOR_DATA,
        "min_prob_threshold": 0.49,
    },
    "Azure Roulette": {
        "bot":       bot_azure,
        "ws_key":    227,
        "chat_id":   -1003835197023,
        "thread_id": 6,
        "db_table":  "roulette_1",
        "color_data": AZURE_COLOR_DATA,
        "min_prob_threshold": 0.49,
    },
}

WS_URL    = "wss://dga.pragmaticplaylive.net/ws"
CASINO_ID = "ppcjd00000007254"
MAX_ATTEMPTS = 5
BASE_BET  = 0.10
VISIBLE   = 50
WARMUP_SPINS = 21

# ─── LABOUCHÈRE ───────────────────────────────────────────────────────────────
LABOUCHERE_SEQUENCE: list[int] = [1, 2, 1]

class Labouchere:
    def __init__(self, sequence: list[int], base: float):
        self.base            = base
        self.original_seq    = list(sequence)
        self.sequence        = list(sequence)
        self.bankroll        = 0.0

    @property
    def step(self) -> int:
        return max(0, len(self.sequence) - len(self.original_seq))

    def is_fresh(self) -> bool:
        return self.sequence == self.original_seq

    def reset(self):
        self.sequence = list(self.original_seq)

    def set_sequence(self, new_seq: list[int]):
        self.original_seq = list(new_seq)
        self.sequence     = list(new_seq)

    def current_bet(self) -> float:
        if not self.sequence:
            self.reset()
        if len(self.sequence) == 1:
            val = self.sequence[0]
        else:
            val = self.sequence[0] + self.sequence[-1]
        return round(self.base * val, 2)

    def win(self) -> float:
        bet = self.current_bet()
        self.bankroll = round(self.bankroll + bet, 2)
        if len(self.sequence) >= 2:
            self.sequence.pop(0)
            self.sequence.pop(-1)
        elif len(self.sequence) == 1:
            self.sequence.pop(0)
        if not self.sequence:
            self.reset()
        return bet

    def loss(self) -> float:
        bet = self.current_bet()
        self.bankroll = round(self.bankroll - bet, 2)
        units = round(bet / self.base)
        self.sequence.append(units if units > 0 else 1)
        return bet

    def sequence_display(self) -> str:
        return " - ".join(str(v) for v in self.sequence)

# ─── MARKOV CHAIN (ventana 60 giros) ─────────────────────────────────────────
class MarkovChainPredictor:
    def __init__(self, window: int = 60, order: int = 2):
        self.window = window
        self.order  = order
        self.transition_counts: dict = {}

    def update(self, spin_history: list):
        self.transition_counts = defaultdict(lambda: defaultdict(int))
        recent = [s["real"] for s in spin_history[-self.window:] if s["real"] != "VERDE"]
        if len(recent) < self.order + 1:
            return
        for i in range(len(recent) - self.order):
            state  = tuple(recent[i : i + self.order])
            next_c = recent[i + self.order]
            if next_c in ("ROJO", "NEGRO"):
                self.transition_counts[state][next_c] += 1

    def predict(self, spin_history: list) -> Optional[dict]:
        recent = [s["real"] for s in spin_history if s["real"] != "VERDE"]
        if len(recent) < self.order:
            return None
        state  = tuple(recent[-self.order:])
        counts = dict(self.transition_counts.get(state, {}))
        total  = sum(counts.values())
        if total < 5:
            return None
        return {
            "ROJO":  counts.get("ROJO",  0) / total,
            "NEGRO": counts.get("NEGRO", 0) / total,
            "total": total,
        }

# ─── ML PATTERN PREDICTOR – COLOR (historial completo) ────────────────────────
class MLPatternPredictor:
    def __init__(self, pattern_length: int = 3):
        self.pattern_length = pattern_length
        self.pattern_counts: dict = defaultdict(lambda: defaultdict(int))
        self._known_len: int = 0

    def add_spin(self, spin_history: list):
        non_verde = [s["real"] for s in spin_history if s["real"] != "VERDE"]
        current_len = len(non_verde)
        if current_len <= self._known_len:
            return
        self._known_len = current_len
        if current_len < self.pattern_length + 1:
            return
        i       = current_len - self.pattern_length - 1
        pattern = tuple(non_verde[i : i + self.pattern_length])
        next_c  = non_verde[i + self.pattern_length]
        if next_c in ("ROJO", "NEGRO"):
            self.pattern_counts[pattern][next_c] += 1

    def predict(self, spin_history: list) -> Optional[dict]:
        non_verde = [s["real"] for s in spin_history if s["real"] != "VERDE"]
        if len(non_verde) < self.pattern_length:
            return None
        pattern = tuple(non_verde[-self.pattern_length:])
        counts  = dict(self.pattern_counts.get(pattern, {}))
        total   = sum(counts.values())
        if total < 2:
            return None
        return {
            "ROJO":  counts.get("ROJO",  0) / total,
            "NEGRO": counts.get("NEGRO", 0) / total,
            "total": total,
        }

# ─── CATEGORY ML PREDICTOR – COLOR + PARIDAD + RANGO ─────────────────────────
class CategoryMLPredictor:
    def __init__(self, pattern_length: int = 3):
        self.pattern_length = pattern_length
        self.color_counts   = defaultdict(lambda: defaultdict(int))
        self.par_counts     = defaultdict(lambda: defaultdict(int))
        self.rang_counts    = defaultdict(lambda: defaultdict(int))
        self.color_history  = []
        self.par_history    = []
        self.rang_history   = []

    def _update(self, history: list, counts: dict, new_val: str):
        history.append(new_val)
        n = len(history)
        if n >= self.pattern_length + 1:
            pattern = tuple(history[-(self.pattern_length + 1):-1])
            counts[pattern][new_val] += 1

    def add_spin(self, number: int, real_color: str):
        if real_color == "VERDE":
            return
        self._update(self.color_history, self.color_counts, real_color)
        par = get_paridad(number)
        if par:
            self._update(self.par_history, self.par_counts, par)
        rang = get_rango(number)
        if rang:
            self._update(self.rang_history, self.rang_counts, rang)

    def _predict(self, history: list, counts: dict) -> Optional[dict]:
        if len(history) < self.pattern_length:
            return None
        pattern = tuple(history[-self.pattern_length:])
        c = dict(counts.get(pattern, {}))
        total = sum(c.values())
        if total < 2:
            return None
        result = {k: v / total for k, v in c.items()}
        result["total"] = total
        return result

    def predict_color(self)   -> Optional[dict]:
        return self._predict(self.color_history,  self.color_counts)

    def predict_paridad(self) -> Optional[dict]:
        return self._predict(self.par_history,   self.par_counts)

    def predict_rango(self)   -> Optional[dict]:
        return self._predict(self.rang_history,  self.rang_counts)

# ─── SISTEMA DE PROBABILIDAD CONJUNTA PONDERADA ────────────────────────────────
class UnifiedProbabilitySystem:
    def __init__(self):
        self.weights = {"markov": 0.35, "ml": 0.65}
        self.prediction_history: deque = deque(maxlen=200)
        self.markov_correct: int = 0
        self.markov_total:   int = 0
        self.ml_correct:     int = 0
        self.ml_total:       int = 0
        self.confidence_factor: float = 0.5
        self.volatility:        float = 1.0
        self.current_streak:    int   = 0
        self.streak_direction: Optional[str] = None
        self.spins_since_weight_update: int = 0
        self.WEIGHT_UPDATE_INTERVAL:    int = 50
        self.base_threshold:    float = 0.50
        self.dynamic_threshold: float = 0.50
        self.ema_trend_factor:  float = 1.0
        self.sr_factor:         float = 1.0

    def calculate_volatility(self, levels: list) -> float:
        if len(levels) < 20: return 1.0
        std_dev = np.std(levels[-20:])
        normalized = min(max(std_dev / 5.0, 0.5), 1.5)
        self.volatility = normalized
        return normalized

    def update_streak(self, color: str):
        if self.streak_direction == color:
            self.current_streak += 1
        else:
            self.streak_direction = color
            self.current_streak = 1

    def update_trend_factors(self, levels: list):
        if len(levels) < 20:
            self.ema_trend_factor = 1.0
            self.sr_factor = 1.0
            return
        ema20 = self._calculate_single_ema(levels, 20)
        if ema20 is not None and levels:
            current = levels[-1]
            diff = (current - ema20) / (abs(ema20) + 1) * 0.2
            self.ema_trend_factor = max(0.8, min(1.2, 1.0 + diff if current > ema20 else 1.0 - abs(diff)))
        sr = find_support_resistance(levels, lookback=30)
        if sr['support'] is not None and sr['resistance'] is not None:
            range_size = sr['resistance'] - sr['support']
            if range_size > 0:
                pos = (levels[-1] - sr['support']) / range_size
                self.sr_factor = max(0.9, min(1.1, 1.0 + (pos - 0.5) * 0.1))
        else:
            self.sr_factor = 1.0

    def _calculate_single_ema(self, data: list, period: int) -> Optional[float]:
        if len(data) < period: return None
        mult = 2 / (period + 1)
        prev = sum(data[:period]) / period
        for i in range(period, len(data)):
            prev = (data[i] * mult) + (prev * (1 - mult))
        return prev

    def calculate_confidence(self, markov_pred, ml_pred, color: str) -> float:
        if markov_pred is None and ml_pred is None: return 0.3
        if markov_pred is None or ml_pred is None: return 0.5
        m_prob  = markov_pred.get(color, 0.5)
        ml_prob = ml_pred.get(color, 0.5)
        agreement = 1.0 - abs(m_prob - ml_prob)
        self.confidence_factor = 0.4 + agreement * 0.6
        return self.confidence_factor

    def calculate_dynamic_threshold(self) -> float:
        vol_factor    = self.volatility
        streak_factor = 1.0 + min(self.current_streak * 0.02, 0.3)
        conf_factor   = 1.0 - (self.confidence_factor - 0.5) * 0.4
        self.dynamic_threshold = max(0.45, min(0.65,
            self.base_threshold * vol_factor * streak_factor * conf_factor))
        return self.dynamic_threshold

    def record_prediction(self, color: str, markov_pred, ml_pred, actual: str):
        self.prediction_history.append({
            "color": color,
            "markov_pred": markov_pred.get(color, 0.5) if markov_pred else None,
            "ml_pred":     ml_pred.get(color, 0.5)     if ml_pred     else None,
            "actual":      actual,
            "timestamp":   time.time()
        })
        if markov_pred is not None:
            self.markov_total += 1
            if (markov_pred.get(color, 0) > 0.5) == (actual == color):
                self.markov_correct += 1
        if ml_pred is not None:
            self.ml_total += 1
            if (ml_pred.get(color, 0) > 0.5) == (actual == color):
                self.ml_correct += 1

    def update_weights(self):
        self.spins_since_weight_update += 1
        if self.spins_since_weight_update < self.WEIGHT_UPDATE_INTERVAL: return
        self.spins_since_weight_update = 0
        markov_acc = self.markov_correct / max(self.markov_total, 1)
        ml_acc     = self.ml_correct     / max(self.ml_total,     1)
        total_acc  = markov_acc + ml_acc
        if total_acc > 0:
            self.weights["markov"] = markov_acc / total_acc
            self.weights["ml"]     = ml_acc     / total_acc
        self.weights["markov"] = max(0.2, min(0.6, self.weights["markov"]))
        self.weights["ml"]     = max(0.4, min(0.8, self.weights["ml"]))
        total = self.weights["markov"] + self.weights["ml"]
        self.weights["markov"] /= total
        self.weights["ml"]     /= total
        logger.info(f"[AMX V22] Pesos: Markov={self.weights['markov']:.2f} ML={self.weights['ml']:.2f} | M={markov_acc:.2%} ML={ml_acc:.2%}")
        self.markov_correct = self.markov_total = self.ml_correct = self.ml_total = 0

    def get_joint_probability(self, markov_pred, ml_pred, color: str, table_prob: float) -> dict:
        markov_prob = markov_pred.get(color, 0.5) if markov_pred else 0.5
        ml_prob     = ml_pred.get(color, 0.5)     if ml_pred     else 0.5
        model_prob  = self.weights["markov"] * markov_prob + self.weights["ml"] * ml_prob
        confidence  = self.calculate_confidence(markov_pred, ml_pred, color)
        if markov_pred is None and ml_pred is None:
            combined_prob = table_prob
        else:
            table_weight  = max(0.1, 1.0 - confidence) * 0.3
            combined_prob = (1.0 - table_weight) * model_prob + table_weight * table_prob
        combined_prob *= self.ema_trend_factor * self.sr_factor
        combined_prob  = max(0.3, min(0.9, combined_prob))
        threshold      = self.calculate_dynamic_threshold()
        signal_strength = ("strong"   if combined_prob >= threshold + 0.1 else
                           "moderate" if combined_prob >= threshold        else "weak")
        return {
            "combined_prob":    combined_prob,
            "markov_prob":      markov_prob,
            "ml_prob":          ml_prob,
            "table_prob":       table_prob,
            "confidence":       confidence,
            "threshold":        threshold,
            "signal_strength":  signal_strength,
            "weights":          self.weights.copy(),
            "ema_trend_factor": self.ema_trend_factor,
            "sr_factor":        self.sr_factor,
            "volatility":       self.volatility,
        }

# ─── DETAILED STATS ───────────────────────────────────────────────────────────
class DetailedStats:
    def __init__(self):
        self.signal_history: deque = deque(maxlen=50)
        self.wins_attempt_1: int = 0
        self.wins_attempt_2: int = 0
        self.wins_attempt_3: int = 0
        self.wins_attempt_4: int = 0
        self.wins_attempt_5: int = 0
        self.losses:         int = 0
        self.total_signals:  int = 0
        self.history_24h: deque = deque()
        self.batch_start_bankroll: Optional[float] = None
        self.batch_start_wins:  int = 0
        self.batch_start_losses:int = 0
        self.batch_start_w1:    int = 0
        self.batch_start_w2:    int = 0
        self.batch_start_w3:    int = 0
        self.batch_start_w4:    int = 0
        self.batch_start_w5:    int = 0
        self.last_stats_at: int = 0

    def record_signal_result(self, attempt_won: int, final_result: bool,
                             bet_amount: float, bankroll: float):
        entry = {"attempt_won": attempt_won, "won": final_result,
                 "bet": bet_amount, "bankroll": bankroll, "timestamp": time.time()}
        self.signal_history.append(entry)
        self.total_signals += 1
        if final_result:
            if attempt_won == 1:   self.wins_attempt_1 += 1
            elif attempt_won == 2: self.wins_attempt_2 += 1
            elif attempt_won == 3: self.wins_attempt_3 += 1
            elif attempt_won == 4: self.wins_attempt_4 += 1
            elif attempt_won == 5: self.wins_attempt_5 += 1
        else:
            self.losses += 1
        self.history_24h.append(entry)
        self._trim_24h()

    def _trim_24h(self):
        cutoff = time.time() - 86400
        while self.history_24h and self.history_24h[0]["timestamp"] < cutoff:
            self.history_24h.popleft()

    def should_send_stats(self) -> bool:
        return (self.total_signals - self.last_stats_at) >= 20

    def mark_stats_sent(self, bankroll: float):
        self.last_stats_at        = self.total_signals
        self.batch_start_bankroll = bankroll
        self.batch_start_wins     = (self.wins_attempt_1 + self.wins_attempt_2 +
                                     self.wins_attempt_3 + self.wins_attempt_4 +
                                     self.wins_attempt_5)
        self.batch_start_losses   = self.losses
        self.batch_start_w1 = self.wins_attempt_1
        self.batch_start_w2 = self.wins_attempt_2
        self.batch_start_w3 = self.wins_attempt_3
        self.batch_start_w4 = self.wins_attempt_4
        self.batch_start_w5 = self.wins_attempt_5

    def get_batch_stats(self, current_bankroll: float) -> dict:
        n = self.total_signals - self.last_stats_at
        if n == 0: return {}
        w1 = self.wins_attempt_1 - self.batch_start_w1
        w2 = self.wins_attempt_2 - self.batch_start_w2
        w3 = self.wins_attempt_3 - self.batch_start_w3
        w4 = self.wins_attempt_4 - self.batch_start_w4
        w5 = self.wins_attempt_5 - self.batch_start_w5
        l  = self.losses - self.batch_start_losses
        w  = w1 + w2 + w3 + w4 + w5
        return {"total": n, "wins": w, "losses": l,
                "w1": w1, "w2": w2, "w3": w3, "w4": w4, "w5": w5,
                "efficiency": round(w/n*100,1) if n else 0.0,
                "e_w1": round(w1/n*100,2) if n else 0.0,
                "e_w2": round(w2/n*100,2) if n else 0.0,
                "e_w3": round(w3/n*100,2) if n else 0.0,
                "e_w4": round(w4/n*100,2) if n else 0.0,
                "e_w5": round(w5/n*100,2) if n else 0.0,
                "e_loss": round(l/n*100,2) if n else 0.0,
                "bankroll_delta": round(current_bankroll - self.batch_start_bankroll, 2)
                    if self.batch_start_bankroll is not None else 0.0}

    def get_24h_stats(self, current_bankroll: float) -> dict:
        self._trim_24h()
        t = len(self.history_24h)
        if t == 0: return {}
        w  = sum(1 for e in self.history_24h if e["won"])
        l  = t - w
        w1 = sum(1 for e in self.history_24h if e["attempt_won"] == 1)
        w2 = sum(1 for e in self.history_24h if e["attempt_won"] == 2)
        w3 = sum(1 for e in self.history_24h if e["attempt_won"] == 3)
        w4 = sum(1 for e in self.history_24h if e["attempt_won"] == 4)
        w5 = sum(1 for e in self.history_24h if e["attempt_won"] == 5)
        bk24 = (round(self.history_24h[-1]["bankroll"] - self.history_24h[0]["bankroll"], 2)
                if t >= 2 else 0.0)
        return {"total": t, "wins": w, "losses": l,
                "w1": w1, "w2": w2, "w3": w3, "w4": w4, "w5": w5,
                "efficiency": round(w/t*100,1) if t else 0.0,
                "e_w1": round(w1/t*100,2) if t else 0.0,
                "e_w2": round(w2/t*100,2) if t else 0.0,
                "e_w3": round(w3/t*100,2) if t else 0.0,
                "e_w4": round(w4/t*100,2) if t else 0.0,
                "e_w5": round(w5/t*100,2) if t else 0.0,
                "e_loss": round(l/t*100,2) if t else 0.0,
                "bankroll_delta": bk24}

    def reset(self):
        self.signal_history.clear(); self.history_24h.clear()
        self.wins_attempt_1 = self.wins_attempt_2 = self.wins_attempt_3 = 0
        self.wins_attempt_4 = self.wins_attempt_5 = 0
        self.losses = self.total_signals = self.last_stats_at = 0
        self.batch_start_bankroll = None

# ─── AMX SIGNAL SYSTEM (sin cooldown) ────────────────────────────────────────
class AMXSignalSystem:
    def __init__(self, mode: Literal["tendencia", "moderado"] = "moderado"):
        self.mode = mode
        self.last_signal_time: float = 0
        self.cooldown_seconds: int   = 0
        self.so_cooldown: Optional[float] = None
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
        if len(positions) < 20: return None
        ema4  = self.calculate_ema(positions, 4)
        ema8  = self.calculate_ema(positions, 8)
        ema20 = self.calculate_ema(positions, 20)
        if any(v is None for v in [ema4[-1], ema8[-1], ema20[-1], ema4[-2], ema8[-2], ema20[-2]]):
            return None
        current_pos   = positions[-1]
        cruce_alcista = ema4[-2] <= ema20[-2] and ema4[-1] > ema20[-1]
        sobre_tres    = current_pos > ema4[-1] and current_pos > ema8[-1] and current_pos > ema20[-1]
        cruce_ema8    = ema8[-2] <= ema20[-2] and ema8[-1] > ema20[-1]
        cerca_ema4    = abs(current_pos - ema4[-1]) <= 0.5
        dos_ok        = len(self.last_two_expected) >= 2 and all(self.last_two_expected)
        if not ((cruce_alcista or sobre_tres) or cruce_ema8 or
                (sobre_tres and dos_ok) or (sobre_tres and cerca_ema4)):
            return None
        entry = next((e for e in color_data if e["id"] == current_number), None)
        if not entry or entry["senal"] == "NO APOSTAR": return None
        prob = entry["rojo"] if expected_color == "ROJO" else entry["negro"]
        if entry["senal"] != expected_color or prob < prob_threshold: return None
        return {"type": "SKRILL_2.0", "mode": "tendencia",
                "expected_color": expected_color,
                "probability": prob, "trigger_number": current_number,
                "strength": "strong" if (cruce_alcista or cruce_ema8) else "moderate"}

    def check_signal_moderado(self, positions, color_data, current_number,
                              expected_color, prob_threshold) -> Optional[dict]:
        if len(positions) < 20: return None
        ema4  = self.calculate_ema(positions, 4)
        ema8  = self.calculate_ema(positions, 8)
        ema20 = self.calculate_ema(positions, 20)
        if any(v is None for v in [ema4[-1], ema8[-1], ema20[-1], ema8[-2], ema20[-2]]):
            return None
        cruce_ema8  = ema8[-2] <= ema20[-2] and ema8[-1] > ema20[-1]
        sobre_emas  = positions[-1] > ema4[-1] and positions[-1] > ema8[-1]
        patron_v    = False
        if len(positions) >= 3:
            a, b, c = positions[-3], positions[-2], positions[-1]
            patron_v = b < a and b < c and abs(a - c) <= 1 and c > a
        dos_ok   = len(self.last_two_expected) >= 2 and all(self.last_two_expected)
        emas_alc = ema4[-1] > ema8[-1] > ema20[-1]
        cond_racha = dos_ok and emas_alc and sobre_emas
        if not (cruce_ema8 or patron_v or cond_racha): return None
        entry = next((e for e in color_data if e["id"] == current_number), None)
        if not entry or entry["senal"] == "NO APOSTAR": return None
        prob = entry["rojo"] if expected_color == "ROJO" else entry["negro"]
        if entry["senal"] != expected_color or prob < prob_threshold: return None
        return {"type": "ALERTA_2.0", "mode": "moderado",
                "expected_color": expected_color,
                "probability": prob, "trigger_number": current_number,
                "pattern": "V" if patron_v else "EMA_CROSS"}

    def register_signal_sent(self):
        self.last_signal_time = time.time()

    def register_so_failed(self):
        self.so_cooldown = time.time()

# ─── SOPORTE Y RESISTENCIA ────────────────────────────────────────────────────
def find_support_resistance(levels: list, lookback: int = 30) -> dict:
    if len(levels) < lookback:
        return {'support': None, 'resistance': None}
    recent = levels[-lookback:]
    supp, res = [], []
    for i in range(2, len(recent) - 2):
        if recent[i] < recent[i-1] and recent[i] < recent[i-2] and \
           recent[i] < recent[i+1] and recent[i] < recent[i+2]:
            supp.append(recent[i])
        if recent[i] > recent[i-1] and recent[i] > recent[i-2] and \
           recent[i] > recent[i+1] and recent[i] > recent[i+2]:
            res.append(recent[i])
    return {'support': supp[-1] if supp else None, 'resistance': res[-1] if res else None}

# ─── CHART GENERATION ────────────────────────────────────────────────────────
def generate_chart(levels: list, spin_history: list, bet_color: str,
                   visible: int = VISIBLE,
                   markov_pred: Optional[dict] = None,
                   ml_pred: Optional[dict] = None,
                   unified_prob: Optional[dict] = None) -> io.BytesIO:
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
    visible_levels = arr[sl]
    last_level = visible_levels[-1] if len(visible_levels) > 0 else 0
    lookback_50 = min(50, len(arr))
    recent_50 = arr[-lookback_50:]
    min_level_50, max_level_50 = np.min(recent_50), np.max(recent_50)
    data_range = max_level_50 - min_level_50
    margin = max(data_range * 0.15, 1.0)
    offset_from_last_to_min = last_level - min_level_50
    y_min = min_level_50 - margin - offset_from_last_to_min * 0.3
    y_max = max_level_50 + margin + offset_from_last_to_min * 0.3
    visible_height = y_max - y_min
    last_level_position = (last_level - y_min) / visible_height if visible_height > 0 else 0.5
    if last_level_position < 0.2:
        y_min = last_level - visible_height * 0.2
    elif last_level_position > 0.8:
        y_max = last_level + visible_height * 0.2
    is_rojo = bet_color == "ROJO"
    bg, ax_bg, grid_c = "#0b101f", "#0f1a2a", "#1e2e48"
    line_c  = "#e84040" if is_rojo else "#9090bb"
    ema4_c, ema8_c, ema20_c = "#ff9f43", "#48dbfb", "#1dd1a1"
    title_c = "#ff8080" if is_rojo else "#b0b8d0"
    fig, ax = plt.subplots(figsize=(8, 3.8), facecolor=bg)
    ax.set_facecolor(ax_bg)
    y, e4, e8, e20 = arr[sl], ema4[sl], ema8[sl], ema20[sl]
    ax.fill_between(x, y, alpha=0.10, color=line_c)
    ax.plot(x, y, color=line_c, linewidth=0.8, zorder=3)
    ax.plot(x, e4, color=ema4_c, linewidth=0.7, linestyle="--", label="EMA 4", zorder=4)
    ax.plot(x, e8, color=ema8_c, linewidth=0.7, linestyle="--", label="EMA 8", zorder=4)
    ax.plot(x, e20, color=ema20_c, linewidth=1.0, label="EMA 20", zorder=4)
    ax.set_ylim(y_min, y_max)
    dot_colors = {"ROJO": "#e84040", "NEGRO": "#aaaacc", "VERDE": "#2ecc71"}
    for i, spin in enumerate(hist_sl):
        c = dot_colors.get(spin["real"], "#ffffff")
        ax.scatter(i, y[i], color=c, s=22, zorder=5, edgecolors="white", linewidths=0.3)
    sr = find_support_resistance(levels, lookback=30)
    sup_v, res_v = sr['support'], sr['resistance']
    res_color = "#e84040" if is_rojo else "#888888"
    sup_color = "#888888" if is_rojo else "#e84040"
    if sup_v is not None:
        ax.axhline(y=sup_v, color=sup_color, linestyle='--', linewidth=1.5, alpha=0.7)
        ax.text(x[-1], sup_v, f' S {sup_v:.1f}', color=sup_color, fontsize=7, va='bottom', ha='right')
    if res_v is not None:
        ax.axhline(y=res_v, color=res_color, linestyle='--', linewidth=1.5, alpha=0.7)
        ax.text(x[-1], res_v, f' R {res_v:.1f}', color=res_color, fontsize=7, va='top', ha='right')
    tick_step = max(1, len(x) // 8)
    tick_x    = list(range(0, len(x), tick_step))
    tick_lbs  = [str(hist_sl[i]["number"]) if i < len(hist_sl) else "" for i in tick_x]
    ax.set_xticks(tick_x); ax.set_xticklabels(tick_lbs, color="#8899bb", fontsize=7)
    ax.tick_params(axis='y', colors="#8899bb", labelsize=7)
    ax.tick_params(axis='x', colors="#8899bb", labelsize=7)
    ax.spines['bottom'].set_color(grid_c); ax.spines['left'].set_color(grid_c)
    ax.spines['top'].set_visible(False);   ax.spines['right'].set_visible(False)
    ax.grid(axis='y', color=grid_c, linewidth=0.4, alpha=0.5)
    pred_info = ""
    if unified_prob:
        pred_info += f" | Unif:{unified_prob['combined_prob']*100:.0f}%"
        pred_info += f" | M:{unified_prob['markov_prob']*100:.0f}% ML:{unified_prob['ml_prob']*100:.0f}%"
    emoji = "🔴" if is_rojo else "⚫️"
    ax.set_title(f"{emoji} {bet_color} — últimos {visible} giros · EMA 4/8/20{pred_info}",
                 color=title_c, fontsize=8.5, pad=6)
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
    if sup_v is not None:
        legend_els.append(Line2D([0],[0], color=sup_color, linestyle='--', linewidth=1.5, label='Soporte'))
    if res_v is not None:
        legend_els.append(Line2D([0],[0], color=res_color, linestyle='--', linewidth=1.5, label='Resistencia'))
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
            logger.warning(f"Telegram error (attempt {attempt}/{_TG_MAX_RETRIES}): {e}")
            if attempt < _TG_MAX_RETRIES:
                time.sleep(delay)
                delay = min(delay * 2, 60)
            else:
                logger.error(f"Telegram call failed: {e}")
                return None

def tg_send_photo(bot_inst, chat_id, thread_id, photo_buf, caption) -> Optional[int]:
    photo_buf.seek(0)
    msg = _tg_call(bot_inst.send_photo, chat_id=chat_id, photo=photo_buf,
                   caption=caption, parse_mode="HTML", message_thread_id=thread_id)
    return msg.message_id if msg else None

def tg_send_text(bot_inst, chat_id, thread_id, text) -> Optional[int]:
    msg = _tg_call(bot_inst.send_message, chat_id=chat_id, text=text,
                   parse_mode="HTML", message_thread_id=thread_id)
    return msg.message_id if msg else None

def tg_delete(bot_inst, chat_id, msg_id):
    _tg_call(bot_inst.delete_message, chat_id=chat_id, message_id=msg_id)

# ─── ROULETTE ENGINE ──────────────────────────────────────────────────────────
class RouletteEngine:
    def __init__(self, name: str, cfg: dict):
        self.name       = name
        self.bot        = cfg["bot"]
        self.ws_key     = cfg["ws_key"]
        self.chat_id    = cfg["chat_id"]
        self.thread_id  = cfg["thread_id"]
        self.db_table   = cfg["db_table"]
        self.color_data = cfg["color_data"]

        self.spin_history:      list = []
        self.original_levels:   list = []
        self.inverted_levels:   list = []
        self.last_nonzero_color: Optional[str] = None
        self.anti_block: set  = set()

        # ── Estado de la señal ─────────────────────────────────
        self.signal_active:          bool = False
        self.waiting_for_attempt:    bool = False
        self.waiting_attempt_number: int  = 0
        self.skip_one_after_zero:    bool = False

        self.active_category:  Optional[str] = None
        self.bet_value:        Optional[str] = None
        self.bet_color:        Optional[str] = None
        self.attempts_left:    int  = 0
        self.total_attempts:   int  = 0
        self.trigger_number:   Optional[int] = None

        self.signal_msg_ids: list = []
        self.waiting_msg_id: Optional[int] = None
        self.result_sequence: deque = deque(maxlen=10)

        # ── Labouchère ────────────────────────────────────────
        self.bet_sys = Labouchere(LABOUCHERE_SEQUENCE, BASE_BET)

        # ── Recuperación ──────────────────────────────────────
        self.consec_losses:   int   = 0
        self.recovery_active: bool  = False
        self.recovery_target: float = 0.0
        self.level1_bankroll: float = 0.0
        self.signal_is_level1: bool = False

        # ── AMX V22 ───────────────────────────────────────────
        self.amx_system = AMXSignalSystem(mode="moderado")
        self.min_prob_threshold = cfg.get("min_prob_threshold", 0.49)

        # ── Probabilidad Unificada ─────────────────────────────
        self.unified_prob_system = UnifiedProbabilitySystem()

        # ── Predictores individuales (se mantienen para compatibilidad) ──
        self.markov       = MarkovChainPredictor(window=60, order=2)
        self.ml_predictor = MLPatternPredictor(pattern_length=3)
        self.category_ml  = CategoryMLPredictor(pattern_length=3)

        # ── NUEVO: Predictor combinado (512 patrones) ────────────────────
        self.combined_markov = CombinedMarkovPredictor(order=3)
        self.combined_history: list = []   # secuencia de códigos "R-P-B", etc.

        # ── Estadísticas ──────────────────────────────────────
        self.stats = DetailedStats()

        self.ws      = None
        self.running = True

        # ── Calentamiento WebSocket ────────────────────────────
        self.ws_spins_count: int  = 0
        self.warmup_done:    bool = False

        # ── Pre-entrenamiento ─────────────────────────────────
        self._pretrain_from_db(DB_PATH, self.db_table)

    # ─── PRE-ENTRENAMIENTO DESDE DB ──────────────────────────────────────────
    def _pretrain_from_db(self, db_path: str, table_name: str):
        if not os.path.exists(db_path):
            logger.warning(f"[{self.name}] DB no encontrada: {db_path}")
            return
        spins = []
        try:
            pattern = re.compile(rf'INSERT INTO "{table_name}" VALUES \(\d+,(\d+),')
            with open(db_path, "r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    m = pattern.search(line)
                    if m:
                        spins.append(int(m.group(1)))
        except Exception as e:
            logger.error(f"[{self.name}] Error leyendo DB: {e}")
            return

        if not spins:
            logger.warning(f"[{self.name}] No se encontraron spins en tabla '{table_name}'")
            return

        temp_history = []
        temp_combined = []
        for n in spins:
            real = REAL_COLOR_MAP.get(n, "VERDE")
            temp_history.append({"number": n, "real": real})
            self.markov.update(temp_history)
            self.ml_predictor.add_spin(temp_history)
            self.category_ml.add_spin(n, real)
            comb = get_combined_class(n, real)
            if comb:
                temp_combined.append(comb)

        self.combined_history = temp_combined[-300:]  # mantener tamaño manejable
        self.combined_markov.update(self.combined_history)

        logger.info(f"[{self.name}] Pre-entrenado con {len(spins)} giros (tabla: {table_name})")

    # ─── HELPERS ─────────────────────────────────────────────────────────────
    def set_mode(self, mode: Literal["tendencia", "moderado"]):
        self.amx_system = AMXSignalSystem(mode=mode)
        logger.info(f"[{self.name}] Modo → {mode}")

    @staticmethod
    def calculate_ema(data: list, period: int) -> list:
        if len(data) < period:
            return [None] * len(data)
        mult = 2 / (period + 1)
        out  = [None] * (period - 1)
        prev = sum(data[:period]) / period
        out.append(prev)
        for i in range(period, len(data)):
            prev = (data[i] * mult) + (prev * (1 - mult))
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

    def _opposite_color(self, color: str) -> str:
        return "NEGRO" if color == "ROJO" else "ROJO"

    def _category_icon(self, value: str) -> str:
        return CATEGORY_ICONS.get(value, "❓")

    def _trigger_display_short(self, number: int, category: str, bet_value: str) -> str:
        """Formatea '16 PAR 🟣' o '14 ROJO 🔴' según la categoría."""
        if category == "COLOR":
            real = REAL_COLOR_MAP.get(number, "VERDE")
            icon = CATEGORY_ICONS.get(real, "❓")
            return f"{number} {real} {icon}"
        elif category == "PARIDAD":
            par = get_paridad(number)
            icon = CATEGORY_ICONS.get(par, "❓") if par else "🟢"
            return f"{number} {par} {icon}" if par else f"{number} VERDE 🟢"
        else:  # RANGO
            rang = get_rango(number)
            icon = CATEGORY_ICONS.get(rang, "❓") if rang else "🟢"
            return f"{number} {rang} {icon}" if rang else f"{number} VERDE 🟢"

    def _is_win(self, number: int, real: str) -> Optional[bool]:
        if number == 0:
            return None
        if self.active_category == "COLOR":
            return real == self.bet_value
        elif self.active_category == "PARIDAD":
            return get_paridad(number) == self.bet_value
        else:  # RANGO
            return get_rango(number) == self.bet_value

    # ─── DETECCIÓN DE SEÑAL USANDO MARGINALIZACIÓN (NUEVO) ─────────────────
    def _detect_best_category_signal(self) -> Optional[dict]:
        """Evalúa las 3 categorías vía marginalización del predictor combinado."""
        if len(self.combined_history) < self.combined_markov.order:
            return None

        pred = self.combined_markov.predict(self.combined_history)
        if not pred:
            return None

        # Eliminar 'total'
        probs = {k: v for k, v in pred.items() if k != "total"}
        if not probs:
            return None

        # Marginalizar para cada categoría
        color_probs = {"ROJO": 0.0, "NEGRO": 0.0}
        par_probs   = {"PAR": 0.0, "IMPAR": 0.0}
        rang_probs  = {"BAJO": 0.0, "ALTO": 0.0}

        for comb, prob in probs.items():
            vals = combined_to_values(comb)
            color_probs[vals["color"]] += prob
            par_probs[vals["paridad"]] += prob
            rang_probs[vals["rango"]] += prob

        # Encontrar el mejor valor en cada categoría
        best_color = max(color_probs, key=color_probs.get)
        best_par   = max(par_probs, key=par_probs.get)
        best_rang  = max(rang_probs, key=rang_probs.get)

        p_color = color_probs[best_color]
        p_par   = par_probs[best_par]
        p_rang  = rang_probs[best_rang]

        # Umbral mínimo
        candidates = []
        if p_color >= self.min_prob_threshold:
            candidates.append(("COLOR", best_color, p_color))
        if p_par >= self.min_prob_threshold:
            candidates.append(("PARIDAD", best_par, p_par))
        if p_rang >= self.min_prob_threshold:
            candidates.append(("RANGO", best_rang, p_rang))

        if not candidates:
            return None

        # Elegir la categoría con mayor probabilidad marginal
        best_cat, best_val, best_prob = max(candidates, key=lambda x: x[2])
        trigger_number = self.spin_history[-1]["number"] if self.spin_history else 0

        return {
            "category": best_cat,
            "bet_value": best_val,
            "probability": best_prob,
            "trigger_number": trigger_number,
        }

    def _best_retry_value(self, trigger_number: int) -> Optional[str]:
        """Re-evalúa la categoría activa usando marginalización."""
        if not self.active_category:
            return None

        pred = self.combined_markov.predict(self.combined_history)
        if not pred:
            return None
        probs = {k: v for k, v in pred.items() if k != "total"}
        if not probs:
            return None

        if self.active_category == "COLOR":
            probs_cat = {"ROJO": 0.0, "NEGRO": 0.0}
            for comb, prob in probs.items():
                vals = combined_to_values(comb)
                probs_cat[vals["color"]] += prob
        elif self.active_category == "PARIDAD":
            probs_cat = {"PAR": 0.0, "IMPAR": 0.0}
            for comb, prob in probs.items():
                vals = combined_to_values(comb)
                probs_cat[vals["paridad"]] += prob
        else:  # RANGO
            probs_cat = {"BAJO": 0.0, "ALTO": 0.0}
            for comb, prob in probs.items():
                vals = combined_to_values(comb)
                probs_cat[vals["rango"]] += prob

        best_val = max(probs_cat, key=probs_cat.get)
        if probs_cat[best_val] >= self.min_prob_threshold:
            return best_val
        return None

    def _get_category_probability(self, category: str, bet_value: str,
                                  trigger_number: int) -> dict:
        """Retorna un dict compatible con UnifiedProbabilitySystem para la UI."""
        prob = 0.5
        if category == "COLOR":
            prob = self.get_prob(trigger_number, bet_value)
        # Para simplificar, usamos la probabilidad marginal calculada previamente
        # En la señal principal ya tenemos el valor exacto.
        return {
            "combined_prob": prob,
            "markov_prob": 0.5,
            "ml_prob": 0.5,
            "table_prob": prob,
            "confidence": 0.6,
            "threshold": self.min_prob_threshold,
            "signal_strength": "moderate",
            "weights": self.unified_prob_system.weights.copy(),
            "ema_trend_factor": 1.0,
            "sr_factor": 1.0,
            "volatility": 1.0,
        }

    # ─── AMX POSITIONS ────────────────────────────────────────────────────────
    def _update_amx_positions(self, color: str):
        last_pos = self.amx_system.ultimos_puntos[-1] if self.amx_system.ultimos_puntos else 0
        if color == "ROJO":      new_pos = last_pos + 1
        elif color == "NEGRO":   new_pos = last_pos - 1
        else:                    new_pos = last_pos
        self.amx_system.ultimos_puntos.append(new_pos)
        if len(self.amx_system.ultimos_puntos) > 300:
            self.amx_system.ultimos_puntos = self.amx_system.ultimos_puntos[-200:]

    # ─── ENVÍO DE MENSAJES ────────────────────────────────────────────────────
    def _build_caption(self, attempt: int, unified_prob: Optional[dict]) -> str:
        bet = self.bet_sys.current_bet()
        prob_pct = int((unified_prob["combined_prob"] if unified_prob else 0.5) * 100)
        valor_display = f"{self.bet_value} {self._category_icon(self.bet_value)}"
        trig_disp = self._trigger_display_short(self.trigger_number, self.active_category, self.bet_value)
        return (
            f"☑️☑️ <b>SEÑAL CONFIRMADA</b> ☑️☑️\n\n"
            f"🎰 Juego: {self.name}\n"
            f"👉 Después de: {trig_disp}\n"
            f"🎯 Apostar a: {valor_display}\n"
            f"🤖 Probabilidad Unificada: {prob_pct}%\n"
            f"🎲 Labouchère: [{self.bet_sys.sequence_display()}]\n"
            f"📍 Apuesta: {bet:.2f} usd\n\n"
            f"♻️ Intento {attempt}/{MAX_ATTEMPTS}"
        )

    def _send_signal(self, attempt: int, unified_prob: Optional[dict] = None):
        self.signal_is_level1 = (self.bet_sys.is_fresh() and not self.recovery_active)
        if self.signal_is_level1:
            self.level1_bankroll = self.bet_sys.bankroll
        caption = self._build_caption(attempt, unified_prob)

        # Gráfico: usamos el color de referencia (para COLOR es el valor, para otros fijo)
        if self.active_category == "COLOR":
            chart_color = self.bet_value
        else:
            chart_color = "ROJO"  # neutro
        self.bet_color = chart_color
        levels = self.original_levels[:] if chart_color == "ROJO" else self.inverted_levels[:]
        mp  = self.markov.predict(self.spin_history)
        ml  = self.ml_predictor.predict(self.spin_history)
        chart = generate_chart(levels, self.spin_history[:], chart_color,
                               markov_pred=mp, ml_pred=ml, unified_prob=unified_prob)

        msg_id = tg_send_photo(self.bot, self.chat_id, self.thread_id, chart, caption)
        if msg_id:
            self.signal_msg_ids.append(msg_id)
        logger.info(f"[{self.name}] Señal [{self.active_category}] {self.bet_value} "
                    f"trig={self.trigger_number} prob={int((unified_prob['combined_prob'] if unified_prob else 0.5)*100)}%")

    def _send_waiting_message(self, attempt_number: int):
        for msg_id in self.signal_msg_ids:
            tg_delete(self.bot, self.chat_id, msg_id)
        self.signal_msg_ids = []
        if self.waiting_msg_id:
            tg_delete(self.bot, self.chat_id, self.waiting_msg_id)
            self.waiting_msg_id = None
        ords = {2:"2°", 3:"3°", 4:"4°", 5:"5°"}
        ord_str = ords.get(attempt_number, f"{attempt_number}°")
        caption = (
            f"⚠️ <b>Esperando condiciones para el {ord_str} intento</b>\n\n"
            f"🎰 <b>{self.name}</b>\n"
            f"🔍 <i>Analizando siguiente oportunidad...</i>\n"
        )
        chart_color = self.bet_color or "ROJO"
        levels = self.original_levels[:] if chart_color == "ROJO" else self.inverted_levels[:]
        mp = self.markov.predict(self.spin_history)
        ml = self.ml_predictor.predict(self.spin_history)
        chart = generate_chart(levels, self.spin_history[:], chart_color,
                               markov_pred=mp, ml_pred=ml)
        msg_id = tg_send_photo(self.bot, self.chat_id, self.thread_id, chart, caption)
        if msg_id:
            self.waiting_msg_id = msg_id

    def _send_result(self, number: int, real: str, won: bool, bet: float,
                     attempt_won: int, delete_signals: bool = True):
        bankroll = self.bet_sys.bankroll
        icon = {"ROJO": "🔴", "NEGRO": "⚫️", "VERDE": "🟢"}.get(real, "❓")
        if delete_signals:
            for msg_id in self.signal_msg_ids:
                tg_delete(self.bot, self.chat_id, msg_id)
            self.signal_msg_ids = []
            if self.waiting_msg_id:
                tg_delete(self.bot, self.chat_id, self.waiting_msg_id)
                self.waiting_msg_id = None
        chart_color = self.bet_color or "ROJO"
        levels = self.original_levels[:] if chart_color == "ROJO" else self.inverted_levels[:]
        mp = self.markov.predict(self.spin_history)
        ml = self.ml_predictor.predict(self.spin_history)
        chart = generate_chart(levels, self.spin_history[:], chart_color,
                               markov_pred=mp, ml_pred=ml)
        result_text = f"{'✅' if won else '❌'} Resultado: {number} {icon} — {'Acierto!' if won else 'Fallo'}"
        tg_send_photo(self.bot, self.chat_id, self.thread_id, chart, result_text)
        logger.info(f"[{self.name}] {'WIN' if won else 'LOSS'} #{number} bankroll={bankroll:.2f}")

    def _check_stats(self):
        if not self.stats.should_send_stats(): return
        current_bankroll = self.bet_sys.bankroll
        s20 = self.stats.get_batch_stats(current_bankroll)
        s24 = self.stats.get_24h_stats(current_bankroll)
        self.stats.mark_stats_sent(current_bankroll)
        if not s20 and not s24: return
        stats_text = ""
        if s20:
            stats_text += (
                f"👉🏼 <b>ESTADISTICAS {s20['total']} SENALES</b>\n"
                f"🈯️ <b>T:</b> {s20['total']} 📈 <b>E:</b> {s20['efficiency']}%\n"
                f"1️⃣ <b>W:</b> {s20['w1']} --> <b>E:</b> {s20['e_w1']}%\n"
                f"2️⃣ <b>W:</b> {s20['w2']} --> <b>E:</b> {s20['e_w2']}%\n"
                f"3️⃣ <b>W:</b> {s20['w3']} --> <b>E:</b> {s20['e_w3']}%\n"
                f"4️⃣ <b>W:</b> {s20['w4']} --> <b>E:</b> {s20['e_w4']}%\n"
                f"5️⃣ <b>W:</b> {s20['w5']} --> <b>E:</b> {s20['e_w5']}%\n"
                f"🈲 <b>L:</b> {s20['losses']} --> <b>E:</b> {s20['e_loss']}%\n"
                f"💰 <i>Bankroll: {s20['bankroll_delta']:.2f} usd</i>\n\n"
            )
        if s24:
            stats_text += (
                f"👉🏼 <b>ESTADISTICAS 24 HORAS</b>\n"
                f"🈯️ <b>T:</b> {s24['total']} 📈 <b>E:</b> {s24['efficiency']}%\n"
                f"1️⃣ <b>W:</b> {s24['w1']} --> <b>E:</b> {s24['e_w1']}%\n"
                f"2️⃣ <b>W:</b> {s24['w2']} --> <b>E:</b> {s24['e_w2']}%\n"
                f"3️⃣ <b>W:</b> {s24['w3']} --> <b>E:</b> {s24['e_w3']}%\n"
                f"4️⃣ <b>W:</b> {s24['w4']} --> <b>E:</b> {s24['e_w4']}%\n"
                f"5️⃣ <b>W:</b> {s24['w5']} --> <b>E:</b> {s24['e_w5']}%\n"
                f"🈲 <b>L:</b> {s24['losses']} --> <b>E:</b> {s24['e_loss']}%\n"
                f"💰 <i>Bankroll: {s24['bankroll_delta']:.2f} usd</i>\n"
            )
        tg_send_text(self.bot, self.chat_id, self.thread_id, stats_text)

    # ─── PROCESO PRINCIPAL ────────────────────────────────────────────────────
    def process_number(self, number: int):
        real = REAL_COLOR_MAP.get(number, "VERDE")

        # Historial
        self.spin_history.append({"number": number, "real": real})
        if len(self.spin_history) > 300:
            self.spin_history.pop(0)
        self.result_sequence.append({"number": number, "real": real})

        # Niveles para gráficos
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

        # Actualizar historial combinado
        comb = get_combined_class(number, real)
        if comb:
            self.combined_history.append(comb)
            if len(self.combined_history) > 300:
                self.combined_history.pop(0)
            self.combined_markov.update(self.combined_history)

        # AMX
        self._update_amx_positions(real)
        self.amx_system.update_streak(real, self.get_signal(number))

        # Predictores individuales (mantenidos para gráficos/compatibilidad)
        self.markov.update(self.spin_history)
        self.ml_predictor.add_spin(self.spin_history)
        self.category_ml.add_spin(number, real)

        # Calentamiento WS
        if not self.warmup_done:
            self.ws_spins_count += 1
            if self.ws_spins_count < WARMUP_SPINS:
                logger.info(f"[{self.name}] Calentamiento WS: {self.ws_spins_count}/{WARMUP_SPINS} giros")
                return
            self.warmup_done = True
            logger.info(f"[{self.name}] ✅ Calentamiento completado ({WARMUP_SPINS} giros). Iniciando señales.")
            tg_send_text(self.bot, self.chat_id, self.thread_id,
                         f"🟢 <b>{self.name}</b> — Sistema listo. Iniciando señales a partir del giro {WARMUP_SPINS}.")

        # ══════════════════════════════════════════════════════════════════════
        #  MÁQUINA DE ESTADOS
        # ══════════════════════════════════════════════════════════════════════

        # Señal activa
        if self.signal_active:
            result = self._is_win(number, real)
            if result is None:  # Verde
                self.attempts_left -= 1
                if self.attempts_left <= 0:
                    self._handle_full_loss(number, real)
                    return
                attempt_number = MAX_ATTEMPTS - self.attempts_left + 1
                if self.signal_msg_ids:
                    tg_delete(self.bot, self.chat_id, self.signal_msg_ids.pop())
                self.signal_active = False
                self.waiting_for_attempt = True
                self.waiting_attempt_number = attempt_number
                self.skip_one_after_zero = True
                self._send_waiting_message(attempt_number)
                return

            current_attempt = MAX_ATTEMPTS - self.attempts_left + 1
            if result:
                bet = self.bet_sys.win()
                self.stats.record_signal_result(current_attempt, True, bet, self.bet_sys.bankroll)
                self.signal_active   = False
                self.active_category = None
                self._check_recovery()
                self._send_result(number, real, True, bet, current_attempt)
                self._check_stats()
                self.signal_msg_ids = []
            else:
                self.attempts_left -= 1
                bet = self.bet_sys.loss()
                if self.attempts_left <= 0:
                    self._handle_full_loss(number, real, bet)
                else:
                    attempt_number = MAX_ATTEMPTS - self.attempts_left + 1
                    if self.signal_msg_ids:
                        tg_delete(self.bot, self.chat_id, self.signal_msg_ids.pop())
                    chosen = self._best_retry_value(number)
                    if chosen is not None:
                        self.bet_value      = chosen
                        self.trigger_number = number
                        unified_prob = self._get_category_probability(
                            self.active_category, chosen, number)
                        self._send_signal(attempt_number, unified_prob)
                    else:
                        self.signal_active          = False
                        self.waiting_for_attempt    = True
                        self.waiting_attempt_number = attempt_number
                        self._send_waiting_message(attempt_number)

        # Esperando reintento
        elif self.waiting_for_attempt:
            if real == "VERDE":
                self.skip_one_after_zero = True
                return
            if self.skip_one_after_zero:
                self.skip_one_after_zero = False
                return
            attempt_number = self.waiting_attempt_number
            chosen = self._best_retry_value(number)
            if chosen is not None:
                if self.waiting_msg_id:
                    tg_delete(self.bot, self.chat_id, self.waiting_msg_id)
                    self.waiting_msg_id = None
                self.bet_value          = chosen
                self.trigger_number     = number
                self.signal_active      = True
                self.waiting_for_attempt = False
                unified_prob = self._get_category_probability(
                    self.active_category, chosen, number)
                self._send_signal(attempt_number, unified_prob)

        # Idle
        else:
            self.signal_msg_ids = []
            best = self._detect_best_category_signal()
            if best:
                self.signal_active   = True
                self.active_category = best["category"]
                self.bet_value       = best["bet_value"]
                self.bet_color       = best["bet_value"] if best["category"] == "COLOR" else "ROJO"
                self.attempts_left   = MAX_ATTEMPTS
                self.total_attempts  = MAX_ATTEMPTS
                self.trigger_number  = best["trigger_number"]
                unified_prob = self._get_category_probability(
                    best["category"], best["bet_value"], best["trigger_number"])
                unified_prob["combined_prob"] = best["probability"]
                self._send_signal(1, unified_prob)
                self.amx_system.register_signal_sent()

    def _handle_full_loss(self, number: int, real: str, bet: float = None):
        if bet is None:
            bet = self.bet_sys.loss()
        self.consec_losses += 1
        if self.consec_losses >= 10:
            self.consec_losses = 0
            self.recovery_active = False
            self.recovery_target = 0.0
        else:
            self.recovery_active = True
            self.recovery_target = self.level1_bankroll + BASE_BET
        self.stats.record_signal_result(0, False, bet, self.bet_sys.bankroll)
        self.signal_active   = False
        self.active_category = None
        self._send_result(number, real, False, bet, 0)
        self._check_stats()
        self.signal_msg_ids = []

    def _check_recovery(self):
        if not self.recovery_active: return
        if self.bet_sys.bankroll >= self.recovery_target:
            logger.info(f"[{self.name}] Recuperación completada!")
            self.consec_losses = 0
            self.recovery_active = False
            self.recovery_target = 0.0
            self.bet_sys.reset()

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
                    logger.info(f"[{self.name}] WS conectado")
                    await ws.send(json.dumps({
                        "type": "subscribe", "casinoId": CASINO_ID,
                        "currency": "USD", "key": [self.ws_key],
                    }))
                    async for message in ws:
                        if not self.running: break
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
                                if len(self.anti_block) > 1000:
                                    self.anti_block.clear()
                                self.anti_block.add(gid)
                                self.process_number(n)
            except Exception as e:
                logger.warning(f"[{self.name}] WS error: {e}. Reconectando en {reconnect_delay}s")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 60)

# ─── FLASK KEEPALIVE ──────────────────────────────────────────────────────────
app = Flask(__name__)

@app.route("/")
def index():
    return jsonify({"status": "ok", "bot": "Roulette Signal Bot AMX V22", "ts": time.time()})

@app.route("/ping")
def ping():
    return jsonify({"pong": True, "ts": time.time()})

@app.route("/health")
def health():
    return jsonify({"healthy": True})

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

def _register_handlers(b: telebot.TeleBot):
    @b.message_handler(commands=['start', 'help'])
    def cmd_start(message):
        seq = " - ".join(str(v) for v in LABOUCHERE_SEQUENCE)
        help_text = f"""
<b>🎰 Roulette Bot AMX UNIFIED (Combinado)</b>
Dos ruletas en un proceso: Russian Roulette + Azure Roulette

<b>Características:</b>
• Sin cooldown entre señales
• 5 intentos por señal (Labouchère)
• Secuencia actual: <code>{seq}</code>
• Calentamiento WS: 21 giros silenciosos
• Predicción basada en combinaciones (512 patrones)
• Selección automática de la mejor categoría (COLOR/PARIDAD/RANGO)

Comandos:
/moderado - Modo MODERADO
/tendencia - Modo TENDENCIA
/status - Estado de ruletas
/secuencia 1 2 1 - Cambiar secuencia Labouchère
/reset - Resetear estadísticas
/help - Esta ayuda
        """
        b.reply_to(message, help_text, parse_mode="HTML")

    @b.message_handler(commands=['moderado'])
    def cmd_moderado(message):
        for engine in engines.values():
            engine.set_mode("moderado")
        b.reply_to(message, "✅ <b>Modo MODERADO activado</b>", parse_mode="HTML")

    @b.message_handler(commands=['tendencia'])
    def cmd_tendencia(message):
        for engine in engines.values():
            engine.set_mode("tendencia")
        b.reply_to(message, "📈 <b>Modo TENDENCIA activado</b>", parse_mode="HTML")

    @b.message_handler(commands=['status'])
    def cmd_status(message):
        lines = ["<b>📊 ESTADO AMX UNIFIED</b>\n"]
        for name, engine in engines.items():
            mode_icon = "📈" if engine.amx_system.mode == "tendencia" else "📊"
            if engine.signal_active:
                cat  = engine.active_category or "?"
                val  = engine.bet_value or "?"
                icon = CATEGORY_ICONS.get(val, "")
                st   = f"🟢 [{cat}] {val}{icon} intento {MAX_ATTEMPTS - engine.attempts_left + 1}/{MAX_ATTEMPTS}"
            elif engine.waiting_for_attempt:
                st = f"⏳ Esperando intento {engine.waiting_attempt_number}/{MAX_ATTEMPTS}"
            else:
                st = "⚪ Idle"
            lines.append(f"<b>{name}</b>: {mode_icon} — {st}")
        b.reply_to(message, "\n".join(lines), parse_mode="HTML")

    @b.message_handler(commands=['secuencia'])
    def cmd_secuencia(message):
        global LABOUCHERE_SEQUENCE
        parts = message.text.strip().split()[1:]
        if not parts:
            seq_str = " - ".join(str(v) for v in LABOUCHERE_SEQUENCE)
            b.reply_to(message,
                f"🎲 Secuencia actual: <code>{seq_str}</code>\n"
                f"Uso: /secuencia 1 2 1",
                parse_mode="HTML")
            return
        try:
            new_seq = [int(x) for x in parts if int(x) > 0]
            if not new_seq:
                raise ValueError("secuencia vacía")
        except ValueError:
            b.reply_to(message,
                "⚠️ Formato inválido. Usa números enteros positivos.\n"
                "Ejemplo: <code>/secuencia 1 2 3 2 1</code>",
                parse_mode="HTML")
            return
        LABOUCHERE_SEQUENCE = new_seq
        for engine in engines.values():
            engine.bet_sys.set_sequence(new_seq)
        seq_str = " - ".join(str(v) for v in new_seq)
        total_units = sum(new_seq)
        b.reply_to(message,
            f"✅ <b>Secuencia Labouchère actualizada</b>\n\n"
            f"🎲 Nueva secuencia: <code>{seq_str}</code>\n"
            f"💰 Apuesta inicial: {(new_seq[0]+new_seq[-1])*BASE_BET:.2f} usd "
            f"({new_seq[0]+new_seq[-1]} unidades)\n"
            f"📊 Total a recuperar: {total_units*BASE_BET:.2f} usd ({total_units} unidades)",
            parse_mode="HTML")

    @b.message_handler(commands=['reset'])
    def cmd_reset(message):
        for engine in engines.values():
            engine.stats = DetailedStats()
        b.reply_to(message, "🔄 <b>Estadísticas reseteadas</b>", parse_mode="HTML")

# ─── MAIN ─────────────────────────────────────────────────────────────────────
def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)

async def main():
    global engines
    engines = {name: RouletteEngine(name, cfg) for name, cfg in ROULETTE_CONFIGS.items()}
    tasks   = [asyncio.create_task(e.run_ws()) for e in engines.values()]
    tasks.append(asyncio.create_task(self_ping_loop()))

    _register_handlers(bot_russian)
    _register_handlers(bot_azure)

    def _poll(b: telebot.TeleBot, label: str):
        logger.info(f"Iniciando polling Telegram — {label}")
        b.polling(none_stop=True, interval=1, timeout=30)

    for b, lbl in [(bot_russian, "Russian"), (bot_azure, "Azure")]:
        threading.Thread(target=_poll, args=(b, lbl), daemon=True).start()

    logger.info("🎰 Roulette Bot AMX UNIFIED (Combinado) iniciado")
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    logger.info("Flask server started.")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped.")
