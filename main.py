#!/usr/bin/env python3
"""
Roulette Telegram Signal Bot - Sistema AMX V23
CAMBIOS V23:
  1. pattern_length = 4 (ML y CategoryML)
  2. Señales COLOR siguen Markov+ML (no tabla). Cero = ACIERTO en señal activa.
     No se activa señal cuando el número disparador es 0.
  3. Markov extiende análisis a PARIDAD y RANGO.
     Scoring multi-factor: ML + Markov + EMA + SR + streaks + acuerdo entre modelos.
  4. Mejora intento 1 y 2: umbral inicial elevado, confirmación de tendencia,
     acuerdo dual obligatorio, mínimo de muestras > 10, threshold dinámico por intento.
  5. Análisis estadístico aplicado: las señales exigen mayor calidad de patrón
     para reducir W3 y Loss y aumentar W1/W2.
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

# ─── DB CONFIG ────────────────────────────────────────────────────────────────
DB_PATH  = "russian-azure.db"
DB_TABLE = "roulette_1"

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

COLOR_DATA = [
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
    "Azure Roulette": {
        "ws_key": 227,
        "chat_id": -1003835197023,
        "thread_id": 6,
        "color_data": COLOR_DATA,
        "betting_system": "dalembert",
        "min_prob_threshold": 0.49,
        # Umbral de calidad para emitir señal inicial (más estricto)
        "signal_quality_threshold": 0.54,
    },
}

WS_URL    = "wss://dga.pragmaticplaylive.net/ws"
CASINO_ID = "ppcjd00000007254"
MAX_ATTEMPTS = 3
BASE_BET  = 0.10
VISIBLE   = 50

# Umbrales por intento para reintentos
RETRY_THRESHOLD = {1: 0.54, 2: 0.56, 3: 0.59}
# Mínimo de muestras por intento en el predictor ML
MIN_SAMPLES = {1: 10, 2: 6, 3: 5}

# ─── D'ALEMBERT ───────────────────────────────────────────────────────────────
class D_Alembert:
    def __init__(self, base: float):
        self.base = base; self.step = 0; self.bankroll = 0.0; self.max_step = 20

    def current_bet(self) -> float:
        return round(self.base * (self.step + 1), 2)

    def win(self) -> float:
        bet = self.current_bet()
        self.bankroll = round(self.bankroll + bet, 2)
        if self.step > 0: self.step -= 1
        return bet

    def loss(self) -> float:
        bet = self.current_bet()
        self.bankroll = round(self.bankroll - bet, 2)
        if self.step >= self.max_step - 1: self.step = 0
        else: self.step += 1
        return bet

# ─── MARKOV CHAIN EXTENDIDO (COLOR + PARIDAD + RANGO, ventana 60) ─────────────
class MarkovChainPredictor:
    """
    Cadena de Markov orden 2 que analiza COLOR, PARIDAD y RANGO
    sobre los últimos 60 giros.
    """
    def __init__(self, window: int = 60, order: int = 2):
        self.window = window
        self.order  = order
        # Tablas de transición independientes por categoría
        self.color_trans: dict = {}
        self.par_trans:   dict = {}
        self.rang_trans:  dict = {}

    def _build_trans(self, seq: list) -> dict:
        trans = defaultdict(lambda: defaultdict(int))
        if len(seq) < self.order + 1:
            return trans
        for i in range(len(seq) - self.order):
            state  = tuple(seq[i : i + self.order])
            next_v = seq[i + self.order]
            trans[state][next_v] += 1
        return trans

    def update(self, spin_history: list):
        recent = spin_history[-self.window:]
        colors = [s["real"] for s in recent if s["real"] in ("ROJO", "NEGRO")]
        pars   = [get_paridad(s["number"]) for s in recent if get_paridad(s["number"])]
        rangs  = [get_rango(s["number"])   for s in recent if get_rango(s["number"])]
        self.color_trans = self._build_trans(colors)
        self.par_trans   = self._build_trans(pars)
        self.rang_trans  = self._build_trans(rangs)

    def _predict_seq(self, seq: list, trans: dict, min_total: int = 5) -> Optional[dict]:
        if len(seq) < self.order: return None
        state  = tuple(seq[-self.order:])
        counts = dict(trans.get(state, {}))
        total  = sum(counts.values())
        if total < min_total: return None
        result = {k: v / total for k, v in counts.items()}
        result["total"] = total
        return result

    def predict(self, spin_history: list) -> Optional[dict]:
        return self.predict_color(spin_history)

    def predict_color(self, spin_history: list) -> Optional[dict]:
        seq = [s["real"] for s in spin_history if s["real"] in ("ROJO", "NEGRO")]
        return self._predict_seq(seq, self.color_trans)

    def predict_paridad(self, spin_history: list) -> Optional[dict]:
        seq = [get_paridad(s["number"]) for s in spin_history if get_paridad(s["number"])]
        return self._predict_seq(seq, self.par_trans)

    def predict_rango(self, spin_history: list) -> Optional[dict]:
        seq = [get_rango(s["number"]) for s in spin_history if get_rango(s["number"])]
        return self._predict_seq(seq, self.rang_trans)

# ─── ML PATTERN PREDICTOR – COLOR (historial completo, pattern_length=4) ──────
class MLPatternPredictor:
    """Predictor ML de COLOR con patrón de longitud 4."""
    def __init__(self, pattern_length: int = 4):
        self.pattern_length = pattern_length
        self.pattern_counts: dict = defaultdict(lambda: defaultdict(int))
        self._known_len: int = 0

    def add_spin(self, spin_history: list):
        non_verde = [s["real"] for s in spin_history if s["real"] != "VERDE"]
        current_len = len(non_verde)
        if current_len <= self._known_len: return
        self._known_len = current_len
        if current_len < self.pattern_length + 1: return
        i = current_len - self.pattern_length - 1
        pattern = tuple(non_verde[i : i + self.pattern_length])
        next_c  = non_verde[i + self.pattern_length]
        if next_c in ("ROJO", "NEGRO"):
            self.pattern_counts[pattern][next_c] += 1

    def predict(self, spin_history: list, min_total: int = 2) -> Optional[dict]:
        non_verde = [s["real"] for s in spin_history if s["real"] != "VERDE"]
        if len(non_verde) < self.pattern_length: return None
        pattern = tuple(non_verde[-self.pattern_length:])
        counts  = dict(self.pattern_counts.get(pattern, {}))
        total   = sum(counts.values())
        if total < min_total: return None
        return {
            "ROJO":  counts.get("ROJO",  0) / total,
            "NEGRO": counts.get("NEGRO", 0) / total,
            "total": total,
        }

# ─── CATEGORY ML PREDICTOR (pattern_length=4) ─────────────────────────────────
class CategoryMLPredictor:
    """
    Predictor ML para COLOR, PARIDAD y RANGO con patrón de longitud 4.
    Mínimo de muestras configurable para controlar la calidad de predicción.
    """
    def __init__(self, pattern_length: int = 4):
        self.pattern_length = pattern_length
        self.color_counts = defaultdict(lambda: defaultdict(int))
        self.par_counts   = defaultdict(lambda: defaultdict(int))
        self.rang_counts  = defaultdict(lambda: defaultdict(int))
        self.color_history: list = []
        self.par_history:   list = []
        self.rang_history:  list = []

    def _update(self, history: list, counts: dict, new_val: str):
        history.append(new_val)
        if len(history) >= self.pattern_length + 1:
            pattern = tuple(history[-(self.pattern_length + 1):-1])
            counts[pattern][new_val] += 1

    def add_spin(self, number: int, real_color: str):
        if real_color == "VERDE": return
        self._update(self.color_history, self.color_counts, real_color)
        par  = get_paridad(number)
        rang = get_rango(number)
        if par:  self._update(self.par_history,  self.par_counts,  par)
        if rang: self._update(self.rang_history, self.rang_counts, rang)

    def _predict(self, history: list, counts: dict, min_total: int = 2) -> Optional[dict]:
        if len(history) < self.pattern_length: return None
        pattern = tuple(history[-self.pattern_length:])
        c = dict(counts.get(pattern, {}))
        total = sum(c.values())
        if total < min_total: return None
        result = {k: v / total for k, v in c.items()}
        result["total"] = total
        return result

    def predict_color(self,   min_total: int = 2) -> Optional[dict]:
        return self._predict(self.color_history, self.color_counts, min_total)

    def predict_paridad(self, min_total: int = 2) -> Optional[dict]:
        return self._predict(self.par_history,   self.par_counts,   min_total)

    def predict_rango(self,   min_total: int = 2) -> Optional[dict]:
        return self._predict(self.rang_history,  self.rang_counts,  min_total)

# ─── SISTEMA DE PROBABILIDAD CONJUNTA PONDERADA ────────────────────────────────
class UnifiedProbabilitySystem:
    """Markov=0.35, ML=0.65 con actualización adaptativa cada 50 resultados."""
    def __init__(self):
        self.weights = {"markov": 0.35, "ml": 0.65}
        self.prediction_history: deque = deque(maxlen=200)
        self.markov_correct = self.markov_total = 0
        self.ml_correct     = self.ml_total     = 0
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
        self.volatility = min(max(float(np.std(levels[-20:])) / 5.0, 0.5), 1.5)
        return self.volatility

    def update_streak(self, color: str):
        if self.streak_direction == color: self.current_streak += 1
        else: self.streak_direction = color; self.current_streak = 1

    def update_trend_factors(self, levels: list):
        if len(levels) < 20:
            self.ema_trend_factor = self.sr_factor = 1.0
            return
        ema20 = self._ema_single(levels, 20)
        if ema20 is not None:
            diff = (levels[-1] - ema20) / (abs(ema20) + 1) * 0.2
            self.ema_trend_factor = max(0.8, min(1.2,
                1.0 + diff if levels[-1] > ema20 else 1.0 - abs(diff)))
        sr = find_support_resistance(levels, lookback=30)
        if sr['support'] is not None and sr['resistance'] is not None:
            rng = sr['resistance'] - sr['support']
            if rng > 0:
                pos = (levels[-1] - sr['support']) / rng
                self.sr_factor = max(0.9, min(1.1, 1.0 + (pos - 0.5) * 0.1))
        else:
            self.sr_factor = 1.0

    def _ema_single(self, data: list, period: int) -> Optional[float]:
        if len(data) < period: return None
        mult = 2 / (period + 1)
        prev = sum(data[:period]) / period
        for i in range(period, len(data)):
            prev = data[i] * mult + prev * (1 - mult)
        return prev

    def calculate_confidence(self, markov_pred, ml_pred, color: str) -> float:
        if markov_pred is None and ml_pred is None: return 0.3
        if markov_pred is None or ml_pred is None:  return 0.5
        agreement = 1.0 - abs(markov_pred.get(color, 0.5) - ml_pred.get(color, 0.5))
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
            "actual": actual, "timestamp": time.time()
        })
        if markov_pred:
            self.markov_total += 1
            if (markov_pred.get(color, 0) > 0.5) == (actual == color):
                self.markov_correct += 1
        if ml_pred:
            self.ml_total += 1
            if (ml_pred.get(color, 0) > 0.5) == (actual == color):
                self.ml_correct += 1

    def update_weights(self):
        self.spins_since_weight_update += 1
        if self.spins_since_weight_update < self.WEIGHT_UPDATE_INTERVAL: return
        self.spins_since_weight_update = 0
        m_acc = self.markov_correct / max(self.markov_total, 1)
        ml_acc = self.ml_correct    / max(self.ml_total,    1)
        total_acc = m_acc + ml_acc
        if total_acc > 0:
            self.weights["markov"] = m_acc  / total_acc
            self.weights["ml"]     = ml_acc / total_acc
        self.weights["markov"] = max(0.2, min(0.6, self.weights["markov"]))
        self.weights["ml"]     = max(0.4, min(0.8, self.weights["ml"]))
        t = self.weights["markov"] + self.weights["ml"]
        self.weights["markov"] /= t; self.weights["ml"] /= t
        logger.info(f"[AMX V23] Pesos M={self.weights['markov']:.2f} ML={self.weights['ml']:.2f} | acc M={m_acc:.2%} ML={ml_acc:.2%}")
        self.markov_correct = self.markov_total = self.ml_correct = self.ml_total = 0

    def get_joint_probability(self, markov_pred, ml_pred, color: str, table_prob: float) -> dict:
        mk_p = markov_pred.get(color, 0.5) if markov_pred else 0.5
        ml_p = ml_pred.get(color, 0.5)     if ml_pred     else 0.5
        model_prob = self.weights["markov"] * mk_p + self.weights["ml"] * ml_p
        confidence = self.calculate_confidence(markov_pred, ml_pred, color)
        if markov_pred is None and ml_pred is None:
            combined = table_prob
        else:
            tw = max(0.1, 1.0 - confidence) * 0.3
            combined = (1.0 - tw) * model_prob + tw * table_prob
        combined = max(0.3, min(0.9, combined * self.ema_trend_factor * self.sr_factor))
        thr = self.calculate_dynamic_threshold()
        return {
            "combined_prob": combined, "markov_prob": mk_p, "ml_prob": ml_p,
            "table_prob": table_prob, "confidence": confidence, "threshold": thr,
            "signal_strength": ("strong" if combined >= thr + 0.1 else
                                "moderate" if combined >= thr else "weak"),
            "weights": self.weights.copy(),
            "ema_trend_factor": self.ema_trend_factor,
            "sr_factor": self.sr_factor, "volatility": self.volatility,
        }

# ─── DETAILED STATS ───────────────────────────────────────────────────────────
class DetailedStats:
    def __init__(self):
        self.signal_history: deque = deque(maxlen=50)
        self.wins_attempt_1 = self.wins_attempt_2 = self.wins_attempt_3 = 0
        self.losses = self.total_signals = 0
        self.history_24h: deque = deque()
        self.batch_start_bankroll: Optional[float] = None
        self.batch_start_wins = self.batch_start_losses = 0
        self.batch_start_w1 = self.batch_start_w2 = self.batch_start_w3 = 0
        self.last_stats_at: int = 0

    def record_signal_result(self, attempt_won: int, final_result: bool,
                             bet_amount: float, bankroll: float):
        entry = {"attempt_won": attempt_won, "won": final_result,
                 "bet": bet_amount, "bankroll": bankroll, "timestamp": time.time()}
        self.signal_history.append(entry)
        self.total_signals += 1
        if final_result:
            if   attempt_won == 1: self.wins_attempt_1 += 1
            elif attempt_won == 2: self.wins_attempt_2 += 1
            elif attempt_won == 3: self.wins_attempt_3 += 1
        else: self.losses += 1
        self.history_24h.append(entry)
        self._trim_24h()

    def _trim_24h(self):
        cutoff = time.time() - 86400
        while self.history_24h and self.history_24h[0]["timestamp"] < cutoff:
            self.history_24h.popleft()

    def should_send_stats(self) -> bool:
        return (self.total_signals - self.last_stats_at) >= 20

    def mark_stats_sent(self, bankroll: float):
        self.last_stats_at = self.total_signals
        self.batch_start_bankroll = bankroll
        self.batch_start_w1 = self.wins_attempt_1
        self.batch_start_w2 = self.wins_attempt_2
        self.batch_start_w3 = self.wins_attempt_3
        self.batch_start_losses = self.losses

    def _make_stats_dict(self, items, bankroll_delta):
        t = len(items)
        if t == 0: return {}
        w  = sum(1 for e in items if e["won"])
        l  = t - w
        w1 = sum(1 for e in items if e["attempt_won"] == 1)
        w2 = sum(1 for e in items if e["attempt_won"] == 2)
        w3 = sum(1 for e in items if e["attempt_won"] == 3)
        return {"total": t, "wins": w, "losses": l, "w1": w1, "w2": w2, "w3": w3,
                "efficiency": round(w/t*100,1),
                "e_w1": round(w1/t*100,2), "e_w2": round(w2/t*100,2),
                "e_w3": round(w3/t*100,2), "e_loss": round(l/t*100,2),
                "bankroll_delta": bankroll_delta}

    def get_batch_stats(self, current_bankroll: float) -> dict:
        n = self.total_signals - self.last_stats_at
        if n == 0: return {}
        recent = list(self.signal_history)[-n:]
        delta  = (round(current_bankroll - self.batch_start_bankroll, 2)
                  if self.batch_start_bankroll is not None else 0.0)
        return self._make_stats_dict(recent, delta)

    def get_24h_stats(self, current_bankroll: float) -> dict:
        self._trim_24h()
        items = list(self.history_24h)
        if not items: return {}
        bk = (round(items[-1]["bankroll"] - items[0]["bankroll"], 2) if len(items) >= 2 else 0.0)
        return self._make_stats_dict(items, bk)

    def reset(self):
        self.signal_history.clear(); self.history_24h.clear()
        self.wins_attempt_1 = self.wins_attempt_2 = self.wins_attempt_3 = 0
        self.losses = self.total_signals = self.last_stats_at = 0
        self.batch_start_bankroll = None

# ─── AMX SIGNAL SYSTEM (sin cooldown, solo EMAs) ─────────────────────────────
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
        if len(data) < period: return [None] * len(data)
        mult = 2 / (period + 1)
        ema  = [None] * (period - 1)
        prev = sum(data[:period]) / period
        ema.append(prev)
        for i in range(period, len(data)):
            prev = data[i] * mult + prev * (1 - mult)
            ema.append(prev)
        return ema

    def check_ema_conditions(self, positions: list, color_candidate: str) -> bool:
        """
        Verifica condiciones EMA puras sin usar la tabla de probabilidades.
        Devuelve True si las EMAs favorecen el color candidato.
        """
        if len(positions) < 20: return False
        ema4  = self.calculate_ema(positions, 4)
        ema8  = self.calculate_ema(positions, 8)
        ema20 = self.calculate_ema(positions, 20)
        if any(v is None for v in [ema4[-1], ema8[-1], ema20[-1], ema8[-2], ema20[-2]]):
            return False
        cur = positions[-1]
        cruce_ema8   = ema8[-2] <= ema20[-2] and ema8[-1] > ema20[-1]
        sobre_emas   = cur > ema4[-1] and cur > ema8[-1]
        emas_alcistas = ema4[-1] > ema8[-1] > ema20[-1]
        patron_v     = False
        if len(positions) >= 3:
            a, b, c = positions[-3], positions[-2], positions[-1]
            patron_v = b < a and b < c and abs(a - c) <= 1 and c > a
        dos_ok = len(self.last_two_expected) >= 2 and all(self.last_two_expected)
        cond_racha = dos_ok and emas_alcistas and sobre_emas
        return cruce_ema8 or patron_v or cond_racha

    def register_signal_sent(self):
        self.last_signal_time = time.time()

# ─── SOPORTE Y RESISTENCIA ────────────────────────────────────────────────────
def find_support_resistance(levels: list, lookback: int = 30) -> dict:
    if len(levels) < lookback: return {'support': None, 'resistance': None}
    recent = levels[-lookback:]
    supp, res = [], []
    for i in range(2, len(recent) - 2):
        if all(recent[i] < recent[j] for j in [i-1, i-2, i+1, i+2]):
            supp.append(recent[i])
        if all(recent[i] > recent[j] for j in [i-1, i-2, i+1, i+2]):
            res.append(recent[i])
    return {'support': supp[-1] if supp else None, 'resistance': res[-1] if res else None}

# ─── CHART GENERATION ────────────────────────────────────────────────────────
def generate_chart(levels: list, spin_history: list, bet_color: str,
                   visible: int = VISIBLE,
                   markov_pred: Optional[dict] = None,
                   ml_pred: Optional[dict] = None,
                   unified_prob: Optional[dict] = None) -> io.BytesIO:
    arr = np.array(levels, dtype=float); n = len(arr)
    def calc_ema(data, period):
        if len(data) < period: return np.full(len(data), np.nan)
        mult = 2 / (period + 1)
        out = np.full(len(data), np.nan)
        out[period - 1] = np.mean(data[:period])
        for i in range(period, len(data)):
            out[i] = (data[i] - out[i-1]) * mult + out[i-1]
        return out
    ema4 = calc_ema(arr,4); ema8 = calc_ema(arr,8); ema20 = calc_ema(arr,20)
    start = max(0, n - visible); sl = slice(start, n)
    x = np.arange(len(arr[sl])); hist_sl = spin_history[start:]
    vl = arr[sl]
    last_level = vl[-1] if len(vl) > 0 else 0
    rb = min(50, len(arr)); r50 = arr[-rb:]
    mn50, mx50 = float(np.min(r50)), float(np.max(r50))
    dr = mx50 - mn50; margin = max(dr * 0.15, 1.0)
    off = last_level - mn50
    y_min = mn50 - margin - off * 0.3; y_max = mx50 + margin + off * 0.3
    vh = y_max - y_min
    lp = (last_level - y_min) / vh if vh > 0 else 0.5
    if lp < 0.2: y_min = last_level - vh * 0.2
    elif lp > 0.8: y_max = last_level + vh * 0.2
    is_rojo = bet_color == "ROJO"
    bg, ax_bg, grid_c = "#0b101f", "#0f1a2a", "#1e2e48"
    line_c = "#e84040" if is_rojo else "#9090bb"
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
    dc = {"ROJO": "#e84040", "NEGRO": "#aaaacc", "VERDE": "#2ecc71"}
    for i, spin in enumerate(hist_sl):
        ax.scatter(i, y[i], color=dc.get(spin["real"], "#fff"),
                   s=22, zorder=5, edgecolors="white", linewidths=0.3)
    sr = find_support_resistance(levels, lookback=30)
    sup_v, res_v = sr['support'], sr['resistance']
    rc = "#e84040" if is_rojo else "#888888"; sc = "#888888" if is_rojo else "#e84040"
    if sup_v is not None:
        ax.axhline(y=sup_v, color=sc, linestyle='--', linewidth=1.5, alpha=0.7)
        ax.text(x[-1], sup_v, f' S {sup_v:.1f}', color=sc, fontsize=7, va='bottom', ha='right')
    if res_v is not None:
        ax.axhline(y=res_v, color=rc, linestyle='--', linewidth=1.5, alpha=0.7)
        ax.text(x[-1], res_v, f' R {res_v:.1f}', color=rc, fontsize=7, va='top', ha='right')
    tick_step = max(1, len(x) // 8)
    tick_x    = list(range(0, len(x), tick_step))
    tick_lbs  = [str(hist_sl[i]["number"]) if i < len(hist_sl) else "" for i in tick_x]
    ax.set_xticks(tick_x); ax.set_xticklabels(tick_lbs, color="#8899bb", fontsize=7)
    ax.tick_params(axis='y', colors="#8899bb", labelsize=7)
    ax.tick_params(axis='x', colors="#8899bb", labelsize=7)
    ax.spines['bottom'].set_color(grid_c); ax.spines['left'].set_color(grid_c)
    ax.spines['top'].set_visible(False); ax.spines['right'].set_visible(False)
    ax.grid(axis='y', color=grid_c, linewidth=0.4, alpha=0.5)
    pred_info = ""
    if unified_prob:
        pred_info += f" | Unif:{unified_prob['combined_prob']*100:.0f}%"
        pred_info += f" | M:{unified_prob['markov_prob']*100:.0f}% ML:{unified_prob['ml_prob']*100:.0f}%"
    emoji = "🔴" if is_rojo else "⚫️"
    ax.set_title(f"{emoji} {bet_color} — últimos {visible} giros · EMA 4/8/20{pred_info}",
                 color=title_c, fontsize=8.5, pad=6)
    from matplotlib.lines import Line2D
    les = [
        Line2D([0],[0], color=line_c,  linewidth=0.8, label="Nivel"),
        Line2D([0],[0], color=ema4_c,  linewidth=0.7, linestyle="--", label="EMA 4"),
        Line2D([0],[0], color=ema8_c,  linewidth=0.7, linestyle="--", label="EMA 8"),
        Line2D([0],[0], color=ema20_c, linewidth=1.0, label="EMA 20"),
        Line2D([0],[0], marker='o', color='w', markerfacecolor='#e84040', markersize=5, label="Rojo"),
        Line2D([0],[0], marker='o', color='w', markerfacecolor='#aaaacc', markersize=5, label="Negro"),
        Line2D([0],[0], marker='o', color='w', markerfacecolor='#2ecc71', markersize=5, label="Verde"),
    ]
    if sup_v: les.append(Line2D([0],[0], color=sc, linestyle='--', linewidth=1.5, label='Soporte'))
    if res_v: les.append(Line2D([0],[0], color=rc, linestyle='--', linewidth=1.5, label='Resistencia'))
    ax.legend(handles=les, loc="upper left", fontsize=6.5,
              facecolor="#0b101f", edgecolor=grid_c, labelcolor="white",
              framealpha=0.8, ncol=2)
    plt.tight_layout(pad=0.8)
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=120, facecolor=bg)
    plt.close(fig); buf.seek(0)
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
                time.sleep(wait); continue
            logger.warning(f"Telegram error (attempt {attempt}/{_TG_MAX_RETRIES}): {e}")
            if attempt < _TG_MAX_RETRIES:
                time.sleep(delay); delay = min(delay * 2, 60)
            else:
                logger.error(f"Telegram call failed: {e}"); return None

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

        self.spin_history:       list = []
        self.original_levels:    list = []
        self.inverted_levels:    list = []
        self.last_nonzero_color: Optional[str] = None
        self.anti_block: set  = set()

        # ── Estado de señal ────────────────────────────────────
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
        self.current_attempt_number: int = 1   # para threshold por intento

        self.signal_msg_ids: list = []
        self.waiting_msg_id: Optional[int] = None
        self.result_sequence: deque = deque(maxlen=10)

        # ── D'Alembert ────────────────────────────────────────
        self.bet_sys = D_Alembert(BASE_BET)

        # ── Recuperación ──────────────────────────────────────
        self.consec_losses:    int   = 0
        self.recovery_active:  bool  = False
        self.recovery_target:  float = 0.0
        self.level1_bankroll:  float = 0.0
        self.signal_is_level1: bool  = False

        # ── AMX V23 ───────────────────────────────────────────
        self.amx_system = AMXSignalSystem(mode="moderado")
        self.min_prob_threshold    = cfg.get("min_prob_threshold", 0.49)
        self.signal_quality_threshold = cfg.get("signal_quality_threshold", 0.54)

        # ── Sistemas ──────────────────────────────────────────
        self.unified_prob_system = UnifiedProbabilitySystem()
        self.markov       = MarkovChainPredictor(window=60, order=2)
        self.ml_predictor = MLPatternPredictor(pattern_length=4)
        self.category_ml  = CategoryMLPredictor(pattern_length=4)
        self.stats        = DetailedStats()

        self.ws      = None
        self.running = True

        # ── Fase de aprendizaje ───────────────────────────────
        self.learning_phase      = True
        self.learning_spin_count = 0
        self.learning_initial_numbers: list = []
        self.max_learning_spins  = 20

        # ── Pre-entrenamiento con DB ──────────────────────────
        self._pretrain_from_db(DB_PATH, DB_TABLE)

    # ─── PRE-ENTRENAMIENTO ────────────────────────────────────────────────────
    def _pretrain_from_db(self, db_path: str, table_name: str):
        if not os.path.exists(db_path):
            logger.warning(f"[{self.name}] DB no encontrada: {db_path}"); return
        spins = []
        try:
            pat = re.compile(rf'INSERT INTO "{table_name}" VALUES \(\d+,(\d+),')
            with open(db_path, "r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    m = pat.search(line)
                    if m: spins.append(int(m.group(1)))
        except Exception as e:
            logger.error(f"[{self.name}] Error DB: {e}"); return
        if not spins:
            logger.warning(f"[{self.name}] Sin spins en '{table_name}'"); return
        tmp = []
        for n in spins:
            real = REAL_COLOR_MAP.get(n, "VERDE")
            tmp.append({"number": n, "real": real})
            self.markov.update(tmp)
            self.ml_predictor.add_spin(tmp)
            self.category_ml.add_spin(n, real)
        logger.info(f"[{self.name}] Pre-entrenado con {len(spins)} giros ({table_name})")

    # ─── HELPERS GENERALES ───────────────────────────────────────────────────
    def set_mode(self, mode: Literal["tendencia", "moderado"]):
        self.amx_system = AMXSignalSystem(mode=mode)
        logger.info(f"[{self.name}] Modo → {mode}")

    @staticmethod
    def calculate_ema(data: list, period: int) -> list:
        if len(data) < period: return [None] * len(data)
        mult = 2 / (period + 1)
        out  = [None] * (period - 1)
        prev = sum(data[:period]) / period
        out.append(prev)
        for i in range(period, len(data)):
            prev = data[i] * mult + prev * (1 - mult); out.append(prev)
        return out

    def get_entry(self, number: int) -> Optional[dict]:
        return next((e for e in self.color_data if e["id"] == number), None)

    def get_signal(self, number: int) -> Optional[str]:
        e = self.get_entry(number); return e["senal"] if e else None

    def get_prob(self, number: int, color: str) -> float:
        e = self.get_entry(number)
        if not e: return 0.0
        return e["rojo"] if color == "ROJO" else e["negro"]

    def _opposite_color(self, c: str) -> str:
        return "NEGRO" if c == "ROJO" else "ROJO"

    def _category_icon(self, value: str) -> str:
        return CATEGORY_ICONS.get(value, "❓")

    def _trigger_display(self, number: int, category: str) -> str:
        if category == "COLOR":
            c = REAL_COLOR_MAP.get(number, "VERDE")
            return f"{number} {c} {self._category_icon(c)}"
        elif category == "PARIDAD":
            par = get_paridad(number)
            return f"{number} {par} {self._category_icon(par)}" if par else f"{number} VERDE 🟢"
        else:
            rang = get_rango(number)
            return f"{number} {rang} {self._category_icon(rang)}" if rang else f"{number} VERDE 🟢"

    # ─── CERO = ACIERTO ───────────────────────────────────────────────────────
    def _is_win(self, number: int, real_color: str) -> bool:
        """
        True  = ganó (incluye número 0 → ACIERTO automático).
        False = perdió.
        Nunca retorna None: el cero siempre cuenta como acierto.
        """
        if number == 0:
            return True   # ← CAMBIO: cero = acierto
        if self.active_category == "COLOR":
            return real_color == self.bet_value
        elif self.active_category == "PARIDAD":
            return get_paridad(number) == self.bet_value
        else:
            return get_rango(number) == self.bet_value

    # ─── EMA FILTER DIRECTO ───────────────────────────────────────────────────
    def _passes_ema_filter(self, color_candidate: str) -> bool:
        """
        Verifica si los niveles están favorablemente posicionados vs EMA20.
        Para COLOR: usa los niveles del color candidato.
        Para PARIDAD/RANGO: usa promedio de ambas series.
        """
        if color_candidate in ("ROJO", "NEGRO"):
            levels = self.original_levels if color_candidate == "ROJO" else self.inverted_levels
            if len(levels) < 20: return True
            ema20 = self.calculate_ema(levels, 20)
            li = len(levels) - 1
            return ema20[li] is not None and levels[li] > ema20[li]
        else:
            # Para PAR/IMPAR/BAJO/ALTO: checar ambas series
            if len(self.original_levels) < 20: return True
            ema20o = self.calculate_ema(self.original_levels, 20)
            li = len(self.original_levels) - 1
            return ema20o[li] is not None and self.original_levels[li] > ema20o[li]

    def _passes_sr_filter(self, color_candidate: str) -> bool:
        """
        Verifica soporte/resistencia. Retorna False solo si hay resistencia
        muy cercana que bloquea la dirección del candidato.
        """
        levels = (self.original_levels if color_candidate in ("ROJO", "BAJO", "PAR")
                  else self.inverted_levels)
        if len(levels) < 30: return True
        sr = find_support_resistance(levels, lookback=30)
        if sr['resistance'] is None: return True
        current = levels[-1]
        gap = sr['resistance'] - current
        # Si estamos a menos del 5% del rango total de la resistencia → bloquear
        rng = (sr['resistance'] - sr['support']) if sr['support'] else 10
        if rng > 0 and gap / rng < 0.05:
            return False
        return True

    # ─── SCORE MULTI-FACTOR PARA UNA CATEGORÍA ────────────────────────────────
    def _score_category(self, category: str, bet_value: str,
                         attempt: int = 1) -> float:
        """
        Score combinado: ML + Markov + EMA + SR + acuerdo + streak_trend.
        attempt define el umbral mínimo de muestras.
        Retorna 0.0 si no pasa los filtros, o el score real (0–1).
        """
        min_s = MIN_SAMPLES.get(attempt, 5)

        # ── Obtener predicciones por categoría ──
        if category == "COLOR":
            ml_pred = self.ml_predictor.predict(self.spin_history, min_total=min_s)
            cat_ml  = self.category_ml.predict_color(min_total=min_s)
            mk_pred = self.markov.predict_color(self.spin_history)
            # Combinar ml_predictor y category_ml para color
            ml_vals  = [p.get(bet_value, 0.5) for p in [ml_pred, cat_ml] if p is not None]
            ml_prob  = sum(ml_vals) / len(ml_vals) if ml_vals else 0.5
            mk_prob  = mk_pred.get(bet_value, 0.5) if mk_pred else 0.5
            ml_total = min(ml_pred["total"] if ml_pred else 0,
                           cat_ml["total"]  if cat_ml  else 0)
        elif category == "PARIDAD":
            cat_ml  = self.category_ml.predict_paridad(min_total=min_s)
            mk_pred = self.markov.predict_paridad(self.spin_history)
            if cat_ml is None: return 0.0
            ml_prob  = cat_ml.get(bet_value, 0.5)
            mk_prob  = mk_pred.get(bet_value, 0.5) if mk_pred else 0.5
            ml_total = cat_ml.get("total", 0)
        else:  # RANGO
            cat_ml  = self.category_ml.predict_rango(min_total=min_s)
            mk_pred = self.markov.predict_rango(self.spin_history)
            if cat_ml is None: return 0.0
            ml_prob  = cat_ml.get(bet_value, 0.5)
            mk_prob  = mk_pred.get(bet_value, 0.5) if mk_pred else 0.5
            ml_total = cat_ml.get("total", 0)

        if ml_prob < self.min_prob_threshold:
            return 0.0

        # ── Score ponderado base ──
        w_ml  = self.unified_prob_system.weights["ml"]
        w_mk  = self.unified_prob_system.weights["markov"]
        base_score = w_ml * ml_prob + w_mk * mk_prob

        # ── Factor de acuerdo ML/Markov ──
        # Acuerdo alto → boost; desacuerdo → reducción
        agreement = 1.0 - abs(ml_prob - mk_prob)
        agree_factor = 0.88 + agreement * 0.24   # rango 0.88–1.12

        # ── Para intento 1: exigir acuerdo real (ambos > threshold) ──
        if attempt == 1:
            if mk_pred is not None and mk_prob < self.min_prob_threshold:
                return 0.0   # Markov en contra → bloquear señal inicial

        # ── EMA factor ──
        ema_ok = self._passes_ema_filter(bet_value)
        ema_factor = 1.08 if ema_ok else 0.88

        # ── SR factor ──
        sr_ok = self._passes_sr_filter(bet_value)
        sr_factor = 1.0 if sr_ok else 0.88

        # ── Streak trend: últimos 4 resultados en esa categoría ──
        streak_factor = self._streak_trend_factor(category, bet_value, lookback=4)

        # ── Muestra factor: más muestras = más confianza ──
        sample_factor = min(1.1, 0.9 + min(ml_total, 50) / 500.0)

        score = base_score * agree_factor * ema_factor * sr_factor * streak_factor * sample_factor
        return min(0.95, max(0.0, score))

    def _streak_trend_factor(self, category: str, bet_value: str, lookback: int = 4) -> float:
        """
        Analiza los últimos `lookback` resultados no-verde en la categoría.
        Si la tendencia reciente favorece bet_value → boost.
        Si favorece el opuesto → reducción (señal más arriesgada).
        """
        recent = [s for s in self.spin_history[-lookback*2:] if s["number"] != 0][-lookback:]
        if len(recent) < 2: return 1.0

        matches = 0
        for spin in recent:
            if category == "COLOR":
                if spin["real"] == bet_value: matches += 1
            elif category == "PARIDAD":
                if get_paridad(spin["number"]) == bet_value: matches += 1
            else:
                if get_rango(spin["number"]) == bet_value: matches += 1

        ratio = matches / len(recent)
        # ratio ≥ 0.75: tendencia fuerte → pequeño boost (el patrón es claro)
        # ratio 0.50–0.74: neutro
        # ratio < 0.50: tendencia opuesta → penalizar (pero no bloquear)
        if ratio >= 0.75: return 1.08
        if ratio >= 0.50: return 1.0
        if ratio >= 0.25: return 0.94
        return 0.88

    # ─── DETECCIÓN DE MEJOR SEÑAL ─────────────────────────────────────────────
    def _detect_best_category_signal(self) -> Optional[dict]:
        """
        Evalúa las 3 categorías con scoring multi-factor.
        Solo emite si el score supera el umbral de calidad para intento 1.
        No emite si el número disparador es 0 (verde).
        """
        if not self.spin_history: return None
        last_num = self.spin_history[-1]["number"]

        # ← No activar señal cuando el número es 0
        if last_num == 0: return None

        candidates = []

        # COLOR: basado en Markov+ML (no tabla)
        color_cand = self._evaluate_color_candidate_v23()
        if color_cand:
            candidates.append(color_cand)

        # PARIDAD
        for val in ("PAR", "IMPAR"):
            score = self._score_category("PARIDAD", val, attempt=1)
            if score >= self.signal_quality_threshold:
                candidates.append({
                    "category": "PARIDAD", "bet_value": val,
                    "probability": score, "trigger_number": last_num,
                })

        # RANGO
        for val in ("BAJO", "ALTO"):
            score = self._score_category("RANGO", val, attempt=1)
            if score >= self.signal_quality_threshold:
                candidates.append({
                    "category": "RANGO", "bet_value": val,
                    "probability": score, "trigger_number": last_num,
                })

        if not candidates: return None
        return max(candidates, key=lambda x: x["probability"])

    def _evaluate_color_candidate_v23(self) -> Optional[dict]:
        """
        COLOR determinado únicamente por Markov+ML (sin tabla).
        Aplica filtro EMA, acuerdo dual y umbral de calidad.
        """
        mk_pred = self.markov.predict_color(self.spin_history)
        ml_pred = self.ml_predictor.predict(self.spin_history, min_total=MIN_SAMPLES[1])
        cat_col = self.category_ml.predict_color(min_total=MIN_SAMPLES[1])

        # Combinar ml_predictor + category_ml para color
        preds_disponibles = [p for p in [ml_pred, cat_col] if p is not None]
        if not preds_disponibles and mk_pred is None:
            return None

        def avg_prob(preds, color):
            vals = [p.get(color, 0.5) for p in preds]
            return sum(vals) / len(vals) if vals else 0.5

        w_ml = self.unified_prob_system.weights["ml"]
        w_mk = self.unified_prob_system.weights["markov"]

        mk_rojo  = mk_pred.get("ROJO",  0.5) if mk_pred else 0.5
        mk_negro = mk_pred.get("NEGRO", 0.5) if mk_pred else 0.5
        ml_rojo  = avg_prob(preds_disponibles, "ROJO")
        ml_negro = avg_prob(preds_disponibles, "NEGRO")

        rojo_score  = w_ml * ml_rojo  + w_mk * mk_rojo
        negro_score = w_ml * ml_negro + w_mk * mk_negro

        if rojo_score > negro_score:
            color_cand, base_score = "ROJO",  rojo_score
        else:
            color_cand, base_score = "NEGRO", negro_score

        # Verificar umbral base
        if base_score < self.min_prob_threshold:
            return None

        # Calcular score completo con factores
        score = self._score_category("COLOR", color_cand, attempt=1)
        if score < self.signal_quality_threshold:
            return None

        # EMA check final
        if not self._passes_ema_filter(color_cand):
            # Si EMA no favorece, comprobar si AMX tiene señal EMA
            if len(self.amx_system.ultimos_puntos) < 20:
                return None
            if not self.amx_system.check_ema_conditions(
                    self.amx_system.ultimos_puntos, color_cand):
                return None

        last_num = self.spin_history[-1]["number"]
        return {
            "category": "COLOR", "bet_value": color_cand,
            "probability": score, "trigger_number": last_num,
        }

    # ─── REINTENTO ────────────────────────────────────────────────────────────
    def _best_retry_value(self, trigger_number: int, attempt: int) -> Optional[str]:
        """
        Evalúa la mejor apuesta para la categoría activa en reintentos.
        Aplica umbral más estricto según el número de intento.
        """
        thr = RETRY_THRESHOLD.get(attempt, 0.55)

        if self.active_category == "COLOR":
            # Intentar con el mismo color y el opuesto
            for candidate in [self.bet_value, self._opposite_color(self.bet_value)]:
                score = self._score_category("COLOR", candidate, attempt=attempt)
                if score >= thr:
                    return candidate
            return None
        elif self.active_category == "PARIDAD":
            candidates = sorted(
                [(v, self._score_category("PARIDAD", v, attempt)) for v in ("PAR", "IMPAR")],
                key=lambda x: -x[1]
            )
        else:  # RANGO
            candidates = sorted(
                [(v, self._score_category("RANGO", v, attempt)) for v in ("BAJO", "ALTO")],
                key=lambda x: -x[1]
            )

        if candidates and candidates[0][1] >= thr:
            return candidates[0][0]
        return None

    # ─── FILTROS DE COMPATIBILIDAD ────────────────────────────────────────────
    def _passes_markov_ml_filter(self, color: str) -> bool:
        mp = self.markov.predict_color(self.spin_history)
        ml = self.ml_predictor.predict(self.spin_history)
        if mp is not None and mp.get(color, 0) < 0.50:
            logger.info(f"[{self.name}] Bloqueada Markov {mp.get(color,0)*100:.0f}%<50%")
            return False
        if ml is not None and ml.get(color, 0) < 0.50:
            logger.info(f"[{self.name}] Bloqueada ML {ml.get(color,0)*100:.0f}%<50%")
            return False
        return True

    def _get_predictor_votes(self, color: str) -> int:
        votes = 0
        if self.markov.predict_color(self.spin_history) and \
           self.markov.predict_color(self.spin_history).get(color, 0) > 0.50: votes += 1
        ml = self.ml_predictor.predict(self.spin_history)
        if ml and ml.get(color, 0) > 0.50: votes += 1
        return votes

    # ─── PROBABILIDAD UNIFICADA ───────────────────────────────────────────────
    def _update_unified_system(self, color: str):
        levels = self.original_levels if color == "ROJO" else self.inverted_levels
        self.unified_prob_system.calculate_volatility(levels)
        self.unified_prob_system.update_trend_factors(levels)
        self.unified_prob_system.update_weights()

    def _get_category_probability(self, category: str, bet_value: str,
                                   trigger_number: int) -> dict:
        if category == "COLOR":
            mk = self.markov.predict_color(self.spin_history)
            ml = self.ml_predictor.predict(self.spin_history)
            return self.unified_prob_system.get_joint_probability(
                mk, ml, bet_value,
                self.get_prob(trigger_number, bet_value))
        # Para paridad/rango: usar score como combined_prob
        score = self._score_category(category, bet_value, attempt=self.current_attempt_number)
        return {
            "combined_prob": score, "markov_prob": 0.5, "ml_prob": score,
            "table_prob": score, "confidence": 0.65,
            "threshold": self.signal_quality_threshold, "signal_strength": "moderate",
            "weights": self.unified_prob_system.weights.copy(),
            "ema_trend_factor": 1.0, "sr_factor": 1.0, "volatility": 1.0,
        }

    def _record_prediction_result(self, color: str, actual: str):
        mk = self.markov.predict_color(self.spin_history)
        ml = self.ml_predictor.predict(self.spin_history)
        self.unified_prob_system.record_prediction(color, mk, ml, actual)

    # ─── UPDATE HISTORIAL Y NIVELES ──────────────────────────────────────────
    def _update_history_and_levels(self, number: int, real: str):
        self.spin_history.append({"number": number, "real": real})
        if len(self.spin_history) > 300: self.spin_history.pop(0)
        self.result_sequence.append({"number": number, "real": real})

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

        while len(self.original_levels) > len(self.spin_history): self.original_levels.pop(0)
        while len(self.inverted_levels) > len(self.spin_history): self.inverted_levels.pop(0)
        ml = min(len(self.original_levels), len(self.inverted_levels))
        self.original_levels = self.original_levels[-ml:]
        self.inverted_levels = self.inverted_levels[-ml:]

        self._update_amx_positions(real)
        self.amx_system.update_streak(real, self.get_signal(number))
        if self.signal_active or self.waiting_for_attempt:
            ref = self.bet_value if self.active_category == "COLOR" else "ROJO"
            self._update_unified_system(ref)
        if real != "VERDE":
            self.unified_prob_system.update_streak(real)

        self.markov.update(self.spin_history)
        self.ml_predictor.add_spin(self.spin_history)
        self.category_ml.add_spin(number, real)

    def _update_amx_positions(self, color: str):
        last_pos = self.amx_system.ultimos_puntos[-1] if self.amx_system.ultimos_puntos else 0
        if color == "ROJO":    new_pos = last_pos + 1
        elif color == "NEGRO": new_pos = last_pos - 1
        else:                  new_pos = last_pos
        self.amx_system.ultimos_puntos.append(new_pos)
        if len(self.amx_system.ultimos_puntos) > 300:
            self.amx_system.ultimos_puntos = self.amx_system.ultimos_puntos[-200:]

    # ─── MENSAJES ─────────────────────────────────────────────────────────────
    def _format_sequence(self, spin_history: list) -> str:
        emojis = {"ROJO": "🔴", "NEGRO": "⚫️", "VERDE": "🟢"}
        recent = spin_history[-10:] if len(spin_history) >= 10 else spin_history
        return " --> ".join(emojis.get(s["real"], "❓") for s in recent)

    def _build_caption(self, attempt: int, unified_prob: Optional[dict]) -> str:
        bet      = self.bet_sys.current_bet()
        step     = self.bet_sys.step + 1
        prob_pct = int((unified_prob["combined_prob"] if unified_prob else 0.5) * 100)
        val_icon = self._category_icon(self.bet_value)
        trig_dis = self._trigger_display(self.trigger_number, self.active_category)
        return (
            f"☑️☑️ <b>SEÑAL CONFIRMADA</b> ☑️☑️\n\n"
            f"🎰 Juego: {self.name}\n"
            f"👉 Después de: {trig_dis}\n"
            f"🎯 Apostar a: {self.bet_value} {val_icon}\n"
            f"🤖 Probabilidad Unificada: {prob_pct}%\n"
            f"🌀 D'Alembert paso {step} de 20\n"
            f"📍 Apuesta: {bet:.2f} usd\n\n"
            f"♻️ Intento {attempt}/{MAX_ATTEMPTS}"
        )

    def _chart_color(self) -> str:
        if self.active_category == "COLOR":
            return self.bet_value if self.bet_value in ("ROJO", "NEGRO") else "ROJO"
        return "ROJO"

    def _send_signal(self, attempt: int, unified_prob: Optional[dict] = None):
        self.current_attempt_number = attempt
        self.signal_is_level1 = (self.bet_sys.step == 0 and not self.recovery_active)
        if self.signal_is_level1: self.level1_bankroll = self.bet_sys.bankroll
        caption     = self._build_caption(attempt, unified_prob)
        chart_color = self._chart_color()
        self.bet_color = chart_color
        levels = self.original_levels[:] if chart_color == "ROJO" else self.inverted_levels[:]
        mp = self.markov.predict_color(self.spin_history)
        ml = self.ml_predictor.predict(self.spin_history)
        chart  = generate_chart(levels, self.spin_history[:], chart_color,
                                markov_pred=mp, ml_pred=ml, unified_prob=unified_prob)
        msg_id = tg_send_photo(self.chat_id, self.thread_id, chart, caption)
        if msg_id: self.signal_msg_ids.append(msg_id)
        logger.info(f"[{self.name}] Señal [{self.active_category}] {self.bet_value} "
                    f"intento={attempt} prob={int((unified_prob['combined_prob'] if unified_prob else 0)*100)}%")

    def _send_waiting_message(self, attempt_number: int):
        for mid in self.signal_msg_ids: tg_delete(self.chat_id, mid)
        self.signal_msg_ids = []
        if self.waiting_msg_id: tg_delete(self.chat_id, self.waiting_msg_id); self.waiting_msg_id = None
        ord_str = "2°" if attempt_number == 2 else "3°"
        caption = (
            f"⚠️ <b>Esperando condiciones para el {ord_str} intento</b>\n\n"
            f"🎰 <b>{self.name}</b>\n"
            f"🔍 <i>Analizando {self.active_category} en cada giro...</i>\n"
        )
        chart_color = self._chart_color()
        levels = self.original_levels[:] if chart_color == "ROJO" else self.inverted_levels[:]
        mp = self.markov.predict_color(self.spin_history)
        ml = self.ml_predictor.predict(self.spin_history)
        chart = generate_chart(levels, self.spin_history[:], chart_color, markov_pred=mp, ml_pred=ml)
        msg_id = tg_send_photo(self.chat_id, self.thread_id, chart, caption)
        if msg_id: self.waiting_msg_id = msg_id

    def _send_result(self, number: int, real: str, won: bool, bet: float,
                     attempt_won: int, delete_signals: bool = True):
        bankroll = self.bet_sys.bankroll
        icon = {"ROJO": "🔴", "NEGRO": "⚫️", "VERDE": "🟢"}.get(real, "❓")
        if delete_signals:
            for mid in self.signal_msg_ids: tg_delete(self.chat_id, mid)
            self.signal_msg_ids = []
            if self.waiting_msg_id: tg_delete(self.chat_id, self.waiting_msg_id); self.waiting_msg_id = None
        chart_color = self._chart_color()
        levels = self.original_levels[:] if chart_color == "ROJO" else self.inverted_levels[:]
        mp = self.markov.predict_color(self.spin_history)
        ml = self.ml_predictor.predict(self.spin_history)
        chart = generate_chart(levels, self.spin_history[:], chart_color, markov_pred=mp, ml_pred=ml)
        tg_send_photo(self.chat_id, self.thread_id, chart,
                      f"{'✅' if won else '❌'} Resultado: {number} {icon} — {'Acierto!' if won else 'Fallo'} 💰 {bankroll:.2f} usd")
        logger.info(f"[{self.name}] {'WIN' if won else 'LOSS'} #{number} bankroll={bankroll:.2f}")

    def _check_recovery(self):
        if not self.recovery_active: return
        if self.bet_sys.bankroll >= self.recovery_target:
            self.consec_losses = 0; self.recovery_active = False
            self.recovery_target = 0.0; self.bet_sys.step = 0

    def _check_stats(self):
        if not self.stats.should_send_stats(): return
        bk = self.bet_sys.bankroll
        s20, s24 = self.stats.get_batch_stats(bk), self.stats.get_24h_stats(bk)
        self.stats.mark_stats_sent(bk)
        if not s20 and not s24: return
        txt = ""
        for label, s in [(f"{s20.get('total','?')} SENALES", s20), ("24 HORAS", s24)]:
            if s:
                txt += (
                    f"👉🏼 <b>ESTADISTICAS {label}</b>\n"
                    f"🈯️ <b>T:</b> {s['total']} 📈 <b>E:</b> {s['efficiency']}%\n"
                    f"1️⃣ <b>W:</b> {s['w1']} --> <b>E:</b> {s['e_w1']}%\n"
                    f"2️⃣ <b>W:</b> {s['w2']} --> <b>E:</b> {s['e_w2']}%\n"
                    f"3️⃣ <b>W:</b> {s['w3']} --> <b>E:</b> {s['e_w3']}%\n"
                    f"🈲 <b>L:</b> {s['losses']} --> <b>E:</b> {s['e_loss']}%\n"
                    f"💰 <i>Bankroll: {s['bankroll_delta']:.2f} usd</i>\n\n"
                )
        tg_send_text(self.chat_id, self.thread_id, txt.strip())

    # ─── PROCESO PRINCIPAL ────────────────────────────────────────────────────
    def process_number(self, number: int):
        real = REAL_COLOR_MAP.get(number, "VERDE")

        # ── FASE DE APRENDIZAJE ──────────────────────────────────────────────
        if self.learning_phase:
            self.learning_spin_count += 1
            self.learning_initial_numbers.append(number)
            logger.info(f"[{self.name}] Giro {self.learning_spin_count}/{self.max_learning_spins}: {number} {real}")
            self._update_history_and_levels(number, real)
            if self.learning_spin_count >= self.max_learning_spins:
                self.learning_phase = False
                logger.info(f"[{self.name}] Aprendizaje completado. Iniciando señales.")
            return

        # ── FASE NORMAL ──────────────────────────────────────────────────────
        self._update_history_and_levels(number, real)

        # ── ESTADO 1: Señal activa ───────────────────────────────────────────
        if self.signal_active:
            is_win = self._is_win(number, real)   # 0 siempre retorna True
            current_attempt = MAX_ATTEMPTS - self.attempts_left + 1

            if is_win:
                bet = self.bet_sys.win()
                self.stats.record_signal_result(current_attempt, True, bet, self.bet_sys.bankroll)
                if self.active_category == "COLOR":
                    self._record_prediction_result(self.bet_value, real)
                self.signal_active = False; self.active_category = None
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
                        tg_delete(self.chat_id, self.signal_msg_ids.pop())
                    chosen = self._best_retry_value(number, attempt_number)
                    if chosen is not None:
                        self.bet_value       = chosen
                        self.trigger_number  = number
                        unified_prob = self._get_category_probability(
                            self.active_category, chosen, number)
                        self._send_signal(attempt_number, unified_prob)
                    else:
                        self.signal_active          = False
                        self.waiting_for_attempt    = True
                        self.waiting_attempt_number = attempt_number
                        self._send_waiting_message(attempt_number)

        # ── ESTADO 2: Esperando condiciones para reintento ───────────────────
        elif self.waiting_for_attempt:
            if real == "VERDE":
                self.skip_one_after_zero = True; return
            if self.skip_one_after_zero:
                self.skip_one_after_zero = False; return
            attempt_number = self.waiting_attempt_number
            chosen = self._best_retry_value(number, attempt_number)
            if chosen is not None:
                if self.waiting_msg_id:
                    tg_delete(self.chat_id, self.waiting_msg_id); self.waiting_msg_id = None
                self.bet_value           = chosen
                self.trigger_number      = number
                self.signal_active       = True
                self.waiting_for_attempt = False
                unified_prob = self._get_category_probability(
                    self.active_category, chosen, number)
                self._send_signal(attempt_number, unified_prob)

        # ── ESTADO 3: Idle – buscar nueva señal ─────────────────────────────
        else:
            self.signal_msg_ids = []
            best = self._detect_best_category_signal()
            if best:
                self.signal_active    = True
                self.active_category  = best["category"]
                self.bet_value        = best["bet_value"]
                self.bet_color        = best["bet_value"] if best["category"] == "COLOR" else "ROJO"
                self.attempts_left    = MAX_ATTEMPTS
                self.total_attempts   = MAX_ATTEMPTS
                self.trigger_number   = best["trigger_number"]
                unified_prob = self._get_category_probability(
                    best["category"], best["bet_value"], best["trigger_number"])
                self._send_signal(1, unified_prob)
                self.amx_system.register_signal_sent()

    def _handle_full_loss(self, number: int, real: str, bet: float = None):
        if bet is None: bet = self.bet_sys.loss()
        self.consec_losses += 1
        if self.consec_losses >= 10:
            self.consec_losses = 0; self.recovery_active = False; self.recovery_target = 0.0
        else:
            self.recovery_active = True
            self.recovery_target = self.level1_bankroll + BASE_BET
        self.stats.record_signal_result(0, False, bet, self.bet_sys.bankroll)
        if self.active_category == "COLOR":
            self._record_prediction_result(self.bet_value, real)
        self.signal_active = False; self.active_category = None
        self._send_result(number, real, False, bet, 0)
        self._check_stats()
        self.signal_msg_ids = []

    # ─── WEBSOCKET ────────────────────────────────────────────────────────────
    async def run_ws(self):
        reconnect_delay = 5
        while self.running:
            try:
                async with websockets.connect(
                    WS_URL, ping_interval=30, ping_timeout=60, close_timeout=10
                ) as ws:
                    self.ws = ws; reconnect_delay = 5
                    logger.info(f"[{self.name}] WS conectado")
                    await ws.send(json.dumps({
                        "type": "subscribe", "casinoId": CASINO_ID,
                        "currency": "USD", "key": [self.ws_key],
                    }))
                    async for message in ws:
                        if not self.running: break
                        try: data = json.loads(message)
                        except: continue
                        if "last20Results" in data and isinstance(data["last20Results"], list):
                            tmp = []
                            for r in data["last20Results"]:
                                gid = r.get("gameId"); num = r.get("result")
                                if gid and num is not None:
                                    try: n = int(num)
                                    except: continue
                                    if 0 <= n <= 36 and gid not in self.anti_block:
                                        tmp.append((gid, n)); self.anti_block.add(gid)
                                        if len(self.anti_block) > 1000: self.anti_block.clear()
                            for _, n in reversed(tmp): self.process_number(n)
                        gid = data.get("gameId"); res = data.get("result")
                        if gid and res is not None:
                            try: n = int(res)
                            except: continue
                            if 0 <= n <= 36 and gid not in self.anti_block:
                                if len(self.anti_block) > 1000: self.anti_block.clear()
                                self.anti_block.add(gid); self.process_number(n)
            except Exception as e:
                logger.warning(f"[{self.name}] WS error: {e}. Reconectando {reconnect_delay}s")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 60)

# ─── FLASK KEEPALIVE ──────────────────────────────────────────────────────────
app = Flask(__name__)

@app.route("/")
def index():
    return jsonify({"status": "ok", "bot": "AMX V23", "ts": time.time()})

@app.route("/ping")
def ping():
    return jsonify({"pong": True, "ts": time.time()})

@app.route("/health")
def health():
    return jsonify({"healthy": True})

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
    help_text = """
<b>🎰 Roulette Bot - Sistema AMX V23</b>

<b>Novedades V23:</b>
• <b>pattern_length=4</b> para ML y CategoryML
• Señales COLOR: basadas en <b>Markov+ML</b> (sin tabla)
• <b>0 = ACIERTO automático</b> cuando señal activa
• No se activa señal cuando sale 0
• <b>Markov extendido</b>: analiza COLOR, PARIDAD y RANGO
• Scoring multi-factor: ML + Markov + EMA + SR + streaks + acuerdo
• <b>Umbral elevado</b> para señal inicial (0.54) → más W1/W2
• Threshold por intento: I1=0.54, I2=0.56, I3=0.59
• Mín. muestras por intento: I1=10, I2=6, I3=5
• Fase de aprendizaje: 20 giros sin señales al inicio

Comandos:
/moderado - Modo MODERADO
/tendencia - Modo TENDENCIA
/status - Estado
/reset - Resetear stats
/help - Ayuda
    """
    bot.reply_to(message, help_text, parse_mode="HTML")

@bot.message_handler(commands=['moderado'])
def cmd_moderado(message):
    for e in engines.values(): e.set_mode("moderado")
    bot.reply_to(message, "✅ <b>Modo MODERADO activado</b>", parse_mode="HTML")

@bot.message_handler(commands=['tendencia'])
def cmd_tendencia(message):
    for e in engines.values(): e.set_mode("tendencia")
    bot.reply_to(message, "📈 <b>Modo TENDENCIA activado</b>", parse_mode="HTML")

@bot.message_handler(commands=['status'])
def cmd_status(message):
    lines = ["<b>📊 ESTADO AMX V23</b>\n"]
    for name, engine in engines.items():
        mode_icon = "📈" if engine.amx_system.mode == "tendencia" else "📊"
        if engine.learning_phase:
            st = f"📚 Aprendiendo ({engine.learning_spin_count}/{engine.max_learning_spins})"
        elif engine.signal_active:
            cat  = engine.active_category or "?"
            val  = engine.bet_value or "?"
            icon = CATEGORY_ICONS.get(val, "")
            st   = f"🟢 [{cat}] {val}{icon} int={engine.current_attempt_number}/{MAX_ATTEMPTS}"
        elif engine.waiting_for_attempt:
            st = f"⏳ Esperando int.{engine.waiting_attempt_number}/{MAX_ATTEMPTS}"
        else:
            st = "⚪ Idle"
        w = engine.unified_prob_system.weights
        lines.append(f"<b>{name}</b>: {mode_icon} — {st} [M:{w['markov']:.2f} ML:{w['ml']:.2f}]")
    bot.reply_to(message, "\n".join(lines), parse_mode="HTML")

@bot.message_handler(commands=['reset'])
def cmd_reset(message):
    for engine in engines.values(): engine.stats = DetailedStats()
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

    threading.Thread(target=telegram_polling, daemon=True).start()
    logger.info("🎰 Roulette Bot AMX V23 iniciado (Azure)")
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    threading.Thread(target=run_flask, daemon=True).start()
    logger.info("Flask server started.")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped.")
