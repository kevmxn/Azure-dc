#!/usr/bin/env python3
"""
Roulette Telegram Signal Bot - Sistema AMX V21
VERSION 3.0:
  - Probabilidad Conjunta Ponderada: Markov + ML con pesos adaptativos
  - Umbrales dinámicos basados en volatilidad, rachas y confianza de modelos
  - Pesos adaptativos actualizados con backtesting online (cada 50 resultados)
  - Métricas de tendencia (EMA, soporte/resistencia) como factores de ajuste
  - Gráfico con alturas dinámicas basadas en niveles min/max de últimos 50
  - Nuevo formato de señales: ☑️☑️ SEÑAL CONFIRMADA ☑️☑️
  - Nuevo formato de resultados con secuencia y gráfico actualizado
  - Estadísticas detalladas: últimos 20 y 24h con eficiencia por intento (W1/W2/W3/L)
  - ML Enriquecido (Color+Paridad) activado selectivamente por ruleta.
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
        "min_prob_threshold": 0.52,
        "use_enriched_ml": True,   # <--- Activamos Color+Paridad para esta ruleta
    },
    # Añade aquí otras ruletas si las tienes. Por defecto usarán solo Color.
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

# ─── ML PATTERN PREDICTOR ORIGINAL (SOLO COLOR) ────────────────────────────────
class MLPatternPredictorColor:
    """
    Predictor basado en patrones de longitud N (solo color).
    """
    def __init__(self, pattern_length: int = 4):
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
            "ROJO":  counts.get("ROJO", 0)  / total,
            "NEGRO": counts.get("NEGRO", 0) / total,
            "total": total,
        }

# ─── ML PATTERN PREDICTOR ENRIQUECIDO (COLOR + PARIDAD) ────────────────────────
class MLPatternPredictorEnriched:
    """
    Predictor basado en patrones de longitud N usando COLOR + PARIDAD.
    Ignora el 0.
    """
    def __init__(self, pattern_length: int = 4):
        self.pattern_length = pattern_length
        self.pattern_counts: dict = defaultdict(lambda: defaultdict(int))
        self._known_len: int = 0

    def _get_color_parity(self, spin_number: int, real_color: str) -> Optional[str]:
        if real_color == "VERDE" or spin_number == 0:
            return None
        paridad = "PAR" if spin_number % 2 == 0 else "IMPAR"
        return f"{real_color}_{paridad}"

    def add_spin(self, spin_history: list):
        tokens = []
        for s in spin_history:
            token = self._get_color_parity(s["number"], s["real"])
            if token:
                tokens.append(token)

        current_len = len(tokens)
        if current_len <= self._known_len:
            return
        self._known_len = current_len

        if current_len < self.pattern_length + 1:
            return

        i = current_len - self.pattern_length - 1
        pattern = tuple(tokens[i : i + self.pattern_length])
        next_token = tokens[i + self.pattern_length]
        self.pattern_counts[pattern][next_token] += 1

    def predict(self, spin_history: list) -> Optional[dict]:
        tokens = []
        for s in spin_history:
            token = self._get_color_parity(s["number"], s["real"])
            if token:
                tokens.append(token)

        if len(tokens) < self.pattern_length:
            return None

        pattern = tuple(tokens[-self.pattern_length:])
        counts = dict(self.pattern_counts.get(pattern, {}))
        total = sum(counts.values())

        if total < 3:  # mínimo estadístico más alto por tener más estados
            return None

        prob_color = {"ROJO": 0.0, "NEGRO": 0.0}
        for token, count in counts.items():
            color = token.split("_")[0]
            prob_color[color] += count / total

        return {
            "ROJO": prob_color["ROJO"],
            "NEGRO": prob_color["NEGRO"],
            "total": total,
        }

# ─── SISTEMA DE PROBABILIDAD CONJUNTA PONDERADA ────────────────────────────────
class UnifiedProbabilitySystem:
    """
    Sistema de Probabilidad Conjunta Ponderada (AMX V21)
    - Combina Markov y ML con pesos adaptativos
    - Los pesos se actualizan cada 50 resultados basado en precisión observada
    - Incluye factores de tendencia (EMA, soporte/resistencia)
    - Umbrales dinámicos según volatilidad y rachas
    """

    def __init__(self):
        self.weights = {"markov": 0.5, "ml": 0.5}
        self.prediction_history: deque = deque(maxlen=200)
        self.markov_correct: int = 0
        self.markov_total: int = 0
        self.ml_correct: int = 0
        self.ml_total: int = 0
        self.confidence_factor: float = 0.5
        self.volatility: float = 1.0
        self.current_streak: int = 0
        self.streak_direction: Optional[str] = None
        self.spins_since_weight_update: int = 0
        self.WEIGHT_UPDATE_INTERVAL: int = 50
        self.base_threshold: float = 0.50
        self.dynamic_threshold: float = 0.50
        self.ema_trend_factor: float = 1.0
        self.sr_factor: float = 1.0

    def calculate_volatility(self, levels: list) -> float:
        if len(levels) < 20:
            return 1.0
        recent = levels[-20:]
        std_dev = np.std(recent)
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
        if ema20 is not None and len(levels) > 0:
            current = levels[-1]
            if current > ema20:
                self.ema_trend_factor = 1.0 + (current - ema20) / (abs(ema20) + 1) * 0.2
            else:
                self.ema_trend_factor = 1.0 - (ema20 - current) / (abs(ema20) + 1) * 0.2
            self.ema_trend_factor = max(0.8, min(1.2, self.ema_trend_factor))

        sr = find_support_resistance(levels, lookback=30)
        if sr['support'] is not None and sr['resistance'] is not None:
            range_size = sr['resistance'] - sr['support']
            if range_size > 0:
                pos_in_range = (levels[-1] - sr['support']) / range_size
                self.sr_factor = 1.0 + (pos_in_range - 0.5) * 0.1
                self.sr_factor = max(0.9, min(1.1, self.sr_factor))
        else:
            self.sr_factor = 1.0

    def _calculate_single_ema(self, data: list, period: int) -> Optional[float]:
        if len(data) < period:
            return None
        mult = 2 / (period + 1)
        prev = sum(data[:period]) / period
        for i in range(period, len(data)):
            prev = (data[i] * mult) + (prev * (1 - mult))
        return prev

    def calculate_confidence(self, markov_pred: Optional[dict], ml_pred: Optional[dict], color: str) -> float:
        if markov_pred is None and ml_pred is None:
            return 0.3
        if markov_pred is None or ml_pred is None:
            return 0.5

        m_prob = markov_pred.get(color, 0.5)
        ml_prob = ml_pred.get(color, 0.5)

        agreement = 1.0 - abs(m_prob - ml_prob)
        self.confidence_factor = 0.4 + agreement * 0.6
        return self.confidence_factor

    def calculate_dynamic_threshold(self) -> float:
        vol_factor = self.volatility
        streak_factor = 1.0 + min(self.current_streak * 0.02, 0.3)
        conf_factor = 1.0 - (self.confidence_factor - 0.5) * 0.4
        self.dynamic_threshold = self.base_threshold * vol_factor * streak_factor * conf_factor
        self.dynamic_threshold = max(0.45, min(0.65, self.dynamic_threshold))
        return self.dynamic_threshold

    def record_prediction(self, color: str, markov_pred: Optional[dict],
                          ml_pred: Optional[dict], actual: str):
        self.prediction_history.append({
            "color": color,
            "markov_pred": markov_pred.get(color, 0.5) if markov_pred else None,
            "ml_pred": ml_pred.get(color, 0.5) if ml_pred else None,
            "actual": actual,
            "timestamp": time.time()
        })

        if markov_pred is not None:
            self.markov_total += 1
            if (markov_pred.get(color, 0) > 0.5 and actual == color) or \
               (markov_pred.get(color, 0) <= 0.5 and actual != color):
                self.markov_correct += 1

        if ml_pred is not None:
            self.ml_total += 1
            if (ml_pred.get(color, 0) > 0.5 and actual == color) or \
               (ml_pred.get(color, 0) <= 0.5 and actual != color):
                self.ml_correct += 1

    def update_weights(self):
        self.spins_since_weight_update += 1
        if self.spins_since_weight_update < self.WEIGHT_UPDATE_INTERVAL:
            return

        self.spins_since_weight_update = 0

        markov_acc = self.markov_correct / max(self.markov_total, 1)
        ml_acc = self.ml_correct / max(self.ml_total, 1)

        total_acc = markov_acc + ml_acc
        if total_acc > 0:
            self.weights["markov"] = markov_acc / total_acc
            self.weights["ml"] = ml_acc / total_acc

        min_weight = 0.2
        max_weight = 0.8
        self.weights["markov"] = max(min_weight, min(max_weight, self.weights["markov"]))
        self.weights["ml"] = max(min_weight, min(max_weight, self.weights["ml"]))

        total = self.weights["markov"] + self.weights["ml"]
        self.weights["markov"] /= total
        self.weights["ml"] /= total

        logger.info(f"[AMX V21] Pesos actualizados: Markov={self.weights['markov']:.2f}, "
                   f"ML={self.weights['ml']:.2f} | Precisión: M={markov_acc:.2%}, ML={ml_acc:.2%}")

        self.markov_correct = 0
        self.markov_total = 0
        self.ml_correct = 0
        self.ml_total = 0

    def get_joint_probability(self, markov_pred: Optional[dict], ml_pred: Optional[dict],
                              color: str, table_prob: float) -> dict:
        markov_prob = markov_pred.get(color, 0.5) if markov_pred else 0.5
        ml_prob = ml_pred.get(color, 0.5) if ml_pred else 0.5

        model_prob = (
            self.weights["markov"] * markov_prob +
            self.weights["ml"] * ml_prob
        )

        confidence = self.calculate_confidence(markov_pred, ml_pred, color)

        if markov_pred is None and ml_pred is None:
            combined_prob = table_prob
        else:
            table_weight = max(0.1, 1.0 - confidence) * 0.3
            model_weight = 1.0 - table_weight
            combined_prob = model_weight * model_prob + table_weight * table_prob

        combined_prob *= self.ema_trend_factor
        combined_prob *= self.sr_factor

        combined_prob = max(0.3, min(0.9, combined_prob))

        threshold = self.calculate_dynamic_threshold()
        signal_strength = "strong" if combined_prob >= threshold + 0.1 else \
                         "moderate" if combined_prob >= threshold else "weak"

        return {
            "combined_prob": combined_prob,
            "markov_prob": markov_prob,
            "ml_prob": ml_prob,
            "table_prob": table_prob,
            "confidence": confidence,
            "threshold": threshold,
            "signal_strength": signal_strength,
            "weights": self.weights.copy(),
            "ema_trend_factor": self.ema_trend_factor,
            "sr_factor": self.sr_factor,
            "volatility": self.volatility
        }

# ─── STATISTICS CON DETALLE POR INTENTO ───────────────────────────────────────
class DetailedStats:
    def __init__(self):
        self.signal_history: deque = deque(maxlen=50)
        self.wins_attempt_1: int = 0
        self.wins_attempt_2: int = 0
        self.wins_attempt_3: int = 0
        self.losses: int = 0
        self.total_signals: int = 0
        self.history_24h: deque = deque()
        self.batch_start_bankroll: Optional[float] = None
        self.batch_start_wins: int = 0
        self.batch_start_losses: int = 0
        self.batch_start_w1: int = 0
        self.batch_start_w2: int = 0
        self.batch_start_w3: int = 0
        self.last_stats_at: int = 0

    def record_signal_result(self, attempt_won: int, final_result: bool,
                            bet_amount: float, bankroll: float):
        entry = {
            "attempt_won": attempt_won,
            "won": final_result,
            "bet": bet_amount,
            "bankroll": bankroll,
            "timestamp": time.time()
        }
        self.signal_history.append(entry)

        self.total_signals += 1

        if final_result:
            if attempt_won == 1:
                self.wins_attempt_1 += 1
            elif attempt_won == 2:
                self.wins_attempt_2 += 1
            elif attempt_won == 3:
                self.wins_attempt_3 += 1
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
        self.last_stats_at = self.total_signals
        self.batch_start_bankroll = bankroll
        self.batch_start_wins = self.wins_attempt_1 + self.wins_attempt_2 + self.wins_attempt_3
        self.batch_start_losses = self.losses
        self.batch_start_w1 = self.wins_attempt_1
        self.batch_start_w2 = self.wins_attempt_2
        self.batch_start_w3 = self.wins_attempt_3

    def get_batch_stats(self, current_bankroll: float) -> dict:
        n = self.total_signals - self.last_stats_at
        if n == 0:
            return {}

        w1 = self.wins_attempt_1 - self.batch_start_w1
        w2 = self.wins_attempt_2 - self.batch_start_w2
        w3 = self.wins_attempt_3 - self.batch_start_w3
        l = self.losses - self.batch_start_losses
        w = w1 + w2 + w3

        efficiency = round(w / n * 100, 1) if n > 0 else 0.0

        e_w1  = round(w1 / n * 100, 2) if n > 0 else 0.0
        e_w2  = round(w2 / n * 100, 2) if n > 0 else 0.0
        e_w3  = round(w3 / n * 100, 2) if n > 0 else 0.0
        e_loss = round(l  / n * 100, 2) if n > 0 else 0.0

        bankroll_delta = round(current_bankroll - self.batch_start_bankroll, 2) \
                        if self.batch_start_bankroll is not None else 0.0

        return {
            "total": n,
            "wins": w,
            "losses": l,
            "w1": w1,
            "w2": w2,
            "w3": w3,
            "efficiency": efficiency,
            "e_w1": e_w1,
            "e_w2": e_w2,
            "e_w3": e_w3,
            "e_loss": e_loss,
            "bankroll_delta": bankroll_delta
        }

    def get_24h_stats(self, current_bankroll: float) -> dict:
        self._trim_24h()
        t = len(self.history_24h)
        if t == 0:
            return {}

        w = sum(1 for e in self.history_24h if e["won"])
        l = t - w

        w1 = sum(1 for e in self.history_24h if e["attempt_won"] == 1)
        w2 = sum(1 for e in self.history_24h if e["attempt_won"] == 2)
        w3 = sum(1 for e in self.history_24h if e["attempt_won"] == 3)

        efficiency = round(w / t * 100, 1) if t > 0 else 0.0

        e_w1  = round(w1 / t * 100, 2) if t > 0 else 0.0
        e_w2  = round(w2 / t * 100, 2) if t > 0 else 0.0
        e_w3  = round(w3 / t * 100, 2) if t > 0 else 0.0
        e_loss = round(l  / t * 100, 2) if t > 0 else 0.0

        if t >= 2:
            bk24 = round(self.history_24h[-1]["bankroll"] - self.history_24h[0]["bankroll"], 2)
        else:
            bk24 = 0.0

        return {
            "total": t,
            "wins": w,
            "losses": l,
            "w1": w1,
            "w2": w2,
            "w3": w3,
            "efficiency": efficiency,
            "e_w1": e_w1,
            "e_w2": e_w2,
            "e_w3": e_w3,
            "e_loss": e_loss,
            "bankroll_delta": bk24
        }

    def reset(self):
        self.signal_history.clear()
        self.history_24h.clear()
        self.wins_attempt_1 = 0
        self.wins_attempt_2 = 0
        self.wins_attempt_3 = 0
        self.losses = 0
        self.total_signals = 0
        self.last_stats_at = 0
        self.batch_start_bankroll = None

# ─── SISTEMA AMX V21 ──────────────────────────────────────────────────────────
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

# ─── CHART GENERATION CON ALTURAS DINÁMICAS ────────────────────────────────────
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
    min_level_50 = np.min(recent_50)
    max_level_50 = np.max(recent_50)
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

    ax.set_ylim(y_min, y_max)

    dot_colors = {"ROJO": "#e84040", "NEGRO": "#aaaacc", "VERDE": "#2ecc71"}
    for i, spin in enumerate(hist_sl):
        c = dot_colors.get(spin["real"], "#ffffff")
        ax.scatter(i, y[i], color=c, s=22, zorder=5, edgecolors="white", linewidths=0.3)

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

    pred_info = ""
    if unified_prob:
        pred_info += f" | Unif:{unified_prob['combined_prob']*100:.0f}%"
        pred_info += f" | M:{unified_prob['markov_prob']*100:.0f}% ML:{unified_prob['ml_prob']*100:.0f}%"
    elif markov_pred:
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

        self.signal_active:   bool  = False
        self.waiting_for_attempt: bool = False
        self.waiting_attempt_number: int  = 0
        self.skip_one_after_zero: bool = False

        self.expected_color:   Optional[str] = None
        self.bet_color:        Optional[str] = None
        self.attempts_left:    int  = 0
        self.total_attempts:   int  = 0
        self.trigger_number:   Optional[int] = None

        self.signal_msg_ids: list    = []
        self.waiting_msg_id: Optional[int] = None

        self.result_sequence: deque = deque(maxlen=10)

        self.bet_sys = D_Alembert(BASE_BET)

        self.consec_losses:   int   = 0
        self.recovery_active: bool  = False
        self.recovery_target: float = 0.0
        self.level1_bankroll: float = 0.0
        self.signal_is_level1: bool = False

        self.amx_system = AMXSignalSystem(mode="moderado")
        self.min_prob_threshold = cfg.get("min_prob_threshold", 0.48)

        self.unified_prob_system = UnifiedProbabilitySystem()

        # ─── SELECCIÓN DEL PREDICTOR ML SEGÚN CONFIGURACIÓN ─────────────────
        use_enriched = cfg.get("use_enriched_ml", False)
        if use_enriched:
            self.ml_predictor = MLPatternPredictorEnriched(pattern_length=4)
            logger.info(f"[{self.name}] Usando ML Enriquecido (Color+Paridad)")
        else:
            self.ml_predictor = MLPatternPredictorColor(pattern_length=4)
            logger.info(f"[{self.name}] Usando ML Original (Solo Color)")

        self.markov = MarkovChainPredictor(window=100, order=2)

        self.stats = DetailedStats()

        self.ws = None
        self.running = True

    # ── Helpers de configuración ──────────────────────────────────────────────
    def set_mode(self, mode: Literal["tendencia", "moderado"]):
        self.amx_system = AMXSignalSystem(mode=mode)
        logger.info(f"[{self.name}] Modo AMX V21 → {mode}")

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
        votes = 0
        mp = self.markov.predict(self.spin_history)
        if mp and mp.get(color, 0) > 0.50:
            votes += 1
        ml = self.ml_predictor.predict(self.spin_history)
        if ml and ml.get(color, 0) > 0.50:
            votes += 1
        return votes

    def _check_retry_conditions(self, color: str, trigger_number: int) -> bool:
        entry = self.get_entry(trigger_number)
        if not entry or entry["senal"] == "NO APOSTAR":
            return False
        if entry["senal"] != color:
            return False
        prob = entry["rojo"] if color == "ROJO" else entry["negro"]
        if prob < self.min_prob_threshold:
            return False

        levels = self.original_levels if color == "ROJO" else self.inverted_levels
        if len(levels) < 20:
            return False
        ema20 = self.calculate_ema(levels, 20)
        li    = len(levels) - 1
        if ema20[li] is None or levels[li] <= ema20[li]:
            return False

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
        same_ok = self._check_retry_conditions(self.bet_color, trigger_number)
        opp     = self._opposite_color(self.bet_color)
        opp_ok  = self._check_retry_conditions(opp, trigger_number)

        if same_ok and opp_ok:
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

    def _passes_markov_ml_filter(self, color: str) -> bool:
        mp = self.markov.predict(self.spin_history)
        ml = self.ml_predictor.predict(self.spin_history)

        markov_ok = (mp is None) or (mp.get(color, 0) >= 0.50)
        ml_ok     = (ml is None) or (ml.get(color, 0) >= 0.50)

        if mp is not None and not markov_ok:
            logger.info(f"[{self.name}] Señal bloqueada: Markov {mp.get(color,0)*100:.0f}% < 50% para {color}")
            return False
        if ml is not None and not ml_ok:
            logger.info(f"[{self.name}] Señal bloqueada: ML {ml.get(color,0)*100:.0f}% < 50% para {color}")
            return False
        return True

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

    def _update_unified_system(self, color: str):
        levels = self.original_levels if color == "ROJO" else self.inverted_levels
        self.unified_prob_system.calculate_volatility(levels)
        self.unified_prob_system.update_trend_factors(levels)
        self.unified_prob_system.update_weights()

    def _get_unified_probability(self, color: str, trigger_number: int) -> dict:
        table_prob = self.get_prob(trigger_number, color)
        markov_pred = self.markov.predict(self.spin_history)
        ml_pred = self.ml_predictor.predict(self.spin_history)

        return self.unified_prob_system.get_joint_probability(
            markov_pred, ml_pred, color, table_prob
        )

    def _record_prediction_result(self, color: str, actual: str):
        markov_pred = self.markov.predict(self.spin_history)
        ml_pred = self.ml_predictor.predict(self.spin_history)
        self.unified_prob_system.record_prediction(color, markov_pred, ml_pred, actual)

    def _format_sequence(self, spin_history: list) -> str:
        emojis = {"ROJO": "🔴", "NEGRO": "⚫️", "VERDE": "🟢"}
        recent = spin_history[-10:] if len(spin_history) >= 10 else spin_history
        seq_parts = []
        for spin in recent:
            emoji = emojis.get(spin["real"], "❓")
            seq_parts.append(emoji)
        return " --> ".join(seq_parts)

    def _send_signal(self, trigger: int, attempt: int, amx_signal: Optional[dict] = None,
                    unified_prob: Optional[dict] = None):
        bet        = self.bet_sys.current_bet()
        table_prob = int(self.get_prob(trigger, self.bet_color) * 100)
        color_icon = "🔴" if self.bet_color == "ROJO" else "⚫️"
        step       = self.bet_sys.step + 1

        unified_prob_pct = int(unified_prob["combined_prob"] * 100) if unified_prob else table_prob

        trigger_real_color = REAL_COLOR_MAP.get(trigger, "VERDE")
        if trigger_real_color == "ROJO":
            trigger_icon = "🔴"
        elif trigger_real_color == "NEGRO":
            trigger_icon = "⚫️"
        else:
            trigger_icon = "🟢"

        self.signal_is_level1 = (self.bet_sys.step == 0 and not self.recovery_active)
        if self.signal_is_level1:
            self.level1_bankroll = self.bet_sys.bankroll

        caption = (
            f"☑️☑️ <b>SEÑAL CONFIRMADA</b> ☑️☑️\n\n"
            f"🎰 Juego: {self.name}\n"
            f"👉 Después de: {trigger} {trigger_real_color} {trigger_icon}\n"
            f"🎯 Apostar a: {self.bet_color} {color_icon}\n"
            f"🤖 Probabilidad Unificada: {unified_prob_pct}%\n"
            f"🌀 D'Alembert paso {step} de 20\n"
            f"📍 Apuesta: {bet:.2f} usd\n\n"
            f"♻️ Intento {attempt}/{MAX_ATTEMPTS}"
        )

        levels = self.original_levels[:] if self.bet_color == "ROJO" else self.inverted_levels[:]
        mp     = self.markov.predict(self.spin_history)
        ml     = self.ml_predictor.predict(self.spin_history)
        chart  = generate_chart(levels, self.spin_history[:], self.bet_color,
                                markov_pred=mp, ml_pred=ml, unified_prob=unified_prob)
        msg_id = tg_send_photo(self.chat_id, self.thread_id, chart, caption)
        if msg_id:
            self.signal_msg_ids.append(msg_id)
        logger.info(f"[{self.name}] Signal sent: {self.bet_color} after {trigger} ({trigger_real_color}), "
                    f"bet={bet:.2f}, step={step}, unified_prob={unified_prob_pct}%")

    def _send_retry_signal(self, trigger: int, new_bet: float, attempt_number: int,
                          unified_prob: Optional[dict] = None):
        table_prob = int(self.get_prob(trigger, self.bet_color) * 100)
        unified_prob_pct = int(unified_prob["combined_prob"] * 100) if unified_prob else table_prob
        color_icon = "🔴" if self.bet_color == "ROJO" else "⚫️"
        step       = self.bet_sys.step + 1

        trigger_real_color = REAL_COLOR_MAP.get(trigger, "VERDE")
        if trigger_real_color == "ROJO":
            trigger_icon = "🔴"
        elif trigger_real_color == "NEGRO":
            trigger_icon = "⚫️"
        else:
            trigger_icon = "🟢"

        caption = (
            f"☑️☑️ <b>SEÑAL CONFIRMADA</b> ☑️☑️\n\n"
            f"🎰 Juego: {self.name}\n"
            f"👉 Después de: {trigger} {trigger_real_color} {trigger_icon}\n"
            f"🎯 Apostar a: {self.bet_color} {color_icon}\n"
            f"🤖 Probabilidad Unificada: {unified_prob_pct}%\n"
            f"🌀 D'Alembert paso {step} de 20\n"
            f"📍 Apuesta: {new_bet:.2f} usd\n\n"
            f"♻️ Intento {attempt_number}/{MAX_ATTEMPTS}"
        )
        levels = self.original_levels[:] if self.bet_color == "ROJO" else self.inverted_levels[:]
        mp     = self.markov.predict(self.spin_history)
        ml     = self.ml_predictor.predict(self.spin_history)
        chart  = generate_chart(levels, self.spin_history[:], self.bet_color,
                                markov_pred=mp, ml_pred=ml, unified_prob=unified_prob)
        msg_id = tg_send_photo(self.chat_id, self.thread_id, chart, caption)
        if msg_id:
            self.signal_msg_ids.append(msg_id)
        logger.info(f"[{self.name}] Retry {attempt_number}/{MAX_ATTEMPTS}: "
                    f"{self.bet_color} after {trigger} ({trigger_real_color}), bet={new_bet:.2f}, unified_prob={unified_prob_pct}%")

    def _send_waiting_message(self, attempt_number: int):
        for msg_id in self.signal_msg_ids:
            tg_delete(self.chat_id, msg_id)
        self.signal_msg_ids = []

        if self.waiting_msg_id:
            tg_delete(self.chat_id, self.waiting_msg_id)
            self.waiting_msg_id = None

        ord_str = "2°" if attempt_number == 2 else "3°"
        caption = (
            f"⚠️ <b>Esperando condiciones para el {ord_str} intento</b>\n\n"
            f"🎰 <b>{self.name}</b>\n"
            f"🔍 <i>Analizando ROJO 🔴 y NEGRO ⚫️ en cada giro...</i>\n"
        )
        levels = self.original_levels[:] if self.bet_color == "ROJO" else self.inverted_levels[:]
        mp     = self.markov.predict(self.spin_history)
        ml     = self.ml_predictor.predict(self.spin_history)
        chart  = generate_chart(levels, self.spin_history[:], self.bet_color,
                                markov_pred=mp, ml_pred=ml)
        msg_id = tg_send_photo(self.chat_id, self.thread_id, chart, caption)
        if msg_id:
            self.waiting_msg_id = msg_id
        logger.info(f"[{self.name}] Waiting for attempt {attempt_number}")

    def _send_result(self, number: int, real: str, won: bool, bet: float,
                    attempt_won: int, delete_signals: bool = True):
        bankroll = self.bet_sys.bankroll
        icon = "🔴" if real == "ROJO" else ("⚫️" if real == "NEGRO" else "🟢")
        result_icon = "✅" if won else "❌"

        sequence = self._format_sequence(self.spin_history)

        if delete_signals:
            for msg_id in self.signal_msg_ids:
                tg_delete(self.chat_id, msg_id)
            self.signal_msg_ids = []
            if self.waiting_msg_id:
                tg_delete(self.chat_id, self.waiting_msg_id)
                self.waiting_msg_id = None

        levels = self.original_levels[:] if self.bet_color == "ROJO" else self.inverted_levels[:]
        mp     = self.markov.predict(self.spin_history)
        ml     = self.ml_predictor.predict(self.spin_history)
        chart  = generate_chart(levels, self.spin_history[:], self.bet_color,
                                markov_pred=mp, ml_pred=ml)

        result_text = f"{'✅' if won else '❌'} Resultado: {number} {icon} — {'Acierto!' if won else 'Fallo'}"
        tg_send_photo(self.chat_id, self.thread_id, chart, result_text)

        logger.info(f"[{self.name}] Result: {'WIN' if won else 'LOSS'} #{number}, "
                   f"bankroll={bankroll:.2f}, attempt_won={attempt_won}")

    def _check_stats(self):
        if not self.stats.should_send_stats():
            return
        current_bankroll = self.bet_sys.bankroll

        s20 = self.stats.get_batch_stats(current_bankroll)
        s24 = self.stats.get_24h_stats(current_bankroll)

        self.stats.mark_stats_sent(current_bankroll)

        if not s20 and not s24:
            return

        stats_text = ""

        if s20:
            stats_text += (
                f"👉🏼 <b>ESTADISTICAS {s20['total']} SENALES</b>\n"
                f"🈯️ <b>T:</b> {s20['total']} 📈 <b>E:</b> {s20['efficiency']}%\n"
                f"1️⃣ <b>W:</b> {s20['w1']} --> <b>E:</b> {s20['e_w1']}%\n"
                f"2️⃣ <b>W:</b> {s20['w2']} --> <b>E:</b> {s20['e_w2']}%\n"
                f"3️⃣ <b>W:</b> {s20['w3']} --> <b>E:</b> {s20['e_w3']}%\n"
                f"🈲 <b>L:</b> {s20['losses']} --> <b>E:</b> {s20['e_loss']}%\n"
                f"💰 <i>Bankroll acumulado: {s20['bankroll_delta']:.2f} usd</i>\n\n"
            )

        if s24:
            stats_text += (
                f"👉🏼 <b>ESTADISTICAS 24 HORAS</b>\n"
                f"🈯️ <b>T:</b> {s24['total']} 📈 <b>E:</b> {s24['efficiency']}%\n"
                f"1️⃣ <b>W:</b> {s24['w1']} --> <b>E:</b> {s24['e_w1']}%\n"
                f"2️⃣ <b>W:</b> {s24['w2']} --> <b>E:</b> {s24['e_w2']}%\n"
                f"3️⃣ <b>W:</b> {s24['w3']} --> <b>E:</b> {s24['e_w3']}%\n"
                f"🈲 <b>L:</b> {s24['losses']} --> <b>E:</b> {s24['e_loss']}%\n"
                f"💰 <i>Bankroll acumulado: {s24['bankroll_delta']:.2f} usd</i>\n"
            )

        tg_send_text(self.chat_id, self.thread_id, stats_text)
        logger.info(f"[{self.name}] Stats sent: 20s={s20}, 24h={s24}")

    def process_number(self, number: int):
        real = REAL_COLOR_MAP.get(number, "VERDE")

        self.spin_history.append({"number": number, "real": real})
        if len(self.spin_history) > 300:
            self.spin_history.pop(0)

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

        while len(self.original_levels) > len(self.spin_history):
            self.original_levels.pop(0)
        while len(self.inverted_levels) > len(self.spin_history):
            self.inverted_levels.pop(0)
        min_len = min(len(self.original_levels), len(self.inverted_levels))
        self.original_levels = self.original_levels[-min_len:]
        self.inverted_levels = self.inverted_levels[-min_len:]

        self._update_amx_positions(real)
        expected_signal = self.get_signal(number)
        self.amx_system.update_streak(real, expected_signal)

        if self.signal_active or self.waiting_for_attempt:
            self._update_unified_system(self.bet_color)

        if real != "VERDE":
            self.unified_prob_system.update_streak(real)

        self.markov.update(self.spin_history)
        self.ml_predictor.add_spin(self.spin_history)

        # ══════════════════════════════════════════════════════════════════════
        #  MÁQUINA DE ESTADOS
        # ══════════════════════════════════════════════════════════════════════

        if self.signal_active:
            is_win = (
                (self.bet_color == "ROJO"  and real == "ROJO") or
                (self.bet_color == "NEGRO" and real == "NEGRO")
            )

            current_attempt = MAX_ATTEMPTS - self.attempts_left + 1

            if is_win:
                bet = self.bet_sys.win()
                self.stats.record_signal_result(current_attempt, True, bet, self.bet_sys.bankroll)
                self._record_prediction_result(self.bet_color, real)

                self.signal_active = False
                self._check_recovery()
                self._send_result(number, real, True, bet, current_attempt)
                self._check_stats()
                self.signal_msg_ids = []

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
                    self.stats.record_signal_result(0, False, bet, self.bet_sys.bankroll)
                    self._record_prediction_result(self.bet_color, real)
                    self.signal_active = False
                    self._send_result(number, real, False, bet, 0)
                    self._check_stats()
                    self.signal_msg_ids = []

                else:
                    attempt_number = MAX_ATTEMPTS - self.attempts_left + 1

                    if self.signal_msg_ids:
                        last_id = self.signal_msg_ids.pop()
                        tg_delete(self.chat_id, last_id)

                    if real == "VERDE":
                        self.signal_active = False
                        self.waiting_for_attempt  = True
                        self.waiting_attempt_number = attempt_number
                        self.skip_one_after_zero  = True
                        self._send_waiting_message(attempt_number)
                        return

                    chosen = self._best_retry_color(number)

                    if chosen is not None:
                        self.bet_color      = chosen
                        self.trigger_number = number
                        unified_prob = self._get_unified_probability(chosen, number)
                        self._send_retry_signal(number, self.bet_sys.current_bet(),
                                               attempt_number, unified_prob)
                    else:
                        self.signal_active          = False
                        self.waiting_for_attempt    = True
                        self.waiting_attempt_number = attempt_number
                        self._send_waiting_message(attempt_number)

        elif self.waiting_for_attempt:
            if real == "VERDE":
                self.skip_one_after_zero = True
                return

            if self.skip_one_after_zero:
                self.skip_one_after_zero = False
                return

            attempt_number = self.waiting_attempt_number
            chosen = self._best_retry_color(number)

            if chosen is not None:
                if self.waiting_msg_id:
                    tg_delete(self.chat_id, self.waiting_msg_id)
                    self.waiting_msg_id = None

                self.bet_color          = chosen
                self.trigger_number     = number
                self.signal_active      = True
                self.waiting_for_attempt = False
                unified_prob = self._get_unified_probability(chosen, number)
                self._send_retry_signal(number, self.bet_sys.current_bet(),
                                       attempt_number, unified_prob)

        else:
            self.signal_msg_ids = []

            signal = self._detect_amx_signal()

            if signal:
                if not self._passes_markov_ml_filter(signal["expected_color"]):
                    return

                self.signal_active   = True
                self.expected_color  = signal["expected_color"]
                self.bet_color       = signal["expected_color"]
                self.attempts_left   = MAX_ATTEMPTS
                self.total_attempts  = MAX_ATTEMPTS
                self.trigger_number  = signal["trigger_number"]

                unified_prob = self._get_unified_probability(self.bet_color, signal["trigger_number"])

                self._send_signal(signal["trigger_number"], 1, amx_signal=signal,
                                  unified_prob=unified_prob)
                self.amx_system.register_signal_sent()
            else:
                expected = self.should_activate()
                if expected:
                    bet_color_candidate = self.determine_bet_color(expected)
                    if not self._passes_markov_ml_filter(bet_color_candidate):
                        return

                    self.signal_active  = True
                    self.expected_color = expected
                    self.bet_color      = bet_color_candidate
                    self.attempts_left  = MAX_ATTEMPTS
                    self.total_attempts = MAX_ATTEMPTS
                    self.trigger_number = number

                    unified_prob = self._get_unified_probability(self.bet_color, number)

                    self._send_signal(number, 1, unified_prob=unified_prob)

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
    return jsonify({"status": "ok", "bot": "Roulette Signal Bot AMX V21", "ts": time.time()})

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
<b>🎰 Roulette Bot - Sistema AMX V21</b>

<b>Novedades V21:</b>
• Probabilidad Conjunta Ponderada (Markov + ML con pesos adaptativos)
• Umbrales dinámicos según volatilidad y rachas
• Pesos actualizados cada 50 resultados (backtesting online)
• Métricas de tendencia (EMA, soporte/resistencia)
• Gráfico con alturas dinámicas basadas en min/max
• Nuevo formato de señales y resultados
• Estadísticas detalladas por intento (W1/W2/W3/L)
• ML Enriquecido (Color+Paridad) para Azure Roulette

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

        w = engine.unified_prob_system.weights
        weights_str = f"[M:{w['markov']:.2f} ML:{w['ml']:.2f}]"
        ml_type = "ML++" if isinstance(engine.ml_predictor, MLPatternPredictorEnriched) else "ML"

        lines.append(f"<b>{name}</b>: {mode_icon} {engine.amx_system.mode} — {st} {weights_str} [{ml_type}]")
    bot.reply_to(message, "\n".join(lines), parse_mode="HTML")

@bot.message_handler(commands=['reset'])
def cmd_reset(message):
    for engine in engines.values():
        engine.stats = DetailedStats()
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

    logger.info("🎰 Roulette Bot AMX V21 iniciado")
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    logger.info("Flask server started.")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped.")
