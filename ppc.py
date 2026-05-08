#!/usr/bin/env python3
"""
Roulette Telegram Signal Bot — Martingala con AMX tendencia
  · Máximo 2 intentos por señal
  · Martingala avanza 1 nivel por pérdida (dentro y entre señales)
  · Si pierde en nivel 6, vuelve a nivel 1
  · Reinicio de ficha a nivel 1 al ganar
  · Modo AMX tendencia por defecto
  · Cooldown 5 spins post-pérdida
  · Estadísticas unificadas con historial de 20 señales + 24 horas
  · Pre-entrenamiento con russian-azure.db
  · HTML tags corregidos; INTENTO muestra nivel Martingala real
  · Alerta "POSIBLE ENTRADA" según categorías CPR restantes
"""

import asyncio
import json
import logging
import os
import re
import sqlite3
import threading
import time
import urllib.request
from collections import deque, defaultdict
from typing import Optional, Literal

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
TOKEN   = "8714149875:AAFJugWY0E5A4C0lrxn2bMcKsQEieqo_t5M"

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

# ─── DB CONFIG ────────────────────────────────────────────────────────────────
DB_PATH       = "russian-azure.db"
LIVE_DB_PATH  = "russian_live.db"

def _get_live_db() -> "sqlite3.Connection":
    import sqlite3 as _sq
    conn = _sq.connect(LIVE_DB_PATH, check_same_thread=False)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS live_spins (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            table_name TEXT NOT NULL,
            number    INTEGER NOT NULL,
            ts        INTEGER NOT NULL
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_table ON live_spins(table_name, id)")
    conn.commit()
    return conn

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
    if number == 0:
        return None
    return "PAR" if number % 2 == 0 else "IMPAR"

def get_rango(number: int) -> Optional[str]:
    if number == 0:
        return None
    return "BAJO" if 1 <= number <= 18 else "ALTO"

def get_dozen(n: int) -> int:
    if n == 0: return 0
    return (n - 1) // 12 + 1

def get_column(n: int) -> int:
    if n == 0: return 0
    return ((n - 1) % 3) + 1

CATEGORY_ICONS = {
    "ROJO": "🔴", "NEGRO": "⚫️",
    "PAR":  "🟣", "IMPAR": "🟡",
    "BAJO": "🟤", "ALTO":  "🔵",
    "VERDE": "🟢",
    "D1": "🟡", "D2": "🔵", "D3": "🟠",
    "C1": "🟡", "C2": "🔵", "C3": "🟠",
}

# ─── PARES DOCENA / COLUMNA ───────────────────────────────────────────────────
DOZEN_PAIRS  = {1:(2,3), 2:(1,3), 3:(1,2)}
COLUMN_PAIRS = {1:(2,3), 2:(1,3), 3:(1,2)}

# ─── ROULETTE CONFIGS ─────────────────────────────────────────────────────────
ROULETTE_CONFIGS = {
    "RUSSIAN ROULETTE": {
        "bot":       bot,
        "ws_key":    221,
        "chat_id":   -1003835197023,
        "thread_id": 8344,
        "db_table":  "russian_roulette",
        "min_prob_threshold": 0.55,
    },
}

WS_URL    = "wss://dga.pragmaticplaylive.net/ws"
CASINO_ID = "ppcjd00000007254"
MAX_ATTEMPTS = 2   # Intentos por señal

BASE_BET  = 0.50   # Apuesta base nivel 1
VISIBLE   = 50
WARMUP_SPINS = 21

# ─── MARTINGALA (AVANCE DE 1 NIVEL POR PÉRDIDA, RESET A 1 SI NIVEL 6 PIERDE) ─
class Martingale:
    def __init__(self, base: float):
        self.base = base
        self.level = 1
        self.bankroll = 0.0
        self.consecutive_losses = 0

    def current_bet(self) -> float:
        bets = {1: self.base,
                2: self.base * 2,
                3: self.base * 4,
                4: self.base * 8,
                5: self.base * 16,
                6: self.base * 32}
        return round(bets.get(self.level, self.base * 32), 2)

    def win(self) -> float:
        bet = self.current_bet()
        self.bankroll = round(self.bankroll + bet, 2)
        self.level = 1
        self.consecutive_losses = 0
        return bet

    def loss(self) -> float:
        """Registra una pérdida: descuenta apuesta, sube 1 nivel (máx 6).
        Si ya está en nivel 6, vuelve a nivel 1."""
        bet = self.current_bet()
        self.bankroll = round(self.bankroll - bet, 2)
        if self.level >= 6:
            self.level = 1
        else:
            self.level = self.level + 1
        return bet

    def full_loss(self) -> float:
        """Señal completamente perdida (sin intentos restantes): aplica pérdida y cuenta consecutiva."""
        bet = self.loss()
        self.consecutive_losses += 1
        return bet

    def reset(self):
        self.level = 1
        self.consecutive_losses = 0

# ─── MARKOV CHAIN ────────────────────────────────────────────────────────────
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
        if total < 8:
            return None
        return {
            "ROJO":  counts.get("ROJO",  0) / total,
            "NEGRO": counts.get("NEGRO", 0) / total,
            "total": total,
        }

# ─── ML PATTERN PREDICTOR – COLOR ────────────────────────────────────────────
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

# ─── CATEGORY PREDICTOR ──────────────────────────────────────────────────────
class CategoryPredictor:
    PATTERN_LEN = 11

    def __init__(self):
        self._hist: dict[str, list[str]] = {
            "COLOR":[], "PARIDAD":[], "RANGO":[], "DOCENA":[], "COLUMNA":[],
        }
        self._counts: dict[str, dict] = {
            "COLOR":   defaultdict(lambda: defaultdict(int)),
            "PARIDAD": defaultdict(lambda: defaultdict(int)),
            "RANGO":   defaultdict(lambda: defaultdict(int)),
            "DOCENA":  defaultdict(lambda: defaultdict(int)),
            "COLUMNA": defaultdict(lambda: defaultdict(int)),
        }

    def add_spin(self, number: int, real_color: str):
        if number == 0 or real_color == "VERDE":
            return
        par    = get_paridad(number)
        rang   = get_rango(number)
        dozen  = get_dozen(number)
        column = get_column(number)
        if not par or not rang:
            return
        new_vals = {
            "COLOR":   real_color,
            "PARIDAD": par,
            "RANGO":   rang,
            "DOCENA":  f"D{dozen}",
            "COLUMNA": f"C{column}",
        }
        for cat, val in new_vals.items():
            hist = self._hist[cat]
            if len(hist) >= self.PATTERN_LEN:
                pattern = tuple(hist[-self.PATTERN_LEN:])
                self._counts[cat][pattern][val] += 1
            hist.append(val)

    def predict_category(self, category: str) -> Optional[dict]:
        hist   = self._hist.get(category, [])
        counts = self._counts.get(category, {})
        if len(hist) < self.PATTERN_LEN:
            return None
        pattern = tuple(hist[-self.PATTERN_LEN:])
        c = dict(counts.get(pattern, {}))
        total = sum(c.values())
        if total < 5:
            return None
        result = {k: v / total for k, v in c.items()}
        result["total"] = total
        return result

    def best_category(self, threshold: float = 0.49) -> Optional[dict]:
        best = None
        for cat in ("COLOR","PARIDAD","RANGO","DOCENA","COLUMNA"):
            pred = self.predict_category(cat)
            if pred is None:
                continue
            clean   = {k: v for k, v in pred.items() if k != "total"}
            if not clean: continue
            top_val = max(clean, key=clean.get)
            top_prob= clean[top_val]
            if top_prob >= threshold:
                if best is None or top_prob > best["probability"]:
                    best = {"category":cat,"bet_value":top_val,"probability":top_prob}
        return best

    @property
    def par_history(self) -> list:
        return list(self._hist["PARIDAD"])

    @property
    def rang_history(self) -> list:
        return list(self._hist["RANGO"])

# ─── AMX SIGNAL SYSTEM ───────────────────────────────────────────────────────
class AMXSignalSystem:
    def __init__(self, mode: Literal["tendencia", "moderado"] = "tendencia"):
        self.mode = mode
        self.last_signal_time: float = 0
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

    def check_signal(self, positions: list, expected_color: str) -> Optional[dict]:
        if len(positions) < 20:
            return None
        ema4  = self.calculate_ema(positions, 4)
        ema8  = self.calculate_ema(positions, 8)
        ema20 = self.calculate_ema(positions, 20)
        if any(v is None for v in [ema4[-1], ema8[-1], ema20[-1],
                                    ema4[-2], ema8[-2], ema20[-2]]):
            return None
        cur = positions[-1]
        ce4, ce8, ce20 = ema4[-1], ema8[-1], ema20[-1]
        pe4, pe8, pe20 = ema4[-2], ema8[-2], ema20[-2]

        cruce_4_20  = pe4 <= pe20 and ce4 > ce20
        cruce_8_20  = pe8 <= pe20 and ce8 > ce20
        sobre_3     = cur > ce4 and cur > ce8 and cur > ce20
        sobre_2     = cur > ce4 and cur > ce8
        patron_v    = False
        if len(positions) >= 3:
            a, b, c = positions[-3], positions[-2], positions[-1]
            patron_v = b < a and b < c and c > a
        emas_alin   = ce4 > ce8 > ce20
        racha_ok    = len(self.last_two_expected) >= 2 and all(self.last_two_expected)

        score = 0
        mode  = "moderado"
        if cruce_4_20:   score += 3; mode = "tendencia"
        if cruce_8_20:   score += 2
        if sobre_3:      score += 2
        if sobre_2:      score += 1
        if patron_v:     score += 2
        if emas_alin:    score += 1
        if racha_ok:     score += 1

        if score < 3:
            return None

        strength = "strong" if score >= 5 else "moderate"
        return {
            "type":           "AMX_EMA",
            "mode":           mode,
            "expected_color": expected_color,
            "score":          score,
            "strength":       strength,
            "trigger_number": 0,
            "pattern":        ("V" if patron_v else
                               "CROSS_4_20" if cruce_4_20 else
                               "CROSS_8_20" if cruce_8_20 else "EMA"),
        }

    def register_signal_sent(self):
        self.last_signal_time = time.time()

# ─── SOPORTE Y RESISTENCIA ────────────────────────────────────────────────────
def find_support_resistance(levels: list,
                            tolerancia: float = 0.5,
                            min_rebotes: int = 2,
                            ventana: int = 5,
                            max_niveles: int = 3) -> dict:
    if len(levels) < ventana * 2 + 2:
        return {"supports": [], "resistances": [],
                "support": None, "resistance": None}

    soportes:     list = []
    resistencias: list = []

    for i in range(ventana, len(levels) - ventana):
        window = levels[i - ventana: i + ventana + 1]
        minimo = min(window)
        if (window.index(minimo) == ventana and
                levels[i] < levels[i-1] and levels[i] < levels[i+1]):
            nivel     = levels[i]
            fuerza    = abs(levels[i-1] - levels[i]) + abs(levels[i+1] - levels[i])
            existente = next((s for s in soportes
                              if abs(s["nivel"] - nivel) <= tolerancia), None)
            if existente:
                existente["rebotes"]      += 1
                existente["fuerza_total"] += fuerza
                existente["ultimos_idx"].append(i)
                if nivel > existente["nivel"]: existente["nivel"] = nivel
            else:
                soportes.append({"nivel": nivel, "rebotes": 1,
                                  "fuerza_total": fuerza,
                                  "ultimos_idx": [i], "fuerte": False})

    for i in range(ventana, len(levels) - ventana):
        window = levels[i - ventana: i + ventana + 1]
        maximo = max(window)
        if (window.index(maximo) == ventana and
                levels[i] > levels[i-1] and levels[i] > levels[i+1]):
            nivel     = levels[i]
            fuerza    = abs(levels[i-1] - levels[i]) + abs(levels[i+1] - levels[i])
            existente = next((r for r in resistencias
                              if abs(r["nivel"] - nivel) <= tolerancia), None)
            if existente:
                existente["rebotes"]      += 1
                existente["fuerza_total"] += fuerza
                existente["ultimos_idx"].append(i)
                if nivel > existente["nivel"]: existente["nivel"] = nivel
            else:
                resistencias.append({"nivel": nivel, "rebotes": 1,
                                      "fuerza_total": fuerza,
                                      "ultimos_idx": [i], "fuerte": False})

    soportes_val = [s for s in soportes if s["rebotes"] >= min_rebotes]
    resist_val   = [r for r in resistencias if r["rebotes"] >= min_rebotes]

    for s in soportes_val:
        s["fuerte"] = s["rebotes"] >= 3 or s["fuerza_total"] >= 4
    for r in resist_val:
        r["fuerte"] = r["rebotes"] >= 3 or r["fuerza_total"] >= 4

    soportes_final   = sorted(soportes_val,
                               key=lambda x: x["ultimos_idx"][-1],
                               reverse=True)[:max_niveles]
    resistencias_final = sorted(resist_val,
                                 key=lambda x: x["ultimos_idx"][-1],
                                 reverse=True)[:max_niveles]

    sup_simple = soportes_final[0]["nivel"]    if soportes_final    else None
    res_simple = resistencias_final[0]["nivel"] if resistencias_final else None

    return {
        "supports":    soportes_final,
        "resistances": resistencias_final,
        "support":     sup_simple,
        "resistance":  res_simple,
    }

# ─── PROBABILIDAD UNIFICADA ──────────────────────────────────────────────────
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
        if len(levels) < 20:
            return 1.0
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
        sr = find_support_resistance(levels[-30:] if len(levels) > 30 else levels)
        if sr['support'] is not None and sr['resistance'] is not None:
            range_size = sr['resistance'] - sr['support']
            if range_size > 0:
                pos = (levels[-1] - sr['support']) / range_size
                self.sr_factor = max(0.9, min(1.1, 1.0 + (pos - 0.5) * 0.1))
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

    def calculate_confidence(self, markov_pred, ml_pred, color: str) -> float:
        if markov_pred is None and ml_pred is None:
            return 0.3
        if markov_pred is None or ml_pred is None:
            return 0.5
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
        if self.spins_since_weight_update < self.WEIGHT_UPDATE_INTERVAL:
            return
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

    def get_joint_probability(self, category: str, bet_value: str,
                              markov_pred, ml_pred, cat_prob: Optional[float]) -> dict:
        if category == "COLOR":
            m_p  = markov_pred.get(bet_value, 0.5) if markov_pred else 0.5
            ml_p = ml_pred.get(bet_value, 0.5)     if ml_pred     else 0.5
        else:
            m_p  = cat_prob if cat_prob is not None else 0.5
            ml_p = cat_prob if cat_prob is not None else 0.5

        model_prob    = self.weights["markov"] * m_p + self.weights["ml"] * ml_p
        combined_prob = model_prob * self.ema_trend_factor * self.sr_factor
        combined_prob = max(0.30, min(0.95, combined_prob))

        strength = ("strong"   if combined_prob >= 0.70 else
                    "moderate" if combined_prob >= 0.60 else "weak")
        return {
            "combined_prob":    combined_prob,
            "markov_prob":      m_p,
            "ml_prob":          ml_p,
            "signal_strength":  strength,
            "weights":          self.weights.copy(),
            "ema_trend_factor": self.ema_trend_factor,
            "sr_factor":        self.sr_factor,
            "volatility":       self.volatility,
        }

# ─── ESTADÍSTICAS DETALLADAS ─────────────────────────────────────────────────
class DetailedStats:
    def __init__(self):
        self.signal_history: deque = deque(maxlen=50)

        self.wins_1: int = 0
        self.wins_2: int = 0
        self.losses: int = 0
        self.total_signals: int = 0

        self.last_stats_at: int = 0
        self.batch_start_bankroll: Optional[float] = None
        self.batch_start_w1: int = 0
        self.batch_start_w2: int = 0
        self.batch_start_losses: int = 0

        self.last_daily_date      = ""
        self.daily_start_bankroll: Optional[float] = None
        self.daily_w1:   int = 0
        self.daily_w2:   int = 0
        self.daily_losses: int = 0
        self.daily_signals: int = 0

    def record_signal_result(self, attempt_won: int, final_result: bool,
                             bet_amount: float, bankroll: float, category: str):
        entry = {"attempt_won": attempt_won, "won": final_result,
                 "bet": bet_amount, "bankroll": bankroll, "timestamp": time.time(),
                 "category": category}
        self.signal_history.append(entry)
        self.total_signals += 1

        if final_result:
            if attempt_won == 1:
                self.wins_1 += 1
            elif attempt_won == 2:
                self.wins_2 += 1
        else:
            self.losses += 1

        self.daily_signals += 1
        if final_result:
            if attempt_won == 1:
                self.daily_w1 += 1
            elif attempt_won == 2:
                self.daily_w2 += 1
        else:
            self.daily_losses += 1

        if self.daily_start_bankroll is None:
            self.daily_start_bankroll = bankroll

    def should_send_stats(self) -> bool:
        return (self.total_signals - self.last_stats_at) >= 20

    def mark_stats_sent(self, bankroll: float):
        self.last_stats_at        = self.total_signals
        self.batch_start_bankroll = bankroll
        self.batch_start_w1       = self.wins_1
        self.batch_start_w2       = self.wins_2
        self.batch_start_losses   = self.losses

    def get_batch_stats(self, current_bankroll: float) -> dict:
        w1 = self.wins_1 - self.batch_start_w1
        w2 = self.wins_2 - self.batch_start_w2
        l  = self.losses - self.batch_start_losses
        wins = w1 + w2
        total = wins + l
        bk = round(current_bankroll - self.batch_start_bankroll, 2) if self.batch_start_bankroll is not None else 0.0
        return {
            "total": total,
            "wins": wins,
            "losses": l,
            "w1": w1,
            "w2": w2,
            "efficiency": round(wins / total * 100, 1) if total else 0.0,
            "e_w1": round(w1 / total * 100, 2) if total else 0.0,
            "e_w2": round(w2 / total * 100, 2) if total else 0.0,
            "e_loss": round(l / total * 100, 2) if total else 0.0,
            "bankroll_delta": bk,
        }

    def get_daily_stats(self, current_bankroll: float) -> dict:
        w1 = self.daily_w1
        w2 = self.daily_w2
        l  = self.daily_losses
        wins = w1 + w2
        total = wins + l
        bk = round(current_bankroll - self.daily_start_bankroll, 2) if self.daily_start_bankroll is not None else 0.0
        return {
            "total": total,
            "wins": wins,
            "losses": l,
            "w1": w1,
            "w2": w2,
            "efficiency": round(wins / total * 100, 1) if total else 0.0,
            "e_w1": round(w1 / total * 100, 2) if total else 0.0,
            "e_w2": round(w2 / total * 100, 2) if total else 0.0,
            "e_loss": round(l / total * 100, 2) if total else 0.0,
            "bankroll_delta": bk,
        }

    def reset_daily(self, date_str: str, current_bankroll: float):
        self.last_daily_date      = date_str
        self.daily_start_bankroll = current_bankroll
        self.daily_w1   = 0
        self.daily_w2   = 0
        self.daily_losses = 0
        self.daily_signals = 0

    def reset(self):
        self.signal_history.clear()
        self.wins_1 = 0
        self.wins_2 = 0
        self.losses = 0
        self.total_signals = 0
        self.last_stats_at = 0
        self.batch_start_bankroll = None
        self.reset_daily("", 0.0)


# ─── TELEGRAM HELPERS ─────────────────────────────────────────────────────────
_TG_MAX_RETRIES = 12

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

        self.spin_history:      list = []
        self.rojo_levels:   list = []
        self.negro_levels:   list = []
        self.par_levels:        list = []
        self.impar_levels:      list = []
        self.alto_levels:       list = []
        self.bajo_levels:       list = []
        self.d1_levels:         list = []
        self.d2_levels:         list = []
        self.d3_levels:         list = []
        self.c1_levels:         list = []
        self.c2_levels:         list = []
        self.c3_levels:         list = []

        self.last_nonzero_color: Optional[str] = None
        self.anti_block: set  = set()

        self.signal_active:          bool = False
        self.waiting_for_attempt:    bool = False
        self.waiting_attempt_number: int  = 0
        self.skip_one_after_zero:    bool = False

        self.active_category:  Optional[str] = None
        self.bet_value:        Optional[str] = None
        self.signal_pair:       tuple          = ()
        self.bet_color:        Optional[str] = None
        self.attempts_left:    int  = 0
        self.total_attempts:   int  = 0
        self.trigger_number:   Optional[int] = None

        self.zero_wait_category: Optional[str] = None
        self.zero_wait_bet_value: Optional[str] = None

        self.signal_msg_ids: list = []
        self.waiting_msg_id: Optional[int] = None
        self.no_confirmation_msg_id: Optional[int] = None
        self.result_sequence: deque = deque(maxlen=10)

        # Martingala (avance de 1 nivel por pérdida, si nivel 6 pierde vuelve a 1)
        self.bet_sys = Martingale(BASE_BET)

        # Cooldown: spins a ignorar tras pérdida total antes de aceptar nueva señal
        self.LOSS_COOLDOWN_SPINS: int = 5
        self.spins_since_loss:    int = 9999  # inicia "listo"

        self.amx_system = AMXSignalSystem(mode="tendencia")   # FORZADO tendencia
        # Ciclo CPR: registra qué categorías (COLOR/PARIDAD/RANGO) ya se
        # resolvieron en el ciclo actual. Al completar las 3 se reinicia.
        self.cpr_cycle_used: set = set()
        self.min_prob_threshold = cfg.get("min_prob_threshold", 0.55)

        self.unified_prob_system = UnifiedProbabilitySystem()

        self.markov       = MarkovChainPredictor(window=60, order=2)
        self.ml_predictor = MLPatternPredictor(pattern_length=3)
        self.category_ml  = CategoryPredictor()

        self.stats = DetailedStats()

        self.ws      = None
        self.running = True

        self._live_conn = _get_live_db()

        # Alerta "POSIBLE ENTRADA"
        self.pre_alert_msg_id: Optional[int] = None
        self._last_near_cats: list = []
        
        # Bandera para procesar estadísticas después de registrar la señal
        self._pending_stats = False

        self._pretrain_from_db(DB_PATH, self.db_table)
        live_loaded = self._load_live_history()

        self.ws_spins_count: int  = live_loaded
        self.warmup_done:    bool = live_loaded >= WARMUP_SPINS

    # ── Métodos auxiliares (sin cambios significativos) ──────────────────
    def _pretrain_from_db(self, db_path: str, table_name: str):
        if not os.path.exists(db_path):
            logger.warning(f"[{self.name}] DB no encontrada: {db_path}")
            return
        spins = []
        try:
            pattern = re.compile(
                rf'INSERT INTO "{table_name}" VALUES \(\d+,(\d+),'
            )
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
        for n in spins:
            real = REAL_COLOR_MAP.get(n, "VERDE")
            temp_history.append({"number": n, "real": real})
            self.markov.update(temp_history)
            self.ml_predictor.add_spin(temp_history)
            self.category_ml.add_spin(n, real)

        logger.info(f"[{self.name}] Pre-entrenado con {len(spins)} giros (tabla: {table_name})")

    def _load_live_history(self) -> int:
        import time as _time
        try:
            cutoff = int(_time.time()) - 7 * 86400
            cur = self._live_conn.execute(
                "SELECT number FROM live_spins WHERE table_name=? AND ts>=? ORDER BY id ASC",
                (self.db_table, cutoff)
            )
            rows = cur.fetchall()
        except Exception as e:
            logger.warning(f"[{self.name}] Error cargando live history: {e}")
            return 0

        if not rows:
            return 0

        temp_history = []
        for (n,) in rows:
            real = REAL_COLOR_MAP.get(n, "VERDE")
            temp_history.append({"number": n, "real": real})
            self.markov.update(temp_history)
            self.ml_predictor.add_spin(temp_history)
            self.category_ml.add_spin(n, real)
            last_o  = self.rojo_levels[-1] if self.rojo_levels else 0
            last_i  = self.negro_levels[-1]  if self.negro_levels  else 0
            last_p  = self.par_levels[-1]        if self.par_levels        else 0
            last_ip = self.impar_levels[-1]      if self.impar_levels      else 0
            last_a  = self.alto_levels[-1]        if self.alto_levels        else 0
            last_b  = self.bajo_levels[-1]        if self.bajo_levels        else 0
            if n == 0:
                self.rojo_levels.append(last_o); self.negro_levels.append(last_i)
                self.par_levels.append(last_p);      self.impar_levels.append(last_ip)
                self.alto_levels.append(last_a);     self.bajo_levels.append(last_b)
                last_d1 = self.d1_levels[-1] if self.d1_levels else 0
                last_d2 = self.d2_levels[-1] if self.d2_levels else 0
                last_d3 = self.d3_levels[-1] if self.d3_levels else 0
                last_c1 = self.c1_levels[-1] if self.c1_levels else 0
                last_c2 = self.c2_levels[-1] if self.c2_levels else 0
                last_c3 = self.c3_levels[-1] if self.c3_levels else 0
                self.d1_levels.append(last_d1); self.d2_levels.append(last_d2); self.d3_levels.append(last_d3)
                self.c1_levels.append(last_c1); self.c2_levels.append(last_c2); self.c3_levels.append(last_c3)
            else:
                par    = get_paridad(n)
                rang   = get_rango(n)
                dozen  = get_dozen(n)
                column = get_column(n)
                self.rojo_levels.append((self.rojo_levels[-1] if self.rojo_levels else 0) + (1 if real=="ROJO"  else -1))
                self.negro_levels.append((self.negro_levels[-1] if self.negro_levels else 0) + (1 if real=="NEGRO" else -1))
                self.par_levels.append((self.par_levels[-1] if self.par_levels else 0)     + (1 if par=="PAR"   else -1))
                self.impar_levels.append((self.impar_levels[-1] if self.impar_levels else 0) + (1 if par=="IMPAR" else -1))
                self.alto_levels.append((self.alto_levels[-1] if self.alto_levels else 0)  + (1 if rang=="ALTO" else -1))
                self.bajo_levels.append((self.bajo_levels[-1] if self.bajo_levels else 0)  + (1 if rang=="BAJO" else -1))
                self.d1_levels.append((self.d1_levels[-1] if self.d1_levels else 0) + (1 if dozen==1 else -1))
                self.d2_levels.append((self.d2_levels[-1] if self.d2_levels else 0) + (1 if dozen==2 else -1))
                self.d3_levels.append((self.d3_levels[-1] if self.d3_levels else 0) + (1 if dozen==3 else -1))
                self.c1_levels.append((self.c1_levels[-1] if self.c1_levels else 0) + (1 if column==1 else -1))
                self.c2_levels.append((self.c2_levels[-1] if self.c2_levels else 0) + (1 if column==2 else -1))
                self.c3_levels.append((self.c3_levels[-1] if self.c3_levels else 0) + (1 if column==3 else -1))
            self.spin_history.append({"number": n, "real": real})
            if len(self.spin_history) > 300:
                self.spin_history.pop(0)

        logger.info(f"[{self.name}] ✅ Historial ML cargado: {len(rows)} giros")
        return len(rows)

    def _persist_spin(self, number: int):
        import time as _time
        try:
            self._live_conn.execute(
                "INSERT INTO live_spins (table_name, number, ts) VALUES (?,?,?)",
                (self.db_table, number, int(_time.time()))
            )
            self._live_conn.commit()
        except Exception as e:
            logger.warning(f"[{self.name}] SQLite error, reconectando: {e}")
            try:
                self._live_conn = _get_live_db()
                self._live_conn.execute(
                    "INSERT INTO live_spins (table_name, number, ts) VALUES (?,?,?)",
                    (self.db_table, number, int(_time.time()))
                )
                self._live_conn.commit()
            except Exception as e2:
                logger.error(f"[{self.name}] SQLite irrecuperable: {e2}")

    def _cleanup_old_live_spins(self):
        import time as _time
        try:
            cutoff = int(_time.time()) - 7 * 86400
            self._live_conn.execute(
                "DELETE FROM live_spins WHERE table_name=? AND ts<?",
                (self.db_table, cutoff)
            )
            self._live_conn.commit()
            logger.info(f"[{self.name}] Limpieza SQLite: giros > 7 días eliminados")
        except Exception as e:
            logger.debug(f"[{self.name}] Error limpiando live_db: {e}")

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

    def _opposite_color(self, color: str) -> str:
        return "NEGRO" if color == "ROJO" else "ROJO"

    def _category_icon(self, value: str) -> str:
        return CATEGORY_ICONS.get(value, "❓")

    def _trigger_display(self, number: int, category: str) -> str:
        if number == 0:
            return "0 VERDE 🟢"
        if category == "COLOR":     val = REAL_COLOR_MAP.get(number, "VERDE")
        elif category == "PARIDAD": val = get_paridad(number) or "VERDE"
        elif category == "RANGO":   val = get_rango(number) or "VERDE"
        elif category == "DOCENA":  val = f"D{get_dozen(number)}"
        elif category == "COLUMNA": val = f"C{get_column(number)}"
        else: val = REAL_COLOR_MAP.get(number, "VERDE")
        return f"{number} {val} {self._category_icon(val)}"

    def _is_win(self, number: int, real_color: str) -> Optional[bool]:
        if number == 0: return None
        cat = self.active_category
        if cat == "COLOR":   return real_color == self.bet_value
        if cat == "PARIDAD": return get_paridad(number) == self.bet_value
        if cat == "RANGO":   return get_rango(number) == self.bet_value
        if cat == "DOCENA":
            result = f"D{get_dozen(number)}"
            return result in self.signal_pair if self.signal_pair else result == self.bet_value
        if cat == "COLUMNA":
            result = f"C{get_column(number)}"
            return result in self.signal_pair if self.signal_pair else result == self.bet_value
        return False

    def _levels_for(self, category: str, bet_value: str) -> list:
        return {
            ("COLOR",   "ROJO"):  self.rojo_levels,
            ("COLOR",   "NEGRO"): self.negro_levels,
            ("PARIDAD", "PAR"):   self.par_levels,
            ("PARIDAD", "IMPAR"): self.impar_levels,
            ("RANGO",   "ALTO"):  self.alto_levels,
            ("RANGO",   "BAJO"):  self.bajo_levels,
            ("DOCENA",  "D1"):    self.d1_levels,
            ("DOCENA",  "D2"):    self.d2_levels,
            ("DOCENA",  "D3"):    self.d3_levels,
            ("COLUMNA", "C1"):    self.c1_levels,
            ("COLUMNA", "C2"):    self.c2_levels,
            ("COLUMNA", "C3"):    self.c3_levels,
        }.get((category, bet_value), [])

    def _evaluate_category(self, category: str) -> Optional[dict]:
        trigger = self.spin_history[-1]["number"] if self.spin_history else 0

        pred = self.category_ml.predict_category(category)
        if pred is None or pred.get("total", 0) < 5:
            return None
        clean = {k: v for k, v in pred.items() if k != "total"}
        if not clean:
            return None
        best_val = max(clean, key=clean.get)
        cat_prob = clean[best_val]
        if cat_prob < self.min_prob_threshold:
            return None

        levels  = self._levels_for(category, best_val)
        ema_sig = self.amx_system.check_signal(levels, best_val)
        if ema_sig is None:
            return None
        ema_sig["trigger_number"] = trigger

        markov_pred = self.markov.predict(self.spin_history)
        ml_pred     = self.ml_predictor.predict(self.spin_history)
        unified     = self.unified_prob_system.get_joint_probability(
            category, best_val, markov_pred, ml_pred, cat_prob)
        ema_bonus  = 0.03 if ema_sig.get("strength") == "strong" else 0.0
        final_prob = min(0.95, unified["combined_prob"] + ema_bonus)
        if final_prob < self.min_prob_threshold:
            return None

        if category == "DOCENA":
            excl_num  = int(best_val[1])
            pair      = DOZEN_PAIRS[excl_num]
            pair_strs = (f"D{pair[0]}", f"D{pair[1]}")
            return {
                "category":       category,
                "bet_value":      best_val,
                "signal_pair":    pair_strs,
                "probability":    final_prob,
                "trigger_number": trigger,
                "ema_score":      ema_sig.get("score", 0),
                "ema_pattern":    ema_sig.get("pattern", ""),
            }
        if category == "COLUMNA":
            excl_num  = int(best_val[1])
            pair      = COLUMN_PAIRS[excl_num]
            pair_strs = (f"C{pair[0]}", f"C{pair[1]}")
            return {
                "category":       category,
                "bet_value":      best_val,
                "signal_pair":    pair_strs,
                "probability":    final_prob,
                "trigger_number": trigger,
                "ema_score":      ema_sig.get("score", 0),
                "ema_pattern":    ema_sig.get("pattern", ""),
            }

        return {
            "category":       category,
            "bet_value":      best_val,
            "signal_pair":    (),
            "probability":    final_prob,
            "trigger_number": trigger,
            "ema_score":      ema_sig.get("score", 0),
            "ema_pattern":    ema_sig.get("pattern", ""),
        }

    def _detect_best_category_signal(self) -> Optional[dict]:
        candidates = []
        CPR = {"COLOR", "PARIDAD", "RANGO"}
        for cat in ("COLOR", "PARIDAD", "RANGO", "DOCENA", "COLUMNA"):
            if cat in CPR and cat in self.cpr_cycle_used:
                continue
            cand = self._evaluate_category(cat)
            if cand:
                candidates.append(cand)
        if not candidates:
            return None
        return max(candidates, key=lambda x: x["probability"])

    def _get_predictor_votes(self, color: str) -> int:
        votes = 0
        mp = self.markov.predict(self.spin_history)
        if mp and mp.get(color, 0) > 0.50: votes += 1
        ml = self.ml_predictor.predict(self.spin_history)
        if ml and ml.get(color, 0) > 0.50: votes += 1
        return votes

    def _check_recovery(self):
        pass

    def _mark_cpr_used(self, category: str):
        if category not in ("COLOR", "PARIDAD", "RANGO"):
            return
        self.cpr_cycle_used.add(category)
        logger.info(
            f"[{self.name}] Ciclo CPR: usadas={self.cpr_cycle_used} "
            f"({len(self.cpr_cycle_used)}/3)")
        if self.cpr_cycle_used >= {"COLOR", "PARIDAD", "RANGO"}:
            self.cpr_cycle_used.clear()
            logger.info(f"[{self.name}] 🔄 Ciclo CPR completado — reiniciando ciclo")

    def _get_unified_probability(self, color: str, trigger_number: int) -> dict:
        markov_pred = self.markov.predict(self.spin_history)
        ml_pred     = self.ml_predictor.predict(self.spin_history)
        return self.unified_prob_system.get_joint_probability(
            "COLOR", color, markov_pred, ml_pred, None)

    def _get_category_probability(self, category: str, bet_value: str,
                                  trigger_number: int) -> dict:
        if category == "COLOR":
            return self._get_unified_probability(bet_value, trigger_number)
        pred = self.category_ml.predict_category(category)
        prob = pred.get(bet_value, 0.5) if pred else 0.5
        return {
            "combined_prob": prob, "markov_prob": 0.5, "ml_prob": prob,
            "confidence": 0.6,
            "threshold": self.min_prob_threshold, "signal_strength": "moderate",
            "weights": self.unified_prob_system.weights.copy(),
            "ema_trend_factor": 1.0, "sr_factor": 1.0, "volatility": 1.0,
        }

    def _chart_color(self) -> str:
        if self.active_category == "COLOR":
            return self.bet_value if self.bet_value in ("ROJO", "NEGRO") else "ROJO"
        return "ROJO"

    def _cat_val(self, number: int, real: str) -> tuple[str, str]:
        cat = self.active_category or "COLOR"
        if cat == "COLOR":   val = real
        elif cat == "PARIDAD": val = get_paridad(number) or "VERDE"
        elif cat == "RANGO":   val = get_rango(number) or "VERDE"
        elif cat == "DOCENA":  val = f"D{get_dozen(number)}" if number != 0 else "VERDE"
        elif cat == "COLUMNA": val = f"C{get_column(number)}" if number != 0 else "VERDE"
        else: val = real
        return val, self._category_icon(val)

    # ── MENSAJES ─────────────────────────────────────────────────────────
    def _build_signal_text(self, attempt: int, unified_prob: Optional[dict]) -> str:
        bet      = self.bet_sys.current_bet()
        prob_pct = int((unified_prob["combined_prob"] if unified_prob else 0.5) * 100)
        val_icon  = self._category_icon(self.bet_value or "")
        trig_disp = self._trigger_display(self.trigger_number, self.active_category or "COLOR")
        cat_labels = {"COLOR":"🔴⚫ COLOR","PARIDAD":"🟣🟡 PARIDAD",
                      "RANGO":"🔵🟤 RANGO","DOCENA":"🤍 DOCENA","COLUMNA":"💚 COLUMNA"}
        cat_label = cat_labels.get(self.active_category or "COLOR", self.active_category or "")
        nivel_actual = self.bet_sys.level
        if self.signal_pair and self.active_category in ("DOCENA","COLUMNA"):
            p1, p2 = self.signal_pair
            i1 = self._category_icon(p1); i2 = self._category_icon(p2)
            apuesta_str = f"<b>{p1}</b> {i1} y <b>{p2}</b> {i2}"
        else:
            apuesta_str = f"<b>{self.bet_value}</b> {val_icon}"
        return (
            f"🎯 <b>SEÑAL CONFIRMADA</b> 🎯\n\n"
            f"🎰 <b>{self.name}</b>\n"
            f"👉 <b>ÚLTIMO NÚMERO: {trig_disp}</b>\n"
            f"❄️ <b>ENTRAR EN: {apuesta_str}</b>\n\n"
            f"💡 <i>PROBABILIDAD IA {prob_pct}%</i>\n"
            f"📈 <i>NIVEL SECUENCIA: {nivel_actual}/6</i>\n"
            f"📍 <i>MONTO APUESTA: {bet:.2f} usd</i>\n"
        )

    def _send_signal(self, attempt: int, unified_prob: dict):
        # Eliminar pre-alert si existe
        if self.pre_alert_msg_id:
            tg_delete(self.bot, self.chat_id, self.pre_alert_msg_id)
            self.pre_alert_msg_id = None

        if self.signal_msg_ids:
            for mid in self.signal_msg_ids:
                tg_delete(self.bot, self.chat_id, mid)
            self.signal_msg_ids = []
        if self.no_confirmation_msg_id:
            tg_delete(self.bot, self.chat_id, self.no_confirmation_msg_id)
            self.no_confirmation_msg_id = None

        text   = self._build_signal_text(attempt, unified_prob)
        msg_id = tg_send_text(self.bot, self.chat_id, self.thread_id, text)
        if msg_id:
            self.signal_msg_ids.append(msg_id)

        prob_pct = int((unified_prob["combined_prob"] if unified_prob else 0.5) * 100)
        logger.info(
            f"[{self.name}] 🎯 [{self.active_category}] {self.bet_value} "
            f"intento={attempt} nivel={self.bet_sys.level} trig={self.trigger_number} prob={prob_pct}%"
        )

    def _send_waiting_message(self, attempt_number: int):
        if self.signal_msg_ids:
            for mid in self.signal_msg_ids:
                tg_delete(self.bot, self.chat_id, mid)
            self.signal_msg_ids = []
        if self.no_confirmation_msg_id:
            tg_delete(self.bot, self.chat_id, self.no_confirmation_msg_id)
            self.no_confirmation_msg_id = None
        logger.info(f"[{self.name}] ⏳ Esperando condiciones intento {attempt_number}")

    def _send_result(self, number: int, real: str, won: bool, bet: float,
                     level_used: int, delete_signals: bool = True):
        if delete_signals and self.signal_msg_ids:
            for mid in self.signal_msg_ids:
                tg_delete(self.bot, self.chat_id, mid)
            self.signal_msg_ids = []
        if self.no_confirmation_msg_id:
            tg_delete(self.bot, self.chat_id, self.no_confirmation_msg_id)
            self.no_confirmation_msg_id = None

        bankroll         = self.bet_sys.bankroll
        cat_val, cat_icon = self._cat_val(number, real)
        bet_icon          = self._category_icon(self.bet_value or "")
        status            = f"✅ <b>¡GREEN {number} {cat_val}!</b> {cat_icon}!" if won else f"❌ <b>¡LOSS {number} {cat_val}!</b> {cat_icon}"

        text = (
            f"{status}\n\n"
            f"❄️ <b>CATEGORIA: {self.bet_value}</b> {bet_icon}\n"
            f"💰 <i>BANKROLL: {bankroll:.2f} usd</i>\n"
            f"♻️ <i>NIVEL DE INTENTO {level_used}/6</i>"
        )
        tg_send_text(self.bot, self.chat_id, self.thread_id, text)
        logger.info(f"[{self.name}] {'WIN' if won else 'LOSS'} #{number} "
                    f"cat_val={cat_val} nivel_usado={level_used} bankroll={bankroll:.2f}")

    # ── Alerta "POSIBLE ENTRADA" ───────────────────────────────────────────
    def _check_and_send_pre_alert(self):
        if self.signal_active or self.waiting_for_attempt:
            return
        remaining = {"COLOR", "PARIDAD", "RANGO"} - self.cpr_cycle_used
        near_cats = []
        for cat in sorted(remaining):
            pred = self.category_ml.predict_category(cat)
            if pred is None or pred.get("total", 0) < 5:
                continue
            clean = {k: v for k, v in pred.items() if k != "total"}
            if not clean:
                continue
            best_val = max(clean, key=clean.get)
            cat_prob = clean[best_val]
            if cat_prob < 0.50:          # umbral reducido para "posible"
                continue
            levels = self._levels_for(cat, best_val)
            ema_sig = self.amx_system.check_signal(levels, best_val)
            if ema_sig is None:
                continue
            near_cats.append(cat)

        if near_cats:
            cat_list = ' | '.join(near_cats)
            text = f"🚨 <b>POSIBLE ENTRADA</b> 🚨\n\n⭐️ <b>CATEGORIAS: {cat_list}</b> 🌟"
            if set(near_cats) != set(self._last_near_cats):
                if self.pre_alert_msg_id:
                    tg_delete(self.bot, self.chat_id, self.pre_alert_msg_id)
                    self.pre_alert_msg_id = None
                msg_id = tg_send_text(self.bot, self.chat_id, self.thread_id, text)
                if msg_id:
                    self.pre_alert_msg_id = msg_id
                self._last_near_cats = near_cats.copy()
        else:
            if self.pre_alert_msg_id:
                tg_delete(self.bot, self.chat_id, self.pre_alert_msg_id)
                self.pre_alert_msg_id = None
            self._last_near_cats = []

    # ── ESTADÍSTICAS (CORREGIDO: se envían después de registrar la señal) ──
    def _check_stats(self):
        if not self.stats.should_send_stats():
            return
        current_bankroll = self.bet_sys.bankroll
        self.stats.mark_stats_sent(current_bankroll)

        parts = []

        # 1) Últimas 20 señales
        last20 = list(self.stats.signal_history)[-20:]
        if last20:
            lines = ["📊 <b>ESTADISTICAS 20 SEÑALES</b>"]
            for i, entry in enumerate(last20, start=1):
                if entry['won']:
                    gale = "GALE #0" if entry['attempt_won'] == 1 else "GALE #1"
                    line = f"✅ WIN {i}, CATEGORIA {entry['category']}, {gale}"
                else:
                    line = f"❌ LOSS {i}, CATEGORIA {entry['category']}, GALE #1"
                lines.append(line)
            parts.append("\n".join(lines))

        # 2) Estadísticas diarias (24h)
        sd = self.stats.get_daily_stats(current_bankroll)
        if sd['total'] > 0:
            daily_lines = [
                "📅 <b>ESTADISTICAS 24 HORAS</b>",
                f"🈯️ TOTAL DE SEÑALES: {sd['total']} = {sd['efficiency']}%",
                f"1️⃣ GALE #0: {sd['w1']} = {sd['e_w1']}%",
                f"2️⃣ GALE #1: {sd['w2']} = {sd['e_w2']}%",
                f"🈲 LOSS: {sd['losses']} = {sd['e_loss']}%",
                f"💰 CAPITAL ACUMULADO: {sd['bankroll_delta']:.2f} usd"
            ]
            parts.append("\n".join(daily_lines))

        if parts:
            tg_send_text(self.bot, self.chat_id, self.thread_id, "\n\n".join(parts))

    def _check_daily_report(self):
        import datetime
        tz_ar = datetime.timezone(datetime.timedelta(hours=-3))
        now_ar = datetime.datetime.now(tz=tz_ar)
        if now_ar.hour < 12:
            return
        today_str = now_ar.strftime("%Y-%m-%d")
        if self.stats.last_daily_date == today_str:
            return
        current_bankroll = self.bet_sys.bankroll
        self.stats.reset_daily(today_str, current_bankroll)
        logger.info(f"[{self.name}] Daily counters reset for {today_str}")

    def process_number(self, number: int):
        try:
            self._process_number_inner(number)
        except Exception as e:
            logger.error(f"[{self.name}] ❌ Error en process_number({number}): {e}", exc_info=True)
            if self.signal_active:
                self.signal_active       = False
                self.waiting_for_attempt = False
                self.attempts_left       = MAX_ATTEMPTS

    def _process_number_inner(self, number: int):
        real = REAL_COLOR_MAP.get(number, "VERDE")

        self._persist_spin(number)

        if len(self.spin_history) > 0 and len(self.spin_history) % 5000 == 0:
            self._cleanup_old_live_spins()

        self.spin_history.append({"number": number, "real": real})
        if len(self.spin_history) > 300:
            self.spin_history.pop(0)
        self.result_sequence.append({"number": number, "real": real})

        if number == 0:
            self.rojo_levels.append(self.rojo_levels[-1] if self.rojo_levels else 0)
            self.negro_levels.append(self.negro_levels[-1] if self.negro_levels else 0)
            self.par_levels.append(self.par_levels[-1] if self.par_levels else 0)
            self.impar_levels.append(self.impar_levels[-1] if self.impar_levels else 0)
            self.alto_levels.append(self.alto_levels[-1] if self.alto_levels else 0)
            self.bajo_levels.append(self.bajo_levels[-1] if self.bajo_levels else 0)
            self.d1_levels.append(self.d1_levels[-1] if self.d1_levels else 0)
            self.d2_levels.append(self.d2_levels[-1] if self.d2_levels else 0)
            self.d3_levels.append(self.d3_levels[-1] if self.d3_levels else 0)
            self.c1_levels.append(self.c1_levels[-1] if self.c1_levels else 0)
            self.c2_levels.append(self.c2_levels[-1] if self.c2_levels else 0)
            self.c3_levels.append(self.c3_levels[-1] if self.c3_levels else 0)
        else:
            par    = get_paridad(number)
            rang   = get_rango(number)
            dozen  = get_dozen(number)
            column = get_column(number)
            self.rojo_levels.append((self.rojo_levels[-1] if self.rojo_levels else 0) + (1 if real=="ROJO"  else -1))
            self.negro_levels.append((self.negro_levels[-1] if self.negro_levels else 0) + (1 if real=="NEGRO" else -1))
            self.par_levels.append((self.par_levels[-1] if self.par_levels else 0)     + (1 if par=="PAR"   else -1))
            self.impar_levels.append((self.impar_levels[-1] if self.impar_levels else 0) + (1 if par=="IMPAR" else -1))
            self.alto_levels.append((self.alto_levels[-1] if self.alto_levels else 0)  + (1 if rang=="ALTO" else -1))
            self.bajo_levels.append((self.bajo_levels[-1] if self.bajo_levels else 0)  + (1 if rang=="BAJO" else -1))
            self.d1_levels.append((self.d1_levels[-1] if self.d1_levels else 0) + (1 if dozen==1 else -1))
            self.d2_levels.append((self.d2_levels[-1] if self.d2_levels else 0) + (1 if dozen==2 else -1))
            self.d3_levels.append((self.d3_levels[-1] if self.d3_levels else 0) + (1 if dozen==3 else -1))
            self.c1_levels.append((self.c1_levels[-1] if self.c1_levels else 0) + (1 if column==1 else -1))
            self.c2_levels.append((self.c2_levels[-1] if self.c2_levels else 0) + (1 if column==2 else -1))
            self.c3_levels.append((self.c3_levels[-1] if self.c3_levels else 0) + (1 if column==3 else -1))
            self.last_nonzero_color = real

        while len(self.rojo_levels) > len(self.spin_history):
            self.rojo_levels.pop(0)
        while len(self.negro_levels) > len(self.spin_history):
            self.negro_levels.pop(0)
        min_len = min(len(self.rojo_levels), len(self.negro_levels))
        self.rojo_levels = self.rojo_levels[-min_len:]
        self.negro_levels = self.negro_levels[-min_len:]

        self.amx_system.update_streak(real, None)
        if real != "VERDE":
            self.unified_prob_system.update_streak(real)
            ref_lv = self.rojo_levels if real == "ROJO" else self.negro_levels
            self.unified_prob_system.calculate_volatility(ref_lv)
            self.unified_prob_system.update_trend_factors(ref_lv)

        self.markov.update(self.spin_history)
        self.ml_predictor.add_spin(self.spin_history)
        if not (self.signal_active or self.waiting_for_attempt):
            self.spins_since_loss += 1
        self.category_ml.add_spin(number, real)
        self.unified_prob_system.update_weights()

        # ── LOG POR GIRO ────────────────────────────────────────────────
        par_v  = get_paridad(number) or "VERDE"
        rang_v = get_rango(number)   or "VERDE"
        doz_v  = f"D{get_dozen(number)}"  if number != 0 else "VERDE"
        col_v  = f"C{get_column(number)}" if number != 0 else "VERDE"
        if self.signal_active:
            sig_state = (f"🟢 [{self.active_category}] {self.bet_value} "
                         f"Martingala nivel {self.bet_sys.level}/6")
        elif self.waiting_for_attempt:
            sig_state = f"⏳ Esperando intento {self.waiting_attempt_number}"
        else:
            remaining = {"COLOR","PARIDAD","RANGO"} - self.cpr_cycle_used
            sig_state = f"⚪ Idle | CPR disp: {'|'.join(sorted(remaining)) if remaining else 'ninguna'}"
        logger.info(
            f"[{self.name}] 🎰 #{len(self.spin_history)} "
            f"N:{number:>2} {real:<5} | {par_v:<5} {rang_v:<4} {doz_v} {col_v} | "
            f"BK:{self.bet_sys.bankroll:>7.2f}$ Martingala Nivel {self.bet_sys.level} | "
            f"{sig_state}")

        # ═══ MÁQUINA DE ESTADOS ═══
        if self.signal_active:
            result = self._is_win(number, real)

            if result is None:          # 0 (VERDE)
                level_used = self.bet_sys.level
                bet = self.bet_sys.loss()            # avanza 1 nivel
                self.attempts_left -= 1
                if self.attempts_left <= 0:
                    self.spins_since_loss = 0
                    self.stats.record_signal_result(0, False, bet,
                                                    self.bet_sys.bankroll, self.active_category)
                    self._mark_cpr_used(self.active_category)
                    self.signal_active   = False
                    self.active_category = None
                    self.zero_wait_category = None
                    self.zero_wait_bet_value = None
                    self._send_result(number, real, False, bet, level_used)
                    self._pending_stats = True   # ← activar estadísticas
                    self.signal_msg_ids = []
                    return
                attempt_number = self.total_attempts - self.attempts_left + 1
                self.zero_wait_category = self.active_category
                self.zero_wait_bet_value = self.bet_value
                self.signal_active = False
                self.waiting_for_attempt = True
                self.waiting_attempt_number = attempt_number
                self.skip_one_after_zero = True
                self._send_waiting_message(attempt_number)
                return

            current_attempt = self.total_attempts - self.attempts_left + 1

            if result:
                level_used = self.bet_sys.level
                bet = self.bet_sys.win()
                self.stats.record_signal_result(current_attempt, True, bet,
                                                self.bet_sys.bankroll, self.active_category)
                self._mark_cpr_used(self.active_category)
                self.signal_active   = False
                self.active_category = None
                self.zero_wait_category = None
                self.zero_wait_bet_value = None
                self._send_result(number, real, True, bet, level_used)
                self._pending_stats = True   # ← activar estadísticas
                self.signal_msg_ids = []
            else:
                # Pérdida normal (sin cero)
                self.attempts_left -= 1
                if self.attempts_left <= 0:
                    level_used = self.bet_sys.level
                    self._handle_full_loss(number, real, level_used)
                else:
                    self.bet_sys.loss()   # avanza 1 nivel antes del siguiente intento
                    attempt_number = self.total_attempts - self.attempts_left + 1
                    self.trigger_number = number
                    unified_prob = self._get_category_probability(
                        self.active_category, self.bet_value, number)
                    self._send_signal(attempt_number, unified_prob)

        elif self.waiting_for_attempt:
            if real == "VERDE":
                self.skip_one_after_zero = True
                return
            if self.skip_one_after_zero:
                self.skip_one_after_zero = False
                return

            if self.zero_wait_category is not None:
                self.active_category = self.zero_wait_category
                self.bet_value = self.zero_wait_bet_value
                self.bet_color = self.bet_value if self.active_category == "COLOR" else "ROJO"
                self.zero_wait_category = None
                self.zero_wait_bet_value = None
                self.trigger_number = number
                unified_prob = self._get_category_probability(
                    self.active_category, self.bet_value, number)
                self.signal_active = True
                self.waiting_for_attempt = False
                self._send_signal(self.waiting_attempt_number, unified_prob)
                return

            attempt_number = self.waiting_attempt_number
            best = self._detect_best_category_signal()
            if not best or best["probability"] < self.min_prob_threshold:
                ords = {2:"2°"}
                ord_str = ords.get(attempt_number, f"{attempt_number}°")
                if self.no_confirmation_msg_id:
                    tg_delete(self.bot, self.chat_id, self.no_confirmation_msg_id)
                msg = tg_send_text(self.bot, self.chat_id, self.thread_id,
                                   f"🔔 Sin confirmación para enviar señal para el intento {ord_str}")
                if msg:
                    self.no_confirmation_msg_id = msg
            elif best and best["probability"] >= self.min_prob_threshold:
                unified_prob = self._get_category_probability(
                    best["category"], best["bet_value"], number)
                if unified_prob["combined_prob"] < self.min_prob_threshold:
                    logger.debug(
                        f"[{self.name}] Reintento descartado [{best['category']}] "
                        f"{best['bet_value']} prob={unified_prob['combined_prob']*100:.0f}% < 60%")
                    ords = {2:"2°"}
                    ord_str = ords.get(attempt_number, f"{attempt_number}°")
                    if self.no_confirmation_msg_id:
                        tg_delete(self.bot, self.chat_id, self.no_confirmation_msg_id)
                    msg = tg_send_text(self.bot, self.chat_id, self.thread_id,
                                       f"🔔 Sin confirmación para enviar señal para el intento {ord_str}")
                    if msg:
                        self.no_confirmation_msg_id = msg
                else:
                    if self.no_confirmation_msg_id:
                        tg_delete(self.bot, self.chat_id, self.no_confirmation_msg_id)
                        self.no_confirmation_msg_id = None
                    self.active_category    = best["category"]
                    self.bet_value          = best["bet_value"]
                    self.bet_color          = best["bet_value"] if best["category"] == "COLOR" else "ROJO"
                    self.trigger_number     = number
                    self.signal_active      = True
                    self.waiting_for_attempt = False
                    self._send_signal(attempt_number, unified_prob)

        else:
            self.signal_msg_ids = []
            if self.spins_since_loss < self.LOSS_COOLDOWN_SPINS:
                logger.debug(
                    f"[{self.name}] Cooldown post-pérdida: {self.spins_since_loss}/{self.LOSS_COOLDOWN_SPINS} spins")
                return
            best = self._detect_best_category_signal()
            if best:
                unified_prob = self._get_category_probability(
                    best["category"], best["bet_value"], best["trigger_number"])
                if unified_prob["combined_prob"] < self.min_prob_threshold:
                    logger.debug(
                        f"[{self.name}] Señal descartada [{best['category']}] "
                        f"{best['bet_value']} prob={unified_prob['combined_prob']*100:.0f}% < 60%")
                    return
                self.signal_active   = True
                self.active_category = best["category"]
                self.bet_value       = best["bet_value"]
                self.signal_pair     = best.get("signal_pair", ())
                self.bet_color       = best["bet_value"] if best["category"] == "COLOR" else "ROJO"
                self.trigger_number  = number
                self.total_attempts  = MAX_ATTEMPTS
                self.attempts_left   = MAX_ATTEMPTS
                self._send_signal(1, unified_prob)
                self.amx_system.register_signal_sent()

        # --- Procesar estadísticas pendientes (después de registrar la señal) ---
        if self._pending_stats:
            self._check_daily_report()
            self._check_stats()
            self._pending_stats = False

        # ── LLAMADA PRE-ALERT (solo en reposo) ──────────────────────────
        if not self.signal_active and not self.waiting_for_attempt:
            self._check_and_send_pre_alert()

        if not self.warmup_done:
            self.ws_spins_count += 1
            if self.ws_spins_count < WARMUP_SPINS:
                logger.info(f"[{self.name}] Calentamiento WS: {self.ws_spins_count}/{WARMUP_SPINS} giros")
                return
            self.warmup_done = True
            logger.info(f"[{self.name}] ✅ Calentamiento completado. Iniciando señales.")
            tg_send_text(self.bot, self.chat_id, self.thread_id,
                         f"🟢 <b>{self.name}</b> — Sistema listo. Emitiendo señales.")

    def _handle_full_loss(self, number: int, real: str, level_used: int):
        """Pérdida definitiva de la señal: aplica full_loss (sube 1 nivel y cuenta consecutiva)."""
        bet = self.bet_sys.full_loss()
        self.spins_since_loss = 0
        self.stats.record_signal_result(0, False, bet,
                                        self.bet_sys.bankroll, self.active_category)
        self._mark_cpr_used(self.active_category)
        self.signal_active   = False
        self.active_category = None
        self.zero_wait_category = None
        self.zero_wait_bet_value = None
        self._send_result(number, real, False, bet, level_used)
        self._pending_stats = True   # ← activar estadísticas
        self.signal_msg_ids = []

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
                logger.warning(f"[{self.name}] WS desconectado: {e}. Recon en {reconnect_delay}s")
                try:
                    tg_send_text(self.bot, self.chat_id, self.thread_id,
                                 f"⚠️ <b>{self.name}</b> — Conexión perdida. Reconectando en {reconnect_delay}s...")
                except Exception:
                    pass
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 60)

# ─── FLASK KEEPALIVE ──────────────────────────────────────────────────────────
app = Flask(__name__)

@app.route("/")
def index():
    return jsonify({"status": "ok", "bot": "Roulette Signal Bot Martingala AMX", "ts": time.time()})

@app.route("/ping")
def ping():
    return jsonify({"pong": True, "ts": time.time()})

@app.route("/health")
def health():
    return jsonify({"healthy": True})

async def self_ping_loop():
    url = os.environ.get("RENDER_EXTERNAL_URL", "").rstrip("/")
    if not url:
        logger.warning("[KeepAlive] RENDER_EXTERNAL_URL no definida — self-ping inactivo")
        return
    ping_url = f"{url}/ping"
    logger.info(f"[KeepAlive] ✅ Self-ping activo → {ping_url} cada 4 min")
    await asyncio.sleep(30)
    while True:
        try:
            with urllib.request.urlopen(ping_url, timeout=15) as r:
                logger.info(f"[KeepAlive] 🟢 OK [{r.status}] → {ping_url}")
        except Exception as e:
            logger.warning(f"[KeepAlive] ❌ Falló → {ping_url}: {e}")
        await asyncio.sleep(240)

# ─── COMANDOS TELEGRAM ────────────────────────────────────────────────────────
engines: dict[str, RouletteEngine] = {}

def _register_handlers(b: telebot.TeleBot):
    @b.message_handler(commands=['start', 'help'])
    def cmd_start(message):
        help_text = f"""
<b>🎰 Roulette Bot AMX — Martingala</b>
Russian Roulette

<b>Características:</b>
• Sistema Martingala con niveles: 0.50 / 2.00 / 8.00 / 16.00 € (avance x1)
• Máximo 2 intentos por señal
• Reinicio a nivel 1 al ganar; si nivel 6 pierde vuelve a 1
• Modo AMX <b>tendencia</b> activado por defecto
• Cooldown 5 spins tras pérdida
• Calentamiento WS: 21 giros silenciosos
• Estadísticas unificadas con historial de 20 señales + 24 horas
• Alerta "POSIBLE ENTRADA" según categorías CPR restantes

Comandos:
/moderado - Cambiar a modo MODERADO (menos agresivo)
/tendencia - Cambiar a modo TENDENCIA (más agresivo)
/status - Estado actual
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
        lines = ["<b>📊 ESTADO AMX MARTINGALA</b>\n"]
        for name, engine in engines.items():
            mode_icon = "📈" if engine.amx_system.mode == "tendencia" else "📊"
            if engine.signal_active:
                cat  = engine.active_category or "?"
                val  = engine.bet_value or "?"
                icon = CATEGORY_ICONS.get(val, "")
                st   = f"🟢 [{cat}] {val}{icon} nivel {engine.bet_sys.level}/6"
            elif engine.waiting_for_attempt:
                st = f"⏳ Esperando intento {engine.waiting_attempt_number}/{engine.total_attempts}"
            else:
                st = "⚪ Idle"
            w = engine.unified_prob_system.weights
            remaining = {"COLOR","PARIDAD","RANGO"} - engine.cpr_cycle_used
            cycle_str = f"CPR pendientes: {'|'.join(sorted(remaining)) if remaining else '🔄 reiniciando'}"
            lines.append(f"<b>{name}</b>: {mode_icon} — {st} [M:{w['markov']:.2f} ML:{w['ml']:.2f}] | {cycle_str}")
        b.reply_to(message, "\n".join(lines), parse_mode="HTML")

    @b.message_handler(commands=['reset'])
    def cmd_reset(message):
        for engine in engines.values():
            engine.stats = DetailedStats()
            engine.cpr_cycle_used.clear()
            engine.bet_sys.reset()
        b.reply_to(message, "🔄 <b>Estadísticas y Martingala reseteadas</b>", parse_mode="HTML")

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)

async def main():
    global engines
    engines = {name: RouletteEngine(name, cfg) for name, cfg in ROULETTE_CONFIGS.items()}
    tasks   = [asyncio.create_task(e.run_ws()) for e in engines.values()]
    tasks.append(asyncio.create_task(self_ping_loop()))

    _register_handlers(bot)

    def _poll(b: telebot.TeleBot, label: str):
        logger.info(f"Iniciando polling Telegram — {label}")
        b.polling(none_stop=True, interval=1, timeout=30)

    threading.Thread(target=_poll, args=(bot, "Russian"), daemon=True).start()

    logger.info("🎰 Russian Roulette Bot AMX Martingala iniciado")
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    logger.info("Flask server started.")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped.")
