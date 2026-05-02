#!/usr/bin/env python3
"""
Roulette Telegram Signal Bot - Sistema AMX UNIFIED
  - Russian Roulette
  - Pre-entrenamiento con russian-azure.db (tabla russian_roulette)
  - 5 intentos por señal (D'Alembert)
  - Calentamiento WS: 21 giros silenciosos antes de emitir señales
  - Gráficos por categoría: COLOR / PARIDAD 🟣🟡 / RANGO 🟤🔵
  - Lock de categoría activa hasta resolución
  - Tras resolución: evalúa las 3 categorías y elige la de mayor probabilidad
"""

import asyncio
import io
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
DB_PATH       = "russian-azure.db"   # dump SQL histórico
LIVE_DB_PATH  = "russian_live.db"    # SQLite persistente de giros en vivo

def _get_live_db() -> "sqlite3.Connection":
    """Abre (o crea) la DB SQLite de giros en vivo."""
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
    """Retorna PAR, IMPAR, o None si es 0."""
    if number == 0:
        return None
    return "PAR" if number % 2 == 0 else "IMPAR"

def get_rango(number: int) -> Optional[str]:
    """Retorna BAJO (1-18), ALTO (19-36), o None si es 0."""
    if number == 0:
        return None
    return "BAJO" if 1 <= number <= 18 else "ALTO"

CATEGORY_ICONS = {
    "ROJO": "🔴", "NEGRO": "⚫️",
    "PAR":  "🟣", "IMPAR": "🟡",
    "BAJO": "🟤", "ALTO":  "🔵",
    "VERDE": "🟢",
}

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

# ─── ROULETTE CONFIGS ─────────────────────────────────────────────────────────
ROULETTE_CONFIGS = {
    "Russian Roulette": {
        "bot":       bot,
        "ws_key":    221,
        "chat_id":   -1003835197023,
        "thread_id": 8344,
        "db_table":  "russian_roulette",
        "min_prob_threshold": 0.60,
    },
}

WS_URL    = "wss://dga.pragmaticplaylive.net/ws"
CASINO_ID = "ppcjd00000007254"
MAX_ATTEMPTS = 5
BASE_BET  = 0.10
VISIBLE   = 50
WARMUP_SPINS = 21   # giros WS mínimos antes de emitir señales

# ─── LABOUCHÈRE ───────────────────────────────────────────────────────────────
LABOUCHERE_SEQUENCE: list[int] = [1, 2, 1]   # secuencia global por defecto

class Labouchere:
    """
    Sistema de cancelación Labouchère.
    - Apuesta = primer + último elemento de la secuencia.
    - Si solo queda un elemento, apuesta = ese elemento.
    - Victoria → elimina primero y último.
    - Derrota  → añade la apuesta al final.
    - Secuencia vacía → ciclo completado, reiniciar.
    Los valores de la secuencia son multiplicadores de `base`.
    """
    def __init__(self, sequence: list[int], base: float):
        self.base            = base
        self.original_seq    = list(sequence)
        self.sequence        = list(sequence)
        self.bankroll        = 0.0

    # ── acceso de compatibilidad ──────────────────────────────────────────────
    @property
    def step(self) -> int:
        """Número de elementos añadidos respecto a la secuencia original."""
        return max(0, len(self.sequence) - len(self.original_seq))

    def is_fresh(self) -> bool:
        """True si la secuencia está en su estado inicial (sin pérdidas pendientes)."""
        return self.sequence == self.original_seq

    def reset(self):
        self.sequence = list(self.original_seq)

    def set_sequence(self, new_seq: list[int]):
        """Cambia la secuencia base (toma efecto en el próximo ciclo si hay señal activa)."""
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
            self.reset()          # ciclo completo → reiniciar
        return bet

    def loss(self) -> float:
        bet = self.current_bet()
        self.bankroll = round(self.bankroll - bet, 2)
        # Añade el valor apostado (en unidades base) al final
        units = round(bet / self.base)
        self.sequence.append(units if units > 0 else 1)
        return bet

    def sequence_display(self) -> str:
        return " - ".join(str(v) for v in self.sequence)

# ─── MARKOV CHAIN (ventana 60 giros) ─────────────────────────────────────────
class MarkovChainPredictor:
    """
    Cadena de Markov orden 2 sobre los últimos 60 giros no-verde.
    """
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
    """
    Predictor basado en patrones de COLOR longitud 3.
    Usado para filtro Markov+ML y probabilidad unificada.
    """
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

# ─── CATEGORY PREDICTOR — 16 combinaciones por categoría (2⁴) ───────────────
class CategoryPredictor:
    """
    Tres predictores independientes (COLOR, PARIDAD, RANGO), cada uno con
    patrón de longitud 4 → 2⁴ = 16 combinaciones posibles por categoría.

    COLOR  : ROJO / NEGRO
    PARIDAD: PAR  / IMPAR
    RANGO  : ALTO / BAJO

    Elige la categoría con mayor probabilidad en el próximo valor.
    """
    PATTERN_LEN = 10  # 2¹⁰ = 1024 patrones por categoría

    def __init__(self):
        # Historial de valores por categoría
        self._hist: dict[str, list[str]] = {
            "COLOR":   [],
            "PARIDAD": [],
            "RANGO":   [],
        }
        # Tabla de conteos: counts[cat][patron_tuple][siguiente_valor]
        self._counts: dict[str, dict] = {
            "COLOR":   defaultdict(lambda: defaultdict(int)),
            "PARIDAD": defaultdict(lambda: defaultdict(int)),
            "RANGO":   defaultdict(lambda: defaultdict(int)),
        }

    def add_spin(self, number: int, real_color: str):
        """Registra un giro. Ignora el 0 (VERDE)."""
        if number == 0 or real_color == "VERDE":
            return
        par  = get_paridad(number)
        rang = get_rango(number)
        if not par or not rang:
            return
        new_vals = {"COLOR": real_color, "PARIDAD": par, "RANGO": rang}
        for cat, val in new_vals.items():
            hist = self._hist[cat]
            if len(hist) >= self.PATTERN_LEN:
                pattern = tuple(hist[-self.PATTERN_LEN:])
                self._counts[cat][pattern][val] += 1
            hist.append(val)

    def predict_category(self, category: str) -> Optional[dict]:
        """
        Distribución del próximo valor para la categoría dada.
        Retorna {valor: prob, ..., "total": n} o None si no hay datos.
        """
        hist   = self._hist[category]
        counts = self._counts[category]
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
        """
        Evalúa las 3 categorías y retorna la de mayor probabilidad.
        Retorna {category, bet_value, probability} o None.
        """
        best = None
        for cat in ("COLOR", "PARIDAD", "RANGO"):
            pred = self.predict_category(cat)
            if pred is None:
                continue
            clean = {k: v for k, v in pred.items() if k != "total"}
            if not clean:
                continue
            top_val  = max(clean, key=clean.get)
            top_prob = clean[top_val]
            if top_prob >= threshold:
                if best is None or top_prob > best["probability"]:
                    best = {"category": cat, "bet_value": top_val, "probability": top_prob}
        return best

    # ── Compatibilidad para gráficos ──────────────────────────────────────────
    @property
    def par_history(self) -> list:
        return list(self._hist["PARIDAD"])

    @property
    def rang_history(self) -> list:
        return list(self._hist["RANGO"])

# ─── SISTEMA DE PROBABILIDAD CONJUNTA PONDERADA ────────────────────────────────
class UnifiedProbabilitySystem:
    """
    Combina Markov y ML con pesos adaptativos.
    Pesos iniciales: Markov=0.35, ML=0.65 (más peso al ML).
    """
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
        sr = find_support_resistance(levels, ventana=30)
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
        # Rango: ML siempre al menos 0.4, máximo 0.8
        self.weights["markov"] = max(0.2, min(0.6, self.weights["markov"]))
        self.weights["ml"]     = max(0.4, min(0.8, self.weights["ml"]))
        total = self.weights["markov"] + self.weights["ml"]
        self.weights["markov"] /= total
        self.weights["ml"]     /= total
        logger.info(f"[AMX V22] Pesos: Markov={self.weights['markov']:.2f} ML={self.weights['ml']:.2f} | M={markov_acc:.2%} ML={ml_acc:.2%}")
        self.markov_correct = self.markov_total = self.ml_correct = self.ml_total = 0

    def get_joint_probability(self, category: str, bet_value: str,
                              markov_pred, ml_pred, cat_prob: Optional[float]) -> dict:
        """
        Probabilidad unificada Markov + ML + CategoryPredictor.
        Sin tabla predefinida: para COLOR usa Markov/ML directo (table_prob neutral 0.5).
        Para PARIDAD/RANGO usa cat_prob como señal principal.
        """
        if category == "COLOR":
            m_p  = markov_pred.get(bet_value, 0.5) if markov_pred else 0.5
            ml_p = ml_pred.get(bet_value, 0.5)     if ml_pred     else 0.5
        else:
            m_p  = cat_prob if cat_prob is not None else 0.5
            ml_p = cat_prob if cat_prob is not None else 0.5

        model_prob    = self.weights["markov"] * m_p + self.weights["ml"] * ml_p
        # Factores dinámicos (EMA trend + SR + volatilidad)
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


class AMXSignalSystem:
    def __init__(self, mode: Literal["tendencia", "moderado"] = "moderado"):
        self.mode = mode
        self.last_signal_time: float = 0
        self.cooldown_seconds: int   = 0   # ← SIN COOLDOWN
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

    def check_signal(self, positions: list, expected_color: str) -> Optional[dict]:
        """
        Evalúa condiciones EMA puras — sin tabla de color predefinida.
        Combina modo moderado y tendencia; retorna el resultado más fuerte.
        """
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

        # ── Condiciones ───────────────────────────────────────────────────────
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

        # ── Fuerza de la señal ─────────────────────────────────────────────
        score = 0
        mode  = "moderado"
        if cruce_4_20:   score += 3; mode = "tendencia"
        if cruce_8_20:   score += 2
        if sobre_3:      score += 2
        if sobre_2:      score += 1
        if patron_v:     score += 2
        if emas_alin:    score += 1
        if racha_ok:     score += 1

        if score < 2:    # umbral mínimo de condiciones
            return None

        strength = "strong" if score >= 5 else "moderate"
        return {
            "type":           "AMX_EMA",
            "mode":           mode,
            "expected_color": expected_color,
            "score":          score,
            "strength":       strength,
            "trigger_number": 0,   # se rellena por el caller
            "pattern":        ("V" if patron_v else
                               "CROSS_4_20" if cruce_4_20 else
                               "CROSS_8_20" if cruce_8_20 else "EMA"),
        }

    # Aliases de compatibilidad con código existente
    def check_signal_tendencia(self, positions, color_data, current_number,
                               expected_color, prob_threshold) -> Optional[dict]:
        sig = self.check_signal(positions, expected_color)
        if sig and sig["score"] >= 4:
            sig["trigger_number"] = current_number
            return sig
        return None

    def check_signal_moderado(self, positions, color_data, current_number,
                              expected_color, prob_threshold) -> Optional[dict]:
        sig = self.check_signal(positions, expected_color)
        if sig:
            sig["trigger_number"] = current_number
            return sig
        return None

    def register_signal_sent(self):
        self.last_signal_time = time.time()

    def register_so_failed(self):
        self.so_cooldown = time.time()

# ─── SOPORTE Y RESISTENCIA ────────────────────────────────────────────────────
def find_support_resistance(levels: list,
                            tolerancia: float = 0.5,
                            min_rebotes: int = 2,
                            ventana: int = 5,
                            max_niveles: int = 3) -> dict:
    """
    Detecta múltiples soportes y resistencias usando mínimos/máximos locales.
    Algoritmo traducido del JS (index_v3.html · detectarSoportesResistencias).

    - Ventana deslizante de (2*ventana+1) puntos.
    - Soporte: punto es mínimo local en su ventana y < punto anterior y siguiente.
    - Resistencia: punto es máximo local en su ventana y > punto anterior y siguiente.
    - Agrupa niveles dentro de `tolerancia` y cuenta rebotes.
    - Solo válidos con ≥ min_rebotes rebotes.
    - Marca como fuerte si ≥3 rebotes o fuerza_total ≥4.
    - Retorna los `max_niveles` más recientes de cada tipo.
    """
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
    sr = find_support_resistance(levels, ventana=30)
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

# ─── CATEGORY CHART GENERATION (PARIDAD / RANGO) ─────────────────────────────
def generate_category_chart(
    category: str,
    bet_value: str,
    cat_history: list,         # lista de strings (e.g. ["PAR","IMPAR",...])
    spin_history: list,        # para mostrar los números en el eje X
    unified_prob: Optional[dict] = None,
    visible: int = VISIBLE,
) -> io.BytesIO:
    """
    Genera un gráfico de nivel acumulado para PARIDAD o RANGO.
      - PARIDAD: +1 por PAR, -1 por IMPAR
      - RANGO:   +1 por ALTO, -1 por BAJO
    Los dots se colorean según el valor de la categoría.
    """
    # ── Parámetros de categoría ───────────────────────────────────────────────
    if category == "PARIDAD":
        pos_val, neg_val   = "PAR", "IMPAR"
        pos_color, neg_color = "#a855f7", "#eab308"   # morado / amarillo
        bet_icon = CATEGORY_ICONS.get(bet_value, "")
        title_color = "#c084fc" if bet_value == "PAR" else "#fde047"
    else:  # RANGO
        pos_val, neg_val   = "ALTO", "BAJO"
        pos_color, neg_color = "#3b82f6", "#92400e"   # azul / marrón
        bet_icon = CATEGORY_ICONS.get(bet_value, "")
        title_color = "#60a5fa" if bet_value == "ALTO" else "#d97706"

    # ── Construir niveles acumulados ──────────────────────────────────────────
    levels = []
    acc = 0
    for v in cat_history:
        if v == pos_val:
            acc += 1
        elif v == neg_val:
            acc -= 1
        levels.append(acc)

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

    # Números de giro visibles (para eje X)
    hist_sl = spin_history[-(len(arr[sl])):]

    visible_levels = arr[sl]
    if len(visible_levels) == 0:
        # Sin datos: imagen en blanco con mensaje
        fig, ax = plt.subplots(figsize=(8, 3.8), facecolor="#0b101f")
        ax.text(0.5, 0.5, "Sin datos suficientes", color="white",
                ha="center", va="center", transform=ax.transAxes, fontsize=14)
        buf = io.BytesIO()
        fig.savefig(buf, format="png", dpi=120, facecolor="#0b101f")
        plt.close(fig)
        buf.seek(0)
        return buf

    # ── Y limits ─────────────────────────────────────────────────────────────
    lookback = min(50, len(arr))
    recent   = arr[-lookback:]
    mn, mx   = np.min(recent), np.max(recent)
    rng      = max(mx - mn, 1.0)
    margin   = rng * 0.18
    last_lv  = visible_levels[-1]
    y_min = mn - margin
    y_max = mx + margin
    lp = (last_lv - y_min) / (y_max - y_min) if (y_max - y_min) > 0 else 0.5
    if lp < 0.2:  y_min = last_lv - (y_max - y_min) * 0.2
    if lp > 0.8:  y_max = last_lv + (y_max - y_min) * 0.2

    # ── Colores de la línea según bet_value ───────────────────────────────────
    line_c   = pos_color if bet_value == pos_val else neg_color
    bg, ax_bg, grid_c = "#0b101f", "#0f1a2a", "#1e2e48"
    ema4_c, ema8_c, ema20_c = "#ff9f43", "#48dbfb", "#1dd1a1"

    fig, ax = plt.subplots(figsize=(8, 3.8), facecolor=bg)
    ax.set_facecolor(ax_bg)

    y   = arr[sl]
    e4  = ema4[sl]
    e8  = ema8[sl]
    e20 = ema20[sl]

    ax.fill_between(x, y, alpha=0.10, color=line_c)
    ax.plot(x, y,  color=line_c,  linewidth=0.8, zorder=3)
    ax.plot(x, e4, color=ema4_c,  linewidth=0.7, linestyle="--", label="EMA 4",  zorder=4)
    ax.plot(x, e8, color=ema8_c,  linewidth=0.7, linestyle="--", label="EMA 8",  zorder=4)
    ax.plot(x, e20,color=ema20_c, linewidth=1.0,                 label="EMA 20", zorder=4)
    ax.set_ylim(y_min, y_max)

    # Dots por valor de categoría
    dot_map = {pos_val: pos_color, neg_val: neg_color}
    cat_slice = cat_history[start:]
    for i, val in enumerate(cat_slice):
        c = dot_map.get(val, "#ffffff")
        if i < len(y):
            ax.scatter(i, y[i], color=c, s=22, zorder=5,
                       edgecolors="white", linewidths=0.3)

    # Soporte / Resistencia
    sr = find_support_resistance(list(arr), ventana=30)
    sup_v, res_v = sr['support'], sr['resistance']
    res_color = line_c
    sup_color = neg_color if bet_value == pos_val else pos_color
    if sup_v is not None:
        ax.axhline(y=sup_v, color=sup_color, linestyle='--', linewidth=1.5, alpha=0.7)
        ax.text(x[-1], sup_v, f' S {sup_v:.1f}', color=sup_color,
                fontsize=7, va='bottom', ha='right')
    if res_v is not None:
        ax.axhline(y=res_v, color=res_color, linestyle='--', linewidth=1.5, alpha=0.7)
        ax.text(x[-1], res_v, f' R {res_v:.1f}', color=res_color,
                fontsize=7, va='top', ha='right')

    # Eje X con números de giro
    tick_step = max(1, len(x) // 8)
    tick_x    = list(range(0, len(x), tick_step))
    tick_lbs  = [str(hist_sl[i]["number"]) if i < len(hist_sl) else "" for i in tick_x]
    ax.set_xticks(tick_x); ax.set_xticklabels(tick_lbs, color="#8899bb", fontsize=7)
    ax.tick_params(axis='y', colors="#8899bb", labelsize=7)
    ax.tick_params(axis='x', colors="#8899bb", labelsize=7)
    ax.spines['bottom'].set_color(grid_c); ax.spines['left'].set_color(grid_c)
    ax.spines['top'].set_visible(False);   ax.spines['right'].set_visible(False)
    ax.grid(axis='y', color=grid_c, linewidth=0.4, alpha=0.5)

    # Título
    pred_info = ""
    if unified_prob:
        pred_info += f" | Unif:{unified_prob['combined_prob']*100:.0f}%"
        pred_info += f" | ML:{unified_prob['ml_prob']*100:.0f}%"
    ax.set_title(
        f"{bet_icon} {category}: {bet_value} — últimos {visible} giros · EMA 4/8/20{pred_info}",
        color=title_color, fontsize=8.5, pad=6
    )

    # Leyenda
    from matplotlib.lines import Line2D
    legend_els = [
        Line2D([0],[0], color=line_c,  linewidth=0.8, label="Nivel"),
        Line2D([0],[0], color=ema4_c,  linewidth=0.7, linestyle="--", label="EMA 4"),
        Line2D([0],[0], color=ema8_c,  linewidth=0.7, linestyle="--", label="EMA 8"),
        Line2D([0],[0], color=ema20_c, linewidth=1.0,                 label="EMA 20"),
        Line2D([0],[0], marker='o', color='w', markerfacecolor=pos_color,
               markersize=5, label=pos_val),
        Line2D([0],[0], marker='o', color='w', markerfacecolor=neg_color,
               markersize=5, label=neg_val),
    ]
    if sup_v is not None:
        legend_els.append(Line2D([0],[0], color=sup_color, linestyle='--',
                                  linewidth=1.5, label='Soporte'))
    if res_v is not None:
        legend_els.append(Line2D([0],[0], color=res_color, linestyle='--',
                                  linewidth=1.5, label='Resistencia'))
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
_TG_MAX_RETRIES = 12   # backoff hasta 60s

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
        self.last_stats_at:  int = 0
        self.batch_start_bankroll: Optional[float] = None
        self.batch_start_wins:  int = 0
        self.batch_start_losses:int = 0
        self.batch_start_w1: int = 0
        self.batch_start_w2: int = 0
        self.batch_start_w3: int = 0
        self.batch_start_w4: int = 0
        self.batch_start_w5: int = 0
        # Diario
        self.last_daily_date      = ""
        self.daily_start_bankroll: Optional[float] = None
        self.daily_signals:  int = 0
        self.daily_wins:     int = 0
        self.daily_losses:   int = 0
        self.daily_w1: int = 0
        self.daily_w2: int = 0
        self.daily_w3: int = 0
        self.daily_w4: int = 0
        self.daily_w5: int = 0

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
            elif attempt_won == 4: self.wins_attempt_4 += 1
            elif attempt_won == 5: self.wins_attempt_5 += 1
        else:
            self.losses += 1
        # Diario
        self.daily_signals += 1
        if final_result:
            self.daily_wins += 1
            if   attempt_won == 1: self.daily_w1 += 1
            elif attempt_won == 2: self.daily_w2 += 1
            elif attempt_won == 3: self.daily_w3 += 1
            elif attempt_won == 4: self.daily_w4 += 1
            elif attempt_won == 5: self.daily_w5 += 1
        else:
            self.daily_losses += 1
        if self.daily_start_bankroll is None:
            self.daily_start_bankroll = bankroll

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
        w  = w1+w2+w3+w4+w5
        bk = round(current_bankroll - self.batch_start_bankroll, 2)              if self.batch_start_bankroll is not None else 0.0
        return {"total":n,"wins":w,"losses":l,"w1":w1,"w2":w2,"w3":w3,"w4":w4,"w5":w5,
                "efficiency":round(w/n*100,1) if n else 0.0,
                "e_w1":round(w1/n*100,2) if n else 0.0,
                "e_w2":round(w2/n*100,2) if n else 0.0,
                "e_w3":round(w3/n*100,2) if n else 0.0,
                "e_w4":round(w4/n*100,2) if n else 0.0,
                "e_w5":round(w5/n*100,2) if n else 0.0,
                "e_loss":round(l/n*100,2) if n else 0.0,
                "bankroll_delta":bk}

    def get_daily_stats(self, current_bankroll: float) -> dict:
        t  = self.daily_signals
        w  = self.daily_wins
        l  = self.daily_losses
        bk = round(current_bankroll - self.daily_start_bankroll, 2)              if self.daily_start_bankroll is not None else 0.0
        return {"total":t,"wins":w,"losses":l,
                "w1":self.daily_w1,"w2":self.daily_w2,"w3":self.daily_w3,
                "w4":self.daily_w4,"w5":self.daily_w5,
                "efficiency":round(w/t*100,1) if t else 0.0,
                "e_w1":round(self.daily_w1/t*100,2) if t else 0.0,
                "e_w2":round(self.daily_w2/t*100,2) if t else 0.0,
                "e_w3":round(self.daily_w3/t*100,2) if t else 0.0,
                "e_w4":round(self.daily_w4/t*100,2) if t else 0.0,
                "e_w5":round(self.daily_w5/t*100,2) if t else 0.0,
                "e_loss":round(l/t*100,2) if t else 0.0,
                "bankroll_delta":bk}

    def reset_daily(self, date_str: str, current_bankroll: float):
        self.last_daily_date      = date_str
        self.daily_start_bankroll = current_bankroll
        self.daily_signals  = 0
        self.daily_wins     = 0
        self.daily_losses   = 0
        self.daily_w1 = self.daily_w2 = self.daily_w3 = 0
        self.daily_w4 = self.daily_w5 = 0

    def reset(self):
        self.signal_history.clear()
        self.wins_attempt_1 = self.wins_attempt_2 = self.wins_attempt_3 = 0
        self.wins_attempt_4 = self.wins_attempt_5 = 0
        self.losses = self.total_signals = self.last_stats_at = 0
        self.batch_start_bankroll = None
        self.reset_daily("", 0.0)

class RouletteEngine:
    def __init__(self, name: str, cfg: dict):
        self.name       = name
        self.bot        = cfg["bot"]          # instancia TeleBot propia
        self.ws_key     = cfg["ws_key"]
        self.chat_id    = cfg["chat_id"]
        self.thread_id  = cfg["thread_id"]
        self.db_table   = cfg["db_table"]     # tabla de pre-entrenamiento

        self.spin_history:      list = []
        # Niveles acumulados por categoría (para EMA independiente)
        # COLOR
        self.original_levels:   list = []   # +1 ROJO,  -1 NEGRO
        self.inverted_levels:   list = []   # +1 NEGRO, -1 ROJO
        # PARIDAD
        self.par_levels:        list = []   # +1 PAR,   -1 IMPAR
        self.impar_levels:      list = []   # +1 IMPAR, -1 PAR
        # RANGO
        self.alto_levels:       list = []   # +1 ALTO,  -1 BAJO
        self.bajo_levels:       list = []   # +1 BAJO,  -1 ALTO
        self.last_nonzero_color: Optional[str] = None
        self.anti_block: set  = set()

        # ── Estado de la señal ─────────────────────────────────
        self.signal_active:          bool = False
        self.waiting_for_attempt:    bool = False
        self.waiting_attempt_number: int  = 0
        self.skip_one_after_zero:    bool = False

        # Categoría activa: "COLOR", "PARIDAD", "RANGO" o None
        self.active_category:  Optional[str] = None
        self.bet_value:        Optional[str] = None   # ROJO/NEGRO/PAR/IMPAR/BAJO/ALTO
        self.bet_color:        Optional[str] = None   # para gráfico (solo COLOR)
        self.attempts_left:    int  = 0
        self.total_attempts:   int  = 0
        self.trigger_number:   Optional[int] = None

        self.signal_msg_ids: list = []
        self.waiting_msg_id: Optional[int] = None
        self.result_sequence: deque = deque(maxlen=10)

        # ── D'Alembert ────────────────────────────────────────
        self.bet_sys = Labouchere(LABOUCHERE_SEQUENCE, BASE_BET)

        # ── Recuperación ──────────────────────────────────────
        self.consec_losses:   int   = 0
        self.recovery_active: bool  = False
        self.recovery_target: float = 0.0
        self.level1_bankroll: float = 0.0
        self.signal_is_level1: bool = False

        # ── AMX V22 ───────────────────────────────────────────
        self.amx_system = AMXSignalSystem(mode="moderado")
        self.min_prob_threshold = cfg.get("min_prob_threshold", 0.60)

        # ── Probabilidad Unificada ─────────────────────────────
        self.unified_prob_system = UnifiedProbabilitySystem()

        # ── Predictores ───────────────────────────────────────
        self.markov       = MarkovChainPredictor(window=60, order=2)
        self.ml_predictor = MLPatternPredictor(pattern_length=3)
        self.category_ml  = CategoryPredictor()

        # ── Estadísticas ──────────────────────────────────────
        self.stats = DetailedStats()

        self.ws      = None
        self.running = True

        # ── Persistencia live ──────────────────────────────────
        self._live_conn = _get_live_db()   # conexión SQLite compartida

        # ── Pre-entrenamiento: dump histórico + giros live guardados ──────────
        self._pretrain_from_db(DB_PATH, self.db_table)
        live_loaded = self._load_live_history()

        # ── Calentamiento WebSocket ────────────────────────────
        # Si ya cargamos suficientes giros live, no necesitamos esperar
        self.ws_spins_count: int  = live_loaded
        self.warmup_done:    bool = live_loaded >= WARMUP_SPINS

    # ─── PRE-ENTRENAMIENTO DESDE DB ──────────────────────────────────────────
    def _pretrain_from_db(self, db_path: str, table_name: str):
        """
        Carga el historial de giros desde el dump SQL y entrena
        Markov, MLPatternPredictor y CategoryMLPredictor.
        """
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
        """
        Carga los giros en vivo guardados en SQLite desde el último reinicio.
        Entrena todos los predictores con esos datos y devuelve la cantidad cargada.
        Los giros más antiguos de 7 días se descartan automáticamente.
        """
        import time as _time
        try:
            cutoff = int(_time.time()) - 7 * 86400   # últimos 7 días
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
            # Niveles acumulados para las 3 categorías
            last_o  = self.original_levels[-1] if self.original_levels else 0
            last_i  = self.inverted_levels[-1]  if self.inverted_levels  else 0
            last_p  = self.par_levels[-1]        if self.par_levels        else 0
            last_ip = self.impar_levels[-1]      if self.impar_levels      else 0
            last_a  = self.alto_levels[-1]        if self.alto_levels        else 0
            last_b  = self.bajo_levels[-1]        if self.bajo_levels        else 0
            if n == 0:
                self.original_levels.append(last_o); self.inverted_levels.append(last_i)
                self.par_levels.append(last_p);      self.impar_levels.append(last_ip)
                self.alto_levels.append(last_a);     self.bajo_levels.append(last_b)
            else:
                par  = get_paridad(n)
                rang = get_rango(n)
                self.original_levels.append(last_o  + (1 if real == "ROJO"   else -1))
                self.inverted_levels.append(last_i  + (1 if real == "NEGRO"  else -1))
                self.par_levels.append(last_p       + (1 if par  == "PAR"    else -1))
                self.impar_levels.append(last_ip    + (1 if par  == "IMPAR"  else -1))
                self.alto_levels.append(last_a      + (1 if rang == "ALTO"   else -1))
                self.bajo_levels.append(last_b      + (1 if rang == "BAJO"   else -1))
            self.spin_history.append({"number": n, "real": real})
            if len(self.spin_history) > 300:
                self.spin_history.pop(0)

        logger.info(f"[{self.name}] ✅ Historial ML cargado: {len(rows)} giros")
        return len(rows)

    def _persist_spin(self, number: int):
        """
        Guarda el giro en SQLite — núcleo de la continuidad 24/7.
        Si la conexión falla, la reabre automáticamente.
        """
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
        """Limpia registros de más de 7 días para que la DB no crezca indefinidamente."""
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
        return None  # color_data eliminada — señales basadas en EMA+ML

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

    def _trigger_display(self, number: int, category: str) -> str:
        """Muestra número + valor de la misma categoría que se apuesta."""
        if number == 0:
            return "0 VERDE 🟢"
        if category == "COLOR":
            val = REAL_COLOR_MAP.get(number, "VERDE")
        elif category == "PARIDAD":
            val = get_paridad(number) or "VERDE"
        else:  # RANGO
            val = get_rango(number) or "VERDE"
        return f"{number} {val} {self._category_icon(val)}"

    def _is_win(self, number: int, real_color: str) -> Optional[bool]:
        """
        True=ganó, False=perdió, None=verde (neutral para paridad/rango).
        """
        if number == 0:
            return None  # verde → esperar
        if self.active_category == "COLOR":
            return real_color == self.bet_value
        elif self.active_category == "PARIDAD":
            return get_paridad(number) == self.bet_value
        else:  # RANGO
            return get_rango(number) == self.bet_value

    # ─── DETECCIÓN DE SEÑAL POR CATEGORÍA ────────────────────────────────────

    # ── Mapas de niveles por categoría y valor ──────────────────────────────────
    def _levels_for(self, category: str, bet_value: str) -> list:
        """Retorna el array de niveles acumulados para categoría+valor."""
        return {
            ("COLOR",   "ROJO"):  self.original_levels,
            ("COLOR",   "NEGRO"): self.inverted_levels,
            ("PARIDAD", "PAR"):   self.par_levels,
            ("PARIDAD", "IMPAR"): self.impar_levels,
            ("RANGO",   "ALTO"):  self.alto_levels,
            ("RANGO",   "BAJO"):  self.bajo_levels,
        }.get((category, bet_value), [])

    def _evaluate_category(self, category: str) -> Optional[dict]:
        """
        Sistema unificado de detección para COLOR, PARIDAD y RANGO.
        Misma lógica para las 3 categorías:
          1. CategoryPredictor → valor con mayor probabilidad ≥ min_prob_threshold
          2. EMA 4/8/20 sobre nivel acumulado confirma la señal (AMXSignalSystem)
          3. Probabilidad unificada Markov+ML ≥ min_prob_threshold
        Retorna {category, bet_value, probability, trigger_number, ema_score} o None.
        """
        trigger = self.spin_history[-1]["number"] if self.spin_history else 0

        # Paso 1 — CategoryPredictor
        pred = self.category_ml.predict_category(category)
        if pred is None or pred.get("total", 0) < 5:
            return None
        clean     = {k: v for k, v in pred.items() if k != "total"}
        if not clean:
            return None
        best_val  = max(clean, key=clean.get)
        cat_prob  = clean[best_val]
        if cat_prob < self.min_prob_threshold:
            return None

        # Paso 2 — EMA confirma la señal
        levels  = self._levels_for(category, best_val)
        ema_sig = self.amx_system.check_signal(levels, best_val)
        if ema_sig is None:
            return None
        ema_sig["trigger_number"] = trigger

        # Paso 3 — Probabilidad unificada Markov+ML
        markov_pred = self.markov.predict(self.spin_history)
        ml_pred     = self.ml_predictor.predict(self.spin_history)
        unified     = self.unified_prob_system.get_joint_probability(
            category, best_val, markov_pred, ml_pred, cat_prob)
        # Bonus EMA fuerte
        ema_bonus  = 0.03 if ema_sig.get("strength") == "strong" else 0.0
        final_prob = min(0.95, unified["combined_prob"] + ema_bonus)

        if final_prob < self.min_prob_threshold:
            return None

        return {
            "category":       category,
            "bet_value":      best_val,
            "probability":    final_prob,
            "trigger_number": trigger,
            "ema_score":      ema_sig.get("score", 0),
            "ema_pattern":    ema_sig.get("pattern", ""),
        }

    def _evaluate_ml_category(self, category: str) -> Optional[dict]:
        """Alias de compatibilidad → usa _evaluate_category."""
        return self._evaluate_category(category)

    def _evaluate_color_candidate(self) -> Optional[dict]:
        """Alias de compatibilidad → usa _evaluate_category("COLOR")."""
        return self._evaluate_category("COLOR")

    def _detect_best_category_signal(self) -> Optional[dict]:
        """
        Evalúa las 3 categorías con el sistema unificado (_evaluate_category).
        Cada categoría usa: CategoryPredictor → EMA 4/8/20 → prob unificada.
        Retorna la categoría con mayor probabilidad final.
        """
        candidates = []
        for cat in ("COLOR", "PARIDAD", "RANGO"):
            cand = self._evaluate_category(cat)
            if cand:
                candidates.append(cand)
        if not candidates:
            return None
        return max(candidates, key=lambda x: x["probability"])

    def _detect_best_in_category(self, category: str) -> Optional[dict]:
        """Reintento: re-evalúa con el sistema unificado para la categoría activa."""
        return self._evaluate_category(category)

    # ─── FILTROS ─────────────────────────────────────────────────────────────
    def _get_predictor_votes(self, color: str) -> int:
        votes = 0
        mp = self.markov.predict(self.spin_history)
        if mp and mp.get(color, 0) > 0.50: votes += 1
        ml = self.ml_predictor.predict(self.spin_history)
        if ml and ml.get(color, 0) > 0.50: votes += 1
        return votes

    def _passes_markov_ml_filter(self, color: str) -> bool:
        mp = self.markov.predict(self.spin_history)
        ml = self.ml_predictor.predict(self.spin_history)
        if mp is not None and mp.get(color, 0) < 0.50:
            logger.info(f"[{self.name}] Bloqueada: Markov {mp.get(color,0)*100:.0f}% < 50%")
            return False
        if ml is not None and ml.get(color, 0) < 0.50:
            logger.info(f"[{self.name}] Bloqueada: ML {ml.get(color,0)*100:.0f}% < 50%")
            return False
        return True

    # ─── REINTENTO ────────────────────────────────────────────────────────────
    def _best_retry_value(self, trigger_number: int) -> Optional[str]:
        """
        Evalúa si hay condiciones para continuar en la categoría activa.
        Retorna el bet_value a usar, o None.
        """
        if self.active_category == "COLOR":
            return self._best_retry_color(trigger_number)
        # PARIDAD / RANGO → desagregar desde combo predictor
        pred = self.category_ml.predict_category(self.active_category)
        if pred is None or pred.get("total", 0) < 3:
            return self.bet_value  # sin datos suficientes, mantener
        pred_clean = {k: v for k, v in pred.items() if k != "total"}
        best_val   = max(pred_clean, key=pred_clean.get)
        best_prob  = pred_clean[best_val]
        if best_prob >= self.min_prob_threshold:
            return best_val
        return None

    def _check_retry_conditions(self, color: str, trigger_number: int) -> bool:
        entry = self.get_entry(trigger_number)
        if not entry or entry["senal"] == "NO APOSTAR": return False
        if entry["senal"] != color: return False
        prob = entry["rojo"] if color == "ROJO" else entry["negro"]
        if prob < self.min_prob_threshold: return False
        levels = self.original_levels if color == "ROJO" else self.inverted_levels
        if len(levels) < 20: return False
        ema20 = self.calculate_ema(levels, 20)
        li = len(levels) - 1
        if ema20[li] is None or levels[li] <= ema20[li]: return False
        opp = self._opposite_color(color)
        mp = self.markov.predict(self.spin_history)
        ml = self.ml_predictor.predict(self.spin_history)
        if mp and ml and mp.get(opp, 0) > 0.65 and ml.get(opp, 0) > 0.65:
            return False
        return True

    def _best_retry_color(self, trigger_number: int) -> Optional[str]:
        same_ok = self._check_retry_conditions(self.bet_value, trigger_number)
        opp     = self._opposite_color(self.bet_value)
        opp_ok  = self._check_retry_conditions(opp, trigger_number)
        if same_ok and opp_ok:
            chosen = self.bet_value if self._get_predictor_votes(self.bet_value) >= self._get_predictor_votes(opp) else opp
            return chosen
        if same_ok: return self.bet_value
        if opp_ok:  return opp
        return None

    # ─── DETECCIÓN AMX / SHOULD_ACTIVATE ─────────────────────────────────────
    def _detect_amx_signal(self) -> Optional[dict]:
        """Alias de compatibilidad → usa _evaluate_category("COLOR")."""
        return self._evaluate_category("COLOR")


    def should_activate(self) -> Optional[str]:
        losses   = self.consec_losses
        min_spin = 22 + losses * 2
        if len(self.spin_history) < min_spin: return None
        last_num = self.spin_history[-1]["number"]
        entry = self.get_entry(last_num)
        if not entry or entry["senal"] == "NO APOSTAR": return None
        expected = entry["senal"]
        if len(self.original_levels) < 20 or len(self.inverted_levels) < 20: return None
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
                if i < 0 or i >= len(levels) or i >= len(e20): return False
                if e20[i] is None or levels[i] <= e20[i]: return False
                if losses >= 2:
                    if i >= len(e8) or e8[i] is None or levels[i] <= e8[i]: return False
                if losses >= 4:
                    if i >= len(e4) or e4[i] is None or levels[i] <= e4[i]: return False
            return True
        if expected == "ROJO"  and check(self.original_levels, ema20o, ema8o, ema4o, li): return "ROJO"
        if expected == "NEGRO" and check(self.inverted_levels, ema20i, ema8i, ema4i, li): return "NEGRO"
        return None

    def determine_bet_color(self, expected: str) -> str:
        if len(self.spin_history) < 20: return expected
        ema20o = self.calculate_ema(self.original_levels, 20)
        ema20i = self.calculate_ema(self.inverted_levels, 20)
        li = len(self.original_levels) - 1
        if li < 0 or li >= len(ema20o) or li >= len(ema20i): return expected
        if ema20o[li] is None or ema20i[li] is None: return expected
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
        if not self.recovery_active: return
        if self.bet_sys.bankroll >= self.recovery_target:
            logger.info(f"[{self.name}] Recuperación completada!")
            self.consec_losses = 0
            self.recovery_active = False
            self.recovery_target = 0.0
            self.bet_sys.reset()

    # ─── AMX POSITIONS ────────────────────────────────────────────────────────
    def _update_amx_positions(self, color: str):
        last_pos = self.amx_system.ultimos_puntos[-1] if self.amx_system.ultimos_puntos else 0
        if color == "ROJO":      new_pos = last_pos + 1
        elif color == "NEGRO":   new_pos = last_pos - 1
        else:                    new_pos = last_pos
        self.amx_system.ultimos_puntos.append(new_pos)
        if len(self.amx_system.ultimos_puntos) > 300:
            self.amx_system.ultimos_puntos = self.amx_system.ultimos_puntos[-200:]

    # ─── PROBABILIDAD UNIFICADA ───────────────────────────────────────────────
    def _update_unified_system(self, color: str):
        levels = self.original_levels if color == "ROJO" else self.inverted_levels
        self.unified_prob_system.calculate_volatility(levels)
        self.unified_prob_system.update_trend_factors(levels)
        self.unified_prob_system.update_weights()

    def _get_unified_probability(self, color: str, trigger_number: int) -> dict:
        """Sin tabla: usa solo Markov/ML para COLOR."""
        markov_pred = self.markov.predict(self.spin_history)
        ml_pred     = self.ml_predictor.predict(self.spin_history)
        return self.unified_prob_system.get_joint_probability(
            "COLOR", color, markov_pred, ml_pred, None)

    def _get_category_probability(self, category: str, bet_value: str,
                                  trigger_number: int) -> dict:
        """Probabilidad unificada según categoría."""
        if category == "COLOR":
            return self._get_unified_probability(bet_value, trigger_number)
        # Para paridad/rango/color vía combo predictor
        pred = self.category_ml.predict_category(category)
        prob = pred.get(bet_value, 0.5) if pred else 0.5
        return {
            "combined_prob": prob, "markov_prob": 0.5, "ml_prob": prob,
            "confidence": 0.6,
            "threshold": self.min_prob_threshold, "signal_strength": "moderate",
            "weights": self.unified_prob_system.weights.copy(),
            "ema_trend_factor": 1.0, "sr_factor": 1.0, "volatility": 1.0,
        }

    def _record_prediction_result(self, color: str, actual: str):
        markov_pred = self.markov.predict(self.spin_history)
        ml_pred     = self.ml_predictor.predict(self.spin_history)
        self.unified_prob_system.record_prediction(color, markov_pred, ml_pred, actual)

    # ─── ENVÍO DE MENSAJES ────────────────────────────────────────────────────
    def _format_sequence(self, spin_history: list) -> str:
        emojis = {"ROJO": "🔴", "NEGRO": "⚫️", "VERDE": "🟢"}
        recent = spin_history[-10:] if len(spin_history) >= 10 else spin_history
        return " --> ".join(emojis.get(s["real"], "❓") for s in recent)

    def _build_caption(self, attempt: int, unified_prob: Optional[dict]) -> str:
        """Construye el caption de señal — formato unificado."""
        bet      = self.bet_sys.current_bet()
        prob_pct = int((unified_prob["combined_prob"] if unified_prob else 0.5) * 100)
        val_icon = self._category_icon(self.bet_value)
        trig_disp = self._trigger_display(self.trigger_number, self.active_category)
        return (
            f"☑️☑️ <b>SEÑAL CONFIRMADA</b> ☑️☑️\n\n"
            f"🎰 Juego: {self.name}\n\n"
            f"👉 Después de: {trig_disp}\n"
            f"🎯 Apostar a: <b>{self.bet_value}</b> {val_icon}\n"
            f"🤖 Probabilidad Unificada: {prob_pct}%\n"
            f"🎲 Labouchère: [{self.bet_sys.sequence_display()}]\n"
            f"📍 Apuesta: {bet:.2f} usd\n\n"
            f"♻️ Intento {attempt}/{MAX_ATTEMPTS}"
        )

    def _chart_color(self) -> str:
        """Color para el gráfico: ROJO si es COLOR/BAJO/PAR, NEGRO otherwise."""
        if self.active_category == "COLOR":
            return self.bet_value if self.bet_value in ("ROJO", "NEGRO") else "ROJO"
        return "ROJO"  # default para paridad/rango

    def _cat_val(self, number: int, real: str) -> tuple[str, str]:
        """Retorna (valor_categoría, icono) del número salido según categoría activa."""
        cat = self.active_category or "COLOR"
        if cat == "COLOR":
            val = real
        elif cat == "PARIDAD":
            val = get_paridad(number) or "VERDE"
        else:
            val = get_rango(number) or "VERDE"
        return val, self._category_icon(val)

    def _build_signal_text(self, attempt: int, unified_prob: Optional[dict]) -> str:
        """Texto del mensaje de señal activa (se reemplaza en cada intento)."""
        bet      = self.bet_sys.current_bet()
        prob_pct = int((unified_prob["combined_prob"] if unified_prob else 0.5) * 100)
        val_icon = self._category_icon(self.bet_value or "")
        trig_disp = self._trigger_display(self.trigger_number, self.active_category)
        return (
            f"☑️☑️ <b>SEÑAL CONFIRMADA</b> ☑️☑️\n\n"
            f"🎰 Juego: {self.name}\n\n"
            f"👉 Después de: {trig_disp}\n"
            f"🎯 Apostar a: <b>{self.bet_value}</b> {val_icon}\n"
            f"🤖 Probabilidad Unificada: {prob_pct}%\n"
            f"🎲 Labouchère: [{self.bet_sys.sequence_display()}]\n"
            f"📍 Apuesta: {bet:.2f} usd\n\n"
            f"♻️ Intento {attempt}/{MAX_ATTEMPTS}"
        )

    def _send_signal(self, attempt: int, unified_prob: Optional[dict] = None):
        """
        Envía (o reemplaza) el mensaje de señal activa.
        Borra el mensaje anterior del mismo ciclo antes de enviar el nuevo.
        """
        self.signal_is_level1 = (self.bet_sys.is_fresh() and not self.recovery_active)
        if self.signal_is_level1:
            self.level1_bankroll = self.bet_sys.bankroll

        # Borrar mensaje de señal anterior si existe
        if self.signal_msg_ids:
            for mid in self.signal_msg_ids:
                tg_delete(self.bot, self.chat_id, mid)
            self.signal_msg_ids = []

        text   = self._build_signal_text(attempt, unified_prob)
        msg_id = tg_send_text(self.bot, self.chat_id, self.thread_id, text)
        if msg_id:
            self.signal_msg_ids.append(msg_id)

        prob_pct = int((unified_prob["combined_prob"] if unified_prob else 0.5) * 100)
        logger.info(
            f"[{self.name}] 🎯 [{self.active_category}] {self.bet_value} "
            f"intento={attempt} trig={self.trigger_number} prob={prob_pct}%"
        )

    def _send_waiting_message(self, attempt_number: int):
        """Borra señal anterior y envía mensaje de espera mientras analiza el próximo giro."""
        if self.signal_msg_ids:
            for mid in self.signal_msg_ids:
                tg_delete(self.bot, self.chat_id, mid)
            self.signal_msg_ids = []
        logger.info(f"[{self.name}] ⏳ Esperando condiciones intento {attempt_number}")

    def _send_result(self, number: int, real: str, won: bool, bet: float,
                     attempt_won: int, delete_signals: bool = True):
        """
        Al resolver el ciclo:
          - Borra el mensaje de señal activa.
          - Envía el resultado final con el valor de la categoría apostada.
        """
        # Borrar señal activa
        if delete_signals and self.signal_msg_ids:
            for mid in self.signal_msg_ids:
                tg_delete(self.bot, self.chat_id, mid)
            self.signal_msg_ids = []

        bankroll         = self.bet_sys.bankroll
        cat_val, cat_icon = self._cat_val(number, real)
        bet_icon          = self._category_icon(self.bet_value or "")
        status            = "✅ Acierto" if won else "❌ Fallo"

        text = (
            f"{status}\n\n"
            f"🎯 Aposté a: <b>{self.bet_value}</b> {bet_icon}\n"
            f"🔢 Salió: {number} <b>{cat_val}</b> {cat_icon}\n"
            f"💰 Bankroll: {bankroll:.2f} usd"
        )
        tg_send_text(self.bot, self.chat_id, self.thread_id, text)
        logger.info(f"[{self.name}] {'WIN' if won else 'LOSS'} #{number} "
                    f"cat_val={cat_val} intento={attempt_won} bankroll={bankroll:.2f}")

    def _check_stats(self):
        """Cada 20 señales envía: estadísticas del lote + estadísticas acumuladas del día (24h)."""
        if not self.stats.should_send_stats(): return
        current_bankroll = self.bet_sys.bankroll
        s20 = self.stats.get_batch_stats(current_bankroll)
        s24 = self.stats.get_daily_stats(current_bankroll)
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
        if s24 and s24.get("total", 0) > 0:
            stats_text += (
                f"👉🏼 <b>ESTADISTICAS 24 HORAS</b>\n"
                f"🈯️ <b>T:</b> {s24['total']} 📈 <b>E:</b> {s24['efficiency']}%\n"
                f"1️⃣ <b>W:</b> {s24['w1']} --> <b>E:</b> {s24['e_w1']}%\n"
                f"2️⃣ <b>W:</b> {s24['w2']} --> <b>E:</b> {s24['e_w2']}%\n"
                f"3️⃣ <b>W:</b> {s24['w3']} --> <b>E:</b> {s24['e_w3']}%\n"
                f"4️⃣ <b>W:</b> {s24['w4']} --> <b>E:</b> {s24['e_w4']}%\n"
                f"5️⃣ <b>W:</b> {s24['w5']} --> <b>E:</b> {s24['e_w5']}%\n"
                f"🈲 <b>L:</b> {s24['losses']} --> <b>E:</b> {s24['e_loss']}%\n"
                f"💰 <i>Bankroll 24h: {s24['bankroll_delta']:.2f} usd</i>"
            )
        if stats_text:
            tg_send_text(self.bot, self.chat_id, self.thread_id, stats_text)

    def _check_daily_report(self):
        """
        Envía el reporte del día si son las 12:00 hora Argentina (ART = UTC-3).
        Se activa una sola vez por día: cuando el primer giro tras las 12:00 AR
        detecta que la fecha ART cambió respecto al último reporte.
        """
        import datetime
        tz_ar = datetime.timezone(datetime.timedelta(hours=-3))
        now_ar = datetime.datetime.now(tz=tz_ar)
        # Solo ejecutar a partir de las 12:00 (mediodía)
        if now_ar.hour < 12:
            return
        today_str = now_ar.strftime("%Y-%m-%d")
        if self.stats.last_daily_date == today_str:
            return  # Ya se envió el reporte de hoy

        # Es mediodía AR y no se ha enviado todavía
        current_bankroll = self.bet_sys.bankroll
        sd = self.stats.get_daily_stats(current_bankroll)

        if sd["total"] == 0:
            # Sin señales en el día, igual registrar la fecha y reiniciar
            self.stats.reset_daily(today_str, current_bankroll)
            return

        date_label = now_ar.strftime("%d/%m/%Y")
        stats_text = (
            f"📅 <b>REPORTE DIARIO — {date_label}</b>\n"
            f"🕛 Actualizado a las 12:00 hs (AR)\n\n"
            f"🎰 Juego: <b>{self.name}</b>\n"
            f"🈯️ <b>Total señales:</b> {sd['total']}\n"
            f"📈 <b>Eficiencia:</b> {sd['efficiency']}%\n\n"
            f"1️⃣ <b>W:</b> {sd['w1']} ({sd['e_w1']}%)\n"
            f"2️⃣ <b>W:</b> {sd['w2']} ({sd['e_w2']}%)\n"
            f"3️⃣ <b>W:</b> {sd['w3']} ({sd['e_w3']}%)\n"
            f"4️⃣ <b>W:</b> {sd['w4']} ({sd['e_w4']}%)\n"
            f"5️⃣ <b>W:</b> {sd['w5']} ({sd['e_w5']}%)\n"
            f"🈲 <b>L:</b> {sd['losses']} ({sd['e_loss']}%)\n\n"
            f"💰 <b>Balance del día: {sd['bankroll_delta']:+.2f} usd</b>"
        )
        tg_send_text(self.bot, self.chat_id, self.thread_id, stats_text)
        logger.info(f"[{self.name}] Reporte diario enviado ({today_str})")
        # Reiniciar contadores para el nuevo día
        self.stats.reset_daily(today_str, current_bankroll)

    # ─── PROCESO PRINCIPAL DE CADA NÚMERO ────────────────────────────────────
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

        # Persistir giro en SQLite (estado 24/7)
        self._persist_spin(number)

        # Limpieza semanal (cada ~5000 giros)
        if len(self.spin_history) > 0 and len(self.spin_history) % 5000 == 0:
            self._cleanup_old_live_spins()

        # Actualizar historial
        self.spin_history.append({"number": number, "real": real})
        if len(self.spin_history) > 300:
            self.spin_history.pop(0)
        self.result_sequence.append({"number": number, "real": real})

        # Niveles acumulados — COLOR, PARIDAD, RANGO
        last_o = self.original_levels[-1] if self.original_levels else 0
        last_i = self.inverted_levels[-1]  if self.inverted_levels  else 0
        last_p = self.par_levels[-1]        if self.par_levels        else 0
        last_ip= self.impar_levels[-1]      if self.impar_levels      else 0
        last_a = self.alto_levels[-1]        if self.alto_levels        else 0
        last_b = self.bajo_levels[-1]        if self.bajo_levels        else 0

        if number == 0:  # VERDE — repite el último valor
            lnz = self.last_nonzero_color
            self.original_levels.append(last_o + (1 if lnz == 'ROJO'  else -1) if lnz else last_o)
            self.inverted_levels.append(last_i + (1 if lnz == 'NEGRO' else -1) if lnz else last_i)
            self.par_levels.append(last_p);   self.impar_levels.append(last_ip)
            self.alto_levels.append(last_a);  self.bajo_levels.append(last_b)
        else:
            par  = get_paridad(number)  # 'PAR' | 'IMPAR'
            rang = get_rango(number)    # 'ALTO'| 'BAJO'
            self.original_levels.append(last_o + (1 if real == 'ROJO'  else -1))
            self.inverted_levels.append(last_i + (1 if real == 'NEGRO' else -1))
            self.par_levels.append(last_p  + (1 if par  == 'PAR'  else -1))
            self.impar_levels.append(last_ip + (1 if par  == 'IMPAR' else -1))
            self.alto_levels.append(last_a + (1 if rang == 'ALTO' else -1))
            self.bajo_levels.append(last_b + (1 if rang == 'BAJO' else -1))
            self.last_nonzero_color = real

        while len(self.original_levels) > len(self.spin_history):
            self.original_levels.pop(0)
        while len(self.inverted_levels) > len(self.spin_history):
            self.inverted_levels.pop(0)
        min_len = min(len(self.original_levels), len(self.inverted_levels))
        self.original_levels = self.original_levels[-min_len:]
        self.inverted_levels = self.inverted_levels[-min_len:]

        # AMX y rachas
        self._update_amx_positions(real)
        self.amx_system.update_streak(real, self.get_signal(number))
        if self.signal_active or self.waiting_for_attempt:
            ref_color = self.bet_value if self.active_category == "COLOR" else "ROJO"
            self._update_unified_system(ref_color)
        if real != "VERDE":
            self.unified_prob_system.update_streak(real)

        # Actualizar predictores
        self.markov.update(self.spin_history)
        self.ml_predictor.add_spin(self.spin_history)
        self.category_ml.add_spin(number, real)
        # Mejoras Mega: streak + volatilidad + factores EMA/SR + pesos adaptativos
        self.unified_prob_system.update_streak(real)
        _lv = self.original_levels if real == "ROJO" else self.inverted_levels
        self.unified_prob_system.calculate_volatility(_lv)
        self.unified_prob_system.update_trend_factors(_lv)
        self.unified_prob_system.update_weights()

        # ── Control de calentamiento WS ───────────────────────────────────────
        if not self.warmup_done:
            self.ws_spins_count += 1
            if self.ws_spins_count < WARMUP_SPINS:
                logger.info(f"[{self.name}] Calentamiento WS: {self.ws_spins_count}/{WARMUP_SPINS} giros")
                return
            # Umbral alcanzado → activar señales
            self.warmup_done = True
            logger.info(f"[{self.name}] ✅ Calentamiento completado. Iniciando señales.")
            tg_send_text(self.bot, self.chat_id, self.thread_id,
                         f"🟢 <b>{self.name}</b> — Sistema listo. Emitiendo señales.")

        # ══════════════════════════════════════════════════════════════════════
        #  MÁQUINA DE ESTADOS
        # ══════════════════════════════════════════════════════════════════════

        # ── ESTADO 1: Señal activa ─────────────────────────────────────────
        if self.signal_active:
            result = self._is_win(number, real)

            # Verde → esperar
            if result is None:
                self.attempts_left -= 1
                if self.attempts_left <= 0:
                    # Contar como pérdida
                    self._handle_full_loss(number, real)
                    return
                attempt_number = MAX_ATTEMPTS - self.attempts_left + 1
                self.signal_active = False
                self.waiting_for_attempt = True
                self.waiting_attempt_number = attempt_number
                self.skip_one_after_zero = True
                self._send_waiting_message(attempt_number)
                return

            current_attempt = MAX_ATTEMPTS - self.attempts_left + 1

            if result:
                # ── GANAMOS ───────────────────────────────────────────────
                bet = self.bet_sys.win()
                self.stats.record_signal_result(current_attempt, True, bet, self.bet_sys.bankroll)
                if self.active_category == "COLOR":
                    self._record_prediction_result(self.bet_value, real)
                self.signal_active   = False
                self.active_category = None
                self._check_recovery()
                self._send_result(number, real, True, bet, current_attempt)
                self._check_daily_report()
                self._check_stats()
                self.signal_msg_ids = []
            else:
                # ── PERDEMOS ──────────────────────────────────────────────
                self.attempts_left -= 1
                bet = self.bet_sys.loss()
                if self.attempts_left <= 0:
                    self._handle_full_loss(number, real, bet)
                else:
                    attempt_number = MAX_ATTEMPTS - self.attempts_left + 1
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

        # ── ESTADO 2: Esperando condiciones para intento 2/3 ──────────────
        elif self.waiting_for_attempt:
            if real == "VERDE":
                self.skip_one_after_zero = True
                return
            if self.skip_one_after_zero:
                self.skip_one_after_zero = False
                return
            attempt_number = self.waiting_attempt_number
            # Re-evaluar TODAS las categorías y elegir la de mayor probabilidad ≥ 60%
            best = self._detect_best_category_signal()
            if not best or best["probability"] < self.min_prob_threshold:
                ords = {2:"2°",3:"3°",4:"4°",5:"5°"}
                ord_str = ords.get(attempt_number, f"{attempt_number}°")
                tg_send_text(self.bot, self.chat_id, self.thread_id,
                             f"🔔 Sin confirmación para enviar señal para el intento {ord_str}")
            elif best and best["probability"] >= self.min_prob_threshold:
                unified_prob = self._get_category_probability(
                    best["category"], best["bet_value"], number)
                # Verificar umbral 60% en probabilidad unificada
                if unified_prob["combined_prob"] < self.min_prob_threshold:
                    logger.debug(
                        f"[{self.name}] Reintento descartado [{best['category']}] "
                        f"{best['bet_value']} prob={unified_prob['combined_prob']*100:.0f}% < 60%")
                    ords = {2:"2°",3:"3°",4:"4°",5:"5°"}
                    ord_str = ords.get(attempt_number, f"{attempt_number}°")
                    tg_send_text(self.bot, self.chat_id, self.thread_id,
                                 f"🔔 Sin confirmación para enviar señal para el intento {ord_str}")
                else:
                    self.active_category    = best["category"]
                    self.bet_value          = best["bet_value"]
                    self.bet_color          = best["bet_value"] if best["category"] == "COLOR" else "ROJO"
                    self.trigger_number     = number
                    self.signal_active      = True
                    self.waiting_for_attempt = False
                    self._send_signal(attempt_number, unified_prob)

        # ── ESTADO 3: Idle – buscar señal ────────────────────────────────
        else:
            self.signal_msg_ids = []
            best = self._detect_best_category_signal()
            if best:
                unified_prob = self._get_category_probability(
                    best["category"], best["bet_value"], best["trigger_number"])
                # Verificar umbral 60% antes de emitir
                if unified_prob["combined_prob"] < self.min_prob_threshold:
                    logger.debug(
                        f"[{self.name}] Señal descartada [{best['category']}] "
                        f"{best['bet_value']} prob={unified_prob['combined_prob']*100:.0f}% < 60%")
                    return
                self.signal_active   = True
                self.active_category = best["category"]
                self.bet_value       = best["bet_value"]
                self.bet_color       = best["bet_value"] if best["category"] == "COLOR" else "ROJO"
                self.attempts_left   = MAX_ATTEMPTS
                self.total_attempts  = MAX_ATTEMPTS
                self.trigger_number  = best["trigger_number"]
                self._send_signal(1, unified_prob)
                self.amx_system.register_signal_sent()

    def _handle_full_loss(self, number: int, real: str, bet: float = None):
        """Maneja la pérdida final (3er intento fallido o verde agotando intentos)."""
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
        if self.active_category == "COLOR":
            self._record_prediction_result(self.bet_value, real)
        self.signal_active   = False
        self.active_category = None
        self._send_result(number, real, False, bet, 0)
        self._check_daily_report()
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
    return jsonify({"status": "ok", "bot": "Roulette Signal Bot AMX V22", "ts": time.time()})

@app.route("/ping")
def ping():
    return jsonify({"pong": True, "ts": time.time()})

@app.route("/health")
def health():
    return jsonify({"healthy": True})

# ─── SELF-PING ────────────────────────────────────────────────────────────────
async def self_ping_loop():
    """
    Self-ping cada 4 minutos — anti-sleep Render Free.
    Render duerme tras 15 min sin tráfico; 4 min garantiza continuidad.
    """
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
        await asyncio.sleep(240)   # 4 minutos

# ─── COMANDOS TELEGRAM ────────────────────────────────────────────────────────
engines: dict[str, RouletteEngine] = {}

def _register_handlers(b: telebot.TeleBot):
    """Registra los comandos de control en un bot dado."""

    @b.message_handler(commands=['start', 'help'])
    def cmd_start(message):
        seq = " - ".join(str(v) for v in LABOUCHERE_SEQUENCE)
        help_text = f"""
<b>🎰 Roulette Bot AMX UNIFIED</b>
Russian Roulette

<b>Características:</b>
• Sin cooldown entre señales
• 5 intentos por señal (Labouchère)
• Secuencia actual: <code>{seq}</code>
• Calentamiento WS: 21 giros silenciosos
• Gráficos por categoría: COLOR 🔴⚫️ | PARIDAD 🟣🟡 | RANGO 🟤🔵
• Pre-entrenamiento con DB histórica (~16k giros/tabla)

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
            w = engine.unified_prob_system.weights
            lines.append(f"<b>{name}</b>: {mode_icon} — {st} [M:{w['markov']:.2f} ML:{w['ml']:.2f}]")
        b.reply_to(message, "\n".join(lines), parse_mode="HTML")

    @b.message_handler(commands=['secuencia'])
    def cmd_secuencia(message):
        global LABOUCHERE_SEQUENCE
        parts = message.text.strip().split()[1:]   # descarta "/secuencia"
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

    # Registrar handlers y lanzar polling independiente para cada bot
    _register_handlers(bot)

    def _poll(b: telebot.TeleBot, label: str):
        logger.info(f"Iniciando polling Telegram — {label}")
        b.polling(none_stop=True, interval=1, timeout=30)

    threading.Thread(target=_poll, args=(bot, "Russian"), daemon=True).start()

    logger.info("🎰 Russian Roulette Bot AMX iniciado")
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    logger.info("Flask server started.")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped.")
