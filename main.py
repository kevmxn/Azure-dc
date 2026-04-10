#!/usr/bin/env python3
"""
Roulette Telegram Signal Bot - Sistema AMX V20 + ML + Markov (ventana deslizante)
+ IMI Intradía + Fractales + Anti-Cero + Rate Limiter + Tabla como señal primaria
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
from telebot import apihelper
import websockets
from flask import Flask, jsonify
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ─── ESCAPE HTML ──────────────────────────────────────────────────────────────
def escape_html(text: str) -> str:
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
logger = logging.getLogger("RussianRouletteBotAMX")

# ─── TELEGRAM ─────────────────────────────────────────────────────────────────
TOKEN = "8714149875:AAFJugWY0E5A4C0lrxn2bMcKsQEieqo_t5M"

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
apihelper.CONNECT_TIMEOUT = 10
apihelper.READ_TIMEOUT = 60

# ─── COLOR MAP ────────────────────────────────────────────────────────────────
REAL_COLOR_MAP = {
    0:"VERDE",1:"ROJO",2:"NEGRO",3:"ROJO",4:"NEGRO",5:"ROJO",6:"NEGRO",
    7:"ROJO",8:"NEGRO",9:"ROJO",10:"NEGRO",11:"NEGRO",12:"ROJO",13:"NEGRO",
    14:"ROJO",15:"NEGRO",16:"ROJO",17:"NEGRO",18:"ROJO",19:"ROJO",20:"NEGRO",
    21:"ROJO",22:"NEGRO",23:"ROJO",24:"NEGRO",25:"ROJO",26:"NEGRO",27:"ROJO",
    28:"NEGRO",29:"NEGRO",30:"ROJO",31:"NEGRO",32:"ROJO",33:"NEGRO",34:"ROJO",
    35:"NEGRO",36:"ROJO"
}

COLOR_DATA_AZURE = [
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

ROULETTE_CONFIGS = {
    "Russian Roulette": {
        "ws_key": 221,
        "chat_id": -1003835197023,
        "thread_id": 8344,
        "color_data": COLOR_DATA_AZURE,
        "betting_system": "dalembert",
        "min_prob_threshold": 0.49,
    },
}

WS_URL    = "wss://dga.pragmaticplaylive.net/ws"
CASINO_ID = "ppcjd00000007254"
MAX_ATTEMPTS = 3
BASE_BET     = 0.10
VISIBLE      = 60


# ══════════════════════════════════════════════════════════════════════════════
# ─── IMI INTRADÍA ─────────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════
class IMICalculator:
    IMI_PERIOD    = 14
    SIGNAL_PERIOD = 7
    SMA_PERIOD    = 15

    def __init__(self):
        self.imi_line:    list = []
        self.signal_line: list = []
        self.sma_line:    list = []
        self.current_value: float = 50.0

    def calculate(self, positions: list):
        if len(positions) < self.IMI_PERIOD:
            return
        self.imi_line = []
        self.signal_line = []
        self.sma_line = []

        for i in range(len(positions)):
            if i < self.IMI_PERIOD - 1:
                self.imi_line.append(None)
                self.signal_line.append(None)
                self.sma_line.append(None)
                continue
            up = dn = 0
            for j in range(i - self.IMI_PERIOD + 1, i + 1):
                if j > 0:
                    d = positions[j] - positions[j-1]
                    if d > 0:   up += 1
                    elif d < 0: dn += 1
            total = up + dn
            imi = (up / total * 100) if total > 0 else 50.0
            self.imi_line.append(imi)
            self.current_value = imi

            valid = [v for v in self.imi_line if v is not None]
            if len(valid) >= self.SIGNAL_PERIOD:
                k = 2 / (self.SIGNAL_PERIOD + 1)
                ev = sum(valid[:self.SIGNAL_PERIOD]) / self.SIGNAL_PERIOD
                for m in range(self.SIGNAL_PERIOD, len(valid)):
                    ev = valid[m] * k + ev * (1 - k)
                self.signal_line.append(ev)
            else:
                self.signal_line.append(None)

            if len(valid) >= self.SMA_PERIOD:
                self.sma_line.append(sum(valid[-self.SMA_PERIOD:]) / self.SMA_PERIOD)
            else:
                self.sma_line.append(None)

    def get_current_imi(self) -> float:
        v = [x for x in self.imi_line if x is not None]
        return v[-1] if v else 50.0

    def is_overbought(self) -> bool: return self.get_current_imi() > 70
    def is_oversold(self)  -> bool: return self.get_current_imi() < 30
    def normalized_value(self) -> float: return self.get_current_imi() / 100.0

    def momentum_tag(self) -> str:
        v = self.get_current_imi()
        if v > 70: return "Probable Reversion Bajista 🔴"
        if v < 30: return "Posible Reversion Alcista 🟢"
        return "Tendencia Neutra 🟡"


# ══════════════════════════════════════════════════════════════════════════════
# ─── FRACTALES ────────────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════
def detect_fractals(positions: list) -> list:
    fractals = []
    if len(positions) < 5:
        return fractals
    for i in range(2, len(positions) - 2):
        if (positions[i] < positions[i-1] and positions[i] < positions[i-2] and
                positions[i] < positions[i+1] and positions[i] < positions[i+2]):
            if i >= 5 and positions[i] > positions[i-5]:
                fractals.append({"index": i, "tipo": "up", "valor": positions[i]})
        if (positions[i] > positions[i-1] and positions[i] > positions[i-2] and
                positions[i] > positions[i+1] and positions[i] > positions[i+2]):
            if i >= 5 and positions[i] < positions[i-5]:
                fractals.append({"index": i, "tipo": "down", "valor": positions[i]})
    return fractals

def fractal_score(positions: list) -> float:
    """
    Retorna score entre -1 y 1:
    +1 = último fractal es UP (alcista)
    -1 = último fractal es DOWN (bajista)
     0 = sin fractal reciente
    """
    if len(positions) < 10:
        return 0.0
    w = positions[-20:] if len(positions) >= 20 else positions
    fs = detect_fractals(w)
    if not fs:
        return 0.0
    return 1.0 if fs[-1]["tipo"] == "up" else -1.0


# ══════════════════════════════════════════════════════════════════════════════
# ─── ZERO TRACKER ─────────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════
class ZeroTracker:
    ZERO_WINDOW = 20

    def __init__(self):
        self.zeros_in_signal:          int = 0
        self.total_zero_interruptions: int = 0
        self.zero_near_signal_count:   int = 0
        self._recent: deque = deque(maxlen=self.ZERO_WINDOW)

    def register_number(self, n: int):
        self._recent.append(n)

    def register_zero_in_signal(self):
        self.zeros_in_signal += 1
        self.total_zero_interruptions += 1

    def register_zero_near_signal(self):
        self.zero_near_signal_count += 1

    def recent_zero_density(self) -> float:
        if not self._recent:
            return 0.0
        return sum(1 for n in self._recent if n == 0) / len(self._recent)

    def zero_risk_score(self) -> float:
        excess = max(0.0, self.recent_zero_density() - 1/37)
        return min(excess * 15, 1.0)

    def stats_str(self) -> str:
        return (f"interrupciones={self.total_zero_interruptions} | "
                f"bloqueadas={self.zero_near_signal_count} | "
                f"densidad={self.recent_zero_density():.1%}")


# ══════════════════════════════════════════════════════════════════════════════
# ─── MARKOV CHAIN — VENTANA DESLIZANTE + DECAIMIENTO ─────────────────────────
# ══════════════════════════════════════════════════════════════════════════════
class MarkovChain:
    """
    Markov de orden 1 con ventana deslizante corta (maxlen=40).
    El decaimiento exponencial asegura que el modelo responde a cambios recientes
    y nunca acumula historial infinito que bloquee las predicciones.
    """
    COLORS = ("ROJO", "NEGRO")

    def __init__(self, order: int = 1, laplace_alpha: float = 0.5, decay: float = 0.95):
        self.order  = order
        self.alpha  = laplace_alpha
        self.decay  = decay
        self.transitions: dict = {}
        # Ventana corta: solo los últimos 40 colores alimentan los estados
        self._history: deque = deque(maxlen=40)

    def update(self, color: str):
        if color not in self.COLORS:
            return
        self._history.append(color)
        if len(self._history) < self.order + 1:
            return
        h     = list(self._history)
        state = tuple(h[-(self.order + 1):-1])
        nc    = h[-1]
        if state in self.transitions:
            for c in self.COLORS:
                self.transitions[state][c] *= self.decay
        else:
            self.transitions[state] = {c: 0.0 for c in self.COLORS}
        self.transitions[state][nc] += 1.0

    def predict(self) -> dict:
        if len(self._history) < self.order:
            return {c: 0.5 for c in self.COLORS}
        state  = tuple(list(self._history)[-self.order:])
        counts = self.transitions.get(state, {c: 0.0 for c in self.COLORS})
        total  = sum(counts.values()) + self.alpha * len(self.COLORS)
        return {c: (counts.get(c, 0.0) + self.alpha) / total for c in self.COLORS}

    def confidence(self) -> float:
        if len(self._history) < self.order:
            return 0.0
        state = tuple(list(self._history)[-self.order:])
        return float(sum(self.transitions.get(state, {}).values()))

    def state_info(self) -> str:
        if len(self._history) < self.order:
            return "Sin datos"
        state = tuple(list(self._history)[-self.order:])
        p     = self.predict()
        return (f"Estado:{state} R={p['ROJO']:.2f} N={p['NEGRO']:.2f} "
                f"conf={self.confidence():.1f} decay={self.decay}")


# ══════════════════════════════════════════════════════════════════════════════
# ─── ONLINE LOGISTIC REGRESSION ───────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════
class OnlineLogisticRegression:
    N_FEATURES = 12  # tabla_prob, markov, conf, ema4>20, ema8>20, ema4>8,
                     # momentum, above5, consec_losses, bet_step, imi, frac

    def __init__(self, lr: float = 0.08, reg: float = 0.005, min_samples: int = 10):
        self.weights    = np.zeros(self.N_FEATURES)
        self.bias       = 0.0
        self.lr         = lr
        self.reg        = reg
        self.min_samples = min_samples
        self.n_samples   = 0
        self._feat_mean  = np.zeros(self.N_FEATURES)
        self._feat_var   = np.ones(self.N_FEATURES)
        self._feat_n     = 0

    def _update_stats(self, x):
        self._feat_n += 1
        d = x - self._feat_mean
        self._feat_mean += d / self._feat_n
        self._feat_var  += d * (x - self._feat_mean)

    def _normalize(self, x):
        if self._feat_n < 2:
            return x
        std = np.sqrt(self._feat_var / max(self._feat_n - 1, 1))
        std = np.where(std < 1e-8, 1.0, std)
        return (x - self._feat_mean) / std

    @staticmethod
    def _sigmoid(z):
        return 1.0 / (1.0 + np.exp(-np.clip(z, -15, 15)))

    def predict_proba(self, raw: np.ndarray) -> float:
        return self._sigmoid(float(np.dot(self.weights, self._normalize(raw))) + self.bias)

    def update(self, raw: np.ndarray, label: int):
        self._update_stats(raw)
        x     = self._normalize(raw)
        pred  = self._sigmoid(float(np.dot(self.weights, x)) + self.bias)
        err   = label - pred
        self.weights += self.lr * (err * x - self.reg * self.weights)
        self.bias    += self.lr * err
        self.n_samples += 1

    @property
    def ready(self) -> bool:
        return self.n_samples >= self.min_samples

    def summary(self) -> str:
        return f"n={self.n_samples} ready={self.ready}"


# ══════════════════════════════════════════════════════════════════════════════
# ─── ML SIGNAL FILTER CON FRACTAL BOOST ───────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════
class MLSignalFilter:
    N_FEATURES = 12

    def __init__(self, markov_order=1, markov_threshold=0.52,
                 ml_threshold=0.54, ml_threshold_retry=0.50,
                 ml_min_samples=20, markov_decay=0.97):
        self.markov = MarkovChain(order=markov_order, decay=markov_decay)
        self.model  = OnlineLogisticRegression(min_samples=ml_min_samples)
        self.markov_threshold   = markov_threshold
        self.ml_threshold       = ml_threshold
        self.ml_threshold_retry = ml_threshold_retry
        self._last_features: Optional[np.ndarray] = None
        # Factor de boost máximo por fractal (0.10 = ±10%)
        self.fractal_boost_factor = 0.10

    def extract_features(self, bet_color, tabla_prob, ema4, ema8, ema20,
                         positions, momentum_count, consec_losses, bet_step,
                         last_two_expected, recovery_active,
                         imi_value=0.5, frac_score=0.0) -> np.ndarray:
        li   = len(ema4) - 1
        safe = li >= 0 and ema4[li] is not None and ema8[li] is not None and ema20[li] is not None

        f0  = tabla_prob
        f1  = self.markov.predict().get(bet_color, 0.5)
        f2  = min(self.markov.confidence(), 20.0) / 20.0   # normalizado a ventana corta
        f3  = float(ema4[li] > ema20[li]) if safe else 0.5
        f4  = float(ema8[li] > ema20[li]) if safe else 0.5
        f5  = float(ema4[li] > ema8[li])  if safe else 0.5

        above5 = 0.0
        if safe and len(positions) >= 5:
            for k in range(5):
                pi = len(positions) - 5 + k
                ei = li - 4 + k
                if 0 <= pi < len(positions) and 0 <= ei < len(ema20) and ema20[ei] is not None:
                    if positions[pi] > ema20[ei]:
                        above5 += 1.0
        f6  = above5 / 5.0
        f7  = min(momentum_count, 5) / 5.0
        f8  = min(consec_losses, 10) / 10.0
        f9  = min(bet_step, 20) / 20.0
        f10 = float(np.clip(imi_value, 0, 1))
        # f11: fractal score normalizado [0,1] para features
        f11 = float(np.clip((frac_score + 1) / 2, 0, 1))

        feats = np.array([f0,f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11], dtype=np.float32)
        self._last_features = feats
        return feats

    def tabla_adjusted_markov(self, bet_color: str, tabla_prob: float, 
                              frac_score: float = 0.0) -> tuple[float, float, str]:
        """
        Ajusta la probabilidad de Markov usando la tabla Y el fractal como moduladores.

        Lógica:
          1. tabla_prob > 0.50 → tabla CONFIRMA bet_color → boost al markov_raw
          2. tabla_prob < 0.50 → tabla CONTRADICE bet_color → penaliza markov_raw
          3. Fractal UP para el color → boost adicional a markov
          4. Fractal DOWN para el color → penalización adicional a markov

        Formula tabla: adjusted = markov_raw * (tabla_prob / 0.50)
        Formula fractal: boost = 1 + (frac_score * fractal_boost_factor)
          - frac_score = +1 (up) → boost = 1.10 (+10%)
          - frac_score = -1 (down) → boost = 0.90 (-10%)
          - frac_score = 0 → boost = 1.00 (sin cambio)
        """
        markov_raw  = self.markov.predict().get(bet_color, 0.5)
        
        # Ajuste por tabla
        factor_tabla = tabla_prob / 0.50
        factor_tabla = max(0.70, min(1.30, factor_tabla))
        
        # Ajuste por fractal (direccional)
        # Si bet_color es ROJO y fractal UP en original_levels → boost positivo
        # Si bet_color es NEGRO y fractal UP en inverted_levels → boost positivo
        factor_fractal = 1.0 + (frac_score * self.fractal_boost_factor)
        factor_fractal = max(0.80, min(1.20, factor_fractal))  # limitar a ±20%
        
        # Aplicar ambos factores
        adjusted = markov_raw * factor_tabla * factor_fractal
        adjusted = max(0.0, min(1.0, adjusted))
        
        # Tags para logging
        tag_tabla = "↑refuerza" if factor_tabla > 1.0 else ("↓debilita" if factor_tabla < 1.0 else "=neutral")
        tag_frac = ""
        if frac_score > 0.3:
            tag_frac = " 📈FRAC-UP"
        elif frac_score < -0.3:
            tag_frac = " 📉FRAC-DOWN"
        
        return adjusted, markov_raw, f"tabla={tabla_prob:.2f}({tag_tabla}) fractal={frac_score:+.1f}{tag_frac}"

    def should_emit_signal(self, features, bet_color, tabla_prob=0.5,
                           is_retry=False, attempt_number=1,
                           reversal_risk: bool = False, frac_score: float = 0.0) -> tuple:
        """
        Decisión de emisión:
          1. Markov ajustado por tabla + fractal (modulación bidireccional)
          2. ML (cuando listo) — refinamiento adicional
          3. Bloqueo por riesgo de reversión
        """
        # ── Umbrales por intento (intento 2 y 3 son iguales) ──
        if attempt_number == 1:
            mk_th = self.markov_threshold
            ml_th = self.ml_threshold if not is_retry else self.ml_threshold_retry
        else:
            mk_th = self.markov_threshold + 0.02
            ml_th = (self.ml_threshold if not is_retry else self.ml_threshold_retry) + 0.02

        # ── Markov ajustado por tabla + fractal ──
        adj_markov, markov_raw, adj_tag = self.tabla_adjusted_markov(bet_color, tabla_prob, frac_score)

        if adj_markov < mk_th:
            return False, adj_markov, 0.0, (
                f"Markov ajustado bloqueó: {adj_markov:.2f}<{mk_th:.2f} [{adj_tag}]")

        # ── Riesgo de reversión (racha larga + Markov opuesto + IMI) ──
        if reversal_risk:
            return False, adj_markov, 0.0, (
                f"Riesgo reversión bloqueó: mk_adj={adj_markov:.2f} [{adj_tag}]")

        if not self.model.ready:
            return True, adj_markov, 0.0, (
                f"Markov_adj={adj_markov:.2f} [{adj_tag}] ML warm-up {self.model.n_samples}/{self.model.min_samples}")

        ml_prob = self.model.predict_proba(features)
        if ml_prob < ml_th:
            return False, adj_markov, ml_prob, (
                f"ML bloqueó: {ml_prob:.2f}<{ml_th:.2f} mk_adj={adj_markov:.2f}")

        return True, adj_markov, ml_prob, (
            f"OK — mk_raw={markov_raw:.2f} mk_adj={adj_markov:.2f} ML={ml_prob:.2f} [{adj_tag}]")

    def update_result(self, won: bool):
        if self._last_features is not None:
            self.model.update(self._last_features, int(won))

    def observe_color(self, color: str):
        if color in ("ROJO", "NEGRO"):
            self.markov.update(color)

    def info(self) -> str:
        return f"Markov: {self.markov.state_info()} | ML: {self.model.summary()}"


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
        if self.step > 0: self.step -= 1
        return bet

    def loss(self) -> float:
        bet = self.current_bet()
        self.bankroll = round(self.bankroll - bet, 2)
        self.step = 0 if self.step >= self.max_step - 1 else self.step + 1
        return bet


# ─── SISTEMA AMX V20 ──────────────────────────────────────────────────────────
class AMXSignalSystem:
    def __init__(self, mode: Literal["tendencia", "moderado"] = "moderado"):
        self.mode = mode
        self.last_signal_time: float = 0
        self.cooldown_seconds: int   = 6   # bajado de 8
        self.so_cooldown: Optional[float] = None
        self.ultimos_puntos: list = []
        self.last_two_expected: deque = deque(maxlen=2)
        self.last_two_colors:   deque = deque(maxlen=2)

    def update_streak(self, real_color: str, expected_color: Optional[str]):
        if expected_color:
            self.last_two_expected.append(real_color == expected_color)
        self.last_two_colors.append(real_color)

    @staticmethod
    def _ema(data, period):
        if len(data) < period:
            return [None] * len(data)
        mult = 2 / (period + 1)
        out  = [None] * (period - 1)
        prev = sum(data[:period]) / period
        out.append(prev)
        for i in range(period, len(data)):
            prev = data[i] * mult + prev * (1 - mult)
            out.append(prev)
        return out

    def check_signal_tendencia(self, positions, color_data, num, expected, thresh):
        if len(positions) < 20: return None
        if time.time() - self.last_signal_time < self.cooldown_seconds: return None
        e4  = self._ema(positions, 4)
        e8  = self._ema(positions, 8)
        e20 = self._ema(positions, 20)
        if any(v is None for v in [e4[-1],e8[-1],e20[-1],e4[-2],e8[-2],e20[-2]]): return None
        p = positions[-1]
        cruce  = e4[-2] <= e20[-2] and e4[-1] > e20[-1]
        sobre  = p > e4[-1] and p > e8[-1] and p > e20[-1]
        cruce8 = e8[-2] <= e20[-2] and e8[-1] > e20[-1]
        if not (cruce or sobre or cruce8): return None
        entry = next((e for e in color_data if e["id"] == num), None)
        if not entry or entry["senal"] == "NO APOSTAR": return None
        prob = entry["rojo"] if expected == "ROJO" else entry["negro"]
        if entry["senal"] != expected or prob < thresh: return None
        return {"type":"SKRILL_2.0","mode":"tendencia","expected_color":expected,
                "probability":prob,"trigger_number":num,"strength":"strong" if cruce or cruce8 else "moderate"}

    def check_signal_moderado(self, positions, color_data, num, expected, thresh):
        if len(positions) < 20: return None
        if time.time() - self.last_signal_time < self.cooldown_seconds: return None
        e4  = self._ema(positions, 4)
        e8  = self._ema(positions, 8)
        e20 = self._ema(positions, 20)
        if any(v is None for v in [e4[-1],e8[-1],e20[-1],e8[-2],e20[-2]]): return None
        cruce8 = e8[-2] <= e20[-2] and e8[-1] > e20[-1]
        sobre  = positions[-1] > e4[-1] and positions[-1] > e8[-1]
        patron_v = False
        if len(positions) >= 3:
            a,b,c = positions[-3],positions[-2],positions[-1]
            patron_v = b < a and b < c and abs(a-c) <= 1 and c > a
        dos = len(self.last_two_expected) >= 2 and all(self.last_two_expected)
        alcistas = e4[-1] > e8[-1] > e20[-1]
        if not (cruce8 or patron_v or (dos and alcistas and sobre)): return None
        entry = next((e for e in color_data if e["id"] == num), None)
        if not entry or entry["senal"] == "NO APOSTAR": return None
        prob = entry["rojo"] if expected == "ROJO" else entry["negro"]
        if entry["senal"] != expected or prob < thresh: return None
        return {"type":"ALERTA_2.0","mode":"moderado","expected_color":expected,
                "probability":prob,"trigger_number":num,"pattern":"V" if patron_v else "EMA_CROSS"}

    def register_signal_sent(self):
        self.last_signal_time = time.time()


# ─── STATISTICS ───────────────────────────────────────────────────────────────
class Stats:
    def __init__(self):
        self.total = self.wins = self.losses = 0
        self.last_stats_at = 0
        self._h24: deque = deque()
        self.batch_start_bankroll = None
        self._wins_at_last_batch  = 0

    def record(self, is_win: bool, bankroll: float):
        self.total += 1
        if is_win: self.wins   += 1
        else:      self.losses += 1
        self._h24.append((time.time(), is_win, bankroll))
        self._trim24()

    def _trim24(self):
        cut = time.time() - 86400
        while self._h24 and self._h24[0][0] < cut:
            self._h24.popleft()

    def should_send_stats(self) -> bool:
        return (self.total - self.last_stats_at) >= 20

    def mark_stats_sent(self, bk):
        self.last_stats_at        = self.total
        self.batch_start_bankroll = bk
        self._wins_at_last_batch  = self.wins

    def batch_stats(self, bk):
        n  = self.total - self.last_stats_at
        w  = self.wins  - self._wins_at_last_batch
        l  = n - w
        e  = round(w/n*100,1) if n else 0.0
        return w, l, n, e, round(bk - (self.batch_start_bankroll or 0), 2)

    def stats_24h(self, bk):
        self._trim24()
        t = len(self._h24)
        w = sum(1 for _,iw,_ in self._h24 if iw)
        e = round(w/t*100,1) if t else 0.0
        bk24 = round(self._h24[-1][2]-self._h24[0][2],2) if t >= 2 else 0.0
        return w, t-w, t, e, bk24


# ─── SOPORTE/RESISTENCIA ──────────────────────────────────────────────────────
def find_support_resistance(levels, lookback=30):
    if len(levels) < lookback:
        return {'support': None, 'resistance': None}
    r = levels[-lookback:]
    sup = [r[i] for i in range(2, len(r)-2) if all(r[i] < r[j] for j in (i-1,i-2,i+1,i+2))]
    res = [r[i] for i in range(2, len(r)-2) if all(r[i] > r[j] for j in (i-1,i-2,i+1,i+2))]
    return {'support': sup[-1] if sup else None, 'resistance': res[-1] if res else None}


# ─── CHART ────────────────────────────────────────────────────────────────────
def generate_chart(levels, spin_history, bet_color, markov_prob=0.0, ml_prob=0.0,
                   imi_value=50.0, frac_tipo="", tabla_prob=0.0, visible=VISIBLE):
    arr = np.array(levels, dtype=float)
    n   = len(arr)

    def ema_np(d, p):
        if len(d) < p: return np.full(len(d), np.nan)
        m = 2/(p+1); o = np.full(len(d), np.nan)
        o[p-1] = np.mean(d[:p])
        for i in range(p, len(d)): o[i] = (d[i]-o[i-1])*m + o[i-1]
        return o

    e4  = ema_np(arr, 4)
    e8  = ema_np(arr, 8)
    e20 = ema_np(arr, 20)

    st = max(0, n - visible); sl = slice(st, n)
    x  = np.arange(len(arr[sl])); hs = spin_history[st:]

    rojo  = bet_color == "ROJO"
    bg    = "#0b101f"; ax_bg = "#0f1a2a"; gc = "#1e2e48"
    lc    = "#e84040" if rojo else "#9090bb"
    e4c,e8c,e20c = "#ff9f43","#48dbfb","#1dd1a1"
    tc    = "#ff8080" if rojo else "#b0b8d0"

    fig, ax = plt.subplots(figsize=(8, 3.6), facecolor=bg)
    ax.set_facecolor(ax_bg)
    y,ev4,ev8,ev20 = arr[sl],e4[sl],e8[sl],e20[sl]
    ax.fill_between(x,y,alpha=0.10,color=lc)
    ax.plot(x,y,color=lc,linewidth=0.8,zorder=3)
    ax.plot(x,ev4,color=e4c,linewidth=0.7,linestyle="--",label="EMA 4",zorder=4)
    ax.plot(x,ev8,color=e8c,linewidth=0.7,linestyle="--",label="EMA 8",zorder=4)
    ax.plot(x,ev20,color=e20c,linewidth=1.0,label="EMA 20",zorder=4)

    dc = {"ROJO":"#e84040","NEGRO":"#aaaacc","VERDE":"#2ecc71"}
    for i,s in enumerate(hs):
        ax.scatter(i, y[i], color=dc.get(s["real"],"#fff"), s=22, zorder=5,
                   edgecolors="white", linewidths=0.3)

    sr = find_support_resistance(levels)
    rc = "#e84040" if rojo else "#888888"; sc = "#888888" if rojo else "#e84040"
    if sr['support']:
        ax.axhline(y=sr['support'],color=sc,linestyle='--',linewidth=1.5,alpha=0.7)
        ax.text(x[-1],sr['support'],f' S {sr["support"]:.1f}',color=sc,fontsize=8,va='bottom',ha='right')
    if sr['resistance']:
        ax.axhline(y=sr['resistance'],color=rc,linestyle='--',linewidth=1.5,alpha=0.7)
        ax.text(x[-1],sr['resistance'],f' R {sr["resistance"]:.1f}',color=rc,fontsize=8,va='top',ha='right')

    parts = []
    if tabla_prob > 0:  parts.append(f"Tabla {tabla_prob*100:.0f}%")
    if markov_prob > 0: parts.append(f"Markov {markov_prob*100:.0f}%")
    if ml_prob > 0:     parts.append(f"ML {ml_prob*100:.0f}%")
    parts.append(f"IMI {imi_value:.0f}")
    if frac_tipo: parts.append(f"Frac {'↑' if frac_tipo=='up' else '↓'}")
    ax.text(0.01,0.97,"  |  ".join(parts),transform=ax.transAxes,
            color="#f0e040",fontsize=7.5,va='top',ha='left',
            bbox=dict(boxstyle='round,pad=0.2',facecolor='#0b101f',alpha=0.7))

    ts = max(1,len(x)//8); tx = list(range(0,len(x),ts))
    ax.set_xticks(tx)
    ax.set_xticklabels([str(hs[i]["number"]) if i<len(hs) else "" for i in tx], color="#8899bb",fontsize=7)
    ax.tick_params(axis='y',colors="#8899bb",labelsize=7)
    ax.tick_params(axis='x',colors="#8899bb",labelsize=7)
    for sp in ('bottom','left'): ax.spines[sp].set_color(gc)
    ax.spines['top'].set_visible(False); ax.spines['right'].set_visible(False)
    ax.grid(axis='y',color=gc,linewidth=0.4,alpha=0.5)
    ax.set_title(f"{'🔴' if rojo else '⚫️'} {'ROJO' if rojo else 'NEGRO'} — últimos {visible} giros · EMA+IMI+FRACTAL",
                 color=tc,fontsize=9,pad=6)

    from matplotlib.lines import Line2D
    leg = [Line2D([0],[0],color=lc,linewidth=0.8,label="Nivel"),
           Line2D([0],[0],color=e4c,linewidth=0.7,linestyle="--",label="EMA 4"),
           Line2D([0],[0],color=e8c,linewidth=0.7,linestyle="--",label="EMA 8"),
           Line2D([0],[0],color=e20c,linewidth=1.0,label="EMA 20"),
           Line2D([0],[0],marker='o',color='w',markerfacecolor='#e84040',markersize=5,label="Rojo"),
           Line2D([0],[0],marker='o',color='w',markerfacecolor='#aaaacc',markersize=5,label="Negro"),
           Line2D([0],[0],marker='o',color='w',markerfacecolor='#2ecc71',markersize=5,label="Verde")]
    if sr['support']:    leg.append(Line2D([0],[0],color=sc,linestyle='--',linewidth=1.5,label='Soporte'))
    if sr['resistance']: leg.append(Line2D([0],[0],color=rc,linestyle='--',linewidth=1.5,label='Resistencia'))
    ax.legend(handles=leg,loc="upper left",fontsize=6.5,facecolor="#0b101f",
              edgecolor=gc,labelcolor="white",framealpha=0.8,ncol=2)

    plt.tight_layout(pad=0.8)
    buf = io.BytesIO()
    fig.savefig(buf,format="png",dpi=120,facecolor=bg)
    plt.close(fig); buf.seek(0)
    return buf


# ─── TELEGRAM HELPERS ─────────────────────────────────────────────────────────
_TG_MAX = 5

def _tg_call(fn, *args, **kwargs):
    delay = 2.0
    for i in range(1, _TG_MAX+1):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            err = str(e)
            if "retry after" in err.lower():
                try: wait = int(''.join(filter(str.isdigit,err)))+1
                except: wait = 30
                time.sleep(wait); continue
            if i < _TG_MAX:
                time.sleep(delay); delay = min(delay*2, 60)
            else:
                return None

def tg_send_photo(chat_id, thread_id, buf, caption) -> Optional[int]:
    buf.seek(0)
    m = _tg_call(bot.send_photo, chat_id=chat_id, photo=buf,
                 caption=caption, parse_mode="HTML", message_thread_id=thread_id)
    return m.message_id if m else None

def tg_send_text(chat_id, thread_id, text) -> Optional[int]:
    m = _tg_call(bot.send_message, chat_id=chat_id, text=text,
                 parse_mode="HTML", message_thread_id=thread_id)
    return m.message_id if m else None

def tg_delete(chat_id, mid):
    _tg_call(bot.delete_message, chat_id=chat_id, message_id=mid)


# ══════════════════════════════════════════════════════════════════════════════
# ─── ROULETTE ENGINE ──────────────────────────────────────────────────────────
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
        self.expected_color: Optional[str] = None
        self.bet_color:      Optional[str] = None
        self.attempts_left:  int = 0
        self.trigger_number: Optional[int] = None

        self.result_until:    float = 0.0
        self.consec_losses:   int   = 0
        self.recovery_active: bool  = False
        self.recovery_target: float = 0.0
        self.level1_bankroll: float = 0.0

        self.bet_sys = D_Alembert(BASE_BET)
        self.stats   = Stats()
        self.signal_msg_ids: list = []
        self.ws = None
        self.running = True

        self.amx_system         = AMXSignalSystem(mode="moderado")
        self.min_prob_threshold = cfg.get("min_prob_threshold", 0.48)

        self.ml_filter = MLSignalFilter(
            markov_order=1,
            markov_threshold=0.52,
            ml_threshold=0.54,
            ml_threshold_retry=0.50,
            ml_min_samples=20,
            markov_decay=0.97,
        )
        self._last_markov_prob: float = 0.0
        self._last_ml_prob:     float = 0.0
        self._last_frac_score:  float = 0.0
        self.signal_sequence_colors: list = []
        self.signal_history: list = []

        self.imi_original = IMICalculator()
        self.imi_inverted = IMICalculator()

        self.zero_tracker = ZeroTracker()
        self.zeros_in_current_signal: int = 0
        self.zero_pause_msg_id: Optional[int] = None

        self.waiting_for_retry       = False
        self.waiting_attempt_number  = 0
        self.waiting_message_id      = None

        # Condiciones para inversión por intento
        self.attempt_conditions = {1: 2, 2: 3, 3: 3}

    # ── Helpers tabla ──────────────────────────────────────────────────────────
    def get_entry(self, number: int) -> Optional[dict]:
        return next((e for e in self.color_data if e["id"] == number), None)

    def get_signal(self, number: int) -> Optional[str]:
        e = self.get_entry(number)
        return e["senal"] if e else None

    def get_prob(self, number: int, color: str) -> float:
        e = self.get_entry(number)
        if not e: return 0.0
        return e["rojo"] if color == "ROJO" else e["negro"]

    def tabla_direction(self, number: int) -> tuple[str, float]:
        """Retorna (color_dominante, probabilidad) según la tabla."""
        e = self.get_entry(number)
        if not e or e["senal"] == "NO APOSTAR":
            return "", 0.0
        return e["senal"], e["rojo"] if e["senal"] == "ROJO" else e["negro"]

    # ── IMI ────────────────────────────────────────────────────────────────────
    def _current_imi(self, color: str) -> IMICalculator:
        return self.imi_original if color == "ROJO" else self.imi_inverted

    def _update_imi(self):
        if len(self.original_levels) >= IMICalculator.IMI_PERIOD:
            self.imi_original.calculate(self.original_levels)
        if len(self.inverted_levels) >= IMICalculator.IMI_PERIOD:
            self.imi_inverted.calculate(self.inverted_levels)

    # ── EMA helper ────────────────────────────────────────────────────────────
    @staticmethod
    def _ema(data, period):
        if len(data) < period: return [None]*len(data)
        m = 2/(period+1); out = [None]*(period-1)
        p = sum(data[:period])/period; out.append(p)
        for i in range(period, len(data)):
            p = (data[i]-p)*m+p; out.append(p)
        return out

    # ══════════════════════════════════════════════════════════════════════════
    # DETECCIÓN DE SEÑAL PRIMARIA — BASADA EN TABLA
    # ══════════════════════════════════════════════════════════════════════════
    def _tabla_signal(self, number: int) -> Optional[dict]:
        """
        Señal primaria basada en las probabilidades de la tabla.
        Condiciones mínimas:
          1. El número tiene señal en la tabla (no NO APOSTAR)
          2. prob_tabla >= min_prob_threshold
          3. Al menos 1 EMA confirma la dirección (ema4 o ema8 > ema20)
          4. Mínimo 15 giros de historial
        """
        if number == 0 or len(self.spin_history) < 60:
            return None
        color, prob = self.tabla_direction(number)
        if not color or prob < self.min_prob_threshold:
            return None

        positions = self.original_levels if color == "ROJO" else self.inverted_levels
        if len(positions) < 8:
            return None

        e4  = self._ema(positions, 4)
        e8  = self._ema(positions, 8)
        e20 = self._ema(positions, 20)

        if e20[-1] is None:
            return None

        # Al menos una EMA confirma la dirección
        ema_confirms = (
            (e4[-1] is not None  and e4[-1]  > e20[-1]) or
            (e8[-1] is not None  and e8[-1]  > e20[-1])
        )
        if not ema_confirms:
            return None

        return {
            "type": "TABLA",
            "expected_color": color,
            "probability": prob,
            "trigger_number": number,
        }

    # ══════════════════════════════════════════════════════════════════════════
    # SHOULD_ACTIVATE — simplificado con tabla
    # ══════════════════════════════════════════════════════════════════════════
    def should_activate(self) -> Optional[str]:
        """Detector clásico de tendencia (requiere EMA20 sostenida)."""
        if len(self.spin_history) < 60:
            return None
        last_num = self.spin_history[-1]["number"]
        if last_num == 0:
            return None
        entry = self.get_entry(last_num)
        if not entry or entry["senal"] == "NO APOSTAR":
            return None
        expected = entry["senal"]

        positions = self.original_levels if expected == "ROJO" else self.inverted_levels
        if len(positions) < 20:
            return None

        e4  = self._ema(positions, 4)
        e8  = self._ema(positions, 8)
        e20 = self._ema(positions, 20)
        li  = len(positions) - 1
        if e20[li] is None:
            return None

        # 3 barras consecutivas sobre EMA20 (más permisivo que antes)
        req = min(3 + self.consec_losses, 8)
        ok  = True
        for off in range(req):
            i = li - (req - 1) + off
            if i < 0 or i >= len(positions) or e20[i] is None or positions[i] <= e20[i]:
                ok = False; break
        if not ok:
            return None
        return expected

    # ══════════════════════════════════════════════════════════════════════════
    # DETECT AMX SIGNAL
    # ══════════════════════════════════════════════════════════════════════════
    def _detect_amx_signal(self) -> Optional[dict]:
        if len(self.amx_system.ultimos_puntos) < 20:
            return None
        num = self.spin_history[-1]["number"] if self.spin_history else 0
        if num == 0:
            return None
        entry = self.get_entry(num)
        if not entry or entry["senal"] == "NO APOSTAR":
            return None
        expected = entry["senal"]
        # No requerir momentum mínimo — la tabla ya confirma la dirección
        try:
            if self.amx_system.mode == "tendencia":
                return self.amx_system.check_signal_tendencia(
                    self.amx_system.ultimos_puntos, self.color_data,
                    num, expected, self.min_prob_threshold)
            else:
                return self.amx_system.check_signal_moderado(
                    self.amx_system.ultimos_puntos, self.color_data,
                    num, expected, self.min_prob_threshold)
        except Exception as e:
            logger.warning(f"[{self.name}] AMX error: {e}")
            return None

    # ══════════════════════════════════════════════════════════════════════════
    # BUILD FEATURES CON FRACTAL
    # ══════════════════════════════════════════════════════════════════════════
    def _build_features(self, bet_color: str, tabla_prob: Optional[float] = None) -> tuple:
        """
        Retorna (features, frac_score) para usar en la decisión ML y Markov.
        """
        if tabla_prob is None:
            last = self.spin_history[-1]["number"] if self.spin_history else 0
            tabla_prob = self.get_prob(last, bet_color)
        positions = self.original_levels if bet_color == "ROJO" else self.inverted_levels
        e4  = self._ema(positions, 4)
        e8  = self._ema(positions, 8)
        e20 = self._ema(positions, 20)
        recent   = [s["real"] for s in self.spin_history[-5:]]
        momentum = sum(1 for c in reversed(recent) if c == bet_color)
        imi_n    = self._current_imi(bet_color).normalized_value()
        # Calcular fractal score para este color
        frac     = fractal_score(positions)
        
        feats = self.ml_filter.extract_features(
            bet_color, tabla_prob, e4, e8, e20, positions,
            momentum, self.consec_losses, self.bet_sys.step,
            self.amx_system.last_two_expected, self.recovery_active,
            imi_value=imi_n, frac_score=frac)
        
        return feats, frac

    # ══════════════════════════════════════════════════════════════════════════
    # DETECCIÓN DE RIESGO DE REVERSIÓN
    # ══════════════════════════════════════════════════════════════════════════
    def _reversal_risk(self, bet_color: str) -> tuple[bool, str]:
        """
        Detecta señales débiles por riesgo de reversión de tendencia.

        Condición activa cuando SE CUMPLEN LAS TRES A LA VEZ:
          1. Racha de 2+ giros consecutivos del mismo color que bet_color
             (indica que la tendencia puede estar agotada)
          2. Markov RAW favorece el color opuesto (p.ej. markov[NEGRO] > markov[ROJO])
             aunque Markov ajustado por tabla pase el umbral
          3. IMI muestra zona de sobrecompra (>70) o sobreventa (<30),
             indicando probable cambio de dirección inminente

        Esta combinación indica que aunque el Markov ajustado parece favorable,
        la racha larga + el Markov puro + el IMI apuntan a reversión.
        """
        opposite = "NEGRO" if bet_color == "ROJO" else "ROJO"

        # 1. Contar racha consecutiva del color apostado (excluye ceros)
        racha = 0
        for spin in reversed(self.spin_history[-10:]):
            if spin["real"] == bet_color:
                racha += 1
            elif spin["real"] != "VERDE":
                break

        if racha < 2:
            return False, f"racha={racha} (OK)"

        # 2. Markov RAW favorece el opuesto
        mk_probs   = self.ml_filter.markov.predict()
        mk_raw_bet = mk_probs.get(bet_color, 0.5)
        mk_raw_opp = mk_probs.get(opposite, 0.5)
        markov_opuesto = mk_raw_opp > mk_raw_bet

        if not markov_opuesto:
            return False, f"racha={racha} markov_raw ok ({mk_raw_bet:.2f}>{mk_raw_opp:.2f})"

        # 3. IMI en zona de reversión
        imi = self._current_imi(bet_color)
        imi_reversal = imi.is_overbought() or imi.is_oversold()

        if not imi_reversal:
            return False, (f"racha={racha} mk_opp={mk_raw_opp:.2f} "
                           f"IMI={imi.get_current_imi():.0f} (no crítico)")

        reason = (f"REVERSIÓN: racha={racha}x{bet_color} | "
                  f"mk_raw_{opposite}={mk_raw_opp:.2f} > mk_{bet_color}={mk_raw_bet:.2f} | "
                  f"IMI={imi.get_current_imi():.0f}")
        return True, reason

    # ══════════════════════════════════════════════════════════════════════════
    # EVALUACIÓN DE INVERSIÓN CON FRACTAL
    # ══════════════════════════════════════════════════════════════════════════
    def _evaluate_inversion(self, expected_color, trigger_number, attempt_number) -> tuple:
        opposite = "NEGRO" if expected_color == "ROJO" else "ROJO"
        required = self.attempt_conditions.get(attempt_number, 2)

        # Obtener fractal scores para ambos colores
        pos_orig = self.original_levels if expected_color == "ROJO" else self.inverted_levels
        pos_opp = self.inverted_levels if expected_color == "ROJO" else self.original_levels
        frac_orig = fractal_score(pos_orig)
        frac_opp = fractal_score(pos_opp)

        prob_mk_opp  = self.ml_filter.markov.predict().get(opposite, 0.5)
        prob_mk_orig = self.ml_filter.markov.predict().get(expected_color, 0.5)
        
        # Ajustar probs de Markov por fractal
        factor_orig = 1.0 + (frac_orig * self.ml_filter.fractal_boost_factor)
        factor_opp = 1.0 + (frac_opp * self.ml_filter.fractal_boost_factor)
        prob_mk_orig_adj = min(1.0, prob_mk_orig * factor_orig)
        prob_mk_opp_adj = min(1.0, prob_mk_opp * factor_opp)

        _, tabla_prob_orig = self.tabla_direction(trigger_number)
        _, tabla_prob_opp  = self.tabla_direction(trigger_number) if opposite != expected_color else (opposite, 0.0)

        feats_opp, _  = self._build_features(opposite)
        feats_orig, _ = self._build_features(expected_color)
        prob_ml_opp  = self.ml_filter.model.predict_proba(feats_opp)  if self.ml_filter.model.ready else 0.5
        prob_ml_orig = self.ml_filter.model.predict_proba(feats_orig) if self.ml_filter.model.ready else 0.5

        levels_opp = self.inverted_levels if expected_color == "ROJO" else self.original_levels
        e20_opp = self._ema(levels_opp, 20)
        e4_opp  = self._ema(levels_opp, 4)
        idx = len(levels_opp) - 1
        tech_opp = (idx >= 0 and e20_opp[idx] is not None and
                    levels_opp[idx] > e20_opp[idx] and
                    e4_opp[idx] is not None and e4_opp[idx] > e20_opp[idx])

        losses_in_exp = sum(1 for s in self.signal_history[-3:]
                            if s["expected"] == expected_color and not s["won"])

        imi_opp  = self._current_imi(opposite)
        imi_orig = self._current_imi(expected_color)
        imi_ok   = (opposite == "ROJO" and imi_opp.is_oversold()) or \
                   (opposite == "NEGRO" and imi_orig.is_overbought())

        cond = 0
        if losses_in_exp >= 2:                             cond += 1
        if prob_ml_opp  > 0.55 and prob_ml_orig  < 0.48:  cond += 1
        if prob_mk_opp_adj  > 0.53 and prob_mk_orig_adj  < 0.50:  cond += 1
        if tech_opp and prob_ml_orig < 0.50:               cond += 1
        if imi_ok:                                         cond += 1
        # Bonus por fractal favorable en opuesto
        if frac_opp > 0.3 and frac_orig < -0.3:           cond += 1

        if cond >= required:
            return opposite, prob_ml_opp, f"Inversión {cond}/{required} (frac_orig={frac_orig:+.1f}, frac_opp={frac_opp:+.1f})"
        return expected_color, prob_ml_orig, f"Sin inversión ({cond}/{required}, frac_orig={frac_orig:+.1f}, frac_opp={frac_opp:+.1f})"

    # ══════════════════════════════════════════════════════════════════════════
    # SEND MESSAGES CON FRACTAL INFO
    # ══════════════════════════════════════════════════════════════════════════
    def _imi_frac_info(self):
        imi   = self._current_imi(self.bet_color)
        pos   = self.original_levels if self.bet_color == "ROJO" else self.inverted_levels
        fracs = detect_fractals(pos[-20:] if len(pos) >= 20 else pos)
        frac_tipo = fracs[-1]["tipo"] if fracs else ""
        frac_score = fractal_score(pos)
        return imi.get_current_imi(), frac_tipo, frac_score

    def _caption_signal(self, trigger, attempt, new_bet=None):
        bet    = new_bet if new_bet is not None else self.bet_sys.current_bet()
        icon   = "🔴" if self.bet_color == "ROJO" else "⚫️"
        step   = self.bet_sys.step + 1
        mk_adj  = self._last_markov_prob * 100   # ya viene ajustado por tabla+fractal
        imi_v, frac_tipo, frac_score = self._imi_frac_info()
        tabla_p  = self.get_prob(trigger, self.bet_color)
        frac_s   = "↑" if frac_tipo == "up" else ("↓" if frac_tipo == "down" else "–")
        # Señal de alineación tabla-Markov
        mk_raw   = self.ml_filter.markov.predict().get(self.bet_color, 0.5) * 100
        align    = "✅" if tabla_p >= 0.50 else "⚠️"
        rev_risk, _ = self._reversal_risk(self.bet_color)
        rev_warn = "\n⚠️ <i>Riesgo de reversión detectado</i>" if rev_risk else ""
        
        # Indicador de boost por fractal
        frac_boost = ""
        if abs(frac_score) > 0.3:
            frac_boost = f" {'📈' if frac_score > 0 else '📉'} Fractal {abs(frac_score)*100:.0f}%"
        
        return (
            f"✅☑️ <b>SEÑAL CONFIRMADA</b> ☑️✅\n\n"
            f"🎰 <b>Juego: {escape_html(self.name)}</b>\n"
            f"👉🏼 <b>Después de: {escape_html(str(trigger))}</b>\n"
            f"🎯 <b>Apostar a: {escape_html(self.bet_color)}</b> {icon}\n\n"
            f"📊 <i>Tabla #{trigger}: {escape_html(self.bet_color)} {tabla_p*100:.0f}% {align}</i>\n"
            f"💠 <i>Markov raw: {mk_raw:.0f}% → ajustado: {mk_adj:.0f}%</i>\n"
            f"📈 <i>IMI: {imi_v:.0f} — {escape_html(self._current_imi(self.bet_color).momentum_tag())}</i>\n"
            f"🔷 <i>Fractal: {frac_s}{frac_boost}</i>\n"
            f"🌀 <i>D'Alembert paso {step}/20</i>\n"
            f"📍 <i>Apuesta: {bet:.2f} usd</i>\n\n"
            f"♻️ <i>Intento {attempt}/{MAX_ATTEMPTS}</i>{rev_warn}"
        )

    def _send_signal(self, trigger, attempt, amx_signal=None):
        if attempt == 1:
            self.signal_is_level1 = (self.bet_sys.step == 0 and not self.recovery_active)
            if self.signal_is_level1:
                self.level1_bankroll = self.bet_sys.bankroll
        imi_v, frac_tipo, frac_score = self._imi_frac_info()
        self._last_frac_score = frac_score
        tabla_p = self.get_prob(trigger, self.bet_color)
        levels  = self.original_levels[:] if self.bet_color == "ROJO" else self.inverted_levels[:]
        chart   = generate_chart(levels, self.spin_history[:], self.bet_color,
                                 markov_prob=self._last_markov_prob, ml_prob=self._last_ml_prob,
                                 imi_value=imi_v, frac_tipo=frac_tipo, tabla_prob=tabla_p)
        mid = tg_send_photo(self.chat_id, self.thread_id, chart, self._caption_signal(trigger, attempt))
        if mid: self.signal_msg_ids.append(mid)
        logger.info(f"[{self.name}] ▶ Señal {self.bet_color} after {trigger} | "
                    f"tabla={tabla_p:.2f} mk={self._last_markov_prob:.2f} frac={frac_score:+.1f} IMI={imi_v:.0f} | "
                    f"intento {attempt}")

    def _send_retry_signal(self, trigger, new_bet, attempt_number):
        imi_v, frac_tipo, frac_score = self._imi_frac_info()
        self._last_frac_score = frac_score
        tabla_p = self.get_prob(trigger, self.bet_color)
        levels  = self.original_levels[:] if self.bet_color == "ROJO" else self.inverted_levels[:]
        chart   = generate_chart(levels, self.spin_history[:], self.bet_color,
                                 markov_prob=self._last_markov_prob, ml_prob=self._last_ml_prob,
                                 imi_value=imi_v, frac_tipo=frac_tipo, tabla_prob=tabla_p)
        mid = tg_send_photo(self.chat_id, self.thread_id, chart,
                            self._caption_signal(trigger, attempt_number, new_bet))
        if mid: self.signal_msg_ids.append(mid)
        logger.info(f"[{self.name}] ♻ Reintento #{attempt_number}: {self.bet_color}, bet={new_bet:.2f}, frac={frac_score:+.1f}")

    def _send_waiting_message(self, trigger, attempt_number):
        icon = "🔴" if self.bet_color == "ROJO" else "⚫️"
        txt  = (f"⚠️ Esperando condiciones para el {'2°' if attempt_number==2 else '3°'} intento\n"
                f"{icon} Apostando a <b>{escape_html(self.bet_color)}</b>")
        imi  = self._current_imi(self.bet_color)
        levels = self.original_levels[:] if self.bet_color == "ROJO" else self.inverted_levels[:]
        chart  = generate_chart(levels, self.spin_history[:], self.bet_color,
                                imi_value=imi.get_current_imi())
        mid = tg_send_photo(self.chat_id, self.thread_id, chart, txt)
        if mid: self.waiting_message_id = mid

    def _send_zero_pause(self):
        icon = "🔴" if self.bet_color == "ROJO" else "⚫️"
        imi  = self._current_imi(self.bet_color)
        caption = (f"🟢 <b>CERO</b> — señal pausada {icon} {escape_html(self.bet_color)}\n"
                   f"🔢 Ceros en señal: {self.zeros_in_current_signal}\n"
                   f"📊 IMI: {imi.get_current_imi():.0f} — {escape_html(imi.momentum_tag())}\n"
                   f"⏳ Esperando próximo giro real...")
        levels = self.original_levels[:] if self.bet_color == "ROJO" else self.inverted_levels[:]
        chart  = generate_chart(levels, self.spin_history[:], self.bet_color,
                                imi_value=imi.get_current_imi())
        mid = tg_send_photo(self.chat_id, self.thread_id, chart, caption)
        if mid: self.zero_pause_msg_id = mid

    def _finalize_signal(self, won, number, real, bet):
        for mid in self.signal_msg_ids: tg_delete(self.chat_id, mid)
        self.signal_msg_ids.clear()
        for attr in ('waiting_message_id', 'zero_pause_msg_id'):
            mid = getattr(self, attr, None)
            if mid:
                tg_delete(self.chat_id, mid)
                setattr(self, attr, None)

        emod = {"ROJO":"🔴","NEGRO":"⚫️","VERDE":"🟢"}
        seq  = " → ".join(emod.get(c,"⚪") for c in self.signal_sequence_colors)
        bk   = self.bet_sys.bankroll
        z    = f"\n🟢 <i>Ceros en señal: {self.zeros_in_current_signal}</i>" if self.zeros_in_current_signal else ""

        # Probabilidad tabla del número resultado
        tabla_res = self.get_prob(number, self.bet_color)
        caption = (
            f"🆔 <i>Secuencia:</i> {seq}\n\n"
            f"{'✅' if won else '❌'} <i>Resultado: {number} {real}</i>\n"
            f"📊 <i>Tabla #{number}: {tabla_res*100:.0f}% para {escape_html(self.bet_color)}</i>\n"
            f"💰 <i>Bankroll: {bk:.2f} usd</i>{z}"
        )
        imi_v, frac_tipo, _ = self._imi_frac_info()
        levels = self.original_levels[:] if self.bet_color == "ROJO" else self.inverted_levels[:]
        chart  = generate_chart(levels, self.spin_history[:], self.bet_color,
                                imi_value=imi_v, frac_tipo=frac_tipo, tabla_prob=tabla_res)
        tg_send_photo(self.chat_id, self.thread_id, chart, caption)

        self.signal_active           = False
        self.signal_sequence_colors.clear()
        self.waiting_for_retry       = False
        self.zeros_in_current_signal = 0
        self._check_recovery()
        self._check_stats()
        self.result_until = time.time() + 7.0
        logger.info(f"[{self.name}] {'WIN' if won else 'LOSS'} #{number} bk={bk:.2f}")

    def _check_recovery(self):
        if not self.recovery_active: return
        if self.bet_sys.bankroll >= self.recovery_target:
            self.consec_losses = 0; self.recovery_active = False
            self.recovery_target = 0.0; self.bet_sys.step = 0

    def _update_amx_positions(self, color):
        last  = self.amx_system.ultimos_puntos[-1] if self.amx_system.ultimos_puntos else 0
        delta = 1 if color == "ROJO" else (-1 if color == "NEGRO" else 0)
        self.amx_system.ultimos_puntos.append(last + delta)
        if len(self.amx_system.ultimos_puntos) > 300:
            self.amx_system.ultimos_puntos = self.amx_system.ultimos_puntos[-200:]

    def _check_stats(self):
        if not self.stats.should_send_stats(): return
        bk = self.bet_sys.bankroll
        w20,l20,t20,e20,bk20 = self.stats.batch_stats(bk)
        self.stats.mark_stats_sent(bk)
        w24,l24,t24,e24,bk24 = self.stats.stats_24h(bk)
        zt = self.zero_tracker
        tg_send_text(self.chat_id, self.thread_id,
            f"👉🏼 <b>ESTADÍSTICAS {t20} SEÑALES</b>\n"
            f"🈯️ W:{w20} 🈲 L:{l20} 🈺 T:{t20} E:{e20}% 💰{bk20:+.2f} usd\n\n"
            f"👉🏼 <b>24 HORAS</b>\n"
            f"🈯️ W:{w24} 🈲 L:{l24} 🈺 T:{t24} E:{e24}% 💰{bk24:+.2f} usd\n\n"
            f"🟢 <b>CERO</b>: {zt.stats_str()}\n"
        )

    # ══════════════════════════════════════════════════════════════════════════
    # EVALUATE BET RESULT CON FRACTAL
    # ══════════════════════════════════════════════════════════════════════════
    def _evaluate_bet_result(self, number, real) -> bool:
        is_win = ((self.bet_color == "ROJO" and real == "ROJO") or
                  (self.bet_color == "NEGRO" and real == "NEGRO"))

        self.signal_sequence_colors.append(real)
        self.signal_history.append({"expected": self.expected_color, "won": is_win})
        if len(self.signal_history) > 50: self.signal_history.pop(0)

        self.ml_filter.update_result(is_win)

        if is_win:
            bet = self.bet_sys.win()
            self.stats.record(True, self.bet_sys.bankroll)
            self.zeros_in_current_signal = 0
            self._finalize_signal(True, number, real, bet)
            return True

        self.attempts_left -= 1
        bet = self.bet_sys.loss()

        if self.attempts_left <= 0:
            self.consec_losses += 1
            if self.consec_losses >= 10:
                self.consec_losses = 0; self.recovery_active = False; self.recovery_target = 0.0
            else:
                self.recovery_active = True
                self.recovery_target = self.level1_bankroll + BASE_BET
            self.stats.record(False, self.bet_sys.bankroll)
            self.zeros_in_current_signal = 0
            self._finalize_signal(False, number, real, bet)
            return True

        # ── Reintento: usar tabla del número actual + Markov + Fractal ──
        if self.signal_msg_ids: tg_delete(self.chat_id, self.signal_msg_ids.pop())
        self.trigger_number  = number
        attempt_number       = MAX_ATTEMPTS - self.attempts_left + 1

        # Re-evaluar dirección usando tabla del número actual
        tabla_color_now, tabla_prob_now = self.tabla_direction(number)
        if tabla_color_now and tabla_color_now != self.expected_color and tabla_prob_now >= 0.54:
            # La tabla del número actual sugiere otro color con fuerza
            logger.info(f"[{self.name}] Tabla sugiere {tabla_color_now} ({tabla_prob_now:.2f}) en reintento")
            self.bet_color = tabla_color_now
        else:
            new_color, _, inv_reason = self._evaluate_inversion(self.expected_color, number, attempt_number)
            if new_color != self.bet_color:
                logger.info(f"[{self.name}] Inversión: {self.bet_color}→{new_color}")
                self.bet_color = new_color

        # Obtener features y fractal score actualizado
        feats, frac_score = self._build_features(self.bet_color)
        tabla_p = self.get_prob(number, self.bet_color)
        rev_risk, rev_reason = self._reversal_risk(self.bet_color)
        
        if rev_risk:
            logger.info(f"[{self.name}] Reintento bloqueado por reversión: {rev_reason}")
        
        # Pasar frac_score explícitamente a should_emit_signal
        emit, mp, mlp, reason = self.ml_filter.should_emit_signal(
            feats, self.bet_color, tabla_prob=tabla_p,
            is_retry=True, attempt_number=attempt_number,
            reversal_risk=rev_risk, frac_score=frac_score)
        
        self._last_markov_prob = mp; self._last_ml_prob = mlp

        if emit:
            self._send_retry_signal(number, self.bet_sys.current_bet(), attempt_number)
        else:
            logger.info(f"[{self.name}] Reintento bloqueado → espera ({reason})")
            self.waiting_for_retry      = True
            self.waiting_attempt_number = attempt_number
            self._send_waiting_message(number, attempt_number)
            self.result_until = time.time() + 1000
        return False

    # ══════════════════════════════════════════════════════════════════════════
    # PROCESS NUMBER — flujo principal CON FRACTAL
    # ══════════════════════════════════════════════════════════════════════════
    def process_number(self, number: int):
        real = REAL_COLOR_MAP.get(number, "VERDE")
        self.spin_history.append({"number": number, "real": real})
        if len(self.spin_history) > 300: self.spin_history.pop(0)

        self.zero_tracker.register_number(number)

        last_o = self.original_levels[-1] if self.original_levels else 0
        last_i = self.inverted_levels[-1] if self.inverted_levels else 0

        if number == 0:
            ref = self.last_nonzero_color
            self.original_levels.append(last_o + (1 if ref == "ROJO" else (-1 if ref else 0)))
            self.inverted_levels.append(last_i + (1 if ref == "NEGRO" else (-1 if ref else 0)))
        else:
            self.original_levels.append(last_o + (1 if real == "ROJO" else -1))
            self.inverted_levels.append(last_i + (1 if real == "NEGRO" else -1))
            self.last_nonzero_color = real

        ml = min(len(self.original_levels), len(self.inverted_levels), len(self.spin_history))
        self.original_levels = self.original_levels[-ml:]
        self.inverted_levels = self.inverted_levels[-ml:]

        self._update_imi()
        self.ml_filter.observe_color(real)
        self._update_amx_positions(real)
        self.amx_system.update_streak(real, self.get_signal(number))

        # ── 1. Cero en señal activa ──
        if self.signal_active and number == 0:
            self.zeros_in_current_signal += 1
            self.zero_tracker.register_zero_in_signal()
            self.result_until = max(self.result_until, time.time() + 30)
            self._send_zero_pause()
            return

        if self.zero_pause_msg_id and number != 0:
            tg_delete(self.chat_id, self.zero_pause_msg_id)
            self.zero_pause_msg_id = None

        # ── 2. Señal activa ──
        if self.signal_active and number != 0:
            if self.waiting_for_retry:
                # Verificar si ya se puede emitir el reintento
                new_color, _, _ = self._evaluate_inversion(
                    self.expected_color, number, self.waiting_attempt_number)
                if new_color != self.bet_color:
                    self.bet_color = new_color
                # Obtener features y fractal score
                feats, frac_score = self._build_features(self.bet_color)
                tabla_p = self.get_prob(number, self.bet_color)
                rev_risk, rev_reason = self._reversal_risk(self.bet_color)
                emit, mp, mlp, reason = self.ml_filter.should_emit_signal(
                    feats, self.bet_color, tabla_prob=tabla_p,
                    is_retry=True, attempt_number=self.waiting_attempt_number,
                    reversal_risk=rev_risk, frac_score=frac_score)
                if emit:
                    self.waiting_for_retry = False
                    if self.waiting_message_id:
                        tg_delete(self.chat_id, self.waiting_message_id)
                        self.waiting_message_id = None
                    self._last_markov_prob = mp; self._last_ml_prob = mlp
                    self._send_retry_signal(number, self.bet_sys.current_bet(),
                                            self.waiting_attempt_number)
                return

            done = self._evaluate_bet_result(number, real)
            if done:
                self.signal_active = False
                self.result_until  = time.time() + 7.0
            return

        # ── 3. Buscar nueva señal ──
        if not self.signal_active and time.time() > self.result_until:
            if number == 0:
                return

            self.signal_msg_ids.clear()
            self.signal_sequence_colors.clear()
            self.waiting_for_retry       = False
            self.zeros_in_current_signal = 0

            # Prioridad de detectores: TABLA → AMX → CLASSIC
            signal = self._tabla_signal(number)
            if not signal:
                signal = self._detect_amx_signal()
            if not signal:
                ec = self.should_activate()
                if ec:
                    signal = {"type":"CLASSIC","expected_color":ec,
                              "probability":self.get_prob(number, ec),"trigger_number":number}
            if not signal:
                return

            base_color  = signal["expected_color"]
            trigger_num = signal["trigger_number"]
            tabla_prob  = signal["probability"]

            # Evaluar inversión (ahora incluye fractal)
            final_color, _, inv_reason = self._evaluate_inversion(base_color, trigger_num, 1)

            # Obtener features y fractal score para el color final
            feats, frac_score = self._build_features(final_color, tabla_prob=self.get_prob(trigger_num, final_color))
            rev_risk, rev_reason = self._reversal_risk(final_color)
            
            if rev_risk:
                logger.info(f"[{self.name}] Señal nueva con riesgo reversión: {rev_reason}")
            
            # Pasar frac_score explícitamente
            emit, mp, mlp, reason = self.ml_filter.should_emit_signal(
                feats, final_color, tabla_prob=self.get_prob(trigger_num, final_color),
                is_retry=False, attempt_number=1, reversal_risk=rev_risk, frac_score=frac_score)

            logger.info(f"[{self.name}] {signal['type']}: {base_color}→{final_color} "
                        f"tabla={tabla_prob:.2f} frac={frac_score:+.1f} {reason} ({inv_reason})")

            if emit:
                self._last_markov_prob = mp; self._last_ml_prob = mlp
                self._last_frac_score = frac_score
                self.signal_active   = True
                self.expected_color  = base_color
                self.bet_color       = final_color
                self.attempts_left   = MAX_ATTEMPTS
                self.trigger_number  = trigger_num
                self._send_signal(trigger_num, 1,
                                  amx_signal=signal if signal["type"] != "CLASSIC" else None)

    def set_mode(self, mode: Literal["tendencia","moderado"]):
        self.amx_system = AMXSignalSystem(mode=mode)
        logger.info(f"[{self.name}] Modo: {mode}")

    async def run_ws(self):
        delay = 5
        while self.running:
            try:
                async with websockets.connect(WS_URL, ping_interval=30,
                                              ping_timeout=60, close_timeout=10) as ws:
                    self.ws = ws; delay = 5
                    logger.info(f"[{self.name}] WS connected")
                    await ws.send(json.dumps({"type":"subscribe","casinoId":CASINO_ID,
                                              "currency":"USD","key":[self.ws_key]}))
                    async for msg in ws:
                        if not self.running: break
                        try: data = json.loads(msg)
                        except: continue
                        if "last20Results" in data and isinstance(data["last20Results"],list):
                            tmp = []
                            for r in data["last20Results"]:
                                gid = r.get("gameId"); num = r.get("result")
                                if gid and num is not None:
                                    try: n = int(num)
                                    except: continue
                                    if 0 <= n <= 36 and gid not in self.anti_block:
                                        tmp.append((gid,n)); self.anti_block.add(gid)
                                        if len(self.anti_block)>1000: self.anti_block.clear()
                            for _, n in reversed(tmp): self.process_number(n)
                        gid = data.get("gameId"); res = data.get("result")
                        if gid and res is not None:
                            try: n = int(res)
                            except: continue
                            if 0 <= n <= 36 and gid not in self.anti_block:
                                self.anti_block.add(gid)
                                if len(self.anti_block)>1000: self.anti_block.clear()
                                self.process_number(n)
            except Exception as e:
                logger.warning(f"[{self.name}] WS error: {e}. Retry in {delay}s")
                await asyncio.sleep(delay)
                delay = min(delay*2, 60)


# ─── FLASK ────────────────────────────────────────────────────────────────────
app = Flask(__name__)

@app.route("/")
def index(): return jsonify({"status":"ok","bot":"AMX V20+ML+IMI+Tabla+Fractal","ts":time.time()})
@app.route("/ping")
def ping(): return jsonify({"pong":True,"ts":time.time()})
@app.route("/health")
def health(): return jsonify({"healthy":True})

import os, urllib.request

async def self_ping_loop():
    port = int(os.environ.get("PORT",10000))
    url  = os.environ.get("RENDER_EXTERNAL_URL",f"http://localhost:{port}")
    while True:
        await asyncio.sleep(300)
        try:
            with urllib.request.urlopen(f"{url}/ping",timeout=10) as r:
                logger.info(f"Self-ping OK: {r.status}")
        except Exception as e:
            logger.warning(f"Self-ping failed: {e}")


# ─── COMANDOS TELEGRAM ────────────────────────────────────────────────────────
engines: dict[str, RouletteEngine] = {}

@bot.message_handler(commands=['start','help'])
def cmd_start(m):
    bot.reply_to(m, """
<b>🎰 AMX V20 + ML + Markov + IMI + Tabla + Fractal</b>

/moderado  — Modo MODERADO
/tendencia — Modo TENDENCIA
/mlstatus  — Estado ML / Markov
/imistatus — Estado IMI + Fractales
/zerostats — Impacto del cero
/markov    — Probabilidades Markov actuales
/fractal   — Estado actual de fractales
/setdecay  — Ajustar decaimiento (0.5-1.0)
/setfrac   — Ajustar boost fractal (0.0-0.3)
/mlreset   — Reset ML (Markov se conserva)
/status    — Estado ruletas
/reset     — Reset estadísticas
""", parse_mode="HTML")

@bot.message_handler(commands=['mlstatus'])
def cmd_mlstatus(m):
    lines = ["<b>🧠 ML / MARKOV</b>\n"]
    for name, e in engines.items():
        mk = e.ml_filter.markov; ml = e.ml_filter.model
        lines.append(
            f"<b>{name}</b>\n"
            f"  {mk.state_info()}\n"
            f"  ML: {ml.summary()} | th={e.ml_filter.ml_threshold:.2f}\n"
            f"  Fractal boost: {e.ml_filter.fractal_boost_factor:.2f}\n"
        )
    bot.reply_to(m, "\n".join(lines), parse_mode="HTML")

@bot.message_handler(commands=['imistatus'])
def cmd_imistatus(m):
    lines = ["<b>📈 IMI + FRACTALES</b>\n"]
    for name, e in engines.items():
        io_ = e.imi_original; ii = e.imi_inverted
        lines.append(
            f"<b>{name}</b>\n"
            f"  IMI ROJO: {io_.get_current_imi():.1f} {io_.momentum_tag()}\n"
            f"  IMI NEGRO: {ii.get_current_imi():.1f} {ii.momentum_tag()}\n"
            f"  Fractal R: {'↑' if fractal_score(e.original_levels)>0 else '↓' if fractal_score(e.original_levels)<0 else '–'}\n"
            f"  Fractal N: {'↑' if fractal_score(e.inverted_levels)>0 else '↓' if fractal_score(e.inverted_levels)<0 else '–'}\n"
        )
    bot.reply_to(m, "\n".join(lines), parse_mode="HTML")

@bot.message_handler(commands=['zerostats'])
def cmd_zerostats(m):
    lines = ["<b>🟢 CERO</b>\n"]
    for name, e in engines.items():
        zt = e.zero_tracker
        lines.append(f"<b>{name}</b>\n  {zt.stats_str()}\n  Riesgo: {zt.zero_risk_score():.2f}\n")
    bot.reply_to(m, "\n".join(lines), parse_mode="HTML")

@bot.message_handler(commands=['markov'])
def cmd_markov(m):
    lines = ["<b>📊 MARKOV</b>\n"]
    for name, e in engines.items():
        p = e.ml_filter.markov.predict()
        lines.append(
            f"<b>{name}</b>\n"
            f"  {e.ml_filter.markov.state_info()}\n"
            f"  ROJO:{p['ROJO']:.3f} NEGRO:{p['NEGRO']:.3f}\n"
        )
    bot.reply_to(m, "\n".join(lines), parse_mode="HTML")

@bot.message_handler(commands=['fractal'])
def cmd_fractal(m):
    lines = ["<b>🔷 FRACTALES</b>\n"]
    for name, e in engines.items():
        frac_r = fractal_score(e.original_levels)
        frac_n = fractal_score(e.inverted_levels)
        boost = e.ml_filter.fractal_boost_factor
        
        # Calcular impacto actual en Markov
        mk = e.ml_filter.markov.predict()
        mk_r_adj = min(1.0, mk['ROJO'] * (1 + frac_r * boost))
        mk_n_adj = min(1.0, mk['NEGRO'] * (1 + frac_n * boost))
        
        lines.append(
            f"<b>{name}</b>\n"
            f"  Fractal ROJO: {frac_r:+.1f} → Markov: {mk['ROJO']:.3f}→{mk_r_adj:.3f}\n"
            f"  Fractal NEGRO: {frac_n:+.1f} → Markov: {mk['NEGRO']:.3f}→{mk_n_adj:.3f}\n"
            f"  Boost factor: {boost:.2f} ({boost*100:.0f}%)\n"
        )
    bot.reply_to(m, "\n".join(lines), parse_mode="HTML")

@bot.message_handler(commands=['setdecay'])
def cmd_setdecay(m):
    try:
        v = float(m.text.split()[1])
        if 0.5 < v <= 1.0:
            for e in engines.values(): e.ml_filter.markov.decay = v
            bot.reply_to(m, f"✅ Decay Markov = {v}")
        else:
            bot.reply_to(m, "❌ Rango: 0.5–1.0")
    except: bot.reply_to(m, "Uso: /setdecay 0.95")

@bot.message_handler(commands=['setfrac'])
def cmd_setfrac(m):
    try:
        v = float(m.text.split()[1])
        if 0.0 <= v <= 0.3:
            for e in engines.values(): e.ml_filter.fractal_boost_factor = v
            bot.reply_to(m, f"✅ Fractal boost = {v} ({v*100:.0f}%)")
        else:
            bot.reply_to(m, "❌ Rango: 0.0–0.3 (0.10 = 10%)")
    except: bot.reply_to(m, "Uso: /setfrac 0.10")

@bot.message_handler(commands=['mlreset'])
def cmd_mlreset(m):
    for e in engines.values():
        e.ml_filter.model = OnlineLogisticRegression(min_samples=10)
    bot.reply_to(m, "🔄 ML reseteado", parse_mode="HTML")

@bot.message_handler(commands=['moderado'])
def cmd_moderado(m):
    for _,e in engines.items(): e.set_mode("moderado")
    bot.reply_to(m, "📊 Modo MODERADO", parse_mode="HTML")

@bot.message_handler(commands=['tendencia'])
def cmd_tendencia(m):
    for _,e in engines.items(): e.set_mode("tendencia")
    bot.reply_to(m, "📈 Modo TENDENCIA", parse_mode="HTML")

@bot.message_handler(commands=['status'])
def cmd_status(m):
    lines = ["<b>📊 ESTADO</b>\n"]
    for name, e in engines.items():
        lines.append(f"<b>{name}</b>: {'📈' if e.amx_system.mode=='tendencia' else '📊'} "
                     f"{'🟢 activa' if e.signal_active else '⚪'}")
    bot.reply_to(m, "\n".join(lines), parse_mode="HTML")

@bot.message_handler(commands=['reset'])
def cmd_reset(m):
    for e in engines.values(): e.stats = Stats()
    bot.reply_to(m, "🔄 Estadísticas reseteadas", parse_mode="HTML")


# ─── MAIN ────────────────────────────────────────────────────────────────────
def run_flask():
    port = int(os.environ.get("PORT",10000))
    app.run(host="0.0.0.0",port=port,debug=False,use_reloader=False)

async def main():
    global engines
    engines = {name: RouletteEngine(name, cfg) for name, cfg in ROULETTE_CONFIGS.items()}
    tasks   = [asyncio.create_task(e.run_ws()) for e in engines.values()]
    tasks.append(asyncio.create_task(self_ping_loop()))

    def poll():
        logger.info("Telegram polling...")
        while True:
            try:
                bot.polling(none_stop=False,interval=1,timeout=20,long_polling_timeout=30)
            except requests.exceptions.ReadTimeout:
                time.sleep(5)
            except telebot.apihelper.ApiTelegramException as e:
                s = str(e)
                if "retry after" in s.lower():
                    try: w = int(''.join(filter(str.isdigit,s)))+1
                    except: w = 30
                    time.sleep(w)
                else: time.sleep(15)
            except Exception: time.sleep(15)

    threading.Thread(target=poll,daemon=True).start()
    logger.info("🎰 Bot iniciado — AMX V20+ML+IMI+Tabla+Fractal+RateLimit")
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    threading.Thread(target=run_flask,daemon=True).start()
    logger.info("Flask started.")
    try: asyncio.run(main())
    except KeyboardInterrupt: logger.info("Bot stopped.")
