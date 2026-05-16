#!/usr/bin/env python3
"""
Auto Roulette — Bot de señales Híbrido Unificado
Sistema de Chance Simples: COLOR, PARIDAD, ZONA

  - Ventana Móvil Markov 60 giros (Orden 3) + ML Cruzado + Resonancia/Ruptura.
  - Análisis de Niveles (+1/-1) con Lógica de Cero (±1) y EMA 20 por categoría.
  - COLOR (ROJO/NEGRO): Lógica Auto-Roulette-Color Moderado exclusivo.
  - PARIDAD / ZONA: Señal solo cuando el nivel EMA del target está POR ENCIMA de EMA20.
  - Gestión Martingala: Apuesta base × 2 en pérdida; reset en ganancia — mínimo 1 ficha.
    · COLOR: máx 2 intentos; PARIDAD/ZONA: máx 3 intentos.
  - Sesiones Sin Límite: Solo se cierran al cumplir la meta.
"""

import asyncio
import json
import logging
import os
import sqlite3
import threading
import time
import urllib.request
from collections import deque, defaultdict
from typing import Optional, Dict, Set
from datetime import datetime, timedelta

import numpy as np
from sklearn.naive_bayes import MultinomialNB
from sklearn.linear_model import SGDClassifier

import telebot
import websockets
from aiohttp import web
import aiohttp
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ─── LOGGING ──────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [AutoRoulette] %(levelname)s %(message)s')
logger = logging.getLogger("AutoRoulette")

for _ln in ['werkzeug', 'flask.app', 'flask', 'urllib3']:
    logging.getLogger(_ln).setLevel(logging.ERROR)
logging.getLogger('websockets').setLevel(logging.CRITICAL)
logging.getLogger('websockets.server').setLevel(logging.CRITICAL)
logging.getLogger('websockets.http11').setLevel(logging.CRITICAL)


class _TeleBotFilter(logging.Filter):
    def filter(self, record):
        msg = record.getMessage()
        if "409" in msg or "terminated by other getUpdates request" in msg:
            logger.warning("⚠️ Conflicto 409 detectado: Otra instancia del bot se está ejecutando.")
            return False
        return True


logging.getLogger('telebot').addFilter(_TeleBotFilter())
logging.getLogger('telebot.apihelper').addFilter(_TeleBotFilter())

# ─── TELEGRAM ─────────────────────────────────────────────────────────────────
TOKEN = "8308452662:AAGZFIZyYsmVR39SvIOSlKD3OY_YNMOsEQU"
CHAT_ID = -1003522684671
STATS_THREAD_ID = 40034

_session = requests.Session()
_retry = Retry(total=5, backoff_factor=1.5, status_forcelist=[429, 500, 502, 503, 504],
               allowed_methods=["GET", "POST"], raise_on_status=False)
_session.mount("https://", HTTPAdapter(max_retries=_retry, pool_connections=10, pool_maxsize=20))
_session.mount("http://", HTTPAdapter(max_retries=_retry, pool_connections=10, pool_maxsize=20))
bot = telebot.TeleBot(TOKEN, threaded=False)
bot.session = _session

# ─── RULETA CONFIGURACIÓN ────────────────────────────────────────────────────
ROULETTES = [
    {"key": 225, "name": "AUTO ROULETTE 🎰"},
]

ROULETTE_LINKS = {
    "AUTO ROULETTE 🎰": "https://1win.lat/casino/play/v_pragmatic:1winautoroulette",
}


def get_roulette_url(name: str) -> Optional[str]:
    clean = name.upper().strip()
    for key, url in ROULETTE_LINKS.items():
        if key.upper() in clean or clean in key.upper():
            return url
    return None


def tg_send_with_button(text: str, roulette_name: str) -> Optional[int]:
    url = get_roulette_url(roulette_name)
    if not url:
        return tg_send(text)
    markup = telebot.types.InlineKeyboardMarkup()
    markup.add(telebot.types.InlineKeyboardButton("🎰 ACCEDER A LA RULETA", url=url))
    msg = _tg_call(bot.send_message, chat_id=CHAT_ID, text=text,
                   parse_mode="HTML", reply_markup=markup,
                   disable_web_page_preview=True)
    return msg.message_id if msg else None


# ─── CONSTANTES ───────────────────────────────────────────────────────────────
WS_URL = "wss://dga.pragmaticplaylive.net/ws"
CASINO_ID = "ppcjd00000007254"
WARMUP_SPINS = 20
MIN_PROB = 0.70
TRAIN_INTERVAL = 50
SESSION_TARGET = 10

SEQUENCE_COLOR = ["ROJO", "NEGRO", "ROJO", "ROJO", "NEGRO", "NEGRO", "ROJO", "NEGRO", "ROJO", "ROJO", "NEGRO", "NEGRO", "ROJO", "NEGRO", "ROJO"]
SEQUENCE_PARIDAD = ["PAR", "IMPAR", "PAR", "PAR", "IMPAR", "IMPAR", "PAR", "IMPAR", "PAR", "PAR", "IMPAR", "IMPAR", "PAR", "IMPAR", "PAR"]
SEQUENCE_ZONA = ["MENOR", "MAYOR", "MENOR", "MENOR", "MAYOR", "MAYOR", "MENOR", "MAYOR", "MENOR", "MENOR", "MAYOR", "MAYOR", "MENOR", "MAYOR", "MENOR"]

EMOJI_MAP = {"ROJO": "🔴", "NEGRO": "⚫️", "PAR": "🟣", "IMPAR": "🟡", "MENOR": "🟤", "MAYOR": "🔵", "CERO": "🟢"}
POS_VALUE = {"COLOR": "ROJO", "PARIDAD": "PAR", "ZONA": "MENOR"}
NEG_VALUE = {"COLOR": "NEGRO", "PARIDAD": "IMPAR", "ZONA": "MAYOR"}

COLOR_MAP = {0: "CERO", 1: "ROJO", 2: "NEGRO", 3: "ROJO", 4: "NEGRO", 5: "ROJO", 6: "NEGRO", 7: "ROJO", 8: "NEGRO", 9: "ROJO", 10: "NEGRO", 11: "NEGRO", 12: "ROJO", 13: "NEGRO", 14: "ROJO", 15: "NEGRO", 16: "ROJO", 17: "NEGRO", 18: "ROJO", 19: "ROJO", 20: "NEGRO", 21: "ROJO", 22: "NEGRO", 23: "ROJO", 24: "NEGRO", 25: "ROJO", 26: "NEGRO", 27: "ROJO", 28: "NEGRO", 29: "NEGRO", 30: "ROJO", 31: "NEGRO", 32: "ROJO", 33: "NEGRO", 34: "ROJO", 35: "NEGRO", 36: "ROJO"}
PARIDAD_MAP = {n: ("PAR" if n > 0 and n % 2 == 0 else ("IMPAR" if n > 0 else "CERO")) for n in range(37)}
ZONA_MAP = {n: ("MENOR" if 1 <= n <= 18 else ("MAYOR" if n >= 19 else "CERO")) for n in range(37)}

# ─── TABLA PREDEFINIDA PARA COLOR (Auto-Roulette-Color) ───────────────────────
COLOR_DATA_AUTO = [
    {"id":0,  "real":"VERDE", "rojo":0.44, "negro":0.56, "senal":"NEGRO"},
    {"id":1,  "real":"ROJO",  "rojo":0.52, "negro":0.44, "senal":"ROJO"},
    {"id":2,  "real":"NEGRO", "rojo":0.40, "negro":0.56, "senal":"NEGRO"},
    {"id":3,  "real":"ROJO",  "rojo":0.40, "negro":0.56, "senal":"NEGRO"},
    {"id":4,  "real":"NEGRO", "rojo":0.40, "negro":0.56, "senal":"NEGRO"},
    {"id":5,  "real":"ROJO",  "rojo":0.52, "negro":0.48, "senal":"ROJO"},
    {"id":6,  "real":"NEGRO", "rojo":0.40, "negro":0.60, "senal":"NEGRO"},
    {"id":7,  "real":"ROJO",  "rojo":0.40, "negro":0.56, "senal":"NEGRO"},
    {"id":8,  "real":"NEGRO", "rojo":0.49, "negro":0.48, "senal":"ROJO"},
    {"id":9,  "real":"ROJO",  "rojo":0.49, "negro":0.48, "senal":"ROJO"},
    {"id":10, "real":"NEGRO", "rojo":0.49, "negro":0.48, "senal":"ROJO"},
    {"id":11, "real":"NEGRO", "rojo":0.48, "negro":0.52, "senal":"NEGRO"},
    {"id":12, "real":"ROJO",  "rojo":0.40, "negro":0.56, "senal":"NEGRO"},
    {"id":13, "real":"NEGRO", "rojo":0.44, "negro":0.56, "senal":"NEGRO"},
    {"id":14, "real":"ROJO",  "rojo":0.49, "negro":0.48, "senal":"ROJO"},
    {"id":15, "real":"NEGRO", "rojo":0.44, "negro":0.56, "senal":"NEGRO"},
    {"id":16, "real":"ROJO",  "rojo":0.52, "negro":0.44, "senal":"ROJO"},
    {"id":17, "real":"NEGRO", "rojo":0.36, "negro":0.60, "senal":"NEGRO"},
    {"id":18, "real":"ROJO",  "rojo":0.44, "negro":0.52, "senal":"NEGRO"},
    {"id":19, "real":"ROJO",  "rojo":0.56, "negro":0.44, "senal":"ROJO"},
    {"id":20, "real":"NEGRO", "rojo":0.48, "negro":0.52, "senal":"NEGRO"},
    {"id":21, "real":"ROJO",  "rojo":0.56, "negro":0.40, "senal":"ROJO"},
    {"id":22, "real":"NEGRO", "rojo":0.52, "negro":0.48, "senal":"ROJO"},
    {"id":23, "real":"ROJO",  "rojo":0.48, "negro":0.49, "senal":"NEGRO"},
    {"id":24, "real":"NEGRO", "rojo":0.44, "negro":0.52, "senal":"NEGRO"},
    {"id":25, "real":"ROJO",  "rojo":0.60, "negro":0.40, "senal":"ROJO"},
    {"id":26, "real":"NEGRO", "rojo":0.56, "negro":0.40, "senal":"ROJO"},
    {"id":27, "real":"ROJO",  "rojo":0.56, "negro":0.40, "senal":"ROJO"},
    {"id":28, "real":"NEGRO", "rojo":0.56, "negro":0.40, "senal":"ROJO"},
    {"id":29, "real":"NEGRO", "rojo":0.56, "negro":0.44, "senal":"ROJO"},
    {"id":30, "real":"ROJO",  "rojo":0.48, "negro":0.49, "senal":"NEGRO"},
    {"id":31, "real":"NEGRO", "rojo":0.48, "negro":0.49, "senal":"NEGRO"},
    {"id":32, "real":"ROJO",  "rojo":0.56, "negro":0.44, "senal":"ROJO"},
    {"id":33, "real":"NEGRO", "rojo":0.44, "negro":0.52, "senal":"NEGRO"},
    {"id":34, "real":"ROJO",  "rojo":0.60, "negro":0.36, "senal":"ROJO"},
    {"id":35, "real":"NEGRO", "rojo":0.56, "negro":0.40, "senal":"ROJO"},
    {"id":36, "real":"ROJO",  "rojo":0.52, "negro":0.44, "senal":"ROJO"},
]
MAX_ATTEMPTS_COLOR = 2   # intentos de la lógica de color (igual que Auto-Roulette)

_ws_clients: Set[asyncio.Queue] = set()


def queue_broadcast(data: dict):
    for q in list(_ws_clients):
        try:
            q.put_nowait(data)
        except Exception:
            pass


_TG_RETRIES = 12


def _tg_call(fn, *a, **kw):
    delay = 2.0
    for attempt in range(1, _TG_RETRIES + 1):
        try:
            return fn(*a, **kw)
        except Exception as e:
            err = str(e)
            if "retry after" in err.lower():
                try:
                    wait = int(''.join(filter(str.isdigit, err))) + 1
                except Exception:
                    wait = 30
                time.sleep(wait)
                continue
            if attempt == _TG_RETRIES:
                return None
            time.sleep(delay)
            delay = min(delay * 2, 60)
    return None


def tg_send(text: str) -> Optional[int]:
    msg = _tg_call(bot.send_message, chat_id=CHAT_ID, text=text, parse_mode="HTML")
    return msg.message_id if msg else None


def tg_send_stats(text: str) -> Optional[int]:
    msg = _tg_call(bot.send_message, chat_id=CHAT_ID, text=text, parse_mode="HTML", message_thread_id=STATS_THREAD_ID)
    return msg.message_id if msg else None


def tg_delete(chat_id: int, message_id: int):
    try:
        _tg_call(bot.delete_message, chat_id=chat_id, message_id=message_id)
    except Exception:
        pass


def fmt_money(val) -> str:
    if val == int(val):
        return f"${int(val)}"
    return f"${val:,.2f}"


# ─── MONEDAS ──────────────────────────────────────────────────────────────────
CURRENCY_RATES = {"USD": 0.10, "MXN": 2.00, "PEN": 0.50, "COP": 500.0, "ARS": 250.0, "CLP": 100.0}
CURRENCY_FLAGS = {"USD": "🇺🇲", "MXN": "🇲🇽", "PEN": "🇵🇪", "COP": "🇨🇴", "ARS": "🇦🇷", "CLP": "🇨🇱"}
CURRENCY_SYMBOLS = {"USD": "$", "MXN": "$", "PEN": "./S", "COP": "$", "ARS": "$", "CLP": "$"}


def fmt_currency_amount(chips: float, currency: str) -> str:
    amount = chips * CURRENCY_RATES[currency]
    sym = CURRENCY_SYMBOLS[currency]
    if currency in ("USD", "MXN", "PEN"):
        return f"{sym} {amount:.2f}"
    return f"{sym} {int(round(amount))}"


def fmt_gestion_signal(chips: float) -> str:
    usd = fmt_currency_amount(chips, "USD")
    mxn = fmt_currency_amount(chips, "MXN")
    pen = fmt_currency_amount(chips, "PEN")
    cop = fmt_currency_amount(chips, "COP")
    ars = fmt_currency_amount(chips, "ARS")
    clp = fmt_currency_amount(chips, "CLP")
    return (f"🇺🇲 USD: {usd} — 🇲🇽 MXN: {mxn}\n"
            f"🇵🇪 PEN: {pen} — 🇨🇴 COP: {cop}\n"
            f"🇦🇷 ARS: {ars} — 🇨🇱 CLP: {clp}")


def fmt_gestion_bankroll(chips: float) -> str:
    usd = fmt_currency_amount(chips, "USD")
    mxn = fmt_currency_amount(chips, "MXN")
    pen = fmt_currency_amount(chips, "PEN")
    cop = fmt_currency_amount(chips, "COP")
    ars = fmt_currency_amount(chips, "ARS")
    clp = fmt_currency_amount(chips, "CLP")
    return (f"🇺🇲 USD: {usd} — 🇲🇽 MXN: {mxn}\n"
            f"🇵🇪 PEN: {pen} — 🇨🇴 COP: {cop}\n"
            f"🇦🇷 ARS: {ars} — 🇨🇱 CLP: {clp}")


# ─── FUNCIONES MATEMÁTICAS (EMA) ─────────────────────────────────────────────
def calculate_ema(data: list, period: int = 20) -> list:
    if len(data) < period:
        return []
    multiplier = 2 / (period + 1)
    ema = [sum(data[:period]) / period]
    for price in data[period:]:
        ema.append((price - ema[-1]) * multiplier + ema[-1])
    return ema


# ─── PREDICTORES ML ──────────────────────────────────────────────────────────
class SmoothedMarkovPredictor:
    def __init__(self, window: int = 60, order: int = 3):
        self.window = window
        self.order = order
        self.transition_counts: dict = {}

    def update(self, sequence: list):
        self.transition_counts = defaultdict(lambda: defaultdict(int))
        recent = sequence[-self.window:]
        if len(recent) < self.order + 1:
            return
        for i in range(len(recent) - self.order):
            key = tuple(recent[i:i + self.order])
            self.transition_counts[key][recent[i + self.order]] += 1

    def predict(self, sequence: list, classes: list) -> Optional[dict]:
        if len(sequence) < self.order:
            return None
        counts = dict(self.transition_counts.get(tuple(sequence[-self.order:]), {}))
        total = sum(counts.values())
        if total < 5:
            return None
        alpha = 1.0
        vocab_size = len(classes)
        probs = {k: (v + alpha) / (total + alpha * vocab_size) for k, v in counts.items()}
        for c in classes:
            if c not in probs:
                probs[c] = alpha / (total + alpha * vocab_size)
        return probs


class OnlineEnsemblePredictor:
    WINDOW = 5
    CLASSES_COLOR = ["ROJO", "NEGRO", "CERO"]
    CLASSES_PARIDAD = ["PAR", "IMPAR", "CERO"]
    CLASSES_ZONA = ["MENOR", "MAYOR", "CERO"]

    def __init__(self):
        self.mnb_color = MultinomialNB(alpha=1.0)
        self.sgd_color = SGDClassifier(loss='log_loss', learning_rate='adaptive', eta0=0.01, penalty='l2', alpha=0.001)
        self.mnb_paridad = MultinomialNB(alpha=1.0)
        self.sgd_paridad = SGDClassifier(loss='log_loss', learning_rate='adaptive', eta0=0.01, penalty='l2', alpha=0.001)
        self.mnb_zona = MultinomialNB(alpha=1.0)
        self.sgd_zona = SGDClassifier(loss='log_loss', learning_rate='adaptive', eta0=0.01, penalty='l2', alpha=0.001)
        self.trained = {"COLOR": False, "PARIDAD": False, "ZONA": False}
        self.sample_count = 0

    def _extract_features(self, hist_c, hist_p, hist_z) -> Optional[list]:
        if len(hist_c) < self.WINDOW:
            return None
        features = []
        for i in range(1, self.WINDOW + 1):
            c, p, z = hist_c[-i], hist_p[-i], hist_z[-i]
            vec_c = [1 if x == c else 0 for x in self.CLASSES_COLOR]
            vec_p = [1 if x == p else 0 for x in self.CLASSES_PARIDAD]
            vec_z = [1 if x == z else 0 for x in self.CLASSES_ZONA]
            features.extend(vec_c + vec_p + vec_z)
        return features

    def partial_train(self, hist_c, hist_p, hist_z, target_c, target_p, target_z):
        feats = self._extract_features(hist_c, hist_p, hist_z)
        if feats is None:
            return
        X = np.array(feats).reshape(1, -1)
        for cat, target, mnb, sgd in [
            ("COLOR", target_c, self.mnb_color, self.sgd_color),
            ("PARIDAD", target_p, self.mnb_paridad, self.sgd_paridad),
            ("ZONA", target_z, self.mnb_zona, self.sgd_zona)
        ]:
            y = np.array([target])
            classes = getattr(self, f"CLASSES_{cat}")
            if not self.trained[cat]:
                mnb.partial_fit(X, y, classes=classes)
                sgd.partial_fit(X, y, classes=classes)
                self.trained[cat] = True
            else:
                mnb.partial_fit(X, y)
                sgd.partial_fit(X, y)
        self.sample_count += 1

    def predict(self, hist_c, hist_p, hist_z, cat: str) -> Optional[dict]:
        if not self.trained[cat]:
            return None
        feats = self._extract_features(hist_c, hist_p, hist_z)
        if feats is None:
            return None
        X = np.array(feats).reshape(1, -1)
        try:
            mnb = getattr(self, f"mnb_{cat.lower()}")
            sgd = getattr(self, f"sgd_{cat.lower()}")
            nb_p = mnb.predict_proba(X)[0]
            sg_p = sgd.predict_proba(X)[0]
            final = 0.5 * nb_p + 0.5 * sg_p
            classes = getattr(self, f"CLASSES_{cat.upper()}")
            return {classes[i]: float(p) for i, p in enumerate(final)}
        except Exception:
            return None


class AMXAnalyzer:
    def adjust_probability(self, base_prob: float, target: str, predictions: dict,
                           recent_hist: list, seq_state,
                           levels: list = None, ema: list = None) -> float:
        cross_boost = 0.0
        target_cat = "COLOR" if target in ["ROJO", "NEGRO"] else ("PARIDAD" if target in ["PAR", "IMPAR"] else "ZONA")

        # ── Cruces Simétricos ──────────────────────────────────────────────────
        if target_cat == "COLOR":
            if target == "ROJO":
                if predictions.get("PARIDAD", {}).get("PAR", 0.5) > 0.55:
                    cross_boost += 0.02
                if predictions.get("ZONA", {}).get("MENOR", 0.5) > 0.55:
                    cross_boost += 0.02
            elif target == "NEGRO":
                if predictions.get("PARIDAD", {}).get("IMPAR", 0.5) > 0.55:
                    cross_boost += 0.02
                if predictions.get("ZONA", {}).get("MAYOR", 0.5) > 0.55:
                    cross_boost += 0.02
        elif target_cat == "PARIDAD":
            if target == "PAR":
                if predictions.get("COLOR", {}).get("NEGRO", 0.5) > 0.55:
                    cross_boost += 0.02
                if predictions.get("ZONA", {}).get("MENOR", 0.5) > 0.55:
                    cross_boost += 0.02
            elif target == "IMPAR":
                if predictions.get("COLOR", {}).get("ROJO", 0.5) > 0.55:
                    cross_boost += 0.02
                if predictions.get("ZONA", {}).get("MAYOR", 0.5) > 0.55:
                    cross_boost += 0.02
        elif target_cat == "ZONA":
            if target == "MENOR":
                if predictions.get("PARIDAD", {}).get("PAR", 0.5) > 0.55:
                    cross_boost += 0.02
                if predictions.get("COLOR", {}).get("NEGRO", 0.5) > 0.55:
                    cross_boost += 0.02
            elif target == "MAYOR":
                if predictions.get("PARIDAD", {}).get("IMPAR", 0.5) > 0.55:
                    cross_boost += 0.02
                if predictions.get("COLOR", {}).get("ROJO", 0.5) > 0.55:
                    cross_boost += 0.02

        # ── Resonancia por Racha Real ─────────────────────────────────────────
        matches = 0
        for i in range(1, min(4, len(recent_hist)) + 1):
            if recent_hist[-i] == target:
                matches += 1
            else:
                break
        if matches == 1:
            cross_boost += 0.03
        elif matches == 2:
            cross_boost += 0.07
        elif matches >= 3:
            cross_boost += 0.12

        # ── Ruptura de Repetición ─────────────────────────────────────────────
        if (len(recent_hist) >= 2 and recent_hist[-1] == recent_hist[-2]
                and recent_hist[-1] != "CERO" and recent_hist[-1] != target):
            cross_boost += 0.04

        # ── Bonus de Alineación con Secuencia Fija ────────────────────────────
        # ROJO: +6% cuando la tabla señala ROJO Y el nivel está por encima de EMA20
        # Resto de targets: +3% estándar
        if target == seq_state.expected():
            if target == "ROJO" and levels and ema and len(levels) > 0 and len(ema) > 0 and levels[-1] > ema[-1]:
                cross_boost += 0.06
            else:
                cross_boost += 0.03

        # ── Boost especial: por encima de EMA20 + racha últimos 5 ───────────
        # Aplica a ROJO, MENOR (nivel orig) y NEGRO, MAYOR (nivel inv)
        if target in ("ROJO", "MENOR", "NEGRO", "MAYOR") and levels and ema:
            above_ema = len(levels) > 0 and len(ema) > 0 and levels[-1] > ema[-1]
            if above_ema:
                last5 = recent_hist[-5:] if len(recent_hist) >= 5 else recent_hist
                streak5 = sum(1 for v in last5 if v == target)
                if streak5 >= 3:
                    cross_boost += 0.06
                if streak5 >= 4:
                    cross_boost += 0.04   # acumulado +0.10 para rachas muy fuertes
                if len(levels) >= 3 and levels[-1] > levels[-2] > levels[-3]:
                    cross_boost += 0.03

        return min(1.0, base_prob + cross_boost)


# ─── GESTIÓN Y SECUENCIAS ────────────────────────────────────────────────────
class Martingala:
    """
    Sistema Martingala clásico.
    - Apuesta base inicial: BASE_BET fichas.
    - WIN  → resetear apuesta a BASE_BET. Retorna True (ciclo completado).
    - LOSS → duplicar apuesta (apuesta × 2). Limitar a MAX_BET fichas.
    - Apuesta mínima: 1 ficha.

    El multiplicador actual se expone como self.multiplier para la UI.
    """
    BASE_BET = 1
    MIN_BET  = 1
    MAX_BET  = 64   # techo: 6 pérdidas consecutivas

    def __init__(self):
        self.current_bet: int = self.BASE_BET
        self.multiplier:  int = 1
        self.total_lost:  int = 0

    @property
    def sequence(self) -> list:
        """Compatibilidad UI — muestra [multiplicador×] actual."""
        return [self.current_bet]

    def get_bet(self) -> int:
        return max(self.MIN_BET, self.current_bet)

    def win(self) -> bool:
        """Reset a la apuesta base. Siempre retorna True."""
        self.current_bet = self.BASE_BET
        self.multiplier  = 1
        self.total_lost  = 0
        return True

    def loss(self):
        """Duplica la apuesta (Martingala clásica)."""
        self.total_lost += self.current_bet
        self.current_bet = min(self.current_bet * 2, self.MAX_BET)
        self.multiplier  = self.current_bet // self.BASE_BET

    def reset(self):
        self.current_bet = self.BASE_BET
        self.multiplier  = 1
        self.total_lost  = 0


class SequenceState:
    def __init__(self, category: str):
        self.category = category
        if category == "COLOR":
            self.sequence = SEQUENCE_COLOR
        elif category == "PARIDAD":
            self.sequence = SEQUENCE_PARIDAD
        else:
            self.sequence = SEQUENCE_ZONA
        self.idx = 0

    def advance(self):
        self.idx = (self.idx + 1) % len(self.sequence)

    def expected(self) -> str:
        return self.sequence[self.idx]

    def initialize_from_last_value(self, last_val: str):
        try:
            idx = self.sequence.index(last_val)
            self.idx = (idx + 1) % len(self.sequence)
        except ValueError:
            self.idx = 0


class GlobalStats:
    def __init__(self):
        self.wins = 0
        self.zeros = 0
        self.losses = 0
        self.consecutive = 0
        self.last_20 = deque(maxlen=20)
        self.signals_processed = 0
        self.global_chips: int = 0
        self.last_report_signals = 0

    def record(self, result_type: str, attempt: int, number: int, val, type_str: str, roulette_name: str):
        self.signals_processed += 1
        if result_type == 'WIN':
            self.wins += 1
            self.consecutive += 1
        elif result_type == 'LOSS':
            self.losses += 1
            self.consecutive = 0
        elif result_type == 'EMPATE':
            self.zeros += 1
            self.consecutive = 0
        self.last_20.append({
            "result": result_type, "attempt": attempt, "number": number,
            "val": val, "type": type_str, "roulette": roulette_name
        })

    def should_send(self) -> bool:
        return (self.signals_processed - self.last_report_signals) >= 20

    def mark_sent(self):
        self.last_report_signals = self.signals_processed

    def get_stats_text(self) -> str:
        total = self.wins + self.zeros + self.losses
        eff = ((self.wins + self.zeros) / total * 100) if total > 0 else 0.0
        text = "📊 RESUMEN — AUTO ROULETTE 🎰\n 🕛 Reporte 12:00 hs\n"
        text += f"► PLACAR = ✅{self.wins} | 🟠{self.zeros} | 🚫{self.losses}\n"
        text += f"► Consecutivas = {self.consecutive}\n"
        text += f"► Assertividade = {eff:.2f}%\n"
        text += f"► Bankroll Global: 💵 {fmt_currency_amount(self.global_chips, 'USD')}\n"
        text += f"► Total señales del día: {total}\n\n📌 Últimas 20 SEÑALES 📌\n"
        for s in reversed(list(self.last_20)):
            a_str = f"🔄 INTENTO #{s['attempt']}"
            if s['result'] == 'WIN':
                b_str = f"💵 +{fmt_currency_amount(s['val'], 'USD')}"
            else:
                b_str = f"💵 -{fmt_currency_amount(s['val'], 'USD')}"
            if s['result'] == 'WIN':
                text += f"✅ WIN #{s['number']} {s['type']} | {a_str} | {b_str}\n\n"
            elif s['result'] == 'EMPATE':
                text += f"🟠 EMPATE #0 ZERO | {a_str} | {b_str}\n\n"
            else:
                text += f"❌ LOSS #{s['number']} {s['type']} | {a_str} | {b_str}\n\n"
        return text


GLOBAL_STATS = GlobalStats()


class SessionManager:
    ARG_UTC_OFFSET = -3

    def __init__(self, engines):
        self.engines = engines
        self.current_idx = 0
        self.session_active = False
        self.session_start_time = 0.0
        self.session_start_chips = 0

    def _now_arg(self):
        return datetime.utcnow() + timedelta(hours=self.ARG_UTC_OFFSET)

    def seconds_to_next_slot(self) -> float:
        now = self._now_arg()
        if now.minute < 30:
            target = now.replace(minute=30, second=0, microsecond=0)
        else:
            target = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
        return max(1.0, (target - now).total_seconds())

    def _start_session(self):
        self.session_active = True
        self.session_start_time = time.time()
        self.session_start_chips = GLOBAL_STATS.global_chips
        engine = self.engines[self.current_idx]
        logger.info(f"[Session] 🟢 Iniciada: {engine.name} | Bankroll: {fmt_currency_amount(self.session_start_chips, 'USD')}")
        queue_broadcast({"type": "session", "status": "active"})

    def _reset_cycle_on_meta(self):
        """Al cumplir la meta, resetea Martingala a apuesta base y continúa la sesión."""
        engine = self.engines[self.current_idx]
        engine.martingala.reset()
        engine.cycle_active = False
        engine.signal_active = False
        engine.attempt = 1
        if engine.active_signal_msg_id:
            tg_delete(CHAT_ID, engine.active_signal_msg_id)
            engine.active_signal_msg_id = None
        if engine.analyzing_msg_id:
            tg_delete(CHAT_ID, engine.analyzing_msg_id)
            engine.analyzing_msg_id = None
        self.session_start_chips = GLOBAL_STATS.global_chips   # resetear baseline
        logger.info(f"[Session] 🎯 Meta cumplida — Martingala reseteado a base, sesión continúa.")
        queue_broadcast({"type": "session", "status": "active"})  # sigue activa

    def _end_session_legacy(self):
        """Mantener por compatibilidad con /reset. No se usa en flujo normal."""
        engine = self.engines[self.current_idx]
        self.session_active = False
        engine.martingala.reset()
        engine.cycle_active = False
        engine.signal_active = False
        engine.attempt = 1
        if engine.active_signal_msg_id:
            tg_delete(CHAT_ID, engine.active_signal_msg_id)
            engine.active_signal_msg_id = None
        if engine.analyzing_msg_id:
            tg_delete(CHAT_ID, engine.analyzing_msg_id)
            engine.analyzing_msg_id = None
        logger.info(f"[Session] 🔴 Terminada: {engine.name}")
        queue_broadcast({"type": "session", "status": "closed"})

    async def session_watchdog(self):
        self._reminder_msg_id: Optional[int] = None
        while True:
            wait = self.seconds_to_next_slot()
            logger.info(f"[Session] ⏳ Esperando {wait/60:.1f} min para el próximo slot (:00 o :30)...")

            REMINDER_SECS = 300
            if wait > REMINDER_SECS + 10:
                await asyncio.sleep(wait - REMINDER_SECS)
                await asyncio.sleep(REMINDER_SECS)
            else:
                await asyncio.sleep(wait)

            self._start_session()

            while self.session_active:
                await asyncio.sleep(1)
                if GLOBAL_STATS.global_chips >= self.session_start_chips + SESSION_TARGET:
                    self._reset_cycle_on_meta()


# ─── MOTOR DE RULETA ─────────────────────────────────────────────────────────
class RouletteEngine:
    def __init__(self, ws_key: int, name: str):
        self.ws_key = ws_key
        self.name = name
        self.db_path = f"main_roulette_{ws_key}.db"
        self.spin_history: list = []
        self.hist_color: list = []
        self.hist_paridad: list = []
        self.hist_zona: list = []

        # Niveles y EMA para las 3 categorías
        self.levels = {"COLOR": [], "PARIDAD": [], "ZONA": []}
        self.inv_levels = {"COLOR": [], "PARIDAD": [], "ZONA": []}
        self.last_non_zero = {"COLOR": None, "PARIDAD": None, "ZONA": None}

        self.CLASSES = {
            "COLOR": ["ROJO", "NEGRO", "CERO"],
            "PARIDAD": ["PAR", "IMPAR", "CERO"],
            "ZONA": ["MENOR", "MAYOR", "CERO"]
        }
        self.seq_states = {cat: SequenceState(cat) for cat in ["COLOR", "PARIDAD", "ZONA"]}
        self.martingala = Martingala()
        self.attempt = 1
        self.cycle_active = False
        self.wait_next_spin = False
        self.analyzing_msg_id = None
        self.markov = {cat: SmoothedMarkovPredictor() for cat in ["COLOR", "PARIDAD", "ZONA"]}
        self.ensemble = OnlineEnsemblePredictor()
        self.amx = AMXAnalyzer()
        self.signal_active = False
        self.active_type = None
        self.active_target = ""
        self.active_chips = 0
        self._last_signal_prob = 0.0
        self.active_signal_msg_id = None
        self.spins_since_train = 0
        self.ws_count = 0
        self.warmup_done = False
        # Color-specific state (Auto-Roulette-Color logic)
        self.signal_mode: str = "moderado"     # "tendencia" | "moderado" — moderado por defecto
        self.color_consecutive_losses: int = 0
        self.color_loss_block_until: float = 0.0
        self._db = self._get_db()
        live = self._load_live_history()
        self.ws_count = live
        self.warmup_done = live >= WARMUP_SPINS

    def _get_db(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.execute("CREATE TABLE IF NOT EXISTS live_spins (id INTEGER PRIMARY KEY AUTOINCREMENT, number INTEGER NOT NULL, ts INTEGER NOT NULL)")
        conn.commit()
        return conn

    def _persist(self, number: int):
        try:
            self._db.execute("INSERT INTO live_spins(number,ts) VALUES(?,?)", (number, int(time.time())))
            self._db.commit()
        except Exception:
            pass

    def _load_live_history(self) -> int:
        try:
            rows = self._db.execute("SELECT number FROM live_spins ORDER BY id ASC").fetchall()
        except Exception:
            return 0
        for (n,) in rows:
            self._update_state(n, persist=False, train_model=False)
        if rows:
            self._train_models()
            self.initialize_sequences_from_history()
        return len(rows)

    def initialize_sequences_from_history(self):
        if any(s["color"] == "NEGRO" for s in reversed(self.spin_history)):
            self.seq_states["COLOR"].initialize_from_last_value("NEGRO")
        if any(s["paridad"] == "IMPAR" for s in reversed(self.spin_history)):
            self.seq_states["PARIDAD"].initialize_from_last_value("IMPAR")
        if any(s["zona"] == "MAYOR" for s in reversed(self.spin_history)):
            self.seq_states["ZONA"].initialize_from_last_value("MAYOR")

    def _train_models(self):
        for cat in ["COLOR", "PARIDAD", "ZONA"]:
            self.markov[cat].update(getattr(self, f"hist_{cat.lower()}"))

    def _update_levels(self, cat: str, val: str):
        pos = POS_VALUE[cat]
        neg = NEG_VALUE[cat]
        lv = self.levels[cat]
        il = self.inv_levels[cat]
        last_l = lv[-1] if lv else 0
        last_il = il[-1] if il else 0
        last_nz = self.last_non_zero[cat]

        if val == "CERO":
            if last_nz:
                ch = 1 if last_nz == pos else -1
                ich = 1 if last_nz == neg else -1
            else:
                ch = 0
                ich = 0
        else:
            ch = 1 if val == pos else -1
            ich = 1 if val == neg else -1
            self.last_non_zero[cat] = val

        lv.append(last_l + ch)
        il.append(last_il + ich)
        if len(lv) > 300:
            lv.pop(0)
            il.pop(0)

    def get_chart_data(self) -> dict:
        out = {}
        for cat in ["COLOR", "PARIDAD", "ZONA"]:
            lv = self.levels[cat][-100:]
            il = self.inv_levels[cat][-100:]
            out[cat] = {
                "levels": lv,
                "inv_levels": il,
                "ema": calculate_ema(lv, 20) if len(lv) >= 20 else [],
                "inv_ema": calculate_ema(il, 20) if len(il) >= 20 else []
            }
        return out

    def _update_state(self, number: int, persist=True, train_model=True):
        c = COLOR_MAP[number]
        p = PARIDAD_MAP[number]
        z = ZONA_MAP[number]
        self.spin_history.append({"number": number, "color": c, "paridad": p, "zona": z})
        self.hist_color.append(c)
        self.hist_paridad.append(p)
        self.hist_zona.append(z)

        for cat, val in [("COLOR", c), ("PARIDAD", p), ("ZONA", z)]:
            self._update_levels(cat, val)
        for cat in ["COLOR", "PARIDAD", "ZONA"]:
            self.seq_states[cat].advance()

        if train_model and number != 0 and len(self.hist_color) > 5:
            self.ensemble.partial_train(self.hist_color, self.hist_paridad, self.hist_zona, c, p, z)
            self.spins_since_train += 1
            if self.spins_since_train >= TRAIN_INTERVAL:
                self._train_models()
                self.spins_since_train = 0
        if persist:
            self._persist(number)

        queue_broadcast({
            "type": "spin", "number": number, "color": c, "paridad": p, "zona": z,
            "bankroll": GLOBAL_STATS.global_chips, "martingala_bet": self.martingala.get_bet(), "martingala_mult": self.martingala.multiplier, "martingala_seq": list(self.martingala.sequence),
            "charts": self.get_chart_data()
        })

    def _get_predictions(self, cat: str) -> dict:
        hist = getattr(self, f"hist_{cat.lower()}")
        classes = self.CLASSES[cat]
        mk_probs = self.markov[cat].predict(hist, classes) or {c: 1 / 3 for c in classes}
        ens_probs = self.ensemble.predict(self.hist_color, self.hist_paridad, self.hist_zona, cat) or {c: 1 / 3 for c in classes}
        return {c: 0.4 * mk_probs.get(c, 0) + 0.6 * ens_probs.get(c, 0) for c in classes}

    def _get_color_entry(self, number: int) -> Optional[dict]:
        for e in COLOR_DATA_AUTO:
            if e["id"] == number:
                return e
        return None

    def _determine_bet_color(self, expected: str) -> str:
        """Igual que determineBetColor del Auto-Roulette-Color."""
        if len(self.spin_history) < 20:
            return expected
        lv  = self.levels["COLOR"]
        ilv = self.inv_levels["COLOR"]
        ema20_o = calculate_ema(lv,  20)
        ema20_i = calculate_ema(ilv, 20)
        last_idx = len(lv) - 1
        last_num = self.spin_history[-1]["number"]
        entry = self._get_color_entry(last_num)
        last_sig = entry["senal"] if entry else None
        if expected == "ROJO":
            if ema20_o and ema20_o[last_idx] is not None and lv[last_idx] < ema20_o[last_idx]:
                return "NEGRO" if last_sig == "NEGRO" else "ROJO"
            return "ROJO"
        else:
            if ema20_i and ema20_i[last_idx] is not None and ilv[last_idx] < ema20_i[last_idx]:
                return "ROJO" if last_sig == "ROJO" else "NEGRO"
            return "NEGRO"

    def _detect_color_signal_tendencia(self, consecutive_losses: int = 0) -> Optional[str]:
        """Lógica shouldActivateSignalTendencia del Auto-Roulette-Color."""
        min_spins = 22 + consecutive_losses * 2
        if len(self.spin_history) < min_spins:
            return None
        last_num = self.spin_history[-1]["number"]
        entry = self._get_color_entry(last_num)
        if not entry or entry["senal"] == "NO APOSTAR":
            return None
        expected = entry["senal"]
        lv  = self.levels["COLOR"]
        ilv = self.inv_levels["COLOR"]
        ema4_o  = calculate_ema(lv,  4)
        ema8_o  = calculate_ema(lv,  8)
        ema20_o = calculate_ema(lv,  20)
        ema4_i  = calculate_ema(ilv, 4)
        ema8_i  = calculate_ema(ilv, 8)
        ema20_i = calculate_ema(ilv, 20)
        last_idx = len(lv) - 1
        required_consec = min(3 + consecutive_losses, 6)

        def check_consec(levels, ema20, ema8, ema4, idx):
            for off in range(required_consec):
                i = idx - (required_consec - 1) + off
                if i < 0: return False
                if ema20[i] is None or levels[i] <= ema20[i]: return False
                if consecutive_losses >= 2 and ema8[i] is not None and levels[i] <= ema8[i]: return False
                if consecutive_losses >= 3 and ema4[i] is not None and levels[i] <= ema4[i]: return False
            return True

        if expected == "ROJO":
            if not check_consec(lv, ema20_o, ema8_o, ema4_o, last_idx): return None
        elif expected == "NEGRO":
            if not check_consec(ilv, ema20_i, ema8_i, ema4_i, last_idx): return None
        return expected

    def _detect_color_signal_moderado(self, consecutive_losses: int = 0) -> Optional[str]:
        """Lógica shouldActivateSignalModerado del Auto-Roulette-Color."""
        min_spins = 12 + consecutive_losses * 3
        if len(self.spin_history) < min_spins:
            return None
        last_idx = len(self.spin_history) - 1
        prev_idx = last_idx - 1
        last_num = self.spin_history[last_idx]["number"]
        entry = self._get_color_entry(last_num)
        if not entry or entry["senal"] == "NO APOSTAR":
            return None
        expected = entry["senal"]
        lv  = self.levels["COLOR"]
        ilv = self.inv_levels["COLOR"]
        ema4_o  = calculate_ema(lv,  4)
        ema8_o  = calculate_ema(lv,  8)
        ema20_o = calculate_ema(lv,  20)
        ema4_i  = calculate_ema(ilv, 4)
        ema8_i  = calculate_ema(ilv, 8)
        ema20_i = calculate_ema(ilv, 20)
        li, pi = len(lv) - 1, len(lv) - 2

        def check_moderate(levels, e4, e8, e20, li, pi):
            if li < 1 or pi < 0: return False
            if e4[li] is None or e8[li] is None or e4[pi] is None or e8[pi] is None: return False
            cross_up = e4[li] > e8[li] and e4[pi] <= e8[pi]
            if not cross_up: return False
            if consecutive_losses >= 1 and e20[li] is not None and e8[li] <= e20[li]: return False
            if consecutive_losses >= 2 and e20[li] is not None and levels[li] <= e20[li]: return False
            if consecutive_losses >= 3:
                prev2 = li - 1
                if prev2 < 0 or e4[prev2] is None or levels[prev2] <= e4[prev2]: return False
                if levels[li] <= e4[li]: return False
            return True

        if expected == "ROJO":
            if check_moderate(lv, ema4_o, ema8_o, ema20_o, li, pi): return expected
        elif expected == "NEGRO":
            if check_moderate(ilv, ema4_i, ema8_i, ema20_i, li, pi): return expected
        return None


    # ─── DETECCIÓN MODERADO: PARIDAD ─────────────────────────────────────────
    def _detect_paridad_signal_moderado(self, consecutive_losses: int = 0) -> Optional[str]:
        """
        Lógica Moderado para PARIDAD (espejo de _detect_color_signal_moderado).
        Cruce EMA4 > EMA8 en el gráfico del target:
          • PAR   → levels["PARIDAD"]     (gráfico positivo)
          • IMPAR → inv_levels["PARIDAD"] (gráfico inverso)
        Filtros adicionales por pérdidas consecutivas (igual que COLOR).
        """
        min_spins = 12 + consecutive_losses * 3
        if len(self.spin_history) < min_spins:
            return None

        lv  = self.levels["PARIDAD"]
        ilv = self.inv_levels["PARIDAD"]
        if len(lv) < 8 or len(ilv) < 8:
            return None

        ema4_o  = calculate_ema(lv,  4)
        ema8_o  = calculate_ema(lv,  8)
        ema20_o = calculate_ema(lv,  20) if len(lv)  >= 20 else []
        ema4_i  = calculate_ema(ilv, 4)
        ema8_i  = calculate_ema(ilv, 8)
        ema20_i = calculate_ema(ilv, 20) if len(ilv) >= 20 else []
        li = len(lv) - 1
        pi = li - 1

        def check_moderate(levels, e4, e8, e20, li, pi):
            if li < 1 or pi < 0: return False
            if not e4 or not e8: return False
            if li >= len(e4) or li >= len(e8): return False
            if pi >= len(e4) or pi >= len(e8): return False
            cross_up = e4[li] > e8[li] and e4[pi] <= e8[pi]
            if not cross_up: return False
            if consecutive_losses >= 1 and e20 and li < len(e20) and e8[li] <= e20[li]: return False
            if consecutive_losses >= 2 and e20 and li < len(e20) and levels[li] <= e20[li]: return False
            if consecutive_losses >= 3:
                prev2 = li - 1
                if prev2 < 0 or prev2 >= len(e4): return False
                if levels[prev2] <= e4[prev2]: return False
                if levels[li] <= e4[li]: return False
            return True

        # Probar PAR (gráfico positivo)
        if check_moderate(lv, ema4_o, ema8_o, ema20_o, li, pi):
            # Confirmar que el nivel PAR también está sobre EMA20
            if not ema20_o or lv[li] > ema20_o[li]:
                return "PAR"

        # Probar IMPAR (gráfico inverso)
        if check_moderate(ilv, ema4_i, ema8_i, ema20_i, li, pi):
            if not ema20_i or ilv[li] > ema20_i[li]:
                return "IMPAR"

        return None

    # ─── DETECCIÓN MODERADO: ZONA ─────────────────────────────────────────────
    def _detect_zona_signal_moderado(self, consecutive_losses: int = 0) -> Optional[str]:
        """
        Lógica Moderado para ZONA (espejo de _detect_color_signal_moderado).
        Cruce EMA4 > EMA8 en el gráfico del target:
          • MENOR → levels["ZONA"]     (gráfico positivo)
          • MAYOR → inv_levels["ZONA"] (gráfico inverso)
        Filtros adicionales por pérdidas consecutivas (igual que COLOR).
        """
        min_spins = 12 + consecutive_losses * 3
        if len(self.spin_history) < min_spins:
            return None

        lv  = self.levels["ZONA"]
        ilv = self.inv_levels["ZONA"]
        if len(lv) < 8 or len(ilv) < 8:
            return None

        ema4_o  = calculate_ema(lv,  4)
        ema8_o  = calculate_ema(lv,  8)
        ema20_o = calculate_ema(lv,  20) if len(lv)  >= 20 else []
        ema4_i  = calculate_ema(ilv, 4)
        ema8_i  = calculate_ema(ilv, 8)
        ema20_i = calculate_ema(ilv, 20) if len(ilv) >= 20 else []
        li = len(lv) - 1
        pi = li - 1

        def check_moderate(levels, e4, e8, e20, li, pi):
            if li < 1 or pi < 0: return False
            if not e4 or not e8: return False
            if li >= len(e4) or li >= len(e8): return False
            if pi >= len(e4) or pi >= len(e8): return False
            cross_up = e4[li] > e8[li] and e4[pi] <= e8[pi]
            if not cross_up: return False
            if consecutive_losses >= 1 and e20 and li < len(e20) and e8[li] <= e20[li]: return False
            if consecutive_losses >= 2 and e20 and li < len(e20) and levels[li] <= e20[li]: return False
            if consecutive_losses >= 3:
                prev2 = li - 1
                if prev2 < 0 or prev2 >= len(e4): return False
                if levels[prev2] <= e4[prev2]: return False
                if levels[li] <= e4[li]: return False
            return True

        # Probar MENOR (gráfico positivo)
        if check_moderate(lv, ema4_o, ema8_o, ema20_o, li, pi):
            if not ema20_o or lv[li] > ema20_o[li]:
                return "MENOR"

        # Probar MAYOR (gráfico inverso)
        if check_moderate(ilv, ema4_i, ema8_i, ema20_i, li, pi):
            if not ema20_i or ilv[li] > ema20_i[li]:
                return "MAYOR"

        return None

    def detect_color_signal(self, signal_mode: str = "moderado", consecutive_losses: int = 0) -> Optional[dict]:
        """Detecta señal para COLOR usando lógica del Auto-Roulette-Color."""
        if signal_mode == "tendencia":
            expected = self._detect_color_signal_tendencia(consecutive_losses)
        else:
            expected = self._detect_color_signal_moderado(consecutive_losses)
        if not expected:
            return None
        bet_color = self._determine_bet_color(expected)
        last_num = self.spin_history[-1]["number"] if self.spin_history else 0
        entry = self._get_color_entry(last_num)
        prob = entry["rojo"] if bet_color == "ROJO" else entry["negro"] if entry else 0.5
        return {
            "type": "COLOR",
            "target": bet_color,
            "prob": prob,
            "chips": self.martingala.get_bet(),
            "mode": signal_mode,
        }

    def detect_signal(self, signal_mode: str = "moderado", color_consecutive_losses: int = 0) -> Optional[dict]:
        """
        Detección unificada — SIEMPRE modo Moderado para las 3 categorías.

        Pipeline por categoría:
          COLOR   → _detect_color_signal_moderado   (EMA4/8/20 cruce, tabla Auto-Roulette)
          PARIDAD → _detect_paridad_signal_moderado  (EMA4/8/20 cruce en gráfico PAR/IMPAR)
          ZONA    → _detect_zona_signal_moderado     (EMA4/8/20 cruce en gráfico MENOR/MAYOR)

        Tras la detección Moderado, la prob de la señal se refina con:
          • Markov ord-3 ventana-60  (40%)
          • Ensemble ML NaiveBayes + SGD (60%)
          • AMX: cruces entre categorías, racha real, ruptura, secuencia
          • Filtro EMA20 obligatorio confirmado dentro de cada _detect_*_moderado

        Se emite la señal con mayor probabilidad ajustada (≥ MIN_PROB).
        """
        # ── Predicciones ML (Markov + Ensemble) para las 3 categorías ─────────
        # Se calculan una vez y se reutilizan para prob_base y cruces AMX.
        all_predictions = {cat: self._get_predictions(cat) for cat in ["COLOR", "PARIDAD", "ZONA"]}

        valid_signals = {}
        best_prob = 0.0
        best_info = "—"

        # ── COLOR: lógica Moderado (Auto-Roulette-Color) ──────────────────────
        color_sig = self.detect_color_signal("moderado", color_consecutive_losses)
        if color_sig:
            valid_signals["COLOR"] = {"target": color_sig["target"], "prob": color_sig["prob"]}
            if color_sig["prob"] > best_prob:
                best_prob = color_sig["prob"]
                best_info = f"COLOR -> {color_sig['target']} ({color_sig['prob']*100:.1f}%)"

        # ── PARIDAD: lógica Moderado + Markov/ML/AMX ─────────────────────────
        par_losses = self.color_consecutive_losses if hasattr(self, "color_consecutive_losses") else 0
        paridad_target = self._detect_paridad_signal_moderado(par_losses)
        if paridad_target:
            lv  = self.levels["PARIDAD"]
            ilv = self.inv_levels["PARIDAD"]
            ref_levels = ilv if paridad_target == "IMPAR" else lv
            ref_ema    = calculate_ema(ref_levels, 20) if len(ref_levels) >= 20 else []
            hist_par   = self.hist_paridad
            base_prob  = all_predictions["PARIDAD"].get(paridad_target, 0.0)
            final_prob = self.amx.adjust_probability(
                base_prob, paridad_target, all_predictions,
                hist_par, self.seq_states["PARIDAD"],
                levels=ref_levels, ema=ref_ema
            )
            final_prob = max(final_prob, MIN_PROB)   # moderado ya valida cruce; garantizar umbral
            logger.debug(f"  📊 PARIDAD/{paridad_target}: base={base_prob:.3f} → AMX={final_prob:.3f} | Moderado ✅")
            valid_signals["PARIDAD"] = {"target": paridad_target, "prob": final_prob}
            if final_prob > best_prob:
                best_prob = final_prob
                best_info = f"PARIDAD -> {paridad_target} ({final_prob*100:.1f}%)"

        # ── ZONA: lógica Moderado + Markov/ML/AMX ────────────────────────────
        zona_target = self._detect_zona_signal_moderado(par_losses)
        if zona_target:
            lv  = self.levels["ZONA"]
            ilv = self.inv_levels["ZONA"]
            ref_levels = ilv if zona_target == "MAYOR" else lv
            ref_ema    = calculate_ema(ref_levels, 20) if len(ref_levels) >= 20 else []
            hist_zona  = self.hist_zona
            base_prob  = all_predictions["ZONA"].get(zona_target, 0.0)
            final_prob = self.amx.adjust_probability(
                base_prob, zona_target, all_predictions,
                hist_zona, self.seq_states["ZONA"],
                levels=ref_levels, ema=ref_ema
            )
            final_prob = max(final_prob, MIN_PROB)
            logger.debug(f"  📊 ZONA/{zona_target}: base={base_prob:.3f} → AMX={final_prob:.3f} | Moderado ✅")
            valid_signals["ZONA"] = {"target": zona_target, "prob": final_prob}
            if final_prob > best_prob:
                best_prob = final_prob
                best_info = f"ZONA -> {zona_target} ({final_prob*100:.1f}%)"

        last_num      = self.spin_history[-1]["number"] if self.spin_history else "?"
        signal_status = "🟢 SEÑAL VALIDA" if valid_signals else "🔴 Sin señal"
        logger.info(f"🎲 Giro #{last_num} | Prob Máx: {best_info} | {signal_status}")

        if not valid_signals:
            return None
        best_cat = max(valid_signals, key=lambda k: valid_signals[k]["prob"])
        best_sig = valid_signals[best_cat]
        return {
            "type":   best_cat,
            "target": best_sig["target"],
            "prob":   best_sig["prob"],
            "chips":  self.martingala.get_bet(),
            "mode":   "moderado"
        }

    def _build_signal_text(self) -> str:
        last_num = self.spin_history[-1]["number"] if self.spin_history else 0
        target = self.active_target
        target_emoji = EMOJI_MAP.get(target, "")
        target_display = f"{target} {target_emoji}"
        if target == "MENOR":
            target_display = f"MENOR (1-18) {target_emoji}"
        elif target == "MAYOR":
            target_display = f"MAYOR (19-36) {target_emoji}"
        elif target == "PAR":
            target_display = f"PARES {target_emoji}"
        elif target == "IMPAR":
            target_display = f"IMPARES {target_emoji}"
        gestion = fmt_gestion_signal(self.active_chips)
        return (f"✅ SEÑAL CONFIRMADA — {target_display} ✅\n\n🎰 {self.name}\n"
                f"👉 ÚLTIMO NÚMERO: {last_num}\n♦️ ENTRAR EN: {target_display}\n"
                f"🔹 INTENTO: {self.attempt} DE {2 if self.active_type == 'COLOR' else 3}\n\n💡 PROBABILIDAD PATRÓN — {self._last_signal_prob:.1f}%\n🎲 MARTINGALA ×{self.martingala.multiplier}\n"
                f"🚨 MONTO DE APUESTA POR PAIS:\n{gestion}")

    def send_signal(self):
        if self.analyzing_msg_id:
            tg_delete(CHAT_ID, self.analyzing_msg_id)
            self.analyzing_msg_id = None
        if self.active_signal_msg_id:
            tg_delete(CHAT_ID, self.active_signal_msg_id)
        msg_id = tg_send_with_button(self._build_signal_text(), self.name)
        if msg_id:
            self.active_signal_msg_id = msg_id
        queue_broadcast({
            "type": "signal", "target": self.active_target, "chips": self.active_chips,
            "attempt": self.attempt, "prob": self._last_signal_prob, "bankroll": GLOBAL_STATS.global_chips,
            "signal_mode": self.signal_mode,
            "martingala_bet": self.martingala.get_bet(), "martingala_mult": self.martingala.multiplier, "martingala_seq": list(self.martingala.sequence)
        })

    def iniciar_senal(self, sig: dict):
        self.cycle_active = True
        self.signal_active = True
        self.active_type = sig["type"]
        self.active_target = sig["target"]
        self._last_signal_prob = sig["prob"] * 100
        self.active_chips = sig["chips"]
        self.attempt = 1
        self.send_signal()

    def resolve(self, number: int):
        c = COLOR_MAP[number]
        p = PARIDAD_MAP[number]
        z = ZONA_MAP[number]
        if self.active_type == "COLOR":
            actual_val = c
        elif self.active_type == "PARIDAD":
            actual_val = p
        else:
            actual_val = z
        won = (actual_val == self.active_target)
        is_zero = (number == 0)
        current_bet = self.active_chips
        # Max intentos para COLOR es 2, para otras categorías es 3
        max_attempts = MAX_ATTEMPTS_COLOR if self.active_type == "COLOR" else 3

        if won:
            cycle_done = self.martingala.win()
            GLOBAL_STATS.global_chips += current_bet
            GLOBAL_STATS.record('WIN', self.attempt, number, current_bet, self.active_type, self.name)
            tg_send(f"✅ WIN {number} — {self.active_type} {self.active_target}\n🎉 ¡Ganaste {fmt_currency_amount(current_bet, 'USD')}!")
            if cycle_done and GLOBAL_STATS.global_chips < SESSION_MANAGER_REF.session_start_chips + SESSION_TARGET:
                tg_send(f"🎉 CICLO LABOUCHERE COMPLETADO 🎉\n🚨 GESTION ACTUAL POR PAIS:\n{fmt_gestion_bankroll(GLOBAL_STATS.global_chips)}")
            # Reset consecutive losses for COLOR on WIN
            if self.active_type == "COLOR":
                self.color_consecutive_losses = 0
                self.color_loss_block_until = 0.0
            self.wait_next_spin = True
            self._send_analyzing_msg()
            self._end_cycle()
            queue_broadcast({"type": "result", "result": "WIN", "number": number, "bankroll": GLOBAL_STATS.global_chips, "martingala_bet": self.martingala.get_bet(), "martingala_seq": list(self.martingala.sequence)})
        else:
            GLOBAL_STATS.global_chips -= current_bet
            self.martingala.loss()
            if self.attempt < max_attempts:
                lost_attempt = self.attempt
                self.attempt += 1
                self.signal_active = False
                self.wait_next_spin = True
                self._send_analyzing_msg()
                queue_broadcast({
                    "type": "result_retry",
                    "lost_attempt": lost_attempt,
                    "attempt": self.attempt,
                    "signal_category": self.active_type,
                    "bankroll": GLOBAL_STATS.global_chips,
                    "martingala_bet": self.martingala.get_bet(), "martingala_seq": list(self.martingala.sequence)
                })
            else:
                GLOBAL_STATS.record('EMPATE' if is_zero else 'LOSS', self.attempt, number, current_bet, self.active_type, self.name)
                msg = "🟠 EMPATE 0" if is_zero else f"❌ LOSS TOTAL {number} — {self.active_type}"
                n_intentos = max_attempts
                tg_send(f"{msg}\n🚨 Racha de {n_intentos} intentos perdida.")
                # Track consecutive losses for COLOR
                if self.active_type == "COLOR":
                    self.color_consecutive_losses = min(self.color_consecutive_losses + 1, 4)
                    self.color_loss_block_until = time.time() + min(8.0 * self.color_consecutive_losses, 30.0)
                self.wait_next_spin = True
                self._send_analyzing_msg()
                self._end_cycle()
                queue_broadcast({
                    "type": "result",
                    "result": "EMPATE" if is_zero else "LOSS",
                    "attempt": self.attempt,
                    "number": number,
                    "bankroll": GLOBAL_STATS.global_chips,
                    "martingala_bet": self.martingala.get_bet(), "martingala_seq": list(self.martingala.sequence)
                })

    def _end_cycle(self):
        self.attempt = 1
        self.cycle_active = False
        self.signal_active = False
        self.active_type = None
        self.active_target = ""
        self.active_signal_msg_id = None
        self._last_signal_prob = 0.0
        self.active_chips = 0
        self._check_stats()

    def _send_analyzing_msg(self):
        if self.analyzing_msg_id:
            tg_delete(CHAT_ID, self.analyzing_msg_id)
        self.analyzing_msg_id = tg_send("🚨 ANALIZADO PATRONES EN CADA GIRO 🚨")

    def _check_stats(self):
        if GLOBAL_STATS.should_send():
            tg_send(GLOBAL_STATS.get_stats_text())
            GLOBAL_STATS.mark_sent()

    def process_spin(self, number: int, session_active: bool):
        try:
            self._update_state(number)
            if not self.warmup_done:
                self.ws_count += 1
                if self.ws_count >= WARMUP_SPINS:
                    self.warmup_done = True
                    tg_send("🟢 <b>AUTO ROULETTE 🎰</b> — Sistema Listo.")
                return
            if not session_active:
                return
            if self.signal_active:
                self.resolve(number)
                return
            if self.wait_next_spin:
                self.wait_next_spin = False
                return
            # Block COLOR if in cooldown after consecutive losses
            if time.time() < self.color_loss_block_until:
                sig = self.detect_signal("moderado", 999)  # skip color (cooldown)
            else:
                sig = self.detect_signal("moderado", self.color_consecutive_losses)
            if not self.signal_active:
                if sig:
                    if self.cycle_active:
                        self.active_type = sig["type"]
                        self.active_target = sig["target"]
                        self._last_signal_prob = sig["prob"] * 100
                        self.active_chips = self.martingala.get_bet()
                        self.signal_active = True
                        self.send_signal()
                    else:
                        self.iniciar_senal(sig)
        except Exception as e:
            logger.error(f"Error: {e}", exc_info=True)
            self._end_cycle()


# ─── WS & MAIN LOGIC ─────────────────────────────────────────────────────────
async def ws_reader(ws_key: int, engine: RouletteEngine, session_mgr: SessionManager):
    reconnect_delay = 5
    initial_loaded = False
    seen_ids: set = set()
    seen_ids_queue: deque = deque(maxlen=200)

    def is_new_id(gid: str) -> bool:
        if not gid or gid in seen_ids:
            return False
        if len(seen_ids_queue) == seen_ids_queue.maxlen:
            seen_ids.discard(seen_ids_queue[0])
        seen_ids.add(gid)
        seen_ids_queue.append(gid)
        return True

    while True:
        try:
            async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=40, close_timeout=10) as ws:
                await ws.send(json.dumps({"type": "subscribe", "key": ws_key, "casinoId": CASINO_ID}))
                logger.info(f"[WS-{ws_key}] ✅ Conectado")
                reconnect_delay = 5

                async def poll_1s():
                    while True:
                        await asyncio.sleep(1)
                        try:
                            await ws.send(json.dumps({"type": "subscribe", "key": ws_key, "casinoId": CASINO_ID}))
                        except Exception:
                            break

                poll_task = asyncio.create_task(poll_1s())
                try:
                    async for raw in ws:
                        try:
                            data = json.loads(raw)
                        except Exception:
                            continue
                        if not isinstance(data, dict):
                            continue

                        results = data.get("last20Results")
                        if results and isinstance(results, list):
                            if not initial_loaded:
                                initial_loaded = True
                                for item in reversed(results):
                                    gid_init = str(item.get("gameId", ""))
                                    if gid_init:
                                        if len(seen_ids_queue) == seen_ids_queue.maxlen:
                                            seen_ids.discard(seen_ids_queue[0])
                                        seen_ids.add(gid_init)
                                        seen_ids_queue.append(gid_init)
                                    try:
                                        n = int(item.get("result", ""))
                                    except Exception:
                                        continue
                                    if 0 <= n <= 36:
                                        engine._update_state(n, persist=False, train_model=True)
                                engine._train_models()
                                engine.initialize_sequences_from_history()
                                if not engine.warmup_done and len(engine.spin_history) >= WARMUP_SPINS:
                                    engine.warmup_done = True
                                    engine.ws_count = len(engine.spin_history)
                                continue

                            latest = results[0]
                            gid = str(latest.get("gameId", ""))
                            if not is_new_id(gid):
                                continue
                            try:
                                n = int(latest.get("result", ""))
                            except Exception:
                                continue
                            if 0 <= n <= 36:
                                engine.process_spin(n, session_mgr.session_active)
                            continue

                        fallback_gid = str(data.get("gameId", "")).strip()
                        if not fallback_gid:
                            for key in ("result", "number", "outcome", "winningNumber"):
                                if key in data:
                                    fallback_gid = f"{ws_key}_{data[key]}_{int(time.time())}"
                                    break
                        if not fallback_gid or not is_new_id(fallback_gid):
                            continue
                        for key in ("result", "number", "outcome", "winningNumber"):
                            if key in data:
                                try:
                                    n = int(data[key])
                                    if 0 <= n <= 36:
                                        engine.process_spin(n, session_mgr.session_active)
                                except Exception:
                                    pass
                                break

                finally:
                    poll_task.cancel()
                    try:
                        await poll_task
                    except asyncio.CancelledError:
                        pass

        except Exception as e:
            logger.warning(f"[WS-{ws_key}] Desconectado: {e}")
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 60)


# ─── WS SERVER PARA HTML EXTERNO (aiohttp — mismo puerto que HTTP) ────────────
async def ws_client_handler(request):
    """
    Maneja conexiones WebSocket entrantes desde el HTML externo.
    Accesible en: wss://azureroulette.onrender.com/ws
    """
    ws = web.WebSocketResponse(heartbeat=30)
    await ws.prepare(request)

    q = asyncio.Queue()
    _ws_clients.add(q)
    logger.info(f"[WSClient] ✅ Cliente conectado: {request.remote}")

    try:
        if engines_global:
            e = engines_global[0]
            state = {
                "type": "init",
                "bankroll": GLOBAL_STATS.global_chips,
                "session_active": session_mgr_global.session_active if session_mgr_global else False,
                "signal_mode": e.signal_mode,
                "martingala_bet": e.martingala.get_bet(), "martingala_mult": e.martingala.multiplier, "martingala_seq": list(e.martingala.sequence),
                "history": e.spin_history[-100:],
                "charts": e.get_chart_data()
            }
            await ws.send_str(json.dumps(state))

        # Enviar mensajes de la cola al cliente; leer mensajes entrantes (ping/pong)
        async def sender():
            while not ws.closed:
                try:
                    data = await asyncio.wait_for(q.get(), timeout=1.0)
                    await ws.send_str(json.dumps(data))
                except asyncio.TimeoutError:
                    continue
                except Exception:
                    break

        sender_task = asyncio.create_task(sender())
        async for msg in ws:
            if msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSE):
                break
            if msg.type == aiohttp.WSMsgType.TEXT:
                try:
                    cmd = json.loads(msg.data)
                    if cmd.get("type") == "set_signal_mode":
                        # Sistema siempre en modo moderado — ignorar cambios
                        queue_broadcast({"type": "signal_mode", "mode": "moderado"})
                except Exception:
                    pass
        sender_task.cancel()

    except Exception as e:
        logger.warning(f"[WSClient] Desconectado: {e}")
    finally:
        _ws_clients.discard(q)
        logger.info(f"[WSClient] ❌ Cliente desconectado: {request.remote}")

    return ws


# ─── HTTP ROUTES ──────────────────────────────────────────────────────────────
engines_global: list = []
session_mgr_global: Optional[SessionManager] = None
SESSION_MANAGER_REF: Optional[SessionManager] = None


async def route_home(request):
    return web.json_response({"status": "ok"})


async def route_ping(request):
    return web.json_response({"status": "pong", "ts": time.time()})


async def route_health(request):
    if not engines_global:
        return web.json_response({"status": "initializing"})
    e = engines_global[0]
    sa = "Active" if session_mgr_global and session_mgr_global.session_active else "Inactive"
    return web.json_response({"bankroll": GLOBAL_STATS.global_chips, "session": sa})


async def _aiohttp_server():
    """Servidor HTTP + WebSocket unificado en el puerto PORT de Render."""
    PORT = int(os.environ.get("PORT", 10000))
    app_http = web.Application()
    app_http.router.add_get("/", route_home)
    app_http.router.add_get("/ping", route_ping)
    app_http.router.add_get("/health", route_health)
    app_http.router.add_get("/ws", ws_client_handler)   # ← punto de conexión WS

    runner = web.AppRunner(app_http)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logger.info(f"[Server] 🌐 HTTP+WebSocket escuchando en puerto {PORT}  →  /ws")
    await asyncio.Future()   # mantener vivo


async def self_ping_loop():
    url = os.environ.get("RENDER_EXTERNAL_URL", "").rstrip("/")
    if not url:
        return
    await asyncio.sleep(30)
    while True:
        try:
            urllib.request.urlopen(f"{url}/ping", timeout=15)
        except Exception:
            pass
        await asyncio.sleep(240)


async def daily_stats_loop():
    while True:
        now_utc = datetime.utcnow()
        now_arg = now_utc + timedelta(hours=-3)
        target = now_arg.replace(hour=12, minute=0, second=0, microsecond=0)
        if now_arg >= target:
            target += timedelta(days=1)
        await asyncio.sleep((target - now_arg).total_seconds())
        tg_send_stats(GLOBAL_STATS.get_stats_text())


# ─── BOT COMMANDS ─────────────────────────────────────────────────────────────
@bot.message_handler(commands=['start', 'help'])
def cmd_start(m):
    bot.reply_to(m, "<b>🎰 AUTO ROULETTE</b>\nSesión ilimitada hasta +$0.80 USD\nMartingala ×2 — Mínimo 1 ficha\n/status /stats /reset", parse_mode="HTML")


@bot.message_handler(commands=['status'])
def cmd_status(m):
    if not engines_global:
        return
    e = engines_global[0]
    sa = "🟢 Activa" if session_mgr_global and session_mgr_global.session_active else "⚪ Inactiva"
    bot.reply_to(m, f"<b>Sesión:</b> {sa}\n<b>Bankroll:</b> 🪙 {fmt_currency_amount(GLOBAL_STATS.global_chips, 'USD')}\n<b>Martingala apuesta:</b> {fmt_currency_amount(e.martingala.get_bet(), 'USD')}", parse_mode="HTML")


@bot.message_handler(commands=['stats'])
def cmd_stats(m):
    tg_send_stats(GLOBAL_STATS.get_stats_text())


@bot.message_handler(commands=['modo'])
def cmd_modo(m):
    bot.reply_to(m, "ℹ️ Sistema fijo en modo <b>MODERADO</b> para todas las categorías.", parse_mode="HTML")
def cmd_reset(m):
    global GLOBAL_STATS
    GLOBAL_STATS = GlobalStats()
    for e in engines_global:
        e.martingala.reset()
        e._end_cycle()
    if session_mgr_global:
        session_mgr_global._end_session_legacy()
    bot.reply_to(m, "🔄 <b>Resetado</b>", parse_mode="HTML")


def run_bot():
    bot.remove_webhook()
    while True:
        try:
            bot.polling(none_stop=True, interval=1, timeout=30)
        except Exception as e:
            if "409" in str(e):
                logger.warning("⚠️ Conflicto 409: Otra instancia del bot está corriendo. Reintentando en 15s...")
                time.sleep(15)
            else:
                logger.error(f"Error en bot.polling: {e}")
                time.sleep(5)


async def main():
    global engines_global, session_mgr_global, SESSION_MANAGER_REF
    engines_global = [RouletteEngine(r["key"], r["name"]) for r in ROULETTES]
    session_mgr_global = SessionManager(engines_global)
    SESSION_MANAGER_REF = session_mgr_global
    threading.Thread(target=run_bot, daemon=True).start()
    await asyncio.sleep(5)
    tasks = [
        asyncio.create_task(session_mgr_global.session_watchdog()),
        asyncio.create_task(daily_stats_loop()),
        asyncio.create_task(self_ping_loop()),
        asyncio.create_task(_aiohttp_server()),        # HTTP + WS en el mismo puerto
    ]
    for i, r in enumerate(ROULETTES):
        tasks.append(asyncio.create_task(ws_reader(r["key"], engines_global[i], session_mgr_global)))
    logger.info("[Main] 🎰 Bot Unificado iniciado")
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
