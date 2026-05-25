#!/usr/bin/env python3
"""
Russian Roulette — Bot DC v33 (Control de señales + Canal secundario + Marcador diario)
===========================================================================
Agregados:
  ⑨ Comandos /detenersenal, /encendersenal, /resetearmarcador.
  ⑩ Canal secundario fijo (-1003613599867) que siempre recibe señales.
  ⑪ Marcador diario (ganancias/pérdidas del día) reiniciable.
  ⑫ Menú de comandos en Telegram (corregido: sin await en set_my_commands).
"""

import asyncio
import logging
import os
import sqlite3
import threading
import time
import math
from datetime import datetime
from collections import deque, defaultdict
from typing import Optional, Dict, Tuple, List

import numpy as np
from sklearn.naive_bayes import MultinomialNB
from sklearn.linear_model import SGDClassifier

import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton, BotCommand
import aiohttp
from flask import Flask, jsonify
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ─── LOGGING ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [RussianDC] %(levelname)s %(message)s'
)
logger = logging.getLogger("RussianDC")
for _ln in ['werkzeug', 'flask.app', 'flask', 'urllib3']:
    logging.getLogger(_ln).setLevel(logging.ERROR)

# ─── CREDENCIALES ─────────────────────────────────────────────────────────────
TOKEN   = "8615799238:AAG2kLg-Ostc4Y4E98HXDIoje_U4F7oqdzU"
CHAT_ID = -1003821352139           # Canal principal
SECONDARY_CHAT_ID = -1003613599867 # Canal secundario (siempre recibe señales)

# ─── URL RULETA ──────────────────────────────────────────────────────────────
ROULETTE_URL = "https://www.casino.org/immersive-roulette"

def roulette_keyboard() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("🎰 JUGAR IMMERSIVE ROULETTE", url=ROULETTE_URL))
    return kb

# ─── TELEGRAM ─────────────────────────────────────────────────────────────────
_session = requests.Session()
_retry = Retry(
    total=5, backoff_factor=1.5,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "POST"], raise_on_status=False
)
_session.mount("https://", HTTPAdapter(max_retries=_retry, pool_connections=10, pool_maxsize=20))
_session.mount("http://",  HTTPAdapter(max_retries=_retry, pool_connections=10, pool_maxsize=20))

try:
    bot = telebot.TeleBot(TOKEN, threaded=False)
    bot.session = _session
    logger.info("✅ Telegram bot initialized")
except Exception as e:
    logger.error(f"❌ Failed to initialize Telegram bot: {e}")
    exit(1)

# ─── CONSTANTES ───────────────────────────────────────────────────────────────
STATS_URL       = "https://crashstake-ulmx.onrender.com"
TARGET_ROULETTE = "IMMERSIVE"
POLL_INTERVAL   = 1
LIVE_DB         = "russian_live.db"

BASE_BET        = 0.10      # USD
MAX_NIVEL       = 6
WARMUP_SPINS    = 25
MIN_PROB_DOZEN = 0.78
MIN_PROB_REPETICION = 0.60
MAX_INTENTOS    = 3
TRAIN_INTERVAL  = 100

# Pesos PHF
PHTML_W         = 0.80
PH_W_COMBINE    = 0.20

# Pesos señal E1
PF_W_NORM  = 0.70; PH_W_NORM  = 0.30
BASE_W_NORM= 0.55; ML_W_NORM  = 0.45

# Estrategias
STRAT_E1 = 1
STRAT_E2_REP = 2
STRAT_E3 = 3
STRAT_E4_COLOR = 4
STRAT_E5_ZONE = 5

# Patrones
PATTERNS_COLOR = [
    (['N','N','N','R','N'], "Negro"),
    (['R','R','R','N','R'], "Rojo"),
    (['N','N','N','R','R','N','N'], "Rojo"),
    (['R','R','R','N','N','R','R'], "Negro"),
]
PATTERNS_ZONE = [
    (['B','B','B','A','B'], "Bajo (1-18)"),
    (['A','A','A','B','A'], "Alto (19-36)"),
    (['B','B','B','A','A','B','B'], "Bajo (1-18)"),
    (['A','A','A','B','B','A','A'], "Alto (19-36)"),
]

GLOBAL_WEIGHT = 0.7
SPECIFIC_WEIGHT = 0.3

# ─── FICHAS POR MONEDA ────────────────────────────────────────────────────────
CURRENCY_CHIPS: Dict[str, float] = {
    "USD": 0.10, "MXN": 2.00, "PEN": 0.40,
    "COP": 500.0, "ARS": 200.0, "CLP": 50.0
}
CURRENCY_SYMBOLS   = {"USD":"$","MXN":"$","PEN":"S/.","COP":"$","ARS":"$","CLP":"$"}
CURRENCY_FLAGS     = {"USD":"🇺🇲","MXN":"🇲🇽","PEN":"🇵🇪","COP":"🇨🇴","ARS":"🇦🇷","CLP":"🇨🇱"}
CURRENCY_DECIMALS  = {"USD":2,"MXN":2,"PEN":2,"COP":0,"ARS":0,"CLP":0}
CURRENCY_MULTIPLIERS = {k: v / BASE_BET for k, v in CURRENCY_CHIPS.items()}

# ─── TABLA PHTML (docenas) ──────────────────────────────────────────────────
DOZEN_TABLE: Dict[int, Dict[str, int]] = {
    0:  {"d1": 32, "d2": 32, "d3": 32},
    1:  {"d1": 28, "d2": 32, "d3": 36},
    2:  {"d1": 36, "d2": 28, "d3": 32},
    3:  {"d1": 24, "d2": 32, "d3": 36},
    4:  {"d1": 32, "d2": 40, "d3": 24},
    5:  {"d1": 40, "d2": 24, "d3": 36},
    6:  {"d1": 32, "d2": 24, "d3": 40},
    7:  {"d1": 36, "d2": 24, "d3": 40},
    8:  {"d1": 32, "d2": 36, "d3": 28},
    9:  {"d1": 28, "d2": 36, "d3": 32},
    10: {"d1": 40, "d2": 32, "d3": 28},
    11: {"d1": 36, "d2": 24, "d3": 36},
    12: {"d1": 32, "d2": 28, "d3": 36},
    13: {"d1": 32, "d2": 28, "d3": 36},
    14: {"d1": 16, "d2": 48, "d3": 32},
    15: {"d1": 36, "d2": 28, "d3": 32},
    16: {"d1": 28, "d2": 32, "d3": 36},
    17: {"d1": 20, "d2": 44, "d3": 32},
    18: {"d1": 32, "d2": 28, "d3": 36},
    19: {"d1": 36, "d2": 28, "d3": 32},
    20: {"d1": 36, "d2": 36, "d3": 28},
    21: {"d1": 24, "d2": 44, "d3": 28},
    22: {"d1": 36, "d2": 36, "d3": 28},
    23: {"d1": 24, "d2": 32, "d3": 40},
    24: {"d1": 44, "d2": 32, "d3": 24},
    25: {"d1": 36, "d2": 24, "d3": 36},
    26: {"d1": 40, "d2": 28, "d3": 32},
    27: {"d1": 32, "d2": 28, "d3": 36},
    28: {"d1": 36, "d2": 28, "d3": 32},
    29: {"d1": 32, "d2": 24, "d3": 40},
    30: {"d1": 36, "d2": 36, "d3": 28},
    31: {"d1": 32, "d2": 36, "d3": 24},
    32: {"d1": 32, "d2": 36, "d3": 28},
    33: {"d1": 28, "d2": 32, "d3": 36},
    34: {"d1": 36, "d2": 28, "d3": 32},
    35: {"d1": 36, "d2": 32, "d3": 24},
    36: {"d1": 28, "d2": 36, "d3": 32},
}

# ─── HELPERS ──────────────────────────────────────────────────────────────────
def get_dozen(n: int) -> int:
    if n == 0: return 0
    return (n - 1) // 12 + 1

def get_column(n: int) -> int:
    if n == 0: return 0
    return ((n - 1) % 3) + 1

def _get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(LIVE_DB, check_same_thread=False)
    conn.execute("""CREATE TABLE IF NOT EXISTS live_spins (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        number INTEGER NOT NULL,
        ts INTEGER NOT NULL
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS repetition_signals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts REAL NOT NULL,
        rep_type TEXT NOT NULL,
        rep_value INTEGER NOT NULL,
        numbers TEXT NOT NULL,
        result TEXT,
        resolved_ts REAL
    )""")
    conn.commit()
    return conn

# ─── TELEGRAM HELPERS (envío dual) ────────────────────────────────────────────
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
                except:
                    wait = 30
                logger.warning(f"⏳ Rate limited. Waiting {wait}s...")
                time.sleep(wait)
                continue
            if attempt == _TG_RETRIES:
                logger.error(f"❌ TG call failed after {_TG_RETRIES} attempts: {err}")
                return None
            time.sleep(delay)
            delay = min(delay * 2, 60)
    return None

def tg_send(chat_id: int, text: str, markup: InlineKeyboardMarkup = None) -> Optional[int]:
    if not text:
        return None
    try:
        msg = _tg_call(
            bot.send_message,
            chat_id=chat_id, text=text,
            parse_mode="HTML", reply_markup=markup,
        )
        if msg:
            logger.info(f"✅ Message sent to {chat_id} (ID: {msg.message_id})")
            return msg.message_id
        return None
    except Exception as e:
        logger.error(f"❌ Exception in tg_send: {e}")
        return None

def tg_send_main(text: str, markup: InlineKeyboardMarkup = None) -> Optional[int]:
    if not engine or engine.signals_paused:
        logger.info("📵 Señales pausadas, no se envía al canal principal.")
        return None
    return tg_send(CHAT_ID, text, markup)

def tg_send_secondary(text: str, markup: InlineKeyboardMarkup = None) -> Optional[int]:
    return tg_send(SECONDARY_CHAT_ID, text, markup)

def tg_send_both(text: str, markup: InlineKeyboardMarkup = None):
    tg_send_secondary(text, markup)
    if engine and not engine.signals_paused:
        tg_send(CHAT_ID, text, markup)

def tg_delete(chat_id: int, message_id: int):
    try:
        _tg_call(bot.delete_message, chat_id=chat_id, message_id=message_id)
    except Exception as e:
        logger.warning(f"⚠️ Failed to delete message: {e}")

# ─── CLIENTE STATS ────────────────────────────────────────────────────────────
class StatsClient:
    def __init__(self):
        self.stats_dozen  = {}
        self.stats_column = {}
        self.stats_color  = {}
        self.stats_zone   = {}
        self.last_20      = []
        self.last_colors  = []
        self.last_zones   = []
        self.total_spins  = 0
        self.connected    = False
        self.poll_count   = 0
        self.last_poll_ok = 0.0
        self.last_error   = None

    def update(self, data: dict):
        try:
            self.last_20      = data.get("last_20",     self.last_20)
            self.last_colors  = data.get("last_colors", self.last_colors)
            self.last_zones   = data.get("last_zones",  self.last_zones)
            self.stats_dozen  = data.get("stats_dozen", self.stats_dozen)
            self.stats_column = data.get("stats_column", self.stats_column)
            self.stats_color  = data.get("stats_color", self.stats_color)
            self.stats_zone   = data.get("stats_zone", self.stats_zone)
            self.total_spins  = data.get("total_spins", self.total_spins)
            self.connected    = True
            self.poll_count  += 1
            self.last_poll_ok = time.time()
            self.last_error   = None
        except Exception as e:
            self.last_error = str(e)

    def get_ph_probs_raw(self, number: int) -> Optional[Dict]:
        num_key = str(number)
        if num_key not in self.stats_dozen:
            return None
        data = self.stats_dozen[num_key]
        if data.get("total", 0) < 10:
            return None
        return {
            1: data.get("1", 0) / 100.0,
            2: data.get("2", 0) / 100.0,
            3: data.get("3", 0) / 100.0,
        }

    def get_ph_pair(self, number: int) -> Optional[Dict]:
        probs = self.get_ph_probs_raw(number)
        if probs is None:
            return None
        sorted_p = sorted(probs.items(), key=lambda x: x[1], reverse=True)
        if sorted_p[0][1] == 0:
            return None
        pair    = tuple(sorted([sorted_p[0][0], sorted_p[1][0]]))
        missing = list({1, 2, 3} - set(pair))[0]
        prob    = sorted_p[0][1] + sorted_p[1][1]
        return {"pair": pair, "missing": missing, "prob": prob}

    def get_column_probs(self, number: int) -> Optional[Dict]:
        num_key = str(number)
        if num_key not in self.stats_column:
            return None
        data = self.stats_column[num_key]
        if data.get("total", 0) < 10:
            return None
        return {
            1: data.get("1", 0) / 100.0,
            2: data.get("2", 0) / 100.0,
            3: data.get("3", 0) / 100.0,
        }

    async def get_pattern_stats(self, pattern_type: str, pattern_seq: str) -> dict:
        url = f"{STATS_URL}/patterns/{TARGET_ROULETTE}/stats?type={pattern_type}&pattern={pattern_seq}"
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url, timeout=5) as resp:
                    if resp.status == 200:
                        return await resp.json()
            except:
                pass
        return {"total":0, "wins":0, "losses":0, "win_rate":0.0, "bet":""}

    async def get_sequence_history(self, numbers: List[int], pattern_type: str) -> List[dict]:
        numbers_str = ",".join(str(n) for n in numbers)
        url = f"{STATS_URL}/patterns/{TARGET_ROULETTE}/sequence?numbers={numbers_str}&type={pattern_type}"
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url, timeout=5) as resp:
                    if resp.status == 200:
                        return await resp.json()
            except:
                pass
        return []

# ─── EMA ──────────────────────────────────────────────────────────────────────
def calc_ema(data, period):
    if len(data) < period:
        return [None] * len(data)
    mult = 2 / (period + 1)
    out  = [None] * (period - 1)
    prev = sum(data[:period]) / period
    out.append(prev)
    for v in data[period:]:
        prev = v * mult + prev * (1 - mult)
        out.append(prev)
    return out

def ema_signal(levels, mode="moderado"):
    if len(levels) < 20:
        return False
    e4, e8, e20 = calc_ema(levels, 4), calc_ema(levels, 8), calc_ema(levels, 20)
    li = len(levels) - 1
    if any(v is None for v in [e4[li], e8[li], e20[li]]):
        return False
    cur  = levels[li]
    ce4, ce8, ce20 = e4[li], e8[li], e20[li]
    pe4  = e4[li-1]  if li > 0 and e4[li-1]  is not None else ce4
    pe8  = e8[li-1]  if li > 0 and e8[li-1]  is not None else ce8
    pe20 = e20[li-1] if li > 0 and e20[li-1] is not None else ce20
    if mode == "tendencia":
        return (pe4 <= pe20 and ce4 > ce20) or (cur > ce4 and cur > ce8 and cur > ce20)
    else:
        vp = False
        if len(levels) >= 3:
            a, b, c = levels[-3], levels[-2], levels[-1]
            vp = (b < a) and (b < c) and (c > a)
        return (pe4 <= pe8 and ce4 > ce8) or (pe8 <= pe20 and ce8 > ce20) or \
               (cur > ce4 and cur > ce8) or vp

def ema_trend_str(levels) -> str:
    if len(levels) < 20:
        return "neutral"
    e4  = calc_ema(levels, 4)
    e8  = calc_ema(levels, 8)
    e20 = calc_ema(levels, 20)
    li  = len(levels) - 1
    v4, v8, v20 = e4[li], e8[li], e20[li]
    if any(v is None for v in [v4, v8, v20]):
        return "neutral"
    cur = levels[li]
    if cur > v4 and v4 > v8 and v8 > v20:
        return "bull"
    if cur < v4 and v4 < v8 and v8 < v20:
        return "bear"
    return "neutral"

def ema_trend_pair(trend: str) -> Dict:
    if trend == "bull":
        return {"pair": (1, 2), "missing": 3, "label": "ALCISTA"}
    if trend == "bear":
        return {"pair": (2, 3), "missing": 1, "label": "BAJISTA"}
    return {"pair": (1, 3), "missing": 2, "label": "NEUTRAL"}

# ─── MARKOV y ENSEMBLE ML ─────────────────────────────────────────────────────
class SmoothedMarkovPredictor:
    def __init__(self, window=60, order=2):
        self.window            = window
        self.order             = order
        self.transition_counts = {}

    def update(self, sequence):
        self.transition_counts = defaultdict(lambda: defaultdict(int))
        recent = sequence[-self.window:]
        if len(recent) < self.order + 1:
            return
        for i in range(len(recent) - self.order):
            self.transition_counts[tuple(recent[i:i+self.order])][recent[i+self.order]] += 1

    def predict(self, sequence):
        if len(sequence) < self.order:
            return None
        counts = dict(self.transition_counts.get(tuple(sequence[-self.order:]), {}))
        total  = sum(counts.values())
        if total < 10:
            return None
        alpha = 2.0; vs = 3
        probs = {k: (v + alpha) / (total + alpha * vs) for k, v in counts.items()}
        for c in [1, 2, 3]:
            if c not in probs:
                probs[c] = alpha / (total + alpha * vs)
        return probs

class OnlineEnsemblePredictor:
    WINDOW  = 5
    CLASSES = [1, 2, 3]

    def __init__(self):
        self.mnb     = MultinomialNB(alpha=2.0, class_prior=[0.333, 0.333, 0.333])
        self.sgd     = SGDClassifier(
            loss='log_loss', learning_rate='adaptive', eta0=0.005,
            penalty='l2', alpha=0.01, epsilon=0.2
        )
        self.trained = False

    def _extract_features(self, hist_d, pf_pd, ph_pd):
        if len(hist_d) < self.WINDOW:
            return None
        features = []
        for i in range(1, self.WINDOW + 1):
            d   = hist_d[-i]
            vec = [0, 0, 0]
            vec[d - 1] = 1
            features.extend(vec)
        for pair in (pf_pd, ph_pd):
            vec = [0, 0, 0]
            for x in pair:
                vec[x - 1] = 1
            features.extend(vec)
        return features

    def partial_train(self, hist_d, target, pf_d, ph_d):
        feats = self._extract_features(hist_d[:-1], pf_d, ph_d)
        if feats is None:
            return
        X = np.array(feats).reshape(1, -1)
        y = np.array([target])
        if not self.trained:
            self.mnb.partial_fit(X, y, classes=self.CLASSES)
            self.sgd.partial_fit(X, y, classes=self.CLASSES)
            self.trained = True
        else:
            self.mnb.partial_fit(X, y)
            self.sgd.partial_fit(X, y)

    def predict(self, hist_d, pf_d, ph_d):
        if not self.trained:
            return None
        feats = self._extract_features(hist_d, pf_d, ph_d)
        if feats is None:
            return None
        X = np.array(feats).reshape(1, -1)
        try:
            pm = dict(zip(self.CLASSES, self.mnb.predict_proba(X)[0]))
            ps = dict(zip(self.CLASSES, self.sgd.predict_proba(X)[0]))
            return {c: 0.5 * pm[c] + 0.5 * ps[c] for c in self.CLASSES}
        except Exception:
            return None

# ─── GESTOR DE FICHAS Y NIVELES ───────────────────────────────────────────────
class GestorDocenas:
    def __init__(self):
        self.nivel      = 1
        self.b0         = 0.0
        self.debt_stack = []

    def iniciar_senal(self, balance: float):
        self.b0 = balance

    def get_bet(self, intento: int = 1) -> float:
        if intento == 1:
            return self.nivel * BASE_BET
        return 3 * self.nivel * BASE_BET

    def registrar_perdida_senal(self):
        self.debt_stack.append(self.b0)
        self.nivel = self.nivel + 1 if self.nivel < MAX_NIVEL else 1
        logger.info(f"[RussianDC] 📋 Deuda | B0={self.b0:.2f} | Pila={len(self.debt_stack)} | Nivel→{self.nivel}")

    def verificar_recuperacion(self, balance: float):
        while self.debt_stack:
            if balance >= self.debt_stack[-1] + BASE_BET * 0.9:
                self.debt_stack.pop()
            else:
                break
        if not self.debt_stack:
            self.nivel = 1

# ─── SIGNAL LEARNER ───────────────────────────────────────────────────────────
class SignalLearner:
    MAX_HISTORY = 500
    WINDOW      = 50
    MIN_SAMPLES = 5

    _COLS = [
        "id", "ts", "strategy", "pair", "missing", "prob",
        "intento_start", "nivel", "pf_prob", "phf_prob",
        "ema_trend", "last_number", "dozen_seq_5",
        "result", "intento_fin", "reason"
    ]

    def __init__(self, db: sqlite3.Connection):
        self.db         = db
        self.history    = deque(maxlen=self.MAX_HISTORY)
        self.pending_id: Optional[int] = None
        self._init_db()
        self._load_history()

    def _init_db(self):
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS signal_log (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                ts           REAL    NOT NULL,
                strategy     INTEGER,
                pair         TEXT,
                missing      INTEGER,
                prob         REAL,
                intento_start INTEGER,
                nivel        INTEGER,
                pf_prob      REAL,
                phf_prob     REAL,
                ema_trend    TEXT,
                last_number  INTEGER,
                dozen_seq_5  TEXT,
                result       TEXT,
                intento_fin  INTEGER,
                reason       TEXT
            )
        """)
        self.db.commit()

    def _row_to_dict(self, row) -> dict:
        d = dict(zip(self._COLS, row))
        raw = d.get("pair", "")
        try:
            d["pair"] = tuple(int(x) for x in raw.split(",") if x)
        except Exception:
            d["pair"] = ()
        return d

    def _load_history(self):
        try:
            rows = self.db.execute(
                "SELECT * FROM signal_log WHERE result IS NOT NULL ORDER BY id DESC LIMIT ?",
                (self.MAX_HISTORY,)
            ).fetchall()
            for row in reversed(rows):
                self.history.append(self._row_to_dict(row))
            logger.info(f"[Learner] 📚 Historial cargado: {len(self.history)} señales previas")
        except Exception as e:
            logger.warning(f"[Learner] ⚠️ Error cargando historial: {e}")

    def register_signal(self, strategy: int, pair: tuple, missing: int,
                        prob: float, nivel: int, pf_prob: float,
                        phf_prob: float, ema_trend: str,
                        last_number: int, dozen_seq_5: list):
        try:
            cur = self.db.execute(
                """INSERT INTO signal_log
                   (ts, strategy, pair, missing, prob, intento_start, nivel,
                    pf_prob, phf_prob, ema_trend, last_number, dozen_seq_5)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    time.time(), strategy,
                    ",".join(str(x) for x in sorted(pair)),
                    missing, round(prob, 6), 1, nivel,
                    round(pf_prob, 6), round(phf_prob, 6),
                    ema_trend, last_number,
                    ",".join(str(x) for x in dozen_seq_5)
                )
            )
            self.db.commit()
            self.pending_id = cur.lastrowid
            logger.debug(f"[Learner] 📝 Señal registrada ID={self.pending_id} strat={strategy} par={pair} prob={prob:.0%}")
        except Exception as e:
            logger.warning(f"[Learner] ⚠️ Error registrando señal: {e}")

    def resolve(self, result: str, intento_fin: int, reason: str = ""):
        if self.pending_id is None:
            return
        try:
            self.db.execute(
                "UPDATE signal_log SET result=?, intento_fin=?, reason=? WHERE id=?",
                (result, intento_fin, reason, self.pending_id)
            )
            self.db.commit()
            row = self.db.execute("SELECT * FROM signal_log WHERE id=?", (self.pending_id,)).fetchone()
            if row:
                self.history.append(self._row_to_dict(row))
            logger.info(f"[Learner] {'✅' if result=='WIN' else '❌'} ID={self.pending_id} → {result} Int.{intento_fin} | {reason}")
        except Exception as e:
            logger.warning(f"[Learner] ⚠️ Error resolviendo señal: {e}")
        finally:
            self.pending_id = None

    def _recent(self) -> List[dict]:
        completed = [s for s in self.history if s.get("result") in ("WIN", "LOSS")]
        return list(completed)[-self.WINDOW:]

    def _win_rate(self, subset: list) -> Optional[float]:
        if len(subset) < self.MIN_SAMPLES:
            return None
        return sum(1 for s in subset if s["result"] == "WIN") / len(subset)

    @staticmethod
    def _adj(win_rate: Optional[float], scale: float) -> float:
        if win_rate is None:
            return 0.0
        return round((win_rate - 0.5) * 2.0 * scale, 4)

    def strat_adjustment(self, strategy: int) -> float:
        subset = [s for s in self._recent() if s.get("strategy") == strategy]
        return self._adj(self._win_rate(subset), 0.08)

    def pair_adjustment(self, pair: tuple) -> float:
        key = tuple(sorted(pair))
        subset = [s for s in self._recent() if tuple(sorted(s.get("pair", ()))) == key]
        return self._adj(self._win_rate(subset), 0.06)

    def trend_adjustment(self, ema_trend: str) -> float:
        subset = [s for s in self._recent() if s.get("ema_trend") == ema_trend]
        return self._adj(self._win_rate(subset), 0.05)

    def nivel_adjustment(self, nivel: int) -> float:
        subset = [s for s in self._recent() if s.get("nivel") == nivel]
        return self._adj(self._win_rate(subset), 0.04)

    def get_adjustment(self, strategy: int, pair: tuple,
                       ema_trend: str, nivel: int) -> Tuple[float, str]:
        s_adj = self.strat_adjustment(strategy)
        p_adj = self.pair_adjustment(pair)
        t_adj = self.trend_adjustment(ema_trend)
        n_adj = self.nivel_adjustment(nivel)
        total = round(max(-0.23, min(0.23, s_adj + p_adj + t_adj + n_adj)), 4)

        parts = []
        if abs(s_adj) >= 0.005: parts.append(f"Strat:{s_adj:+.3f}")
        if abs(p_adj) >= 0.005: parts.append(f"Par:{p_adj:+.3f}")
        if abs(t_adj) >= 0.005: parts.append(f"EMA:{t_adj:+.3f}")
        if abs(n_adj) >= 0.005: parts.append(f"Nv:{n_adj:+.3f}")
        detail = " | ".join(parts) if parts else "sin ajuste aún"
        return total, detail

    def get_summary(self, n: int = 30) -> str:
        recent = self._recent()[-n:]
        total_db = 0
        try:
            row = self.db.execute("SELECT COUNT(*) FROM signal_log WHERE result IS NOT NULL").fetchone()
            total_db = row[0] if row else 0
        except Exception:
            pass

        if not recent:
            return (
                "🧠 <b>Aprendizaje adaptativo activo</b>\n\n"
                "Aún no hay señales resueltas.\n"
                f"tras {self.MIN_SAMPLES} señales por categoría."
            )

        total = len(recent)
        wins  = sum(1 for s in recent if s["result"] == "WIN")
        eff   = wins / total * 100 if total > 0 else 0

        si = {STRAT_E1:"🅐E1", STRAT_E2_REP:"🅑E2(Rep)", STRAT_E3:"🅒E3", STRAT_E4_COLOR:"🅓E4(Color)", STRAT_E5_ZONE:"🅔E5(Zona)"}
        lines = [
            f"🧠 <b>APRENDIZAJE ADAPTATIVO</b>",
            f"Base de datos: {total_db} señales totales",
            f"Ventana activa: {total} últimas | Aciertos: {wins}/{total} ({eff:.1f}%)\n",
            "<b>📊 Por estrategia:</b>"
        ]

        for st in [STRAT_E1, STRAT_E2_REP, STRAT_E3, STRAT_E4_COLOR, STRAT_E5_ZONE]:
            sb  = [s for s in recent if s.get("strategy") == st]
            if not sb:
                lines.append(f"  {si[st]}: sin datos")
                continue
            sw  = sum(1 for s in sb if s["result"] == "WIN")
            wr  = sw / len(sb) * 100
            adj = self.strat_adjustment(st)
            bar = "▓" * int(wr / 10) + "░" * (10 - int(wr / 10))
            lines.append(f"  {si[st]}: {sw}/{len(sb)} ({wr:.0f}%) {bar} adj:{adj:+.3f}")

        # Por par
        pair_stats: Dict[str, list] = defaultdict(lambda: [0, 0])
        for s in recent:
            p = s.get("pair", ())
            if len(p) == 2 and s.get("strategy") in (STRAT_E1, STRAT_E3):
                key = f"D{p[0]}+D{p[1]}"
                pair_stats[key][1] += 1
                if s["result"] == "WIN":
                    pair_stats[key][0] += 1
        if pair_stats:
            lines.append("\n<b>🎯 Por par de docenas:</b>")
            for pk in sorted(pair_stats):
                pw, pt = pair_stats[pk]
                wr_p   = pw / pt * 100 if pt else 0
                lines.append(f"  {pk}: {pw}/{pt} ({wr_p:.0f}%)")

        # Por tendencia EMA
        trend_stats: Dict[str, list] = defaultdict(lambda: [0, 0])
        for s in recent:
            t = s.get("ema_trend", "neutral")
            trend_stats[t][1] += 1
            if s["result"] == "WIN":
                trend_stats[t][0] += 1
        if trend_stats:
            lines.append("\n<b>📈 Por tendencia EMA:</b>")
            trend_labels = {"bull": "🟢 Bull", "neutral": "⬜ Neutral", "bear": "🔴 Bear"}
            for t in ["bull", "neutral", "bear"]:
                if t not in trend_stats:
                    continue
                tw, tt = trend_stats[t]
                wr_t   = tw / tt * 100 if tt else 0
                adj    = self.trend_adjustment(t)
                lines.append(f"  {trend_labels[t]}: {tw}/{tt} ({wr_t:.0f}%) adj:{adj:+.3f}")

        # Por nivel
        nivel_stats: Dict[int, list] = defaultdict(lambda: [0, 0])
        for s in recent:
            nv = s.get("nivel", 1)
            nivel_stats[nv][1] += 1
            if s["result"] == "WIN":
                nivel_stats[nv][0] += 1
        if nivel_stats:
            lines.append("\n<b>🎚 Por nivel de apuesta:</b>")
            for nv in sorted(nivel_stats):
                nw, nt = nivel_stats[nv]
                wr_n   = nw / nt * 100 if nt else 0
                adj    = self.nivel_adjustment(nv)
                lines.append(f"  Nv.{nv}: {nw}/{nt} ({wr_n:.0f}%) adj:{adj:+.3f}")

        # Últimas 6 señales
        lines.append("\n<b>🕐 Últimas señales registradas:</b>")
        for s in list(recent)[-6:]:
            icon  = "✅" if s["result"] == "WIN" else "❌"
            strat = si.get(s.get("strategy"), "?")
            p     = s.get("pair", ())
            if len(p)==2:
                pr = f"D{p[0]}+D{p[1]}"
            else:
                pr = str(s.get("pair", "?"))
            fin   = s.get("intento_fin", "?")
            prob  = s.get("prob", 0)
            rsn   = s.get("reason", "")
            lines.append(f"{icon} {strat} {pr} Int.{fin} ({prob:.0%}) — {rsn}")

        return "\n".join(lines)

# ─── ESTADÍSTICAS DETALLADAS (con marcador diario) ─────────────────────────────
class DailyStats:
    def __init__(self):
        self.date = datetime.now().date()
        self.wins = 0
        self.losses = 0
        self.net_profit = 0.0

    def reset_if_new_day(self):
        today = datetime.now().date()
        if today != self.date:
            self.date = today
            self.wins = 0
            self.losses = 0
            self.net_profit = 0.0
            logger.info("📅 Marcador diario reiniciado automáticamente por nuevo día.")
            tg_send_secondary("📅 *NUEVO DÍA* — El marcador diario ha sido reiniciado automáticamente.", parse_mode="Markdown")

    def record(self, won: bool, profit: float):
        self.reset_if_new_day()
        if won:
            self.wins += 1
        else:
            self.losses += 1
        self.net_profit += profit

    def reset(self):
        self.date = datetime.now().date()
        self.wins = 0
        self.losses = 0
        self.net_profit = 0.0
        logger.info("🗑️ Marcador diario reiniciado manualmente.")
        tg_send_secondary("🗑️ *Marcador diario reiniciado manualmente.*", parse_mode="Markdown")

    def get_text(self) -> str:
        total = self.wins + self.losses
        eff = (self.wins / total * 100) if total > 0 else 0
        profit_str = f"+${self.net_profit:.2f}" if self.net_profit >= 0 else f"-${abs(self.net_profit):.2f}"
        return (
            f"📅 *MARCADOR DIARIO* ({self.date.strftime('%d/%m/%Y')})\n"
            f"✅ Aciertos: {self.wins}\n"
            f"❌ Fallos: {self.losses}\n"
            f"📊 Efectividad: {eff:.1f}%\n"
            f"💰 Neto: {profit_str} USD"
        )

class DetailedStats:
    def __init__(self):
        self.wins              = 0
        self.losses            = 0
        self.consecutive       = 0
        self.last_20           = deque(maxlen=20)
        self.signals_processed = 0
        self.last_report_sigs  = 0

    def record(self, result_type: str, intento: int, number: int,
               val: int, bankroll: float, strat: int):
        self.signals_processed += 1
        if result_type == 'WIN':
            self.wins        += 1
            self.consecutive += 1
        else:
            self.losses      += 1
            self.consecutive  = 0
        self.last_20.append({
            "result": result_type, "intento": intento, "number": number,
            "val": val, "balance": bankroll, "strat": strat
        })

    def should_send(self):
        return (self.signals_processed - self.last_report_sigs) >= 20

    def mark_sent(self):
        self.last_report_sigs = self.signals_processed

    def get_stats_text(self, bankroll: float) -> str:
        total = self.wins + self.losses
        eff   = (self.wins / total * 100) if total > 0 else 0.0
        text  = (
            f"📊 RESUMEN TOTAL 📊\n"
            f"► ✅{self.wins} | 🚫{self.losses}\n"
            f"► Consecutivas = {self.consecutive}\n"
            f"► Assert = {eff:.2f}%\n"
            f"► Balance: 💰 ${bankroll:.2f} USD\n"
            f"► Total señales: {total}\n\n"
            f"📌 Últimas 20 📌\n"
        )
        _si = {1:"🅐",2:"🅑",3:"🅒",4:"🅓",5:"🅔"}
        for s in reversed(list(self.last_20)):
            si   = _si.get(s['strat'], "?")
            opp  = f"Int.{s['intento']}"
            b    = f"💰${s['balance']:.2f}"
            v    = f"D{s['val']}" if s['strat'] in (1,2,3) else (s['val'] if isinstance(s['val'],str) else f"{s['val']}")
            r    = s['result']
            icon = "✅" if r == 'WIN' else "🚫"
            text += f"{icon} WIN #{s['number']} {v} {si} | {opp} | {b}\n" \
                    if r == 'WIN' else \
                    f"🚫 LOSS #{s['number']} {v} {si} | {opp} | {b}\n"
        return text

# ─── ANÁLISIS DE REPETICIONES ─────────────────────────────────────────────────
class RepetitionAnalyzer:
    def __init__(self, db_conn):
        self.db = db_conn
        self.cache = {}

    def register_signal(self, rep_type: str, rep_value: int, numbers: List[int]) -> int:
        numbers_str = ",".join(str(n) for n in numbers)
        cur = self.db.execute(
            "INSERT INTO repetition_signals (ts, rep_type, rep_value, numbers, result) VALUES (?,?,?,?,?)",
            (time.time(), rep_type, rep_value, numbers_str, None)
        )
        self.db.commit()
        return cur.lastrowid

    def resolve_signal(self, signal_id: int, result: str):
        self.db.execute(
            "UPDATE repetition_signals SET result=?, resolved_ts=? WHERE id=?",
            (result, time.time(), signal_id)
        )
        self.db.commit()

    def get_win_rate(self, rep_type: str, rep_value: int) -> float:
        rows = self.db.execute(
            "SELECT result, ts FROM repetition_signals WHERE rep_type=? AND rep_value=? AND result IS NOT NULL",
            (rep_type, rep_value)
        ).fetchall()
        if not rows:
            return 0.5
        now = time.time()
        window = 30 * 24 * 3600
        total_weight = 0.0
        wins_weight = 0.0
        for row in rows:
            age = now - row['ts']
            weight = math.exp(-age / window)
            total_weight += weight
            if row['result'] == 'WIN':
                wins_weight += weight
        if total_weight < 1.0:
            return 0.5
        return wins_weight / total_weight

    def get_deviation_prob(self, rep_type: str, rep_value: int, numbers: List[int]) -> float:
        numbers_str = ",".join(str(n) for n in numbers)
        rows = self.db.execute(
            "SELECT result FROM repetition_signals WHERE rep_type=? AND numbers=? AND result IS NOT NULL",
            (rep_type, numbers_str)
        ).fetchall()
        if rows:
            total = len(rows)
            wins = sum(1 for r in rows if r['result'] == 'WIN')
            specific_rate = wins / total
            general_rate = self.get_win_rate(rep_type, rep_value)
            prob = 0.7 * specific_rate + 0.3 * general_rate
        else:
            prob = self.get_win_rate(rep_type, rep_value)
        return 1.0 - prob

# ─── ENGINE PRINCIPAL ─────────────────────────────────────────────────────────
class RussianRouletteEngine:
    def __init__(self, stats_client: StatsClient):
        self.stats_client = stats_client
        self.spin_history = []
        self.dozen_seq = []
        self.d_levels = {1: [], 2: [], 3: []}
        self.doc_levels = []
        self._last_doc_inc = 0
        self.after_number_dozen = defaultdict(lambda: defaultdict(int))
        self.markov_d = SmoothedMarkovPredictor()
        self.ensemble_d = OnlineEnsemblePredictor()
        self.spins_since_train = 0

        self.signal_active = False
        self.active_strategy = None
        self.active_pair = ()
        self.active_missing = 0
        self.active_intento = 1
        self.total_signal_loss = 0.0
        self.active_signal_msg_id = None

        self.gestor = GestorDocenas()
        self.bankroll = 100.0

        self.stats = DetailedStats()
        self.daily_stats = DailyStats()
        self._db = _get_db()
        self.learner = SignalLearner(self._db)
        self.rep_analyzer = RepetitionAnalyzer(self._db)

        self.active_rep_signal_id = None
        self.active_rep_type = None
        self.active_rep_value = None
        self.active_rep_numbers = None

        self.processed_game_ids = set()
        self.MAX_PROCESSED_IDS = 300

        self.signals_paused = False   # ← Control de pausa de señales en canal principal

        live_loaded = self._load_live_history()
        self.ws_count = live_loaded
        self.warmup_done = live_loaded >= WARMUP_SPINS
        logger.info(f"[RussianDC] 📦 Pre-cargados: {live_loaded} | Warmup: {'✅' if self.warmup_done else '⏳'}")

    # ── DB y estado ──────────────────────────────────────────────────────────
    def _load_live_history(self) -> int:
        try:
            rows = self._db.execute("SELECT number FROM live_spins ORDER BY id ASC").fetchall()
        except:
            return 0
        for (n,) in rows:
            self._update_state(n, persist=False, train_model=False)
        if rows:
            self.markov_d.update(self.dozen_seq)
        return len(rows)

    def _persist(self, number: int):
        try:
            self._db.execute("INSERT INTO live_spins(number,ts) VALUES(?,?)", (number, int(time.time())))
            self._db.commit()
        except Exception as e:
            logger.debug(f"⚠️ DB persist error: {e}")

    def _update_state(self, number: int, persist=True, train_model=True):
        d = get_dozen(number)
        if number != 0 and self.spin_history:
            prev = self.spin_history[-1]["number"]
            if prev != 0:
                self.after_number_dozen[prev][d] += 1
        self.spin_history.append({"number": number})
        if d != 0:
            for dd in (1, 2, 3):
                prev = self.d_levels[dd][-1] if self.d_levels[dd] else 0
                self.d_levels[dd].append(prev + (1 if d == dd else -1))
                if len(self.d_levels[dd]) > 300:
                    self.d_levels[dd].pop(0)
            self.dozen_seq.append(d)
            if len(self.dozen_seq) > 200:
                self.dozen_seq.pop(0)
            if train_model and len(self.dozen_seq) > 5:
                pf_d = self._get_pf()
                ph_d = self._get_ph(number)
                if pf_d and ph_d:
                    self.ensemble_d.partial_train(self.dozen_seq, d, pf_d["pair"], ph_d["pair"])
                self.spins_since_train += 1
                if self.spins_since_train >= TRAIN_INTERVAL:
                    self.markov_d.update(self.dozen_seq)
                    self.spins_since_train = 0
        if number != 0:
            inc = 1 if d == 1 else (-1 if d == 3 else (1 if number <= 18 else -1))
            self._last_doc_inc = inc
        else:
            inc = self._last_doc_inc
        prev_lvl = self.doc_levels[-1] if self.doc_levels else 0
        self.doc_levels.append(prev_lvl + inc)
        if len(self.doc_levels) > 300:
            self.doc_levels.pop(0)
        if persist:
            self._persist(number)

    # ── Estrategias ──────────────────────────────────────────────────────────
    def _get_pf(self) -> Optional[Dict]:
        if len(self.spin_history) < 5:
            return None
        counts = {1: 0, 2: 0, 3: 0}
        for s in self.spin_history[-5:]:
            n = s["number"]
            if n != 0:
                counts[get_dozen(n)] += 1
        active = [k for k, v in counts.items() if v > 0]
        if len(active) != 2:
            return None
        pair = tuple(sorted(active))
        missing = list({1, 2, 3} - set(pair))[0]
        return {"pair": pair, "missing": missing, "prob": sum(counts[a] for a in pair) / 5.0}

    def _get_ph(self, number: Optional[int] = None) -> Optional[Dict]:
        if number is None:
            if not self.spin_history:
                return None
            number = self.spin_history[-1]["number"]
        if number == 0:
            return None
        server_ph = self.stats_client.get_ph_pair(number)
        if server_ph:
            return server_ph
        counts = self.after_number_dozen.get(number, {})
        total = sum(counts.values())
        if total < 10:
            return None
        sc = sorted(counts.items(), key=lambda x: x[1], reverse=True)
        if len(sc) < 2:
            return None
        pair = tuple(sorted([sc[0][0], sc[1][0]]))
        missing = list({1, 2, 3} - set(pair))[0]
        return {"pair": pair, "missing": missing, "prob": (sc[0][1] + sc[1][1]) / total}

    def _get_phtml_probs(self, number: int) -> Optional[Dict]:
        if number == 0:
            return None
        entry = DOZEN_TABLE.get(number)
        if not entry:
            return None
        d1, d2, d3 = entry["d1"], entry["d2"], entry["d3"]
        total = d1 + d2 + d3
        if total == 0:
            return None
        return {1: d1 / total, 2: d2 / total, 3: d3 / total}

    def _get_phtml_pair(self, number: int) -> Optional[Dict]:
        probs = self._get_phtml_probs(number)
        if probs is None:
            return None
        sorted_d = sorted(probs.items(), key=lambda x: x[1], reverse=True)
        pair = tuple(sorted([sorted_d[0][0], sorted_d[1][0]]))
        missing = list({1, 2, 3} - set(pair))[0]
        prob = probs[sorted_d[0][0]] + probs[sorted_d[1][0]]
        return {"pair": pair, "missing": missing, "prob": prob}

    def _get_phf(self, number: int) -> Optional[Dict]:
        if number == 0:
            return None
        phtml = self._get_phtml_probs(number)
        if phtml is None:
            return None
        ph = self.stats_client.get_ph_probs_raw(number)
        if ph is None:
            counts = self.after_number_dozen.get(number, {})
            total = sum(counts.values())
            if total >= 10:
                ph = {1: counts.get(1,0)/total, 2: counts.get(2,0)/total, 3: counts.get(3,0)/total}
        if ph is not None:
            phf_raw = {d: PHTML_W * phtml[d] + PH_W_COMBINE * ph[d] for d in [1, 2, 3]}
        else:
            phf_raw = dict(phtml)
        total = sum(phf_raw.values())
        if total == 0:
            return None
        phf = {d: v / total for d, v in phf_raw.items()}
        sorted_d = sorted(phf.items(), key=lambda x: x[1], reverse=True)
        pair = tuple(sorted([sorted_d[0][0], sorted_d[1][0]]))
        missing = list({1, 2, 3} - set(pair))[0]
        prob = phf[sorted_d[0][0]] + phf[sorted_d[1][0]]
        return {"pair": pair, "missing": missing, "prob": prob, "probs": phf}

    def _predict_pair_ml(self, missing_num: int) -> float:
        mk_pred = self.markov_d.predict(self.dozen_seq)
        m_p_miss = mk_pred.get(missing_num, 1/3) if mk_pred else 1/3
        pf_d = self._get_pf()
        ph_d = self._get_ph()
        ens_p_miss = 1/3
        if pf_d and ph_d:
            ens = self.ensemble_d.predict(self.dozen_seq, pf_d["pair"], ph_d["pair"])
            if ens:
                ens_p_miss = ens.get(missing_num, 1/3)
        ml_miss = 0.4 * m_p_miss + 0.6 * ens_p_miss
        levels = self.d_levels.get(missing_num, [])
        if len(levels) >= 20:
            if ema_signal(levels, "tendencia"): ml_miss *= 0.85
            elif ema_signal(levels, "moderado"): ml_miss *= 0.92
        return 1.0 - ml_miss

    def _detect_e1(self) -> Optional[Dict]:
        if not self.warmup_done or not self.spin_history:
            return None
        last_num = self.spin_history[-1]["number"]
        if last_num == 0:
            return None
        pf_d = self._get_pf()
        if not pf_d:
            return None
        phf_d = self._get_phf(last_num)
        if not phf_d:
            return None
        if set(pf_d["pair"]) != set(phf_d["pair"]):
            return None
        base = PF_W_NORM * pf_d["prob"] + PH_W_NORM * phf_d["prob"]
        ml = self._predict_pair_ml(pf_d["missing"])
        prob = BASE_W_NORM * base + ML_W_NORM * ml
        trend = ema_trend_str(self.doc_levels)
        adj, adj_detail = self.learner.get_adjustment(STRAT_E1, pf_d["pair"], trend, self.gestor.nivel)
        prob_adj = round(max(0.0, min(1.0, prob + adj)), 4)
        if prob_adj < MIN_PROB_DOZEN:
            return None
        return {
            "strategy": STRAT_E1,
            "pair": pf_d["pair"],
            "missing": pf_d["missing"],
            "prob": prob_adj,
            "label": "PF+PHF+ML",
            "pf_prob": pf_d["prob"],
            "phf_prob": phf_d["prob"],
            "ema_trend": trend,
            "last_number": last_num,
            "adj": adj,
            "adj_detail": adj_detail
        }

    def _detect_e2_repetition(self) -> Optional[Dict]:
        if not self.warmup_done or len(self.spin_history) < 3:
            return None
        non_zero = [s['number'] for s in self.spin_history if s['number'] != 0]
        if len(non_zero) < 3:
            return None
        last3 = non_zero[-3:]
        d1, d2, d3 = (get_dozen(n) for n in last3)
        if d1 == d2 == d3 and d1 != 0:
            rep_type = 'dozen'
            rep_value = d1
            numbers_str = ",".join(str(n) for n in last3)
            prob_deviation = self.rep_analyzer.get_deviation_prob(rep_type, rep_value, last3)
            prob = prob_deviation
            trend = ema_trend_str(self.doc_levels)
            adj, adj_detail = self.learner.get_adjustment(STRAT_E2_REP, (rep_value,), trend, self.gestor.nivel)
            prob_adj = round(max(0.0, min(1.0, prob + adj)), 4)
            if prob_adj >= MIN_PROB_REPETICION:
                logger.info(f"[E2] Repetición docena {rep_value} números {last3} | prob desviación {prob:.0%} adj:{adj:+.3f} -> {prob_adj:.0%}")
                return {
                    "strategy": STRAT_E2_REP,
                    "pair": (f"D{rep_value} (contrario)",),
                    "missing": rep_value,
                    "prob": prob_adj,
                    "label": f"Repetición Docena {rep_value} → contraria",
                    "pf_prob": prob,
                    "phf_prob": 0.0,
                    "ema_trend": trend,
                    "last_number": self.spin_history[-1]["number"],
                    "adj": adj,
                    "adj_detail": adj_detail,
                    "rep_type": rep_type,
                    "rep_value": rep_value,
                    "numbers_str": numbers_str,
                    "numbers_list": last3
                }
        c1, c2, c3 = (get_column(n) for n in last3)
        if c1 == c2 == c3 and c1 != 0:
            rep_type = 'column'
            rep_value = c1
            numbers_str = ",".join(str(n) for n in last3)
            prob_deviation = self.rep_analyzer.get_deviation_prob(rep_type, rep_value, last3)
            prob = prob_deviation
            trend = ema_trend_str(self.doc_levels)
            adj, adj_detail = self.learner.get_adjustment(STRAT_E2_REP, (rep_value,), trend, self.gestor.nivel)
            prob_adj = round(max(0.0, min(1.0, prob + adj)), 4)
            if prob_adj >= MIN_PROB_REPETICION:
                logger.info(f"[E2] Repetición columna {rep_value} números {last3} | prob desviación {prob:.0%} adj:{adj:+.3f} -> {prob_adj:.0%}")
                return {
                    "strategy": STRAT_E2_REP,
                    "pair": (f"C{rep_value} (contrario)",),
                    "missing": rep_value,
                    "prob": prob_adj,
                    "label": f"Repetición Columna {rep_value} → contraria",
                    "pf_prob": prob,
                    "phf_prob": 0.0,
                    "ema_trend": trend,
                    "last_number": self.spin_history[-1]["number"],
                    "adj": adj,
                    "adj_detail": adj_detail,
                    "rep_type": rep_type,
                    "rep_value": rep_value,
                    "numbers_str": numbers_str,
                    "numbers_list": last3
                }
        return None

    def _detect_e3(self) -> Optional[Dict]:
        if not self.warmup_done:
            return None
        non_zero = [s["number"] for s in self.spin_history if s["number"] != 0]
        if len(non_zero) < 6:
            return None
        prev5   = non_zero[-6:-1]
        last_n  = non_zero[-1]
        cats5   = list(set(get_dozen(n) for n in prev5))
        if len(cats5) != 2:
            return None
        pair       = tuple(sorted(cats5))
        last_dozen = get_dozen(last_n)
        if last_dozen in pair:
            return None
        streak = 0
        for n in reversed(non_zero[:-1]):
            if get_dozen(n) in pair:
                streak += 1
            else:
                break
        phf_break = self._get_phf(last_n)
        if phf_break is None or set(phf_break["pair"]) != set(pair):
            return None
        return_prob = self._calc_return_prob(pair, streak, last_n)
        trend = ema_trend_str(self.doc_levels)
        adj, adj_detail = self.learner.get_adjustment(STRAT_E3, pair, trend, self.gestor.nivel)
        return_prob_adj = round(max(0.0, min(1.0, return_prob + adj)), 4)
        if return_prob_adj < MIN_PROB_DOZEN:
            return None
        return {
            "strategy": STRAT_E3,
            "pair": pair,
            "missing": last_dozen,
            "prob": return_prob_adj,
            "label": f"RETORNO (racha {streak}g)",
            "streak": streak,
            "break_number": last_n,
            "pf_prob": return_prob,
            "phf_prob": phf_break["prob"],
            "ema_trend": trend,
            "last_number": last_n,
            "adj": adj,
            "adj_detail": adj_detail
        }

    def _calc_return_prob(self, pair: tuple, streak: int, break_num: int) -> float:
        non_zero   = [s["number"] for s in self.spin_history if s["number"] != 0]
        last20     = non_zero[-20:]
        pair_count = sum(1 for n in last20 if get_dozen(n) in pair)
        base_prob  = pair_count / len(last20) if last20 else 0.66
        streak_bst = min(0.40, streak * 0.04)
        brk_d      = get_dozen(break_num)
        brk_adj    = 0.02 if brk_d in pair else -0.04
        missing    = list({1, 2, 3} - set(pair))[0]
        levels     = self.d_levels.get(missing, [])
        ema_adj    = 0.0
        if len(levels) >= 20:
            if ema_signal(levels, "tendencia"): ema_adj = -0.08
            elif ema_signal(levels, "moderado"): ema_adj = -0.04
        return round(max(0.35, min(0.97, base_prob + streak_bst + brk_adj + ema_adj)), 4)

    async def _detect_e4(self) -> Optional[Dict]:
        if not self.warmup_done:
            return None
        colors = self.stats_client.last_colors
        if len(colors) < 3:
            return None
        for pattern, bet in PATTERNS_COLOR:
            if len(colors) >= len(pattern) and colors[-len(pattern):] == pattern:
                pattern_seq_str = ",".join(pattern)
                global_stats = await self.stats_client.get_pattern_stats("color", pattern_seq_str)
                global_win_rate = global_stats.get("win_rate", 0.0)
                last_numbers = [s["number"] for s in self.spin_history if s["number"] != 0][-len(pattern):]
                specific_rate = 0.5
                if len(last_numbers) == len(pattern):
                    specific_history = await self.stats_client.get_sequence_history(last_numbers, "color")
                    if specific_history:
                        specific_wins = sum(1 for h in specific_history if h["result"] == "WIN")
                        specific_total = len(specific_history)
                        specific_rate = specific_wins / specific_total if specific_total > 0 else 0.5
                prob = GLOBAL_WEIGHT * global_win_rate + SPECIFIC_WEIGHT * specific_rate
                trend = ema_trend_str(self.doc_levels)
                adj, adj_detail = self.learner.get_adjustment(STRAT_E4_COLOR, (1,2), trend, self.gestor.nivel)
                prob_adj = round(max(0.0, min(1.0, prob + adj)), 4)
                if prob_adj >= MIN_PROB_REPETICION:
                    logger.info(f"[E4] Patrón color {pattern} -> {bet} | prob:{prob:.0%} -> {prob_adj:.0%}")
                    return {
                        "strategy": STRAT_E4_COLOR,
                        "pair": (bet,),
                        "missing": 0,
                        "prob": prob_adj,
                        "label": f"Patrón Color {bet}",
                        "pf_prob": global_win_rate,
                        "phf_prob": specific_rate,
                        "ema_trend": trend,
                        "last_number": self.spin_history[-1]["number"],
                        "adj": adj,
                        "adj_detail": adj_detail,
                        "bet_str": bet
                    }
        return None

    async def _detect_e5(self) -> Optional[Dict]:
        if not self.warmup_done:
            return None
        zones = self.stats_client.last_zones
        if len(zones) < 3:
            return None
        for pattern, bet in PATTERNS_ZONE:
            if len(zones) >= len(pattern) and zones[-len(pattern):] == pattern:
                pattern_seq_str = ",".join(pattern)
                global_stats = await self.stats_client.get_pattern_stats("zone", pattern_seq_str)
                global_win_rate = global_stats.get("win_rate", 0.0)
                last_numbers = [s["number"] for s in self.spin_history if s["number"] != 0][-len(pattern):]
                specific_rate = 0.5
                if len(last_numbers) == len(pattern):
                    specific_history = await self.stats_client.get_sequence_history(last_numbers, "zone")
                    if specific_history:
                        specific_wins = sum(1 for h in specific_history if h["result"] == "WIN")
                        specific_total = len(specific_history)
                        specific_rate = specific_wins / specific_total if specific_total > 0 else 0.5
                prob = GLOBAL_WEIGHT * global_win_rate + SPECIFIC_WEIGHT * specific_rate
                trend = ema_trend_str(self.doc_levels)
                adj, adj_detail = self.learner.get_adjustment(STRAT_E5_ZONE, (1,2), trend, self.gestor.nivel)
                prob_adj = round(max(0.0, min(1.0, prob + adj)), 4)
                if prob_adj >= MIN_PROB_REPETICION:
                    logger.info(f"[E5] Patrón zona {pattern} -> {bet} | prob:{prob:.0%} -> {prob_adj:.0%}")
                    return {
                        "strategy": STRAT_E5_ZONE,
                        "pair": (bet,),
                        "missing": 0,
                        "prob": prob_adj,
                        "label": f"Patrón Zona {bet}",
                        "pf_prob": global_win_rate,
                        "phf_prob": specific_rate,
                        "ema_trend": trend,
                        "last_number": self.spin_history[-1]["number"],
                        "adj": adj,
                        "adj_detail": adj_detail,
                        "bet_str": bet
                    }
        return None

    async def _select_best_signal(self) -> Optional[Dict]:
        e1 = self._detect_e1()
        e2 = self._detect_e2_repetition()
        e3 = self._detect_e3()
        e4 = await self._detect_e4()
        e5 = await self._detect_e5()
        candidates = [s for s in [e1, e2, e3, e4, e5] if s]
        if not candidates:
            return None
        def priority(s):
            if s["strategy"] == STRAT_E1: return 4
            if s["strategy"] == STRAT_E2_REP: return 3
            if s["strategy"] == STRAT_E3: return 2
            return 1
        candidates.sort(key=lambda x: (x["prob"], priority(x)), reverse=True)
        return candidates[0]

    def _format_bets(self, bet_usd: float) -> str:
        lines = []
        for curr in ["USD", "MXN", "PEN", "COP", "ARS", "CLP"]:
            sym = CURRENCY_SYMBOLS[curr]
            mult = CURRENCY_MULTIPLIERS[curr]
            dec = CURRENCY_DECIMALS[curr]
            flag = CURRENCY_FLAGS[curr]
            bet_loc = bet_usd * mult
            lines.append(f"{flag} {curr}: {sym}{bet_loc:.{dec}f} x Apuesta")
        return "\n".join(lines)

    def _intento_header(self, intento: int) -> str:
        if intento == 1: return "✅✅ ENTRADA CONFIRMADA ✅✅"
        if intento == 2: return "🚨 SEGUNDA OPORTUNIDAD 🚨"
        return "🚨 TERCERA OPORTUNIDAD 🚨"

    def _strat_icon(self) -> str:
        return {STRAT_E1:"🅐", STRAT_E2_REP:"🅑", STRAT_E3:"🅒", STRAT_E4_COLOR:"🅓", STRAT_E5_ZONE:"🅔"}.get(self.active_strategy, "?")

    def _build_signal_text(self) -> str:
        bet_usd = self.gestor.get_bet(self.active_intento)
        if self.active_strategy == STRAT_E2_REP:
            target = self.active_pair[0]
        elif self.active_strategy in (STRAT_E4_COLOR, STRAT_E5_ZONE):
            target = self.active_pair[0]
        else:
            target = f"D{self.active_pair[0]} y D{self.active_pair[1]}"
        header = self._intento_header(self.active_intento)
        icon = self._strat_icon()
        nivel_tag = f" Nv.{self.gestor.nivel}" if self.gestor.nivel > 1 else ""
        return (
            f"{header}\n\n"
            f"🕹️ IMMERSIVE ROULETTE {icon}{nivel_tag}\n"
            f"🎯 Apuesta: {target}\n\n"
            f"🚨 MONTO DE APUESTA:\n"
            f"{self._format_bets(bet_usd)}"
        )

    def _send_signal(self):
        text = self._build_signal_text()
        markup = roulette_keyboard()
        tg_send_secondary(text, markup)
        if not self.signals_paused:
            tg_send_main(text, markup)

    def _activate_signal(self, sig: Dict):
        self.signal_active = True
        self.active_strategy = sig["strategy"]
        self.active_pair = sig["pair"]
        self.active_missing = sig["missing"]
        self.active_intento = 1
        self.total_signal_loss = 0.0
        self.gestor.iniciar_senal(self.bankroll)

        if self.active_strategy == STRAT_E2_REP:
            signal_id = self.rep_analyzer.register_signal(
                sig['rep_type'], sig['rep_value'], sig['numbers_list']
            )
            self.active_rep_signal_id = signal_id
            self.active_rep_type = sig['rep_type']
            self.active_rep_value = sig['rep_value']
            self.active_rep_numbers = sig['numbers_list']
        else:
            self.active_rep_signal_id = None

        self._send_signal()

        trend = sig.get("ema_trend", ema_trend_str(self.doc_levels))
        self.learner.register_signal(
            strategy    = sig["strategy"],
            pair        = sig["pair"] if self.active_strategy in (STRAT_E1, STRAT_E3) else (1,2),
            missing     = sig["missing"],
            prob        = sig["prob"],
            nivel       = self.gestor.nivel,
            pf_prob     = sig.get("pf_prob", 0.0),
            phf_prob    = sig.get("phf_prob", 0.0),
            ema_trend   = trend,
            last_number = sig.get("last_number", self.spin_history[-1]["number"] if self.spin_history else 0),
            dozen_seq_5 = self.dozen_seq[-5:] if self.dozen_seq else []
        )
        adj_info = f" | Ajuste aprendizaje: {sig.get('adj_detail','—')}" if sig.get('adj') else ""
        logger.info(f"[RussianDC] 🎯 SEÑAL {sig['label']}: prob={sig['prob']:.0%} | Nivel {self.gestor.nivel}{adj_info}")

    def _resolve(self, number: int):
        d = get_dozen(number)
        bet_usd = self.gestor.get_bet(self.active_intento)

        if self.active_strategy == STRAT_E2_REP:
            invest = bet_usd
            if self.active_pair[0].startswith('D'):
                rep_dozen = self.active_missing
                won = (d != 0 and d != rep_dozen)
            else:
                rep_col = self.active_missing
                real_col = get_column(number)
                won = (real_col != rep_col)
            payout = 3 * bet_usd
        elif self.active_strategy in (STRAT_E4_COLOR, STRAT_E5_ZONE):
            invest = bet_usd
            if self.active_strategy == STRAT_E4_COLOR:
                real = self.stats_client.last_colors[-1] if self.stats_client.last_colors else ""
                bet_target = self.active_pair[0]
                won = (real == bet_target)
            else:
                real = self.stats_client.last_zones[-1] if self.stats_client.last_zones else ""
                bet_target = self.active_pair[0]
                won = (real == bet_target)
            payout = 2 * bet_usd
        else:
            invest = 2 * bet_usd
            won = (d != 0 and d in self.active_pair)
            payout = 3 * bet_usd

        profit = payout - invest
        if won:
            spin_profit = profit
            self.bankroll += spin_profit
            signal_profit = spin_profit - self.total_signal_loss
            self.gestor.verificar_recuperacion(self.bankroll)
            sign = "+" if signal_profit >= 0 else ""
            result_text = (
                f"✅ WIN #{number} — Op. #{self.active_intento}\n"
                f"🎉 {sign}{signal_profit:.2f} USD 🎉\n"
                f"💰 Balance: ${self.bankroll:.2f} USD | Nivel: {self.gestor.nivel}"
            )
            tg_send_both(result_text, markup=roulette_keyboard())
            self.stats.record('WIN', self.active_intento, number,
                              self.active_missing if self.active_strategy == STRAT_E2_REP else d,
                              self.bankroll, self.active_strategy)
            self.daily_stats.record(True, profit)
            reason = f"WIN int.{self.active_intento} | apuesta {self.active_pair}"
            self.learner.resolve("WIN", self.active_intento, reason)

            if self.active_strategy == STRAT_E2_REP and self.active_rep_signal_id:
                self.rep_analyzer.resolve_signal(self.active_rep_signal_id, 'WIN')

            self._check_stats()
            self._reset_signal()
        else:
            self.bankroll -= invest
            self.total_signal_loss += invest
            if self.active_intento < MAX_INTENTOS:
                if self.active_signal_msg_id:
                    tg_delete(CHAT_ID, self.active_signal_msg_id)
                    self.active_signal_msg_id = None
                self.active_intento += 1
                self._send_signal()
            else:
                loss_text = (
                    f"❌ LOSS #{number} — {self._strat_icon()} 3 intentos\n"
                    f"🚨 -{self.total_signal_loss:.2f} USD 🚨\n"
                    f"💰 Balance: ${self.bankroll:.2f} USD | Nivel: {self.gestor.nivel}→{self.gestor.nivel+1 if self.gestor.nivel < MAX_NIVEL else 1}"
                )
                tg_send_both(loss_text, markup=roulette_keyboard())
                self.stats.record('LOSS', self.active_intento, number,
                                  self.active_missing if self.active_strategy == STRAT_E2_REP else d,
                                  self.bankroll, self.active_strategy)
                self.daily_stats.record(False, -invest)
                reason = f"LOSS int.{self.active_intento} | apuesta {self.active_pair} | cayó {number}"
                self.learner.resolve("LOSS", self.active_intento, reason)

                if self.active_strategy == STRAT_E2_REP and self.active_rep_signal_id:
                    self.rep_analyzer.resolve_signal(self.active_rep_signal_id, 'LOSS')

                self.gestor.registrar_perdida_senal()
                self._check_stats()
                self._reset_signal()

    def _reset_signal(self):
        self.signal_active = False
        self.active_strategy = None
        self.active_pair = ()
        self.active_missing = 0
        self.active_intento = 1
        self.total_signal_loss = 0.0
        self.active_signal_msg_id = None
        self.active_rep_signal_id = None
        self.active_rep_type = None
        self.active_rep_value = None
        self.active_rep_numbers = None

    def _check_stats(self):
        if self.stats.should_send():
            tg_send_both(self.stats.get_stats_text(self.bankroll))
            self.stats.mark_sent()

    def process_batch(self, batch):
        new_spins = []
        for spin in reversed(batch):
            gid = spin.get("game_id")
            if not gid or gid in self.processed_game_ids:
                continue
            new_spins.append(spin)
        if not new_spins:
            return
        for spin in new_spins:
            gid = spin["game_id"]
            number = spin["number"]
            self.processed_game_ids.add(gid)
            if 0 <= number <= 36:
                try:
                    self._process_inner(number)
                except Exception as e:
                    logger.error(f"Error processing spin: {e}", exc_info=True)
                    self._reset_signal()
        if len(self.processed_game_ids) > self.MAX_PROCESSED_IDS:
            for gid in list(self.processed_game_ids)[:150]:
                self.processed_game_ids.discard(gid)

    def _process_inner(self, number: int):
        d = get_dozen(number)
        logger.info(f"[RussianDC] 🎰 #{len(self.spin_history)+1}: {number} D{d}")
        self._update_state(number)

        if not self.warmup_done:
            self.ws_count += 1
            if self.ws_count < WARMUP_SPINS:
                return
            self.warmup_done = True
            tg_send_both(
                "🟢 <b>IMMERSIVE ROULETTE DC v33</b> — Sistema listo.\n"
                "5 estrategias · Niveles · Repetición Docena/Columna · Control de señales · Marcador diario"
            )

        if self.signal_active:
            self._resolve(number)
        else:
            loop = asyncio.get_event_loop()
            sig = loop.run_until_complete(self._select_best_signal())
            if sig:
                self._activate_signal(sig)

    async def poll_loop(self):
        url = f"{STATS_URL}/latest/{TARGET_ROULETTE}"
        logger.info(f"[RussianDC] 🔄 Iniciando polling cada {POLL_INTERVAL}s → {url}")
        async with aiohttp.ClientSession() as session:
            while True:
                try:
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            self.stats_client.update(data)
                            last_20 = data.get("last_20", [])
                            if isinstance(last_20, list) and last_20 and isinstance(last_20[0], dict):
                                self.process_batch(last_20)
                        else:
                            self.stats_client.connected = False
                            logger.warning(f"[RussianDC] ⚠️ Poll status: {resp.status}")
                except Exception as e:
                    self.stats_client.connected = False
                    logger.debug(f"[RussianDC] Poll error: {e}")
                await asyncio.sleep(POLL_INTERVAL)

# ─── FLASK ────────────────────────────────────────────────────────────────────
app = Flask(__name__)
engine: Optional[RussianRouletteEngine] = None

@app.route("/")
def home():
    return jsonify({
        "status": "ok", "bot": "Russian Roulette DC v33",
        "strategies": ["E1: PF+PHF+ML", "E2: Repetición (desviación)", "E3: Retorno", "E4: Color", "E5: Zona"],
        "umbral_repeticion": MIN_PROB_REPETICION,
        "intentos": MAX_INTENTOS, "niveles": MAX_NIVEL,
        "learning": "SignalLearner activo",
        "signals_paused": engine.signals_paused if engine else False
    })

@app.route("/ping")
def ping():
    return jsonify({"status": "pong", "ts": time.time()})

@app.route("/health")
def health():
    if not engine:
        return jsonify({"status": "not_ready"}), 503
    strat_names = {STRAT_E1:"E1", STRAT_E2_REP:"E2(Rep)", STRAT_E3:"E3", STRAT_E4_COLOR:"E4(Color)", STRAT_E5_ZONE:"E5(Zona)"}
    bet_curr = engine.gestor.get_bet(engine.active_intento if engine.signal_active else 1)
    recent = engine.learner._recent()
    wins_r = sum(1 for s in recent if s["result"] == "WIN")
    return jsonify({
        "warmup": engine.warmup_done,
        "spins": len(engine.spin_history),
        "balance": f"${engine.bankroll:.2f} USD",
        "stats_connected": engine.stats_client.connected,
        "polls": engine.stats_client.poll_count,
        "signal_active": engine.signal_active,
        "active_strategy": strat_names.get(engine.active_strategy, "—"),
        "active_pair": str(engine.active_pair),
        "active_intento": engine.active_intento,
        "nivel": engine.gestor.nivel,
        "bet_current_usd": f"${bet_curr:.2f}",
        "debt_count": len(engine.gestor.debt_stack),
        "learner_signals": len(engine.learner.history),
        "learner_win_rate": f"{wins_r/len(recent)*100:.1f}%" if recent else "—",
        "signals_paused": engine.signals_paused
    })

# ─── COMANDOS TELEGRAM ─────────────────────────────────────────────────────────
@bot.message_handler(commands=['start'])
def cmd_start(m):
    chat_id = m.chat.id
    if chat_id == CHAT_ID:
        tg_send_secondary("🆕 *Canal principal reiniciado* — El bot está activo.", parse_mode="Markdown")
    bot.reply_to(m,
        "<b>🎰 IMMERSIVE ROULETTE DC v33</b>\n\n"
        "🤖 Bot de señales para ruleta Immersive.\n"
        "📌 Comandos disponibles:\n"
        "/start - Iniciar / estado del bot\n"
        "/detenersenal - Pausa el envío de señales al canal principal\n"
        "/encendersenal - Reanuda el envío de señales al canal principal\n"
        "/resetearmarcador - Reinicia el marcador diario\n"
        "/status - Estado actual del bot\n"
        "/stats - Estadísticas totales\n"
        "/aprendizaje - Resumen de aprendizaje adaptativo\n"
        "/niveles - Mostrar niveles de apuesta\n"
        "/debug - Información de depuración\n"
        "/reset - Reinicia el balance y nivel (conserva aprendizaje)\n"
        "/reset_learning - Borra todo el historial de aprendizaje\n\n"
        "🔔 <i>Las señales se emiten en el canal secundario de forma ininterrumpida.</i>",
        parse_mode="HTML")

@bot.message_handler(commands=['detenersenal'])
def cmd_stop_signals(m):
    if m.chat.id != CHAT_ID:
        bot.reply_to(m, "❌ Este comando solo funciona en el canal principal.")
        return
    if engine:
        engine.signals_paused = True
        bot.reply_to(m, "🔇 *Señales detenidas* — Ya no se enviarán señales al canal principal. El canal secundario sigue activo.", parse_mode="Markdown")
        tg_send_secondary("🔇 *Señales pausadas* en el canal principal. El bot sigue funcionando en segundo plano.", parse_mode="Markdown")
    else:
        bot.reply_to(m, "❌ Engine no inicializado.")

@bot.message_handler(commands=['encendersenal'])
def cmd_start_signals(m):
    if m.chat.id != CHAT_ID:
        bot.reply_to(m, "❌ Este comando solo funciona en el canal principal.")
        return
    if engine:
        engine.signals_paused = False
        bot.reply_to(m, "🔊 *Señales reanudadas* — El bot volverá a enviar señales al canal principal.", parse_mode="Markdown")
        tg_send_secondary("🔊 *Señales reanudadas* en el canal principal.", parse_mode="Markdown")
    else:
        bot.reply_to(m, "❌ Engine no inicializado.")

@bot.message_handler(commands=['resetearmarcador'])
def cmd_reset_daily(m):
    if m.chat.id != CHAT_ID:
        bot.reply_to(m, "❌ Este comando solo funciona en el canal principal.")
        return
    if engine:
        engine.daily_stats.reset()
        bot.reply_to(m, "🗑️ *Marcador diario reiniciado.*", parse_mode="Markdown")
        tg_send_secondary("🗑️ *Marcador diario reiniciado manualmente.*", parse_mode="Markdown")
    else:
        bot.reply_to(m, "❌ Engine no inicializado.")

@bot.message_handler(commands=['status'])
def cmd_status(m):
    if not engine:
        bot.reply_to(m, "❌ Engine no inicializado")
        return
    _strat_lbl = {
        STRAT_E1: "🅐 E1 PF+PHF+ML",
        STRAT_E2_REP: "🅑 E2 Repetición",
        STRAT_E3: "🅒 E3 Retorno",
        STRAT_E4_COLOR: "🅓 E4 Color",
        STRAT_E5_ZONE: "🅔 E5 Zona"
    }
    if engine.signal_active:
        lbl = _strat_lbl.get(engine.active_strategy, "—")
        pair = engine.active_pair[0] if engine.active_strategy in (STRAT_E2_REP, STRAT_E4_COLOR, STRAT_E5_ZONE) else f"D{engine.active_pair[0]}+D{engine.active_pair[1]}"
        bet_curr = engine.gestor.get_bet(engine.active_intento)
        st = f"🟢 {lbl} | {pair} | Int.{engine.active_intento}/{MAX_INTENTOS} | ${bet_curr:.2f}"
    else:
        st = "⚪ Idle"
    conn = "🟢 Conectado" if engine.stats_client.connected else "🔴 Desconectado"
    ago = (time.time() - engine.stats_client.last_poll_ok if engine.stats_client.last_poll_ok > 0 else 0)
    recent = engine.learner._recent()
    wins_r = sum(1 for s in recent if s["result"] == "WIN")
    wr_txt = f"{wins_r}/{len(recent)} ({wins_r/len(recent)*100:.0f}%)" if recent else "sin datos"
    daily_text = engine.daily_stats.get_text()
    bot.reply_to(m,
        f"<b>Estado:</b> {st}\n"
        f"<b>Señales pausadas:</b> {'SÍ' if engine.signals_paused else 'NO'}\n"
        f"<b>Giros:</b> {len(engine.spin_history)}\n"
        f"<b>Balance:</b> ${engine.bankroll:.2f} USD\n"
        f"<b>Nivel:</b> {engine.gestor.nivel}/{MAX_NIVEL} | Deudas: {len(engine.gestor.debt_stack)}\n"
        f"<b>Servidor:</b> {conn} ({ago:.0f}s)\n"
        f"<b>🧠 Aciertos recientes:</b> {wr_txt}\n\n"
        f"{daily_text}",
        parse_mode="HTML")

@bot.message_handler(commands=['stats'])
def cmd_stats(m):
    if not engine:
        bot.reply_to(m, "❌ Engine no inicializado")
        return
    bot.reply_to(m, engine.stats.get_stats_text(engine.bankroll), parse_mode="HTML")

@bot.message_handler(commands=['aprendizaje', 'learning'])
def cmd_learning(m):
    if not engine:
        bot.reply_to(m, "❌ Engine no inicializado")
        return
    bot.reply_to(m, engine.learner.get_summary(30), parse_mode="HTML")

@bot.message_handler(commands=['niveles'])
def cmd_niveles(m):
    if not engine:
        bot.reply_to(m, "❌ Engine no inicializado")
        return
    g = engine.gestor
    lines = [f"<b>🎚 Niveles de Apuesta</b>\n", f"Nivel actual: {g.nivel}/{MAX_NIVEL}", f"Deudas: {len(g.debt_stack)}"]
    for i, b0 in enumerate(g.debt_stack, 1):
        falta = max(0, b0 + BASE_BET * 0.9 - engine.bankroll)
        lines.append(f"  · Deuda {i}: B0=${b0:.2f} (falta ${falta:.2f})")
    lines.append(f"\n<b>Tabla de apuestas:</b>")
    for nv in range(1, MAX_NIVEL + 1):
        tag = " ← actual" if nv == g.nivel else ""
        lines.append(f"  Nv.{nv}: Int.1 ${nv*BASE_BET:.2f} | Int.2-3 ${3*nv*BASE_BET:.2f}{tag}")
    bot.reply_to(m, "\n".join(lines), parse_mode="HTML")

@bot.message_handler(commands=['debug'])
def cmd_debug(m):
    if not engine or not engine.warmup_done:
        bot.reply_to(m, "⏳ Sistema calentando...", parse_mode="HTML")
        return
    last_num = engine.spin_history[-1]["number"] if engine.spin_history else None
    trend = ema_trend_str(engine.doc_levels)
    e1 = engine._detect_e1()
    e2 = engine._detect_e2_repetition()
    e3 = engine._detect_e3()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    e4 = loop.run_until_complete(engine._detect_e4())
    e5 = loop.run_until_complete(engine._detect_e5())
    loop.close()
    def sig_txt(s):
        if not s: return "— Sin señal"
        if s["strategy"] in (STRAT_E2_REP, STRAT_E4_COLOR, STRAT_E5_ZONE):
            return f"✅ {s['pair'][0]} ({s['prob']:.0%})"
        return f"✅ D{s['pair']} ({s['prob']:.0%})"
    phf = engine._get_phf(last_num) if last_num and last_num != 0 else None
    phf_txt = f"D{phf['pair']} ({phf['prob']:.0%})" if phf else "N/A"
    bot.reply_to(m,
        f"<b>🔬 Debug — Último: #{last_num} | EMA: {trend.upper()}</b>\n\n"
        f"<b>🅐 E1:</b> {sig_txt(e1)}\n"
        f"<b>🅑 E2 (Rep):</b> {sig_txt(e2)}\n"
        f"<b>🅒 E3:</b> {sig_txt(e3)}\n"
        f"<b>🅓 E4 Color:</b> {sig_txt(e4)}\n"
        f"<b>🅔 E5 Zona:</b> {sig_txt(e5)}\n\n"
        f"<b>PHF(#{last_num}):</b> {phf_txt}\n\n"
        f"<b>Nivel:</b> {engine.gestor.nivel} | "
        f"<b>Docenas:</b> {engine.dozen_seq[-5:] if engine.dozen_seq else []}",
        parse_mode="HTML")

@bot.message_handler(commands=['reset'])
def cmd_reset(m):
    if engine:
        engine.stats = DetailedStats()
        engine.bankroll = 100.0
        engine.gestor.nivel = 1
        engine.gestor.debt_stack = []
        engine.processed_game_ids.clear()
        engine._reset_signal()
        engine.signals_paused = False
    bot.reply_to(m, f"🔄 <b>Resetado — Balance: ${engine.bankroll:.2f} USD | Nivel: 1</b>\n<i>🧠 Aprendizaje conservado ({len(engine.learner.history)} señales)</i>\n🔊 Señales reanudadas.", parse_mode="HTML")

@bot.message_handler(commands=['reset_learning'])
def cmd_reset_learning(m):
    if not engine:
        bot.reply_to(m, "❌ Engine no inicializado")
        return
    try:
        engine._db.execute("DELETE FROM signal_log")
        engine._db.execute("DELETE FROM repetition_signals")
        engine._db.commit()
        engine.learner.history.clear()
        engine.learner.pending_id = None
        engine.rep_analyzer.cache.clear()
        bot.reply_to(m, "🗑️ <b>Historial de aprendizaje y repeticiones borrado.</b>", parse_mode="HTML")
    except Exception as e:
        bot.reply_to(m, f"❌ Error: {e}", parse_mode="HTML")

# ─── SELF PING ────────────────────────────────────────────────────────────────
async def self_ping_loop():
    url = os.environ.get("RENDER_EXTERNAL_URL", "").rstrip("/")
    if not url or "localhost" in url:
        return
    await asyncio.sleep(30)
    while True:
        try:
            import urllib.request as _ur
            _ur.urlopen(f"{url}/ping", timeout=15)
        except:
            pass
        await asyncio.sleep(240)

def run_flask():
    app.run(host="0.0.0.0", port=10005, debug=False, use_reloader=False)

# ─── MAIN ─────────────────────────────────────────────────────────────────────
async def main():
    global engine
    stats_client = StatsClient()
    engine = RussianRouletteEngine(stats_client)
    # Configurar menú de comandos (síncrono, sin await)
    commands = [
        BotCommand("start", "Iniciar / estado del bot"),
        BotCommand("detenersenal", "Pausar señales en el canal principal"),
        BotCommand("encendersenal", "Reanudar señales en el canal principal"),
        BotCommand("resetearmarcador", "Reiniciar marcador diario"),
        BotCommand("status", "Estado actual del bot"),
        BotCommand("stats", "Estadísticas totales"),
        BotCommand("aprendizaje", "Resumen de aprendizaje adaptativo"),
        BotCommand("niveles", "Mostrar niveles de apuesta"),
        BotCommand("debug", "Información de depuración"),
        BotCommand("reset", "Reiniciar balance y nivel"),
        BotCommand("reset_learning", "Borrar historial de aprendizaje")
    ]
    bot.set_my_commands(commands)   # ← SIN await, es síncrono
    logger.info("✅ Menú de comandos configurado.")
    threading.Thread(target=lambda: bot.polling(none_stop=True, interval=1, timeout=30), daemon=True).start()
    logger.info(f"[RussianDC] 🎰 Immersive Roulette DC v33 — Control de señales activo. Canal secundario: {SECONDARY_CHAT_ID}")
    await asyncio.gather(
        asyncio.create_task(engine.poll_loop()),
        asyncio.create_task(self_ping_loop())
    )

if __name__ == "__main__":
    threading.Thread(target=run_flask, daemon=True).start()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot detenido.")
