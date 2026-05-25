#!/usr/bin/env python3
"""
Immersive Roulette Bot — DC v36.1
===========================================================================
  - Comandos de menú: /detenersenal, /encendersenal, /resetearmarcador.
  - Canal secundario (ID: -1003613599867) siempre recibe señales.
  - Flag signal_sending_enabled controla envíos al principal.
  - Estrategia E2 (docenas y columnas): triple histórico con efectividad ponderada.
  - Triples guardados en DB (números, resultado, acierto).
  - Columnas ahora con E1, E2, E3 (modelos ML propios).
  - Selección unificada de señal (docena o columna, la de mayor prob).
  - Antiduplicados en webhook y tg_send.
  - Marcador diario centralizado.
  - Correcciones: _pending_triple_column_pair, NoneType en resolución.
"""

import asyncio
import json
import logging
import os
import sqlite3
import threading
import time
from collections import deque, defaultdict
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Tuple, List

import numpy as np
from sklearn.naive_bayes import MultinomialNB
from sklearn.linear_model import SGDClassifier

import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
import aiohttp
from flask import Flask, jsonify, request
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ─── LOGGING ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [ImmersiveDC] %(levelname)s %(message)s"
)
logger = logging.getLogger("ImmersiveDC")
for _ln in ["werkzeug", "flask.app", "flask", "urllib3"]:
    logging.getLogger(_ln).setLevel(logging.ERROR)

# ─── CREDENCIALES ─────────────────────────────────────────────────────────────
TOKEN   = "8657427877:AAG9E5JozV40mm3IQoREIHvTnBFEFPgRSQo"
CHAT_ID = -1003610988961           # Canal principal
SECONDARY_CHAT_ID = -1003613599867 # Canal secundario (siempre recibe)

# ─── URL RULETA ───────────────────────────────────────────────────────────────
IMMERSIVE_URL = "https://1win.lat/casino/play/v_evolution:immersiveroulette"

def immersive_keyboard() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("🎡 IMMERSIVE ROULETTE", url=IMMERSIVE_URL))
    return kb

# ─── TELEGRAM ─────────────────────────────────────────────────────────────────
_session = requests.Session()
_retry   = Retry(
    total=5, backoff_factor=1.5,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "POST"], raise_on_status=False
)
_session.mount("https://", HTTPAdapter(max_retries=_retry, pool_connections=10, pool_maxsize=20))
_session.mount("http://",  HTTPAdapter(max_retries=_retry, pool_connections=10, pool_maxsize=20))

try:
    bot = telebot.TeleBot(TOKEN, threaded=False)
    bot.session = _session
    logger.info("✅ Telegram bot inicializado")
except Exception as e:
    logger.error(f"❌ Error inicializando Telegram: {e}")
    exit(1)

# ─── CONSTANTES ───────────────────────────────────────────────────────────────
STATS_URL       = "https://crashstake-ulmx.onrender.com"
TARGET_ROULETTE = "IMMERSIVE"
POLL_INTERVAL   = 2
LIVE_DB         = "immersive_live.db"

WARMUP_SPINS    = 25
MIN_PROB        = 0.78
TRAIN_INTERVAL  = 100

PHTML_W         = 0.00
PH_W_COMBINE    = 1.00
PF_W_NORM       = 0.70;  PH_W_NORM  = 0.30
BASE_W_NORM     = 0.55;  ML_W_NORM  = 0.45

MIN_PROB_COLOR_ZONE = 0.62

P1_W_COLOR = 0.35
P3_W_COLOR = 0.65
TRANS_MIN_SAMPLES = 15

DOZEN_HIST_MIN   = 5
DOZEN_HIST_SCALE = 0.12

ART = timezone(timedelta(hours=-3))

STRAT_E1    = 1
STRAT_E2    = 2
STRAT_E3    = 3
STRAT_COLOR = 4
STRAT_ZONE  = 5
STRAT_COL_SEQ = 6

signal_sending_enabled = True

# ─── MAPAS ────────────────────────────────────────────────────────────────────
COLOR_MAP: Dict[int, str] = {
    0:"V",  1:"R",  2:"N",  3:"R",  4:"N",  5:"R",  6:"N",
    7:"R",  8:"N",  9:"R",  10:"N", 11:"N", 12:"R",
    13:"N", 14:"R", 15:"N", 16:"R", 17:"N", 18:"R",
    19:"R", 20:"N", 21:"R", 22:"N", 23:"R", 24:"N",
    25:"R", 26:"N", 27:"R", 28:"N", 29:"N", 30:"R",
    31:"N", 32:"R", 33:"N", 34:"R", 35:"N", 36:"R",
}

def get_color(n: int) -> str:  return COLOR_MAP.get(n, "V")
def get_zone(n: int)  -> str:  return "Z" if n == 0 else ("B" if n <= 18 else "A")
def get_dozen(n: int) -> int:  return 0 if n == 0 else (n - 1) // 12 + 1
def get_column(n: int) -> int: return 0 if n == 0 else ((n - 1) % 3) + 1

# ─── TABLA PHTML (docenas) ────────────────────────────────────────────────────
DOZEN_TABLE: Dict[int, Dict[str, int]] = {
    0:  {"d1":32,"d2":32,"d3":32},  1: {"d1":28,"d2":32,"d3":36},
    2:  {"d1":36,"d2":28,"d3":32},  3: {"d1":24,"d2":32,"d3":36},
    4:  {"d1":32,"d2":40,"d3":24},  5: {"d1":40,"d2":24,"d3":36},
    6:  {"d1":32,"d2":24,"d3":40},  7: {"d1":36,"d2":24,"d3":40},
    8:  {"d1":32,"d2":36,"d3":28},  9: {"d1":28,"d2":36,"d3":32},
    10: {"d1":40,"d2":32,"d3":28}, 11: {"d1":36,"d2":24,"d3":36},
    12: {"d1":32,"d2":28,"d3":36}, 13: {"d1":32,"d2":28,"d3":36},
    14: {"d1":16,"d2":48,"d3":32}, 15: {"d1":36,"d2":28,"d3":32},
    16: {"d1":28,"d2":32,"d3":36}, 17: {"d1":20,"d2":44,"d3":32},
    18: {"d1":32,"d2":28,"d3":36}, 19: {"d1":36,"d2":28,"d3":32},
    20: {"d1":36,"d2":36,"d3":28}, 21: {"d1":24,"d2":44,"d3":28},
    22: {"d1":36,"d2":36,"d3":28}, 23: {"d1":24,"d2":32,"d3":40},
    24: {"d1":44,"d2":32,"d3":24}, 25: {"d1":36,"d2":24,"d3":36},
    26: {"d1":40,"d2":28,"d3":32}, 27: {"d1":32,"d2":28,"d3":36},
    28: {"d1":36,"d2":28,"d3":32}, 29: {"d1":32,"d2":24,"d3":40},
    30: {"d1":36,"d2":36,"d3":28}, 31: {"d1":32,"d2":36,"d3":24},
    32: {"d1":32,"d2":36,"d3":28}, 33: {"d1":28,"d2":32,"d3":36},
    34: {"d1":36,"d2":28,"d3":32}, 35: {"d1":36,"d2":32,"d3":24},
    36: {"d1":28,"d2":36,"d3":32},
}

# ─── DB LOCAL ─────────────────────────────────────────────────────────────────
def _get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(LIVE_DB, check_same_thread=False)
    conn.execute("""CREATE TABLE IF NOT EXISTS live_spins (
        id     INTEGER PRIMARY KEY AUTOINCREMENT,
        number INTEGER NOT NULL,
        color  TEXT    NOT NULL,
        zone   TEXT    NOT NULL,
        ts     INTEGER NOT NULL
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS processed_spins (
        game_id TEXT PRIMARY KEY,
        ts      INTEGER NOT NULL
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS signal_log (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        ts            REAL    NOT NULL,
        strategy      INTEGER,
        pair          TEXT,
        missing       INTEGER,
        prob          REAL,
        intento_start INTEGER,
        nivel         INTEGER DEFAULT 1,
        pf_prob       REAL,
        phf_prob      REAL,
        ema_trend     TEXT,
        last_number   INTEGER,
        dozen_seq_5   TEXT,
        result        TEXT,
        intento_fin   INTEGER,
        reason        TEXT
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS triple_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts REAL NOT NULL,
        type TEXT NOT NULL,
        triple TEXT NOT NULL,
        numbers TEXT,
        next_dozen INTEGER,
        next_column INTEGER,
        win INTEGER,
        pair TEXT
    )""")
    conn.commit()
    return conn

# ─── TELEGRAM HELPERS ─────────────────────────────────────────────────────────
_TG_RETRIES = 12

def _tg_call(fn, *a, **kw):
    delay = 2.0
    for attempt in range(1, _TG_RETRIES + 1):
        try:
            return fn(*a, **kw)
        except Exception as e:
            err = str(e)
            if "retry after" in err.lower():
                try:    wait = int("".join(filter(str.isdigit, err))) + 1
                except: wait = 30
                logger.warning(f"⏳ Rate limited. Esperando {wait}s...")
                time.sleep(wait)
                continue
            if attempt == _TG_RETRIES:
                logger.error(f"❌ TG falló tras {_TG_RETRIES} intentos: {err}")
                return None
            time.sleep(delay)
            delay = min(delay * 2, 60)
    return None

_RECENT_MESSAGES = {}
_DEDUP_WINDOW = 5

def tg_send(text: str, markup: InlineKeyboardMarkup = None) -> Optional[int]:
    if not text: return None
    now = time.time()
    key = hash(text)
    for k in list(_RECENT_MESSAGES.keys()):
        if now - _RECENT_MESSAGES[k] > _DEDUP_WINDOW:
            del _RECENT_MESSAGES[k]
    if key in _RECENT_MESSAGES:
        logger.info(f"🚫 Mensaje duplicado bloqueado: {text[:60]}...")
        return None
    _RECENT_MESSAGES[key] = now

    # Enviar SIEMPRE al secundario
    try:
        _tg_call(bot.send_message, chat_id=SECONDARY_CHAT_ID, text=text,
                 parse_mode="HTML", reply_markup=markup)
    except Exception as e:
        logger.error(f"❌ Error enviando a secundario: {e}")

    main_msg_id = None
    if signal_sending_enabled:
        try:
            msg = _tg_call(bot.send_message, chat_id=CHAT_ID, text=text,
                           parse_mode="HTML", reply_markup=markup)
            if msg:
                main_msg_id = msg.message_id
                logger.info(f"✅ Mensaje enviado a principal (ID: {msg.message_id})")
        except Exception as e:
            logger.error(f"❌ Error enviando a principal: {e}")
    return main_msg_id

def tg_delete(chat_id: int, message_id: int):
    try:
        _tg_call(bot.delete_message, chat_id=chat_id, message_id=message_id)
    except Exception as e:
        logger.warning(f"⚠️ Error borrando mensaje: {e}")

PROCESSED_UPDATE_IDS = set()
MAX_UPDATE_IDS = 500

# ─── MARCADOR DIARIO ──────────────────────────────────────────────────────────
class DailyScoreboard:
    def __init__(self):
        self.wins:   int = 0
        self.losses: int = 0
        self._current_day: int = self._art_day()

    @staticmethod
    def _art_day() -> int:
        return datetime.now(ART).day

    def _check_reset(self):
        today = self._art_day()
        if today != self._current_day:
            logger.info(f"[Scoreboard] 🔄 Nuevo día Argentina → reset")
            self.wins   = 0
            self.losses = 0
            self._current_day = today

    def record_win(self):
        self._check_reset()
        self.wins += 1

    def record_loss(self):
        self._check_reset()
        self.losses += 1

    def get_text(self) -> str:
        self._check_reset()
        total = self.wins + self.losses
        pct   = (self.wins / total * 100) if total > 0 else 0.0
        return (
            f"📊 MARCADOR DIARIO:\n"
            f"✅ GANADAS: {self.wins}\n"
            f"❌ PERDIDAS: {self.losses}\n"
            f"📈 ACIERTOS = {pct:.2f}%"
        )

    def send(self):
        tg_send(self.get_text())

scoreboard = DailyScoreboard()

# ─── STATS CLIENT (COMPLETO) ──────────────────────────────────────────────────
class StatsClient:
    def __init__(self):
        self.stats_dozen    = {}
        self.stats_column   = {}
        self.stats_color    = {}
        self.stats_zone     = {}
        self.color_patterns = {}
        self.zone_patterns  = {}
        self.dozen_signals  = {}
        self.column_signals = {}
        self.dozen_seq_patterns  = {}
        self.column_seq_patterns = {}
        self.last_20        = []
        self.total_spins    = 0
        self.connected      = False
        self.poll_count     = 0
        self.last_poll_ok   = 0.0
        self.last_error     = None

    def update(self, data: dict):
        try:
            self.last_20             = data.get("last_20",             self.last_20)
            self.stats_dozen         = data.get("stats_dozen",         self.stats_dozen)
            self.stats_column        = data.get("stats_column",        self.stats_column)
            self.stats_color         = data.get("stats_color",         self.stats_color)
            self.stats_zone          = data.get("stats_zone",          self.stats_zone)
            self.color_patterns      = data.get("color_patterns",      self.color_patterns)
            self.zone_patterns       = data.get("zone_patterns",       self.zone_patterns)
            self.dozen_signals       = data.get("dozen_signals",       self.dozen_signals)
            self.column_signals      = data.get("column_signals",      self.column_signals)
            self.dozen_seq_patterns  = data.get("dozen_seq_patterns",  self.dozen_seq_patterns)
            self.column_seq_patterns = data.get("column_seq_patterns", self.column_seq_patterns)
            self.total_spins         = data.get("total_spins",         self.total_spins)
            self.connected           = True
            self.poll_count         += 1
            self.last_poll_ok        = time.time()
            self.last_error          = None
        except Exception as e:
            self.last_error = str(e)

    def get_ph_probs_raw(self, number: int) -> Optional[Dict]:
        data = self.stats_dozen.get(str(number), {})
        if data.get("total", 0) < 10: return None
        return {1: data.get("1",0)/100.0, 2: data.get("2",0)/100.0, 3: data.get("3",0)/100.0}

    def get_ph_pair(self, number: int) -> Optional[Dict]:
        probs = self.get_ph_probs_raw(number)
        if probs is None: return None
        sp = sorted(probs.items(), key=lambda x: x[1], reverse=True)
        if sp[0][1] == 0: return None
        pair    = tuple(sorted([sp[0][0], sp[1][0]]))
        missing = list({1, 2, 3} - set(pair))[0]
        return {"pair": pair, "missing": missing, "prob": sp[0][1] + sp[1][1]}

    def get_dozen_signal_winrate(self, last_number: int) -> Optional[float]:
        by_num = self.dozen_signals.get("by_number", {})
        stats  = by_num.get(str(last_number), {})
        total  = stats.get("total", 0)
        if total < DOZEN_HIST_MIN:
            return None
        return stats.get("aciertos", 0) / total

    def get_column_signal_winrate(self, last_number: int) -> Optional[float]:
        by_num = self.column_signals.get("by_number", {})
        stats  = by_num.get(str(last_number), {})
        total  = stats.get("total", 0)
        if total < DOZEN_HIST_MIN:
            return None
        return stats.get("aciertos", 0) / total

    def get_dozen_seq_top_pair(self, last_number: int) -> Optional[dict]:
        by_num = self.dozen_seq_patterns.get("by_number", {})
        stats  = by_num.get(str(last_number), {})
        total  = stats.get("total", 0)
        if total < DOZEN_HIST_MIN:
            return None
        top = stats.get("top_pair", {})
        return {
            "pair":            top.get("pair"),
            "efectividad":     stats.get("efectividad", 0.0),
            "top_efectividad": top.get("efectividad", 0.0),
            "total":           total,
        }

    def get_column_seq_top_pair(self, last_number: int) -> Optional[dict]:
        by_num = self.column_seq_patterns.get("by_number", {})
        stats  = by_num.get(str(last_number), {})
        total  = stats.get("total", 0)
        if total < DOZEN_HIST_MIN:
            return None
        top = stats.get("top_pair", {})
        return {
            "pair":            top.get("pair"),
            "efectividad":     stats.get("efectividad", 0.0),
            "top_efectividad": top.get("efectividad", 0.0),
            "total":           total,
        }

    def post_column_signal(self, strategy, pair, missing, prob, last_number):
        try:
            resp = requests.post(
                f"{STATS_URL}/signals/{TARGET_ROULETTE}/column",
                json={
                    "strategy":    str(strategy),
                    "pair":        list(pair),
                    "missing":     missing,
                    "prob":        round(prob, 6),
                    "last_number": last_number,
                },
                timeout=3
            )
            if resp.status_code == 200:
                logger.debug(f"[SERVER] ✅ Señal columna registrada: par={pair} last={last_number}")
            else:
                logger.debug(f"[SERVER] ⚠️ Señal columna: HTTP {resp.status_code}")
        except Exception as e:
            logger.debug(f"[SERVER] Señal columna no registrada (no crítico): {e}")

    def post_dozen_signal(self, strategy, pair, missing, prob, last_number):
        try:
            resp = requests.post(
                f"{STATS_URL}/signals/{TARGET_ROULETTE}/dozen",
                json={
                    "strategy":    str(strategy),
                    "pair":        list(pair),
                    "missing":     missing,
                    "prob":        round(prob, 6),
                    "last_number": last_number,
                },
                timeout=3
            )
            if resp.status_code == 200:
                logger.debug(f"[SERVER] ✅ Señal docena registrada: par={pair} last={last_number}")
            else:
                logger.debug(f"[SERVER] ⚠️ Señal docena: HTTP {resp.status_code}")
        except Exception as e:
            logger.debug(f"[SERVER] Señal docena no registrada (no crítico): {e}")

    def predict_color_signals(self, last_number: int) -> List[Dict]:
        raw = self.color_patterns.get("pendings")
        if raw is None:
            single = self.color_patterns.get("pending")
            raw = [single] if single else []
        summary = self.color_patterns.get("summary", {})
        results = []
        for pending in raw:
            if not pending: continue
            pid      = pending.get("pid", "")
            bet      = pending.get("bet", "")
            sequence = pending.get("sequence", [])
            p3 = self._global_eff(summary, pid)
            if p3 is None: continue
            p1 = None
            if last_number is not None and last_number != 0:
                color_trans = self.stats_color.get(str(last_number), {})
                if color_trans.get("total", 0) >= TRANS_MIN_SAMPLES:
                    if bet == "Negro":   p1 = color_trans.get("N", 0) / 100.0
                    elif bet == "Rojo":  p1 = color_trans.get("R", 0) / 100.0
            if p1 is not None:
                prob = round(P1_W_COLOR * p1 + P3_W_COLOR * p3, 4)
                components = f"Trans={p1:.0%} | Global={p3:.0%}"
            else:
                prob = p3
                components = f"Global={p3:.0%}"
            results.append({
                "type": "color", "pid": pid, "bet": bet, "prob": prob,
                "p1_trans": p1 or 0, "p3_global": p3,
                "sequence": sequence, "components": components,
            })
        return results

    def predict_zone_signals(self, last_number: int) -> List[Dict]:
        raw = self.zone_patterns.get("pendings")
        if raw is None:
            single = self.zone_patterns.get("pending")
            raw = [single] if single else []
        summary = self.zone_patterns.get("summary", {})
        results = []
        for pending in raw:
            if not pending: continue
            pid      = pending.get("pid", "")
            bet      = pending.get("bet", "")
            sequence = pending.get("sequence", [])
            p3 = self._global_eff(summary, pid)
            if p3 is None: continue
            p1 = None
            if last_number is not None and last_number != 0:
                zone_trans = self.stats_zone.get(str(last_number), {})
                if zone_trans.get("total", 0) >= TRANS_MIN_SAMPLES:
                    if bet == "Bajo":   p1 = zone_trans.get("B", 0) / 100.0
                    elif bet == "Alto": p1 = zone_trans.get("A", 0) / 100.0
            if p1 is not None:
                prob = round(P1_W_COLOR * p1 + P3_W_COLOR * p3, 4)
                components = f"Trans={p1:.0%} | Global={p3:.0%}"
            else:
                prob = p3
                components = f"Global={p3:.0%}"
            results.append({
                "type": "zone", "pid": pid, "bet": bet, "prob": prob,
                "p1_trans": p1 or 0, "p3_global": p3,
                "sequence": sequence, "components": components,
            })
        return results

    @staticmethod
    def _global_eff(summary, pid) -> Optional[float]:
        d = summary.get(pid, {})
        t = d.get("total", 0)
        if t < 5: return None
        return round(d.get("aciertos", 0) / t, 4)

# ─── EMA ──────────────────────────────────────────────────────────────────────
def calc_ema(data, period):
    if len(data) < period: return [None] * len(data)
    mult = 2 / (period + 1)
    out  = [None] * (period - 1)
    prev = sum(data[:period]) / period
    out.append(prev)
    for v in data[period:]:
        prev = v*mult + prev*(1-mult)
        out.append(prev)
    return out

def ema_signal(levels, mode="moderado"):
    if len(levels) < 20: return False
    e4, e8, e20 = calc_ema(levels,4), calc_ema(levels,8), calc_ema(levels,20)
    li = len(levels) - 1
    if any(v is None for v in [e4[li],e8[li],e20[li]]): return False
    cur = levels[li]
    ce4,ce8,ce20 = e4[li],e8[li],e20[li]
    pe4  = e4[li-1]  if li > 0 and e4[li-1]  is not None else ce4
    pe8  = e8[li-1]  if li > 0 and e8[li-1]  is not None else ce8
    pe20 = e20[li-1] if li > 0 and e20[li-1] is not None else ce20
    if mode == "tendencia":
        return (pe4<=pe20 and ce4>ce20) or (cur>ce4 and cur>ce8 and cur>ce20)
    vp = False
    if len(levels) >= 3:
        a,b,c = levels[-3],levels[-2],levels[-1]
        vp = (b<a) and (b<c) and (c>a)
    return (pe4<=pe8 and ce4>ce8) or (pe8<=pe20 and ce8>ce20) or \
           (cur>ce4 and cur>ce8) or vp

def ema_trend_str(levels) -> str:
    if len(levels) < 20: return "neutral"
    e4,e8,e20 = calc_ema(levels,4),calc_ema(levels,8),calc_ema(levels,20)
    li = len(levels)-1
    v4,v8,v20 = e4[li],e8[li],e20[li]
    if any(v is None for v in [v4,v8,v20]): return "neutral"
    cur = levels[li]
    if cur>v4 and v4>v8 and v8>v20: return "bull"
    if cur<v4 and v4<v8 and v8<v20: return "bear"
    return "neutral"

# ─── MARKOV ───────────────────────────────────────────────────────────────────
class SmoothedMarkovPredictor:
    def __init__(self, window=60, order=2):
        self.window=window; self.order=order; self.transition_counts={}

    def update(self, sequence):
        self.transition_counts = defaultdict(lambda: defaultdict(int))
        recent = sequence[-self.window:]
        if len(recent) < self.order+1: return
        for i in range(len(recent)-self.order):
            self.transition_counts[tuple(recent[i:i+self.order])][recent[i+self.order]] += 1

    def predict(self, sequence):
        if len(sequence) < self.order: return None
        counts = dict(self.transition_counts.get(tuple(sequence[-self.order:]),{}))
        total  = sum(counts.values())
        if total < 10: return None
        alpha=2.0; vs=3
        probs = {k:(v+alpha)/(total+alpha*vs) for k,v in counts.items()}
        for c in [1,2,3]:
            if c not in probs: probs[c] = alpha/(total+alpha*vs)
        return probs

# ─── ENSEMBLE ML ──────────────────────────────────────────────────────────────
class OnlineEnsemblePredictor:
    WINDOW=5; CLASSES=[1,2,3]

    def __init__(self):
        self.mnb = MultinomialNB(alpha=2.0, class_prior=[0.333,0.333,0.333])
        self.sgd = SGDClassifier(loss="log_loss",learning_rate="adaptive",eta0=0.005,
                                 penalty="l2",alpha=0.01,epsilon=0.2)
        self.trained=False

    def _extract_features(self, hist, pf_p, ph_p):
        if len(hist)<self.WINDOW: return None
        features=[]
        for i in range(1,self.WINDOW+1):
            d=hist[-i]; vec=[0,0,0]; vec[d-1]=1; features.extend(vec)
        for pair in (pf_p,ph_p):
            vec=[0,0,0]
            for x in pair: vec[x-1]=1
            features.extend(vec)
        return features

    def partial_train(self, hist, target, pf_p, ph_p):
        feats=self._extract_features(hist[:-1],pf_p,ph_p)
        if feats is None: return
        X=np.array(feats).reshape(1,-1); y=np.array([target])
        if not self.trained:
            self.mnb.partial_fit(X,y,classes=self.CLASSES)
            self.sgd.partial_fit(X,y,classes=self.CLASSES)
            self.trained=True
        else:
            self.mnb.partial_fit(X,y); self.sgd.partial_fit(X,y)

    def predict(self, hist, pf_p, ph_p):
        if not self.trained: return None
        feats=self._extract_features(hist,pf_p,ph_p)
        if feats is None: return None
        X=np.array(feats).reshape(1,-1)
        try:
            pm=dict(zip(self.CLASSES,self.mnb.predict_proba(X)[0]))
            ps=dict(zip(self.CLASSES,self.sgd.predict_proba(X)[0]))
            return {c:0.5*pm[c]+0.5*ps[c] for c in self.CLASSES}
        except Exception: return None

# ─── SIGNAL LEARNER ───────────────────────────────────────────────────────────
class SignalLearner:
    MAX_HISTORY=500; WINDOW=50; MIN_SAMPLES=5

    _COLS=["id","ts","strategy","pair","missing","prob","intento_start","nivel",
           "pf_prob","phf_prob","ema_trend","last_number","dozen_seq_5",
           "result","intento_fin","reason"]

    def __init__(self, db: sqlite3.Connection):
        self.db=db; self.history=deque(maxlen=self.MAX_HISTORY)
        self.pending_id: Optional[int]=None
        self._init_db(); self._load_history()

    def _init_db(self):
        self.db.execute("""CREATE TABLE IF NOT EXISTS signal_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL NOT NULL,
            strategy INTEGER, pair TEXT, missing INTEGER, prob REAL,
            intento_start INTEGER, nivel INTEGER DEFAULT 1,
            pf_prob REAL, phf_prob REAL, ema_trend TEXT,
            last_number INTEGER, dozen_seq_5 TEXT,
            result TEXT, intento_fin INTEGER, reason TEXT
        )"""); self.db.commit()

    def _row_to_dict(self, row) -> dict:
        d=dict(zip(self._COLS,row))
        raw=d.get("pair","")
        try: d["pair"]=tuple(int(x) for x in raw.split(",") if x)
        except: d["pair"]=()
        return d

    def _load_history(self):
        try:
            rows=self.db.execute(
                "SELECT * FROM signal_log WHERE result IS NOT NULL ORDER BY id DESC LIMIT ?",
                (self.MAX_HISTORY,)
            ).fetchall()
            for row in reversed(rows): self.history.append(self._row_to_dict(row))
            logger.info(f"[Learner] 📚 {len(self.history)} señales cargadas")
        except Exception as e:
            logger.warning(f"[Learner] ⚠️ Error cargando historial: {e}")

    def register_signal(self, strategy, pair, missing, prob,
                        pf_prob, phf_prob, ema_trend, last_number, dozen_seq_5):
        try:
            cur=self.db.execute(
                """INSERT INTO signal_log
                   (ts,strategy,pair,missing,prob,intento_start,nivel,pf_prob,
                    phf_prob,ema_trend,last_number,dozen_seq_5)
                   VALUES(?,?,?,?,?,1,1,?,?,?,?,?)""",
                (time.time(),strategy,
                 ",".join(str(x) for x in sorted(pair)),
                 missing,round(prob,6),round(pf_prob,6),round(phf_prob,6),
                 ema_trend,last_number,",".join(str(x) for x in dozen_seq_5))
            )
            self.db.commit(); self.pending_id=cur.lastrowid
        except Exception as e:
            logger.warning(f"[Learner] ⚠️ Error registrando señal: {e}")

    def resolve(self, result: str, reason: str=""):
        if self.pending_id is None: return
        try:
            self.db.execute(
                "UPDATE signal_log SET result=?,intento_fin=1,reason=? WHERE id=?",
                (result,reason,self.pending_id)
            )
            self.db.commit()
            row=self.db.execute("SELECT * FROM signal_log WHERE id=?",
                                (self.pending_id,)).fetchone()
            if row: self.history.append(self._row_to_dict(row))
        except Exception as e:
            logger.warning(f"[Learner] ⚠️ Error resolviendo señal: {e}")
        finally:
            self.pending_id=None

    def _recent(self) -> List[dict]:
        completed=[s for s in self.history if s.get("result") in ("WIN","LOSS")]
        return list(completed)[-self.WINDOW:]

    def _win_rate(self, subset) -> Optional[float]:
        if len(subset)<self.MIN_SAMPLES: return None
        return sum(1 for s in subset if s["result"]=="WIN")/len(subset)

    @staticmethod
    def _adj(wr,scale):
        return 0.0 if wr is None else round((wr-0.5)*2.0*scale,4)

    def strat_adj(self,s): return self._adj(self._win_rate([x for x in self._recent() if x.get("strategy")==s]),0.08)
    def pair_adj(self,p):
        key=tuple(sorted(p))
        return self._adj(self._win_rate([x for x in self._recent() if tuple(sorted(x.get("pair",())))== key]),0.06)
    def trend_adj(self,t): return self._adj(self._win_rate([x for x in self._recent() if x.get("ema_trend")==t]),0.05)

    def get_adjustment(self, strategy, pair, ema_trend) -> Tuple[float, str]:
        s=self.strat_adj(strategy); p=self.pair_adj(pair); t=self.trend_adj(ema_trend)
        total=round(max(-0.18,min(0.18,s+p+t)),4)
        parts=[]
        if abs(s)>=0.005: parts.append(f"Strat:{s:+.3f}")
        if abs(p)>=0.005: parts.append(f"Par:{p:+.3f}")
        if abs(t)>=0.005: parts.append(f"EMA:{t:+.3f}")
        return total," | ".join(parts) if parts else "sin ajuste"

    def get_summary(self, n=30) -> str:
        recent=self._recent()[-n:]
        total_db=0
        try:
            row=self.db.execute("SELECT COUNT(*) FROM signal_log WHERE result IS NOT NULL").fetchone()
            total_db=row[0] if row else 0
        except: pass
        if not recent:
            return "🧠 <b>Aprendizaje activo</b>\n\nAún sin señales resueltas."
        total=len(recent); wins=sum(1 for s in recent if s["result"]=="WIN")
        eff=wins/total*100
        si={STRAT_E1:"🅐E1",STRAT_E2:"🅑E2",STRAT_E3:"🅒E3",STRAT_COLOR:"🎨Color",STRAT_ZONE:"🗺Zona"}
        lines=[
            "🧠 <b>APRENDIZAJE ADAPTATIVO</b>",
            f"Total DB: {total_db} | Ventana: {total} | {wins}/{total} ({eff:.1f}%)\n",
            "<b>Por estrategia:</b>",
        ]
        for st in [STRAT_E1,STRAT_E2,STRAT_E3,STRAT_COLOR,STRAT_ZONE]:
            sb=[s for s in recent if s.get("strategy")==st]
            if not sb: continue
            sw=sum(1 for s in sb if s["result"]=="WIN"); wr=sw/len(sb)*100
            adj=self.strat_adj(st); bar="▓"*int(wr/10)+"░"*(10-int(wr/10))
            lines.append(f"  {si[st]}: {sw}/{len(sb)} ({wr:.0f}%) {bar} adj:{adj:+.3f}")
        return "\n".join(lines)

# ─── ENGINE PRINCIPAL ─────────────────────────────────────────────────────────
class ImmersiveRouletteEngine:
    def __init__(self, sc: StatsClient):
        self.sc = sc

        self.spin_history: List[dict] = []
        self.dozen_seq: List[int] = []
        self.column_seq: List[int] = []
        self.color_seq: List[str] = []
        self.zone_seq: List[str] = []
        self.d_levels: Dict[int,List] = {1:[],2:[],3:[]}
        self.c_levels: Dict[int,List] = {1:[],2:[],3:[]}
        self.doc_levels: List[float] = []
        self.col_levels: List[float] = []
        self._last_doc_inc: float = 0
        self._last_col_inc: float = 0

        self.after_number_dozen = defaultdict(lambda: defaultdict(int))
        self.after_number_column = defaultdict(lambda: defaultdict(int))

        self.markov_d = SmoothedMarkovPredictor()
        self.ensemble_d = OnlineEnsemblePredictor()
        self.spins_since_train_d = 0

        self.markov_col = SmoothedMarkovPredictor()
        self.ensemble_col = OnlineEnsemblePredictor()
        self.spins_since_train_col = 0

        self.MAX_INTENTOS_DOCENA  = 2
        self.MAX_INTENTOS_COLOR   = 2
        self.MAX_INTENTOS_ZONA    = 2
        self.MAX_INTENTOS_COLUMNA = 2

        self.signal_active = False
        self.active_strategy = None
        self.active_pair = ()
        self.active_missing = 0
        self.active_signal_msg_id_main = None
        self.active_intento = 1
        self.active_type = None

        self.color_signals: Dict[str, dict] = {}
        self.zone_signals: Dict[str, dict] = {}

        self._db = _get_db()
        self.learner = SignalLearner(self._db)

        self.processed_game_ids = {}
        self.MAX_PROCESSED_IDS = 300
        self._load_processed_ids()
        self._first_poll_done = False

        self._scoreboard_dirty = False

        self._pending_triple_dozen_id = None
        self._pending_triple_dozen_pair = None
        self._pending_triple_column_id = None
        self._pending_triple_column_pair = None

        live_loaded = self._load_live_history()
        self.ws_count = live_loaded
        self.warmup_done = live_loaded >= WARMUP_SPINS
        logger.info(f"[ImmersiveDC] 📦 Pre-cargados: {live_loaded} | Warmup: {'✅' if self.warmup_done else '⏳'} | Learner: {len(self.learner.history)}")

    def _load_processed_ids(self):
        try:
            cutoff = int(time.time()) - 3600
            rows = self._db.execute(
                "SELECT game_id FROM processed_spins WHERE ts > ?", (cutoff,)
            ).fetchall()
            for row in rows:
                self.processed_game_ids[row[0]] = True
            logger.info(f"[ImmersiveDC] 🔒 {len(rows)} game_ids cargados desde DB (anti-dup)")
        except Exception as e:
            logger.warning(f"[ImmersiveDC] ⚠️ Error cargando processed_ids: {e}")

    def _load_live_history(self) -> int:
        try:
            rows = self._db.execute(
                "SELECT number,color,zone FROM live_spins ORDER BY id ASC"
            ).fetchall()
        except: return 0
        for (n, c, z) in rows:
            self._update_state(n, persist=False, train_model=False)
        if rows:
            self.markov_d.update(self.dozen_seq)
            self.markov_col.update(self.column_seq)
        return len(rows)

    def _persist(self, number, color, zone):
        try:
            self._db.execute("INSERT INTO live_spins(number,color,zone,ts) VALUES(?,?,?,?)",
                             (number, color, zone, int(time.time())))
            self._db.commit()
        except Exception as e:
            logger.debug(f"⚠️ DB persist: {e}")

    def _update_state(self, number: int, persist=True, train_model=True):
        color = get_color(number)
        zone = get_zone(number)
        d = get_dozen(number)
        col = get_column(number)

        if number != 0 and self.spin_history:
            prev = self.spin_history[-1]["number"]
            if prev != 0:
                self.after_number_dozen[prev][d] += 1
                self.after_number_column[prev][col] += 1

        self.spin_history.append({"number": number, "color": color, "zone": zone})
        self.color_seq.append(color)
        self.zone_seq.append(zone)
        if len(self.color_seq) > 200: self.color_seq.pop(0)
        if len(self.zone_seq) > 200:  self.zone_seq.pop(0)

        if d != 0:
            for dd in (1, 2, 3):
                prev_lvl = self.d_levels[dd][-1] if self.d_levels[dd] else 0
                self.d_levels[dd].append(prev_lvl + (1 if d == dd else -1))
                if len(self.d_levels[dd]) > 300: self.d_levels[dd].pop(0)
            self.dozen_seq.append(d)
            if len(self.dozen_seq) > 200: self.dozen_seq.pop(0)
            if train_model and len(self.dozen_seq) > 5:
                pf_d = self._get_pf()
                ph_d = self._get_ph()
                if pf_d and ph_d:
                    self.ensemble_d.partial_train(self.dozen_seq, d, pf_d["pair"], ph_d["pair"])
                self.spins_since_train_d += 1
                if self.spins_since_train_d >= TRAIN_INTERVAL:
                    self.markov_d.update(self.dozen_seq)
                    self.spins_since_train_d = 0

        if col != 0:
            for cc in (1, 2, 3):
                prev_lvl = self.c_levels[cc][-1] if self.c_levels[cc] else 0
                self.c_levels[cc].append(prev_lvl + (1 if col == cc else -1))
                if len(self.c_levels[cc]) > 300: self.c_levels[cc].pop(0)
            self.column_seq.append(col)
            if len(self.column_seq) > 200: self.column_seq.pop(0)
            if train_model and len(self.column_seq) > 5:
                pf_c = self._get_col_pf()
                ph_c = self._get_col_ph()
                if pf_c and ph_c:
                    self.ensemble_col.partial_train(self.column_seq, col, pf_c["pair"], ph_c["pair"])
                self.spins_since_train_col += 1
                if self.spins_since_train_col >= TRAIN_INTERVAL:
                    self.markov_col.update(self.column_seq)
                    self.spins_since_train_col = 0

        inc_d = 1 if d == 1 else (-1 if d == 3 else (1 if number != 0 and number <= 18 else -1))
        if number == 0: inc_d = self._last_doc_inc
        else: self._last_doc_inc = inc_d
        prev_d = self.doc_levels[-1] if self.doc_levels else 0
        self.doc_levels.append(prev_d + inc_d)
        if len(self.doc_levels) > 300: self.doc_levels.pop(0)

        inc_c = 1 if col == 1 else (-1 if col == 3 else 0)
        if number == 0: inc_c = self._last_col_inc
        else: self._last_col_inc = inc_c
        prev_c = self.col_levels[-1] if self.col_levels else 0
        self.col_levels.append(prev_c + inc_c)
        if len(self.col_levels) > 300: self.col_levels.pop(0)

        if persist:
            self._persist(number, color, zone)

    # ── PF / PH / PHF para docenas ─────────────────────────────────────────────
    def _get_pf(self):
        if len(self.spin_history) < 5: return None
        counts = {1:0,2:0,3:0}
        for s in self.spin_history[-5:]:
            n = s["number"]
            if n != 0: counts[get_dozen(n)] += 1
        active = [k for k, v in counts.items() if v > 0]
        if len(active) != 2: return None
        pair = tuple(sorted(active))
        missing = list({1,2,3} - set(pair))[0]
        return {"pair": pair, "missing": missing, "prob": sum(counts[a] for a in pair) / 5.0}

    def _get_ph(self, number=None):
        if number is None:
            if not self.spin_history: return None
            number = self.spin_history[-1]["number"]
        if number == 0: return None
        srv = self.sc.get_ph_pair(number)
        if srv: return srv
        counts = self.after_number_dozen.get(number, {})
        total = sum(counts.values())
        if total < 10: return None
        sc = sorted(counts.items(), key=lambda x: x[1], reverse=True)
        if len(sc) < 2: return None
        pair = tuple(sorted([sc[0][0], sc[1][0]]))
        missing = list({1,2,3} - set(pair))[0]
        return {"pair": pair, "missing": missing, "prob": (sc[0][1]+sc[1][1])/total}

    def _get_phf(self, number):
        if number == 0: return None
        phtml = self._get_phtml_probs(number)
        if phtml is None: return None
        ph = self.sc.get_ph_probs_raw(number)
        if ph is None:
            counts = self.after_number_dozen.get(number, {})
            total = sum(counts.values())
            if total >= 10:
                ph = {1: counts.get(1,0)/total, 2: counts.get(2,0)/total, 3: counts.get(3,0)/total}
        phf_raw = ({d: PHTML_W * phtml[d] + PH_W_COMBINE * ph[d] for d in [1,2,3]}
                   if ph is not None else dict(phtml))
        total = sum(phf_raw.values())
        if total == 0: return None
        phf = {d: v/total for d, v in phf_raw.items()}
        sd = sorted(phf.items(), key=lambda x: x[1], reverse=True)
        pair = tuple(sorted([sd[0][0], sd[1][0]]))
        missing = list({1,2,3} - set(pair))[0]
        return {"pair": pair, "missing": missing, "prob": phf[sd[0][0]] + phf[sd[1][0]], "probs": phf}

    def _get_phtml_probs(self, number):
        if number == 0: return None
        entry = DOZEN_TABLE.get(number)
        if not entry: return None
        d1, d2, d3 = entry["d1"], entry["d2"], entry["d3"]
        total = d1 + d2 + d3
        if total == 0: return None
        return {1: d1/total, 2: d2/total, 3: d3/total}

    # ── PF / PH / PHF para columnas ────────────────────────────────────────────
    def _get_col_pf(self):
        if len(self.spin_history) < 5: return None
        counts = {1:0,2:0,3:0}
        for s in self.spin_history[-5:]:
            n = s["number"]
            if n != 0: counts[get_column(n)] += 1
        active = [k for k, v in counts.items() if v > 0]
        if len(active) != 2: return None
        pair = tuple(sorted(active))
        missing = list({1,2,3} - set(pair))[0]
        return {"pair": pair, "missing": missing, "prob": sum(counts[a] for a in pair) / 5.0}

    def _get_col_ph(self, number=None):
        if number is None:
            if not self.spin_history: return None
            number = self.spin_history[-1]["number"]
        if number == 0: return None
        counts = self.after_number_column.get(number, {})
        total = sum(counts.values())
        if total < 10: return None
        sc = sorted(counts.items(), key=lambda x: x[1], reverse=True)
        if len(sc) < 2: return None
        pair = tuple(sorted([sc[0][0], sc[1][0]]))
        missing = list({1,2,3} - set(pair))[0]
        return {"pair": pair, "missing": missing, "prob": (sc[0][1]+sc[1][1])/total}

    def _get_col_phf(self, number):
        if number == 0: return None
        pf = self._get_col_pf()
        ph = self._get_col_ph(number)
        if not pf or not ph: return None
        base = PF_W_NORM * pf["prob"] + PH_W_NORM * ph["prob"]
        pair = pf["pair"]
        missing = list({1,2,3} - set(pair))[0]
        return {"pair": pair, "missing": missing, "prob": base}

    # ── ML helpers ─────────────────────────────────────────────────────────────
    def _predict_pair_ml_dozen(self, missing_num):
        mk_pred = self.markov_d.predict(self.dozen_seq)
        m_p_miss = mk_pred.get(missing_num, 1/3) if mk_pred else 1/3
        pf_d = self._get_pf(); ph_d = self._get_ph(); ens_p_miss = 1/3
        if pf_d and ph_d:
            ens = self.ensemble_d.predict(self.dozen_seq, pf_d["pair"], ph_d["pair"])
            if ens: ens_p_miss = ens.get(missing_num, 1/3)
        ml_miss = 0.4*m_p_miss + 0.6*ens_p_miss
        levels = self.d_levels.get(missing_num, [])
        if len(levels) >= 20:
            if ema_signal(levels, "tendencia"): ml_miss *= 0.85
            elif ema_signal(levels, "moderado"): ml_miss *= 0.92
        return 1.0 - ml_miss

    def _predict_pair_ml_column(self, missing_num):
        mk_pred = self.markov_col.predict(self.column_seq)
        m_p_miss = mk_pred.get(missing_num, 1/3) if mk_pred else 1/3
        pf_c = self._get_col_pf(); ph_c = self._get_col_ph(); ens_p_miss = 1/3
        if pf_c and ph_c:
            ens = self.ensemble_col.predict(self.column_seq, pf_c["pair"], ph_c["pair"])
            if ens: ens_p_miss = ens.get(missing_num, 1/3)
        ml_miss = 0.4*m_p_miss + 0.6*ens_p_miss
        levels = self.c_levels.get(missing_num, [])
        if len(levels) >= 20:
            if ema_signal(levels, "tendencia"): ml_miss *= 0.85
            elif ema_signal(levels, "moderado"): ml_miss *= 0.92
        return 1.0 - ml_miss

    # ── E1 / E2 / E3 docenas ───────────────────────────────────────────────────
    def _detect_e1_dozen(self):
        if not self.warmup_done or not self.spin_history: return None
        last_num = self.spin_history[-1]["number"]
        if last_num == 0: return None
        pf_d = self._get_pf()
        if not pf_d: return None
        phf_d = self._get_phf(last_num)
        if not phf_d: return None
        if set(pf_d["pair"]) != set(phf_d["pair"]): return None
        base = PF_W_NORM*pf_d["prob"] + PH_W_NORM*phf_d["prob"]
        ml = self._predict_pair_ml_dozen(pf_d["missing"])
        prob = BASE_W_NORM*base + ML_W_NORM*ml
        trend = ema_trend_str(self.doc_levels)
        adj, _ = self.learner.get_adjustment(STRAT_E1, pf_d["pair"], trend)
        prob_adj = round(max(0.0, min(1.0, prob + adj)), 4)
        if prob_adj < MIN_PROB: return None
        return {"strategy": STRAT_E1, "pair": pf_d["pair"], "missing": pf_d["missing"],
                "prob": prob_adj, "label": "PF+PHF+ML", "pf_prob": pf_d["prob"],
                "phf_prob": phf_d["prob"], "ema_trend": trend, "last_number": last_num,
                "type": "dozen"}

    def _detect_e2_dozen(self):
        if not self.warmup_done or not self.spin_history: return None
        non_zero = [s["number"] for s in self.spin_history if s["number"] != 0]
        if len(non_zero) < 3: return None
        last3 = non_zero[-3:]
        dozens = [get_dozen(n) for n in last3]
        if len(set(dozens)) != 1: return None
        repeating_dozen = dozens[0]
        pair = tuple(sorted({1, 2, 3} - {repeating_dozen}))
        triple_str = ",".join(str(d) for d in dozens)
        prob = self._get_triple_prob("dozen", triple_str)
        return {"strategy": STRAT_E2, "pair": pair, "missing": repeating_dozen,
                "prob": prob, "label": f"Rep.D{repeating_dozen}x3 hist",
                "pf_prob": prob, "phf_prob": prob,
                "ema_trend": ema_trend_str(self.doc_levels),
                "last_number": last3[-1], "type": "dozen"}

    def _detect_e3_dozen(self):
        if not self.warmup_done: return None
        non_zero = [s["number"] for s in self.spin_history if s["number"] != 0]
        if len(non_zero) < 6: return None
        prev5 = non_zero[-6:-1]; last_n = non_zero[-1]
        cats5 = list(set(get_dozen(n) for n in prev5))
        if len(cats5) != 2: return None
        pair = tuple(sorted(cats5)); last_dozen = get_dozen(last_n)
        if last_dozen in pair: return None
        streak = 0
        for n in reversed(non_zero[:-1]):
            if get_dozen(n) in pair: streak += 1
            else: break
        phf_break = self._get_phf(last_n)
        if phf_break is None or set(phf_break["pair"]) != set(pair): return None
        return_prob = self._calc_return_prob_dozen(pair, streak, last_n)
        trend = ema_trend_str(self.doc_levels)
        adj, _ = self.learner.get_adjustment(STRAT_E3, pair, trend)
        return_prob_adj = round(max(0.0, min(1.0, return_prob + adj)), 4)
        if return_prob_adj < MIN_PROB: return None
        return {"strategy": STRAT_E3, "pair": pair, "missing": last_dozen,
                "prob": return_prob_adj, "label": f"RETORNO(racha {streak}g)",
                "pf_prob": return_prob, "phf_prob": phf_break["prob"],
                "ema_trend": trend, "last_number": last_n, "type": "dozen"}

    def _calc_return_prob_dozen(self, pair, streak, break_num):
        non_zero = [s["number"] for s in self.spin_history if s["number"] != 0]
        last20 = non_zero[-20:]
        pair_count = sum(1 for n in last20 if get_dozen(n) in pair)
        base_prob = pair_count / len(last20) if last20 else 0.66
        streak_bst = min(0.40, streak * 0.04)
        brk_d = get_dozen(break_num)
        brk_adj = 0.02 if brk_d in pair else -0.04
        missing = list({1,2,3} - set(pair))[0]
        levels = self.d_levels.get(missing, [])
        ema_adj = 0.0
        if len(levels) >= 20:
            if ema_signal(levels, "tendencia"): ema_adj = -0.08
            elif ema_signal(levels, "moderado"): ema_adj = -0.04
        return round(max(0.35, min(0.97, base_prob + streak_bst + brk_adj + ema_adj)), 4)

    # ── E1 / E2 / E3 columnas ──────────────────────────────────────────────────
    def _detect_e1_column(self):
        if not self.warmup_done or not self.spin_history: return None
        last_num = self.spin_history[-1]["number"]
        if last_num == 0: return None
        pf_c = self._get_col_pf()
        if not pf_c: return None
        phf_c = self._get_col_phf(last_num)
        if not phf_c: return None
        if set(pf_c["pair"]) != set(phf_c["pair"]): return None
        base = PF_W_NORM*pf_c["prob"] + PH_W_NORM*phf_c["prob"]
        ml = self._predict_pair_ml_column(pf_c["missing"])
        prob = BASE_W_NORM*base + ML_W_NORM*ml
        trend = ema_trend_str(self.col_levels)
        adj, _ = self.learner.get_adjustment(STRAT_E1, pf_c["pair"], trend)
        prob_adj = round(max(0.0, min(1.0, prob + adj)), 4)
        if prob_adj < MIN_PROB: return None
        return {"strategy": STRAT_E1, "pair": pf_c["pair"], "missing": pf_c["missing"],
                "prob": prob_adj, "label": "PF+PHF+ML", "pf_prob": pf_c["prob"],
                "phf_prob": phf_c["prob"], "ema_trend": trend, "last_number": last_num,
                "type": "column"}

    def _detect_e2_column(self):
        if not self.warmup_done: return None
        non_zero = [s["number"] for s in self.spin_history if s["number"] != 0]
        if len(non_zero) < 3: return None
        last3 = non_zero[-3:]
        cols = [get_column(n) for n in last3]
        if len(set(cols)) != 1: return None
        repeating_col = cols[0]
        pair = tuple(sorted({1, 2, 3} - {repeating_col}))
        triple_str = ",".join(str(c) for c in cols)
        prob = self._get_triple_prob("column", triple_str)
        return {"strategy": STRAT_E2, "pair": pair, "missing": repeating_col,
                "prob": prob, "label": f"Rep.C{repeating_col}x3 hist",
                "pf_prob": prob, "phf_prob": prob,
                "ema_trend": ema_trend_str(self.col_levels),
                "last_number": last3[-1], "type": "column"}

    def _detect_e3_column(self):
        if not self.warmup_done: return None
        non_zero = [s["number"] for s in self.spin_history if s["number"] != 0]
        if len(non_zero) < 6: return None
        prev5 = non_zero[-6:-1]; last_n = non_zero[-1]
        cats5 = list(set(get_column(n) for n in prev5))
        if len(cats5) != 2: return None
        pair = tuple(sorted(cats5)); last_col = get_column(last_n)
        if last_col in pair: return None
        streak = 0
        for n in reversed(non_zero[:-1]):
            if get_column(n) in pair: streak += 1
            else: break
        pf_break = self._get_col_pf()
        if pf_break is None or set(pf_break["pair"]) != set(pair): return None
        return_prob = self._calc_return_prob_column(pair, streak, last_n)
        trend = ema_trend_str(self.col_levels)
        adj, _ = self.learner.get_adjustment(STRAT_E3, pair, trend)
        return_prob_adj = round(max(0.0, min(1.0, return_prob + adj)), 4)
        if return_prob_adj < MIN_PROB: return None
        return {"strategy": STRAT_E3, "pair": pair, "missing": last_col,
                "prob": return_prob_adj, "label": f"RETORNO col (racha {streak}g)",
                "pf_prob": return_prob, "phf_prob": return_prob,
                "ema_trend": trend, "last_number": last_n, "type": "column"}

    def _calc_return_prob_column(self, pair, streak, break_num):
        non_zero = [s["number"] for s in self.spin_history if s["number"] != 0]
        last20 = non_zero[-20:]
        pair_count = sum(1 for n in last20 if get_column(n) in pair)
        base_prob = pair_count / len(last20) if last20 else 0.66
        streak_bst = min(0.40, streak * 0.04)
        brk_c = get_column(break_num)
        brk_adj = 0.02 if brk_c in pair else -0.04
        missing = list({1,2,3} - set(pair))[0]
        levels = self.c_levels.get(missing, [])
        ema_adj = 0.0
        if len(levels) >= 20:
            if ema_signal(levels, "tendencia"): ema_adj = -0.08
            elif ema_signal(levels, "moderado"): ema_adj = -0.04
        return round(max(0.35, min(0.97, base_prob + streak_bst + brk_adj + ema_adj)), 4)

    # ─── Probabilidad histórica para triples ──────────────────────────────────
    def _get_triple_prob(self, typ: str, triple_str: str) -> float:
        MIN_SAMPLES = 5
        DEFAULT_PROB = 0.80
        LAMBDA = 0.01
        try:
            rows = self._db.execute(
                "SELECT ts, win FROM triple_history WHERE type=? AND triple=? AND win IS NOT NULL",
                (typ, triple_str)
            ).fetchall()
            if len(rows) < MIN_SAMPLES:
                return DEFAULT_PROB
            now = time.time()
            weight_sum = 0.0
            weighted_wins = 0.0
            for ts, win in rows:
                weight = np.exp(-LAMBDA * (now - ts))
                weighted_wins += weight * win
                weight_sum += weight
            if weight_sum == 0:
                return DEFAULT_PROB
            wr = weighted_wins / weight_sum
            return max(0.55, min(0.95, wr))
        except Exception as e:
            logger.error(f"Error calculando triple prob: {e}")
            return DEFAULT_PROB

    # ─── Selección unificada de señal ──────────────────────────────────────────
    def _select_best_signal(self):
        candidates = []
        e1 = self._detect_e1_dozen()
        e2_d = self._detect_e2_dozen()
        e3_d = self._detect_e3_dozen()
        for sig in [e1, e2_d, e3_d]:
            if sig and sig["prob"] >= MIN_PROB:
                sig["type"] = "dozen"
                candidates.append(sig)
        e1_c = self._detect_e1_column()
        e2_c = self._detect_e2_column()
        e3_c = self._detect_e3_column()
        for sig in [e1_c, e2_c, e3_c]:
            if sig and sig["prob"] >= MIN_PROB:
                sig["type"] = "column"
                candidates.append(sig)
        if not candidates:
            return None
        candidates.sort(key=lambda x: x["prob"], reverse=True)
        return candidates[0]

    def _signal_text(self, typ, pair, intento):
        last5 = self._fmt_last_numbers(5)
        if typ == "dozen":
            return (f"✅✅ <b>SEÑAL DOCENAS</b> ✅✅\n\n"
                    f"⚪ Apuesta: D{pair[0]} y D{pair[1]}\n"
                    f"🆔 Intento: {intento}/2\n"
                    f"🕐 Últimos:\n{last5}")
        else:
            return (f"✅✅ <b>SEÑAL COLUMNAS</b> ✅✅\n\n"
                    f"⚪ Apuesta: C{pair[0]} y C{pair[1]}\n"
                    f"🆔 Intento: {intento}/2\n"
                    f"🕐 Últimos:\n{last5}")

    def _activate_signal(self, sig):
        self.signal_active = True
        self.active_strategy = sig["strategy"]
        self.active_pair = sig["pair"]
        self.active_missing = sig["missing"]
        self.active_type = sig["type"]
        p = sig["pair"]
        t = sig["type"]
        trend = sig.get("ema_trend", "neutral")
        msg_id_main = tg_send(self._signal_text(t, p, 1), markup=immersive_keyboard())
        if msg_id_main:
            self.active_signal_msg_id_main = msg_id_main
        self.learner.register_signal(
            strategy=sig["strategy"], pair=sig["pair"], missing=sig["missing"],
            prob=sig["prob"], pf_prob=sig.get("pf_prob",0),
            phf_prob=sig.get("phf_prob",0), ema_trend=trend,
            last_number=sig.get("last_number",0),
            dozen_seq_5=self.dozen_seq[-5:] if self.dozen_seq else []
        )
        if t == "dozen":
            self.sc.post_dozen_signal(sig["strategy"], list(p), sig["missing"], sig["prob"], sig.get("last_number",0))
        else:
            self.sc.post_column_signal(sig["strategy"], list(p), sig["missing"], sig["prob"], sig.get("last_number",0))
        logger.info(f"[ImmersiveDC] 🎯 SEÑAL {t.upper()} {sig['label']}: {p} ({sig['prob']:.0%})")

    def _resolve_signal(self, number):
        if not self.signal_active: return
        if self.active_type == "dozen":
            val = get_dozen(number)
            won = val != 0 and val in self.active_pair
        else:
            val = get_column(number)
            won = val != 0 and val in self.active_pair

        em = self._num_color_emoji(number)
        if won:
            op_txt = "1° OP" if self.active_intento == 1 else "2° OP"
            tg_send(f"✅ WIN #{number} {em} — ☑️ GANADA EN {op_txt}")
            scoreboard.record_win()
            self._scoreboard_dirty = True
            self.learner.resolve("WIN", f"WIN {self.active_type} #{number} par correcto intento {self.active_intento}")
            self._reset_signal()
        else:
            if self.active_intento < self.MAX_INTENTOS_DOCENA:
                if self.active_signal_msg_id_main:
                    tg_delete(CHAT_ID, self.active_signal_msg_id_main)
                self.active_intento += 1
                msg_id = tg_send(self._signal_text(self.active_type, self.active_pair, 2),
                                 markup=immersive_keyboard())
                if msg_id:
                    self.active_signal_msg_id_main = msg_id
                logger.info(f"[SIGNAL] 🔁 Intento 1 fallido → intento 2")
            else:
                tg_send(f"❌ LOSS #{number} {em} — PERDIDA EN 2° OP")
                scoreboard.record_loss()
                self._scoreboard_dirty = True
                self.learner.resolve("LOSS", f"LOSS {self.active_type} #{number} intento {self.active_intento}")
                self._reset_signal()

    def _reset_signal(self):
        self.signal_active = False
        self.active_strategy = None
        self.active_pair = ()
        self.active_missing = 0
        self.active_signal_msg_id_main = None
        self.active_intento = 1
        self.active_type = None

    # ─── Color y Zona ──────────────────────────────────────────────────────────
    def _color_signal_text(self, bet, intento, sequence=None):
        apuesta_txt = {"Negro": "NEGRO ⚫", "Rojo": "ROJO 🔴"}.get(bet, bet)
        pat = self._color_seq_str(sequence) if sequence else "—"
        last5 = self._fmt_last_numbers(5)
        return (f"✅✅ <b>SEÑAL COLOR</b> ✅✅\n\n"
                f"⚪ Apuesta: {apuesta_txt}\n"
                f"🟡 Patrón: {pat}\n"
                f"🆔 Intento: {intento}/2\n"
                f"🕐 Últimos:\n{last5}")

    def _check_color_signal(self, number):
        color = get_color(number)
        em = self._num_color_emoji(number)
        pids_done = []
        for pid, sig in list(self.color_signals.items()):
            bet = sig["bet"]
            won = (bet == "Negro" and color == "N") or (bet == "Rojo" and color == "R")
            if won:
                op_txt = "1° OP" if sig["intento"] == 1 else "2° OP"
                tg_send(f"✅ WIN #{number} {em} — ☑️ GANADA EN {op_txt}")
                scoreboard.record_win()
                self._scoreboard_dirty = True
                self.learner.resolve("WIN", f"COLOR WIN #{number} bet={bet} pid={pid} intento {sig['intento']}")
                pids_done.append(pid)
            else:
                if sig["intento"] < self.MAX_INTENTOS_COLOR:
                    if sig.get("msg_id"):
                        tg_delete(CHAT_ID, sig["msg_id"])
                    sig["intento"] += 1
                    msg_id = tg_send(self._color_signal_text(bet, sig["intento"], sig.get("sequence", [])),
                                     markup=immersive_keyboard())
                    if msg_id: sig["msg_id"] = msg_id
                    logger.info(f"[COLOR] 🔁 {pid} intento 1 fallido → intento 2")
                else:
                    tg_send(f"❌ LOSS #{number} {em} — PERDIDA EN 2° OP")
                    scoreboard.record_loss()
                    self._scoreboard_dirty = True
                    self.learner.resolve("LOSS", f"COLOR LOSS #{number} bet={bet} pid={pid} intento {sig['intento']}")
                    pids_done.append(pid)
        for pid in pids_done:
            self.color_signals.pop(pid, None)
        if self.color_signals:
            return
        preds = self.sc.predict_color_signals(number)
        for pred in preds:
            if pred["prob"] < MIN_PROB_COLOR_ZONE: continue
            pid = pred["pid"]; bet = pred["bet"]; seq = pred.get("sequence", [])
            msg_id = tg_send(self._color_signal_text(bet, 1, seq), markup=immersive_keyboard())
            self.color_signals[pid] = {"bet": bet, "prob": pred["prob"], "sequence": seq,
                                       "msg_id": msg_id, "intento": 1}
            self.learner.register_signal(STRAT_COLOR, (0,0), 0, pred["prob"],
                                         pred.get("p1_trans",0), pred.get("p3_global",0) or 0,
                                         "neutral", number,
                                         self.dozen_seq[-5:] if self.dozen_seq else [])
            logger.info(f"[COLOR] 🔴⚫ Señal {pid}: {bet} ({pred['prob']:.0%})")

    def _zone_signal_text(self, bet, intento, sequence=None):
        apuesta_txt = {"Bajo": "BAJO 🟣", "Alto": "ALTO 🔵"}.get(bet, bet)
        pat = self._color_seq_str(sequence) if sequence else "—"
        last5 = self._fmt_last_zone_numbers(5)
        return (f"✅✅ <b>SEÑAL ZONA</b> ✅✅\n\n"
                f"⚪ Apuesta: {apuesta_txt}\n"
                f"🟡 Patrón: {pat}\n"
                f"🆔 Intento: {intento}/2\n"
                f"🕐 Últimos:\n{last5}")

    def _check_zone_signal(self, number):
        zone = get_zone(number)
        em = self._num_color_emoji(number)
        pids_done = []
        for pid, sig in list(self.zone_signals.items()):
            bet = sig["bet"]
            won = (bet == "Bajo" and zone == "B") or (bet == "Alto" and zone == "A")
            if won:
                op_txt = "1° OP" if sig["intento"] == 1 else "2° OP"
                tg_send(f"✅ WIN #{number} {em} — ☑️ GANADA EN {op_txt}")
                scoreboard.record_win()
                self._scoreboard_dirty = True
                self.learner.resolve("WIN", f"ZONA WIN #{number} bet={bet} pid={pid} intento {sig['intento']}")
                pids_done.append(pid)
            else:
                if sig["intento"] < self.MAX_INTENTOS_ZONA:
                    if sig.get("msg_id"):
                        tg_delete(CHAT_ID, sig["msg_id"])
                    sig["intento"] += 1
                    msg_id = tg_send(self._zone_signal_text(bet, sig["intento"], sig.get("sequence", [])),
                                     markup=immersive_keyboard())
                    if msg_id: sig["msg_id"] = msg_id
                    logger.info(f"[ZONA] 🔁 {pid} intento 1 fallido → intento 2")
                else:
                    tg_send(f"❌ LOSS #{number} {em} — PERDIDA EN 2° OP")
                    scoreboard.record_loss()
                    self._scoreboard_dirty = True
                    self.learner.resolve("LOSS", f"ZONA LOSS #{number} bet={bet} pid={pid} intento {sig['intento']}")
                    pids_done.append(pid)
        for pid in pids_done:
            self.zone_signals.pop(pid, None)
        if self.zone_signals:
            return
        preds = self.sc.predict_zone_signals(number)
        for pred in preds:
            if pred["prob"] < MIN_PROB_COLOR_ZONE: continue
            pid = pred["pid"]; bet = pred["bet"]; seq = pred.get("sequence", [])
            msg_id = tg_send(self._zone_signal_text(bet, 1, seq), markup=immersive_keyboard())
            self.zone_signals[pid] = {"bet": bet, "prob": pred["prob"], "sequence": seq,
                                      "msg_id": msg_id, "intento": 1}
            self.learner.register_signal(STRAT_ZONE, (0,0), 0, pred["prob"],
                                         pred.get("p1_trans",0), pred.get("p3_global",0) or 0,
                                         "neutral", number,
                                         self.dozen_seq[-5:] if self.dozen_seq else [])
            logger.info(f"[ZONA] 🟣🔵 Señal {pid}: {bet} ({pred['prob']:.0%})")

    # ─── Triples pendientes ────────────────────────────────────────────────────
    def _resolve_pending_triples(self, number):
        d = get_dozen(number)
        col = get_column(number)
        if self._pending_triple_dozen_id and self._pending_triple_dozen_pair:
            pair = self._pending_triple_dozen_pair
            win = 1 if d in pair else 0
            self._db.execute("UPDATE triple_history SET next_dozen=?, win=? WHERE id=?",
                             (d, win, self._pending_triple_dozen_id))
            self._db.commit()
            self._pending_triple_dozen_id = None
            self._pending_triple_dozen_pair = None
        if self._pending_triple_column_id and self._pending_triple_column_pair:
            pair = self._pending_triple_column_pair
            win = 1 if col in pair else 0
            self._db.execute("UPDATE triple_history SET next_column=?, win=? WHERE id=?",
                             (col, win, self._pending_triple_column_id))
            self._db.commit()
            self._pending_triple_column_id = None
            self._pending_triple_column_pair = None

    def _check_new_triples(self, number):
        non_zero = [s["number"] for s in self.spin_history if s["number"] != 0]
        if len(non_zero) < 3: return
        # Triple docenas
        last3_d = [get_dozen(n) for n in non_zero[-3:]]
        if len(set(last3_d)) == 1:
            triple_str = ",".join(str(x) for x in last3_d)
            numbers_str = ",".join(str(n) for n in non_zero[-3:])
            pair = tuple(sorted({1,2,3} - {last3_d[0]}))
            cur = self._db.execute(
                "INSERT INTO triple_history (ts, type, triple, numbers, pair) VALUES (?,?,?,?,?)",
                (time.time(), "dozen", triple_str, numbers_str, f"{pair[0]},{pair[1]}")
            )
            self._db.commit()
            self._pending_triple_dozen_id = cur.lastrowid
            self._pending_triple_dozen_pair = pair
            logger.info(f"[TRIPLE] Dozen: {triple_str} nums: {numbers_str}")
        # Triple columna
        last3_c = [get_column(n) for n in non_zero[-3:]]
        if len(set(last3_c)) == 1:
            triple_str = ",".join(str(x) for x in last3_c)
            numbers_str = ",".join(str(n) for n in non_zero[-3:])
            pair = tuple(sorted({1,2,3} - {last3_c[0]}))
            cur = self._db.execute(
                "INSERT INTO triple_history (ts, type, triple, numbers, pair) VALUES (?,?,?,?,?)",
                (time.time(), "column", triple_str, numbers_str, f"{pair[0]},{pair[1]}")
            )
            self._db.commit()
            self._pending_triple_column_id = cur.lastrowid
            self._pending_triple_column_pair = pair   # <-- CORREGIDO
            logger.info(f"[TRIPLE] Column: {triple_str} nums: {numbers_str}")

    # ─── Process batch ─────────────────────────────────────────────────────────
    def process_batch(self, batch):
        new_spins = []
        seen_in_batch = set()
        for spin in reversed(batch):
            gid = spin.get("game_id")
            if not gid or gid in self.processed_game_ids or gid in seen_in_batch:
                continue
            seen_in_batch.add(gid)
            new_spins.append(spin)
        if not new_spins: return
        for spin in new_spins:
            gid = spin["game_id"]; number = spin["number"]
            try:
                inserted = self._db.execute(
                    "INSERT OR IGNORE INTO processed_spins (game_id, ts) VALUES (?, ?)",
                    (gid, int(time.time()))
                ).rowcount
                self._db.commit()
                if inserted == 0:
                    logger.info(f"[ImmersiveDC] 🔒 gid={gid} ya procesado — saltando")
                    continue
            except Exception as e:
                logger.warning(f"[ImmersiveDC] ⚠️ Error persistiendo gid {gid}: {e}")
            self.processed_game_ids[gid] = True
            if 0 <= number <= 36:
                try: self._process_inner(number)
                except Exception as e:
                    logger.error(f"Error procesando spin: {e}", exc_info=True)
                    self._reset_signal()
        if len(self.processed_game_ids) > self.MAX_PROCESSED_IDS:
            keys_old = list(self.processed_game_ids.keys())[:150]
            for k in keys_old:
                self.processed_game_ids.pop(k, None)

    def _process_inner(self, number):
        d = get_dozen(number)
        col = get_column(number)
        logger.info(f"[ImmersiveDC] 🎰 #{len(self.spin_history)+1}: {number} D{d} C{col} {get_color(number)}/{get_zone(number)}")
        self._resolve_pending_triples(number)
        self._update_state(number)

        if not self.warmup_done:
            self.ws_count += 1
            if self.ws_count < WARMUP_SPINS: return
            self.warmup_done = True
            tg_send("🟢 <b>Immersive Roulette DC v36</b> — Sistema listo.\n🎡 Señales: Docenas/Columnas (E1/E2/E3), Color, Zona")

        self._check_color_signal(number)
        self._check_zone_signal(number)

        if self.signal_active:
            self._resolve_signal(number)
        else:
            best = self._select_best_signal()
            if best:
                self._activate_signal(best)

        self._check_new_triples(number)

        if self._scoreboard_dirty:
            scoreboard.send()
            self._scoreboard_dirty = False

    # ─── Helpers de formato ────────────────────────────────────────────────────
    @staticmethod
    def _num_color_emoji(n):
        c = get_color(n)
        return {"R": "🔴", "N": "⚫", "V": "🟢"}.get(c, "⚪")

    def _fmt_last_numbers(self, count=5):
        hist = list(self.spin_history)[-count:][::-1]
        return " ".join(f"{self._num_color_emoji(s['number'])}{s['number']}" for s in hist)

    def _fmt_last_zone_numbers(self, count=5):
        hist = list(self.spin_history)[-count:][::-1]
        parts = []
        for s in hist:
            z = get_zone(s["number"])
            em = {"B": "🟣", "A": "🔵", "Z": "🟢"}.get(z, "⚪")
            parts.append(f"{em}{s['number']}")
        return " ".join(parts)

    @staticmethod
    def _color_seq_str(sequence):
        mapping = {"Negro": "N", "Rojo": "R", "N": "N", "R": "R",
                   "Bajo": "B", "Alto": "A", "B": "B", "A": "A"}
        return "-".join(mapping.get(str(v), str(v)) for v in sequence)

    async def poll_loop(self):
        url = f"{STATS_URL}/latest/{TARGET_ROULETTE}"
        logger.info(f"[ImmersiveDC] 🔄 Polling cada {POLL_INTERVAL}s → {url}")
        async with aiohttp.ClientSession() as session:
            while True:
                try:
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=6)) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            self.sc.update(data)
                            last_20 = data.get("last_20", [])
                            if isinstance(last_20, list) and last_20 and isinstance(last_20[0], dict):
                                if not self._first_poll_done:
                                    for spin in last_20:
                                        gid = spin.get("game_id")
                                        if gid:
                                            self.processed_game_ids[gid] = True
                                    self._first_poll_done = True
                                    logger.info(f"[ImmersiveDC] 🔒 Primera poll: {len(last_20)} giros marcados")
                                else:
                                    self.process_batch(last_20)
                        else:
                            self.sc.connected = False
                except Exception as e:
                    self.sc.connected = False
                    logger.debug(f"Poll error: {e}")
                await asyncio.sleep(POLL_INTERVAL)

# ─── FLASK ────────────────────────────────────────────────────────────────────
flask_app = Flask(__name__)
engine: Optional[ImmersiveRouletteEngine] = None

@flask_app.route("/")
def home():
    return jsonify({"status": "ok", "bot": "Immersive Roulette DC v36"})

@flask_app.route("/ping")
def ping():
    return jsonify({"status": "pong", "ts": time.time()})

@flask_app.route("/health")
def health():
    if not engine: return jsonify({"status": "not_ready"}), 503
    art_now = datetime.now(ART).strftime("%Y-%m-%d %H:%M ART")
    recent = engine.learner._recent()
    wins_r = sum(1 for s in recent if s["result"] == "WIN")
    return jsonify({
        "warmup": engine.warmup_done,
        "spins": len(engine.spin_history),
        "stats_connected": engine.sc.connected,
        "polls": engine.sc.poll_count,
        "signal_active": engine.signal_active,
        "color_signals": len(engine.color_signals),
        "zone_signals": len(engine.zone_signals),
        "scoreboard": scoreboard.get_text().replace("<b>","").replace("</b>",""),
        "art_time": art_now,
        "learner_signals": len(engine.learner.history),
        "learner_wr": f"{wins_r/len(recent)*100:.1f}%" if recent else "—",
    })

# ─── COMANDOS TELEGRAM ────────────────────────────────────────────────────────
@bot.message_handler(commands=["detenersenal"])
def cmd_detener(m):
    global signal_sending_enabled
    signal_sending_enabled = False
    bot.reply_to(m, "🔕 Señales <b>DETENIDAS</b>.", parse_mode="HTML")

@bot.message_handler(commands=["encendersenal"])
def cmd_encender(m):
    global signal_sending_enabled
    signal_sending_enabled = True
    bot.reply_to(m, "🔔 Señales <b>ACTIVADAS</b>.", parse_mode="HTML")

@bot.message_handler(commands=["help"])
def cmd_help(m):
    bot.reply_to(m,
        "<b>🎡 Immersive Roulette DC v36</b>\n\n"
        "Señales sin gestión de apuesta\n"
        "🅐 E1: PF+PHF+ML · 🅑 E2: triple histórico · 🅒 E3: Retorno\n"
        "🎨 Color (P1+P3) · 🗺 Zona (P1+P3)\n"
        "Columna también con E1/E2/E3\n\n"
        "Marcador diario → reset 00:00 ART\n\n"
        "/detenersenal /encendersenal /resetearmarcador /status /marcador /aprendizaje /debug",
        parse_mode="HTML")

@bot.message_handler(commands=["resetearmarcador"])
def cmd_reset_marcador(m):
    scoreboard.wins = 0
    scoreboard.losses = 0
    scoreboard._current_day = scoreboard._art_day()
    bot.reply_to(m, "🔄 <b>Marcador diario reseteado.</b>", parse_mode="HTML")

@bot.message_handler(commands=["marcador", "score"])
def cmd_marcador(m):
    bot.reply_to(m, scoreboard.get_text(), parse_mode="HTML")

@bot.message_handler(commands=["status"])
def cmd_status(m):
    if not engine: bot.reply_to(m, "❌ Engine no inicializado"); return
    d_st = "⚪ Idle"
    if engine.signal_active:
        t = engine.active_type
        p = engine.active_pair
        if t == "dozen":
            d_st = f"🟢 Docena D{p[0]}+D{p[1]}"
        else:
            d_st = f"🟢 Columna C{p[0]}+C{p[1]}"
    conn = "🟢 OK" if engine.sc.connected else "🔴 Desc."
    ago = time.time() - engine.sc.last_poll_ok if engine.sc.last_poll_ok > 0 else 0
    art_now = datetime.now(ART).strftime("%H:%M ART")
    bot.reply_to(m,
        f"<b>🎡 Immersive Roulette DC v36</b>\n"
        f"<b>Señal:</b> {d_st}\n"
        f"<b>Color:</b> {'🟡' if engine.color_signals else '⚪'}\n"
        f"<b>Zona:</b> {'🟡' if engine.zone_signals else '⚪'}\n"
        f"<b>Giros:</b> {len(engine.spin_history)}\n"
        f"<b>Servidor:</b> {conn} ({ago:.0f}s)\n"
        f"<b>Hora:</b> {art_now}\n\n{scoreboard.get_text()}",
        parse_mode="HTML")

@bot.message_handler(commands=["aprendizaje"])
def cmd_aprendizaje(m):
    if not engine: bot.reply_to(m, "❌ Engine no inicializado"); return
    bot.reply_to(m, engine.learner.get_summary(30), parse_mode="HTML")

@bot.message_handler(commands=["debug"])
def cmd_debug(m):
    if not engine or not engine.warmup_done:
        bot.reply_to(m, "⏳ Calentando..."); return
    last_num = engine.spin_history[-1]["number"] if engine.spin_history else None
    trend_d = ema_trend_str(engine.doc_levels)
    trend_c = ema_trend_str(engine.col_levels)
    def st(s): return f"✅ {s['pair']} ({s['prob']:.0%})" if s else "—"
    e1d = engine._detect_e1_dozen(); e2d = engine._detect_e2_dozen(); e3d = engine._detect_e3_dozen()
    e1c = engine._detect_e1_column(); e2c = engine._detect_e2_column(); e3c = engine._detect_e3_column()
    lines = [f"<b>🔬 Debug #{last_num} | EMA Doc:{trend_d.upper()} Col:{trend_c.upper()}</b>\n",
             f"🅐 E1 Doc: {st(e1d)}   🅑 E2 Doc: {st(e2d)}   🅒 E3 Doc: {st(e3d)}",
             f"🅐 E1 Col: {st(e1c)}   🅑 E2 Col: {st(e2c)}   🅒 E3 Col: {st(e3c)}"]
    bot.reply_to(m, "\n".join(lines), parse_mode="HTML")

@bot.message_handler(commands=["reset"])
def cmd_reset(m):
    if engine:
        engine.processed_game_ids.clear()
        engine._reset_signal()
        engine.color_signals.clear()
        engine.zone_signals.clear()
    bot.reply_to(m, "🔄 <b>Señales reseteadas</b>", parse_mode="HTML")

@bot.message_handler(commands=["reset_learning"])
def cmd_reset_learning(m):
    if not engine: return
    try:
        engine._db.execute("DELETE FROM signal_log"); engine._db.commit()
        engine.learner.history.clear(); engine.learner.pending_id = None
        bot.reply_to(m, "🗑️ <b>Historial de aprendizaje borrado.</b>", parse_mode="HTML")
    except Exception as e:
        bot.reply_to(m, f"❌ Error: {e}")

def setup_commands():
    commands = [
        telebot.types.BotCommand("detenersenal", "Detener envío de señales"),
        telebot.types.BotCommand("encendersenal", "Activar envío de señales"),
        telebot.types.BotCommand("resetearmarcador", "Resetear marcador diario"),
    ]
    try:
        bot.set_my_commands(commands)
        logger.info("✅ Comandos de menú configurados")
    except Exception as e:
        logger.error(f"❌ Error configurando comandos: {e}")

# ─── SELF PING ────────────────────────────────────────────────────────────────
async def self_ping_loop():
    url = os.environ.get("RENDER_EXTERNAL_URL", "").rstrip("/")
    if not url or "localhost" in url: return
    await asyncio.sleep(30)
    while True:
        try:
            import urllib.request as _ur
            _ur.urlopen(f"{url}/ping", timeout=15)
        except: pass
        await asyncio.sleep(240)

def run_flask():
    flask_app.run(host="0.0.0.0", port=10005, debug=False, use_reloader=False)

# ─── WEBHOOK ──────────────────────────────────────────────────────────────────
@flask_app.route("/tgwebhook", methods=["POST"])
def tg_webhook():
    try:
        data = request.get_json(force=True)
        update_id = data.get("update_id")
        if update_id and update_id in PROCESSED_UPDATE_IDS:
            logger.info(f"🔄 Update {update_id} ya procesado – ignorado")
            return "", 200
        if update_id:
            PROCESSED_UPDATE_IDS.add(update_id)
            if len(PROCESSED_UPDATE_IDS) > MAX_UPDATE_IDS:
                PROCESSED_UPDATE_IDS.clear()
        json_string = json.dumps(data)
        update = telebot.types.Update.de_json(json_string)
        bot.process_new_updates([update])
    except Exception as e:
        logger.error(f"❌ Error webhook: {e}")
    return "", 200

def setup_webhook():
    render_url = os.environ.get("RENDER_EXTERNAL_URL", "").rstrip("/")
    if not render_url:
        logger.warning("⚠️ RENDER_EXTERNAL_URL no definida")
        return
    webhook_url = f"{render_url}/tgwebhook"
    for attempt in range(3):
        try:
            bot.remove_webhook()
            time.sleep(1)
            bot.set_webhook(url=webhook_url, drop_pending_updates=True)
            logger.info(f"✅ Webhook registrado: {webhook_url}")
            return
        except Exception as e:
            logger.warning(f"⚠️ intento {attempt+1}: {e}")
            time.sleep(3)
    logger.error("❌ No se pudo registrar el webhook")

# ─── MAIN ─────────────────────────────────────────────────────────────────────
async def main():
    global engine
    sc = StatsClient()
    engine = ImmersiveRouletteEngine(sc)
    setup_webhook()
    setup_commands()
    logger.info("[ImmersiveDC] 🎡 v36.1 iniciada")
    await asyncio.gather(
        asyncio.create_task(engine.poll_loop()),
        asyncio.create_task(self_ping_loop()),
    )

if __name__ == "__main__":
    threading.Thread(target=run_flask, daemon=True).start()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot detenido.")
        try: bot.remove_webhook()
        except: pass
