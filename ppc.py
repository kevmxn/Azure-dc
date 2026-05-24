
#!/usr/bin/env python3
"""
Immersive Roulette Bot — DC v33
===========================================================================
  CAMBIOS vs v32:
  - Color/Zona: P2 eliminado. Se usa P1 (transición) + P3 (global).
    P1 = prob. estadística desde stats_color/stats_zone por último número.
    P3 = efectividad global del patrón (historial servidor).
    Combinación: P1*P1_W_COLOR + P3*P3_W_COLOR (configurable).
  - Docenas: análisis del historial de señales por último número.
    Cuando el servidor tiene ≥ DOZEN_HIST_MIN muestras para last_number,
    se aplica un ajuste proporcional al win rate histórico.
  - Docenas: al activar señal E1/E2/E3 → POST al servidor para tracking
    de aciertos/fallos (auto-resuelto en el siguiente spin).
  - Señales sin gestión de apuesta (solo indicación)
  - Marcador diario (reset 00:00 ART)
  - Aprendizaje adaptativo (SignalLearner)
"""

import asyncio
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
from flask import Flask, jsonify
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
CHAT_ID = -1003610988961

# ─── URL RULETA ───────────────────────────────────────────────────────────────
IMMERSIVE_URL = "https://www.casino.org/live-casino/roulette/immersive-roulette/"

def immersive_keyboard() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("👆🏻 ACCEDER a IMMERSIVE ROULETTE", url=IMMERSIVE_URL))
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
STATS_URL       = "https://ruletasbot-rjce.onrender.com"
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

# ── Pesos para predicción Color/Zona (P1 + P3, sin P2) ──────────────────────
# P1 = probabilidad de transición estadística (stats_color/stats_zone por número)
# P3 = efectividad global del patrón en el historial del servidor
P1_W_COLOR = 0.35    # Peso de P1 (transición desde last_number)
P3_W_COLOR = 0.65    # Peso de P3 (efectividad global del patrón)
TRANS_MIN_SAMPLES = 15  # Mínimo de muestras en stats_color/zone para usar P1

# ── Ajuste por historial de señales de docenas por número ───────────────────
DOZEN_HIST_MIN   = 5     # Mínimo de señales resueltas para usar el ajuste
DOZEN_HIST_SCALE = 0.12  # Escala del ajuste (ej: wr=70% → adj=+0.024)

# Argentina UTC-3
ART = timezone(timedelta(hours=-3))

STRAT_E1    = 1
STRAT_E2    = 2
STRAT_E3    = 3
STRAT_COLOR = 4
STRAT_ZONE  = 5
STRAT_COL_SEQ = 6   # Columna secuencia (2 columnas en últimos 5)

# Mínima prob para activar señal de columna por patrón de secuencia
COL_SEQ_MIN_PROB = 0.78

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

def color_label(c: str) -> str:
    return {"R": "🔴 Rojo", "N": "⚫ Negro", "V": "🟢 Verde"}.get(c, c)

def zone_label(z: str) -> str:
    return {"B": "🔵 Bajo (1-18)", "A": "🔴 Alto (19-36)", "Z": "🟢 Zero"}.get(z, z)

# ─── TABLA PHTML ──────────────────────────────────────────────────────────────
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

def tg_send(text: str, markup: InlineKeyboardMarkup = None) -> Optional[int]:
    if not text: return None
    try:
        msg = _tg_call(bot.send_message, chat_id=CHAT_ID, text=text,
                       parse_mode="HTML", reply_markup=markup)
        if msg:
            logger.info(f"✅ Mensaje enviado (ID: {msg.message_id})")
            return msg.message_id
        return None
    except Exception as e:
        logger.error(f"❌ Error en tg_send: {e}")
        return None

def tg_delete(chat_id: int, message_id: int):
    try:
        _tg_call(bot.delete_message, chat_id=chat_id, message_id=message_id)
    except Exception as e:
        logger.warning(f"⚠️ Error borrando mensaje: {e}")

# ─── MARCADOR DIARIO ──────────────────────────────────────────────────────────
class DailyScoreboard:
    """
    Registra aciertos y fallos de TODAS las categorías combinadas.
    Se resetea automáticamente a las 00:00 horario Argentina (UTC-3).
    """
    def __init__(self):
        self.wins:   int = 0
        self.losses: int = 0
        self._current_day: int = self._art_day()

    @staticmethod
    def _art_day() -> int:
        """Día actual en hora Argentina."""
        return datetime.now(ART).day

    def _check_reset(self):
        today = self._art_day()
        if today != self._current_day:
            logger.info(
                f"[Scoreboard] 🔄 Nuevo día Argentina → reset "
                f"(anterior: {self.wins}W/{self.losses}L)"
            )
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
            f"📊 <b>MARCADOR DIARIO:</b>\n"
            f"✅ GANADAS: {self.wins}\n"
            f"❌ PERDIDAS: {self.losses}\n"
            f"📈 ACIERTOS = {pct:.2f}%"
        )

    def send(self):
        tg_send(self.get_text())


scoreboard = DailyScoreboard()

# ─── STATS CLIENT ─────────────────────────────────────────────────────────────
class StatsClient:
    def __init__(self):
        self.stats_dozen    = {}
        self.stats_column   = {}
        self.stats_color    = {}
        self.stats_zone     = {}
        self.color_patterns = {}
        self.zone_patterns  = {}
        self.dozen_signals  = {}   # pending, summary, by_number
        self.column_signals = {}   # pending, summary, by_number
        # Patrones de secuencia 2D / 2C (nuevo)
        self.dozen_seq_patterns  = {}  # pending, history, by_number
        self.column_seq_patterns = {}  # pending, history, by_number
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

    # ── Historial de señales de docenas por número ────────────────────────────
    def get_dozen_signal_winrate(self, last_number: int) -> Optional[float]:
        """Win rate histórico de señales de docena cuando el último número fue last_number."""
        by_num = self.dozen_signals.get("by_number", {})
        stats  = by_num.get(str(last_number), {})
        total  = stats.get("total", 0)
        if total < DOZEN_HIST_MIN:
            return None
        return stats.get("aciertos", 0) / total

    def get_column_signal_winrate(self, last_number: int) -> Optional[float]:
        """Win rate histórico de señales de columna cuando el último número fue last_number."""
        by_num = self.column_signals.get("by_number", {})
        stats  = by_num.get(str(last_number), {})
        total  = stats.get("total", 0)
        if total < DOZEN_HIST_MIN:
            return None
        return stats.get("aciertos", 0) / total

    # ── Consultas a patrones de secuencia (servidor) ──────────────────────────

    def get_dozen_seq_top_pair(self, last_number: int) -> Optional[dict]:
        """Par de docenas más frecuente en patrones de secuencia cuando last_number fue X.

        Devuelve:
            pair            : [d1, d2]
            efectividad     : win rate global del número (%)
            top_efectividad : win rate del par más frecuente (%)
            total           : total de muestras
        Devuelve None si hay menos de DOZEN_HIST_MIN muestras.
        """
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
        """Par de columnas más frecuente en patrones de secuencia cuando last_number fue X."""
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
        """Registra la señal de columna activa en el servidor (tracking aciertos/fallos)."""
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
        """Registra la señal de docena activa en el servidor (tracking aciertos/fallos)."""
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

    # ── Predicción Color: P1 (transición) + P3 (global) ──────────────────────
    def predict_color_signal(self, last_number: int) -> Optional[Dict]:
        pending = self.color_patterns.get("pending")
        if not pending: return None
        pid      = pending.get("pid", "")
        bet      = pending.get("bet", "")
        sequence = pending.get("sequence", [])

        # P3: efectividad global del patrón
        p3 = self._global_eff(self.color_patterns.get("summary", {}), pid)
        if p3 is None:
            logger.info(f"[COLOR] {pid} sin datos globales suficientes — señal omitida")
            return None

        # P1: probabilidad de transición estadística desde last_number
        p1 = None
        if last_number is not None and last_number != 0:
            color_trans = self.stats_color.get(str(last_number), {})
            if color_trans.get("total", 0) >= TRANS_MIN_SAMPLES:
                if bet == "Negro":
                    p1 = color_trans.get("N", 0) / 100.0
                elif bet == "Rojo":
                    p1 = color_trans.get("R", 0) / 100.0

        # Combinar P1 + P3 (sin P2)
        if p1 is not None:
            prob       = round(P1_W_COLOR * p1 + P3_W_COLOR * p3, 4)
            components = f"Trans={p1:.0%} | Global={p3:.0%}"
            logger.info(
                f"[COLOR] {pid} bet={bet} | P1(trans)={p1:.0%} "
                f"P3(global)={p3:.0%} → prob={prob:.0%}"
            )
        else:
            prob       = p3
            components = f"Global={p3:.0%}"
            logger.info(f"[COLOR] {pid} bet={bet} | P3(global)={p3:.0%} (sin datos P1)")

        return {
            "type":       "color",
            "pid":        pid,
            "bet":        bet,
            "prob":       prob,
            "p1_trans":   p1 or 0,
            "p3_global":  p3,
            "sequence":   sequence,
            "components": components,
        }

    # ── Predicción Zona: P1 (transición) + P3 (global) ───────────────────────
    def predict_zone_signal(self, last_number: int) -> Optional[Dict]:
        pending = self.zone_patterns.get("pending")
        if not pending: return None
        pid      = pending.get("pid", "")
        bet      = pending.get("bet", "")
        sequence = pending.get("sequence", [])

        # P3: efectividad global del patrón
        p3 = self._global_eff(self.zone_patterns.get("summary", {}), pid)
        if p3 is None:
            logger.info(f"[ZONA] {pid} sin datos globales suficientes — señal omitida")
            return None

        # P1: probabilidad de transición estadística desde last_number
        p1 = None
        if last_number is not None and last_number != 0:
            zone_trans = self.stats_zone.get(str(last_number), {})
            if zone_trans.get("total", 0) >= TRANS_MIN_SAMPLES:
                if bet == "Bajo":
                    p1 = zone_trans.get("B", 0) / 100.0
                elif bet == "Alto":
                    p1 = zone_trans.get("A", 0) / 100.0

        # Combinar P1 + P3 (sin P2)
        if p1 is not None:
            prob       = round(P1_W_COLOR * p1 + P3_W_COLOR * p3, 4)
            components = f"Trans={p1:.0%} | Global={p3:.0%}"
            logger.info(
                f"[ZONA] {pid} bet={bet} | P1(trans)={p1:.0%} "
                f"P3(global)={p3:.0%} → prob={prob:.0%}"
            )
        else:
            prob       = p3
            components = f"Global={p3:.0%}"
            logger.info(f"[ZONA] {pid} bet={bet} | P3(global)={p3:.0%} (sin datos P1)")

        return {
            "type":       "zone",
            "pid":        pid,
            "bet":        bet,
            "prob":       prob,
            "p1_trans":   p1 or 0,
            "p3_global":  p3,
            "sequence":   sequence,
            "components": components,
        }

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

def ema_trend_pair(trend: str) -> Dict:
    if trend=="bull": return {"pair":(1,2),"missing":3,"label":"ALCISTA"}
    if trend=="bear": return {"pair":(2,3),"missing":1,"label":"BAJISTA"}
    return {"pair":(1,3),"missing":2,"label":"NEUTRAL"}

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

    def _extract_features(self, hist_d, pf_pd, ph_pd):
        if len(hist_d)<self.WINDOW: return None
        features=[]
        for i in range(1,self.WINDOW+1):
            d=hist_d[-i]; vec=[0,0,0]; vec[d-1]=1; features.extend(vec)
        for pair in (pf_pd,ph_pd):
            vec=[0,0,0]
            for x in pair: vec[x-1]=1
            features.extend(vec)
        return features

    def partial_train(self, hist_d, target, pf_d, ph_d):
        feats=self._extract_features(hist_d[:-1],pf_d,ph_d)
        if feats is None: return
        X=np.array(feats).reshape(1,-1); y=np.array([target])
        if not self.trained:
            self.mnb.partial_fit(X,y,classes=self.CLASSES)
            self.sgd.partial_fit(X,y,classes=self.CLASSES)
            self.trained=True
        else:
            self.mnb.partial_fit(X,y); self.sgd.partial_fit(X,y)

    def predict(self, hist_d, pf_d, ph_d):
        if not self.trained: return None
        feats=self._extract_features(hist_d,pf_d,ph_d)
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
        self.sc=sc

        # Historial
        self.spin_history:  List[dict]=[]
        self.dozen_seq:     List[int] =[]
        self.color_seq:     List[str] =[]
        self.zone_seq:      List[str] =[]
        self.d_levels:      Dict[int,List]={1:[],2:[],3:[]}
        self.doc_levels:    List[float]=[]
        self._last_doc_inc: float=0

        # After-number local
        self.after_number_dozen=defaultdict(lambda: defaultdict(int))

        # ML
        self.markov_d=SmoothedMarkovPredictor()
        self.ensemble_d=OnlineEnsemblePredictor()
        self.spins_since_train=0

        # Señal docenas
        self.signal_active      =False
        self.active_strategy    =None
        self.active_pair        =()
        self.active_missing     =0
        self.active_signal_msg_id=None

        # Señal color
        self.color_signal_active   =False
        self.color_signal_bet      =""
        self.color_signal_pid      =""
        self.color_signal_prob     =0.0
        self.color_signal_msg_id   =None
        self.color_signal_sequence =[]

        # Señal zona
        self.zone_signal_active    =False
        self.zone_signal_bet       =""
        self.zone_signal_pid       =""
        self.zone_signal_prob      =0.0
        self.zone_signal_msg_id    =None
        self.zone_signal_sequence  =[]

        # Señal columna (patrón de secuencia 2C en últimos 5)
        self.column_signal_active  =False
        self.column_signal_pair    =()
        self.column_signal_missing =0
        self.column_signal_prob    =0.0
        self.column_signal_msg_id  =None

        # DB y aprendizaje
        self._db    =_get_db()
        self.learner=SignalLearner(self._db)

        self.processed_game_ids:set=set()
        self.MAX_PROCESSED_IDS=300

        # Warmup
        live_loaded=self._load_live_history()
        self.ws_count=live_loaded
        self.warmup_done=live_loaded>=WARMUP_SPINS
        logger.info(
            f"[ImmersiveDC] 📦 Pre-cargados: {live_loaded} | "
            f"Warmup: {'✅' if self.warmup_done else '⏳'} | "
            f"Learner: {len(self.learner.history)} señales"
        )

    # ── DB local ──────────────────────────────────────────────────────────────
    def _load_live_history(self) -> int:
        try:
            rows=self._db.execute(
                "SELECT number,color,zone FROM live_spins ORDER BY id ASC"
            ).fetchall()
        except: return 0
        for (n,c,z) in rows: self._update_state(n,persist=False,train_model=False)
        if rows: self.markov_d.update(self.dozen_seq)
        return len(rows)

    def _persist(self, number,color,zone):
        try:
            self._db.execute("INSERT INTO live_spins(number,color,zone,ts) VALUES(?,?,?,?)",
                             (number,color,zone,int(time.time())))
            self._db.commit()
        except Exception as e: logger.debug(f"⚠️ DB persist: {e}")

    # ── Estado interno ────────────────────────────────────────────────────────
    def _update_state(self, number:int, persist=True, train_model=True):
        color=get_color(number); zone=get_zone(number); d=get_dozen(number)
        if number!=0 and self.spin_history:
            prev=self.spin_history[-1]["number"]
            if prev!=0: self.after_number_dozen[prev][d]+=1
        self.spin_history.append({"number":number,"color":color,"zone":zone})
        self.color_seq.append(color); self.zone_seq.append(zone)
        if len(self.color_seq)>200: self.color_seq.pop(0)
        if len(self.zone_seq)>200:  self.zone_seq.pop(0)
        if d!=0:
            for dd in (1,2,3):
                prev=self.d_levels[dd][-1] if self.d_levels[dd] else 0
                self.d_levels[dd].append(prev+(1 if d==dd else -1))
                if len(self.d_levels[dd])>300: self.d_levels[dd].pop(0)
            self.dozen_seq.append(d)
            if len(self.dozen_seq)>200: self.dozen_seq.pop(0)
            if train_model and len(self.dozen_seq)>5:
                pf_d=self._get_pf(); ph_d=self._get_ph(number)
                if pf_d and ph_d:
                    self.ensemble_d.partial_train(self.dozen_seq,d,pf_d["pair"],ph_d["pair"])
                self.spins_since_train+=1
                if self.spins_since_train>=TRAIN_INTERVAL:
                    self.markov_d.update(self.dozen_seq); self.spins_since_train=0
        inc=1 if d==1 else(-1 if d==3 else(1 if number!=0 and number<=18 else -1))
        if number==0: inc=self._last_doc_inc
        else: self._last_doc_inc=inc
        prev_lvl=self.doc_levels[-1] if self.doc_levels else 0
        self.doc_levels.append(prev_lvl+inc)
        if len(self.doc_levels)>300: self.doc_levels.pop(0)
        if persist: self._persist(number,color,zone)

    # ── PF / PH / PHTML / PHF ─────────────────────────────────────────────────
    def _get_pf(self):
        if len(self.spin_history)<5: return None
        counts={1:0,2:0,3:0}
        for s in self.spin_history[-5:]:
            n=s["number"]
            if n!=0: counts[get_dozen(n)]+=1
        active=[k for k,v in counts.items() if v>0]
        if len(active)!=2: return None
        pair=tuple(sorted(active)); missing=list({1,2,3}-set(pair))[0]
        return {"pair":pair,"missing":missing,"prob":sum(counts[a] for a in pair)/5.0}

    def _get_ph(self, number=None):
        if number is None:
            if not self.spin_history: return None
            number=self.spin_history[-1]["number"]
        if number==0: return None
        srv=self.sc.get_ph_pair(number)
        if srv: return srv
        counts=self.after_number_dozen.get(number,{}); total=sum(counts.values())
        if total<10: return None
        sc=sorted(counts.items(),key=lambda x:x[1],reverse=True)
        if len(sc)<2: return None
        pair=tuple(sorted([sc[0][0],sc[1][0]])); missing=list({1,2,3}-set(pair))[0]
        return {"pair":pair,"missing":missing,"prob":(sc[0][1]+sc[1][1])/total}

    def _get_phtml_probs(self, number):
        if number==0: return None
        entry=DOZEN_TABLE.get(number)
        if not entry: return None
        d1,d2,d3=entry["d1"],entry["d2"],entry["d3"]; total=d1+d2+d3
        if total==0: return None
        return {1:d1/total,2:d2/total,3:d3/total}

    def _get_phtml_pair(self, number):
        probs=self._get_phtml_probs(number)
        if probs is None: return None
        sd=sorted(probs.items(),key=lambda x:x[1],reverse=True)
        pair=tuple(sorted([sd[0][0],sd[1][0]])); missing=list({1,2,3}-set(pair))[0]
        return {"pair":pair,"missing":missing,"prob":probs[sd[0][0]]+probs[sd[1][0]]}

    def _get_phf(self, number):
        if number==0: return None
        phtml=self._get_phtml_probs(number)
        if phtml is None: return None
        ph=self.sc.get_ph_probs_raw(number)
        if ph is None:
            counts=self.after_number_dozen.get(number,{}); total=sum(counts.values())
            if total>=10: ph={1:counts.get(1,0)/total,2:counts.get(2,0)/total,3:counts.get(3,0)/total}
        phf_raw=({d:PHTML_W*phtml[d]+PH_W_COMBINE*ph[d] for d in [1,2,3]}
                 if ph is not None else dict(phtml))
        total=sum(phf_raw.values())
        if total==0: return None
        phf={d:v/total for d,v in phf_raw.items()}
        sd=sorted(phf.items(),key=lambda x:x[1],reverse=True)
        pair=tuple(sorted([sd[0][0],sd[1][0]])); missing=list({1,2,3}-set(pair))[0]
        return {"pair":pair,"missing":missing,"prob":phf[sd[0][0]]+phf[sd[1][0]],"probs":phf}

    # ── ML ────────────────────────────────────────────────────────────────────
    def _predict_pair_ml(self, missing_num):
        mk_pred=self.markov_d.predict(self.dozen_seq)
        m_p_miss=mk_pred.get(missing_num,1/3) if mk_pred else 1/3
        pf_d=self._get_pf(); ph_d=self._get_ph(); ens_p_miss=1/3
        if pf_d and ph_d:
            ens=self.ensemble_d.predict(self.dozen_seq,pf_d["pair"],ph_d["pair"])
            if ens: ens_p_miss=ens.get(missing_num,1/3)
        ml_miss=0.4*m_p_miss+0.6*ens_p_miss
        levels=self.d_levels.get(missing_num,[])
        if len(levels)>=20:
            if ema_signal(levels,"tendencia"): ml_miss*=0.85
            elif ema_signal(levels,"moderado"): ml_miss*=0.92
        return 1.0-ml_miss

    # ── E1/E2/E3 ──────────────────────────────────────────────────────────────
    def _detect_e1(self):
        if not self.warmup_done or not self.spin_history: return None
        last_num=self.spin_history[-1]["number"]
        if last_num==0: return None
        pf_d=self._get_pf()
        if not pf_d: return None
        phf_d=self._get_phf(last_num)
        if not phf_d: return None
        if set(pf_d["pair"])!=set(phf_d["pair"]): return None
        base=PF_W_NORM*pf_d["prob"]+PH_W_NORM*phf_d["prob"]
        ml=self._predict_pair_ml(pf_d["missing"])
        prob=BASE_W_NORM*base+ML_W_NORM*ml
        trend=ema_trend_str(self.doc_levels)
        adj,adj_d=self.learner.get_adjustment(STRAT_E1,pf_d["pair"],trend)
        prob_adj=round(max(0.0,min(1.0,prob+adj)),4)
        if prob_adj<MIN_PROB: return None
        return {"strategy":STRAT_E1,"pair":pf_d["pair"],"missing":pf_d["missing"],
                "prob":prob_adj,"label":"PF+PHF+ML","pf_prob":pf_d["prob"],
                "phf_prob":phf_d["prob"],"ema_trend":trend,"last_number":last_num}

    def _detect_e2(self, number=None):
        if not self.warmup_done: return None
        if number is None:
            if not self.spin_history: return None
            number=self.spin_history[-1]["number"]
        if number==0: return None
        phtml_pair=self._get_phtml_pair(number)
        if phtml_pair is None: return None
        trend=ema_trend_str(self.doc_levels); e_pair=ema_trend_pair(trend)
        if set(phtml_pair["pair"])!=set(e_pair["pair"]): return None
        prob=phtml_pair["prob"]
        adj,adj_d=self.learner.get_adjustment(STRAT_E2,phtml_pair["pair"],trend)
        prob_adj=round(max(0.0,min(1.0,prob+adj)),4)
        return {"strategy":STRAT_E2,"pair":phtml_pair["pair"],"missing":phtml_pair["missing"],
                "prob":prob_adj,"label":f"PHTML+EMA({e_pair['label']})","ema_trend":trend,
                "pf_prob":prob,"phf_prob":prob,"last_number":number}

    def _detect_e3(self):
        if not self.warmup_done: return None
        non_zero=[s["number"] for s in self.spin_history if s["number"]!=0]
        if len(non_zero)<6: return None
        prev5=non_zero[-6:-1]; last_n=non_zero[-1]
        cats5=list(set(get_dozen(n) for n in prev5))
        if len(cats5)!=2: return None
        pair=tuple(sorted(cats5)); last_dozen=get_dozen(last_n)
        if last_dozen in pair: return None
        streak=0
        for n in reversed(non_zero[:-1]):
            if get_dozen(n) in pair: streak+=1
            else: break
        phf_break=self._get_phf(last_n)
        if phf_break is None or set(phf_break["pair"])!=set(pair): return None
        return_prob=self._calc_return_prob(pair,streak,last_n)
        trend=ema_trend_str(self.doc_levels)
        adj,adj_d=self.learner.get_adjustment(STRAT_E3,pair,trend)
        return_prob_adj=round(max(0.0,min(1.0,return_prob+adj)),4)
        if return_prob_adj<MIN_PROB: return None
        return {"strategy":STRAT_E3,"pair":pair,"missing":last_dozen,
                "prob":return_prob_adj,"label":f"RETORNO(racha {streak}g)",
                "pf_prob":return_prob,"phf_prob":phf_break["prob"],
                "ema_trend":trend,"last_number":last_n}

    def _calc_return_prob(self, pair, streak, break_num):
        non_zero=[s["number"] for s in self.spin_history if s["number"]!=0]
        last20=non_zero[-20:]
        pair_count=sum(1 for n in last20 if get_dozen(n) in pair)
        base_prob=pair_count/len(last20) if last20 else 0.66
        streak_bst=min(0.40,streak*0.04); brk_d=get_dozen(break_num)
        brk_adj=0.02 if brk_d in pair else -0.04
        missing=list({1,2,3}-set(pair))[0]; levels=self.d_levels.get(missing,[]); ema_adj=0.0
        if len(levels)>=20:
            if ema_signal(levels,"tendencia"): ema_adj=-0.08
            elif ema_signal(levels,"moderado"): ema_adj=-0.04
        return round(max(0.35,min(0.97,base_prob+streak_bst+brk_adj+ema_adj)),4)

    def _select_best_signal(self):
        e1,e2,e3=self._detect_e1(),self._detect_e2(),self._detect_e3()
        candidates=[s for s in [e1,e2,e3] if s]
        if not candidates: return None
        if e1 and e2 and set(e1["pair"])==set(e2["pair"]):
            e1["prob"]=min(0.99,e1["prob"]*0.55+e2["prob"]*0.45)
            e1["label"]=f"E1+E2({e1['pair']})"

        # ── Ajuste por historial de señales de docena para el último número ──
        if self.spin_history:
            last_num = self.spin_history[-1]["number"]
            if last_num != 0:
                hist_wr = self.sc.get_dozen_signal_winrate(last_num)
                if hist_wr is not None:
                    hist_adj = round((hist_wr - 0.5) * DOZEN_HIST_SCALE * 2, 4)
                    for cand in candidates:
                        cand["prob"] = round(
                            max(0.0, min(1.0, cand["prob"] + hist_adj)), 4
                        )
                    logger.info(
                        f"[DOCENA] 📊 Ajuste historial señales #{last_num}: "
                        f"wr={hist_wr:.0%} adj={hist_adj:+.3f}"
                    )

                # ── Ajuste por patrón de secuencia histórico del servidor ───
                seq_stats = self.sc.get_dozen_seq_top_pair(last_num)
                if seq_stats and seq_stats.get("pair"):
                    top_pair = tuple(sorted(seq_stats["pair"]))
                    for cand in candidates:
                        if tuple(sorted(cand["pair"])) == top_pair:
                            eff     = seq_stats["top_efectividad"] / 100.0
                            seq_adj = round((eff - 0.5) * DOZEN_HIST_SCALE * 2, 4)
                            cand["prob"] = round(
                                max(0.0, min(1.0, cand["prob"] + seq_adj)), 4
                            )
                            logger.info(
                                f"[DOCENA] 📊 Ajuste seq-pat #{last_num}: "
                                f"top_par=D{top_pair} eff={seq_stats['top_efectividad']:.1f}% "
                                f"adj={seq_adj:+.3f}"
                            )

        _pri={STRAT_E1:3,STRAT_E3:2,STRAT_E2:1}
        candidates.sort(key=lambda x:(x["prob"],_pri.get(x["strategy"],0)),reverse=True)
        return candidates[0]

    # ── Columna: detección de patrón de secuencia (2C en últimos 5) ──────────

    def _get_col_pf(self) -> Optional[dict]:
        """Detecta si los últimos 5 no-cero tienen exactamente 2 columnas distintas.

        Retorna: pair, missing, prob, pattern_str, numbers
        """
        nz = [s for s in self.spin_history if s["number"] != 0]
        if len(nz) < 5:
            return None
        last5    = nz[-5:]
        cols     = [((s["number"] - 1) % 3) + 1 for s in last5]
        unique   = set(cols)
        if len(unique) != 2:
            return None
        pair    = tuple(sorted(unique))
        missing = list({1, 2, 3} - set(pair))[0]
        prob    = sum(1 for c in cols if c in pair) / 5.0
        pattern_str = ",".join(f"C{c}" for c in cols)
        numbers     = [s["number"] for s in last5]
        return {
            "pair":        pair,
            "missing":     missing,
            "prob":        prob,
            "pattern_str": pattern_str,
            "numbers":     numbers,
        }

    def _detect_col_signal(self, last_num: int) -> Optional[dict]:
        """Construye señal de columna combinando detección local + historial servidor.

        Fuentes:
          1. Patrón local: 2 columnas en últimos 5 no-cero (prob base = porción de las 5)
          2. seq_adj: ajuste por top_pair del servidor para este last_number
          3. col_adj: ajuste por win rate histórico de señales de columna para este número
        """
        if not self.warmup_done or last_num == 0:
            return None

        col_pf = self._get_col_pf()
        if not col_pf:
            return None

        pair        = col_pf["pair"]
        missing     = col_pf["missing"]
        base_prob   = col_pf["prob"]
        pattern_str = col_pf["pattern_str"]
        numbers     = col_pf["numbers"]

        # Ajuste por patrón de secuencia histórico (servidor)
        seq_stats = self.sc.get_column_seq_top_pair(last_num)
        seq_adj   = 0.0
        if seq_stats and seq_stats.get("pair"):
            if tuple(sorted(seq_stats["pair"])) == pair:
                eff     = seq_stats["top_efectividad"] / 100.0
                seq_adj = round((eff - 0.5) * DOZEN_HIST_SCALE * 2, 4)
                logger.info(
                    f"[COL-SEQ] Ajuste seq #{last_num}: "
                    f"top_par=C{seq_stats['pair']} eff={seq_stats['top_efectividad']:.1f}% "
                    f"adj={seq_adj:+.3f}"
                )

        # Ajuste por win rate histórico de señales de columna
        col_wr  = self.sc.get_column_signal_winrate(last_num)
        col_adj = 0.0
        if col_wr is not None:
            col_adj = round((col_wr - 0.5) * DOZEN_HIST_SCALE * 2, 4)
            logger.info(
                f"[COL-SEQ] Ajuste col_wr #{last_num}: wr={col_wr:.0%} adj={col_adj:+.3f}"
            )

        prob = round(max(0.0, min(1.0, base_prob + seq_adj + col_adj)), 4)
        logger.info(
            f"[COL-SEQ] #{last_num} pat=[{pattern_str}] nums={numbers} "
            f"base={base_prob:.0%} seq_adj={seq_adj:+.3f} col_adj={col_adj:+.3f} "
            f"→ prob={prob:.0%}"
        )
        return {
            "strategy":    STRAT_COL_SEQ,
            "pair":        pair,
            "missing":     missing,
            "prob":        prob,
            "label":       f"COL-SEQ[{pattern_str}]",
            "pattern_str": pattern_str,
            "numbers":     numbers,
            "seq_adj":     seq_adj,
            "col_adj":     col_adj,
            "last_number": last_num,
        }

    # ── Señales ───────────────────────────────────────────────────────────────
    def _strat_icon(self):
        return {STRAT_E1:"🅐",STRAT_E2:"🅑",STRAT_E3:"🅒"}.get(self.active_strategy,"?")

    def _activate_dozen_signal(self, sig):
        self.signal_active=True
        self.active_strategy=sig["strategy"]
        self.active_pair=sig["pair"]
        self.active_missing=sig["missing"]
        p=sig["pair"]
        icon=self._strat_icon()
        trend=sig.get("ema_trend",ema_trend_str(self.doc_levels))
        msg_id=tg_send(
            f"✅✅ SEÑAL CONFIRMADA ✅✅\n\n"
            f"🎡 IMMERSIVE ROULETTE {icon}\n"
            f"🎯 Apostar Docenas: <b>D{p[0]} y D{p[1]}</b>\n"
            f"📊 Probabilidad: {sig['prob']:.0%}\n"
            f"🔍 Estrategia: {sig['label']}",
            markup=immersive_keyboard()
        )
        if msg_id: self.active_signal_msg_id=msg_id
        self.learner.register_signal(
            strategy=sig["strategy"], pair=sig["pair"], missing=sig["missing"],
            prob=sig["prob"], pf_prob=sig.get("pf_prob",0.0),
            phf_prob=sig.get("phf_prob",0.0), ema_trend=trend,
            last_number=sig.get("last_number",0),
            dozen_seq_5=self.dozen_seq[-5:] if self.dozen_seq else []
        )
        # ── Registrar en servidor para tracking aciertos/fallos ───────────────
        self.sc.post_dozen_signal(
            strategy=sig["strategy"],
            pair=list(sig["pair"]),
            missing=sig["missing"],
            prob=sig["prob"],
            last_number=sig.get("last_number", 0),
        )
        logger.info(f"[ImmersiveDC] 🎯 SEÑAL DOCENA {sig['label']}: D{p} ({sig['prob']:.0%})")

    def _resolve_dozen_signal(self, number):
        d=get_dozen(number); won=(d!=0 and d in self.active_pair)
        icon=self._strat_icon()
        if won:
            tg_send(
                f"✅ WIN #{number} — DOCENA D{d}\n"
                f"🎡 IMMERSIVE ROULETTE {icon}\n"
                f"🎯 Apuesta: D{self.active_pair[0]} y D{self.active_pair[1]}",
                markup=immersive_keyboard()
            )
            scoreboard.record_win()
            self.learner.resolve("WIN",f"WIN D{d} | par correcto {self.active_pair}")
        else:
            tg_send(
                f"❌ LOSS #{number} — DOCENA D{d}\n"
                f"🎡 IMMERSIVE ROULETTE {icon}\n"
                f"🎯 Apuesta era: D{self.active_pair[0]} y D{self.active_pair[1]}",
                markup=immersive_keyboard()
            )
            scoreboard.record_loss()
            self.learner.resolve("LOSS",f"LOSS D{d} cayó | faltaba D{self.active_missing}")
        scoreboard.send()
        self._reset_dozen_signal()

    def _reset_dozen_signal(self):
        self.signal_active=False; self.active_strategy=None
        self.active_pair=(); self.active_missing=0; self.active_signal_msg_id=None

    # ── Color ─────────────────────────────────────────────────────────────────
    def _check_color_signal(self, number):
        if self.color_signal_active:
            color=get_color(number); bet=self.color_signal_bet
            won=(bet=="Negro" and color=="N") or (bet=="Rojo" and color=="R")
            if won:
                tg_send(
                    f"✅ WIN #{number} — {color_label(color)}\n"
                    f"🎡 IMMERSIVE ROULETTE 🎨\n"
                    f"🎯 Apuesta: {bet}",
                    markup=immersive_keyboard()
                )
                scoreboard.record_win()
                self.learner.resolve("WIN",f"COLOR WIN #{number} bet={bet} cayó {color}")
            else:
                tg_send(
                    f"❌ LOSS #{number} — {color_label(color)}\n"
                    f"🎡 IMMERSIVE ROULETTE 🎨\n"
                    f"🎯 Apuesta era: {bet}",
                    markup=immersive_keyboard()
                )
                scoreboard.record_loss()
                self.learner.resolve("LOSS",f"COLOR LOSS #{number} bet={bet} cayó {color}")
            scoreboard.send()
            self._reset_color_signal()
            return

        pred=self.sc.predict_color_signal(number)
        if pred and pred["prob"]>=MIN_PROB_COLOR_ZONE:
            bet=pred["bet"]; prob=pred["prob"]; seq=pred.get("sequence",[])
            self.color_signal_active=True; self.color_signal_bet=bet
            self.color_signal_pid=pred["pid"]; self.color_signal_prob=prob
            self.color_signal_sequence=seq
            msg_id=tg_send(
                f"🔔 SEÑAL COLOR — IMMERSIVE ROULETTE 🎨\n\n"
                f"🎯 Apostar: <b>{bet}</b>\n"
                f"📊 Probabilidad: {prob:.0%}\n"
                f"🔢 Secuencia: {seq}\n"
                f"📈 {pred.get('components','')}",
                markup=immersive_keyboard()
            )
            if msg_id: self.color_signal_msg_id=msg_id
            self.learner.register_signal(
                strategy=STRAT_COLOR,pair=(0,0),missing=0,prob=prob,
                pf_prob=pred.get("p1_trans",0),phf_prob=pred.get("p3_global",0) or 0,
                ema_trend="neutral",last_number=number,
                dozen_seq_5=self.dozen_seq[-5:] if self.dozen_seq else []
            )
            logger.info(f"[COLOR] 🎨 Señal: {bet} ({prob:.0%}) | seq={seq}")

    def _reset_color_signal(self):
        self.color_signal_active=False; self.color_signal_bet=""
        self.color_signal_pid=""; self.color_signal_prob=0.0
        self.color_signal_msg_id=None; self.color_signal_sequence=[]

    # ── Zona ──────────────────────────────────────────────────────────────────
    def _check_zone_signal(self, number):
        if self.zone_signal_active:
            zone=get_zone(number); bet=self.zone_signal_bet
            won=(bet=="Bajo" and zone=="B") or (bet=="Alto" and zone=="A")
            if won:
                tg_send(
                    f"✅ WIN #{number} — {zone_label(zone)}\n"
                    f"🎡 IMMERSIVE ROULETTE 🗺\n"
                    f"🎯 Apuesta: {bet}",
                    markup=immersive_keyboard()
                )
                scoreboard.record_win()
                self.learner.resolve("WIN",f"ZONA WIN #{number} bet={bet} cayó {zone}")
            else:
                tg_send(
                    f"❌ LOSS #{number} — {zone_label(zone)}\n"
                    f"🎡 IMMERSIVE ROULETTE 🗺\n"
                    f"🎯 Apuesta era: {bet}",
                    markup=immersive_keyboard()
                )
                scoreboard.record_loss()
                self.learner.resolve("LOSS",f"ZONA LOSS #{number} bet={bet} cayó {zone}")
            scoreboard.send()
            self._reset_zone_signal()
            return

        pred=self.sc.predict_zone_signal(number)
        if pred and pred["prob"]>=MIN_PROB_COLOR_ZONE:
            bet=pred["bet"]; prob=pred["prob"]; seq=pred.get("sequence",[])
            self.zone_signal_active=True; self.zone_signal_bet=bet
            self.zone_signal_pid=pred["pid"]; self.zone_signal_prob=prob
            self.zone_signal_sequence=seq
            msg_id=tg_send(
                f"🔔 SEÑAL ZONA — IMMERSIVE ROULETTE 🗺\n\n"
                f"🎯 Apostar: <b>{bet}</b>\n"
                f"📊 Probabilidad: {prob:.0%}\n"
                f"🔢 Secuencia: {seq}\n"
                f"📈 {pred.get('components','')}",
                markup=immersive_keyboard()
            )
            if msg_id: self.zone_signal_msg_id=msg_id
            self.learner.register_signal(
                strategy=STRAT_ZONE,pair=(0,0),missing=0,prob=prob,
                pf_prob=pred.get("p1_trans",0),phf_prob=pred.get("p3_global",0) or 0,
                ema_trend="neutral",last_number=number,
                dozen_seq_5=self.dozen_seq[-5:] if self.dozen_seq else []
            )
            logger.info(f"[ZONA] 🗺 Señal: {bet} ({prob:.0%}) | seq={seq}")

    def _reset_zone_signal(self):
        self.zone_signal_active=False; self.zone_signal_bet=""
        self.zone_signal_pid=""; self.zone_signal_prob=0.0
        self.zone_signal_msg_id=None; self.zone_signal_sequence=[]

    # ── Columna (patrón de secuencia 2C) ─────────────────────────────────────

    def _check_column_signal(self, number: int):
        """Verifica señal activa de columna o intenta activar una nueva."""
        if self.column_signal_active:
            col  = ((number - 1) % 3) + 1 if number != 0 else 0
            pair = self.column_signal_pair
            won  = (col != 0 and col in pair)
            if won:
                tg_send(
                    f"✅ WIN #{number} — C{col}\n"
                    f"🎡 IMMERSIVE ROULETTE 🏛\n"
                    f"🎯 Apuesta: C{pair[0]} y C{pair[1]}",
                    markup=immersive_keyboard()
                )
                scoreboard.record_win()
                self.learner.resolve("WIN", f"COL WIN #{number} C{col} | par={pair}")
            else:
                tg_send(
                    f"❌ LOSS #{number} — C{col}\n"
                    f"🎡 IMMERSIVE ROULETTE 🏛\n"
                    f"🎯 Apuesta era: C{pair[0]} y C{pair[1]}",
                    markup=immersive_keyboard()
                )
                scoreboard.record_loss()
                self.learner.resolve(
                    "LOSS",
                    f"COL LOSS #{number} C{col} | faltaba C{self.column_signal_missing}"
                )
            scoreboard.send()
            self._reset_column_signal()
            return

        # Intentar activar nueva señal
        last_num = self.spin_history[-1]["number"] if self.spin_history else 0
        sig = self._detect_col_signal(last_num)
        if sig and sig["prob"] >= COL_SEQ_MIN_PROB:
            self._activate_column_signal(sig)

    def _activate_column_signal(self, sig: dict):
        pair = sig["pair"]
        self.column_signal_active  = True
        self.column_signal_pair    = pair
        self.column_signal_missing = sig["missing"]
        self.column_signal_prob    = sig["prob"]
        nums = sig.get("numbers", [])
        pat  = sig.get("pattern_str", "")
        msg_id = tg_send(
            f"✅✅ SEÑAL COLUMNA ✅✅\n\n"
            f"🎡 IMMERSIVE ROULETTE 🏛\n"
            f"🎯 Apostar Columnas: <b>C{pair[0]} y C{pair[1]}</b>\n"
            f"📊 Probabilidad: {sig['prob']:.0%}\n"
            f"🔢 Secuencia: {nums}\n"
            f"🔍 Patrón: [{pat}]",
            markup=immersive_keyboard()
        )
        if msg_id:
            self.column_signal_msg_id = msg_id
        self.learner.register_signal(
            strategy=STRAT_COL_SEQ, pair=pair, missing=sig["missing"],
            prob=sig["prob"], pf_prob=sig["prob"], phf_prob=sig["prob"],
            ema_trend="neutral", last_number=sig.get("last_number", 0),
            dozen_seq_5=self.dozen_seq[-5:] if self.dozen_seq else []
        )
        # Registrar en servidor para tracking aciertos/fallos
        self.sc.post_column_signal(
            strategy=STRAT_COL_SEQ,
            pair=list(pair),
            missing=sig["missing"],
            prob=sig["prob"],
            last_number=sig.get("last_number", 0),
        )
        logger.info(
            f"[COL-SEQ] 🎯 SEÑAL COLUMNA: C{pair} ({sig['prob']:.0%}) | "
            f"pat=[{pat}] nums={nums}"
        )

    def _reset_column_signal(self):
        self.column_signal_active  = False
        self.column_signal_pair    = ()
        self.column_signal_missing = 0
        self.column_signal_prob    = 0.0
        self.column_signal_msg_id  = None

    # ── Loop principal ────────────────────────────────────────────────────────
    def process_batch(self, batch):
        new_spins=[]
        for spin in reversed(batch):
            gid=spin.get("game_id")
            if not gid or gid in self.processed_game_ids: continue
            new_spins.append(spin)
        if not new_spins: return
        for spin in new_spins:
            gid=spin["game_id"]; number=spin["number"]
            self.processed_game_ids.add(gid)
            if 0<=number<=36:
                try: self._process_inner(number)
                except Exception as e:
                    logger.error(f"Error procesando spin: {e}",exc_info=True)
                    self._reset_dozen_signal()
        if len(self.processed_game_ids)>self.MAX_PROCESSED_IDS:
            for gid in list(self.processed_game_ids)[:150]:
                self.processed_game_ids.discard(gid)

    def _process_inner(self, number:int):
        d=get_dozen(number)
        logger.info(f"[ImmersiveDC] 🎰 #{len(self.spin_history)+1}: "
                    f"{number} D{d} {get_color(number)}/{get_zone(number)}")
        self._update_state(number)

        if not self.warmup_done:
            self.ws_count+=1
            if self.ws_count<WARMUP_SPINS: return
            self.warmup_done=True
            tg_send(
                "🟢 <b>Immersive Roulette DC v33</b> — Sistema listo.\n"
                "🎡 Señales: 🅐🅑🅒 Docenas · 🎨 Color (P1+P3) · 🗺 Zona (P1+P3)\n"
                "🧠 Aprendizaje adaptativo + tracking aciertos/fallos activo"
            )

        # Color y Zona independientes de Docenas
        self._check_color_signal(number)
        self._check_zone_signal(number)

        # Columna (patrón de secuencia 2C en últimos 5)
        self._check_column_signal(number)

        # Docenas
        if self.signal_active:
            self._resolve_dozen_signal(number)
        else:
            sig=self._select_best_signal()
            if sig: self._activate_dozen_signal(sig)

    async def poll_loop(self):
        url=f"{STATS_URL}/latest/{TARGET_ROULETTE}"
        logger.info(f"[ImmersiveDC] 🔄 Polling cada {POLL_INTERVAL}s → {url}")
        async with aiohttp.ClientSession() as session:
            while True:
                try:
                    async with session.get(url,timeout=aiohttp.ClientTimeout(total=6)) as resp:
                        if resp.status==200:
                            data=await resp.json()
                            self.sc.update(data)
                            last_20=data.get("last_20",[])
                            if isinstance(last_20,list) and last_20 and isinstance(last_20[0],dict):
                                self.process_batch(last_20)
                        else:
                            self.sc.connected=False
                except Exception as e:
                    self.sc.connected=False
                    logger.debug(f"Poll error: {e}")
                await asyncio.sleep(POLL_INTERVAL)

# ─── FLASK ────────────────────────────────────────────────────────────────────
flask_app=Flask(__name__)
engine: Optional[ImmersiveRouletteEngine]=None

@flask_app.route("/")
def home():
    return jsonify({"status":"ok","bot":"Immersive Roulette DC v33"})

@flask_app.route("/ping")
def ping():
    return jsonify({"status":"pong","ts":time.time()})

@flask_app.route("/health")
def health():
    if not engine: return jsonify({"status":"not_ready"}),503
    art_now=datetime.now(ART).strftime("%Y-%m-%d %H:%M ART")
    recent=engine.learner._recent()
    wins_r=sum(1 for s in recent if s["result"]=="WIN")
    return jsonify({
        "warmup":          engine.warmup_done,
        "spins":           len(engine.spin_history),
        "stats_connected": engine.sc.connected,
        "polls":           engine.sc.poll_count,
        "dozen_signal":    engine.signal_active,
        "color_signal":    engine.color_signal_active,
        "zone_signal":     engine.zone_signal_active,
        "column_signal":   engine.column_signal_active,
        "scoreboard":      scoreboard.get_text().replace("<b>","").replace("</b>",""),
        "art_time":        art_now,
        "learner_signals": len(engine.learner.history),
        "learner_wr":      f"{wins_r/len(recent)*100:.1f}%" if recent else "—",
    })

# ─── COMANDOS TELEGRAM ────────────────────────────────────────────────────────
@bot.message_handler(commands=["start","help"])
def cmd_start(m):
    bot.reply_to(m,
        "<b>🎡 Immersive Roulette DC v33</b>\n\n"
        "Señales sin gestión de apuesta\n"
        "🅐 E1: PF+PHF+ML · 🅑 E2: PHTML+EMA · 🅒 E3: Retorno\n"
        "🎨 Color (P1+P3) · 🗺 Zona (P1+P3)\n\n"
        "Marcador diario → reset 00:00 ART\n\n"
        "/status /marcador /aprendizaje /debug /reset",
        parse_mode="HTML")

@bot.message_handler(commands=["marcador","score"])
def cmd_marcador(m):
    bot.reply_to(m, scoreboard.get_text(), parse_mode="HTML")

@bot.message_handler(commands=["status"])
def cmd_status(m):
    if not engine: bot.reply_to(m,"❌ Engine no inicializado",parse_mode="HTML"); return
    _strat={STRAT_E1:"🅐E1",STRAT_E2:"🅑E2",STRAT_E3:"🅒E3"}
    if engine.signal_active:
        lbl=_strat.get(engine.active_strategy,"—")
        pair=f"D{engine.active_pair[0]}+D{engine.active_pair[1]}" if engine.active_pair else "—"
        d_st=f"🟢 {lbl} | {pair}"
    else: d_st="⚪ Idle"
    c_st=(f"🟡 {engine.color_signal_bet} ({engine.color_signal_prob:.0%})"
          if engine.color_signal_active else "⚪")
    z_st=(f"🟡 {engine.zone_signal_bet} ({engine.zone_signal_prob:.0%})"
          if engine.zone_signal_active else "⚪")
    col_pair = engine.column_signal_pair
    col_st   = (
        f"🟡 C{col_pair[0]}+C{col_pair[1]} ({engine.column_signal_prob:.0%})"
        if engine.column_signal_active else "⚪"
    )
    conn="🟢 OK" if engine.sc.connected else "🔴 Desc."
    ago=time.time()-engine.sc.last_poll_ok if engine.sc.last_poll_ok>0 else 0
    art_now=datetime.now(ART).strftime("%H:%M ART")
    bot.reply_to(m,
        f"<b>🎡 Immersive Roulette DC v33</b>\n"
        f"<b>Docenas:</b> {d_st}\n"
        f"<b>Columnas:</b> {col_st}\n"
        f"<b>Color:</b> {c_st}\n"
        f"<b>Zona:</b> {z_st}\n"
        f"<b>Giros:</b> {len(engine.spin_history)}\n"
        f"<b>Servidor:</b> {conn} ({ago:.0f}s)\n"
        f"<b>Hora:</b> {art_now}\n\n"
        f"{scoreboard.get_text()}",
        parse_mode="HTML")

@bot.message_handler(commands=["debug"])
def cmd_debug(m):
    if not engine or not engine.warmup_done:
        bot.reply_to(m,"⏳ Calentando...",parse_mode="HTML"); return
    last_num=engine.spin_history[-1]["number"] if engine.spin_history else None
    trend=ema_trend_str(engine.doc_levels)
    e1,e2,e3=engine._detect_e1(),engine._detect_e2(),engine._detect_e3()
    def st(s): return f"✅ D{s['pair']} ({s['prob']:.0%})" if s else "—"
    cp=engine.sc.predict_color_signal(last_num) if last_num else None
    zp=engine.sc.predict_zone_signal(last_num)  if last_num else None
    col_sig=engine._detect_col_signal(last_num) if last_num else None
    cp_txt=(f"✅ {cp['bet']} ({cp['prob']:.0%}) {cp.get('components','')}" if cp else "—")
    zp_txt=(f"✅ {zp['bet']} ({zp['prob']:.0%}) {zp.get('components','')}" if zp else "—")
    col_txt=(
        f"✅ C{col_sig['pair']} ({col_sig['prob']:.0%}) pat=[{col_sig.get('pattern_str','')}]"
        if col_sig else "—"
    )

    # Seq stats servidor para last_num
    d_seq  = engine.sc.get_dozen_seq_top_pair(last_num)  if last_num else None
    c_seq  = engine.sc.get_column_seq_top_pair(last_num) if last_num else None
    d_seq_txt = (
        f"D{d_seq['pair']} eff={d_seq['top_efectividad']:.1f}% n={d_seq['total']}"
        if d_seq else "sin datos"
    )
    c_seq_txt = (
        f"C{c_seq['pair']} eff={c_seq['top_efectividad']:.1f}% n={c_seq['total']}"
        if c_seq else "sin datos"
    )

    bot.reply_to(m,
        f"<b>🔬 Debug #{last_num} | EMA {trend.upper()}</b>\n\n"
        f"🅐 E1: {st(e1)}\n🅑 E2: {st(e2)}\n🅒 E3: {st(e3)}\n\n"
        f"🎨 Color: {cp_txt}\n🗺 Zona:  {zp_txt}\n🏛 Columna: {col_txt}\n\n"
        f"📊 Seq-D #{last_num}: {d_seq_txt}\n"
        f"📊 Seq-C #{last_num}: {c_seq_txt}\n\n"
        f"Colores últimos 5: {engine.color_seq[-5:]}\n"
        f"Zonas últimas 5:  {engine.zone_seq[-5:]}\n"
        f"Docenas últimas 5:{engine.dozen_seq[-5:]}\n\n"
        f"📊 Hist. docena #{last_num}: "
        f"{engine.sc.get_dozen_signal_winrate(last_num):.0%}"
        if engine.sc.get_dozen_signal_winrate(last_num) is not None
        else f"📊 Hist. docena #{last_num}: sin datos",
        parse_mode="HTML")

@bot.message_handler(commands=["aprendizaje"])
def cmd_aprendizaje(m):
    if not engine: bot.reply_to(m,"❌ Engine no inicializado",parse_mode="HTML"); return
    bot.reply_to(m,engine.learner.get_summary(30),parse_mode="HTML")

@bot.message_handler(commands=["reset"])
def cmd_reset(m):
    if engine:
        engine.processed_game_ids.clear()
        engine._reset_dozen_signal()
        engine._reset_color_signal()
        engine._reset_zone_signal()
        engine._reset_column_signal()
    bot.reply_to(m,
        "🔄 <b>Señales reseteadas</b>\n"
        "<i>🧠 Aprendizaje conservado</i>",
        parse_mode="HTML")

@bot.message_handler(commands=["reset_marcador"])
def cmd_reset_marcador(m):
    scoreboard.wins=0; scoreboard.losses=0
    scoreboard._current_day=scoreboard._art_day()
    bot.reply_to(m,"🔄 <b>Marcador diario reseteado.</b>",parse_mode="HTML")

@bot.message_handler(commands=["reset_learning"])
def cmd_reset_learning(m):
    if not engine: bot.reply_to(m,"❌ Engine no inicializado",parse_mode="HTML"); return
    try:
        engine._db.execute("DELETE FROM signal_log"); engine._db.commit()
        engine.learner.history.clear(); engine.learner.pending_id=None
        bot.reply_to(m,"🗑️ <b>Historial de aprendizaje borrado.</b>",parse_mode="HTML")
    except Exception as e:
        bot.reply_to(m,f"❌ Error: {e}",parse_mode="HTML")

# ─── SELF PING ────────────────────────────────────────────────────────────────
async def self_ping_loop():
    url=os.environ.get("RENDER_EXTERNAL_URL","").rstrip("/")
    if not url or "localhost" in url: return
    await asyncio.sleep(30)
    while True:
        try:
            import urllib.request as _ur
            _ur.urlopen(f"{url}/ping",timeout=15)
        except: pass
        await asyncio.sleep(240)

def run_flask():
    flask_app.run(host="0.0.0.0",port=10005,debug=False,use_reloader=False)

def run_telegram():
    """
    Inicia el polling de Telegram con manejo robusto de conflictos.

    CLAVE: usa none_stop=False para que nuestro except capture el 409.
    Con none_stop=True telebot lo captura internamente y nunca llega aqui,
    por eso el bot seguia reintentando cada 6s sin backoff.

    Backoff exponencial en 409: 15s -> 30s -> 60s -> 120s max.
    """
    # Limpiar webhook/sesion anterior (hasta 3 intentos)
    for attempt in range(3):
        try:
            bot.delete_webhook(drop_pending_updates=True)
            logger.info("✅ Webhook eliminado / sesiones anteriores limpiadas")
            break
        except Exception as e:
            logger.warning(f"⚠️ delete_webhook intento {attempt+1}: {e}")
            time.sleep(3)

    time.sleep(45)  # Espera a que Render termine la instancia anterior (~30-60s)

    delay = 15  # backoff inicial
    while True:
        try:
            # none_stop=False: las excepciones llegan a nuestro except
            bot.polling(none_stop=False, interval=1, timeout=30)
            delay = 15  # reset backoff si salio limpio
        except telebot.apihelper.ApiTelegramException as e:
            if e.error_code == 409:
                logger.warning(
                    f"⚠️ 409 Conflict: otra instancia activa. "
                    f"Esperando {delay}s... "
                    f"(verificar en Render que solo haya UN servicio activo)"
                )
                try:
                    bot.stop_polling()
                except Exception:
                    pass
                time.sleep(delay)
                delay = min(delay * 2, 120)  # 15->30->60->120s
            elif e.error_code == 429:
                try:
                    wait = int("".join(filter(str.isdigit, str(e)))) + 2
                except Exception:
                    wait = 30
                logger.warning(f"⏳ Rate limit 429. Esperando {wait}s...")
                time.sleep(wait)
            else:
                logger.error(f"❌ Error API Telegram ({e.error_code}): {e}")
                time.sleep(5)
        except Exception as e:
            logger.error(f"❌ Error inesperado en polling: {e}")
            time.sleep(5)

# ─── MAIN ─────────────────────────────────────────────────────────────────────
async def main():
    global engine
    sc=StatsClient(); engine=ImmersiveRouletteEngine(sc)
    threading.Thread(
        target=run_telegram,
        daemon=True
    ).start()
    logger.info(
        f"[ImmersiveDC] 🎡 Immersive Roulette DC v33 — "
        f"HTTP Polling {POLL_INTERVAL}s | Señales sin apuesta | "
        f"Color/Zona: P1+P3 | Tracking docenas → servidor | "
        f"Marcador diario ART | 🧠 Learner ({len(engine.learner.history)} señales)"
    )
    await asyncio.gather(
        asyncio.create_task(engine.poll_loop()),
        asyncio.create_task(self_ping_loop()),
    )

if __name__=="__main__":
    threading.Thread(target=run_flask,daemon=True).start()
    try: asyncio.run(main())
    except KeyboardInterrupt: logger.info("Bot detenido.")
