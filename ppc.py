#!/usr/bin/env python3
"""
Auto Roulette — Bot de señales Híbrido (Secuencias + ML + AMX + Labouchere)
Sistema de Chance Simples: COLOR, PARIDAD, ZONA

  - Ventana Móvil Markov 60 giros (Orden 3) + ML Cruzado + Resonancia/Ruptura.
  - Gestión Labouchere: Secuencia inicial [$250, $500, $250] (Ganar la secuencia = +$1000).
  - Sesiones Sin Límite: Solo se cierran al cumplir la meta de +$1500.
  - Inicio de Sesión: Estricto en minutos :00 o :30.
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
from flask import Flask, jsonify
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
TOKEN           = "8308452662:AAGZFIZyYsmVR39SvIOSlKD3OY_YNMOsEQU"
CHAT_ID         = -1003522684671
STATS_THREAD_ID = 40034

_session = requests.Session()
_retry = Retry(total=5, backoff_factor=1.5, status_forcelist=[429, 500, 502, 503, 504],
               allowed_methods=["GET", "POST"], raise_on_status=False)
_session.mount("https://", HTTPAdapter(max_retries=_retry, pool_connections=10, pool_maxsize=20))
_session.mount("http://",  HTTPAdapter(max_retries=_retry, pool_connections=10, pool_maxsize=20))
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
WS_URL              = "wss://dga.pragmaticplaylive.net/ws"
CASINO_ID           = "ppcjd00000007254"
WARMUP_SPINS        = 25
MIN_PROB            = 0.65
TRAIN_INTERVAL      = 50
WS_SERVER_PORT      = int(os.environ.get("WS_SERVER_PORT", 8765))
SESSION_TARGET      = 2000  # Meta de $1500 por sesión

SEQUENCE_COLOR   = ["ROJO", "NEGRO", "ROJO", "ROJO", "NEGRO", "NEGRO", "ROJO", "NEGRO", "ROJO", "ROJO", "NEGRO", "NEGRO", "ROJO", "NEGRO", "ROJO"]
SEQUENCE_PARIDAD = ["PAR", "IMPAR", "PAR", "PAR", "IMPAR", "IMPAR", "PAR", "IMPAR", "PAR", "PAR", "IMPAR", "IMPAR", "PAR", "IMPAR", "PAR"]
SEQUENCE_ZONA    = ["MENOR", "MAYOR", "MENOR", "MENOR", "MAYOR", "MAYOR", "MENOR", "MAYOR", "MENOR", "MENOR", "MAYOR", "MAYOR", "MENOR", "MAYOR", "MENOR"]

EMOJI_MAP = {"ROJO": "🔴", "NEGRO": "⚫️", "PAR": "🟣", "IMPAR": "🟡", "MENOR": "🟤", "MAYOR": "🔵", "CERO": "🟢"}

COLOR_MAP: dict = {0:"CERO",1:"ROJO",2:"NEGRO",3:"ROJO",4:"NEGRO",5:"ROJO",6:"NEGRO",7:"ROJO",8:"NEGRO",9:"ROJO",10:"NEGRO",11:"NEGRO",12:"ROJO",13:"NEGRO",14:"ROJO",15:"NEGRO",16:"ROJO",17:"NEGRO",18:"ROJO",19:"ROJO",20:"NEGRO",21:"ROJO",22:"NEGRO",23:"ROJO",24:"NEGRO",25:"ROJO",26:"NEGRO",27:"ROJO",28:"NEGRO",29:"NEGRO",30:"ROJO",31:"NEGRO",32:"ROJO",33:"NEGRO",34:"ROJO",35:"NEGRO",36:"ROJO"}
PARIDAD_MAP: dict = {n: ("PAR" if n > 0 and n % 2 == 0 else ("IMPAR" if n > 0 else "CERO")) for n in range(37)}
ZONA_MAP: dict = {n: ("MENOR" if 1 <= n <= 18 else ("MAYOR" if n >= 19 else "CERO")) for n in range(37)}

_ws_clients: Set[asyncio.Queue] = set()

def queue_broadcast(data: dict):
    for q in list(_ws_clients):
        try: 
            q.put_nowait(data)
        except: 
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
                except:
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
    except:
        pass

def fmt_money(val) -> str:
    if val == int(val):
        return f"${int(val)}"
    return f"${val:,.2f}"

# ─── MONEDAS ──────────────────────────────────────────────────────────────────
CURRENCY_RATES = {
    "USD": 0.10,
    "MXN": 2.00,
    "PEN": 0.50,
    "COP": 500.0,
    "ARS": 250.0,
    "CLP": 100.0,
}
CURRENCY_FLAGS = {
    "USD": "🇺🇲", "MXN": "🇲🇽", "PEN": "🇵🇪",
    "COP": "🇨🇴", "ARS": "🇦🇷", "CLP": "🇨🇱",
}
CURRENCY_SYMBOLS = {
    "USD": "$", "MXN": "$", "PEN": "./S",
    "COP": "$", "ARS": "$", "CLP": "$",
}

def fmt_currency_amount(chips: float, currency: str) -> str:
    amount = chips * CURRENCY_RATES[currency]
    sym = CURRENCY_SYMBOLS[currency]
    if currency in ("USD", "MXN", "PEN"):
        return f"{sym} {amount:.2f}"
    return f"{sym} {int(round(amount))}"

def fmt_gestion_signal(chips: float) -> str:
    """Gestión por país para el mensaje de señal (dos por línea)."""
    usd = fmt_currency_amount(chips, "USD")
    mxn = fmt_currency_amount(chips, "MXN")
    pen = fmt_currency_amount(chips, "PEN")
    cop = fmt_currency_amount(chips, "COP")
    ars = fmt_currency_amount(chips, "ARS")
    clp = fmt_currency_amount(chips, "CLP")
    return (
        f"🇺🇲 USD: {usd} — 🇲🇽 MXN: {mxn}\n"
        f"🇵🇪 PEN: {pen} — 🇨🇴 COP: {cop}\n"
        f"🇦🇷 ARS: {ars} — 🇨🇱 CLP: {clp}"
    )

def fmt_gestion_bankroll(chips: float) -> str:
    """Gestión por país para el mensaje de ciclo completado (dos por línea)."""
    usd = fmt_currency_amount(chips, "USD")
    mxn = fmt_currency_amount(chips, "MXN")
    pen = fmt_currency_amount(chips, "PEN")
    cop = fmt_currency_amount(chips, "COP")
    ars = fmt_currency_amount(chips, "ARS")
    clp = fmt_currency_amount(chips, "CLP")
    return (
        f"🇺🇲 USD: {usd} — 🇲🇽 MXN: {mxn}\n"
        f"🇵🇪 PEN: {pen} — 🇨🇴 COP: {cop}\n"
        f"🇦🇷 ARS: {ars} — 🇨🇱 CLP: {clp}"
    )


class SmoothedMarkovPredictor:
    def __init__(self, window: int = 60, order: int = 3):  # Orden 3
        self.window = window
        self.order = order
        self.transition_counts: dict = {}

    def update(self, sequence: list):
        self.transition_counts = defaultdict(lambda: defaultdict(int))
        recent = sequence[-self.window:]
        if len(recent) < self.order + 1:
            return
        for i in range(len(recent) - self.order):
            self.transition_counts[tuple(recent[i:i + self.order])][recent[i + self.order]] += 1

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
            ("ZONA", target_z, self.mnb_zona, self.sgd_zona),
        ]:
            y = np.array([target])
            classes = getattr(self, f"CLASSES_{cat.upper()}")
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
        except:
            return None


class AMXAnalyzer:
    def adjust_probability(self, base_prob: float, target: str, predictions: dict, recent_hist: list, seq_state) -> float:
        cross_boost = 0.0
        target_cat = "COLOR" if target in ["ROJO", "NEGRO"] else ("PARIDAD" if target in ["PAR", "IMPAR"] else "ZONA")
        
        # ── Cruces Simétricos (Ahora ROJO/PAR/MENOR tienen los mismos derechos) ──
        if target_cat == "COLOR":
            if target == "ROJO":
                if predictions.get("PARIDAD", {}).get("PAR", 0.5) > 0.55: cross_boost += 0.02
                if predictions.get("ZONA", {}).get("MENOR", 0.5) > 0.55: cross_boost += 0.02
            elif target == "NEGRO":
                if predictions.get("PARIDAD", {}).get("IMPAR", 0.5) > 0.55: cross_boost += 0.02
                if predictions.get("ZONA", {}).get("MAYOR", 0.5) > 0.55: cross_boost += 0.02
        elif target_cat == "PARIDAD":
            if target == "PAR":
                if predictions.get("COLOR", {}).get("NEGRO", 0.5) > 0.55: cross_boost += 0.02
                if predictions.get("ZONA", {}).get("MENOR", 0.5) > 0.55: cross_boost += 0.02
            elif target == "IMPAR":
                if predictions.get("COLOR", {}).get("ROJO", 0.5) > 0.55: cross_boost += 0.02
                if predictions.get("ZONA", {}).get("MAYOR", 0.5) > 0.55: cross_boost += 0.02
        elif target_cat == "ZONA":
            if target == "MENOR":
                if predictions.get("PARIDAD", {}).get("PAR", 0.5) > 0.55: cross_boost += 0.02
                if predictions.get("COLOR", {}).get("NEGRO", 0.5) > 0.55: cross_boost += 0.02
            elif target == "MAYOR":
                if predictions.get("PARIDAD", {}).get("IMPAR", 0.5) > 0.55: cross_boost += 0.02
                if predictions.get("COLOR", {}).get("ROJO", 0.5) > 0.55: cross_boost += 0.02

        # ── Resonancia por Racha Real (Reemplaza la resonancia de secuencia fija) ──
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

        # ── Ruptura de Repetición ──
        if len(recent_hist) >= 2 and recent_hist[-1] == recent_hist[-2] and recent_hist[-1] != "CERO" and recent_hist[-1] != target:
            cross_boost += 0.04

        # ── Bonus de Alineación con Secuencia Fija ──
        if target == seq_state.expected():
            cross_boost += 0.03

        return min(1.0, base_prob + cross_boost)


class Labouchere:
    def __init__(self):
        self.base_seq = [1, 2, 1]
        self.seq = list(self.base_seq)

    def get_bet(self) -> int:
        if not self.seq:
            return 250
        if len(self.seq) == 1:
            return self.seq[0]
        return self.seq[0] + self.seq[-1]

    def win(self) -> bool:
        if len(self.seq) >= 2:
            self.seq.pop(0)
            self.seq.pop(-1)
        elif len(self.seq) == 1:
            self.seq.pop(0)
        if not self.seq:
            self.seq = list(self.base_seq)
            return True
        return False

    def loss(self, bet: int):
        self.seq.append(bet)


class SequenceState:
    def __init__(self, category: str):
        self.category = category
        self.sequence = SEQUENCE_COLOR if category == "COLOR" else (SEQUENCE_PARIDAD if category == "PARIDAD" else SEQUENCE_ZONA)
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
        text += f"► Total señales del día: {total}\n\n"
        text += "📌 Últimas 20 SEÑALES 📌\n"
        for s in reversed(list(self.last_20)):
            a_str = f"🔄 INTENTO #{s['attempt']}"
            b_str = f"💵 +{fmt_currency_amount(s['val'], 'USD')}" if s['result'] == 'WIN' else f"💵 -{fmt_currency_amount(s['val'], 'USD')}"
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
        wait = (target - now).total_seconds()
        return max(1.0, wait)

    def _start_session(self):
        self.session_active = True
        self.session_start_time = time.time()
        self.session_start_chips = GLOBAL_STATS.global_chips
        engine = self.engines[self.current_idx]
        logger.info(f"[Session] 🟢 Iniciada: {engine.name} | Bankroll Inicio: {fmt_currency_amount(self.session_start_chips, 'USD')}")
        tg_send(f"🟢 SESIÓN INICIADA 🟢\n🎰 {engine.name}\n💵 Meta: +{fmt_currency_amount(SESSION_TARGET, 'USD')}\n⏱ La sesión no cierra hasta cumplir la meta")
        queue_broadcast({"type": "session", "status": "active"})

    def _end_session(self):
        engine = self.engines[self.current_idx]
        self.session_active = False
        
        duration_mins = int((time.time() - self.session_start_time) / 60)

        engine.labouchere = Labouchere()
        engine.cycle_active = False
        engine.signal_active = False
        engine.attempt = 1
        if engine.active_signal_msg_id:
            tg_delete(CHAT_ID, engine.active_signal_msg_id)
            engine.active_signal_msg_id = None
        if engine.analyzing_msg_id:
            tg_delete(CHAT_ID, engine.analyzing_msg_id)
            engine.analyzing_msg_id = None

        logger.info(f"[Session] 🔴 Terminada: {engine.name} | Meta cumplida!")
        tg_send(
            f"🔴 SESIÓN CERRADA 🔴\n"
            f"⏱ Duración: {duration_mins} minutos\n"
            f"💵 BALANCE GLOBAL POR PAIS 💵\n"
            f"{fmt_gestion_bankroll(GLOBAL_STATS.global_chips)}"
        )
        queue_broadcast({"type": "session", "status": "closed"})

    async def session_watchdog(self):
        while True:
            wait = self.seconds_to_next_slot()
            logger.info(f"[Session] ⏳ Esperando {wait/60:.1f} min para el próximo slot (:00 o :30)...")
            await asyncio.sleep(wait)
            self._start_session()

            while self.session_active:
                await asyncio.sleep(1)
                if GLOBAL_STATS.global_chips >= self.session_start_chips + SESSION_TARGET:
                    self._end_session()
                    break


class RouletteEngine:
    def __init__(self, ws_key: int, name: str):
        self.ws_key = ws_key
        self.name = name
        self.db_path = f"main_roulette_{ws_key}.db"
        self.spin_history: list = []
        self.hist_color: list = []
        self.hist_paridad: list = []
        self.hist_zona: list = []
        self.CLASSES = {
            "COLOR": ["ROJO", "NEGRO", "CERO"],
            "PARIDAD": ["PAR", "IMPAR", "CERO"],
            "ZONA": ["MENOR", "MAYOR", "CERO"]
        }
        self.seq_states = {cat: SequenceState(cat) for cat in ["COLOR", "PARIDAD", "ZONA"]}
        self.labouchere = Labouchere()
        self.attempt = 1
        self.cycle_active = False
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
        except:
            pass

    def _load_live_history(self) -> int:
        try:
            rows = self._db.execute("SELECT number FROM live_spins ORDER BY id ASC").fetchall()
        except:
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

    def _update_state(self, number: int, persist=True, train_model=True):
        c = COLOR_MAP[number]
        p = PARIDAD_MAP[number]
        z = ZONA_MAP[number]
        self.spin_history.append({"number": number, "color": c, "paridad": p, "zona": z})
        self.hist_color.append(c)
        self.hist_paridad.append(p)
        self.hist_zona.append(z)
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
            "bankroll": GLOBAL_STATS.global_chips
        })

    def _get_predictions(self, cat: str) -> dict:
        hist = getattr(self, f"hist_{cat.lower()}")
        classes = self.CLASSES[cat]
        mk_probs = self.markov[cat].predict(hist, classes) or {c: 1 / 3 for c in classes}
        ens_probs = self.ensemble.predict(self.hist_color, self.hist_paridad, self.hist_zona, cat) or {c: 1 / 3 for c in classes}
        return {c: 0.4 * mk_probs.get(c, 0) + 0.6 * ens_probs.get(c, 0) for c in classes}

    def detect_signal(self) -> Optional[dict]:
        predictions = {c: self._get_predictions(c) for c in ["COLOR", "PARIDAD", "ZONA"]}
        valid_signals = {}
        
        best_prob = 0.0
        best_info = ""
        
        targets_map = {
            "COLOR": ["ROJO", "NEGRO"],
            "PARIDAD": ["PAR", "IMPAR"],
            "ZONA": ["MENOR", "MAYOR"]
        }
        
        # ── EVALUAR AMBOS LADOS (Ya no hay ceguedad) ──
        for cat in ["COLOR", "PARIDAD", "ZONA"]:
            hist = getattr(self, f"hist_{cat.lower()}")
            best_cat_prob = 0.0
            best_cat_target = None
            
            for target in targets_map[cat]:
                base_prob = predictions[cat].get(target, 0)
                final_prob = self.amx.adjust_probability(base_prob, target, predictions, hist, self.seq_states[cat])
                
                if final_prob > best_cat_prob:
                    best_cat_prob = final_prob
                    best_cat_target = target
                    
            if best_cat_prob > best_prob:
                best_prob = best_cat_prob
                best_info = f"{cat} -> {best_cat_target} ({best_cat_prob*100:.1f}%)"
                
            if best_cat_prob >= MIN_PROB:
                valid_signals[cat] = {"target": best_cat_target, "prob": best_cat_prob}

        # ── LOG DE TESTING ──────────────────────────────────────
        last_num = self.spin_history[-1]["number"] if self.spin_history else "?"
        signal_status = "🟢 SEÑAL VALIDA" if valid_signals else "🔴 Sin señal"
        logger.info(f"🎲 Giro #{last_num} | Prob Máxima: {best_info} | {signal_status}")
        # ─────────────────────────────────────────────────────────

        if not valid_signals:
            return None
        best_cat = max(valid_signals, key=lambda k: valid_signals[k]["prob"])
        best_sig = valid_signals[best_cat]
        return {"type": best_cat, "target": best_sig["target"], "prob": best_sig["prob"], "chips": self.labouchere.get_bet()}

    def _build_signal_text(self) -> str:
        last_num = self.spin_history[-1]["number"] if self.spin_history else 0
        target = self.active_target
        target_emoji = EMOJI_MAP.get(target, "")
        target_display = target
        if target == "MENOR":
            target_display = f"MENOR (1-18) {target_emoji}"
        elif target == "MAYOR":
            target_display = f"MAYOR (19-36) {target_emoji}"
        elif target == "PAR":
            target_display = f"PARES {target_emoji}"
        elif target == "IMPAR":
            target_display = f"IMPARES {target_emoji}"
        else:
            target_display = f"{target} {target_emoji}"
        gestion = fmt_gestion_signal(self.active_chips)
        return (
            f"✅ SEÑAL CONFIRMADA — {target_display} ✅\n\n"
            f"🎰 {self.name}\n"
            f"👉 ÚLTIMO NÚMERO: {last_num}\n"
            f"♦️ ENTRAR EN: {target_display}\n"
            f"🔹 INTENTO: {self.attempt} DE 3\n\n"
            f"💡 PROBABILIDAD PATRÓN — {self._last_signal_prob:.1f}%\n"
            f"🚨 MONTO DE APUESTA POR PAIS:\n{gestion}"
        )

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
            "attempt": self.attempt, "prob": self._last_signal_prob, "bankroll": GLOBAL_STATS.global_chips
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
        actual_val = c if self.active_type == "COLOR" else (p if self.active_type == "PARIDAD" else z)
        won = (actual_val == self.active_target)
        is_zero = (number == 0)
        current_bet = self.active_chips
        
        if won:
            seq_completed = self.labouchere.win()
            GLOBAL_STATS.global_chips += current_bet
            GLOBAL_STATS.record('WIN', self.attempt, number, current_bet, self.active_type, self.name)
            msg = (
                f"✅ WIN {number} — {self.active_type} {self.active_target}\n"
                f"🎉 ¡Ganaste {fmt_currency_amount(current_bet, 'USD')}!"
            )
            tg_send(msg)
            if seq_completed:
                cycle_msg = (
                    f"🎉 CICLO DE LABOUCHER COMPLETADO 🎉\n"
                    f"🚨 GESTION ACTUAL POR PAIS:\n"
                    f"{fmt_gestion_bankroll(GLOBAL_STATS.global_chips)}"
                )
                tg_send(cycle_msg)
            self._send_analyzing_msg()
            self._end_cycle()
            queue_broadcast({"type": "result", "result": "WIN", "number": number, "bankroll": GLOBAL_STATS.global_chips})
        else:
            GLOBAL_STATS.global_chips -= current_bet
            self.labouchere.loss(current_bet)
            if self.attempt < 3:
                self.attempt += 1
                self.signal_active = False
                self._send_analyzing_msg()
                queue_broadcast({"type": "result_retry", "attempt": self.attempt, "bankroll": GLOBAL_STATS.global_chips})
            else:
                GLOBAL_STATS.record('EMPATE' if is_zero else 'LOSS', self.attempt, number, current_bet, self.active_type, self.name)
                msg = f"🟠 EMPATE 0" if is_zero else f"❌ LOSS TOTAL {number} — {self.active_type}"
                tg_send(f"{msg}\n🚨 Racha de 3 intentos perdida.")
                self._send_analyzing_msg()
                self._end_cycle()
                queue_broadcast({"type": "result", "result": "EMPATE" if is_zero else "LOSS", "number": number, "bankroll": GLOBAL_STATS.global_chips})

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
        if not GLOBAL_STATS.should_send():
            return
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
                
            sig = self.detect_signal()
            
            if not self.signal_active:
                if sig:
                    if self.cycle_active:
                        self.active_type = sig["type"]
                        self.active_target = sig["target"]
                        self._last_signal_prob = sig["prob"] * 100
                        self.active_chips = self.labouchere.get_bet()
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
                        except:
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
                                    except:
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
                            except:
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
                                except:
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


# ─── WS SERVER PARA HTML EXTERNO ─────────────────────────────────────────────
async def ws_client_handler(websocket):
    q = asyncio.Queue()
    _ws_clients.add(q)
    try:
        if engines_global:
            e = engines_global[0]
            state = {
                "type": "init", "bankroll": GLOBAL_STATS.global_chips,
                "session_active": session_mgr_global.session_active if session_mgr_global else False,
                "labouchere": e.labouchere.seq, "history": e.spin_history[-20:]
            }
            await websocket.send(json.dumps(state))
        
        while True:
            data = await q.get()
            await websocket.send(json.dumps(data))
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        _ws_clients.discard(q)

async def _ws_server_main():
    try:
        async with websockets.serve(ws_client_handler, "0.0.0.0", WS_SERVER_PORT):
            logger.info(f"[WSServer] 🌐 Servidor WebSocket para HTML iniciado en puerto {WS_SERVER_PORT}")
            await asyncio.Future()
    except Exception as e:
        logger.error(f"[WSServer] Error al iniciar servidor WS: {e}")


# ─── FLASK ────────────────────────────────────────────────────────────────────
app = Flask(__name__)
engines_global: list = []
session_mgr_global: Optional[SessionManager] = None


@app.route("/")
def home():
    return jsonify({"status": "ok"})


@app.route("/ping")
def ping():
    return jsonify({"status": "pong", "ts": time.time()})


@app.route("/health")
def health():
    if not engines_global:
        return jsonify({"status": "initializing"})
    e = engines_global[0]
    sa = "Active" if session_mgr_global and session_mgr_global.session_active else "Inactive"
    return jsonify({"bankroll": GLOBAL_STATS.global_chips, "session": sa})


async def self_ping_loop():
    url = os.environ.get("RENDER_EXTERNAL_URL", "").rstrip("/")
    if not url:
        return
    await asyncio.sleep(30)
    while True:
        try:
            urllib.request.urlopen(f"{url}/ping", timeout=15)
        except:
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
    bot.reply_to(m, "<b>🎰 AUTO ROULETTE</b>\nSesión ilimitada hasta meta\nLabouchere [1,2,1] — Mín USD $0.10\n/status /stats /reset", parse_mode="HTML")


@bot.message_handler(commands=['status'])
def cmd_status(m):
    if not engines_global:
        return
    e = engines_global[0]
    sa = "🟢 Activa" if session_mgr_global and session_mgr_global.session_active else "⚪ Inactiva"
    bot.reply_to(m, f"<b>Sesión:</b> {sa}\n<b>Bankroll:</b> 🪙 {fmt_currency_amount(GLOBAL_STATS.global_chips, 'USD')}\n<b>Labouchere:</b> {e.labouchere.seq}", parse_mode="HTML")


@bot.message_handler(commands=['stats'])
def cmd_stats(m):
    tg_send_stats(GLOBAL_STATS.get_stats_text())


@bot.message_handler(commands=['reset'])
def cmd_reset(m):
    global GLOBAL_STATS
    GLOBAL_STATS = GlobalStats()
    for e in engines_global:
        e.labouchere = Labouchere()
        e._end_cycle()
    bot.reply_to(m, "🔄 <b>Resetado</b>", parse_mode="HTML")


def run_flask():
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)), debug=False, use_reloader=False)

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
    global engines_global, session_mgr_global
    engines_global = [RouletteEngine(r["key"], r["name"]) for r in ROULETTES]
    session_mgr_global = SessionManager(engines_global)
    threading.Thread(target=run_flask, daemon=True).start()
    threading.Thread(target=run_bot, daemon=True).start()
    await asyncio.sleep(5)
    tasks = [
        asyncio.create_task(session_mgr_global.session_watchdog()),
        asyncio.create_task(daily_stats_loop()),
        asyncio.create_task(self_ping_loop()),
        asyncio.create_task(_ws_server_main()),
    ]
    for i, r in enumerate(ROULETTES):
        tasks.append(asyncio.create_task(ws_reader(r["key"], engines_global[i], session_mgr_global)))
    logger.info(f"[Main] 🎰 Bot iniciado")
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
