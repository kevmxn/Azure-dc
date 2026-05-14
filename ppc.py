#!/usr/bin/env python3
"""
Speed Roulette 2 — Bot de señales Híbrido (Secuencias + ML + AMX)
Sistema de Chance Simples: COLOR, PARIDAD, ZONA

  - Secuencia fija de 15 pasos corriendo en 2º plano desde el inicio.
  - Filtro de IA (Markov + Ensemble ML + AMX): Solo apuesta si la confianza >= 60%.
  - Modelos aprenden en tiempo real con cada giro (Online Learning).
  - Gestión D'Alembert: Apuesta inicial 5 fichas. Win: -1 ficha (mín 5). Loss: +1 ficha (máx 20).
  - 5 intentos máximos por racha. Si falla 5 veces seguidas, reinicia intento a 1.
  - Sesiones de 30 min (25 activos + 5 pausa). Meta por sesión: +5 fichas.
  - Cada sesión rota la categoría (Color -> Paridad -> Zona -> Color...).
  - Máx 3 señales emitidas por sesión.
  - WebSocket server en puerto 8765 para HTML externo.
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
                    format='%(asctime)s [Speed2DC] %(levelname)s %(message)s')
logger = logging.getLogger("Speed2DC")
for _ln in ['werkzeug', 'flask.app', 'flask', 'urllib3']:
    logging.getLogger(_ln).setLevel(logging.ERROR)

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
    {"key": 205, "name": "SPEED ROULETTE 2"},
]

ROULETTE_LINKS = {
    "SPEED ROULETTE 2": "https://1win.lat/casino/play/v_pragmatic:speedroulette2",
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
SESSION_ACTIVE      = 25 * 60
SESSION_PAUSE       = 5  * 60
SESSION_TOTAL       = SESSION_ACTIVE + SESSION_PAUSE
WARMUP_SPINS        = 25
MIN_PROB            = 0.60  # 60% de confianza IA
TRAIN_INTERVAL      = 50
MAX_SIGNALS         = 3
SIGNAL_WAIT_TIMEOUT = 120
WS_SERVER_PORT      = int(os.environ.get("WS_SERVER_PORT", 8765))

# Secuencias y Mapeos
SEQUENCE_COLOR   = ["ROJO", "NEGRO", "ROJO", "ROJO", "NEGRO", "NEGRO", "ROJO", "NEGRO", "ROJO", "ROJO", "NEGRO", "NEGRO", "ROJO", "NEGRO", "ROJO"]
SEQUENCE_PARITY  = ["PAR", "IMPAR", "PAR", "PAR", "IMPAR", "IMPAR", "PAR", "IMPAR", "PAR", "PAR", "IMPAR", "IMPAR", "PAR", "IMPAR", "PAR"]
SEQUENCE_ZONE    = ["MENOR", "MAYOR", "MENOR", "MENOR", "MAYOR", "MAYOR", "MENOR", "MAYOR", "MENOR", "MENOR", "MAYOR", "MAYOR", "MENOR", "MAYOR", "MENOR"]

EMOJI_MAP = {
    "ROJO": "🔴", "NEGRO": "⚫️", 
    "PAR": "🟣", "IMPAR": "🟡", 
    "MENOR": "🟤", "MAYOR": "🔵", 
    "CERO": "🟢"
}

COLOR_MAP: dict = {
    0:"CERO",1:"ROJO",2:"NEGRO",3:"ROJO",4:"NEGRO",5:"ROJO",6:"NEGRO",
    7:"ROJO",8:"NEGRO",9:"ROJO",10:"NEGRO",11:"NEGRO",12:"ROJO",13:"NEGRO",
    14:"ROJO",15:"NEGRO",16:"ROJO",17:"NEGRO",18:"ROJO",19:"ROJO",20:"NEGRO",
    21:"ROJO",22:"NEGRO",23:"ROJO",24:"NEGRO",25:"ROJO",26:"NEGRO",27:"ROJO",
    28:"NEGRO",29:"NEGRO",30:"ROJO",31:"NEGRO",32:"ROJO",33:"NEGRO",34:"ROJO",
    35:"NEGRO",36:"ROJO",
}

PARITY_MAP: dict = {n: ("PAR" if n > 0 and n % 2 == 0 else ("IMPAR" if n > 0 else "CERO")) for n in range(37)}
ZONE_MAP: dict = {n: ("MENOR" if 1 <= n <= 18 else ("MAYOR" if n >= 19 else "CERO")) for n in range(37)}

CATEGORIES = ["COLOR", "PARIDAD", "ZONA"]

# ─── COLA DE BROADCAST PARA HTML ──────────────────────────────────────────────
_ws_clients: Set[asyncio.Queue] = set()

def queue_broadcast(data: dict):
    for q in list(_ws_clients):
        try: q.put_nowait(data)
        except: pass

# ─── TELEGRAM HELPERS ─────────────────────────────────────────────────────────
_TG_RETRIES = 12

def _tg_call(fn, *a, **kw):
    delay = 2.0
    for attempt in range(1, _TG_RETRIES + 1):
        try: return fn(*a, **kw)
        except Exception as e:
            err = str(e)
            if "retry after" in err.lower():
                try: wait = int(''.join(filter(str.isdigit, err))) + 1
                except: wait = 30
                time.sleep(wait); continue
            if attempt == _TG_RETRIES: return None
            time.sleep(delay); delay = min(delay * 2, 60)
    return None

def tg_send(text: str) -> Optional[int]:
    msg = _tg_call(bot.send_message, chat_id=CHAT_ID, text=text, parse_mode="HTML")
    return msg.message_id if msg else None

def tg_send_stats(text: str) -> Optional[int]:
    msg = _tg_call(bot.send_message, chat_id=CHAT_ID, text=text,
                   parse_mode="HTML", message_thread_id=STATS_THREAD_ID)
    return msg.message_id if msg else None

def tg_delete(chat_id: int, message_id: int):
    try: _tg_call(bot.delete_message, chat_id=chat_id, message_id=message_id)
    except: pass

def tg_edit(chat_id: int, message_id: int, text: str):
    try:
        _tg_call(bot.edit_message_text, text=text, chat_id=chat_id,
                 message_id=message_id, parse_mode="HTML")
    except: pass

# ─── MARKOV ───────────────────────────────────────────────────────────────────
class SmoothedMarkovPredictor:
    def __init__(self, window: int = 60, order: int = 2):
        self.window = window; self.order = order
        self.transition_counts: dict = {}

    def update(self, sequence: list):
        self.transition_counts = defaultdict(lambda: defaultdict(int))
        recent = sequence[-self.window:]
        if len(recent) < self.order + 1: return
        for i in range(len(recent) - self.order):
            state = tuple(recent[i:i + self.order])
            nxt = recent[i + self.order]
            self.transition_counts[state][nxt] += 1

    def predict(self, sequence: list, classes: list) -> Optional[dict]:
        if len(sequence) < self.order: return None
        state = tuple(sequence[-self.order:])
        counts = dict(self.transition_counts.get(state, {}))
        total = sum(counts.values())
        if total < 5: return None
        alpha = 1.0; vocab_size = len(classes)
        probs = {k: (v + alpha) / (total + alpha * vocab_size) for k, v in counts.items()}
        for c in classes:
            if c not in probs: probs[c] = alpha / (total + alpha * vocab_size)
        return probs

# ─── ENSEMBLE ML ──────────────────────────────────────────────────────────────
class OnlineEnsemblePredictor:
    WINDOW = 5; CLASSES_COLOR = ["ROJO", "NEGRO", "CERO"]
    CLASSES_PARITY = ["PAR", "IMPAR", "CERO"]; CLASSES_ZONE = ["MENOR", "MAYOR", "CERO"]

    def __init__(self):
        self.mnb_color = MultinomialNB(alpha=1.0); self.sgd_color = SGDClassifier(loss='log_loss', learning_rate='adaptive', eta0=0.01, penalty='l2', alpha=0.001)
        self.mnb_parity = MultinomialNB(alpha=1.0); self.sgd_parity = SGDClassifier(loss='log_loss', learning_rate='adaptive', eta0=0.01, penalty='l2', alpha=0.001)
        self.mnb_zone = MultinomialNB(alpha=1.0); self.sgd_zone = SGDClassifier(loss='log_loss', learning_rate='adaptive', eta0=0.01, penalty='l2', alpha=0.001)
        self.trained = {"COLOR": False, "PARIDAD": False, "ZONA": False}
        self.sample_count = 0

    def _extract_features(self, hist_c, hist_p, hist_z) -> Optional[list]:
        if len(hist_c) < self.WINDOW: return None
        features = []
        for i in range(1, self.WINDOW + 1):
            c, p, z = hist_c[-i], hist_p[-i], hist_z[-i]
            vec_c = [1 if x==c else 0 for x in self.CLASSES_COLOR]
            vec_p = [1 if x==p else 0 for x in self.CLASSES_PARITY]
            vec_z = [1 if x==z else 0 for x in self.CLASSES_ZONE]
            features.extend(vec_c + vec_p + vec_z)
        return features

    def partial_train(self, hist_c, hist_p, hist_z, target_c, target_p, target_z):
        feats = self._extract_features(hist_c, hist_p, hist_z)
        if feats is None: return
        X = np.array(feats).reshape(1, -1)
        
        for cat, target, mnb, sgd in [
            ("COLOR", target_c, self.mnb_color, self.sgd_color),
            ("PARIDAD", target_p, self.mnb_parity, self.sgd_parity),
            ("ZONA", target_z, self.mnb_zone, self.sgd_zone)
        ]:
            y = np.array([target])
            classes = getattr(self, f"CLASSES_{cat.upper() if cat=='COLOR' else ('PARITY' if cat=='PARIDAD' else 'ZONE')}")
            if not self.trained[cat]:
                mnb.partial_fit(X, y, classes=classes); sgd.partial_fit(X, y, classes=classes)
                self.trained[cat] = True
            else:
                mnb.partial_fit(X, y); sgd.partial_fit(X, y)
        self.sample_count += 1

    def predict(self, hist_c, hist_p, hist_z, cat: str) -> Optional[dict]:
        if not self.trained[cat]: return None
        feats = self._extract_features(hist_c, hist_p, hist_z)
        if feats is None: return None
        X = np.array(feats).reshape(1, -1)
        try:
            if cat == "COLOR": mnb, sgd = self.mnb_color, self.sgd_color
            elif cat == "PARIDAD": mnb, sgd = self.mnb_parity, self.sgd_parity
            else: mnb, sgd = self.mnb_zone, self.sgd_zone
            
            nb_p = mnb.predict_proba(X)[0]; sg_p = sgd.predict_proba(X)[0]
            final = 0.5 * nb_p + 0.5 * sg_p
            classes = getattr(self, f"CLASSES_{cat.upper() if cat=='COLOR' else ('PARITY' if cat=='PARIDAD' else 'ZONE')}")
            return {classes[i]: float(p) for i, p in enumerate(final)}
        except: return None

# ─── AMX (Análisis Matricial Cruzado) ────────────────────────────────────────
class AMXAnalyzer:
    """Ajusta probabilidades cruzando Color, Paridad y Zona."""
    def adjust_probability(self, base_prob: float, target: str, 
                           predictions: dict) -> float:
        cross_boost = 0.0
        target_cat = "COLOR" if target in ["ROJO", "NEGRO"] else ("PARIDAD" if target in ["PAR", "IMPAR"] else "ZONA")
        
        # Lógica de refuerzo cruzado básica
        if target_cat == "COLOR":
            p_par = predictions.get("PARIDAD", {}).get("PAR", 0.5)
            p_may = predictions.get("ZONA", {}).get("MAYOR", 0.5)
            # Intersecciones favorables en la ruleta
            if target == "ROJO" and p_par > 0.55: cross_boost += 0.02
            if target == "NEGRO" and p_may > 0.55: cross_boost += 0.02
            
        elif target_cat == "PARIDAD":
            p_rojo = predictions.get("COLOR", {}).get("ROJO", 0.5)
            p_may = predictions.get("ZONA", {}).get("MAYOR", 0.5)
            if target == "IMPAR" and p_rojo > 0.55: cross_boost += 0.02
            if target == "PAR" and p_may > 0.55: cross_boost += 0.02
            
        elif target_cat == "ZONA":
            p_rojo = predictions.get("COLOR", {}).get("ROJO", 0.5)
            p_par = predictions.get("PARIDAD", {}).get("PAR", 0.5)
            if target == "MENOR" and p_par > 0.55: cross_boost += 0.02
            if target == "MAYOR" and p_rojo > 0.55: cross_boost += 0.02

        return min(1.0, base_prob + cross_boost)

# ─── SECUENCIA Y D'ALEMBERT STATE ────────────────────────────────────────────
class SequenceState:
    def __init__(self, category: str):
        self.category = category
        self.sequence = SEQUENCE_COLOR if category == "COLOR" else (SEQUENCE_PARITY if category == "PARIDAD" else SEQUENCE_ZONE)
        self.idx = 0
        self.attempt = 1
        self.chips = 5
        self.session_profit = 0
        self.waiting_activation = True  # Espera primer opuesto para arrancar

    def advance(self):
        self.idx = (self.idx + 1) % len(self.sequence)

    def expected(self) -> str:
        return self.sequence[self.idx]

    def win(self):
        self.session_profit += self.chips
        self.chips = max(5, self.chips - 1)
        self.attempt = 1

    def loss(self):
        self.chips = min(20, self.chips + 1)
        if self.attempt >= 5:
            self.attempt = 1  # Resetea intentos, la racha se rompió
        else:
            self.attempt += 1

# ─── STATS GLOBAL ─────────────────────────────────────────────────────────────
class GlobalStats:
    def __init__(self):
        self.wins = 0; self.zeros = 0; self.losses = 0
        self.consecutive = 0
        self.last_20 = deque(maxlen=20)
        self.signals_processed = 0
        self.global_chips: int = 0
        self.last_report_signals = 0

    def record(self, result_type: str, attempt: int, number: int, val, type_str: str, roulette_name: str):
        self.signals_processed += 1
        if result_type == 'WIN':
            self.wins += 1; self.consecutive += 1; self.global_chips += val
        elif result_type == 'LOSS':
            self.losses += 1; self.consecutive = 0; self.global_chips -= val
        elif result_type == 'EMPATE':
            self.zeros += 1; self.global_chips -= val
        self.last_20.append({
            "result": result_type, "attempt": attempt,
            "number": number, "val": val, "type": type_str,
            "roulette": roulette_name, "balance": self.global_chips
        })

    def should_send(self) -> bool:
        return (self.signals_processed - self.last_report_signals) >= 20

    def mark_sent(self):
        self.last_report_signals = self.signals_processed

    def get_stats_text(self) -> str:
        total = self.wins + self.zeros + self.losses
        eff = ((self.wins + self.zeros) / total * 100) if total > 0 else 0.0
        text  = "📊 RESUMEN DIARIO — SPEED ROULETTE 2 📊\n"
        text += "🕛 Reporte 12:00 hs (Argentina)\n\n"
        text += f"► PLACAR = ✅{self.wins} | 🟠{self.zeros} | 🚫{self.losses}\n"
        text += f"► Consecutivas = {self.consecutive}\n"
        text += f"► Assertividade = {eff:.2f}%\n"
        text += f"► Fichas globales: 🪙 {self.global_chips}\n"
        text += f"► Total señales del día: {total}\n\n"
        text += "📌 Últimas 20 SEÑALES 📌\n"
        for s in reversed(list(self.last_20)):
            a_str = f"🔄 GALE #{s['attempt']}"
            b_str = f"🪙 {s['balance']}"
            rl    = s['roulette'][:14]
            if s['result'] == 'WIN':
                text += f"✅ WIN #{s['number']} {s['type']} | {rl} | {a_str} | {b_str}\n"
            elif s['result'] == 'EMPATE':
                text += f"🟠 EMPATE #0 ZERO | {rl} | {a_str} | {b_str}\n"
            else:
                text += f"🚫 LOSS #{s['number']} {s['type']} | {rl} | {a_str} | {b_str}\n"
        return text

GLOBAL_STATS = GlobalStats()

# ─── ENGINE ───────────────────────────────────────────────────────────────────
class RouletteEngine:

    def __init__(self, ws_key: int, name: str):
        self.ws_key  = ws_key
        self.name    = name
        self.db_path = f"main_roulette_{ws_key}.db"

        self.spin_history: list = []
        self.hist_color: list = []; self.hist_parity: list = []; self.hist_zone: list = []
        
        self.seq_states = {cat: SequenceState(cat) for cat in CATEGORIES}
        self.active_category_idx = 0  # Rota cada sesión

        self.markov = {cat: SmoothedMarkovPredictor() for cat in CATEGORIES}
        self.ensemble = OnlineEnsemblePredictor()
        self.amx = AMXAnalyzer()
        
        self.signal_active     = False
        self.active_type       = None
        self.active_target     = ""
        self._last_signal_prob = 0.0
        self.active_signal_msg_id = None
        self.spins_since_train = 0
        self.last_game_id      = None
        self.ws_count          = 0
        self.warmup_done       = False

        self._db  = self._get_db()
        live = self._load_live_history()
        self.ws_count    = live
        self.warmup_done = live >= WARMUP_SPINS
        logger.info(f"[{name}] Pre-cargados: {live} giros | Warmup: {'✅' if self.warmup_done else '⏳'}")

    @property
    def current_category(self) -> str:
        return CATEGORIES[self.active_category_idx]

    def rotate_category(self):
        self.active_category_idx = (self.active_category_idx + 1) % len(CATEGORIES)
        # Resetear profit de sesión al rotar
        for s in self.seq_states.values(): s.session_profit = 0

    def _get_db(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.execute("""CREATE TABLE IF NOT EXISTS live_spins (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            number INTEGER NOT NULL,
            ts INTEGER NOT NULL
        )""")
        conn.commit()
        return conn

    def _persist(self, number: int):
        try:
            self._db.execute("INSERT INTO live_spins(number,ts) VALUES(?,?)",
                             (number, int(time.time())))
            self._db.commit()
        except: pass

    def _load_live_history(self) -> int:
        try: rows = self._db.execute("SELECT number FROM live_spins ORDER BY id ASC").fetchall()
        except: return 0
        for (n,) in rows: self._update_state(n, persist=False, train_model=False)
        if rows: self._train_models()
        return len(rows)

    def _train_models(self):
        for cat in CATEGORIES:
            hist = getattr(self, f"hist_{cat.lower()}")
            self.markov[cat].update(hist)

    def _update_state(self, number: int, persist=True, train_model=True):
        c = COLOR_MAP[number]; p = PARITY_MAP[number]; z = ZONE_MAP[number]
        self.spin_history.append({"number": number, "color": c, "parity": p, "zone": z})
        self.hist_color.append(c); self.hist_parity.append(p); self.hist_zone.append(z)
        
        # Avance de secuencias en 2º plano
        for cat in CATEGORIES:
            state = self.seq_states[cat]
            if state.waiting_activation:
                # Activación: espera el opuesto al primer elemento para empezar
                if state.expected() != getattr(self, f"hist_{cat.lower()}")[-1]:
                    state.waiting_activation = False
            else:
                state.advance()

        if train_model and number != 0 and len(self.hist_color) > 5:
            self.ensemble.partial_train(
                self.hist_color, self.hist_parity, self.hist_zone,
                c, p, z
            )
            self.spins_since_train += 1
            if self.spins_since_train >= TRAIN_INTERVAL:
                self._train_models(); self.spins_since_train = 0
        
        if persist: self._persist(number)

    def _get_predictions(self, cat: str) -> dict:
        """Obtiene predicciones de Markov y ML para una categoría."""
        hist = getattr(self, f"hist_{cat.lower()}")
        classes = OnlineEnsemblePredictor.CLASSES_COLOR if cat=="COLOR" else (OnlineEnsemblePredictor.CLASSES_PARITY if cat=="PARIDAD" else OnlineEnsemblePredictor.CLASSES_ZONE)
        
        mk_pred = self.markov[cat].predict(hist, classes)
        mk_probs = mk_pred if mk_pred else {c: 1/3 for c in classes}
        
        ens_probs = self.ensemble.predict(self.hist_color, self.hist_parity, self.hist_zone, cat)
        if not ens_probs: ens_probs = {c: 1/3 for c in classes}
        
        final_probs = {}
        for c in classes:
            final_probs[c] = 0.4 * mk_probs.get(c, 0) + 0.6 * ens_probs.get(c, 0)
            
        return final_probs

    def detect_signal(self) -> Optional[dict]:
        cat = self.current_category
        state = self.seq_states[cat]
        
        if state.waiting_activation or state.session_profit >= 5:
            return None
            
        target = state.expected()
        predictions = {c: self._get_predictions(c) for c in CATEGORIES}
        base_prob = predictions[cat].get(target, 0)
        
        # AMX Filter
        final_prob = self.amx.adjust_probability(base_prob, target, predictions)
        
        if final_prob >= MIN_PROB:
            return {"type": cat, "target": target, "prob": final_prob, "chips": state.chips, "attempt": state.attempt}
        return None

    def _build_signal_text(self) -> str:
        last_num = self.spin_history[-1]["number"] if self.spin_history else 0
        cat = self.active_type
        target = self.active_target
        state = self.seq_states[cat]
        
        # Emoji del último número basado en la categoría de la señal
        last_val = COLOR_MAP[last_num] if cat=="COLOR" else (PARITY_MAP[last_num] if cat=="PARIDAD" else ZONE_MAP[last_num])
        last_emoji = EMOJI_MAP.get(last_val, "")
        target_emoji = EMOJI_MAP.get(target, "")
        
        target_display = target
        if cat == "ZONA":
            target_display = f"MENOR (1-18) {target_emoji}" if target == "MENOR" else f"MAYOR (19-36) {target_emoji}"
        elif cat == "PARIDAD":
            target_display = f"PARES {target_emoji}" if target == "PAR" else f"IMPARES {target_emoji}"
        else:
            target_display = f"{target} {target_emoji}"

        return (
            f"✅ SEÑAL CONFIRMADA — {cat} ✅\n\n"
            f"🎰 {self.name}\n"
            f"👉 INGRESAR DESPUÉS DE: {last_num} {last_emoji}\n"
            f"♦️ ENTRAR EN: {target_display}\n"
            f"🔸 APUESTA: {state.chips} — FICHAS\n\n"
            f"💡 Probabilidad de Patrón — {self._last_signal_prob:.1f}%"
        )

    def send_signal(self):
        msg_id = tg_send(self._build_signal_text())
        if msg_id: self.active_signal_msg_id = msg_id
        
        state = self.seq_states[self.active_type]
        queue_broadcast({
            "type": "signal", "roulette": self.name, "signal_type": self.active_type,
            "target": self.active_target, "prob": round(self._last_signal_prob, 2),
            "attempt": state.attempt, "chips": state.chips, "category": self.active_type,
            "session_profit": state.session_profit
        })

    def iniciar_senal(self, sig: dict):
        self.signal_active     = True
        self.active_type       = sig["type"]
        self.active_target     = sig["target"]
        self._last_signal_prob = sig["prob"] * 100
        self.send_signal()
        logger.info(f"[{self.name}] 🎯 SEÑAL {sig['type']} {sig['target']} ({sig['prob']:.0%})")

    def resolve(self, number: int) -> bool:
        c = COLOR_MAP[number]; p = PARITY_MAP[number]; z = ZONE_MAP[number]
        actual_val = c if self.active_type == "COLOR" else (p if self.active_type == "PARIDAD" else z)
        state = self.seq_states[self.active_type]
        
        won = (actual_val == self.active_target)
        is_zero = (number == 0)

        if is_zero:
            # CERO = Pérdida total de la ficha apostada
            state.loss()
            GLOBAL_STATS.record('EMPATE', state.attempt, number, state.chips, self.active_type, self.name)
            tg_send(
                f"🟠 EMPATE 0 — ZERO — {self.active_type}\n"
                f"🚨 Cero caído. Pérdida de {state.chips} fichas.\n"
                f"🪙 Balance sesión: {state.session_profit} fichas"
            )
            self._check_stats(); self._reset_signal()
            return True

        if won:
            state.win()
            GLOBAL_STATS.record('WIN', state.attempt, number, state.chips, self.active_type, self.name)
            tg_send(
                f"✅ WIN {number} — {self.active_type} {self.active_target}\n"
                f"🎉 ¡Ganaste {state.chips} fichas!\n"
                f"🪙 Balance sesión: {state.session_profit} fichas"
            )
            if state.session_profit >= 5:
                tg_send("🏆 META DE SESIÓN ALCANZADA (+5 Fichas) 🏆")
            self._check_stats(); self._reset_signal()
            return True
        else:
            chips_lost = state.chips
            state.loss()
            if state.attempt == 1:  # Acaba de resetear, significan que perdieron 5 veces
                GLOBAL_STATS.record('LOSS', 5, number, chips_lost, self.active_type, self.name)
                tg_send(
                    f"❌ LOSS TOTAL {number} — {self.active_type}\n"
                    f"🚨 Racha de 5 intentos perdida. Pérdida total acumulada.\n"
                    f"🪙 Balance sesión: {state.session_profit} fichas"
                )
                self._check_stats(); self._reset_signal()
                return True
            else:
                GLOBAL_STATS.record('LOSS', state.attempt - 1, number, chips_lost, self.active_type, self.name)
                if self.active_signal_msg_id:
                    tg_delete(CHAT_ID, self.active_signal_msg_id)
                    self.active_signal_msg_id = None
                # No cerramos señal, esperamos próximo filtro
                self.signal_active = False 
                return False

    def _check_stats(self):
        if not GLOBAL_STATS.should_send(): return
        tg_send(GLOBAL_STATS.get_stats_text())
        GLOBAL_STATS.mark_sent()

    def _reset_signal(self):
        self.signal_active     = False
        self.active_pair       = ()
        self.active_type       = None
        self.active_target     = ""
        self.active_signal_msg_id = None
        self._last_signal_prob = 0.0

    def feed_number(self, number: int, active: bool = False):
        try:
            self._feed_inner(number, active)
        except Exception as e:
            logger.error(f"[{self.name}] Error en feed_number: {e}", exc_info=True)
            self._reset_signal()

    def _feed_inner(self, number: int, active: bool = False):
        c = COLOR_MAP[number]; p = PARITY_MAP[number]; z = ZONE_MAP[number]
        tag = "🟢 ACTIVA" if active else "⚫ pasiva"
        spin_n = len(self.spin_history) + 1

        self._update_state(number)

        if not self.warmup_done:
            self.ws_count += 1
            warmup_tag = f"⏳ warmup {self.ws_count}/{WARMUP_SPINS}"
            if self.ws_count >= WARMUP_SPINS:
                self.warmup_done = True
                warmup_tag = "✅ WARMUP listo"
                tg_send("🟢 <b>Speed Roulette 2</b> — Sistema Híbrido Listo.")
        else:
            warmup_tag = "✔"

        logger.info(
            f"[{self.name}] 🎰 #{spin_n:>4} | {number:>2} {c[:3]} {p[:3]} {z[:3]} "
            f"| {tag} | {warmup_tag} | Cat: {self.current_category} | 🪙{self.seq_states[self.current_category].session_profit}"
        )

# ─── GESTOR DE SESIONES ───────────────────────────────────────────────────────
class SessionManager:
    ARG_UTC_OFFSET = -3

    def __init__(self):
        self.engines: list[RouletteEngine] = [
            RouletteEngine(r["key"], r["name"]) for r in ROULETTES
        ]
        self.current_idx          = 0
        self.session_start        = 0.0
        self.session_active       = False
        self.signals_this_session = 0

        self.prev_start_msg_id: Optional[int] = None
        self.prev_end_msg_id:   Optional[int] = None

        logger.info("[SessionManager] Iniciado — SPEED ROULETTE 2 / 30 min / máx 3 señales / Meta +5 Fichas")

    def _now_arg(self):
        import datetime
        return datetime.datetime.utcnow() + datetime.timedelta(hours=self.ARG_UTC_OFFSET)

    def seconds_to_next_slot(self) -> float:
        import datetime
        now = self._now_arg()
        if now.second <= 5 and now.minute in (0, 30):
            return 0.0
        if now.minute < 30:
            target = now.replace(minute=30, second=0, microsecond=0)
        else:
            target = (now + datetime.timedelta(hours=1)).replace(
                minute=0, second=0, microsecond=0)
        return max(0.0, (target - now).total_seconds())

    def _slot_index_for_now(self) -> int:
        now = self._now_arg()
        slot_of_day = now.hour * 2 + (1 if now.minute >= 30 else 0)
        return slot_of_day % len(self.engines)

    def _start_session(self, initial: bool = False):
        import datetime
        if initial:
            self.current_idx = self._slot_index_for_now()
        else:
            self.current_idx = (self.current_idx + 1) % len(self.engines)

        self.session_start        = time.time()
        self.session_active       = True
        self.signals_this_session = 0
        
        engine  = self.engines[self.current_idx]
        engine.rotate_category() # Cambia de categoría cada sesión
        
        now_str = self._now_arg().strftime("%H:%M")
        end_str = (self._now_arg() + datetime.timedelta(minutes=25)).strftime("%H:%M")
        logger.info(
            f"[SessionManager] 🟢 Sesión iniciada: {engine.name} | Cat: {engine.current_category} | "
            f"{now_str}–{end_str} (ARG)"
        )

        if self.prev_start_msg_id:
            tg_delete(CHAT_ID, self.prev_start_msg_id)
            self.prev_start_msg_id = None

        msg_id = tg_send(f"🔔 SESION INICIADA — {engine.name} — Modo: {engine.current_category} 🔔")
        self.prev_start_msg_id = msg_id

        queue_broadcast({
            "type": "session", "status": "started", "roulette": engine.name,
            "category": engine.current_category, "remaining": SESSION_ACTIVE,
            "signals_count": 0, "max_signals": MAX_SIGNALS,
            "session_profit": 0
        })

    def _end_session(self):
        engine    = self.engines[self.current_idx]
        next_idx  = (self.current_idx + 1) % len(self.engines)
        next_name = self.engines[next_idx].name
        logger.info(f"[SessionManager] ⏸ Sesión terminada: {engine.name} → siguiente: {next_name}")
        self.session_active = False

        if self.prev_end_msg_id:
            tg_delete(CHAT_ID, self.prev_end_msg_id)
            self.prev_end_msg_id = None

        text = (
            f"⏸ SESIÓN CERRADA — {engine.name} ({engine.current_category})\n"
            f"🎰 PRÓXIMA RULETA — {next_name} 🎰\n\n"
            f"💵 ¿COMO OPERAR LAS SEÑALES?\n\n"
            f"1° Op. = 5 FICHAS MÍNIMO\n"
            f"2° Op. = D'ALEMBERT (+1 FICHA)\n"
            f"Máximo 5 intentos por racha\n\n"
            f"🎯 FUNCIONAMIENTO DE LAS SEÑALES 🎯\n\n"
            f"  • Se envían si IA confiable >= 60%\n"
            f"  • Sesión se cierra → Ej: 12:25 o 12:55\n"
            f"  • Meta por sesión: +5 FICHAS\n\n"
            f"♦️ POR SESION SE ENVÍAN 3 SEÑALES MÁXIMAS ♦️"
        )
        msg_id = tg_send_with_button(text, next_name)
        self.prev_end_msg_id = msg_id

        queue_broadcast({
            "type": "session", "status": "ended", "roulette": engine.name,
            "next_roulette": next_name, "remaining": 0,
            "signals_count": self.signals_this_session, "max_signals": MAX_SIGNALS
        })

    async def session_watchdog(self):
        wait = self.seconds_to_next_slot()
        logger.info(f"[SessionManager] ⏳ Esperando {wait/60:.1f} min para el primer slot...")
        await asyncio.sleep(wait)
        self._start_session(initial=True)

        _waiting_signal_since: Optional[float] = None

        while True:
            await asyncio.sleep(1)
            now     = time.time()
            elapsed = now - self.session_start
            engine  = self.engines[self.current_idx]

            if self.session_active:
                if elapsed >= SESSION_ACTIVE:
                    if engine.signal_active:
                        if _waiting_signal_since is None:
                            _waiting_signal_since = now
                        elif now - _waiting_signal_since >= SIGNAL_WAIT_TIMEOUT:
                            if engine.active_signal_msg_id:
                                tg_edit(CHAT_ID, engine.active_signal_msg_id,
                                        engine._build_signal_text() +
                                        "\n\n⚠️ Señal cancelada — tiempo de sesión agotado.")
                            engine._reset_signal()
                            _waiting_signal_since = None
                        else: continue
                    else: _waiting_signal_since = None

                    end_time = time.time()
                    self._end_session()
                    pause_remaining = SESSION_TOTAL - (end_time - self.session_start)
                    if pause_remaining > 0:
                        await asyncio.sleep(pause_remaining)
                    self._start_session()
            else:
                if elapsed >= SESSION_TOTAL:
                    self._start_session()

    def tick_active(self, engine: RouletteEngine, number: int):
        engine.feed_number(number, active=True)

        if not self.session_active: return
        elapsed = time.time() - self.session_start
        if elapsed >= SESSION_ACTIVE: return

        if engine.signal_active:
            finished = engine.resolve(number)
            if finished:
                # Chequear meta de +5 fichas
                if engine.seq_states[engine.current_category].session_profit >= 5:
                    logger.info(f"[SessionManager] 🏆 Meta de +5 fichas alcanzada en {engine.name}. Cerrando sesión anticipadamente.")
                    self._end_session()
                    # Forzar pausa hasta el próximo ciclo
                    pause_remaining = SESSION_TOTAL - (time.time() - self.session_start)
                    if pause_remaining > 0:
                        threading.Timer(pause_remaining, self._start_session).start()
                        self.session_active = False
            return

        state = engine.seq_states[engine.current_category]
        if self.signals_this_session < MAX_SIGNALS and engine.warmup_done and state.session_profit < 5:
            sig = engine.detect_signal()
            if sig:
                logger.info(f"[SessionManager] 🎯 Señal #{self.signals_this_session + 1} detectada en {engine.name}: {sig}")
                engine.iniciar_senal(sig)
                self.signals_this_session += 1

    def tick_passive(self, engine: RouletteEngine, number: int):
        engine.feed_number(number, active=False)

    def on_number(self, ws_key: int, number: int):
        for i, engine in enumerate(self.engines):
            if engine.ws_key != ws_key: continue
            if i == self.current_idx:
                self.tick_active(engine, number)
            else:
                self.tick_passive(engine, number)

            color = COLOR_MAP[number]; p = PARITY_MAP[number]; z = ZONE_MAP[number]
            last20 = engine.spin_history[-20:]
            queue_broadcast({
                "type": "spin", "number": number, "color": color, "parity": p, "zone": z,
                "spin_count": len(engine.spin_history), "warmup_done": engine.warmup_done,
                "last20": [{"number": s["number"], "color": s["color"], "parity": s["parity"], "zone": s["zone"]} for s in last20],
                "signal_active": engine.signal_active, "category": engine.current_category,
                "session_profit": engine.seq_states[engine.current_category].session_profit
            })
            break

    def _advance_session(self):
        self._end_session()
        self.current_idx          = (self.current_idx + 1) % len(self.engines)
        self.session_start        = time.time()
        self.session_active       = True
        self.signals_this_session = 0


# ─── WS READER ────────────────────────────────────────────────────────────────
async def ws_reader(ws_key: int, session_mgr: SessionManager):
    reconnect_delay = 5
    initial_loaded  = False
    seen_ids: set       = set()
    seen_ids_queue: deque = deque(maxlen=200)

    def is_new_id(gid: str) -> bool:
        if not gid or gid in seen_ids: return False
        if len(seen_ids_queue) == seen_ids_queue.maxlen:
            seen_ids.discard(seen_ids_queue[0])
        seen_ids.add(gid); seen_ids_queue.append(gid)
        return True

    while True:
        try:
            async with websockets.connect(
                WS_URL, ping_interval=20, ping_timeout=40, close_timeout=10
            ) as ws:
                await ws.send(json.dumps({
                    "type": "subscribe", "key": ws_key, "casinoId": CASINO_ID
                }))
                logger.info(f"[WS-{ws_key}] ✅ Conectado | polling 1s | dedup activo")
                reconnect_delay = 5

                async def poll_1s():
                    while True:
                        await asyncio.sleep(1)
                        try:
                            await ws.send(json.dumps({
                                "type": "subscribe", "key": ws_key, "casinoId": CASINO_ID
                            }))
                        except Exception: break

                poll_task = asyncio.create_task(poll_1s())

                try:
                    async for raw in ws:
                        try: data = json.loads(raw)
                        except Exception: continue
                        if not isinstance(data, dict): continue

                        results = data.get("last20Results")
                        if results and isinstance(results, list):
                            if not initial_loaded:
                                initial_loaded = True
                                engine = next(
                                    (e for e in session_mgr.engines if e.ws_key == ws_key), None
                                )
                                loaded_count = 0
                                if engine:
                                    for item in reversed(results):
                                        gid_init = str(item.get("gameId", ""))
                                        if gid_init:
                                            if len(seen_ids_queue) == seen_ids_queue.maxlen:
                                                seen_ids.discard(seen_ids_queue[0])
                                            seen_ids.add(gid_init); seen_ids_queue.append(gid_init)
                                        try: n = int(item.get("result", ""))
                                        except (ValueError, TypeError): continue
                                        if 0 <= n <= 36:
                                            engine._update_state(n, persist=False, train_model=True)
                                            loaded_count += 1
                                    engine._train_models()
                                    if not engine.warmup_done and len(engine.spin_history) >= WARMUP_SPINS:
                                        engine.warmup_done = True
                                        engine.ws_count = len(engine.spin_history)
                                    logger.info(f"[WS-{ws_key}] 📦 {loaded_count} giros iniciales cargados")
                                continue

                            latest = results[0]
                            gid = str(latest.get("gameId", ""))
                            if not is_new_id(gid): continue
                            try: n = int(latest.get("result", ""))
                            except (ValueError, TypeError): continue
                            if 0 <= n <= 36:
                                session_mgr.on_number(ws_key, n)
                            continue

                        fallback_gid = str(data.get("gameId", "")).strip()
                        if not fallback_gid:
                            for key in ("result", "number", "outcome", "winningNumber"):
                                if key in data:
                                    fallback_gid = f"{ws_key}_{data[key]}_{int(time.time())}"
                                    break
                        if not fallback_gid or not is_new_id(fallback_gid): continue

                        for key in ("result", "number", "outcome", "winningNumber"):
                            if key in data:
                                try:
                                    n = int(data[key])
                                    if 0 <= n <= 36:
                                        session_mgr.on_number(ws_key, n)
                                except (ValueError, TypeError): pass
                                break

                finally:
                    poll_task.cancel()
                    try: await poll_task
                    except asyncio.CancelledError: pass

        except Exception as e:
            logger.warning(f"[WS-{ws_key}] Desconectado: {e}. Reconectando en {reconnect_delay}s")
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 60)

# ─── WEBSOCKET SERVER PARA HTML ───────────────────────────────────────────────
async def _ws_server_handler(websocket):
    q = asyncio.Queue(maxsize=200)
    _ws_clients.add(q)
    remote = websocket.remote_address if websocket.remote_address else "unknown"
    logger.info(f"[WS-Server] 📡 Cliente conectado desde {remote} | Total: {len(_ws_clients)}")
    try:
        if session_mgr_global and session_mgr_global.engines:
            engine = session_mgr_global.engines[0]
            last20 = engine.spin_history[-20:]
            init_msg = {
                "type": "init", "roulette": engine.name, "category": engine.current_category,
                "spins": last20, "session_profit": engine.seq_states[engine.current_category].session_profit,
                "stats": {"wins": GLOBAL_STATS.wins, "losses": GLOBAL_STATS.losses, "empates": GLOBAL_STATS.zeros}
            }
            await websocket.send(json.dumps(init_msg))

        async def sender():
            while True:
                data = await q.get()
                try: await websocket.send(json.dumps(data))
                except Exception: break

        async def receiver():
            async for msg in websocket: pass

        sender_task = asyncio.create_task(sender())
        receiver_task = asyncio.create_task(receiver())
        try: await asyncio.gather(sender_task, receiver_task, return_exceptions=True)
        finally:
            sender_task.cancel(); receiver_task.cancel()
            try: await asyncio.gather(sender_task, receiver_task, return_exceptions=True)
            except Exception: pass

    except websockets.exceptions.ConnectionClosed: pass
    except Exception as e: logger.debug(f"[WS-Server] Error: {e}")
    finally:
        _ws_clients.discard(q)
        logger.info(f"[WS-Server] 📡 Cliente desconectado | Total: {len(_ws_clients)}")

async def _ws_server_main():
    logger.info(f"[WS-Server] 🌐 Iniciando WebSocket server en 0.0.0.0:{WS_SERVER_PORT}")
    async with websockets.serve(_ws_server_handler, "0.0.0.0", WS_SERVER_PORT, ping_interval=20, ping_timeout=40, close_timeout=10):
        await asyncio.Future()

# ─── FLASK ────────────────────────────────────────────────────────────────────
app = Flask(__name__)
session_mgr_global: Optional[SessionManager] = None

@app.route("/")
def home():
    return jsonify({"status": "ok", "bot": "Speed Roulette 2 — Híbrido Secuencias + ML"})

@app.route("/ping")
def ping():
    return jsonify({"status": "pong", "ts": time.time()})

@app.route("/health")
def health():
    if not session_mgr_global: return jsonify({"status": "initializing"})
    active = session_mgr_global.engines[session_mgr_global.current_idx]
    return jsonify({
        "active_roulette": active.name, "category": active.current_category,
        "session_profit": active.seq_states[active.current_category].session_profit,
        "signal_active": active.signal_active
    })

async def self_ping_loop():
    url = os.environ.get("RENDER_EXTERNAL_URL", "").rstrip("/")
    if not url: return
    await asyncio.sleep(30)
    while True:
        try: urllib.request.urlopen(f"{url}/ping", timeout=15)
        except: pass
        await asyncio.sleep(240)

async def daily_stats_loop():
    import datetime
    while True:
        now_utc = datetime.datetime.utcnow()
        now_arg = now_utc + datetime.timedelta(hours=-3)
        target  = now_arg.replace(hour=12, minute=0, second=0, microsecond=0)
        if now_arg >= target: target += datetime.timedelta(days=1)
        wait_secs = (target - now_arg).total_seconds()
        await asyncio.sleep(wait_secs)
        if session_mgr_global: tg_send_stats(GLOBAL_STATS.get_stats_text())

# ─── BOT COMMANDS ─────────────────────────────────────────────────────────────
@bot.message_handler(commands=['start', 'help'])
def cmd_start(m):
    bot.reply_to(m,
        "<b>🎰 Speed Roulette 2 — Híbrido</b>\n\n"
        "Secuencias + IA (Markov, ML, AMX) >= 60%\n"
        "D'Alembert (5 intentos) | Meta sesión: +5 fichas\n\n"
        "/status — Estado actual\n"
        "/stats — Ver estadísticas\n"
        "/reset — Resetear stats", parse_mode="HTML")

@bot.message_handler(commands=['status'])
def cmd_status(m):
    if not session_mgr_global: return
    active = session_mgr_global.engines[session_mgr_global.current_idx]
    state = active.seq_states[active.current_category]
    bot.reply_to(m,
        f"<b>Ruleta:</b> {active.name}\n"
        f"<b>Categoría:</b> {active.current_category}\n"
        f"<b>Señal activa:</b> {'🟢 Sí' if active.signal_active else '⚪ No'}\n"
        f"<b>Profit Sesión:</b> 🪙 {state.session_profit} fichas\n"
        f"<b>Nivel D'Alembert:</b> {state.chips} fichas\n"
        f"<b>Intento actual:</b> {state.attempt}/5", parse_mode="HTML")

@bot.message_handler(commands=['stats'])
def cmd_stats(m):
    if not session_mgr_global: return
    tg_send_stats(GLOBAL_STATS.get_stats_text())

@bot.message_handler(commands=['reset'])
def cmd_reset(m):
    if not session_mgr_global: return
    global GLOBAL_STATS
    GLOBAL_STATS = GlobalStats()
    for e in session_mgr_global.engines:
        for s in e.seq_states.values(): s.session_profit = 0; s.chips = 5; s.attempt = 1
    bot.reply_to(m, "🔄 <b>Resetado — Stats y Fichas a 0</b>", parse_mode="HTML")

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)

# ─── MAIN ─────────────────────────────────────────────────────────────────────
async def main():
    global session_mgr_global
    session_mgr_global = SessionManager()

    threading.Thread(target=run_flask, daemon=True).start()
    threading.Thread(target=lambda: bot.polling(none_stop=True, interval=1, timeout=30), daemon=True).start()

    await asyncio.sleep(5)

    tasks = [
        asyncio.create_task(session_mgr_global.session_watchdog()),
        asyncio.create_task(daily_stats_loop()),
        asyncio.create_task(self_ping_loop()),
        asyncio.create_task(_ws_server_main()),
    ]
    for r in ROULETTES:
        tasks.append(asyncio.create_task(ws_reader(r["key"], session_mgr_global)))

    logger.info(f"[Main] 🎰 Bot Híbrido iniciado — WS server puerto {WS_SERVER_PORT}")
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    try: asyncio.run(main())
    except KeyboardInterrupt: logger.info("[Main] 🛑 Bot detenido")
    except Exception as e: logger.error(f"[Main] 💥 Error fatal: {e}"); raise
