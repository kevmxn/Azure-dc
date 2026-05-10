#!/usr/bin/env python3
"""
Russian Roulette — Bot de señales para Docenas y Columnas exclusivamente
Sistema AMX · Ensemble ML (Naive Bayes + SGD + Markov Suavizado) + EMA
  - Optimizado para Render (RAM constante mediante Online Learning)
  - Precalentamiento con russian-azure.db
  - Persistencia SQLite 24/7
  - Análisis últimos 5 resultados y validación histórica
  - Umbral 80%
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
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Tuple

import numpy as np
from sklearn.naive_bayes import MultinomialNB
from sklearn.linear_model import SGDClassifier
from sklearn.preprocessing import LabelBinarizer

import telebot
import websockets
from flask import Flask, jsonify
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ─── LOGGING ──────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format='%(asctime)s [RussianDC] %(levelname)s %(message)s')
logger = logging.getLogger("RussianDC")
for _ln in ['werkzeug', 'flask.app', 'flask', 'urllib3']:
    logging.getLogger(_ln).setLevel(logging.ERROR)

# ─── TELEGRAM ─────────────────────────────────────────────────────────────────
TOKEN   = "8714149875:AAFJugWY0E5A4C0lrxn2bMcKsQEieqo_t5M"
CHAT_ID = -1003835197023
THREAD_ID = 8344

_session = requests.Session()
_retry = Retry(total=5, backoff_factor=1.5, status_forcelist=[429, 500, 502, 503, 504],
               allowed_methods=["GET","POST"], raise_on_status=False)
_session.mount("https://", HTTPAdapter(max_retries=_retry, pool_connections=10, pool_maxsize=20))
_session.mount("http://",  HTTPAdapter(max_retries=_retry, pool_connections=10, pool_maxsize=20))
bot = telebot.TeleBot(TOKEN, threaded=False); bot.session = _session

# ─── CONSTANTES ───────────────────────────────────────────────────────────────
WS_URL    = "wss://dga.pragmaticplaylive.net/ws"
CASINO_ID = "ppcjd00000007254"
WS_KEY    = 221
LIVE_DB   = "russian_live.db"
AZURE_DB  = "russian-azure.db"
AZURE_TABLE = "russian_roulette"

BASE_BET     = 0.50
MAX_ATTEMPTS = 2
WARMUP_SPINS = 25
MIN_PROB     = 0.80

REAL_COLOR_MAP: dict[int, str] = {
    0:"VERDE",1:"ROJO",2:"NEGRO",3:"ROJO",4:"NEGRO",5:"ROJO",6:"NEGRO",
    7:"ROJO",8:"NEGRO",9:"ROJO",10:"NEGRO",11:"NEGRO",12:"ROJO",13:"NEGRO",
    14:"ROJO",15:"NEGRO",16:"ROJO",17:"NEGRO",18:"ROJO",19:"ROJO",20:"NEGRO",
    21:"ROJO",22:"NEGRO",23:"ROJO",24:"NEGRO",25:"ROJO",26:"NEGRO",27:"ROJO",
    28:"NEGRO",29:"NEGRO",30:"ROJO",31:"NEGRO",32:"ROJO",33:"NEGRO",34:"ROJO",
    35:"NEGRO",36:"ROJO",
}
COLOR_EMOJI = {"ROJO":"🔴","NEGRO":"⚫️","VERDE":"🟢"}

def get_dozen(n: int) -> int:
    if n == 0: return 0
    return (n - 1) // 12 + 1

def get_column(n: int) -> int:
    if n == 0: return 0
    return ((n - 1) % 3) + 1

# ─── SQLITE ───────────────────────────────────────────────────────────────────
def _get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(LIVE_DB, check_same_thread=False)
    conn.execute("""CREATE TABLE IF NOT EXISTS live_spins ( id INTEGER PRIMARY KEY AUTOINCREMENT, number INTEGER NOT NULL, ts INTEGER NOT NULL )""")
    conn.commit()
    return conn

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
            time.sleep(delay); delay = min(delay*2, 60)
    return None

def tg_send(text: str) -> Optional[int]:
    kwargs = dict(chat_id=CHAT_ID, text=text, parse_mode="HTML")
    if THREAD_ID: kwargs["message_thread_id"] = THREAD_ID
    msg = _tg_call(bot.send_message, **kwargs)
    return msg.message_id if msg else None

def tg_delete(msg_id: int): _tg_call(bot.delete_message, chat_id=CHAT_ID, message_id=msg_id)

# ─── EMA ──────────────────────────────────────────────────────────────────────
def calc_ema(data: list, period: int) -> list:
    if len(data) < period: return [None]*len(data)
    mult = 2 / (period + 1); out = [None]*(period-1); prev = sum(data[:period]) / period; out.append(prev)
    for v in data[period:]: prev = v*mult + prev*(1-mult); out.append(prev)
    return out

def ema_signal(levels: list, mode: str = "moderado") -> bool:
    if len(levels) < 20: return False
    e4, e8, e20 = calc_ema(levels, 4), calc_ema(levels, 8), calc_ema(levels, 20)
    li = len(levels) - 1
    if any(v is None for v in [e4[li], e8[li], e20[li]]): return False
    cur = levels[li]; ce4, ce8, ce20 = e4[li], e8[li], e20[li]
    pe4  = e4[li-1]  if li > 0 and e4[li-1]  is not None else ce4
    pe8  = e8[li-1]  if li > 0 and e8[li-1]  is not None else ce8
    pe20 = e20[li-1] if li > 0 and e20[li-1] is not None else ce20
    if mode == "tendencia":
        return ((pe4 <= pe20 and ce4 > ce20) or (cur > ce4 and cur > ce8 and cur > ce20))
    else:
        v_pattern = False
        if len(levels) >= 3: a, b, c = levels[-3], levels[-2], levels[-1]; v_pattern = (b < a) and (b < c) and (c > a)
        return ((pe4 <= pe8 and ce4 > ce8) or (pe8 <= pe20 and ce8 > ce20) or (cur > ce4 and cur > ce8) or v_pattern)

# ─── MARKOV SUAVIZADO (Laplace Smoothing) ─────────────────────────────────────
class SmoothedMarkovPredictor:
    def __init__(self, window: int = 60, order: int = 2):
        self.window = window; self.order = order; self.transition_counts: dict = {}

    def update(self, sequence: list):
        self.transition_counts = defaultdict(lambda: defaultdict(int))
        recent = sequence[-self.window:]
        if len(recent) < self.order + 1: return
        for i in range(len(recent) - self.order):
            state = tuple(recent[i:i+self.order]); nxt = recent[i+self.order]
            self.transition_counts[state][nxt] += 1

    def predict(self, sequence: list) -> Optional[dict]:
        if len(sequence) < self.order: return None
        state = tuple(sequence[-self.order:]); counts = dict(self.transition_counts.get(state, {})); total = sum(counts.values())
        if total < 5: return None
        # Laplace Smoothing (Alpha=1) para evitar probabilidades 0
        alpha = 1.0; vocab_size = 3
        probs = {k: (v + alpha) / (total + alpha * vocab_size) for k,v in counts.items()}
        # Rellenar las que no aparecieron
        for c in [1,2,3]:
            if c not in probs: probs[c] = alpha / (total + alpha * vocab_size)
        return probs

# ─── ENSEMBLE ML (NB + SGD) ──────────────────────────────────────────────────
class OnlineEnsemblePredictor:
    """
    Sistema de Ensemble Online. Consume RAM constante (O(1)).
    - Naive Bayes: Captura patrones de secuencia generalizados.
    - SGD: Se adapta rápidamente a cambios de sesgo en la rueda.
    """
    WINDOW = 5
    CLASSES = [1, 2, 3]

    def __init__(self):
        self.mnb = MultinomialNB(alpha=1.0, class_prior=[0.333, 0.333, 0.333])
        self.sgd = SGDClassifier(loss='log_loss', learning_rate='adaptive', eta0=0.01, penalty='l2')
        self.trained = False
        self.sample_count = 0

    def _extract_features(self, history: list) -> Optional[list]:
        """One-Hot Encoding deslizante de los últimos 5 giros + Freq Ratio"""
        h = [x for x in history if x != 0]
        if len(h) < self.WINDOW: return None
        
        window = h[-self.WINDOW:]
        # One-hot encoding (3 clases * 5 window = 15 features)
        features = []
        for val in window:
            vec = [0, 0, 0]
            if val in self.CLASSES: vec[val-1] = 1
            features.extend(vec)
            
        # Features de Frecuencia Reciente (últimos 20 giros)
        recent = h[-20:]
        total = len(recent)
        freqs = [recent.count(c)/total if total > 0 else 0.333 for c in self.CLASSES]
        features.extend(freqs)
        
        return features

    def partial_train(self, history: list, target: int):
        """Entrenamiento incremental en tiempo real"""
        feats = self._extract_features(history[:-1]) # Usamos el historial ANTES del target
        if feats is None: return
        
        X = np.array(feats).reshape(1, -1)
        y = np.array([target])
        
        if not self.trained:
            self.mnb.partial_fit(X, y, classes=self.CLASSES)
            self.sgd.partial_fit(X, y, classes=self.CLASSES)
            self.trained = True
        else:
            self.mnb.partial_fit(X, y)
            self.sgd.partial_fit(X, y)
            
        self.sample_count += 1

    def predict(self, history: list) -> Optional[dict]:
        if not self.trained: return None
        feats = self._extract_features(history)
        if feats is None: return None
        
        X = np.array(feats).reshape(1, -1)
        
        try:
            # Probabilidades de los 3 modelos base
            nb_probs = self.mnb.predict_proba(X)[0]
            sgd_probs = self.sgd.predict_proba(X)[0]
            
            # Ensemble Promedio (Se podrían ajustar pesos dinámicamente)
            final_probs = (0.5 * nb_probs + 0.5 * sgd_probs)
            
            return {c+1: float(p) for c, p in enumerate(final_probs)}
        except Exception as e:
            logger.debug(f"Ensemble predict error: {e}")
            return None

# ─── DETAILED STATS ───────────────────────────────────────────────────────────
class DetailedStats:
    def __init__(self):
        self.total = self.wins_a1 = self.wins_a2 = self.losses = 0
        self.last_stats_at = 0; self.batch_start_bankroll: Optional[float] = None
        self.batch_w1 = self.batch_w2 = self.batch_l = 0
        self.last_daily_date = ""; self.daily_start_bankroll: Optional[float] = None
        self.daily_total = self.daily_w1 = self.daily_w2 = self.daily_l = 0

    def record(self, attempt: int, won: bool, bankroll: float):
        self.total += 1; self.daily_total += 1
        if won: 
            if attempt == 1: self.wins_a1 += 1; self.daily_w1 += 1
            else: self.wins_a2 += 1; self.daily_w2 += 1
        else: self.losses += 1; self.daily_l += 1
        if self.daily_start_bankroll is None: self.daily_start_bankroll = bankroll

    def should_send(self) -> bool: return (self.total - self.last_stats_at) >= 20
    def mark_sent(self, bankroll: float):
        self.last_stats_at = self.total; self.batch_start_bankroll = bankroll
        self.batch_w1 = self.wins_a1; self.batch_w2 = self.wins_a2; self.batch_l = self.losses

    def batch_stats(self, bankroll: float) -> dict:
        n = self.total - self.last_stats_at; w1 = self.wins_a1 - self.batch_w1; w2 = self.wins_a2 - self.batch_w2; l = self.losses - self.batch_l; w = w1 + w2
        bk = round(bankroll - self.batch_start_bankroll, 2) if self.batch_start_bankroll is not None else 0.0
        return {"n":n,"w1":w1,"w2":w2,"l":l,"w":w,"eff":round(w/n*100,1) if n else 0.0,"bk":bk}

    def daily_stats(self, bankroll: float) -> dict:
        n = self.daily_total; w = self.daily_w1 + self.daily_w2
        bk = round(bankroll - self.daily_start_bankroll, 2) if self.daily_start_bankroll is not None else 0.0
        return {"n":n,"w1":self.daily_w1,"w2":self.daily_w2,"l":self.daily_l,"w":w,"eff":round(w/n*100,1) if n else 0.0,"bk":bk}

    def reset_daily(self, date_str: str, bankroll: float):
        self.last_daily_date = date_str; self.daily_start_bankroll = bankroll
        self.daily_total = self.daily_w1 = self.daily_w2 = self.daily_l = 0

# ─── ROULETTE ENGINE ──────────────────────────────────────────────────────────
class RussianRouletteEngine:
    def __init__(self):
        self.spin_history: list = []
        self.dozen_seq: list = []; self.column_seq: list = []
        self.d_levels: dict[int, list] = {1:[], 2:[], 3:[]}; self.c_levels: dict[int, list] = {1:[], 2:[], 3:[]}

        # Motores Optimizados
        self.markov_d = SmoothedMarkovPredictor(window=60, order=2)
        self.markov_c = SmoothedMarkovPredictor(window=60, order=2)
        self.ensemble_d = OnlineEnsemblePredictor()
        self.ensemble_c = OnlineEnsemblePredictor()

        # Estado y Martingala
        self.signal_active: bool = False; self.active_type: Optional[str] = None
        self.active_pair: tuple = (); self.active_missing: str = ""
        self.attempts_left: int = MAX_ATTEMPTS; self.signal_msg_ids: list = []
        self.bet_level = 1; self.cum_loss = 0.0; self.bankroll: float = 0.0
        self.trigger_number: int = 0; self.trigger_color: str = ""

        # DB y Precalentamiento
        self.stats = DetailedStats(); self._db = _get_db()
        live_loaded = self._load_live_history()
        self._pretrain_from_db(AZURE_DB, AZURE_TABLE)
        
        self.ws_count: int = live_loaded
        self.warmup_done: bool = False 
        self.last_game_id: Optional[str] = None

    def current_bet(self) -> float:
        needed = self.cum_loss + BASE_BET; return round(max(needed, BASE_BET), 2)

    def _pretrain_from_db(self, db_path: str, table_name: str):
        if not os.path.exists(db_path): return
        spins = []
        try:
            pattern = re.compile(rf'INSERT INTO "{table_name}" VALUES \(\d+,(\d+),')
            with open(db_path, "r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    m = pattern.search(line)
                    if m: spins.append(int(m.group(1)))
        except: return
        if not spins: return
        for n in spins: self._update_state(n, persist=False)
        logger.info(f"[RussianDC] 🔥 Pre-entrenado con {len(spins)} giros")

    def _load_live_history(self) -> int:
        try: rows = self._db.execute("SELECT number FROM live_spins ORDER BY id ASC").fetchall()
        except: return 0
        for (n,) in rows: self._update_state(n, persist=False)
        logger.info(f"[RussianDC] ✅ {len(rows)} giros en vivo cargados")
        return len(rows)

    def _persist(self, number: int):
        try: self._db.execute("INSERT INTO live_spins(number,ts) VALUES(?,?)", (number, int(time.time()))); self._db.commit()
        except: pass

    def _update_state(self, number: int, persist: bool = True):
        color = REAL_COLOR_MAP.get(number, "VERDE"); d = get_dozen(number); c = get_column(number)
        self.spin_history.append({"number":number,"color":color})
        
        if d != 0:
            self.dozen_seq.append(d)
            for dd in (1,2,3):
                delta = 1 if d == dd else -1; prev = self.d_levels[dd][-1] if self.d_levels[dd] else 0
                self.d_levels[dd].append(prev + delta)
            
            # Entrenamiento Online
            self.ensemble_d.partial_train(self.dozen_seq, d)
            self.markov_d.update(self.dozen_seq)

        if c != 0:
            self.column_seq.append(c)
            for cc in (1,2,3):
                delta = 1 if c == cc else -1; prev = self.c_levels[cc][-1] if self.c_levels[cc] else 0
                self.c_levels[cc].append(prev + delta)
            
            # Entrenamiento Online
            self.ensemble_c.partial_train(self.column_seq, c)
            self.markov_c.update(self.column_seq)

        if persist: self._persist(number)

    def _classify_last5(self) -> Optional[Dict]:
        if len(self.spin_history) < 5: return None
        last5 = self.spin_history[-5:]
        if any(s["number"] == 0 for s in last5): return None
        d_set = set(f"D{get_dozen(s['number'])}" for s in last5)
        c_set = set(f"C{get_column(s['number'])}" for s in last5)
        res = {"dozen_pattern": None, "column_pattern": None}
        if len(d_set) == 2: missing_d = ({"D1","D2","D3"} - d_set).pop(); res["dozen_pattern"] = {"present": list(d_set), "missing": missing_d}
        if len(c_set) == 2: missing_c = ({"C1","C2","C3"} - c_set).pop(); res["column_pattern"] = {"present": list(c_set), "missing": missing_c}
        if not res["dozen_pattern"] and not res["column_pattern"]: return None
        return res

    def _validate_historical(self, cat_type: str, pair_set: set) -> bool:
        seq = self.dozen_seq if cat_type == "DOCENA" else self.column_seq
        if len(seq) < 10: return False
        count = 0
        for i in range(len(seq) - 4):
            if set(seq[i:i+5]) == pair_set: count += 1
        return count >= 2

    def _unified_prob(self, cat_type: str, missing_num: int) -> float:
        mk = self.markov_d if cat_type == "DOCENA" else self.markov_c
        ens = self.ensemble_d if cat_type == "DOCENA" else self.ensemble_c
        hist = self.dozen_seq if cat_type == "DOCENA" else self.column_seq
        levels = (self.d_levels if cat_type == "DOCENA" else self.c_levels).get(missing_num, [])

        # 1. Markov Suavizado (30%)
        m_p = mk.predict(hist).get(missing_num, 1/3) if mk.predict(hist) else 1/3
        
        # 2. Ensemble NB+SGD (60%)
        ens_p = ens.predict(hist).get(missing_num, 1/3) if ens.predict(hist) else 1/3
        
        # 3. Prior Bayesiano (10%)
        prior = 1/3

        # Fusión
        raw_missing = 0.30*m_p + 0.60*ens_p + 0.10*prior
        
        # AMX Trend Factor
        if len(levels) >= 20:
            if ema_signal(levels, "tendencia"): raw_missing *= 0.95  # Si la EMA indica tendencia en contra del ausente, bajamos su prob
            elif ema_signal(levels, "moderado"): raw_missing *= 0.98

        pair_prob = 1.0 - min(raw_missing, 0.99)
        return pair_prob

    def _detect_signal(self) -> Optional[dict]:
        classification = self._classify_last5()
        if not classification: return None
        candidates = []

        if classification["dozen_pattern"]:
            dp = classification["dozen_pattern"]; pair_set = set(dp["present"])
            if self._validate_historical("DOCENA", pair_set):
                missing_num = int(dp["missing"][1])
                pp = self._unified_prob("DOCENA", missing_num)
                top2 = sorted([1,2,3], key=lambda x: self._unified_prob("DOCENA", x), reverse=True)[:2]
                if set([f"D{top2[0]}", f"D{top2[1]}"]) == pair_set and pp >= MIN_PROB:
                    candidates.append({"type":"DOCENA", "pair":(f"D{top2[0]}", f"D{top2[1]}"), "missing":dp["missing"], "prob":pp})

        if classification["column_pattern"]:
            cp = classification["column_pattern"]; pair_set = set(cp["present"])
            if self._validate_historical("COLUMNA", pair_set):
                missing_num = int(cp["missing"][1])
                pp = self._unified_prob("COLUMNA", missing_num)
                top2 = sorted([1,2,3], key=lambda x: self._unified_prob("COLUMNA", x), reverse=True)[:2]
                if set([f"C{top2[0]}", f"C{top2[1]}"]) == pair_set and pp >= MIN_PROB:
                    candidates.append({"type":"COLUMNA", "pair":(f"C{top2[0]}", f"C{top2[1]}"), "missing":cp["missing"], "prob":pp})

        if not candidates: return None
        return max(candidates, key=lambda x: x["prob"])

    # ── Mensajes y Resolución ─────────────────────────────────────────────────
    def _fmt_pair_display(self, pair: tuple) -> str:
        nums = sorted([p[1:].zfill(2) for p in pair]); return f"{nums[0]} y {nums[1]}"

    def _build_signal_text(self, attempt: int, prob: float) -> str:
        bet = self.current_bet(); c_emoji = COLOR_EMOJI.get(self.trigger_color, "")
        pair_disp = self._fmt_pair_display(self.active_pair)
        line1 = f"❄️ ENTRAR EN DOCENAS: {pair_disp}" if self.active_type == "DOCENA" else f"☢ ENTRAR EN COLUMNAS: {pair_disp}"
        line2 = f"♦️ APUESTA EN DOCENA: {bet:.2f}" if self.active_type == "DOCENA" else f"♦️ APUESTA EN COLUMNA: {bet:.2f}"
        return (f"🎯 <b>SEÑAL CONFIRMADA</b> 🎯\n\n🎰 <b>RUSSIAN ROULETTE</b>\n"
                f"👉 ÚLTIMO NÚMERO: {self.trigger_number} {self.trigger_color} {c_emoji}\n"
                f"{line1}\n{line2}\n\n♻️ Intento {attempt}/{MAX_ATTEMPTS} <i>[{int(prob*100)}%]</i>")

    def _send_signal(self, attempt: int, prob: float):
        for mid in self.signal_msg_ids: tg_delete(mid)
        self.signal_msg_ids = []
        msg_id = tg_send(self._build_signal_text(attempt, prob))
        if msg_id: self.signal_msg_ids.append(msg_id)

    def _resolve(self, number: int, color: str):
        d, c = get_dozen(number), get_column(number)
        won = (self.active_type == "DOCENA" and d != 0 and f"D{d}" in self.active_pair) or \
              (self.active_type == "COLUMNA" and c != 0 and f"C{c}" in self.active_pair)

        if won:
            bet = self.current_bet(); self.bankroll = round(self.bankroll + bet, 2)
            self.bet_level = 1; self.cum_loss = 0.0
            attempt = MAX_ATTEMPTS - self.attempts_left + 1
            for mid in self.signal_msg_ids: tg_delete(mid)
            self.signal_msg_ids = []
            val = f"D{d}" if self.active_type == "DOCENA" else f"C{c}"
            tg_send(f"✅ <b>¡GREEN {val}!</b> -- {number} {color} {COLOR_EMOJI.get(color,'')}\n💰 <i>BANKROLL: {self.bankroll:.2f} usd</i>")
            self.stats.record(attempt, True, self.bankroll); self._check_daily_report(); self._check_stats(); self._reset_signal()
        else:
            loss_amt = self.current_bet() * 2; self.bankroll = round(self.bankroll - loss_amt, 2)
            self.cum_loss = round(self.cum_loss + loss_amt, 2)
            self.attempts_left -= 1
            if self.attempts_left > 0:
                if self.bet_level < 6: self.bet_level += 1
                else: self.bet_level = 1; self.cum_loss = 0.0
                self._send_signal(2, 0.80)
            else:
                for mid in self.signal_msg_ids: tg_delete(mid)
                self.signal_msg_ids = []
                val = f"D{d}" if self.active_type == "DOCENA" else f"C{c}"
                tg_send(f"❌ <b>¡LOSS {val}!</b> -- {number} {color}\n💰 <i>BANKROLL: {self.bankroll:.2f} usd</i>\n📈 <i>NIVEL: {self.bet_level}/6</i>")
                self.stats.record(0, False, self.bankroll); self._check_daily_report(); self._check_stats(); self._reset_signal()

    def _reset_signal(self): self.signal_active = False; self.active_pair = (); self.attempts_left = MAX_ATTEMPTS; self.signal_msg_ids = []

    def process_number(self, number: int):
        try: self._process_inner(number)
        except Exception as e: logger.error(f"Error: {e}", exc_info=True); self._reset_signal()

    def _process_inner(self, number: int):
        color = REAL_COLOR_MAP.get(number, "VERDE")
        self._update_state(number)
        if not self.warmup_done:
            self.ws_count += 1
            if self.ws_count < WARMUP_SPINS: return
            self.warmup_done = True; tg_send("🟢 <b>Russian Roulette DC</b> — IA Optimizada lista.")
        if self.signal_active: self._resolve(number, color)
        else:
            sig = self._detect_signal()
            if sig:
                self.signal_active = True; self.active_type = sig["type"]
                self.active_pair = sig["pair"]; self.active_missing = sig["missing"]
                self.attempts_left = MAX_ATTEMPTS; self.trigger_number = number; self.trigger_color = color
                self._send_signal(1, sig["prob"])

    # ── Stats y Reportes ──────────────────────────────────────────────────────
    def _check_stats(self):
        if not self.stats.should_send(): return
        s20 = self.stats.batch_stats(self.bankroll); s24 = self.stats.daily_stats(self.bankroll)
        self.stats.mark_sent(self.bankroll)
        text = "📊 <b>ESTADÍSTICAS DC</b>\n\n"
        if s20: text += f"👉🏼 <b>ÚLTIMAS {s20['n']} SEÑALES</b>\n🈯️ T:{s20['n']} 📈 E:{s20['eff']}%\n💰 Bankroll: {s20['bk']:+.2f} usd\n\n"
        if s24 and s24['n'] > 0: text += f"👉🏼 <b>24 HORAS</b>\n🈯️ T:{s24['n']} 📈 E:{s24['eff']}%\n💰 Bankroll 24h: {s24['bk']:+.2f} usd"
        tg_send(text)

    def _check_daily_report(self):
        tz_ar = timezone(timedelta(hours=-3)); now_ar = datetime.now(tz_ar)
        if now_ar.hour < 12: return
        today = now_ar.strftime("%Y-%m-%d")
        if self.stats.last_daily_date == today: return
        sd = self.stats.daily_stats(self.bankroll)
        if sd["n"] == 0: self.stats.reset_daily(today, self.bankroll); return
        tg_send(f"📅 <b>REPORTE DIARIO — {now_ar.strftime('%d/%m/%Y')}</b>\n🕛 12:00 hs (AR)\n\n🈯️ Total: {sd['n']}\n📈 Efic: {sd['eff']}%\n💰 Balance: {sd['bk']:+.2f} usd")
        self.stats.reset_daily(today, self.bankroll)

    # ── WebSocket ─────────────────────────────────────────────────────────────
    async def run_ws(self):
        reconnect_delay = 5
        while True:
            try:
                async with websockets.connect(WS_URL, ping_interval=30, ping_timeout=60, close_timeout=10) as ws:
                    await ws.send(json.dumps({"type":"subscribe","key":WS_KEY,"casinoId":CASINO_ID}))
                    logger.info(f"✅ WS conectado key={WS_KEY}"); reconnect_delay = 5
                    async for raw in ws:
                        try: data = json.loads(raw)
                        except: continue
                        if not isinstance(data, dict): continue
                        results = data.get("last20Results")
                        if results and isinstance(results, list):
                            latest = results[0]; game_id = str(latest.get("gameId",""))
                            if game_id == self.last_game_id: continue
                            self.last_game_id = game_id
                            try: number = int(latest.get("result",""))
                            except: continue
                            if 0 <= number <= 36: self.process_number(number)
                            continue
                        for key in ("result","number","outcome","winningNumber"):
                            if key in data:
                                try:
                                    n = int(data[key])
                                    if 0 <= n <= 36: self.process_number(n)
                                except: pass; break
            except Exception as e:
                logger.warning(f"WS desconectado: {e}. Recon en {reconnect_delay}s")
                await asyncio.sleep(reconnect_delay); reconnect_delay = min(reconnect_delay*2, 60)

# ─── FLASK & SELF-PING ───────────────────────────────────────────────────────
app = Flask(__name__); engine: Optional[RussianRouletteEngine] = None
@app.route("/")
def home(): return jsonify({"status": "ok", "bot": "Russian DC AMX Optimized"})
@app.route("/ping")
def ping(): return jsonify({"status":"pong","ts":time.time()})
@app.route("/health")
def health(): return jsonify({"status":"healthy","warmup": engine.warmup_done if engine else False})

async def self_ping_loop():
    url = os.environ.get("RENDER_EXTERNAL_URL","").rstrip("/")
    if not url: return
    ping_url = f"{url}/ping"; await asyncio.sleep(30)
    while True:
        try: urllib.request.urlopen(ping_url, timeout=15)
        except: pass
        await asyncio.sleep(240)

# ─── TELEGRAM HANDLERS ────────────────────────────────────────────────────────
@bot.message_handler(commands=['start','help'])
def cmd_start(message):
    bot.reply_to(message, "<b>🎰 Russian DC Bot (Optimized ML)</b>\n\nMotor: NB + SGD + Markov + AMX\nUmbral: 80%\n\n/status\n/stats\n/reset", parse_mode="HTML")

@bot.message_handler(commands=['status'])
def cmd_status(message):
    if not engine: return
    st = f"🟢 Señal activa: {engine.active_pair}" if engine.signal_active else "⚪ Idle"
    bot.reply_to(message, f"<b>📊 ESTADO</b>\n\nEstado: {st}\nGiros: {len(engine.spin_history)}\nNivel: {engine.bet_level}/6\nBankroll: {engine.bankroll:.2f} usd", parse_mode="HTML")

@bot.message_handler(commands=['stats'])
def cmd_stats(message):
    if not engine: return
    sd = engine.stats.daily_stats(engine.bankroll)
    bot.reply_to(message, f"📊 <b>ESTADÍSTICAS HOY</b>\n\n🈯️ T:{sd['n']} 📈 E:{sd['eff']}%\n💰 {sd['bk']:+.2f} usd", parse_mode="HTML")

@bot.message_handler(commands=['reset'])
def cmd_reset(message):
    if engine: engine.stats = DetailedStats(); engine.bet_level = 1; engine.cum_loss = 0.0
    bot.reply_to(message,"🔄 <b>Resetado</b>",parse_mode="HTML")

# ─── MAIN ─────────────────────────────────────────────────────────────────────
def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)

async def main():
    global engine
    engine = RussianRouletteEngine()
    tasks = [asyncio.create_task(engine.run_ws()), asyncio.create_task(self_ping_loop())]
    def _poll(): bot.polling(none_stop=True, interval=1, timeout=30)
    threading.Thread(target=_poll, daemon=True).start()
    logger.info("🎰 Russian Roulette DC Optimized Bot iniciado")
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    threading.Thread(target=run_flask, daemon=True).start()
    try: asyncio.run(main())
    except KeyboardInterrupt: logger.info("Bot detenido.")
