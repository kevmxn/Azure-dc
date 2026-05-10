
#!/usr/bin/env python3
"""
Russian Roulette — Bot de señales para Docenas y Columnas exclusivamente
Sistema AMX · Markov + Decision Tree ML + Patrones(5) + EMA 4/8/20
  - Precalentamiento con russian-azure.db
  - Persistencia SQLite 24/7 (Muestras DT y Markov guardadas)
  - Análisis últimos 5 resultados: si pertenecen a 2 docenas o 2 columnas
  - Validación histórica de secuencia
  - Predice 6° giro: Markov + ML + Patrones(5) + AMX (Umbral 80%)
  - Self-ping cada 4 min (anti-sleep Render)
  - Stats: cada 20 señales + 24h (actualiza a las 12:00 AR)
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
from typing import Optional, Dict

import numpy as np
from sklearn.tree import DecisionTreeClassifier
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
AZURE_DB  = "russian-azure.db"  # Archivo de precalentamiento
AZURE_TABLE = "russian_roulette"

BASE_BET     = 0.50
MAX_ATTEMPTS = 2
WARMUP_SPINS = 25
MIN_PROB     = 0.80

# ─── MAPAS ────────────────────────────────────────────────────────────────────
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
    conn.execute("""CREATE TABLE IF NOT EXISTS markov_counts ( cat TEXT NOT NULL, pattern TEXT NOT NULL, result INTEGER NOT NULL, count INTEGER NOT NULL DEFAULT 1, PRIMARY KEY (cat, pattern, result) )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS dt_samples ( cat TEXT NOT NULL, id INTEGER PRIMARY KEY AUTOINCREMENT, features TEXT NOT NULL, target INTEGER NOT NULL )""")
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

# ─── MARKOV ───────────────────────────────────────────────────────────────────
class MarkovPredictor:
    def __init__(self):
        self.counts1: dict = defaultdict(lambda: defaultdict(int))
        self.counts2: dict = defaultdict(lambda: defaultdict(int))

    def update(self, history: list):
        h = [x for x in history if x != 0]
        for i in range(len(h) - 1): self.counts1[(h[i],)][h[i+1]] += 1
        for i in range(len(h) - 2): self.counts2[(h[i], h[i+1])][h[i+2]] += 1

    def predict(self, history: list) -> Optional[dict]:
        h = [x for x in history if x != 0]
        if len(h) < 1: return None
        results: dict = {}
        if len(h) >= 2:
            pat2, c2 = (h[-2], h[-1]), dict(self.counts2.get(pat2, {})); tot2 = sum(c2.values())
            if tot2 >= 3:
                for k,v in c2.items(): results[k] = 0.6 * v / tot2
        pat1, c1 = (h[-1],), dict(self.counts1.get(pat1, {})); tot1 = sum(c1.values())
        if tot1 >= 2:
            w = 0.4 if results else 1.0
            for k,v in c1.items(): results[k] = results.get(k, 0) + w * v / tot1
        if not results: return None
        total = sum(results.values())
        return {k: v/total for k,v in results.items()} if total > 0 else None

# ─── DECISION TREE ML ─────────────────────────────────────────────────────────
class DecisionTreePredictor:
    WINDOW = 5
    def __init__(self):
        self.clf = DecisionTreeClassifier(max_depth=10, min_samples_split=4, min_samples_leaf=2)
        self.X, self.y = [], []; self.trained = False

    def _get_freq(self, history: list, window: int = 15) -> dict:
        h = [x for x in history if x != 0][-window:]
        if not h: return {1:1/3, 2:1/3, 3:1/3}
        total = len(h); return {c: h.count(c)/total for c in (1,2,3)}

    def _make_features(self, history: list, levels: list, streak: int) -> list:
        h = [x for x in history if x != 0]
        window = h[-self.WINDOW:] if len(h) >= self.WINDOW else [0]*(self.WINDOW - len(h)) + h
        lv_prev = levels[:-1] if len(levels) > 1 else levels
        e4, e8, e20 = calc_ema(lv_prev, 4), calc_ema(lv_prev, 8), calc_ema(lv_prev, 20)
        v4 = e4[-1] if e4 and e4[-1] is not None else 0.0
        v8 = e8[-1] if e8 and e8[-1] is not None else 0.0
        v20 = e20[-1] if e20 and e20[-1] is not None else 0.0
        freq = self._get_freq(h)
        return window + [round(v4,3), round(v8,3), round(v20,3), streak] + [round(freq.get(1,1/3),3), round(freq.get(2,1/3),3), round(freq.get(3,1/3),3)]

    def add_sample_return(self, history: list, levels: list, streak: int) -> Optional[dict]:
        h = [x for x in history if x != 0]
        if len(h) < self.WINDOW + 1: return None
        target, prev_h = h[-1], h[:-1]
        feats = self._make_features(prev_h, levels, streak)
        self.X.append(feats); self.y.append(target)
        if len(self.X) >= 30 and len(self.X) % 5 == 0: self._train()
        return {"features": feats, "target": target}

    def _train(self):
        if len(set(self.y)) < 2: return
        try: self.clf.fit(self.X, self.y); self.trained = True
        except: pass

    def predict(self, history: list, levels: list, streak: int) -> Optional[dict]:
        if not self.trained: return None
        h = [x for x in history if x != 0]
        if len(h) < self.WINDOW: return None
        feats = self._make_features(h, levels, streak)
        try:
            proba = self.clf.predict_proba([feats])[0]; classes = self.clf.classes_
            return {int(c): float(p) for c, p in zip(classes, proba)}
        except: return None

# ─── PATTERN 5 PREDICTOR ─────────────────────────────────────────────────────
class PatternPredictor5:
    def __init__(self): self._counts: dict = defaultdict(lambda: defaultdict(int))
    def update(self, history: list):
        h = [x for x in history if x != 0]
        if len(h) >= 6: self._counts[tuple(h[-6:-1])][h[-1]] += 1
    def predict(self, history: list) -> Optional[dict]:
        h = [x for x in history if x != 0]
        if len(h) < 5: return None
        c = dict(self._counts.get(tuple(h[-5:]), {})); total = sum(c.values())
        if total < 3: return None
        return {k: v/total for k,v in c.items()}

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
        self.dozen_history: list = []; self.column_history: list = []
        self.d_levels: dict[int, list] = {1:[], 2:[], 3:[]}; self.c_levels: dict[int, list] = {1:[], 2:[], 3:[]}
        self.d_streak, self.c_streak = 0, 0; self.last_d, self.last_c = 0, 0

        self.markov_d = MarkovPredictor(); self.dt_d = DecisionTreePredictor(); self.pat_d = PatternPredictor5()
        self.markov_c = MarkovPredictor(); self.dt_c = DecisionTreePredictor(); self.pat_c = PatternPredictor5()

        self.signal_active: bool = False; self.active_type: Optional[str] = None
        self.active_pair: tuple = (); self.active_missing: str = ""
        self.attempts_left: int = MAX_ATTEMPTS; self.signal_msg_ids: list = []
        self.bet_level = 1; self.cum_loss = 0.0; self.bankroll: float = 0.0
        self.trigger_number: int = 0; self.trigger_color: str = ""

        self.stats = DetailedStats(); self._db = _get_db()
        
        # 1. Carga historial en vivo y persistencia de IA
        live_loaded = self._load_live_history()
        
        # 2. Precalentamiento con Base de Datos Externa (russian-azure.db)
        self._pretrain_from_db(AZURE_DB, AZURE_TABLE)
        
        self.ws_count: int = live_loaded
        self.warmup_done: bool = False # Se requiere spins en vivo para sincronizar timing
        self.last_game_id: Optional[str] = None

    def current_bet(self) -> float:
        needed = self.cum_loss + BASE_BET; return round(max(needed, BASE_BET), 2)

    # ── Precalentamiento DB ────────────────────────────────────────────────────
    def _pretrain_from_db(self, db_path: str, table_name: str):
        if not os.path.exists(db_path):
            logger.warning(f"[RussianDC] DB de preentrenamiento no encontrada: {db_path}")
            return
        spins = []
        try:
            pattern = re.compile(rf'INSERT INTO "{table_name}" VALUES \(\d+,(\d+),')
            with open(db_path, "r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    m = pattern.search(line)
                    if m: spins.append(int(m.group(1)))
        except Exception as e:
            logger.error(f"[RussianDC] Error leyendo DB preentrenamiento: {e}")
            return
        if not spins:
            logger.warning(f"[RussianDC] Sin spins en tabla '{table_name}'")
            return

        for n in spins: self._update_state(n, persist=False, save_patterns=False)

        logger.info(f"[RussianDC] 🔥 Pre-entrenado con {len(spins)} giros (tabla: {table_name})")

    # ── Persistencia En Vivo ──────────────────────────────────────────────────
    def _load_live_history(self) -> int:
        try: rows = self._db.execute("SELECT number FROM live_spins ORDER BY id ASC").fetchall()
        except: return 0
        
        # Restaurar DT
        for cat, dt_obj in [("D", self.dt_d), ("C", self.dt_c)]:
            try:
                dt_rows = self._db.execute("SELECT features, target FROM dt_samples WHERE cat=?", (cat,)).fetchall()
                for (feats_str, target) in dt_rows:
                    feats = [float(x) for x in feats_str.split(",")]
                    dt_obj.X.append(feats); dt_obj.y.append(int(target))
                if len(dt_obj.X) >= 30: dt_obj._train()
            except: pass

        # Restaurar Markov
        for cat, mk_obj in [("D", self.markov_d), ("C", self.markov_c)]:
            try:
                mk_rows = self._db.execute("SELECT pattern, result, count FROM markov_counts WHERE cat=?", (cat,)).fetchall()
                for (pattern_str, result, count) in mk_rows:
                    pattern = tuple(int(x) for x in pattern_str.split(","))
                    mk_obj.counts1[pattern][result] += count
                    if len(pattern) == 2: mk_obj.counts2[pattern][result] += count
            except: pass

        # Aplicar giros pasados
        for (n,) in rows: self._update_state(n, persist=False, save_patterns=False)
        logger.info(f"[RussianDC] ✅ {len(rows)} giros en vivo + IA persistida cargados")
        return len(rows)

    def _persist(self, number: int):
        try: self._db.execute("INSERT INTO live_spins(number,ts) VALUES(?,?)", (number, int(time.time()))); self._db.commit()
        except: pass

    def _persist_dt_sample(self, cat: str, features: list, target: int):
        try:
            feats_str = ",".join(str(round(f, 4)) for f in features)
            self._db.execute("INSERT INTO dt_samples(cat, features, target) VALUES(?,?,?)", (cat, feats_str, target)); self._db.commit()
        except: pass

    def _persist_markov_update(self, cat: str, history: list):
        h = [x for x in history if x != 0]
        if len(h) < 2: return
        try:
            p1 = str(h[-2]); res = h[-1]
            self._db.execute("INSERT INTO markov_counts(cat,pattern,result,count) VALUES(?,?,?,1) ON CONFLICT(cat,pattern,result) DO UPDATE SET count=count+1", (cat, p1, res))
            if len(h) >= 3:
                p2 = f"{h[-3]},{h[-2]}"
                self._db.execute("INSERT INTO markov_counts(cat,pattern,result,count) VALUES(?,?,?,1) ON CONFLICT(cat,pattern,result) DO UPDATE SET count=count+1", (cat, p2, res))
            self._db.commit()
        except: pass

    # ── Actualizar estado ──────────────────────────────────────────────────────
    def _update_state(self, number: int, persist: bool = True, save_patterns: bool = True):
        color = REAL_COLOR_MAP.get(number, "VERDE"); d = get_dozen(number); c = get_column(number)
        self.spin_history.append({"number":number,"color":color})
        self.dozen_history.append(d); self.column_history.append(c)

        if d != 0:
            if d == self.last_d: self.d_streak += 1
            else: self.last_d = d; self.d_streak = 1
            for dd in (1,2,3):
                delta = 1 if d == dd else -1; prev = self.d_levels[dd][-1] if self.d_levels[dd] else 0
                self.d_levels[dd].append(prev + delta)
            self.markov_d.update(self.dozen_history)
            sample = self.dt_d.add_sample_return(self.dozen_history, self.d_levels[d], self.d_streak)
            self.pat_d.update(self.dozen_history)
            if save_patterns and sample: self._persist_dt_sample("D", sample["features"], sample["target"]); self._persist_markov_update("D", self.dozen_history)

        if c != 0:
            if c == self.last_c: self.c_streak += 1
            else: self.last_c = c; self.c_streak = 1
            for cc in (1,2,3):
                delta = 1 if c == cc else -1; prev = self.c_levels[cc][-1] if self.c_levels[cc] else 0
                self.c_levels[cc].append(prev + delta)
            self.markov_c.update(self.column_history)
            sample = self.dt_c.add_sample_return(self.column_history, self.c_levels[c], self.c_streak)
            self.pat_c.update(self.column_history)
            if save_patterns and sample: self._persist_dt_sample("C", sample["features"], sample["target"]); self._persist_markov_update("C", self.column_history)

        if persist: self._persist(number)

    # ── Clasificación y Validación ────────────────────────────────────────────
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
        seq = self.dozen_history if cat_type == "DOCENA" else self.column_history
        if len(seq) < 10: return False
        count = 0
        for i in range(len(seq) - 4):
            if set(seq[i:i+5]) == pair_set: count += 1
        return count >= 2

    # ── Probabilidad Unificada ────────────────────────────────────────────────
    def _unified_prob(self, cat_type: str, missing_num: int) -> float:
        mk = self.markov_d if cat_type == "DOCENA" else self.markov_c
        dt = self.dt_d if cat_type == "DOCENA" else self.dt_c
        pt = self.pat_d if cat_type == "DOCENA" else self.pat_c
        hist = self.dozen_history if cat_type == "DOCENA" else self.column_history
        levels = (self.d_levels if cat_type == "DOCENA" else self.c_levels).get(missing_num, [])
        streak = self.d_streak if cat_type == "DOCENA" else self.c_streak

        m_p = mk.predict(hist).get(missing_num, 1/3) if mk.predict(hist) else 1/3
        dt_p = dt.predict(hist, levels, streak).get(missing_num, 1/3) if dt.predict(hist, levels, streak) else 1/3
        p_p = pt.predict(hist).get(missing_num, 1/3) if pt.predict(hist) else 1/3

        raw_missing = 0.25*m_p + 0.35*dt_p + 0.25*p_p + 0.15*(1/3)
        
        if len(levels) >= 20:
            if ema_signal(levels, "tendencia"): raw_missing *= 0.95
            elif ema_signal(levels, "moderado"): raw_missing *= 0.98

        pair_prob = 1.0 - min(raw_missing, 0.99)
        return pair_prob

    # ── Detección de señal ────────────────────────────────────────────────────
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
            self.warmup_done = True; tg_send("🟢 <b>Russian Roulette DC</b> — Sistema listo y precalentado.")
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

# ─── FLASK ────────────────────────────────────────────────────────────────────
app = Flask(__name__); engine: Optional[RussianRouletteEngine] = None

@app.route("/")
def home(): return jsonify({"status": "ok", "bot": "Russian Roulette DC AMX"})

@app.route("/ping")
def ping(): return jsonify({"status":"pong","ts":time.time()})

@app.route("/health")
def health(): return jsonify({"status":"healthy","warmup": engine.warmup_done if engine else False})

# ─── SELF-PING ANTI-SLEEP ─────────────────────────────────────────────────────
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
    bot.reply_to(message, "<b>🎰 Russian Roulette DC Bot</b>\n\nAnálisis Exclusivo: Docenas y Columnas\nSistema: Markov + DT + Patrones(5) + AMX\nUmbral: 80% | Precalentamiento DB Activo\n\n/status\n/stats\n/reset", parse_mode="HTML")

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
    logger.info("🎰 Russian Roulette DC Bot iniciado")
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    threading.Thread(target=run_flask, daemon=True).start()
    try: asyncio.run(main())
    except KeyboardInterrupt: logger.info("Bot detenido.")


