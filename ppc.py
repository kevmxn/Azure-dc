#!/usr/bin/env python3
"""
Roulette Telegram Signal Bot — Docenas y Columnas Exclusivamente
  · Analiza últimos 5 resultados: si pertenecen a 2 docenas o 2 columnas
  · Valida que la secuencia se haya repetido históricamente
  · Predice 6° giro: Markov + ML + Patrones(longitud 5) + AMX
  · Las 2 más probables deben coincidir con el flujo y superar 80%
  · Compatible con Render (Flask silenciado)
"""

import asyncio
import json
import logging
import os
import re
import sqlite3
import threading
import time
from collections import deque, defaultdict
from typing import Optional, Tuple, Dict

import numpy as np
import telebot
import websockets
from flask import Flask, jsonify

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Evitar reloader y banners de Flask
os.environ['WERKZEUG_RUN_MAIN'] = 'true'

# ═══════════════════════════════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════════════════════════════
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(name)s] %(levelname)s %(message)s')
logger = logging.getLogger("RouletteBotDC")

# Silenciar Flask y Werkzeug
for _ln in ['werkzeug', 'flask.app', 'flask', 'urllib3']:
    logging.getLogger(_ln).setLevel(logging.ERROR)

# ═══════════════════════════════════════════════════════════════════════════════
# TELEGRAM & NETWORK
# ═══════════════════════════════════════════════════════════════════════════════
TOKEN = "8714149875:AAFJugWY0E5A4C0lrxn2bMcKsQEieqo_t5M"

_session = requests.Session()
_retry = Retry(total=5, backoff_factor=1.5, status_forcelist=[429, 500, 502, 503, 504],
               allowed_methods=["GET", "POST"], raise_on_status=False)
_adapter = HTTPAdapter(max_retries=_retry, pool_connections=10, pool_maxsize=20)
_session.mount("https://", _adapter)
_session.mount("http://", _adapter)

bot = telebot.TeleBot(TOKEN, threaded=False)
bot.session = _session

# ═══════════════════════════════════════════════════════════════════════════════
# DATABASE
# ═══════════════════════════════════════════════════════════════════════════════
DB_PATH      = "russian-azure.db"
LIVE_DB_PATH = "russian_live.db"

def _get_live_db():
    conn = sqlite3.connect(LIVE_DB_PATH, check_same_thread=False)
    conn.execute("""CREATE TABLE IF NOT EXISTS live_spins (
        id INTEGER PRIMARY KEY AUTOINCREMENT, table_name TEXT NOT NULL,
        number INTEGER NOT NULL, ts INTEGER NOT NULL)""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_table ON live_spins(table_name, id)")
    conn.commit()
    return conn

# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS & CONFIG
# ═══════════════════════════════════════════════════════════════════════════════
REAL_COLOR_MAP = {
    0:"VERDE",1:"ROJO",2:"NEGRO",3:"ROJO",4:"NEGRO",5:"ROJO",6:"NEGRO",
    7:"ROJO",8:"NEGRO",9:"ROJO",10:"NEGRO",11:"NEGRO",12:"ROJO",13:"NEGRO",
    14:"ROJO",15:"NEGRO",16:"ROJO",17:"NEGRO",18:"ROJO",19:"ROJO",20:"NEGRO",
    21:"ROJO",22:"NEGRO",23:"ROJO",24:"NEGRO",25:"ROJO",26:"NEGRO",27:"ROJO",
    28:"NEGRO",29:"NEGRO",30:"ROJO",31:"NEGRO",32:"ROJO",33:"NEGRO",34:"ROJO",
    35:"NEGRO",36:"ROJO"
}
COLOR_ICON = {"ROJO":"🔴", "NEGRO":"⚫️", "VERDE":"🟢"}

def get_dozen(n: int) -> int: return 0 if n == 0 else (n - 1) // 12 + 1
def get_column(n: int) -> int: return 0 if n == 0 else ((n - 1) % 3) + 1

ROULETTE_CONFIGS = {
    "RUSSIAN ROULETTE": {
        "bot": bot, "ws_key": 221, "chat_id": -1003835197023,
        "thread_id": 8344, "db_table": "russian_roulette",
        "min_prob_threshold": 0.80,  # 80% Mínimo umbral requerido
    },
}
WS_URL    = "wss://dga.pragmaticplaylive.net/ws"
CASINO_ID = "ppcjd00000007254"
MAX_ATTEMPTS  = 2
BASE_BET      = 0.50
WARMUP_SPINS  = 21
LOSS_COOLDOWN = 5

# ═══════════════════════════════════════════════════════════════════════════════
# MARTINGALE (Adaptado para 2 docenas/columnas - Pago 2 a 1)
# ═══════════════════════════════════════════════════════════════════════════════
class Martingale:
    def __init__(self, base: float):
        self.base = base; self.level = 1; self.bankroll = 0.0
        self.cumulative_loss = 0.0; self.consecutive_losses = 0

    def current_bet(self) -> float:
        needed = self.cumulative_loss + self.base
        return round(max(needed, self.base), 2)

    def win(self) -> float:
        bet = self.current_bet()
        self.bankroll = round(self.bankroll + bet, 2)
        self.level = 1; self.cumulative_loss = 0.0; self.consecutive_losses = 0
        return bet

    def loss(self) -> float:
        bet = self.current_bet()
        total_loss = round(bet * 2, 2)
        self.bankroll = round(self.bankroll - total_loss, 2)
        self.cumulative_loss = round(self.cumulative_loss + total_loss, 2)
        if self.level >= 6: self.level = 1; self.cumulative_loss = 0.0
        else: self.level += 1
        self.consecutive_losses += 1
        return total_loss

    def reset(self): self.level = 1; self.cumulative_loss = 0.0; self.consecutive_losses = 0

# ═══════════════════════════════════════════════════════════════════════════════
# PREDICTORES
# ═══════════════════════════════════════════════════════════════════════════════
class MarkovChainPredictor:
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
        state = tuple(sequence[-self.order:])
        counts = dict(self.transition_counts.get(state, {})); total = sum(counts.values())
        if total < 8: return None
        probs = {k: v/total for k,v in counts.items()}; probs["total"] = total
        return probs

class MLPatternPredictor:
    def __init__(self, pattern_length: int = 3):
        self.pattern_length = pattern_length
        self.pattern_counts: dict = defaultdict(lambda: defaultdict(int)); self._known_len: int = 0

    def add_spin(self, sequence: list):
        clen = len(sequence)
        if clen <= self._known_len: return
        self._known_len = clen
        if clen < self.pattern_length + 1: return
        i = clen - self.pattern_length - 1
        pattern = tuple(sequence[i:i+self.pattern_length]); nxt = sequence[i+self.pattern_length]
        self.pattern_counts[pattern][nxt] += 1

    def predict(self, sequence: list) -> Optional[dict]:
        if len(sequence) < self.pattern_length: return None
        pattern = tuple(sequence[-self.pattern_length:])
        counts = dict(self.pattern_counts.get(pattern, {})); total = sum(counts.values())
        if total < 2: return None
        probs = {k: v/total for k,v in counts.items()}; probs["total"] = total
        return probs

class CategoryPredictorDC:
    """Patrones de 5 últimos números exclusivamente para D y C"""
    PATTERN_LEN = 5
    def __init__(self):
        self._hist: dict = {"DOCENA": [], "COLUMNA": []}
        self._counts: dict = {
            "DOCENA": defaultdict(lambda: defaultdict(int)),
            "COLUMNA": defaultdict(lambda: defaultdict(int)),
        }

    def add_spin(self, number: int):
        if number == 0: return
        d = f"D{get_dozen(number)}"; c = f"C{get_column(number)}"
        for cat, val in [("DOCENA", d), ("COLUMNA", c)]:
            hist = self._hist[cat]
            if len(hist) >= self.PATTERN_LEN:
                pattern = tuple(hist[-self.PATTERN_LEN:])
                self._counts[cat][pattern][val] += 1
            hist.append(val)

    def predict_category(self, category: str) -> Optional[dict]:
        hist = self._hist.get(category, []); counts = self._counts.get(category, {})
        if len(hist) < self.PATTERN_LEN: return None
        pattern = tuple(hist[-self.PATTERN_LEN:])
        c = dict(counts.get(pattern, {})); total = sum(c.values())
        if total < 5: return None
        result = {k: v/total for k,v in c.items()}; result["total"] = total
        return result

class AMXSignalSystemDC:
    @staticmethod
    def _ema(data: list, period: int) -> list:
        if len(data) < period: return [None]*len(data)
        mult = 2/(period+1); ema = [None]*(period-1)
        prev = sum(data[:period])/period; ema.append(prev)
        for i in range(period, len(data)): prev = (data[i]*mult)+(prev*(1-mult)); ema.append(prev)
        return ema

    def check_signal(self, pair_levels: list, missing_levels: list) -> Optional[dict]:
        if len(pair_levels) < 20: return None
        ema4 = self._ema(pair_levels, 4); ema8 = self._ema(pair_levels, 8); ema20 = self._ema(pair_levels, 20)
        if any(v is None for v in [ema4[-1], ema8[-1], ema20[-1], ema4[-2], ema8[-2], ema20[-2]]): return None
        
        ce4,ce8,ce20 = ema4[-1],ema8[-1],ema20[-1]; pe4,pe8,pe20 = ema4[-2],ema8[-2],ema20[-2]
        cur = pair_levels[-1]
        
        cross_4_20 = pe4 <= pe20 and ce4 > ce20; cross_8_20 = pe8 <= pe20 and ce8 > ce20
        sobre_3 = cur > ce4 and cur > ce8 and cur > ce20; emas_alin = ce4 > ce8 > ce20
        
        miss_ema4 = self._ema(missing_levels, 4) if len(missing_levels)>=20 else [None]*len(missing_levels)
        miss_ema20 = self._ema(missing_levels, 20) if len(missing_levels)>=20 else [None]*len(missing_levels)
        missing_declining = False
        if miss_ema4[-1] is not None and miss_ema20[-1] is not None:
            missing_declining = miss_ema4[-1] < miss_ema20[-1]
            
        score = 0
        if cross_4_20: score += 3
        if cross_8_20: score += 2
        if sobre_3: score += 2
        if emas_alin: score += 1
        if missing_declining: score += 2
        
        if score < 3: return None
        strength = "strong" if score >= 6 else "moderate"
        boost = 0.05 if strength == "strong" else 0.02
        return {"score": score, "strength": strength, "boost": boost}

# ═══════════════════════════════════════════════════════════════════════════════
# STATISTICS & TG HELPERS
# ═══════════════════════════════════════════════════════════════════════════════
class DetailedStats:
    def __init__(self): self.signal_history: deque = deque(maxlen=50); self.wins_1 = 0; self.wins_2 = 0; self.losses = 0; self.total_signals = 0
    def record(self, attempt_won: int, final_result: bool, bet: float, bankroll: float, category: str, pair: str):
        self.signal_history.append({"attempt_won": attempt_won, "won": final_result, "bet": bet, "bankroll": bankroll, "timestamp": time.time(), "category": category, "pair": pair})
        self.total_signals += 1
        if final_result:
            if attempt_won == 1: self.wins_1 += 1
            else: self.wins_2 += 1
        else: self.losses += 1
    def summary(self, bankroll: float) -> str:
        w = self.wins_1 + self.wins_2; t = w + self.losses; eff = round(w/t*100,1) if t else 0
        return (f"📊 <b>ESTADÍSTICAS</b>\n\n✅ W1: {self.wins_1} | ✅ W2: {self.wins_2}\n❌ Losses: {self.losses}\n📈 Eff: {eff}%\n💰 Bankroll: {bankroll:.2f} usd")

_TG_MAX_RETRIES = 12
def _tg_call(fn, *args, **kwargs):
    delay = 2.0
    for attempt in range(1, _TG_MAX_RETRIES+1):
        try: return fn(*args, **kwargs)
        except Exception as e:
            err = str(e)
            if "retry after" in err.lower():
                try: wait = int(''.join(filter(str.isdigit, err)))+1
                except: wait = 30
                time.sleep(wait); continue
            if attempt < _TG_MAX_RETRIES: time.sleep(delay); delay = min(delay*2, 60)
            else: return None

def tg_send(bot_inst, chat_id, thread_id, text) -> Optional[int]:
    msg = _tg_call(bot_inst.send_message, chat_id=chat_id, text=text, parse_mode="HTML", message_thread_id=thread_id)
    return msg.message_id if msg else None

def tg_delete(bot_inst, chat_id, msg_id): _tg_call(bot_inst.delete_message, chat_id=chat_id, message_id=msg_id)

# ═══════════════════════════════════════════════════════════════════════════════
# ROULETTE ENGINE — LÓGICA EXCLUSIVA DOCENAS Y COLUMNAS
# ═══════════════════════════════════════════════════════════════════════════════
class RouletteEngine:
    def __init__(self, name: str, cfg: dict):
        self.name = name; self.bot = cfg["bot"]; self.ws_key = cfg["ws_key"]
        self.chat_id = cfg["chat_id"]; self.thread_id = cfg["thread_id"]; self.db_table = cfg["db_table"]
        self.min_prob_threshold = cfg.get("min_prob_threshold", 0.80)

        self.spin_history: list = []; self.dozen_seq: list = []; self.column_seq: list = []
        self.d_levels = {1:[], 2:[], 3:[]}; self.c_levels = {1:[], 2:[], 3:[]}

        self.markov_dozen = MarkovChainPredictor(window=60, order=2); self.markov_column = MarkovChainPredictor(window=60, order=2)
        self.ml_dozen = MLPatternPredictor(pattern_length=3); self.ml_column = MLPatternPredictor(pattern_length=3)
        self.cat_predictor = CategoryPredictorDC(); self.amx_system = AMXSignalSystemDC()

        self.signal_active = False; self.attempts_left = 0
        self.active_type: Optional[str] = None; self.active_pair: Tuple = (); self.active_missing: str = ""
        self.trigger_number: int = 0; self.signal_msg_ids: list = []

        self.bet_sys = Martingale(BASE_BET); self.spins_since_loss: int = 999
        self.stats = DetailedStats()
        self.ws = None; self.running = True; self._live_conn = _get_live_db()

        self._pretrain_from_db(DB_PATH, self.db_table)
        live_loaded = self._load_live_history()
        self.ws_spins_count: int = live_loaded; self.warmup_done: bool = live_loaded >= WARMUP_SPINS

    def _pretrain_from_db(self, db_path: str, table_name: str):
        if not os.path.exists(db_path): return
        spins = []
        try:
            pattern = re.compile(rf'INSERT INTO "{table_name}" VALUES \(\d+,(\d+),')
            with open(db_path, "r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    m = pattern.search(line)
                    if m: spins.append(int(m.group(1)))
        except Exception: return
        if not spins: return
        for n in spins: self._process_spin_internal(n, from_pretrain=True)

    def _load_live_history(self) -> int:
        try:
            cutoff = int(time.time()) - 7*86400
            cur = self._live_conn.execute("SELECT number FROM live_spins WHERE table_name=? AND ts>=? ORDER BY id ASC", (self.db_table, cutoff))
            rows = cur.fetchall()
        except Exception: return 0
        if not rows: return 0
        for (n,) in rows: self._process_spin_internal(n, from_pretrain=True)
        return len(rows)

    def _process_spin_internal(self, number: int, from_pretrain: bool = False):
        real = REAL_COLOR_MAP.get(number, "VERDE"); self.spin_history.append({"number": number, "real": real})
        if number == 0:
            for d in (1,2,3):
                prev = self.d_levels[d][-1] if self.d_levels[d] else 0; self.d_levels[d].append(prev - 0.5)
            for c in (1,2,3):
                prev = self.c_levels[c][-1] if self.c_levels[c] else 0; self.c_levels[c].append(prev - 0.5)
            return

        d = get_dozen(number); c = get_column(number); d_val = f"D{d}"; c_val = f"C{c}"
        self.dozen_seq.append(d_val); self.column_seq.append(c_val)
        self.markov_dozen.update(self.dozen_seq); self.markov_column.update(self.column_seq)
        self.ml_dozen.add_spin(self.dozen_seq); self.ml_column.add_spin(self.column_seq)
        self.cat_predictor.add_spin(number)

        for dd in (1,2,3):
            prev = self.d_levels[dd][-1] if self.d_levels[dd] else 0; inc = 1.0 if dd == d else -0.5; self.d_levels[dd].append(prev + inc)
        for cc in (1,2,3):
            prev = self.c_levels[cc][-1] if self.c_levels[cc] else 0; inc = 1.0 if cc == c else -0.5; self.c_levels[cc].append(prev + inc)

    def _persist_spin(self, number: int):
        try:
            self._live_conn.execute("INSERT INTO live_spins (table_name, number, ts) VALUES (?,?,?)", (self.db_table, number, int(time.time()))); self._live_conn.commit()
        except Exception:
            try:
                self._live_conn = _get_live_db(); self._live_conn.execute("INSERT INTO live_spins (table_name, number, ts) VALUES (?,?,?)", (self.db_table, number, int(time.time()))); self._live_conn.commit()
            except Exception: pass

    # ══════════════════════════════════════════════════════════════════
    # 1. CLASIFICACIÓN ÚLTIMOS 5 RESULTADOS
    # ══════════════════════════════════════════════════════════════════
    def _classify_last5(self) -> Optional[Dict]:
        if len(self.spin_history) < 5: return None
        last5 = self.spin_history[-5:]
        if any(s["number"] == 0 for s in last5): return None
        
        dozens_set = set(f"D{get_dozen(s['number'])}" for s in last5)
        columns_set = set(f"C{get_column(s['number'])}" for s in last5)
        
        result = {"dozen_pattern": None, "column_pattern": None}
        if len(dozens_set) == 2:
            missing_d = ({"D1","D2","D3"} - dozens_set).pop()
            result["dozen_pattern"] = {"present": list(dozens_set), "missing": missing_d}
        if len(columns_set) == 2:
            missing_c = ({"C1","C2","C3"} - columns_set).pop()
            result["column_pattern"] = {"present": list(columns_set), "missing": missing_c}
            
        if not result["dozen_pattern"] and not result["column_pattern"]: return None
        return result

    # ══════════════════════════════════════════════════════════════════
    # 2. VALIDACIÓN HISTÓRICA DE SECUENCIA
    # ══════════════════════════════════════════════════════════════════
    def _validate_historical_pair(self, cat_type: str, pair_set: set) -> bool:
        seq = self.dozen_seq if cat_type == "DOCENA" else self.column_seq
        if len(seq) < 10: return False
        count = 0
        for i in range(len(seq) - 4):
            if set(seq[i:i+5]) == pair_set: count += 1
        return count >= 2  # La actual + al menos 1 previa

    # ══════════════════════════════════════════════════════════════════
    # 3. PREDICCIÓN MARKOV + ML + PATRONES(5) + AMX
    # ══════════════════════════════════════════════════════════════════
    def _predict_top_2(self, cat_type: str) -> Tuple[list, float]:
        all_vals = ["D1","D2","D3"] if cat_type == "DOCENA" else ["C1","C2","C3"]
        markov_probs = {v:0.0 for v in all_vals}; ml_probs = {v:0.0 for v in all_vals}; pattern_probs = {v:0.0 for v in all_vals}
        
        if cat_type == "DOCENA":
            m_pred = self.markov_dozen.predict(self.dozen_seq); ml_pred = self.ml_dozen.predict(self.dozen_seq)
        else:
            m_pred = self.markov_column.predict(self.column_seq); ml_pred = self.ml_column.predict(self.column_seq)
        cat_pred = self.cat_predictor.predict_category(cat_type)
        
        if m_pred and m_pred.get("total",0) >= 8:
            for v in all_vals: markov_probs[v] = m_pred.get(v, 0)
        if ml_pred and ml_pred.get("total",0) >= 2:
            for v in all_vals: ml_probs[v] = ml_pred.get(v, 0)
        if cat_pred and cat_pred.get("total",0) >= 5:
            for v in all_vals: pattern_probs[v] = cat_pred.get(v, 0)
            
        w_m, w_ml, w_p, w_pr = 0.25, 0.25, 0.35, 0.15; prior = 1.0/3.0
        final_probs = {}
        for v in all_vals:
            raw = (w_m * markov_probs[v] + w_ml * ml_probs[v] + w_p * pattern_probs[v] + w_pr * prior)
            final_probs[v] = raw
            
        sorted_probs = sorted(final_probs.items(), key=lambda x: x[1], reverse=True)
        top2_vals = [sorted_probs[0][0], sorted_probs[1][0]]
        top2_prob_sum = sorted_probs[0][1] + sorted_probs[1][1]
        
        # AMX Boost
        amx_boost = 0.0
        if len(self.d_levels[1]) >= 20:
            pair_nums = [int(v[1]) for v in top2_vals]; miss_num = list({1,2,3} - set(pair_nums))[0]
            if cat_type == "DOCENA":
                pair_lv = [self.d_levels[pair_nums[0]][i] + self.d_levels[pair_nums[1]][i] for i in range(min(len(self.d_levels[pair_nums[0]]), len(self.d_levels[pair_nums[1]])))]
                miss_lv = self.d_levels[miss_num]
            else:
                pair_lv = [self.c_levels[pair_nums[0]][i] + self.c_levels[pair_nums[1]][i] for i in range(min(len(self.c_levels[pair_nums[0]]), len(self.c_levels[pair_nums[1]])))]
                miss_lv = self.c_levels[miss_num]
            if len(pair_lv) >= 20 and len(miss_lv) >= 20:
                amx_sig = self.amx_system.check_signal(pair_lv, miss_lv)
                if amx_sig: amx_boost = amx_sig.get("boost", 0.0)
                
        top2_prob_sum = min(0.99, top2_prob_sum + amx_boost)
        return top2_vals, top2_prob_sum

    # ══════════════════════════════════════════════════════════════════
    # DETECCIÓN Y EMISIÓN DE SEÑAL
    # ══════════════════════════════════════════════════════════════════
    def _try_detect_signal(self):
        classification = self._classify_last5()
        if classification is None: return
        
        candidates = []
        if classification["dozen_pattern"]:
            dp = classification["dozen_pattern"]; pair_set = set(dp["present"])
            if self._validate_historical_pair("DOCENA", pair_set):
                top2, prob = self._predict_top_2("DOCENA")
                if set(top2) == pair_set and prob >= self.min_prob_threshold:
                    candidates.append({"type": "DOCENA", "pair": top2, "missing": dp["missing"], "prob": prob})
                    
        if classification["column_pattern"]:
            cp = classification["column_pattern"]; pair_set = set(cp["present"])
            if self._validate_historical_pair("COLUMNA", pair_set):
                top2, prob = self._predict_top_2("COLUMNA")
                if set(top2) == pair_set and prob >= self.min_prob_threshold:
                    candidates.append({"type": "COLUMNA", "pair": top2, "missing": cp["missing"], "prob": prob})
                    
        if not candidates: return
        best = max(candidates, key=lambda x: x["prob"])
        self._emit_signal(best)

    def _format_pair_display(self, pair: Tuple) -> str:
        nums = sorted([p[1:].zfill(2) for p in pair])
        return f"{nums[0]} y {nums[1]}"

    def _emit_signal(self, signal_data: dict):
        self.signal_active = True; self.attempts_left = MAX_ATTEMPTS
        self.active_type = signal_data["type"]; self.active_pair = signal_data["pair"]; self.active_missing = signal_data["missing"]
        self.trigger_number = self.spin_history[-1]["number"] if self.spin_history else 0
        self._send_signal_message(1, signal_data["prob"])

    def _send_signal_message(self, attempt: int, prob: float):
        for mid in self.signal_msg_ids: tg_delete(self.bot, self.chat_id, mid)
        self.signal_msg_ids = []
        
        bet = self.bet_sys.current_bet(); trig_n = self.trigger_number
        trig_real = REAL_COLOR_MAP.get(trig_n, "VERDE"); trig_icon = COLOR_ICON.get(trig_real, "🟢")
        pair_display = self._format_pair_display(self.active_pair)
        
        if self.active_type == "DOCENA":
            line1 = f"❄️ ENTRAR EN DOCENAS: {pair_display}"
            line2 = f"♦️ APUESTA EN DOCENA: {bet:.2f}"
        else:
            line1 = f"☢ ENTRAR EN COLUMNAS: {pair_display}"
            line2 = f"♦️ APUESTA EN COLUMNA: {bet:.2f}"
            
        text = (
            f"🎯 <b>SEÑAL CONFIRMADA</b> 🎯\n\n"
            f"🎰 <b>{self.name}</b>\n"
            f"👉 ÚLTIMO NÚMERO: {trig_n} {trig_real} {trig_icon}\n"
            f"{line1}\n"
            f"{line2}"
        )
        msg_id = tg_send(self.bot, self.chat_id, self.thread_id, text)
        if msg_id: self.signal_msg_ids.append(msg_id)
        logger.info(f"[{self.name}] 🎯 SEÑAL {self.active_type}: {self.active_pair} (Prob: {prob:.0%})")

    # ── Verificar resultado y 2° Intento ──────────────────────────────
    def _check_signal_result(self, number: int, real_color: str):
        won = False
        if number != 0:
            if self.active_type == "DOCENA": result = f"D{get_dozen(number)}"; won = result in self.active_pair
            elif self.active_type == "COLUMNA": result = f"C{get_column(number)}"; won = result in self.active_pair

        if won:
            bet = self.bet_sys.win(); self._send_result_message(number, real_color, True, bet, MAX_ATTEMPTS - self.attempts_left + 1)
            self.stats.record(MAX_ATTEMPTS - self.attempts_left + 1, True, bet, self.bet_sys.bankroll, self.active_type, f"{self.active_pair[0]}+{self.active_pair[1]}")
            self._deactivate_signal()
        else:
            self.attempts_left -= 1
            if self.attempts_left <= 0:
                loss_amount = self.bet_sys.loss(); self._send_result_message(number, real_color, False, loss_amount, MAX_ATTEMPTS)
                self.stats.record(MAX_ATTEMPTS, False, loss_amount, self.bet_sys.bankroll, self.active_type, f"{self.active_pair[0]}+{self.active_pair[1]}")
                self._deactivate_signal(loss=True)
            else:
                self._reevaluate_2nd_attempt()

    def _reevaluate_2nd_attempt(self):
        top2, prob = self._predict_top_2(self.active_type)
        self.active_pair = top2
        self.active_missing = list(({"D1","D2","D3"} if self.active_type == "DOCENA" else {"C1","C2","C3"}) - set(top2))[0]
        self._send_signal_message(2, prob)

    def _deactivate_signal(self, loss: bool = False):
        self.signal_active = False; self.active_type = None; self.active_pair = (); self.active_missing = ""
        if loss: self.spins_since_loss = 0

    def _send_result_message(self, number: int, real: str, won: bool, amount: float, attempt_num: int):
        for mid in self.signal_msg_ids: tg_delete(self.bot, self.chat_id, mid)
        self.signal_msg_ids = []
        
        d_val = f"D{get_dozen(number)}" if number != 0 else "0"; c_val = f"C{get_column(number)}" if number != 0 else "0"
        pair_display = self._format_pair_display(self.active_pair)
        status = f"✅ <b>¡GREEN! {number} {real} ({d_val}/{c_val})</b>" if won else f"❌ <b>¡LOSS! {number} {real} ({d_val}/{c_val})</b>"
        detail = f"💰 Ganancia: +{amount:.2f} usd" if won else f"💰 Pérdida: -{amount:.2f} usd"
        
        text = (
            f"{status}\n\n"
            f"🎲 <b>{self.active_type}: {pair_display}</b>\n"
            f"🔄 Intento: {attempt_num}/{MAX_ATTEMPTS}\n"
            f"{detail}\n"
            f"💰 <i>BANKROLL: {self.bet_sys.bankroll:.2f} usd</i>\n"
            f"📈 <i>NIVEL: {self.bet_sys.level}/6</i>"
        )
        tg_send(self.bot, self.chat_id, self.thread_id, text)

    # ══════════════════════════════════════════════════════════════════
    # MAIN SPIN PROCESSOR
    # ══════════════════════════════════════════════════════════════════
    def process_spin(self, number: int):
        real = REAL_COLOR_MAP.get(number, "VERDE"); self._process_spin_internal(number); self._persist_spin(number)
        if not self.warmup_done:
            self.ws_spins_count += 1
            if self.ws_spins_count >= WARMUP_SPINS: self.warmup_done = True; logger.info(f"[{self.name}] ✅ Warmup completado")
            return
        if self.signal_active: self._check_signal_result(number, real); return
        if self.spins_since_loss < LOSS_COOLDOWN: self.spins_since_loss += 1; return
        self._try_detect_signal()

    async def ws_loop(self):
        while self.running:
            try:
                async with websockets.connect(WS_URL, additional_headers={"Origin": "https://www.pragmaticplay.com"}, ping_interval=20, ping_timeout=60, close_timeout=5) as ws:
                    self.ws = ws; await ws.send(json.dumps({"method": "subscribe", "params": {"casinoId": CASINO_ID, "tableId": self.ws_key}}))
                    logger.info(f"[{self.name}] ✅ WebSocket conectado")
                    async for raw_msg in ws:
                        if not self.running: break
                        try:
                            msg = json.loads(raw_msg); number = self._extract_number(msg)
                            if number is not None: self.process_spin(number)
                        except: pass
            except Exception as e:
                logger.error(f"[{self.name}] WS error: {e}"); await asyncio.sleep(5)

    def _extract_number(self, msg: dict) -> Optional[int]:
        try:
            for src in [msg.get("params",{}), msg.get("data",{})]:
                if "result" in src:
                    r = src["result"]
                    if isinstance(r, int) and 0<=r<=36: return r
                    if isinstance(r, dict) and "winningNumber" in r:
                        n = int(r["winningNumber"])
                        if 0<=n<=36: return n
            if msg.get("method") == "gameResult":
                r = msg.get("params",{}).get("result")
                if isinstance(r, int) and 0<=r<=36: return r
        except: pass
        return None

# ═══════════════════════════════════════════════════════════════════════════════
# FLASK HEALTH CHECK — SILENCIOSO PARA RENDER
# ═══════════════════════════════════════════════════════════════════════════════
app = Flask(__name__)

@app.route("/health")
def health():
    return jsonify({"status": "ok", "time": time.time()})

def _run_flask():
    import logging as _log
    _log.getLogger('werkzeug').setLevel(_log.ERROR)
    _log.getLogger('flask.app').setLevel(_log.ERROR)
    app.run(host="0.0.0.0", port=5002, debug=False, use_reloader=False, threaded=True)

# ═══════════════════════════════════════════════════════════════════════════════
# TELEGRAM BOT COMMANDS
# ═══════════════════════════════════════════════════════════════════════════════
engines: dict = {}

@bot.message_handler(commands=["start"])
def cmd_start(msg):
    bot.reply_to(msg, "🤖 <b>Bot Ruleta — Docenas &amp; Columnas</b>\nAnálisis: Markov + ML + Patrones(5) + AMX\nUmbral: 80%\n\n/stats\n/reset\n/status", parse_mode="HTML")

@bot.message_handler(commands=["stats"])
def cmd_stats(msg):
    for eng in engines.values(): _tg_call(bot.send_message, chat_id=msg.chat.id, text=eng.stats.summary(eng.bet_sys.bankroll), parse_mode="HTML")

@bot.message_handler(commands=["reset"])
def cmd_reset(msg):
    for eng in engines.values(): eng.bet_sys.reset(); eng.signal_active = False; eng.spins_since_loss = 999
    bot.reply_to(msg, "♻️ Martingala reiniciada.", parse_mode="HTML")

@bot.message_handler(commands=["status"])
def cmd_status(msg):
    for name, eng in engines.items():
        last5_text = "N/A"
        if len(eng.spin_history) >= 5:
            parts = []
            for s in eng.spin_history[-5:]:
                n = s["number"]
                if n == 0: parts.append("0🟢")
                else: parts.append(f"{n} D{get_dozen(n)} C{get_column(n)}")
            last5_text = " | ".join(parts)
        text = (f"🎰 <b>{name}</b>\n📌 Últimos 5: {last5_text}\n"
                f"📈 Nivel: {eng.bet_sys.level}/6\n💰 Bankroll: {eng.bet_sys.bankroll:.2f} usd\n"
                f"⏳ Cooldown: {eng.spins_since_loss}/{LOSS_COOLDOWN}")
        _tg_call(bot.send_message, chat_id=msg.chat.id, text=text, parse_mode="HTML")

# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════
def main():
    global engines
    for name, cfg in ROULETTE_CONFIGS.items(): engines[name] = RouletteEngine(name, cfg)
    
    # Flask silencioso en Render
    threading.Thread(target=_run_flask, daemon=True).start()
    
    # Telegram bot polling
    def _poll_bot():
        while True:
            try: bot.infinity_polling(timeout=10, long_polling_timeout=5)
            except Exception as e:
                logger.error(f"Bot polling error: {e}"); time.sleep(5)
    threading.Thread(target=_poll_bot, daemon=True).start()
    
    # WebSocket loops en asyncio
    loop = asyncio.new_event_loop(); asyncio.set_event_loop(loop)
    tasks = [asyncio.ensure_future(eng.ws_loop()) for eng in engines.values()]
    try: loop.run_until_complete(asyncio.gather(*tasks))
    except KeyboardInterrupt: 
        for eng in engines.values(): eng.running = False
    finally: loop.close()

if __name__ == "__main__":
    main()
