#!/usr/bin/env python3
"""
Roulette Telegram Signal Bot — Enfoque Docenas/Columnas + AMX + Markov/ML
  · Máximo 2 intentos por señal
  · Martingala avanza niveles para pago 2:1 (2 Docenas/2 Columnas)
  · Análisis de los últimos 5 números para determinar Top 2
  · Probabilidad Histórica + Markov + ML ≥ 80% para disparar
  · Formato de señal personalizado
"""

import asyncio
import json
import logging
import os
import re
import sqlite3
import time
from collections import deque, defaultdict
from typing import Optional, Literal

import numpy as np
import telebot
import websockets
from flask import Flask, jsonify

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ─── LOGGING ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] %(levelname)s %(message)s'
)
logger = logging.getLogger("RouletteBotDCZ")

# ─── TELEGRAM ─────────────────────────────────────────────────────────────────
TOKEN   = "8714149875:AAFJugWY0E5A4C0lrxn2bMcKsQEieqo_t5M"

_session = requests.Session()
_retry = Retry(
    total=5,
    backoff_factor=1.5,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "POST"],
    raise_on_status=False,
)
_adapter = HTTPAdapter(max_retries=_retry, pool_connections=10, pool_maxsize=20)
_session.mount("https://", _adapter)
_session.mount("http://",  _adapter)

bot = telebot.TeleBot(TOKEN, threaded=False)
bot.session = _session

# ─── DB CONFIG ────────────────────────────────────────────────────────────────
DB_PATH       = "russian-azure.db"
LIVE_DB_PATH  = "russian_live.db"

def _get_live_db() -> "sqlite3.Connection":
    import sqlite3 as _sq
    conn = _sq.connect(LIVE_DB_PATH, check_same_thread=False)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS live_spins (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            table_name TEXT NOT NULL,
            number    INTEGER NOT NULL,
            ts        INTEGER NOT NULL
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_table ON live_spins(table_name, id)")
    conn.commit()
    return conn

# ─── ROULETTE COLOR MAPS ──────────────────────────────────────────────────────
REAL_COLOR_MAP = {
    0:"VERDE",1:"ROJO",2:"NEGRO",3:"ROJO",4:"NEGRO",5:"ROJO",6:"NEGRO",
    7:"ROJO",8:"NEGRO",9:"ROJO",10:"NEGRO",11:"NEGRO",12:"ROJO",13:"NEGRO",
    14:"ROJO",15:"NEGRO",16:"ROJO",17:"NEGRO",18:"ROJO",19:"ROJO",20:"NEGRO",
    21:"ROJO",22:"NEGRO",23:"ROJO",24:"NEGRO",25:"ROJO",26:"NEGRO",27:"ROJO",
    28:"NEGRO",29:"NEGRO",30:"ROJO",31:"NEGRO",32:"ROJO",33:"NEGRO",34:"ROJO",
    35:"NEGRO",36:"ROJO"
}

# ─── CATEGORÍAS HELPER ────────────────────────────────────────────────────────
def get_paridad(number: int) -> Optional[str]:
    if number == 0: return None
    return "PAR" if number % 2 == 0 else "IMPAR"

def get_rango(number: int) -> Optional[str]:
    if number == 0: return None
    return "BAJO" if 1 <= number <= 18 else "ALTO"

def get_dozen(n: int) -> int:
    if n == 0: return 0
    return (n - 1) // 12 + 1

def get_column(n: int) -> int:
    if n == 0: return 0
    return ((n - 1) % 3) + 1

CATEGORY_ICONS = {
    "ROJO": "🔴", "NEGRO": "⚫️", "VERDE": "🟢",
    "D1": "🟡", "D2": "🔵", "D3": "🟠",
    "C1": "🟡", "C2": "🔵", "C3": "🟠",
}

DOZEN_PAIRS  = {1:(2,3), 2:(1,3), 3:(1,2)}
COLUMN_PAIRS = {1:(2,3), 2:(1,3), 3:(1,2)}

# ─── ROULETTE CONFIGS ─────────────────────────────────────────────────────────
ROULETTE_CONFIGS = {
    "RUSSIAN ROULETTE": {
        "bot":       bot,
        "ws_key":    221,
        "chat_id":   -1003835197023,
        "thread_id": 8344,
        "db_table":  "russian_roulette",
        "min_prob_threshold": 0.80, # 80% Umbral principal
    },
}

WS_URL    = "wss://dga.pragmaticplaylive.net/ws"
CASINO_ID = "ppcjd00000007254"
MAX_ATTEMPTS = 2
BASE_BET  = 0.50
VISIBLE   = 50
WARMUP_SPINS = 21

# ─── MARTINGALA PARA 2 DOCENAS/COLUMNAS (Pago 2:1) ────────────────────────────
class Martingale2Dozen:
    def __init__(self, base: float):
        self.base = base
        self.level = 1
        self.bankroll = 0.0
        self.consecutive_losses = 0
        self.progression = [1, 1.5, 3, 6, 15, 38] # Multiplicadores base para pago 2:1

    def current_bet_per_item(self) -> float:
        mult = self.progression[min(self.level - 1, len(self.progression) - 1)]
        return round(self.base * mult, 2)

    def current_total_bet(self) -> float:
        return round(self.current_bet_per_item() * 2, 2)

    def win(self) -> float:
        profit = self.current_bet_per_item() # Net win es igual a la apuesta por item
        self.bankroll = round(self.bankroll + profit, 2)
        self.level = 1
        self.consecutive_losses = 0
        return profit

    def loss(self) -> float:
        total = self.current_total_bet()
        self.bankroll = round(self.bankroll - total, 2)
        if self.level >= 6: self.level = 1
        else: self.level += 1
        return total

    def full_loss(self) -> float:
        bet = self.loss()
        self.consecutive_losses += 1
        return bet

    def reset(self):
        self.level = 1
        self.consecutive_losses = 0

# ─── MARKOV CHAIN ─────────────────────────────────────────────────────────────
class MarkovChainPredictor:
    def __init__(self, window: int = 60, order: int = 2):
        self.window = window; self.order  = order; self.transition_counts: dict = {}

    def update(self, sequence: list):
        self.transition_counts = defaultdict(lambda: defaultdict(int))
        recent = sequence[-self.window:]
        if len(recent) < self.order + 1: return
        for i in range(len(recent) - self.order):
            state  = tuple(recent[i : i + self.order])
            next_c = recent[i + self.order]
            self.transition_counts[state][next_c] += 1

    def predict(self, sequence: list) -> Optional[dict]:
        if len(sequence) < self.order: return None
        state  = tuple(sequence[-self.order:])
        counts = dict(self.transition_counts.get(state, {}))
        total  = sum(counts.values())
        if total < 8: return None
        probs = {k: v / total for k, v in counts.items()}; probs["total"] = total
        return probs

# ─── ML PATTERN PREDICTOR ─────────────────────────────────────────────────────
class MLPatternPredictor:
    def __init__(self, pattern_length: int = 3):
        self.pattern_length = pattern_length
        self.pattern_counts: dict = defaultdict(lambda: defaultdict(int))
        self._known_len: int = 0

    def add_spin(self, sequence: list):
        current_len = len(sequence)
        if current_len <= self._known_len: return
        self._known_len = current_len
        if current_len < self.pattern_length + 1: return
        i = current_len - self.pattern_length - 1
        pattern = tuple(sequence[i : i + self.pattern_length])
        next_c  = sequence[i + self.pattern_length]
        self.pattern_counts[pattern][next_c] += 1

    def predict(self, sequence: list) -> Optional[dict]:
        if len(sequence) < self.pattern_length: return None
        pattern = tuple(sequence[-self.pattern_length:])
        counts  = dict(self.pattern_counts.get(pattern, {}))
        total   = sum(counts.values())
        if total < 2: return None
        probs = {k: v / total for k, v in counts.items()}; probs["total"] = total
        return probs

# ─── CATEGORY PREDICTOR (10 patrones) ────────────────────────────────────────
class CategoryPredictor:
    PATTERN_LEN = 10
    def __init__(self):
        self._hist: dict[str, list[str]] = {"DOCENA":[], "COLUMNA":[]}
        self._counts: dict[str, dict] = {
            "DOCENA": defaultdict(lambda: defaultdict(int)), 
            "COLUMNA": defaultdict(lambda: defaultdict(int))
        }

    def add_spin(self, number: int, real_color: str):
        if number == 0: return
        dozen = get_dozen(number); column = get_column(number)
        new_vals = {"DOCENA": f"D{dozen}", "COLUMNA": f"C{column}"}
        for cat, val in new_vals.items():
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
        result = {k: v / total for k, v in c.items()}; result["total"] = total
        return result

# ─── LAST 5 PREDICTOR (TOP 2) ────────────────────────────────────────────────
class DozenColumnLast5Predictor:
    def __init__(self):
        self.dozen_history: list[str] = []
        self.column_history: list[str] = []
        self.dozen_pattern_results: dict = defaultdict(lambda: {"hits": 0, "total": 0})
        self.column_pattern_results: dict = defaultdict(lambda: {"hits": 0, "total": 0})

    def add_spin(self, number: int):
        if number == 0: return
        dozen = f"D{get_dozen(number)}"
        column = f"C{get_column(number)}"

        if len(self.dozen_history) >= 5:
            last5_d = self.dozen_history[-5:]
            counts = defaultdict(int)
            for d in last5_d: counts[d] += 1
            sorted_d = sorted(counts.items(), key=lambda x: x[1], reverse=True)
            top2_key = tuple(sorted([sorted_d[0][0], sorted_d[1][0]]))
            self.dozen_pattern_results[top2_key]["total"] += 1
            if dozen in top2_key: self.dozen_pattern_results[top2_key]["hits"] += 1

        if len(self.column_history) >= 5:
            last5_c = self.column_history[-5:]
            counts = defaultdict(int)
            for c in last5_c: counts[c] += 1
            sorted_c = sorted(counts.items(), key=lambda x: x[1], reverse=True)
            top2_key = tuple(sorted([sorted_c[0][0], sorted_c[1][0]]))
            self.column_pattern_results[top2_key]["total"] += 1
            if column in top2_key: self.column_pattern_results[top2_key]["hits"] += 1

        self.dozen_history.append(dozen)
        self.column_history.append(column)

    def predict_dozen(self, last5_numbers: list) -> Optional[dict]:
        if len(self.dozen_history) < 5: return None
        dozen_counts: dict = defaultdict(int)
        for n in last5_numbers:
            if n == 0: continue
            dozen_counts[f"D{get_dozen(n)}"] += 1
        sorted_d = sorted(dozen_counts.items(), key=lambda x: x[1], reverse=True)
        if len(sorted_d) < 2: return None
        top2 = tuple(sorted([sorted_d[0][0], sorted_d[1][0]]))
        result = self.dozen_pattern_results.get(top2)
        if not result or result["total"] < 5: return None
        prob = result["hits"] / result["total"]
        return {"top2": list(top2), "probability": prob, "total_samples": result["total"], "hits": result["hits"]}

    def predict_column(self, last5_numbers: list) -> Optional[dict]:
        if len(self.column_history) < 5: return None
        column_counts: dict = defaultdict(int)
        for n in last5_numbers:
            if n == 0: continue
            column_counts[f"C{get_column(n)}"] += 1
        sorted_c = sorted(column_counts.items(), key=lambda x: x[1], reverse=True)
        if len(sorted_c) < 2: return None
        top2 = tuple(sorted([sorted_c[0][0], sorted_c[1][0]]))
        result = self.column_pattern_results.get(top2)
        if not result or result["total"] < 5: return None
        prob = result["hits"] / result["total"]
        return {"top2": list(top2), "probability": prob, "total_samples": result["total"], "hits": result["hits"]}

# ─── AMX SIGNAL SYSTEM ───────────────────────────────────────────────────────
class AMXSignalSystem:
    def __init__(self, mode: Literal["tendencia", "moderado"] = "tendencia"):
        self.mode = mode; self.last_signal_time: float = 0
        self.last_two_expected = deque(maxlen=2)

    def calculate_ema(self, data: list, period: int) -> list:
        if len(data) < period: return [None] * len(data)
        mult = 2 / (period + 1); ema  = [None] * (period - 1); prev = sum(data[:period]) / period; ema.append(prev)
        for i in range(period, len(data)): prev = (data[i] * mult) + (prev * (1 - mult)); ema.append(prev)
        return ema

    def check_signal(self, positions: list, val: str) -> Optional[dict]:
        if len(positions) < 20: return None
        ema4 = self.calculate_ema(positions, 4); ema8 = self.calculate_ema(positions, 8)
        # CORRECCIÓN DE SINTAXIS AQUÍ (is None)
        if ema4[-1] is None or ema8[-1] is None: return None
        cruce_4_8 = ema4[-2] <= ema8[-2] and ema4[-1] > ema8[-1]
        sobre = positions[-1] > ema4[-1] and positions[-1] > ema8[-1]
        score = 0
        if cruce_4_8: score += 3
        if sobre: score += 2
        if score < 3: return None
        return {"type": "AMX_EMA", "strength": "strong" if score >= 5 else "moderate"}

    def register_signal_sent(self): self.last_signal_time = time.time()

# ─── UNIFIED PROBABILITY ─────────────────────────────────────────────────────
class UnifiedProbabilitySystem:
    def __init__(self):
        self.weights = {"markov": 0.35, "ml": 0.65}
        self.ema_trend_factor: float = 1.0; self.sr_factor: float = 1.0; self.volatility: float = 1.0

# ─── DETAILED STATS ──────────────────────────────────────────────────────────
class DetailedStats:
    def __init__(self):
        self.signal_history: deque = deque(maxlen=50); self.wins_1: int = 0; self.wins_2: int = 0
        self.losses: int = 0; self.total_signals: int = 0; self.last_stats_at: int = 0
        self.batch_start_bankroll: Optional[float] = None; self.batch_start_w1: int = 0
        self.batch_start_w2: int = 0; self.batch_start_losses: int = 0

    def record_signal_result(self, attempt_won: int, final_result: bool, bet_amount: float, bankroll: float, category: str):
        self.total_signals += 1
        if final_result:
            if attempt_won == 1: self.wins_1 += 1
            elif attempt_won == 2: self.wins_2 += 1
        else: self.losses += 1

    def should_send_stats(self) -> bool: return (self.total_signals - self.last_stats_at) >= 20
    
    def mark_stats_sent(self, bankroll: float): 
        self.last_stats_at = self.total_signals
        self.batch_start_bankroll = bankroll
        self.batch_start_w1 = self.wins_1
        self.batch_start_w2 = self.wins_2
        self.batch_start_losses = self.losses

    def get_batch_stats(self, current_bankroll: float) -> dict:
        w1 = self.wins_1 - self.batch_start_w1; w2 = self.wins_2 - self.batch_start_w2
        l = self.losses - self.batch_start_losses
        wins = w1 + w2; total = wins + l
        bk = round(current_bankroll - self.batch_start_bankroll, 2) if self.batch_start_bankroll is not None else 0.0
        return {"total": total, "wins": wins, "losses": l, "efficiency": round(wins / total * 100, 1) if total else 0.0, "bankroll_delta": bk}

# ─── TELEGRAM HELPERS ─────────────────────────────────────────────────────────
_TG_MAX_RETRIES = 12
def _tg_call(fn, *args, **kwargs):
    delay = 2.0
    for attempt in range(1, _TG_MAX_RETRIES + 1):
        try: return fn(*args, **kwargs)
        except Exception as e:
            err = str(e)
            if "retry after" in err.lower():
                try: wait = int(''.join(filter(str.isdigit, err))) + 1
                except: wait = 30
                time.sleep(wait); continue
            if attempt < _TG_MAX_RETRIES: time.sleep(delay); delay = min(delay * 2, 60)
            else: return None

def tg_send_text(bot_inst, chat_id, thread_id, text) -> Optional[int]:
    msg = _tg_call(bot_inst.send_message, chat_id=chat_id, text=text, parse_mode="HTML", message_thread_id=thread_id)
    return msg.message_id if msg else None

def tg_delete(bot_inst, chat_id, msg_id): _tg_call(bot_inst.delete_message, chat_id=chat_id, message_id=msg_id)

# ─── ROULETTE ENGINE ──────────────────────────────────────────────────────────
class RouletteEngine:
    def __init__(self, name: str, cfg: dict):
        self.name = name; self.bot = cfg["bot"]; self.ws_key = cfg["ws_key"]; self.chat_id = cfg["chat_id"]; self.thread_id = cfg["thread_id"]; self.db_table = cfg["db_table"]
        self.spin_history: list = []; 
        
        # Levels Trackers
        self.d1_levels: list = []; self.d2_levels: list = []; self.d3_levels: list = []
        self.c1_levels: list = []; self.c2_levels: list = []; self.c3_levels: list = []
        
        self.signal_active: bool = False; self.waiting_for_attempt: bool = False; self.waiting_attempt_number: int = 0
        self.active_category: Optional[str] = None; self.bet_value: Optional[str] = None; self.signal_pair: tuple = ()
        self.attempts_left: int = 0; self.total_attempts: int = 0; self.trigger_number: Optional[int] = None
        self.signal_msg_ids: list = []; 
        
        # Sistema Especializado 2 Docenas/Columnas
        self.bet_sys = Martingale2Dozen(BASE_BET)
        self.min_prob_threshold = cfg.get("min_prob_threshold", 0.80)
        self.pre_alert_threshold: float = 0.60
        
        self.amx_system = AMXSignalSystem(mode="tendencia")
        self.unified_prob_system = UnifiedProbabilitySystem()
        self.stats = DetailedStats()
        
        # Predictores
        self.markov_docena  = MarkovChainPredictor(window=60, order=2)
        self.markov_columna = MarkovChainPredictor(window=60, order=2)
        self.ml_docena      = MLPatternPredictor(pattern_length=3)
        self.ml_columna     = MLPatternPredictor(pattern_length=3)
        self.category_ml    = CategoryPredictor()
        self.dcz_predictor  = DozenColumnLast5Predictor()
        
        self.ws = None; self.running = True; self._live_conn = _get_live_db()
        self.pending_prediction: Optional[dict] = None
        
        # Variables Combo
        self.bet_dozen_pair: tuple = ()
        self.bet_column_pair: tuple = ()

        # Inicialización y Pre-entrenamiento
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
        temp_history = []
        for n in spins:
            real = REAL_COLOR_MAP.get(n, "VERDE"); temp_history.append({"number": n, "real": real})
            self._update_all_predictors(n, real, temp_history)

    def _load_live_history(self) -> int:
        try:
            cutoff = int(time.time()) - 7 * 86400; cur = self._live_conn.execute("SELECT number FROM live_spins WHERE table_name=? AND ts>=? ORDER BY id ASC", (self.db_table, cutoff)); rows = cur.fetchall()
        except Exception: return 0
        if not rows: return 0
        temp_history = []
        for (n,) in rows: real = REAL_COLOR_MAP.get(n, "VERDE"); temp_history.append({"number": n, "real": real}); self._update_all_predictors(n, real, temp_history)
        return len(rows)

    def _update_all_predictors(self, number: int, real: str, history: list):
        self.category_ml.add_spin(number, real)
        self.dcz_predictor.add_spin(number)

        if number != 0:
            docena_seq = [f"D{get_dozen(s['number'])}" for s in history if s['number'] != 0]
            columna_seq = [f"C{get_column(s['number'])}" for s in history if s['number'] != 0]
            if len(docena_seq) >= self.markov_docena.order:
                self.markov_docena.update(docena_seq)
                self.ml_docena.add_spin(docena_seq)
            if len(columna_seq) >= self.markov_columna.order:
                self.markov_columna.update(columna_seq)
                self.ml_columna.add_spin(columna_seq)

            # Actualizar levels para AMX
            d = get_dozen(number); c = get_column(number)
            if d == 1: self.d1_levels.append(1); self.d2_levels.append(0); self.d3_levels.append(0)
            elif d == 2: self.d1_levels.append(0); self.d2_levels.append(1); self.d3_levels.append(0)
            elif d == 3: self.d1_levels.append(0); self.d2_levels.append(0); self.d3_levels.append(1)
            if c == 1: self.c1_levels.append(1); self.c2_levels.append(0); self.c3_levels.append(0)
            elif c == 2: self.c1_levels.append(0); self.c2_levels.append(1); self.c3_levels.append(0)
            elif c == 3: self.c1_levels.append(0); self.c2_levels.append(0); self.c3_levels.append(1)

    def _persist_spin(self, number: int):
        try: self._live_conn.execute("INSERT INTO live_spins (table_name, number, ts) VALUES (?,?,?)", (self.db_table, number, int(time.time()))); self._live_conn.commit()
        except Exception: pass

    def _category_icon(self, value: str) -> str: return CATEGORY_ICONS.get(value, "❓")

    def _trigger_display(self, number: int) -> str:
        if number == 0: return "0 VERDE 🟢"
        val = REAL_COLOR_MAP.get(number, "VERDE")
        return f"{number} {val} {self._category_icon(val)}"

    def _format_pair_numbers(self, pair: tuple) -> str:
        nums = []
        for p in pair:
            num_str = str(p[-1])
            nums.append(f"0{num_str}" if len(num_str) == 1 else num_str)
        return f"{nums[0]} y {nums[1]}"

    def _is_win(self, number: int) -> Optional[bool]:
        if number == 0: return None
        cat = self.active_category
        if cat == "DOCENA":
            result = f"D{get_dozen(number)}"
            return result in self.signal_pair
        if cat == "COLUMNA":
            result = f"C{get_column(number)}"
            return result in self.signal_pair
        return False

    def _levels_for(self, category: str, bet_value: str) -> list:
        return {("DOCENA","D1"): self.d1_levels, ("DOCENA","D2"): self.d2_levels, ("DOCENA","D3"): self.d3_levels, ("COLUMNA","C1"): self.c1_levels, ("COLUMNA","C2"): self.c2_levels, ("COLUMNA","C3"): self.c3_levels}.get((category, bet_value), [])

    # ══════════════════════════════════════════════════════════════════════
    # SISTEMA DE PROBABILIDAD UNIFICADA DCZ
    # ══════════════════════════════════════════════════════════════════════
    def _analyze_dcz_last5(self) -> Optional[dict]:
        non_zero = [s["number"] for s in self.spin_history if s["number"] != 0]
        if len(non_zero) < 5: return None
        last5 = non_zero[-5:]

        dozen_counts: dict = defaultdict(int); column_counts: dict = defaultdict(int)
        for n in last5:
            dozen_counts[f"D{get_dozen(n)}"] += 1
            column_counts[f"C{get_column(n)}"] += 1

        sorted_d = sorted(dozen_counts.items(), key=lambda x: x[1], reverse=True)
        top2_dozen = [sorted_d[0][0], sorted_d[1][0]] if len(sorted_d) >= 2 else []
        d_hit_count = sum(v for k, v in sorted_d[:2])

        sorted_c = sorted(column_counts.items(), key=lambda x: x[1], reverse=True)
        top2_column = [sorted_c[0][0], sorted_c[1][0]] if len(sorted_c) >= 2 else []
        c_hit_count = sum(v for k, v in sorted_c[:2])

        return {
            "last5": last5, 
            "top2_dozen": top2_dozen, "dozen_hit_count": d_hit_count,
            "top2_column": top2_column, "column_hit_count": c_hit_count,
        }

    def _calculate_dcz_probability(self, analysis: dict, mode: Literal["docena", "columna"]) -> Optional[dict]:
        w = self.unified_prob_system.weights

        # Secuencias completas para Markov/ML
        docena_seq = [f"D{get_dozen(s['number'])}" for s in self.spin_history if s['number'] != 0]
        columna_seq = [f"C{get_column(s['number'])}" for s in self.spin_history if s['number'] != 0]

        if mode == "docena":
            top2 = analysis["top2_dozen"]
            dcz = self.dcz_predictor.predict_dozen(analysis["last5"])
            m_pred = self.markov_docena.predict(docena_seq)
            ml_pred = self.ml_docena.predict(docena_seq)
            cat_pred = self.category_ml.predict_category("DOCENA")
            hit_count = analysis["dozen_hit_count"]
        else:
            top2 = analysis["top2_column"]
            dcz = self.dcz_predictor.predict_column(analysis["last5"])
            m_pred = self.markov_columna.predict(columna_seq)
            ml_pred = self.ml_columna.predict(columna_seq)
            cat_pred = self.category_ml.predict_category("COLUMNA")
            hit_count = analysis["column_hit_count"]

        if len(top2) < 2: return None

        base_prob = 24.0 / 37.0
        last5_prob = dcz["probability"] if dcz else base_prob

        markov_prob = base_prob
        if m_pred:
            p1 = m_pred.get(top2[0], 0); p2 = m_pred.get(top2[1], 0)
            markov_prob = p1 + p2

        ml_prob = base_prob
        if ml_pred:
            p1 = ml_pred.get(top2[0], 0); p2 = ml_pred.get(top2[1], 0)
            ml_prob = p1 + p2

        cat_prob = base_prob
        if cat_pred:
            p1 = cat_pred.get(top2[0], 0); p2 = cat_pred.get(top2[1], 0)
            cat_prob = p1 + p2

        raw_prob = (0.40 * last5_prob) + (0.35 * (w["markov"] * markov_prob + w["ml"] * ml_prob)) + (0.25 * cat_prob)
        adjusted = max(0.30, min(0.97, raw_prob * self.unified_prob_system.ema_trend_factor))

        if hit_count >= 5: adjusted = min(0.97, adjusted * 1.05)
        elif hit_count >= 4: adjusted = min(0.97, adjusted * 1.02)

        ema_bonus = 0.0
        for val in top2:
            levels = self._levels_for("DOCENA" if mode == "docena" else "COLUMNA", val)
            if levels:
                ema_sig = self.amx_system.check_signal(levels, val)
                if ema_sig and ema_sig.get("strength") == "strong": ema_bonus = max(ema_bonus, 0.03)
                elif ema_sig: ema_bonus = max(ema_bonus, 0.01)

        final_prob = min(0.97, adjusted + ema_bonus)
        strength = "strong" if final_prob >= 0.85 else "moderate" if final_prob >= 0.80 else "weak"

        return {
            "category": "DOCENA" if mode == "docena" else "COLUMNA",
            "top2": top2, "probability": final_prob, "signal_strength": strength, "hit_count": hit_count
        }

    def _detect_best_category_signal(self) -> Optional[dict]:
        analysis = self._analyze_dcz_last5()
        if analysis is None: return None

        candidates = []
        dozen_result = self._calculate_dcz_probability(analysis, "docena")
        if dozen_result and dozen_result["probability"] >= self.pre_alert_threshold:
            dozen_result["trigger_number"] = self.spin_history[-1]["number"] if self.spin_history else 0
            dozen_result["signal_pair"] = tuple(dozen_result["top2"])
            candidates.append(dozen_result)

        column_result = self._calculate_dcz_probability(analysis, "columna")
        if column_result and column_result["probability"] >= self.pre_alert_threshold:
            column_result["trigger_number"] = self.spin_history[-1]["number"] if self.spin_history else 0
            column_result["signal_pair"] = tuple(column_result["top2"])
            candidates.append(column_result)

        if not candidates: return None
        best = max(candidates, key=lambda x: x["probability"])
        best["bet_value"] = " + ".join(best["top2"])
        return best

    # ══════════════════════════════════════════════════════════════════════
    # GESTIÓN DE SEÑALES Y MENSAJES
    # ══════════════════════════════════════════════════════════════════════
    def _evaluate_combo_signal(self):
        analysis = self._analyze_dcz_last5()
        if analysis is None: return
        self.bet_dozen_pair = tuple(analysis["top2_dozen"])
        self.bet_column_pair = tuple(analysis["top2_column"])

    def _build_signal_text(self, attempt: int, unified_prob: Optional[dict]) -> str:
        bet_per_item = self.bet_sys.current_bet_per_item()
        trig_disp = self._trigger_display(self.trigger_number)
        nivel_actual = self.bet_sys.level
        prob_pct = int((unified_prob["combined_prob"] if unified_prob else 0.5) * 100)

        lines = [
            f"🎯 <b>SEÑAL CONFIRMADA</b> 🎯\n",
            f"🎰 <b>{self.name}</b>",
            f"👉 <b>ÚLTIMO NÚMERO: {trig_disp}</b>"
        ]

        if self.active_category == "DOCENA" and self.bet_dozen_pair:
            pair_str = self._format_pair_numbers(self.bet_dozen_pair)
            lines.append(f"❄️ <b>ENTRAR EN DOCENAS: {pair_str}</b>")
            lines.append(f"♦️ <b>APUESTA EN DOCENA: {bet_per_item:.2f}</b>")
        elif self.active_category == "COLUMNA" and self.bet_column_pair:
            pair_str = self._format_pair_numbers(self.bet_column_pair)
            lines.append(f"☢ <b>ENTRAR EN COLUMNAS: {pair_str}</b>")
            lines.append(f"♦️ <b>APUESTA EN COLUMNA: {bet_per_item:.2f}</b>")

        lines.append(f"💡 <i>PROBABILIDAD IA {prob_pct}% | NIVEL {nivel_actual}/6</i>")

        return "\n".join(lines)

    def _send_signal(self, attempt: int, unified_prob: dict):
        if self.signal_msg_ids:
            for mid in self.signal_msg_ids: tg_delete(self.bot, self.chat_id, mid)
            self.signal_msg_ids = []
        text = self._build_signal_text(attempt, unified_prob)
        msg_id = tg_send_text(self.bot, self.chat_id, self.thread_id, text)
        if msg_id: self.signal_msg_ids.append(msg_id)

    def _send_result(self, number: int, won: bool):
        if self.signal_msg_ids:
            for mid in self.signal_msg_ids: tg_delete(self.bot, self.chat_id, mid)
            self.signal_msg_ids = []

        bankroll = self.bet_sys.bankroll
        status = f"✅ <b>¡GREEN {number}!</b>" if won else f"❌ <b>¡LOSS {number}!</b>"
        cat_label = "2 DOCENAS" if self.active_category == "DOCENA" else "2 COLUMNAS"

        tg_send_text(
            self.bot, self.chat_id, self.thread_id,
            f"{status}\n\n"
            f"❄️ <b>CATEGORÍA: {cat_label} → {self.bet_value}</b>\n"
            f"💰 <i>BANKROLL: {bankroll:.2f} usd</i>\n"
            f"♻️ <i>NIVEL DE INTENTO {self.bet_sys.level}/6</i>"
        )

    def _activate_signal(self, best_signal: dict):
        self.active_category = best_signal["category"]
        self.bet_value = best_signal["bet_value"]
        self.signal_pair = best_signal["signal_pair"]
        self.trigger_number = best_signal["trigger_number"]
        self.total_attempts = MAX_ATTEMPTS
        self.attempts_left = MAX_ATTEMPTS
        self.signal_active = True
        self.waiting_for_attempt = True
        self.waiting_attempt_number = 1

        self._evaluate_combo_signal()

        unified_prob = {"combined_prob": best_signal["probability"]}
        self._send_signal(1, unified_prob)
        self.amx_system.register_signal_sent()

    def _evaluate_2nd_attempt_choice(self) -> Optional[dict]:
        cat = self.active_category
        analysis = self._analyze_dcz_last5()
        if analysis is None: return None
        mode = "docena" if cat == "DOCENA" else "columna"
        result = self._calculate_dcz_probability(analysis, mode)
        if result and result["probability"] > 0.5:
            new_pair = tuple(result["top2"])
            new_val = " + ".join(result["top2"])
            if new_pair != self.signal_pair:
                self.signal_pair = new_pair
                self.bet_value = new_val
            return {"combined_prob": result["probability"]}
        return None

    def _check_stats(self):
        if not self.stats.should_send_stats(): return
        current_bankroll = self.bet_sys.bankroll
        self.stats.mark_stats_sent(current_bankroll)
        batch = self.stats.get_batch_stats(current_bankroll)
        
        text = (
            f"📊 <b>ESTADÍSTICAS DEL BATCH</b>\n\n"
            f"🔸 Señales: {batch['total']}\n"
            f"✅ Aciertos: {batch['wins']} ({batch['efficiency']}%)\n"
            f"❌ Pérdidas: {batch['losses']}\n"
            f"💰 Bankroll Δ: {batch['bankroll_delta']:.2f} usd"
        )
        tg_send_text(self.bot, self.chat_id, self.thread_id, text)

    # ══════════════════════════════════════════════════════════════════════
    # PROCESAMIENTO PRINCIPAL DE GIROS
    # ══════════════════════════════════════════════════════════════════════
    def process_spin(self, number: int):
        real = REAL_COLOR_MAP.get(number, "VERDE")
        self.spin_history.append({"number": number, "real": real})
        self._update_all_predictors(number, real, self.spin_history)
        self._persist_spin(number)
        self.ws_spins_count += 1
        if self.ws_spins_count >= WARMUP_SPINS: self.warmup_done = True
        if not self.warmup_done: return

        # 1. Si hay señal activa, verificar resultado
        if self.signal_active and self.waiting_for_attempt:
            won = self._is_win(number)
            level_used = self.bet_sys.level

            if won is True:
                profit = self.bet_sys.win()
                self.stats.record_signal_result(self.waiting_attempt_number, True, profit, self.bet_sys.bankroll, self.active_category)
                self._send_result(number, True)
                self.signal_active = False
                self.waiting_for_attempt = False
            elif won is False:
                lost = self.bet_sys.full_loss()
                if self.attempts_left > 1:
                    # Segundo intento
                    self.attempts_left -= 1
                    self.waiting_attempt_number = 2
                    self._evaluate_2nd_attempt_choice()
                    pred = {"combined_prob": 0.80} # Default para texto
                    self._send_signal(2, pred)
                else:
                    # Pérdida total
                    self.stats.record_signal_result(2, False, lost, self.bet_sys.bankroll, self.active_category)
                    self._send_result(number, False)
                    self.signal_active = False
                    self.waiting_for_attempt = False
            # Si cayó 0 (Verde), won es None, se repite intento sin consecuencias
            
            self._check_stats()
            return

        # 2. Buscar nueva señal si no hay activa
        if not self.signal_active:
            best_signal = self._detect_best_category_signal()
            if best_signal and best_signal["probability"] >= self.min_prob_threshold:
                self._activate_signal(best_signal)


# ─── FLASK & WEBSOCKET LOOP ──────────────────────────────────────────────────
app = Flask(__name__)
engines = {}

for name, cfg in ROULETTE_CONFIGS.items():
    engines[name] = RouletteEngine(name, cfg)

@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "ok"})

async def ws_listen(engine: RouletteEngine):
    while engine.running:
        try:
            async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=30) as ws:
                logger.info(f"[{engine.name}] Conectado a WS")
                init_msg = {
                    "type": "subscribe",
                    "key": engine.ws_key,
                    "casinoId": CASINO_ID
                }
                await ws.send(json.dumps(init_msg))
                
                while engine.running:
                    msg = await asyncio.wait_for(ws.recv(), timeout=60)
                    data = json.loads(msg)
                    
                    # Parsear número del formato Pragmatic Play
                    if data.get("type") == "stats" and "result" in data:
                        try:
                            num = int(data["result"])
                            logger.info(f"[{engine.name}] Spin recibido: {num}")
                            engine.process_spin(num)
                        except ValueError:
                            pass
                            
        except asyncio.TimeoutError:
            logger.warning(f"[{engine.name}] WS Timeout, reenviando subscribe...")
            continue
        except websockets.exceptions.ConnectionClosed:
            logger.error(f"[{engine.name}] WS Conexión cerrada. Reconectando en 10s...")
            await asyncio.sleep(10)
        except Exception as e:
            logger.error(f"[{engine.name}] WS Error: {e}. Reconectando en 10s...")
            await asyncio.sleep(10)

def run_flask():
    port = int(os.environ.get('PORT', 5000))
    app.run(host="0.0.0.0", port=port, threaded=True)

def run_ws_loop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    tasks = [ws_listen(eng) for eng in engines.values()]
    loop.run_until_complete(asyncio.gather(*tasks))

if __name__ == "__main__":
    import threading
    threading.Thread(target=run_flask, daemon=True).start()
    run_ws_loop()
