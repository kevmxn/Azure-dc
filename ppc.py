#!/usr/bin/env python3
"""
Roulette Telegram Signal Bot — Martingala con AMX + Predicción Markov/ML/Patrones
  · Máximo 2 intentos por señal
  · Martingala avanza 1 nivel por pérdida (dentro y entre señales)
  · Si pierde en nivel 6, vuelve a nivel 1
  · Reinicio de ficha a nivel 1 al ganar
  · Modo AMX tendencia por defecto
  · Cooldown 5 spins post-pérdida
  · Estadísticas unificadas con historial de 20 señales + 24 horas
  · Pre-entrenamiento con russian-azure.db
  · PRE‑ALERTA (≥50% prob 11°) → Simula acierto → Señal 12° (si ≥60%)
  · 2° Oportunidad (13° giro): Evalúa misma categoría y apuesta al valor de mayor probabilidad (opuesto o mismo)
  · Solo se emite señal tras confirmación de pre‑alerta
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
logger = logging.getLogger("RouletteBotAMX")

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
    "ROJO": "🔴", "NEGRO": "⚫️",
    "PAR":  "🟣", "IMPAR": "🟡",
    "BAJO": "🟤", "ALTO":  "🔵",
    "VERDE": "🟢",
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
        "min_prob_threshold": 0.60,
    },
}

WS_URL    = "wss://dga.pragmaticplaylive.net/ws"
CASINO_ID = "ppcjd00000007254"
MAX_ATTEMPTS = 2

BASE_BET  = 0.50
VISIBLE   = 50
WARMUP_SPINS = 21

# ─── MARTINGALA ───────────────────────────────────────────────────────────────
class Martingale:
    def __init__(self, base: float):
        self.base = base
        self.level = 1
        self.bankroll = 0.0
        self.consecutive_losses = 0

    def current_bet(self) -> float:
        bets = {1: self.base, 2: self.base * 2, 3: self.base * 4,
                4: self.base * 8, 5: self.base * 16, 6: self.base * 32}
        return round(bets.get(self.level, self.base * 32), 2)

    def win(self) -> float:
        bet = self.current_bet()
        self.bankroll = round(self.bankroll + bet, 2)
        self.level = 1
        self.consecutive_losses = 0
        return bet

    def loss(self) -> float:
        bet = self.current_bet()
        self.bankroll = round(self.bankroll - bet, 2)
        if self.level >= 6: self.level = 1
        else: self.level += 1
        return bet

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

# ─── CATEGORY PREDICTOR (patrones de 10) ─────────────────────────────────────
class CategoryPredictor:
    PATTERN_LEN = 10
    def __init__(self):
        self._hist: dict[str, list[str]] = {"COLOR":[], "PARIDAD":[], "RANGO":[], "DOCENA":[], "COLUMNA":[]}
        self._counts: dict[str, dict] = {
            "COLOR": defaultdict(lambda: defaultdict(int)), "PARIDAD": defaultdict(lambda: defaultdict(int)),
            "RANGO": defaultdict(lambda: defaultdict(int)), "DOCENA": defaultdict(lambda: defaultdict(int)),
            "COLUMNA": defaultdict(lambda: defaultdict(int))
        }

    def add_spin(self, number: int, real_color: str):
        if number == 0 or real_color == "VERDE": return
        par = get_paridad(number); rang = get_rango(number); dozen = get_dozen(number); column = get_column(number)
        if not par or not rang: return
        new_vals = {"COLOR": real_color, "PARIDAD": par, "RANGO": rang, "DOCENA": f"D{dozen}", "COLUMNA": f"C{column}"}
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

# ─── AMX SIGNAL SYSTEM ───────────────────────────────────────────────────────
class AMXSignalSystem:
    def __init__(self, mode: Literal["tendencia", "moderado"] = "tendencia"):
        self.mode = mode; self.last_signal_time: float = 0
        self.last_two_expected = deque(maxlen=2); self.last_two_colors   = deque(maxlen=2)

    def update_streak(self, real_color: str, expected_color: Optional[str]):
        if expected_color: self.last_two_expected.append(real_color == expected_color)
        self.last_two_colors.append(real_color)

    def calculate_ema(self, data: list, period: int) -> list:
        if len(data) < period: return [None] * len(data)
        mult = 2 / (period + 1); ema  = [None] * (period - 1); prev = sum(data[:period]) / period; ema.append(prev)
        for i in range(period, len(data)): prev = (data[i] * mult) + (prev * (1 - mult)); ema.append(prev)
        return ema

    def check_signal(self, positions: list, expected_color: str) -> Optional[dict]:
        if len(positions) < 20: return None
        ema4 = self.calculate_ema(positions, 4); ema8 = self.calculate_ema(positions, 8); ema20 = self.calculate_ema(positions, 20)
        if any(v is None for v in [ema4[-1], ema8[-1], ema20[-1], ema4[-2], ema8[-2], ema20[-2]]): return None
        cur = positions[-1]; ce4, ce8, ce20 = ema4[-1], ema8[-1], ema20[-1]; pe4, pe8, pe20 = ema4[-2], ema8[-2], ema20[-2]
        cruce_4_20 = pe4 <= pe20 and ce4 > ce20; cruce_8_20 = pe8 <= pe20 and ce8 > ce20
        sobre_3 = cur > ce4 and cur > ce8 and cur > ce20; sobre_2 = cur > ce4 and cur > ce8
        patron_v = False
        if len(positions) >= 3:
            a, b, c = positions[-3], positions[-2], positions[-1]; patron_v = b < a and b < c and c > a
        emas_alin = ce4 > ce8 > ce20; racha_ok = len(self.last_two_expected) >= 2 and all(self.last_two_expected)
        score = 0; mode  = "moderado"
        if cruce_4_20: score += 3; mode = "tendencia"
        if cruce_8_20: score += 2; if sobre_3: score += 2; if sobre_2: score += 1
        if patron_v: score += 2; if emas_alin: score += 1; if racha_ok: score += 1
        if score < 3: return None
        strength = "strong" if score >= 5 else "moderate"
        return {"type": "AMX_EMA", "mode": mode, "expected_color": expected_color, "score": score, "strength": strength, "trigger_number": 0, "pattern": ("V" if patron_v else "CROSS_4_20" if cruce_4_20 else "CROSS_8_20" if cruce_8_20 else "EMA")}

    def register_signal_sent(self): self.last_signal_time = time.time()

# ─── SOPORTE Y RESISTENCIA ────────────────────────────────────────────────────
def find_support_resistance(levels: list, tolerancia: float = 0.5, min_rebotes: int = 2, ventana: int = 5, max_niveles: int = 3) -> dict:
    if len(levels) < ventana * 2 + 2: return {"supports": [], "resistances": [], "support": None, "resistance": None}
    soportes, resistencias = [], []
    for i in range(ventana, len(levels) - ventana):
        window = levels[i - ventana: i + ventana + 1]; minimo = min(window)
        if (window.index(minimo) == ventana and levels[i] < levels[i-1] and levels[i] < levels[i+1]):
            nivel = levels[i]; fuerza = abs(levels[i-1] - levels[i]) + abs(levels[i+1] - levels[i])
            existente = next((s for s in soportes if abs(s["nivel"] - nivel) <= tolerancia), None)
            if existente: existente["rebotes"] += 1; existente["fuerza_total"] += fuerza; existente["ultimos_idx"].append(i); existente["nivel"] = max(existente["nivel"], nivel)
            else: soportes.append({"nivel": nivel, "rebotes": 1, "fuerza_total": fuerza, "ultimos_idx": [i], "fuerte": False})
    for i in range(ventana, len(levels) - ventana):
        window = levels[i - ventana: i + ventana + 1]; maximo = max(window)
        if (window.index(maximo) == ventana and levels[i] > levels[i-1] and levels[i] > levels[i+1]):
            nivel = levels[i]; fuerza = abs(levels[i-1] - levels[i]) + abs(levels[i+1] - levels[i])
            existente = next((r for r in resistencias if abs(r["nivel"] - nivel) <= tolerancia), None)
            if existente: existente["rebotes"] += 1; existente["fuerza_total"] += fuerza; existente["ultimos_idx"].append(i); existente["nivel"] = max(existente["nivel"], nivel)
            else: resistencias.append({"nivel": nivel, "rebotes": 1, "fuerza_total": fuerza, "ultimos_idx": [i], "fuerte": False})
    soportes_val = [s for s in soportes if s["rebotes"] >= min_rebotes]; resist_val = [r for r in resistencias if r["rebotes"] >= min_rebotes]
    for s in soportes_val: s["fuerte"] = s["rebotes"] >= 3 or s["fuerza_total"] >= 4
    for r in resist_val: r["fuerte"] = r["rebotes"] >= 3 or r["fuerza_total"] >= 4
    soportes_final = sorted(soportes_val, key=lambda x: x["ultimos_idx"][-1], reverse=True)[:max_niveles]
    resistencias_final = sorted(resist_val, key=lambda x: x["ultimos_idx"][-1], reverse=True)[:max_niveles]
    return {"supports": soportes_final, "resistances": resistencias_final, "support": soportes_final[0]["nivel"] if soportes_final else None, "resistance": resistencias_final[0]["nivel"] if resistencias_final else None}

# ─── PROBABILIDAD UNIFICADA ──────────────────────────────────────────────────
class UnifiedProbabilitySystem:
    def __init__(self):
        self.weights = {"markov": 0.35, "ml": 0.65}; self.prediction_history: deque = deque(maxlen=200)
        self.markov_correct: int = 0; self.markov_total: int = 0; self.ml_correct: int = 0; self.ml_total: int = 0
        self.confidence_factor: float = 0.5; self.volatility: float = 1.0; self.current_streak: int = 0; self.streak_direction: Optional[str] = None
        self.spins_since_weight_update: int = 0; self.WEIGHT_UPDATE_INTERVAL: int = 50
        self.base_threshold: float = 0.50; self.dynamic_threshold: float = 0.50; self.ema_trend_factor: float = 1.0; self.sr_factor: float = 1.0

    def calculate_volatility(self, levels: list) -> float:
        if len(levels) < 20: return 1.0
        std_dev = np.std(levels[-20:]); normalized = min(max(std_dev / 5.0, 0.5), 1.5); self.volatility = normalized; return normalized

    def update_streak(self, color: str):
        if self.streak_direction == color: self.current_streak += 1
        else: self.streak_direction = color; self.current_streak = 1

    def update_trend_factors(self, levels: list):
        if len(levels) < 20: self.ema_trend_factor = 1.0; self.sr_factor = 1.0; return
        ema20 = self._calculate_single_ema(levels, 20)
        if ema20 is not None and levels:
            current = levels[-1]; diff = (current - ema20) / (abs(ema20) + 1) * 0.2
            self.ema_trend_factor = max(0.8, min(1.2, 1.0 + diff if current > ema20 else 1.0 - abs(diff)))
        sr = find_support_resistance(levels[-30:] if len(levels) > 30 else levels)
        if sr['support'] is not None and sr['resistance'] is not None:
            range_size = sr['resistance'] - sr['support']
            if range_size > 0: pos = (levels[-1] - sr['support']) / range_size; self.sr_factor = max(0.9, min(1.1, 1.0 + (pos - 0.5) * 0.1))
        else: self.sr_factor = 1.0

    def _calculate_single_ema(self, data: list, period: int) -> Optional[float]:
        if len(data) < period: return None
        mult = 2 / (period + 1); prev = sum(data[:period]) / period
        for i in range(period, len(data)): prev = (data[i] * mult) + (prev * (1 - mult))
        return prev

    def calculate_dynamic_threshold(self) -> float:
        vol_factor = self.volatility; streak_factor = 1.0 + min(self.current_streak * 0.02, 0.3); conf_factor = 1.0 - (self.confidence_factor - 0.5) * 0.4
        self.dynamic_threshold = max(0.45, min(0.65, self.base_threshold * vol_factor * streak_factor * conf_factor)); return self.dynamic_threshold

    def update_weights(self):
        self.spins_since_weight_update += 1
        if self.spins_since_weight_update < self.WEIGHT_UPDATE_INTERVAL: return
        self.spins_since_weight_update = 0
        markov_acc = self.markov_correct / max(self.markov_total, 1); ml_acc = self.ml_correct / max(self.ml_total, 1); total_acc = markov_acc + ml_acc
        if total_acc > 0: self.weights["markov"] = markov_acc / total_acc; self.weights["ml"] = ml_acc / total_acc
        self.weights["markov"] = max(0.2, min(0.6, self.weights["markov"])); self.weights["ml"] = max(0.4, min(0.8, self.weights["ml"]))
        total = self.weights["markov"] + self.weights["ml"]; self.weights["markov"] /= total; self.weights["ml"] /= total
        self.markov_correct = self.markov_total = self.ml_correct = self.ml_total = 0

# ─── ESTADÍSTICAS DETALLADAS ─────────────────────────────────────────────────
class DetailedStats:
    def __init__(self):
        self.signal_history: deque = deque(maxlen=50); self.wins_1: int = 0; self.wins_2: int = 0; self.losses: int = 0; self.total_signals: int = 0
        self.last_stats_at: int = 0; self.batch_start_bankroll: Optional[float] = None; self.batch_start_w1: int = 0; self.batch_start_w2: int = 0; self.batch_start_losses: int = 0
        self.last_daily_date = ""; self.daily_start_bankroll: Optional[float] = None; self.daily_w1: int = 0; self.daily_w2: int = 0; self.daily_losses: int = 0

    def record_signal_result(self, attempt_won: int, final_result: bool, bet_amount: float, bankroll: float, category: str):
        self.signal_history.append({"attempt_won": attempt_won, "won": final_result, "bet": bet_amount, "bankroll": bankroll, "timestamp": time.time(), "category": category})
        self.total_signals += 1
        if final_result:
            if attempt_won == 1: self.wins_1 += 1; self.daily_w1 += 1
            elif attempt_won == 2: self.wins_2 += 1; self.daily_w2 += 1
        else: self.losses += 1; self.daily_losses += 1
        if self.daily_start_bankroll is None: self.daily_start_bankroll = bankroll

    def should_send_stats(self) -> bool: return (self.total_signals - self.last_stats_at) >= 20
    def mark_stats_sent(self, bankroll: float): self.last_stats_at = self.total_signals; self.batch_start_bankroll = bankroll; self.batch_start_w1 = self.wins_1; self.batch_start_w2 = self.wins_2; self.batch_start_losses = self.losses
    def get_batch_stats(self, current_bankroll: float) -> dict:
        w1 = self.wins_1 - self.batch_start_w1; w2 = self.wins_2 - self.batch_start_w2; l = self.losses - self.batch_start_losses; wins = w1 + w2; total = wins + l
        bk = round(current_bankroll - self.batch_start_bankroll, 2) if self.batch_start_bankroll is not None else 0.0
        return {"total": total, "wins": wins, "losses": l, "w1": w1, "w2": w2, "efficiency": round(wins / total * 100, 1) if total else 0.0, "e_w1": round(w1 / total * 100, 2) if total else 0.0, "e_w2": round(w2 / total * 100, 2) if total else 0.0, "e_loss": round(l / total * 100, 2) if total else 0.0, "bankroll_delta": bk}
    def get_daily_stats(self, current_bankroll: float) -> dict:
        w1 = self.daily_w1; w2 = self.daily_w2; l = self.daily_losses; wins = w1 + w2; total = wins + l
        bk = round(current_bankroll - self.daily_start_bankroll, 2) if self.daily_start_bankroll is not None else 0.0
        return {"total": total, "wins": wins, "losses": l, "w1": w1, "w2": w2, "efficiency": round(wins / total * 100, 1) if total else 0.0, "e_w1": round(w1 / total * 100, 2) if total else 0.0, "e_w2": round(w2 / total * 100, 2) if total else 0.0, "e_loss": round(l / total * 100, 2) if total else 0.0, "bankroll_delta": bk}
    def reset_daily(self, date_str: str, current_bankroll: float): self.last_daily_date = date_str; self.daily_start_bankroll = current_bankroll; self.daily_w1 = 0; self.daily_w2 = 0; self.daily_losses = 0
    def reset(self): self.signal_history.clear(); self.wins_1 = 0; self.wins_2 = 0; self.losses = 0; self.total_signals = 0; self.last_stats_at = 0; self.batch_start_bankroll = None; self.reset_daily("", 0.0)

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
        self.spin_history: list = []; self.rojo_levels: list = []; self.negro_levels: list = []; self.par_levels: list = []; self.impar_levels: list = []
        self.alto_levels: list = []; self.bajo_levels: list = []; self.d1_levels: list = []; self.d2_levels: list = []; self.d3_levels: list = []
        self.c1_levels: list = []; self.c2_levels: list = []; self.c3_levels: list = []
        self.last_nonzero_color: Optional[str] = None; self.anti_block: set = set()
        self.signal_active: bool = False; self.waiting_for_attempt: bool = False; self.waiting_attempt_number: int = 0; self.skip_one_after_zero: bool = False
        self.active_category: Optional[str] = None; self.bet_value: Optional[str] = None; self.signal_pair: tuple = ()
        self.bet_color: Optional[str] = None; self.attempts_left: int = 0; self.total_attempts: int = 0; self.trigger_number: Optional[int] = None
        self.zero_wait_category: Optional[str] = None; self.zero_wait_bet_value: Optional[str] = None
        self.signal_msg_ids: list = []; self.waiting_msg_id: Optional[int] = None; self.no_confirmation_msg_id: Optional[int] = None; self.result_sequence: deque = deque(maxlen=10)
        self.bet_sys = Martingale(BASE_BET); self.LOSS_COOLDOWN_SPINS: int = 5; self.spins_since_loss: int = 9999
        self.amx_system = AMXSignalSystem(mode="tendencia"); self.min_prob_threshold = cfg.get("min_prob_threshold", 0.60)
        self.unified_prob_system = UnifiedProbabilitySystem()
        self.markov_color = MarkovChainPredictor(window=60, order=2); self.ml_color = MLPatternPredictor(pattern_length=3)
        self.markov_paridad = MarkovChainPredictor(window=60, order=2); self.markov_rango = MarkovChainPredictor(window=60, order=2)
        self.ml_paridad = MLPatternPredictor(pattern_length=3); self.ml_rango = MLPatternPredictor(pattern_length=3)
        self.category_ml = CategoryPredictor(); self.stats = DetailedStats()
        self.ws = None; self.running = True; self._live_conn = _get_live_db()
        self.pending_prediction: Optional[dict] = None; self.pre_alert_threshold: float = 0.50
        self._pretrain_from_db(DB_PATH, self.db_table); live_loaded = self._load_live_history()
        self.ws_spins_count: int = live_loaded; self.warmup_done: bool = live_loaded >= WARMUP_SPINS; self._pending_stats: bool = False

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
            self.markov_color.update([s["real"] for s in temp_history if s["real"]!="VERDE"]); self.ml_color.add_spin([s["real"] for s in temp_history if s["real"]!="VERDE"]); self.category_ml.add_spin(n, real)

    def _load_live_history(self) -> int:
        try:
            cutoff = int(time.time()) - 7 * 86400; cur = self._live_conn.execute("SELECT number FROM live_spins WHERE table_name=? AND ts>=? ORDER BY id ASC", (self.db_table, cutoff)); rows = cur.fetchall()
        except Exception: return 0
        if not rows: return 0
        temp_history = []
        for (n,) in rows: real = REAL_COLOR_MAP.get(n, "VERDE"); temp_history.append({"number": n, "real": real}); self._update_all_predictors(n, real, temp_history)
        return len(rows)

    def _update_all_predictors(self, number: int, real: str, history: list):
        self.markov_color.update([s["real"] for s in history if s["real"]!="VERDE"]); self.ml_color.add_spin([s["real"] for s in history if s["real"]!="VERDE"]); self.category_ml.add_spin(number, real)
        if number != 0:
            par_seq = [s["paridad"] for s in self._paridad_rango_seq if s["paridad"]]; rang_seq = [s["rango"] for s in self._paridad_rango_seq if s["rango"]]
            if par_seq: self.markov_paridad.update(par_seq); self.ml_paridad.add_spin(par_seq)
            if rang_seq: self.markov_rango.update(rang_seq); self.ml_rango.add_spin(rang_seq)

    @property
    def _paridad_rango_seq(self) -> list:
        seq = []
        for s in self.spin_history:
            n = s["number"]
            if n == 0: seq.append({"paridad": None, "rango": None})
            else: seq.append({"paridad": get_paridad(n), "rango": get_rango(n)})
        return seq

    def _persist_spin(self, number: int):
        try: self._live_conn.execute("INSERT INTO live_spins (table_name, number, ts) VALUES (?,?,?)", (self.db_table, number, int(time.time()))); self._live_conn.commit()
        except Exception:
            try: self._live_conn = _get_live_db(); self._live_conn.execute("INSERT INTO live_spins (table_name, number, ts) VALUES (?,?,?)", (self.db_table, number, int(time.time()))); self._live_conn.commit()
            except Exception: pass

    def _cleanup_old_live_spins(self):
        try: cutoff = int(time.time()) - 7 * 86400; self._live_conn.execute("DELETE FROM live_spins WHERE table_name=? AND ts<?", (self.db_table, cutoff)); self._live_conn.commit()
        except Exception: pass

    def set_mode(self, mode: Literal["tendencia", "moderado"]): self.amx_system = AMXSignalSystem(mode=mode)

    def _category_icon(self, value: str) -> str: return CATEGORY_ICONS.get(value, "❓")

    def _trigger_display(self, number: int, category: str) -> str:
        if number == 0: return "0 VERDE 🟢"
        if category == "COLOR": val = REAL_COLOR_MAP.get(number, "VERDE")
        elif category == "PARIDAD": val = get_paridad(number) or "VERDE"
        elif category == "RANGO": val = get_rango(number) or "VERDE"
        elif category == "DOCENA": val = f"D{get_dozen(number)}"
        elif category == "COLUMNA": val = f"C{get_column(number)}"
        else: val = REAL_COLOR_MAP.get(number, "VERDE")
        return f"{number} {val} {self._category_icon(val)}"

    def _is_win(self, number: int, real_color: str) -> Optional[bool]:
        if number == 0: return None
        cat = self.active_category
        if cat == "COLOR": return real_color == self.bet_value
        if cat == "PARIDAD": return get_paridad(number) == self.bet_value
        if cat == "RANGO": return get_rango(number) == self.bet_value
        if cat == "DOCENA":
            result = f"D{get_dozen(number)}"; return result in self.signal_pair if self.signal_pair else result == self.bet_value
        if cat == "COLUMNA":
            result = f"C{get_column(number)}"; return result in self.signal_pair if self.signal_pair else result == self.bet_value
        return False

    def _levels_for(self, category: str, bet_value: str) -> list:
        return {("COLOR","ROJO"): self.rojo_levels, ("COLOR","NEGRO"): self.negro_levels, ("PARIDAD","PAR"): self.par_levels, ("PARIDAD","IMPAR"): self.impar_levels, ("RANGO","ALTO"): self.alto_levels, ("RANGO","BAJO"): self.bajo_levels, ("DOCENA","D1"): self.d1_levels, ("DOCENA","D2"): self.d2_levels, ("DOCENA","D3"): self.d3_levels, ("COLUMNA","C1"): self.c1_levels, ("COLUMNA","C2"): self.c2_levels, ("COLUMNA","C3"): self.c3_levels}.get((category, bet_value), [])

    def _blend_prediction(self, category: str, bet_value: str) -> Optional[dict]:
        cat_pred = self.category_ml.predict_category(category); cat_prob = cat_pred.get(bet_value, 0.5) if cat_pred else 0.5
        markov_prob = 0.5; ml_prob = 0.5
        if category in ("COLOR", "PARIDAD", "RANGO"):
            if category == "COLOR":
                m_seq = [s["real"] for s in self.spin_history if s["real"]!="VERDE"]; m_pred = self.markov_color.predict(m_seq) if len(m_seq) >= self.markov_color.order else None; ml_pred = self.ml_color.predict(m_seq)
            elif category == "PARIDAD":
                seq = [s["paridad"] for s in self._paridad_rango_seq if s["paridad"]]; m_pred = self.markov_paridad.predict(seq) if len(seq) >= self.markov_paridad.order else None; ml_pred = self.ml_paridad.predict(seq)
            else:
                seq = [s["rango"] for s in self._paridad_rango_seq if s["rango"]]; m_pred = self.markov_rango.predict(seq) if len(seq) >= self.markov_rango.order else None; ml_pred = self.ml_rango.predict(seq)
            markov_prob = m_pred.get(bet_value, 0.5) if m_pred else 0.5; ml_prob = ml_pred.get(bet_value, 0.5) if ml_pred else 0.5
        else: markov_prob = None; ml_prob = None
        w = self.unified_prob_system.weights
        if markov_prob is not None and ml_prob is not None: raw_prob = w["markov"] * markov_prob + w["ml"] * ml_prob; raw_prob = 0.6 * raw_prob + 0.4 * cat_prob
        else: raw_prob = cat_prob
        base_prob = max(0.30, min(0.99, raw_prob * self.unified_prob_system.ema_trend_factor * self.unified_prob_system.sr_factor))
        levels = self._levels_for(category, bet_value); ema_sig = self.amx_system.check_signal(levels, bet_value)
        final_prob = min(0.95, base_prob + (0.03 if (ema_sig and ema_sig.get("strength") == "strong") else 0.0))
        return {"combined_prob": final_prob, "markov_prob": markov_prob, "ml_prob": ml_prob, "cat_pattern_prob": cat_prob, "signal_strength": "strong" if final_prob >= 0.70 else "moderate" if final_prob >= 0.60 else "weak", "weights": w, "ema_trend_factor": self.unified_prob_system.ema_trend_factor, "sr_factor": self.unified_prob_system.sr_factor, "volatility": self.unified_prob_system.volatility}

    def _evaluate_category(self, category: str) -> Optional[dict]:
        trigger = self.spin_history[-1]["number"] if self.spin_history else 0
        pred = self.category_ml.predict_category(category)
        if pred is None or pred.get("total", 0) < 5: return None
        clean = {k: v for k, v in pred.items() if k != "total"}
        if not clean: return None
        best_val = max(clean, key=clean.get)
        levels  = self._levels_for(category, best_val); ema_sig = self.amx_system.check_signal(levels, best_val)
        if ema_sig is None: return None
        unified = self._blend_prediction(category, best_val); final_prob = unified["combined_prob"]
        if category == "DOCENA": pair_strs = (f"D{DOZEN_PAIRS[int(best_val[1])][0]}", f"D{DOZEN_PAIRS[int(best_val[1])][1]}")
        elif category == "COLUMNA": pair_strs = (f"C{COLUMN_PAIRS[int(best_val[1])][0]}", f"C{COLUMN_PAIRS[int(best_val[1])][1]}")
        else: pair_strs = ()
        return {"category": category, "bet_value": best_val, "signal_pair": pair_strs, "probability": final_prob, "trigger_number": trigger, "ema_score": ema_sig.get("score", 0), "signal_prob_details": unified}

    def _detect_best_category_signal(self) -> Optional[dict]:
        candidates = []
        for cat in ("COLOR", "PARIDAD", "RANGO", "DOCENA", "COLUMNA"):
            cand = self._evaluate_category(cat)
            if cand: candidates.append(cand)
        return max(candidates, key=lambda x: x["probability"]) if candidates else None

    # ══════════════════════════════════════════════════════════════════════
    # SISTEMA DE PRE‑ALERTA 11° → SIMULACIÓN ACIERTO → 12° ≥ 60%
    # ══════════════════════════════════════════════════════════════════════
    def _evaluate_pre_alert(self) -> Optional[dict]:
        best_11 = self._detect_best_category_signal()
        if best_11 is None or best_11["probability"] < self.pre_alert_threshold: return None
        sim_number = self._get_simulated_number(best_11)
        if sim_number is None: return None
        sim_real = REAL_COLOR_MAP.get(sim_number, "VERDE")
        best_12 = None; best_12_prob = 0.0
        for cat in ("COLOR", "PARIDAD", "RANGO", "DOCENA", "COLUMNA"):
            result = self._simulate_spin12_category(cat, sim_number, sim_real)
            if result and result["probability"] > best_12_prob: best_12 = result; best_12_prob = result["probability"]
        if best_12 is None or best_12_prob < self.min_prob_threshold: return None
        return {"pre_alert_11": best_11, "pre_alert_12": best_12, "sim_number": sim_number}

    def _simulate_spin12_category(self, cat: str, sim_number: int, sim_real: str) -> Optional[dict]:
        PLEN = self.category_ml.PATTERN_LEN; hist = list(self.category_ml._hist.get(cat, []))
        if len(hist) < PLEN: return None
        sim_val = self._sim_category_value(cat, sim_number, sim_real)
        if sim_val is None: return None
        new_pattern = tuple(list(hist[-(PLEN - 1):]) + [sim_val])
        raw_counts = dict(self.category_ml._counts.get(cat, {}).get(new_pattern, {})); total_cat = sum(raw_counts.values())
        if total_cat < 5: return None
        cat_probs = {k: v / total_cat for k, v in raw_counts.items()}; best_val = max(cat_probs, key=cat_probs.get); cat_prob = cat_probs[best_val]
        markov_prob = None; ml_prob = None
        if cat in ("COLOR", "PARIDAD", "RANGO"):
            sim_seq = self._build_simulated_sequence(cat, sim_val)
            if sim_seq is not None: markov_prob = self._markov_predict_simulated(cat, sim_seq, best_val); ml_prob = self._ml_predict_simulated(cat, sim_seq, best_val)
        w = self.unified_prob_system.weights
        if markov_prob is not None and ml_prob is not None: raw = w["markov"] * markov_prob + w["ml"] * ml_prob; raw = 0.6 * raw + 0.4 * cat_prob
        else: raw = cat_prob
        final = max(0.30, min(0.95, raw * self.unified_prob_system.ema_trend_factor * self.unified_prob_system.sr_factor))
        signal_pair = ()
        if cat == "DOCENA" and best_val.startswith("D"): pair = DOZEN_PAIRS[int(best_val[1])]; signal_pair = (f"D{pair[0]}", f"D{pair[1]}")
        elif cat == "COLUMNA" and best_val.startswith("C"): pair = COLUMN_PAIRS[int(best_val[1])]; signal_pair = (f"C{pair[0]}", f"C{pair[1]}")
        return {"category": cat, "bet_value": best_val, "signal_pair": signal_pair, "probability": final, "cat_pattern_prob": cat_prob, "signal_prob_details": {"combined_prob": final, "markov_prob": markov_prob if markov_prob else 0.5, "ml_prob": ml_prob if ml_prob else 0.5, "cat_pattern_prob": cat_prob, "signal_strength": "strong" if final >= 0.70 else "moderate" if final >= 0.60 else "weak", "weights": w, "ema_trend_factor": self.unified_prob_system.ema_trend_factor, "sr_factor": self.unified_prob_system.sr_factor, "volatility": self.unified_prob_system.volatility}}

    def _sim_category_value(self, cat: str, sim_number: int, sim_real: str) -> Optional[str]:
        if cat == "COLOR": return sim_real
        if cat == "PARIDAD": return get_paridad(sim_number)
        if cat == "RANGO": return get_rango(sim_number)
        if cat == "DOCENA": return f"D{get_dozen(sim_number)}"
        if cat == "COLUMNA": return f"C{get_column(sim_number)}"
        return None

    def _get_simulated_number(self, prediction: dict) -> Optional[int]:
        cat, val = prediction["category"], prediction["bet_value"]
        for n in range(1, 37):
            if cat == "COLOR" and REAL_COLOR_MAP.get(n) == val: return n
            if cat == "PARIDAD" and get_paridad(n) == val: return n
            if cat == "RANGO" and get_rango(n) == val: return n
            if cat == "DOCENA" and f"D{get_dozen(n)}" == val: return n
            if cat == "COLUMNA" and f"C{get_column(n)}" == val: return n
        return None

    def _build_simulated_sequence(self, cat: str, sim_val: str) -> Optional[list]:
        if cat == "COLOR": base = [s["real"] for s in self.spin_history if s["real"] != "VERDE"]
        elif cat == "PARIDAD": base = [s["paridad"] for s in self._paridad_rango_seq if s["paridad"]]
        elif cat == "RANGO": base = [s["rango"] for s in self._paridad_rango_seq if s["rango"]]
        else: return None
        return base + [sim_val]

    def _markov_predict_simulated(self, cat: str, sim_seq: list, target_val: str) -> Optional[float]:
        if len(sim_seq) < self.markov_color.order: return None
        state = tuple(sim_seq[-self.markov_color.order:])
        if cat == "COLOR": counts = dict(self.markov_color.transition_counts.get(state, {}))
        elif cat == "PARIDAD": counts = dict(self.markov_paridad.transition_counts.get(state, {}))
        elif cat == "RANGO": counts = dict(self.markov_rango.transition_counts.get(state, {}))
        else: return None
        total = sum(counts.values())
        return counts.get(target_val, 0) / total if total >= 8 else None

    def _ml_predict_simulated(self, cat: str, sim_seq: list, target_val: str) -> Optional[float]:
        if len(sim_seq) < 3: return None
        pattern = tuple(sim_seq[-3:])
        if cat == "COLOR": counts = dict(self.ml_color.pattern_counts.get(pattern, {}))
        elif cat == "PARIDAD": counts = dict(self.ml_paridad.pattern_counts.get(pattern, {}))
        elif cat == "RANGO": counts = dict(self.ml_rango.pattern_counts.get(pattern, {}))
        else: return None
        total = sum(counts.values())
        return counts.get(target_val, 0) / total if total >= 2 else None

    # ── DISEÑO DE PRE‑ALERTA ──────────────────────────────────
    def _send_pre_alert(self, pre_alert_data: dict):
        best_11 = pre_alert_data["pre_alert_11"]; best_12 = pre_alert_data["pre_alert_12"]
        val_11 = best_11["bet_value"]; icon_11 = self._category_icon(val_11)
        if best_12.get("signal_pair"):
            p1, p2 = best_12["signal_pair"]; signal_str = f"{p1} {self._category_icon(p1)} y {p2} {self._category_icon(p2)}"
        else:
            val_12 = best_12["bet_value"]; signal_str = f"{val_12} {self._category_icon(val_12)}"
        text = (f"🚨 <b>ATENCIÓN POSIBLE ENTRADA</b> 🚨\n\n💡 <i>CONFIRMACION: {val_11}</i> {icon_11}\n❄️ <i>POSIBLE SEÑAL EN: {signal_str}</i>")
        msg_id = tg_send_text(self.bot, self.chat_id, self.thread_id, text)
        if msg_id: self.pending_prediction = {"pre_alert_msg_id": msg_id, "pre_alert_11": best_11, "pre_alert_12": best_12, "trigger_number": best_11.get("trigger_number", 0)}

    def _activate_prealert_signal(self, signal_12: Optional[dict], trigger_number: int):
        self.signal_active = True; self.active_category = signal_12["category"]; self.bet_value = signal_12["bet_value"]
        self.signal_pair = signal_12.get("signal_pair", ()); self.bet_color = signal_12["bet_value"] if signal_12["category"] == "COLOR" else "ROJO"
        self.trigger_number = trigger_number; self.total_attempts = MAX_ATTEMPTS; self.attempts_left = MAX_ATTEMPTS
        self._send_signal(1, signal_12["signal_prob_details"]); self.amx_system.register_signal_sent()

    # ══════════════════════════════════════════════════════════════════════
    # EVALUACIÓN INTELIGENTE 2° OPORTUNIDAD (13° GIRO)
    # ══════════════════════════════════════════════════════════════════════
    def _evaluate_2nd_attempt_choice(self) -> Optional[dict]:
        """
        Evalúa si continuar con el mismo valor en la categoría o cambiar al opuesto 
        (o al de mayor probabilidad en Doc/Col) para el 13° giro.
        Retorna el unified_prob del valor elegido.
        """
        cat = self.active_category
        if not cat: return self._blend_prediction(cat, self.bet_value)

        possible_vals = []
        if cat == "COLOR": possible_vals = ["ROJO", "NEGRO"]
        elif cat == "PARIDAD": possible_vals = ["PAR", "IMPAR"]
        elif cat == "RANGO": possible_vals = ["ALTO", "BAJO"]
        elif cat == "DOCENA": possible_vals = ["D1", "D2", "D3"]
        elif cat == "COLUMNA": possible_vals = ["C1", "C2", "C3"]
        else: return self._blend_prediction(cat, self.bet_value)

        best_val = None; best_prob = -1.0; best_pred = None; best_pair = ()

        for val in possible_vals:
            pred = self._blend_prediction(cat, val)
            if pred and pred["combined_prob"] > best_prob:
                best_prob = pred["combined_prob"]; best_val = val; best_pred = pred
                if cat == "DOCENA": pair = DOZEN_PAIRS[int(val[1])]; best_pair = (f"D{pair[0]}", f"D{pair[1]}")
                elif cat == "COLUMNA": pair = COLUMN_PAIRS[int(val[1])]; best_pair = (f"C{pair[0]}", f"C{pair[1]}")
                else: best_pair = ()

        if best_val and best_val != self.bet_value:
            logger.info(f"[{self.name}] 🔄 2° Intento: Cambiando a {cat} -> {best_val} ({best_prob:.0%})")
            self.bet_value = best_val; self.signal_pair = best_pair; self.bet_color = self.bet_value if cat == "COLOR" else "ROJO"
        elif best_val:
            logger.info(f"[{self.name}] 🔄 2° Intento: Manteniendo {cat} -> {best_val} ({best_prob:.0%})")

        return best_pred if best_pred else self._blend_prediction(cat, self.bet_value)

    # ── Mensajes y Resultados ─────────────────────────────────────────
    def _build_signal_text(self, attempt: int, unified_prob: Optional[dict]) -> str:
        bet = self.bet_sys.current_bet(); prob_pct = int((unified_prob["combined_prob"] if unified_prob else 0.5) * 100)
        val_icon = self._category_icon(self.bet_value or ""); trig_disp = self._trigger_display(self.trigger_number, self.active_category or "COLOR")
        nivel_actual = self.bet_sys.level
        if self.signal_pair and self.active_category in ("DOCENA","COLUMNA"):
            p1, p2 = self.signal_pair; i1 = self._category_icon(p1); i2 = self._category_icon(p2)
            apuesta_str = f"<b>{p1}</b> {i1} y <b>{p2}</b> {i2}"
        else: apuesta_str = f"<b>{self.bet_value}</b> {val_icon}"
        return (f"🎯 <b>SEÑAL CONFIRMADA</b> 🎯\n\n🎰 <b>{self.name}</b>\n👉 <b>ÚLTIMO NÚMERO: {trig_disp}</b>\n❄️ <b>ENTRAR EN: {apuesta_str}</b>\n\n💡 <i>PROBABILIDAD IA {prob_pct}%</i>\n📈 <i>NIVEL SECUENCIA: {nivel_actual}/6</i>\n📍 <i>MONTO APUESTA: {bet:.2f} usd</i>\n")

    def _send_signal(self, attempt: int, unified_prob: dict):
        if self.pending_prediction and self.pending_prediction.get("pre_alert_msg_id"): tg_delete(self.bot, self.chat_id, self.pending_prediction["pre_alert_msg_id"]); self.pending_prediction = None
        if self.signal_msg_ids:
            for mid in self.signal_msg_ids: tg_delete(self.bot, self.chat_id, mid)
            self.signal_msg_ids = []
        if self.no_confirmation_msg_id: tg_delete(self.bot, self.chat_id, self.no_confirmation_msg_id); self.no_confirmation_msg_id = None
        text = self._build_signal_text(attempt, unified_prob); msg_id = tg_send_text(self.bot, self.chat_id, self.thread_id, text)
        if msg_id: self.signal_msg_ids.append(msg_id)
        prob_pct = int((unified_prob["combined_prob"] if unified_prob else 0.5) * 100)
        logger.info(f"[{self.name}] 🎯 [{self.active_category}] {self.bet_value} intento={attempt} nivel={self.bet_sys.level} trig={self.trigger_number} prob={prob_pct}%")

    def _send_waiting_message(self, attempt_number: int):
        if self.signal_msg_ids:
            for mid in self.signal_msg_ids: tg_delete(self.bot, self.chat_id, mid)
            self.signal_msg_ids = []
        if self.no_confirmation_msg_id: tg_delete(self.bot, self.chat_id, self.no_confirmation_msg_id); self.no_confirmation_msg_id = None
        logger.info(f"[{self.name}] ⏳ Esperando condiciones intento {attempt_number}")

    def _send_result(self, number: int, real: str, won: bool, bet: float, level_used: int, delete_signals: bool = True):
        if delete_signals and self.signal_msg_ids:
            for mid in self.signal_msg_ids: tg_delete(self.bot, self.chat_id, mid)
            self.signal_msg_ids = []
        if self.no_confirmation_msg_id: tg_delete(self.bot, self.chat_id, self.no_confirmation_msg_id); self.no_confirmation_msg_id = None
        bankroll = self.bet_sys.bankroll; cat_val, cat_icon = self._cat_val(number, real)
        bet_icon = self._category_icon(self.bet_value or "")
        status = f"✅ <b>¡GREEN {number} {cat_val}!</b> {cat_icon}" if won else f"❌ <b>¡LOSS {number} {cat_val}!</b> {cat_icon}"
        text = (f"{status}\n\n❄️ <b>CATEGORIA: {self.bet_value}</b> {bet_icon}\n💰 <i>BANKROLL: {bankroll:.2f} usd</i>\n♻️ <i>NIVEL DE INTENTO {level_used}/6</i>")
        tg_send_text(self.bot, self.chat_id, self.thread_id, text)

    def _cat_val(self, number: int, real: str) -> tuple[str, str]:
        cat = self.active_category or "COLOR"
        if cat == "COLOR": val = real
        elif cat == "PARIDAD": val = get_paridad(number) or "VERDE"
        elif cat == "RANGO": val = get_rango(number) or "VERDE"
        elif cat == "DOCENA": val = f"D{get_dozen(number)}" if number != 0 else "VERDE"
        elif cat == "COLUMNA": val = f"C{get_column(number)}" if number != 0 else "VERDE"
        else: val = real
        return val, self._category_icon(val)

    def _check_stats(self):
        if not self.stats.should_send_stats(): return
        current_bankroll = self.bet_sys.bankroll; self.stats.mark_stats_sent(current_bankroll)
        parts = []; last20 = list(self.stats.signal_history)[-20:]
        if last20:
            lines = ["📊 <b>ESTADISTICAS 20 SEÑALES</b>"]
            for i, entry in enumerate(last20, start=1):
                if entry['won']: line = f"✅ WIN {i}, CATEGORIA {entry['category']}, GALE #{entry['attempt_won']-1}"
                else: line = f"❌ LOSS {i}, CATEGORIA {entry['category']}, GALE #1"
                lines.append(line)
            parts.append("\n".join(lines))
        sd = self.stats.get_daily_stats(current_bankroll)
        if sd['total'] > 0:
            daily_lines = ["📅 <b>ESTADISTICAS 24 HORAS</b>", f"🈯️ TOTAL DE SEÑALES: {sd['total']} = {sd['efficiency']}%", f"1️⃣ GALE #0: {sd['w1']} = {sd['e_w1']}%", f"2️⃣ GALE #1: {sd['w2']} = {sd['e_w2']}%", f"🈲 LOSS: {sd['losses']} = {sd['e_loss']}%", f"💰 CAPITAL ACUMULADO: {sd['bankroll_delta']:.2f} usd"]
            parts.append("\n".join(daily_lines))
        if parts: tg_send_text(self.bot, self.chat_id, self.thread_id, "\n\n".join(parts))

    def _check_daily_report(self):
        import datetime
        tz_ar = datetime.timezone(datetime.timedelta(hours=-3)); now_ar = datetime.datetime.now(tz=tz_ar)
        if now_ar.hour < 12: return
        today_str = now_ar.strftime("%Y-%m-%d")
        if self.stats.last_daily_date == today_str: return
        self.stats.reset_daily(today_str, self.bet_sys.bankroll)

    # ── PROCESAMIENTO PRINCIPAL ──────────────────────────────────────────
    def process_number(self, number: int):
        try: self._process_number_inner(number)
        except Exception as e:
            logger.error(f"[{self.name}] ❌ Error en process_number({number}): {e}", exc_info=True)
            if self.signal_active: self.signal_active = False; self.waiting_for_attempt = False; self.attempts_left = MAX_ATTEMPTS

    def _process_number_inner(self, number: int):
        real = REAL_COLOR_MAP.get(number, "VERDE")
        self._persist_spin(number)
        if len(self.spin_history) > 0 and len(self.spin_history) % 5000 == 0: self._cleanup_old_live_spins()
        self.spin_history.append({"number": number, "real": real})
        if len(self.spin_history) > 300: self.spin_history.pop(0)
        self.result_sequence.append({"number": number, "real": real})

        if number == 0:
            for lst in [self.rojo_levels, self.negro_levels, self.par_levels, self.impar_levels, self.alto_levels, self.bajo_levels, self.d1_levels, self.d2_levels, self.d3_levels, self.c1_levels, self.c2_levels, self.c3_levels]: lst.append(lst[-1] if lst else 0)
        else:
            par = get_paridad(number); rang = get_rango(number); dozen = get_dozen(number); column = get_column(number)
            self.rojo_levels.append((self.rojo_levels[-1] if self.rojo_levels else 0) + (1 if real=="ROJO" else -1)); self.negro_levels.append((self.negro_levels[-1] if self.negro_levels else 0) + (1 if real=="NEGRO" else -1))
            self.par_levels.append((self.par_levels[-1] if self.par_levels else 0) + (1 if par=="PAR" else -1)); self.impar_levels.append((self.impar_levels[-1] if self.impar_levels else 0) + (1 if par=="IMPAR" else -1))
            self.alto_levels.append((self.alto_levels[-1] if self.alto_levels else 0) + (1 if rang=="ALTO" else -1)); self.bajo_levels.append((self.bajo_levels[-1] if self.bajo_levels else 0) + (1 if rang=="BAJO" else -1))
            self.d1_levels.append((self.d1_levels[-1] if self.d1_levels else 0) + (1 if dozen==1 else -1)); self.d2_levels.append((self.d2_levels[-1] if self.d2_levels else 0) + (1 if dozen==2 else -1)); self.d3_levels.append((self.d3_levels[-1] if self.d3_levels else 0) + (1 if dozen==3 else -1))
            self.c1_levels.append((self.c1_levels[-1] if self.c1_levels else 0) + (1 if column==1 else -1)); self.c2_levels.append((self.c2_levels[-1] if self.c2_levels else 0) + (1 if column==2 else -1)); self.c3_levels.append((self.c3_levels[-1] if self.c3_levels else 0) + (1 if column==3 else -1))
            self.last_nonzero_color = real

        self._update_all_predictors(number, real, self.spin_history)
        self.amx_system.update_streak(real, None)
        if real != "VERDE":
            self.unified_prob_system.update_streak(real); ref_lv = self.rojo_levels if real == "ROJO" else self.negro_levels
            self.unified_prob_system.calculate_volatility(ref_lv); self.unified_prob_system.update_trend_factors(ref_lv)
        if not (self.signal_active or self.waiting_for_attempt): self.spins_since_loss += 1
        self.unified_prob_system.update_weights()

        # ══════ COMPROBAR PRE‑ALERTA ANTERIOR ══════
        if self.pending_prediction is not None and not self.signal_active and not self.waiting_for_attempt:
            pred = self.pending_prediction; cat = pred.get("pre_alert_11", {}).get("category"); val = pred.get("pre_alert_11", {}).get("bet_value")
            actual_val = None
            if number != 0:
                if cat == "COLOR": actual_val = REAL_COLOR_MAP.get(number)
                elif cat == "PARIDAD": actual_val = get_paridad(number)
                elif cat == "RANGO": actual_val = get_rango(number)
                elif cat == "DOCENA": actual_val = f"D{get_dozen(number)}"
                elif cat == "COLUMNA": actual_val = f"C{get_column(number)}"
            if actual_val == val:
                if pred.get("pre_alert_msg_id"): tg_delete(self.bot, self.chat_id, pred["pre_alert_msg_id"])
                self.pending_prediction = None
                best_12 = self._detect_best_category_signal()
                if best_12 and best_12["probability"] >= self.min_prob_threshold: self._activate_prealert_signal(best_12, number); return
            else:
                if pred.get("pre_alert_msg_id"): tg_delete(self.bot, self.chat_id, pred["pre_alert_msg_id"])
                self.pending_prediction = None

        # ═══ MÁQUINA DE ESTADOS (SEÑALES ACTIVAS) ═══
        if self.signal_active:
            result = self._is_win(number, real)
            if result is None:
                level_used = self.bet_sys.level; bet = self.bet_sys.loss(); self.attempts_left -= 1
                if self.attempts_left <= 0:
                    self.spins_since_loss = 0; self.stats.record_signal_result(0, False, bet, self.bet_sys.bankroll, self.active_category)
                    self.signal_active = False; self.active_category = None; self.zero_wait_category = None; self.zero_wait_bet_value = None
                    self._send_result(number, real, False, bet, level_used); self._pending_stats = True; self.signal_msg_ids = []; return
                attempt_number = self.total_attempts - self.attempts_left + 1
                self.zero_wait_category = self.active_category; self.zero_wait_bet_value = self.bet_value
                self.signal_active = False; self.waiting_for_attempt = True; self.waiting_attempt_number = attempt_number
                self.skip_one_after_zero = True; self._send_waiting_message(attempt_number); return

            current_attempt = self.total_attempts - self.attempts_left + 1
            if result:
                level_used = self.bet_sys.level; bet = self.bet_sys.win()
                self.stats.record_signal_result(current_attempt, True, bet, self.bet_sys.bankroll, self.active_category)
                self.signal_active = False; self.active_category = None; self.zero_wait_category = None; self.zero_wait_bet_value = None
                self._send_result(number, real, True, bet, level_used); self._pending_stats = True; self.signal_msg_ids = []
            else:
                self.attempts_left -= 1
                if self.attempts_left <= 0: level_used = self.bet_sys.level; self._handle_full_loss(number, real, level_used)
                else:
                    self.bet_sys.loss()
                    best_pred_2nd = self._evaluate_2nd_attempt_choice() # <--- EVALUAR MISMA CATEGORÍA Y VALOR CONTRARIO
                    attempt_number = self.total_attempts - self.attempts_left + 1; self.trigger_number = number
                    if not best_pred_2nd: best_pred_2nd = self._blend_prediction(self.active_category, self.bet_value)
                    self._send_signal(attempt_number, best_pred_2nd)

        elif self.waiting_for_attempt:
            if real == "VERDE": self.skip_one_after_zero = True; return
            if self.skip_one_after_zero: self.skip_one_after_zero = False; return
            if self.zero_wait_category is not None:
                self.active_category = self.zero_wait_category; self.bet_value = self.zero_wait_bet_value
                self.bet_color = self.bet_value if self.active_category == "COLOR" else "ROJO"
                self.zero_wait_category = None; self.zero_wait_bet_value = None; self.trigger_number = number
                best_pred_2nd = self._evaluate_2nd_attempt_choice() if self.waiting_attempt_number == 2 else self._blend_prediction(self.active_category, self.bet_value)
                self.signal_active = True; self.waiting_for_attempt = False; self._send_signal(self.waiting_attempt_number, best_pred_2nd); return

            attempt_number = self.waiting_attempt_number
            if attempt_number == 2:
                best_pred_2nd = self._evaluate_2nd_attempt_choice()
                if best_pred_2nd and best_pred_2nd["combined_prob"] >= self.min_prob_threshold:
                    if self.no_confirmation_msg_id: tg_delete(self.bot, self.chat_id, self.no_confirmation_msg_id); self.no_confirmation_msg_id = None
                    self.trigger_number = number; self.signal_active = True; self.waiting_for_attempt = False; self._send_signal(attempt_number, best_pred_2nd)
                else:
                    if self.no_confirmation_msg_id: tg_delete(self.bot, self.chat_id, self.no_confirmation_msg_id)
                    msg = tg_send_text(self.bot, self.chat_id, self.thread_id, f"🔔 Sin confirmación para enviar señal para el intento 2°")
                    if msg: self.no_confirmation_msg_id = msg
            else:
                best = self._detect_best_category_signal()
                if not best or best["probability"] < self.min_prob_threshold:
                    if self.no_confirmation_msg_id: tg_delete(self.bot, self.chat_id, self.no_confirmation_msg_id)
                    msg = tg_send_text(self.bot, self.chat_id, self.thread_id, f"🔔 Sin confirmación para enviar señal para el intento 1°")
                    if msg: self.no_confirmation_msg_id = msg
                else:
                    unified_prob = best["signal_prob_details"]
                    if unified_prob["combined_prob"] < self.min_prob_threshold:
                        if self.no_confirmation_msg_id: tg_delete(self.bot, self.chat_id, self.no_confirmation_msg_id)
                        msg = tg_send_text(self.bot, self.chat_id, self.thread_id, f"🔔 Sin confirmación para enviar señal para el intento 1°")
                        if msg: self.no_confirmation_msg_id = msg
                    else:
                        if self.no_confirmation_msg_id: tg_delete(self.bot, self.chat_id, self.no_confirmation_msg_id); self.no_confirmation_msg_id = None
                        self.active_category = best["category"]; self.bet_value = best["bet_value"]; self.bet_color = best["bet_value"] if best["category"] == "COLOR" else "ROJO"
                        self.trigger_number = number; self.signal_active = True; self.waiting_for_attempt = False; self._send_signal(attempt_number, unified_prob)
        else:
            self.signal_msg_ids = []
            if self.spins_since_loss >= self.LOSS_COOLDOWN_SPINS and self.pending_prediction is None:
                pre_alert = self._evaluate_pre_alert()
                if pre_alert: self._send_pre_alert(pre_alert)

        if self._pending_stats: self._check_daily_report(); self._check_stats(); self._pending_stats = False
        if not self.warmup_done:
            self.ws_spins_count += 1
            if self.ws_spins_count < WARMUP_SPINS: return
            self.warmup_done = True
            tg_send_text(self.bot, self.chat_id, self.thread_id, f"🟢 <b>{self.name}</b> — Sistema listo. Emitiendo señales.")

    def _handle_full_loss(self, number: int, real: str, level_used: int):
        bet = self.bet_sys.full_loss(); self.spins_since_loss = 0
        self.stats.record_signal_result(0, False, bet, self.bet_sys.bankroll, self.active_category)
        self.signal_active = False; self.active_category = None; self.zero_wait_category = None; self.zero_wait_bet_value = None
        self._send_result(number, real, False, bet, level_used); self._pending_stats = True; self.signal_msg_ids = []

    async def run_ws(self):
        reconnect_delay = 5
        while self.running:
            try:
                async with websockets.connect(WS_URL, ping_interval=30, ping_timeout=60, close_timeout=10) as ws:
                    self.ws = ws; reconnect_delay = 5
                    await ws.send(json.dumps({"type": "subscribe", "casinoId": CASINO_ID, "currency": "USD", "key": [self.ws_key]}))
                    async for message in ws:
                        if not self.running: break
                        try: data = json.loads(message)
                        except Exception: continue
                        if "last20Results" in data and isinstance(data["last20Results"], list):
                            tmp = []
                            for r in data["last20Results"]:
                                gid = r.get("gameId"); num = r.get("result")
                                if gid and num is not None:
                                    try: n = int(num)
                                    except: continue
                                    if 0 <= n <= 36 and gid not in self.anti_block: tmp.append((gid, n)); self.anti_block.add(gid)
                                    if len(self.anti_block) > 1000: self.anti_block.clear()
                            for gid, n in reversed(tmp): self.process_number(n)
                        gid = data.get("gameId"); res = data.get("result")
                        if gid and res is not None:
                            try: n = int(res)
                            except: continue
                            if 0 <= n <= 36 and gid not in self.anti_block:
                                if len(self.anti_block) > 1000: self.anti_block.clear()
                                self.anti_block.add(gid); self.process_number(n)
            except Exception as e:
                try: tg_send_text(self.bot, self.chat_id, self.thread_id, f"⚠️ <b>{self.name}</b> — Conexión perdida. Reconectando...")
                except Exception: pass
                await asyncio.sleep(reconnect_delay); reconnect_delay = min(reconnect_delay * 2, 60)

# ─── FLASK & MAIN ─────────────────────────────────────────────────────────────
app = Flask(__name__)
@app.route("/")
def index(): return jsonify({"status": "ok", "bot": "Roulette Signal Bot Martingala AMX", "ts": time.time()})
@app.route("/ping")
def ping(): return jsonify({"pong": True, "ts": time.time()})

async def self_ping_loop():
    url = os.environ.get("RENDER_EXTERNAL_URL", "").rstrip("/")
    if not url: return
    ping_url = f"{url}/ping"; await asyncio.sleep(30)
    while True:
        try: urllib.request.urlopen(ping_url, timeout=15)
        except: pass
        await asyncio.sleep(240)

engines: dict[str, RouletteEngine] = {}

def _register_handlers(b: telebot.TeleBot):
    @b.message_handler(commands=['start', 'help'])
    def cmd_start(message):
        help_text = """<b>🎰 Roulette Bot AMX — Martingala</b>\nRussian Roulette\n\n<b>Características:</b>\n• Martingala con máx. 2 intentos\n• 2° Intento: Evalúa si mantiene o apusta al valor contrario\n• Solo se emite señal tras confirmación de pre‑alerta (11° giro)\n\nComandos:\n/moderado - Modo moderado\n/tendencia - Modo tendencia\n/status - Estado actual\n/reset - Resetear estadísticas\n/help - Esta ayuda"""
        b.reply_to(message, help_text, parse_mode="HTML")
    @b.message_handler(commands=['moderado'])
    def cmd_moderado(message):
        for engine in engines.values(): engine.set_mode("moderado")
        b.reply_to(message, "✅ <b>Modo MODERADO activado</b>", parse_mode="HTML")
    @b.message_handler(commands=['tendencia'])
    def cmd_tendencia(message):
        for engine in engines.values(): engine.set_mode("tendencia")
        b.reply_to(message, "📈 <b>Modo TENDENCIA activado</b>", parse_mode="HTML")
    @b.message_handler(commands=['status'])
    def cmd_status(message):
        lines = ["<b>📊 ESTADO AMX MARTINGALA</b>\n"]
        for name, engine in engines.items():
            mode_icon = "📈" if engine.amx_system.mode == "tendencia" else "📊"
            if engine.signal_active: st = f"🟢 [{engine.active_category}] {engine.bet_value} nivel {engine.bet_sys.level}/6"
            elif engine.waiting_for_attempt: st = f"⏳ Esperando intento {engine.waiting_attempt_number}/{engine.total_attempts}"
            else:
                if engine.pending_prediction: st = f"🔮 Pre‑alerta pendiente: 11° giro"
                else: st = "⚪ Idle"
            lines.append(f"<b>{name}</b>: {mode_icon} — {st}")
        b.reply_to(message, "\n".join(lines), parse_mode="HTML")
    @b.message_handler(commands=['reset'])
    def cmd_reset(message):
        for engine in engines.values(): engine.stats = DetailedStats(); engine.bet_sys.reset(); engine.pending_prediction = None
        b.reply_to(message, "🔄 <b>Estadísticas y Martingala reseteadas</b>", parse_mode="HTML")

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)

async def main():
    global engines
    engines = {name: RouletteEngine(name, cfg) for name, cfg in ROULETTE_CONFIGS.items()}
    tasks   = [asyncio.create_task(e.run_ws()) for e in engines.values()]
    tasks.append(asyncio.create_task(self_ping_loop()))
    _register_handlers(bot)
    def _poll(b: telebot.TeleBot, label: str): b.polling(none_stop=True, interval=1, timeout=30)
    threading.Thread(target=_poll, args=(bot, "Russian"), daemon=True).start()
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    threading.Thread(target=run_flask, daemon=True).start()
    try: asyncio.run(main())
    except KeyboardInterrupt: pass
