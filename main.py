#!/usr/bin/env python3
"""
Roulette Telegram Signal Bot - Sistema 3 D'Alembert con recuperación +1 ficha
Connects via WebSocket to Pragmatic Play roulettes, detects EMA 4/8/20 signals,
manages D'Alembert bets and sends alerts to Telegram topics.
"""

import asyncio
import io
import json
import logging
import threading
import time
from collections import deque
from typing import Optional

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import telebot
import websockets
from flask import Flask, jsonify

# ─── LOGGING ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] %(levelname)s %(message)s'
)
logger = logging.getLogger("RouletteBot")

# ─── TELEGRAM ─────────────────────────────────────────────────────────────────
TOKEN = "8308452662:AAGZFIZyYsmVR39SvIOSlKD3OY_YNMOsEQU"

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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

# ─── ROULETTE COLOR MAPS ──────────────────────────────────────────────────────
REAL_COLOR_MAP = {
    0:"VERDE",1:"ROJO",2:"NEGRO",3:"ROJO",4:"NEGRO",5:"ROJO",6:"NEGRO",
    7:"ROJO",8:"NEGRO",9:"ROJO",10:"NEGRO",11:"NEGRO",12:"ROJO",13:"NEGRO",
    14:"ROJO",15:"NEGRO",16:"ROJO",17:"NEGRO",18:"ROJO",19:"ROJO",20:"NEGRO",
    21:"ROJO",22:"NEGRO",23:"ROJO",24:"NEGRO",25:"ROJO",26:"NEGRO",27:"ROJO",
    28:"NEGRO",29:"NEGRO",30:"ROJO",31:"NEGRO",32:"ROJO",33:"NEGRO",34:"ROJO",
    35:"NEGRO",36:"ROJO"
}

COLOR_DATA_AUTO = [
    {"id":0,"rojo":0.44,"negro":0.56,"senal":"NEGRO"},
    {"id":1,"rojo":0.52,"negro":0.44,"senal":"ROJO"},
    {"id":2,"rojo":0.40,"negro":0.56,"senal":"NEGRO"},
    {"id":3,"rojo":0.40,"negro":0.56,"senal":"NEGRO"},
    {"id":4,"rojo":0.40,"negro":0.56,"senal":"NEGRO"},
    {"id":5,"rojo":0.52,"negro":0.48,"senal":"ROJO"},
    {"id":6,"rojo":0.40,"negro":0.60,"senal":"NEGRO"},
    {"id":7,"rojo":0.40,"negro":0.56,"senal":"NEGRO"},
    {"id":8,"rojo":0.49,"negro":0.48,"senal":"ROJO"},
    {"id":9,"rojo":0.49,"negro":0.48,"senal":"ROJO"},
    {"id":10,"rojo":0.49,"negro":0.48,"senal":"ROJO"},
    {"id":11,"rojo":0.48,"negro":0.52,"senal":"NEGRO"},
    {"id":12,"rojo":0.40,"negro":0.56,"senal":"NEGRO"},
    {"id":13,"rojo":0.44,"negro":0.56,"senal":"NEGRO"},
    {"id":14,"rojo":0.49,"negro":0.48,"senal":"ROJO"},
    {"id":15,"rojo":0.44,"negro":0.56,"senal":"NEGRO"},
    {"id":16,"rojo":0.52,"negro":0.44,"senal":"ROJO"},
    {"id":17,"rojo":0.36,"negro":0.60,"senal":"NEGRO"},
    {"id":18,"rojo":0.44,"negro":0.52,"senal":"NEGRO"},
    {"id":19,"rojo":0.56,"negro":0.44,"senal":"ROJO"},
    {"id":20,"rojo":0.48,"negro":0.52,"senal":"NEGRO"},
    {"id":21,"rojo":0.56,"negro":0.40,"senal":"ROJO"},
    {"id":22,"rojo":0.52,"negro":0.48,"senal":"ROJO"},
    {"id":23,"rojo":0.48,"negro":0.49,"senal":"NEGRO"},
    {"id":24,"rojo":0.44,"negro":0.52,"senal":"NEGRO"},
    {"id":25,"rojo":0.60,"negro":0.40,"senal":"ROJO"},
    {"id":26,"rojo":0.56,"negro":0.40,"senal":"ROJO"},
    {"id":27,"rojo":0.56,"negro":0.40,"senal":"ROJO"},
    {"id":28,"rojo":0.56,"negro":0.40,"senal":"ROJO"},
    {"id":29,"rojo":0.56,"negro":0.44,"senal":"ROJO"},
    {"id":30,"rojo":0.48,"negro":0.49,"senal":"NEGRO"},
    {"id":31,"rojo":0.48,"negro":0.49,"senal":"NEGRO"},
    {"id":32,"rojo":0.56,"negro":0.44,"senal":"ROJO"},
    {"id":33,"rojo":0.44,"negro":0.52,"senal":"NEGRO"},
    {"id":34,"rojo":0.60,"negro":0.36,"senal":"ROJO"},
    {"id":35,"rojo":0.56,"negro":0.40,"senal":"ROJO"},
    {"id":36,"rojo":0.52,"negro":0.44,"senal":"ROJO"},
]

COLOR_DATA_RUSSIAN = [
    {"id":0,"rojo":0.56,"negro":0.40,"senal":"ROJO"},
    {"id":1,"rojo":0.49,"negro":0.48,"senal":"ROJO"},
    {"id":2,"rojo":0.56,"negro":0.40,"senal":"ROJO"},
    {"id":3,"rojo":0.56,"negro":0.40,"senal":"ROJO"},
    {"id":4,"rojo":0.56,"negro":0.40,"senal":"ROJO"},
    {"id":5,"rojo":0.48,"negro":0.52,"senal":"NEGRO"},
    {"id":6,"rojo":0.48,"negro":0.49,"senal":"NEGRO"},
    {"id":7,"rojo":0.52,"negro":0.48,"senal":"ROJO"},
    {"id":8,"rojo":0.48,"negro":0.49,"senal":"NEGRO"},
    {"id":9,"rojo":0.48,"negro":0.52,"senal":"NEGRO"},
    {"id":10,"rojo":0.52,"negro":0.48,"senal":"ROJO"},
    {"id":11,"rojo":0.48,"negro":0.49,"senal":"NEGRO"},
    {"id":12,"rojo":0.52,"negro":0.44,"senal":"ROJO"},
    {"id":13,"rojo":0.48,"negro":0.52,"senal":"NEGRO"},
    {"id":14,"rojo":0.48,"negro":0.52,"senal":"NEGRO"},
    {"id":15,"rojo":0.56,"negro":0.40,"senal":"ROJO"},
    {"id":16,"rojo":0.48,"negro":0.49,"senal":"NEGRO"},
    {"id":17,"rojo":0.52,"negro":0.44,"senal":"ROJO"},
    {"id":18,"rojo":0.48,"negro":0.52,"senal":"NEGRO"},
    {"id":19,"rojo":0.40,"negro":0.56,"senal":"NEGRO"},
    {"id":20,"rojo":0.48,"negro":0.49,"senal":"NEGRO"},
    {"id":21,"rojo":0.40,"negro":0.56,"senal":"NEGRO"},
    {"id":22,"rojo":0.52,"negro":0.48,"senal":"ROJO"},
    {"id":23,"rojo":0.49,"negro":0.48,"senal":"ROJO"},
    {"id":24,"rojo":0.48,"negro":0.49,"senal":"NEGRO"},
    {"id":25,"rojo":0.44,"negro":0.56,"senal":"NEGRO"},
    {"id":26,"rojo":0.40,"negro":0.56,"senal":"NEGRO"},
    {"id":27,"rojo":0.52,"negro":0.48,"senal":"ROJO"},
    {"id":28,"rojo":0.44,"negro":0.52,"senal":"NEGRO"},
    {"id":29,"rojo":0.48,"negro":0.49,"senal":"NEGRO"},
    {"id":30,"rojo":0.49,"negro":0.48,"senal":"ROJO"},
    {"id":31,"rojo":0.52,"negro":0.48,"senal":"ROJO"},
    {"id":32,"rojo":0.40,"negro":0.56,"senal":"NEGRO"},
    {"id":33,"rojo":0.48,"negro":0.49,"senal":"ROJO"},
    {"id":34,"rojo":0.48,"negro":0.56,"senal":"NEGRO"},
    {"id":35,"rojo":0.44,"negro":0.52,"senal":"NEGRO"},
    {"id":36,"rojo":0.49,"negro":0.48,"senal":"ROJO"},
]

COLOR_DATA_AZURE = [
    {"id":0,"rojo":0.52,"negro":0.44,"senal":"ROJO"},
    {"id":1,"rojo":0.52,"negro":0.48,"senal":"ROJO"},
    {"id":2,"rojo":0.60,"negro":0.40,"senal":"ROJO"},
    {"id":3,"rojo":0.56,"negro":0.40,"senal":"ROJO"},
    {"id":4,"rojo":0.56,"negro":0.40,"senal":"ROJO"},
    {"id":5,"rojo":0.52,"negro":0.48,"senal":"ROJO"},
    {"id":6,"rojo":0.56,"negro":0.40,"senal":"ROJO"},
    {"id":7,"rojo":0.52,"negro":0.44,"senal":"ROJO"},
    {"id":8,"rojo":0.52,"negro":0.44,"senal":"ROJO"},
    {"id":9,"rojo":0.52,"negro":0.44,"senal":"ROJO"},
    {"id":10,"rojo":0.48,"negro":0.48,"senal":"NO APOSTAR"},
    {"id":11,"rojo":0.52,"negro":0.44,"senal":"ROJO"},
    {"id":12,"rojo":0.56,"negro":0.44,"senal":"ROJO"},
    {"id":13,"rojo":0.56,"negro":0.40,"senal":"ROJO"},
    {"id":14,"rojo":0.52,"negro":0.44,"senal":"ROJO"},
    {"id":15,"rojo":0.56,"negro":0.44,"senal":"ROJO"},
    {"id":16,"rojo":0.52,"negro":0.44,"senal":"ROJO"},
    {"id":17,"rojo":0.60,"negro":0.36,"senal":"ROJO"},
    {"id":18,"rojo":0.52,"negro":0.48,"senal":"ROJO"},
    {"id":19,"rojo":0.40,"negro":0.56,"senal":"NEGRO"},
    {"id":20,"rojo":0.44,"negro":0.52,"senal":"NEGRO"},
    {"id":21,"rojo":0.40,"negro":0.56,"senal":"NEGRO"},
    {"id":22,"rojo":0.48,"negro":0.52,"senal":"NEGRO"},
    {"id":23,"rojo":0.48,"negro":0.52,"senal":"NEGRO"},
    {"id":24,"rojo":0.44,"negro":0.52,"senal":"NEGRO"},
    {"id":25,"rojo":0.36,"negro":0.60,"senal":"NEGRO"},
    {"id":26,"rojo":0.44,"negro":0.56,"senal":"NEGRO"},
    {"id":27,"rojo":0.40,"negro":0.56,"senal":"NEGRO"},
    {"id":28,"rojo":0.44,"negro":0.52,"senal":"NEGRO"},
    {"id":29,"rojo":0.44,"negro":0.52,"senal":"NEGRO"},
    {"id":30,"rojo":0.44,"negro":0.52,"senal":"NEGRO"},
    {"id":31,"rojo":0.44,"negro":0.52,"senal":"NEGRO"},
    {"id":32,"rojo":0.44,"negro":0.52,"senal":"NEGRO"},
    {"id":33,"rojo":0.48,"negro":0.52,"senal":"NEGRO"},
    {"id":34,"rojo":0.40,"negro":0.60,"senal":"NEGRO"},
    {"id":35,"rojo":0.40,"negro":0.56,"senal":"NEGRO"},
    {"id":36,"rojo":0.44,"negro":0.56,"senal":"NEGRO"},
]

# ─── ROULETTE CONFIGS ─────────────────────────────────────────────────────────
ROULETTE_CONFIGS = {
    "Auto Roulette": {
        "ws_key": 225,
        "chat_id": -1003835197023,
        "thread_id": 2,
        "color_data": COLOR_DATA_AUTO,
        "betting_system": "dalembert",
    },
    "Russian Roulette": {
        "ws_key": 221,
        "chat_id": -1003835197023,
        "thread_id": 7,
        "color_data": COLOR_DATA_RUSSIAN,
        "betting_system": "dalembert",
    },
    "Azure Roulette 1": {
        "ws_key": 227,
        "chat_id": -1003835197023,
        "thread_id": 6,
        "color_data": COLOR_DATA_AZURE,
        "betting_system": "dalembert",
    },
}

WS_URL    = "wss://dga.pragmaticplaylive.net/ws"
CASINO_ID = "ppcjd00000007254"
MAX_ATTEMPTS = 2
BASE_BET  = 0.10   # USD
VISIBLE   = 40

# ─── D'ALEMBERT (SISTEMA 3) ──────────────────────────────────────────────────
class D_Alembert:
    def __init__(self, base: float):
        self.base      = base
        self.step      = 0
        self.bankroll  = 0.0
        self.max_step  = 20

    def current_bet(self) -> float:
        return round(self.base * (self.step + 1), 2)

    def win(self) -> float:
        bet = self.current_bet()
        self.bankroll = round(self.bankroll + bet, 2)
        if self.step > 0:
            self.step -= 1
        return bet

    def loss(self) -> float:
        bet = self.current_bet()
        self.bankroll = round(self.bankroll - bet, 2)
        if self.step >= self.max_step - 1:
            self.step = 0
        else:
            self.step += 1
        return bet


# ─── STATISTICS ───────────────────────────────────────────────────────────────
class Stats:
    def __init__(self):
        self.total      = 0
        self.wins       = 0
        self.losses     = 0
        self.last_stats_at = 0
        self._h24: deque = deque()
        self.batch_start_bankroll = None
        self._wins_at_last_batch = 0

    def record(self, is_win: bool, bankroll: float):
        self.total += 1
        if is_win:
            self.wins += 1
        else:
            self.losses += 1
        now = time.time()
        self._h24.append((now, is_win, bankroll))
        self._trim24()

    def _trim24(self):
        cutoff = time.time() - 86400
        while self._h24 and self._h24[0][0] < cutoff:
            self._h24.popleft()

    def should_send_stats(self) -> bool:
        return (self.total - self.last_stats_at) >= 20

    def mark_stats_sent(self, bankroll: float):
        self.last_stats_at = self.total
        self.batch_start_bankroll = bankroll
        self._wins_at_last_batch = self.wins

    def batch_stats(self, current_bankroll: float):
        n = self.total - self.last_stats_at
        w = self.wins - self._wins_at_last_batch
        l = n - w
        e = round(w / n * 100, 1) if n else 0.0
        if self.batch_start_bankroll is not None:
            batch_bankroll = round(current_bankroll - self.batch_start_bankroll, 2)
        else:
            batch_bankroll = 0.0
        return w, l, n, e, batch_bankroll

    def stats_24h(self, current_bankroll: float):
        self._trim24()
        t = len(self._h24)
        w = sum(1 for _, iw, _ in self._h24 if iw)
        l = t - w
        e = round(w / t * 100, 1) if t else 0.0
        if t >= 2:
            first_bankroll = self._h24[0][2]
            last_bankroll  = self._h24[-1][2]
            bk24 = round(last_bankroll - first_bankroll, 2)
        else:
            bk24 = 0.0
        return w, l, t, e, bk24


# ─── CHART GENERATION ─────────────────────────────────────────────────────────
def generate_chart(levels: list, spin_history: list, bet_color: str, visible: int = VISIBLE) -> io.BytesIO:
    arr = np.array(levels, dtype=float)
    n   = len(arr)

    def calc_ema(data, period):
        if len(data) < period:
            return np.full(len(data), np.nan)
        mult = 2 / (period + 1)
        out = np.full(len(data), np.nan)
        out[period - 1] = np.mean(data[:period])
        for i in range(period, len(data)):
            out[i] = (data[i] - out[i - 1]) * mult + out[i - 1]
        return out

    ema4 = calc_ema(arr, 4)
    ema8 = calc_ema(arr, 8)
    ema20 = calc_ema(arr, 20)

    start = max(0, n - visible)
    sl = slice(start, n)
    x = np.arange(len(arr[sl]))
    hist_sl = spin_history[start:]

    is_rojo = bet_color == "ROJO"
    bg = "#0b101f"
    ax_bg = "#0f1a2a"
    grid_c = "#1e2e48"
    line_c = "#e84040" if is_rojo else "#9090bb"
    ema4_c = "#ff9f43"
    ema8_c = "#48dbfb"
    ema20_c = "#1dd1a1"
    title_c = "#ff8080" if is_rojo else "#b0b8d0"

    fig, ax = plt.subplots(figsize=(8, 3.6), facecolor=bg)
    ax.set_facecolor(ax_bg)

    y = arr[sl]
    e4 = ema4[sl]
    e8 = ema8[sl]
    e20 = ema20[sl]

    ax.fill_between(x, y, alpha=0.10, color=line_c)
    ax.plot(x, y,   color=line_c,  linewidth=0.8, zorder=3)
    ax.plot(x, e4,  color=ema4_c,  linewidth=0.7, linestyle="--", label="EMA 4",  zorder=4)
    ax.plot(x, e8,  color=ema8_c,  linewidth=0.7, linestyle="--", label="EMA 8",  zorder=4)
    ax.plot(x, e20, color=ema20_c, linewidth=1.0, label="EMA 20", zorder=4)

    dot_colors = {"ROJO": "#e84040", "NEGRO": "#aaaacc", "VERDE": "#2ecc71"}
    for i, spin in enumerate(hist_sl):
        c = dot_colors.get(spin["real"], "#ffffff")
        ax.scatter(i, y[i], color=c, s=22, zorder=5, edgecolors="white", linewidths=0.3)

    tick_step = max(1, len(x) // 8)
    tick_x = list(range(0, len(x), tick_step))
    tick_lbs = [str(hist_sl[i]["number"]) if i < len(hist_sl) else "" for i in tick_x]
    ax.set_xticks(tick_x)
    ax.set_xticklabels(tick_lbs, color="#8899bb", fontsize=7)
    ax.tick_params(axis='y', colors="#8899bb", labelsize=7)
    ax.tick_params(axis='x', colors="#8899bb", labelsize=7)

    ax.spines['bottom'].set_color(grid_c)
    ax.spines['left'].set_color(grid_c)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.grid(axis='y', color=grid_c, linewidth=0.4, alpha=0.5)

    emoji = "🔴" if is_rojo else "⚫️"
    ax.set_title(f"{emoji} Señal {'ROJO' if is_rojo else 'NEGRO'} — últimos {visible} giros · EMA 4/8/20",
                 color=title_c, fontsize=9, pad=6)

    from matplotlib.lines import Line2D
    legend_els = [
        Line2D([0],[0], color=line_c,  linewidth=0.8, label="Nivel"),
        Line2D([0],[0], color=ema4_c,  linewidth=0.7, linestyle="--", label="EMA 4"),
        Line2D([0],[0], color=ema8_c,  linewidth=0.7, linestyle="--", label="EMA 8"),
        Line2D([0],[0], color=ema20_c, linewidth=1.0, label="EMA 20"),
        Line2D([0],[0], marker='o', color='w', markerfacecolor='#e84040', markersize=5, label="Rojo"),
        Line2D([0],[0], marker='o', color='w', markerfacecolor='#aaaacc', markersize=5, label="Negro"),
        Line2D([0],[0], marker='o', color='w', markerfacecolor='#2ecc71', markersize=5, label="Verde"),
    ]
    ax.legend(handles=legend_els, loc="upper left", fontsize=6.5,
              facecolor="#0b101f", edgecolor=grid_c, labelcolor="white", framealpha=0.8, ncol=2)

    plt.tight_layout(pad=0.8)
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=120, facecolor=bg)
    plt.close(fig)
    buf.seek(0)
    return buf


# ─── TELEGRAM HELPERS ─────────────────────────────────────────────────────────
_TG_MAX_RETRIES = 5

def _tg_call(fn, *args, **kwargs):
    delay = 2.0
    for attempt in range(1, _TG_MAX_RETRIES + 1):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            err = str(e)
            if "retry after" in err.lower():
                try:
                    wait = int(''.join(filter(str.isdigit, err))) + 1
                except Exception:
                    wait = 30
                logger.warning(f"Telegram flood-wait {wait}s")
                time.sleep(wait)
                continue
            logger.warning(f"Telegram error (attempt {attempt}/{_TG_MAX_RETRIES}): {e}")
            if attempt < _TG_MAX_RETRIES:
                time.sleep(delay)
                delay = min(delay * 2, 60)
            else:
                logger.error(f"Telegram call failed after {_TG_MAX_RETRIES} attempts: {e}")
                return None

def tg_send_photo(chat_id: int, thread_id: int, photo_buf: io.BytesIO, caption: str) -> Optional[int]:
    photo_buf.seek(0)
    msg = _tg_call(bot.send_photo, chat_id=chat_id, photo=photo_buf, caption=caption,
                   parse_mode="HTML", message_thread_id=thread_id)
    return msg.message_id if msg else None

def tg_send_text(chat_id: int, thread_id: int, text: str) -> Optional[int]:
    msg = _tg_call(bot.send_message, chat_id=chat_id, text=text,
                   parse_mode="HTML", message_thread_id=thread_id)
    return msg.message_id if msg else None

def tg_delete(chat_id: int, msg_id: int):
    _tg_call(bot.delete_message, chat_id=chat_id, message_id=msg_id)


# ─── ROULETTE ENGINE ──────────────────────────────────────────────────────────
class RouletteEngine:
    def __init__(self, name: str, cfg: dict):
        self.name      = name
        self.ws_key    = cfg["ws_key"]
        self.chat_id   = cfg["chat_id"]
        self.thread_id = cfg["thread_id"]
        self.color_data: list = cfg["color_data"]

        self.spin_history:     list = []
        self.original_levels:  list = []
        self.inverted_levels:  list = []
        self.last_nonzero_color: Optional[str] = None
        self.anti_block: set = set()

        self.signal_active:    bool = False
        self.expected_color:   Optional[str] = None
        self.bet_color:        Optional[str] = None
        self.attempts_left:    int = 0
        self.total_attempts:   int = 0
        self.trigger_number:   Optional[int] = None

        self.result_until:     float = 0.0
        self.consec_losses:    int = 0          # Nivel de pérdidas (0..9)
        # Nuevas variables para la recuperación +1 ficha
        self.recovery_active:  bool  = False
        self.recovery_target:  float = 0.0
        # Bankroll al inicio de cada señal de nivel 1
        self.level1_bankroll:  float = 0.0
        # ¿La señal activa fue disparada en nivel 1?
        self.signal_is_level1: bool  = False

        self.betting_system_name = cfg.get("betting_system", "dalembert")
        self.bet_sys = D_Alembert(BASE_BET)

        self.stats = Stats()
        self.signal_msg_id: Optional[int] = None
        self.ws = None
        self.running = True

    @staticmethod
    def calculate_ema(data: list, period: int) -> list:
        if len(data) < period:
            return [None] * len(data)
        mult = 2 / (period + 1)
        out = [None] * (period - 1)
        prev = sum(data[:period]) / period
        out.append(prev)
        for i in range(period, len(data)):
            prev = (data[i] - prev) * mult + prev
            out.append(prev)
        return out

    def get_entry(self, number: int) -> Optional[dict]:
        for e in self.color_data:
            if e["id"] == number:
                return e
        return None

    def get_signal(self, number: int) -> Optional[str]:
        e = self.get_entry(number)
        return e["senal"] if e else None

    def get_prob(self, number: int, color: str) -> float:
        e = self.get_entry(number)
        if not e:
            return 0.0
        return e["rojo"] if color == "ROJO" else e["negro"]

    def determine_bet_color(self, expected: str) -> str:
        if len(self.spin_history) < 20:
            return expected
        ema20o = self.calculate_ema(self.original_levels, 20)
        ema20i = self.calculate_ema(self.inverted_levels, 20)
        li = len(self.original_levels) - 1
        last_sig = self.get_signal(self.spin_history[-1]["number"])
        if expected == "ROJO":
            if ema20o[li] is not None and self.original_levels[li] < ema20o[li]:
                return "NEGRO" if last_sig == "NEGRO" else "ROJO"
            return "ROJO"
        else:
            if ema20i[li] is not None and self.inverted_levels[li] < ema20i[li]:
                return "ROJO" if last_sig == "ROJO" else "NEGRO"
            return "NEGRO"

    def should_activate(self) -> Optional[str]:
        # Ya no hay bloqueo temporal; solo se verifica el nivel de pérdidas
        losses = self.consec_losses
        min_spin = 22 + losses * 2
        if len(self.spin_history) < min_spin:
            return None

        last_num = self.spin_history[-1]["number"]
        entry = self.get_entry(last_num)
        if not entry or entry["senal"] == "NO APOSTAR":
            return None
        expected = entry["senal"]

        ema4o = self.calculate_ema(self.original_levels, 4)
        ema8o = self.calculate_ema(self.original_levels, 8)
        ema20o = self.calculate_ema(self.original_levels, 20)
        ema4i = self.calculate_ema(self.inverted_levels, 4)
        ema8i = self.calculate_ema(self.inverted_levels, 8)
        ema20i = self.calculate_ema(self.inverted_levels, 20)

        req = min(3 + losses, 13)
        li = len(self.original_levels) - 1

        def check(levels, e20, e8, e4, idx):
            for off in range(req):
                i = idx - (req - 1) + off
                if i < 0:
                    return False
                if e20[i] is None or levels[i] <= e20[i]:
                    return False
                if losses >= 2 and e8[i] is not None and levels[i] <= e8[i]:
                    return False
                if losses >= 4 and e4[i] is not None and levels[i] <= e4[i]:
                    return False
            return True

        if expected == "ROJO":
            if check(self.original_levels, ema20o, ema8o, ema4o, li):
                return "ROJO"
        elif expected == "NEGRO":
            if check(self.inverted_levels, ema20i, ema8i, ema4i, li):
                return "NEGRO"
        return None

    def _check_recovery(self):
        """Verifica si se alcanzó recovery_target y resetea a nivel 1."""
        if not self.recovery_active:
            return
        if self.bet_sys.bankroll >= self.recovery_target:
            logger.info(
                f"[{self.name}] Recuperación completada! "
                f"bankroll={self.bet_sys.bankroll:.2f} >= objetivo={self.recovery_target:.2f}. "
                f"Reseteando a nivel 1."
            )
            self.consec_losses    = 0
            self.recovery_active  = False
            self.recovery_target  = 0.0
            self.bet_sys.step     = 0   # vuelve al nivel 1 de D'Alembert

    def process_number(self, number: int):
        real = REAL_COLOR_MAP.get(number, "VERDE")
        self.spin_history.append({"number": number, "real": real})
        if len(self.spin_history) > 300:
            self.spin_history.pop(0)

        last_o = self.original_levels[-1] if self.original_levels else 0
        last_i = self.inverted_levels[-1] if self.inverted_levels else 0

        if number == 0:
            if self.last_nonzero_color:
                self.original_levels.append(last_o + (1 if self.last_nonzero_color == "ROJO" else -1))
                self.inverted_levels.append(last_i + (1 if self.last_nonzero_color == "NEGRO" else -1))
            else:
                self.original_levels.append(last_o)
                self.inverted_levels.append(last_i)
        else:
            self.original_levels.append(last_o + (1 if real == "ROJO" else -1))
            self.inverted_levels.append(last_i + (1 if real == "NEGRO" else -1))
            self.last_nonzero_color = real

        while len(self.original_levels) > len(self.spin_history):
            self.original_levels.pop(0)
        while len(self.inverted_levels) > len(self.spin_history):
            self.inverted_levels.pop(0)

        # ── Resolve active signal ─────────────────────────────────────────────
        if self.signal_active and time.time() > self.result_until:
            is_win = (self.bet_color == "ROJO" and real == "ROJO") or \
                     (self.bet_color == "NEGRO" and real == "NEGRO")
            if is_win:
                bet = self.bet_sys.win()
                self.stats.record(True, self.bet_sys.bankroll)
                self.signal_active = False
                # Al ganar, se comprueba si se ha completado la recuperación
                self._check_recovery()
                self._send_result(number, real, True, bet)
                self._check_stats()
            else:
                self.attempts_left -= 1
                bet = self.bet_sys.loss()
                if self.attempts_left <= 0:
                    # Pérdida definitiva del segundo intento → aumentar nivel y activar recuperación
                    self.consec_losses += 1
                    if self.consec_losses >= 10:
                        # Máximo nivel alcanzado: reset total
                        self.consec_losses = 0
                        self.recovery_active = False
                        self.recovery_target = 0.0
                        logger.info(f"[{self.name}] Max 10 losses → nivel reiniciado, bankroll {self.bet_sys.bankroll:.2f}")
                    else:
                        # Activar modo recuperación
                        # target = bankroll registrado en nivel 1 + 1 ficha positiva
                        self.recovery_active  = True
                        if self.signal_is_level1:
                            self.recovery_target = self.level1_bankroll + BASE_BET
                        else:
                            # Si la señal no era nivel 1 pero hubo pérdida,
                            # usar el último level1_bankroll conocido + 1 ficha
                            self.recovery_target = self.level1_bankroll + BASE_BET
                        logger.info(
                            f"[{self.name}] Pérdida nivel {self.consec_losses}. "
                            f"Modo recuperación activado. "
                            f"level1_bankroll={self.level1_bankroll:.2f} "
                            f"objetivo={self.recovery_target:.2f}"
                        )
                    self.stats.record(False, self.bet_sys.bankroll)
                    self.signal_active = False
                    self._send_result(number, real, False, bet)
                    self._check_stats()
                else:
                    # Primer intento fallido → pasar al segundo sin cambios en nivel
                    self.trigger_number = number
                    new_bet = self.bet_sys.current_bet()
                    self._send_retry_signal(number, new_bet)

        # ── Activate new signal ───────────────────────────────────────────────
        if not self.signal_active and time.time() > self.result_until:
            expected = self.should_activate()
            if expected:
                self.signal_active = True
                self.expected_color = expected
                self.bet_color = self.determine_bet_color(expected)
                self.attempts_left = MAX_ATTEMPTS
                self.total_attempts = MAX_ATTEMPTS
                self.trigger_number = number
                self._send_signal(number, 1)

    # ── Telegram: send initial signal ─────────────────────────────────────────
    def _send_signal(self, trigger: int, attempt: int):
        bet = self.bet_sys.current_bet()
        prob = int(self.get_prob(trigger, self.bet_color) * 100)
        color_icon = "🔴" if self.bet_color == "ROJO" else "⚫️"
        step = self.bet_sys.step + 1

        # Registrar bankroll cuando se activa en nivel 1 (step 0, sin recuperación)
        self.signal_is_level1 = (self.bet_sys.step == 0 and not self.recovery_active)
        if self.signal_is_level1:
            self.level1_bankroll = self.bet_sys.bankroll
            logger.info(f"[{self.name}] Señal nivel 1 — bankroll registrado: {self.level1_bankroll:.2f}")

        recovery_tag = " 🔄" if self.recovery_active else ""
        sys_line = f"🌀 <i>D'Alembert paso {step} de 20{recovery_tag}</i>\n"
        caption = (
            f"✅☑️ <b>SEÑAL CONFIRMADA</b> ☑️✅\n\n"
            f"🎰 <b>Juego: {self.name}</b>\n"
            f"👉 <b>Después de: {trigger}</b>\n"
            f"🎯 <b>Apostar a: {self.bet_color}</b> {color_icon}\n\n"
            f"💡 <i>Probabilidad de señal: {prob}%</i>\n"
            f"{sys_line}"
            f"📍 <i>Apuesta: {bet:.2f} usd</i>\n\n"
            f"♻️ <i>Intento {attempt}/{MAX_ATTEMPTS}</i>"
        )
        levels = self.original_levels[:] if self.bet_color == "ROJO" else self.inverted_levels[:]
        chart = generate_chart(levels, self.spin_history[:], self.bet_color)
        msg_id = tg_send_photo(self.chat_id, self.thread_id, chart, caption)
        self.signal_msg_id = msg_id
        logger.info(f"[{self.name}] Signal sent: {self.bet_color} after {trigger}, bet={bet:.2f}, step={step}, recovery={self.recovery_active}")

    # ── Telegram: send retry signal (segundo intento) ─────────────────────────
    def _send_retry_signal(self, trigger: int, new_bet: float):
        if self.signal_msg_id:
            tg_delete(self.chat_id, self.signal_msg_id)
            self.signal_msg_id = None
        prob = int(self.get_prob(trigger, self.bet_color) * 100)
        color_icon = "🔴" if self.bet_color == "ROJO" else "⚫️"
        step = self.bet_sys.step + 1
        sys_line = f"🌀 <i>D'Alembert paso {step} de 20</i>\n"
        recovery_note = " 🔄 (modo recuperación)" if self.recovery_active else ""
        caption = (
            f"✅☑️ <b>SEÑAL CONFIRMADA</b> ☑️✅\n\n"
            f"🎰 <b>Juego: {self.name}</b>\n"
            f"👉🏼 <b>Después de: {trigger}</b>\n"
            f"🎯 <b>Apostar a: {self.bet_color}</b> {color_icon}\n\n"
            f"💡 <i>Probabilidad de señal: {prob}%</i>\n"
            f"{sys_line}"
            f"📍 <i>Apuesta: {new_bet:.2f} usd</i>\n\n"
            f"♻️ <i>Intento 2/{MAX_ATTEMPTS}</i>"
        )
        levels = self.original_levels[:] if self.bet_color == "ROJO" else self.inverted_levels[:]
        chart = generate_chart(levels, self.spin_history[:], self.bet_color)
        msg_id = tg_send_photo(self.chat_id, self.thread_id, chart, caption)
        self.signal_msg_id = msg_id
        logger.info(f"[{self.name}] Retry signal sent: {self.bet_color} after {trigger}, bet={new_bet:.2f}")

    def _send_result(self, number: int, real: str, won: bool, bet: float):
        bankroll = self.bet_sys.bankroll
        icon = "🔴" if real == "ROJO" else ("⚫️" if real == "NEGRO" else "🟢")
        if won:
            text = f"💎 <b>RESULTADO: {number}</b> {icon}\n💰 <i>Bankroll Actual: {bankroll:.2f} usd</i>"
        else:
            text = f"❌ <b>RESULTADO: {number}</b> {icon}\n💰 <i>Bankroll Actual: {bankroll:.2f} usd</i>"
        self.result_until = time.time() + 7.0
        tg_send_text(self.chat_id, self.thread_id, text)
        logger.info(f"[{self.name}] Result: {'WIN' if won else 'LOSS'} #{number}, bankroll={bankroll:.2f}")

    def _check_stats(self):
        if not self.stats.should_send_stats():
            return
        current_bankroll = self.bet_sys.bankroll
        w20, l20, t20, e20, batch_bankroll = self.stats.batch_stats(current_bankroll)
        self.stats.mark_stats_sent(current_bankroll)
        w24, l24, t24, e24, bk24 = self.stats.stats_24h(current_bankroll)
        text = (
            f"👉🏼 <b>ESTADÍSTICAS {t20} SEÑALES</b>\n"
            f"🈯️ <b>W: {w20}</b> 🈲 <b>L: {l20}</b> 🈺 <b>T: {t20}</b> "
            f"📈 <b>E: {e20}%</b>\n"
            f"💰 <i>Bankroll acumulado: {batch_bankroll:.2f} usd</i>\n\n"
            f"👉🏼 <b>ESTADÍSTICAS 24 HORAS</b>\n"
            f"🈯️ <b>W: {w24}</b> 🈲 <b>L: {l24}</b> 🈺 <b>T: {t24}</b> "
            f"📈 <b>E: {e24}%</b>\n"
            f"💰 <i>Bankroll acumulado: {bk24:.2f} usd</i>"
        )
        tg_send_text(self.chat_id, self.thread_id, text)
        logger.info(f"[{self.name}] Stats sent: {t20} signals")

    async def run_ws(self):
        reconnect_delay = 5
        while self.running:
            try:
                async with websockets.connect(WS_URL, ping_interval=30, ping_timeout=60, close_timeout=10) as ws:
                    self.ws = ws
                    reconnect_delay = 5
                    logger.info(f"[{self.name}] WS connected")
                    await ws.send(json.dumps({
                        "type": "subscribe",
                        "casinoId": CASINO_ID,
                        "currency": "USD",
                        "key": [self.ws_key]
                    }))
                    async for message in ws:
                        if not self.running:
                            break
                        try:
                            data = json.loads(message)
                        except Exception:
                            continue
                        if "last20Results" in data and isinstance(data["last20Results"], list):
                            tmp = []
                            for r in data["last20Results"]:
                                gid = r.get("gameId")
                                num = r.get("result")
                                if gid and num is not None:
                                    try:
                                        n = int(num)
                                    except Exception:
                                        continue
                                    if 0 <= n <= 36 and gid not in self.anti_block:
                                        tmp.append((gid, n))
                                        if len(self.anti_block) > 1000:
                                            self.anti_block.clear()
                                        self.anti_block.add(gid)
                            for gid, n in reversed(tmp):
                                self.process_number(n)
                        gid = data.get("gameId")
                        res = data.get("result")
                        if gid and res is not None:
                            try:
                                n = int(res)
                            except Exception:
                                continue
                            if 0 <= n <= 36 and gid not in self.anti_block:
                                if len(self.anti_block) > 1000:
                                    self.anti_block.clear()
                                self.anti_block.add(gid)
                                self.process_number(n)
            except Exception as e:
                logger.warning(f"[{self.name}] WS error: {e}. Reconnecting in {reconnect_delay}s")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 60)


# ─── FLASK KEEPALIVE ──────────────────────────────────────────────────────────
app = Flask(__name__)

@app.route("/")
def index():
    return jsonify({"status": "ok", "bot": "Roulette Signal Bot", "ts": time.time()})

@app.route("/ping")
def ping():
    return jsonify({"pong": True, "ts": time.time()})

@app.route("/health")
def health():
    return jsonify({"healthy": True})


# ─── SELF-PING TASK ──────────────────────────────────────────────────────────
import os
import urllib.request

async def self_ping_loop():
    port = int(os.environ.get("PORT", 10000))
    url = os.environ.get("RENDER_EXTERNAL_URL", f"http://localhost:{port}")
    ping_url = f"{url}/ping"
    while True:
        await asyncio.sleep(300)
        try:
            with urllib.request.urlopen(ping_url, timeout=10) as r:
                logger.info(f"Self-ping OK: {r.status}")
        except Exception as e:
            logger.warning(f"Self-ping failed: {e}")


# ─── MAIN ────────────────────────────────────────────────────────────────────
def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)


async def main():
    engines = [RouletteEngine(name, cfg) for name, cfg in ROULETTE_CONFIGS.items()]
    tasks = [asyncio.create_task(e.run_ws()) for e in engines]
    tasks.append(asyncio.create_task(self_ping_loop()))
    logger.info("All roulette engines started (Sistema 3: D'Alembert con recuperación +1 ficha).")
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    logger.info("Flask server started.")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped.")
