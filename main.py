#!/usr/bin/env python3
"""
Roulette Telegram Signal Bot - Sistema 4 Labouchère
Connects via WebSocket to Pragmatic Play roulettes, detects EMA 4/8/20 signals,
manages Labouchère bets and sends alerts to Telegram topics.
"""

import asyncio
import io
import json
import logging
import threading
import time
from collections import deque
from datetime import datetime, timedelta
from typing import Optional

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
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

# Use a custom requests Session with automatic retries and connection pooling
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

_session = requests.Session()
_retry = Retry(
    total=5,
    backoff_factor=1.5,         # waits 0s, 1.5s, 3s, 6s, 12s between retries
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "POST"],
    raise_on_status=False,
)
_adapter = HTTPAdapter(max_retries=_retry, pool_connections=10, pool_maxsize=20)
_session.mount("https://", _adapter)
_session.mount("http://",  _adapter)

bot = telebot.TeleBot(TOKEN, threaded=False)
# Inject our resilient session into telebot's internal requester
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
        "betting_system": "fibonacci",   # Sistema 2
    },
    "Russian Roulette": {
        "ws_key": 221,
        "chat_id": -1003835197023,
        "thread_id": 7,
        "color_data": COLOR_DATA_RUSSIAN,
        "betting_system": "fibonacci",   # Sistema 2
    },
    "Azure Roulette 1": {
        "ws_key": 227,
        "chat_id": -1003835197023,
        "thread_id": 6,
        "color_data": COLOR_DATA_AZURE,
        "betting_system": "labouchere",  # Sistema 4
    },
}

WS_URL    = "wss://dga.pragmaticplaylive.net/ws"
CASINO_ID = "ppcjd00000007254"
MAX_ATTEMPTS = 2
LABOUCHERE_SEQ = [1, 2, 3, 2, 1]
BASE_BET  = 0.10   # USD
VISIBLE   = 40     # last N spins for chart

# ─── LABOUCHÈRE ───────────────────────────────────────────────────────────────
class Labouchere:
    def __init__(self, init_seq: list, base: float):
        self.init_seq  = init_seq[:]
        self.base      = base
        self.seq       = init_seq[:]
        self.bankroll  = 0.0

    def _ensure_seq(self):
        if not self.seq:
            self.seq = self.init_seq[:]

    def current_bet(self) -> float:
        self._ensure_seq()
        if len(self.seq) == 1:
            return round(self.seq[0] * self.base, 2)
        return round((self.seq[0] + self.seq[-1]) * self.base, 2)

    def win(self) -> float:
        bet = self.current_bet()
        self.bankroll = round(self.bankroll + bet, 2)
        if len(self.seq) <= 2:
            self.seq = []
        else:
            self.seq = self.seq[1:-1]
        self._ensure_seq()
        return bet

    def loss(self) -> float:
        bet = self.current_bet()
        self.bankroll = round(self.bankroll - bet, 2)
        added = max(1, round(bet / self.base))
        self.seq.append(added)
        return bet


# ─── FIBONACCI ────────────────────────────────────────────────────────────────
_FIB_SEQ = [1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144]
_FIB_MAX = len(_FIB_SEQ) - 1

class Fibonacci:
    def __init__(self, base: float):
        self.base     = base
        self.step     = 0
        self.bankroll = 0.0

    def current_bet(self) -> float:
        return round(_FIB_SEQ[self.step] * self.base, 2)

    def win(self) -> float:
        bet = self.current_bet()
        self.bankroll = round(self.bankroll + bet, 2)
        if self.step <= 1:
            self.step = 0
        else:
            self.step = max(0, self.step - 2)
        return bet

    def loss(self) -> float:
        bet = self.current_bet()
        self.bankroll = round(self.bankroll - bet, 2)
        if self.step >= _FIB_MAX:
            self.step = 0
        else:
            self.step += 1
        return bet


# ─── STATISTICS ───────────────────────────────────────────────────────────────
class Stats:
    """Tracks wins/losses in two windows: all-time batches of 20, and 24hr."""
    def __init__(self):
        self.total      = 0    # completed signals (all time since last batch reset)
        self.wins       = 0
        self.losses     = 0
        self.last_stats_at = 0  # at which total we last sent stats

        # 24hr ring buffer: each entry = (timestamp, is_win, bankroll_at_time)
        self._h24: deque = deque()

        # Batch bankroll tracking
        self.batch_start_bankroll = None   # bankroll at start of current batch

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
        """Call this when sending stats (resets batch counters)."""
        self.last_stats_at = self.total
        self.batch_start_bankroll = bankroll
        self._wins_at_last_batch = self.wins

    def batch_stats(self, current_bankroll: float):
        """Stats for the last 20 signals (since last batch)."""
        n = self.total - self.last_stats_at
        w = self.wins - getattr(self, '_wins_at_last_batch', 0)
        l = n - w
        e = round(w / n * 100, 1) if n else 0.0
        # Bankroll accumulated in this batch (net result)
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
        # Bankroll accumulated in last 24h = last bankroll - first bankroll in window
        if t >= 2:
            first_bankroll = self._h24[0][2]
            last_bankroll  = self._h24[-1][2]
            bk24 = round(last_bankroll - first_bankroll, 2)
        elif t == 1:
            # Only one signal: bankroll change is that signal's delta
            # We don't have prior bankroll, assume change is 0? Actually we can approximate
            # But to be safe, use current - previous? Not possible. Set 0.
            bk24 = 0.0
        else:
            bk24 = 0.0
        return w, l, t, e, bk24


# ─── CHART GENERATION ─────────────────────────────────────────────────────────
def generate_chart(
    levels: list,
    spin_history: list,      # list of {"number": int, "real": str}
    bet_color: str,
    visible: int = VISIBLE
) -> io.BytesIO:
    """
    Chart of last `visible` cumulative levels with EMA 4/8/20.
    Each point is drawn as a coloured dot: red=ROJO, dark=NEGRO, green=VERDE.
    """
    arr = np.array(levels, dtype=float)
    n   = len(arr)

    def calc_ema(data, period):
        if len(data) < period:
            return np.full(len(data), np.nan)
        mult  = 2 / (period + 1)
        out   = np.full(len(data), np.nan)
        out[period - 1] = np.mean(data[:period])
        for i in range(period, len(data)):
            out[i] = (data[i] - out[i - 1]) * mult + out[i - 1]
        return out

    ema4  = calc_ema(arr, 4)
    ema8  = calc_ema(arr, 8)
    ema20 = calc_ema(arr, 20)

    start = max(0, n - visible)
    sl    = slice(start, n)
    x     = np.arange(len(arr[sl]))
    hist_sl = spin_history[start:]      # matching spin history slice

    is_rojo = bet_color == "ROJO"

    # Theme
    bg      = "#0b101f"
    ax_bg   = "#0f1a2a"
    grid_c  = "#1e2e48"
    line_c  = "#e84040" if is_rojo else "#9090bb"
    ema4_c  = "#ff9f43"
    ema8_c  = "#48dbfb"
    ema20_c = "#1dd1a1"
    title_c = "#ff8080" if is_rojo else "#b0b8d0"

    fig, ax = plt.subplots(figsize=(8, 3.6), facecolor=bg)
    ax.set_facecolor(ax_bg)

    y   = arr[sl]
    e4  = ema4[sl]
    e8  = ema8[sl]
    e20 = ema20[sl]

    # ── Main level line (thin) ────────────────────────────────────────────────
    ax.fill_between(x, y, alpha=0.10, color=line_c)
    ax.plot(x, y,   color=line_c,  linewidth=0.8, zorder=3)
    ax.plot(x, e4,  color=ema4_c,  linewidth=0.7, linestyle="--", label="EMA 4",  zorder=4)
    ax.plot(x, e8,  color=ema8_c,  linewidth=0.7, linestyle="--", label="EMA 8",  zorder=4)
    ax.plot(x, e20, color=ema20_c, linewidth=1.0, label="EMA 20", zorder=4)

    # ── Coloured dots for each spin ───────────────────────────────────────────
    dot_colors = {
        "ROJO":  "#e84040",
        "NEGRO": "#aaaacc",
        "VERDE": "#2ecc71",
    }
    for i, spin in enumerate(hist_sl):
        c = dot_colors.get(spin["real"], "#ffffff")
        ax.scatter(i, y[i], color=c, s=22, zorder=5,
                   edgecolors="white", linewidths=0.3)

    # ── X-axis: roulette numbers ──────────────────────────────────────────────
    tick_step = max(1, len(x) // 8)
    tick_x    = list(range(0, len(x), tick_step))
    tick_lbs  = [str(hist_sl[i]["number"]) if i < len(hist_sl) else "" for i in tick_x]
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

    # Custom legend with colour patches for dots
    from matplotlib.lines  import Line2D
    from matplotlib.patches import Patch
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
              facecolor="#0b101f", edgecolor=grid_c,
              labelcolor="white", framealpha=0.8, ncol=2)

    plt.tight_layout(pad=0.8)
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=120, facecolor=bg)
    plt.close(fig)
    buf.seek(0)
    return buf


# ─── TELEGRAM HELPERS ─────────────────────────────────────────────────────────
_TG_MAX_RETRIES = 5

def _tg_call(fn, *args, **kwargs):
    """Call any telebot function with automatic retry + exponential backoff."""
    delay = 2.0
    for attempt in range(1, _TG_MAX_RETRIES + 1):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            err = str(e)
            # Flood-wait: Telegram tells us how long to wait
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
    msg = _tg_call(
        bot.send_photo,
        chat_id=chat_id,
        photo=photo_buf,
        caption=caption,
        parse_mode="HTML",
        message_thread_id=thread_id
    )
    return msg.message_id if msg else None

def tg_send_text(chat_id: int, thread_id: int, text: str) -> Optional[int]:
    msg = _tg_call(
        bot.send_message,
        chat_id=chat_id,
        text=text,
        parse_mode="HTML",
        message_thread_id=thread_id
    )
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

        # Spin state
        self.spin_history:     list = []
        self.original_levels:  list = []   # cumulative +1 ROJO / -1 NEGRO
        self.inverted_levels:  list = []   # cumulative +1 NEGRO / -1 ROJO
        self.last_nonzero_color: Optional[str] = None
        self.anti_block: set = set()

        # Signal state
        self.signal_active:    bool  = False
        self.expected_color:   Optional[str] = None
        self.bet_color:        Optional[str] = None
        self.attempts_left:    int   = 0
        self.total_attempts:   int   = 0
        self.trigger_number:   Optional[int] = None

        # Result cooldown (show result for 7s before accepting new signal)
        self.result_until:     float = 0.0

        # Anti-spam consecutive losses
        self.consec_losses:    int   = 0
        self.loss_block_until: float = 0.0

        # Betting system — Fibonacci (Sistema 2) or Labouchère (Sistema 4)
        self.betting_system_name = cfg.get("betting_system", "labouchere")
        if self.betting_system_name == "fibonacci":
            self.bet_sys = Fibonacci(BASE_BET)
        else:
            self.bet_sys = Labouchere(LABOUCHERE_SEQ, BASE_BET)

        # Stats
        self.stats = Stats()

        # Telegram message tracking
        self.signal_msg_id: Optional[int] = None   # current pending signal msg

        # WS
        self.ws = None
        self.running = True

    # ── EMA ──────────────────────────────────────────────────────────────────
    @staticmethod
    def calculate_ema(data: list, period: int) -> list:
        if len(data) < period:
            return [None] * len(data)
        mult = 2 / (period + 1)
        out  = [None] * (period - 1)
        prev = sum(data[:period]) / period
        out.append(prev)
        for i in range(period, len(data)):
            prev = (data[i] - prev) * mult + prev
            out.append(prev)
        return out

    # ── Color data helpers ────────────────────────────────────────────────────
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

    # ── Determine actual bet color (may differ from expected) ─────────────────
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

    # ── Signal activation (tendencia mode) ───────────────────────────────────
    def should_activate(self) -> Optional[str]:
        if time.time() < self.loss_block_until:
            return None
        losses   = self.consec_losses          # 0–9
        min_spin = 22 + losses * 2
        if len(self.spin_history) < min_spin:
            return None

        last_num = self.spin_history[-1]["number"]
        entry    = self.get_entry(last_num)
        if not entry or entry["senal"] == "NO APOSTAR":
            return None
        expected = entry["senal"]

        ema4o  = self.calculate_ema(self.original_levels, 4)
        ema8o  = self.calculate_ema(self.original_levels, 8)
        ema20o = self.calculate_ema(self.original_levels, 20)
        ema4i  = self.calculate_ema(self.inverted_levels, 4)
        ema8i  = self.calculate_ema(self.inverted_levels, 8)
        ema20i = self.calculate_ema(self.inverted_levels, 20)

        # Required consecutive spins above EMA: grows +1 per loss level (max 13)
        req = min(3 + losses, 13)
        li  = len(self.original_levels) - 1

        def check(levels, e20, e8, e4, idx):
            for off in range(req):
                i = idx - (req - 1) + off
                if i < 0:
                    return False
                # Level 0+: must be above EMA20
                if e20[i] is None or levels[i] <= e20[i]:
                    return False
                # Level 2+: must also be above EMA8
                if losses >= 2 and e8[i] is not None and levels[i] <= e8[i]:
                    return False
                # Level 4+: must also be above EMA4
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

    # ── Process one roulette number ───────────────────────────────────────────
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

        # Keep same length
        while len(self.original_levels) > len(self.spin_history):
            self.original_levels.pop(0)
        while len(self.inverted_levels) > len(self.spin_history):
            self.inverted_levels.pop(0)

        # ── Resolve active signal ─────────────────────────────────────────────
        if self.signal_active and time.time() > self.result_until:
            is_win = (self.bet_color == "ROJO" and real == "ROJO") or \
                     (self.bet_color == "NEGRO" and real == "NEGRO")
            attempt_no = self.total_attempts - self.attempts_left + 1

            if is_win:
                bet = self.bet_sys.win()
                self.stats.record(True, self.bet_sys.bankroll)
                self.signal_active  = False
                self.consec_losses  = 0
                self.loss_block_until = 0.0
                self._send_result(number, real, True, bet)
                self._check_stats()
            else:
                self.attempts_left -= 1
                bet = self.bet_sys.loss()

                if self.attempts_left <= 0:
                    # Final loss — increase restriction level
                    self.consec_losses += 1
                    if self.consec_losses >= 10:
                        # Max 10 levels → reset restrictions, keep bankroll
                        self.consec_losses    = 0
                        self.loss_block_until = 0.0
                        logger.info(f"[{self.name}] Max 10 losses → restrictions reset, bankroll kept at {self.bet_sys.bankroll:.2f}")
                    else:
                        self.loss_block_until = time.time() + min(3.0 * self.consec_losses, 30)
                    self.stats.record(False, self.bet_sys.bankroll)
                    self.signal_active = False
                    self._send_result(number, real, False, bet)
                    self._check_stats()
                else:
                    # Intento 2: delete old message, send new signal
                    self.trigger_number = number
                    new_bet = self.bet_sys.current_bet()
                    self._send_retry_signal(number, new_bet)

        # ── Activate new signal ───────────────────────────────────────────────
        if not self.signal_active and time.time() > self.result_until:
            expected = self.should_activate()
            if expected:
                self.signal_active   = True
                self.expected_color  = expected
                self.bet_color       = self.determine_bet_color(expected)
                self.attempts_left   = MAX_ATTEMPTS
                self.total_attempts  = MAX_ATTEMPTS
                self.trigger_number  = number
                self._send_signal(number, 1)

    # ── Telegram: send initial signal ─────────────────────────────────────────
    def _send_signal(self, trigger: int, attempt: int):
        bet        = self.bet_sys.current_bet()
        prob       = int(self.get_prob(trigger, self.bet_color) * 100)
        color_icon = "🔴" if self.bet_color == "ROJO" else "⚫️"

        # Extra info line depending on system
        if self.betting_system_name == "fibonacci":
            step = self.bet_sys.step + 1
            sys_line = f"🌀 <i>Fibonacci paso {step}/12 · ×{_FIB_SEQ[self.bet_sys.step]}</i>\n"
        else:
            sys_line = f"💠 <i>Labouchère [{','.join(str(v) for v in self.bet_sys.seq)}]</i>\n"

        caption = (
            f"✅ <b>SEÑAL CONFIRMADA</b> ✅\n\n"
            f"🎰 <b>Juego: {self.name}</b>\n"
            f"👉 <b>Ingresar después del: {trigger}</b>\n"
            f"🎯 <b>Apostar a: {self.bet_color}</b> {color_icon}\n\n"
            f"💡 <i>Probabilidad de señal: {prob}%</i>\n"
            f"📍 <i>Apuesta: {bet:.2f} usd</i>\n"
            f"{sys_line}\n\n"
            f"♻️ <i>Intento {attempt}/{MAX_ATTEMPTS}</i>"
        )

        levels = self.original_levels[:] if self.bet_color == "ROJO" else self.inverted_levels[:]
        chart  = generate_chart(levels, self.spin_history[:], self.bet_color)

        msg_id = tg_send_photo(self.chat_id, self.thread_id, chart, caption)
        self.signal_msg_id = msg_id
        logger.info(f"[{self.name}] Signal sent: {self.bet_color} after {trigger}, bet={bet:.2f}, sys={self.betting_system_name}")

    # ── Telegram: send retry signal (attempt 2) ───────────────────────────────
    def _send_retry_signal(self, trigger: int, new_bet: float):
        # Delete old signal message
        if self.signal_msg_id:
            tg_delete(self.chat_id, self.signal_msg_id)
            self.signal_msg_id = None

        prob       = int(self.get_prob(trigger, self.bet_color) * 100)
        color_icon = "🔴" if self.bet_color == "ROJO" else "⚫️"

        if self.betting_system_name == "fibonacci":
            step = self.bet_sys.step + 1
            sys_line = f"🌀 <i>Fibonacci paso {step}/12 · ×{_FIB_SEQ[self.bet_sys.step]}</i>\n"
        else:
            sys_line = f"💠 <i>Labouchère [{','.join(str(v) for v in self.bet_sys.seq)}]</i>\n"

        caption = (
            f"✅ <b>SEÑAL CONFIRMADA</b> ✅\n\n"
            f"🎰 <b>Juego: {self.name}</b>\n"
            f"👉 <b>Ingresar después del: {trigger}</b>\n"
            f"🎯 <b>Apostar a: {self.bet_color}</b> {color_icon}\n\n"
            f"💡 <i>Probabilidad de señal: {prob}%</i>\n"
            f"📍 <i>Apuesta: {new_bet:.2f} usd</i>\n"
            f"{sys_line}\n\n"
            f"♻️ <i>Intento 2/{MAX_ATTEMPTS}</i>"
        )

        levels = self.original_levels[:] if self.bet_color == "ROJO" else self.inverted_levels[:]
        chart  = generate_chart(levels, self.spin_history[:], self.bet_color)

        msg_id = tg_send_photo(self.chat_id, self.thread_id, chart, caption)
        self.signal_msg_id = msg_id
        logger.info(f"[{self.name}] Retry signal sent: {self.bet_color} after {trigger}, bet={new_bet:.2f}")

    # ── Telegram: send result ─────────────────────────────────────────────────
    def _send_result(self, number: int, real: str, won: bool, bet: float):
        bankroll = self.bet_sys.bankroll
        if won:
            icon = "🔴" if real == "ROJO" else ("⚫️" if real == "NEGRO" else "🟢")
            text = (
                f"✅ <b>RESULTADO: {number}</b> {icon}\n"
                f"💰 <i>Bankroll Actual: {bankroll:.2f} usd</i>"
            )
        else:
            icon = "🔴" if real == "ROJO" else ("⚫️" if real == "NEGRO" else "🟢")
            text = (
                f"❌ <b>RESULTADO: {number}</b> {icon}\n"
                f"💰 <i>Bankroll Actual: {bankroll:.2f} usd</i>"
            )

        self.result_until = time.time() + 7.0
        tg_send_text(self.chat_id, self.thread_id, text)
        logger.info(f"[{self.name}] Result: {'WIN' if won else 'LOSS'} #{number}, bankroll={bankroll:.2f}")

    # ── Stats every 20 completed signals ──────────────────────────────────────
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
            f"💰 <i>Bankroll acumulado en estas 20 señales: {batch_bankroll:.2f} usd</i>\n\n"
            f"👉🏼 <b>ESTADÍSTICAS 24 HORAS</b>\n"
            f"🈯️ <b>W: {w24}</b> 🈲 <b>L: {l24}</b> 🈺 <b>T: {t24}</b> "
            f"📈 <b>E: {e24}%</b>\n"
            f"💰 <i>Bankroll acumulado últimas 24h: {bk24:.2f} usd</i>"
        )
        tg_send_text(self.chat_id, self.thread_id, text)
        logger.info(f"[{self.name}] Stats sent: {t20} signals")

    # ── WebSocket connection loop ──────────────────────────────────────────────
    async def run_ws(self):
        reconnect_delay = 5
        while self.running:
            try:
                async with websockets.connect(
                    WS_URL,
                    ping_interval=30,
                    ping_timeout=60,
                    close_timeout=10
                ) as ws:
                    self.ws = ws
                    reconnect_delay = 5  # reset on success
                    logger.info(f"[{self.name}] WS connected")

                    # Subscribe
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

                        # Bulk historical
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

                        # Live result
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
    """Ping own endpoint every 5 minutes to keep Render warm."""
    port = int(os.environ.get("PORT", 10000))
    url  = os.environ.get("RENDER_EXTERNAL_URL", f"http://localhost:{port}")
    ping_url = f"{url}/ping"
    while True:
        await asyncio.sleep(300)  # 5 minutes
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
    tasks   = [asyncio.create_task(e.run_ws()) for e in engines]
    tasks.append(asyncio.create_task(self_ping_loop()))
    logger.info("All roulette engines started.")
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    # Flask in background thread
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    logger.info("Flask server started.")

    # Bot asyncio loop
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped.")
