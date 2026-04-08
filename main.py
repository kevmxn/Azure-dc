#!/usr/bin/env python3
"""
Roulette Telegram Signal Bot - Modo Dual (Color AMX V20 / Docenas Tabla+EMA)
"""

import asyncio
import io
import json
import logging
import threading
import time
from collections import deque
from typing import Optional, Literal

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
import websockets
from flask import Flask, jsonify

# ─── LOGGING ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] %(levelname)s %(message)s'
)
logger = logging.getLogger("RouletteBotDual")

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

# ─── ROULETTE COLOR MAPS (solo para mostrar el color real) ────────────────────
REAL_COLOR_MAP = {
    0:"VERDE",1:"ROJO",2:"NEGRO",3:"ROJO",4:"NEGRO",5:"ROJO",6:"NEGRO",
    7:"ROJO",8:"NEGRO",9:"ROJO",10:"NEGRO",11:"NEGRO",12:"ROJO",13:"NEGRO",
    14:"ROJO",15:"NEGRO",16:"ROJO",17:"NEGRO",18:"ROJO",19:"ROJO",20:"NEGRO",
    21:"ROJO",22:"NEGRO",23:"ROJO",24:"NEGRO",25:"ROJO",26:"NEGRO",27:"ROJO",
    28:"NEGRO",29:"NEGRO",30:"ROJO",31:"NEGRO",32:"ROJO",33:"NEGRO",34:"ROJO",
    35:"NEGRO",36:"ROJO"
}

# ─── ROULETTE CONFIGS ─────────────────────────────────────────────────────────
ROULETTE_CONFIGS = {
    "Auto Roulette": {
        "ws_key": 225,
        "chat_id": -1003835197023,
        "thread_id": 2,
        "betting_system": "dalembert",
    },
    "Russian Roulette": {
        "ws_key": 221,
        "chat_id": -1003835197023,
        "thread_id": 7,
        "betting_system": "dalembert",
    },
    "Azure Roulette 1": {
        "ws_key": 227,
        "chat_id": -1003835197023,
        "thread_id": 6,
        "betting_system": "dalembert",
    },
}

WS_URL    = "wss://dga.pragmaticplaylive.net/ws"
CASINO_ID = "ppcjd00000007254"
MAX_ATTEMPTS = 2
BASE_BET  = 0.10   # USD
VISIBLE   = 50

# ─── TABLAS DE DOCENAS (37 elementos por ruleta) ──────────────────────────────
DOZEN_TABLES = {
    "Auto Roulette": [
        {"id": 0, "docena1": 32, "docena2": 44, "docena3": 24, "probability": 76, "senal": "DOCENA 1 y DOCENA 2"},
        {"id": 1, "docena1": 36, "docena2": 40, "docena3": 20, "probability": 76, "senal": "DOCENA 1 y DOCENA 2"},
        {"id": 2, "docena1": 28, "docena2": 32, "docena3": 36, "probability": 68, "senal": "DOCENA 2 y DOCENA 3"},
        {"id": 3, "docena1": 32, "docena2": 36, "docena3": 28, "probability": 68, "senal": "DOCENA 1 y DOCENA 2"},
        {"id": 4, "docena1": 36, "docena2": 32, "docena3": 28, "probability": 68, "senal": "DOCENA 1 y DOCENA 2"},
        {"id": 5, "docena1": 36, "docena2": 32, "docena3": 32, "probability": 68, "senal": "DOCENA 1 y DOCENA 3"},
        {"id": 6, "docena1": 28, "docena2": 32, "docena3": 40, "probability": 72, "senal": "DOCENA 2 y DOCENA 3"},
        {"id": 7, "docena1": 40, "docena2": 20, "docena3": 36, "probability": 76, "senal": "DOCENA 1 y DOCENA 3"},
        {"id": 8, "docena1": 28, "docena2": 36, "docena3": 32, "probability": 68, "senal": "DOCENA 2 y DOCENA 3"},
        {"id": 9, "docena1": 44, "docena2": 24, "docena3": 28, "probability": 76, "senal": "DOCENA 1 y DOCENA 3"},
        {"id": 10, "docena1": 24, "docena2": 36, "docena3": 36, "probability": 72, "senal": "DOCENA 2 y DOCENA 3"},
        {"id": 11, "docena1": 32, "docena2": 36, "docena3": 28, "probability": 68, "senal": "DOCENA 1 y DOCENA 2"},
        {"id": 12, "docena1": 28, "docena2": 36, "docena3": 32, "probability": 68, "senal": "DOCENA 2 y DOCENA 3"},
        {"id": 13, "docena1": 36, "docena2": 28, "docena3": 36, "probability": 72, "senal": "DOCENA 1 y DOCENA 3"},
        {"id": 14, "docena1": 36, "docena2": 40, "docena3": 20, "probability": 76, "senal": "DOCENA 1 y DOCENA 2"},
        {"id": 15, "docena1": 44, "docena2": 32, "docena3": 24, "probability": 76, "senal": "DOCENA 1 y DOCENA 2"},
        {"id": 16, "docena1": 36, "docena2": 32, "docena3": 28, "probability": 68, "senal": "DOCENA 1 y DOCENA 2"},
        {"id": 17, "docena1": 36, "docena2": 32, "docena3": 28, "probability": 68, "senal": "DOCENA 1 y DOCENA 2"},
        {"id": 18, "docena1": 36, "docena2": 32, "docena3": 28, "probability": 68, "senal": "DOCENA 1 y DOCENA 2"},
        {"id": 19, "docena1": 36, "docena2": 28, "docena3": 36, "probability": 72, "senal": "DOCENA 1 y DOCENA 3"},
        {"id": 20, "docena1": 32, "docena2": 32, "docena3": 40, "probability": 72, "senal": "DOCENA 1 y DOCENA 3"},
        {"id": 21, "docena1": 28, "docena2": 32, "docena3": 40, "probability": 72, "senal": "DOCENA 2 y DOCENA 3"},
        {"id": 22, "docena1": 28, "docena2": 36, "docena3": 28, "probability": 72, "senal": "DOCENA 1 y DOCENA 2"},
        {"id": 23, "docena1": 24, "docena2": 36, "docena3": 40, "probability": 76, "senal": "DOCENA 2 y DOCENA 3"},
        {"id": 24, "docena1": 28, "docena2": 32, "docena3": 36, "probability": 68, "senal": "DOCENA 2 y DOCENA 3"},
        {"id": 25, "docena1": 24, "docena2": 32, "docena3": 40, "probability": 72, "senal": "DOCENA 2 y DOCENA 3"},
        {"id": 26, "docena1": 28, "docena2": 36, "docena3": 32, "probability": 68, "senal": "DOCENA 2 y DOCENA 3"},
        {"id": 27, "docena1": 36, "docena2": 28, "docena3": 32, "probability": 68, "senal": "DOCENA 1 y DOCENA 3"},
        {"id": 28, "docena1": 36, "docena2": 32, "docena3": 28, "probability": 68, "senal": "DOCENA 1 y DOCENA 2"},
        {"id": 29, "docena1": 36, "docena2": 28, "docena3": 32, "probability": 68, "senal": "DOCENA 1 y DOCENA 3"},
        {"id": 30, "docena1": 28, "docena2": 32, "docena3": 40, "probability": 72, "senal": "DOCENA 2 y DOCENA 3"},
        {"id": 31, "docena1": 40, "docena2": 24, "docena3": 32, "probability": 72, "senal": "DOCENA 1 y DOCENA 3"},
        {"id": 32, "docena1": 24, "docena2": 32, "docena3": 40, "probability": 72, "senal": "DOCENA 2 y DOCENA 3"},
        {"id": 33, "docena1": 28, "docena2": 36, "docena3": 36, "probability": 72, "senal": "DOCENA 2 y DOCENA 3"},
        {"id": 34, "docena1": 32, "docena2": 24, "docena3": 36, "probability": 68, "senal": "DOCENA 1 y DOCENA 3"},
        {"id": 35, "docena1": 32, "docena2": 40, "docena3": 24, "probability": 72, "senal": "DOCENA 1 y DOCENA 2"},
        {"id": 36, "docena1": 36, "docena2": 36, "docena3": 24, "probability": 72, "senal": "DOCENA 1 y DOCENA 2"},
    ],
    "Russian Roulette": [
        {"id": 0, "docena1": 32, "docena2": 32, "docena3": 32, "probability": 32, "senal": "NO APOSTAR"},
        {"id": 1, "docena1": 28, "docena2": 32, "docena3": 36, "probability": 68, "senal": "DOCENA 2 y DOCENA 3"},
        {"id": 2, "docena1": 36, "docena2": 28, "docena3": 32, "probability": 68, "senal": "DOCENA 1 y DOCENA 3"},
        {"id": 3, "docena1": 24, "docena2": 32, "docena3": 36, "probability": 68, "senal": "DOCENA 2 y DOCENA 3"},
        {"id": 4, "docena1": 32, "docena2": 40, "docena3": 24, "probability": 72, "senal": "DOCENA 1 y DOCENA 2"},
        {"id": 5, "docena1": 40, "docena2": 24, "docena3": 36, "probability": 76, "senal": "DOCENA 1 y DOCENA 3"},
        {"id": 6, "docena1": 32, "docena2": 24, "docena3": 40, "probability": 72, "senal": "DOCENA 1 y DOCENA 3"},
        {"id": 7, "docena1": 36, "docena2": 24, "docena3": 40, "probability": 76, "senal": "DOCENA 1 y DOCENA 3"},
        {"id": 8, "docena1": 32, "docena2": 36, "docena3": 28, "probability": 68, "senal": "DOCENA 1 y DOCENA 2"},
        {"id": 9, "docena1": 28, "docena2": 36, "docena3": 32, "probability": 72, "senal": "DOCENA 2 y DOCENA 3"},
        {"id": 10, "docena1": 40, "docena2": 32, "docena3": 28, "probability": 72, "senal": "DOCENA 1 y DOCENA 2"},
        {"id": 11, "docena1": 36, "docena2": 24, "docena3": 36, "probability": 72, "senal": "DOCENA 1 y DOCENA 3"},
        {"id": 12, "docena1": 32, "docena2": 28, "docena3": 36, "probability": 68, "senal": "DOCENA 1 y DOCENA 3"},
        {"id": 13, "docena1": 32, "docena2": 28, "docena3": 36, "probability": 68, "senal": "DOCENA 1 y DOCENA 3"},
        {"id": 14, "docena1": 16, "docena2": 48, "docena3": 32, "probability": 80, "senal": "DOCENA 2 y DOCENA 3"},
        {"id": 15, "docena1": 36, "docena2": 28, "docena3": 32, "probability": 68, "senal": "DOCENA 1 y DOCENA 3"},
        {"id": 16, "docena1": 28, "docena2": 32, "docena3": 36, "probability": 68, "senal": "DOCENA 2 y DOCENA 3"},
        {"id": 17, "docena1": 20, "docena2": 44, "docena3": 32, "probability": 72, "senal": "DOCENA 2 y DOCENA 3"},
        {"id": 18, "docena1": 32, "docena2": 28, "docena3": 36, "probability": 68, "senal": "DOCENA 1 y DOCENA 3"},
        {"id": 19, "docena1": 36, "docena2": 28, "docena3": 32, "probability": 68, "senal": "DOCENA 1 y DOCENA 3"},
        {"id": 20, "docena1": 36, "docena2": 36, "docena3": 28, "probability": 72, "senal": "DOCENA 1 y DOCENA 2"},
        {"id": 21, "docena1": 24, "docena2": 44, "docena3": 28, "probability": 72, "senal": "DOCENA 2 y DOCENA 3"},
        {"id": 22, "docena1": 36, "docena2": 36, "docena3": 28, "probability": 72, "senal": "DOCENA 1 y DOCENA 2"},
        {"id": 23, "docena1": 24, "docena2": 32, "docena3": 40, "probability": 72, "senal": "DOCENA 2 y DOCENA 3"},
        {"id": 24, "docena1": 44, "docena2": 32, "docena3": 24, "probability": 76, "senal": "DOCENA 1 y DOCENA 2"},
        {"id": 25, "docena1": 36, "docena2": 24, "docena3": 36, "probability": 72, "senal": "DOCENA 1 y DOCENA 3"},
        {"id": 26, "docena1": 40, "docena2": 28, "docena3": 32, "probability": 72, "senal": "DOCENA 1 y DOCENA 3"},
        {"id": 27, "docena1": 32, "docena2": 28, "docena3": 36, "probability": 68, "senal": "DOCENA 1 y DOCENA 3"},
        {"id": 28, "docena1": 36, "docena2": 28, "docena3": 32, "probability": 68, "senal": "DOCENA 1 y DOCENA 3"},
        {"id": 29, "docena1": 32, "docena2": 24, "docena3": 40, "probability": 72, "senal": "DOCENA 1 y DOCENA 3"},
        {"id": 30, "docena1": 36, "docena2": 36, "docena3": 28, "probability": 72, "senal": "DOCENA 1 y DOCENA 2"},
        {"id": 31, "docena1": 32, "docena2": 36, "docena3": 24, "probability": 68, "senal": "DOCENA 1 y DOCENA 2"},
        {"id": 32, "docena1": 32, "docena2": 36, "docena3": 28, "probability": 68, "senal": "DOCENA 1 y DOCENA 2"},
        {"id": 33, "docena1": 28, "docena2": 32, "docena3": 36, "probability": 68, "senal": "DOCENA 2 y DOCENA 3"},
        {"id": 34, "docena1": 36, "docena2": 28, "docena3": 32, "probability": 68, "senal": "DOCENA 1 y DOCENA 3"},
        {"id": 35, "docena1": 36, "docena2": 32, "docena3": 24, "probability": 68, "senal": "DOCENA 1 y DOCENA 2"},
        {"id": 36, "docena1": 28, "docena2": 36, "docena3": 32, "probability": 68, "senal": "DOCENA 2 y DOCENA 3"},
    ],
    "Azure Roulette 1": [
        {"id": 0, "docena1": 28, "docena2": 32, "docena3": 36, "probability": 68, "senal": "DOCENA 2 y DOCENA 3"},
        {"id": 1, "docena1": 24, "docena2": 36, "docena3": 40, "probability": 76, "senal": "DOCENA 2 y DOCENA 3"},
        {"id": 2, "docena1": 36, "docena2": 36, "docena3": 28, "probability": 72, "senal": "DOCENA 1 y DOCENA 2"},
        {"id": 3, "docena1": 32, "docena2": 36, "docena3": 28, "probability": 68, "senal": "DOCENA 1 y DOCENA 2"},
        {"id": 4, "docena1": 36, "docena2": 24, "docena3": 36, "probability": 72, "senal": "DOCENA 1 y DOCENA 3"},
        {"id": 5, "docena1": 28, "docena2": 32, "docena3": 40, "probability": 72, "senal": "DOCENA 2 y DOCENA 3"},
        {"id": 6, "docena1": 32, "docena2": 32, "docena3": 32, "probability": 32, "senal": "NO APOSTAR"},
        {"id": 7, "docena1": 36, "docena2": 24, "docena3": 36, "probability": 72, "senal": "DOCENA 1 y DOCENA 3"},
        {"id": 8, "docena1": 36, "docena2": 28, "docena3": 32, "probability": 68, "senal": "DOCENA 1 y DOCENA 3"},
        {"id": 9, "docena1": 24, "docena2": 28, "docena3": 44, "probability": 72, "senal": "DOCENA 2 y DOCENA 3"},
        {"id": 10, "docena1": 32, "docena2": 40, "docena3": 28, "probability": 72, "senal": "DOCENA 1 y DOCENA 2"},
        {"id": 11, "docena1": 40, "docena2": 28, "docena3": 28, "probability": 68, "senal": "DOCENA 1 y DOCENA 3"},
        {"id": 12, "docena1": 24, "docena2": 36, "docena3": 40, "probability": 76, "senal": "DOCENA 2 y DOCENA 3"},
        {"id": 13, "docena1": 32, "docena2": 36, "docena3": 28, "probability": 68, "senal": "DOCENA 1 y DOCENA 2"},
        {"id": 14, "docena1": 28, "docena2": 28, "docena3": 40, "probability": 40, "senal": "NO APOSTAR"},
        {"id": 15, "docena1": 32, "docena2": 36, "docena3": 32, "probability": 32, "senal": "NO APOSTAR"},
        {"id": 16, "docena1": 36, "docena2": 28, "docena3": 32, "probability": 68, "senal": "DOCENA 1 y DOCENA 3"},
        {"id": 17, "docena1": 36, "docena2": 36, "docena3": 28, "probability": 72, "senal": "DOCENA 1 y DOCENA 2"},
        {"id": 18, "docena1": 24, "docena2": 40, "docena3": 36, "probability": 76, "senal": "DOCENA 2 y DOCENA 3"},
        {"id": 19, "docena1": 28, "docena2": 36, "docena3": 32, "probability": 68, "senal": "DOCENA 2 y DOCENA 3"},
        {"id": 20, "docena1": 40, "docena2": 32, "docena3": 28, "probability": 72, "senal": "DOCENA 1 y DOCENA 2"},
        {"id": 21, "docena1": 28, "docena2": 32, "docena3": 36, "probability": 68, "senal": "DOCENA 2 y DOCENA 3"},
        {"id": 22, "docena1": 40, "docena2": 40, "docena3": 20, "probability": 80, "senal": "DOCENA 1 y DOCENA 2"},
        {"id": 23, "docena1": 32, "docena2": 36, "docena3": 32, "probability": 32, "senal": "NO APOSTAR"},
        {"id": 24, "docena1": 36, "docena2": 28, "docena3": 32, "probability": 68, "senal": "DOCENA 1 y DOCENA 3"},
        {"id": 25, "docena1": 32, "docena2": 32, "docena3": 32, "probability": 32, "senal": "NO APOSTAR"},
        {"id": 26, "docena1": 32, "docena2": 36, "docena3": 32, "probability": 36, "senal": "NO APOSTAR"},
        {"id": 27, "docena1": 36, "docena2": 24, "docena3": 36, "probability": 72, "senal": "DOCENA 1 y DOCENA 3"},
        {"id": 28, "docena1": 28, "docena2": 44, "docena3": 24, "probability": 72, "senal": "DOCENA 1 y DOCENA 2"},
        {"id": 29, "docena1": 44, "docena2": 24, "docena3": 28, "probability": 72, "senal": "DOCENA 1 y DOCENA 3"},
        {"id": 30, "docena1": 24, "docena2": 36, "docena3": 36, "probability": 72, "senal": "DOCENA 2 y DOCENA 3"},
        {"id": 31, "docena1": 36, "docena2": 32, "docena3": 32, "probability": 68, "senal": "DOCENA 1 y DOCENA 2"},
        {"id": 32, "docena1": 36, "docena2": 28, "docena3": 32, "probability": 68, "senal": "DOCENA 1 y DOCENA 3"},
        {"id": 33, "docena1": 36, "docena2": 36, "docena3": 32, "probability": 68, "senal": "DOCENA 1 y DOCENA 2"},
        {"id": 34, "docena1": 28, "docena2": 32, "docena3": 40, "probability": 72, "senal": "DOCENA 2 y DOCENA 3"},
        {"id": 35, "docena1": 40, "docena2": 24, "docena3": 32, "probability": 72, "senal": "DOCENA 1 y DOCENA 3"},
        {"id": 36, "docena1": 28, "docena2": 36, "docena3": 36, "probability": 72, "senal": "DOCENA 2 y DOCENA 3"},
    ],
}

# ─── D'ALEMBERT (SOPORTA ODDS VARIABLES) ──────────────────────────────────────
class D_Alembert:
    def __init__(self, base: float, odds: float = 2.0):
        self.base      = base
        self.odds      = odds
        self.step      = 0
        self.bankroll  = 0.0
        self.max_step  = 20

    def current_bet(self) -> float:
        return round(self.base * (self.step + 1), 2)

    def win(self) -> float:
        bet = self.current_bet()
        profit = bet * (self.odds - 1)   # Ganancia neta
        self.bankroll = round(self.bankroll + profit, 2)
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

# ─── SISTEMA AMX V20 (COLOR) ──────────────────────────────────────────────────
class AMXSignalSystem:
    def __init__(self, mode: Literal["tendencia", "moderado"] = "moderado"):
        self.mode = mode
        self.last_signal_time: float = 0
        self.cooldown_seconds: int = 8
        self.so_cooldown: Optional[float] = None

    def calculate_ema(self, data: list, period: int) -> list:
        if len(data) < period:
            return [None] * len(data)
        mult = 2 / (period + 1)
        ema = [None] * (period - 1)
        prev = sum(data[:period]) / period
        ema.append(prev)
        for i in range(period, len(data)):
            prev = (data[i] * mult) + (prev * (1 - mult))
            ema.append(prev)
        return ema

    def check_signal_tendencia(self, positions: list) -> Optional[dict]:
        if len(positions) < 20:
            return None
        ahora = time.time()
        if ahora - self.last_signal_time < self.cooldown_seconds:
            return None
        if self.so_cooldown and ahora - self.so_cooldown < 8:
            return None

        ema4 = self.calculate_ema(positions, 4)
        ema20 = self.calculate_ema(positions, 20)
        if len(ema4) < 2 or len(ema20) < 1 or ema4[-1] is None or ema20[-1] is None or ema4[-2] is None or ema20[-2] is None:
            return None

        current_pos = positions[-1]
        cruce_alcista = ema4[-2] <= ema20[-2] and ema4[-1] > ema20[-1]
        sobre_tres_emas = current_pos > ema4[-1] and current_pos > ema20[-1]

        if cruce_alcista or sobre_tres_emas:
            expected_color = "ROJO" if current_pos > ema20[-1] else "NEGRO"
            return {
                "type": "SKRILL_2.0",
                "mode": "tendencia",
                "expected_color": expected_color,
                "trigger_number": None,
                "strength": "strong" if cruce_alcista else "moderate"
            }
        return None

    def check_signal_moderado(self, positions: list) -> Optional[dict]:
        if len(positions) < 20:
            return None
        ahora = time.time()
        if ahora - self.last_signal_time < self.cooldown_seconds:
            return None
        if self.so_cooldown and ahora - self.so_cooldown < 8:
            return None

        ema4 = self.calculate_ema(positions, 4)
        ema8 = self.calculate_ema(positions, 8)
        ema20 = self.calculate_ema(positions, 20)
        if len(ema8) < 2 or len(ema20) < 1 or ema8[-1] is None or ema20[-1] is None or ema8[-2] is None or ema20[-2] is None:
            return None

        cruce_ema8 = ema8[-2] <= ema20[-2] and ema8[-1] > ema20[-1]
        sobre_emas = positions[-1] > ema4[-1] and positions[-1] > ema8[-1]

        patron_v = False
        if len(positions) >= 3:
            a, b, c = positions[-3], positions[-2], positions[-1]
            patron_v = b < a and b < c and abs(a - c) <= 1 and c > a

        if (cruce_ema8 or patron_v) and sobre_emas:
            expected_color = "ROJO" if positions[-1] > ema20[-1] else "NEGRO"
            return {
                "type": "ALERTA_2.0",
                "mode": "moderado",
                "expected_color": expected_color,
                "trigger_number": None,
                "pattern": "V" if patron_v else "EMA_CROSS"
            }
        return None

    def register_signal_sent(self):
        self.last_signal_time = time.time()

    def register_so_failed(self):
        self.so_cooldown = time.time()

# ─── SISTEMA DE DOCENAS (TABLA + TENDENCIA EMA) ───────────────────────────────
class DozenSignalSystem:
    def __init__(self, table: list):
        self.table = {entry["id"]: entry for entry in table}
        self.levels: list = [0]
        self.last_dozen: Optional[int] = None
        self.last_d2_number: Optional[int] = None
        self.cooldown_seconds = 8
        self.last_signal_time: float = 0

    @staticmethod
    def get_dozen(number: int) -> int:
        if number == 0:
            return 0
        elif 1 <= number <= 12:
            return 1
        elif 13 <= number <= 24:
            return 2
        else:
            return 3

    def update_level(self, number: int):
        dozen = self.get_dozen(number)
        last_level = self.levels[-1] if self.levels else 0

        if dozen == 0:
            if self.last_dozen == 1:
                change = 1
            elif self.last_dozen == 2:
                change = 1 if (self.last_d2_number and self.last_d2_number <= 18) else -1
            elif self.last_dozen == 3:
                change = -1
            else:
                change = 0
        else:
            if dozen == 1:
                change = 1
            elif dozen == 2:
                change = 1 if number <= 18 else -1
            else:
                change = -1

        new_level = last_level + change
        self.levels.append(new_level)
        if len(self.levels) > 300:
            self.levels = self.levels[-200:]

        if dozen != 0:
            self.last_dozen = dozen
            if dozen == 2:
                self.last_d2_number = number

    def calculate_ema(self, data: list, period: int) -> list:
        if len(data) < period:
            return [None] * len(data)
        mult = 2 / (period + 1)
        ema = [None] * (period - 1)
        prev = sum(data[:period]) / period
        ema.append(prev)
        for i in range(period, len(data)):
            prev = (data[i] * mult) + (prev * (1 - mult))
            ema.append(prev)
        return ema

    def get_trend(self) -> str:
        if len(self.levels) < 20:
            return 'neutral'
        ema4 = self.calculate_ema(self.levels, 4)
        ema8 = self.calculate_ema(self.levels, 8)
        ema20 = self.calculate_ema(self.levels, 20)

        cur = self.levels[-1]
        e4 = ema4[-1]
        e8 = ema8[-1]
        e20 = ema20[-1]
        if None in (e4, e8, e20):
            return 'neutral'

        if cur > e4 > e8 > e20:
            return 'bullish'
        elif cur < e4 < e8 < e20:
            return 'bearish'
        else:
            return 'neutral'

    def get_trend_dozens(self) -> tuple:
        trend = self.get_trend()
        if trend == 'bullish':
            return (1, 2)
        elif trend == 'bearish':
            return (2, 3)
        else:
            return (1, 3)

    def check_signal(self, trigger_number: int) -> Optional[dict]:
        ahora = time.time()
        if ahora - self.last_signal_time < self.cooldown_seconds:
            return None

        entry = self.table.get(trigger_number)
        if not entry:
            return None

        senal = entry["senal"]
        dozens = []
        if "DOCENA 1" in senal and "DOCENA 2" in senal:
            dozens = [1, 2]
        elif "DOCENA 1" in senal and "DOCENA 3" in senal:
            dozens = [1, 3]
        elif "DOCENA 2" in senal and "DOCENA 3" in senal:
            dozens = [2, 3]
        else:
            return None

        trend_dozens = self.get_trend_dozens()
        if set(dozens) == set(trend_dozens):
            self.last_signal_time = ahora
            return {
                "type": "DOZEN",
                "dozens": dozens,
                "probability": entry["probability"],
                "trend": self.get_trend()
            }
        return None

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

# ─── CHART GENERATION (COLOR) ─────────────────────────────────────────────────
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

# ─── CHART GENERATION (DOCENAS) ───────────────────────────────────────────────
def generate_dozen_chart(levels: list, spin_history: list, bet_dozens: list, visible: int = VISIBLE) -> io.BytesIO:
    arr = np.array(levels, dtype=float)
    n = len(arr)

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

    color_map = {1: "#5bc8fa", 2: "#f0c040", 3: "#c0392b", 0: "#2ecc71"}
    if set(bet_dozens) == {1,2}:
        main_color = "#5bc8fa"
    elif set(bet_dozens) == {2,3}:
        main_color = "#c0392b"
    else:
        main_color = "#f39c12"

    bg = "#0b101f"
    ax_bg = "#0f1a2a"
    grid_c = "#1e2e48"
    ema4_c = "#ffd700"
    ema8_c = "#ff922b"
    ema20_c = "#ff4d4d"
    title_c = main_color

    fig, ax = plt.subplots(figsize=(8, 3.6), facecolor=bg)
    ax.set_facecolor(ax_bg)

    y = arr[sl]
    e4 = ema4[sl]
    e8 = ema8[sl]
    e20 = ema20[sl]

    ax.fill_between(x, y, alpha=0.10, color=main_color)
    ax.plot(x, y, color=main_color, linewidth=0.8, zorder=3)
    ax.plot(x, e4, color=ema4_c, linewidth=0.7, linestyle="--", label="EMA 4", zorder=4)
    ax.plot(x, e8, color=ema8_c, linewidth=0.7, linestyle="--", label="EMA 8", zorder=4)
    ax.plot(x, e20, color=ema20_c, linewidth=1.0, label="EMA 20", zorder=4)

    for i, spin in enumerate(hist_sl):
        num = spin["number"]
        dozen = DozenSignalSystem.get_dozen(num)
        c = color_map.get(dozen, "#ffffff")
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

    dozen_str = " + ".join(f"D{d}" for d in bet_dozens)
    ax.set_title(f"🎯 Señal Docenas: {dozen_str} — últimos {visible} giros · EMA 4/8/20",
                 color=title_c, fontsize=9, pad=6)

    from matplotlib.lines import Line2D
    legend_els = [
        Line2D([0],[0], color=main_color, linewidth=0.8, label="Nivel"),
        Line2D([0],[0], color=ema4_c, linewidth=0.7, linestyle="--", label="EMA 4"),
        Line2D([0],[0], color=ema8_c, linewidth=0.7, linestyle="--", label="EMA 8"),
        Line2D([0],[0], color=ema20_c, linewidth=1.0, label="EMA 20"),
        Line2D([0],[0], marker='o', color='w', markerfacecolor=color_map[1], markersize=5, label="D1 (1-12)"),
        Line2D([0],[0], marker='o', color='w', markerfacecolor=color_map[2], markersize=5, label="D2 (13-24)"),
        Line2D([0],[0], marker='o', color='w', markerfacecolor=color_map[3], markersize=5, label="D3 (25-36)"),
        Line2D([0],[0], marker='o', color='w', markerfacecolor=color_map[0], markersize=5, label="0"),
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

# ─── ROULETTE ENGINE (DUAL MODE) ──────────────────────────────────────────────
class RouletteEngine:
    def __init__(self, name: str, cfg: dict):
        self.name      = name
        self.ws_key    = cfg["ws_key"]
        self.chat_id   = cfg["chat_id"]
        self.thread_id = cfg["thread_id"]

        self.spin_history:     list = []
        self.original_levels:  list = []
        self.inverted_levels:  list = []
        self.last_nonzero_color: Optional[str] = None
        self.anti_block: set = set()

        self.signal_active:    bool = False
        self.expected_color:   Optional[str] = None
        self.bet_color:        Optional[str] = None
        self.bet_dozens:       Optional[list] = None
        self.attempts_left:    int = 0
        self.total_attempts:   int = 0
        self.trigger_number:   Optional[int] = None

        self.result_until:     float = 0.0
        self.consec_losses:    int = 0
        self.recovery_active:  bool  = False
        self.recovery_target:  float = 0.0
        self.level1_bankroll:  float = 0.0
        self.signal_is_level1: bool  = False

        self.betting_system_name = cfg.get("betting_system", "dalembert")
        self.mode: Literal["color", "dozen"] = "color"
        self.bet_sys = D_Alembert(BASE_BET, odds=2.0)

        self.stats = Stats()
        self.signal_msg_ids: list = []
        self.ws = None
        self.running = True

        # Sistemas de señal
        self.amx_system = AMXSignalSystem(mode="moderado")
        self.amx_positions: list = [0]
        self.dozen_system = DozenSignalSystem(DOZEN_TABLES[name])

    def set_mode(self, mode: Literal["color", "dozen"]):
        self.mode = mode
        if mode == "color":
            self.bet_sys = D_Alembert(BASE_BET, odds=2.0)
        else:
            self.bet_sys = D_Alembert(BASE_BET, odds=1.5)
        logger.info(f"[{self.name}] Modo cambiado a: {mode}, odds={self.bet_sys.odds}")

    def _update_amx_positions(self, color: str):
        last_pos = self.amx_positions[-1] if self.amx_positions else 0
        if color == "ROJO":
            new_pos = last_pos + 1
        elif color == "NEGRO":
            new_pos = last_pos - 1
        else:
            new_pos = last_pos
        self.amx_positions.append(new_pos)
        if len(self.amx_positions) > 300:
            self.amx_positions = self.amx_positions[-200:]

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
        min_len = min(len(self.original_levels), len(self.inverted_levels))
        self.original_levels = self.original_levels[-min_len:]
        self.inverted_levels = self.inverted_levels[-min_len:]

        self._update_amx_positions(real)
        self.dozen_system.update_level(number)

        # ── Resolver señal activa ─────────────────────────────────────────────
        if self.signal_active and time.time() > self.result_until:
            if self.mode == "color":
                is_win = (self.bet_color == "ROJO" and real == "ROJO") or (self.bet_color == "NEGRO" and real == "NEGRO")
            else:
                dozen = self.dozen_system.get_dozen(number)
                is_win = dozen in self.bet_dozens

            current_attempt = MAX_ATTEMPTS - self.attempts_left + 1

            if is_win:
                bet = self.bet_sys.win()
                self.stats.record(True, self.bet_sys.bankroll)
                if len(self.signal_msg_ids) > 1:
                    for msg_id in self.signal_msg_ids[:-1]:
                        tg_delete(self.chat_id, msg_id)
                    self.signal_msg_ids = [self.signal_msg_ids[-1]]
                self.signal_active = False
                self._check_recovery()
                self._send_result(number, real, True, bet)
                self._check_stats()
                self.signal_msg_ids = []
            else:
                self.attempts_left -= 1
                bet = self.bet_sys.loss()
                if self.attempts_left <= 0:
                    self.consec_losses += 1
                    if self.consec_losses >= 10:
                        self.consec_losses = 0
                        self.recovery_active = False
                        self.recovery_target = 0.0
                        logger.info(f"[{self.name}] Max 10 losses → nivel reiniciado, bankroll {self.bet_sys.bankroll:.2f}")
                    else:
                        self.recovery_active = True
                        if self.signal_is_level1:
                            self.recovery_target = self.level1_bankroll + BASE_BET
                        else:
                            self.recovery_target = self.level1_bankroll + BASE_BET
                        logger.info(f"[{self.name}] Pérdida nivel {self.consec_losses}. Recuperación activada, objetivo={self.recovery_target:.2f}")
                    self.stats.record(False, self.bet_sys.bankroll)
                    self.signal_active = False
                    self._send_result(number, real, False, bet)
                    self._check_stats()
                    self.signal_msg_ids = []
                else:
                    if self.signal_msg_ids:
                        last_msg_id = self.signal_msg_ids.pop()
                        tg_delete(self.chat_id, last_msg_id)
                    self.trigger_number = number
                    new_bet = self.bet_sys.current_bet()
                    attempt_number = MAX_ATTEMPTS - self.attempts_left + 1
                    self._send_retry_signal(number, new_bet, attempt_number)

        # ── Activar nueva señal ─────────────────────────────────────────────────
        if not self.signal_active and time.time() > self.result_until:
            self.signal_msg_ids = []
            signal = None
            if self.mode == "color":
                signal = self._detect_amx_signal()
                if signal:
                    self.bet_color = signal["expected_color"]
                    self.expected_color = signal["expected_color"]
            else:
                signal = self._detect_dozen_signal()
                if signal:
                    self.bet_dozens = signal["dozens"]

            if signal:
                self.signal_active = True
                self.attempts_left = MAX_ATTEMPTS
                self.total_attempts = MAX_ATTEMPTS
                self.trigger_number = self.spin_history[-1]["number"]
                self._send_signal(self.trigger_number, 1, amx_signal=signal)

    def _detect_amx_signal(self) -> Optional[dict]:
        if len(self.amx_positions) < 20:
            return None
        recent_colors = [s["real"] for s in self.spin_history[-5:] if s["real"] != "VERDE"]
        if len(recent_colors) < 2 or recent_colors[-1] != recent_colors[-2]:
            return None
        momentum_color = recent_colors[-1]
        try:
            if self.amx_system.mode == "tendencia":
                signal = self.amx_system.check_signal_tendencia(self.amx_positions)
            else:
                signal = self.amx_system.check_signal_moderado(self.amx_positions)
        except Exception as e:
            logger.warning(f"[{self.name}] Error en detección AMX: {e}")
            return None
        if signal and signal["expected_color"] == momentum_color:
            return signal
        return None

    def _detect_dozen_signal(self) -> Optional[dict]:
        if len(self.spin_history) < 21:
            return None
        trigger = self.spin_history[-1]["number"]
        return self.dozen_system.check_signal(trigger)

    def _check_recovery(self):
        if not self.recovery_active:
            return
        if self.bet_sys.bankroll >= self.recovery_target:
            logger.info(f"[{self.name}] Recuperación completada! bankroll={self.bet_sys.bankroll:.2f} >= objetivo={self.recovery_target:.2f}. Reseteando a nivel 1.")
            self.consec_losses    = 0
            self.recovery_active  = False
            self.recovery_target  = 0.0
            self.bet_sys.step     = 0

    def _send_signal(self, trigger: int, attempt: int, amx_signal: Optional[dict] = None):
        bet_total = self.bet_sys.current_bet()
        if self.mode == "color":
            color_icon = "🔴" if self.bet_color == "ROJO" else "⚫️"
            step = self.bet_sys.step + 1
            self.signal_is_level1 = (self.bet_sys.step == 0 and not self.recovery_active)
            if self.signal_is_level1:
                self.level1_bankroll = self.bet_sys.bankroll
            sys_line = f"🌀 <i>D'Alembert paso {step} de 20</i>\n"
            amx_line = ""
            if amx_signal:
                mode_icon = "📈" if amx_signal["mode"] == "tendencia" else "📊"
                amx_line = f"{mode_icon} <i>AMX V20 • {amx_signal['mode'].upper()}</i>\n"
            caption = (
                f"✅☑️ <b>SEÑAL CONFIRMADA</b> ☑️✅\n\n"
                f"🎰 <b>Juego: {self.name}</b>\n"
                f"👉 <b>Después de: {trigger}</b>\n"
                f"🎯 <b>Apostar a: {self.bet_color}</b> {color_icon}\n\n"
                f"{amx_line}"
                f"{sys_line}"
                f"📍 <i>Apuesta: {bet_total:.2f} usd</i>\n\n"
                f"♻️ <i>Intento {attempt}/{MAX_ATTEMPTS}</i>\n"
            )
            levels = self.original_levels[:] if self.bet_color == "ROJO" else self.inverted_levels[:]
            chart = generate_chart(levels, self.spin_history[:], self.bet_color)
        else:
            bet_per_dozen = bet_total / 2
            dozens_str = " + ".join(f"D{d}" for d in self.bet_dozens)
            trend = amx_signal.get("trend", "neutral") if amx_signal else "neutral"
            trend_emoji = {"bullish": "📈", "bearish": "📉", "neutral": "📊"}.get(trend, "")
            prob = amx_signal.get("probability", 0) if amx_signal else 0
            caption = (
                f"✅☑️ <b>SEÑAL DOCENAS CONFIRMADA</b> ☑️✅\n\n"
                f"🎰 <b>Juego: {self.name}</b>\n"
                f"👉 <b>Después de: {trigger}</b>\n"
                f"🎯 <b>Apostar a: {dozens_str}</b>\n\n"
                f"{trend_emoji} Tendencia EMA: {trend.upper()}\n"
                f"📊 Probabilidad tabla: {prob}%\n\n"
                f"💰 Apuesta total: ${bet_total:.2f}\n"
                f"   (${bet_per_dozen:.2f} por docena)\n\n"
                f"♻️ <i>Intento {attempt}/{MAX_ATTEMPTS}</i>\n"
            )
            chart = generate_dozen_chart(self.dozen_system.levels, self.spin_history, self.bet_dozens)

        msg_id = tg_send_photo(self.chat_id, self.thread_id, chart, caption)
        if msg_id:
            self.signal_msg_ids.append(msg_id)
        logger.info(f"[{self.name}] Signal sent: {self.mode} after {trigger}")

    def _send_retry_signal(self, trigger: int, new_bet: float, attempt_number: int):
        if self.mode == "color":
            color_icon = "🔴" if self.bet_color == "ROJO" else "⚫️"
            step = self.bet_sys.step + 1
            sys_line = f"🌀 <i>D'Alembert paso {step} de 20</i>\n"
            caption = (
                f"✅☑️ <b>SEÑAL CONFIRMADA</b> ☑️✅\n\n"
                f"🎰 <b>Juego: {self.name}</b>\n"
                f"👉🏼 <b>Después de: {trigger}</b>\n"
                f"🎯 <b>Apostar a: {self.bet_color}</b> {color_icon}\n\n"
                f"{sys_line}"
                f"📍 <i>Apuesta: {new_bet:.2f} usd</i>\n\n"
                f"♻️ <i>Intento {attempt_number}/{MAX_ATTEMPTS}</i>\n"
            )
            levels = self.original_levels[:] if self.bet_color == "ROJO" else self.inverted_levels[:]
            chart = generate_chart(levels, self.spin_history[:], self.bet_color)
        else:
            bet_per_dozen = new_bet / 2
            dozens_str = " + ".join(f"D{d}" for d in self.bet_dozens)
            caption = (
                f"✅☑️ <b>SEÑAL DOCENAS CONFIRMADA</b> ☑️✅\n\n"
                f"🎰 <b>Juego: {self.name}</b>\n"
                f"👉🏼 <b>Después de: {trigger}</b>\n"
                f"🎯 <b>Apostar a: {dozens_str}</b>\n\n"
                f"💰 Apuesta total: ${new_bet:.2f}\n"
                f"   (${bet_per_dozen:.2f} por docena)\n\n"
                f"♻️ <i>Intento {attempt_number}/{MAX_ATTEMPTS}</i>\n"
            )
            chart = generate_dozen_chart(self.dozen_system.levels, self.spin_history, self.bet_dozens)

        msg_id = tg_send_photo(self.chat_id, self.thread_id, chart, caption)
        if msg_id:
            self.signal_msg_ids.append(msg_id)
        logger.info(f"[{self.name}] Retry signal sent: {self.mode} after {trigger}")

    def _send_result(self, number: int, real: str, won: bool, bet: float):
        bankroll = self.bet_sys.bankroll
        icon = "🔴" if real == "ROJO" else ("⚫️" if real == "NEGRO" else "🟢")
        if won:
            text = f"💎 <b>RESULTADO: {number}</b> {icon}\n💰 <i>Bankroll Actual: {bankroll:.2f} usd</i>\n"
        else:
            text = f"❌ <b>RESULTADO: {number}</b> {icon}\n💰 <i>Bankroll Actual: {bankroll:.2f} usd</i>\n"
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
            f"🈯️ <b>W: {w20}</b> 🈲 <b>L: {l20}</b> 🈺 <b>T: {t20}</b> 📈 <b>E: {e20}%</b>\n"
            f"💰 <i>Bankroll acumulado: {batch_bankroll:.2f} usd</i>\n\n"
            f"👉🏼 <b>ESTADÍSTICAS 24 HORAS</b>\n"
            f"🈯️ <b>W: {w24}</b> 🈲 <b>L: {l24}</b> 🈺 <b>T: {t24}</b> 📈 <b>E: {e24}%</b>\n"
            f"💰 <i>Bankroll acumulado: {bk24:.2f} usd</i>\n"
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
    return jsonify({"status": "ok", "bot": "Roulette Signal Bot Dual Mode", "ts": time.time()})

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

# ─── COMANDOS TELEGRAM ───────────────────────────────────────────────────────
engines: dict[str, RouletteEngine] = {}

@bot.message_handler(commands=['start', 'help'])
def cmd_start(message):
    help_text = """
<b>🎰 Roulette Bot - Modo Dual</b>

Comandos disponibles:
/color - Activa modo COLOR (AMX V20)
/docena - Activa modo DOCENAS (tabla + tendencia EMA)
/modo <ruleta> <color|dozen> - Cambia modo de una ruleta específica
/status - Muestra estado de todas las ruletas
/reset - Resetea estadísticas
/help - Muestra esta ayuda

El bot envía señales con gráficos. Solo una señal activa a la vez.
    """
    bot.reply_to(message, help_text, parse_mode="HTML")

@bot.message_handler(commands=['color'])
def cmd_color(message):
    for engine in engines.values():
        engine.set_mode("color")
    bot.reply_to(message, "✅ <b>Modo COLOR activado en todas las ruletas</b>", parse_mode="HTML")

@bot.message_handler(commands=['docena'])
def cmd_docena(message):
    for engine in engines.values():
        engine.set_mode("dozen")
    bot.reply_to(message, "✅ <b>Modo DOCENAS activado en todas las ruletas</b>", parse_mode="HTML")

@bot.message_handler(commands=['modo'])
def cmd_modo(message):
    try:
        parts = message.text.split()
        if len(parts) != 3:
            raise ValueError
        _, ruleta, nuevo_modo = parts
    except:
        bot.reply_to(message, "Uso: /modo <ruleta> <color|dozen>")
        return

    engine = engines.get(ruleta)
    if not engine:
        bot.reply_to(message, f"Ruleta no encontrada. Disponibles: {', '.join(engines.keys())}")
        return
    if nuevo_modo not in ("color", "dozen"):
        bot.reply_to(message, "Modo debe ser 'color' o 'dozen'")
        return

    engine.set_mode(nuevo_modo)
    bot.reply_to(message, f"✅ {ruleta} ahora en modo {nuevo_modo}")

@bot.message_handler(commands=['status'])
def cmd_status(message):
    lines = ["<b>📊 ESTADO</b>\n"]
    for name, engine in engines.items():
        mode_icon = "🎨" if engine.mode == "color" else "📊"
        signal_status = "🟢" if engine.signal_active else "⚪"
        lines.append(f"<b>{name}</b>: {mode_icon} {engine.mode} {signal_status}")
    bot.reply_to(message, "\n".join(lines), parse_mode="HTML")

@bot.message_handler(commands=['reset'])
def cmd_reset(message):
    for engine in engines.values():
        engine.stats = Stats()
    bot.reply_to(message, "🔄 <b>Estadísticas reseteadas</b>", parse_mode="HTML")

# ─── MAIN ────────────────────────────────────────────────────────────────────
def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)

async def main():
    global engines
    engines = {name: RouletteEngine(name, cfg) for name, cfg in ROULETTE_CONFIGS.items()}
    tasks = [asyncio.create_task(e.run_ws()) for e in engines.values()]
    tasks.append(asyncio.create_task(self_ping_loop()))

    def telegram_polling():
        logger.info("Iniciando polling de Telegram...")
        bot.polling(none_stop=True, interval=1, timeout=30)

    tg_thread = threading.Thread(target=telegram_polling, daemon=True)
    tg_thread.start()

    logger.info("🎰 Roulette Bot Dual Mode iniciado (Color AMX V20 / Docenas Tabla+EMA)")
    logger.info("Comandos: /color, /docena, /modo, /status, /reset, /help")

    await asyncio.gather(*tasks)

if __name__ == "__main__":
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    logger.info("Flask server started.")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped.")
