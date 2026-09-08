"""
╔══════════════════════════════════════════════════════════════╗
║   BOT UNIFICADO — SPEED ROULETTE 2 (key 205)                 ║
║   - Detección: 4 agentes de PATRONES DE DOCENAS              ║
║       V2: aaba (4)  |  V3: aaaba (5)                        ║
║       V4: abaa (4)  |  V6: aaaabaa (7)                     ║
║   - Agentes de ZONA: baaaabbb (8) y aaaabbbbaa (10)        ║
║     (patrones solo con a/b, el cero es comodín)             ║
║   - Si el cero está cerca del final, se invierte la         ║
║     secuencia de apuestas (opuesto primero)                 ║
║   - Señales D1+D2/D2+D3: todos los agentes, sin modelo      ║
║   - Señales D1+D3: solo agentes de 4 valores y             ║
║     requieren modelo entrenado                              ║
║   - Conversión: D1+D2 -> BAJA, D2+D3 -> ALTA,              ║
║     D1+D3 -> opuesto de última zona                        ║
║   - Para D1+D2 y D2+D3, el segundo intento puede ser       ║
║     opuesto si el modelo indica baja efectividad del        ║
║     segundo intento al mismo lado (tendencia agotamiento)  ║
║   - Confirmación de patrón "-1 valor"                       ║
║   - 2 intentos para ZONA (apuestas), 3 intentos para ML      ║
║   - Gestión Labouchère + marcador diario (win1/win2/loss)   ║
║   - Mensajes combinados: resolución + nueva señal          ║
║   - Telegram / HTTP API / self-ping / persistencia           ║
║   - Tendencia global basada en 20 giros                      ║
╚══════════════════════════════════════════════════════════════
"""

import asyncio
import json
import logging
import os
import sqlite3
import sys
import time
from typing import Optional, Callable, Awaitable, List

import websockets
from aiohttp import web, ClientSession, ClientTimeout

try:
    from telebot.async_telebot import AsyncTeleBot
    TELEBOT_OK = True
except ImportError:
    AsyncTeleBot = None
    TELEBOT_OK = False

# ──────────────────────────────────────────────
#  CONFIGURACIÓN
# ──────────────────────────────────────────────
WS_URL        = "wss://dga.pragmaticplaylive.net/ws"
CASINO_ID     = "ppcdk00000005349"
CURRENCY_ID   = "BRL"
PING_INTERVAL = 240
SAVE_INTERVAL = 30

ROULETTE_KEYS = {205: 205}   # Speed Roulette 2

# ── Lógica de docenas (detección) ──
DOZEN_MAX_ATTEMPTS = 3          # para entrenamiento del modelo
ZONE_MAX_ATTEMPTS = 2           # para apuestas reales (señales de zona)
DOZEN_BACKTEST_WINDOW = 60
DOZEN_CONTEXT_WINDOW = 20
DOZEN_MIN_SAMPLES_GATE = 6
DOZEN_MIN_WIN_RATE = 0.30
DOZEN_MIN_SPIN_TO_SIGNAL = 21

# ── Entrenamiento ML ──
ML_MIN_SIGNALS_TO_TRAIN = 50
ML_RETRAIN_INTERVAL_SECONDS = 30 * 60

AMX_STRENGTH_THRESHOLDS = {"strong": 1.0, "weak": 0.5}
AMX_ADJUST_FACTOR_STRONG = 0.8
AMX_ADJUST_FACTOR_WEAK = 1.2

DOZEN_COOLDOWN_AFTER_LOSSES = 3
DOZEN_COOLDOWN_ROUNDS = 5

# ── Umbral para decidir opuesto en segundo intento ──
SECOND_ATTEMPT_OPPOSITE_THRESHOLD = 0.35

# ── Labouchère (gestión de capital, de Roulette 1) ──
LABOUCHERE_BASE_AMOUNT = 500
LABOUCHERE_INITIAL_SEQUENCE = [1, 1, 1, 1, 1]

REAL_COLOR_MAP = {
    0: "VERDE", 1: "ROJO", 2: "NEGRO", 3: "ROJO", 4: "NEGRO", 5: "ROJO", 6: "NEGRO",
    7: "ROJO", 8: "NEGRO", 9: "ROJO", 10: "NEGRO", 11: "NEGRO", 12: "ROJO", 13: "NEGRO",
    14: "ROJO", 15: "NEGRO", 16: "ROJO", 17: "NEGRO", 18: "ROJO", 19: "ROJO", 20: "NEGRO",
    21: "ROJO", 22: "NEGRO", 23: "ROJO", 24: "NEGRO", 25: "ROJO", 26: "NEGRO", 27: "ROJO",
    28: "NEGRO", 29: "NEGRO", 30: "ROJO", 31: "NEGRO", 32: "ROJO", 33: "NEGRO", 34: "ROJO",
    35: "NEGRO", 36: "ROJO"
}

DOZEN_VALUES = ("D1", "D2", "D3")
DOZEN_NUM = {"D1": 1, "D2": 2, "D3": 3, "VERDE": 0}
NUM_DOZEN = {1: "D1", 2: "D2", 3: "D3"}

# ── Zonas (apuesta real, mensajes de Roulette 1) ──
ZONE_VALUES = ("BAJA", "ALTA", "VERDE")
ZONE_EMOJI = {"BAJA": "🔵", "ALTA": "🟠", "VERDE": "🟢"}
ZONE_NUM = {"BAJA": 1, "ALTA": 2, "VERDE": 0}
NUM_ZONE = {1: "BAJA", 2: "ALTA", 0: "VERDE"}

EMA_TREND_MIN_HISTORY = 20
TREND_FAVORED_DOZENS = {
    "bullish": {1, 2},
    "bearish": {2, 3},
    "neutral": {1, 3},
}

AGENT_TREND_CONFIG = {
    "agent2": {"method": "ema", "strictness": "strict", "min_diff": 0.5, "amx_periods": None},
    "agent3": {"method": "amx", "strictness": "relaxed", "min_diff": None, "amx_periods": [5, 10, 20]},
    "agent4": {"method": "amx", "strictness": "very_strict", "min_diff": None, "amx_periods": [3, 8, 15]},
    "agent6": {"method": "amx", "strictness": "relaxed", "min_diff": None, "amx_periods": [5, 10, 20]},
}

# ── Telegram ──
BOT_TOKEN      = os.environ.get("BOT_TOKEN", "8347707121:AAH1cPEDMLbm-scTJ8mUuufeEhzw3Axv2Lw")
CHAT_ID_BASE   = int(os.environ.get("CHAT_ID_BASE", "-1003986868798"))
THREAD_SIGNALS = int(os.environ.get("THREAD_SIGNALS", "4396"))
THREAD_STATS   = int(os.environ.get("THREAD_STATS", "4398"))
THREAD_SIGNALS_ZONE = int(os.environ.get("THREAD_SIGNALS_ZONE", str(THREAD_SIGNALS)))
THREAD_STATS_ZONE   = int(os.environ.get("THREAD_STATS_ZONE", str(THREAD_STATS)))
TABLE_LINK     = os.environ.get("TABLE_LINK", "https://1win.lat/casino/play/v_pragmatic:speedroulette2")
TABLE_NAME     = "Speed Roulette 2"

HISTORY_SEED_PATH  = os.environ.get("HISTORY_SEED_PATH", "russian-azure.db")
HISTORY_SEED_TABLE = os.environ.get("HISTORY_SEED_TABLE", "roulette_1")

# ──────────────────────────────────────────────
#  LOGGING
# ──────────────────────────────────────────────
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s | %(levelname)s | %(message)s",
                    datefmt="%H:%M:%S", handlers=[logging.StreamHandler(sys.stdout)])
log = logging.getLogger(__name__)


# ══════════════════════════════════════════════
#  FUNCIONES AUXILIARES
# ══════════════════════════════════════════════
def color_of(n):
    return REAL_COLOR_MAP.get(n, "VERDE")

def dozen_of(n):
    if n == 0: return "VERDE"
    if 1 <= n <= 12: return "D1"
    if 13 <= n <= 24: return "D2"
    return "D3"

def zone_of(n):
    if n is None or n == 0:
        return "VERDE"
    return "BAJA" if 1 <= n <= 18 else "ALTA"

def zone_win(zone: str, number) -> bool:
    if number is None or number == 0:
        return False
    if zone == "BAJA":
        return 1 <= number <= 18
    if zone == "ALTA":
        return 19 <= number <= 36
    return False

def dozen_bet_to_zone(bet_dozens, pattern) -> Optional[str]:
    s = set(bet_dozens)
    if s == {"D1", "D2"}:
        return "BAJA"
    if s == {"D2", "D3"}:
        return "ALTA"
    if s == {"D1", "D3"}:
        return None
    pred = pattern[-1]
    if pred == "D1":
        return "BAJA"
    if pred == "D3":
        return "ALTA"
    return None

def format_cop(amount: int) -> str:
    sign = '+' if amount >= 0 else '-'
    return f"{sign}${abs(amount):,} COP"

def calc_ema(data, period):
    if not data or len(data) < period: return []
    k = 2 / (period + 1)
    result = [None] * (period - 1)
    ema = sum(data[:period]) / period
    result.append(ema)
    for i in range(period, len(data)):
        ema = data[i] * k + ema * (1 - k)
        result.append(ema)
    return result

def ema_trend(level_history, strictness="relaxed", min_diff=0.0):
    if len(level_history) < EMA_TREND_MIN_HISTORY:
        return None if strictness in ("strict", "very_strict") else "neutral"
    ema4 = calc_ema(level_history, 4)
    ema8 = calc_ema(level_history, 8)
    ema20 = calc_ema(level_history, 20)
    if not ema4 or not ema8 or not ema20:
        return None if strictness != "relaxed" else "neutral"
    cur, e4, e8, e20 = level_history[-1], ema4[-1], ema8[-1], ema20[-1]
    if any(v is None for v in (e4, e8, e20)):
        return None if strictness != "relaxed" else "neutral"
    bullish = cur > e4 > e8 > e20
    bearish = cur < e4 < e8 < e20
    if strictness == "relaxed":
        if bullish: return "bullish"
        if bearish: return "bearish"
        return "neutral"
    elif strictness == "strict":
        if bullish:
            if abs(cur - e4) > min_diff and abs(e4 - e8) > min_diff and abs(e8 - e20) > min_diff:
                return "bullish"
            return "neutral"
        if bearish:
            if abs(cur - e4) > min_diff and abs(e4 - e8) > min_diff and abs(e8 - e20) > min_diff:
                return "bearish"
            return "neutral"
        return "neutral"
    elif strictness == "very_strict":
        if bullish:
            if abs(cur - e4) > min_diff and abs(e4 - e8) > min_diff and abs(e8 - e20) > min_diff:
                return "bullish"
            return None
        if bearish:
            if abs(cur - e4) > min_diff and abs(e4 - e8) > min_diff and abs(e8 - e20) > min_diff:
                return "bearish"
            return None
        return None
    return "neutral"

def calc_momentum(history, period):
    if len(history) < period + 1:
        return 0
    return history[-1] - history[-period-1]

def amx_trend(level_history, periods, strictness="relaxed", threshold=0.5):
    if len(level_history) < max(periods) + 1:
        return None if strictness == "very_strict" else "neutral"
    momentum_values = [calc_momentum(level_history, p) for p in periods]
    amx = sum(momentum_values) / len(periods)
    if strictness == "relaxed":
        if amx > 0: return "bullish"
        if amx < 0: return "bearish"
        return "neutral"
    elif strictness == "strict":
        if amx > threshold: return "bullish"
        if amx < -threshold: return "bearish"
        return "neutral"
    elif strictness == "very_strict":
        if amx > threshold: return "bullish"
        if amx < -threshold: return "bearish"
        return None
    return "neutral"

def trend_favored_dozens(trend):
    if trend is None:
        return set()
    return TREND_FAVORED_DOZENS.get(trend, TREND_FAVORED_DOZENS["neutral"])

def amx_strength(level_history, periods):
    if len(level_history) < max(periods) + 1:
        return 0.0
    momentum_values = [calc_momentum(level_history, p) for p in periods]
    amx = sum(momentum_values) / len(periods)
    return abs(amx)


# ══════════════════════════════════════════════
#  LABOUCHÈRE MANAGER
# ══════════════════════════════════════════════
class LabouchereManager:
    def __init__(self, base_amount: int = LABOUCHERE_BASE_AMOUNT,
                 initial_sequence: List[int] = None):
        self.initial_sequence = list(initial_sequence if initial_sequence else LABOUCHERE_INITIAL_SEQUENCE)
        self.capital = 0
        self.balance = 0
        self.base_amount = base_amount
        self.sequence = list(self.initial_sequence)
        self.current_bet = self._calculate_bet()
        self.cycles_completed = 0
        self.total_bet = 0
        self.total_won = 0

    def _calculate_bet(self) -> int:
        if not self.sequence:
            return 0
        if len(self.sequence) == 1:
            return self.sequence[0] * self.base_amount
        return (self.sequence[0] + self.sequence[-1]) * self.base_amount

    def get_bet(self) -> int:
        return self.current_bet

    def seq_str(self) -> str:
        return ",".join(str(x) for x in self.sequence)

    def _restart_cycle(self):
        self.base_amount = LABOUCHERE_BASE_AMOUNT
        self.sequence = list(self.initial_sequence)
        self.current_bet = self._calculate_bet()
        log.info(f"♾️ GESTIÓN REINICIADA · Acumulado: {'+' if self.balance >= 0 else '-'}"
                 f"{format_cop(abs(self.balance))} · "
                 f"Base: {format_cop(self.base_amount)} · Secuencia: [{self.seq_str()}] · "
                 f"Apuesta: {format_cop(self.current_bet)}")

    def update(self, win: bool) -> bool:
        if not self.sequence:
            self._restart_cycle()
            return False

        bet_amount = self.current_bet
        self.total_bet += bet_amount

        if win:
            self.total_won += bet_amount
            self.balance += bet_amount
            if len(self.sequence) >= 2:
                self.sequence.pop(0)
                self.sequence.pop()
            else:
                self.sequence.pop()
        else:
            self.balance -= bet_amount
            if len(self.sequence) == 1:
                bet_units = self.sequence[0]
            else:
                bet_units = self.sequence[0] + self.sequence[-1]
            self.sequence.append(bet_units)

        if not self.sequence:
            self.cycles_completed += 1
            log.info(f"💰 CICLO LABOUCHÈRE COMPLETADO #{self.cycles_completed}")
            self._restart_cycle()
            return True
        else:
            self.current_bet = self._calculate_bet()
            return False

    def get_state(self) -> dict:
        return {
            "sequence": self.sequence,
            "bet_amount": self.current_bet,
            "base_amount": self.base_amount,
            "initial_sequence": self.initial_sequence,
            "cycles_completed": self.cycles_completed,
            "capital": self.capital,
            "balance": self.balance,
            "total_bet": self.total_bet,
            "total_won": self.total_won,
            "profit": self.balance,
        }


# ══════════════════════════════════════════════
#  TELEGRAM
# ══════════════════════════════════════════════
bot = AsyncTeleBot(BOT_TOKEN, parse_mode="HTML") if (TELEBOT_OK and BOT_TOKEN) else None
if bot is None:
    log.warning("Telegram deshabilitado (falta BOT_TOKEN o la librería 'telebot').")

async def send_msg(text: str, thread_id: int, retries: int = 3) -> Optional[int]:
    if bot is None: return None
    delay = 1.0
    for attempt in range(1, retries + 1):
        try:
            msg = await bot.send_message(chat_id=CHAT_ID_BASE, text=text, parse_mode="HTML",
                                         disable_web_page_preview=True, message_thread_id=thread_id)
            return msg.message_id
        except Exception as e:
            retry_after = None
            try:
                retry_after = e.result_json.get("parameters", {}).get("retry_after")
            except Exception:
                pass
            wait = retry_after if retry_after else delay
            if attempt < retries:
                log.warning(f"[Telegram] Error enviando mensaje (thread={thread_id}, intento {attempt}/{retries}): {e} -> reintentando en {wait}s")
                await asyncio.sleep(wait)
                delay *= 2
            else:
                log.error(f"[Telegram] Fallo definitivo enviando mensaje (thread={thread_id}) tras {retries} intentos: {e}")
                return None

async def edit_msg(msg_id: int, text: str) -> bool:
    if bot is None or msg_id is None: return False
    try:
        await bot.edit_message_text(chat_id=CHAT_ID_BASE, message_id=msg_id, text=text,
                                    parse_mode="HTML", disable_web_page_preview=True)
        return True
    except Exception as e:
        log.debug(f"[Telegram] Error editando mensaje {msg_id}: {e}")
        return False

async def delete_msg(msg_id: int) -> bool:
    if bot is None or msg_id is None: return False
    try:
        await bot.delete_message(chat_id=CHAT_ID_BASE, message_id=msg_id)
        return True
    except Exception as e:
        log.debug(f"[Telegram] Error eliminando mensaje {msg_id}: {e}")
        return False

def build_entry_message_zone(last_number, bet_zone, bet_amount=None, start_attempt=1, sequence_str: str = "") -> str:
    numero = last_number if last_number is not None else "-"
    numero_emoji = ZONE_EMOJI.get(zone_of(last_number), "🟢") if last_number is not None else ""
    zone = bet_zone
    emoji = ZONE_EMOJI.get(zone, "")
    if zone == "BAJA":
        zone_line = f"🧨 ZONA BAJA: 1-18 ({emoji})"
    elif zone == "ALTA":
        zone_line = f"🧨 ZONA ALTA: 19-36 ({emoji})"
    else:
        zone_line = f"🧨 ZONA: -"
    if bet_amount is not None:
        apuesta_line = f"\n🇨🇴 APUESTA: {format_cop(bet_amount)}"
    else:
        apuesta_line = ""
    link_line = f'🎮 <a href="{TABLE_LINK}">{TABLE_NAME}</a>' if TABLE_LINK else f"🎮 {TABLE_NAME}"
    return (f"👉 INGRESAR DESPUÉS: {numero} ({numero_emoji})\n"
            f"{zone_line}\n"
            f"{apuesta_line}\n\n"
            f"💫 ¡Juegue con Responsabilidad!\n{link_line}")

def build_resolution_message(win: bool, numbers: list, balance: int) -> str:
    numbers_str = " | ".join(str(n) for n in numbers)
    if win:
        header = "✅✅ 👍🏻"
    else:
        header = "❌❌ 👎🏻"
    return f"{header} ({numbers_str}) | Apuesta: {format_cop(balance)}"

def build_daily_marker_message(stats: dict) -> str:
    win1 = stats.get("win1", 0)
    win2 = stats.get("win2", 0)
    loss = stats.get("loss", 0)
    total = win1 + win2 + loss
    if total == 0:
        return "📆 MARCADOR DIARIO\nSin señales aún."
    win1_pct = (win1 / total) * 100
    win2_pct = (win2 / total) * 100
    loss_pct = (loss / total) * 100
    global_pct = ((win1 + win2) / total) * 100
    return (f"📆 MARCADOR DIARIO\n"
            f"✅ Win 1: {win1} | Acierto: {win1_pct:.2f}%\n"
            f"✅ Win 2: {win2} | Acierto: {win2_pct:.2f}%\n"
            f"❌ Loss: {loss} | Fallos: {loss_pct:.2f}%\n"
            f"🎯 Total señales: {total}\n"
            f"📈 Efectividad Global: {global_pct:.2f}%")

def build_status_message(server_state) -> str:
    agent_keys = ["agent2", "agent3", "agent4", "agent6"]
    zone_keys = ["zone_agent1", "zone_agent2"]
    lines = ["📊 ESTADÍSTICAS POR PATRÓN"]
    for key, table in server_state.tables.items():
        lines.append(f"🎲 Mesa {key} ({TABLE_NAME})")
        lab_state = table.labouchere.get_state()
        seq_str = ','.join(str(x) for x in lab_state['sequence'])
        sign = '+' if lab_state['balance'] >= 0 else '-'
        lines.append(f"💹 Labouchère | Acum: {sign}{format_cop(abs(lab_state['balance']))} | Sec: [{seq_str}] | Sig: {format_cop(lab_state['bet_amount'])} | Ciclos: {lab_state['cycles_completed']}")
        for akey in agent_keys:
            agente = getattr(table, akey, None)
            if agente is None:
                continue
            s = agente.stats
            total = s.get("total", 0)
            won = s.get("won", 0)
            lost = s.get("lost", 0)
            rate = round((won / total) * 100, 1) if total else 0.0
            estado = "🟢 activa" if agente.train_state["active"] else "⚪ inactiva"
            rec_attempt, rec_pct = agente.overall_recommended_attempt()
            rec_line = (f"🧠 Intento recomendado: {rec_attempt} ({rec_pct}%)"
                        if rec_attempt else "🧠 Intento recomendado: aún sin datos suficientes")
            if agente.trained:
                modelo_line = "🤖 Modelo: entrenado"
            else:
                modelo_line = f"🤖 Modelo: en entrenamiento ({agente.total_processed}/{ML_MIN_SIGNALS_TO_TRAIN} señales)"
            lines.append(f"{agente.label}\n✅ {won}  ❌ {lost}  🎯 {total}  📈 {rate}%  {estado}\n{modelo_line}\n{rec_line}")
        for zkey in zone_keys:
            agente = getattr(table, zkey, None)
            if agente is None:
                continue
            s = agente.stats
            total = s.get("total", 0)
            won = s.get("won", 0)
            lost = s.get("lost", 0)
            rate = round((won / total) * 100, 1) if total else 0.0
            estado = "🟢 activa" if agente.train_state["active"] else "⚪ inactiva"
            rec_attempt, rec_pct = agente.overall_recommended_attempt()
            rec_line = (f"🧠 Intento recomendado: {rec_attempt} ({rec_pct}%)"
                        if rec_attempt else "🧠 Intento recomendado: aún sin datos suficientes")
            if agente.trained:
                modelo_line = "🤖 Modelo: entrenado"
            else:
                modelo_line = f"🤖 Modelo: en entrenamiento ({agente.total_processed}/{ML_MIN_SIGNALS_TO_TRAIN} señales)"
            lines.append(f"{agente.label}\n✅ {won}  ❌ {lost}  🎯 {total}  📈 {rate}%  {estado}\n{modelo_line}\n{rec_line}")
    return "\n\n".join(lines)

if bot is not None:
    @bot.message_handler(commands=["status"])
    async def handle_status_command(message):
        if _server_state is None:
            await bot.reply_to(message, "⏳ El servidor todavía se está iniciando, intenta de nuevo en unos segundos.")
            return
        try:
            await bot.reply_to(message, build_status_message(_server_state))
        except Exception as e:
            log.warning(f"[Telegram] Error respondiendo /status: {e}")


# ──────────────────────────────────────────────
#  DAILY MARKER
# ──────────────────────────────────────────────
class DailyMarker:
    def __init__(self, thread_signals=None):
        self.stats = {"win1": 0, "win2": 0, "loss": 0}
        self.thread_signals = thread_signals if thread_signals is not None else THREAD_SIGNALS

    async def record(self, win: bool, attempt: int = None):
        if win and attempt == 1:
            self.stats["win1"] = self.stats.get("win1", 0) + 1
        elif win and attempt == 2:
            self.stats["win2"] = self.stats.get("win2", 0) + 1
        elif not win:
            self.stats["loss"] = self.stats.get("loss", 0) + 1


# ══════════════════════════════════════════════
#  AGENTE DE PATRÓN DE DOCENAS (sin cambios)
# ══════════════════════════════════════════════
class DozenPatternAgent:
    def __init__(self, pattern_len: int, name: str, label: str, mode: str, daily_marker=None,
                 thread_signals=None, thread_stats=None):
        self.pattern_len = pattern_len
        self.name = name
        self.label = label
        self.mode = mode
        self.daily_marker = daily_marker
        self.thread_signals = thread_signals if thread_signals is not None else THREAD_SIGNALS_ZONE
        self.thread_stats = thread_stats if thread_stats is not None else THREAD_STATS_ZONE

        self.train_state = {
            "active": False, "pattern": None, "bet_dozens": None, "bet_zone": None,
            "attempts_left": 0, "total_attempts": DOZEN_MAX_ATTEMPTS,
            "context": None, "current_attempt": 0, "start_attempt": 1,
        }
        self.train_attempt_results = []
        self.live_enabled = True
        self.candidate_signal = None

        self.confirming = False
        self.pending_pattern = None

        self.history_log = []
        self.history_counter = 0
        self.stats = {"total": 0, "won": 0, "lost": 0}
        self.pattern_context = {}
        self.backtest = {"triggers": 0, "hits": 0, "accuracy": None}
        self.consecutive_losses = 0
        self.cooldown_remaining = 0
        self.msg_id = None
        self.entry_text = None
        self._last_raw_number = None
        self.total_processed = 0
        self.trained = False
        self.last_train_ts = 0.0
        self.trained_snapshot = {}

    # ── Matching de patrones completos ──
    def _match(self, window):
        if len(window) != self.pattern_len:
            return None
        if self.mode == "aaaba":
            a, b = window[0], window[3]
            ok = (window[1] == a and window[2] == a and window[4] == a)
            extra_ok = True
        elif self.mode == "aaba":
            a, b = window[0], window[2]
            ok = (window[1] == a and window[3] == a)
            extra_ok = True
        elif self.mode == "abaa":
            a, b = window[0], window[1]
            ok = (window[2] == a and window[3] == a)
            extra_ok = True
        elif self.mode == "aaaabaa":
            a, b = window[0], window[4]
            ok = (window[1] == a and window[2] == a and window[3] == a and window[5] == a and window[6] == a)
            extra_ok = (b != a)
        else:
            return None
        if not (ok and extra_ok and a in DOZEN_VALUES and b in DOZEN_VALUES and a != b):
            return None
        return (a, b)

    # ── Matching parcial (confirmación "-1 valor") ──
    def _match_partial(self, window):
        if len(window) != self.pattern_len - 1:
            return None
        if self.mode == "aaba":          # a a b a -> parcial a a b
            a, b = window[0], window[2]
            if window[1] == a and a in DOZEN_VALUES and b in DOZEN_VALUES and a != b:
                return (a, b, a)
        elif self.mode == "abaa":        # a b a a -> parcial a b a
            a, b = window[0], window[1]
            if window[2] == a and a in DOZEN_VALUES and b in DOZEN_VALUES and a != b:
                return (a, b, a)
        elif self.mode == "aaaba":       # a a a b a -> parcial a a a b
            a, b = window[0], window[3]
            if window[1] == a and window[2] == a and a in DOZEN_VALUES and b in DOZEN_VALUES and a != b:
                return (a, b, a)
        elif self.mode == "aaaabaa":     # a a a a b a a -> parcial a a a a b a
            a, b = window[0], window[4]
            if (window[1] == a and window[2] == a and window[3] == a and window[5] == a
                    and a in DOZEN_VALUES and b in DOZEN_VALUES and a != b):
                return (a, b, a)
        return None

    @staticmethod
    def _bet_dozens(pattern):
        return tuple(pattern)

    @staticmethod
    def _key(pattern):
        return ">".join(pattern)

    def _record_context(self, pattern, hit_attempt: int):
        key = self._key(pattern)
        arr = self.pattern_context.setdefault(key, [])
        arr.append(hit_attempt)
        if len(arr) > DOZEN_CONTEXT_WINDOW:
            del arr[0]

    def _maybe_train(self, timestamp: float):
        if self.total_processed < ML_MIN_SIGNALS_TO_TRAIN:
            return
        if not self.trained or (timestamp - self.last_train_ts) >= ML_RETRAIN_INTERVAL_SECONDS:
            self._train(timestamp)

    def _train(self, timestamp: float):
        self.trained_snapshot = {k: list(v) for k, v in self.pattern_context.items()}
        self.trained = True
        self.last_train_ts = timestamp

    def force_train(self, timestamp: float):
        self._train(timestamp)

    def _win_rate(self, pattern):
        if not self.trained:
            return None
        arr = self.trained_snapshot.get(self._key(pattern), [])
        if len(arr) < DOZEN_MIN_SAMPLES_GATE:
            return None
        return sum(1 for v in arr if v > 0) / len(arr)

    def _gated(self, pattern, required_win_rate):
        rate = self._win_rate(pattern)
        if rate is None:
            return False
        return rate < required_win_rate

    def _recommended_attempt(self, pattern):
        if not self.trained:
            return None
        arr = self.trained_snapshot.get(self._key(pattern), [])
        if len(arr) < DOZEN_MIN_SAMPLES_GATE:
            return None
        c1 = sum(1 for v in arr if v == 1)
        c2 = sum(1 for v in arr if v == 2)
        if c1 == 0 and c2 == 0:
            return None
        return 1 if c1 >= c2 else 2

    def overall_recommended_attempt(self):
        if not self.trained:
            return None, 0.0
        c1 = c2 = 0
        for arr in self.trained_snapshot.values():
            c1 += sum(1 for v in arr if v == 1)
            c2 += sum(1 for v in arr if v == 2)
        total = c1 + c2
        if total < DOZEN_MIN_SAMPLES_GATE:
            return None, 0.0
        if c1 >= c2:
            return 1, round(c1 / total * 100, 1)
        return 2, round(c2 / total * 100, 1)

    def _ml_should_signal(self, pattern, trend_dozens, amx_strength_val):
        if self.cooldown_remaining > 0:
            return False
        if trend_dozens is not None:
            expected_num = DOZEN_NUM.get(pattern[-1])
            if expected_num not in trend_dozens:
                return False
        base_rate = DOZEN_MIN_WIN_RATE
        if amx_strength_val >= AMX_STRENGTH_THRESHOLDS["strong"]:
            required_rate = base_rate * AMX_ADJUST_FACTOR_STRONG
        elif amx_strength_val < AMX_STRENGTH_THRESHOLDS["weak"]:
            required_rate = base_rate * AMX_ADJUST_FACTOR_WEAK
        else:
            required_rate = base_rate
        required_rate = max(0.20, min(0.60, required_rate))
        if self._gated(pattern, required_rate):
            return False
        return True

    # ── NUEVO: calcula la tasa de acierto del segundo intento dado que el primero falló ──
    def _second_attempt_win_rate(self, pattern):
        if not self.trained:
            return None
        arr = self.trained_snapshot.get(self._key(pattern), [])
        if len(arr) < DOZEN_MIN_SAMPLES_GATE:
            return None
        filtered = [v for v in arr if v != 1]
        if not filtered:
            return None
        wins = sum(1 for v in filtered if v == 2)
        return wins / len(filtered)

    def run_backtest(self, dozen_history):
        window = dozen_history[-DOZEN_BACKTEST_WINDOW:]
        triggers, hits = 0, 0
        for i in range(self.pattern_len, len(window) + 1):
            seg = window[i - self.pattern_len:i]
            pattern = self._match(seg)
            if not pattern:
                continue
            bet_dozens = self._bet_dozens(pattern)
            future = window[i:i + DOZEN_MAX_ATTEMPTS]
            triggers += 1
            if any(d in future for d in bet_dozens) or "VERDE" in future:
                hits += 1
        self.backtest = {
            "triggers": triggers, "hits": hits,
            "accuracy": round(hits / triggers, 4) if triggers else None
        }

    def _full_pattern(self, a, b, expected):
        return (a, b)

    def update(self, dozen_history, timestamp, blocked: bool = False,
               trend_dozens=None, amx_strength_val=0.0, last_number=None,
               live_enabled: bool = True):
        self._last_raw_number = last_number
        self.live_enabled = live_enabled
        self.candidate_signal = None
        if not dozen_history:
            return
        last = dozen_history[-1]

        # 1) Shadow tracking
        if self.train_state["active"]:
            self.train_state["current_attempt"] += 1
            attempt = self.train_state["start_attempt"] + self.train_state["current_attempt"] - 1
            is_zero = (last == "VERDE")
            is_win = is_zero or (last in self.train_state["bet_dozens"])
            self.train_attempt_results.append(last_number)
            if is_win:
                self._close_shadow(True, last, attempt, timestamp)
            else:
                self.train_state["attempts_left"] -= 1
                if self.train_state["attempts_left"] <= 0:
                    self._close_shadow(False, last, attempt, timestamp)

        # 2) Backtest / cooldown / ML
        self.run_backtest(dozen_history)
        if self.cooldown_remaining > 0:
            self.cooldown_remaining -= 1
        self._maybe_train(timestamp)

        # 3) Buscar patrón parcial para confirmación
        if (not self.train_state["active"] and not self.confirming
                and len(dozen_history) >= self.pattern_len - 1
                and len(dozen_history) >= DOZEN_MIN_SPIN_TO_SIGNAL
                and not blocked):
            partial = self._match_partial(dozen_history[-(self.pattern_len - 1):])
            if partial:
                a, b, expected = partial
                full_pattern = self._full_pattern(a, b, expected)
                if self._ml_should_signal(full_pattern, trend_dozens, amx_strength_val):
                    self.confirming = True
                    self.pending_pattern = (a, b, expected)
                    self.candidate_signal = {
                        "pattern": full_pattern,
                        "confirming": True,
                        "expected_last": expected,
                        "amx_strength": amx_strength_val,
                    }
                    log.info(f"🔍 {self.name} confirmación pendiente: {a},{b} -> esperado {expected}")
                    return

        # 4) Evaluar confirmación
        if self.confirming and self.pending_pattern:
            a, b, expected = self.pending_pattern
            if last == expected:
                pattern = self._full_pattern(a, b, expected)
                bet_dozens = self._bet_dozens(pattern)
                zone = dozen_bet_to_zone(bet_dozens, pattern)
                context = list(dozen_history[-DOZEN_CONTEXT_WINDOW:])
                self.candidate_signal = {
                    "pattern": pattern,
                    "bet_dozens": bet_dozens,
                    "bet_zone": (zone,) if zone is not None else None,
                    "context": context,
                    "start_attempt": 1,
                    "amx_strength": amx_strength_val,
                    "score": self._win_rate(pattern) or 0.0,
                    "confirming": False,
                }
                log.info(f"✅ {self.name} confirmación correcta: {pattern} -> ZONA {zone if zone else 'a decidir (opuesto)'}")
                self.train_state = {
                    "active": True, "pattern": pattern, "bet_dozens": bet_dozens,
                    "bet_zone": zone,
                    "attempts_left": DOZEN_MAX_ATTEMPTS, "total_attempts": DOZEN_MAX_ATTEMPTS,
                    "context": context, "current_attempt": 0, "start_attempt": 1,
                }
            else:
                log.info(f"❌ {self.name} confirmación fallida: esperaba {expected}, salió {last}")
            self.confirming = False
            self.pending_pattern = None

    def _close_shadow(self, win: bool, result_dozen, attempt, timestamp):
        pattern = tuple(self.train_state["pattern"])
        bet_dozens = tuple(self.train_state["bet_dozens"])
        hit_attempt = attempt if win else 0
        self.history_counter += 1
        self.history_log.append({
            "n": self.history_counter, "pattern": ">".join(pattern),
            "bet_dozens": list(bet_dozens), "bet_zone": self.train_state["bet_zone"],
            "result": result_dozen, "attempt": attempt, "win": win,
            "hit_attempt": hit_attempt, "context": self.train_state.get("context"),
            "time": timestamp, "shadow": True,
        })
        self.history_log = self.history_log[-200:]
        self.stats["total"] += 1
        self.stats["won" if win else "lost"] += 1
        self._record_context(pattern, hit_attempt)
        self.total_processed += 1
        self._maybe_train(timestamp)

        if win:
            self.consecutive_losses = 0
        else:
            self.consecutive_losses += 1
            if self.consecutive_losses >= DOZEN_COOLDOWN_AFTER_LOSSES:
                self.cooldown_remaining = DOZEN_COOLDOWN_ROUNDS

        self.train_state = {
            "active": False, "pattern": None, "bet_dozens": None, "bet_zone": None,
            "attempts_left": 0, "total_attempts": DOZEN_MAX_ATTEMPTS,
            "context": None, "current_attempt": 0, "start_attempt": 1,
        }
        self.train_attempt_results = []

    def reset_transient(self):
        self.confirming = False
        self.pending_pattern = None
        self.candidate_signal = None
        self.train_state = {
            "active": False, "pattern": None, "bet_dozens": None, "bet_zone": None,
            "attempts_left": 0, "total_attempts": DOZEN_MAX_ATTEMPTS,
            "context": None, "current_attempt": 0, "start_attempt": 1,
        }
        self.train_attempt_results = []

    def get_state(self):
        rec_attempt, rec_pct = self.overall_recommended_attempt()
        pattern_recommendations = {
            key: self._recommended_attempt(tuple(key.split(">")))
            for key in self.pattern_context
        }
        pattern_recommendations = {k: v for k, v in pattern_recommendations.items() if v is not None}
        return {
            "name": self.name,
            "pattern_len": self.pattern_len,
            "mode": self.mode,
            "train_state": self.train_state,
            "stats": self.stats,
            "history": self.history_log[-30:],
            "backtest_60": self.backtest,
            "pattern_context": self.pattern_context,
            "consecutive_losses": self.consecutive_losses,
            "cooldown_remaining": self.cooldown_remaining,
            "recommended_attempt": rec_attempt,
            "recommended_attempt_pct": rec_pct,
            "pattern_recommendations": pattern_recommendations,
            "confirming": self.confirming,
            "live_enabled": self.live_enabled,
            "ml_model": {
                "trained": self.trained,
                "total_processed": self.total_processed,
                "min_signals_to_train": ML_MIN_SIGNALS_TO_TRAIN,
                "last_train_ts": self.last_train_ts,
                "retrain_interval_seconds": ML_RETRAIN_INTERVAL_SECONDS,
            },
        }

    def to_persist(self):
        return {
            "pattern_context": self.pattern_context,
            "stats": self.stats,
            "history_counter": self.history_counter,
            "consecutive_losses": self.consecutive_losses,
            "cooldown_remaining": self.cooldown_remaining,
            "total_processed": self.total_processed,
            "trained": self.trained,
            "last_train_ts": self.last_train_ts,
            "trained_snapshot": self.trained_snapshot,
        }

    def load_persist(self, data):
        if not data: return
        self.pattern_context = data.get("pattern_context", {})
        self.stats = data.get("stats", self.stats)
        self.history_counter = data.get("history_counter", 0)
        self.consecutive_losses = data.get("consecutive_losses", 0)
        self.cooldown_remaining = data.get("cooldown_remaining", 0)
        self.total_processed = data.get("total_processed", 0)
        self.trained = data.get("trained", False)
        self.last_train_ts = data.get("last_train_ts", 0.0)
        self.trained_snapshot = data.get("trained_snapshot", {})


# ══════════════════════════════════════════════
#  AGENTE DE PATRÓN DE ZONAS (ACTUALIZADO CON COMODÍN Y CERCANÍA)
# ══════════════════════════════════════════════
class ZonePatternAgent:
    """
    Agente que detecta patrones sobre la secuencia de zonas (BAJA/ALTA/VERDE).
    El patrón es una cadena de caracteres donde 'a' = BAJA, 'b' = ALTA.
    El cero (VERDE) es un comodín que puede aparecer en cualquier posición de la historia.
    La zona predicha es la correspondiente a la última letra del patrón (debe ser 'a' o 'b').
    Si el cero está cerca del final (en las últimas 2 posiciones), se invierte la secuencia:
    primer intento = opuesto, segundo = predicha.
    """
    def __init__(self, pattern: str, name: str, label: str, daily_marker=None,
                 thread_signals=None, thread_stats=None):
        self.pattern = pattern  # solo 'a' y 'b'
        self.pattern_len = len(pattern)
        self.name = name
        self.label = label
        self.daily_marker = daily_marker
        self.thread_signals = thread_signals if thread_signals is not None else THREAD_SIGNALS_ZONE
        self.thread_stats = thread_stats if thread_stats is not None else THREAD_STATS_ZONE

        # Mapeo de letras a zonas (sin verde)
        self.letter_to_zone = {'a': 'BAJA', 'b': 'ALTA'}
        # Última letra debe ser 'a' o 'b' para que haya apuesta
        self.predicted_zone = self.letter_to_zone.get(pattern[-1]) if pattern[-1] in ('a','b') else None
        if self.predicted_zone is None:
            log.warning(f"El patrón {pattern} termina en '{pattern[-1]}', no se puede predecir zona. Se desactivará.")
            self.active = False
        else:
            self.active = True
        # Umbral de distancia para considerar cero cerca (posiciones desde el final)
        self.zero_proximity_threshold = 2  # si el cero está en las últimas 2 posiciones

        self.train_state = {
            "active": False, "pattern": None, "bet_zone": None,
            "attempts_left": 0, "total_attempts": DOZEN_MAX_ATTEMPTS,
            "context": None, "current_attempt": 0, "start_attempt": 1,
        }
        self.train_attempt_results = []
        self.live_enabled = True
        self.candidate_signal = None

        self.confirming = False
        self.pending_pattern = None
        self.pending_window = None   # guardar la ventana que confirmó el patrón para evaluar ceros

        self.history_log = []
        self.history_counter = 0
        self.stats = {"total": 0, "won": 0, "lost": 0}
        self.pattern_context = {}
        self.backtest = {"triggers": 0, "hits": 0, "accuracy": None}
        self.consecutive_losses = 0
        self.cooldown_remaining = 0
        self.msg_id = None
        self.entry_text = None
        self._last_raw_number = None
        self.total_processed = 0
        self.trained = False
        self.last_train_ts = 0.0
        self.trained_snapshot = {}

    # ── Matching de patrones completos con comodín (cero) ──
    def _match(self, window):
        """window es lista de zonas (strings) de longitud self.pattern_len.
           Retorna la tupla de zonas que conforman el patrón si coincide,
           considerando que 'VERDE' en la historia coincide con cualquier letra del patrón.
           Retorna None si no coincide."""
        if len(window) != self.pattern_len:
            return None
        # Convertir las letras del patrón a zonas esperadas
        expected = [self.letter_to_zone.get(ch) for ch in self.pattern]
        if any(e is None for e in expected):
            return None
        # Comparar: si la historia es VERDE, siempre coincide; si no, debe ser igual
        for w, e in zip(window, expected):
            if w == "VERDE":
                continue
            if w != e:
                return None
        # Si llegamos aquí, coincide. Devolvemos la tupla de zonas esperadas (sin comodines)
        return tuple(expected)

    # ── Matching parcial (confirmación "-1 valor") ──
    def _match_partial(self, window):
        """window es lista de zonas de longitud self.pattern_len - 1.
           Retorna el patrón completo esperado si los primeros N-1 coinciden,
           considerando el cero como comodín.
           El último elemento del patrón es el que se espera en el siguiente giro."""
        if len(window) != self.pattern_len - 1:
            return None
        expected_full = [self.letter_to_zone.get(ch) for ch in self.pattern]
        if any(e is None for e in expected_full):
            return None
        # Verificar que los primeros N-1 coincidan (con comodín)
        for w, e in zip(window, expected_full[:-1]):
            if w == "VERDE":
                continue
            if w != e:
                return None
        # Si coincide, devolvemos el patrón completo (sin comodines)
        return tuple(expected_full)

    @staticmethod
    def _key(pattern_tuple):
        return ">".join(pattern_tuple)

    def _record_context(self, pattern_tuple, hit_attempt: int):
        key = self._key(pattern_tuple)
        arr = self.pattern_context.setdefault(key, [])
        arr.append(hit_attempt)
        if len(arr) > DOZEN_CONTEXT_WINDOW:
            del arr[0]

    def _maybe_train(self, timestamp: float):
        if self.total_processed < ML_MIN_SIGNALS_TO_TRAIN:
            return
        if not self.trained or (timestamp - self.last_train_ts) >= ML_RETRAIN_INTERVAL_SECONDS:
            self._train(timestamp)

    def _train(self, timestamp: float):
        self.trained_snapshot = {k: list(v) for k, v in self.pattern_context.items()}
        self.trained = True
        self.last_train_ts = timestamp

    def force_train(self, timestamp: float):
        self._train(timestamp)

    def _win_rate(self, pattern_tuple):
        if not self.trained:
            return None
        arr = self.trained_snapshot.get(self._key(pattern_tuple), [])
        if len(arr) < DOZEN_MIN_SAMPLES_GATE:
            return None
        return sum(1 for v in arr if v > 0) / len(arr)

    def _gated(self, pattern_tuple, required_win_rate):
        rate = self._win_rate(pattern_tuple)
        if rate is None:
            return False
        return rate < required_win_rate

    def _recommended_attempt(self, pattern_tuple):
        if not self.trained:
            return None
        arr = self.trained_snapshot.get(self._key(pattern_tuple), [])
        if len(arr) < DOZEN_MIN_SAMPLES_GATE:
            return None
        c1 = sum(1 for v in arr if v == 1)
        c2 = sum(1 for v in arr if v == 2)
        if c1 == 0 and c2 == 0:
            return None
        return 1 if c1 >= c2 else 2

    def overall_recommended_attempt(self):
        if not self.trained:
            return None, 0.0
        c1 = c2 = 0
        for arr in self.trained_snapshot.values():
            c1 += sum(1 for v in arr if v == 1)
            c2 += sum(1 for v in arr if v == 2)
        total = c1 + c2
        if total < DOZEN_MIN_SAMPLES_GATE:
            return None, 0.0
        if c1 >= c2:
            return 1, round(c1 / total * 100, 1)
        return 2, round(c2 / total * 100, 1)

    def _ml_should_signal(self, pattern_tuple, amx_strength_val):
        if self.cooldown_remaining > 0:
            return False
        base_rate = DOZEN_MIN_WIN_RATE
        if amx_strength_val >= AMX_STRENGTH_THRESHOLDS["strong"]:
            required_rate = base_rate * AMX_ADJUST_FACTOR_STRONG
        elif amx_strength_val < AMX_STRENGTH_THRESHOLDS["weak"]:
            required_rate = base_rate * AMX_ADJUST_FACTOR_WEAK
        else:
            required_rate = base_rate
        required_rate = max(0.20, min(0.60, required_rate))
        if self._gated(pattern_tuple, required_rate):
            return False
        return True

    def run_backtest(self, zone_history):
        window = zone_history[-DOZEN_BACKTEST_WINDOW:]
        triggers, hits = 0, 0
        for i in range(self.pattern_len, len(window) + 1):
            seg = window[i - self.pattern_len:i]
            pattern_tuple = self._match(seg)
            if not pattern_tuple:
                continue
            predicted_zone = pattern_tuple[-1]
            if predicted_zone == "VERDE":
                continue
            future = window[i:i + DOZEN_MAX_ATTEMPTS]
            triggers += 1
            if any(z == predicted_zone for z in future):
                hits += 1
        self.backtest = {
            "triggers": triggers, "hits": hits,
            "accuracy": round(hits / triggers, 4) if triggers else None
        }

    def update(self, zone_history, timestamp, blocked: bool = False,
               amx_strength_val=0.0, last_number=None,
               live_enabled: bool = True):
        if not self.active:
            return
        self._last_raw_number = last_number
        self.live_enabled = live_enabled
        self.candidate_signal = None
        if not zone_history:
            return
        last_zone = zone_history[-1]

        # 1) Shadow tracking
        if self.train_state["active"]:
            self.train_state["current_attempt"] += 1
            attempt = self.train_state["start_attempt"] + self.train_state["current_attempt"] - 1
            bet_zone = self.train_state["bet_zone"]
            is_win = zone_win(bet_zone, last_number) if last_number is not None else False
            self.train_attempt_results.append(last_number)
            if is_win:
                self._close_shadow(True, last_zone, attempt, timestamp, last_number)
            else:
                self.train_state["attempts_left"] -= 1
                if self.train_state["attempts_left"] <= 0:
                    self._close_shadow(False, last_zone, attempt, timestamp, last_number)

        # 2) Backtest / cooldown / ML
        self.run_backtest(zone_history)
        if self.cooldown_remaining > 0:
            self.cooldown_remaining -= 1
        self._maybe_train(timestamp)

        # 3) Buscar patrón parcial para confirmación
        if (not self.train_state["active"] and not self.confirming
                and len(zone_history) >= self.pattern_len - 1
                and len(zone_history) >= DOZEN_MIN_SPIN_TO_SIGNAL
                and not blocked):
            partial = self._match_partial(zone_history[-(self.pattern_len - 1):])
            if partial:
                pattern_tuple = partial
                predicted_zone = pattern_tuple[-1]
                if predicted_zone == "VERDE":
                    log.info(f"⛔ {self.name}: patrón termina en VERDE, no se genera señal")
                    return
                if self._ml_should_signal(pattern_tuple, amx_strength_val):
                    self.confirming = True
                    self.pending_pattern = pattern_tuple
                    # Guardamos la ventana que usamos para la confirmación (últimos pattern_len-1)
                    self.pending_window = zone_history[-(self.pattern_len - 1):]
                    self.candidate_signal = {
                        "pattern": pattern_tuple,
                        "confirming": True,
                        "expected_last": predicted_zone,
                        "amx_strength": amx_strength_val,
                    }
                    log.info(f"🔍 {self.name} confirmación pendiente: patrón {pattern_tuple} -> esperado {predicted_zone}")
                    return

        # 4) Evaluar confirmación
        if self.confirming and self.pending_pattern:
            pattern_tuple = self.pending_pattern
            expected_last = pattern_tuple[-1]
            if last_zone == expected_last:
                # Patrón confirmado
                predicted_zone = pattern_tuple[-1]
                if predicted_zone == "VERDE":
                    log.info(f"⛔ {self.name}: patrón confirmado pero termina en VERDE, no se genera señal")
                    self.confirming = False
                    self.pending_pattern = None
                    self.pending_window = None
                    return

                # Evaluar cercanía del cero en la ventana de confirmación (incluyendo el último giro)
                # La ventana completa que confirma el patrón son los últimos pattern_len elementos
                confirm_window = zone_history[-self.pattern_len:]
                # Buscar posiciones de ceros
                zero_positions = [i for i, z in enumerate(confirm_window) if z == "VERDE"]
                near_zero = False
                if zero_positions:
                    closest_zero = max(zero_positions)  # índice desde el inicio de la ventana
                    distance_from_end = self.pattern_len - 1 - closest_zero
                    if distance_from_end <= self.zero_proximity_threshold:
                        near_zero = True
                        log.info(f"🔄 {self.name}: cero cerca (distancia {distance_from_end} desde el final), se invertirá la secuencia")

                context = list(zone_history[-DOZEN_CONTEXT_WINDOW:])
                # Determinar secuencia de zonas (dos intentos)
                opposite = "ALTA" if predicted_zone == "BAJA" else "BAJA"
                if near_zero:
                    # Invertir: primero opuesto, luego predicho
                    zone_sequence = [opposite, predicted_zone]
                else:
                    # Normal: primero predicho, luego opuesto
                    zone_sequence = [predicted_zone, opposite]

                self.candidate_signal = {
                    "pattern": pattern_tuple,
                    "bet_zone": (predicted_zone,),  # la zona predicha (útil para referencia)
                    "zone_sequence": zone_sequence,  # secuencia ya calculada
                    "context": context,
                    "start_attempt": 1,
                    "amx_strength": amx_strength_val,
                    "score": self._win_rate(pattern_tuple) or 0.0,
                    "confirming": False,
                    "near_zero": near_zero,
                }
                log.info(f"✅ {self.name} confirmación correcta: {pattern_tuple} -> ZONA {predicted_zone}, secuencia {zone_sequence}")
                self.train_state = {
                    "active": True, "pattern": pattern_tuple, "bet_zone": predicted_zone,
                    "attempts_left": DOZEN_MAX_ATTEMPTS, "total_attempts": DOZEN_MAX_ATTEMPTS,
                    "context": context, "current_attempt": 0, "start_attempt": 1,
                }
            else:
                log.info(f"❌ {self.name} confirmación fallida: esperaba {expected_last}, salió {last_zone}")
            self.confirming = False
            self.pending_pattern = None
            self.pending_window = None

    def _close_shadow(self, win: bool, result_zone, attempt, timestamp, last_number):
        pattern_tuple = tuple(self.train_state["pattern"])
        hit_attempt = attempt if win else 0
        self.history_counter += 1
        self.history_log.append({
            "n": self.history_counter, "pattern": ">".join(pattern_tuple),
            "bet_zone": self.train_state["bet_zone"],
            "result": result_zone, "attempt": attempt, "win": win,
            "hit_attempt": hit_attempt, "context": self.train_state.get("context"),
            "time": timestamp, "shadow": True,
        })
        self.history_log = self.history_log[-200:]
        self.stats["total"] += 1
        self.stats["won" if win else "lost"] += 1
        self._record_context(pattern_tuple, hit_attempt)
        self.total_processed += 1
        self._maybe_train(timestamp)

        if win:
            self.consecutive_losses = 0
        else:
            self.consecutive_losses += 1
            if self.consecutive_losses >= DOZEN_COOLDOWN_AFTER_LOSSES:
                self.cooldown_remaining = DOZEN_COOLDOWN_ROUNDS

        self.train_state = {
            "active": False, "pattern": None, "bet_zone": None,
            "attempts_left": 0, "total_attempts": DOZEN_MAX_ATTEMPTS,
            "context": None, "current_attempt": 0, "start_attempt": 1,
        }
        self.train_attempt_results = []

    def reset_transient(self):
        self.confirming = False
        self.pending_pattern = None
        self.pending_window = None
        self.candidate_signal = None
        self.train_state = {
            "active": False, "pattern": None, "bet_zone": None,
            "attempts_left": 0, "total_attempts": DOZEN_MAX_ATTEMPTS,
            "context": None, "current_attempt": 0, "start_attempt": 1,
        }
        self.train_attempt_results = []

    def get_state(self):
        rec_attempt, rec_pct = self.overall_recommended_attempt()
        pattern_recommendations = {
            key: self._recommended_attempt(tuple(key.split(">")))
            for key in self.pattern_context
        }
        pattern_recommendations = {k: v for k, v in pattern_recommendations.items() if v is not None}
        return {
            "name": self.name,
            "pattern_len": self.pattern_len,
            "pattern": self.pattern,
            "train_state": self.train_state,
            "stats": self.stats,
            "history": self.history_log[-30:],
            "backtest_60": self.backtest,
            "pattern_context": self.pattern_context,
            "consecutive_losses": self.consecutive_losses,
            "cooldown_remaining": self.cooldown_remaining,
            "recommended_attempt": rec_attempt,
            "recommended_attempt_pct": rec_pct,
            "pattern_recommendations": pattern_recommendations,
            "confirming": self.confirming,
            "live_enabled": self.live_enabled,
            "ml_model": {
                "trained": self.trained,
                "total_processed": self.total_processed,
                "min_signals_to_train": ML_MIN_SIGNALS_TO_TRAIN,
                "last_train_ts": self.last_train_ts,
                "retrain_interval_seconds": ML_RETRAIN_INTERVAL_SECONDS,
            },
        }

    def to_persist(self):
        return {
            "pattern_context": self.pattern_context,
            "stats": self.stats,
            "history_counter": self.history_counter,
            "consecutive_losses": self.consecutive_losses,
            "cooldown_remaining": self.cooldown_remaining,
            "total_processed": self.total_processed,
            "trained": self.trained,
            "last_train_ts": self.last_train_ts,
            "trained_snapshot": self.trained_snapshot,
        }

    def load_persist(self, data):
        if not data: return
        self.pattern_context = data.get("pattern_context", {})
        self.stats = data.get("stats", self.stats)
        self.history_counter = data.get("history_counter", 0)
        self.consecutive_losses = data.get("consecutive_losses", 0)
        self.cooldown_remaining = data.get("cooldown_remaining", 0)
        self.total_processed = data.get("total_processed", 0)
        self.trained = data.get("trained", False)
        self.last_train_ts = data.get("last_train_ts", 0.0)
        self.trained_snapshot = data.get("trained_snapshot", {})


# ══════════════════════════════════════════════
#  ROULETTE TABLE (ajustes para usar secuencia directa de agentes zona)
# ══════════════════════════════════════════════
class RouletteTable:
    def __init__(self, key: int):
        self.key = key
        self.spin_history = []
        self.prev_number = None
        self.last_update_time = time.time()
        self.total_spins_seen = 0
        self.live_spins_seen = 0

        self.dozen_history = []
        self.zone_history = []
        self.daily_marker = DailyMarker()
        self.labouchere = LabouchereManager(base_amount=LABOUCHERE_BASE_AMOUNT)
        self.cycle_pending = 0

        self.signal_sequence = []
        self.current_attempt_index = 0
        self.signal_status = None
        self.attempt_numbers = []
        self.attempt_zones = []
        self.attempt_bets = []
        self.entry_msg_ids = []
        self.confirming = False
        self.pending_agent = None
        self.pending_candidate = None
        self.confirmation_msg_id = None

        self._pending_new_signal = None
        self._signal_included = False

        # ── AGENTES DE DOCENAS ──
        self.agent2 = DozenPatternAgent(pattern_len=4, name="AGENTE_2", label="PATRON V2 💎 (aaba)", mode="aaba", daily_marker=self.daily_marker)
        self.agent3 = DozenPatternAgent(pattern_len=5, name="AGENTE_3", label="PATRON V3 💎 (aaaba)", mode="aaaba", daily_marker=self.daily_marker)
        self.agent4 = DozenPatternAgent(pattern_len=4, name="AGENTE_4", label="PATRON V4 💎 (abaa)", mode="abaa", daily_marker=self.daily_marker)
        self.agent6 = DozenPatternAgent(pattern_len=7, name="AGENTE_6", label="PATRON V6 💎 (aaaabaa)", mode="aaaabaa", daily_marker=self.daily_marker)

        # ── AGENTES DE ZONA ──
        # Patrones solo con a/b; el cero es comodín
        self.zone_agent1 = ZonePatternAgent(pattern='baaaabbb', name="ZONE_AGENT_1", label="ZONA LARGA 1 (b+4a+3b)", daily_marker=self.daily_marker)
        self.zone_agent2 = ZonePatternAgent(pattern='aaaabbbbaa', name="ZONE_AGENT_2", label="ZONA LARGA 2 (4a+4b+2a)", daily_marker=self.daily_marker)
        # Se pueden añadir más con patrones como 'aaabbb', etc.

        self.level_history = []
        self.level_current = 0
        self.last_dozen_num = None
        self.last_d2_number = None
        self.trend = "neutral"
        self.last_nonzero_zone = "BAJA"

    def _level_change(self, number: int, real_dozen_num: int) -> int:
        if real_dozen_num == 1: return 1
        if real_dozen_num == 2: return 1 if number <= 18 else -1
        if real_dozen_num == 3: return -1
        if self.last_dozen_num == 1: return 1
        if self.last_dozen_num == 2: return 1 if (self.last_d2_number is not None and self.last_d2_number <= 18) else -1
        if self.last_dozen_num == 3: return -1
        return 0

    async def _send_confirmation(self):
        self.confirmation_msg_id = None
        return None

    async def _send_entry(self, agent, zone, bet_amount, attempt_number):
        seq_txt = self.labouchere.seq_str()
        original = build_entry_message_zone(
            agent._last_raw_number,
            zone,
            bet_amount=bet_amount,
            sequence_str=seq_txt
        )
        new_header = f"🚨🚨 ENTRADA INTENTO {attempt_number} 🚨🚨"
        entry_text = f"{new_header}\n\n{original}"
        msg_id = await send_msg(entry_text, agent.thread_signals)

        if len(self.attempt_bets) >= attempt_number:
            self.attempt_bets[attempt_number - 1] = bet_amount
        else:
            self.attempt_bets.append(bet_amount)

        if len(self.entry_msg_ids) >= attempt_number:
            self.entry_msg_ids[attempt_number - 1] = msg_id
        else:
            self.entry_msg_ids.append(msg_id)
        if attempt_number > 1 and len(self.entry_msg_ids) >= attempt_number - 1:
            prev_id = self.entry_msg_ids[attempt_number - 2]
            if prev_id:
                await delete_msg(prev_id)
        return msg_id

    async def _send_resolution(self, win: bool, numbers: list, balance: int, extra_text: str = ""):
        res_text = build_resolution_message(win, numbers, balance)
        if extra_text:
            full_text = f"{res_text}\n————————————————————\n{extra_text}"
        else:
            full_text = res_text
        await send_msg(full_text, THREAD_SIGNALS)
        if win:
            simple = "✅ WIN"
        else:
            simple = "🚫 LOSS"
        await send_msg(simple, THREAD_STATS)

    async def _send_daily_marker_and_cycle(self):
        total = (self.daily_marker.stats.get("win1", 0) + self.daily_marker.stats.get("win2", 0)
                 + self.daily_marker.stats.get("loss", 0))
        if total > 0:
            text = build_daily_marker_message(self.daily_marker.stats)
            await send_msg(text, self.daily_marker.thread_signals)
        if self.cycle_pending > 0:
            lab_state = self.labouchere.get_state()
            sign = '+' if lab_state['balance'] >= 0 else '-'
            msg = (f"🎉🎉 CICLO #{self.cycle_pending} COMPLETA 🎉🎉\n"
                   f"📈 Acumulado: {sign}{format_cop(abs(lab_state['balance']))}\n"
                   f"🇨🇴 Apuesta Base: {format_cop(lab_state['base_amount'])}\n")
            await send_msg(msg, THREAD_SIGNALS)
            self.cycle_pending = 0

    def _finalize_sequence(self, win: bool, winning_attempt: int = None):
        if win and winning_attempt == 1:
            balance = self.attempt_bets[0]
        elif win and winning_attempt == 2:
            balance = self.attempt_bets[1] - self.attempt_bets[0]
        else:
            balance = -(self.attempt_bets[0] + self.attempt_bets[1])

        extra_text = ""
        if self._pending_new_signal is not None:
            new_signal = self._pending_new_signal
            agent = new_signal["agent"]
            zone_sequence = new_signal["zone_sequence"]
            if self.current_attempt_index == 0:
                header = "🔥🔥 NUEVA SEÑAL CONFIRMADA 🔥🔥"
            else:
                header = "🔥🔥 REPETIR SEÑAL CONFIRMADA 🔥🔥"
            last_num = agent._last_raw_number
            zone = zone_sequence[0]
            bet_amount = self.labouchere.get_bet()
            entry_body = build_entry_message_zone(last_num, zone, bet_amount=bet_amount)
            extra_text = f"{header}\n\n{entry_body}"
            self._pending_new_signal = None
            self._signal_included = True

        asyncio.create_task(self._send_resolution(win, self.attempt_numbers, balance, extra_text))
        asyncio.create_task(self.daily_marker.record(win, winning_attempt))
        asyncio.create_task(self._send_daily_marker_and_cycle())

        self.signal_sequence = []
        self.current_attempt_index = 0
        self.signal_status = None
        self.attempt_numbers = []
        self.attempt_zones = []
        self.attempt_bets = []
        self.entry_msg_ids = []
        self.pending_agent = None
        self.pending_candidate = None
        self.confirming = False
        if self.confirmation_msg_id:
            asyncio.create_task(delete_msg(self.confirmation_msg_id))
            self.confirmation_msg_id = None

    def _select_best_candidate(self, candidates):
        if not candidates:
            return None, None
        best_agent, best_score, best_candidate = max(candidates, key=lambda x: x[1])
        return best_agent, best_candidate

    def _determine_zone_sequence(self, agent, candidate, bet_zone_tuple, amx_strength):
        """
        Para agentes de zona, la secuencia ya viene calculada en el candidato.
        Para agentes de docenas, se aplica la lógica de segundo intento adaptativo.
        """
        if isinstance(agent, ZonePatternAgent):
            # La secuencia ya está en el candidato
            return candidate.get("zone_sequence")
        else:
            # Agente de docenas
            zone = bet_zone_tuple[0]
            pattern = candidate["pattern"]
            second_rate = agent._second_attempt_win_rate(pattern)
            use_opposite = False
            if second_rate is not None:
                if second_rate < SECOND_ATTEMPT_OPPOSITE_THRESHOLD:
                    use_opposite = True
                    log.info(f"🔄 {agent.name} patrón {pattern}: segundo intento al mismo lado tiene tasa {second_rate:.2f} < umbral, se usará opuesto")
            else:
                if amx_strength < AMX_STRENGTH_THRESHOLDS["weak"]:
                    use_opposite = True
                    log.info(f"🔄 {agent.name} patrón {pattern}: sin datos de segundo intento, AMX débil ({amx_strength:.2f}), se usará opuesto")
            if use_opposite:
                opposite = "ALTA" if zone == "BAJA" else "BAJA"
                return [zone, opposite]
            else:
                return [zone, zone]

    def _prepare_new_signal(self, agent, candidate, last_number):
        """Prepara la nueva señal para ser incluida en el mensaje de resolución."""
        bet_zone_tuple = candidate.get("bet_zone")
        amx_strength = candidate.get("amx_strength", 0.0)
        if bet_zone_tuple is not None:
            zone_sequence = self._determine_zone_sequence(agent, candidate, bet_zone_tuple, amx_strength)
        else:
            # D1+D3: opuesto
            last_zone = self.last_nonzero_zone
            opposite = "ALTA" if last_zone == "BAJA" else "BAJA"
            zone_sequence = [opposite, opposite]
            log.info(f"🔀 Señal D1+D3 → opuesto de última zona ({last_zone}) → {opposite} en ambos intentos")

        self._pending_new_signal = {
            "agent": agent,
            "zone_sequence": zone_sequence,
        }
        agent.candidate_signal = None

    def _activate_new_signal(self, agent, candidate, bet_amount):
        """Activa una nueva señal (envía entrada) cuando no hay señal activa."""
        bet_zone_tuple = candidate.get("bet_zone")
        amx_strength = candidate.get("amx_strength", 0.0)
        if bet_zone_tuple is not None:
            zone_sequence = self._determine_zone_sequence(agent, candidate, bet_zone_tuple, amx_strength)
        else:
            # D1+D3: opuesto
            last_zone = self.last_nonzero_zone
            opposite = "ALTA" if last_zone == "BAJA" else "BAJA"
            zone_sequence = [opposite, opposite]
            log.info(f"🔀 Señal D1+D3 → opuesto de última zona ({last_zone}) → {opposite} en ambos intentos")

        new_entry = {
            "agent": agent,
            "original": candidate,
            "zone_sequence": zone_sequence,
        }

        if self.signal_status == "waiting_pattern":
            self.signal_sequence = [new_entry]
            self.current_attempt_index = 1
            self.signal_status = "active"
            asyncio.create_task(self._send_entry(agent, zone_sequence[1], bet_amount, 2))
            log.info(f"🔔 NUEVO PATRÓN TRAS CERO -> INTENTO 2: {agent.name} -> ZONA {zone_sequence[1]}")
            return

        self.signal_sequence = [new_entry]
        self.current_attempt_index = 0
        self.signal_status = "active"
        self.attempt_numbers = []
        self.attempt_zones = []
        self.attempt_bets = []
        self.entry_msg_ids = []
        asyncio.create_task(self._send_entry(agent, zone_sequence[0], bet_amount, 1))
        log.info(f"🔔 SEÑAL INTENTO 1: {agent.name} -> ZONA {zone_sequence[0]}")
        agent.candidate_signal = None

    def _handle_signal_sequence(self, all_agents, last_number, bet_amount):
        candidates = []
        confirmation_resolved = False
        new_confirming_agent = None

        for agente in all_agents:
            if self.confirming and agente is self.pending_agent and not agente.confirming:
                confirmation_resolved = True
            if agente.candidate_signal is None:
                continue
            if not agente.live_enabled:
                continue
            if agente.candidate_signal.get("confirming", False):
                if (self.signal_status is None and not self.confirming
                        and new_confirming_agent is None):
                    new_confirming_agent = agente
            else:
                # Candidato real (confirmado)
                bet_zone_tuple = agente.candidate_signal.get("bet_zone")
                bet_zone = bet_zone_tuple[0] if bet_zone_tuple is not None else None
                pattern = agente.candidate_signal.get("pattern")
                amx_str = agente.candidate_signal.get("amx_strength", 0.0)

                if isinstance(agente, DozenPatternAgent):
                    if bet_zone is None:
                        # D1+D3: solo agentes de 4 valores y modelo entrenado
                        if agente.mode not in ("aaba", "abaa"):
                            continue
                        win_rate = agente._win_rate(pattern)
                        if win_rate is None:
                            continue
                        score = win_rate * (1 + amx_str)
                        candidates.append((agente, score, agente.candidate_signal))
                    else:
                        win_rate = agente._win_rate(pattern) or 0.0
                        score = win_rate * (1 + amx_str)
                        candidates.append((agente, score, agente.candidate_signal))
                elif isinstance(agente, ZonePatternAgent):
                    if bet_zone is None:
                        continue
                    win_rate = agente._win_rate(pattern) or 0.0
                    score = win_rate * (1 + amx_str)
                    candidates.append((agente, score, agente.candidate_signal))
                else:
                    continue

        if self.confirming and confirmation_resolved:
            self.confirming = False
            self.pending_agent = None
            self.pending_candidate = None

        # Secuencia activa
        if self.signal_status == "active":
            if not self.signal_sequence:
                self.signal_status = None
                return False

            current_entry = self.signal_sequence[0]
            agent = current_entry["agent"]
            attempt_idx = self.current_attempt_index
            zone_sequence = current_entry["zone_sequence"]
            if attempt_idx < len(zone_sequence):
                bet_zone = zone_sequence[attempt_idx]
            else:
                bet_zone = zone_sequence[0]

            if last_number == 0:
                self.attempt_numbers.append(0)
                self.attempt_zones.append(bet_zone)
                cycle_completed = self.labouchere.update(False)
                if cycle_completed:
                    self.cycle_pending = self.labouchere.cycles_completed
                if self.current_attempt_index == 0:
                    log.info("🟢 CERO en intento 1 - esperando nuevo patrón para intento 2")
                    self.signal_status = "waiting_pattern"
                    self.signal_sequence = []
                    asyncio.create_task(send_msg(
                        "🟢 CERO en INTENTO 1 · Se ajusta la gestión Labouchère · Esperando nueva confirmación para INTENTO 2",
                        THREAD_SIGNALS))
                    return True
                else:
                    log.info(f"🚫 CERO en intento {self.current_attempt_index+1} - señal perdida")
                    self.signal_status = "lost"
                    if candidates:
                        best_agent, best_candidate = self._select_best_candidate(candidates)
                        if best_agent is not None:
                            self._prepare_new_signal(best_agent, best_candidate, last_number)
                    self._finalize_sequence(False, None)
                    return True

            is_win = zone_win(bet_zone, last_number)
            self.attempt_numbers.append(last_number if last_number is not None else 0)
            self.attempt_zones.append(bet_zone)

            cycle_completed = self.labouchere.update(is_win)
            if cycle_completed:
                self.cycle_pending = self.labouchere.cycles_completed

            if is_win:
                self.signal_status = "won"
                winning_attempt = self.current_attempt_index + 1
                log.info(f"✅ SECUENCIA GANADA en intento {winning_attempt} (zona {bet_zone})")
                if candidates:
                    best_agent, best_candidate = self._select_best_candidate(candidates)
                    if best_agent is not None:
                        self._prepare_new_signal(best_agent, best_candidate, last_number)
                self._finalize_sequence(True, winning_attempt)
                return True
            else:
                if self.current_attempt_index < ZONE_MAX_ATTEMPTS - 1:
                    self.current_attempt_index += 1
                    new_bet = self.labouchere.get_bet()
                    next_zone = zone_sequence[self.current_attempt_index] if self.current_attempt_index < len(zone_sequence) else zone_sequence[-1]
                    asyncio.create_task(self._send_entry(agent, next_zone, new_bet, self.current_attempt_index + 1))
                    log.info(f"🔄 INTENTO {self.current_attempt_index+1}: zona {next_zone}")
                    return True
                else:
                    self.signal_status = "lost"
                    log.info("❌ SECUENCIA PERDIDA (2 intentos fallidos)")
                    if candidates:
                        best_agent, best_candidate = self._select_best_candidate(candidates)
                        if best_agent is not None:
                            self._prepare_new_signal(best_agent, best_candidate, last_number)
                    self._finalize_sequence(False, None)
                    return True

        if self.confirming:
            return True

        if self.signal_status is None and not self._signal_included and candidates:
            best_agent, best_candidate = self._select_best_candidate(candidates)
            if best_agent is not None:
                self._activate_new_signal(best_agent, best_candidate, bet_amount)
                return True

        if new_confirming_agent is not None:
            self.pending_agent = new_confirming_agent
            self.pending_candidate = new_confirming_agent.candidate_signal
            self.confirming = True
            asyncio.create_task(self._send_confirmation())
            log.info(f"🔍 Confirmación de patrón pendiente para {new_confirming_agent.name}")
            new_confirming_agent.candidate_signal = None
            return True

        return False

    def update(self, number: int, real_color: str, timestamp: float = None, training: bool = False):
        if timestamp is None: timestamp = time.time()
        self.spin_history.append({"number": number, "color": real_color, "timestamp": timestamp})
        if len(self.spin_history) > 200: self.spin_history.pop(0)
        self.prev_number = number
        self.total_spins_seen += 1
        if not training:
            self.live_spins_seen += 1

        z = zone_of(number)
        self.zone_history.append(z)
        if len(self.zone_history) > 300: self.zone_history = self.zone_history[-300:]

        if number != 0:
            self.last_nonzero_zone = z

        dz = dozen_of(number)
        self.dozen_history.append(dz)
        if len(self.dozen_history) > 300: self.dozen_history = self.dozen_history[-300:]

        real_dozen_num = DOZEN_NUM[dz]
        change = self._level_change(number, real_dozen_num)
        self.level_current += change
        self.level_history.append(self.level_current)
        if len(self.level_history) > 100: self.level_history.pop(0)
        if real_dozen_num != 0:
            self.last_dozen_num = real_dozen_num
            if real_dozen_num == 2: self.last_d2_number = number

        TREND_LOOKBACK = 20
        if len(self.level_history) >= TREND_LOOKBACK:
            diff = self.level_history[-1] - self.level_history[-TREND_LOOKBACK]
            if diff > 0.8:
                self.trend = "bullish"
            elif diff < -0.8:
                self.trend = "bearish"
            else:
                self.trend = "neutral"
        else:
            self.trend = "neutral"

        # Actualizar agentes de docenas
        agent_list = [self.agent2, self.agent3, self.agent4, self.agent6]
        agent_keys = ["agent2", "agent3", "agent4", "agent6"]

        for agente, key in zip(agent_list, agent_keys):
            config = AGENT_TREND_CONFIG.get(key, {})
            if config.get("method") == "ema":
                trend = ema_trend(self.level_history,
                                  strictness=config.get("strictness", "relaxed"),
                                  min_diff=config.get("min_diff", 0.0))
                amx_strength_val = 0.0
            else:
                periods = config.get("amx_periods", [5, 10, 20])
                trend = amx_trend(self.level_history, periods,
                                  strictness=config.get("strictness", "relaxed"),
                                  threshold=config.get("threshold", 0.5))
                amx_strength_val = amx_strength(self.level_history, periods)
            favored = trend_favored_dozens(trend)

            blocked = (self.signal_status is not None) or self.confirming
            live_ok = (not training) and (self.live_spins_seen >= DOZEN_MIN_SPIN_TO_SIGNAL)

            agente.update(self.dozen_history, timestamp, blocked=blocked,
                          trend_dozens=favored, amx_strength_val=amx_strength_val,
                          last_number=number, live_enabled=live_ok)

        # Actualizar agentes de zona
        zone_agents = [self.zone_agent1, self.zone_agent2]
        for zagente in zone_agents:
            blocked = (self.signal_status is not None) or self.confirming
            live_ok = (not training) and (self.live_spins_seen >= DOZEN_MIN_SPIN_TO_SIGNAL)
            amx_strength_val = 0.0
            zagente.update(self.zone_history, timestamp, blocked=blocked,
                           amx_strength_val=amx_strength_val,
                           last_number=number, live_enabled=live_ok)

        all_agents = agent_list + zone_agents
        self._signal_included = False

        if not training:
            self._handle_signal_sequence(all_agents, number, self.labouchere.get_bet())

        if training:
            return
        lab_state = self.labouchere.get_state()
        lab_seq = ','.join(str(x) for x in lab_state['sequence'])
        lab_bet = lab_state['bet_amount']
        last10 = ",".join(self.dozen_history[-10:])
        seq_status = f"Sec: {self.signal_status}" if self.signal_status else "Sin secuencia"
        log.info(
            f"🎰 Mesa {self.key} | Giro #{len(self.dozen_history)}: {number} ({real_color}) → {dz} "
            f"(docena {real_dozen_num}) | Zona: {z} | Nivel: {self.level_current} | "
            f"{seq_status} | Lab: [{lab_seq}] {format_cop(lab_bet)} | Últimas 10 docenas: [{last10}] | "
            f"Live spins: {self.live_spins_seen}/{DOZEN_MIN_SPIN_TO_SIGNAL} | Tendencia (20g): {self.trend} | Última zona no nula: {self.last_nonzero_zone}"
        )

    def get_state(self, limit: int = 40):
        hist = self.spin_history[-limit:] if self.spin_history else []
        return {
            "key": self.key,
            "table_name": TABLE_NAME,
            "spin_history": hist,
            "dozen_history": self.dozen_history[-limit:],
            "zone_history": self.zone_history[-limit:],
            "agent2": self.agent2.get_state(),
            "agent3": self.agent3.get_state(),
            "agent4": self.agent4.get_state(),
            "agent6": self.agent6.get_state(),
            "zone_agent1": self.zone_agent1.get_state(),
            "zone_agent2": self.zone_agent2.get_state(),
            "trend": self.trend,
            "trend_favored_dozens": sorted(NUM_DOZEN[d] for d in trend_favored_dozens(self.trend)),
            "level_current": self.level_current,
            "labouchere": self.labouchere.get_state(),
            "live_spins_seen": self.live_spins_seen,
            "total_spins_seen": self.total_spins_seen,
            "signal_status": self.signal_status,
            "current_attempt": self.current_attempt_index + 1 if self.signal_status == "active" else 0,
            "total_attempts": ZONE_MAX_ATTEMPTS if self.signal_status == "active" else 0,
            "last_nonzero_zone": self.last_nonzero_zone,
        }


# ══════════════════════════════════════════════
#  ENTRENAMIENTO CON HISTORIAL
# ══════════════════════════════════════════════
BATCH_SIZE = 250

def load_history_seed(path: str = HISTORY_SEED_PATH, table_name: str = HISTORY_SEED_TABLE) -> list:
    if not path or not os.path.exists(path):
        log.warning(f"[Historial] No se encontró '{path}'; se arranca sin pre-entrenamiento.")
        return []
    try:
        conn = sqlite3.connect(":memory:")
        with open(path, "r", encoding="utf-8") as f:
            conn.executescript(f.read())
        cur = conn.execute(f'SELECT spin_number FROM "{table_name}" ORDER BY id ASC')
        spins = [int(row[0]) for row in cur.fetchall()]
        conn.close()
        log.info(f"[Historial] {len(spins)} giros cargados desde '{path}' (tabla '{table_name}').")
        return spins
    except Exception as e:
        log.warning(f"[Historial] Error leyendo '{path}': {e}")
        return []

async def train_table_from_history(table: "RouletteTable", spins: list, timestamp: float) -> None:
    if not spins:
        return
    log.info(f"[Entrenamiento] Mesa {table.key}: procesando {len(spins)} giros históricos en bloques de {BATCH_SIZE}...")
    total = len(spins)
    for start in range(0, total, BATCH_SIZE):
        batch = spins[start:start + BATCH_SIZE]
        log.info(f"[Entrenamiento] Mesa {table.key}: bloque {start//BATCH_SIZE + 1} ({len(batch)} giros)")
        for i, number in enumerate(batch):
            if not (0 <= number <= 36):
                continue
            table.update(number, color_of(number), timestamp=timestamp, training=True)
            if i % 100 == 0:
                await asyncio.sleep(0)
        agents = [table.agent2, table.agent3, table.agent4, table.agent6,
                  table.zone_agent1, table.zone_agent2]
        for agent in agents:
            agent.force_train(timestamp)
        log.info(f"[Entrenamiento] Mesa {table.key}: entrenamiento forzado tras bloque {start//BATCH_SIZE + 1}")
        await asyncio.sleep(0.1)

    for agent in agents:
        agent.force_train(timestamp)
        agent.reset_transient()
    log.info(
        f"[Entrenamiento] Mesa {table.key}: listo. giros_vistos={table.total_spins_seen} "
        f"nivel={table.level_current}"
    )


# ══════════════════════════════════════════════
#  SERVER STATE
# ══════════════════════════════════════════════
class ServerState:
    def __init__(self):
        self.tables = {k: RouletteTable(k) for k in ROULETTE_KEYS.values()}
        self.history_seed_trained = {k: False for k in ROULETTE_KEYS.values()}

    async def update_mesa(self, key: int, number: int, broadcast: bool = True, training: bool = False):
        if key not in self.tables:
            return
        table = self.tables[key]
        real_color = color_of(number)
        table.update(number, real_color, training=training)

    def get_state_for_mesa(self, key: int):
        if key not in self.tables:
            return None
        return self.tables[key].get_state(limit=40)

    def load_all_models(self):
        for key in self.tables:
            self._load_model(key)

    def _load_model(self, key: int):
        filename = f"model_{key}.json"
        if not os.path.exists(filename):
            return
        try:
            with open(filename, "r") as f:
                data = json.load(f)
                table = self.tables[key]
                table.agent2.load_persist(data.get("agent2"))
                table.agent3.load_persist(data.get("agent3"))
                table.agent4.load_persist(data.get("agent4"))
                table.agent6.load_persist(data.get("agent6"))
                table.zone_agent1.load_persist(data.get("zone_agent1"))
                table.zone_agent2.load_persist(data.get("zone_agent2"))
                table.total_spins_seen = data.get("table_total_spins_seen", table.total_spins_seen)
                self.history_seed_trained[key] = data.get("history_seed_trained", False)
                log.info(f"Modelo cargado para mesa {key}")
        except Exception as e:
            log.warning(f"Error cargando modelo mesa {key}: {e}")

    def save_all_models(self):
        for key, table in self.tables.items():
            self._save_model(key)

    def _save_model(self, key: int):
        table = self.tables[key]
        data = {
            "agent2": table.agent2.to_persist(),
            "agent3": table.agent3.to_persist(),
            "agent4": table.agent4.to_persist(),
            "agent6": table.agent6.to_persist(),
            "zone_agent1": table.zone_agent1.to_persist(),
            "zone_agent2": table.zone_agent2.to_persist(),
            "table_total_spins_seen": table.total_spins_seen,
            "history_seed_trained": self.history_seed_trained.get(key, False),
        }
        filename = f"model_{key}.json"
        try:
            with open(filename, "w") as f:
                json.dump(data, f)
        except Exception as e:
            log.warning(f"Error guardando modelo mesa {key}: {e}")

    async def train_from_history(self):
        spins_cache = None
        for key, table in self.tables.items():
            if self.history_seed_trained.get(key):
                log.info(f"[Entrenamiento] Mesa {key}: ya estaba entrenada con el historial, se omite.")
                continue
            if spins_cache is None:
                spins_cache = load_history_seed()
            if not spins_cache:
                continue
            now = time.time()
            await train_table_from_history(table, spins_cache, now)
            self.history_seed_trained[key] = True
            self._save_model(key)


# ══════════════════════════════════════════════
#  WEBSOCKET HANDLER
# ══════════════════════════════════════════════
class PragmaticWebSocketHandler:
    def __init__(self, key: int, on_spin_callback: Callable[[int, bool, bool], Awaitable[None]]):
        self.key = key
        self.on_spin_callback = on_spin_callback
        self.seen = set()

    async def run(self):
        sub = {"type": "subscribe", "casinoId": CASINO_ID, "currency": CURRENCY_ID, "key": [self.key]}
        delay = 5
        while True:
            try:
                async with websockets.connect(WS_URL, ping_interval=30, ping_timeout=60, close_timeout=10) as ws:
                    await ws.send(json.dumps(sub))
                    log.info(f"✅ WS Pragmatic conectado (key={self.key})")
                    delay = 5
                    async for raw in ws:
                        try:
                            data = json.loads(raw)
                        except Exception:
                            continue
                        if not isinstance(data, dict):
                            continue

                        results = data.get("last20Results")
                        if isinstance(results, list):
                            log.debug(f"📦 Recibido last20Results con {len(results)} elementos")
                            for r in results:
                                await self._feed(r.get("gameId"), r.get("result"), emit=True)

                        if data.get("gameId") is not None and data.get("result") is not None:
                            await self._feed(data.get("gameId"), data.get("result"), emit=True)

            except Exception as e:
                log.warning(f"🔌 WS key={self.key}: {e}. Reconectando en {delay}s…")
            await asyncio.sleep(delay)
            delay = min(delay * 2, 60)

    async def _feed(self, gid, result, emit: bool):
        if gid is None or result is None:
            return
        try:
            num = int(result)
        except (TypeError, ValueError):
            return
        if not (0 <= num <= 36):
            return
        if gid in self.seen:
            log.debug(f"⏩ gameId {gid} ya procesado (número {num})")
            return
        self.seen.add(gid)
        if len(self.seen) > 3000:
            self.seen.clear()
        log.info(f"🔄 Nuevo giro: gameId={gid}, número={num}")
        if self.on_spin_callback:
            await self.on_spin_callback(num, emit, training=not emit)


# ══════════════════════════════════════════════
#  HTTP
# ══════════════════════════════════════════════
_server_state: Optional[ServerState] = None

async def http_ping(request: web.Request):
    return web.json_response({"status": "pong", "ts": time.time()})

async def http_health(request: web.Request):
    if _server_state is None:
        return web.json_response({"status": "not_ready"}, status=503)
    return web.json_response({
        "status": "ok",
        "mesas": list(_server_state.tables.keys()),
        "total_spins": sum(len(t.spin_history) for t in _server_state.tables.values())
    })

async def http_api_state(request: web.Request):
    if _server_state is None:
        return web.json_response({"error": "server not ready"}, status=503)
    try:
        mesa = int(request.match_info["mesa"])
    except (KeyError, ValueError):
        return web.json_response({"error": "mesa inválida"}, status=400)
    if mesa not in ROULETTE_KEYS.values():
        return web.json_response({"error": "mesa no soportada"}, status=404)
    state = _server_state.get_state_for_mesa(mesa)
    if state is None:
        return web.json_response({"error": "mesa no encontrada"}, status=404)
    return web.json_response(state)

async def http_api_all(request: web.Request):
    if _server_state is None:
        return web.json_response({"error": "server not ready"}, status=503)
    result = {str(key): _server_state.get_state_for_mesa(key) for key in ROULETTE_KEYS.values()}
    return web.json_response(result)


# ══════════════════════════════════════════════
#  SELF-PING
# ══════════════════════════════════════════════
async def self_ping_loop():
    render_url = os.environ.get("RENDER_EXTERNAL_URL", "").rstrip("/")
    if not render_url or "localhost" in render_url:
        log.info("Self-ping desactivado (no URL)")
        return
    await asyncio.sleep(30)
    log.info(f"Self-ping activo → {render_url}/ping cada {PING_INTERVAL}s")
    timeout = ClientTimeout(total=15)
    async with ClientSession(timeout=timeout) as session:
        while True:
            try:
                async with session.get(f"{render_url}/ping") as resp:
                    await resp.read()
            except Exception:
                pass
            await asyncio.sleep(PING_INTERVAL)


# ══════════════════════════════════════════════
#  TELEGRAM POLLING
# ══════════════════════════════════════════════
async def bot_polling_loop():
    if bot is None:
        return
    if os.environ.get("DISABLE_TELEGRAM", "").lower() in ("1", "true", "yes"):
        log.info("Telegram deshabilitado por variable DISABLE_TELEGRAM")
        return

    delay = 5
    while True:
        try:
            await bot.delete_webhook(drop_pending_updates=True)
        except Exception as e:
            log.warning(f"[Telegram] No se pudo eliminar webhook: {e}")

        started = time.time()
        try:
            await bot.infinity_polling(skip_pending=True, timeout=20, request_timeout=30)
        except Exception as e:
            if "409" in str(e):
                log.warning("[Telegram] Conflicto 409 detectado (otra instancia activa). Esperando 60s...")
                await asyncio.sleep(60)
                continue
            log.warning(f"[Telegram] Polling interrumpido: {e}")

        ran_for = time.time() - started
        if ran_for < 60:
            delay = min(delay * 2, 120)
        else:
            delay = 5
        log.warning(f"[Telegram] Reintentando polling en {delay}s…")
        await asyncio.sleep(delay)


# ══════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════
def build_http_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/ping", http_ping)
    app.router.add_get("/health", http_health)
    app.router.add_get("/api/state/{mesa}", http_api_state)
    app.router.add_get("/api/all", http_api_all)
    return app

async def main():
    global _server_state
    log.info("═" * 60)
    log.info("BOT UNIFICADO — DOCENAS → ZONAS | SPEED ROULETTE 2 (solo backend)")
    log.info(f"Mesas: {', '.join(str(k) for k in ROULETTE_KEYS.values())}")
    log.info("═" * 60)

    server_state = ServerState()
    server_state.load_all_models()
    _server_state = server_state

    await server_state.train_from_history()

    async def save_loop():
        while True:
            await asyncio.sleep(SAVE_INTERVAL)
            server_state.save_all_models()

    async def on_spin(key: int, num: int, emit: bool, training: bool = False):
        await server_state.update_mesa(key, num, broadcast=emit, training=training)

    tasks = []
    for key in ROULETTE_KEYS.values():
        handler = PragmaticWebSocketHandler(key, lambda num, emit, training=False, k=key: on_spin(k, num, emit, training))
        tasks.append(asyncio.create_task(handler.run()))

    tasks.append(asyncio.create_task(save_loop()))
    tasks.append(asyncio.create_task(self_ping_loop()))
    if bot is not None:
        tasks.append(asyncio.create_task(bot_polling_loop()))

    port = int(os.environ.get("PORT", 10000))
    app = build_http_app()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    log.info(f"Servidor HTTP escuchando en puerto {port} (API: /ping, /health, /api/state/205, /api/all)")

    try:
        await asyncio.Event().wait()
    finally:
        for t in tasks:
            t.cancel()
        await runner.cleanup()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Servidor detenido")
