"""
╔══════════════════════════════════════════════════════════════╗
║   SERVIDOR ADAPTATIVO — AZURE ROULETTE (key 227)              ║
║   - 8 agentes de COLOR, 8 de ZONA, 8 de PARIDAD              ║
║   - ML adaptativo con tendencia AMX y EMA (50,70,200)       ║
║   - Gestión Labouchere GLOBAL (base 500 COP)                ║
║   - Persistencia y aprendizaje continuo                    ║
║   - Parámetros ajustados para efectividad ~85%             ║
║   - Filtros más estrictos: win_rate 0.55, muestras 10      ║
║   - Agente V5 (ababa): 3 señales consecutivas → desactivación temporal ║
║   - Nuevos patrones V7 (aaabaaa) y V8 (aaabaa)             ║
║   - Todas las señales a 3 intentos                          ║
║   - Comando /status acortado                                ║
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
from aiohttp import web, WSMsgType, ClientSession, ClientTimeout
try:
    from telebot.async_telebot import AsyncTeleBot
    TELEBOT_OK = True
except ImportError:
    AsyncTeleBot = None
    TELEBOT_OK = False

# ──────────────────────────────────────────────
# CONFIGURACIÓN (AJUSTADA PARA MAYOR EFECTIVIDAD)
# ──────────────────────────────────────────────
WS_URL        = "wss://dga.pragmaticplaylive.net/ws"
CASINO_ID     = "ppcdk00000005349"
CURRENCY_ID   = "BRL"
PING_INTERVAL = 240
SAVE_INTERVAL = 30
ROULETTE_KEYS = {227: 227}

LABOUCHERE_BASE_AMOUNT = 500
LABOUCHERE_INITIAL_SEQUENCE = [1, 1, 1, 1, 1]
LABOUCHERE_INFINITE_MODE   = True
LABOUCHERE_INITIAL_CAPITAL = 0

COLOR_MAX_ATTEMPTS = 3                     # AHORA 3 INTENTOS
COLOR_BACKTEST_WINDOW = 80
COLOR_CONTEXT_WINDOW = 20
COLOR_MIN_SAMPLES_GATE = 10
COLOR_MIN_WIN_RATE = 0.55
COLOR_MIN_SPIN_TO_SIGNAL = 21
COLOR_ANALYSIS_WINDOW = 3
CONTEXT_SIMILARITY_THRESHOLD = 0.85

LIVE_MIN_SPINS_TO_SIGNAL = 21
ML_MIN_SIGNALS_TO_TRAIN = 50
ML_RETRAIN_INTERVAL_SECONDS = 30 * 60
TABLE_MIN_SPINS_LIVE = 500
CATEGORY_MIN_PROCESSED_LIVE = 120
LIVE_FASTTRACK_MIN_SAMPLES = 10
LIVE_FASTTRACK_MIN_WIN_RATE = 0.90

AMX_STRENGTH_THRESHOLDS = {"strong": 1.0, "weak": 0.5}
AMX_ADJUST_FACTOR_STRONG = 0.7
AMX_ADJUST_FACTOR_WEAK = 0.9

COLOR_COOLDOWN_AFTER_LOSSES = 3
COLOR_COOLDOWN_ROUNDS = 5

REAL_COLOR_MAP = {
    0:  "VERDE", 1:  "ROJO", 2:  "NEGRO", 3:  "ROJO", 4:  "NEGRO", 5:  "ROJO", 6:  "NEGRO",
    7:  "ROJO", 8:  "NEGRO", 9:  "ROJO", 10: "NEGRO", 11: "NEGRO", 12: "ROJO", 13: "NEGRO",
    14: "ROJO", 15: "NEGRO", 16: "ROJO", 17: "NEGRO", 18: "ROJO", 19: "ROJO", 20: "NEGRO",
    21: "ROJO", 22: "NEGRO", 23: "ROJO", 24: "NEGRO", 25: "ROJO", 26: "NEGRO", 27: "ROJO",
    28: "NEGRO", 29: "NEGRO", 30: "ROJO", 31: "NEGRO", 32: "ROJO", 33: "NEGRO", 34: "ROJO",
    35: "NEGRO", 36: "ROJO"
}

COLOR_VALUES = ("ROJO", "NEGRO")
COLOR_EMOJI = {"ROJO": "🔴", "NEGRO": "⚫", "VERDE": "🟢"}
COLOR_NUM = {"ROJO": 1, "NEGRO": 2, "VERDE": 0}
NUM_COLOR = {1: "ROJO", 2: "NEGRO"}

ZONE_VALUES = ("BAJA", "ALTA")
ZONE_EMOJI = {"BAJA": "🔵", "ALTA": "🟠"}
ZONE_NUM = {"BAJA": 1, "ALTA": 2, "VERDE": 0}
NUM_ZONE = {1: "BAJA", 2: "ALTA"}
TREND_FAVORED_ZONES = {"bullish": {1}, "bearish": {2}, "neutral": {1, 2}}

PARIDAD_VALUES = ("PAR", "IMPAR")
PARIDAD_EMOJI = {"PAR": "🟣", "IMPAR": "🟤"}
PARIDAD_NUM = {"PAR": 1, "IMPAR": 2, "VERDE": 0}
NUM_PARIDAD = {1: "PAR", 2: "IMPAR"}
TREND_FAVORED_PARIDAD = {"bullish": {1}, "bearish": {2}, "neutral": {1, 2}}

EMA_TREND_MIN_HISTORY = 20
TREND_FAVORED_COLORS = {"bullish": {1}, "bearish": {2}, "neutral": {1, 2}}

AGENT_TREND_CONFIG = {
    "agent1": {"method": "amx", "strictness": "relaxed", "min_diff": None, "amx_periods": [5, 10, 20]},
    "agent2": {"method": "ema", "strictness": "strict", "min_diff": 0.5, "ema_periods": [4, 8, 20]},
    "agent3": {"method": "ema_long", "strictness": "strict", "min_diff": 0.5, "ema_periods": [50, 70, 200]},
    "agent4": {"method": "ema_long", "strictness": "strict", "min_diff": 0.5, "ema_periods": [50, 70, 200]},
    "agent5": {"method": "amx", "strictness": "relaxed", "min_diff": None, "amx_periods": [5, 10, 20]},
    "agent6": {"method": "amx", "strictness": "relaxed", "min_diff": None, "amx_periods": [5, 10, 20]},
    "agent7": {"method": "ema_long", "strictness": "strict", "min_diff": 0.5, "ema_periods": [50, 70, 200]},
    "agent8": {"method": "ema_long", "strictness": "strict", "min_diff": 0.5, "ema_periods": [50, 70, 200]},
}

# ── Telegram ──
BOT_TOKEN      = os.environ.get("BOT_TOKEN", "8347707121:AAH1cPEDMLbm-scTJ8mUuufeEhzw3Axv2Lw")
CHAT_ID_BASE   = int(os.environ.get("CHAT_ID_BASE", "-1003965615775"))
THREAD_SIGNALS = int(os.environ.get("THREAD_SIGNALS", "5589"))
THREAD_STATS   = int(os.environ.get("THREAD_STATS", "5593"))
THREAD_SIGNALS_ZONE = int(os.environ.get("THREAD_SIGNALS_ZONE", str(THREAD_SIGNALS)))
THREAD_STATS_ZONE   = int(os.environ.get("THREAD_STATS_ZONE", str(THREAD_STATS)))
THREAD_SIGNALS_PARIDAD = int(os.environ.get("THREAD_SIGNALS_PARIDAD", str(THREAD_SIGNALS)))
THREAD_STATS_PARIDAD   = int(os.environ.get("THREAD_STATS_PARIDAD", str(THREAD_STATS)))
TABLE_LINK     = os.environ.get("TABLE_LINK", "https://1win.lat/casino/play/v_pragmatic:roulette1")

HISTORY_SEED_PATH  = os.environ.get("HISTORY_SEED_PATH", "russian-azure.db")
HISTORY_SEED_TABLE = os.environ.get("HISTORY_SEED_TABLE", "roulette_1")

# ──────────────────────────────────────────────
# LOGGING
# ──────────────────────────────────────────────
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s | %(levelname)s | %(message)s",
                    datefmt="%H:%M:%S", handlers=[logging.StreamHandler(sys.stdout)])
log = logging.getLogger(__name__)

# ══════════════════════════════════════════════
# FUNCIONES AUXILIARES
# ══════════════════════════════════════════════
def color_of(n):
    return REAL_COLOR_MAP.get(n, "VERDE")

def zone_of(n):
    if n is None or n == 0:
        return "VERDE"
    return "BAJA" if 1 <= n <= 18 else "ALTA"

def paridad_of(n):
    if n is None or n == 0:
        return "VERDE"
    return "PAR" if n % 2 == 0 else "IMPAR"

def format_cop(amount: int) -> str:
    return f"${amount:,} COP"

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

def ema_trend(level_history, periods, strictness="relaxed", min_diff=0.0):
    if len(level_history) < max(periods):
        return None if strictness in ("strict", "very_strict") else "neutral"
    emas = [calc_ema(level_history, p) for p in periods]
    if any(not ema for ema in emas):
        return None if strictness != "relaxed" else "neutral"
    ema_vals = [ema[-1] for ema in emas]
    cur = level_history[-1]
    if any(v is None for v in ema_vals):
        return None if strictness != "relaxed" else "neutral"
    bullish = cur > ema_vals[0] and all(ema_vals[i] > ema_vals[i-1] for i in range(1, len(ema_vals)))
    bearish = cur < ema_vals[0] and all(ema_vals[i] < ema_vals[i-1] for i in range(1, len(ema_vals)))
    if strictness == "relaxed":
        if bullish: return "bullish"
        if bearish: return "bearish"
        return "neutral"
    elif strictness == "strict":
        if bullish:
            if abs(cur - ema_vals[0]) > min_diff and all(abs(ema_vals[i] - ema_vals[i-1]) > min_diff for i in range(1, len(ema_vals))):
                return "bullish"
            return "neutral"
        if bearish:
            if abs(cur - ema_vals[0]) > min_diff and all(abs(ema_vals[i] - ema_vals[i-1]) > min_diff for i in range(1, len(ema_vals))):
                return "bearish"
            return "neutral"
        return "neutral"
    elif strictness == "very_strict":
        if bullish:
            if abs(cur - ema_vals[0]) > min_diff and all(abs(ema_vals[i] - ema_vals[i-1]) > min_diff for i in range(1, len(ema_vals))):
                return "bullish"
            return None
        if bearish:
            if abs(cur - ema_vals[0]) > min_diff and all(abs(ema_vals[i] - ema_vals[i-1]) > min_diff for i in range(1, len(ema_vals))):
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

def trend_favored_colors(trend):
    if trend is None:
        return set()
    return TREND_FAVORED_COLORS.get(trend, TREND_FAVORED_COLORS["neutral"])

def trend_favored_zones(trend):
    if trend is None:
        return set()
    return TREND_FAVORED_ZONES.get(trend, TREND_FAVORED_ZONES["neutral"])

def trend_favored_paridad(trend):
    if trend is None:
        return set()
    return TREND_FAVORED_PARIDAD.get(trend, TREND_FAVORED_PARIDAD["neutral"])

def amx_strength(level_history, periods):
    if len(level_history) < max(periods) + 1:
        return 0.0
    momentum_values = [calc_momentum(level_history, p) for p in periods]
    amx = sum(momentum_values) / len(periods)
    return abs(amx)

# ══════════════════════════════════════════════
# LABOUCHERE MANAGER
# ══════════════════════════════════════════════
class LabouchereManager:
    def __init__(self, base_amount: int = LABOUCHERE_BASE_AMOUNT,
                 initial_sequence: List[int] = None,
                 initial_capital: int = LABOUCHERE_INITIAL_CAPITAL):
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

    def reset(self):
        self.sequence = list(self.initial_sequence)
        self.current_bet = self._calculate_bet()

    def _restart_cycle(self):
        self.base_amount = LABOUCHERE_BASE_AMOUNT
        self.sequence = list(self.initial_sequence)
        self.current_bet = self._calculate_bet()
        log.info(f"♾️ GESTIÓN REINICIADA · Acumulado: {'+' if self.balance >= 0 else '-'}"
                 f"{format_cop(abs(self.balance))} · "
                 f"Base: {format_cop(self.base_amount)} · Secuencia: [{self.seq_str()}] · "
                 f"Apuesta: {format_cop(self.current_bet)}")

    def update(self, win: bool):
        if not self.sequence:
            self._restart_cycle()
            return
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
        else:
            self.current_bet = self._calculate_bet()

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
# TELEGRAM
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

# ── Formatos de mensaje ──
def build_entry_message(last_number, bet_colors, bet_amount=None, start_attempt=1, sequence_str: str = "") -> str:
    numero = last_number if last_number is not None else "-"
    numero_emoji = COLOR_EMOJI.get(color_of(last_number), "🟢") if last_number is not None else ""
    color = bet_colors[0] if bet_colors else "-"
    emoji = COLOR_EMOJI.get(color, "")
    if bet_amount is not None:
        apuesta_line = f"\n🇨🇴 APUESTA: {format_cop(bet_amount)}"
    else:
        apuesta_line = ""
    seq_line = f"\n📋 Secuencia: [{sequence_str}]" if sequence_str else ""
    link_line = f'🎮 <a href="{TABLE_LINK}">Azure Roulette 1</a>' if TABLE_LINK else "🎮 Azure Roulette 1"
    return (f"🚨🚨 ENTRADA PARA COLOR 🚨🚨\n\n"
            f"👉 INGRESAR DESPUÉS: {numero} ({numero_emoji})\n"
            f"🧨 COLOR: {color} ({emoji})\n"
            f"{apuesta_line}\n\n"
            f"💫 ¡Juegue con Responsabilidad!\n{link_line}")

def build_entry_message_zone(last_number, bet_zones, bet_amount=None, start_attempt=1, sequence_str: str = "") -> str:
    numero = last_number if last_number is not None else "-"
    numero_emoji = ZONE_EMOJI.get(zone_of(last_number), "🟢") if last_number is not None else ""
    zone = bet_zones[0] if bet_zones else "-"
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
    link_line = f'🎮 <a href="{TABLE_LINK}">Azure Roulette 1</a>' if TABLE_LINK else "🎮 Azure Roulette 1"
    return (f"🚨🚨 ENTRADA PARA ZONA 🚨🚨\n\n"
            f"👉 INGRESAR DESPUÉS: {numero} ({numero_emoji})\n"
            f"{zone_line}\n"
            f"{apuesta_line}\n\n"
            f"💫 ¡Juegue con Responsabilidad!\n{link_line}")

def build_entry_message_paridad(last_number, bet_paridad, bet_amount=None, start_attempt=1, sequence_str: str = "") -> str:
    numero = last_number if last_number is not None else "-"
    numero_emoji = PARIDAD_EMOJI.get(paridad_of(last_number), "🟢") if last_number is not None else ""
    paridad = bet_paridad[0] if bet_paridad else "-"
    if paridad == "PAR":
        paridad_line = f"🧨 NUMEROS PARES ({PARIDAD_EMOJI['PAR']})"
    elif paridad == "IMPAR":
        paridad_line = f"🧨 NUMEROS IMPARES ({PARIDAD_EMOJI['IMPAR']})"
    else:
        paridad_line = f"🧨 NUMEROS: -"
    if bet_amount is not None:
        apuesta_line = f"\n🇨🇴 APUESTA: {format_cop(bet_amount)}"
    else:
        apuesta_line = ""
    link_line = f'🎮 <a href="{TABLE_LINK}">Azure Roulette 1</a>' if TABLE_LINK else "🎮 Azure Roulette 1"
    return (f"🚨🚨 ENTRADA PARIDAD 🚨🚨\n\n"
            f"👉 INGRESAR DESPUÉS: {numero} ({numero_emoji})\n"
            f"{paridad_line}\n"
            f"{apuesta_line}\n\n"
            f"💫 ¡Juegue con Responsabilidad!\n{link_line}")

def attempt_win_label(attempt: int) -> str:
    return f"✅ WIN INTENTO {attempt}"

ATTEMPT_LOSS_LABEL = "🚫 LOSS"

def build_follow_up_message(new_bet: int, next_attempt: int, sequence_str: str = "") -> str:
    if sequence_str:
        return (f"🧠 SEGUIR CON LA GESTIÓN\n"
                f"🇨🇴 APUESTA: {format_cop(new_bet)}\n")
    return (f"🧠 SEGUIR CON LA GESTIÓN\n"
            f"🇨🇴 APUESTA: {format_cop(new_bet)}\n")

def build_resolution_message(win: bool, attempt_results: list, bet_amount=None) -> str:
    body = " | ".join(str(v) for v in attempt_results)
    header = "✅✅✅ 👍🏻" if win else "🚫🚫🚫👎🏻"
    if bet_amount is not None:
        apuesta_line = f" | Apuesta: {format_cop(bet_amount)}"
    else:
        apuesta_line = ""
    return f"{header} ({body}){apuesta_line}"

def build_daily_marker_message(stats: dict) -> str:
    total = stats.get("total", 0)
    won = stats.get("won", 0)
    lost = stats.get("lost", 0)
    rate = round((won / total) * 100, 1) if total else 0.0
    return f"📆 MARCADOR DIARIO 💎\n✅ Ganadas: {won}\n❌ Perdidas: {lost}\n🎯 Total señales: {total}\n📈 Efectividad: {rate}%"

def build_status_message(server_state) -> str:
    agent_keys = ["agent1", "agent2", "agent3", "agent4", "agent5", "agent6", "agent7", "agent8"]
    zone_agent_keys = ["zone_agent1", "zone_agent2", "zone_agent3", "zone_agent4", "zone_agent5", "zone_agent6", "zone_agent7", "zone_agent8"]
    paridad_agent_keys = ["paridad_agent1", "paridad_agent2", "paridad_agent3", "paridad_agent4", "paridad_agent5", "paridad_agent6", "paridad_agent7", "paridad_agent8"]
    lines = ["📊 ESTADÍSTICAS POR PATRÓN"]
    for key, table in server_state.tables.items():
        lines.append(f"🎲 Mesa {key}")
        lab_state = table.labouchere.get_state()
        seq_str = ','.join(str(x) for x in lab_state['sequence'])
        sign = '+' if lab_state['balance'] >= 0 else '-'
        lines.append(f"💹 Labouchère | Acum: {sign}{format_cop(abs(lab_state['balance']))} | Sec: [{seq_str}] | Sig: {format_cop(lab_state['bet_amount'])} | Ciclos: {lab_state['cycles_completed']}")
        for akey in agent_keys + zone_agent_keys + paridad_agent_keys:
            agente = getattr(table, akey, None)
            if agente is None:
                continue
            s = agente.stats
            total = s.get("total", 0)
            won = s.get("won", 0)
            rate = round((won / total) * 100, 1) if total else 0.0
            estado = "🟢" if agente.state["active"] else ("🌘" if agente.train_state["active"] else "⚪")
            live = "📡" if agente.live_enabled else "🧪"
            trained = "🤖" if agente.trained else ("⚡" if agente.is_fasttrack_ready() else "⏳")
            signal = "🔇" if not agente.signal_enabled else "🔊"
            lines.append(f"{agente.label} {live}{trained}{signal} {estado} {total} ({rate}%)")
    return "\n".join(lines)

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
# DAILY MARKER
# ──────────────────────────────────────────────
class DailyMarker:
    def __init__(self, thread_signals=None):
        self.stats = {"total": 0, "won": 0, "lost": 0}
        self.thread_signals = thread_signals if thread_signals is not None else THREAD_SIGNALS

    async def record(self, win: bool):
        self.stats["total"] += 1
        self.stats["won" if win else "lost"] += 1

# ══════════════════════════════════════════════
# AGENTE DE PATRÓN CON ML
# ══════════════════════════════════════════════
class ColorPatternAgent:
    def __init__(self, pattern_len: int, name: str, label: str, mode: str, daily_marker=None,
                 values=None, num_map=None, zero_label="VERDE", entry_builder=None,
                 thread_signals=None, thread_stats=None, target_symbol: str = 'b'):
        self.pattern_len = pattern_len
        self.name = name
        self.label = label
        self.mode = mode
        self.daily_marker = daily_marker
        self.values = values if values is not None else COLOR_VALUES
        self.num_map = num_map if num_map is not None else COLOR_NUM
        self.zero_label = zero_label
        self.entry_builder = entry_builder if entry_builder is not None else build_entry_message
        self.thread_signals = thread_signals if thread_signals is not None else THREAD_SIGNALS
        self.thread_stats = thread_stats if thread_stats is not None else THREAD_STATS
        self.target_symbol = target_symbol
        self.table = None

        self.state = {
            "active": False, "pattern": None, "bet_colors": None,
            "attempts_left": 0, "total_attempts": COLOR_MAX_ATTEMPTS,
            "context": None, "bet_amount": 0, "current_attempt": 0,
            "waiting_for_start": False, "spins_until_start": 0, "start_attempt": 1,
        }
        self.train_state = {
            "active": False, "pattern": None, "bet_colors": None,
            "attempts_left": 0, "total_attempts": COLOR_MAX_ATTEMPTS,
            "context": None, "bet_amount": 0, "current_attempt": 0,
            "waiting_for_start": False, "spins_until_start": 0, "start_attempt": 1,
        }
        self.train_attempt_results = []
        self.live_enabled = True
        self.direction_stats = {"repeat": {"wins": 0, "losses": 0}, "change": {"wins": 0, "losses": 0}}
        self.history_log = []
        self.history_counter = 0
        self.stats = {"total": 0, "won": 0, "lost": 0}
        self.pattern_context = {}
        self.backtest = {"triggers": 0, "hits": 0, "accuracy": None}
        self.consecutive_losses = 0
        self.cooldown_remaining = 0
        self.msg_id = None
        self.entry_text = None
        self.attempt_results = []
        self._last_raw_number = None
        self.total_processed = 0
        self.trained = False
        self.last_train_ts = 0.0
        self.trained_snapshot = {}
        self.wait_spins_after_resolution = 0
        # Para V5: conteo de señales consecutivas
        self.consecutive_signals = 0
        self.signal_enabled = True
        # Para victorias consecutivas (ya no se usa para V5, pero lo dejamos por si acaso)
        self.consecutive_wins = 0

    def _match(self, window):
        if len(window) != self.pattern_len:
            return None
        if self.mode == "aaaba":
            a, b = window[0], window[3]
            ok = (window[1] == a and window[2] == a and window[4] == a)
        elif self.mode == "aaabbaa":
            a, b = window[0], window[3]
            ok = (window[1] == a and window[2] == a and window[4] == b and window[5] == a and window[6] == a)
        elif self.mode == "aabbaa":
            a, b = window[0], window[2]
            ok = (window[1] == a and window[3] == b and window[4] == a and window[5] == a)
        elif self.mode == "ababa":
            a, b = window[0], window[1]
            ok = (window[2] == a and window[3] == b and window[4] == a)
        elif self.mode == "aaabbb":
            a, b = window[0], window[3]
            ok = (window[1] == a and window[2] == a and window[4] == b and window[5] == b)
        elif self.mode == "aaaaba":
            a, b = window[0], window[4]
            ok = (window[1] == a and window[2] == a and window[3] == a and window[5] == a)
        elif self.mode == "aaabaaa":
            a, b = window[0], window[3]
            ok = (window[1] == a and window[2] == a and window[4] == b and window[5] == a and window[6] == a)
        elif self.mode == "aaabaa":
            a, b = window[0], window[2]
            ok = (window[1] == a and window[3] == b and window[4] == a and window[5] == a)
        else:
            return None
        if not (ok and a in self.values and b in self.values and a != b):
            return None
        return (a, b)

    def _bet_colors(self, pattern):
        if self.target_symbol == 'a':
            return (pattern[0],)
        else:
            return (pattern[1],)

    @staticmethod
    def _key(pattern):
        return ">".join(pattern)

    def _get_context_signature(self, color_history, trend, amx_strength_val, direction):
        recent = color_history[-5:] if len(color_history) >= 5 else color_history
        recent_str = ",".join(recent)
        if amx_strength_val >= AMX_STRENGTH_THRESHOLDS["strong"]:
            strength_cat = "strong"
        elif amx_strength_val < AMX_STRENGTH_THRESHOLDS["weak"]:
            strength_cat = "weak"
        else:
            strength_cat = "medium"
        return f"{recent_str}|{trend}|{strength_cat}|{direction}"

    def _calculate_context_similarity(self, sig1: str, sig2: str) -> float:
        parts1 = sig1.split("|")
        parts2 = sig2.split("|")
        if len(parts1) != len(parts2):
            return 0.0
        similarity = 0.0
        weights = [0.6, 0.15, 0.15, 0.10]
        for i, (p1, p2) in enumerate(zip(parts1, parts2)):
            if p1 == p2:
                similarity += weights[i]
            elif i == 0:
                seq1 = p1.split(",")
                seq2 = p2.split(",")
                if len(seq1) == len(seq2):
                    matches = sum(1 for a, b in zip(seq1, seq2) if a == b)
                    similarity += weights[i] * (matches / len(seq1))
        return similarity

    def _record_context(self, pattern, hit_attempt: int, context_signature: str, filters: dict):
        key = self._key(pattern)
        arr = self.pattern_context.setdefault(key, [])
        arr.append({
            "hit_attempt": hit_attempt,
            "context_signature": context_signature,
            "filters": filters
        })
        if len(arr) > COLOR_CONTEXT_WINDOW:
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

    def _win_rate(self, pattern):
        if not self.trained:
            return None
        arr = self.trained_snapshot.get(self._key(pattern), [])
        if len(arr) < COLOR_MIN_SAMPLES_GATE:
            return None
        return sum(1 for v in arr if v["hit_attempt"] > 0) / len(arr)

    def _gated(self, pattern, required_win_rate):
        rate = self._win_rate(pattern)
        if rate is None:
            return False
        return rate < required_win_rate

    def _recommended_start_attempt(self, pattern, current_context_signature: str, current_filters: dict):
        if not self.trained:
            return 1, 0.0
        arr = self.trained_snapshot.get(self._key(pattern), [])
        if len(arr) < COLOR_MIN_SAMPLES_GATE:
            return 1, 0.0
        similar_cases = []
        for case in arr:
            similarity = self._calculate_context_similarity(current_context_signature, case["context_signature"])
            if similarity >= CONTEXT_SIMILARITY_THRESHOLD:
                similar_cases.append((case, similarity))
        if len(similar_cases) < 3:
            similar_cases = [(case, 1.0) for case in arr]
        start_effectiveness = {}
        for start in range(1, COLOR_ANALYSIS_WINDOW + 1):
            hits = 0
            for case, similarity in similar_cases:
                hit = case["hit_attempt"]
                if start <= hit <= start + 1:
                    hits += similarity
            start_effectiveness[start] = hits
        best_start = max(start_effectiveness, key=start_effectiveness.get)
        best_hits = start_effectiveness[best_start]
        total_weight = sum(s for _, s in similar_cases)
        best_pct = round((best_hits / total_weight) * 100, 1) if total_weight > 0 else 0.0
        return best_start, best_pct

    def is_fasttrack_ready(self):
        total = self.stats.get("total", 0)
        if total < LIVE_FASTTRACK_MIN_SAMPLES:
            return False
        won = self.stats.get("won", 0)
        return (won / total) >= LIVE_FASTTRACK_MIN_WIN_RATE

    def overall_recommended_attempt(self):
        if not self.trained:
            return None, 0.0
        start_effectiveness = {i: 0 for i in range(1, COLOR_ANALYSIS_WINDOW + 1)}
        total_patterns = 0
        for arr in self.trained_snapshot.values():
            for case in arr:
                total_patterns += 1
                hit = case["hit_attempt"]
                if hit > 0:
                    for start in range(1, COLOR_ANALYSIS_WINDOW + 1):
                        if start <= hit <= start + 1:
                            start_effectiveness[start] += 1
        if total_patterns < COLOR_MIN_SAMPLES_GATE:
            return None, 0.0
        best_start = max(start_effectiveness, key=start_effectiveness.get)
        best_hits = start_effectiveness[best_start]
        best_pct = round((best_hits / total_patterns) * 100, 1)
        return best_start, best_pct

    def _preferred_direction(self, min_samples: int = 10):
        rep = self.direction_stats["repeat"]
        chg = self.direction_stats["change"]
        rep_total = rep["wins"] + rep["losses"]
        chg_total = chg["wins"] + chg["losses"]
        if rep_total < min_samples and chg_total < min_samples:
            return None
        rep_rate = (rep["wins"] / rep_total) if rep_total >= min_samples else -1
        chg_rate = (chg["wins"] / chg_total) if chg_total >= min_samples else -1
        if rep_rate < 0 and chg_rate < 0:
            return None
        return "repeat" if rep_rate >= chg_rate else "change"

    def _ml_should_signal(self, pattern, trend_colors, amx_strength_val):
        if not self.signal_enabled:
            return False
        if self.cooldown_remaining > 0:
            return False
        base_rate = COLOR_MIN_WIN_RATE
        if amx_strength_val >= AMX_STRENGTH_THRESHOLDS["strong"]:
            required_rate = base_rate * AMX_ADJUST_FACTOR_STRONG
        elif amx_strength_val < AMX_STRENGTH_THRESHOLDS["weak"]:
            required_rate = base_rate * AMX_ADJUST_FACTOR_WEAK
        else:
            required_rate = base_rate
        required_rate = max(0.45, min(0.75, required_rate))
        if self._gated(pattern, required_rate):
            return False
        return True

    def run_backtest(self, color_history):
        window = color_history[-COLOR_BACKTEST_WINDOW:]
        triggers, hits = 0, 0
        for i in range(self.pattern_len, len(window) + 1):
            seg = window[i - self.pattern_len:i]
            pattern = self._match(seg)
            if not pattern:
                continue
            bet_colors = self._bet_colors(pattern)
            future = window[i:i + COLOR_MAX_ATTEMPTS]
            triggers += 1
            if any(d in future for d in bet_colors):
                hits += 1
        self.backtest = {
            "triggers": triggers, "hits": hits,
            "accuracy": round(hits / triggers, 4) if triggers else None
        }

    def update(self, color_history, timestamp, blocked: bool = False,
               trend_colors=None, amx_strength_val=0.0, last_number=None,
               live_enabled: bool = True, bet_amount: int = 0, trend=None, direction=None):
        self._last_raw_number = last_number
        self.live_enabled = live_enabled
        if not color_history:
            return
        last = color_history[-1]

        allow_new_signal = True
        if self.wait_spins_after_resolution > 0:
            self.wait_spins_after_resolution -= 1
            allow_new_signal = False

        # ── Resolución de señal REAL activa ──
        if self.state["active"]:
            if self.state["waiting_for_start"]:
                self.state["spins_until_start"] -= 1
                if self.state["spins_until_start"] <= 0:
                    self.state["waiting_for_start"] = False
                    self.state["current_attempt"] = 0
                    log.info(f"🎯 {self.name} Iniciando apuesta en intento {self.state['start_attempt']}")
            else:
                self.state["current_attempt"] += 1
                attempt = self.state["start_attempt"] + self.state["current_attempt"] - 1
                is_win = (last in self.state["bet_colors"])
                self.attempt_results.append(last_number)

                if self.table is not None:
                    prev_cycles = self.table.labouchere.cycles_completed
                    self.table.labouchere.update(is_win)
                    if self.table.labouchere.cycles_completed > prev_cycles:
                        self.table._mark_cycle_pending(self.table.labouchere.cycles_completed)

                if is_win:
                    asyncio.create_task(send_msg(attempt_win_label(attempt), self.thread_stats))
                    self._close(True, last, attempt, timestamp, self.state, dispatch=True)
                else:
                    self.state["attempts_left"] -= 1
                    if self.state["attempts_left"] > 0:
                        if self.table is not None:
                            new_bet = self.table.labouchere.get_bet()
                            seq_txt = self.table.labouchere.seq_str()
                            self.state["bet_amount"] = new_bet
                        else:
                            new_bet, seq_txt = self.state.get("bet_amount", 0), ""
                        next_attempt = attempt + 1
                        msg = build_follow_up_message(new_bet, next_attempt, seq_txt)
                        asyncio.create_task(send_msg(msg, self.thread_signals))
                    else:
                        asyncio.create_task(send_msg(ATTEMPT_LOSS_LABEL, self.thread_stats))
                        self._close(False, last, attempt, timestamp, self.state, dispatch=True)

        # ── Resolución de señal de SOMBRA ──
        if self.train_state["active"]:
            if self.train_state["waiting_for_start"]:
                self.train_state["spins_until_start"] -= 1
                if self.train_state["spins_until_start"] <= 0:
                    self.train_state["waiting_for_start"] = False
                    self.train_state["current_attempt"] = 0
            else:
                self.train_state["current_attempt"] += 1
                attempt = self.train_state["start_attempt"] + self.train_state["current_attempt"] - 1
                is_win = (last in self.train_state["bet_colors"])
                self.train_attempt_results.append(last_number)
                if is_win:
                    self._close(True, last, attempt, timestamp, self.train_state, dispatch=False)
                else:
                    self.train_state["attempts_left"] -= 1
                    if self.train_state["attempts_left"] <= 0:
                        self._close(False, last, attempt, timestamp, self.train_state, dispatch=False)

        self.run_backtest(color_history)
        if self.cooldown_remaining > 0:
            self.cooldown_remaining -= 1
        self._maybe_train(timestamp)

        # ── Detectar nuevo patrón ──
        if allow_new_signal and (not self.state["active"] and not self.train_state["active"]
                and len(color_history) >= self.pattern_len
                and len(color_history) >= COLOR_MIN_SPIN_TO_SIGNAL):
            pattern = self._match(color_history[-self.pattern_len:])
            direction = "change"
            if pattern and self._ml_should_signal(pattern, trend_colors, amx_strength_val):
                bet_colors = self._bet_colors(pattern)
                context = list(color_history[-COLOR_CONTEXT_WINDOW:])
                context_signature = self._get_context_signature(color_history, trend, amx_strength_val, direction)
                current_filters = {
                    "trend": trend,
                    "amx_strength": amx_strength_val,
                    "direction": direction,
                    "trend_colors": list(trend_colors) if trend_colors else []
                }
                start_attempt, start_pct = self._recommended_start_attempt(pattern, context_signature, current_filters)
                new_state = {
                    "active": True, "pattern": list(pattern), "bet_colors": list(bet_colors),
                    "attempts_left": COLOR_MAX_ATTEMPTS, "total_attempts": COLOR_MAX_ATTEMPTS,
                    "context": context, "direction": direction, "bet_amount": bet_amount,
                    "current_attempt": 0, "waiting_for_start": start_attempt > 1,
                    "spins_until_start": start_attempt - 1, "start_attempt": start_attempt,
                    "context_signature": context_signature, "filters": current_filters,
                }
                if self.table is not None:
                    bet_amount = self.table.labouchere.get_bet()
                if not blocked and self.live_enabled:
                    # === Manejo especial para agente V5 (ababa) ===
                    if self.mode == "ababa":
                        # Si está desactivado por haber alcanzado 3 señales consecutivas,
                        # se reactiva y se reinicia el contador (nuevas condiciones)
                        if not self.signal_enabled:
                            self.signal_enabled = True
                            self.consecutive_signals = 0
                            log.info(f"🔄 {self.name} reactivado (nuevas condiciones)")
                        # Incrementar contador de señales consecutivas
                        self.consecutive_signals += 1
                        if self.consecutive_signals >= 3:
                            # Desactivar después de emitir la tercera señal
                            self.signal_enabled = False
                            log.info(f"🔇 {self.name} desactivado tras 3 señales consecutivas")
                    # === Fin manejo V5 ===
                    self.state = new_state
                    self.state["bet_amount"] = bet_amount
                    self.attempt_results = []
                    self.wait_spins_after_resolution = 0
                    seq_txt = self.table.labouchere.seq_str() if self.table is not None else ""
                    entry_text = self.entry_builder(self._last_raw_number, self.state["bet_colors"],
                                                   bet_amount=bet_amount, start_attempt=start_attempt,
                                                   sequence_str=seq_txt)
                    self.entry_text = entry_text
                    log.info(f"🔔 {self.name} NUEVA SEÑAL: {pattern} -> {bet_colors} (apuesta {format_cop(bet_amount)})")
                    asyncio.create_task(self._dispatch_entry(entry_text))
                else:
                    self.train_state = new_state
                    self.train_attempt_results = []

    async def _dispatch_entry(self, entry_text: str):
        self.msg_id = await send_msg(entry_text, self.thread_signals)

    async def _dispatch_resolution(self, win: bool, attempt_results: list, bet_amount: int):
        resolution_text = build_resolution_message(win, attempt_results, bet_amount)
        await send_msg(resolution_text, self.thread_signals)
        if self.daily_marker is not None:
            await self.daily_marker.record(win)
        if self.table is not None:
            await self.table.maybe_send_daily_marker_and_cycle()

    def _close(self, win: bool, result_color, attempt, timestamp, state: dict, dispatch: bool):
        pattern = tuple(state["pattern"])
        bet_colors = tuple(state["bet_colors"])
        direction = state.get("direction")
        bet_amount = state.get("bet_amount", 0)
        hit_attempt = attempt if win else 0
        context_signature = state.get("context_signature", "")
        filters = state.get("filters", {})

        self.history_counter += 1
        self.history_log.append({
            "n": self.history_counter, "pattern": ">".join(pattern),
            "bet_colors": list(bet_colors), "result": result_color,
            "attempt": attempt, "win": win, "hit_attempt": hit_attempt,
            "context": state.get("context"), "time": timestamp, "shadow": not dispatch,
            "bet_amount": bet_amount, "context_signature": context_signature,
        })
        self.history_log = self.history_log[-200:]
        self.stats["total"] += 1
        self.stats["won" if win else "lost"] += 1
        self._record_context(pattern, hit_attempt, context_signature, filters)
        self.total_processed += 1
        self._maybe_train(timestamp)

        if direction in ("repeat", "change"):
            self.direction_stats[direction]["wins" if win else "losses"] += 1

        # Ya no usamos consecutive_wins para V5, pero lo mantenemos por compatibilidad
        if self.mode == "ababa":
            if win:
                self.consecutive_wins += 1
            else:
                self.consecutive_wins = 0

        reset_state = {
            "active": False, "pattern": None, "bet_colors": None,
            "attempts_left": 0, "total_attempts": COLOR_MAX_ATTEMPTS,
            "context": None, "direction": None, "bet_amount": 0,
            "current_attempt": 0, "waiting_for_start": False, "spins_until_start": 0, "start_attempt": 1,
        }

        if dispatch:
            if win:
                self.consecutive_losses = 0
            else:
                self.consecutive_losses += 1
                if self.consecutive_losses >= COLOR_COOLDOWN_AFTER_LOSSES:
                    self.cooldown_remaining = COLOR_COOLDOWN_ROUNDS
            self.state = reset_state
            self.wait_spins_after_resolution = 1
            attempt_results = list(self.attempt_results)
            asyncio.create_task(self._dispatch_resolution(win, attempt_results, bet_amount))
            self.msg_id = None
            self.entry_text = None
            self.attempt_results = []
        else:
            self.train_state = reset_state
            self.train_attempt_results = []

    def get_state(self):
        rec_attempt, rec_pct = self.overall_recommended_attempt()
        pattern_recommendations = {}
        for key in self.pattern_context:
            pattern_tuple = tuple(key.split(">"))
            rec, pct = self._recommended_start_attempt(pattern_tuple, "", {})
            if rec is not None:
                pattern_recommendations[key] = {"start_attempt": rec, "pct": pct}
        return {
            "name": self.name,
            "pattern_len": self.pattern_len,
            "signal_state": self.state,
            "stats": self.stats,
            "history": self.history_log[-30:],
            "backtest_60": self.backtest,
            "pattern_context": self.pattern_context,
            "consecutive_losses": self.consecutive_losses,
            "cooldown_remaining": self.cooldown_remaining,
            "recommended_start_attempt": rec_attempt,
            "recommended_start_attempt_pct": rec_pct,
            "pattern_recommendations": pattern_recommendations,
            "signal_enabled": self.signal_enabled,
            "consecutive_signals": self.consecutive_signals,
            "consecutive_wins": self.consecutive_wins,
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
            "direction_stats": self.direction_stats,
            "consecutive_signals": self.consecutive_signals,
            "signal_enabled": self.signal_enabled,
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
        self.direction_stats = data.get("direction_stats", self.direction_stats)
        self.consecutive_signals = data.get("consecutive_signals", 0)
        self.signal_enabled = data.get("signal_enabled", True)

# ══════════════════════════════════════════════
# ROULETTE TABLE
# ══════════════════════════════════════════════
class RouletteTable:
    def __init__(self, key: int):
        self.key = key
        self.spin_history = []
        self.prev_number = None
        self.last_update_time = time.time()
        self.color_history = []
        self.zone_history = []
        self.paridad_history = []
        self.total_spins_seen = 0
        self.live_spins_seen = 0
        self.daily_marker = DailyMarker()
        self.labouchere = LabouchereManager(base_amount=LABOUCHERE_BASE_AMOUNT)
        self.cycle_pending = False
        self.cycle_number = 0

        # Agentes de COLOR (8)
        self.agent1 = ColorPatternAgent(pattern_len=6, name="AGENTE_1", label="PATRON V1 💎", mode="aaaaba",
                                        daily_marker=self.daily_marker, target_symbol='a')
        self.agent2 = ColorPatternAgent(pattern_len=5, name="AGENTE_2", label="PATRON V2 💎", mode="aaaba",
                                        daily_marker=self.daily_marker, target_symbol='a')
        self.agent3 = ColorPatternAgent(pattern_len=6, name="AGENTE_3", label="PATRON V3 💎", mode="aabbaa",
                                        daily_marker=self.daily_marker, target_symbol='b')
        self.agent4 = ColorPatternAgent(pattern_len=7, name="AGENTE_4", label="PATRON V4 💎", mode="aaabbaa",
                                        daily_marker=self.daily_marker, target_symbol='a')
        self.agent5 = ColorPatternAgent(pattern_len=5, name="AGENTE_5", label="PATRON V5 💎", mode="ababa",
                                        daily_marker=self.daily_marker, target_symbol='b')
        self.agent6 = ColorPatternAgent(pattern_len=6, name="AGENTE_6", label="PATRON V6 💎", mode="aaabbb",
                                        daily_marker=self.daily_marker, target_symbol='a')
        self.agent7 = ColorPatternAgent(pattern_len=7, name="AGENTE_7", label="PATRON V7 💎", mode="aaabaaa",
                                        daily_marker=self.daily_marker, target_symbol='a')
        self.agent8 = ColorPatternAgent(pattern_len=6, name="AGENTE_8", label="PATRON V8 💎", mode="aaabaa",
                                        daily_marker=self.daily_marker, target_symbol='a')

        # Agentes de ZONA (8)
        zone_kwargs = dict(values=ZONE_VALUES, num_map=ZONE_NUM, zero_label="VERDE",
                           entry_builder=build_entry_message_zone,
                           thread_signals=THREAD_SIGNALS_ZONE, thread_stats=THREAD_STATS_ZONE)
        self.zone_agent1 = ColorPatternAgent(pattern_len=6, name="ZONA_AGENTE_1", label="PATRON ZONA V1 💎", mode="aaaaba",
                                             daily_marker=self.daily_marker, target_symbol='a', **zone_kwargs)
        self.zone_agent2 = ColorPatternAgent(pattern_len=5, name="ZONA_AGENTE_2", label="PATRON ZONA V2 💎", mode="aaaba",
                                             daily_marker=self.daily_marker, target_symbol='a', **zone_kwargs)
        self.zone_agent3 = ColorPatternAgent(pattern_len=6, name="ZONA_AGENTE_3", label="PATRON ZONA V3 💎", mode="aabbaa",
                                             daily_marker=self.daily_marker, target_symbol='b', **zone_kwargs)
        self.zone_agent4 = ColorPatternAgent(pattern_len=7, name="ZONA_AGENTE_4", label="PATRON ZONA V4 💎", mode="aaabbaa",
                                             daily_marker=self.daily_marker, target_symbol='a', **zone_kwargs)
        self.zone_agent5 = ColorPatternAgent(pattern_len=5, name="ZONA_AGENTE_5", label="PATRON ZONA V5 💎", mode="ababa",
                                             daily_marker=self.daily_marker, target_symbol='b', **zone_kwargs)
        self.zone_agent6 = ColorPatternAgent(pattern_len=6, name="ZONA_AGENTE_6", label="PATRON ZONA V6 💎", mode="aaabbb",
                                             daily_marker=self.daily_marker, target_symbol='a', **zone_kwargs)
        self.zone_agent7 = ColorPatternAgent(pattern_len=7, name="ZONA_AGENTE_7", label="PATRON ZONA V7 💎", mode="aaabaaa",
                                             daily_marker=self.daily_marker, target_symbol='a', **zone_kwargs)
        self.zone_agent8 = ColorPatternAgent(pattern_len=6, name="ZONA_AGENTE_8", label="PATRON ZONA V8 💎", mode="aaabaa",
                                             daily_marker=self.daily_marker, target_symbol='a', **zone_kwargs)

        # Agentes de PARIDAD (8)
        paridad_kwargs = dict(values=PARIDAD_VALUES, num_map=PARIDAD_NUM, zero_label="VERDE",
                              entry_builder=build_entry_message_paridad,
                              thread_signals=THREAD_SIGNALS_PARIDAD, thread_stats=THREAD_STATS_PARIDAD)
        self.paridad_agent1 = ColorPatternAgent(pattern_len=6, name="PARIDAD_AGENTE_1", label="PATRON PARIDAD V1 💎", mode="aaaaba",
                                                daily_marker=self.daily_marker, target_symbol='a', **paridad_kwargs)
        self.paridad_agent2 = ColorPatternAgent(pattern_len=5, name="PARIDAD_AGENTE_2", label="PATRON PARIDAD V2 💎", mode="aaaba",
                                                daily_marker=self.daily_marker, target_symbol='a', **paridad_kwargs)
        self.paridad_agent3 = ColorPatternAgent(pattern_len=6, name="PARIDAD_AGENTE_3", label="PATRON PARIDAD V3 💎", mode="aabbaa",
                                                daily_marker=self.daily_marker, target_symbol='b', **paridad_kwargs)
        self.paridad_agent4 = ColorPatternAgent(pattern_len=7, name="PARIDAD_AGENTE_4", label="PATRON PARIDAD V4 💎", mode="aaabbaa",
                                                daily_marker=self.daily_marker, target_symbol='a', **paridad_kwargs)
        self.paridad_agent5 = ColorPatternAgent(pattern_len=5, name="PARIDAD_AGENTE_5", label="PATRON PARIDAD V5 💎", mode="ababa",
                                                daily_marker=self.daily_marker, target_symbol='b', **paridad_kwargs)
        self.paridad_agent6 = ColorPatternAgent(pattern_len=6, name="PARIDAD_AGENTE_6", label="PATRON PARIDAD V6 💎", mode="aaabbb",
                                                daily_marker=self.daily_marker, target_symbol='a', **paridad_kwargs)
        self.paridad_agent7 = ColorPatternAgent(pattern_len=7, name="PARIDAD_AGENTE_7", label="PATRON PARIDAD V7 💎", mode="aaabaaa",
                                                daily_marker=self.daily_marker, target_symbol='a', **paridad_kwargs)
        self.paridad_agent8 = ColorPatternAgent(pattern_len=6, name="PARIDAD_AGENTE_8", label="PATRON PARIDAD V8 💎", mode="aaabaa",
                                                daily_marker=self.daily_marker, target_symbol='a', **paridad_kwargs)

        for attr in dir(self):
            obj = getattr(self, attr)
            if isinstance(obj, ColorPatternAgent):
                obj.table = self

        self.zone_level_history = []
        self.zone_level_current = 0
        self.last_zone_num = None
        self.paridad_level_history = []
        self.paridad_level_current = 0
        self.last_paridad_num = None
        self.level_history = []
        self.level_current = 0
        self.last_color_num = None
        self.trend = "neutral"

    def _mark_cycle_pending(self, num: int):
        self.cycle_pending = True
        self.cycle_number = num

    async def _send_cycle_message(self):
        if not self.cycle_pending:
            return
        lab_state = self.labouchere.get_state()
        sign = '+' if lab_state['balance'] >= 0 else '-'
        msg = (f"🎉🎉 CICLO #{self.cycle_number} COMPLETA 🎉🎉\n"
               f"📈 Acumulado: {sign}{format_cop(abs(lab_state['balance']))}\n"
               f"🇨🇴 Apuesta Base: {format_cop(lab_state['base_amount'])}\n")
        await send_msg(msg, THREAD_SIGNALS)
        self.cycle_pending = False
        self.cycle_number = 0

    async def maybe_send_daily_marker_and_cycle(self):
        active = any(
            a.state["active"] for a in [
                self.agent1, self.agent2, self.agent3, self.agent4, self.agent5, self.agent6, self.agent7, self.agent8,
                self.zone_agent1, self.zone_agent2, self.zone_agent3, self.zone_agent4,
                self.zone_agent5, self.zone_agent6, self.zone_agent7, self.zone_agent8,
                self.paridad_agent1, self.paridad_agent2, self.paridad_agent3, self.paridad_agent4,
                self.paridad_agent5, self.paridad_agent6, self.paridad_agent7, self.paridad_agent8
            ]
        )
        if not active:
            if self.daily_marker.stats["total"] > 0:
                text = build_daily_marker_message(self.daily_marker.stats)
                await send_msg(text, self.daily_marker.thread_signals)
            if self.cycle_pending:
                await self._send_cycle_message()

    def _level_change(self, real_color_num: int) -> int:
        if real_color_num == 1: return 1
        if real_color_num == 2: return -1
        if self.last_color_num == 1: return 1
        if self.last_color_num == 2: return -1
        return 0

    def _zone_level_change(self, zone_num: int) -> int:
        if zone_num == 1: return 1
        if zone_num == 2: return -1
        if self.last_zone_num == 1: return 1
        if self.last_zone_num == 2: return -1
        return 0

    def _paridad_level_change(self, paridad_num: int) -> int:
        if paridad_num == 1: return 1
        if paridad_num == 2: return -1
        if self.last_paridad_num == 1: return 1
        if self.last_paridad_num == 2: return -1
        return 0

    def update(self, number: int, real_color: str, timestamp: float = None, signal_mode: str = "tendencia",
               training: bool = False):
        if timestamp is None: timestamp = time.time()
        self.spin_history.append({"number": number, "color": real_color, "timestamp": timestamp})
        if len(self.spin_history) > 200: self.spin_history.pop(0)
        self.prev_number = number
        self.total_spins_seen += 1
        if not training:
            self.live_spins_seen += 1

        dz = real_color
        self.color_history.append(dz)
        if len(self.color_history) > 300: self.color_history = self.color_history[-300:]

        zdz = zone_of(number)
        self.zone_history.append(zdz)
        if len(self.zone_history) > 300: self.zone_history = self.zone_history[-300:]

        pdz = paridad_of(number)
        self.paridad_history.append(pdz)
        if len(self.paridad_history) > 300: self.paridad_history = self.paridad_history[-300:]

        real_color_num = COLOR_NUM[dz]
        change = self._level_change(real_color_num)
        self.level_current += change
        self.level_history.append(self.level_current)
        if len(self.level_history) > 100: self.level_history.pop(0)
        if real_color_num != 0:
            self.last_color_num = real_color_num

        zone_num = ZONE_NUM[zdz]
        zone_change = self._zone_level_change(zone_num)
        self.zone_level_current += zone_change
        self.zone_level_history.append(self.zone_level_current)
        if len(self.zone_level_history) > 100: self.zone_level_history.pop(0)
        if zone_num != 0:
            self.last_zone_num = zone_num

        paridad_num = PARIDAD_NUM[pdz]
        paridad_change = self._paridad_level_change(paridad_num)
        self.paridad_level_current += paridad_change
        self.paridad_level_history.append(self.paridad_level_current)
        if len(self.paridad_level_history) > 100: self.paridad_level_history.pop(0)
        if paridad_num != 0:
            self.last_paridad_num = paridad_num

        # ── Actualizar agentes ──
        agent_list = [self.agent1, self.agent2, self.agent3, self.agent4, self.agent5, self.agent6, self.agent7, self.agent8]
        agent_keys = ["agent1", "agent2", "agent3", "agent4", "agent5", "agent6", "agent7", "agent8"]
        zone_agent_list = [self.zone_agent1, self.zone_agent2, self.zone_agent3,
                           self.zone_agent4, self.zone_agent5, self.zone_agent6, self.zone_agent7, self.zone_agent8]
        zone_agent_keys = ["agent1", "agent2", "agent3", "agent4", "agent5", "agent6", "agent7", "agent8"]
        paridad_agent_list = [self.paridad_agent1, self.paridad_agent2, self.paridad_agent3,
                              self.paridad_agent4, self.paridad_agent5, self.paridad_agent6, self.paridad_agent7, self.paridad_agent8]
        paridad_agent_keys = ["agent1", "agent2", "agent3", "agent4", "agent5", "agent6", "agent7", "agent8"]
        all_agents = agent_list + zone_agent_list + paridad_agent_list
        table_ready = self.total_spins_seen >= TABLE_MIN_SPINS_LIVE
        color_processed = sum(a.total_processed for a in agent_list)
        zone_processed = sum(a.total_processed for a in zone_agent_list)
        paridad_processed = sum(a.total_processed for a in paridad_agent_list)
        color_category_ready = table_ready and color_processed >= CATEGORY_MIN_PROCESSED_LIVE
        zone_category_ready = table_ready and zone_processed >= CATEGORY_MIN_PROCESSED_LIVE
        paridad_category_ready = table_ready and paridad_processed >= CATEGORY_MIN_PROCESSED_LIVE
        bet_amount = self.labouchere.get_bet()

        # ── 1) ZONA ──
        for agente, key in zip(zone_agent_list, zone_agent_keys):
            config = AGENT_TREND_CONFIG.get(key, {})
            method = config.get("method", "amx")
            if method == "ema":
                periods = config.get("ema_periods", [4, 8, 20])
                trend = ema_trend(self.zone_level_history, periods,
                                  strictness=config.get("strictness", "relaxed"),
                                  min_diff=config.get("min_diff", 0.0))
                amx_strength_val = 0.0
            elif method == "ema_long":
                periods = config.get("ema_periods", [50, 70, 200])
                trend = ema_trend(self.zone_level_history, periods,
                                  strictness=config.get("strictness", "relaxed"),
                                  min_diff=config.get("min_diff", 0.0))
                amx_strength_val = 0.0
            else:  # amx
                periods = config.get("amx_periods", [5, 10, 20])
                trend = amx_trend(self.zone_level_history, periods,
                                  strictness=config.get("strictness", "relaxed"),
                                  threshold=config.get("threshold", 0.5))
                amx_strength_val = amx_strength(self.zone_level_history, periods)
            favored = trend_favored_zones(trend)
            blocked = any(a.state["active"] for a in all_agents if a is not agente)
            live_ok = (not training) and zone_category_ready and (agente.trained or agente.is_fasttrack_ready())
            live_ok = live_ok and (self.live_spins_seen >= LIVE_MIN_SPINS_TO_SIGNAL)
            agente.update(self.zone_history, timestamp, blocked=blocked,
                          trend_colors=favored, amx_strength_val=amx_strength_val,
                          last_number=number,
                          live_enabled=live_ok,
                          bet_amount=bet_amount if not blocked else 0,
                          trend=trend, direction=None)

        # ── 2) PARIDAD ──
        for agente, key in zip(paridad_agent_list, paridad_agent_keys):
            config = AGENT_TREND_CONFIG.get(key, {})
            method = config.get("method", "amx")
            if method == "ema":
                periods = config.get("ema_periods", [4, 8, 20])
                trend = ema_trend(self.paridad_level_history, periods,
                                  strictness=config.get("strictness", "relaxed"),
                                  min_diff=config.get("min_diff", 0.0))
                amx_strength_val = 0.0
            elif method == "ema_long":
                periods = config.get("ema_periods", [50, 70, 200])
                trend = ema_trend(self.paridad_level_history, periods,
                                  strictness=config.get("strictness", "relaxed"),
                                  min_diff=config.get("min_diff", 0.0))
                amx_strength_val = 0.0
            else:
                periods = config.get("amx_periods", [5, 10, 20])
                trend = amx_trend(self.paridad_level_history, periods,
                                  strictness=config.get("strictness", "relaxed"),
                                  threshold=config.get("threshold", 0.5))
                amx_strength_val = amx_strength(self.paridad_level_history, periods)
            favored = trend_favored_paridad(trend)
            blocked = any(a.state["active"] for a in all_agents if a is not agente)
            live_ok = (not training) and paridad_category_ready and (agente.trained or agente.is_fasttrack_ready())
            live_ok = live_ok and (self.live_spins_seen >= LIVE_MIN_SPINS_TO_SIGNAL)
            agente.update(self.paridad_history, timestamp, blocked=blocked,
                          trend_colors=favored, amx_strength_val=amx_strength_val,
                          last_number=number,
                          live_enabled=live_ok,
                          bet_amount=bet_amount if not blocked else 0,
                          trend=trend, direction=None)

        # ── 3) COLOR ──
        for agente, key in zip(agent_list, agent_keys):
            config = AGENT_TREND_CONFIG.get(key, {})
            method = config.get("method", "amx")
            if method == "ema":
                periods = config.get("ema_periods", [4, 8, 20])
                trend = ema_trend(self.level_history, periods,
                                  strictness=config.get("strictness", "relaxed"),
                                  min_diff=config.get("min_diff", 0.0))
                amx_strength_val = 0.0
            elif method == "ema_long":
                periods = config.get("ema_periods", [50, 70, 200])
                trend = ema_trend(self.level_history, periods,
                                  strictness=config.get("strictness", "relaxed"),
                                  min_diff=config.get("min_diff", 0.0))
                amx_strength_val = 0.0
            else:
                periods = config.get("amx_periods", [5, 10, 20])
                trend = amx_trend(self.level_history, periods,
                                  strictness=config.get("strictness", "relaxed"),
                                  threshold=config.get("threshold", 0.5))
                amx_strength_val = amx_strength(self.level_history, periods)
            favored = trend_favored_colors(trend)
            blocked = any(a.state["active"] for a in all_agents if a is not agente)
            live_ok = (not training) and color_category_ready and (agente.trained or agente.is_fasttrack_ready())
            live_ok = live_ok and (self.live_spins_seen >= LIVE_MIN_SPINS_TO_SIGNAL)
            agente.update(self.color_history, timestamp, blocked=blocked,
                          trend_colors=favored, amx_strength_val=amx_strength_val,
                          last_number=number,
                          live_enabled=live_ok,
                          bet_amount=bet_amount if not blocked else 0,
                          trend=trend, direction=None)

        # ── Logging ──
        if training:
            return
        lab_state = self.labouchere.get_state()
        lab_seq = ','.join(str(x) for x in lab_state['sequence'])
        lab_bet = lab_state['bet_amount']
        last10 = ",".join(self.color_history[-10:])
        activos = [f"{a.name}({'+'.join(a.state['bet_colors'])}, inicio={a.state['start_attempt']}, {a.state['current_attempt']}/{a.state['total_attempts']})"
                   for a in agent_list if a.state["active"]]
        activos += [f"{a.name}({'+'.join(a.state['bet_colors'])}, inicio={a.state['start_attempt']}, {a.state['current_attempt']}/{a.state['total_attempts']})"
                    for a in zone_agent_list if a.state["active"]]
        activos += [f"{a.name}({'+'.join(a.state['bet_colors'])}, inicio={a.state['start_attempt']}, {a.state['current_attempt']}/{a.state['total_attempts']})"
                    for a in paridad_agent_list if a.state["active"]]
        activos_txt = " | ".join(activos) if activos else "ninguna"
        log.info(
            f"🎰 Mesa {self.key} | Giro #{len(self.color_history)}: {number} ({real_color}) → {dz}/{zdz}/{pdz} "
            f"(color {real_color_num}, zona {zone_num}, paridad {paridad_num}) | "
            f"Nivel tendencia color: {self.level_current} | zona: {self.zone_level_current} | paridad: {self.paridad_level_current} | "
            f"Live: color={color_category_ready} zona={zone_category_ready} paridad={paridad_category_ready} "
            f"(giros={self.total_spins_seen}/{TABLE_MIN_SPINS_LIVE}, "
            f"procesadas color={color_processed} zona={zone_processed} paridad={paridad_processed} de {CATEGORY_MIN_PROCESSED_LIVE}) | "
            f"Lab: [{lab_seq}] {format_cop(lab_bet)} | Últimos 10 colores: [{last10}] | Señales activas: {activos_txt} | "
            f"Live spins: {self.live_spins_seen}/{LIVE_MIN_SPINS_TO_SIGNAL}"
        )

    def get_state(self, limit: int = 40):
        hist = self.spin_history[-limit:] if self.spin_history else []
        return {
            "key": self.key,
            "spin_history": hist,
            "color_history": self.color_history[-limit:],
            "zone_history": self.zone_history[-limit:],
            "paridad_history": self.paridad_history[-limit:],
            "agent1": self.agent1.get_state(),
            "agent2": self.agent2.get_state(),
            "agent3": self.agent3.get_state(),
            "agent4": self.agent4.get_state(),
            "agent5": self.agent5.get_state(),
            "agent6": self.agent6.get_state(),
            "agent7": self.agent7.get_state(),
            "agent8": self.agent8.get_state(),
            "zone_agent1": self.zone_agent1.get_state(),
            "zone_agent2": self.zone_agent2.get_state(),
            "zone_agent3": self.zone_agent3.get_state(),
            "zone_agent4": self.zone_agent4.get_state(),
            "zone_agent5": self.zone_agent5.get_state(),
            "zone_agent6": self.zone_agent6.get_state(),
            "zone_agent7": self.zone_agent7.get_state(),
            "zone_agent8": self.zone_agent8.get_state(),
            "paridad_agent1": self.paridad_agent1.get_state(),
            "paridad_agent2": self.paridad_agent2.get_state(),
            "paridad_agent3": self.paridad_agent3.get_state(),
            "paridad_agent4": self.paridad_agent4.get_state(),
            "paridad_agent5": self.paridad_agent5.get_state(),
            "paridad_agent6": self.paridad_agent6.get_state(),
            "paridad_agent7": self.paridad_agent7.get_state(),
            "paridad_agent8": self.paridad_agent8.get_state(),
            "trend": self.trend,
            "trend_favored_colors": sorted(NUM_COLOR[d] for d in trend_favored_colors(self.trend)),
            "level_current": self.level_current,
            "labouchere": self.labouchere.get_state(),
            "live_spins_seen": self.live_spins_seen,
        }

# ══════════════════════════════════════════════
# ENTRENAMIENTO CON HISTORIAL (arranque)
# ══════════════════════════════════════════════
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

async def train_table_from_history(table: "RouletteTable", spins: list) -> None:
    if not spins:
        return
    log.info(f"[Entrenamiento] Mesa {table.key}: procesando {len(spins)} giros históricos…")
    now = time.time()
    for i, number in enumerate(spins):
        if not (0 <= number <= 36):
            continue
        table.update(number, color_of(number), timestamp=now, training=True)
        if i % 1000 == 0:
            await asyncio.sleep(0)
    log.info(
        f"[Entrenamiento] Mesa {table.key}: listo. giros_vistos={table.total_spins_seen} "
        f"nivel_color={table.level_current} nivel_zona={table.zone_level_current} "
        f"nivel_paridad={table.paridad_level_current}"
    )

# ══════════════════════════════════════════════
# SERVER STATE
# ══════════════════════════════════════════════
class ServerState:
    def __init__(self):
        self.tables = {k: RouletteTable(k) for k in ROULETTE_KEYS.values()}
        self.ws_server = None
        self.signal_mode = "tendencia"
        self.history_seed_trained = {k: False for k in ROULETTE_KEYS.values()}

    def set_ws_server(self, ws_server):
        self.ws_server = ws_server

    def set_signal_mode(self, mode: str):
        if mode in ("tendencia", "moderado"):
            self.signal_mode = mode

    async def update_mesa(self, key: int, number: int, broadcast: bool = True, training: bool = False):
        if key not in self.tables:
            return
        table = self.tables[key]
        real_color = color_of(number)
        table.update(number, real_color, signal_mode=self.signal_mode, training=training)
        if broadcast and self.ws_server and not training:
            state = table.get_state(limit=40)
            state["signal_mode"] = self.signal_mode
            await self.ws_server.broadcast_to_mesa(str(key), "update", state)

    def get_state_for_mesa(self, key: int):
        if key not in self.tables:
            return None
        state = self.tables[key].get_state(limit=40)
        state["signal_mode"] = self.signal_mode
        return state

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
                table.agent1.load_persist(data.get("agent1"))
                table.agent2.load_persist(data.get("agent2"))
                table.agent3.load_persist(data.get("agent3"))
                table.agent4.load_persist(data.get("agent4"))
                table.agent5.load_persist(data.get("agent5"))
                table.agent6.load_persist(data.get("agent6"))
                table.agent7.load_persist(data.get("agent7"))
                table.agent8.load_persist(data.get("agent8"))
                table.zone_agent1.load_persist(data.get("zone_agent1"))
                table.zone_agent2.load_persist(data.get("zone_agent2"))
                table.zone_agent3.load_persist(data.get("zone_agent3"))
                table.zone_agent4.load_persist(data.get("zone_agent4"))
                table.zone_agent5.load_persist(data.get("zone_agent5"))
                table.zone_agent6.load_persist(data.get("zone_agent6"))
                table.zone_agent7.load_persist(data.get("zone_agent7"))
                table.zone_agent8.load_persist(data.get("zone_agent8"))
                table.paridad_agent1.load_persist(data.get("paridad_agent1"))
                table.paridad_agent2.load_persist(data.get("paridad_agent2"))
                table.paridad_agent3.load_persist(data.get("paridad_agent3"))
                table.paridad_agent4.load_persist(data.get("paridad_agent4"))
                table.paridad_agent5.load_persist(data.get("paridad_agent5"))
                table.paridad_agent6.load_persist(data.get("paridad_agent6"))
                table.paridad_agent7.load_persist(data.get("paridad_agent7"))
                table.paridad_agent8.load_persist(data.get("paridad_agent8"))
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
            "agent1": table.agent1.to_persist(),
            "agent2": table.agent2.to_persist(),
            "agent3": table.agent3.to_persist(),
            "agent4": table.agent4.to_persist(),
            "agent5": table.agent5.to_persist(),
            "agent6": table.agent6.to_persist(),
            "agent7": table.agent7.to_persist(),
            "agent8": table.agent8.to_persist(),
            "zone_agent1": table.zone_agent1.to_persist(),
            "zone_agent2": table.zone_agent2.to_persist(),
            "zone_agent3": table.zone_agent3.to_persist(),
            "zone_agent4": table.zone_agent4.to_persist(),
            "zone_agent5": table.zone_agent5.to_persist(),
            "zone_agent6": table.zone_agent6.to_persist(),
            "zone_agent7": table.zone_agent7.to_persist(),
            "zone_agent8": table.zone_agent8.to_persist(),
            "paridad_agent1": table.paridad_agent1.to_persist(),
            "paridad_agent2": table.paridad_agent2.to_persist(),
            "paridad_agent3": table.paridad_agent3.to_persist(),
            "paridad_agent4": table.paridad_agent4.to_persist(),
            "paridad_agent5": table.paridad_agent5.to_persist(),
            "paridad_agent6": table.paridad_agent6.to_persist(),
            "paridad_agent7": table.paridad_agent7.to_persist(),
            "paridad_agent8": table.paridad_agent8.to_persist(),
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
            await train_table_from_history(table, spins_cache)
            self.history_seed_trained[key] = True
            self._save_model(key)

# ══════════════════════════════════════════════
# WEBSOCKET SERVER
# ══════════════════════════════════════════════
class WebSocketServer:
    def __init__(self, server_state: ServerState):
        self.server_state = server_state
        self.rooms = {}
        self.current_mesa = {}

    async def handle(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse(heartbeat=30)
        await ws.prepare(request)
        try:
            async for msg in ws:
                if msg.type != WSMsgType.TEXT:
                    continue
                try:
                    data = json.loads(msg.data)
                except Exception:
                    continue
                if data.get("type") == "subscribe":
                    mesa_raw = data.get("mesa")
                    try:
                        int_mesa = int(mesa_raw)
                    except (TypeError, ValueError):
                        continue
                    if int_mesa not in ROULETTE_KEYS.values():
                        continue
                    mesa = str(int_mesa)
                    prev_mesa = self.current_mesa.get(ws)
                    if prev_mesa is not None and prev_mesa != mesa:
                        prev_room = self.rooms.get(prev_mesa)
                        if prev_room:
                            prev_room.discard(ws)
                    self.current_mesa[ws] = mesa
                    self.rooms.setdefault(mesa, set()).add(ws)
                    state = self.server_state.get_state_for_mesa(int_mesa)
                    if state:
                        await ws.send_str(json.dumps({"type": "initial", "data": state}))
                elif data.get("type") == "set_mode":
                    mode = data.get("mode")
                    self.server_state.set_signal_mode(mode)
                    await self.broadcast_mode(self.server_state.signal_mode)
        finally:
            for room in self.rooms.values():
                room.discard(ws)
            self.current_mesa.pop(ws, None)
        return ws

    async def broadcast_mode(self, mode: str):
        msg = json.dumps({"type": "mode", "data": {"signal_mode": mode}})
        for room in self.rooms.values():
            dead = []
            for ws in room.copy():
                try:
                    await ws.send_str(msg)
                except Exception:
                    dead.append(ws)
            for ws in dead:
                room.discard(ws)

    async def broadcast_to_mesa(self, mesa: str, event: str, data: dict):
        room = self.rooms.get(mesa)
        if not room:
            return
        msg = json.dumps({"type": event, "data": data})
        dead = []
        for ws in room.copy():
            try:
                await ws.send_str(msg)
            except Exception:
                dead.append(ws)
        for ws in dead:
            room.discard(ws)

# ══════════════════════════════════════════════
# WEBSOCKET HANDLER (conexión a Pragmatic Play)
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
                    batch_done = False
                    async for raw in ws:
                        try:
                            data = json.loads(raw)
                        except Exception:
                            continue
                        if not isinstance(data, dict):
                            continue
                        results = data.get("last20Results")
                        if isinstance(results, list):
                            for r in reversed(results):
                                await self._feed(r.get("gameId"), r.get("result"), emit=batch_done)
                            batch_done = True
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
        if not (0 <= num <= 36) or gid in self.seen:
            return
        self.seen.add(gid)
        if len(self.seen) > 3000:
            self.seen.clear()
        if self.on_spin_callback:
            await self.on_spin_callback(num, emit, training=not emit)

# ══════════════════════════════════════════════
# HTTP
# ══════════════════════════════════════════════
_server_state: Optional[ServerState] = None

async def http_home(request: web.Request):
    if request.headers.get("Upgrade", "").lower() == "websocket":
        return await ws_entry(request)
    return web.json_response({"status": "ok", "service": "Adaptive Roulette Server",
                              "mesas": list(ROULETTE_KEYS.values())})

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

async def ws_entry(request: web.Request):
    return await _server_state.ws_server.handle(request)

# ══════════════════════════════════════════════
# SELF-PING
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
# TELEGRAM POLLING
# ══════════════════════════════════════════════
async def bot_polling_loop():
    if bot is None:
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
            log.warning(f"[Telegram] Polling interrumpido: {e}")
        ran_for = time.time() - started
        delay = 5 if ran_for > 60 else min(delay * 2, 60)
        log.warning(f"[Telegram] Reintentando polling en {delay}s…")
        await asyncio.sleep(delay)

# ══════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════
def build_http_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/", http_home)
    app.router.add_get("/ping", http_ping)
    app.router.add_get("/health", http_health)
    app.router.add_get("/api/state/{mesa}", http_api_state)
    app.router.add_get("/api/all", http_api_all)
    app.router.add_get("/ws", ws_entry)
    return app

async def main():
    global _server_state
    log.info("═" * 60)
    log.info("SERVIDOR ADAPTATIVO POR NÚMERO Y MESA (aiohttp + WS)")
    log.info(f"Mesas: {', '.join(str(k) for k in ROULETTE_KEYS.values())}")
    log.info("═" * 60)
    server_state = ServerState()
    server_state.load_all_models()
    _server_state = server_state
    await server_state.train_from_history()
    ws_server = WebSocketServer(server_state)
    server_state.set_ws_server(ws_server)

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
    log.info(f"Servidor HTTP/WebSocket escuchando en puerto {port}")

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
