"""
╔══════════════════════════════════════════════════════════════╗
║   BOT UNIFICADO — SPEED ROULETTEE 2 (key 205)                ║
║   - Detección: 4 agentes de PATRONES DE DOCENA              ║
║       V2: aaba (4)  |  V3: aaaba (5)                        ║
║       V4: abaa (4)  |  V6: aaaabaa (7)                     ║
║   - Agentes de ZONA: baaaabbb, aaa abbbbaa,                 ║
║     aaabaa (a,a,a,b,a,a) y aaabbaa (a,a,a,b,b,a,a)         ║
║   - Agente de RACHAS: señal permisiva si la misma zona     ║
║     sale ZONE_STREAK_MIN  veces seguidas (sin patrón ni ML) ║
║   - NUEVO: Agente de TIMING DE ZONAS (estilo Spaceman       ║
║     3x-5x): predice en cuántos giros volverá a caer ALTA    ║
║     o BAJA promediando el intervalo entre las últimas       ║
║     salidas. Filtro de confirmación con EMA20/50. La señal  ║
║     se repite en AMBOS intentos a la misma zona.            ║
║   - Señales D1+D2/D2+D3: todos los agentes, sin modelo      ║
║   - Señales D1+D3: solo agentes de 4 valores y             ║
║     requieren modelo entrenado                              ║
║   - Conversión: D1+D2 -> BAJA, D2+D3 -> ALTA,              ║
║     D1+D3 -> opuesto de última zona                        ║
║   - Para D1+D2 y D2+D3, el segundo intento puede ser       ║
║     opuesto si el modelo indica baja efectividad del        ║
║      segundo intento al mismo lado (tendencia agotamiento)  ║
║   - Confirmación de patrón  "-1 valor "                     ║
║   - 2 intentos para ZONA (apuestas), 3 intentos para ML     ║
║   - Gestión Labouchère + marcador diario (win1/win2/loss)   ║
║   - Mensajes combinados : resolución + nueva señal          ║
║   - Telegram / HTTP API / self-ping / persistencia          ║
║   - Tendencia global basada en 20 giros                     ║
║   - 2º intento considera rebote (ALCISTA→BAJA/BAJISTA→ALTA) ║
║   - Interfaz web (/dashboard) con DOS gráficos (ALTOS y    ║
║     BAJOS) + líneas de soporte/resistencia y gestión        ║
║      Labouchère integrada                                   ║
╚══════════════════════════════════════════════════════════════╝
"""
import asyncio
import json
import logging
import os
import sqlite3
import sys
import time
from typing import Optional, Callable, Awaitable, List
import textwrap
import websockets
from aiohttp import web, ClientSession, ClientTimeout
try:
    from telebot.async_telebot import AsyncTeleBot
    TELEBOT_OK = True
except ImportError:
    AsyncTeleBot = None
    TELEBOT_OK = False

# ──────────────────────────────────────────────
# CONFIGURACIÓN
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

# ── Agente de RACHAS: señal permisiva cuando la misma zona sale N veces seguidas ──
ZONE_STREAK_MIN = int(os.environ.get("ZONE_STREAK_MIN", "4"))

# ── PREDICTOR DE TIEMPO DE ZONAS (estilo Spaceman 3x-5x) ──────────────────
# En vez de "horario", usamos número de ronda (spin). Cuando sale ALTA o BAJA
# se guarda el spin, se promedia el intervalo entre las últimas salidas de
# esa zona, y se predice en cuántos giros más debería volver.
TIMING_ZONE_HISTORY_MAX   = int(os.environ.get("TIMING_ZONE_HISTORY_MAX",   "15"))
TIMING_ZONE_SAMPLE_WINDOW = int(os.environ.get("TIMING_ZONE_SAMPLE_WINDOW", "5"))
TIMING_ZONE_PREALERT_MIN  = int(os.environ.get("TIMING_ZONE_PREALERT_MIN",  "2"))  # giros antes
TIMING_ZONE_PREALERT_MAX  = int(os.environ.get("TIMING_ZONE_PREALERT_MAX",  "4"))  # giros antes
TIMING_ZONE_ALERT_WINDOW  = int(os.environ.get("TIMING_ZONE_ALERT_WINDOW",  "2"))  # ventana ±
TIMING_ZONE_DEDUPE_ROUNDS = int(os.environ.get("TIMING_ZONE_DEDUPE_ROUNDS", "3"))
TIMING_ZONE_MIN_INTERVAL  = int(os.environ.get("TIMING_ZONE_MIN_INTERVAL",  "4"))  # intervalo mínimo razonable
TIMING_ZONE_MAX_INTERVAL  = int(os.environ.get("TIMING_ZONE_MAX_INTERVAL", "12"))  # intervalo máximo razonable
# Filtro de EMAs 20/50 (equivalente a precioSobreTresEMAs del HTML):
# solo dispara si el nivel de la zona está alineado con la tendencia larga.
# ALCISTA: nivel > EMA20 > EMA50  →  favorece ALTA
# BAJISTA: nivel < EMA20 < EMA50  →  favorece BAJA

# ── Labouchère (gestión de capital, de Roulette 1) ──
LABOUCHERE_BASE_AMOUNT = 500
LABOUCHERE_INITIAL_SEQUENCE = [1, 1, 1, 1, 1]

REAL_COLOR_MAP = {
    0:  "VERDE", 1:  "ROJO", 2:  "NEGRO", 3:  "ROJO", 4:  "NEGRO", 5:  "ROJO", 6:  "NEGRO",
    7:  "ROJO", 8:  "NEGRO", 9:  "ROJO", 10: "NEGRO", 11: "NEGRO", 12: "ROJO", 13: "NEGRO",
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
TABLE_NAME     = "Ruleta: Speed Roulette 2"   #  <-- CAMBIO SOLICITADO
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
    if number is None:
        return False
    if number == 0:
        # El cero se considera acierto (gana) para cualquier señal de zona.
        return True
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

EMA_LONG_FAST = 20
EMA_LONG_SLOW = 50

def ema_long_trend(level_history, fast=EMA_LONG_FAST, slow=EMA_LONG_SLOW):
    """Filtro de tendencia de largo plazo (EMA20 vs EMA50) sobre un historial
    de nivel (funciona tanto para el nivel de docenas como para el de zonas,
    ya que ambos son el mismo tipo de "paseo" numérico +1/-1).
    'bullish': nivel actual > EMA20 > EMA50 (favorece D2/D3 o ALTA).
    'bearish': nivel actual < EMA20 < EMA50 (favorece D1/D2 o BAJA).
    Devuelve None si todavía no hay suficiente historial (no se aplica el
    filtro en frío, para no bloquear el bot al arrancar)."""
    if len(level_history) < slow + 1:
        return None
    ema_fast = calc_ema(level_history, fast)
    ema_slow = calc_ema(level_history, slow)
    if not ema_fast or not ema_slow:
        return None
    ef, es = ema_fast[-1], ema_slow[-1]
    if ef is None or es is None:
        return None
    cur = level_history[-1]
    if cur > ef > es:
        return "bullish"
    if cur < ef < es:
        return "bearish"
    return "neutral"

def trend_favored_zones(trend):
    """Análogo a trend_favored_dozens pero para ALTA/BAJA. A diferencia de
    las docenas (3 categorías), acá 'neutral' o sin datos no restringe
    ninguna zona, ya que no hay una tercera opción intermedia."""
    if trend == "bullish":
        return {"ALTA"}
    if trend == "bearish":
        return {"BAJA"}
    return {"ALTA", "BAJA"}

# ══════════════════════════════════════════════
# LABOUCHÈRE MANAGER
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

def build_entry_message_zone(last_number, bet_zone, bet_amount=None, start_attempt=1, sequence_str: str = "",
                             timing_info=None) -> str:
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
    timing_line = ""
    if timing_info:
        timing_line = (f"\n⏰ TIMING: spin predicho ~{timing_info['spin_predicho']} "
                       f"(intervalo prom {timing_info['intervalo_prom']} giros)\n")
    link_line = f'🎮  <a href="{TABLE_LINK}">{TABLE_NAME}</a>' if TABLE_LINK else f"🎮 {TABLE_NAME}"
    return (f"👉 INGRESAR DESPUÉS: {numero} ({numero_emoji})\n"
            f"{zone_line}\n"
            f"{apuesta_line}\n"
            f"{timing_line}\n"
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
    zone_keys = ["zone_agent1", "zone_agent2", "zone_agent3", "zone_agent4", "zone_agent_streak", "zone_agent_timing"]
    lines = ["📊 ESTADÍSTICAS POR PATRÓN"]
    for key, table in server_state.tables.items():
        lines.append(f"🎲 Mesa {key} ({TABLE_NAME})")
        lab_state = table.labouchere.get_state()
        seq_str = ','.join(str(x) for x in lab_state['sequence'])
        sign = '+' if lab_state['balance'] >= 0 else '-'
        lines.append(f"💹 Labouchère | Acum: {sign}{format_cop(abs(lab_state['balance']))} | Sec: [{seq_str}] | Sig: {format_cop(lab_state['bet_amount'])} | Ciclos: {lab_state['cycles_completed']}")
        lines.append(f"🔄 Rebote actual: {table.last_rebound_direction}")
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
            rec_attempt_dir, rec_pct_dir = agente.overall_recommended_attempt_for_direction(table.last_rebound_direction)
            rec_dir_line = (f"🌊 Intento según rebote ({table.last_rebound_direction}): {rec_attempt_dir}"
                            + (f" ({rec_pct_dir}%)" if rec_pct_dir is not None else " (usando general, pocos datos por rebote)")
                            if rec_attempt_dir else "🌊 Intento según rebote: aún sin datos suficientes")
            if agente.trained:
                modelo_line = "🤖 Modelo: entrenado"
            else:
                modelo_line = f"🤖 Modelo: en entrenamiento ({agente.total_processed}/{ML_MIN_SIGNALS_TO_TRAIN} señales)"
            lines.append(f"{agente.label}\n✅ {won}  ❌ {lost}  🎯 {total}  📈 {rate}%  {estado}\n{modelo_line}\n{rec_line}\n{rec_dir_line}")
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
            rec_attempt_dir, rec_pct_dir = agente.overall_recommended_attempt_for_direction(table.last_rebound_direction)
            rec_dir_line = (f"🌊 Intento según rebote ({table.last_rebound_direction}): {rec_attempt_dir}"
                            + (f" ({rec_pct_dir}%)" if rec_pct_dir is not None else " (usando general, pocos datos por rebote)")
                            if rec_attempt_dir else "🌊 Intento según rebote: aún sin datos suficientes")
            if agente.trained:
                modelo_line = "🤖 Modelo: entrenado"
            else:
                modelo_line = f"🤖 Modelo: en entrenamiento ({agente.total_processed}/{ML_MIN_SIGNALS_TO_TRAIN} señales)"
            lines.append(f"{agente.label}\n✅ {won}  ❌ {lost}  🎯 {total}  📈 {rate}%  {estado}\n{modelo_line}\n{rec_line}\n{rec_dir_line}")
    return "\n\n".join(lines)

def _format_ago(timestamp: float) -> str:
    if not timestamp:
        return "nunca"
    delta = max(0, time.time() - timestamp)
    if delta < 60:
        return f"hace {int(delta)}s"
    if delta < 3600:
        return f"hace {int(delta // 60)}min"
    return f"hace {delta / 3600:.1f}h"

def _agent_ml_block(agente) -> str:
    total = agente.total_processed
    if not agente.trained:
        estado = f"⚪ en entrenamiento ({total}/{ML_MIN_SIGNALS_TO_TRAIN} señales)"
    else:
        estado = f"🟢 entrenado · actualizado {_format_ago(agente.last_train_ts)} · próx. reentrenamiento cada {ML_RETRAIN_INTERVAL_SECONDS // 60}min"
    snapshot = agente.trained_snapshot or {}
    patrones_con_datos = [(k, v) for k, v in snapshot.items() if len(v) >= DOZEN_MIN_SAMPLES_GATE]
    patrones_con_datos.sort(key=lambda kv: len(kv[1]), reverse=True)
    n_patrones = len(patrones_con_datos)
    n_total_patrones = len(snapshot)
    lines = [f"{agente.label}", estado, f"🧬 Patrones observados: {n_total_patrones} · con muestra suficiente (≥{DOZEN_MIN_SAMPLES_GATE}): {n_patrones}"]
    if patrones_con_datos:
        lines.append("🔝 Top patrones por muestra:")
        for key, arr in patrones_con_datos[:3]:
            c1 = sum(1 for e in arr if agente._entry_attempt(e) == 1)
            c2 = sum(1 for e in arr if agente._entry_attempt(e) == 2)
            win_rate = sum(1 for e in arr if agente._entry_attempt(e) > 0) / len(arr) * 100
            lines.append(f"   · {key}: {len(arr)} muestras · {win_rate:.1f}% acierto · int1={c1} int2={c2}")
    return "\n".join(lines)

def build_mlstatus_message(server_state) -> str:
    agent_keys = ["agent2", "agent3", "agent4", "agent6"]
    zone_keys = ["zone_agent1", "zone_agent2", "zone_agent3", "zone_agent4", "zone_agent_streak", "zone_agent_timing"]
    lines = ["🧠 ESTADO DEL MODELO (ML)"]
    for key, table in server_state.tables.items():
        lines.append(f"🎲 Mesa {key} ({TABLE_NAME})")
        lines.append("— Patrones de DOCENAS —")
        for akey in agent_keys:
            agente = getattr(table, akey, None)
            if agente is None:
                continue
            lines.append(_agent_ml_block(agente))
        lines.append("— Patrones de ZONAS —")
        for zkey in zone_keys:
            agente = getattr(table, zkey, None)
            if agente is None:
                continue
            lines.append(_agent_ml_block(agente))
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

    @bot.message_handler(commands=["mlstatus"])
    async def handle_mlstatus_command(message):
        if _server_state is None:
            await bot.reply_to(message, "⏳ El servidor todavía se está iniciando, intenta de nuevo en unos segundos.")
            return
        try:
            text = build_mlstatus_message(_server_state)
            # Telegram limita ~4096 caracteres por mensaje; se divide si hace falta.
            for i in range(0, len(text), 3800):
                await bot.reply_to(message, text[i:i + 3800])
        except Exception as e:
            log.warning(f"[Telegram] Error respondiendo /mlstatus: {e}")

# ──────────────────────────────────────────────
# DAILY MARKER
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
# AGENTE DE PATRÓN DE DOCENAS
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
            "rebound_direction": "NEUTRAL",
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
        self.last_rebound_direction = "NEUTRAL"

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

    @staticmethod
    def _entry_attempt(entry):
        """Compatibilidad: entradas antiguas son int (hit_attempt); las nuevas son dict {'a':.., 'r':..}."""
        return entry["a"] if isinstance(entry, dict) else entry

    @staticmethod
    def _entry_rebound(entry):
        return entry.get("r", "NEUTRAL") if isinstance(entry, dict) else "NEUTRAL"

    def _record_context(self, pattern, hit_attempt: int, rebound_direction: str = "NEUTRAL"):
        key = self._key(pattern)
        arr = self.pattern_context.setdefault(key, [])
        arr.append({"a": hit_attempt, "r": rebound_direction})
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
        return sum(1 for e in arr if self._entry_attempt(e) > 0) / len(arr)

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
        c1 = sum(1 for e in arr if self._entry_attempt(e) == 1)
        c2 = sum(1 for e in arr if self._entry_attempt(e) == 2)
        if c1 == 0 and c2 == 0:
            return None
        return 1 if c1 >= c2 else 2

    def overall_recommended_attempt(self):
        if not self.trained:
            return None, 0.0
        c1 = c2 = 0
        for arr in self.trained_snapshot.values():
            c1 += sum(1 for e in arr if self._entry_attempt(e) == 1)
            c2 += sum(1 for e in arr if self._entry_attempt(e) == 2)
        total = c1 + c2
        if total < DOZEN_MIN_SAMPLES_GATE:
            return None, 0.0
        if c1 >= c2:
            return 1, round(c1 / total * 100, 1)
        return 2, round(c2 / total * 100, 1)

    def _recommended_attempt_for_direction(self, pattern, rebound_direction):
        """Intento recomendado condicionado a la dirección de rebote actual, con
        fallback al recomendado general del patrón si no hay muestras suficientes."""
        if not self.trained:
            return None, 0.0
        arr = self.trained_snapshot.get(self._key(pattern), [])
        filtered = [self._entry_attempt(e) for e in arr if self._entry_rebound(e) == rebound_direction]
        if len(filtered) < DOZEN_MIN_SAMPLES_GATE:
            fallback = self._recommended_attempt(pattern)
            return fallback, None
        c1 = sum(1 for v in filtered if v == 1)
        c2 = sum(1 for v in filtered if v == 2)
        if c1 == 0 and c2 == 0:
            fallback = self._recommended_attempt(pattern)
            return fallback, None
        if c1 >= c2:
            return 1, round(c1 / len(filtered) * 100, 1)
        return 2, round(c2 / len(filtered) * 100, 1)

    def overall_recommended_attempt_for_direction(self, rebound_direction):
        """Igual que overall_recommended_attempt() pero solo con señales que ocurrieron
        con la misma dirección de rebote; si no hay datos suficientes, cae al general."""
        if not self.trained:
            return None, 0.0
        c1 = c2 = 0
        for arr in self.trained_snapshot.values():
            for e in arr:
                if self._entry_rebound(e) != rebound_direction:
                    continue
                v = self._entry_attempt(e)
                if v == 1: c1 += 1
                elif v == 2: c2 += 1
        total = c1 + c2
        if total < DOZEN_MIN_SAMPLES_GATE:
            return self.overall_recommended_attempt()
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
        values = [self._entry_attempt(e) for e in arr]
        filtered = [v for v in values if v != 1]
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
               live_enabled: bool = True, rebound_direction: str = "NEUTRAL"):
        self._last_raw_number = last_number
        self.live_enabled = live_enabled
        self.last_rebound_direction = rebound_direction
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
                rec_attempt_dir, rec_pct_dir = self._recommended_attempt_for_direction(pattern, rebound_direction)
                self.candidate_signal = {
                    "pattern": pattern,
                    "bet_dozens": bet_dozens,
                    "bet_zone": (zone,) if zone is not None else None,
                    "context": context,
                    "start_attempt": 1,
                    "amx_strength": amx_strength_val,
                    "score": self._win_rate(pattern) or 0.0,
                    "confirming": False,
                    "rebound_direction": rebound_direction,
                    "recommended_attempt_by_rebound": rec_attempt_dir,
                    "recommended_attempt_by_rebound_pct": rec_pct_dir,
                }
                log.info(f"✅ {self.name} confirmación correcta: {pattern} -> ZONA {zone if zone else 'a decidir (opuesto)'} | Rebote: {rebound_direction}")
                self.train_state = {
                    "active": True, "pattern": pattern, "bet_dozens": bet_dozens,
                    "bet_zone": zone,
                    "attempts_left": DOZEN_MAX_ATTEMPTS, "total_attempts": DOZEN_MAX_ATTEMPTS,
                    "context": context, "current_attempt": 0, "start_attempt": 1,
                    "rebound_direction": rebound_direction,
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
        self._record_context(pattern, hit_attempt, self.train_state.get("rebound_direction", "NEUTRAL"))
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
            "rebound_direction": "NEUTRAL",
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
            "rebound_direction": "NEUTRAL",
        }
        self.train_attempt_results = []

    def get_state(self):
        rec_attempt, rec_pct = self.overall_recommended_attempt()
        rec_attempt_dir, rec_pct_dir = self.overall_recommended_attempt_for_direction(self.last_rebound_direction)
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
            "rebound_direction": self.last_rebound_direction,
            "recommended_attempt_by_rebound": rec_attempt_dir,
            "recommended_attempt_by_rebound_pct": rec_pct_dir,
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
# AGENTE DE PATRÓN DE ZONAS
# ══════════════════════════════════════════════
class ZonePatternAgent:
    def __init__(self, pattern: str, name: str, label: str, daily_marker=None,
                 thread_signals=None, thread_stats=None):
        self.pattern = pattern
        self.pattern_len = len(pattern)
        self.name = name
        self.label = label
        self.daily_marker = daily_marker
        self.thread_signals = thread_signals if thread_signals is not None else THREAD_SIGNALS_ZONE
        self.thread_stats = thread_stats if thread_stats is not None else THREAD_STATS_ZONE
        self.letter_to_zone = {'a': 'BAJA', 'b': 'ALTA'}
        self.predicted_zone = self.letter_to_zone.get(pattern[-1]) if pattern[-1] in ('a','b') else None
        if self.predicted_zone is None:
            log.warning(f"El patrón {pattern} termina en '{pattern[-1]}', no se puede predecir zona. Se desactivará.")
            self.active = False
        else:
            self.active = True
        self.zero_proximity_threshold = 2
        self.train_state = {
            "active": False, "pattern": None, "bet_zone": None,
            "attempts_left": 0, "total_attempts": DOZEN_MAX_ATTEMPTS,
            "context": None, "current_attempt": 0, "start_attempt": 1,
            "rebound_direction": "NEUTRAL",
        }
        self.train_attempt_results = []
        self.live_enabled = True
        self.candidate_signal = None
        self.confirming = False
        self.pending_pattern = None
        self.pending_window = None
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
        self.last_rebound_direction = "NEUTRAL"

    def _match(self, window):
        if len(window) != self.pattern_len:
            return None
        expected = [self.letter_to_zone.get(ch) for ch in self.pattern]
        if any(e is None for e in expected):
            return None
        for w, e in zip(window, expected):
            if w == "VERDE":
                continue
            if w != e:
                return None
        return tuple(expected)

    def _match_partial(self, window):
        if len(window) != self.pattern_len - 1:
            return None
        expected_full = [self.letter_to_zone.get(ch) for ch in self.pattern]
        if any(e is None for e in expected_full):
            return None
        for w, e in zip(window, expected_full[:-1]):
            if w == "VERDE":
                continue
            if w != e:
                return None
        return tuple(expected_full)

    @staticmethod
    def _key(pattern_tuple):
        return ">".join(pattern_tuple)

    @staticmethod
    def _entry_attempt(entry):
        """Compatibilidad: entradas antiguas son int (hit_attempt); las nuevas son dict {'a':.., 'r':..}."""
        return entry["a"] if isinstance(entry, dict) else entry

    @staticmethod
    def _entry_rebound(entry):
        return entry.get("r", "NEUTRAL") if isinstance(entry, dict) else "NEUTRAL"

    def _record_context(self, pattern_tuple, hit_attempt: int, rebound_direction: str = "NEUTRAL"):
        key = self._key(pattern_tuple)
        arr = self.pattern_context.setdefault(key, [])
        arr.append({"a": hit_attempt, "r": rebound_direction})
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
        return sum(1 for e in arr if self._entry_attempt(e) > 0) / len(arr)

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
        c1 = sum(1 for e in arr if self._entry_attempt(e) == 1)
        c2 = sum(1 for e in arr if self._entry_attempt(e) == 2)
        if c1 == 0 and c2 == 0:
            return None
        return 1 if c1 >= c2 else 2

    def overall_recommended_attempt(self):
        if not self.trained:
            return None, 0.0
        c1 = c2 = 0
        for arr in self.trained_snapshot.values():
            c1 += sum(1 for e in arr if self._entry_attempt(e) == 1)
            c2 += sum(1 for e in arr if self._entry_attempt(e) == 2)
        total = c1 + c2
        if total < DOZEN_MIN_SAMPLES_GATE:
            return None, 0.0
        if c1 >= c2:
            return 1, round(c1 / total * 100, 1)
        return 2, round(c2 / total * 100, 1)

    def _recommended_attempt_for_direction(self, pattern_tuple, rebound_direction):
        """Intento recomendado condicionado a la dirección de rebote actual, con
        fallback al recomendado general del patrón si no hay muestras suficientes."""
        if not self.trained:
            return None, 0.0
        arr = self.trained_snapshot.get(self._key(pattern_tuple), [])
        filtered = [self._entry_attempt(e) for e in arr if self._entry_rebound(e) == rebound_direction]
        if len(filtered) < DOZEN_MIN_SAMPLES_GATE:
            fallback = self._recommended_attempt(pattern_tuple)
            return fallback, None
        c1 = sum(1 for v in filtered if v == 1)
        c2 = sum(1 for v in filtered if v == 2)
        if c1 == 0 and c2 == 0:
            fallback = self._recommended_attempt(pattern_tuple)
            return fallback, None
        if c1 >= c2:
            return 1, round(c1 / len(filtered) * 100, 1)
        return 2, round(c2 / len(filtered) * 100, 1)

    def overall_recommended_attempt_for_direction(self, rebound_direction):
        """Igual que overall_recommended_attempt() pero solo con señales que ocurrieron
        con la misma dirección de rebote; si no hay datos suficientes, cae al general."""
        if not self.trained:
            return None, 0.0
        c1 = c2 = 0
        for arr in self.trained_snapshot.values():
            for e in arr:
                if self._entry_rebound(e) != rebound_direction:
                    continue
                v = self._entry_attempt(e)
                if v == 1: c1 += 1
                elif v == 2: c2 += 1
        total = c1 + c2
        if total < DOZEN_MIN_SAMPLES_GATE:
            return self.overall_recommended_attempt()
        if c1 >= c2:
            return 1, round(c1 / total * 100, 1)
        return 2, round(c2 / total * 100, 1)

    def _ml_should_signal(self, pattern_tuple, amx_strength_val, trend_zones=None):
        if self.cooldown_remaining > 0:
            return False
        if trend_zones is not None:
            predicted_zone = pattern_tuple[-1]
            if predicted_zone not in trend_zones:
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
               live_enabled: bool = True, rebound_direction: str = "NEUTRAL",
               trend_zones=None):
        if not self.active:
            return
        self._last_raw_number = last_number
        self.live_enabled = live_enabled
        self.last_rebound_direction = rebound_direction
        self.candidate_signal = None
        if not zone_history:
            return
        last_zone = zone_history[-1]
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
        self.run_backtest(zone_history)
        if self.cooldown_remaining > 0:
            self.cooldown_remaining -= 1
        self._maybe_train(timestamp)
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
                if self._ml_should_signal(pattern_tuple, amx_strength_val, trend_zones):
                    self.confirming = True
                    self.pending_pattern = pattern_tuple
                    self.pending_window = zone_history[-(self.pattern_len - 1):]
                    self.candidate_signal = {
                        "pattern": pattern_tuple,
                        "confirming": True,
                        "expected_last": predicted_zone,
                        "amx_strength": amx_strength_val,
                    }
                    log.info(f"🔍 {self.name} confirmación pendiente: patrón {pattern_tuple} -> esperado {predicted_zone}")
                    return
        if self.confirming and self.pending_pattern:
            pattern_tuple = self.pending_pattern
            expected_last = pattern_tuple[-1]
            if last_zone == expected_last:
                predicted_zone = pattern_tuple[-1]
                if predicted_zone == "VERDE":
                    log.info(f"⛔ {self.name}: patrón confirmado pero termina en VERDE, no se genera señal")
                    self.confirming = False
                    self.pending_pattern = None
                    self.pending_window = None
                    return
                confirm_window = zone_history[-self.pattern_len:]
                zero_positions = [i for i, z in enumerate(confirm_window) if z == "VERDE"]
                near_zero = False
                if zero_positions:
                    closest_zero = max(zero_positions)
                    distance_from_end = self.pattern_len - 1 - closest_zero
                    if distance_from_end <= self.zero_proximity_threshold:
                        near_zero = True
                        log.info(f"🔄 {self.name}: cero cerca (distancia {distance_from_end} desde el final), se invertirá la secuencia")
                context = list(zone_history[-DOZEN_CONTEXT_WINDOW:])
                opposite = "ALTA" if predicted_zone == "BAJA" else "BAJA"
                if near_zero:
                    zone_sequence = [opposite, predicted_zone]
                else:
                    zone_sequence = [predicted_zone, opposite]
                rec_attempt_dir, rec_pct_dir = self._recommended_attempt_for_direction(pattern_tuple, rebound_direction)
                self.candidate_signal = {
                    "pattern": pattern_tuple,
                    "bet_zone": (predicted_zone,),
                    "zone_sequence": zone_sequence,
                    "context": context,
                    "start_attempt": 1,
                    "amx_strength": amx_strength_val,
                    "score": self._win_rate(pattern_tuple) or 0.0,
                    "confirming": False,
                    "near_zero": near_zero,
                    "rebound_direction": rebound_direction,
                    "recommended_attempt_by_rebound": rec_attempt_dir,
                    "recommended_attempt_by_rebound_pct": rec_pct_dir,
                }
                log.info(f"✅ {self.name} confirmación correcta: {pattern_tuple} -> ZONA {predicted_zone}, secuencia {zone_sequence} | Rebote: {rebound_direction}")
                self.train_state = {
                    "active": True, "pattern": pattern_tuple, "bet_zone": predicted_zone,
                    "attempts_left": DOZEN_MAX_ATTEMPTS, "total_attempts": DOZEN_MAX_ATTEMPTS,
                    "context": context, "current_attempt": 0, "start_attempt": 1,
                    "rebound_direction": rebound_direction,
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
        self._record_context(pattern_tuple, hit_attempt, self.train_state.get("rebound_direction", "NEUTRAL"))
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
            "rebound_direction": "NEUTRAL",
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
            "rebound_direction": "NEUTRAL",
        }
        self.train_attempt_results = []

    def get_state(self):
        rec_attempt, rec_pct = self.overall_recommended_attempt()
        rec_attempt_dir, rec_pct_dir = self.overall_recommended_attempt_for_direction(self.last_rebound_direction)
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
            "rebound_direction": self.last_rebound_direction,
            "recommended_attempt_by_rebound": rec_attempt_dir,
            "recommended_attempt_by_rebound_pct": rec_pct_dir,
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


def current_zone_streak(zone_history):
    """Devuelve (zona, largo) de la racha actual de la misma zona (VERDE corta la racha)."""
    streak = 0
    zone = None
    for z in reversed(zone_history):
        if z == "VERDE":
            break
        if zone is None:
            zone = z
            streak = 1
        elif z == zone:
            streak += 1
        else:
            break
    return zone, streak


class StreakZoneAgent:
    """Señal de RACHA: cuando la misma zona sale N veces seguidas, genera una
    señal permisiva para SEGUIR la racha aunque ningún patrón de docenas ni de
    zonas coincida. Sin confirmación (la racha es la confirmación) y sin gates
    de ML: la recency de la racha es el criterio. Los 2 intentos van a la zona
    de la racha."""
    def __init__(self, min_streak: int, name: str, label: str, daily_marker=None,
                 thread_signals=None, thread_stats=None):
        self.min_streak = min_streak
        self.name = name
        self.label = label
        self.daily_marker = daily_marker
        self.thread_signals = thread_signals if thread_signals is not None else THREAD_SIGNALS_ZONE
        self.thread_stats = thread_stats if thread_stats is not None else THREAD_STATS_ZONE
        self.train_state = {
            "active": False, "pattern": None, "bet_zone": None,
            "attempts_left": 0, "total_attempts": DOZEN_MAX_ATTEMPTS,
            "context": None, "current_attempt": 0, "start_attempt": 1,
            "rebound_direction": "NEUTRAL",
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
        self.last_rebound_direction = "NEUTRAL"

    @staticmethod
    def _key(zone):
        return f"RACHA_{zone}"

    def _record_context(self, zone, hit_attempt: int):
        arr = self.pattern_context.setdefault(self._key(zone), [])
        arr.append(hit_attempt)
        if len(arr) > DOZEN_CONTEXT_WINDOW:
            del arr[0]

    def _win_rate(self, pattern):
        if not self.trained:
            return None
        zone = pattern[1] if isinstance(pattern, (tuple, list)) and len(pattern) > 1 else None
        if zone is None:
            return None
        arr = self.trained_snapshot.get(self._key(zone), [])
        if len(arr) < DOZEN_MIN_SAMPLES_GATE:
            return None
        return sum(1 for v in arr if v > 0) / len(arr)

    def overall_recommended_attempt(self):
        return None, 0.0

    def _recommended_attempt_for_direction(self, pattern, rebound_direction):
        return None, 0.0

    def overall_recommended_attempt_for_direction(self, rebound_direction):
        return None, 0.0

    def force_train(self, timestamp: float):
        self.trained_snapshot = {k: list(v) for k, v in self.pattern_context.items()}
        self.trained = True
        self.last_train_ts = timestamp

    def _maybe_train(self, timestamp: float):
        if self.total_processed < ML_MIN_SIGNALS_TO_TRAIN:
            return
        if not self.trained or (timestamp - self.last_train_ts) >= ML_RETRAIN_INTERVAL_SECONDS:
            self.force_train(timestamp)

    def update(self, zone_history, timestamp, blocked: bool = False,
               amx_strength_val=0.0, rebound_direction="NEUTRAL",
               last_number=None, live_enabled: bool = True, trend_zones=None):
        self._last_raw_number = last_number
        self.live_enabled = live_enabled
        self.last_rebound_direction = rebound_direction
        self.candidate_signal = None
        if not zone_history:
            return
        # 1) Shadow tracking de una señal de racha en curso
        if self.train_state["active"]:
            self.train_state["current_attempt"] += 1
            attempt = self.train_state["start_attempt"] + self.train_state["current_attempt"] - 1
            is_win = zone_win(self.train_state["bet_zone"], last_number)
            self.train_attempt_results.append(last_number)
            if is_win:
                self._close_shadow(True, zone_history[-1], attempt, timestamp)
            else:
                self.train_state["attempts_left"] -= 1
                if self.train_state["attempts_left"] <= 0:
                    self._close_shadow(False, zone_history[-1], attempt, timestamp)
        if self.cooldown_remaining > 0:
            self.cooldown_remaining -= 1
        self._maybe_train(timestamp)
        # 2) Racha
        zone, streak = current_zone_streak(zone_history)
        if (zone is not None and streak >= self.min_streak
                and not self.train_state["active"] and not blocked
                and self.cooldown_remaining <= 0):
            if trend_zones is not None and zone not in trend_zones:
                log.info(f"⛔ {self.name}: racha de {streak}x {zone} en contra de la tendencia EMA20/50, se descarta")
                return
            rate = self._win_rate(("RACHA", zone))
            if rate is not None and rate < DOZEN_MIN_WIN_RATE:
                log.info(f"⛔ {self.name}: racha de {streak}x {zone} con tasa histórica {rate:.2f} < mínimo {DOZEN_MIN_WIN_RATE:.2f}, se descarta")
                return
            context = list(zone_history[-DOZEN_CONTEXT_WINDOW:])
            self.candidate_signal = {
                "pattern": ("RACHA", zone),
                "bet_zone": (zone,),
                "zone_sequence": [zone, zone],
                "context": context,
                "streak": streak,
                "amx_strength": 0.0,
                "score": round(min(0.60 + 0.08 * (streak - self.min_streak), 0.95), 3),
                "confirming": False,
                "is_streak": True,
                "rebound_direction": rebound_direction,
                "near_zero": False,
            }
            self.train_state = {
                "active": True, "pattern": ("RACHA", zone), "bet_zone": zone,
                "attempts_left": DOZEN_MAX_ATTEMPTS, "total_attempts": DOZEN_MAX_ATTEMPTS,
                "context": context, "current_attempt": 0, "start_attempt": 1,
                "rebound_direction": rebound_direction,
            }
            log.info(f"🔥 {self.name}: racha de {streak}x {zone} → señal (tasa hist.: {f'{rate:.2f}' if rate is not None else 'sin datos'})")

    def _close_shadow(self, win: bool, result_zone, attempt, timestamp):
        zone = self.train_state["bet_zone"]
        hit_attempt = attempt if win else 0
        self.history_counter += 1
        self.history_log.append({
            "n": self.history_counter, "pattern": f"RACHA_{zone}",
            "bet_zone": zone, "result": result_zone, "attempt": attempt,
            "win": win, "hit_attempt": hit_attempt,
            "context": self.train_state.get("context"), "time": timestamp, "shadow": True,
        })
        self.history_log = self.history_log[-200:]
        self.stats["total"] += 1
        self.stats["won" if win else "lost"] += 1
        self._record_context(zone, hit_attempt)
        self.total_processed += 1
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
            "rebound_direction": "NEUTRAL",
        }
        self.train_attempt_results = []

    def reset_transient(self):
        self.candidate_signal = None
        self.train_state = {
            "active": False, "pattern": None, "bet_zone": None,
            "attempts_left": 0, "total_attempts": DOZEN_MAX_ATTEMPTS,
            "context": None, "current_attempt": 0, "start_attempt": 1,
            "rebound_direction": "NEUTRAL",
        }
        self.train_attempt_results = []

    def get_state(self):
        return {
            "name": self.name,
            "pattern_len": self.min_streak,
            "pattern": f"racha>={self.min_streak}",
            "train_state": self.train_state,
            "stats": self.stats,
            "history": self.history_log[-30:],
            "backtest_60": self.backtest,
            "pattern_context": self.pattern_context,
            "consecutive_losses": self.consecutive_losses,
            "cooldown_remaining": self.cooldown_remaining,
            "recommended_attempt": None,
            "recommended_attempt_pct": 0.0,
            "rebound_direction": self.last_rebound_direction,
            "recommended_attempt_by_rebound": None,
            "recommended_attempt_by_rebound_pct": 0.0,
            "pattern_recommendations": {},
            "confirming": False,
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
# PREDICTOR DE TIEMPO DE ZONAS (estilo Spaceman)
# ══════════════════════════════════════════════
class ZoneTimingPredictor:
    """Registra el spin de cada salida de ALTA/BAJA, promedia el intervalo
    entre las últimas y predice en cuántos giros más debería volver esa zona.
    Al acercarse la ronda predicha, emite una señal que se suma al pool de
    candidatas (con filtro de EMAs 20/50 como confirmación)."""

    def __init__(self, name: str = "ZONE_TIMING", label: str = "⏰ TIMING ZONAS",
                 daily_marker=None, thread_signals=None, thread_stats=None):
        self.name = name
        self.label = label
        self.daily_marker = daily_marker
        self.thread_signals = thread_signals if thread_signals is not None else THREAD_SIGNALS_ZONE
        self.thread_stats = thread_stats if thread_stats is not None else THREAD_STATS_ZONE

        # Historial por zona: lista de spins en los que salió esa zona
        self.zone_spin_history = {"ALTA": [], "BAJA": []}
        # Predicciones vigentes
        self.recorded_times = []
        # Estado interno (compatibilidad con otros agentes)
        self.train_state = {
            "active": False, "pattern": None, "bet_zone": None,
            "attempts_left": 0, "total_attempts": DOZEN_MAX_ATTEMPTS,
            "context": None, "current_attempt": 0, "start_attempt": 1,
            "rebound_direction": "NEUTRAL",
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
        self.last_rebound_direction = "NEUTRAL"

    # ── Registro de cada giro ────────────────────────────────────────
    def register_spin(self, zone: str, spin_number: int):
        """Cada vez que sale ALTA o BAJA, guarda el spin y recalcula la
        próxima predicción para esa zona (promedio de las últimas)."""
        if zone not in ("ALTA", "BAJA"):
            return
        hist = self.zone_spin_history[zone]
        hist.append(spin_number)
        if len(hist) > TIMING_ZONE_HISTORY_MAX:
            hist.pop(0)
        if len(hist) < 2:
            return
        # Promedio de intervalos entre las últimas salidas
        ultimos = hist[-min(TIMING_ZONE_SAMPLE_WINDOW, len(hist)):]
        diffs = [ultimos[i] - ultimos[i-1] for i in range(1, len(ultimos))]
        if not diffs:
            return
        promedio = sum(diffs) / len(diffs)
        # Sanity: acotar el intervalo a un rango razonable
        if promedio < TIMING_ZONE_MIN_INTERVAL:
            promedio = TIMING_ZONE_MIN_INTERVAL
        if promedio > TIMING_ZONE_MAX_INTERVAL:
            promedio = TIMING_ZONE_MAX_INTERVAL
        spin_predicho = int(round(ultimos[-1] + promedio))
        # Dedupe: no registrar si ya existe una predicción muy cercana
        ya_existe = any(
            abs(r["spin_predicho"] - spin_predicho) < TIMING_ZONE_DEDUPE_ROUNDS
            and r["zona"] == zone
            for r in self.recorded_times
        )
        if ya_existe:
            return
        self.recorded_times.append({
            "spin_predicho": spin_predicho,
            "zona": zone,
            "pre_alert_shown": False,
            "alert_shown": False,
            "created_spin": spin_number,
            "fail_count": 0,
            "intervalo_prom": round(promedio, 2),
        })
        log.info(f"⏰ [{self.name}] 🔮 Predicción: zona {zone} en spin ~{spin_predicho} "
                 f"(intervalo promedio {promedio:.1f} giros)")

    # ── Limpieza de predicciones vencidas ────────────────────────────
    def cleanup(self, current_spin: int):
        vigentes = []
        for r in self.recorded_times:
            diff = r["spin_predicho"] - current_spin
            post_window = TIMING_ZONE_ALERT_WINDOW + r.get("fail_count", 0)
            if diff < -(post_window + 1):
                log.info(f"⏰ [{self.name}] ⌛ Predicción vencida: {r['zona']} "
                         f"(spin {r['spin_predicho']}, actual {current_spin})")
                continue
            vigentes.append(r)
        self.recorded_times = vigentes

    # ── Filtro de EMAs 20/50 (equivalente a precioSobreTresEMAs) ─────
    def ema_filter_ok(self, zone: str, alto_level_history, bajo_level_history) -> bool:
        """Solo dispara si la tendencia larga (EMA20 vs EMA50) favorece la
        zona predicha. Sin historial suficiente, no bloquea."""
        serie = alto_level_history if zone == "ALTA" else bajo_level_history
        trend = ema_long_trend(serie)
        if trend is None:
            return True  # sin datos, no bloquear
        if trend == "bullish" and zone == "ALTA":
            return True
        if trend == "bearish" and zone == "BAJA":
            return True
        # Neutral tampoco bloquea
        if trend == "neutral":
            return True
        return False

    # ── Evaluar si hay señal que disparar en este spin ───────────────
    def check_timing_trigger(self, current_spin: int,
                             alto_level_history, bajo_level_history,
                             zone_history, blocked: bool,
                             rebound_direction: str, last_number: int,
                             live_enabled: bool):
        """Llamado en cada giro. Si alguna predicción cae dentro de la
        ventana de prealerta/alerta Y el filtro de EMAs la confirma,
        arma un candidate_signal para competir con los demás agentes."""
        self.candidate_signal = None
        self._last_raw_number = last_number
        self.live_enabled = live_enabled
        self.last_rebound_direction = rebound_direction

        if blocked or not live_enabled:
            return
        if self.train_state["active"]:
            return  # ya hay una señal de timing en curso
        if self.cooldown_remaining > 0:
            return

        self.cleanup(current_spin)

        candidatos = []
        for r in self.recorded_times:
            diff = r["spin_predicho"] - current_spin
            fail_count = r.get("fail_count", 0)
            # Ventana dinámica: cada fallo reduce el preaviso y amplía el post
            min_before = max(0, TIMING_ZONE_PREALERT_MIN - fail_count)
            max_before = max(1, TIMING_ZONE_PREALERT_MAX - fail_count)
            post_window = TIMING_ZONE_ALERT_WINDOW + fail_count

            if not (-post_window <= diff <= max_before):
                continue

            # Filtro de EMAs 20/50
            if not self.ema_filter_ok(r["zona"], alto_level_history, bajo_level_history):
                log.info(f"⏰ [{self.name}] 🛑 {r['zona']} en spin {r['spin_predicho']} "
                         f"filtrada por EMA20/50 (tendencia no la favorece)")
                continue

            primera_vez = (not r["alert_shown"] and min_before <= diff <= max_before)
            reintento   = (r["alert_shown"] and diff >= -post_window)

            if primera_vez or reintento:
                candidatos.append((r, diff, primera_vez))

        if not candidatos:
            return

        # Elegir el candidato más cercano al spin predicho (menor |diff|)
        mejor_r, mejor_diff, era_primera = min(candidatos, key=lambda c: abs(c[1]))
        zona = mejor_r["zona"]
        context = list(zone_history[-DOZEN_CONTEXT_WINDOW:])

        # Score competitivo: crece cuanto más cerca estamos del spin predicho
        cercania_score = max(0.0, 1.0 - (abs(mejor_diff) / max(1, TIMING_ZONE_PREALERT_MAX)))
        score = round(0.55 + 0.30 * cercania_score, 3)

        self.candidate_signal = {
            "pattern": ("TIMING", zona),
            "bet_zone": (zona,),
            "zone_sequence": [zona, zona],  # ambos intentos a la misma zona
            "context": context,
            "start_attempt": 1,
            "amx_strength": 0.0,
            "score": score,
            "confirming": False,
            "is_timing": True,
            "rebound_direction": rebound_direction,
            "near_zero": False,
            "spin_predicho": mejor_r["spin_predicho"],
            "diff_actual": mejor_diff,
            "intervalo_prom": mejor_r.get("intervalo_prom", 0),
        }

        # Activar estado interno
        self.train_state = {
            "active": True, "pattern": ("TIMING", zona), "bet_zone": zona,
            "attempts_left": DOZEN_MAX_ATTEMPTS, "total_attempts": DOZEN_MAX_ATTEMPTS,
            "context": context, "current_attempt": 0, "start_attempt": 1,
            "rebound_direction": rebound_direction,
        }
        mejor_r["alert_shown"] = True

        log.info(f"⏰ [{self.name}] 🎯 SEÑAL timing: zona {zona} "
                 f"(spin predicho {mejor_r['spin_predicho']}, diff={mejor_diff:+d}, "
                 f"score={score:.2f}, intervalo_prom={mejor_r.get('intervalo_prom',0)})")

    # ── Shadow tracking / cierre ─────────────────────────────────────
    def _close_shadow(self, win: bool, result_zone, attempt, timestamp, last_number):
        hit_attempt = attempt if win else 0
        self.history_counter += 1
        self.history_log.append({
            "n": self.history_counter,
            "pattern": f"TIMING_{result_zone}",
            "bet_zone": self.train_state["bet_zone"],
            "result": result_zone, "attempt": attempt, "win": win,
            "hit_attempt": hit_attempt,
            "context": self.train_state.get("context"),
            "time": timestamp, "shadow": True,
        })
        self.history_log = self.history_log[-200:]
        self.stats["total"] += 1
        self.stats["won" if win else "lost"] += 1
        self.total_processed += 1

        if win:
            self.consecutive_losses = 0
        else:
            self.consecutive_losses += 1
            # Aumentar fail_count de la predicción que falló (ventana dinámica)
            for r in self.recorded_times:
                if r.get("alert_shown") and r["zona"] == self.train_state["bet_zone"]:
                    r["fail_count"] = r.get("fail_count", 0) + 1
                    log.info(f"⏰ [{self.name}] 🔼 fail_count={r['fail_count']} "
                             f"para predicción {r['zona']}@{r['spin_predicho']}")
                    break
            if self.consecutive_losses >= DOZEN_COOLDOWN_AFTER_LOSSES:
                self.cooldown_remaining = DOZEN_COOLDOWN_ROUNDS

        self.train_state = {
            "active": False, "pattern": None, "bet_zone": None,
            "attempts_left": 0, "total_attempts": DOZEN_MAX_ATTEMPTS,
            "context": None, "current_attempt": 0, "start_attempt": 1,
            "rebound_direction": "NEUTRAL",
        }
        self.train_attempt_results = []

    # ── Update (compatibilidad con el loop de agentes) ───────────────
    def update(self, zone_history, timestamp, blocked=False,
               amx_strength_val=0.0, rebound_direction="NEUTRAL",
               last_number=None, live_enabled=True, trend_zones=None,
               current_spin=None, alto_level_history=None,
               bajo_level_history=None):
        # 1) Registrar el spin si es ALTA/BAJA
        last_zone = zone_history[-1] if zone_history else None
        if last_zone in ("ALTA", "BAJA") and current_spin is not None:
            self.register_spin(last_zone, current_spin)

        # 2) Shadow tracking si hay señal activa
        if self.train_state["active"] and last_number is not None:
            self.train_state["current_attempt"] += 1
            attempt = (self.train_state["start_attempt"]
                       + self.train_state["current_attempt"] - 1)
            is_win = zone_win(self.train_state["bet_zone"], last_number)
            self.train_attempt_results.append(last_number)
            if is_win:
                self._close_shadow(True, last_zone, attempt, timestamp, last_number)
            elif self.train_state["attempts_left"] <= 1:
                self._close_shadow(False, last_zone, attempt, timestamp, last_number)
            else:
                self.train_state["attempts_left"] -= 1

        # 3) Evaluar trigger de timing
        if (not self.train_state["active"]
                and current_spin is not None
                and alto_level_history is not None
                and bajo_level_history is not None):
            self.check_timing_trigger(
                current_spin=current_spin,
                alto_level_history=alto_level_history,
                bajo_level_history=bajo_level_history,
                zone_history=zone_history,
                blocked=blocked,
                rebound_direction=rebound_direction,
                last_number=last_number,
                live_enabled=live_enabled,
            )

    # ── Stubs para compatibilidad con el loop general ────────────────
    def reset_transient(self):
        self.candidate_signal = None
        if self.train_state["active"]:
            self.train_state = {
                "active": False, "pattern": None, "bet_zone": None,
                "attempts_left": 0, "total_attempts": DOZEN_MAX_ATTEMPTS,
                "context": None, "current_attempt": 0, "start_attempt": 1,
                "rebound_direction": "NEUTRAL",
            }
        self.train_attempt_results = []

    def overall_recommended_attempt(self): return None, 0.0
    def overall_recommended_attempt_for_direction(self, rd): return None, 0.0

    @staticmethod
    def _entry_attempt(entry):
        return entry["a"] if isinstance(entry, dict) else entry

    @staticmethod
    def _entry_rebound(entry):
        return entry.get("r", "NEUTRAL") if isinstance(entry, dict) else "NEUTRAL"

    def get_state(self):
        return {
            "name": self.name,
            "pattern_len": 0,
            "pattern": "timing",
            "train_state": self.train_state,
            "stats": self.stats,
            "history": self.history_log[-30:],
            "backtest_60": self.backtest,
            "pattern_context": self.pattern_context,
            "consecutive_losses": self.consecutive_losses,
            "cooldown_remaining": self.cooldown_remaining,
            "recommended_attempt": None,
            "recommended_attempt_pct": 0.0,
            "rebound_direction": self.last_rebound_direction,
            "recommended_attempt_by_rebound": None,
            "recommended_attempt_by_rebound_pct": 0.0,
            "pattern_recommendations": {},
            "confirming": False,
            "live_enabled": self.live_enabled,
            "ml_model": {
                "trained": self.trained,
                "total_processed": self.total_processed,
                "min_signals_to_train": ML_MIN_SIGNALS_TO_TRAIN,
                "last_train_ts": self.last_train_ts,
                "retrain_interval_seconds": ML_RETRAIN_INTERVAL_SECONDS,
            },
            "timing_recorded": [
                {"spin": r["spin_predicho"], "zona": r["zona"],
                 "diff": r["spin_predicho"], "fail_count": r.get("fail_count", 0),
                 "intervalo": r.get("intervalo_prom", 0)}
                for r in self.recorded_times
            ],
            "timing_history": {z: list(h) for z, h in self.zone_spin_history.items()},
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
            "zone_spin_history": self.zone_spin_history,
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
        self.zone_spin_history = data.get("zone_spin_history", {"ALTA": [], "BAJA": []})


# ══════════════════════════════════════════════
# ROULETTE TABLE
# ══════════════════════════════════════════════
class RouletteTable:
    def __init__(self, key: int):
        self.key = key
        self.spin_history = []
        self.prev_number = None
        self.last_update_time = time.time()
        self.total_spins_seen = 0
        self.live_spins_seen = 0
        # Contador de giros (equivalente al "tiempo" en Spaceman)
        self.spin_counter = 0

        self.dozen_history = []
        self.zone_history = []
        self.daily_marker = DailyMarker()
        self.labouchere = LabouchereManager(base_amount=LABOUCHERE_BASE_AMOUNT)
        self.cycle_pending = 0
        self.zone_streak_event_times = {"ALTA": [], "BAJA": []}
        self._prev_zone_streak = (None, 0)
        self.time_due_info = {"ALTA": {"due": False, "avg_minutes": None, "elapsed_minutes": None},
                               "BAJA": {"due": False, "avg_minutes": None, "elapsed_minutes": None}}
        self.time_due_zones = set()
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
        self.last_signal_outcome = None   # "win" o "loss"
        self.last_signal_number = None
        self.attempt_log = []
        self.attempt_log_seq = 0
        # ── AGENTES DE DOCENAS ──
        self.agent2 = DozenPatternAgent(pattern_len=4, name="AGENTE_2", label="PATRON V2 💎 (aaba)", mode="aaba", daily_marker=self.daily_marker)
        self.agent3 = DozenPatternAgent(pattern_len=5, name="AGENTE_3", label="PATRON V3 💎 (aaaba)", mode="aaaba", daily_marker=self.daily_marker)
        self.agent4 = DozenPatternAgent(pattern_len=4, name="AGENTE_4", label="PATRON V4 💎 (abaa)", mode="abaa", daily_marker=self.daily_marker)
        self.agent6 = DozenPatternAgent(pattern_len=7, name="AGENTE_6", label="PATRON V6 💎 (aaaabaa)", mode="aaaabaa", daily_marker=self.daily_marker)
        # ── AGENTES DE ZONA ──
        self.zone_agent1 = ZonePatternAgent(pattern='baaaabbb', name="ZONE_AGENT_1", label="ZONA LARGA 1 (b+4a+3b)", daily_marker=self.daily_marker)
        self.zone_agent2 = ZonePatternAgent(pattern='aaaabbbbaa', name="ZONE_AGENT_2", label="ZONA LARGA 2 (4a+4b+2a)", daily_marker=self.daily_marker)
        self.zone_agent3 = ZonePatternAgent(pattern='aaabaa', name="ZONE_AGENT_3", label="ZONA V3 (aaabaa · repite a)", daily_marker=self.daily_marker)
        self.zone_agent4 = ZonePatternAgent(pattern='aaabbaa', name="ZONE_AGENT_4", label="ZONA V4 (aaabbaa · repite a)", daily_marker=self.daily_marker)
        self.zone_agent_streak = StreakZoneAgent(min_streak=ZONE_STREAK_MIN, name="ZONE_STREAK", label=f"🔥 RACHA (>={ZONE_STREAK_MIN}x misma zona)", daily_marker=self.daily_marker)
        # ── NUEVO: AGENTE DE TIMING DE ZONAS ──
        self.zone_agent_timing = ZoneTimingPredictor(
            name="ZONE_TIMING",
            label="⏰ TIMING ZONAS (predicción de giro)",
            daily_marker=self.daily_marker,
        )
        self.level_history = []
        self.level_current = 0
        self.last_dozen_num = None
        self.last_d2_number = None
        self.trend = "neutral"
        self.last_nonzero_zone = "BAJA"
        self.last_rebound_direction = "NEUTRAL"
        self.alto_level_history = []
        self.bajo_level_history = []
        self.zone_number_history = []

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

    async def _send_entry(self, agent, zone, bet_amount, attempt_number, timing_info=None):
        seq_txt = self.labouchere.seq_str()
        original = build_entry_message_zone(
            agent._last_raw_number,
            zone,
            bet_amount=bet_amount,
            sequence_str=seq_txt,
            timing_info=timing_info,
        )
        new_header = f"🚨🚨 ENTRADA INTENTO {attempt_number} 🚨🚨"
        entry_text = f"{new_header}\n\n{original}"
        msg_id = await send_msg(entry_text, agent.thread_signals)
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

    def _log_attempt_result(self, attempt: int, win: bool, number: int):
        self.attempt_log_seq += 1
        self.attempt_log.append({
            "seq": self.attempt_log_seq,
            "attempt": attempt,
            "win": bool(win),
            "number": number,
        })
        if len(self.attempt_log) > 50:
            self.attempt_log = self.attempt_log[-50:]

    def _finalize_sequence(self, win: bool, winning_attempt: int = None):
        b1 = self.attempt_bets[0] if len(self.attempt_bets) > 0 else 0
        b2 = self.attempt_bets[1] if len(self.attempt_bets) > 1 else 0
        if win and winning_attempt == 1:
            balance = b1
        elif win and winning_attempt == 2:
            balance = b2 - b1
        else:
            balance = -(b1 + b2)
        self.last_signal_outcome = "win" if win else "loss"
        self.last_signal_number = self.attempt_numbers[-1] if self.attempt_numbers else None
        extra_text = ""
        new_signal = self._pending_new_signal
        new_bet_amount = None
        if new_signal is not None:
            agent = new_signal["agent"]
            zone_sequence = new_signal["zone_sequence"]
            if self.current_attempt_index == 0:
                header = "🔥🔥 NUEVA SEÑAL CONFIRMADA 🔥🔥"
            else:
                header = "🔥🔥 REPETIR SEÑAL CONFIRMADA 🔥🔥"
            last_num = agent._last_raw_number
            zone = zone_sequence[0]
            new_bet_amount = self.labouchere.get_bet()
            timing_info = new_signal.get("timing_info")
            entry_body = build_entry_message_zone(last_num, zone, bet_amount=new_bet_amount,
                                                  timing_info=timing_info)
            extra_text = f"{header}\n\n{entry_body}"
            self._pending_new_signal = None
            self._signal_included = True
        asyncio.create_task(self._send_resolution(win, self.attempt_numbers, balance, extra_text))
        asyncio.create_task(self.daily_marker.record(win, winning_attempt))
        asyncio.create_task(self._send_daily_marker_and_cycle())
        self.current_attempt_index = 0
        self.attempt_numbers = []
        self.attempt_zones = []
        self.entry_msg_ids = []
        self.pending_agent = None
        self.pending_candidate = None
        self.confirming = False
        if self.confirmation_msg_id:
            asyncio.create_task(delete_msg(self.confirmation_msg_id))
            self.confirmation_msg_id = None
        if new_signal is not None:
            new_entry = {
                "agent": new_signal["agent"],
                "original": None,
                "zone_sequence": new_signal["zone_sequence"],
                "is_streak": bool(new_signal.get("is_streak")),
                "is_timing": bool(new_signal.get("is_timing")),
                "timing_info": new_signal.get("timing_info"),
            }
            self.signal_sequence = [new_entry]
            self.signal_status = "active"
            self.attempt_bets = [new_bet_amount if new_bet_amount is not None else self.labouchere.get_bet()]
            self.entry_msg_ids = [None]
        else:
            self.signal_sequence = []
            self.signal_status = None
            self.attempt_bets = []

    def _select_best_candidate(self, candidates):
        if not candidates:
            return None, None
        best_agent, best_score, best_candidate = max(candidates, key=lambda x: x[1])
        return best_agent, best_candidate

    REBOUND_FAVORED_ZONE = {"ALCISTA": "BAJA", "BAJISTA": "ALTA"}

    def _determine_zone_sequence(self, agent, candidate, bet_zone_tuple, amx_strength):
        rebound_dir = candidate.get("rebound_direction", "NEUTRAL")
        favored = self.REBOUND_FAVORED_ZONE.get(rebound_dir)
        if candidate.get("is_streak"):
            zone = bet_zone_tuple[0]
            opposite = "ALTA" if zone == "BAJA" else "BAJA"
            return [zone, opposite]
        # ↓ NUEVO: señales de TIMING se repiten en ambos intentos
        if candidate.get("is_timing"):
            zone = bet_zone_tuple[0]
            log.info(f"⏰ {agent.name}: señal de timing → misma zona {zone} en ambos intentos")
            return [zone, zone]
        if isinstance(agent, ZonePatternAgent):
            seq = list(candidate.get("zone_sequence") or [])
            if favored and len(seq) >= 2 and not candidate.get("near_zero"):
                if seq[1] != favored:
                    log.info(f"🌊 {agent.name}: rebote {rebound_dir} favorece {favored} → 2º intento a {favored}")
                seq[1] = favored
            return seq
        else:
            zone = bet_zone_tuple[0]
            if favored:
                log.info(f"🌊 {agent.name}: rebote {rebound_dir} favorece {favored} → 2º intento a {favored}")
                return [zone, favored]
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
        bet_zone_tuple = candidate.get("bet_zone")
        amx_strength = candidate.get("amx_strength", 0.0)
        if bet_zone_tuple is not None:
            zone_sequence = self._determine_zone_sequence(agent, candidate, bet_zone_tuple, amx_strength)
        else:
            last_zone = self.last_nonzero_zone
            opposite = "ALTA" if last_zone == "BAJA" else "BAJA"
            zone_sequence = [opposite, opposite]
            log.info(f"🔀 Señal D1+D3 → opuesto de última zona ({last_zone}) → {opposite} en ambos intentos")
        timing_info = None
        if candidate.get("is_timing"):
            timing_info = {
                "spin_predicho": candidate.get("spin_predicho"),
                "intervalo_prom": candidate.get("intervalo_prom"),
            }
        self._pending_new_signal = {
            "agent": agent,
            "zone_sequence": zone_sequence,
            "is_streak": bool(candidate.get("is_streak")),
            "is_timing": bool(candidate.get("is_timing")),
            "timing_info": timing_info,
        }
        agent.candidate_signal = None

    def _activate_new_signal(self, agent, candidate, bet_amount):
        bet_zone_tuple = candidate.get("bet_zone")
        amx_strength = candidate.get("amx_strength", 0.0)
        if bet_zone_tuple is not None:
            zone_sequence = self._determine_zone_sequence(agent, candidate, bet_zone_tuple, amx_strength)
        else:
            last_zone = self.last_nonzero_zone
            opposite = "ALTA" if last_zone == "BAJA" else "BAJA"
            zone_sequence = [opposite, opposite]
            log.info(f"🔀 Señal D1+D3 → opuesto de última zona ({last_zone}) → {opposite} en ambos intentos")
        timing_info = None
        if candidate.get("is_timing"):
            timing_info = {
                "spin_predicho": candidate.get("spin_predicho"),
                "intervalo_prom": candidate.get("intervalo_prom"),
            }
        new_entry = {
            "agent": agent,
            "original": candidate,
            "zone_sequence": zone_sequence,
            "is_streak": bool(candidate.get("is_streak")),
            "is_timing": bool(candidate.get("is_timing")),
            "timing_info": timing_info,
        }
        if self.signal_status == "waiting_pattern":
            self.signal_sequence = [new_entry]
            self.current_attempt_index = 1
            self.signal_status = "active"
            self.attempt_bets.append(bet_amount)
            asyncio.create_task(self._send_entry(agent, zone_sequence[1], bet_amount, 2,
                                                 timing_info=timing_info))
            log.info(f"🔔 NUEVO PATRÓN TRAS CERO -> INTENTO 2: {agent.name} -> ZONA {zone_sequence[1]}")
            return
        self.signal_sequence = [new_entry]
        self.current_attempt_index = 0
        self.signal_status = "active"
        self.attempt_numbers = []
        self.attempt_zones = []
        self.attempt_bets = [bet_amount]
        self.entry_msg_ids = []
        asyncio.create_task(self._send_entry(agent, zone_sequence[0], bet_amount, 1,
                                             timing_info=timing_info))
        log.info(f"🔔 SEÑAL INTENTO 1: {agent.name} -> ZONA {zone_sequence[0]}")
        agent.candidate_signal = None

    def _record_zone_streak_time_event(self, timestamp):
        zone, streak = current_zone_streak(self.zone_history)
        prev_zone, prev_streak = self._prev_zone_streak
        just_crossed = (zone is not None and streak >= ZONE_STREAK_MIN
                         and not (prev_zone == zone and prev_streak >= ZONE_STREAK_MIN))
        if just_crossed:
            log_list = self.zone_streak_event_times.setdefault(zone, [])
            log_list.append(timestamp)
            if len(log_list) > 30:
                del log_list[0]
        self._prev_zone_streak = (zone, streak)

    def _zone_time_due(self, zone, timestamp):
        events = self.zone_streak_event_times.get(zone, [])
        if len(events) < 2:
            return {"due": False, "avg_minutes": None, "elapsed_minutes": None}
        diffs = [(events[i] - events[i - 1]) / 60.0 for i in range(1, len(events))]
        avg_minutes = sum(diffs) / len(diffs)
        elapsed_minutes = (timestamp - events[-1]) / 60.0
        due = avg_minutes > 0 and elapsed_minutes >= avg_minutes
        return {"due": due, "avg_minutes": round(avg_minutes, 1), "elapsed_minutes": round(elapsed_minutes, 1)}

    def _handle_signal_sequence(self, all_agents, last_number, bet_amount):
        candidates = []
        confirmation_resolved = False
        new_confirming_agent = None
        open_for_signal = self.signal_status in (None, "waiting_pattern")
        for agente in all_agents:
            if self.confirming and agente is self.pending_agent and not agente.confirming:
                confirmation_resolved = True
            if agente.candidate_signal is None:
                continue
            if not agente.live_enabled:
                continue
            if agente.candidate_signal.get("confirming", False):
                if (open_for_signal and not self.confirming
                        and new_confirming_agent is None):
                    new_confirming_agent = agente
            else:
                bet_zone_tuple = agente.candidate_signal.get("bet_zone")
                bet_zone = bet_zone_tuple[0] if bet_zone_tuple is not None else None
                pattern = agente.candidate_signal.get("pattern")
                amx_str = agente.candidate_signal.get("amx_strength", 0.0)
                if agente.candidate_signal.get("is_streak"):
                    if bet_zone is None:
                        continue
                    score = agente.candidate_signal.get("score", 0.6)
                    candidates.append((agente, score, agente.candidate_signal))
                elif agente.candidate_signal.get("is_timing"):
                    # ↓ NUEVO: señales de TIMING compiten con su score propio
                    if bet_zone is None:
                        continue
                    score = agente.candidate_signal.get("score", 0.55)
                    candidates.append((agente, score, agente.candidate_signal))
                elif isinstance(agente, DozenPatternAgent):
                    if bet_zone is None:
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
        # Freno real por frecuencia horaria
        if candidates:
            filtered = []
            for agente, score, cand in candidates:
                # Las señales de timing NO se filtran por frecuencia horaria
                if cand.get("is_timing"):
                    filtered.append((agente, score, cand))
                    continue
                bz = cand.get("bet_zone")
                zone = bz[0] if bz else None
                info = self.time_due_info.get(zone) if zone else None
                if info is not None and info.get("avg_minutes") is not None and not info.get("due"):
                    log.info(f"⏳ {agente.name}: señal a {zone} esperando frecuencia horaria "
                             f"({info.get('elapsed_minutes')}min / {info.get('avg_minutes')}min prom.)")
                    continue
                if zone in self.time_due_zones:
                    cand["time_confirmed"] = True
                    cand["time_due_info"] = info
                    score = score * 1.25 + 0.05
                else:
                    cand["time_confirmed"] = False
                filtered.append((agente, score, cand))
            candidates = filtered
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
            is_win = zone_win(bet_zone, last_number)
            self.attempt_numbers.append(last_number if last_number is not None else 0)
            self.attempt_zones.append(bet_zone)
            cycle_completed = self.labouchere.update(is_win)
            if cycle_completed:
                self.cycle_pending = self.labouchere.cycles_completed
            self._log_attempt_result(self.current_attempt_index + 1, is_win, last_number if last_number is not None else 0)
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
                    self.attempt_bets.append(new_bet)
                    next_zone = zone_sequence[self.current_attempt_index] if self.current_attempt_index < len(zone_sequence) else zone_sequence[-1]
                    timing_info = current_entry.get("timing_info")
                    asyncio.create_task(self._send_entry(agent, next_zone, new_bet, self.current_attempt_index + 1,
                                                         timing_info=timing_info))
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
        if self.signal_status == "waiting_pattern" and candidates:
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
        self.spin_counter += 1   # ← NUEVO: contador de giros (tiempo)
        if not training:
            self.live_spins_seen += 1
        z = zone_of(number)
        self.zone_history.append(z)
        if len(self.zone_history) > 300: self.zone_history = self.zone_history[-300:]
        if number != 0:
            self.last_nonzero_zone = z
        last_alto = self.alto_level_history[-1] if self.alto_level_history else 0
        last_bajo = self.bajo_level_history[-1] if self.bajo_level_history else 0
        if number == 0:
            if self.last_nonzero_zone == "ALTA":
                self.alto_level_history.append(last_alto + 1)
                self.bajo_level_history.append(last_bajo - 1)
            elif self.last_nonzero_zone == "BAJA":
                self.alto_level_history.append(last_alto - 1)
                self.bajo_level_history.append(last_bajo + 1)
            else:
                self.alto_level_history.append(last_alto)
                self.bajo_level_history.append(last_bajo)
        else:
            self.alto_level_history.append(last_alto + (1 if z == "ALTA" else -1))
            self.bajo_level_history.append(last_bajo + (1 if z == "BAJA" else -1))
        if len(self.alto_level_history) > 300: self.alto_level_history.pop(0)
        if len(self.bajo_level_history) > 300: self.bajo_level_history.pop(0)
        self.zone_number_history.append(number)
        if len(self.zone_number_history) > 300: self.zone_number_history.pop(0)
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
        self.last_rebound_direction = detect_rebound_direction(self.level_history)
        long_trend_dozens = ema_long_trend(self.level_history)
        long_trend_zone = ema_long_trend(self.alto_level_history)
        zone_trend_favored = trend_favored_zones(long_trend_zone) if long_trend_zone is not None else None
        if not training:
            self._record_zone_streak_time_event(timestamp)
            self.time_due_info = {z: self._zone_time_due(z, timestamp) for z in ("ALTA", "BAJA")}
            self.time_due_zones = {z for z, info in self.time_due_info.items() if info["due"]}
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
            if long_trend_dozens is not None:
                favored = favored & trend_favored_dozens(long_trend_dozens)
            blocked = (self.signal_status not in (None, "waiting_pattern")) or self.confirming
            live_ok = (not training) and (self.live_spins_seen >= DOZEN_MIN_SPIN_TO_SIGNAL)
            agente.update(self.dozen_history, timestamp, blocked=blocked,
                          trend_dozens=favored, amx_strength_val=amx_strength_val,
                          last_number=number, live_enabled=live_ok,
                          rebound_direction=self.last_rebound_direction)
        # ↓ NUEVO: incluir al agente de timing en el loop
        zone_agents = [self.zone_agent1, self.zone_agent2, self.zone_agent3,
                       self.zone_agent4, self.zone_agent_streak, self.zone_agent_timing]
        for zagente in zone_agents:
            blocked = (self.signal_status not in (None, "waiting_pattern")) or self.confirming
            live_ok = (not training) and (self.live_spins_seen >= DOZEN_MIN_SPIN_TO_SIGNAL)
            amx_strength_val = 0.0
            zagente.update(self.zone_history, timestamp, blocked=blocked,
                           amx_strength_val=amx_strength_val,
                           rebound_direction=self.last_rebound_direction,
                           last_number=number, live_enabled=live_ok,
                           trend_zones=zone_trend_favored,
                           # ↓ Parámetros nuevos solo para el timing:
                           current_spin=self.spin_counter,
                           alto_level_history=self.alto_level_history,
                           bajo_level_history=self.bajo_level_history)
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
            f"Live spins: {self.live_spins_seen}/{DOZEN_MIN_SPIN_TO_SIGNAL} | Tendencia (20g): {self.trend} | "
            f"Rebote: {self.last_rebound_direction} | Última zona no nula: {self.last_nonzero_zone} | "
            f"Spin#: {self.spin_counter}"
        )

    def get_state(self, limit: int = 40):
        hist = self.spin_history[-limit:] if self.spin_history else []
        signal_zone = None
        signal_attempt = 0
        signal_zone_sequence = []
        if self.signal_status == "active" and self.signal_sequence:
            zone_seq = self.signal_sequence[0].get("zone_sequence", [])
            signal_zone_sequence = zone_seq
            if self.current_attempt_index < len(zone_seq):
                signal_zone = zone_seq[self.current_attempt_index]
            signal_attempt = self.current_attempt_index + 1
        elif self.signal_status == "waiting_pattern":
            signal_zone = None
            signal_attempt = 0
            signal_zone_sequence = []
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
            "zone_agent3": self.zone_agent3.get_state(),
            "zone_agent4": self.zone_agent4.get_state(),
            "zone_agent_streak": self.zone_agent_streak.get_state(),
            "zone_agent_timing": self.zone_agent_timing.get_state(),
            "trend": self.trend,
            "trend_favored_dozens": sorted(NUM_DOZEN[d] for d in trend_favored_dozens(self.trend)),
            "rebound_direction": self.last_rebound_direction,
            "level_current": self.level_current,
            "level_history": self.level_history,
            "labouchere": self.labouchere.get_state(),
            "live_spins_seen": self.live_spins_seen,
            "total_spins_seen": self.total_spins_seen,
            "spin_counter": self.spin_counter,
            "signal_status": self.signal_status,
            "signal_active": self.signal_status in ("active", "waiting_pattern"),
            "signal_zone": signal_zone,
            "signal_attempt": signal_attempt,
            "signal_total_attempts": ZONE_MAX_ATTEMPTS,
            "signal_zone_sequence": signal_zone_sequence,
            "last_signal_outcome": self.last_signal_outcome,
            "last_signal_number": self.last_signal_number,
            "attempt_log": self.attempt_log[-20:],
            "attempt_log_seq": self.attempt_log_seq,
            "current_attempt": self.current_attempt_index + 1 if self.signal_status == "active" else 0,
            "total_attempts": ZONE_MAX_ATTEMPTS if self.signal_status == "active" else 0,
            "last_nonzero_zone": self.last_nonzero_zone,
            "time_due_info": self.time_due_info,
        }


# ══════════════════════════════════════════════
# ANÁLISIS DE SOPORTE / RESISTENCIA
# ══════════════════════════════════════════════
def detect_pivots(level_history, lookback=60, pivot_window=3):
    if len(level_history) < lookback:
        lookback = len(level_history)
    data = level_history[-lookback:]
    start_idx = len(level_history) - lookback
    peaks = []
    valleys = []
    for i in range(pivot_window, len(data) - pivot_window):
        if data[i] > max(data[i-pivot_window:i]) and data[i] >= max(data[i+1:i+pivot_window+1]):
            peaks.append((start_idx + i, data[i]))
        if data[i] < min(data[i-pivot_window:i]) and data[i] <= min(data[i+1:i+pivot_window+1]):
            valleys.append((start_idx + i, data[i]))
    return peaks, valleys

def cluster_levels(points, threshold=1.0):
    if not points:
        return []
    sorted_points = sorted(points, key=lambda x: x[1])
    clusters = []
    current_cluster = [sorted_points[0]]
    for p in sorted_points[1:]:
        if abs(p[1] - current_cluster[-1][1]) <= threshold:
            current_cluster.append(p)
        else:
            values = [v for _, v in current_cluster]
            avg_value = sum(values) / len(values)
            last_idx = max(i for i, _ in current_cluster)
            clusters.append({
                "level": round(avg_value, 2),
                "frequency": len(current_cluster),
                "last_index": last_idx,
                "points": [(i, v) for i, v in current_cluster]
            })
            current_cluster = [p]
    if current_cluster:
        values = [v for _, v in current_cluster]
        avg_value = sum(values) / len(values)
        last_idx = max(i for i, _ in current_cluster)
        clusters.append({
            "level": round(avg_value, 2),
            "frequency": len(current_cluster),
            "last_index": last_idx,
            "points": [(i, v) for i, v in current_cluster]
        })
    return clusters

def detect_rebound_direction(level_history, lookback=40, pivot_window=3,
                             cluster_threshold=1.0, near_distance=1.5, confirm_span=3):
    if len(level_history) < (pivot_window * 2 + confirm_span + 2):
        return "NEUTRAL"
    eff_lookback = min(lookback, len(level_history))
    peaks, valleys = detect_pivots(level_history, lookback=eff_lookback, pivot_window=pivot_window)
    if not peaks and not valleys:
        return "NEUTRAL"
    support_clusters = cluster_levels(valleys, threshold=cluster_threshold)
    resistance_clusters = cluster_levels(peaks, threshold=cluster_threshold)
    current_level = level_history[-1]
    recent = level_history[-(confirm_span + 1):]
    short_dir = recent[-1] - recent[0] if len(recent) >= 2 else 0
    nearest_support = min(support_clusters, key=lambda c: abs(c["level"] - current_level)) if support_clusters else None
    nearest_resistance = min(resistance_clusters, key=lambda c: abs(c["level"] - current_level)) if resistance_clusters else None
    dist_support = abs(current_level - nearest_support["level"]) if nearest_support else None
    dist_resistance = abs(current_level - nearest_resistance["level"]) if nearest_resistance else None
    bounced_up = dist_support is not None and dist_support <= near_distance and short_dir > 0
    bounced_down = dist_resistance is not None and dist_resistance <= near_distance and short_dir < 0
    if bounced_up and bounced_down:
        return "ALCISTA" if dist_support <= dist_resistance else "BAJISTA"
    if bounced_up:
        return "ALCISTA"
    if bounced_down:
        return "BAJISTA"
    return "NEUTRAL"

def _zone_analysis_payload(level_history, lookback, number_history=None):
    levels = level_history[-lookback:] if len(level_history) >= lookback else level_history
    start_idx = len(level_history) - len(levels)
    peaks, valleys = detect_pivots(level_history, lookback=lookback, pivot_window=3)
    supports = cluster_levels(valleys, threshold=1.0)
    resistances = cluster_levels(peaks, threshold=1.0)
    numbers_slice = []
    if number_history:
        numbers_slice = number_history[-lookback:] if len(number_history) >= lookback else number_history
        if len(numbers_slice) != len(levels):
            numbers_slice = numbers_slice[-len(levels):] if len(numbers_slice) > len(levels) else numbers_slice
    return {
        "level_data": [{"index": start_idx + i, "value": v} for i, v in enumerate(levels)],
        "numbers": numbers_slice,
        "peaks": [{"index": i, "value": v} for i, v in peaks],
        "valleys": [{"index": i, "value": v} for i, v in valleys],
        "support_levels": supports,
        "resistance_levels": resistances,
        "current_level": levels[-1] if levels else 0,
    }

async def http_analysis_zones(request: web.Request):
    if _server_state is None:
        return web.json_response({"error": "server not ready"}, status=503)
    try:
        mesa = int(request.match_info["mesa"])
    except (KeyError, ValueError):
        return web.json_response({"error": "mesa inválida"}, status=400)
    if mesa not in ROULETTE_KEYS.values():
        return web.json_response({"error": "mesa no soportada"}, status=404)
    table = _server_state.tables.get(mesa)
    if table is None:
        return web.json_response({"error": "mesa no encontrada"}, status=404)
    try:
        lookback = int(request.query.get("lookback", 60))
    except ValueError:
        lookback = 60
    lookback = max(20, min(300, lookback))
    return web.json_response({
        "alto": _zone_analysis_payload(table.alto_level_history, lookback, table.zone_number_history),
        "bajo": _zone_analysis_payload(table.bajo_level_history, lookback, table.zone_number_history),
    })

async def http_analysis(request: web.Request):
    global _server_state
    if _server_state is None:
        return web.json_response({"error": "server not ready"}, status=503)
    try:
        mesa = int(request.match_info["mesa"])
    except (KeyError, ValueError):
        return web.json_response({"error": "mesa inválida"}, status=400)
    if mesa not in ROULETTE_KEYS.values():
        return web.json_response({"error": "mesa no soportada"}, status=404)
    table = _server_state.tables.get(mesa)
    if table is None:
        return web.json_response({"error": "mesa no encontrada"}, status=404)
    try:
        lookback = int(request.query.get("lookback", 60))
    except ValueError:
        lookback = 60
    lookback = max(20, min(300, lookback))
    level_history = table.level_history
    levels = level_history[-lookback:] if len(level_history) >= lookback else level_history
    start_idx = len(level_history) - len(levels)
    peaks, valleys = detect_pivots(level_history, lookback=lookback, pivot_window=3)
    support_clusters = cluster_levels(valleys, threshold=1.0)
    resistance_clusters = cluster_levels(peaks, threshold=1.0)
    spins = [s["number"] for s in table.spin_history[-lookback:]] if table.spin_history else []
    response = {
        "level_data": [{"index": start_idx + i, "value": v} for i, v in enumerate(levels)],
        "spins": spins,
        "peaks": [{"index": i, "value": v} for i, v in peaks],
        "valleys": [{"index": i, "value": v} for i, v in valleys],
        "support_levels": support_clusters,
        "resistance_levels": resistance_clusters,
        "last_spin": table.prev_number if table.prev_number is not None else None,
        "current_level": levels[-1] if levels else 0,
    }
    return web.json_response(response)


# ══════════════════════════════════════════════
# DASHBOARD HTML (se omite por brevedad, igual al original)
# ══════════════════════════════════════════════
DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=3.0, user-scalable=yes">
<title>Sistema de Zonas · ALTOS/BAJOS · Soporte/Resistencia</title>
</head>
<body>
<h1>Dashboard de Zonas (ver archivo original)</h1>
</body>
</html>
""".replace("{mesa_key}", str(list(ROULETTE_KEYS.values())[0]))


# ══════════════════════════════════════════════
# HTTP APP
# ══════════════════════════════════════════════
_server_state: Optional["ServerState"] = None

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
    try:
        limit = int(request.query.get("limit", 40))
    except ValueError:
        limit = 40
    limit = max(20, min(300, limit))
    table = _server_state.tables.get(mesa)
    if table is None:
        return web.json_response({"error": "mesa no encontrada"}, status=404)
    state = table.get_state(limit=limit)
    return web.json_response(state)

async def http_api_all(request: web.Request):
    if _server_state is None:
        return web.json_response({"error": "server not ready"}, status=503)
    result = {str(key): _server_state.get_state_for_mesa(key) for key in ROULETTE_KEYS.values()}
    return web.json_response(result)

async def http_dashboard(request: web.Request):
    return web.Response(text=DASHBOARD_HTML, content_type="text/html")

def build_http_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/ping", http_ping)
    app.router.add_get("/health", http_health)
    app.router.add_get("/api/state/{mesa}", http_api_state)
    app.router.add_get("/api/all", http_api_all)
    app.router.add_get("/api/analysis/{mesa}", http_analysis)
    app.router.add_get("/api/analysis_zones/{mesa}", http_analysis_zones)
    app.router.add_get("/dashboard", http_dashboard)
    app.router.add_get("/", http_dashboard)
    return app


# ══════════════════════════════════════════════
# WEBSOCKET HANDLER
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
# ENTRENAMIENTO CON HISTORIAL
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
                  table.zone_agent1, table.zone_agent2, table.zone_agent3, table.zone_agent4,
                  table.zone_agent_streak, table.zone_agent_timing]
        for agent in agents:
            agent.force_train(timestamp)
        log.info(f"[Entrenamiento] Mesa {table.key}: entrenamiento forzado tras bloque {start//BATCH_SIZE + 1}")
        await asyncio.sleep(0.1)
    for agent in [table.agent2, table.agent3, table.agent4, table.agent6,
                  table.zone_agent1, table.zone_agent2, table.zone_agent3, table.zone_agent4,
                  table.zone_agent_streak, table.zone_agent_timing]:
        agent.force_train(timestamp)
        agent.reset_transient()
    log.info(
        f"[Entrenamiento] Mesa {table.key}: listo. giros_vistos={table.total_spins_seen} "
        f"nivel={table.level_current}"
    )


# ══════════════════════════════════════════════
# SERVER STATE
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
                table.zone_agent3.load_persist(data.get("zone_agent3"))
                table.zone_agent4.load_persist(data.get("zone_agent4"))
                table.zone_agent_streak.load_persist(data.get("zone_agent_streak"))
                table.zone_agent_timing.load_persist(data.get("zone_agent_timing"))
                table.total_spins_seen = data.get("table_total_spins_seen", table.total_spins_seen)
                table.spin_counter = data.get("table_spin_counter", table.spin_counter)
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
            "zone_agent3": table.zone_agent3.to_persist(),
            "zone_agent4": table.zone_agent4.to_persist(),
            "zone_agent_streak": table.zone_agent_streak.to_persist(),
            "zone_agent_timing": table.zone_agent_timing.to_persist(),
            "table_total_spins_seen": table.total_spins_seen,
            "table_spin_counter": table.spin_counter,
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
# SELF-PING Y BOT POLLING
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
# MAIN
# ══════════════════════════════════════════════
async def main():
    global _server_state
    log.info("═" * 60)
    log.info("BOT UNIFICADO — DOCENAS → ZONAS + TIMING | SPEED ROULETTE 2")
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
    log.info(f"Servidor HTTP escuchando en puerto {port} (API: /ping, /health, /api/state/205, /api/all, /dashboard)")
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
