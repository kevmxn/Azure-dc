"""
╔══════════════════════════════════════════════════════════════╗
║   BOT DE SEÑALES POR SESSO DE CRUPIER — SPEED ROULETTE        ║
║                                                               ║
║   Estrategia (nueva, sin ML):                                 ║
║   - Cada crupier se identifica a mano con /crupier <nombre>.  ║
║     Al cambiar de crupier se archiva la sesión anterior y se  ║
║     empieza una nueva; todo queda registrado por crupier.     ║
║   - En cada giro se analizan las últimas RONDAS_ANALISIS (34) ║
║     rondas de la sesión ACTUAL del crupier.                   ║
║   - La rueda se divide en 5 sectores adaptativos:             ║
║       S1 = 13 números (centro = predicción, 6 izq + 6 der)    ║
║       S2..S5 = 6 números cada uno, en sentido HORARIO de S1   ║
║     S1 se recalcula en CADA giro (el centro es el número cuya ║
║     ventana de 13 acumula más caídas en las últimas 34).      ║
║   - Se evalúa si enviar señal: aciertos de S1 sobre lo        ║
║     esperado + confirmación con el histórico de SESIONES      ║
║     PASADAS del mismo crupier (similitud coseno de la         ║
║     distribución por posición de rueda). Si la sesión actual  ║
║     discrepa de las pasadas del mismo crupier, la señal sale  ║
║     marcada como SIN CONFIRMACIÓN.                            ║
║   - Gestión de banca FIBONACCI (1,1,2,3,5,8,13,21): gana 1   ║
║     vez y la gestión resetea. Hasta 8 intentos por señal;     ║
║     en cada intento se recalcula S1.                          ║
║   - HTTP: /ping, /health, /api/state/{mesa}, /api/all         ║
║   - Persistencia en model_<key>.json + self-ping (Render)     ║
╚══════════════════════════════════════════════════════════════╝
"""

import asyncio
import json
import logging
import math
import os
import sqlite3
import sys
import time
from typing import Optional, Callable, Awaitable

import websockets
from aiohttp import web, ClientSession, ClientTimeout

try:
    from telebot.async_telebot import AsyncTeleBot
    from telebot.types import BotCommand
    TELEBOT_OK = True
except ImportError:
    AsyncTeleBot = None
    BotCommand = None
    TELEBOT_OK = False

# ──────────────────────────────────────────────
#  CONFIGURACIÓN
# ──────────────────────────────────────────────
WS_URL        = "wss://dga.pragmaticplaylive.net/ws"
CASINO_ID     = "ppcdk00000005349"
CURRENCY_ID   = "BRL"
PING_INTERVAL = 240
SAVE_INTERVAL = 30

ROULETTE_KEYS = {203: 203}   # Roulette 2 Extra Time

REAL_COLOR_MAP = {
    0: "VERDE", 1: "ROJO", 2: "NEGRO", 3: "ROJO", 4: "NEGRO", 5: "ROJO", 6: "NEGRO",
    7: "ROJO", 8: "NEGRO", 9: "ROJO", 10: "NEGRO", 11: "NEGRO", 12: "ROJO", 13: "NEGRO",
    14: "ROJO", 15: "NEGRO", 16: "ROJO", 17: "NEGRO", 18: "ROJO", 19: "ROJO", 20: "NEGRO",
    21: "ROJO", 22: "NEGRO", 23: "ROJO", 24: "NEGRO", 25: "ROJO", 26: "NEGRO", 27: "ROJO",
    28: "NEGRO", 29: "NEGRO", 30: "ROJO", 31: "NEGRO", 32: "ROJO", 33: "NEGRO", 34: "ROJO",
    35: "NEGRO", 36: "ROJO"
}

# ── Telegram ──
BOT_TOKEN       = os.environ.get("BOT_TOKEN", "8347707121:AAH1cPEDMLbm-scTJ8mUuufeEhzw3Axv2Lw")
CHANNEL_SIGNALS = int(os.environ.get("CHANNEL_SIGNALS", "-1004228660174"))
TABLE_LINK      = os.environ.get("TABLE_LINK", "https://1win.com/es-MX/casino/play/v_pragmatic:speedroulette1")
TABLE_NAME      = "Speed Roulette 1"

HISTORY_SEED_PATH  = os.environ.get("HISTORY_SEED_PATH", "russian-azure.db")
HISTORY_SEED_TABLE = os.environ.get("HISTORY_SEED_TABLE", "roulette_1")

# ── Estrategia: sesgo por crupier + sectores S1..S6 ──
RONDAS_ANALISIS      = 34     # rondas que miramos hacia atrás en la sesión del crupier
S1_RADIO             = 6      # S1 = centro ± 6 (13 números: 6 izq + centro + 6 der)
SECTORES_EXTRA       = 4      # S2..S5
SECTOR_EXTRA_TAM     = 6      # 6 números cada uno (13 + 4*6 = 37)
SESGO_MIN_RONDAS     = int(os.environ.get("SESGO_MIN_RONDAS", "20"))   # mínimo de rondas en la sesión para evaluar
SESGO_MIN_ACIERTOS_S1 = int(os.environ.get("SESGO_MIN_ACIERTOS_S1", "14"))  # aciertos mínimos de S1 en las últimas 34 (esperado al azar ≈ 11.9 con 13 números)
SESGO_SIM_MIN        = float(os.environ.get("SESGO_SIM_MIN", "0.35"))  # similitud mínima vs sesiones pasadas del crupier
# Gestión Fibonacci: multiplicador de ficha por intento. Ganar 1 vez resetea la gestión.
SESGO_FIB            = [1, 1, 2, 3, 5, 8, 13, 21]
SESGO_MAX_INTENTOS   = int(os.environ.get("SESGO_MAX_INTENTOS", str(len(SESGO_FIB))))

def fib_mult(intento: int) -> int:
    return SESGO_FIB[min(intento - 1, len(SESGO_FIB) - 1)]
SESGO_STATS_WINDOW   = 50
SESGO_SEND_MIN_SAMPLES  = int(os.environ.get("SESGO_SEND_MIN_SAMPLES", "30"))
SESGO_SEND_MIN_WIN_RATE = float(os.environ.get("SESGO_SEND_MIN_WIN_RATE", "0.80"))
SESGO_CHIP_VALUE     = int(os.environ.get("SESGO_CHIP_VALUE", "50"))   # COP por número
CRUPIER_SEED_NAME    = os.environ.get("CRUPIER_SEED_NAME", "HISTORICO")
SEED_SPIN_CAP        = 2000   # el seed histórico entra como SESIÓN de referencia de ese pseudo-crupier
MAX_SESIONES_X_CRUPIER = 40   # sesiones archivadas por crupier que se guardan
MAX_SPINS_SESION_MEMORIA = 3000
CANAL_SENALES        = int(os.environ.get("CANAL_SENALES", str(CHANNEL_SIGNALS)))

# ──────────────────────────────────────────────
#  LOGGING
# ──────────────────────────────────────────────
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s | %(levelname)s | %(message)s",
                    datefmt="%H:%M:%S", handlers=[logging.StreamHandler(sys.stdout)])
log = logging.getLogger(__name__)


def color_of(n):
    return REAL_COLOR_MAP.get(n, "VERDE")


_server_state: Optional["ServerState"] = None


# ══════════════════════════════════════════════
#  TELEGRAM
# ══════════════════════════════════════════════
bot = AsyncTeleBot(BOT_TOKEN, parse_mode="HTML") if (TELEBOT_OK and BOT_TOKEN) else None
if bot is None:
    log.warning("Telegram deshabilitado (falta BOT_TOKEN o la librería 'telebot').")

async def send_msg(text: str, chat_id: int, retries: int = 3) -> Optional[int]:
    if bot is None:
        return None
    delay = 1.0
    for attempt in range(1, retries + 1):
        try:
            msg = await bot.send_message(chat_id=chat_id, text=text, parse_mode="HTML",
                                         disable_web_page_preview=True)
            return msg.message_id
        except Exception as e:
            retry_after = None
            try:
                retry_after = e.result_json.get("parameters", {}).get("retry_after")
            except Exception:
                pass
            wait = retry_after if retry_after else delay
            if attempt < retries:
                log.warning(f"[Telegram] Error enviando mensaje (chat={chat_id}, intento {attempt}/{retries}): {e} -> reintentando en {wait}s")
                await asyncio.sleep(wait)
                delay *= 2
            else:
                log.error(f"[Telegram] Fallo definitivo enviando mensaje (chat={chat_id}) tras {retries} intentos: {e}")
                return None

async def edit_msg(msg_id: int, text: str, chat_id: int = CANAL_SENALES) -> bool:
    if bot is None or msg_id is None:
        return False
    try:
        await bot.edit_message_text(chat_id=chat_id, message_id=msg_id, text=text,
                                    parse_mode="HTML", disable_web_page_preview=True)
        return True
    except Exception as e:
        log.debug(f"[Telegram] Error editando mensaje {msg_id}: {e}")
        return False

async def delete_msg(msg_id: int, chat_id: int = CANAL_SENALES) -> bool:
    if bot is None or msg_id is None:
        return False
    try:
        await bot.delete_message(chat_id=chat_id, message_id=msg_id)
        return True
    except Exception as e:
        log.debug(f"[Telegram] Error eliminando mensaje {msg_id}: {e}")
        return False


def build_status_message(server_state) -> str:
    lineas = ["🧑‍💼 SESGO POR CRUPIER — sectores S1..S6 (últimas 34 rondas)"]
    for key, mesa in server_state.tables.items():
        a = mesa.senal_activa
        lineas.append(f"\n🎲 Mesa {key} ({TABLE_NAME})")
        if mesa.crupier_actual:
            n_sesiones = len(mesa.sesiones_por_crupier.get(mesa.crupier_actual, []))
            lineas.append(f"• Crupier actual: {mesa.crupier_actual} | sesión #{n_sesiones + 1} | "
                          f"giros en sesión: {len(mesa.sesion_actual)}")
        else:
            lineas.append("• Crupier actual: ❓ sin definir (usá /crupier <nombre>)")
        if mesa.sesion_actual:
            analisis = analizar_sesgo(mesa.sesion_actual)
            if analisis:
                lineas.append(f"• Últimas {analisis['rondas']}: S1 centro {analisis['centro']} | "
                              f"aciertos S1 {analisis['hits'][0]} (esperados {analisis['esperados'][0]:.1f}) | "
                              f"sectores {analisis['hits']}")
                lineas.append(f"• Últimos números: {'-'.join(str(n) for n in mesa.sesion_actual[-10:])}")
        r = mesa.resultados[-SESGO_STATS_WINDOW:]
        if r:
            w = sum(1 for x in r if x["win"])
            lineas.append(f"• Señales (últimas {len(r)}): {w}/{len(r)} = {w/len(r)*100:.1f}% | "
                          f"azar {azar_s1(SESGO_MAX_INTENTOS)*100:.1f}%")
        if a:
            lineas.append(f"• Señal activa: centro {a['centro']} intento {a['intento']}/{SESGO_MAX_INTENTOS} "
                          f"({'enviada' if a['sent'] else 'sombra'})")
        # Registro por crupier
        conocidos = mesa.sesiones_por_crupier
        if conocidos:
            lineas.append("• Crupiers registrados:")
            for nombre, sesiones in list(conocidos.items())[:12]:
                resumen = resumen_crupier(sesiones)
                lineas.append(f"  - {nombre}: {len(sesiones)} sesión(es) | sesgo medio S1 "
                              f"{resumen['media_s1_hits']:.1f} aciertos/34 | última discrepancia "
                              f"{resumen['ultima_sim'] if resumen['ultima_sim'] is not None else '—'}")
        gate = "ABIERTO ✅" if mesa._gate_ok() else "cerrado ⛔"
        lineas.append(f"• Envío a Telegram: {gate} (mín {SESGO_SEND_MIN_WIN_RATE*100:.0f}% con "
                      f"≥{SESGO_SEND_MIN_SAMPLES} señales)")
    return "\n".join(lineas)


def build_crupier_reply(mesa) -> str:
    if mesa.crupier_actual is None:
        return ("❓ No hay crupier activo.\nUsá /crupier <nombre> cuando cambie el crupier de la mesa "
                "(todo el análisis de sesgo se registra por crupier y por sesión).")
    n_sesiones = len(mesa.sesiones_por_crupier.get(mesa.crupier_actual, []))
    return (f"🧑‍💼 Crupier actual: {mesa.crupier_actual} (sesión #{n_sesiones + 1}, "
            f"{len(mesa.sesion_actual)} giros)\n\nUsá /crupier <nombre> cuando cambie.")


if bot is not None:
    @bot.message_handler(commands=["crupier"])
    async def handle_crupier_command(message):
        if _server_state is None:
            await bot.reply_to(message, "⏳ El servidor todavía se está iniciando, intenta de nuevo en unos segundos.")
            return
        partes = message.text.split(maxsplit=1)
        if len(partes) < 2 or not partes[1].strip():
            for mesa in _server_state.tables.values():
                await bot.reply_to(message, build_crupier_reply(mesa))
            return
        nombre = partes[1].strip()
        for mesa in _server_state.tables.values():
            mesa.cambiar_crupier(nombre)
        await bot.reply_to(message, f"✅ Crupier registrado: {nombre}\nNueva sesión iniciada. "
                                    f"Los giros anteriores quedaron archivados en la sesión anterior.")

    @bot.message_handler(commands=["sesgos"])
    async def handle_sesgos_command(message):
        if _server_state is None:
            await bot.reply_to(message, "⏳ El servidor todavía se está iniciando, intenta de nuevo en unos segundos.")
            return
        try:
            await bot.reply_to(message, build_status_message(_server_state))
        except Exception as e:
            log.warning(f"[Telegram] Error respondiendo /sesgos: {e}")

    async def _register_bot_commands():
        if BotCommand is None:
            return
        try:
            await bot.set_my_commands([
                BotCommand("crupier", "Informar/ cambiar el crupier actual (inicia nueva sesión)"),
                BotCommand("sesgos", "Estado del análisis de sesgo por crupier"),
            ])
        except Exception as e:
            log.warning(f"[Telegram] No se pudo registrar el menú de comandos: {e}")


# ══════════════════════════════════════════════
#  RUEDA + SECTORES S1..S6
# ══════════════════════════════════════════════
# Orden físico de la rueda europea (single zero, sentido horario).
WHEEL_ORDER = (0, 32, 15, 19, 4, 21, 2, 25, 17, 34, 6, 27, 13, 36, 11, 30, 8, 23, 10, 5,
               24, 16, 33, 1, 20, 14, 31, 9, 22, 18, 29, 7, 28, 12, 35, 3, 26)
WHEEL_POS = {n: i for i, n in enumerate(WHEEL_ORDER)}
L_RUEDA = len(WHEEL_ORDER)
# Sentido físico de giro de la rueda: +1 = horario (clockwise) sobre WHEEL_ORDER.
# Los vecinos "izquierda" de S1 quedan en -d y los de "derecha" en +d; S2..S5 avanzan horario.
WHEEL_DIRECTION = 1


def azar_s1(intentos: int = None) -> float:
    a = SESGO_MAX_INTENTOS if intentos is None else intentos
    n = 2 * S1_RADIO + 1
    return 1.0 - (1.0 - n / 37.0) ** a


def construir_sectores(centro: int) -> list:
    """S1 = centro ± 6 (13 números); S2..S5 = 6 números cada uno en sentido horario desde S1."""
    i = WHEEL_POS[centro]
    s1 = [WHEEL_ORDER[(i + WHEEL_DIRECTION * d) % L_RUEDA] for d in range(-S1_RADIO, S1_RADIO + 1)]
    sectores = [s1]
    pos = i + WHEEL_DIRECTION * (S1_RADIO + 1)
    for _ in range(SECTORES_EXTRA):
        sectores.append([WHEEL_ORDER[(pos + WHEEL_DIRECTION * d) % L_RUEDA] for d in range(SECTOR_EXTRA_TAM)])
        pos += WHEEL_DIRECTION * SECTOR_EXTRA_TAM
    return sectores


def analizar_sesgo(numeros: list) -> Optional[dict]:
    """
    Análisis de las últimas RONDAS_ANALISIS rondas: recalcula S1 (centro = número cuya ventana de
    13 acumula más caídas), arma S2..S5 en sentido horario y cuenta aciertos por sector.
    Devuelve None si no hay datos suficientes.
    """
    if len(numeros) < 4:
        return None
    ventana = numeros[-RONDAS_ANALISIS:]
    rondas = len(ventana)
    conteo = [0] * 37
    for n in ventana:
        conteo[n] += 1
    # centro: posición de rueda cuya ventana de ±3 acumula más caídas
    mejor_masa, mejor_idx = -1, 0
    for i in range(L_RUEDA):
        masa = sum(conteo[WHEEL_ORDER[(i + d) % L_RUEDA]] for d in range(-S1_RADIO, S1_RADIO + 1))
        if masa > mejor_masa:
            mejor_masa, mejor_idx = masa, i
    centro = WHEEL_ORDER[mejor_idx]
    sectores = construir_sectores(centro)
    hits = [sum(conteo[n] for n in s) for s in sectores]
    esperados = [rondas * len(sectores[0]) / 37.0] + [rondas * SECTOR_EXTRA_TAM / 37.0] * SECTORES_EXTRA
    return {"centro": centro, "sectores": sectores, "hits": hits, "esperados": esperados,
            "rondas": rondas, "top_numeros": sorted(range(37), key=lambda n: -conteo[n])[:5],
            "conteo": conteo}


# ══════════════════════════════════════════════
#  REGISTRO POR CRUPIER + DISCREPANCIA ENTRE SESIONES
# ══════════════════════════════════════════════
def vector_posicion(numeros: list) -> list:
    """Distribución normalizada por posición de la rueda (37 dims) de una serie de giros."""
    v = [1.0 / 37.0] * 37
    if not numeros:
        return v
    conteo = [0.0] * 37
    for n in numeros[-RONDAS_ANALISIS:]:
        conteo[n] += 1.0
    tot = sum(conteo)
    return [c / tot for c in conteo]


def coseno(a: list, b: list) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(y * y for y in b))
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


def discrepancia_vs_historial(sesiones_pasadas: list, sesion_actual: list) -> Optional[dict]:
    """
    Compara la distribución por posición de rueda de las últimas 34 rondas de la sesión ACTUAL
    contra cada sesión archivada del MISMO crupier (usando sus primeras 34 rondas).
    Devuelve {"sim_media", "sim_max", "n"} o None si no hay sesiones pasadas.
    """
    if not sesiones_pasadas or len(sesion_actual) < 4:
        return None
    v_actual = vector_posicion(sesion_actual)
    sims = []
    for ses in sesiones_pasadas:
        if len(ses) >= 4:
            sims.append(coseno(v_actual, vector_posicion(list(ses))))
    if not sims:
        return None
    return {"sim_media": sum(sims) / len(sims), "sim_max": max(sims), "n": len(sims)}


def resumen_crupier(sesiones: list) -> dict:
    """Sesgo medio del crupier: aciertos medios de S1 por sesión (primeras 34 rondas) + última similitud."""
    hits_s1, sims = [], []
    for ses in sesiones:
        a = analizar_sesgo(list(ses[:RONDAS_ANALISIS]))
        if a:
            hits_s1.append(a["hits"][0])
        if len(ses) >= 4:
            sims.append(coseno(vector_posicion(list(ses[:RONDAS_ANALISIS])),
                               vector_posicion(list(sesiones[-1][:RONDAS_ANALISIS]))))
    return {"media_s1_hits": (sum(hits_s1) / len(hits_s1)) if hits_s1 else 0.0,
            "ultima_sim": (round(sims[-1], 3) if sims else None)}


# ══════════════════════════════════════════════
#  MENSAJES
# ══════════════════════════════════════════════
def build_entrada_message(sig: dict, ultimo_numero, intento: int) -> str:
    color_emoji = {"ROJO": "🔴", "NEGRO": "⚫", "VERDE": "🟢"}
    numero = ultimo_numero if ultimo_numero is not None else "-"
    numero_emoji = color_emoji.get(color_of(ultimo_numero), "🟢") if ultimo_numero is not None else ""
    centro = sig["centro"]
    centro_emoji = color_emoji.get(color_of(centro), "🟢")
    s1 = sig["sectores"][0]
    izq = " - ".join(str(n) for n in s1[:S1_RADIO])
    der = " - ".join(str(n) for n in s1[S1_RADIO + 1:])
    ficha = SESGO_CHIP_VALUE * fib_mult(intento)
    total = ficha * len(s1)
    conf = ""
    if not sig["confirmada"]:
        conf = "\n⚠️ SIN CONFIRMACIÓN (la sesión actual discrepa de las sesiones pasadas de este crupier)"
    else:
        conf = f"\n✅ Confirmación histórica: {sig['sim']*100:.0f}% ({sig['sim_n']} sesión(es) previa(s))"
    link = f'🎮 <a href="{TABLE_LINK}">{TABLE_NAME}</a>' if TABLE_LINK else f"🎮 {TABLE_NAME}"
    return (f"🚨🚨 ENTRADA INTENTO {intento} 🚨🚨\n\n"
            f"🧑‍💼 Crupier: {sig['crupier']} (sesión #{sig['sesion_num']})\n"
            f"👉 INGRESAR DESPUÉS: {numero} ({numero_emoji})\n"
            f"🧨 CUBRIR {S1_RADIO} VECINOS: {centro} ({centro_emoji})\n"
            f"   {izq} | {centro} | {der}\n\n"
            f"📊 Aciertos S1 últimas {sig['rondas']}: {sig['hits'][0]} "
            f"(esperados {sig['esperados'][0]:.1f}){conf}\n\n"
            f"🇨🇴 VALOR DE FICHA: ${ficha:,} COP\n"
            f"🇨🇴 APUESTA TOTAL: ${total:,} COP\n"
            f"📈 Gestión FIBONACCI (1·1·2·3·5·8·13·21): ganá 1 vez y reseteá la gestión\n\n"
            f"💫 ¡Juego Responsable!\n{link}")


def build_resolucion_message(win: bool, sig: dict) -> str:
    numeros_str = " | ".join(str(n) for n in sig["numeros"])
    header = "✅✅ SEÑAL 👍🏻" if win else "❌❌ SEÑAL 👎🏻"
    conf = "" if sig["confirmada"] else " ⚠️(sin confirmación)"
    return (f"{header}{conf} ({numeros_str}) | Centro {sig['centro']} | "
            f"Crupier {sig['crupier']} | Intento {sig['intento']}")


# ══════════════════════════════════════════════
#  MESA
# ══════════════════════════════════════════════
class RouletteTable:
    def __init__(self, key: int):
        self.key = key
        self.spin_history = []
        self.total_spins_seen = 0
        self.live_spins_seen = 0

        # ── Registro por crupier ──
        self.crupier_actual: Optional[str] = None
        self.sesion_actual: list = []                 # giros de la sesión del crupier actual
        self.sesiones_por_crupier: dict = {}          # {nombre: [sesión1, sesión2, ...]}

        # ── Señal ──
        self.senal_activa: Optional[dict] = None
        self.resultados: list = []   # {"win","intento","centro","sent","crupier","confirmada","sim","ts"}
        self.last_sinconf_msg_id: Optional[int] = None  # último mensaje "sin confirmación" pendiente

    # ── gestión de crupier / sesiones ──────────────────────────
    def cambiar_crupier(self, nombre: str):
        if self.crupier_actual == nombre:
            return
        self._archivar_sesion_actual()
        self.crupier_actual = nombre
        self.sesion_actual = []
        # Anular señal activa: cambió el crupier, la sesión ya no aplica
        if self.senal_activa is not None and self.senal_activa.get("sent"):
            asyncio.create_task(send_msg(f"🚫 Señal anulada: cambio de crupier a {nombre}", CANAL_SENALES))
        self.senal_activa = None
        log.info(f"[Crupier] Mesa {self.key}: ahora atiende {nombre} (nueva sesión)")

    def _archivar_sesion_actual(self):
        if self.crupier_actual and len(self.sesion_actual) > 0:
            self.sesiones_por_crupier.setdefault(self.crupier_actual, []).append(
                list(self.sesion_actual[-MAX_SPINS_SESION_MEMORIA:]))
            self.sesiones_por_crupier[self.crupier_actual] = \
                self.sesiones_por_crupier[self.crupier_actual][-MAX_SESIONES_X_CRUPIER:]

    def _num_sesion(self) -> int:
        if not self.crupier_actual:
            return 1
        return len(self.sesiones_por_crupier.get(self.crupier_actual, [])) + 1

    # ── gate de envío ──────────────────────────────────────────
    def _gate_ok(self) -> bool:
        r = self.resultados[-SESGO_STATS_WINDOW:]
        if len(r) < max(1, SESGO_SEND_MIN_SAMPLES):
            return False
        w = sum(1 for x in r if x["win"])
        return (w / len(r)) >= SESGO_SEND_MIN_WIN_RATE

    # ── señal ─────────────────────────────────────────────────
    async def _enviar_entrada(self, sig: dict, ultimo_numero, intento: int):
        # Si quedó un mensaje "sin confirmación" pendiente de la señal anterior, se borra primero
        if self.last_sinconf_msg_id:
            await delete_msg(self.last_sinconf_msg_id, CANAL_SENALES)
            self.last_sinconf_msg_id = None
        texto = build_entrada_message(sig, ultimo_numero, intento)
        sig["msg_id"] = await send_msg(texto, CANAL_SENALES)
        if intento > 1 and sig.get("msg_id_anterior"):
            await delete_msg(sig["msg_id_anterior"], CANAL_SENALES)
        if not sig["confirmada"] and sig.get("msg_id"):
            self.last_sinconf_msg_id = sig["msg_id"]

    def _intentar_abrir(self, ultimo_numero):
        if self.senal_activa is not None or self.crupier_actual is None:
            return
        if len(self.sesion_actual) < SESGO_MIN_RONDAS:
            return
        analisis = analizar_sesgo(self.sesion_actual)
        if analisis is None:
            return
        if analisis["hits"][0] < SESGO_MIN_ACIERTOS_S1:
            log.debug(f"[Sesgo] S1 con {analisis['hits'][0]} aciertos (< {SESGO_MIN_ACIERTOS_S1}), sin señal")
            return
        # Confirmación contra sesiones pasadas del mismo crupier
        pasadas = self.sesiones_por_crupier.get(self.crupier_actual, [])
        disc = discrepancia_vs_historial(pasadas, self.sesion_actual)
        confirmada = True
        sim, sim_n = None, 0
        if disc:
            sim, sim_n = disc["sim_max"], disc["n"]
            confirmada = sim >= SESGO_SIM_MIN
        else:
            confirmada = False   # primeras sesiones del crupier: nada que las confirme
        sig = {
            "crupier": self.crupier_actual, "sesion_num": self._num_sesion(),
            "centro": analisis["centro"], "sectores": analisis["sectores"],
            "hits": analisis["hits"], "esperados": analisis["esperados"],
            "rondas": analisis["rondas"], "confirmada": confirmada,
            "sim": sim, "sim_n": sim_n,
            "intento": 1, "numeros": [], "sent": self._gate_ok(), "msg_id": None, "msg_id_anterior": None,
        }
        self.senal_activa = sig
        log.info(f"🎯 SEÑAL S1 centro {sig['centro']} | {sig['hits'][0]} aciertos/34 | "
                 f"crupier {sig['crupier']} sesión #{sig['sesion_num']} | "
                 f"confirmada={confirmada}" + (f" sim={sim:.2f}" if sim is not None else " (sin hist.)") +
                 f" | {'ENVIADA' if sig['sent'] else 'sombra'}")
        if sig["sent"]:
            asyncio.create_task(self._enviar_entrada(sig, ultimo_numero, 1))

    def _resolver(self, number: int):
        sig = self.senal_activa
        if sig is None:
            return
        sig["numeros"].append(number)
        s1 = set(sig["sectores"][0])
        hit = number in s1
        if hit or sig["intento"] >= SESGO_MAX_INTENTOS:
            self.resultados.append({"win": hit, "intento": sig["intento"], "centro": sig["centro"],
                                    "sent": sig["sent"], "crupier": sig["crupier"],
                                    "confirmada": sig["confirmada"], "sim": sig["sim"], "ts": time.time()})
            if sig["sent"]:
                if sig["confirmada"] and self.last_sinconf_msg_id == sig.get("msg_id"):
                    self.last_sinconf_msg_id = None
                asyncio.create_task(send_msg(build_resolucion_message(hit, sig), CANAL_SENALES))
            log.info(f"🎯 Señal cerrada: {'WIN' if hit else 'LOSS'} intento {sig['intento']} | "
                     f"centro {sig['centro']} | salió {number} | crupier {sig['crupier']}")
            self.senal_activa = None
            return
        # Intento 2: se recalcula S1 con la sesión actualizada
        sig["intento"] += 1
        sig["msg_id_anterior"] = sig.get("msg_id")
        analisis = analizar_sesgo(self.sesion_actual)
        if analisis:
            sig["centro"], sig["sectores"] = analisis["centro"], analisis["sectores"]
            sig["hits"], sig["esperados"], sig["rondas"] = analisis["hits"], analisis["esperados"], analisis["rondas"]
        log.info(f"🎯 Intento {sig['intento']}: S1 recalculado → centro {sig['centro']}")
        if sig["sent"]:
            asyncio.create_task(self._enviar_entrada(sig, number, sig["intento"]))

    # ── persistencia ──────────────────────────────────────────
    def persist(self) -> dict:
        return {
            "table_total_spins_seen": self.total_spins_seen,
            "crupier_actual": self.crupier_actual,
            "sesion_actual": list(self.sesion_actual[-MAX_SPINS_SESION_MEMORIA:]),
            "sesiones_por_crupier": self.sesiones_por_crupier,
            "resultados": self.resultados,
        }

    def load(self, data):
        if not data:
            return
        self.total_spins_seen = data.get("table_total_spins_seen", self.total_spins_seen)
        self.crupier_actual = data.get("crupier_actual")
        self.sesion_actual = list(data.get("sesion_actual", []))
        self.sesiones_por_crupier = {k: [list(s) for s in v]
                                     for k, v in (data.get("sesiones_por_crupier") or {}).items()}
        self.resultados = list(data.get("resultados", []))

    def agregar_seed(self, spins: list):
        """El histórico sin crupier queda como sesión de referencia de un pseudo-crupier."""
        if not spins:
            return
        self.sesiones_por_crupier.setdefault(CRUPIER_SEED_NAME, []).append(list(spins[-SEED_SPIN_CAP:]))

    # ── entrada de giro ───────────────────────────────────────
    def update(self, number: int, real_color: str, timestamp: float = None, training: bool = False):
        if timestamp is None:
            timestamp = time.time()
        self.spin_history.append({"number": number, "color": real_color, "timestamp": timestamp})
        if len(self.spin_history) > 200:
            self.spin_history.pop(0)
        self.total_spins_seen += 1
        if not training:
            self.live_spins_seen += 1
        self.sesion_actual.append(number)
        if len(self.sesion_actual) > MAX_SPINS_SESION_MEMORIA:
            del self.sesion_actual[:len(self.sesion_actual) - MAX_SPINS_SESION_MEMORIA]
        if training:
            return
        self._resolver(number)
        self._intentar_abrir(number)
        crupier_txt = self.crupier_actual or "?"
        log.info(f"🎰 Mesa {self.key} | Giro #{self.total_spins_seen}: {number} ({real_color}) | "
                 f"Crupier {crupier_txt} sesión #{self._num_sesion()} | "
                 f"Señal: {'activa (centro %s, intento %s)' % (self.senal_activa['centro'], self.senal_activa['intento']) if self.senal_activa else 'sin señal'}")

    def get_state(self, limit: int = 40):
        r = self.resultados[-SESGO_STATS_WINDOW:]
        a = self.senal_activa
        analisis = analizar_sesgo(self.sesion_actual)
        return {
            "key": self.key,
            "table_name": TABLE_NAME,
            "spin_history": self.spin_history[-limit:],
            "live_spins_seen": self.live_spins_seen,
            "total_spins_seen": self.total_spins_seen,
            "crupier_actual": self.crupier_actual,
            "sesion_num": self._num_sesion(),
            "giros_sesion_actual": len(self.sesion_actual),
            "sesiones_por_crupier": {k: len(v) for k, v in self.sesiones_por_crupier.items()},
            "analisis": None if analisis is None else {
                "centro": analisis["centro"], "hits": analisis["hits"],
                "esperados": analisis["esperados"], "rondas": analisis["rondas"],
                "sectores": analisis["sectores"],
            },
            "senal": {
                "activa": None if a is None else {
                    "crupier": a["crupier"], "centro": a["centro"], "sectores": a["sectores"],
                    "intento": a["intento"], "intentos_max": SESGO_MAX_INTENTOS,
                    "confirmada": a["confirmada"], "sim": a["sim"], "enviada": a["sent"],
                },
                "senales_cerradas": len(r),
                "aciertos": sum(1 for x in r if x["win"]),
                "win_rate": (sum(1 for x in r if x["win"]) / len(r)) if r else None,
                "azar": azar_s1(),
                "envio_abierto": self._gate_ok(),
            },
        }


# ══════════════════════════════════════════════
#  HTTP APP
# ══════════════════════════════════════════════
async def http_ping(request: web.Request):
    return web.json_response({"status": "pong", "ts": time.time()})

async def http_health(request: web.Request):
    if _server_state is None:
        return web.json_response({"status": "not_ready"}, status=503)
    return web.json_response({
        "status": "ok",
        "mesas": list(_server_state.tables.keys()),
        "total_spins": sum(t.total_spins_seen for t in _server_state.tables.values())
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
    table = _server_state.tables.get(mesa)
    if table is None:
        return web.json_response({"error": "mesa no encontrada"}, status=404)
    try:
        limit = int(request.query.get("limit", 40))
    except ValueError:
        limit = 40
    limit = max(20, min(300, limit))
    return web.json_response(table.get_state(limit=limit))

async def http_api_all(request: web.Request):
    if _server_state is None:
        return web.json_response({"error": "server not ready"}, status=503)
    result = {str(key): _server_state.tables[key].get_state(limit=40) for key in ROULETTE_KEYS.values()}
    return web.json_response(result)

def build_http_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/ping", http_ping)
    app.router.add_get("/health", http_health)
    app.router.add_get("/api/state/{mesa}", http_api_state)
    app.router.add_get("/api/all", http_api_all)
    app.router.add_get("/", http_health)
    return app


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
            return
        self.seen.add(gid)
        if len(self.seen) > 3000:
            self.seen.clear()
        if self.on_spin_callback:
            await self.on_spin_callback(num, emit, training=not emit)


# ══════════════════════════════════════════════
#  SERVER STATE
# ══════════════════════════════════════════════
class ServerState:
    def __init__(self):
        self.tables = {k: RouletteTable(k) for k in ROULETTE_KEYS.values()}
        self.seed_cargado = False

    async def update_mesa(self, key: int, number: int, broadcast: bool = True, training: bool = False):
        if key not in self.tables:
            return
        self.tables[key].update(number, color_of(number), training=training)

    def load_all_models(self):
        for key in self.tables:
            filename = f"model_{key}.json"
            if not os.path.exists(filename):
                continue
            try:
                with open(filename, "r") as f:
                    data = json.load(f)
                self.tables[key].load(data)
                log.info(f"Modelo cargado para mesa {key}")
            except Exception as e:
                log.warning(f"Error cargando modelo mesa {key}: {e}")

    def save_all_models(self):
        for key, table in self.tables.items():
            try:
                with open(f"model_{key}.json", "w") as f:
                    json.dump(table.persist(), f)
            except Exception as e:
                log.warning(f"Error guardando modelo mesa {key}: {e}")

    def cargar_seed(self):
        """El seed histórico queda como sesión de referencia del pseudo-crupier HISTORICO."""
        if self.seed_cargado:
            return
        if not HISTORY_SEED_PATH or not os.path.exists(HISTORY_SEED_PATH):
            return
        try:
            conn = sqlite3.connect(":memory:")
            with open(HISTORY_SEED_PATH, "r", encoding="utf-8") as f:
                conn.executescript(f.read())
            cur = conn.execute(f'SELECT spin_number FROM "{HISTORY_SEED_TABLE}" ORDER BY id ASC')
            spins = [int(row[0]) for row in cur.fetchall()]
            conn.close()
            if spins:
                for table in self.tables.values():
                    table.agregar_seed(spins)
                log.info(f"[Historial] {len(spins)} giros → sesión de referencia '{CRUPIER_SEED_NAME}'.")
            self.seed_cargado = True
        except Exception as e:
            log.warning(f"[Historial] Error leyendo '{HISTORY_SEED_PATH}': {e}")


# ══════════════════════════════════════════════
#  SELF-PING Y BOT POLLING
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
        delay = min(delay * 2, 120) if ran_for < 60 else 5
        log.warning(f"[Telegram] Reintentando polling en {delay}s…")
        await asyncio.sleep(delay)


# ══════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════
async def main():
    global _server_state
    log.info("═" * 60)
    log.info("BOT DE SESGO POR CRUPIER — S1..S6 sobre las últimas 34 rondas")
    log.info(f"Mesas: {', '.join(str(k) for k in ROULETTE_KEYS.values())}")
    log.info("═" * 60)

    server_state = ServerState()
    server_state.load_all_models()
    server_state.cargar_seed()
    _server_state = server_state

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
        tasks.append(asyncio.create_task(_register_bot_commands()))

    port = int(os.environ.get("PORT", 10000))
    app = build_http_app()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    first = next(iter(ROULETTE_KEYS.values()))
    log.info(f"Servidor HTTP escuchando en puerto {port} (API: /ping, /health, /api/state/{first}, /api/all)")

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
