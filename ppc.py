"""
╔══════════════════════════════════════════════════════════════╗
║   BOT DE SEÑALES POR SESSO DE CRUPIER — SPEED ROULETTE        ║
║                                                               ║
║   Estrategia (nueva, sin ML):                                 ║
║   - El crupier se detecta AUTOMÁTICO desde el WS (dealer.name)║
║     (y se puede forzar a mano con /crupier NOMBRE).           ║
║     Al cambiar de crupier se archiva la sesión anterior y se  ║
║     empieza una nueva; todo queda registrado por crupier.     ║
║   - En cada giro se analizan las últimas RONDAS_ANALISIS (34) ║
║     rondas de la sesión ACTUAL del crupier.                   ║
║   - La rueda se divide en 6 sectores adaptativos:             ║
║       S1 = 7 números (centro = predicción, 3 por cada lado)   ║
║       S2..S6 = 6 números cada uno, en sentido HORARIO desde S1║
║     S1 se recalcula en CADA giro (el centro es el número cuya ║
║     ventana de 7 acumula más caídas en las últimas 34).       ║
║   - COBERTURA al apostar: centro ± 6 (13 nº) o ± 8 (17 nº).   ║
║     Cada señal mide ambas; /estadisticas las compara.         ║
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
║   - ESTRATEGIA=desvio (defecto): en cada ronda se anota en qué║
║     zona Sx cayó el número respecto de la S1 predicha (des-   ║
║     viación). Con las últimas 34 desviaciones, árboles ML por ║
║     crupier estiman la zona más probable y los 37 números se  ║
║     prueban como centro de la nueva S1. Se recalcula en cada  ║
║     intento. Giro SIEMPRE horario, de S1 a S6.                ║
║   - ESTRATEGIA=masa / zonas: métodos anteriores (alternativos)║
╚══════════════════════════════════════════════════════════════╝
"""

import asyncio
import html
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

try:   # árboles de decisión (ML) para la estrategia "desvio"; sin scikit-learn se usa el método por frecuencia
    from sklearn.ensemble import RandomForestClassifier
    SKLEARN_OK = True
except ImportError:
    RandomForestClassifier = None
    SKLEARN_OK = False

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
# 1 = la señal incluye líneas extra (vecinos, aciertos, confirmación, gestión Fibonacci). 0 = formato compacto.
SENAL_DETALLE   = os.environ.get("SENAL_DETALLE", "0") == "1"

def esc(x) -> str:
    """Escapa texto dinámico para parse_mode=HTML de Telegram (evita el error 400 'can\'t parse entities')."""
    return html.escape(str(x), quote=False)

def normalizar_nombre(nombre) -> Optional[str]:
    """Limpia el nombre del crupier que llega del servidor. Devuelve None si no es un nombre válido."""
    if not isinstance(nombre, str):
        return None
    n = " ".join(nombre.split())
    if not n or n.upper() in ("N/A", "NA", "NONE", "NULL", "UNKNOWN"):
        return None
    return n[:60]

HISTORY_SEED_PATH  = os.environ.get("HISTORY_SEED_PATH", "russian-azure.db")
HISTORY_SEED_TABLE = os.environ.get("HISTORY_SEED_TABLE", "roulette_1")

# ── Estrategia: sesgo por crupier + sectores S1..S6 ──
RONDAS_ANALISIS      = 34     # rondas que miramos hacia atrás en la sesión del crupier
S1_RADIO             = 3      # S1 = centro ± 3 (7 números: 3 por cada lado + centro)
SECTORES_EXTRA       = 5      # S2..S6
SECTOR_EXTRA_TAM     = 6      # 6 números cada uno (7 + 5*6 = 37)
# Cobertura al apostar la señal: centro ± N vecinos. Se miden AMBAS en cada señal.
COBERTURAS           = (6, 8)   # 6 vecinos = 13 números | 8 vecinos = 17 números
COBERTURA_ENVIO      = int(os.environ.get("COBERTURA_VECINOS", "6"))   # la que se muestra/apuesta en la señal
if COBERTURA_ENVIO not in COBERTURAS:
    COBERTURA_ENVIO = COBERTURAS[0]
# Estrategia: "desvio" (defecto: desviación respecto a la S1 predicha + árboles ML), "masa" (S1 = ventana más caliente de las últimas 34) o "zonas"
ESTRATEGIA           = os.environ.get("ESTRATEGIA", "desvio").strip().lower()
if ESTRATEGIA not in ("zonas", "masa", "desvio"):
    ESTRATEGIA = "desvio"
ZONA_VENTANA         = int(os.environ.get("ZONA_VENTANA", "34"))          # rondas recientes para hallar la zona que más sale (0 = toda la sesión)
ZONA_MIN_CUENTA      = int(os.environ.get("ZONA_MIN_CUENTA", "3"))        # veces mínimas que debe haber salido la zona dominante
ZONA_Z_MIN           = float(os.environ.get("ZONA_Z_MIN", "1.5"))         # cuánto debe superar al azar (en desviaciones) para dar señal
ZONA_HIST_MIN        = int(os.environ.get("ZONA_HIST_MIN", "30"))       # rondas históricas del crupier para confirmar
SESGO_MIN_RONDAS     = int(os.environ.get("SESGO_MIN_RONDAS", "20"))   # mínimo de rondas en la sesión para evaluar
SESGO_MIN_ACIERTOS_S1 = int(os.environ.get("SESGO_MIN_ACIERTOS_S1", "9"))  # aciertos mínimos de S1 (7 nº) en las últimas 34 (esperado al azar ≈ 6.4)
SESGO_SIM_MIN        = float(os.environ.get("SESGO_SIM_MIN", "0.35"))  # similitud mínima vs sesiones pasadas del crupier
# ── Estrategia "desvio": en cada ronda se anota en qué zona Sx (respecto de la S1 predicha) cayó el número; con las últimas
#    34 desviaciones se predice la nueva S1. Los 37 números son candidatos a centro. Modelo: árboles de decisión por crupier.
DESVIO_VENTANA        = RONDAS_ANALISIS
DESVIO_MIN_HIST       = int(os.environ.get("DESVIO_MIN_HIST", "10"))          # desviaciones mínimas en la ventana para predecir
DESVIO_MIN_TRAIN      = int(os.environ.get("DESVIO_MIN_TRAIN", "120"))        # filas mínimas del crupier para entrenar los árboles
DESVIO_REENTRENAR_CADA = int(os.environ.get("DESVIO_REENTRENAR_CADA", "25"))  # giros entre reentrenamientos
DESVIO_MIN_PROB       = float(os.environ.get("DESVIO_MIN_PROB", "0.40"))      # prob. mínima de que el próximo número caiga en la cobertura
DESVIO_MARGEN_VAL     = float(os.environ.get("DESVIO_MARGEN_VAL", "0.03"))    # cuánto debe superar al azar la validación para "confirmar"
DESVIO_MIN_VAL        = int(os.environ.get("DESVIO_MIN_VAL", "40"))           # giros mínimos de validación fuera de muestra
# Filtros para DESCARTAR señales (se aplican al abrir y, con DESVIO_DESCARTAR_EN_INTENTOS, también en cada reintento)
DESVIO_SOLO_CONFIRMADAS = os.environ.get("DESVIO_SOLO_CONFIRMADAS", "1").strip() != "0"   # descarta si la validación fuera de muestra del crupier no supera al azar
DESVIO_REQUIERE_ARBOLES = os.environ.get("DESVIO_REQUIERE_ARBOLES", "1").strip() != "0"   # descarta si no hay modelo de árboles entrenado
DESVIO_MIN_PROB_REINTENTO = float(os.environ.get("DESVIO_MIN_PROB_REINTENTO", "0.37"))    # en un reintento: prob. mínima para seguir (azar 13/37 = 0.351)
DESVIO_DESCARTAR_EN_INTENTOS = os.environ.get("DESVIO_DESCARTAR_EN_INTENTOS", "1").strip() != "0"
DESVIO_LIVE_N         = int(os.environ.get("DESVIO_LIVE_N", "20"))            # últimas señales cerradas del crupier para medir su rendimiento en vivo
DESVIO_LIVE_MIN       = int(os.environ.get("DESVIO_LIVE_MIN", "10"))          # con al menos estas señales se activa el filtro de rendimiento en vivo
DESVIO_SUAVIZADO      = 10.0
DESVIO_SESIONES_ENTRENO = 6
DESVIO_SPINS_X_SESION = 500
DESVIO_MAX_FILAS      = 3000
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


if ESTRATEGIA == "desvio" and not SKLEARN_OK:
    log.warning("ESTRATEGIA=desvio sin scikit-learn: no habrá árboles y, con DESVIO_REQUIERE_ARBOLES=1, ninguna señal "
                "pasará el filtro. Agregá 'scikit-learn' a requirements.txt (o DESVIO_REQUIERE_ARBOLES=0).")


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
            lineas.append(f"• Crupier actual: {esc(mesa.crupier_actual)} | sesión #{n_sesiones + 1} | "
                          f"giros en sesión: {len(mesa.sesion_actual)}")
        else:
            lineas.append("• Crupier actual: ❓ sin definir (esperando dato del servidor o /crupier NOMBRE)")
        if mesa.sesion_actual:
            analisis = analizar_sesgo(mesa.sesion_actual)
            if analisis:
                lineas.append(f"• Últimas {analisis['rondas']}: S1 centro {analisis['centro']} | "
                              f"aciertos S1 {analisis['hits'][0]} (esperados {analisis['esperados'][0]:.1f}) | "
                              f"sectores {analisis['hits']}")
                lineas.append(f"• Últimos números: {'-'.join(str(n) for n in mesa.sesion_actual[-10:])}")
        if ESTRATEGIA == "desvio" and mesa.pred_desvio:
            pd_ = mesa.pred_desvio
            lineas.append(f"• Próxima S1 (desvío): base {pd_['base']} → centro {pd_['centro']} | desviación dominante "
                          f"S{pd_['dom_zona']} ({pd_['dom_cuenta']}/{pd_['n']}) | prob. cobertura {pd_['prob_cov']*100:.0f}% "
                          f"(azar {(2 * COBERTURA_ENVIO + 1) / 37 * 100:.0f}%) | {pd_['metodo']}")
        if ESTRATEGIA == "desvio":
            motivo = mesa._filtro_desvio(mesa.pred_desvio)
            lineas.append("• Filtro de descarte (ahora): " + ("PASA ✅" if motivo is None else f"descartada 🚫 — {motivo}"))
            if mesa.descartes:
                top = sorted(mesa.descartes.items(), key=lambda kv: -kv[1])[:5]
                lineas.append(f"• Candidatas descartadas: {sum(mesa.descartes.values())} (" +
                              " · ".join(f"{k} {v}" for k, v in top) + ")")
        r = mesa.resultados_modo()[-SESGO_STATS_WINDOW:]
        if r:
            w = sum(1 for x in r if x["win"])
            lineas.append(f"• Señales (últimas {len(r)}): {w}/{len(r)} = {w/len(r)*100:.1f}% | "
                          f"azar ({COBERTURA_ENVIO} vecinos) {azar_s1(SESGO_MAX_INTENTOS)*100:.1f}%")
        if a:
            lineas.append(f"• Señal activa: centro {a['centro']} intento {a['intento']}/{SESGO_MAX_INTENTOS} "
                          f"({'enviada' if a['sent'] else 'sombra'})")
        # Análisis por crupier (sesiones archivadas + la sesión en curso del crupier actual)
        conocidos = mesa.sesiones_por_crupier
        nombres = list(conocidos.keys())
        if mesa.crupier_actual and mesa.crupier_actual not in nombres:
            nombres.append(mesa.crupier_actual)
        if nombres:
            lineas.append("• Análisis por crupier (S1 recalculado en cada giro, sectores S1..S6 en sentido horario):")
            for nombre in nombres[:10]:
                actual = mesa.sesion_actual if nombre == mesa.crupier_actual else None
                lineas.append(linea_analisis_crupier(nombre, conocidos.get(nombre, []), actual))
                mod = mesa.modelos.get(nombre)
                if ESTRATEGIA == "desvio" and mod:
                    lineas.append(f"      Árboles ML: {mod['n']} filas | fuera de muestra {mod['val_hit']*100:.1f}% "
                                  f"vs S1 base sin corrección {mod['val_base']*100:.1f}% en {mod['val_n']} giros")
                sen = [x for x in mesa.resultados if x.get("crupier") == nombre and x.get("sent")]
                if sen:
                    w = sum(1 for x in sen if x["win"])
                    lineas.append(f"      Señales enviadas: {w}/{len(sen)} ganadas ({w / len(sen) * 100:.0f}%)")
        gate = "ABIERTO ✅" if mesa._gate_ok() else "cerrado ⛔"
        lineas.append(f"• Envío a Telegram: {gate} (mín {SESGO_SEND_MIN_WIN_RATE*100:.0f}% con "
                      f"≥{SESGO_SEND_MIN_SAMPLES} señales)")
    return "\n".join(lineas)


def calcular_stats_cobertura(resultados: list) -> dict:
    """Aciertos por cobertura (6 y 8 vecinos). Solo cuentan señales que traen el dato de ambas coberturas."""
    validos = [x for x in resultados if x.get("modo", "masa") == ESTRATEGIA and all(f"i{v}" in x for v in COBERTURAS)]
    out = {"total": len(validos), "antiguos": len(resultados) - len(validos)}

    def por_cob(lista):
        d = {}
        for v in COBERTURAS:
            ints = [x[f"i{v}"] for x in lista if x[f"i{v}"] is not None]
            dist = {k: 0 for k in range(1, SESGO_MAX_INTENTOS + 1)}
            for k in ints:
                if k in dist:
                    dist[k] += 1
            d[v] = {"n": len(lista), "wins": len(ints), "dist": dist,
                    "prom": (sum(ints) / len(ints)) if ints else None}
        return d
    out["global"] = por_cob(validos)
    out["enviadas"] = por_cob([x for x in validos if x.get("sent")])
    out["ultimas"] = por_cob(validos[-SESGO_STATS_WINDOW:])
    return out


def build_estadisticas_message(server_state) -> str:
    todos = []
    for mesa in server_state.tables.values():
        todos.extend(mesa.resultados)
    todos.sort(key=lambda x: x.get("ts", 0))
    st = calcular_stats_cobertura(todos)
    if st["total"] == 0:
        extra = f"\n({st['antiguos']} señales de la estrategia anterior o sin dato de 6/8 vecinos)" if st["antiguos"] else ""
        return ("📊 <b>ESTADÍSTICAS</b>\nTodavía no hay señales cerradas con dato de 6 y 8 vecinos. "
                f"Se van acumulando a medida que se cierran señales (incluye las de sombra).{extra}")

    def pct(w, n):
        return f"{w / n * 100:.1f}%" if n else "—"

    g = st["global"]
    lineas = [f"📊 <b>ESTADÍSTICAS GLOBALES — 6 vs 8 vecinos</b>\nEstrategia: {ESTRATEGIA}",
              f"Señales cerradas: {st['total']} (enviadas: {st['enviadas'][COBERTURAS[0]]['n']} | "
              f"sombra: {st['total'] - st['enviadas'][COBERTURAS[0]]['n']}) — hasta {SESGO_MAX_INTENTOS} intentos c/u"]
    for v in COBERTURAS:
        d = g[v]
        marca = " ← cobertura de señal" if v == COBERTURA_ENVIO else ""
        lineas.append(f"\n🧨 <b>{v} VECINOS</b> ({2 * v + 1} números){marca}")
        lineas.append(f"• Aciertos: {d['wins']}/{d['n']} = {pct(d['wins'], d['n'])} | "
                      f"fallos: {d['n'] - d['wins']} | azar: {azar_s1(SESGO_MAX_INTENTOS, v) * 100:.1f}%")
        lineas.append(f"• 1er intento: {d['dist'][1]}/{d['n']} = {pct(d['dist'][1], d['n'])} | "
                      f"azar: {(2 * v + 1) / 37 * 100:.1f}%")
        dist = " · ".join(f"I{k}: {c}" for k, c in d["dist"].items() if c) or "—"
        prom = f"{d['prom']:.2f}" if d["prom"] is not None else "—"
        lineas.append(f"• Acierto por intento: {dist} | intento medio: {prom}")
        e = st["enviadas"][v]
        u = st["ultimas"][v]
        lineas.append(f"• Solo enviadas: {e['wins']}/{e['n']} = {pct(e['wins'], e['n'])} | "
                      f"últimas {u['n']}: {u['wins']}/{u['n']} = {pct(u['wins'], u['n'])}")
    a, b = COBERTURAS
    if g[a]["n"]:
        dif = (g[b]["wins"] - g[a]["wins"]) / g[a]["n"] * 100
        lineas.append(f"\n📌 {b} vecinos acierta {dif:+.1f} pts vs {a} vecinos "
                      f"(apuesta {2 * b + 1} números en vez de {2 * a + 1}).")
    if st["antiguos"]:
        lineas.append(f"({st['antiguos']} señales de la estrategia anterior o sin dato de cobertura no se cuentan)")
    return "\n".join(lineas)


def _hace(ts) -> str:
    seg = max(0, int(time.time() - ts))
    if seg < 60:
        return f"hace {seg} s"
    if seg < 3600:
        return f"hace {seg // 60} min"
    return f"hace {seg // 3600} h {(seg % 3600) // 60} min"


def build_crupier_reply(mesa) -> str:
    """Datos del crupier de la ÚLTIMA RONDA registrada en la mesa."""
    ur = mesa.ultima_ronda
    if not ur or not ur.get("crupier"):
        return (f"🎲 Mesa {mesa.key} ({esc(TABLE_NAME)})\n"
                "❓ Todavía no hay una ronda registrada con crupier identificado.\n"
                "Se detecta solo desde el servidor; también podés forzarlo con /crupier NOMBRE.")
    nombre = ur["crupier"]
    emoji = {"ROJO": "🔴", "NEGRO": "⚫", "VERDE": "🟢"}.get(ur.get("color"), "")
    es_actual = (nombre == mesa.crupier_actual)
    previas = mesa.sesiones_por_crupier.get(nombre, [])
    lineas = [f"🎲 Mesa {mesa.key} ({esc(TABLE_NAME)})",
              f"👤 CRUPIER DE LA ÚLTIMA RONDA: {esc(nombre)}",
              f"🔁 Última ronda: {ur['numero']} ({emoji} {ur.get('color', '')}) — {_hace(ur['ts'])}"
              + (f" | zona S{ur['zona']} respecto a {ur['ref']}" if (ur.get("zona") and ESTRATEGIA == "zonas") else "")]
    if es_actual:
        lineas.append(f"📋 Sesión #{ur['sesion_num']} | giros en la sesión: {len(mesa.sesion_actual)}")
        if mesa.sesion_actual:
            lineas.append(f"🔢 Últimos números: {'-'.join(str(n) for n in mesa.sesion_actual[-10:])}")
            zs = zonas_de_giros(mesa.sesion_actual)
            rz = resumen_zonas(contar_zonas([mesa.sesion_actual]), zs[-10:])
            if rz and ESTRATEGIA == "zonas":
                lineas.append(f"🧭 Zonas S1..S6 de la sesión ({rz['n']} rondas): {rz['partes']}")
                lineas.append(f"↪️ Últimas zonas: {rz['secuencia']} (azar: S1 {tam_zona(1)/37*100:.0f}% · resto {tam_zona(2)/37*100:.0f}%)")
                dom = zona_dominante(zs)
                if dom:
                    lineas.append(f"⭐ Zona que más sale (últimas {dom['n']}): S{dom['zona']} — {dom['cuenta']} veces "
                                  f"(esperado {dom['esperado']:.1f}, {dom['z']:+.1f}σ) → cae "
                                  f"{txt_desplazamiento(dom['zona'])}")
                    lineas.append(f"🔄 Sentido del giro: {GIRO_TXT}")
            a = analizar_sesgo(mesa.sesion_actual)
            if a and ESTRATEGIA == "masa":
                lineas.append(f"🎯 Sesgo (últimas {a['rondas']}): S1 centro {a['centro']} | aciertos S1 "
                              f"{a['hits'][0]} (esperados {a['esperados'][0]:.1f})")
    else:
        lineas.append(f"⚠️ Ahora la mesa la atiende otro crupier: {esc(mesa.crupier_actual or '—')}")
    # Histórico de este crupier
    if previas:
        r = resumen_crupier(previas)
        sim = r["ultima_sim"] if r["ultima_sim"] is not None else "—"
        lineas.append(f"📚 Sesiones archivadas: {len(previas)} | sesgo medio S1 {r['media_s1_hits']:.1f} aciertos/34 "
                      f"| última similitud {sim}")
        rh = resumen_zonas(contar_zonas(previas))
        if rh and ESTRATEGIA == "zonas":
            lineas.append(f"🧭 Zonas históricas del crupier ({rh['n']} rondas): {rh['partes']}")
    else:
        lineas.append("📚 Sesiones archivadas: 0 (primera sesión de este crupier)")
    if ESTRATEGIA == "masa":
        lineas.append("📊 Análisis de este crupier (S1 recalculado en cada giro):")
        lineas.append(linea_analisis_crupier(nombre, previas, mesa.sesion_actual if es_actual else None))
        mod = mesa.modelos.get(nombre)
        if ESTRATEGIA == "desvio" and mod:
            lineas.append(f"  🤖 Árboles ML: {mod['n']} filas | fuera de muestra {mod['val_hit']*100:.1f}% "
                          f"vs S1 base {mod['val_base']*100:.1f}% en {mod['val_n']} giros")
        if ESTRATEGIA == "desvio" and es_actual and mesa.pred_desvio:
            pd_ = mesa.pred_desvio
            lineas.append(f"  🎯 Próxima S1: base {pd_['base']} → {pd_['centro']} (desviación dominante S{pd_['dom_zona']}, "
                          f"prob. {pd_['prob_cov']*100:.0f}%)")
    # Señales de este crupier
    sen = [x for x in mesa.resultados if x.get("crupier") == nombre and x.get("sent")]
    if sen:
        w = sum(1 for x in sen if x["win"])
        lineas.append(f"📡 Señales enviadas con este crupier: {w}/{len(sen)} ganadas ({w/len(sen)*100:.0f}%)")
    else:
        lineas.append("📡 Señales enviadas con este crupier: ninguna todavía")
    return "\n".join(lineas)


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
        nombre = normalizar_nombre(partes[1])
        if nombre is None:
            await bot.reply_to(message, "⚠️ Nombre de crupier no válido.")
            return
        for mesa in _server_state.tables.values():
            mesa.cambiar_crupier(nombre)
        await bot.reply_to(message, f"✅ Crupier registrado: {esc(nombre)}\nNueva sesión iniciada. "
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

    @bot.message_handler(func=lambda m: bool(getattr(m, "text", None)) and m.text.lower().startswith("/estadísticas"))
    @bot.message_handler(commands=["estadisticas"])
    async def handle_estadisticas_command(message):
        if _server_state is None:
            await bot.reply_to(message, "⏳ El servidor todavía se está iniciando, intenta de nuevo en unos segundos.")
            return
        try:
            await bot.reply_to(message, build_estadisticas_message(_server_state))
        except Exception as e:
            log.warning(f"[Telegram] Error respondiendo /estadisticas: {e}")

    async def _register_bot_commands():
        if BotCommand is None:
            return
        try:
            await bot.set_my_commands([
                BotCommand("crupier", "Informar/ cambiar el crupier actual (inicia nueva sesión)"),
                BotCommand("sesgos", "Estado del análisis de sesgo por crupier"),
                BotCommand("estadisticas", "Aciertos globales: 6 vs 8 vecinos"),
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
# La bola gira SIEMPRE en sentido horario (agujas del reloj) y las zonas se arman en ese mismo orden: S1 = 3 números por
# cada lado; S2 arranca en el 4º número horario desde el centro, y S2..S6 siguen en sentido horario hasta que S6 termina
# en el 4º antihorario. WHEEL_ORDER (0-32-15-19-4-21…) está listada en sentido horario, así que WHEEL_DIRECTION=+1 (defecto)
# = horario. -1 = antihorario (solo por si hiciera falta, vía variable de entorno).
WHEEL_DIRECTION = -1 if os.environ.get("WHEEL_DIRECTION", "1").strip() == "-1" else 1
SENTIDO_TXT = "horario" if WHEEL_DIRECTION == 1 else "antihorario"
SENTIDO_OPUESTO_TXT = "antihorario" if WHEEL_DIRECTION == 1 else "horario"
GIRO_TXT = f"sentido {SENTIDO_TXT} (agujas del reloj), de S1 a S6" if WHEEL_DIRECTION == 1 else f"sentido {SENTIDO_TXT}, de S1 a S6"


def azar_s1(intentos: int = None, vecinos: int = None) -> float:
    """Probabilidad de acertar al menos 1 vez en 'intentos' giros cubriendo centro ± vecinos, por azar."""
    a = SESGO_MAX_INTENTOS if intentos is None else intentos
    v = COBERTURA_ENVIO if vecinos is None else vecinos
    n = 2 * v + 1
    return 1.0 - (1.0 - n / 37.0) ** a


def cobertura_numeros(centro: int, vecinos: int) -> list:
    """Números a apostar: centro ± vecinos sobre la rueda física (2*vecinos + 1 números, en orden de rueda)."""
    i = WHEEL_POS[centro]
    return [WHEEL_ORDER[(i + WHEEL_DIRECTION * d) % L_RUEDA] for d in range(-vecinos, vecinos + 1)]


def zona_de(ref: int, x: int) -> int:
    """Zona (1..6) donde cae x con S1 centrado en ref. S1 = ref ± 3; S2..S6 = 6 números c/u en sentido horario (ver WHEEL_DIRECTION)."""
    d = ((WHEEL_POS[x] - WHEEL_POS[ref]) * WHEEL_DIRECTION) % L_RUEDA
    if d <= S1_RADIO or d >= L_RUEDA - S1_RADIO:
        return 1
    return 2 + (d - (S1_RADIO + 1)) // SECTOR_EXTRA_TAM


def tam_zona(k: int) -> int:
    return (2 * S1_RADIO + 1) if k == 1 else SECTOR_EXTRA_TAM


def centro_de_zona(ref: int, zona: int) -> int:
    """Número central de la zona 'zona' medida desde ref (S1 -> el propio ref; zonas de 6 -> el 3er número)."""
    if zona == 1:
        return ref
    d = S1_RADIO + 1 + (zona - 2) * SECTOR_EXTRA_TAM + (SECTOR_EXTRA_TAM // 2 - 1)
    return WHEEL_ORDER[(WHEEL_POS[ref] + WHEEL_DIRECTION * d) % L_RUEDA]


def zonas_de_giros(numeros: list) -> list:
    """Zona de cada ronda respecto del número anterior de la sesión (la 1ª ronda no tiene referencia)."""
    return [zona_de(numeros[i - 1], numeros[i]) for i in range(1, len(numeros))]


def zona_dominante(zonas: list) -> Optional[dict]:
    """Zona que MÁS SALE en las últimas ZONA_VENTANA rondas (desempate: la más reciente) y cuánto supera al azar."""
    z = zonas[-ZONA_VENTANA:] if ZONA_VENTANA > 0 else list(zonas)
    n = len(z)
    if n == 0:
        return None
    cont = {k: z.count(k) for k in range(1, SECTORES_EXTRA + 2)}
    ultima = {k: (max(i for i, v in enumerate(z) if v == k) if cont[k] else -1) for k in cont}
    k = max(cont, key=lambda kk: (cont[kk], ultima[kk]))
    p = tam_zona(k) / 37.0
    esperado = n * p
    sd = math.sqrt(n * p * (1 - p))
    zs = (cont[k] - esperado) / sd if sd > 0 else 0.0
    return {"zona": k, "cuenta": cont[k], "n": n, "esperado": esperado, "z": zs, "cont": cont}


def desplazamiento_zona(k: int) -> int:
    """Casillas (hacia el lado de S2..S6 respecto del número anterior) hasta el centro de la zona k. S1 = 0."""
    return 0 if k == 1 else S1_RADIO + 1 + (k - 2) * SECTOR_EXTRA_TAM + (SECTOR_EXTRA_TAM // 2 - 1)


def txt_desplazamiento(k: int) -> str:
    d = desplazamiento_zona(k)
    return "en el mismo sector del número anterior" if d == 0 else f"a {d} casillas en sentido {SENTIDO_TXT} del número anterior"


def contar_zonas(sesiones: list) -> dict:
    cont = {z: 0 for z in range(1, SECTORES_EXTRA + 2)}
    for ses in sesiones:
        for z in zonas_de_giros(list(ses)):
            cont[z] += 1
    return cont


def share_zona(sesiones: list, k: int):
    cont = contar_zonas(sesiones)
    n = sum(cont.values())
    return (cont[k] / n if n else 0.0), n


def resumen_zonas(cont: dict, ultimas: list = None) -> Optional[dict]:
    n = sum(cont.values())
    if not n:
        return None
    partes = " · ".join(f"S{k}: {c} ({c / n * 100:.0f}%)" for k, c in cont.items())
    return {"n": n, "partes": partes,
            "secuencia": "-".join(f"S{k}" for k in ultimas) if ultimas else ""}


def construir_sectores(centro: int) -> list:
    """S1 = centro ± 3 (7 números); S2..S6 = 6 números cada uno en sentido horario desde S1."""
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
    7 acumula más caídas; desempate: centro con más caídas, luego la caída más reciente), arma S2..S6 en sentido horario y cuenta aciertos por sector.
    Devuelve None si no hay datos suficientes.
    """
    if len(numeros) < 4:
        return None
    ventana = numeros[-RONDAS_ANALISIS:]
    rondas = len(ventana)
    conteo = [0] * 37
    for n in ventana:
        conteo[n] += 1
    # centro: posición de rueda cuya ventana de ±3 acumula más caídas.
    # Desempate (muy frecuente con 34 rondas): 1) más caídas en el propio centro, 2) caída más reciente dentro de la ventana.
    ultima = [-1] * 37
    for j, n in enumerate(ventana):
        ultima[n] = j
    mejor_clave, mejor_idx = None, 0
    for i in range(L_RUEDA):
        nums = [WHEEL_ORDER[(i + d) % L_RUEDA] for d in range(-S1_RADIO, S1_RADIO + 1)]
        clave = (sum(conteo[n] for n in nums), conteo[WHEEL_ORDER[i]], max(ultima[n] for n in nums))
        if mejor_clave is None or clave > mejor_clave:
            mejor_clave, mejor_idx = clave, i
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


def analisis_crupier(sesiones: list, max_sesiones: int = 8, max_rondas: int = 400) -> Optional[dict]:
    """
    Análisis del sesgo de UN crupier sobre todas sus sesiones (archivadas + la actual si se la pasa).
    Repite lo que hace el bot en vivo: en cada giro (con al menos SESGO_MIN_RONDAS previos) calcula el centro de S1
    con las 34 rondas anteriores, arma S2..S6 en sentido horario y mide dónde cae el giro SIGUIENTE:
      - reparto por sector S1..S6 (azar: S1 19% · S2..S6 16% c/u)
      - acierto al 1er intento con centro ± 6 y ± 8 vecinos (azar 35% / 46%)
    """
    sector = [0] * (SECTORES_EXTRA + 1)
    hit = {v: 0 for v in COBERTURAS}
    n = 0
    for ses in sesiones[-max_sesiones:]:
        ses = list(ses)[-max_rondas:]
        for t in range(SESGO_MIN_RONDAS, len(ses)):
            a = analizar_sesgo(ses[max(0, t - RONDAS_ANALISIS):t])
            if a is None:
                continue
            x = ses[t]
            n += 1
            for i, sec in enumerate(a["sectores"]):
                if x in sec:
                    sector[i] += 1
                    break
            for v in COBERTURAS:
                if x in cobertura_numeros(a["centro"], v):
                    hit[v] += 1
    if n == 0:
        return None
    return {"n": n, "sector": sector, "hit": hit}


def linea_analisis_crupier(nombre: str, sesiones: list, sesion_actual: list = None) -> str:
    """Una línea de texto con el análisis S1..S6 de un crupier (sesiones archivadas + sesión en curso)."""
    todas = [list(x) for x in sesiones]
    if sesion_actual:
        todas.append(list(sesion_actual))
    total_rondas = sum(len(x) for x in todas)
    an = analisis_crupier(todas)
    base = f"  - {esc(nombre)}: {len(sesiones)} sesión(es) archivada(s), {total_rondas} rondas"
    if an is None:
        return base + " | análisis: pocos datos todavía"
    reparto = " · ".join(f"S{i + 1} {c / an['n'] * 100:.0f}%" for i, c in enumerate(an["sector"]))
    aciertos = " · ".join(f"{v}v {an['hit'][v] / an['n'] * 100:.1f}% (azar {(2 * v + 1) / 37 * 100:.0f}%)"
                          for v in COBERTURAS)
    return f"{base}\n      {reparto}\n      1er intento: {aciertos} sobre {an['n']} giros"


# ══════════════════════════════════════════════
#  ESTRATEGIA "desvio": desviación respecto de la S1 predicha + árboles de decisión
# ══════════════════════════════════════════════
# Idea: en cada ronda hay una S1 predicha (base = ventana más caliente de las 34 rondas previas). Cuando sale el número
# se anota en qué zona Sx cayó respecto de esa S1 (esa es la "desviación"). Con las últimas 34 desviaciones se estima
# qué zona suele salir más respecto de la predicha y se corre la predicción hacia allí. Los 37 números son candidatos a
# centro de la nueva S1: gana el que más probabilidad acumula en su cobertura (centro ± vecinos).
_COB = {v: [cobertura_numeros(c, v) for c in range(37)] for v in {S1_RADIO, *COBERTURAS}}
_DIST = [[min(abs(WHEEL_POS[a] - WHEEL_POS[b]), L_RUEDA - abs(WHEEL_POS[a] - WHEEL_POS[b])) for b in range(37)]
         for a in range(37)]


def traza_vacia() -> dict:
    return {"bases": [], "hits": [], "devs": []}


def extender_traza(spins: list, traza: dict) -> dict:
    """
    Alinea la traza con la sesión: para cada giro i guarda la S1 base que se predijo ANTES de que saliera (con las 34
    rondas previas), los aciertos de esa S1 y la desviación (zona 1..6 donde cayó el número respecto de esa S1).
    """
    b, h, d = traza["bases"], traza["hits"], traza["devs"]
    if len(b) > len(spins):
        b.clear(); h.clear(); d.clear()
    for i in range(len(b), len(spins)):
        a = analizar_sesgo(spins[max(0, i - RONDAS_ANALISIS):i]) if i >= SESGO_MIN_RONDAS else None
        if a:
            b.append(a["centro"]); h.append(a["hits"][0]); d.append(zona_de(a["centro"], spins[i]))
        else:
            b.append(None); h.append(None); d.append(None)
    return traza


def feats_desvio(devs_prev: list, base: int, ult1: int, ult2: Optional[int], hits_base: int) -> list:
    """Variables para los árboles: reparto de desviaciones (34 y 10 rondas), últimas 5 desviaciones y posición de los
    últimos números respecto de la S1 base."""
    w34 = [x for x in devs_prev[-DESVIO_VENTANA:] if x]
    w10 = [x for x in devs_prev[-10:] if x]
    f = []
    for w in (w34, w10):
        n = len(w)
        f += [(w.count(k) / n) if n else 0.0 for k in range(1, 7)]
    ult = list(devs_prev[-5:])
    f += [0] * (5 - len(ult)) + [(x or 0) for x in ult]
    off = lambda n: ((WHEEL_POS[n] - WHEEL_POS[base]) * WHEEL_DIRECTION) % L_RUEDA if n is not None else -1
    f += [len(w34), hits_base, zona_de(base, ult1), off(ult1), off(ult2), WHEEL_POS[base]]
    return f


def filas_desvio(spins: list, traza: dict):
    """Filas de entrenamiento (X, y, meta) de una sesión. y = desviación real (1..6); meta = (S1 base, número que salió)."""
    X, y, meta = [], [], []
    b, h, d = traza["bases"], traza["hits"], traza["devs"]
    for i in range(min(len(spins), len(b))):
        if b[i] is None:
            continue
        prev = d[max(0, i - DESVIO_VENTANA):i]
        if sum(1 for x in prev if x) < DESVIO_MIN_HIST:
            continue
        X.append(feats_desvio(prev, b[i], spins[i - 1], spins[i - 2] if i >= 2 else None, h[i]))
        y.append(d[i])
        meta.append((b[i], spins[i]))
    return X, y, meta


def probs_frecuencia(devs_prev: list) -> list:
    """Método sin ML: reparto de las últimas 34 desviaciones, suavizado hacia lo esperado por azar."""
    w = [x for x in devs_prev[-DESVIO_VENTANA:] if x]
    n = len(w)
    return [(w.count(k) + DESVIO_SUAVIZADO * tam_zona(k) / 37.0) / (n + DESVIO_SUAVIZADO) for k in range(1, 7)]


def proba6(clf, X: list) -> list:
    """Probabilidades de las 6 zonas (aunque el árbol no haya visto alguna) para cada fila de X."""
    out = []
    for fila in clf.predict_proba(X):
        P = [1e-3] * 6
        for cls, v in zip(clf.classes_, fila):
            P[int(cls) - 1] = max(float(v), 1e-3)
        t = sum(P)
        out.append([x / t for x in P])
    return out


def elegir_centro(base: int, P: list, vecinos: int = None):
    """
    Con P = probabilidad de cada zona (medida desde la S1 base), reparte esa probabilidad entre los números de cada zona y
    prueba LOS 37 NÚMEROS como centro de la nueva S1: gana el de mayor probabilidad en su cobertura (centro ± vecinos).
    Devuelve (centro, probabilidad_de_cobertura, probabilidad_por_número).
    """
    v = COBERTURA_ENVIO if vecinos is None else vecinos
    p = [0.0] * 37
    for k, nums in enumerate(construir_sectores(base)):
        for n in nums:
            p[n] = P[k] / len(nums)
    mejor, mejor_clave = None, None
    for c in range(37):
        cov = sum(p[n] for n in _COB[v][c])
        s1 = sum(p[n] for n in _COB[S1_RADIO][c])
        clave = (round(cov, 9), round(s1, 9), -_DIST[c][base])   # desempate: más masa en S1, luego el más cercano a la base
        if mejor_clave is None or clave > mejor_clave:
            mejor, mejor_clave = c, clave
    return mejor, mejor_clave[0], p


def entrenar_modelo_desvio(X: list, y: list, meta: list) -> Optional[dict]:
    """
    Árboles de decisión (bosque aleatorio) que aprenden, para UN crupier, en qué zona cae el siguiente número respecto de
    la S1 base. Se valida fuera de muestra (último 25 % cronológico): % de veces que el número siguiente cae en la cobertura
    del centro elegido, frente a usar solo la S1 base sin corrección.
    """
    n = len(y)
    if not SKLEARN_OK or n < DESVIO_MIN_TRAIN:
        return None

    def nuevo():
        return RandomForestClassifier(n_estimators=60, max_depth=4, min_samples_leaf=10, max_features="sqrt",
                                      random_state=7, n_jobs=1)
    try:
        corte = int(n * 0.75)
        m = nuevo().fit(X[:corte], y[:corte])
        Xv, mv = X[corte:][-300:], meta[corte:][-300:]
        cob = _COB[COBERTURA_ENVIO]
        ok_ml = ok_base = 0
        for P, (base, real) in zip(proba6(m, Xv), mv):
            c, _, _ = elegir_centro(base, P)
            ok_ml += real in cob[c]
            ok_base += real in cob[base]
        final = nuevo().fit(X, y)
    except Exception as e:
        log.warning(f"[Desvío] No se pudo entrenar el modelo: {e}")
        return None
    nv = max(1, len(mv))
    return {"clf": final, "n": n, "val_n": len(mv), "val_hit": ok_ml / nv, "val_base": ok_base / nv}


# ══════════════════════════════════════════════
#  MENSAJES
# ══════════════════════════════════════════════
def build_entrada_message(sig: dict, ultimo_numero, intento: int) -> str:
    color_emoji = {"ROJO": "🔴", "NEGRO": "⚫", "VERDE": "🟢"}
    numero = ultimo_numero if ultimo_numero is not None else "-"
    numero_emoji = color_emoji.get(color_of(ultimo_numero), "🟢") if ultimo_numero is not None else ""
    centro = sig["centro"]
    centro_emoji = color_emoji.get(color_of(centro), "🟢")
    cobertura = cobertura_numeros(centro, COBERTURA_ENVIO)
    ficha = SESGO_CHIP_VALUE * fib_mult(intento)
    total = ficha * len(cobertura)
    crupier = esc(sig.get("crupier") or "N/A")
    link = f'<a href="{TABLE_LINK}">{esc(TABLE_NAME)}</a>' if TABLE_LINK else esc(TABLE_NAME)

    extra_vecinos = ""
    extra_analisis = ""
    extra_fib = ""
    extra_recalc = ""
    cs = sig.get("centros") or []
    if intento > 1 and len(cs) >= 2:
        ant, nuevo = cs[-2], cs[-1]
        extra_recalc = (f"🔁 S1 recalculado: centro {ant} → {nuevo}\n" if ant != nuevo
                        else f"🔁 S1 recalculado: el centro se mantiene en {nuevo}\n")
    if SENAL_DETALLE:
        atras = " - ".join(str(n) for n in cobertura[:COBERTURA_ENVIO])       # lado contrario al avance de S1→S6
        adelante = " - ".join(str(n) for n in cobertura[COBERTURA_ENVIO + 1:])  # lado hacia donde avanzan S2..S6
        extra_vecinos = f"   ◀ {SENTIDO_OPUESTO_TXT}: {atras} | {centro} | {adelante} :{SENTIDO_TXT} ▶\n"
        if sig.get("modo") == "desvio":
            d = sig.get("desvio") or {}
            azar_c = (2 * COBERTURA_ENVIO + 1) / 37 * 100
            if sig["confirmada"]:
                conf = (f"\n✅ Validación fuera de muestra: {sig['sim']*100:.0f}% al 1er intento en {sig['sim_n']} giros "
                        f"(azar {azar_c:.0f}%)")
            else:
                conf = "\n⚠️ SIN CONFIRMACIÓN (la validación fuera de muestra de este crupier no supera al azar)"
            metodo = "árboles de decisión (ML)" if d.get("metodo") == "arboles" else "frecuencia de desviaciones"
            extra_analisis = (f"\n🧭 Desviación dominante S{d.get('dom_zona', '?')} respecto a la S1 predicha "
                              f"({d.get('dom_cuenta', '?')} de {d.get('n', '?')} rondas, esperado {d.get('dom_esp', 0):.1f})"
                              f"\n🎯 S1 base {d.get('base', '?')} → nueva S1 {sig['centro']} | prob. de caer en los "
                              f"{2 * COBERTURA_ENVIO + 1} números: {d.get('prob_cov', 0) * 100:.0f}% (azar {azar_c:.0f}%)"
                              f"\n🤖 Método: {metodo}\n🔄 Giro: {GIRO_TXT}{conf}\n")
        elif sig.get("zona"):
            k = sig["zona"]
            if sig["confirmada"]:
                conf = (f"\n✅ Histórico: S{k} sale {sig['sim']*100:.0f}% en {sig['sim_n']} rondas previas "
                        f"del crupier (azar {tam_zona(k)/37*100:.0f}%)")
            else:
                conf = "\n⚠️ SIN CONFIRMACIÓN (el histórico de este crupier no respalda esa zona)"
            dom = sig.get("dom") or {}
            extra_analisis = (f"\n📍 Zona dominante S{k} ({dom.get('cuenta', '?')} de {dom.get('n', '?')} rondas, "
                              f"esperado {dom.get('esperado', 0):.1f}) → {txt_desplazamiento(k)} ({sig['ref']})"
                              f"\n🔄 Giro: {GIRO_TXT}{conf}\n")
        else:
            if not sig["confirmada"]:
                conf = "\n⚠️ SIN CONFIRMACIÓN (la sesión actual discrepa de las sesiones pasadas de este crupier)"
            else:
                conf = f"\n✅ Confirmación histórica: {sig['sim']*100:.0f}% ({sig['sim_n']} sesión(es) previa(s))"
            extra_analisis = (f"\n📊 Aciertos S1 últimas {sig['rondas']}: {sig['hits'][0]} "
                              f"(esperados {sig['esperados'][0]:.1f}){conf}\n")
        extra_fib = "📈 Gestión FIBONACCI (1·1·2·3·5·8·13·21): ganá 1 vez y reseteá la gestión\n"

    return (f"🚨🚨 ENTRADA INTENTO {intento} 🚨🚨\n\n"
            f"👤 NAME CRUPIER: {crupier}\n"
            f"👉 INGRESAR DESPUÉS: {numero} ({numero_emoji})\n"
            f"🧨 CUBRIR {COBERTURA_ENVIO} VECINOS: {centro} ({centro_emoji})\n"
            f"{extra_recalc}"
            f"{extra_vecinos}"
            f"{extra_analisis}\n"
            f"🇨🇴 VALOR DE FICHA: ${ficha:,} COP\n"
            f"🇨🇴 APUESTA TOTAL: ${total:,} COP\n"
            f"{extra_fib}\n"
            f"💫 ¡Juego Responsable!\n"
            f"🎮 RULETA: {link}")


def build_resolucion_message(win: bool, sig: dict) -> str:
    numeros_str = " | ".join(str(n) for n in sig["numeros"])
    header = "✅✅ SEÑAL 👍🏻" if win else "❌❌ SEÑAL 👎🏻"
    conf = "" if sig["confirmada"] else " ⚠️(sin confirmación)"
    return (f"{header}{conf} ({numeros_str}) | Centro {sig['centro']} | "
            f"Crupier {esc(sig['crupier'])} | Intento {sig['intento']}")


# ══════════════════════════════════════════════
#  MESA
# ══════════════════════════════════════════════
class RouletteTable:
    def __init__(self, key: int):
        self.key = key
        self.spin_history = []
        self.ultima_ronda: Optional[dict] = None   # {numero,color,crupier,sesion_num,ts} de la última ronda registrada
        self.total_spins_seen = 0
        self.live_spins_seen = 0

        # ── Registro por crupier ──
        self.crupier_actual: Optional[str] = None
        self.sesion_actual: list = []                 # giros de la sesión del crupier actual
        self.sesiones_por_crupier: dict = {}          # {nombre: [sesión1, sesión2, ...]}

        # ── Estrategia "desvio" ──
        self.traza: dict = traza_vacia()              # S1 base predicha + desviación de cada giro de la sesión actual
        self.pred_desvio: Optional[dict] = None       # predicción vigente (se recalcula en cada giro)
        self.modelos: dict = {}                       # {crupier: modelo de árboles entrenado}
        self._ultimo_entreno: dict = {}               # {crupier: total_spins_seen del último entrenamiento}
        self._cache_traza: dict = {}                  # trazas de sesiones archivadas
        self.descartes: dict = {}                     # {motivo: veces} señales candidatas descartadas por el filtro

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
        self.traza = traza_vacia()
        self.pred_desvio = None
        # Anular señal activa: cambió el crupier, la sesión ya no aplica
        if (self.senal_activa is not None and self.senal_activa.get("sent")
                and not self.senal_activa.get("envio_cerrado")):
            asyncio.create_task(send_msg(f"🚫 Señal anulada: cambio de crupier a {esc(nombre)}", CANAL_SENALES))
        self.senal_activa = None
        log.info(f"[Crupier] Mesa {self.key}: ahora atiende {nombre} (nueva sesión)")

    def _archivar_sesion_actual(self):
        if self.crupier_actual and len(self.sesion_actual) > 0:
            self.sesiones_por_crupier.setdefault(self.crupier_actual, []).append(
                list(self.sesion_actual[-MAX_SPINS_SESION_MEMORIA:]))
            self.sesiones_por_crupier[self.crupier_actual] = \
                self.sesiones_por_crupier[self.crupier_actual][-MAX_SESIONES_X_CRUPIER:]

    def resultados_modo(self) -> list:
        """Resultados de la estrategia activa (no se mezclan con los de otra estrategia)."""
        return [x for x in self.resultados if x.get("modo", "masa") == ESTRATEGIA]

    def _num_sesion(self) -> int:
        if not self.crupier_actual:
            return 1
        return len(self.sesiones_por_crupier.get(self.crupier_actual, [])) + 1

    # ── gate de envío ──────────────────────────────────────────
    def _gate_ok(self) -> bool:
        r = self.resultados_modo()[-SESGO_STATS_WINDOW:]
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

    # ── estrategia "desvio" ────────────────────────────────────
    def _filas_entrenamiento(self, nombre: str):
        """Filas (X, y, meta) del crupier: sesiones archivadas recientes + sesión en curso."""
        X, y, meta = [], [], []
        vivos = {}
        for ses in self.sesiones_por_crupier.get(nombre, [])[-DESVIO_SESIONES_ENTRENO:]:
            ent = self._cache_traza.get(id(ses))
            if ent is None or ent[0] is not ses or ent[1] != len(ses):
                corte = ses[-DESVIO_SPINS_X_SESION:]
                ent = (ses, len(ses), corte, extender_traza(corte, traza_vacia()))
            vivos[id(ses)] = ent
            fx, fy, fm = filas_desvio(ent[2], ent[3])
            X += fx; y += fy; meta += fm
        self._cache_traza = vivos
        if nombre == self.crupier_actual:
            extender_traza(self.sesion_actual, self.traza)
            fx, fy, fm = filas_desvio(self.sesion_actual, self.traza)
            X += fx; y += fy; meta += fm
        return X[-DESVIO_MAX_FILAS:], y[-DESVIO_MAX_FILAS:], meta[-DESVIO_MAX_FILAS:]

    def _asegurar_modelo(self):
        """Entrena (o reentrena cada DESVIO_REENTRENAR_CADA giros) los árboles del crupier actual."""
        nombre = self.crupier_actual
        if nombre is None or not SKLEARN_OK:
            return
        ult = self._ultimo_entreno.get(nombre)
        if ult is not None and self.total_spins_seen - ult < DESVIO_REENTRENAR_CADA:
            return
        self._ultimo_entreno[nombre] = self.total_spins_seen
        t0 = time.time()
        X, y, meta = self._filas_entrenamiento(nombre)
        mod = entrenar_modelo_desvio(X, y, meta)
        if mod:
            self.modelos[nombre] = mod
            log.info(f"[Desvío] Árboles de {nombre}: {mod['n']} filas | validación fuera de muestra "
                     f"{mod['val_hit']*100:.1f}% vs base {mod['val_base']*100:.1f}% en {mod['val_n']} giros "
                     f"({time.time() - t0:.2f}s)")
        else:
            log.info(f"[Desvío] {nombre}: {len(y)} filas (< {DESVIO_MIN_TRAIN}), se usa el método por frecuencia")

    def _calcular_desvio(self) -> Optional[dict]:
        """Predice la nueva S1 con las últimas 34 desviaciones del crupier (se llama después de cada giro)."""
        nombre = self.crupier_actual
        spins = self.sesion_actual
        if nombre is None or len(spins) < SESGO_MIN_RONDAS:
            return None
        extender_traza(spins, self.traza)            # incluye la desviación del giro que acaba de salir
        a = analizar_sesgo(spins)
        if a is None:
            return None
        base = a["centro"]                            # S1 predicha por la ventana caliente (referencia de las desviaciones)
        self._asegurar_modelo()
        prev = self.traza["devs"][-DESVIO_VENTANA:]
        P, metodo = probs_frecuencia(prev), "frecuencia"
        mod = self.modelos.get(nombre)
        if mod is not None:
            try:
                x = feats_desvio(prev, base, spins[-1], spins[-2] if len(spins) >= 2 else None, a["hits"][0])
                P, metodo = proba6(mod["clf"], [x])[0], "arboles"
            except Exception as e:
                log.warning(f"[Desvío] Falló la predicción con árboles ({e}); se usa frecuencia")
        centro, prob_cov, _ = elegir_centro(base, P)
        w = [x for x in prev if x]
        cnt = [w.count(k) for k in range(1, 7)]
        esp = [len(w) * tam_zona(k) / 37.0 for k in range(1, 7)]
        kd = max(range(6), key=lambda i: cnt[i] - esp[i])
        return {"centro": centro, "base": base, "P": P, "prob_cov": prob_cov, "metodo": metodo, "n": len(w),
                "dom_zona": kd + 1, "dom_cuenta": cnt[kd], "dom_esp": esp[kd], "hits_base": a["hits"][0]}

    def _val_desvio(self):
        """(confirmada, val_hit, val_n): ¿la validación fuera de muestra de los árboles de este crupier supera al azar?"""
        mod = self.modelos.get(self.crupier_actual) or {}
        azar = (2 * COBERTURA_ENVIO + 1) / 37.0
        val_hit, val_n = mod.get("val_hit"), mod.get("val_n", 0)
        ok = bool(val_hit is not None and val_n >= DESVIO_MIN_VAL and val_hit >= azar + DESVIO_MARGEN_VAL)
        return ok, val_hit, val_n

    def _rendimiento_vivo(self):
        """(aciertos al 1er intento, señales) de las últimas señales cerradas de este crupier con la estrategia desvío."""
        r = [x for x in self.resultados if x.get("modo") == "desvio" and x.get("crupier") == self.crupier_actual]
        r = r[-DESVIO_LIVE_N:]
        return sum(1 for x in r if x.get(f"i{COBERTURA_ENVIO}") == 1), len(r)

    def _filtro_desvio(self, p, reintento: bool = False) -> Optional[str]:
        """
        FILTRO DE DESCARTE. Devuelve None si la señal pasa, o el motivo por el que se descarta:
          - pocos datos            : menos de DESVIO_MIN_HIST desviaciones en la ventana de 34
          - sin modelo de árboles  : el crupier aún no tiene árboles entrenados (DESVIO_REQUIERE_ARBOLES)
          - probabilidad baja      : prob. de cubrir el próximo número < DESVIO_MIN_PROB (al abrir) / DESVIO_MIN_PROB_REINTENTO (reintento)
          - validación no supera al azar : la validación fuera de muestra del crupier no supera al azar (DESVIO_SOLO_CONFIRMADAS)
          - rendimiento en vivo bajo     : sus últimas señales aciertan al 1er intento menos que el azar (solo al abrir)
        """
        if p is None:
            return "sin predicción"
        if p["n"] < DESVIO_MIN_HIST:
            return "pocos datos"
        if DESVIO_REQUIERE_ARBOLES and p["metodo"] != "arboles":
            return "sin modelo de árboles"
        if p["prob_cov"] < (DESVIO_MIN_PROB_REINTENTO if reintento else DESVIO_MIN_PROB):
            return "probabilidad baja"
        if DESVIO_SOLO_CONFIRMADAS and not self._val_desvio()[0]:
            return "validación no supera al azar"
        if not reintento:
            ok, n = self._rendimiento_vivo()
            if n >= DESVIO_LIVE_MIN and ok / n < (2 * COBERTURA_ENVIO + 1) / 37.0:
                return "rendimiento en vivo bajo"
        return None

    def _contar_descarte(self, motivo: str):
        self.descartes[motivo] = self.descartes.get(motivo, 0) + 1

    def _descartar_senal(self, sig: dict, motivo: str):
        """Descarta una señal ya abierta (en un reintento): se deja de apostar y se archiva como pérdida de esa cadena."""
        fallidos = sig["intento"] - 1
        self._contar_descarte(f"en reintento: {motivo}")
        if sig["sent"] and not sig["envio_cerrado"]:
            asyncio.create_task(send_msg(
                f"🚫 SEÑAL DESCARTADA tras {fallidos} intento(s): {motivo}.\n"
                f"No apuestes más y reseteá la gestión. Crupier {esc(sig['crupier'])}", CANAL_SENALES))
        reg = {"win": False, "intento": None, "centro": sig["centro"], "sent": sig["sent"], "crupier": sig["crupier"],
               "confirmada": sig["confirmada"], "sim": sig["sim"], "ts": time.time(), "cob_envio": COBERTURA_ENVIO,
               "modo": "desvio", "zona": None, "descartada": motivo}
        for v in COBERTURAS:
            reg[f"i{v}"] = sig["hit"][v]
        self.resultados.append(reg)
        log.info(f"🚫 Señal descartada tras {fallidos} intento(s): {motivo} | centro {sig['centro']} | crupier {sig['crupier']}")
        self.senal_activa = None

    def _abrir_desvio(self, ultimo_numero):
        p = self.pred_desvio
        motivo = self._filtro_desvio(p)
        if motivo:
            self._contar_descarte(motivo)
            log.debug(f"[Desvío] Candidata descartada: {motivo}")
            return
        confirmada, val_hit, val_n = self._val_desvio()
        sig = {
            "modo": "desvio", "crupier": self.crupier_actual, "sesion_num": self._num_sesion(),
            "centro": p["centro"], "centros": [p["centro"]], "sectores": construir_sectores(p["centro"]),
            "hits": None, "esperados": None, "rondas": p["n"], "confirmada": confirmada,
            "sim": val_hit, "sim_n": val_n, "desvio": p,
            "intento": 1, "numeros": [], "msg_id": None, "msg_id_anterior": None,
            "sent": self._gate_ok(),
            "hit": {v: None for v in COBERTURAS}, "envio_cerrado": False, "intento_envio": None,
        }
        self.senal_activa = sig
        log.info(f"🎯 SEÑAL DESVÍO: base {p['base']} → S1 {p['centro']} | desviación dominante S{p['dom_zona']} "
                 f"({p['dom_cuenta']}/{p['n']}) | prob. cobertura {p['prob_cov']*100:.0f}% | {p['metodo']} | "
                 f"crupier {sig['crupier']} | confirmada={confirmada} | {'ENVIADA' if sig['sent'] else 'sombra'}")
        if sig["sent"]:
            asyncio.create_task(self._enviar_entrada(sig, ultimo_numero, 1))

    def _abrir_zonas(self, ultimo_numero):
        """S1 adaptativo: se toma la zona que MÁS SALE (cada ronda medida contra el número anterior) y se predice esa
        misma zona respecto del último número; se cubre su centro con 6 u 8 vecinos. Solo hay señal si la zona
        dominante supera claramente al azar (ZONA_MIN_CUENTA y ZONA_Z_MIN)."""
        zonas = zonas_de_giros(self.sesion_actual)
        dom = zona_dominante(zonas)
        if dom is None or dom["cuenta"] < ZONA_MIN_CUENTA or dom["z"] < ZONA_Z_MIN:
            return
        k = dom["zona"]
        ref = self.sesion_actual[-1]
        centro = centro_de_zona(ref, k)
        # Confirmación: en las sesiones pasadas del crupier, ¿la zona k sale más de lo esperado por azar?
        pasadas = self.sesiones_por_crupier.get(self.crupier_actual, [])
        share, n_hist = share_zona(pasadas, k)
        confirmada = n_hist >= ZONA_HIST_MIN and share >= tam_zona(k) / 37.0
        sig = {
            "crupier": self.crupier_actual, "sesion_num": self._num_sesion(),
            "centro": centro, "centros": [centro], "sectores": construir_sectores(ref),
            "hits": None, "esperados": None, "rondas": dom["n"], "confirmada": confirmada,
            "sim": share, "sim_n": n_hist, "zona": k, "ref": ref, "dom": dom,
            "intento": 1, "numeros": [], "sent": self._gate_ok(), "msg_id": None, "msg_id_anterior": None,
            "hit": {v: None for v in COBERTURAS}, "envio_cerrado": False, "intento_envio": None,
        }
        self.senal_activa = sig
        log.info(f"🎯 SEÑAL ZONA DOMINANTE S{k} ({dom['cuenta']}/{dom['n']}, esperado {dom['esperado']:.1f}, z={dom['z']:.1f}) "
                 f"desde {ref} → centro {centro} | crupier {sig['crupier']} sesión #{sig['sesion_num']} | "
                 f"confirmada={confirmada} (hist S{k} {share*100:.0f}% en {n_hist} rondas) | "
                 f"{'ENVIADA' if sig['sent'] else 'sombra'}")
        if sig["sent"]:
            asyncio.create_task(self._enviar_entrada(sig, ultimo_numero, 1))

    def _intentar_abrir(self, ultimo_numero):
        if self.senal_activa is not None or self.crupier_actual is None:
            return
        if ESTRATEGIA == "desvio":
            return self._abrir_desvio(ultimo_numero)
        if ESTRATEGIA == "zonas":
            return self._abrir_zonas(ultimo_numero)
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
            "centro": analisis["centro"], "centros": [analisis["centro"]], "sectores": analisis["sectores"],
            "hits": analisis["hits"], "esperados": analisis["esperados"],
            "rondas": analisis["rondas"], "confirmada": confirmada,
            "sim": sim, "sim_n": sim_n,
            "intento": 1, "numeros": [], "sent": self._gate_ok(), "msg_id": None, "msg_id_anterior": None,
            "hit": {v: None for v in COBERTURAS},   # intento en que acertó cada cobertura (None = todavía no)
            "envio_cerrado": False, "intento_envio": None,
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
        intento = sig["intento"]
        # Se mide cada cobertura por separado (centro ± 6 y centro ± 8) con el centro de ESTE intento
        for v in COBERTURAS:
            if sig["hit"][v] is None and number in cobertura_numeros(sig["centro"], v):
                sig["hit"][v] = intento
        win_envio = sig["hit"][COBERTURA_ENVIO] is not None
        agotado = intento >= SESGO_MAX_INTENTOS

        # Cierre de la señal "operativa" (la cobertura que se apuesta / se envía al canal)
        if not sig["envio_cerrado"] and (win_envio or agotado):
            sig["envio_cerrado"] = True
            sig["intento_envio"] = intento
            if sig["sent"]:
                if sig["confirmada"] and self.last_sinconf_msg_id == sig.get("msg_id"):
                    self.last_sinconf_msg_id = None
                asyncio.create_task(send_msg(build_resolucion_message(win_envio, sig), CANAL_SENALES))
            log.info(f"🎯 Señal {COBERTURA_ENVIO}v cerrada: {'WIN' if win_envio else 'LOSS'} intento {intento} | "
                     f"centro {sig['centro']} | salió {number} | crupier {sig['crupier']}")

        # La señal se archiva cuando ambas coberturas quedaron resueltas (o se agotaron los intentos)
        if agotado or all(sig["hit"][v] is not None for v in COBERTURAS):
            reg = {"win": win_envio, "intento": sig["intento_envio"], "centro": sig["centro"],
                   "sent": sig["sent"], "crupier": sig["crupier"],
                   "confirmada": sig["confirmada"], "sim": sig["sim"], "ts": time.time(),
                   "cob_envio": COBERTURA_ENVIO,
                   "modo": sig.get("modo") or ("zonas" if sig.get("zona") else "masa"), "zona": sig.get("zona")}
            for v in COBERTURAS:
                reg[f"i{v}"] = sig["hit"][v]      # intento del acierto, o None si falló en todos
            self.resultados.append(reg)
            log.info("🎯 Señal archivada | " + " | ".join(
                f"{v}v: " + (f"acierto I{sig['hit'][v]}" if sig["hit"][v] else "fallo") for v in COBERTURAS))
            self.senal_activa = None
            return

        # Siguiente intento: se recalcula S1 con la sesión actualizada
        sig["intento"] += 1
        sig["msg_id_anterior"] = sig.get("msg_id")
        if sig.get("modo") == "desvio":
            # Se recalculan las desviaciones de las últimas 34 rondas (ya incluyen el giro que acaba de salir) y la nueva S1
            centro_previo = sig["centro"]
            p = self.pred_desvio
            if p is not None:
                sig["centro"], sig["desvio"], sig["rondas"] = p["centro"], p, p["n"]
                sig["sectores"] = construir_sectores(p["centro"])
            sig.setdefault("centros", [centro_previo]).append(sig["centro"])
            if DESVIO_DESCARTAR_EN_INTENTOS and not sig["envio_cerrado"]:
                motivo = self._filtro_desvio(p, reintento=True)
                if motivo:
                    self._descartar_senal(sig, motivo)
                    return
            log.info(f"🎯 Intento {sig['intento']}: desviación dominante S{(p or {}).get('dom_zona', '?')} → nueva S1 "
                     f"{centro_previo} → {sig['centro']}" + (" (sin cambio)" if sig["centro"] == centro_previo else " (se movió)"))
        elif sig.get("zona"):
            # S1 adaptativo: se recalcula la zona dominante (con la ronda nueva) y se mide desde el número que acaba de salir
            dom = zona_dominante(zonas_de_giros(self.sesion_actual))
            if dom is not None:
                sig["zona"], sig["dom"] = dom["zona"], dom
            sig["ref"] = number
            sig["centro"] = centro_de_zona(number, sig["zona"])
            sig.setdefault("centros", []).append(sig["centro"])
            sig["sectores"] = construir_sectores(number)
            log.info(f"🎯 Intento {sig['intento']}: zona dominante S{sig['zona']} desde {number} → centro {sig['centro']}")
        else:
            centro_previo = sig["centro"]
            analisis = analizar_sesgo(self.sesion_actual)   # incluye el giro que acaba de salir
            if analisis:
                sig["centro"], sig["sectores"] = analisis["centro"], analisis["sectores"]
                sig["hits"], sig["esperados"], sig["rondas"] = analisis["hits"], analisis["esperados"], analisis["rondas"]
            sig.setdefault("centros", [centro_previo]).append(sig["centro"])
            log.info(f"🎯 Intento {sig['intento']}: S1 recalculado → centro {centro_previo} → {sig['centro']}"
                     + (" (sin cambio)" if sig["centro"] == centro_previo else " (se movió)")
                     + (f" | S1 {sig['hits'][0]} aciertos/{sig['rondas']}" if sig.get("hits") else ""))
        if sig["sent"] and not sig["envio_cerrado"]:
            asyncio.create_task(self._enviar_entrada(sig, number, sig["intento"]))

    # ── persistencia ──────────────────────────────────────────
    def persist(self) -> dict:
        return {
            "table_total_spins_seen": self.total_spins_seen,
            "crupier_actual": self.crupier_actual,
            "sesion_actual": list(self.sesion_actual[-MAX_SPINS_SESION_MEMORIA:]),
            "sesiones_por_crupier": self.sesiones_por_crupier,
            "resultados": self.resultados,
            "ultima_ronda": self.ultima_ronda,
            "descartes": self.descartes,
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
        self.ultima_ronda = data.get("ultima_ronda")
        self.descartes = dict(data.get("descartes") or {})

    def agregar_seed(self, spins: list):
        """El histórico sin crupier queda como sesión de referencia de un pseudo-crupier."""
        if not spins:
            return
        self.sesiones_por_crupier.setdefault(CRUPIER_SEED_NAME, []).append(list(spins[-SEED_SPIN_CAP:]))

    # ── entrada de giro ───────────────────────────────────────
    def update(self, number: int, real_color: str, timestamp: float = None, training: bool = False):
        if timestamp is None:
            timestamp = time.time()
        prev = self.sesion_actual[-1] if self.sesion_actual else None
        zona = zona_de(prev, number) if prev is not None else None   # zona de esta ronda respecto del nº anterior
        self.spin_history.append({"number": number, "color": real_color, "timestamp": timestamp,
                                  "crupier": self.crupier_actual, "zona": zona})
        if len(self.spin_history) > 200:
            self.spin_history.pop(0)
        self.total_spins_seen += 1
        if not training:
            self.live_spins_seen += 1
        self.sesion_actual.append(number)
        self.ultima_ronda = {"numero": number, "color": real_color, "crupier": self.crupier_actual,
                             "sesion_num": self._num_sesion(), "ts": timestamp,
                             "zona": zona, "ref": prev}
        if len(self.sesion_actual) > MAX_SPINS_SESION_MEMORIA:
            exceso = len(self.sesion_actual) - MAX_SPINS_SESION_MEMORIA
            del self.sesion_actual[:exceso]
            for lista in self.traza.values():
                del lista[:exceso]
        if training:
            return
        if ESTRATEGIA == "desvio":
            try:
                self.pred_desvio = self._calcular_desvio()   # antes de resolver: cada intento usa la S1 recalculada
            except Exception as e:
                log.warning(f"[Desvío] Error calculando la predicción: {e}")
                self.pred_desvio = None
        self._resolver(number)
        self._intentar_abrir(number)
        crupier_txt = self.crupier_actual or "?"
        log.info(f"🎰 Mesa {self.key} | Giro #{self.total_spins_seen}: {number} ({real_color}) | "
                 f"Crupier {crupier_txt} sesión #{self._num_sesion()} | "
                 f"Señal: {'activa (centro %s, intento %s)' % (self.senal_activa['centro'], self.senal_activa['intento']) if self.senal_activa else 'sin señal'}")

    def get_state(self, limit: int = 40):
        r = self.resultados_modo()[-SESGO_STATS_WINDOW:]
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
            "desvio": None if self.pred_desvio is None else {
                k: self.pred_desvio[k] for k in ("base", "centro", "prob_cov", "metodo", "dom_zona", "n")},
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
                "cobertura_envio": COBERTURA_ENVIO,
                "estrategia": ESTRATEGIA,
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
    def __init__(self, key: int, on_spin_callback: Callable[[int, bool, bool], Awaitable[None]],
                 on_dealer_callback: Optional[Callable[[str], None]] = None):
        self.key = key
        self.on_spin_callback = on_spin_callback
        self.on_dealer_callback = on_dealer_callback
        self.seen = set()
        self.ultimo_dealer: Optional[str] = None   # último nombre recibido del servidor

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
                        # Crupier: el servidor lo manda en data["dealer"]["name"]. Se procesa ANTES de los
                        # resultados para que los giros nuevos caigan en la sesión del crupier correcto.
                        dealer = data.get("dealer")
                        dealer_name = normalizar_nombre(dealer.get("name")) if isinstance(dealer, dict) else None
                        if dealer_name and dealer_name != self.ultimo_dealer:
                            self.ultimo_dealer = dealer_name
                            if self.on_dealer_callback:
                                try:
                                    self.on_dealer_callback(dealer_name)
                                except Exception as e:
                                    log.warning(f"[Crupier] Error aplicando dealer '{dealer_name}': {e}")
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

    def set_dealer(self, key: int, nombre: str):
        """Crupier detectado desde el servidor. Solo dispara nueva sesión si realmente cambió."""
        mesa = self.tables.get(key)
        if mesa is not None:
            mesa.cambiar_crupier(nombre)

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
        handler = PragmaticWebSocketHandler(
            key,
            lambda num, emit, training=False, k=key: on_spin(k, num, emit, training),
            on_dealer_callback=lambda nombre, k=key: server_state.set_dealer(k, nombre),
        )
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
