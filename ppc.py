
"""
╔══════════════════════════════════════════════════════════════╗
║   BOT DE SEÑALES — COLUMNAS Y DOCENAS · Roulette 1 (227)      ║
║                                                               ║
║   Cuatro agentes INDEPENDIENTES (cada uno con su propia       ║
║   señal, su prealerta y su gestión Fibonacci):                ║
║     · Agente_Col  : columnas (a, b, b, c, a)  → busca b       ║
║     · Agente_Doc  : docenas  (a, b, b, c, a)  → busca b       ║
║     · Agente_Col2 : columnas (a, a, b, a, a)  → busca b       ║
║     · Agente_Doc2 : docenas  (a, a, b, a, a)  → busca b       ║
║   Cada letra es una columna/docena; el orden es el de las     ║
║   últimas 5 rondas (la más antigua primero).                  ║
║                                                               ║
║   Flujo:                                                      ║
║   1. Si las últimas 4 rondas son el inicio del patrón         ║
║      (a,b,b,c) o (a,a,b,a) → PREALERTA: "debe salir a".      ║
║   2. Sale a → se borra la prealerta y se envía la SEÑAL a b.  ║
║      No sale a → se borra la prealerta.                       ║
║   3. Fibonacci (1,1,2,3,5,8…) sobre ficha de 50 COP hasta que ║
║      salga b. Cada intento borra el mensaje anterior y envía  ║
║      el nuevo con la ficha actualizada.                       ║
║   4. Pago 2:1 (una sola casilla). Ganancia neta =             ║
║      2 × ficha del intento ganador − fichas perdidas antes.   ║
║   Historial: se toma todo lo que manda el servidor (sin       ║
║   distinguir crupier); al conectar se evalúa si ya hay un     ║
║   patrón en curso.                                            ║
║   El 0 no pertenece a ninguna columna/docena: rompe patrones  ║
║   y cuenta como pérdida si hay una señal activa.              ║
║                                                               ║
║   HTTP: /ping, /health, /api/state/{mesa}, /api/all           ║
║   Telegram: /estado                                           ║
╚══════════════════════════════════════════════════════════════╝
"""

import asyncio
import html
import json
import logging
import os
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

ROULETTE_KEYS = {227: 227}      # Roulette 1
TABLE_NAME    = "Roulette 1"

# ── Telegram ──
BOT_TOKEN      = os.environ.get("BOT_TOKEN", "8347707121:AAH1cPEDMLbm-scTJ8mUuufeEhzw3Axv2Lw")
CANAL_SENALES  = int(os.environ.get("CANAL_SENALES", "-1004228660174"))

# ── Apuesta ──
FICHA_BASE    = int(os.environ.get("FICHA_BASE", "50"))        # COP por ficha
PAGO          = 2                                              # columna / docena paga 2:1
MAX_INTENTOS  = int(os.environ.get("MAX_INTENTOS", "0"))       # 0 = sin límite (hasta que salga b)

HIST_MAX      = 50       # giros que se guardan en memoria
CALENTAMIENTO = 45       # seg. máx. que se esperan las últimas 20 antes de dar por buena la mesa

# ──────────────────────────────────────────────
#  LOGGING
# ──────────────────────────────────────────────
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s | %(levelname)s | %(message)s",
                    datefmt="%H:%M:%S", handlers=[logging.StreamHandler(sys.stdout)])
log = logging.getLogger(__name__)

_server_state: Optional["ServerState"] = None


# ══════════════════════════════════════════════
#  UTILIDADES DE RULETA
# ══════════════════════════════════════════════
def columna_de(n: int) -> Optional[int]:
    """Columna 1 = 1,4,7…34 · Columna 2 = 2,5,8…35 · Columna 3 = 3,6,9…36. El 0 no tiene."""
    return None if n == 0 else (n - 1) % 3 + 1


def docena_de(n: int) -> Optional[int]:
    """Docena 1 = 1-12 · Docena 2 = 13-24 · Docena 3 = 25-36. El 0 no tiene."""
    return None if n == 0 else (n - 1) // 12 + 1


def fib_mult(intento: int) -> int:
    """Multiplicador Fibonacci por intento: 1, 1, 2, 3, 5, 8, 13, 21, …"""
    a, b = 1, 1
    for _ in range(intento - 1):
        a, b = b, a + b
    return a


def es_prealerta(c: list) -> bool:
    """c = últimas 4 categorías (antigua → reciente). Patrón (a, b, b, c) con a, b, c distintas."""
    if len(c) != 4 or any(x is None for x in c):
        return False
    a, b1, b2, c4 = c
    return b1 == b2 and len({a, b1, c4}) == 3


def es_patron(c: list) -> bool:
    """c = últimas 5 categorías. Patrón completo (a, b, b, c, a)."""
    return len(c) == 5 and es_prealerta(c[:4]) and c[4] == c[0]


def es_prealerta_aabaa(c: list) -> bool:
    """c = últimas 4 categorías (antigua → reciente). Patrón (a, a, b, a) con a ≠ b."""
    if len(c) != 4 or any(x is None for x in c):
        return False
    return c[0] == c[1] == c[3] and c[2] != c[0]


def es_patron_aabaa(c: list) -> bool:
    """c = últimas 5 categorías. Patrón completo (a, a, b, a, a)."""
    return len(c) == 5 and es_prealerta_aabaa(c[:4]) and c[4] == c[0]


# Cada patrón: prealerta (últimas 4), patrón completo (últimas 5) y posición de b dentro de las últimas 5.
# En ambos, la categoría que debe salir para completar el patrón es "a" (la primera de las últimas 4).
PATRON_ABBCA = {"txt": "(a, b, b, c, a)", "pre": es_prealerta,       "full": es_patron,       "b_idx": 1}
PATRON_AABAA = {"txt": "(a, a, b, a, a)", "pre": es_prealerta_aabaa, "full": es_patron_aabaa, "b_idx": 2}


# ══════════════════════════════════════════════
#  TELEGRAM
# ══════════════════════════════════════════════
bot = AsyncTeleBot(BOT_TOKEN, parse_mode="HTML") if (TELEBOT_OK and BOT_TOKEN) else None
if bot is None:
    log.warning("Telegram deshabilitado (falta BOT_TOKEN o la librería 'telebot').")


async def send_msg(text: str, chat_id: int = CANAL_SENALES, retries: int = 3) -> Optional[int]:
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
                log.warning(f"[Telegram] Error enviando (chat={chat_id}, intento {attempt}/{retries}): {e} -> reintento en {wait}s")
                await asyncio.sleep(wait)
                delay *= 2
            else:
                log.error(f"[Telegram] Fallo definitivo enviando (chat={chat_id}) tras {retries} intentos: {e}")
                return None


async def delete_msg(msg_id: Optional[int], chat_id: int = CANAL_SENALES) -> bool:
    if bot is None or msg_id is None:
        return False
    try:
        await bot.delete_message(chat_id=chat_id, message_id=msg_id)
        return True
    except Exception as e:
        log.debug(f"[Telegram] Error eliminando mensaje {msg_id}: {e}")
        return False


# ══════════════════════════════════════════════
#  MENSAJES
# ══════════════════════════════════════════════
def msg_prealerta(palabra: str, a: int) -> str:
    return f"🟡 POSIBLE SEÑAL A {palabra} 🟡\n⚪ DEBE SALIR {palabra} {a:02d} ⚪"


def msg_senal(palabra: str, b: int, ficha: int, intento: int, confirmada: bool) -> str:
    # La primera señal lleva el encabezado de confirmación; los reintentos usan el mismo formato.
    return f"✅ SEÑAL A {palabra} CONFIRMADA✅\n🎯 {palabra} {b} – FICHA: ${ficha} COP – INT {intento}"


def msg_acierto(palabra: str, b: int, numero: int, intento: int, neto: int) -> str:
    return (f"✅ GANAMOS ({numero}) INTENTO {intento}\n"
            f"💲 GANANCIA NETA {neto} COP\n"
            f"🎯 {palabra} {b}")


def msg_perdida(palabra: str, b: int, intento: int, perdido: int) -> str:
    return (f"❎ PERDIMOS {palabra} {b} – INTENTO {intento}\n"
            f"💲 PÉRDIDA NETA -{perdido} COP")


# ══════════════════════════════════════════════
#  AGENTE (columna o docena)
# ══════════════════════════════════════════════
class Agente:
    """Un agente = una categoría (COLUMNA o DOCENA). Totalmente independiente del otro."""

    def __init__(self, nombre: str, palabra: str, clasificar: Callable[[int], Optional[int]],
                 patron: dict = PATRON_ABBCA):
        self.nombre = nombre            # Agente_Col / Agente_Doc
        self.palabra = palabra          # COLUMNA / DOCENA
        self.clasificar = clasificar
        self.patron = patron
        self.pre_msg: Optional[int] = None      # id del mensaje de prealerta vigente
        self.sig: Optional[dict] = None         # señal activa
        self.stats = {"senales": 0, "ganadas": 0, "perdidas": 0, "neto": 0, "por_intento": {}, "max_intento": 0}

    # ── helpers ──
    def _ficha(self, intento: int) -> int:
        return FICHA_BASE * fib_mult(intento)

    async def _enviar_intento(self):
        s = self.sig
        s["ficha"] = self._ficha(s["intento"])
        s["msg"] = await send_msg(msg_senal(self.palabra, s["b"], s["ficha"], s["intento"], s["intento"] == 1))
        log.info(f"[{self.nombre}] Señal {self.palabra} {s['b']} · intento {s['intento']} · ficha ${s['ficha']}")

    async def _abrir_senal(self, b: int):
        self.sig = {"b": b, "intento": 1, "perdido": 0, "ficha": 0, "msg": None}
        self.stats["senales"] += 1
        await self._enviar_intento()

    async def _resolver(self, numero: int, cat: Optional[int]):
        """Evalúa el giro contra la señal activa. Borra el mensaje del intento anterior."""
        s = self.sig
        await delete_msg(s["msg"])
        if cat == s["b"]:
            neto = PAGO * s["ficha"] - s["perdido"]
            self.stats["ganadas"] += 1
            self.stats["neto"] += neto
            k = str(s["intento"])
            self.stats["por_intento"][k] = self.stats["por_intento"].get(k, 0) + 1
            self.stats["max_intento"] = max(self.stats["max_intento"], s["intento"])
            log.info(f"[{self.nombre}] ✅ GANADA con {numero} en intento {s['intento']} · neto {neto} COP")
            await send_msg(msg_acierto(self.palabra, s["b"], numero, s["intento"], neto))
            self.sig = None
            return
        # no salió b → pierde este intento
        s["perdido"] += s["ficha"]
        if MAX_INTENTOS and s["intento"] >= MAX_INTENTOS:
            self.stats["perdidas"] += 1
            self.stats["neto"] -= s["perdido"]
            self.stats["max_intento"] = max(self.stats["max_intento"], s["intento"])
            log.info(f"[{self.nombre}] ❎ PERDIDA tras {s['intento']} intentos · -{s['perdido']} COP")
            await send_msg(msg_perdida(self.palabra, s["b"], s["intento"], s["perdido"]))
            self.sig = None
            return
        s["intento"] += 1
        await self._enviar_intento()

    # ── entrada principal: se llama en cada giro en vivo ──
    async def procesar(self, numero: int, historial: list):
        cat = self.clasificar(numero)

        # 1) señal en curso: se resuelve y, si sigue abierta, no se buscan patrones nuevos
        if self.sig:
            await self._resolver(numero, cat)
            if self.sig:
                return

        # 2) la prealerta anterior ya cumplió su función (confirmó o no): se borra
        if self.pre_msg is not None:
            await delete_msg(self.pre_msg)
            self.pre_msg = None

        await self._buscar_patron(historial)

    async def _buscar_patron(self, historial: list):
        cats = [self.clasificar(n) for n in historial[-5:]]

        # 3) patrón completo → señal confirmada a b
        if self.patron["full"](cats):
            await self._abrir_senal(cats[self.patron["b_idx"]])
            return

        # 4) últimas 4 = (a, b, b, c) → prealerta: debe salir a
        if self.patron["pre"](cats[-4:]):
            self.pre_msg = await send_msg(msg_prealerta(self.palabra, cats[-4]))
            log.info(f"[{self.nombre}] Prealerta {self.patron['txt']}: debe salir {self.palabra} {cats[-4]}")

    async def sincronizar(self, historial: list):
        """Evalúa el historial recién cargado del servidor (sin resolver nada): si el patrón ya está en curso, avisa."""
        if self.sig is None and self.pre_msg is None:
            await self._buscar_patron(historial)

    def estado(self) -> dict:
        s = self.sig
        return {
            "agente": self.nombre,
            "prealerta": self.pre_msg is not None,
            "senal": None if s is None else {"b": s["b"], "intento": s["intento"], "ficha": s["ficha"], "perdido": s["perdido"]},
            "stats": self.stats,
        }


# ══════════════════════════════════════════════
#  MESA
# ══════════════════════════════════════════════
class RouletteTable:
    def __init__(self, key: int):
        self.key = key
        self.historial: list = []
        self.total_spins = 0
        self.live_spins = 0
        self.agentes = [
            Agente("Agente_Col", "COLUMNA", columna_de, PATRON_ABBCA),
            Agente("Agente_Doc", "DOCENA", docena_de, PATRON_ABBCA),
            Agente("Agente_Col2", "COLUMNA", columna_de, PATRON_AABAA),
            Agente("Agente_Doc2", "DOCENA", docena_de, PATRON_AABAA),
        ]

    async def update(self, number: int, training: bool = False):
        self.historial.append(number)
        if len(self.historial) > HIST_MAX:
            self.historial.pop(0)
        self.total_spins += 1
        if training:
            return
        self.live_spins += 1
        for ag in self.agentes:
            try:
                await ag.procesar(number, self.historial)
            except Exception as e:
                log.exception(f"[{ag.nombre}] Error procesando el giro {number}: {e}")
        log.info(f"🎰 Mesa {self.key} | Giro #{self.total_spins}: {number} | "
                 f"Col {columna_de(number)} · Doc {docena_de(number)} | "
                 + " | ".join(f"{a.nombre}: " + (f"intento {a.sig['intento']}" if a.sig else "libre") for a in self.agentes))

    async def sincronizar(self):
        """Se llama una vez, cuando termina de cargarse el historial que manda el servidor."""
        log.info(f"📥 Historial del servidor cargado: {len(self.historial)} giros → {self.historial[-10:]}")
        for ag in self.agentes:
            try:
                await ag.sincronizar(self.historial)
            except Exception as e:
                log.exception(f"[{ag.nombre}] Error sincronizando el historial: {e}")

    def get_state(self, limit: int = 40) -> dict:
        return {
            "key": self.key,
            "table_name": TABLE_NAME,
            "ultimos": self.historial[-limit:],
            "total_spins_seen": self.total_spins,
            "live_spins_seen": self.live_spins,
            "agentes": [a.estado() for a in self.agentes],
        }


def build_estado_message(server_state) -> str:
    lineas = [f"📊 <b>{TABLE_NAME}</b> — ficha base ${FICHA_BASE} COP"]
    for mesa in server_state.tables.values():
        lineas.append(f"Giros en vivo: {mesa.live_spins}")
        for a in mesa.agentes:
            st = a.stats
            cerradas = st["ganadas"] + st["perdidas"]
            pct = (st["ganadas"] / cerradas * 100) if cerradas else 0
            activa = f"intento {a.sig['intento']} (ficha ${a.sig['ficha']})" if a.sig else "sin señal activa"
            por = ", ".join(f"INT {k}: {v}" for k, v in sorted(st["por_intento"].items(), key=lambda x: int(x[0]))) or "—"
            lineas.append(
                f"\n<b>{a.nombre}</b> ({a.palabra} {a.patron['txt']})\n"
                f"• Señales: {st['senales']} · Ganadas: {st['ganadas']} · Perdidas: {st['perdidas']} ({pct:.0f}%)\n"
                f"• Aciertos por intento: {por}\n"
                f"• Intento máx.: {st['max_intento']}\n"
                f"• Neto acumulado: {st['neto']} COP\n"
                f"• Ahora: {activa}")
    return "\n".join(lineas)


if bot is not None:
    @bot.message_handler(commands=["estado"])
    async def handle_estado_command(message):
        if _server_state is None:
            await bot.reply_to(message, "⏳ El servidor todavía se está iniciando, intenta de nuevo en unos segundos.")
            return
        try:
            await bot.reply_to(message, build_estado_message(_server_state))
        except Exception as e:
            log.warning(f"[Telegram] Error respondiendo /estado: {e}")

    async def _register_bot_commands():
        if BotCommand is None:
            return
        try:
            await bot.set_my_commands([BotCommand("estado", "Estadísticas de los 4 agentes (columnas y docenas)")])
        except Exception as e:
            log.warning(f"[Telegram] No se pudo registrar el menú de comandos: {e}")
else:
    async def _register_bot_commands():
        return


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
        "total_spins": sum(t.total_spins for t in _server_state.tables.values()),
    })


async def http_api_state(request: web.Request):
    if _server_state is None:
        return web.json_response({"error": "server not ready"}, status=503)
    try:
        mesa = int(request.match_info["mesa"])
    except (KeyError, ValueError):
        return web.json_response({"error": "mesa inválida"}, status=400)
    table = _server_state.tables.get(mesa)
    if table is None:
        return web.json_response({"error": "mesa no soportada"}, status=404)
    try:
        limit = int(request.query.get("limit", 40))
    except ValueError:
        limit = 40
    return web.json_response(table.get_state(limit=max(5, min(HIST_MAX, limit))))


async def http_api_all(request: web.Request):
    if _server_state is None:
        return web.json_response({"error": "server not ready"}, status=503)
    return web.json_response({str(k): t.get_state(limit=40) for k, t in _server_state.tables.items()})


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
def _gid_num(gid) -> Optional[int]:
    try:
        return int(gid)
    except (TypeError, ValueError):
        return None


class PragmaticWebSocketHandler:
    def __init__(self, key: int, on_spin_callback: Callable[..., Awaitable[None]],
                 on_sync_callback: Optional[Callable[[], Awaitable[None]]] = None):
        self.key = key
        self.on_spin_callback = on_spin_callback
        self.on_sync_callback = on_sync_callback
        self.seen = set()
        self.inicializado = False          # True cuando ya llegó la primera tanda last20Results
        self.t_inicio = time.time()

    def _ordenar(self, lista: list, single: Optional[dict]) -> list:
        """Orden cronológico (antiguo → reciente). Con gameId numérico se ordena por id; si no, se asume
        que last20Results viene del más reciente al más antiguo."""
        todos = list(lista) + ([single] if single else [])
        if todos and all(_gid_num(r.get("gameId")) is not None for r in todos):
            return sorted(todos, key=lambda r: _gid_num(r.get("gameId")))
        return list(reversed(lista)) + ([single] if single else [])

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
                        lista = [r for r in results if isinstance(r, dict)] if isinstance(results, list) else []
                        single = None
                        if data.get("gameId") is not None and data.get("result") is not None:
                            single = {"gameId": data.get("gameId"), "result": data.get("result")}
                        if not lista and single is None:
                            continue
                        # La primera tanda solo arma el historial (sin señales); después todo es en vivo.
                        emit = self.inicializado or (time.time() - self.t_inicio > CALENTAMIENTO)
                        for r in self._ordenar(lista, single):
                            await self._feed(r.get("gameId"), r.get("result"), emit=emit)
                        if isinstance(results, list) and not self.inicializado:
                            self.inicializado = True
                            if self.on_sync_callback:
                                await self.on_sync_callback()
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
            await self.on_spin_callback(num, training=not emit)


# ══════════════════════════════════════════════
#  SERVER STATE
# ══════════════════════════════════════════════
class ServerState:
    def __init__(self):
        self.tables = {k: RouletteTable(k) for k in ROULETTE_KEYS.values()}

    async def update_mesa(self, key: int, number: int, training: bool = False):
        mesa = self.tables.get(key)
        if mesa is not None:
            await mesa.update(number, training=training)

    async def sync_mesa(self, key: int):
        mesa = self.tables.get(key)
        if mesa is not None:
            await mesa.sincronizar()


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
    log.info(f"BOT COLUMNAS / DOCENAS — {TABLE_NAME} (clave {', '.join(str(k) for k in ROULETTE_KEYS.values())})")
    log.info(f"Patrón (a,b,b,c,a) · Fibonacci · ficha base ${FICHA_BASE} COP · "
             f"máx. intentos: {MAX_INTENTOS or 'sin límite'}")
    log.info("═" * 60)

    server_state = ServerState()
    _server_state = server_state

    async def on_spin(key: int, num: int, training: bool = False):
        await server_state.update_mesa(key, num, training=training)

    tasks = []
    for key in ROULETTE_KEYS.values():
        handler = PragmaticWebSocketHandler(
            key,
            lambda num, training=False, k=key: on_spin(k, num, training),
            on_sync_callback=lambda k=key: server_state.sync_mesa(k),
        )
        tasks.append(asyncio.create_task(handler.run()))

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
