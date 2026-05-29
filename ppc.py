"""
╔══════════════════════════════════════════════════════════════╗
║       IMMERSIVE ROULETTE — BOT DE SEÑALES TELEGRAM          ║
║       Sistema completo de señales, gales y estadísticas     ║
║       Fuente: Evolution API (polling adaptativo)            ║
║       Flask HTTP — Render-ready (anti-sleep + /health)      ║
║       Canal secundario: historial 100 giros + botón copiar  ║
╚══════════════════════════════════════════════════════════════
"""

import asyncio
import json
import logging
import os
import sys
import threading
import time
from collections import deque
from datetime import date, datetime, timedelta, timezone

import aiohttp
import telebot.async_telebot as tba
import telebot.types as types
from flask import Flask, jsonify

# ══════════════════════════════════════════════════════════════
#  CONFIGURACIÓN ← EDITAR AQUÍ
# ══════════════════════════════════════════════════════════════
BOT_TOKEN         = "8657427877:AAG9E5JozV40mm3IQoREIHvTnBFEFPgRSQo"    # Token de @BotFather
MAIN_CHAT_ID      = -1003610988961    # Canal principal (señales + stats)
SECONDARY_CHAT_ID = -1003613599867   # Canal secundario (historial 100 giros)

# Archivo para persistir el ID del mensaje de historial entre reinicios
HISTORY_ID_FILE = "history_msg_id.json"

# ══════════════════════════════════════════════════════════════
#  CONSTANTES DEL SISTEMA
# ══════════════════════════════════════════════════════════════
ROULETTE_NAME = "IMMERSIVE ROULETTE"

EVOLUTION_URL = (
    "https://api-cs.casino.org/svc-evolution-game-events"
    "/api/immersiveroulette/latest"
)
EVOLUTION_HEADERS = {
    "origin":          "https://www.casino.org",
    "referer":         "https://www.casino.org/",
    "user-agent":      (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/148.0.0.0 Safari/537.36"
    ),
    "accept":          "*/*",
    "accept-language": "es,en;q=0.9",
    "sec-fetch-dest":  "empty",
    "sec-fetch-mode":  "cors",
    "sec-fetch-site":  "same-site",
}

MAX_ATTEMPTS  = 6     # 6 intentos totales: 1/6 … 6/6 → LOSS si falla 6/6
WAIT_SPINS    = 9     # Giros de espera tras resolver una señal
PING_INTERVAL = 240   # Segundos entre auto-pings (anti-sleep Render)

DEFAULT_WAIT  = 20    # Espera fija tras el primer giro registrado
DEFAULT_POLL  = 2     # Polling inicial (s)
POLL_SECS     = 1     # Polling rápido una vez calibrado

AR_TZ = timezone(timedelta(hours=-3))

# ──────────────────────────────────────────────────────────────
#  COLORES REALES — RULETA EUROPEA
# ──────────────────────────────────────────────────────────────
REAL_COLORS: dict[int, str] = {
    0 : "VERDE",
    1 : "ROJO" , 2 : "NEGRO", 3 : "ROJO" , 4 : "NEGRO", 5 : "ROJO" ,
    6 : "NEGRO", 7 : "ROJO" , 8 : "NEGRO", 9 : "ROJO" , 10: "NEGRO",
    11: "NEGRO", 12: "ROJO" , 13: "NEGRO", 14: "ROJO" , 15: "NEGRO",
    16: "ROJO" , 17: "NEGRO", 18: "ROJO" , 19: "ROJO" , 20: "NEGRO",
    21: "ROJO" , 22: "NEGRO", 23: "ROJO" , 24: "NEGRO", 25: "ROJO" ,
    26: "NEGRO", 27: "ROJO" , 28: "NEGRO", 29: "NEGRO", 30: "ROJO" ,
    31: "NEGRO", 32: "ROJO" , 33: "NEGRO", 34: "ROJO" , 35: "NEGRO",
    36: "ROJO" ,
}

SEQUENCE: list[str] = [
    "ROJO", "NEGRO", "ROJO", "ROJO", "NEGRO", "NEGRO",
    "ROJO", "NEGRO", "ROJO", "ROJO", "NEGRO", "NEGRO",
    "ROJO", "NEGRO", "ROJO",
]

COLOR_EMOJI = {"ROJO": "🔴", "NEGRO": "⚫", "VERDE": "🟢"}

# ══════════════════════════════════════════════════════════════
#  LOGGING
# ══════════════════════════════════════════════════════════════
logging.basicConfig(
    level    = logging.INFO,
    format   = "%(asctime)s | %(levelname)s | %(message)s",
    datefmt  = "%H:%M:%S",
    handlers = [logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

for _ln in ["werkzeug", "flask.app", "flask"]:
    logging.getLogger(_ln).setLevel(logging.ERROR)


# ══════════════════════════════════════════════════════════════
#  PERSISTENCIA DEL ID DEL MENSAJE DE HISTORIAL
#  Sobrevive reinicios del bot en Render.
# ══════════════════════════════════════════════════════════════

def load_history_state() -> tuple[int | None, int]:
    """Carga msg_id del batch activo y número de batches completados."""
    try:
        with open(HISTORY_ID_FILE, "r") as f:
            data = json.load(f)
            msg_id = int(data["msg_id"]) if data.get("msg_id") else None
            batch_count = int(data.get("batch_count", 0))
            return msg_id, batch_count
    except Exception:
        return None, 0


def load_history_msg_id() -> int | None:
    msg_id, _ = load_history_state()
    return msg_id


def save_history_state(msg_id: int | None, batch_count: int) -> None:
    try:
        with open(HISTORY_ID_FILE, "w") as f:
            json.dump({"msg_id": msg_id, "batch_count": batch_count}, f)
    except Exception as e:
        log.warning(f"⚠️ No se pudo guardar history_state: {e}")


def save_history_msg_id(msg_id: int | None) -> None:
    _, bc = load_history_state()
    save_history_state(msg_id, bc)


# ══════════════════════════════════════════════════════════════
#  CLASES DEL SISTEMA DE SEÑALES
# ══════════════════════════════════════════════════════════════

class SignalData:
    def __init__(self, trigger_number: int, signal_color: str):
        self.trigger_number  = trigger_number
        self.signal_color    = signal_color
        self.check_color     = signal_color
        self.current_attempt = 0
        self.last_trigger_num: int | None = None

    @property
    def display_attempt(self) -> str:
        return f"{self.current_attempt + 1}/{MAX_ATTEMPTS}"


class SignalManager:
    def __init__(self):
        self.active_signal  : SignalData | None = None
        self.waiting_spins  : int = 0
        self.sequence_index : int = 0

    def advance_sequence(self) -> None:
        self.sequence_index = (self.sequence_index + 1) % len(SEQUENCE)

    def get_sequence_color(self) -> str:
        return SEQUENCE[self.sequence_index]

    def can_generate_signal(self) -> bool:
        return self.active_signal is None and self.waiting_spins == 0

    def start_signal(self, trigger_number: int, signal_color: str) -> None:
        self.active_signal = SignalData(trigger_number, signal_color)

    def tick_wait(self) -> None:
        if self.waiting_spins > 0:
            self.waiting_spins -= 1

    def process_result(
        self,
        spin_number    : int,
        real_color     : str,
        check_color    : str,
        next_gale_color: str | None,
    ) -> dict:
        s = self.active_signal

        if real_color == check_color:
            attempt    = s.current_attempt
            spins_used = attempt + 1
            self.active_signal = None
            self.waiting_spins = max(0, WAIT_SPINS - spins_used)
            return {"type": "win", "attempt": attempt, "check_color": check_color}

        s.current_attempt += 1
        attempt = s.current_attempt

        if attempt < MAX_ATTEMPTS:
            s.last_trigger_num = spin_number
            new_color          = next_gale_color or check_color
            s.check_color      = new_color
            return {"type": "gale", "attempt": attempt, "signal_color": new_color}

        spins_used = attempt
        self.active_signal = None
        self.waiting_spins = max(0, WAIT_SPINS - spins_used)
        return {"type": "loss", "attempt": attempt, "check_color": check_color}


class BotState:
    def __init__(self):
        self.signal_manager  = SignalManager()
        self.last_game_id    : str = ""

        self.last_spin_num   : int = 0
        self.last_spin_color : str = "NEGRO"

        self.signal_msg_id      : int | None = None
        self.waiting_msg_id     : int | None = None
        self.stats_msg_id       : int | None = None
        self.consecutive_msg_id : int | None = None

        self.stats_date       : date = self._ar_date()
        self.won_signals      : int = 0
        self.lost_signals     : int = 0
        self.consecutive_wins : int = 0
        self.c1_wins          : int = 0
        self.c2_wins          : int = 0
        self.c3_wins          : int = 0

        self.last_spin_ts     : float = 0.0

        # Historial por lotes de 100 giros
        _msg_id, _bc          = load_history_state()
        self.current_batch    : list[int]  = []    # lote en curso (< 100)
        self.batch_count      : int        = _bc   # lotes completos ya enviados
        self.batch_msg_id     : int | None = _msg_id  # msg del lote activo

    @staticmethod
    def _ar_date() -> date:
        return datetime.now(AR_TZ).date()

    def reset_daily_stats(self) -> None:
        log.info("🌅 Reseteando estadísticas del día")
        self.stats_date       = self._ar_date()
        self.won_signals      = 0
        self.lost_signals     = 0
        self.consecutive_wins = 0
        self.c1_wins = self.c2_wins = self.c3_wins = 0

    def check_daily_reset(self) -> None:
        if self._ar_date() != self.stats_date:
            log.info("🌅 [fallback] Día nuevo — reseteando estadísticas")
            self.reset_daily_stats()

    @property
    def total_signals(self) -> int:
        return self.won_signals + self.lost_signals

    @property
    def win_rate(self) -> float:
        if self.total_signals == 0:
            return 0.0
        return self.won_signals / self.total_signals * 100


# ══════════════════════════════════════════════════════════════
#  TELEGRAM CLIENT
# ══════════════════════════════════════════════════════════════

# Instancia global del bot (compartida para callbacks y envíos)
_bot = tba.AsyncTeleBot(BOT_TOKEN)


class TelegramClient:
    def __init__(self, chat_id: int):
        self._chat_id = chat_id

    async def send(
        self,
        text    : str,
        keyboard: types.InlineKeyboardMarkup | None = None,
    ) -> int | None:
        try:
            msg = await _bot.send_message(
                self._chat_id, text,
                parse_mode   = "HTML",
                reply_markup = keyboard,
            )
            log.info(
                f"📤 [{self._chat_id}] Enviado (id={msg.message_id}): "
                f"{text[:60].replace(chr(10), ' ')}"
            )
            return msg.message_id
        except Exception as e:
            log.error(f"❌ [{self._chat_id}] Error enviando: {e}")
        return None

    async def edit(
        self,
        message_id: int,
        text      : str,
        keyboard  : types.InlineKeyboardMarkup | None = None,
    ) -> bool:
        try:
            await _bot.edit_message_text(
                text,
                chat_id      = self._chat_id,
                message_id   = message_id,
                parse_mode   = "HTML",
                reply_markup = keyboard,
            )
            log.info(f"✏️  [{self._chat_id}] Editado (id={message_id})")
            return True
        except Exception as e:
            err = str(e).lower()
            # "not modified" no es un error real: el contenido es idéntico
            if "not modified" in err:
                return True
            log.debug(f"No se pudo editar {message_id}: {e}")
            return False

    async def delete(self, message_id: int | None) -> None:
        if message_id is None:
            return
        try:
            await _bot.delete_message(self._chat_id, message_id)
            log.info(f"🗑️  [{self._chat_id}] Eliminado (id={message_id})")
        except Exception as e:
            log.debug(f"No se pudo eliminar {message_id}: {e}")


# ══════════════════════════════════════════════════════════════
#  MESSAGE BUILDER
# ══════════════════════════════════════════════════════════════

class MessageBuilder:

    @staticmethod
    def _ce(color: str) -> str:
        return COLOR_EMOJI.get(color, "⚪")

    def signal(self, last_num: int, last_color: str, signal_color: str, attempt_str: str) -> str:
        return (
            f"🆔 RULETA — {ROULETTE_NAME}\n"
            f"⚪ ÚLTIMO GIRO: {last_num} {last_color} {self._ce(last_color)}\n"
            f"🟡 SEÑAL PARA: {signal_color} {self._ce(signal_color)}\n"
            f"🔵 INTENTO: {attempt_str}"
        )

    def win(self, number: int, color: str) -> str:
        return f"✅ GANADA {number} {self._ce(color)} {color} ✅"

    def loss(self, number: int, color: str) -> str:
        return f"❌ PERDIMOS {number} {self._ce(color)} {color} ❌"

    def consecutive(self, count: int) -> str:
        if count == 0:
            return "☑️ 0 SEÑALES GANADAS SEGUIDAS ☑️"
        return f"☑️ {count} SEÑALES GANADAS SEGUIDAS ☑️"

    def stats(self, state: BotState) -> str:
        st = state.total_signals

        def pct(wins: int) -> str:
            if st == 0:
                return "0.00%"
            return f"{wins / st * 100:.2f}%"

        return (
            f"📆 MARCADOR DIARIO\n"
            f"✅ GANADAS: {state.won_signals}\n"
            f"❌ PERDIDAS: {state.lost_signals}\n\n"
            f"📍 C1: {state.c1_wins} GANADAS — {pct(state.c1_wins)}\n"
            f"📍 C2: {state.c2_wins} GANADAS — {pct(state.c2_wins)}\n"
            f"📍 C3: {state.c3_wins} GANADAS — {pct(state.c3_wins)}\n\n"
            f"📈 ACIERTO: {state.win_rate:.2f}%"
        )

    def daily_report(self, state: BotState) -> str:
        st = state.total_signals

        def pct(wins: int) -> str:
            if st == 0:
                return "0.00%"
            return f"{wins / st * 100:.2f}%"

        return (
            f"📆 MARCADOR DIARIO 00:00 (ARG) — {ROULETTE_NAME}\n"
            f"✅ GANADAS: {state.won_signals}\n"
            f"❌ PERDIDAS: {state.lost_signals}\n\n"
            f"📍 C1: {state.c1_wins} GANADAS — {pct(state.c1_wins)}\n"
            f"📍 C2: {state.c2_wins} GANADAS — {pct(state.c2_wins)}\n"
            f"📍 C3: {state.c3_wins} GANADAS — {pct(state.c3_wins)}\n\n"
            f"📈 ACIERTO: {state.win_rate:.2f}%"
        )

    def waiting(self, spins_left: int) -> str:
        if spins_left == 1:
            return "⚠️ PRÓXIMO GIRO HAY SEÑAL ⚠️"
        return f"⚠️ SIGUIENTE SEÑAL EN {spins_left} GIROS... ⚠️"

    def history_text(self, batch: list, batch_num: int, complete: bool = False) -> str:
        """
        Texto del historial — canal secundario.
        Cada mensaje contiene exactamente 100 giros (o los que lleva el lote actual).
        <pre> genera bloque de código con botón nativo de copiar en Telegram.
        """
        count    = len(batch)
        nums_str = "\n".join(str(n) for n in batch)
        status   = "✅ COMPLETO" if complete else f"{count}/100"
        return (
            f"🆔 PARTE {batch_num} — {ROULETTE_NAME} [{status}]\n"
            f"<pre>{nums_str}</pre>"
        )


# ══════════════════════════════════════════════════════════════
#  SPIN PROCESSOR
# ══════════════════════════════════════════════════════════════

class SpinProcessor:
    def __init__(
        self,
        state  : BotState,
        tg_main: TelegramClient,
        tg_sec : TelegramClient,
        builder: MessageBuilder,
    ):
        self.state   = state
        self.tg      = tg_main
        self.tg_sec  = tg_sec
        self.builder = builder

    # ── Historial (canal secundario) ─────────────────────────

    async def _update_history(self) -> None:
        """
        Canal secundario — lotes de 100 giros.
        · Mientras el lote < 100: edita el mismo mensaje mostrando N/100.
        · Al llegar al giro 100: edita por última vez marcándolo COMPLETO,
          luego limpia el lote → el próximo giro creará un mensaje nuevo (Parte N+1).
        """
        s   = self.state
        bat = s.current_batch
        cnt = len(bat)

        if cnt == 0:
            return  # nada que mostrar aún

        batch_num = s.batch_count + 1   # número de parte actual (1-based)

        if cnt < 100:
            # ── Lote en curso: editar o crear mensaje ───────────────────────
            text = self.builder.history_text(bat, batch_num, complete=False)
            mid  = s.batch_msg_id
            if mid:
                ok = await self.tg_sec.edit(mid, text)
                if not ok:
                    log.warning("⚠️ Historial: mensaje no editable → nuevo")
                    new_id = await self.tg_sec.send(text)
                    s.batch_msg_id = new_id
                    save_history_state(new_id, s.batch_count)
            else:
                new_id = await self.tg_sec.send(text)
                s.batch_msg_id = new_id
                save_history_state(new_id, s.batch_count)

        else:
            # ── Lote completo (100 giros): cerrar y preparar el siguiente ───
            text = self.builder.history_text(bat, batch_num, complete=True)
            mid  = s.batch_msg_id
            if mid:
                await self.tg_sec.edit(mid, text)
            else:
                await self.tg_sec.send(text)
            log.info(f"📦 Parte {batch_num} completada (100 giros) — iniciando nueva parte")
            # Avanzar al siguiente lote
            s.batch_count  += 1
            s.current_batch = []
            s.batch_msg_id  = None
            save_history_state(None, s.batch_count)


    # ── Procesamiento principal ───────────────────────────────

    async def process(self, number: int) -> None:
        s  = self.state
        sm = s.signal_manager

        s.check_daily_reset()

        # Avance de secuencia — siempre, en CADA giro
        sm.advance_sequence()
        seq_color = sm.get_sequence_color()

        real_color        = REAL_COLORS.get(number, "VERDE")
        s.last_spin_num   = number
        s.last_spin_color = real_color
        s.last_spin_ts    = time.time()

        # Añadir al historial
        s.current_batch.append(number)

        log.info(
            f"🎰 {number} | {real_color} | seq={seq_color} | "
            f"espera={sm.waiting_spins} | señal={'SI' if sm.active_signal else 'NO'}"
        )

        # ── PERÍODO DE ESPERA POST-SEÑAL ──────────────────────────────────────
        if sm.waiting_spins > 0:
            sm.tick_wait()
            remaining = sm.waiting_spins
            if remaining > 0:
                await self.tg.delete(s.waiting_msg_id)
                s.waiting_msg_id = await self.tg.send(self.builder.waiting(remaining))
            await self._update_history()
            return

        # ── SEÑAL ACTIVA → VERIFICAR RESULTADO ───────────────────────────────
        if sm.active_signal:
            sig             = sm.active_signal
            check_color     = sig.check_color
            next_gale_color = seq_color

            result = sm.process_result(number, real_color, check_color, next_gale_color)

            if result["type"] == "win":
                s.won_signals      += 1
                s.consecutive_wins += 1
                attempt = result["attempt"]
                if attempt <= 1:
                    s.c1_wins += 1
                elif attempt <= 3:
                    s.c2_wins += 1
                else:
                    s.c3_wins += 1
                s.signal_msg_id = None
                await self.tg.send(self.builder.win(number, real_color))
                await asyncio.sleep(0.4)
                await self.tg.delete(s.consecutive_msg_id)
                s.consecutive_msg_id = await self.tg.send(
                    self.builder.consecutive(s.consecutive_wins)
                )
                await asyncio.sleep(0.4)
                await self.tg.delete(s.stats_msg_id)
                s.stats_msg_id = await self.tg.send(self.builder.stats(s))
                log.info(f"✅ GANADA intento {result['attempt']+1} | racha={s.consecutive_wins}")

            elif result["type"] == "loss":
                s.lost_signals    += 1
                s.consecutive_wins = 0
                s.signal_msg_id   = None
                await self.tg.send(self.builder.loss(number, real_color))
                await asyncio.sleep(0.4)
                await self.tg.delete(s.consecutive_msg_id)
                s.consecutive_msg_id = await self.tg.send(self.builder.consecutive(0))
                await asyncio.sleep(0.4)
                await self.tg.delete(s.stats_msg_id)
                s.stats_msg_id = await self.tg.send(self.builder.stats(s))
                log.info(f"❌ PERDIDA en intento {result['attempt']}")

            else:   # gale
                new_color   = result["signal_color"]
                new_attempt = result["attempt"]
                attempt_str = f"{new_attempt + 1}/{MAX_ATTEMPTS}"
                await self.tg.delete(s.signal_msg_id)
                s.signal_msg_id = await self.tg.send(
                    self.builder.signal(number, real_color, new_color, attempt_str)
                )
                log.info(f"🔁 Gale {new_attempt} | apostar {new_color}")

        # ── SIN SEÑAL ACTIVA → DETECTAR NUEVA ────────────────────────────────
        elif sm.can_generate_signal():
            sm.start_signal(number, seq_color)
            await self.tg.delete(s.waiting_msg_id)
            s.waiting_msg_id = None
            s.signal_msg_id  = await self.tg.send(
                self.builder.signal(number, real_color, seq_color, f"1/{MAX_ATTEMPTS}")
            )
            log.info(f"🟡 Nueva señal: {number} → apostar {seq_color}")

        # Actualizar historial (siempre al final del giro)
        await self._update_history()


# ══════════════════════════════════════════════════════════════


# ══════════════════════════════════════════════════════════════

# ══════════════════════════════════════════════════════════════
#  FLASK — SERVIDOR HTTP (mantiene vivo Render)
# ══════════════════════════════════════════════════════════════

flask_app = Flask(__name__)
_state: BotState | None = None


@flask_app.route("/")
def home():
    return jsonify({
        "status"  : "ok",
        "bot"     : "Immersive Roulette — Bot de Señales Telegram",
        "roulette": ROULETTE_NAME,
    })


@flask_app.route("/ping")
def ping():
    return jsonify({"status": "pong", "ts": time.time()})


@flask_app.route("/health")
def health():
    if _state is None:
        return jsonify({"status": "not_ready"}), 503

    sm     = _state.signal_manager
    ar_now = datetime.now(AR_TZ).strftime("%Y-%m-%d %H:%M ART")
    ago    = round(time.time() - _state.last_spin_ts, 1) if _state.last_spin_ts else None

    return jsonify({
        "status"          : "ok",
        "ar_time"         : ar_now,
        "roulette"        : ROULETTE_NAME,
        "last_spin_num"   : _state.last_spin_num,
        "last_spin_color" : _state.last_spin_color,
        "last_spin_ago_s" : ago,
        "active_signal"   : sm.active_signal is not None,
        "waiting_spins"   : sm.waiting_spins,
        "won_signals"     : _state.won_signals,
        "lost_signals"    : _state.lost_signals,
        "win_rate_pct"    : round(_state.win_rate, 2),
        "consecutive_wins": _state.consecutive_wins,
        "history_count"   : len(_state.current_batch),
        "batch_count"     : _state.batch_count,
        "batch_msg_id"    : _state.batch_msg_id,
    })


def run_flask() -> None:
    port = int(os.environ.get("PORT", 10000))
    flask_app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)


# ══════════════════════════════════════════════════════════════
#  SELF-PING — ANTI-SLEEP RENDER
# ══════════════════════════════════════════════════════════════

async def self_ping_loop() -> None:
    render_url = os.environ.get("RENDER_EXTERNAL_URL", "").rstrip("/")
    if not render_url or "localhost" in render_url:
        log.info("⚙️  RENDER_EXTERNAL_URL no configurada — self-ping desactivado")
        return

    await asyncio.sleep(30)
    log.info(f"🏓 Self-ping activo → {render_url}/ping cada {PING_INTERVAL}s")

    while True:
        try:
            import urllib.request as _ur
            _ur.urlopen(f"{render_url}/ping", timeout=15)
            log.debug("🏓 Self-ping OK")
        except Exception as e:
            log.debug(f"🏓 Self-ping falló (no crítico): {e}")
        await asyncio.sleep(PING_INTERVAL)


# ══════════════════════════════════════════════════════════════
#  REPORTE MEDIANOCHE — 00:00 AR
# ══════════════════════════════════════════════════════════════

async def midnight_report_loop(
    state  : BotState,
    tg_main: TelegramClient,
    builder: MessageBuilder,
) -> None:
    while True:
        now = datetime.now(AR_TZ)
        next_midnight = (
            datetime(now.year, now.month, now.day, tzinfo=AR_TZ)
            + timedelta(days=1)
        )
        seconds_until = (next_midnight - now).total_seconds()
        log.info(
            f"🕛 Reporte nocturno en {seconds_until:.0f}s "
            f"({next_midnight.strftime('%d/%m %H:%M')} AR)"
        )
        await asyncio.sleep(seconds_until)

        try:
            await tg_main.send(builder.daily_report(state))
            log.info("📆 Reporte diario enviado a las 00:00 AR")
        except Exception as e:
            log.error(f"❌ Error enviando reporte nocturno: {e}")

        state.reset_daily_stats()


# ══════════════════════════════════════════════════════════════
#  POLLER — EVOLUTION API
# ══════════════════════════════════════════════════════════════

def _parse_settled_at(s: str) -> float:
    if not s:
        return 0.0
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()
    except Exception:
        return 0.0


async def poll_evolution(processor: SpinProcessor, state: BotState) -> None:
    recon           = 5
    last_id         = state.last_game_id
    last_settled_ts = 0.0
    spin_interval   = 0.0
    poll_secs       = DEFAULT_POLL

    log.info(f"🎰 Poller IMMERSIVE iniciado → {EVOLUTION_URL}")

    async with aiohttp.ClientSession(headers=EVOLUTION_HEADERS) as session:
        while True:
            try:
                async with session.get(
                    EVOLUTION_URL,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    if resp.status != 200:
                        log.warning(f"⚠️ API HTTP {resp.status} — reintento en {recon}s")
                        await asyncio.sleep(recon)
                        recon = min(recon * 2, 60)
                        continue

                    payload = await resp.json(content_type=None)
                    recon   = 5

                    game_id = str(payload.get("id", ""))
                    if not game_id or game_id == last_id:
                        await asyncio.sleep(poll_secs)
                        continue

                    data   = payload.get("data", {})
                    status = data.get("status", "")
                    if status != "Resolved":
                        await asyncio.sleep(poll_secs)
                        continue

                    outcome = data.get("result", {}).get("outcome", {})
                    number  = outcome.get("number")
                    if number is None:
                        await asyncio.sleep(poll_secs)
                        continue

                    number = int(number)
                    if not (0 <= number <= 36):
                        await asyncio.sleep(poll_secs)
                        continue

                    current_settled_ts = _parse_settled_at(data.get("settledAt", ""))
                    if current_settled_ts == 0.0:
                        current_settled_ts = time.time()

                    if last_settled_ts > 0 and current_settled_ts > last_settled_ts:
                        spin_interval = current_settled_ts - last_settled_ts
                        log.info(f"[Poller] ⏱️ Intervalo: {spin_interval:.1f}s")

                    last_settled_ts    = current_settled_ts
                    last_id            = game_id
                    state.last_game_id = game_id

                    await processor.process(number)

                    if spin_interval > 5:
                        poll_secs  = POLL_SECS
                        elapsed    = time.time() - current_settled_ts
                        safe_sleep = max(spin_interval * 0.80 - elapsed, 0.0)
                        if safe_sleep > 1:
                            log.debug(f"[Poller] 😴 Sleep adaptativo {safe_sleep:.1f}s")
                            await asyncio.sleep(safe_sleep)
                    else:
                        log.info(f"[Poller] 🔰 Primer giro — esperando {DEFAULT_WAIT}s")
                        await asyncio.sleep(DEFAULT_WAIT)
                    continue

            except aiohttp.ClientError as e:
                log.warning(f"⚠️ Error de red: {e} — reintento en {recon}s")
                await asyncio.sleep(recon)
                recon = min(recon * 2, 60)
                continue
            except Exception as e:
                log.error(f"❌ Error inesperado en poller: {e}")
                await asyncio.sleep(recon)

            await asyncio.sleep(POLL_SECS)



# ══════════════════════════════════════════════════════════════
#  PUNTO DE ENTRADA
# ══════════════════════════════════════════════════════════════

async def main() -> None:
    global _state

    log.info("═" * 60)
    log.info(f"  {ROULETTE_NAME} — BOT DE SEÑALES TELEGRAM (Render-ready)")
    log.info(f"  Canal principal  : {MAIN_CHAT_ID}")
    log.info(f"  Canal secundario : {SECONDARY_CHAT_ID}")
    log.info(f"  Intentos         : {MAX_ATTEMPTS} (1 señal + {MAX_ATTEMPTS-1} gales)")
    log.info(f"  Espera           : {WAIT_SPINS} giros tras resolver señal")
    log.info(f"  Ping             : cada {PING_INTERVAL}s (anti-sleep Render)")
    log.info(f"  Zona AR          : UTC-3 | Hoy: {datetime.now(AR_TZ).date()}")
    log.info("═" * 60)

    if BOT_TOKEN == "TU_TOKEN_AQUI":
        log.error("❌  DEBES configurar BOT_TOKEN antes de ejecutar el bot.")
        sys.exit(1)

    state   = BotState()
    _state  = state
    tg_main = TelegramClient(MAIN_CHAT_ID)
    tg_sec  = TelegramClient(SECONDARY_CHAT_ID)
    builder = MessageBuilder()
    proc    = SpinProcessor(state, tg_main, tg_sec, builder)

    log.info(
        f"💾 Parte actual: {state.batch_count + 1} | msg_id={state.batch_msg_id or 'nuevo'}"
    )

    ar_now = datetime.now(AR_TZ).strftime("%d/%m/%Y %H:%M:%S")
    await tg_main.send(
        f"🤖 <b>Bot de Señales iniciado</b>\n"
        f"🎰 Mesa: {ROULETTE_NAME}\n"
        f"🕐 {ar_now} (AR)"
    )

    await asyncio.gather(
        poll_evolution(proc, state),
        midnight_report_loop(state, tg_main, builder),
        self_ping_loop(),
    )


if __name__ == "__main__":
    # Flask arranca primero para que Render detecte el puerto a tiempo
    _flask_thread = threading.Thread(target=run_flask, daemon=True)
    _flask_thread.start()
    time.sleep(1)

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("⏹️  Bot detenido por el usuario")
