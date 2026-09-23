"""
╔══════════════════════════════════════════════════════════════╗
║   BOT LATINA ROULETTE (key 233) — PATRÓN P/C DE UN INTENTO  ║
║   - Clasificación de cada número de la ruleta:               ║
║       P = PATRÓN   -> {2,5,4,7,8,11,6,9,14,17,16,19,18,21,  ║
║                        20,23,26,29,28,31,30,33,32,35}        ║
║       C = CONTRARIO-> {0,1,3,10,13,12,15,22,25,24,27,34,36}  ║
║   - Persistencia SQLite (latina_roulette.db):                ║
║       · spins:  cada giro (número, color, clase, ronda)      ║
║       · transitions: tras el número X (0-36), cuántas veces  ║
║         vino P y cuántas C (estadística por número)          ║
║       · runs: rachas P/C con ronda de inicio y fin           ║
║   - Predicción: SIEMPRE tendencia P, UN SOLO INTENTO.        ║
║     Probabilidad combinada de dos fuentes:                   ║
║       1) Transición por número: después del último número,   ║
║          % histórico de P vs C.                              ║
║       2) Sufijo de secuencia: secuencia reciente P/C (ej.    ║
║          P,P,P,C,C,P,P) buscada en el historial; qué vino    ║          después de ocurrencias previas del mismo sufijo.   ║
║     Se envía señal si la probabilidad combinada >= umbral    ║
║     (SIGNAL_MIN_P_PROB, por defecto 65%) y hay muestra       ║
║     suficiente (SIGNAL_MIN_SAMPLES, por defecto 8).          ║
║   - Mensajes exactos:                                        ║
║     ✅CONFIRMACION SEÑAL✅ / ✅ WIN 1 EXP / ❎ LOSS 1 EXP /   ║
║     📆 MARCADOR dd/mm/yy                                     ║
║   - Comandos: /status, /stats, /marcador                     ║
╚══════════════════════════════════════════════════════════════
"""

import asyncio
import json
import logging
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone, timedelta
from typing import Optional, Callable, Awaitable

import websockets
from aiohttp import web
from aiohttp import ClientSession, ClientTimeout

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

ROULETTE_KEYS = {234: 234}          # Latina Roulette
LATINA_KEY    = 234

COL_TZ = timezone(timedelta(hours=-5))

# ── Clasificación de números ──
P_NUMBERS = frozenset({2, 5, 4, 7, 8, 11, 6, 9, 14, 17, 16, 19,
                       18, 21, 20, 23, 26, 29, 28, 31, 30, 33, 32, 35})
C_NUMBERS = frozenset({0, 1, 3, 10, 13, 12, 15, 22, 25, 24, 27, 34, 36})
assert len(P_NUMBERS) + len(C_NUMBERS) == 37, "La clasificación debe cubrir 0-36"

DB_PATH = os.environ.get("DB_PATH", "latina_roulette.db")

# ── Umbral de probabilidad P combinada para enviar la señal ──
SIGNAL_MIN_P_PROB  = float(os.environ.get("SIGNAL_MIN_P_PROB", "0.75"))
# ── Muestra mínima histórica (transiciones por número) ──
SIGNAL_MIN_SAMPLES = int(os.environ.get("SIGNAL_MIN_SAMPLES", "8"))
# ── Giros de espera entre señal y señal (tras resolver una) ──
SIGNAL_COOLDOWN_SPINS = int(os.environ.get("SIGNAL_COOLDOWN_SPINS", "2"))

# ── Análisis por sufijo de secuencia ──
SUFFIX_MIN_HISTORY = 15
SUFFIX_MAX_K       = 3
SUFFIX_MIN_OCC     = 3
# ── No enviar señal si ya venimos de una racha P muy larga ──
MAX_P_STREAK_NO_SIGNAL = 5

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
CHANNEL_STATS   = int(os.environ.get("CHANNEL_STATS", "-1003963076616"))

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s | %(levelname)s | %(message)s",
                    datefmt="%H:%M:%S", handlers=[logging.StreamHandler(sys.stdout)])
log = logging.getLogger(__name__)


def color_of(n):
    return REAL_COLOR_MAP.get(n, "VERDE")


def cls_of(n) -> str:
    """Clasifica un número de ruleta: 'P' (patrón) o 'C' (contrario)."""
    return "P" if n in P_NUMBERS else "C"


# ══════════════════════════════════════════════
#  SQLITE — PERSISTENCIA
# ══════════════════════════════════════════════
class SignalDB:
    """Guarda todo el análisis en SQLite:
    · spins:       cada giro con su clase y ronda (seq autoincremental).
    · transitions: tras el número X (0-36), cuántas veces el SIGUIENTE giro
                   fue P y cuántas C (estadística por número 0-36).
    · runs:        rachas P/C con ronda de inicio y de fin.
    """

    def __init__(self, path: str = DB_PATH):
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self._create()

    def _create(self):
        self.conn.executescript("""
        CREATE TABLE IF NOT EXISTS spins(
            seq      INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id  TEXT,
            number   INTEGER,
            color    TEXT,
            cls      TEXT,
            ts       REAL
        );
        CREATE TABLE IF NOT EXISTS transitions(
            after_number INTEGER NOT NULL,
            next_cls     TEXT NOT NULL,
            cnt          INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (after_number, next_cls)
        );
        CREATE TABLE IF NOT EXISTS runs(
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            cls       TEXT NOT NULL,
            start_seq INTEGER NOT NULL,
            end_seq   INTEGER NOT NULL,
            length    INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_spins_cls ON spins(cls);
        """)
        self.conn.commit()

    def add_spin(self, game_id, number: int, color: str, cls: str, ts: float) -> int:
        cur = self.conn.execute(
            "INSERT INTO spins(game_id, number, color, cls, ts) VALUES(?,?,?,?,?)",
            (str(game_id), number, color, cls, ts))
        self.conn.commit()
        return cur.lastrowid

    def bump_transition(self, after_number: int, next_cls: str):
        """Suma 1 al conteo: 'después del número X vino clase Y'."""
        self.conn.execute("""
            INSERT INTO transitions(after_number, next_cls, cnt) VALUES(?,?,1)
            ON CONFLICT(after_number, next_cls) DO UPDATE SET cnt = cnt + 1
        """, (after_number, next_cls))
        self.conn.commit()

    def get_transition(self, after_number: int):
        """Devuelve (veces_P, veces_C) tras ese número."""
        rows = self.conn.execute(
            "SELECT next_cls, cnt FROM transitions WHERE after_number = ?",
            (after_number,)).fetchall()
        p = sum(c for k, c in rows if k == "P")
        c = sum(c for k, c in rows if k == "C")
        return p, c

    def get_all_transitions(self):
        """Devuelve {numero: (p, c)} para todos los números con datos."""
        out = {}
        for n in range(37):
            p, c = self.get_transition(n)
            if p + c > 0:
                out[n] = (p, c)
        return out

    def close_run(self, cls: str, start_seq: int, end_seq: int):
        self.conn.execute(
            "INSERT INTO runs(cls, start_seq, end_seq, length) VALUES(?,?,?,?)",
            (cls, start_seq, end_seq, end_seq - start_seq + 1))
        self.conn.commit()

    def load_classes(self) -> list:
        """Reconstruye la secuencia completa de clases desde la base."""
        rows = self.conn.execute("SELECT cls FROM spins ORDER BY seq ASC").fetchall()
        return [r[0] for r in rows]

    def load_last(self) -> Optional[tuple]:
        """Último giro guardado: (seq, number, cls) o None."""
        row = self.conn.execute(
            "SELECT seq, number, cls FROM spins ORDER BY seq DESC LIMIT 1").fetchone()
        return row


# ══════════════════════════════════════════════
#  MARCADOR DIARIO
# ══════════════════════════════════════════════
class DailyMarker:
    def __init__(self, chat_id=None):
        self.stats = {"win": 0, "loss": 0}
        self.chat_id = chat_id if chat_id is not None else CHANNEL_STATS
        self.current_date = self._today()

    @staticmethod
    def _today() -> str:
        return datetime.now(COL_TZ).strftime("%d/%m/%y")

    def check_new_day(self) -> bool:
        today = self._today()
        if today != self.current_date:
            self.current_date = today
            self.stats = {"win": 0, "loss": 0}
            return True
        return False

    def record(self, win: bool):
        self.check_new_day()
        self.stats["win" if win else "loss"] = self.stats.get("win" if win else "loss", 0) + 1

    def message(self) -> str:
        w = self.stats.get("win", 0)
        l = self.stats.get("loss", 0)
        total = w + l
        w_pct = (w / total) * 100 if total else 0.0
        l_pct = (l / total) * 100 if total else 0.0
        return (f"📆 MARCADOR {self.current_date}\n\n"
                f"💎 SEÑALES ENVIADA: {total}\n"
                f"✅ Win 1: {w} | Acierto: {w_pct:.2f}%\n"
                f"❌ Loss: {l} | Fallos: {l_pct:.2f}%\n\n"
                f"📈 ACIERTO DEL DIA: {w_pct:.2f}%")


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
                log.error(f"[Telegram] Fallo definitivo enviando mensaje (chat={chat_id}): {e}")
                return None


def build_signal_message(number: int, prob_p: float) -> str:
    """✅CONFIRMACION SEÑAL✅ … formato exacto pedido."""
    p = round(prob_p * 100)
    c = 100 - p
    return (f"✅CONFIRMACION SEÑAL✅\n\n"
            f"🌟ENTRAR DESPUÉS: {number} {color_of(number)}\n"
            f"🧠PROBABILIDAD: {p}:{c}%\n"
            f"💎SOLO UN INTENTO\n\n"
            f"💡JUEGO RESPONSABLE")


def build_win_message(number: int) -> str:
    return f"✅ WIN 1 EXP — {number} {color_of(number)}"


def build_loss_message(number: int) -> str:
    return f"❎ LOSS 1 EXP — {number} {color_of(number)}"


# ══════════════════════════════════════════════
#  MESA LATINA ROULETTE
# ══════════════════════════════════════════════
class LatinaTable:
    def __init__(self, key: int, db: SignalDB):
        self.key = key
        self.db = db
        self.daily_marker = DailyMarker()

        self.spin_history = []          # [{"number","color","cls","seq","ts"}]
        self.class_history = []         # ["P","C","P",...] alineada con spins
        self.last_number = None

        # Rachas (runs) P/C
        self.current_run_cls = None
        self.current_run_start = None

        # Señal activa (un solo intento)
        self.signal_active = False
        self.signal_prob = 0.0
        self.signal_after_number = None
        self.cooldown = 0

        # Estadísticas totales
        self.stats = {"won": 0, "lost": 0}
        self.last_signals = []          # últimas 10 señales resueltas

        self._restore()

    def _restore(self):
        """Reconstruye el estado en memoria desde SQLite (tras un reinicio)."""
        self.class_history = self.db.load_classes()
        last = self.db.load_last()
        if last:
            self.last_number = last[1]
            self.current_run_cls = last[2]
            self.current_run_start = last[0]
        log.info(f"[DB] Restaurados {len(self.class_history)} giros desde '{DB_PATH}'.")

    # ── Probabilidad P basada en el número anterior (transición por número) ──
    def _number_p_rate(self, number: int) -> Optional[float]:
        p, c = self.db.get_transition(number)
        if p + c < SIGNAL_MIN_SAMPLES:
            return None
        return p / (p + c)

    # ── Probabilidad P basada en el sufijo de la secuencia P/C reciente ──
    def _suffix_p_rate(self) -> Optional[float]:
        """Busca la secuencia reciente (ej. P,P,P,C,C,P,P) en el historial y
        mira qué clase vino después de cada ocurrencia previa del mismo
        sufijo. Combina sufijos de longitud 1..SUFFIX_MAX_K ponderados por
        ocurrencias. Ejemplo con 'P,P,P,C,C,P,P': el sufijo 'P,P' indica qué
        tan seguido un doble-P previo fue seguido de otro P."""
        h = self.class_history
        if len(h) < SUFFIX_MIN_HISTORY:
            return None
        rates = []
        for k in range(1, SUFFIX_MAX_K + 1):
            suffix = h[-k:]
            occ = hits = 0
            for i in range(0, len(h) - k):
                if h[i:i + k] == suffix:
                    occ += 1
                    if h[i + k] == "P":
                        hits += 1
            if occ >= SUFFIX_MIN_OCC:
                rates.append((hits / occ, occ))
        if not rates:
            return None
        num = sum(r * o for r, o in rates)
        den = sum(o for _, o in rates)
        return num / den

    def _current_p_streak(self) -> int:
        streak = 0
        for cls in reversed(self.class_history):
            if cls == "P":
                streak += 1
            else:
                break
        return streak

    def _current_c_streak(self) -> int:
        streak = 0
        for cls in reversed(self.class_history):
            if cls == "C":
                streak += 1
            else:
                break
        return streak

    def _combined_p_rate(self) -> tuple:
        """Devuelve (probabilidad_P_combinada, detalle_str) o (None, razón)."""
        num_rate = self._number_p_rate(self.last_number) if self.last_number is not None else None
        suf_rate = self._suffix_p_rate()
        parts = []
        if num_rate is not None:
            parts.append(num_rate)
        if suf_rate is not None:
            parts.append(suf_rate)
        if not parts:
            return None, "sin muestra suficiente aún"
        prob = sum(parts) / len(parts)
        detalle = (f"número={num_rate * 100:.0f}%" if num_rate is not None else "número=s/n") + \
                  " · " + (f"sufijo={suf_rate * 100:.0f}%" if suf_rate is not None else "sufijo=s/n")
        return prob, detalle

    def _maybe_fire_signal(self):
        if self.signal_active or self.cooldown > 0:
            return
        if len(self.class_history) < SUFFIX_MIN_HISTORY:
            return
        # No perseguir P si ya venimos de una racha P larga (tocaría esperar C)
        if self._current_p_streak() >= MAX_P_STREAK_NO_SIGNAL:
            return
        prob, detalle = self._combined_p_rate()
        if prob is None:
            return
        if prob < SIGNAL_MIN_P_PROB:
            log.info(f"⏸️ Sin señal: P={prob * 100:.1f}% < {SIGNAL_MIN_P_PROB * 100:.0f}% ({detalle})")
            return
        self.signal_active = True
        self.signal_prob = prob
        self.signal_after_number = self.last_number
        log.info(f"🚨 SEÑAL P activa tras {self.last_number} con prob {prob * 100:.1f}% ({detalle})")
        asyncio.create_task(send_msg(build_signal_message(self.last_number, prob), CHANNEL_SIGNALS))

    def update(self, number: int, game_id=None, ts: float = None):
        if ts is None:
            ts = time.time()
        self.daily_marker.check_new_day()
        color = color_of(number)
        cls = cls_of(number)

        # 1) Guardar giro en SQLite
        seq = self.db.add_spin(game_id, number, color, cls, ts)

        # 2) Transición: después del número anterior, qué vino ahora
        if self.last_number is not None:
            self.db.bump_transition(self.last_number, cls)

        # 3) Rachas P/C con rondas de inicio/fin
        if self.current_run_cls is None:
            self.current_run_cls = cls
            self.current_run_start = seq
        elif cls != self.current_run_cls:
            self.db.close_run(self.current_run_cls, self.current_run_start, seq - 1)
            self.current_run_cls = cls
            self.current_run_start = seq

        self.spin_history.append({"number": number, "color": color, "cls": cls, "seq": seq, "ts": ts})
        if len(self.spin_history) > 200:
            self.spin_history.pop(0)
        self.class_history.append(cls)
        self.last_number = number

        # 4) Resolver señal activa (un solo intento) o evaluar nueva
        if self.signal_active:
            win = (cls == "P")
            self.signal_active = False
            self.cooldown = SIGNAL_COOLDOWN_SPINS
            self.stats["won" if win else "lost"] += 1
            self.daily_marker.record(win)
            self.last_signals.append({"win": win, "number": number,
                                      "prob": round(self.signal_prob * 100),
                                      "ts": ts})
            self.last_signals = self.last_signals[-10:]
            res_msg = build_win_message(number) if win else build_loss_message(number)
            asyncio.create_task(send_msg(res_msg, CHANNEL_SIGNALS))
            asyncio.create_task(send_msg(self.daily_marker.message(), self.daily_marker.chat_id))
            log.info(f"{'✅ WIN' if win else '❎ LOSS'} 1 EXP — {number} {color} (se esperaba P)")
        else:
            if self.cooldown > 0:
                self.cooldown -= 1
            self._maybe_fire_signal()

        ultimos = "".join(self.class_history[-12:])
        log.info(f"🎰 Latina ({self.key}) | Giro #{seq}: {number} {color} → {cls} | "
                 f"Sec: [{ultimos}] | Racha P×{self._current_p_streak()} C×{self._current_c_streak()} | "
                 f"Señal: {'ACTIVA P' if self.signal_active else 'no'} | "
                 f"W{self.stats['won']}/L{self.stats['lost']}")

    def get_state(self):
        return {
            "key": self.key,
            "giros": len(self.class_history),
            "ultimos_12": "".join(self.class_history[-12:]),
            "racha_P": self._current_p_streak(),
            "racha_C": self._current_c_streak(),
            "senal_activa": self.signal_active,
            "senal_prob": round(self.signal_prob * 100, 1) if self.signal_active else None,
            "stats": self.stats,
            "marcador_hoy": self.daily_marker.stats,
            "ultimas_senales": self.last_signals,
        }


# ══════════════════════════════════════════════
#  COMANDOS TELEGRAM
# ══════════════════════════════════════════════
_server_state: Optional["ServerState"] = None

def build_status_message(state) -> str:
    t = state.table
    s = t.stats
    total = s["won"] + s["lost"]
    rate = f"{(s['won'] / total) * 100:.1f}%" if total else "-"
    senal = (f"🚨 ACTIVA: entrar P después del {t.signal_after_number} "
             f"(prob {t.signal_prob * 100:.0f}%) — SOLO 1 INTENTO"
             if t.signal_active else "⏸️ Sin señal activa")
    ultimos = " ".join(t.class_history[-15:])
    return (f"📊 ESTADO — LATINA ROULETTE (key {t.key})\n\n"
            f"🎰 Giros registrados: {len(t.class_history)}\n"
            f"🔤 Secuencia P/C reciente: {ultimos}\n"
            f"🔥 Racha actual: P×{t._current_p_streak()} · C×{t._current_c_streak()}\n"
            f"📡 Señal: {senal}\n\n"
            f"📈 Total señales: {total} | ✅ {s['won']} ❌ {s['lost']} → Efectividad {rate}\n"
            f"🧠 Umbral: P ≥ {SIGNAL_MIN_P_PROB * 100:.0f}% · muestra ≥ {SIGNAL_MIN_SAMPLES} por número")


def build_stats_message(state) -> str:
    """Tabla de transiciones por número: tras X número, % P vs C."""
    trans = state.table.db.get_all_transitions()
    if not trans:
        return "🧠 ESTADÍSTICAS POR NÚMERO\n\nTodavía no hay datos suficientes."
    filas = []
    for n in range(37):
        if n not in trans:
            continue
        p, c = trans[n]
        tot = p + c
        pct = p / tot * 100
        flag = "🟢" if tot >= SIGNAL_MIN_SAMPLES and pct >= SIGNAL_MIN_P_PROB * 100 else "▫️"
        filas.append((n, p, c, tot, pct, flag))
    filas.sort(key=lambda x: -x[4])
    lines = ["🧠 ESTADÍSTICAS POR NÚMERO",
             "(tras salir el número: cuántas veces el siguiente giro fue P)",
             ""]
    for n, p, c, tot, pct, flag in filas[:15]:
        lines.append(f"{flag} {n:>2} {color_of(n):<6} → P {p:>3} | C {c:>3} | "
                     f"total {tot:>3} | P {pct:.1f}%")
    p_tot = sum(v[0] for v in trans.values())
    c_tot = sum(v[1] for v in trans.values())
    lines.append("")
    lines.append(f"📌 Global: P {p_tot} · C {c_tot} → {p_tot / (p_tot + c_tot) * 100:.1f}% P")
    return "\n".join(lines)


if bot is not None:
    @bot.message_handler(commands=["status"])
    async def handle_status(message):
        if _server_state is None:
            await bot.reply_to(message, "⏳ Iniciando, intenta en unos segundos.")
            return
        try:
            await bot.reply_to(message, build_status_message(_server_state))
        except Exception as e:
            log.warning(f"[Telegram] /status: {e}")

    @bot.message_handler(commands=["stats"])
    async def handle_stats(message):
        if _server_state is None:
            await bot.reply_to(message, "⏳ Iniciando, intenta en unos segundos.")
            return
        try:
            text = build_stats_message(_server_state)
            for i in range(0, len(text), 3800):
                await bot.reply_to(message, text[i:i + 3800])
        except Exception as e:
            log.warning(f"[Telegram] /stats: {e}")

    @bot.message_handler(commands=["marcador"])
    async def handle_marcador(message):
        if _server_state is None:
            await bot.reply_to(message, "⏳ Iniciando, intenta en unos segundos.")
            return
        try:
            await bot.reply_to(message, _server_state.table.daily_marker.message())
        except Exception as e:
            log.warning(f"[Telegram] /marcador: {e}")

    async def _register_bot_commands():
        if BotCommand is None:
            return
        try:
            await bot.set_my_commands([
                BotCommand("status", "Estado general: secuencia, racha y señal activa"),
                BotCommand("stats", "Estadística P/C tras cada número (0-36)"),
                BotCommand("marcador", "Marcador diario de señales"),
            ])
        except Exception as e:
            log.warning(f"[Telegram] No se pudo registrar el menú: {e}")


# ══════════════════════════════════════════════
#  HTTP MINIMAL (ping / health / state)
# ══════════════════════════════════════════════
async def http_ping(request: web.Request):
    return web.json_response({"status": "pong", "ts": time.time()})


async def http_health(request: web.Request):
    if _server_state is None:
        return web.json_response({"status": "not_ready"}, status=503)
    return web.json_response({"status": "ok", "mesa": LATINA_KEY,
                              "giros": len(_server_state.table.class_history)})


async def http_state(request: web.Request):
    if _server_state is None:
        return web.json_response({"error": "not ready"}, status=503)
    return web.json_response(_server_state.table.get_state())


def build_http_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/ping", http_ping)
    app.router.add_get("/health", http_health)
    app.router.add_get("/api/state", http_state)
    app.router.add_get("/", http_health)
    return app


# ══════════════════════════════════════════════
#  WEBSOCKET PRAGMATIC
# ══════════════════════════════════════════════
class PragmaticWebSocketHandler:
    def __init__(self, key: int, on_spin: Callable[[int, object], Awaitable[None]]):
        self.key = key
        self.on_spin = on_spin
        self.seen = set()

    async def run(self):
        sub = {"type": "subscribe", "casinoId": CASINO_ID, "currency": CURRENCY_ID, "key": [self.key]}
        delay = 5
        while True:
            try:
                async with websockets.connect(WS_URL, ping_interval=30, ping_timeout=60, close_timeout=10) as ws:
                    await ws.send(json.dumps(sub))
                    log.info(f"✅ WS Pragmatic conectado (key={self.key} - Latina Roulette)")
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
                                await self._feed(r.get("gameId"), r.get("result"))
                        if data.get("gameId") is not None and data.get("result") is not None:
                            await self._feed(data.get("gameId"), data.get("result"))
            except Exception as e:
                log.warning(f"🔌 WS key={self.key}: {e}. Reconectando en {delay}s…")
            await asyncio.sleep(delay)
            delay = min(delay * 2, 60)

    async def _feed(self, gid, result):
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
        if self.on_spin:
            await self.on_spin(num, gid)


# ══════════════════════════════════════════════
#  SERVER STATE
# ══════════════════════════════════════════════
class ServerState:
    def __init__(self):
        self.db = SignalDB(DB_PATH)
        self.table = LatinaTable(LATINA_KEY, self.db)


# ══════════════════════════════════════════════
#  SELF-PING Y BOT POLLING
# ══════════════════════════════════════════════
async def self_ping_loop():
    render_url = os.environ.get("RENDER_EXTERNAL_URL", "").rstrip("/")
    if not render_url or "localhost" in render_url:
        return
    await asyncio.sleep(30)
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
        return
    delay = 5
    while True:
        try:
            await bot.delete_webhook(drop_pending_updates=True)
        except Exception:
            pass
        started = time.time()
        try:
            await bot.infinity_polling(skip_pending=True, timeout=20, request_timeout=30)
        except Exception as e:
            if "409" in str(e):
                await asyncio.sleep(60)
                continue
            log.warning(f"[Telegram] Polling interrumpido: {e}")
        ran_for = time.time() - started
        delay = 5 if ran_for >= 60 else min(delay * 2, 120)
        await asyncio.sleep(delay)


# ══════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════
async def main():
    global _server_state
    log.info("═" * 60)
    log.info("BOT LATINA ROULETTE (key 233) — SEÑAL P / 1 INTENTO")
    log.info(f"P: {sorted(P_NUMBERS)}")
    log.info(f"C: {sorted(C_NUMBERS)}")
    log.info(f"DB: {DB_PATH} | Umbral P ≥ {SIGNAL_MIN_P_PROB * 100:.0f}% | "
             f"Muestra ≥ {SIGNAL_MIN_SAMPLES} | Cooldown {SIGNAL_COOLDOWN_SPINS} giros")
    log.info("═" * 60)

    server_state = ServerState()
    _server_state = server_state

    async def on_spin(num: int, gid):
        server_state.table.update(num, game_id=gid)

    tasks = [asyncio.create_task(PragmaticWebSocketHandler(LATINA_KEY, on_spin).run())]
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
    log.info(f"HTTP en puerto {port} (ping / health / api/state)")

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
