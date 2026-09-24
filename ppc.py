"""
╔══════════════════════════════════════════════════════════════╗
║   BOT DE PLENOS — SPEED ROULETTE (centro + 9 vecinos por lado)║
║   - Análisis de ZONA DEL CILINDRO: las 3 últimas rondas deben ║
║     caer en la misma zona de la rueda; se toman sus vecinos   ║
║     (3 x 3 = 9 números) y se mira en el histórico qué salió   ║
║     en la ronda siguiente cuando 3 rondas seguidas cayeron    ║
║     dentro de esos 9. Se guardan también las últimas 10.      ║
║   - Análisis de SENTIDO DE GIRO: la rueda gira siempre al     ║
║     mismo lado (antihorario); se estudian los saltos entre    ║
║     giros seguidos en ese sentido, sin espejo.                ║
║   - Centro: RandomForest (últimos 3 giros) + zona + giro; se  ║
║     apuesta al centro y 9 vecinos a cada lado (19 números).   ║
║   - 2 intentos por señal; la ficha se duplica en el intento 2 ║
║   - Comando Telegram: /plenos (win-rate vs azar, envío)       ║
║   - HTTP: /ping, /health, /api/state/{mesa}, /api/all         ║
║   - Persistencia en model_<key>.json + self-ping (Render)     ║
╚══════════════════════════════════════════════════════════════╝
"""

import asyncio
import json
import logging
import os
import sqlite3
import sys
import time
from typing import Optional, Callable, Awaitable

import websockets
from aiohttp import web, ClientSession, ClientTimeout

# PLENOS: numpy + scikit-learn son opcionales (sin ellos el centro usa frecuencias recientes)
try:
    import numpy as np
    from sklearn.ensemble import RandomForestClassifier
    SKLEARN_OK = True
except ImportError:
    np = None
    RandomForestClassifier = None
    SKLEARN_OK = False

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

ROULETTE_KEYS = {205: 205}   # Roulette 2 Extra Time

REAL_COLOR_MAP = {
    0: "VERDE", 1: "ROJO", 2: "NEGRO", 3: "ROJO", 4: "NEGRO", 5: "ROJO", 6: "NEGRO",
    7: "ROJO", 8: "NEGRO", 9: "ROJO", 10: "NEGRO", 11: "NEGRO", 12: "ROJO", 13: "NEGRO",
    14: "ROJO", 15: "NEGRO", 16: "ROJO", 17: "NEGRO", 18: "ROJO", 19: "ROJO", 20: "NEGRO",
    21: "ROJO", 22: "NEGRO", 23: "ROJO", 24: "NEGRO", 25: "ROJO", 26: "NEGRO", 27: "ROJO",
    28: "NEGRO", 29: "NEGRO", 30: "ROJO", 31: "NEGRO", 32: "ROJO", 33: "NEGRO", 34: "ROJO",
    35: "NEGRO", 36: "ROJO"
}


# ── Telegram: canales específicos (ya no un grupo con topics) ──
BOT_TOKEN       = os.environ.get("BOT_TOKEN", "8347707121:AAH1cPEDMLbm-scTJ8mUuufeEhzw3Axv2Lw")
CHANNEL_SIGNALS = int(os.environ.get("CHANNEL_SIGNALS", "-1004228660174"))
TABLE_LINK     = os.environ.get("TABLE_LINK", "https://1win.com/es-MX/casino/play/v_pragmatic:speedroulette2")
TABLE_NAME     = "Ruleta = Speed Roulette 2"

HISTORY_SEED_PATH  = os.environ.get("HISTORY_SEED_PATH", "russian-azure.db")
HISTORY_SEED_TABLE = os.environ.get("HISTORY_SEED_TABLE", "roulette_1")

# ── PLENOS: centro + vecinos en la rueda (RandomForest + zona del cilindro + sentido de giro) ──
# Se apuesta a PLENO_NEIGHBORS_EACH_SIDE números a cada lado del centro X en el
# orden físico de la rueda europea: 9 + centro + 9 = 19 números.
PLENO_NEIGHBORS_EACH_SIDE = int(os.environ.get("PLENO_NEIGHBORS_EACH_SIDE", "9"))
PLENO_MAX_ATTEMPTS        = int(os.environ.get("PLENO_MAX_ATTEMPTS", "2"))
PLENO_WINDOW_SIZE         = 3       # giros previos como features (igual que roulette_prediction.py)
PLENO_TRAIN_MIN_SPINS     = 300     # giros mínimos para entrenar el RandomForest
PLENO_TRAIN_MAX_SPINS     = 2000    # solo se entrena con los últimos N giros
PLENO_RETRAIN_EVERY       = 25      # reentrena cada N giros en vivo
PLENO_HISTORY_MAX         = 3000    # giros que se persisten en model_<key>.json
PLENO_RF_TREES            = 100
PLENO_RF_MIN_LEAF         = 3       # suaviza las probabilidades (con 1 hoja quedan 0/1)
PLENO_PROB_SMOOTHING      = 0.20    # mezcla con distribución uniforme
# ── ANÁLISIS DE ZONA DEL CILINDRO ──
ZONA_VECINOS         = 1      # vecinos por lado de cada una de las 3 últimas rondas (3 x 3 = 9 números)
ZONA_ARCO_MAX        = 10     # las 3 últimas rondas deben caber en un arco de N casillas de la rueda
ZONA_MIN_MATCHES     = 12     # coincidencias históricas mínimas (3 rondas seguidas dentro de los 9)
ZONA_PESO            = 0.4    # peso del histórico de la zona al calcular el centro (el RF recibe el resto)
ZONA_SUAVIZADO       = 0.5    # suavizado Laplace de la distribución histórica de la ronda siguiente
ZONA_MIN_COBERTURA   = 0.60   # % de las rondas siguientes históricas que caen dentro de las 19 casillas
ZONA_CENTRO_TOL      = 3      # el centro del ML puede quedar hasta N casillas fuera del arco de la zona
ZONA_ULTIMAS         = 10     # rondas recientes que se guardan para el análisis
ZONA_MIN_EN_ULTIMAS  = 4      # de las últimas 10 rondas, mínimo N dentro de la zona (las 3 del disparo cuentan)
# ── SENTIDO DE GIRO: la rueda gira siempre al mismo lado (contra las agujas del reloj), nunca cambia ──
# WHEEL_ORDER está escrito en sentido horario; con giro antihorario los casilleros avanzan hacia
# índices menores. Se mide el "salto" entre giros seguidos en ESE sentido (0..36 casillas, sin espejo).
GIRO_ANTIHORARIO     = os.environ.get("GIRO_SENTIDO", "antihorario").lower() != "horario"
GIRO_TOL             = 2      # tolerancia (casillas) al comparar los 2 últimos saltos con los históricos
GIRO_MIN_MATCHES     = 15     # coincidencias mínimas para que el análisis de giro cuente
GIRO_PESO            = 0.2    # peso del análisis de giro al calcular el centro
GIRO_MIN_COBERTURA   = 0.55   # % de destinos históricos por salto que caen dentro de las 19 casillas
PLENO_STATS_WINDOW        = 50      # señales cerradas que entran en el win-rate
PLENO_SEND_MIN_SAMPLES    = int(os.environ.get("PLENO_SEND_MIN_SAMPLES", "30"))
PLENO_SEND_MIN_WIN_RATE   = float(os.environ.get("PLENO_SEND_MIN_WIN_RATE", "0.80"))
CHANNEL_PLENOS            = int(os.environ.get("CHANNEL_PLENOS", str(CHANNEL_SIGNALS)))
PLENO_CHIP_VALUE          = int(os.environ.get("PLENO_CHIP_VALUE", "50"))   # COP por número

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


_server_state: Optional["ServerState"] = None   # forward reference: ServerState se define más abajo


# ══════════════════════════════════════════════
#  TELEGRAM
# ══════════════════════════════════════════════
bot = AsyncTeleBot(BOT_TOKEN, parse_mode="HTML") if (TELEBOT_OK and BOT_TOKEN) else None
if bot is None:
    log.warning("Telegram deshabilitado (falta BOT_TOKEN o la librería 'telebot').")

async def send_msg(text: str, chat_id: int, retries: int = 3) -> Optional[int]:
    if bot is None: return None
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

async def edit_msg(msg_id: int, text: str, chat_id: int = CHANNEL_SIGNALS) -> bool:
    if bot is None or msg_id is None: return False
    try:
        await bot.edit_message_text(chat_id=chat_id, message_id=msg_id, text=text,
                                    parse_mode="HTML", disable_web_page_preview=True)
        return True
    except Exception as e:
        log.debug(f"[Telegram] Error editando mensaje {msg_id}: {e}")
        return False

async def delete_msg(msg_id: int, chat_id: int = CHANNEL_SIGNALS) -> bool:
    if bot is None or msg_id is None: return False
    try:
        await bot.delete_message(chat_id=chat_id, message_id=msg_id)
        return True
    except Exception as e:
        log.debug(f"[Telegram] Error eliminando mensaje {msg_id}: {e}")
        return False

if bot is not None:
    @bot.message_handler(commands=["plenos"])
    async def handle_plenos_command(message):
        if _server_state is None:
            await bot.reply_to(message, "⏳ El servidor todavía se está iniciando, intenta de nuevo en unos segundos.")
            return
        try:
            await bot.reply_to(message, build_pleno_status_message(_server_state))
        except Exception as e:
            log.warning(f"[Telegram] Error respondiendo /plenos: {e}")

    async def _register_bot_commands():
        if BotCommand is None:
            return
        try:
            await bot.set_my_commands([
                BotCommand("plenos", "Centro ± vecinos: win-rate vs azar y estado del envío"),
            ])
        except Exception as e:
            log.warning(f"[Telegram] No se pudo registrar el menú de comandos: {e}")


# ══════════════════════════════════════════════
#  PLENOS: CENTRO (RandomForest) + ZONA DEL CILINDRO + SENTIDO DE GIRO → 9 IZQ + CENTRO + 9 DER
# ══════════════════════════════════════════════
# Orden físico de los números en la rueda europea (single zero, sentido horario).
WHEEL_ORDER = (0, 32, 15, 19, 4, 21, 2, 25, 17, 34, 6, 27, 13, 36, 11, 30, 8, 23, 10, 5,
               24, 16, 33, 1, 20, 14, 31, 9, 22, 18, 29, 7, 28, 12, 35, 3, 26)
WHEEL_POS = {n: i for i, n in enumerate(WHEEL_ORDER)}


def pleno_window_size(each_side: int = None) -> int:
    k = PLENO_NEIGHBORS_EACH_SIDE if each_side is None else each_side
    return min(2 * k + 1, len(WHEEL_ORDER))


def wheel_window(center: int, each_side: int = None) -> list:
    """Devuelve [izq ... centro ... der] siguiendo el orden de la rueda (circular)."""
    k = PLENO_NEIGHBORS_EACH_SIDE if each_side is None else each_side
    k = min(k, (len(WHEEL_ORDER) - 1) // 2)
    i = WHEEL_POS[center]
    L = len(WHEEL_ORDER)
    return [WHEEL_ORDER[(i + d) % L] for d in range(-k, k + 1)]


def pleno_baseline(attempts: int = None, n_numbers: int = None) -> float:
    """Probabilidad de acierto por azar puro con n números y N intentos."""
    a = PLENO_MAX_ATTEMPTS if attempts is None else attempts
    n = pleno_window_size() if n_numbers is None else n_numbers
    return 1.0 - (1.0 - n / 37.0) ** a


class CenterPredictor:
    """
    Predice el próximo número (RandomForest sobre los últimos PLENO_WINDOW_SIZE giros, igual que
    roulette_prediction.py) pero en vez de quedarse con un solo número usa predict_proba y la
    mezcla con el histórico de la zona del cilindro (ZONA_PESO). El CENTRO es el número cuya
    ventana de 19 casillas (9 izq + centro + 9 der en la rueda) acumula más probabilidad.
    """
    def __init__(self):
        self.numbers = []
        self.model = None
        self.classes = []
        self.spins_since_train = 0
        self.last_train_ts = 0.0
        self.last_train_n = 0

    # ── datos ──
    def add(self, number: int, allow_train: bool = True):
        self.numbers.append(int(number))
        if len(self.numbers) > PLENO_HISTORY_MAX:
            del self.numbers[:len(self.numbers) - PLENO_HISTORY_MAX]
        self.spins_since_train += 1
        if not allow_train:
            return
        if self.model is None:
            if len(self.numbers) >= PLENO_TRAIN_MIN_SPINS:
                self.train()
        elif self.spins_since_train >= PLENO_RETRAIN_EVERY:
            self.train()

    def load_numbers(self, numbers):
        self.numbers = [int(n) for n in numbers if 0 <= int(n) <= 36][-PLENO_HISTORY_MAX:]
        self.spins_since_train = 0

    def ready(self) -> bool:
        return len(self.numbers) >= PLENO_TRAIN_MIN_SPINS

    # ── modelo ──
    @staticmethod
    def _prepare(data, w):
        X = np.array([data[i:i + w] for i in range(len(data) - w)])
        y = np.array(data[w:])
        return X, y

    def train(self) -> bool:
        if not SKLEARN_OK:
            return False
        data = self.numbers[-PLENO_TRAIN_MAX_SPINS:]
        if len(data) < max(PLENO_TRAIN_MIN_SPINS, PLENO_WINDOW_SIZE + 20):
            return False
        try:
            X, y = self._prepare(data, PLENO_WINDOW_SIZE)
            model = RandomForestClassifier(n_estimators=PLENO_RF_TREES, random_state=42,
                                           min_samples_leaf=PLENO_RF_MIN_LEAF, n_jobs=1)
            model.fit(X, y)
            self.model = model
            self.classes = [int(c) for c in model.classes_]
            self.spins_since_train = 0
            self.last_train_ts = time.time()
            self.last_train_n = len(data)
            log.info(f"[Plenos] RandomForest entrenado con {len(data)} giros")
            return True
        except Exception as e:
            log.warning(f"[Plenos] Error entrenando RandomForest: {e}")
            return False

    def force_train(self) -> bool:
        return self.train()

    def _base_probs(self):
        """Distribución sobre 0..36 para el próximo número. Devuelve (probs, fuente)."""
        u = 1.0 / 37.0
        if self.model is not None and len(self.numbers) >= PLENO_WINDOW_SIZE:
            try:
                window = np.array(self.numbers[-PLENO_WINDOW_SIZE:]).reshape(1, -1)
                pr = self.model.predict_proba(window)[0]
                p = [0.0] * 37
                for c, v in zip(self.classes, pr):
                    p[c] = float(v)
                a = PLENO_PROB_SMOOTHING
                return [(1 - a) * v + a * u for v in p], "rf"
            except Exception as e:
                log.warning(f"[Plenos] Error en predict_proba: {e}")
        recent = self.numbers[-300:]
        if len(recent) >= 30:
            cnt = [1.0] * 37
            for n in recent:
                cnt[n] += 1.0
            tot = sum(cnt)
            return [c / tot for c in cnt], "freq"
        return [u] * 37, "uniforme"

    def predict_center(self, zona_q=None, giro_q=None) -> dict:
        base, source = self._base_probs()
        wz = ZONA_PESO if zona_q else 0.0
        wg = GIRO_PESO if giro_q else 0.0
        wb = 1.0 - wz - wg
        zq = zona_q or [0.0] * 37
        gq = giro_q or [0.0] * 37
        comb = [wb * b + wz * z + wg * g for b, z, g in zip(base, zq, gq)]
        tot = sum(comb) or 1.0
        comb = [c / tot for c in comb]
        k = min(PLENO_NEIGHBORS_EACH_SIDE, (len(WHEEL_ORDER) - 1) // 2)
        L = len(WHEEL_ORDER)
        best_mass, best_i = -1.0, 0
        for i in range(L):
            mass = sum(comb[WHEEL_ORDER[(i + d) % L]] for d in range(-k, k + 1))
            if mass > best_mass + 1e-12:
                best_mass, best_i = mass, i
        center = WHEEL_ORDER[best_i]
        rf_top = max(range(37), key=lambda n: base[n])
        return {"center": center, "window": wheel_window(center), "mass": best_mass,
                "rf_top": rf_top, "source": source}

    def persist(self) -> list:
        return self.numbers[-PLENO_HISTORY_MAX:]


def wheel_neighbors(n: int, each_side: int = None) -> list:
    """El número y sus vecinos en la rueda (ej. 21 -> [4, 21, 2])."""
    k = ZONA_VECINOS if each_side is None else each_side
    i = WHEEL_POS[n]
    L = len(WHEEL_ORDER)
    return [WHEEL_ORDER[(i + d) % L] for d in range(-k, k + 1)]


def zona_de_tres(last3):
    """
    Zona del cilindro que ocupan las 3 últimas rondas. Devuelve None si no están en la misma zona
    (no caben en un arco de ZONA_ARCO_MAX casillas). S = las 3 rondas + sus vecinos (hasta 9 números);
    'zona' = casillas del arco que cubre a S, en orden de rueda.
    """
    L = len(WHEEL_ORDER)
    pos = sorted(WHEEL_POS[n] for n in last3)
    huecos = [(pos[0], pos[1], pos[1] - pos[0]),
              (pos[1], pos[2], pos[2] - pos[1]),
              (pos[2], pos[0], L - (pos[2] - pos[0]))]
    _, inicio, hueco = max(huecos, key=lambda h: h[2])   # el arco empieza justo después del mayor hueco
    arco = L - hueco + 1
    if arco > ZONA_ARCO_MAX:
        return None
    S = set()
    for n in last3:
        S.update(wheel_neighbors(n))
    ini = (inicio - ZONA_VECINOS) % L
    zona = [WHEEL_ORDER[(ini + i) % L] for i in range(arco + 2 * ZONA_VECINOS)]
    return {"S": S, "zona": zona, "arco": arco}


def zona_historial(numbers, S) -> list:
    """
    Historial: cada vez que en 3 rondas seguidas salieron 3 números de S, se guarda el número
    de la ronda siguiente. (La tripleta actual, que aún no tiene siguiente, queda fuera.)
    """
    out = []
    for i in range(2, len(numbers) - 1):
        if numbers[i] in S and numbers[i - 1] in S and numbers[i - 2] in S:
            out.append(numbers[i + 1])
    return out


def zona_q(nexts) -> list:
    """Distribución (0..36) de la ronda siguiente según el histórico de la zona."""
    cnt = [ZONA_SUAVIZADO] * 37
    for n in nexts:
        cnt[n] += 1.0
    tot = sum(cnt)
    return [c / tot for c in cnt]


def giro_paso(a: int, b: int) -> int:
    """Casillas que avanza la rueda, en su sentido de giro, desde el número a hasta el b (0..36)."""
    L = len(WHEEL_ORDER)
    if GIRO_ANTIHORARIO:
        return (WHEEL_POS[a] - WHEEL_POS[b]) % L
    return (WHEEL_POS[b] - WHEEL_POS[a]) % L


def giro_destino(a: int, paso: int) -> int:
    """Número al que se llega avanzando 'paso' casillas desde a en el sentido de giro."""
    L = len(WHEEL_ORDER)
    if GIRO_ANTIHORARIO:
        return WHEEL_ORDER[(WHEEL_POS[a] - paso) % L]
    return WHEEL_ORDER[(WHEEL_POS[a] + paso) % L]


def _dist_circ(a: int, b: int) -> int:
    L = len(WHEEL_ORDER)
    return min((a - b) % L, (b - a) % L)


def giro_analisis(numbers):
    """
    Saltos entre giros seguidos, siempre en el mismo sentido. Toma los 2 últimos saltos y busca en
    el histórico cuándo ocurrieron saltos parecidos (±GIRO_TOL); el salto que vino después,
    aplicado al último número, da los destinos probables. Devuelve (q, coincidencias, destinos).
    """
    if len(numbers) < 6:
        return None, 0, []
    pasos = [giro_paso(numbers[k], numbers[k + 1]) for k in range(len(numbers) - 1)]
    u, v = pasos[-2], pasos[-1]
    sigs = []
    for k in range(1, len(pasos) - 1):
        if _dist_circ(pasos[k - 1], u) <= GIRO_TOL and _dist_circ(pasos[k], v) <= GIRO_TOL:
            sigs.append(pasos[k + 1])
    ultimo = numbers[-1]
    destinos = [giro_destino(ultimo, p) for p in sigs]
    return zona_q(destinos), len(destinos), destinos


def priors_plenos(numbers, S) -> dict:
    """Histórico de zona (ronda siguiente a 3 rondas dentro de S) + análisis de sentido de giro."""
    nexts = zona_historial(numbers, S)
    gq, gm, destinos = giro_analisis(numbers)
    return {"nexts": nexts, "zona_q": zona_q(nexts),
            "giro_q": gq if gm >= GIRO_MIN_MATCHES else None, "giro_m": gm, "destinos": destinos}


def zona_evaluar(numbers, predictor, ultimas):
    """
    Análisis de plenos por zona del cilindro + sentido de giro. Devuelve (señal, motivo): la señal
    es None si algún filtro falla y 'motivo' explica cuál.
    """
    if len(numbers) < 3:
        return None, "sin datos"
    last3 = numbers[-3:]
    z = zona_de_tres(last3)
    if z is None:
        return None, "3 rondas fuera de una misma zona"
    pr = priors_plenos(numbers, z["S"])
    nexts = pr["nexts"]
    m = len(nexts)
    if m < ZONA_MIN_MATCHES:
        return None, f"pocas coincidencias históricas ({m})"
    zset = set(z["zona"])
    en_ultimas = sum(1 for n in ultimas if n in zset)
    if en_ultimas < ZONA_MIN_EN_ULTIMAS:
        return None, f"zona fría en las últimas {ZONA_ULTIMAS} ({en_ultimas})"
    pred = predictor.predict_center(pr["zona_q"], pr["giro_q"])
    L = len(WHEEL_ORDER)
    cpos = WHEEL_POS[pred["center"]]
    dist = min(min((cpos - WHEEL_POS[n]) % L, (WHEEL_POS[n] - cpos) % L) for n in z["zona"])
    if dist > ZONA_CENTRO_TOL:
        return None, f"centro ML {pred['center']} fuera de la zona (a {dist} casillas)"
    win = set(pred["window"])
    cobertura = sum(1 for n in nexts if n in win) / m
    if cobertura < ZONA_MIN_COBERTURA:
        return None, f"cobertura histórica baja ({cobertura*100:.0f}%)"
    giro_cob = None
    if pr["giro_q"]:
        giro_cob = sum(1 for d in pr["destinos"] if d in win) / pr["giro_m"]
        if giro_cob < GIRO_MIN_COBERTURA:
            return None, f"cobertura de giro baja ({giro_cob*100:.0f}%)"
    sig = {
        "trio": list(last3), "S": sorted(z["S"]), "zona": z["zona"], "matches": m,
        "cobertura": cobertura, "en_ultimas": en_ultimas, "center": pred["center"],
        "window": pred["window"], "rf_top": pred["rf_top"], "fuente": pred["source"],
        "giro_matches": pr["giro_m"], "giro_cobertura": giro_cob,
        "attempt": 1, "numbers": [], "sent": False, "msg_id": None,
    }
    return sig, "ok"


def _pleno_lines(window):
    k = len(window) // 2
    left = " - ".join(str(n) for n in window[:k])
    right = " - ".join(str(n) for n in window[k + 1:])
    return left, window[k], right


def build_pleno_entry_message(last_number, sig: dict, attempt: int) -> str:
    color_emoji = {"ROJO": "🔴", "NEGRO": "⚫", "VERDE": "🟢"}
    numero = last_number if last_number is not None else "-"
    numero_emoji = color_emoji.get(color_of(last_number), "🟢") if last_number is not None else ""
    center = sig["center"]
    center_emoji = color_emoji.get(color_of(center), "🟢")
    k = (len(sig["window"]) - 1) // 2
    # Si se pierde el intento 1, el intento 2 duplica la ficha (x2 por cada intento perdido)
    chip = PLENO_CHIP_VALUE * (2 ** (attempt - 1))
    total = chip * len(sig["window"])
    link_line = f'🎮 <a href="{TABLE_LINK}">{TABLE_NAME}</a>' if TABLE_LINK else f"🎮 {TABLE_NAME}"
    return (f"🚨🚨 ENTRADA INTENTO {attempt} 🚨🚨\n\n"
            f"👉 INGRESAR DESPUÉS: {numero} ({numero_emoji})\n"
            f"🧨 CUBRIR {k} VECINOS: {center} ({center_emoji})\n\n"
            f"🇨🇴 VALOR DE FICHA: ${chip:,} COP\n"
            f"🇨🇴 APUESTA TOTAL: ${total:,} COP\n\n"
            f"💫 ¡Juego Responsable!\n{link_line}")


def build_pleno_resolution_message(win: bool, sig: dict) -> str:
    numbers_str = " | ".join(str(n) for n in sig["numbers"])
    header = "✅✅ PLENOS 👍🏻" if win else "❌❌ PLENOS 👎🏻"
    return f"{header} ({numbers_str}) | Centro {sig['center']} | Intento {sig['attempt']}"


def build_pleno_status_message(server_state) -> str:
    lines = ["🎯 PLENOS (centro ± vecinos en la rueda)"]
    lines.append(f"Sentido de giro: {'antihorario' if GIRO_ANTIHORARIO else 'horario'} (fijo, sin cambio de dirección)")
    base = pleno_baseline()
    lines.append(f"Cobertura: {pleno_window_size()} números | {PLENO_MAX_ATTEMPTS} intentos | "
                 f"azar puro = {base*100:.1f}%")
    for key, table in server_state.tables.items():
        cp = table.center_predictor
        r = table.pleno_results[-PLENO_STATS_WINDOW:]
        n = len(r)
        w = sum(1 for x in r if x["win"])
        rate = (w / n) if n else None
        sent = sum(1 for x in table.pleno_results if x.get("sent"))
        model_txt = (f"RF entrenado con {cp.last_train_n} giros" if cp.model is not None
                     else ("sin sklearn (usa frecuencias)" if not SKLEARN_OK else "RF sin entrenar"))
        lines.append(f"\n🎲 Mesa {key} ({TABLE_NAME})")
        lines.append(f"• Giros en el predictor: {len(cp.numbers)} | {model_txt}")
        if table.zona_ultimas10:
            lines.append(f"• Últimas {len(table.zona_ultimas10)}: {'-'.join(str(n) for n in table.zona_ultimas10)}")
        if rate is None:
            lines.append("• Señales cerradas: 0")
        else:
            diff = (rate - base) * 100
            lines.append(f"• Últimas {n} señales: {w}/{n} = {rate*100:.1f}% ({diff:+.1f} pts vs azar)")
        gate = "ABIERTO ✅" if table._pleno_gate_ok() else "cerrado ⛔"
        lines.append(f"• Envío a Telegram: {gate} (mín {PLENO_SEND_MIN_WIN_RATE*100:.0f}% con "
                     f"≥{PLENO_SEND_MIN_SAMPLES} señales) | enviadas: {sent}")
        if table.pleno_active:
            a = table.pleno_active
            lines.append(f"• Activa: {'-'.join(str(n) for n in a['trio'])} → centro {a['center']} "
                         f"intento {a['attempt']}/{PLENO_MAX_ATTEMPTS} ({a['matches']} coincidencias, "
                         f"{'enviada' if a['sent'] else 'sombra'})")
    return "\n".join(lines)

# ══════════════════════════════════════════════
#  MESA
# ══════════════════════════════════════════════
class RouletteTable:
    def __init__(self, key: int):
        self.key = key
        self.spin_history = []
        self.total_spins_seen = 0
        self.live_spins_seen = 0

        # ── PLENOS: predictor de centro + señal activa + resultados cerrados ──
        self.center_predictor = CenterPredictor()
        self.pleno_active = None
        self.pleno_results = []   # {"win","attempt","center","sent","trio","matches","cobertura","giro_matches","ts"}
        self.zona_ultimas10 = []  # últimas ZONA_ULTIMAS rondas (se persisten)

    # ── PLENOS ───────────────────────────────────────────────
    def _pleno_recent(self):
        r = self.pleno_results[-PLENO_STATS_WINDOW:]
        return len(r), sum(1 for x in r if x["win"])

    def _pleno_gate_ok(self) -> bool:
        n, w = self._pleno_recent()
        return n >= max(1, PLENO_SEND_MIN_SAMPLES) and (w / n) >= PLENO_SEND_MIN_WIN_RATE

    async def _pleno_send_entry(self, sig: dict, last_number, attempt: int):
        text = build_pleno_entry_message(last_number, sig, attempt)
        prev_id = sig.get("msg_id")
        sig["msg_id"] = await send_msg(text, CHANNEL_PLENOS)
        if attempt > 1 and prev_id:
            await delete_msg(prev_id, CHANNEL_PLENOS)

    def _pleno_try_open(self, last_number):
        """Abre una señal de plenos cuando las 3 últimas rondas cumplen el análisis de zona del cilindro."""
        if self.pleno_active is not None or not self.center_predictor.ready():
            return
        sig, motivo = zona_evaluar(self.center_predictor.numbers, self.center_predictor, self.zona_ultimas10)
        if sig is None:
            log.debug(f"[Plenos] sin señal: {motivo}")
            return
        sig["sent"] = self._pleno_gate_ok()
        self.pleno_active = sig
        n, w = self._pleno_recent()
        gc = sig["giro_cobertura"]
        giro_txt = f"{sig['giro_matches']} coinc" + ("" if gc is None else f", cobertura {gc*100:.0f}%")
        log.info(f"🎯 PLENOS zona {'-'.join(str(x) for x in sig['trio'])} | 9 vecinos {sig['S']} | "
                 f"{sig['matches']} coincidencias, cobertura {sig['cobertura']*100:.0f}%, "
                 f"giro {giro_txt}, "
                 f"{sig['en_ultimas']}/{ZONA_ULTIMAS} en zona → centro {sig['center']} "
                 f"(RF top {sig['rf_top']}, fuente {sig['fuente']}) | "
                 f"{'ENVIADA' if sig['sent'] else 'sombra'} | win-rate {w}/{n}")
        if sig["sent"]:
            asyncio.create_task(self._pleno_send_entry(sig, last_number, 1))

    def _pleno_resolve(self, number: int):
        sig = self.pleno_active
        if sig is None:
            return
        sig["numbers"].append(number)
        hit = number in sig["window"]
        if hit or sig["attempt"] >= PLENO_MAX_ATTEMPTS:
            self.pleno_results.append({"win": hit, "attempt": sig["attempt"], "center": sig["center"],
                                       "sent": sig["sent"], "trio": sig["trio"], "matches": sig["matches"],
                                       "cobertura": round(sig["cobertura"], 3),
                                       "giro_matches": sig["giro_matches"], "ts": time.time()})
            if len(self.pleno_results) > 200:
                self.pleno_results = self.pleno_results[-200:]
            log.info(f"🎯 PLENOS cerrado: {'WIN' if hit else 'LOSS'} intento {sig['attempt']} | "
                     f"centro {sig['center']} | salió {number}")
            if sig["sent"]:
                asyncio.create_task(send_msg(build_pleno_resolution_message(hit, sig), CHANNEL_PLENOS))
            self.pleno_active = None
            return
        # Reintento: se recalcula el centro con el modelo actualizado (misma zona y mismo giro)
        sig["attempt"] += 1
        pr = priors_plenos(self.center_predictor.numbers, set(sig["S"]))
        pred = self.center_predictor.predict_center(pr["zona_q"], pr["giro_q"])
        sig["center"], sig["window"] = pred["center"], pred["window"]
        log.info(f"🎯 PLENOS intento {sig['attempt']}: nuevo centro {sig['center']}")
        if sig["sent"]:
            asyncio.create_task(self._pleno_send_entry(sig, number, sig["attempt"]))

    def pleno_persist(self) -> dict:
        return {"numbers": self.center_predictor.persist(), "results": self.pleno_results[-200:],
                "ultimas10": self.zona_ultimas10[-ZONA_ULTIMAS:]}

    def pleno_load(self, data):
        if not data:
            return
        self.center_predictor.load_numbers(data.get("numbers", []))
        self.pleno_results = list(data.get("results", []))[-200:]
        self.zona_ultimas10 = list(data.get("ultimas10") or self.center_predictor.numbers[-ZONA_ULTIMAS:])[-ZONA_ULTIMAS:]
        if self.center_predictor.ready():
            self.center_predictor.train()

    def update(self, number: int, real_color: str, timestamp: float = None, training: bool = False):
        if timestamp is None:
            timestamp = time.time()
        self.spin_history.append({"number": number, "color": real_color, "timestamp": timestamp})
        if len(self.spin_history) > 200:
            self.spin_history.pop(0)
        self.total_spins_seen += 1
        if not training:
            self.live_spins_seen += 1

        # Alimenta el predictor de centro y las últimas rondas; en entrenamiento no se reentrena por giro
        self.center_predictor.add(number, allow_train=not training)
        self.zona_ultimas10 = (self.zona_ultimas10 + [number])[-ZONA_ULTIMAS:]
        if training:
            return

        self._pleno_resolve(number)
        self._pleno_try_open(number)
        log.info(f"🎰 Mesa {self.key} | Giro #{self.total_spins_seen}: {number} ({real_color}) | "
                 f"Últimas {ZONA_ULTIMAS}: [{','.join(str(n) for n in self.zona_ultimas10)}] | "
                 f"Pleno: {'activo (centro %s, intento %s)' % (self.pleno_active['center'], self.pleno_active['attempt']) if self.pleno_active else 'sin señal'}")

    def get_state(self, limit: int = 40):
        n, w = self._pleno_recent()
        a = self.pleno_active
        return {
            "key": self.key,
            "table_name": TABLE_NAME,
            "spin_history": self.spin_history[-limit:],
            "ultimas10": self.zona_ultimas10,
            "live_spins_seen": self.live_spins_seen,
            "total_spins_seen": self.total_spins_seen,
            "pleno": {
                "activa": None if a is None else {
                    "trio": a["trio"], "centro": a["center"], "ventana": a["window"],
                    "intento": a["attempt"], "intentos_max": PLENO_MAX_ATTEMPTS,
                    "coincidencias": a["matches"], "cobertura": a["cobertura"],
                    "giro_coincidencias": a["giro_matches"], "enviada": a["sent"],
                },
                "senales_cerradas": n, "aciertos": w,
                "win_rate": (w / n) if n else None,
                "azar": pleno_baseline(),
                "envio_abierto": self._pleno_gate_ok(),
                "giros_predictor": len(self.center_predictor.numbers),
                "modelo_rf_entrenado": self.center_predictor.model is not None,
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
    return web.json_response(table.get_state(limit=limit))

async def http_api_all(request: web.Request):
    if _server_state is None:
        return web.json_response({"error": "server not ready"}, status=503)
    result = {str(key): _server_state.get_state_for_mesa(key) for key in ROULETTE_KEYS.values()}
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
    for start in range(0, len(spins), BATCH_SIZE):
        for i, number in enumerate(spins[start:start + BATCH_SIZE]):
            if not (0 <= number <= 36):
                continue
            table.update(number, color_of(number), timestamp=timestamp, training=True)
            if i % 100 == 0:
                await asyncio.sleep(0)
        await asyncio.sleep(0.1)
    table.center_predictor.force_train()
    log.info(f"[Entrenamiento] Mesa {table.key}: listo. giros_vistos={table.total_spins_seen}")


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
        self.tables[key].update(number, color_of(number), training=training)

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
            table.total_spins_seen = data.get("table_total_spins_seen", table.total_spins_seen)
            self.history_seed_trained[key] = data.get("history_seed_trained", False)
            table.pleno_load(data.get("pleno"))
            log.info(f"Modelo cargado para mesa {key}")
        except Exception as e:
            log.warning(f"Error cargando modelo mesa {key}: {e}")

    def save_all_models(self):
        for key in self.tables:
            self._save_model(key)

    def _save_model(self, key: int):
        table = self.tables[key]
        data = {
            "table_total_spins_seen": table.total_spins_seen,
            "history_seed_trained": self.history_seed_trained.get(key, False),
            "pleno": table.pleno_persist(),
        }
        try:
            with open(f"model_{key}.json", "w") as f:
                json.dump(data, f)
        except Exception as e:
            log.warning(f"Error guardando modelo mesa {key}: {e}")

    async def train_from_history(self):
        spins_cache = None
        for key, table in self.tables.items():
            if self.history_seed_trained.get(key):
                # Si el predictor de centros quedó corto (p. ej. modelo guardado por una versión
                # anterior), se alimenta con el historial sin repetir todo el entrenamiento.
                if len(table.center_predictor.numbers) < PLENO_TRAIN_MIN_SPINS:
                    if spins_cache is None:
                        spins_cache = load_history_seed()
                    if spins_cache:
                        table.center_predictor.load_numbers(spins_cache)
                        table.center_predictor.force_train()
                log.info(f"[Entrenamiento] Mesa {key}: ya estaba entrenada con el historial, se omite.")
                continue
            if spins_cache is None:
                spins_cache = load_history_seed()
            if not spins_cache:
                continue
            await train_table_from_history(table, spins_cache, time.time())
            self.history_seed_trained[key] = True
            self._save_model(key)


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
        if ran_for < 60:
            delay = min(delay * 2, 120)
        else:
            delay = 5
        log.warning(f"[Telegram] Reintentando polling en {delay}s…")
        await asyncio.sleep(delay)


# ══════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════
async def main():
    global _server_state
    log.info("═" * 60)
    log.info("BOT DE PLENOS — ZONA DEL CILINDRO + SENTIDO DE GIRO | centro ± vecinos")
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
