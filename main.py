#!/usr/bin/env python3
"""
Roulette Telegram Signal Bot - Sistema AMX V24.0
CAMBIOS V24.0 (Ensemble ML Avanzado):
  - TCN: Captura patrones temporales con dilataciones
  - LSTM+Attention: Foco en giros relevantes
  - Transformer: Self-attention para dependencias globales
  - Ensemble dinámico con pesos adaptativos por rendimiento
  - Memoria de corto y largo plazo separada
"""

import asyncio
import io
import json
import logging
import os
import re
import threading
import time
import urllib.request
from collections import deque, defaultdict
from typing import Optional, Literal, Dict, List, Tuple
import math

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import telebot
import websockets
from flask import Flask, jsonify

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(name)s] %(levelname)s %(message)s')
logger = logging.getLogger("RouletteBotAMX")

TOKEN = "8308452662:AAGZFIZyYsmVR39SvIOSlKD3OY_YNMOsEQU"

_session = requests.Session()
_retry = Retry(total=5, backoff_factor=1.5,
               status_forcelist=[429, 500, 502, 503, 504],
               allowed_methods=["GET", "POST"], raise_on_status=False)
_adapter = HTTPAdapter(max_retries=_retry, pool_connections=10, pool_maxsize=20)
_session.mount("https://", _adapter)
_session.mount("http://",  _adapter)
bot = telebot.TeleBot(TOKEN, threaded=False)
bot.session = _session

DB_PATH  = "russian-azure.db"
DB_TABLE = "roulette_1"

REAL_COLOR_MAP = {
    0:"VERDE",1:"ROJO",2:"NEGRO",3:"ROJO",4:"NEGRO",5:"ROJO",6:"NEGRO",
    7:"ROJO",8:"NEGRO",9:"ROJO",10:"NEGRO",11:"NEGRO",12:"ROJO",13:"NEGRO",
    14:"ROJO",15:"NEGRO",16:"ROJO",17:"NEGRO",18:"ROJO",19:"ROJO",20:"NEGRO",
    21:"ROJO",22:"NEGRO",23:"ROJO",24:"NEGRO",25:"ROJO",26:"NEGRO",27:"ROJO",
    28:"NEGRO",29:"NEGRO",30:"ROJO",31:"NEGRO",32:"ROJO",33:"NEGRO",34:"ROJO",
    35:"NEGRO",36:"ROJO"
}

def get_paridad(number: int) -> Optional[str]:
    if number == 0: return None
    return "PAR" if number % 2 == 0 else "IMPAR"

def get_rango(number: int) -> Optional[str]:
    if number == 0: return None
    return "BAJO" if 1 <= number <= 18 else "ALTO"

CATEGORY_ICONS = {
    "ROJO":"🔴","NEGRO":"⚫️","PAR":"🟣","IMPAR":"🟡",
    "BAJO":"🟤","ALTO":"🔵","VERDE":"🟢","CERO":"🟢",
}

COLOR_DATA = [
    {"id":0,"rojo":0.52,"negro":0.44,"senal":"ROJO"},
    {"id":1,"rojo":0.52,"negro":0.48,"senal":"ROJO"},
    {"id":2,"rojo":0.60,"negro":0.40,"senal":"ROJO"},
    {"id":3,"rojo":0.56,"negro":0.40,"senal":"ROJO"},
    {"id":4,"rojo":0.56,"negro":0.40,"senal":"ROJO"},
    {"id":5,"rojo":0.52,"negro":0.48,"senal":"ROJO"},
    {"id":6,"rojo":0.56,"negro":0.40,"senal":"ROJO"},
    {"id":7,"rojo":0.52,"negro":0.44,"senal":"ROJO"},
    {"id":8,"rojo":0.52,"negro":0.44,"senal":"ROJO"},
    {"id":9,"rojo":0.52,"negro":0.44,"senal":"ROJO"},
    {"id":10,"rojo":0.48,"negro":0.48,"senal":"NO APOSTAR"},
    {"id":11,"rojo":0.52,"negro":0.44,"senal":"ROJO"},
    {"id":12,"rojo":0.56,"negro":0.44,"senal":"ROJO"},
    {"id":13,"rojo":0.56,"negro":0.40,"senal":"ROJO"},
    {"id":14,"rojo":0.52,"negro":0.44,"senal":"ROJO"},
    {"id":15,"rojo":0.56,"negro":0.44,"senal":"ROJO"},
    {"id":16,"rojo":0.52,"negro":0.44,"senal":"ROJO"},
    {"id":17,"rojo":0.60,"negro":0.36,"senal":"ROJO"},
    {"id":18,"rojo":0.52,"negro":0.48,"senal":"ROJO"},
    {"id":19,"rojo":0.40,"negro":0.56,"senal":"NEGRO"},
    {"id":20,"rojo":0.44,"negro":0.52,"senal":"NEGRO"},
    {"id":21,"rojo":0.40,"negro":0.56,"senal":"NEGRO"},
    {"id":22,"rojo":0.48,"negro":0.52,"senal":"NEGRO"},
    {"id":23,"rojo":0.48,"negro":0.52,"senal":"NEGRO"},
    {"id":24,"rojo":0.44,"negro":0.52,"senal":"NEGRO"},
    {"id":25,"rojo":0.36,"negro":0.60,"senal":"NEGRO"},
    {"id":26,"rojo":0.44,"negro":0.56,"senal":"NEGRO"},
    {"id":27,"rojo":0.40,"negro":0.56,"senal":"NEGRO"},
    {"id":28,"rojo":0.44,"negro":0.52,"senal":"NEGRO"},
    {"id":29,"rojo":0.44,"negro":0.52,"senal":"NEGRO"},
    {"id":30,"rojo":0.44,"negro":0.52,"senal":"NEGRO"},
    {"id":31,"rojo":0.44,"negro":0.52,"senal":"NEGRO"},
    {"id":32,"rojo":0.44,"negro":0.52,"senal":"NEGRO"},
    {"id":33,"rojo":0.48,"negro":0.52,"senal":"NEGRO"},
    {"id":34,"rojo":0.40,"negro":0.60,"senal":"NEGRO"},
    {"id":35,"rojo":0.40,"negro":0.56,"senal":"NEGRO"},
    {"id":36,"rojo":0.44,"negro":0.56,"senal":"NEGRO"},
]

ROULETTE_CONFIGS = {
    "Azure Roulette - Pragmatic Play": {
        "ws_key": 227, "chat_id": -1003835197023, "thread_id": 6,
        "color_data": COLOR_DATA, "betting_system": "dalembert",
        "min_prob_threshold": 0.49, "signal_quality_threshold": 0.54,
    },
}

WS_URL = "wss://dga.pragmaticplaylive.net/ws"
CASINO_ID = "ppcjd00000007254"
MAX_ATTEMPTS = 3
BASE_BET = 0.10
VISIBLE  = 50

# ─── Parámetros V24.0 (Ensemble ML Avanzado) ─────────────────────────────────
RETRY_THRESHOLD  = {1: 0.55, 2: 0.56, 3: 0.58}
MIN_SAMPLES      = {1: 10,   2: 7,    3: 5}
RETRY_MIN_RECENT_ACC = 0.52
ANTI_BOUNCE_N    = 3
MOMENTUM_REQ     = {2: 1, 3: 1}
CROSS_CATEGORY_PENALTY = 0.00
COOLDOWN_SECONDS = 0
AGREE_MARGIN_RETRY = 0.03

# ─── COLORES DEL GRÁFICO POR VALOR ────────────────────────────────────────────
CAT_CHART_COLORS = {
    "ROJO":  ("#e84040","#ff8080"), "NEGRO": ("#9090bb","#b0b8d0"),
    "PAR":   ("#9b59b6","#d7b8f0"), "IMPAR": ("#f1c40f","#f9e79f"),
    "BAJO":  ("#a0522d","#d4a57a"), "ALTO":  ("#3498db","#aed6f1"),
}

# ─── D'ALEMBERT ───────────────────────────────────────────────────────────────
class D_Alembert:
    def __init__(self, base: float):
        self.base = base; self.step = 0; self.bankroll = 0.0; self.max_step = 20

    def current_bet(self) -> float: return round(self.base*(self.step+1), 2)

    def win(self) -> float:
        bet = self.current_bet()
        self.bankroll = round(self.bankroll+bet, 2)
        if self.step > 0: self.step -= 1
        return bet

    def loss(self) -> float:
        bet = self.current_bet()
        self.bankroll = round(self.bankroll-bet, 2)
        if self.step >= self.max_step-1: self.step = 0
        else: self.step += 1
        return bet

# ═══════════════════════════════════════════════════════════════════════════════
# ═══ MODELOS ML AVANZADOS PARA SERIES TEMPORALES ═══════════════════════════════
# ═══════════════════════════════════════════════════════════════════════════════

class TemporalConvolutionalNetwork:
    """
    TCN: Captura patrones con dilataciones exponenciales
    Efectivo para detectar ciclos y patrones locales en la ruleta
    """
    def __init__(self, pattern_length: int = 8, dilations: List[int] = None):
        self.pattern_length = pattern_length
        self.dilations = dilations or [1, 2, 4, 8]
        self.filters: Dict[Tuple, Dict[str, float]] = {}
        self.history: deque = deque(maxlen=200)
        self._build_causal_filters()
        
    def _build_causal_filters(self):
        """Construye filtros causales con diferentes dilataciones"""
        for dilation in self.dilations:
            self.filters[dilation] = {
                'weights': np.random.randn(self.pattern_length) * 0.1,
                'bias': 0.0,
                'activation_cache': deque(maxlen=50)
            }
    
    def _causal_convolution(self, seq: np.ndarray, dilation: int) -> float:
        """Convolución causal con dilatación"""
        if len(seq) < self.pattern_length * dilation:
            return 0.5
        
        # Aplicar dilatación: saltar elementos
        dilated_seq = seq[::dilation][-self.pattern_length:]
        if len(dilated_seq) < self.pattern_length:
            return 0.5
            
        weights = self.filters[dilation]['weights']
        conv = np.dot(dilated_seq, weights) + self.filters[dilation]['bias']
        
        # ReLU + sigmoid para output de probabilidad
        activated = 1 / (1 + np.exp(-np.maximum(conv, 0)))
        return activated
    
    def add_spin(self, color_encoded: float):
        """0.0 = NEGRO, 1.0 = ROJO, 0.5 = VERDE/NEUTRO"""
        self.history.append(color_encoded)
        
        # Actualizar filtros con gradiente simple (online learning)
        if len(self.history) >= self.pattern_length * max(self.dilations) + 1:
            self._update_filters()
    
    def _update_filters(self):
        """Actualización online simple de los pesos"""
        seq = np.array(list(self.history)[-self.pattern_length * max(self.dilations):])
        
        for dilation in self.dilations:
            if len(seq) >= self.pattern_length * dilation + 1:
                dilated = seq[::dilation][-self.pattern_length-1:-1]
                target = seq[::dilation][-1]
                
                if len(dilated) == self.pattern_length:
                    # Predicción actual
                    pred = self._causal_convolution(seq, dilation)
                    error = target - pred
                    
                    # Gradiente descendente simple
                    lr = 0.01
                    self.filters[dilation]['weights'] += lr * error * dilated
                    self.filters[dilation]['bias'] += lr * error
    
    def predict(self, min_confidence: float = 0.55) -> Optional[Dict]:
        """Predicción ensemble de todas las dilataciones"""
        if len(self.history) < self.pattern_length * max(self.dilations):
            return None
            
        seq = np.array(list(self.history))
        
        predictions = []
        confidences = []
        
        for dilation in self.dilations:
            pred = self._causal_convolution(seq, dilation)
            # Calcular confianza basada en consistencia histórica
            cache = self.filters[dilation]['activation_cache']
            if len(cache) > 10:
                variance = np.var(list(cache)[-10:])
                confidence = 1.0 - min(variance * 2, 0.5)
            else:
                confidence = 0.5
            
            predictions.append(pred)
            confidences.append(confidence)
            self.filters[dilation]['activation_cache'].append(pred)
        
        # Ponderar por confianza
        total_conf = sum(confidences)
        if total_conf > 0:
            weights = [c / total_conf for c in confidences]
            ensemble_pred = sum(p * w for p, w in zip(predictions, weights))
        else:
            ensemble_pred = sum(predictions) / len(predictions)
        
        # Determinar clase y confianza
        rojo_prob = ensemble_pred
        negro_prob = 1 - ensemble_pred
        
        # Ajustar por entropía (incertidumbre)
        entropy = -(rojo_prob * np.log(rojo_prob + 1e-10) + negro_prob * np.log(negro_prob + 1e-10))
        max_entropy = np.log(2)
        uncertainty = entropy / max_entropy
        
        final_confidence = max(0.3, 1.0 - uncertainty)
        
        return {
            "ROJO": rojo_prob,
            "NEGRO": negro_prob,
            "confidence": final_confidence,
            "raw_predictions": predictions,
            "model": "TCN"
        }


class LSTMAttentionPredictor:
    """
    LSTM con mecanismo de atención
    Captura dependencias largas enfocándose en los giros más relevantes
    """
    def __init__(self, hidden_size: int = 16, sequence_length: int = 20):
        self.hidden_size = hidden_size
        self.sequence_length = sequence_length
        
        # Pesos LSTM simplificados
        self.W_f = np.random.randn(hidden_size, hidden_size + 1) * 0.1  # Forget gate
        self.W_i = np.random.randn(hidden_size, hidden_size + 1) * 0.1  # Input gate
        self.W_c = np.random.randn(hidden_size, hidden_size + 1) * 0.1  # Candidate
        self.W_o = np.random.randn(hidden_size, hidden_size + 1) * 0.1  # Output gate
        
        # Pesos de atención
        self.W_q = np.random.randn(hidden_size, hidden_size) * 0.1  # Query
        self.W_k = np.random.randn(hidden_size, hidden_size) * 0.1  # Key
        self.W_v = np.random.randn(hidden_size, hidden_size) * 0.1  # Value
        
        self.history: deque = deque(maxlen=200)
        self.cell_states: deque = deque(maxlen=sequence_length)
        self.hidden_states: deque = deque(maxlen=sequence_length)
        
    def _sigmoid(self, x): return 1 / (1 + np.exp(-np.clip(x, -10, 10)))
    def _tanh(self, x): return np.tanh(x)
    
    def _lstm_step(self, x: float, h_prev: np.ndarray, c_prev: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
        """Un paso de LSTM"""
        concat = np.concatenate([[x], h_prev])
        
        f = self._sigmoid(self.W_f @ concat)
        i = self._sigmoid(self.W_i @ concat)
        c_tilde = self._tanh(self.W_c @ concat)
        o = self._sigmoid(self.W_o @ concat)
        
        c = f * c_prev + i * c_tilde
        h = o * self._tanh(c)
        
        return h, c
    
    def _attention(self, query: np.ndarray, keys: List[np.ndarray], values: List[np.ndarray]) -> np.ndarray:
        """Mecanismo de atención scaled dot-product"""
        if not keys:
            return query
            
        # Calcular scores
        scores = []
        for key in keys:
            score = (self.W_q @ query) @ (self.W_k @ key)
            scores.append(score)
        
        # Softmax
        scores = np.array(scores)
        scores = scores - np.max(scores)  # Numeric stability
        exp_scores = np.exp(scores)
        weights = exp_scores / (np.sum(exp_scores) + 1e-10)
        
        # Weighted sum
        context = sum(w * self.W_v @ v for w, v in zip(weights, values))
        return context, weights
    
    def add_spin(self, color_encoded: float):
        self.history.append(color_encoded)
        
        # Forward pass y almacenar estados
        if len(self.history) >= 1:
            h = np.zeros(self.hidden_size)
            c = np.zeros(self.hidden_size)
            
            # Procesar secuencia
            recent = list(self.history)[-self.sequence_length:]
            for x in recent:
                h, c = self._lstm_step(x, h, c)
            
            self.hidden_states.append(h.copy())
            self.cell_states.append(c.copy())
            
            # Actualizar pesos con gradiente simple si hay suficiente historial
            if len(self.history) > self.sequence_length + 1:
                self._online_update()
    
    def _online_update(self):
        """Actualización online simplificada"""
        # Predicción vs real
        pred = self.predict()
        if pred is None:
            return
            
        actual = list(self.history)[-1]
        target = 1.0 if actual > 0.5 else 0.0
        
        error = (pred["ROJO"] - target)
        lr = 0.001
        
        # Ajuste simple de los pesos de salida (simplificado)
        self.W_o += lr * error * np.random.randn(*self.W_o.shape) * 0.01
    
    def predict(self) -> Optional[Dict]:
        """Predicción con atención sobre los estados ocultos"""
        if len(self.hidden_states) < 5:
            return None
        
        # Usar último estado como query
        query = list(self.hidden_states)[-1]
        keys = list(self.hidden_states)[:-1]
        values = list(self.hidden_states)[:-1]
        
        # Aplicar atención
        context, attention_weights = self._attention(query, keys, values)
        
        # Combinar query + contexto
        combined = query + context
        
        # Capa de salida (simplificada)
        output = self._sigmoid(np.mean(combined))
        
        # Calcular confianza basada en atención
        max_attn = np.max(attention_weights) if len(attention_weights) > 0 else 0.5
        entropy_attn = -np.sum(attention_weights * np.log(attention_weights + 1e-10)) if len(attention_weights) > 0 else 1.0
        
        confidence = max_attn * (1.0 - entropy_attn / np.log(len(attention_weights) + 1))
        
        return {
            "ROJO": output,
            "NEGRO": 1 - output,
            "confidence": float(confidence),
            "attention_focus": int(np.argmax(attention_weights)) if len(attention_weights) > 0 else 0,
            "model": "LSTM+Attention"
        }


class TransformerTimeSeries:
    """
    Transformer simplificado para series temporales
    Self-attention puro sin recurrencia
    """
    def __init__(self, d_model: int = 16, n_heads: int = 2, sequence_length: int = 32):
        self.d_model = d_model
        self.n_heads = n_heads
        self.sequence_length = sequence_length
        self.d_k = d_model // n_heads  # 8 si d_model=16 y n_heads=2
        
        # Pesos de atención multi-cabeza: (d_model, d_k) para cada cabeza
        self.W_q = [np.random.randn(d_model, self.d_k) * 0.1 for _ in range(n_heads)]
        self.W_k = [np.random.randn(d_model, self.d_k) * 0.1 for _ in range(n_heads)]
        self.W_v = [np.random.randn(d_model, self.d_k) * 0.1 for _ in range(n_heads)]
        
        # Proyección de salida: (d_model, d_model)
        # La concatenación de n_heads cabezas da (seq_len, n_heads * d_k) = (seq_len, d_model)
        self.W_o = np.random.randn(d_model, d_model) * 0.1
        
        # Embedding posicional simplificado
        self.pos_encoding = self._positional_encoding(sequence_length, d_model)
        
        self.history: deque = deque(maxlen=200)
        self.attention_maps: deque = deque(maxlen=10)
        
    def _positional_encoding(self, max_len: int, d_model: int) -> np.ndarray:
        """Encoding posicional sinusoidal"""
        pos = np.arange(max_len)[:, np.newaxis]
        div_term = np.exp(np.arange(0, d_model, 2) * -(np.log(10000.0) / d_model))
        
        pe = np.zeros((max_len, d_model))
        pe[:, 0::2] = np.sin(pos * div_term)
        pe[:, 1::2] = np.cos(pos * div_term)
        return pe
    
    def _softmax(self, x: np.ndarray) -> np.ndarray:
        exp_x = np.exp(x - np.max(x, axis=-1, keepdims=True))
        return exp_x / (np.sum(exp_x, axis=-1, keepdims=True) + 1e-10)
    
    def _multi_head_attention(self, X: np.ndarray) -> Tuple[np.ndarray, List[np.ndarray]]:
        """Atención multi-cabeza"""
        seq_len = X.shape[0]
        heads_output = []
        attention_weights = []
        
        for h in range(self.n_heads):
            # Proyecciones: X (seq_len, d_model) @ W (d_model, d_k) -> (seq_len, d_k)
            Q = X @ self.W_q[h]
            K = X @ self.W_k[h]
            V = X @ self.W_v[h]
            
            # Scaled dot-product attention
            # Q @ K.T: (seq_len, d_k) @ (d_k, seq_len) -> (seq_len, seq_len)
            scores = (Q @ K.T) / np.sqrt(self.d_k)
            attn = self._softmax(scores)
            attention_weights.append(attn)
            
            # attn @ V: (seq_len, seq_len) @ (seq_len, d_k) -> (seq_len, d_k)
            head_output = attn @ V
            heads_output.append(head_output)
        
        # Concatenar cabezas: n_heads * d_k = d_model
        # Cada cabeza es (seq_len, d_k), concatenadas -> (seq_len, n_heads * d_k) = (seq_len, d_model)
        multi_head = np.concatenate(heads_output, axis=1)
        
        # Proyección final: (seq_len, d_model) @ (d_model, d_model) -> (seq_len, d_model)
        output = multi_head @ self.W_o
        
        return output, attention_weights
    
    def _feed_forward(self, x: np.ndarray) -> np.ndarray:
        """Red feed-forward simplificada"""
        W1 = np.random.randn(self.d_model, self.d_model * 2) * 0.1
        b1 = np.zeros(self.d_model * 2)
        W2 = np.random.randn(self.d_model * 2, self.d_model) * 0.1
        b2 = np.zeros(self.d_model)
        
        hidden = np.maximum(x @ W1 + b1, 0)  # ReLU
        return hidden @ W2 + b2
    
    def add_spin(self, color_encoded: float):
        self.history.append(color_encoded)
        
        if len(self.history) >= self.sequence_length:
            self._forward_and_update()
    
    def _forward_and_update(self):
        """Forward pass y actualización online"""
        seq = np.array(list(self.history)[-self.sequence_length:])
        
        # Embedding: valor + posición
        X = np.zeros((self.sequence_length, self.d_model))
        X[:, 0] = seq
        X += self.pos_encoding[:self.sequence_length]
        
        # Self-attention
        attn_output, attn_weights = self._multi_head_attention(X)
        
        # Add & Norm (simplificado)
        X = X + attn_output
        X = (X - np.mean(X, axis=0)) / (np.std(X, axis=0) + 1e-10)
        
        # Feed-forward
        ff_output = self._feed_forward(X)
        X = X + ff_output
        
        # Guardar attention map
        self.attention_maps.append(attn_weights[0])
        
        # Actualización online
        self._online_update(X)
    
    def _online_update(self, final_output: np.ndarray):
        """Actualización de pesos con gradiente simplificado"""
        pred = self._sigmoid(np.mean(final_output[-1]))
        actual = list(self.history)[-1]
        target = 1.0 if actual > 0.5 else 0.0
        
        error = pred - target
        lr = 0.0001
        
        self.W_o -= lr * error * np.random.randn(*self.W_o.shape) * 0.01
    
    def _sigmoid(self, x): 
        return 1 / (1 + np.exp(-np.clip(x, -10, 10)))
    
    def predict(self) -> Optional[Dict]:
        """Predicción del transformer"""
        if len(self.history) < self.sequence_length:
            return None
        
        seq = np.array(list(self.history)[-self.sequence_length:])
        
        X = np.zeros((self.sequence_length, self.d_model))
        X[:, 0] = seq
        X += self.pos_encoding[:self.sequence_length]
        
        attn_output, attn_weights = self._multi_head_attention(X)
        X = X + attn_output
        
        # Predicción basada en el último token
        last_token = X[-1]
        prob_rojo = self._sigmoid(np.mean(last_token))
        
        # Calibrar con temperatura
        temperature = 0.8
        prob_rojo = 1 / (1 + np.exp(-(prob_rojo - 0.5) / temperature + 0.5))
        
        # Confianza basada en atención
        last_attn = attn_weights[0][-1] if len(attn_weights) > 0 else np.ones(self.sequence_length) / self.sequence_length
        confidence = np.max(last_attn)
        
        return {
            "ROJO": float(prob_rojo),
            "NEGRO": float(1 - prob_rojo),
            "confidence": float(confidence),
            "attention_pattern": "global" if confidence < 0.3 else "focused",
            "model": "Transformer"
        }

class DynamicEnsemble:
    """
    Ensemble dinámico que combina todos los modelos
    Pesos adaptativos basados en rendimiento reciente
    """
    def __init__(self):
        self.models: Dict[str, object] = {
            'tcn': TemporalConvolutionalNetwork(pattern_length=8),
            'lstm_att': LSTMAttentionPredictor(hidden_size=16, sequence_length=20),
            'transformer': TransformerTimeSeries(d_model=16, n_heads=2, sequence_length=32),
            'markov': None,  # Se asigna desde el engine
            'pattern': None  # MLPatternPredictor original
        }
        
        # Pesos adaptativos
        self.weights = {
            'tcn': 0.20,
            'lstm_att': 0.20,
            'transformer': 0.20,
            'markov': 0.20,
            'pattern': 0.20
        }
        
        # Historial de rendimiento (accuracy reciente)
        self.performance_history: Dict[str, deque] = {
            name: deque(maxlen=50) for name in self.weights.keys()
        }
        
        self.ensemble_predictions: deque = deque(maxlen=100)
        self.last_update = 0
        self.update_interval = 20  # Actualizar pesos cada 20 predicciones
        
    def register_markov(self, markov):
        self.models['markov'] = markov
        
    def register_pattern(self, pattern):
        self.models['pattern'] = pattern
    
    def add_spin(self, number: int, real_color: str):
        """Codificar y enviar a todos los modelos"""
        # Codificación: ROJO=1.0, NEGRO=0.0, VERDE=0.5
        encoded = 1.0 if real_color == "ROJO" else (0.0 if real_color == "NEGRO" else 0.5)
        
        self.models['tcn'].add_spin(encoded)
        self.models['lstm_att'].add_spin(encoded)
        self.models['transformer'].add_spin(encoded)
        # Markov y Pattern se actualizan desde el engine principal
    
    def predict(self, spin_history: list, category: str = "COLOR", 
                bet_value: str = "ROJO", attempt: int = 1) -> Optional[Dict]:
        """
        Predicción ensemble con pesos dinámicos
        """
        predictions = {}
        confidences = {}
        
        # Obtener predicciones de cada modelo
        for name, model in self.models.items():
            if model is None:
                continue
                
            try:
                if name in ['tcn', 'lstm_att', 'transformer']:
                    pred = model.predict()
                    if pred:
                        predictions[name] = pred.get(bet_value, 0.5)
                        confidences[name] = pred.get('confidence', 0.5)
                        
                elif name == 'markov':
                    if category == "COLOR":
                        mk = model.predict_color(spin_history)
                    elif category == "PARIDAD":
                        mk = model.predict_paridad(spin_history)
                    else:
                        mk = model.predict_rango(spin_history)
                    if mk:
                        predictions[name] = mk.get(bet_value, 0.5)
                        confidences[name] = min(mk.get('total', 10) / 20, 1.0)
                        
                elif name == 'pattern':
                    pred = model.predict(spin_history, min_total=MIN_SAMPLES.get(attempt, 5))
                    if pred:
                        predictions[name] = pred.get(bet_value, 0.5)
                        confidences[name] = min(pred.get('total', 5) / 10, 1.0)
                        
            except Exception as e:
                logger.warning(f"Error en predicción {name}: {e}")
                continue
        
        if not predictions:
            return None
        
        # Calcular pesos dinámicos basados en rendimiento reciente
        dynamic_weights = self._calculate_dynamic_weights(predictions.keys())
        
        # Ponderar por confianza y peso dinámico
        weighted_sum = 0.0
        total_weight = 0.0
        
        for name, pred in predictions.items():
            conf = confidences.get(name, 0.5)
            weight = dynamic_weights.get(name, 0.2) * conf
            weighted_sum += pred * weight
            total_weight += weight
        
        if total_weight == 0:
            return None
            
        ensemble_prob = weighted_sum / total_weight
        
        # Calcular confianza del ensemble
        # Varianza entre modelos (menor = más confianza)
        pred_values = list(predictions.values())
        variance = np.var(pred_values) if len(pred_values) > 1 else 0.5
        agreement = 1.0 - min(variance * 4, 1.0)  # Normalizar
        
        # Consistencia de predicciones
        majority_vote = sum(1 for p in pred_values if p > 0.5) / len(pred_values)
        consistency = 1.0 - abs(majority_vote - 0.5) * 2  # 1.0 si todos coinciden
        
        ensemble_confidence = (agreement * 0.6 + consistency * 0.4) * min(total_weight / len(predictions), 1.0)
        
        # Meta-features para debugging
        meta = {
            'individual_predictions': predictions,
            'dynamic_weights': dynamic_weights,
            'confidences': confidences,
            'variance': variance,
            'agreement': agreement,
            'consistency': consistency
        }
        
        return {
            bet_value: ensemble_prob,
            "confidence": ensemble_confidence,
            "opposite": 1 - ensemble_prob,
            "models_used": len(predictions),
            "meta": meta,
            "model": "DynamicEnsemble"
        }
    
    def _calculate_dynamic_weights(self, active_models: set) -> Dict[str, float]:
        """Calcular pesos basados en rendimiento reciente"""
        weights = {}
        
        for name in active_models:
            history = self.performance_history[name]
            if len(history) > 10:
                # Accuracy reciente ponderado (más peso a predicciones recientes)
                recent = list(history)[-20:]
                accuracies = [1 if correct else 0 for correct in recent]
                # Ponderación exponencial
                exp_weights = [0.95 ** i for i in range(len(accuracies)-1, -1, -1)]
                weighted_acc = sum(a * w for a, w in zip(accuracies, exp_weights)) / sum(exp_weights)
                weights[name] = max(0.05, weighted_acc)
            else:
                weights[name] = self.weights.get(name, 0.2)
        
        # Normalizar
        total = sum(weights.values())
        if total > 0:
            weights = {k: v / total for k, v in weights.items()}
        
        return weights
    
    def update_performance(self, model_name: str, correct: bool):
        """Actualizar rendimiento de un modelo"""
        if model_name in self.performance_history:
            self.performance_history[model_name].append(correct)
        
        # Actualizar pesos estáticos periódicamente
        self.last_update += 1
        if self.last_update >= self.update_interval:
            self._adapt_weights()
            self.last_update = 0
    
    def _adapt_weights(self):
        """Adaptar pesos basados en historial de rendimiento"""
        for name, history in self.performance_history.items():
            if len(history) >= 20:
                recent_acc = sum(list(history)[-20:]) / 20
                # Suavizar cambio de pesos
                old_weight = self.weights[name]
                target_weight = max(0.05, min(0.5, recent_acc))
                self.weights[name] = old_weight * 0.7 + target_weight * 0.3
        
        # Renormalizar
        total = sum(self.weights.values())
        self.weights = {k: v / total for k, v in self.weights.items()}
        
        logger.info(f"[Ensemble] Pesos adaptados: {self.weights}")


# ─── MARKOV EXTENDIDO ─────────────────────────────────────────────────────────
class MarkovChainPredictor:
    def __init__(self, window: int = 60, order: int = 2):
        self.window = window; self.order = order
        self.color_trans: dict = {}
        self.par_trans:   dict = {}
        self.rang_trans:  dict = {}

    def _build(self, seq: list) -> dict:
        t = defaultdict(lambda: defaultdict(int))
        if len(seq) < self.order+1: return t
        for i in range(len(seq)-self.order):
            t[tuple(seq[i:i+self.order])][seq[i+self.order]] += 1
        return t

    def update(self, spin_history: list):
        r = spin_history[-self.window:]
        self.color_trans = self._build([s["real"] for s in r if s["real"] in ("ROJO","NEGRO")])
        self.par_trans   = self._build([get_paridad(s["number"]) for s in r if get_paridad(s["number"])])
        self.rang_trans  = self._build([get_rango(s["number"])   for s in r if get_rango(s["number"])])

    def _pred(self, seq, trans, min_t=5) -> Optional[dict]:
        if len(seq) < self.order: return None
        c = dict(trans.get(tuple(seq[-self.order:]), {}))
        total = sum(c.values())
        if total < min_t: return None
        r = {k: v/total for k,v in c.items()}; r["total"]=total; return r

    def predict(self, sh): return self.predict_color(sh)
    def predict_color(self, sh):   return self._pred([s["real"] for s in sh if s["real"] in ("ROJO","NEGRO")], self.color_trans)
    def predict_paridad(self, sh): return self._pred([get_paridad(s["number"]) for s in sh if get_paridad(s["number"])], self.par_trans)
    def predict_rango(self, sh):   return self._pred([get_rango(s["number"]) for s in sh if get_rango(s["number"])], self.rang_trans)


# ─── ML PATTERN PREDICTOR COLOR ───────────────────────────────────────────────
class MLPatternPredictor:
    def __init__(self, pattern_length: int = 4):
        self.pattern_length = pattern_length
        self.pattern_counts: dict = defaultdict(lambda: defaultdict(int))
        self._known_len: int = 0

    def add_spin(self, spin_history: list):
        nv = [s["real"] for s in spin_history if s["real"] != "VERDE"]
        cl = len(nv)
        if cl <= self._known_len: return
        self._known_len = cl
        if cl < self.pattern_length+1: return
        i = cl-self.pattern_length-1
        p = tuple(nv[i:i+self.pattern_length]); nc = nv[i+self.pattern_length]
        if nc in ("ROJO","NEGRO"): self.pattern_counts[p][nc] += 1

    def predict(self, spin_history: list, min_total: int = 2) -> Optional[dict]:
        nv = [s["real"] for s in spin_history if s["real"] != "VERDE"]
        if len(nv) < self.pattern_length: return None
        c = dict(self.pattern_counts.get(tuple(nv[-self.pattern_length:]), {}))
        t = sum(c.values())
        if t < min_total: return None
        return {"ROJO":c.get("ROJO",0)/t, "NEGRO":c.get("NEGRO",0)/t, "total":t}


# ─── CATEGORY ML PREDICTOR ────────────────────────────────────────────────────
class CategoryMLPredictor:
    def __init__(self, pattern_length: int = 4):
        self.pattern_length = pattern_length
        self.color_counts = defaultdict(lambda: defaultdict(int))
        self.par_counts   = defaultdict(lambda: defaultdict(int))
        self.rang_counts  = defaultdict(lambda: defaultdict(int))
        self.color_history: list = []
        self.par_history:   list = []
        self.rang_history:  list = []

    def _upd(self, hist, counts, val):
        hist.append(val)
        if len(hist) >= self.pattern_length+1:
            counts[tuple(hist[-(self.pattern_length+1):-1])][val] += 1

    def add_spin(self, number: int, real_color: str):
        if real_color == "VERDE": return
        self._upd(self.color_history, self.color_counts, real_color)
        p = get_paridad(number); r = get_rango(number)
        if p: self._upd(self.par_history,  self.par_counts,  p)
        if r: self._upd(self.rang_history, self.rang_counts, r)

    def _pred(self, hist, counts, min_t=2) -> Optional[dict]:
        if len(hist) < self.pattern_length: return None
        c = dict(counts.get(tuple(hist[-self.pattern_length:]), {}))
        t = sum(c.values())
        if t < min_t: return None
        res = {k: v/t for k,v in c.items()}; res["total"]=t; return res

    def predict_color(self,   min_total=2): return self._pred(self.color_history, self.color_counts, min_total)
    def predict_paridad(self, min_total=2): return self._pred(self.par_history,   self.par_counts,   min_total)
    def predict_rango(self,   min_total=2): return self._pred(self.rang_history,  self.rang_counts,  min_total)


# ─── UNIFIED PROBABILITY SYSTEM ───────────────────────────────────────────────
class UnifiedProbabilitySystem:
    def __init__(self):
        self.weights = {"markov":0.20,"ml":0.20,"tcn":0.20,"lstm":0.20,"transformer":0.20}
        self.prediction_history: deque = deque(maxlen=200)
        self.model_correct = {name: 0 for name in self.weights.keys()}
        self.model_total = {name: 0 for name in self.weights.keys()}
        self.confidence_factor: float = 0.5
        self.volatility:        float = 1.0
        self.current_streak:    int   = 0
        self.streak_direction: Optional[str] = None
        self.spins_since_weight_update: int = 0
        self.WEIGHT_UPDATE_INTERVAL:    int = 50
        self.base_threshold: float = 0.50
        self.dynamic_threshold: float = 0.50
        self.ema_trend_factor: float = 1.0
        self.sr_factor:        float = 1.0

    def calculate_volatility(self, levels: list) -> float:
        if len(levels) < 20: return 1.0
        self.volatility = min(max(float(np.std(levels[-20:]))/5.0, 0.5), 1.5)
        return self.volatility

    def update_streak(self, color: str):
        if self.streak_direction == color: self.current_streak += 1
        else: self.streak_direction=color; self.current_streak=1

    def update_trend_factors(self, levels: list):
        if len(levels) < 20: self.ema_trend_factor=self.sr_factor=1.0; return
        e20 = self._ema(levels, 20)
        if e20:
            d=(levels[-1]-e20)/(abs(e20)+1)*0.2
            self.ema_trend_factor=max(0.8,min(1.2,1.0+d if levels[-1]>e20 else 1.0-abs(d)))
        sr=find_support_resistance(levels,30)
        if sr['support'] and sr['resistance']:
            rng=sr['resistance']-sr['support']
            if rng>0: self.sr_factor=max(0.9,min(1.1,1.0+(levels[-1]-sr['support'])/rng*0.1-0.05))
        else: self.sr_factor=1.0

    def _ema(self, data, period) -> Optional[float]:
        if len(data)<period: return None
        m=2/(period+1); p=sum(data[:period])/period
        for i in range(period,len(data)): p=data[i]*m+p*(1-m)
        return p

    def calculate_confidence(self, predictions: Dict[str, float], color) -> float:
        if not predictions: return 0.3
        
        # Confianza basada en acuerdo entre modelos
        vals = list(predictions.values())
        mean_pred = np.mean(vals)
        variance = np.var(vals)
        
        # Menor varianza = mayor confianza
        agreement = 1.0 - min(variance * 5, 1.0)
        
        # Consistencia direccional
        directional = sum(1 for v in vals if (v > 0.5) == (mean_pred > 0.5)) / len(vals)
        
        self.confidence_factor = 0.3 + agreement * 0.4 + directional * 0.3
        return self.confidence_factor

    def calculate_dynamic_threshold(self) -> float:
        sf=1.0+min(self.current_streak*0.02,0.3)
        cf=1.0-(self.confidence_factor-0.5)*0.4
        self.dynamic_threshold=max(0.45,min(0.65,self.base_threshold*self.volatility*sf*cf))
        return self.dynamic_threshold

    def record_prediction(self, color, predictions: Dict[str, float], actual):
        self.prediction_history.append({
            "color": color,
            "predictions": predictions,
            "actual": actual,
            "timestamp": time.time()
        })
        
        # Actualizar accuracy por modelo
        for model_name, pred in predictions.items():
            if model_name in self.model_total:
                self.model_total[model_name] += 1
                predicted_class = color if pred > 0.5 else ("NEGRO" if color == "ROJO" else "ROJO")
                self.model_correct[model_name] += int(predicted_class == actual)

    def update_weights(self):
        self.spins_since_weight_update+=1
        if self.spins_since_weight_update<self.WEIGHT_UPDATE_INTERVAL: return
        self.spins_since_weight_update=0
        
        # Actualizar pesos basados en accuracy reciente
        total_weight = 0
        for name in self.weights.keys():
            acc = self.model_correct[name] / max(self.model_total[name], 1)
            # Suavizar
            self.weights[name] = self.weights[name] * 0.7 + acc * 0.3
            total_weight += self.weights[name]
        
        # Normalizar
        if total_weight > 0:
            self.weights = {k: v/total_weight for k, v in self.weights.items()}
        
        logger.info(f"[Pesos Adaptativos] {self.weights}")
        
        # Reset contadores
        self.model_correct = {k: 0 for k in self.model_correct}
        self.model_total = {k: 0 for k in self.model_total}

    def get_joint_probability(self, ensemble_pred: Dict, table_prob) -> dict:
        """Nuevo método usando el ensemble avanzado"""
        prob = ensemble_pred.get('ROJO', 0.5) if isinstance(ensemble_pred, dict) else 0.5
        conf = ensemble_pred.get('confidence', 0.5) if isinstance(ensemble_pred, dict) else 0.5
        
        # Combinar con probabilidad de tabla
        table_weight = max(0.1, 1.0 - conf) * 0.25
        combined = (1.0 - table_weight) * prob + table_weight * table_prob
        
        combined = max(0.3, min(0.9, combined * self.ema_trend_factor * self.sr_factor))
        thr = self.calculate_dynamic_threshold()
        
        return {
            "combined_prob": combined,
            "ensemble_prob": prob,
            "table_prob": table_prob,
            "confidence": conf,
            "threshold": thr,
            "signal_strength": "strong" if combined >= thr + 0.1 else "moderate" if combined >= thr else "weak",
            "weights": self.weights.copy(),
            "ema_trend_factor": self.ema_trend_factor,
            "sr_factor": self.sr_factor,
            "volatility": self.volatility,
            "meta": ensemble_pred.get('meta', {}) if isinstance(ensemble_pred, dict) else {}
        }


# ─── DETAILED STATS ───────────────────────────────────────────────────────────
class DetailedStats:
    def __init__(self):
        self.signal_history: deque = deque(maxlen=50)
        self.wins_attempt_1=self.wins_attempt_2=self.wins_attempt_3=0
        self.losses=self.total_signals=0
        self.history_24h: deque = deque()
        self.batch_start_bankroll: Optional[float] = None
        self.batch_start_w1=self.batch_start_w2=self.batch_start_w3=0
        self.batch_start_losses=0; self.last_stats_at=0

    def record_signal_result(self, attempt_won, final_result, bet_amount, bankroll):
        entry={"attempt_won":attempt_won,"won":final_result,"bet":bet_amount,
               "bankroll":bankroll,"timestamp":time.time()}
        self.signal_history.append(entry); self.total_signals+=1
        if final_result:
            if   attempt_won==1: self.wins_attempt_1+=1
            elif attempt_won==2: self.wins_attempt_2+=1
            elif attempt_won==3: self.wins_attempt_3+=1
        else: self.losses+=1
        self.history_24h.append(entry); self._trim_24h()

    def _trim_24h(self):
        cutoff=time.time()-86400
        while self.history_24h and self.history_24h[0]["timestamp"]<cutoff: self.history_24h.popleft()

    def should_send_stats(self): return (self.total_signals-self.last_stats_at)>=20

    def mark_stats_sent(self, bankroll):
        self.last_stats_at=self.total_signals; self.batch_start_bankroll=bankroll
        self.batch_start_w1=self.wins_attempt_1; self.batch_start_w2=self.wins_attempt_2
        self.batch_start_w3=self.wins_attempt_3; self.batch_start_losses=self.losses

    def _make(self, items, delta):
        t=len(items);
        if t==0: return {}
        w=sum(1 for e in items if e["won"]); l=t-w
        w1=sum(1 for e in items if e["attempt_won"]==1)
        w2=sum(1 for e in items if e["attempt_won"]==2)
        w3=sum(1 for e in items if e["attempt_won"]==3)
        return {"total":t,"wins":w,"losses":l,"w1":w1,"w2":w2,"w3":w3,
                "efficiency":round(w/t*100,1),"e_w1":round(w1/t*100,2),
                "e_w2":round(w2/t*100,2),"e_w3":round(w3/t*100,2),
                "e_loss":round(l/t*100,2),"bankroll_delta":delta}

    def get_batch_stats(self, bk):
        n=self.total_signals-self.last_stats_at
        if n==0: return {}
        delta=round(bk-self.batch_start_bankroll,2) if self.batch_start_bankroll is not None else 0.0
        return self._make(list(self.signal_history)[-n:], delta)

    def get_24h_stats(self, bk):
        self._trim_24h(); items=list(self.history_24h)
        if not items: return {}
        delta=round(items[-1]["bankroll"]-items[0]["bankroll"],2) if len(items)>=2 else 0.0
        return self._make(items, delta)

    def reset(self):
        self.signal_history.clear(); self.history_24h.clear()
        self.wins_attempt_1=self.wins_attempt_2=self.wins_attempt_3=0
        self.losses=self.total_signals=self.last_stats_at=0; self.batch_start_bankroll=None


# ─── AMX SIGNAL SYSTEM ───────────────────────────────────────────────────────
class AMXSignalSystem:
    def __init__(self, mode: Literal["tendencia","moderado"]="moderado"):
        self.mode=mode; self.last_signal_time=0.0; self.cooldown_seconds=COOLDOWN_SECONDS
        self.so_cooldown=None; self.ultimos_puntos: list=[]
        self.last_two_expected=deque(maxlen=2); self.last_two_colors=deque(maxlen=2)

    def update_streak(self, real_color, expected_color):
        if expected_color: self.last_two_expected.append(real_color==expected_color)
        self.last_two_colors.append(real_color)

    def calculate_ema(self, data, period):
        if len(data)<period: return [None]*len(data)
        m=2/(period+1); ema=[None]*(period-1); prev=sum(data[:period])/period; ema.append(prev)
        for i in range(period,len(data)): prev=data[i]*m+prev*(1-m); ema.append(prev)
        return ema

    def check_ema_conditions(self, positions) -> bool:
        if len(positions)<20: return False
        e4=self.calculate_ema(positions,4); e8=self.calculate_ema(positions,8); e20=self.calculate_ema(positions,20)
        if any(v is None for v in [e4[-1],e8[-1],e20[-1],e8[-2],e20[-2]]): return False
        cur=positions[-1]
        cruce=e8[-2]<=e20[-2] and e8[-1]>e20[-1]
        sobre=cur>e4[-1] and cur>e8[-1]; alc=e4[-1]>e8[-1]>e20[-1]
        pv=False
        if len(positions)>=3:
            a,b,c=positions[-3],positions[-2],positions[-1]
            pv=b<a and b<c and abs(a-c)<=1 and c>a
        dos_ok=len(self.last_two_expected)>=2 and all(self.last_two_expected)
        return cruce or pv or (dos_ok and alc and sobre)

    def register_signal_sent(self): self.last_signal_time=time.time()


# ─── SOPORTE / RESISTENCIA ────────────────────────────────────────────────────
def find_support_resistance(levels: list, lookback: int=30) -> dict:
    if len(levels)<lookback: return {'support':None,'resistance':None}
    recent=levels[-lookback:]; supp=[]; res=[]
    for i in range(2,len(recent)-2):
        if all(recent[i]<recent[j] for j in [i-1,i-2,i+1,i+2]): supp.append(recent[i])
        if all(recent[i]>recent[j] for j in [i-1,i-2,i+1,i+2]): res.append(recent[i])
    return {'support':supp[-1] if supp else None,'resistance':res[-1] if res else None}


# ─── CHART GENERATION POR CATEGORÍA ──────────────────────────────────────────
def generate_chart(levels: list, spin_history: list,
                   bet_value: str, category: str,
                   visible: int=VISIBLE,
                   markov_pred=None, ml_pred=None,
                   unified_prob=None) -> io.BytesIO:
    arr=np.array(levels,dtype=float); n=len(arr)
    def ce(data,p):
        if len(data)<p: return np.full(len(data),np.nan)
        m=2/(p+1); o=np.full(len(data),np.nan); o[p-1]=np.mean(data[:p])
        for i in range(p,len(data)): o[i]=(data[i]-o[i-1])*m+o[i-1]
        return o
    ema4=ce(arr,4); ema8=ce(arr,8); ema20=ce(arr,20)
    start=max(0,n-visible); sl=slice(start,n); x=np.arange(len(arr[sl]))
    hist_sl=spin_history[start:]; vl=arr[sl]; ll=vl[-1] if len(vl)>0 else 0
    rb=min(50,len(arr)); r50=arr[-rb:]; mn,mx=float(np.min(r50)),float(np.max(r50))
    dr=mx-mn; mg=max(dr*0.15,1.0); off=ll-mn
    ym=mn-mg-off*0.3; ymx=mx+mg+off*0.3; vh=ymx-ym
    lp=(ll-ym)/vh if vh>0 else 0.5
    if lp<0.2: ym=ll-vh*0.2
    elif lp>0.8: ymx=ll+vh*0.2
    lc,tc=CAT_CHART_COLORS.get(bet_value,("#9090bb","#b0b8d0"))
    bg="#0b101f"; ab="#0f1a2a"; gc="#1e2e48"
    ec4="#ff9f43"; ec8="#48dbfb"; ec20="#1dd1a1"
    fig,ax=plt.subplots(figsize=(8,3.8),facecolor=bg); ax.set_facecolor(ab)
    y,e4,e8,e20=arr[sl],ema4[sl],ema8[sl],ema20[sl]
    ax.fill_between(x,y,alpha=0.10,color=lc); ax.plot(x,y,color=lc,linewidth=0.8,zorder=3)
    ax.plot(x,e4,color=ec4,linewidth=0.7,linestyle="--",label="EMA 4",zorder=4)
    ax.plot(x,e8,color=ec8,linewidth=0.7,linestyle="--",label="EMA 8",zorder=4)
    ax.plot(x,e20,color=ec20,linewidth=1.0,label="EMA 20",zorder=4)
    ax.set_ylim(ym,ymx)

    # Puntos coloreados por categoría
    def dot_color(spin):
        if category=="COLOR": return {"ROJO":"#e84040","NEGRO":"#aaaacc","VERDE":"#2ecc71"}.get(spin["real"],"#fff")
        elif category=="PARIDAD":
            p=get_paridad(spin["number"])
            return "#2ecc71" if p is None else ("#9b59b6" if p=="PAR" else "#f1c40f")
        else:
            r=get_rango(spin["number"])
            return "#2ecc71" if r is None else ("#a0522d" if r=="BAJO" else "#3498db")

    for i,spin in enumerate(hist_sl):
        ax.scatter(i,y[i],color=dot_color(spin),s=22,zorder=5,edgecolors="white",linewidths=0.3)

    sr=find_support_resistance(levels,30); sv,rv=sr['support'],sr['resistance']
    sc="#aaaacc"; rc="#e84040"
    if sv: ax.axhline(y=sv,color=sc,linestyle='--',linewidth=1.5,alpha=0.7); ax.text(x[-1],sv,f' S {sv:.1f}',color=sc,fontsize=7,va='bottom',ha='right')
    if rv: ax.axhline(y=rv,color=rc,linestyle='--',linewidth=1.5,alpha=0.7); ax.text(x[-1],rv,f' R {rv:.1f}',color=rc,fontsize=7,va='top',ha='right')

    ts=max(1,len(x)//8); tx=list(range(0,len(x),ts))
    tl=[str(hist_sl[i]["number"]) if i<len(hist_sl) else "" for i in tx]
    ax.set_xticks(tx); ax.set_xticklabels(tl,color="#8899bb",fontsize=7)
    ax.tick_params(axis='y',colors="#8899bb",labelsize=7); ax.tick_params(axis='x',colors="#8899bb",labelsize=7)
    ax.spines['bottom'].set_color(gc); ax.spines['left'].set_color(gc)
    ax.spines['top'].set_visible(False); ax.spines['right'].set_visible(False)
    ax.grid(axis='y',color=gc,linewidth=0.4,alpha=0.5)

    pi=""
    if unified_prob: pi+=f" | Unif:{unified_prob['combined_prob']*100:.0f}%"
    cat_label={"COLOR":"Color","PARIDAD":"Paridad","RANGO":"Rango"}.get(category,category)
    ax.set_title(f"{CATEGORY_ICONS.get(bet_value,'')} {bet_value} ({cat_label}) — últimos {visible} giros · EMA 4/8/20{pi}",color=tc,fontsize=8.5,pad=6)

    from matplotlib.lines import Line2D
    les=[Line2D([0],[0],color=lc,linewidth=0.8,label="Nivel"),
         Line2D([0],[0],color=ec4,linewidth=0.7,linestyle="--",label="EMA 4"),
         Line2D([0],[0],color=ec8,linewidth=0.7,linestyle="--",label="EMA 8"),
         Line2D([0],[0],color=ec20,linewidth=1.0,label="EMA 20")]
    if category=="COLOR":
        les+=[Line2D([0],[0],marker='o',color='w',markerfacecolor='#e84040',markersize=5,label="Rojo"),
              Line2D([0],[0],marker='o',color='w',markerfacecolor='#aaaacc',markersize=5,label="Negro"),
              Line2D([0],[0],marker='o',color='w',markerfacecolor='#2ecc71',markersize=5,label="Verde")]
    elif category=="PARIDAD":
        les+=[Line2D([0],[0],marker='o',color='w',markerfacecolor='#9b59b6',markersize=5,label="PAR"),
              Line2D([0],[0],marker='o',color='w',markerfacecolor='#f1c40f',markersize=5,label="IMPAR"),
              Line2D([0],[0],marker='o',color='w',markerfacecolor='#2ecc71',markersize=5,label="CERO")]
    else:
        les+=[Line2D([0],[0],marker='o',color='w',markerfacecolor='#a0522d',markersize=5,label="BAJO"),
              Line2D([0],[0],marker='o',color='w',markerfacecolor='#3498db',markersize=5,label="ALTO"),
              Line2D([0],[0],marker='o',color='w',markerfacecolor='#2ecc71',markersize=5,label="CERO")]
    if sv: les.append(Line2D([0],[0],color=sc,linestyle='--',linewidth=1.5,label='Soporte'))
    if rv: les.append(Line2D([0],[0],color=rc,linestyle='--',linewidth=1.5,label='Resistencia'))
    ax.legend(handles=les,loc="upper left",fontsize=6.5,facecolor="#0b101f",edgecolor=gc,labelcolor="white",framealpha=0.8,ncol=2)
    plt.tight_layout(pad=0.8); buf=io.BytesIO()
    fig.savefig(buf,format="png",dpi=120,facecolor=bg); plt.close(fig); buf.seek(0); return buf


# ─── TELEGRAM HELPERS ─────────────────────────────────────────────────────────
_TG_MAX_RETRIES = 5

def _tg_call(fn, *args, **kwargs):
    delay=2.0
    for attempt in range(1,_TG_MAX_RETRIES+1):
        try: return fn(*args, **kwargs)
        except Exception as e:
            err=str(e)
            if "retry after" in err.lower():
                try: wait=int(''.join(filter(str.isdigit,err)))+1
                except: wait=30
                time.sleep(wait); continue
            logger.warning(f"TG error ({attempt}): {e}")
            if attempt<_TG_MAX_RETRIES: time.sleep(delay); delay=min(delay*2,60)
            else: logger.error(f"TG failed: {e}"); return None

def tg_send_photo(chat_id, thread_id, buf, caption) -> Optional[int]:
    buf.seek(0)
    msg=_tg_call(bot.send_photo,chat_id=chat_id,photo=buf,caption=caption,
                 parse_mode="HTML",message_thread_id=thread_id)
    return msg.message_id if msg else None

def tg_send_text(chat_id, thread_id, text) -> Optional[int]:
    msg=_tg_call(bot.send_message,chat_id=chat_id,text=text,
                 parse_mode="HTML",message_thread_id=thread_id)
    return msg.message_id if msg else None

def tg_delete(chat_id, msg_id):
    _tg_call(bot.delete_message,chat_id=chat_id,message_id=msg_id)


# ─── ROULETTE ENGINE ──────────────────────────────────────────────────────────
class RouletteEngine:
    def __init__(self, name: str, cfg: dict):
        self.name=name; self.ws_key=cfg["ws_key"]
        self.chat_id=cfg["chat_id"]; self.thread_id=cfg["thread_id"]
        self.color_data=cfg["color_data"]
        self.spin_history:      list=[]
        # Niveles acumulados por categoría
        self.original_levels:  list=[]   # COLOR: ROJO +1/-1
        self.inverted_levels:  list=[]   # COLOR: NEGRO +1/-1
        self.par_levels:       list=[]   # PARIDAD: PAR +1/-1
        self.impar_levels:     list=[]   # PARIDAD: IMPAR +1/-1
        self.bajo_levels:      list=[]   # RANGO: BAJO +1/-1
        self.alto_levels:      list=[]   # RANGO: ALTO +1/-1
        self.last_nonzero_color: Optional[str]=None
        self.anti_block: set=set()
        # Estado señal
        self.signal_active=False; self.waiting_for_attempt=False
        self.waiting_attempt_number=0; self.skip_one_after_zero=False
        self.active_category: Optional[str]=None
        self.bet_value:       Optional[str]=None
        self.attempts_left=0; self.total_attempts=0
        self.trigger_number: Optional[int]=None
        self.current_attempt_number: int=1
        self.signal_msg_ids: list=[]; self.waiting_msg_id: Optional[int]=None
        self.result_sequence: deque=deque(maxlen=10)
        # D'Alembert
        self.bet_sys=D_Alembert(BASE_BET)
        # Recuperación
        self.consec_losses=0; self.recovery_active=False
        self.recovery_target=0.0; self.level1_bankroll=0.0; self.signal_is_level1=False
        # Sistemas
        self.amx_system=AMXSignalSystem(mode="moderado")
        self.min_prob_threshold=cfg.get("min_prob_threshold",0.49)
        self.signal_quality_threshold=cfg.get("signal_quality_threshold",0.54)
        self.unified_prob_system=UnifiedProbabilitySystem()
        self.markov=MarkovChainPredictor(window=60,order=2)
        self.ml_predictor=MLPatternPredictor(pattern_length=4)
        self.category_ml=CategoryMLPredictor(pattern_length=4)
        self.stats=DetailedStats()
        # NUEVO: Ensemble ML Avanzado
        self.ensemble_ml = DynamicEnsemble()
        self.ensemble_ml.register_markov(self.markov)
        self.ensemble_ml.register_pattern(self.ml_predictor)
        # Historial de predicciones por categoría (últimas 30)
        self.cat_pred_history: deque=deque(maxlen=30)
        self.ws=None; self.running=True
        # Fase de aprendizaje
        self.learning_phase=True; self.learning_spin_count=0
        self.learning_initial_numbers: list=[]; self.max_learning_spins=20
        self._pretrain_from_db(DB_PATH, DB_TABLE)

    # ─── PRE-ENTRENAMIENTO ────────────────────────────────────────────────────
    def _pretrain_from_db(self, db_path, table_name):
        if not os.path.exists(db_path): logger.warning(f"[{self.name}] DB no encontrada"); return
        spins=[]
        try:
            pat=re.compile(rf'INSERT INTO "{table_name}" VALUES \(\d+,(\d+),')
            with open(db_path,"r",encoding="utf-8",errors="ignore") as f:
                for line in f:
                    m=pat.search(line)
                    if m: spins.append(int(m.group(1)))
        except Exception as e: logger.error(f"[{self.name}] DB error: {e}"); return
        if not spins: return
        tmp=[]
        for n in spins:
            real=REAL_COLOR_MAP.get(n,"VERDE"); tmp.append({"number":n,"real":real})
            self.markov.update(tmp); self.ml_predictor.add_spin(tmp)
            self.category_ml.add_spin(n,real)
            # Pre-entrenar ensemble
            self.ensemble_ml.add_spin(n, real)
        logger.info(f"[{self.name}] Pre-entrenado {len(spins)} giros ({table_name})")

    # ─── HELPERS ──────────────────────────────────────────────────────────────
    def set_mode(self, mode): self.amx_system=AMXSignalSystem(mode=mode); logger.info(f"[{self.name}] Modo→{mode}")

    @staticmethod
    def calculate_ema(data, period):
        if len(data)<period: return [None]*len(data)
        m=2/(period+1); o=[None]*(period-1); p=sum(data[:period])/period; o.append(p)
        for i in range(period,len(data)): p=data[i]*m+p*(1-m); o.append(p)
        return o

    def get_entry(self, number): return next((e for e in self.color_data if e["id"]==number),None)
    def get_signal(self, number): e=self.get_entry(number); return e["senal"] if e else None
    def get_prob(self, number, color):
        e=self.get_entry(number)
        if not e: return 0.0
        return e["rojo"] if color=="ROJO" else e["negro"]
    def _opposite_color(self, c): return "NEGRO" if c=="ROJO" else "ROJO"

    # ─── CERO = ACIERTO ───────────────────────────────────────────────────────
    def _is_win(self, number, real_color) -> bool:
        if number==0: return True
        if   self.active_category=="COLOR":   return real_color==self.bet_value
        elif self.active_category=="PARIDAD": return get_paridad(number)==self.bet_value
        else:                                  return get_rango(number)==self.bet_value

    # ─── RESULTADO REAL POR CATEGORÍA ─────────────────────────────────────────
    def _result_display(self, number, real_color) -> str:
        """Retorna texto+emoji según la categoría activa y el número salido."""
        if number==0: return "0 CERO 🟢"
        if self.active_category=="COLOR":
            icon=CATEGORY_ICONS.get(real_color,"❓")
            return f"{number} {real_color} {icon}"
        elif self.active_category=="PARIDAD":
            val=get_paridad(number) or "?"; icon=CATEGORY_ICONS.get(val,"❓")
            return f"{number} {val} {icon}"
        else:
            val=get_rango(number) or "?"; icon=CATEGORY_ICONS.get(val,"❓")
            return f"{number} {val} {icon}"

    # ─── NIVELES POR CATEGORÍA ────────────────────────────────────────────────
    def _get_levels(self, category, bet_value) -> list:
        if   category=="COLOR":   return self.original_levels if bet_value=="ROJO" else self.inverted_levels
        elif category=="PARIDAD": return self.par_levels       if bet_value=="PAR"  else self.impar_levels
        else:                      return self.bajo_levels     if bet_value=="BAJO" else self.alto_levels

    # ─── HISTORIAL RECIENTE ACIERTOS ──────────────────────────────────────────
    def _recent_acc(self, category, bet_value, last_n=10) -> float:
        rel=[e for e in self.cat_pred_history if e["category"]==category and e["bet_value"]==bet_value][-last_n:]
        if len(rel)<3: return 0.5
        return sum(1 for e in rel if e["won"])/len(rel)

    def _record_cat_pred(self, category, bet_value, won):
        self.cat_pred_history.append({"category":category,"bet_value":bet_value,"won":won})

    # ─── FILTROS EMA / SR ─────────────────────────────────────────────────────
    def _passes_ema_filter(self, category, bet_value, attempt=1) -> bool:
        """Versión con intento-aware - más permisivo para reintentos"""
        lvl = self._get_levels(category, bet_value)
        if len(lvl) < 15:  # Reducido de 20
            return True
        
        # Para reintentos, ser más permisivo
        if attempt > 1:
            e20 = self.calculate_ema(lvl, 20)
            li = len(lvl) - 1
            if e20[li] is not None:
                diff = (lvl[li] - e20[li]) / (abs(e20[li]) + 1)
                return diff > -0.1  # Permite estar ligeramente debajo
        
        e20 = self.calculate_ema(lvl, 20)
        li = len(lvl) - 1
        return e20[li] is not None and lvl[li] > e20[li]

    def _passes_sr_filter(self, category, bet_value) -> bool:
        lvl=self._get_levels(category,bet_value)
        if len(lvl)<30: return True
        sr=find_support_resistance(lvl,30)
        if not sr['resistance'] or not sr['support']: return True
        rng=sr['resistance']-sr['support']
        return rng<=0 or (sr['resistance']-lvl[-1])/rng>=0.05

    # ─── ANTI-BOUNCE FILTER ────────────────────────────────────────────────────
    def _passes_anti_bounce(self, category, bet_value, n=ANTI_BOUNCE_N) -> bool:
        recs=[s for s in self.spin_history if s["number"]!=0][-n:]
        if len(recs)<n: return True
        def cv(s):
            if category=="COLOR":   return s["real"]
            elif category=="PARIDAD": return get_paridad(s["number"])
            else:                      return get_rango(s["number"])
        vals=[cv(s) for s in recs if cv(s)]
        if len(vals)<n: return True
        return not all(v!=bet_value for v in vals)

    # ─── MOMENTUM CONFIRMATION ────────────────────────────────────────────────
    def _has_momentum(self, category, bet_value, require=1, attempt=1) -> bool:
        """Versión extendida con más histórico para reintentos"""
        lookback = 6 if attempt > 1 else 4
        recs = [s for s in self.spin_history if s["number"] != 0][-lookback:]
        
        cnt = 0
        for s in recs:
            if (category=="COLOR"   and s["real"]==bet_value) or \
               (category=="PARIDAD" and get_paridad(s["number"])==bet_value) or \
               (category=="RANGO"   and get_rango(s["number"])==bet_value): cnt += 1
        
        # Para I3, aceptar también si hay "anti-momentum" (cambio de tendencia)
        if attempt == 3 and cnt == 0:
            opposite = self._get_opposite(category, bet_value)
            opp_cnt = sum(1 for s in recs if self._value_matches(s, category, opposite))
            if opp_cnt >= 4:  # El opuesto ha salido mucho, puede haber cambio
                return True
        
        return cnt >= require

    def _get_opposite(self, category, value):
        """Obtener el valor opuesto en una categoría"""
        opposites = {
            "COLOR": {"ROJO": "NEGRO", "NEGRO": "ROJO"},
            "PARIDAD": {"PAR": "IMPAR", "IMPAR": "PAR"},
            "RANGO": {"BAJO": "ALTO", "ALTO": "BAJO"}
        }
        return opposites.get(category, {}).get(value, value)
    
    def _value_matches(self, spin, category, value):
        """Helper para verificar si un spin coincide con categoría/valor"""
        if category == "COLOR":
            return spin["real"] == value
        elif category == "PARIDAD":
            return get_paridad(spin["number"]) == value
        else:
            return get_rango(spin["number"]) == value

    # ─── SCORE MULTI-FACTOR ───────────────────────────────────────────────────
    def _score_category(self, category, bet_value, attempt=1) -> float:
        ms=MIN_SAMPLES.get(attempt,5)
        
        # Usar el ensemble ML avanzado
        ensemble_pred = self.ensemble_ml.predict(
            self.spin_history, 
            category=category,
            bet_value=bet_value,
            attempt=attempt
        )
        
        if ensemble_pred is None:
            return 0.0
        
        # Extraer probabilidad del valor objetivo
        ml_prob = ensemble_pred.get(bet_value, 0.5)
        conf = ensemble_pred.get('confidence', 0.5)
        
        # Obtener predicción Markov para comparación
        if category=="COLOR":
            mkp=self.markov.predict_color(self.spin_history)
        elif category=="PARIDAD":
            mkp=self.markov.predict_paridad(self.spin_history)
        else:
            mkp=self.markov.predict_rango(self.spin_history)
        
        mk_prob = mkp.get(bet_value, 0.5) if mkp else 0.5
        
        if ml_prob < self.min_prob_threshold: 
            return 0.0
        if attempt==1 and mk_prob < self.min_prob_threshold: 
            return 0.0
            
        # Margen de acuerdo más permisivo para reintentos
        margin = AGREE_MARGIN_RETRY if attempt > 1 else 0.04
        if attempt > 1 and abs(ml_prob - mk_prob) < margin:
            return 0.0

        # Peso dinámico basado en confianza del ensemble
        w_ensemble = 0.7 + conf * 0.2  # 0.7 - 0.9
        w_markov = 1.0 - w_ensemble
        
        base = w_ensemble * ml_prob + w_markov * mk_prob
        
        # Factores de ajuste
        ag=1.0-abs(ml_prob-mk_prob)
        af=0.88+ag*0.24
        ef=1.08 if self._passes_ema_filter(category,bet_value,attempt) else 0.88
        sf=1.0  if self._passes_sr_filter(category,bet_value)  else 0.88
        stf=self._streak_trend_factor(category,bet_value)
        
        # Factor de muestras basado en meta del ensemble
        meta = ensemble_pred.get('meta', {})
        models_used = ensemble_pred.get('models_used', 3)
        smf=min(1.1,0.9+models_used/10.0)
        
        final_score = min(0.95,max(0.0,base*af*ef*sf*stf*smf))
        
        # Actualizar rendimiento del ensemble
        return final_score

    def _streak_trend_factor(self, category, bet_value, lookback=4) -> float:
        recs=[s for s in self.spin_history[-lookback*2:] if s["number"]!=0][-lookback:]
        if len(recs)<2: return 1.0
        m=sum(1 for s in recs if (
            (category=="COLOR"   and s["real"]==bet_value) or
            (category=="PARIDAD" and get_paridad(s["number"])==bet_value) or
            (category=="RANGO"   and get_rango(s["number"])==bet_value)))
        r=m/len(recs)
        if r>=0.75: return 1.08
        if r>=0.50: return 1.0
        if r>=0.25: return 0.94
        return 0.88

    # ─── DETECCIÓN SEÑAL (intento 1) ──────────────────────────────────────────
    def _detect_best_category_signal(self) -> Optional[dict]:
        # Cooldown entre señales para reducir volatilidad
        if time.time() - self.amx_system.last_signal_time < self.amx_system.cooldown_seconds:
            return None
        if not self.spin_history: return None
        ln=self.spin_history[-1]["number"]
        if ln==0: return None  # No activar en cero
        cands=[]
        # COLOR
        cc=self._eval_color()
        if cc: cands.append(cc)
        # PARIDAD
        for val in ("PAR","IMPAR"):
            s=self._score_category("PARIDAD",val,attempt=1)
            if s>=self.signal_quality_threshold: cands.append({"category":"PARIDAD","bet_value":val,"probability":s,"trigger_number":ln})
        # RANGO
        for val in ("BAJO","ALTO"):
            s=self._score_category("RANGO",val,attempt=1)
            if s>=self.signal_quality_threshold: cands.append({"category":"RANGO","bet_value":val,"probability":s,"trigger_number":ln})
        if not cands: return None
        return max(cands,key=lambda x:x["probability"])

    def _eval_color(self) -> Optional[dict]:
        # Usar ensemble para predicción de color
        ensemble_pred = self.ensemble_ml.predict(
            self.spin_history,
            category="COLOR",
            bet_value="ROJO",
            attempt=1
        )
        
        if ensemble_pred is None:
            return None
        
        # Evaluar ambos colores
        scores = {}
        for color in ["ROJO", "NEGRO"]:
            prob = ensemble_pred.get(color, 0.5)
            mk = self.markov.predict_color(self.spin_history)
            mk_prob = mk.get(color, 0.5) if mk else 0.5
            
            # Combinar
            w_ml = 0.65; w_mk = 0.35
            sc[color] = w_ml * prob + w_mk * mk_prob
        
        bv = max(scores, key=scores.get)
        
        if scores[bv] < self.min_prob_threshold: 
            return None
            
        s = self._score_category("COLOR", bv, attempt=1)
        if s < self.signal_quality_threshold: 
            return None
            
        # Filtro EMA opcional - solo si tenemos pocos datos
        if not self._passes_ema_filter("COLOR", bv):
            if len(self.amx_system.ultimos_puntos) < 20: 
                return None
            # En modo tendencia, ser más permisivo con EMA
            if self.amx_system.mode != "tendencia":
                if not self.amx_system.check_ema_conditions(self.amx_system.ultimos_puntos): 
                    return None
                    
        ln = self.spin_history[-1]["number"]
        return {"category":"COLOR","bet_value":bv,"probability":s,"trigger_number":ln}

    # ─── REINTENTO MEJORADO (evalúa todas las categorías con penalización) ───
    def _best_retry_value(self, trigger_number, attempt) -> Optional[dict]:
        thr = RETRY_THRESHOLD.get(attempt, 0.57)
        best = None
        best_score = 0.0
        original_cat = self.active_category
        original_val = self.bet_value

        categories = ["COLOR", "PARIDAD", "RANGO"]
        
        # Para intento 3, ser aún más permisivo
        anti_bounce_n = ANTI_BOUNCE_N if attempt == 2 else 2  # Más permisivo en I3
        
        for cat in categories:
            if cat == "COLOR":
                candidates = ["ROJO", "NEGRO"]
            elif cat == "PARIDAD":
                candidates = ["PAR", "IMPAR"]
            else:
                candidates = ["BAJO", "ALTO"]

            for val in candidates:
                penalty = 0.0
                if cat != original_cat or val != original_val:
                    penalty = CROSS_CATEGORY_PENALTY  # 0.00 ahora

                # Filtros más permisivos
                if not self._passes_anti_bounce(cat, val, n=anti_bounce_n):
                    continue
                
                # Momentum más flexible para intento 3
                req = MOMENTUM_REQ.get(attempt, 1)
                if attempt == 3:
                    if not self._has_momentum(cat, val, require=req, attempt=3):
                        # Fallback: aceptar si el valor apareció al menos 1 vez en últimos 6 giros
                        recent = [s for s in self.spin_history if s["number"] != 0][-6:]
                        matches = sum(1 for s in recent if self._value_matches(s, cat, val))
                        if matches < 1:
                            continue
                else:
                    if not self._has_momentum(cat, val, require=req):
                        continue
                
                # Acc reciente más permisivo
                acc = self._recent_acc(cat, val, last_n=8 if attempt == 3 else 10)  # Menos histórico en I3
                if acc < RETRY_MIN_RECENT_ACC:
                    continue

                score = self._score_category(cat, val, attempt=attempt) - penalty
                # Umbral dinámico: para I3, aceptar el mejor disponible si supera umbral mínimo
                min_acceptable = thr - (0.02 if attempt == 3 else 0.0)  # 0.56 para I3
                
                if score >= min_acceptable and score > best_score:
                    best = {"category": cat, "bet_value": val, "probability": score}
                    best_score = score

        if best:
            logger.info(f"[{self.name}] Retry {attempt}: {best['category']} {best['bet_value']} score={best_score:.3f}")
        elif attempt == 3:
            # Último recurso: mantener la categoría original si nada más funciona
            # y hay al menos alguna señal débil
            weak_score = self._score_category(original_cat, original_val, attempt=3)
            if weak_score >= 0.50:  # Umbral mínimo de emergencia
                best = {"category": original_cat, "bet_value": original_val, "probability": weak_score}
                logger.info(f"[{self.name}] Retry 3 (fallback): manteniendo {original_cat} {original_val} score={weak_score:.3f}")
        
        return best

    # ─── PROBABILIDAD ─────────────────────────────────────────────────────────
    def _upd_unified(self, bet_value, category):
        lvl=self._get_levels(category,bet_value)
        self.unified_prob_system.calculate_volatility(lvl)
        self.unified_prob_system.update_trend_factors(lvl)
        self.unified_prob_system.update_weights()

    def _get_cat_prob(self, category, bet_value, trigger_number) -> dict:
        # Usar ensemble ML avanzado
        ensemble_pred = self.ensemble_ml.predict(
            self.spin_history,
            category=category,
            bet_value=bet_value,
            attempt=self.current_attempt_number
        )
        
        table_prob = self.get_prob(trigger_number, bet_value) if category == "COLOR" else 0.5
        
        return self.unified_prob_system.get_joint_probability(ensemble_pred, table_prob)

    def _record_pred_result(self, color, actual):
        # Actualizar rendimiento del ensemble
        ensemble_pred = self.ensemble_ml.predict(self.spin_history, category="COLOR", bet_value=color)
        if ensemble_pred:
            pred_value = ensemble_pred.get(color, 0.5)
            predicted_correct = (pred_value > 0.5) == (actual == color)
            
            # Actualizar cada modelo individual
            meta = ensemble_pred.get('meta', {})
            individual = meta.get('individual_predictions', {})
            
            for model_name, pred in individual.items():
                correct = (pred > 0.5) == (actual == color)
                self.ensemble_ml.update_performance(model_name, correct)
        
        # Actualizar unified system
        predictions = individual if ensemble_pred else {}
        self.unified_prob_system.record_prediction(color, predictions, actual)

    # ─── UPDATE HISTORIAL Y TODOS LOS NIVELES ────────────────────────────────
    def _update_history_and_levels(self, number, real):
        self.spin_history.append({"number":number,"real":real})
        if len(self.spin_history)>300: self.spin_history.pop(0)
        self.result_sequence.append({"number":number,"real":real})

        # Actualizar ensemble ML
        self.ensemble_ml.add_spin(number, real)

        lo=self.original_levels[-1] if self.original_levels else 0
        li=self.inverted_levels[-1] if self.inverted_levels else 0
        lp=self.par_levels[-1]   if self.par_levels   else 0
        lip=self.impar_levels[-1] if self.impar_levels else 0
        lb=self.bajo_levels[-1]  if self.bajo_levels  else 0
        la=self.alto_levels[-1]  if self.alto_levels  else 0

        if number==0:
            if self.last_nonzero_color:
                lnc=self.last_nonzero_color
                self.original_levels.append(lo+(1 if lnc=="ROJO" else -1))
                self.inverted_levels.append(li+(1 if lnc=="NEGRO" else -1))
            else:
                self.original_levels.append(lo); self.inverted_levels.append(li)
            # Cero no cambia paridad/rango → propagar
            self.par_levels.append(lp); self.impar_levels.append(lip)
            self.bajo_levels.append(lb); self.alto_levels.append(la)
        else:
            self.original_levels.append(lo+(1 if real=="ROJO" else -1))
            self.inverted_levels.append(li+(1 if real=="NEGRO" else -1))
            self.last_nonzero_color=real
            par=get_paridad(number); rang=get_rango(number)
            self.par_levels.append(lp+(1 if par=="PAR" else -1))
            self.impar_levels.append(lip+(1 if par=="IMPAR" else -1))
            self.bajo_levels.append(lb+(1 if rang=="BAJO" else -1))
            self.alto_levels.append(la+(1 if rang=="ALTO" else -1))

        # Sincronizar con spin_history
        for lst in [self.original_levels,self.inverted_levels,
                    self.par_levels,self.impar_levels,
                    self.bajo_levels,self.alto_levels]:
            while len(lst)>len(self.spin_history): lst.pop(0)
        ml=min(len(self.original_levels),len(self.inverted_levels),
               len(self.par_levels),len(self.impar_levels),
               len(self.bajo_levels),len(self.alto_levels))
        self.original_levels=self.original_levels[-ml:]; self.inverted_levels=self.inverted_levels[-ml:]
        self.par_levels=self.par_levels[-ml:]; self.impar_levels=self.impar_levels[-ml:]
        self.bajo_levels=self.bajo_levels[-ml:]; self.alto_levels=self.alto_levels[-ml:]

        self._upd_amx(real); self.amx_system.update_streak(real,self.get_signal(number))
        if self.signal_active or self.waiting_for_attempt:
            self._upd_unified(self.bet_value,self.active_category)
        if real!="VERDE": self.unified_prob_system.update_streak(real)
        self.markov.update(self.spin_history)
        self.ml_predictor.add_spin(self.spin_history)
        self.category_ml.add_spin(number,real)

    def _upd_amx(self, color):
        lp=self.amx_system.ultimos_puntos[-1] if self.amx_system.ultimos_puntos else 0
        np_=lp+(1 if color=="ROJO" else -1 if color=="NEGRO" else 0)
        self.amx_system.ultimos_puntos.append(np_)
        if len(self.amx_system.ultimos_puntos)>300:
            self.amx_system.ultimos_puntos=self.amx_system.ultimos_puntos[-200:]

    def _check_recovery(self):
        if not self.recovery_active: return
        if self.bet_sys.bankroll>=self.recovery_target:
            self.consec_losses=0; self.recovery_active=False
            self.recovery_target=0.0; self.bet_sys.step=0

    # ─── MENSAJES ─────────────────────────────────────────────────────────────
    def _build_caption(self, attempt, unified_prob) -> str:
        bet=self.bet_sys.current_bet(); step=self.bet_sys.step+1
        pct=int((unified_prob["combined_prob"] if unified_prob else 0.5)*100)
        icon=CATEGORY_ICONS.get(self.bet_value,"❓")
        t=self.trigger_number
        
        # Info de modelos usados
        meta = unified_prob.get('meta', {}) if unified_prob else {}
        models_used = len(meta.get('individual_predictions', {}))
        
        if t==0: td="0 CERO 🟢"
        elif self.active_category=="COLOR":
            c=REAL_COLOR_MAP.get(t,"VERDE"); td=f"{t} {c} {CATEGORY_ICONS.get(c,'')}"
        elif self.active_category=="PARIDAD":
            p=get_paridad(t); td=f"{t} {p} {CATEGORY_ICONS.get(p,'')}" if p else f"{t} CERO 🟢"
        else:
            r=get_rango(t); td=f"{t} {r} {CATEGORY_ICONS.get(r,'')}" if r else f"{t} CERO 🟢"
        
        # Indicador de modelos ML
        ml_indicator = f"🧠 Ensemble ({models_used} modelos)" if models_used > 0 else "🧠 ML"
        
        return (f"☑️☑️ <b>SEÑAL CONFIRMADA</b> ☑️☑️\n\n"
                f"🎰 Juego: {self.name}\n"
                f"👉 Después de: {td}\n"
                f"🎯 Apostar a: {self.bet_value} {icon}\n"
                f"{ml_indicator}\n"
                f"🤖 Probabilidad Unificada: {pct}%\n"
                f"🌀 D'Alembert paso {step} de 20\n"
                f"📍 Apuesta: {bet:.2f} usd\n\n"
                f"♻️ Intento {attempt}/{MAX_ATTEMPTS}")

    def _get_chart(self, unified_prob=None) -> io.BytesIO:
        lvl=self._get_levels(self.active_category,self.bet_value)
        mk=self.markov.predict_color(self.spin_history); ml=self.ml_predictor.predict(self.spin_history)
        return generate_chart(lvl[:],self.spin_history[:],
                              self.bet_value,self.active_category,
                              markov_pred=mk,ml_pred=ml,unified_prob=unified_prob)

    def _send_signal(self, attempt, unified_prob=None):
        self.current_attempt_number=attempt
        self.signal_is_level1=(self.bet_sys.step==0 and not self.recovery_active)
        if self.signal_is_level1: self.level1_bankroll=self.bet_sys.bankroll
        caption=self._build_caption(attempt,unified_prob)
        chart=self._get_chart(unified_prob)
        msg=tg_send_photo(self.chat_id,self.thread_id,chart,caption)
        if msg: self.signal_msg_ids.append(msg)
        
        # Log detallado con info del ensemble
        meta = unified_prob.get('meta', {}) if unified_prob else {}
        logger.info(f"[{self.name}] Señal [{self.active_category}] {self.bet_value} int={attempt} "
                    f"prob={int((unified_prob['combined_prob'] if unified_prob else 0)*100)}% "
                    f"models={meta.get('models_used', 0)}")

    def _send_waiting_message(self, attempt_number):
        for mid in self.signal_msg_ids: tg_delete(self.chat_id,mid)
        self.signal_msg_ids=[]
        if self.waiting_msg_id: tg_delete(self.chat_id,self.waiting_msg_id); self.waiting_msg_id=None
        ord_str="2°" if attempt_number==2 else "3°"
        caption=(f"⚠️ <b>Esperando condiciones para el {ord_str} intento</b>\n\n"
                 f"🎰 <b>{self.name}</b>\n"
                 f"🔍 <i>Analizando {self.active_category} en cada giro...</i>\n")
        chart=self._get_chart()
        msg=tg_send_photo(self.chat_id,self.thread_id,chart,caption)
        if msg: self.waiting_msg_id=msg

    def _send_result(self, number, real, won, bet, attempt_won):
        bk=self.bet_sys.bankroll
        rd=self._result_display(number,real)
        for mid in self.signal_msg_ids: tg_delete(self.chat_id,mid)
        self.signal_msg_ids=[]
        if self.waiting_msg_id: tg_delete(self.chat_id,self.waiting_msg_id); self.waiting_msg_id=None
        chart=self._get_chart()
        text=f"{'✅' if won else '❌'} Resultado: {rd} {'Acierto!' if won else 'Fallo!'} — Bankroll actual: 💰 {bk:.2f} usd"
        tg_send_photo(self.chat_id,self.thread_id,chart,text)
        logger.info(f"[{self.name}] {'WIN' if won else 'LOSS'} #{number} ({rd}) bk={bk:.2f}")

    def _check_stats(self):
        if not self.stats.should_send_stats(): return
        bk=self.bet_sys.bankroll; s20=self.stats.get_batch_stats(bk); s24=self.stats.get_24h_stats(bk)
        self.stats.mark_stats_sent(bk)
        if not s20 and not s24: return
        txt=""
        for label,s in [(f"{s20.get('total','?')} SENALES",s20),("24 HORAS",s24)]:
            if s:
                txt+=(f"👉🏼 <b>ESTADISTICAS {label}</b>\n"
                      f"🈯️ <b>T:</b> {s['total']} 📈 <b>E:</b> {s['efficiency']}%\n"
                      f"1️⃣ <b>W:</b> {s['w1']} --> <b>E:</b> {s['e_w1']}%\n"
                      f"2️⃣ <b>W:</b> {s['w2']} --> <b>E:</b> {s['e_w2']}%\n"
                      f"3️⃣ <b>W:</b> {s['w3']} --> <b>E:</b> {s['e_w3']}%\n"
                      f"🈲 <b>L:</b> {s['losses']} --> <b>E:</b> {s['e_loss']}%\n"
                      f"💰 <i>Bankroll: {s['bankroll_delta']:.2f} usd</i>\n\n")
        tg_send_text(self.chat_id,self.thread_id,txt.strip())

    # ─── PROCESO PRINCIPAL ────────────────────────────────────────────────────
    def process_number(self, number: int):
        real=REAL_COLOR_MAP.get(number,"VERDE")
        if self.learning_phase:
            self.learning_spin_count+=1; self.learning_initial_numbers.append(number)
            logger.info(f"[{self.name}] Giro {self.learning_spin_count}/{self.max_learning_spins}: {number} {real}")
            self._update_history_and_levels(number,real)
            if self.learning_spin_count>=self.max_learning_spins:
                self.learning_phase=False; logger.info(f"[{self.name}] Aprendizaje completado.")
            return
        self._update_history_and_levels(number,real)

        if self.signal_active:
            is_win=self._is_win(number,real)
            ca=MAX_ATTEMPTS-self.attempts_left+1
            if is_win:
                bet=self.bet_sys.win()
                self.stats.record_signal_result(ca,True,bet,self.bet_sys.bankroll)
                self._record_cat_pred(self.active_category,self.bet_value,True)
                if self.active_category=="COLOR": self._record_pred_result(self.bet_value,real)
                self._check_recovery()
                self._send_result(number,real,True,bet,ca)
                self.signal_active=False; self.active_category=None
                self._check_stats(); self.signal_msg_ids=[]
            else:
                self.attempts_left-=1; bet=self.bet_sys.loss()
                if self.attempts_left<=0:
                    self._handle_full_loss(number,real,bet)
                else:
                    an=MAX_ATTEMPTS-self.attempts_left+1
                    
                    # NO borrar mensaje anterior inmediatamente - enviar nuevo primero
                    chosen=self._best_retry_value(number,an)
                    if chosen:
                        # Transición rápida: actualizar y enviar inmediatamente
                        old_msg_ids = self.signal_msg_ids.copy()
                        self.bet_value=chosen["bet_value"]
                        self.active_category=chosen["category"]
                        self.trigger_number=number
                        
                        # Enviar nueva señal PRIMERO
                        up=self._get_cat_prob(self.active_category,self.bet_value,number)
                        self._send_signal(an,up)
                        
                        # Luego limpiar la anterior (pequeño delay visual)
                        for mid in old_msg_ids:
                            tg_delete(self.chat_id, mid)
                        
                        self.signal_msg_ids = [self.signal_msg_ids[-1]]  # Mantener solo el más reciente
                    else:
                        # Si no hay señal inmediata, pasar a espera
                        self.signal_active=False; self.waiting_for_attempt=True
                        self.waiting_attempt_number=an
                        # NO enviar mensaje de espera inmediatamente - esperar 1-2 giros
                        if an == 2:  # Solo esperar en I2, I3 es más agresivo
                            self._send_waiting_message(an)
                        else:
                            # Para I3, mantener la última señal visible y seguir buscando
                            pass

        elif self.waiting_for_attempt:
            if real=="VERDE": self.skip_one_after_zero=True; return
            if self.skip_one_after_zero: self.skip_one_after_zero=False; return
            an=self.waiting_attempt_number
            chosen=self._best_retry_value(number,an)
            
            if chosen:
                # Transición inmediata sin "esperando" mensaje
                if self.waiting_msg_id:
                    tg_delete(self.chat_id,self.waiting_msg_id)
                    self.waiting_msg_id=None
                
                self.bet_value=chosen["bet_value"]
                self.active_category=chosen["category"]
                self.trigger_number=number
                self.signal_active=True; self.waiting_for_attempt=False
                
                up=self._get_cat_prob(self.active_category,self.bet_value,number)
                self._send_signal(an,up)
            else:
                # Para I3, si llevamos muchos giros esperando, forzar señal débil
                if an == 3 and self.waiting_attempt_number == 3:
                    # Contar giros en espera
                    if not hasattr(self, '_waiting_spins'):
                        self._waiting_spins = 0
                    self._waiting_spins += 1
                    
                    if self._waiting_spins >= 3:  # Después de 3 giros esperando
                        # Forzar señal con categoría original
                        weak_score = self._score_category(self.active_category or "COLOR", 
                                                         self.bet_value or "ROJO", 3)
                        if weak_score >= 0.48:  # Umbral muy permisivo
                            chosen = {
                                "category": self.active_category or "COLOR",
                                "bet_value": self.bet_value or "ROJO", 
                                "probability": weak_score
                            }
                            # Reset y enviar
                            self._waiting_spins = 0
                            if self.waiting_msg_id:
                                tg_delete(self.chat_id, self.waiting_msg_id)
                                self.waiting_msg_id = None
                            self.signal_active = True
                            self.waiting_for_attempt = False
                            up = self._get_cat_prob(chosen["category"], chosen["bet_value"], number)
                            self._send_signal(an, up)
                else:
                    self._waiting_spins = 0

        else:
            self.signal_msg_ids=[]
            best=self._detect_best_category_signal()
            if best:
                self.signal_active=True; self.active_category=best["category"]
                self.bet_value=best["bet_value"]; self.attempts_left=MAX_ATTEMPTS
                self.total_attempts=MAX_ATTEMPTS; self.trigger_number=best["trigger_number"]
                up=self._get_cat_prob(best["category"],best["bet_value"],best["trigger_number"])
                self._send_signal(1,up); self.amx_system.register_signal_sent()

    def _handle_full_loss(self, number, real, bet=None):
        if bet is None: bet=self.bet_sys.loss()
        self._record_cat_pred(self.active_category,self.bet_value,False)
        self.consec_losses+=1
        if self.consec_losses>=10:
            self.consec_losses=0; self.recovery_active=False; self.recovery_target=0.0
        else:
            self.recovery_active=True; self.recovery_target=self.level1_bankroll+BASE_BET
        self.stats.record_signal_result(0,False,bet,self.bet_sys.bankroll)
        if self.active_category=="COLOR": self._record_pred_result(self.bet_value,real)
        self._send_result(number,real,False,bet,0)
        self.signal_active=False; self.active_category=None
        self._check_stats(); self.signal_msg_ids=[]

    # ─── WEBSOCKET ────────────────────────────────────────────────────────────
    async def run_ws(self):
        rd=5
        while self.running:
            try:
                async with websockets.connect(WS_URL,ping_interval=30,ping_timeout=60,close_timeout=10) as ws:
                    self.ws=ws; rd=5; logger.info(f"[{self.name}] WS conectado")
                    await ws.send(json.dumps({"type":"subscribe","casinoId":CASINO_ID,"currency":"USD","key":[self.ws_key]}))
                    async for message in ws:
                        if not self.running: break
                        try: data=json.loads(message)
                        except: continue
                        if "last20Results" in data and isinstance(data["last20Results"],list):
                            tmp=[]
                            for r in data["last20Results"]:
                                gid=r.get("gameId"); num=r.get("result")
                                if gid and num is not None:
                                    try: n=int(num)
                                    except: continue
                                    if 0<=n<=36 and gid not in self.anti_block:
                                        tmp.append((gid,n)); self.anti_block.add(gid)
                                        if len(self.anti_block)>1000: self.anti_block.clear()
                            for _,n in reversed(tmp): self.process_number(n)
                        gid=data.get("gameId"); res=data.get("result")
                        if gid and res is not None:
                            try: n=int(res)
                            except: continue
                            if 0<=n<=36 and gid not in self.anti_block:
                                if len(self.anti_block)>1000: self.anti_block.clear()
                                self.anti_block.add(gid); self.process_number(n)
            except Exception as e:
                logger.warning(f"[{self.name}] WS error: {e}. Reconectando {rd}s")
                await asyncio.sleep(rd); rd=min(rd*2,60)


# ─── FLASK ────────────────────────────────────────────────────────────────────
app=Flask(__name__)

@app.route("/")
def index(): return jsonify({"status":"ok","bot":"AMX V24.0","ts":time.time()})
@app.route("/ping")
def ping(): return jsonify({"pong":True,"ts":time.time()})
@app.route("/health")
def health(): return jsonify({"healthy":True})

async def self_ping_loop():
    port=int(os.environ.get("PORT",10000))
    url=os.environ.get("RENDER_EXTERNAL_URL",f"http://localhost:{port}")
    while True:
        await asyncio.sleep(300)
        try:
            with urllib.request.urlopen(f"{url}/ping",timeout=10) as r: logger.info(f"Ping OK:{r.status}")
        except Exception as e: logger.warning(f"Ping failed:{e}")

# ─── COMANDOS TELEGRAM ────────────────────────────────────────────────────────
engines: dict[str, RouletteEngine]={}

@bot.message_handler(commands=['start','help'])
def cmd_start(message):
    bot.reply_to(message,"""
<b>🎰 Roulette Bot - Sistema AMX V24.0</b>

<b>🧠 Ensemble ML Avanzado:</b>
• <b>TCN</b>: Redes convolucionales temporales con dilataciones
• <b>LSTM+Attention</b>: Memoria a largo plazo con foco selectivo  
• <b>Transformer</b>: Self-attention para dependencias globales
• <b>DynamicEnsemble</b>: Pesos adaptativos por rendimiento

<b>Mejoras en V24.0:</b>
• 5 modelos en ensemble con pesos dinámicos
• Aprendizaje online continuo
• Detección de patrones multi-escala
• Mayor efectividad en señales a largo plazo

/moderado /tendencia /status /reset /help
""", parse_mode="HTML")

@bot.message_handler(commands=['moderado'])
def cmd_moderado(message):
    for e in engines.values(): e.set_mode("moderado")
    bot.reply_to(message,"✅ <b>Modo MODERADO activado</b>",parse_mode="HTML")

@bot.message_handler(commands=['tendencia'])
def cmd_tendencia(message):
    for e in engines.values(): e.set_mode("tendencia")
    bot.reply_to(message,"📈 <b>Modo TENDENCIA activado</b>",parse_mode="HTML")

@bot.message_handler(commands=['status'])
def cmd_status(message):
    lines=["<b>📊 ESTADO AMX V24.0</b>\n"]
    for name,engine in engines.items():
        mi="📈" if engine.amx_system.mode=="tendencia" else "📊"
        if engine.learning_phase:
            st=f"📚 Aprendiendo ({engine.learning_spin_count}/{engine.max_learning_spins})"
        elif engine.signal_active:
            v=engine.bet_value or "?"; ic=CATEGORY_ICONS.get(v,"")
            st=f"🟢 [{engine.active_category}] {v}{ic} int={engine.current_attempt_number}/{MAX_ATTEMPTS}"
        elif engine.waiting_for_attempt:
            st=f"⏳ Esperando int.{engine.waiting_attempt_number}/{MAX_ATTEMPTS}"
        else: st="⚪ Idle"
        
        # Info del ensemble
        w=engine.ensemble_ml.weights if hasattr(engine, 'ensemble_ml') else {}
        w_str = f"TCN:{w.get('tcn',0):.2f} LSTM:{w.get('lstm_att',0):.2f} TRF:{w.get('transformer',0):.2f}" if w else "N/A"
        
        lines.append(f"<b>{name}</b>: {mi} — {st}\n🧠 {w_str}")
    bot.reply_to(message,"\n".join(lines),parse_mode="HTML")

@bot.message_handler(commands=['reset'])
def cmd_reset(message):
    for e in engines.values(): 
        e.stats=DetailedStats()
        if hasattr(e, 'ensemble_ml'):
            # Reset performance history
            for name in e.ensemble_ml.performance_history:
                e.ensemble_ml.performance_history[name].clear()
    bot.reply_to(message,"🔄 <b>Estadísticas y pesos del ensemble reseteados</b>",parse_mode="HTML")

# ─── MAIN ─────────────────────────────────────────────────────────────────────
def run_flask():
    app.run(host="0.0.0.0",port=int(os.environ.get("PORT",10000)),debug=False,use_reloader=False)

async def main():
    global engines
    engines={name:RouletteEngine(name,cfg) for name,cfg in ROULETTE_CONFIGS.items()}
    tasks=[asyncio.create_task(e.run_ws()) for e in engines.values()]
    tasks.append(asyncio.create_task(self_ping_loop()))
    threading.Thread(target=lambda:bot.polling(none_stop=True,interval=1,timeout=30),daemon=True).start()
    logger.info("🎰 AMX V24.0 iniciado con Ensemble ML Avanzado")
    await asyncio.gather(*tasks)

if __name__=="__main__":
    threading.Thread(target=run_flask,daemon=True).start()
    try: asyncio.run(main())
    except KeyboardInterrupt: logger.info("Bot stopped.")
