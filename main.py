#!/usr/bin/env python3
"""
Roulette Telegram Signal Bot - Sistema AMX V20
VERSION 3.0 — Mejoras completas:
  1. Probabilidad conjunta ponderada Markov+ML+Tabla con pesos adaptativos
  2. Umbrales dinámicos por volatilidad, rachas y confianza de modelos
  3. Pesos auto-actualizados por backtesting online (cada 10 señales)
  4. EMA + Soporte/Resistencia como factores de ajuste de probabilidad
  5. Gráfico eje-Y: [min(últimos 50 niveles)-1, max+1]
  6. Formato uniforme de señal para intento 1/2/3
  7. Resultado con gráfico actualizado + secuencia de emojis, borra señales del chat
  8. Estadísticas por intento (1°/2°/3°/L) con eficiencias individuales
  9. Verificación por game_id (sin temporizadores)
"""
import asyncio, io, json, logging, os, threading, time, urllib.request
from collections import deque, defaultdict
from typing import Optional, Literal

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
    status_forcelist=[429,500,502,503,504],
    allowed_methods=["GET","POST"], raise_on_status=False)
_adapter = HTTPAdapter(max_retries=_retry, pool_connections=10, pool_maxsize=20)
_session.mount("https://", _adapter); _session.mount("http://", _adapter)
bot = telebot.TeleBot(TOKEN, threaded=False)
bot.session = _session

REAL_COLOR_MAP = {
    0:"VERDE",1:"ROJO",2:"NEGRO",3:"ROJO",4:"NEGRO",5:"ROJO",6:"NEGRO",
    7:"ROJO",8:"NEGRO",9:"ROJO",10:"NEGRO",11:"NEGRO",12:"ROJO",13:"NEGRO",
    14:"ROJO",15:"NEGRO",16:"ROJO",17:"NEGRO",18:"ROJO",19:"ROJO",20:"NEGRO",
    21:"ROJO",22:"NEGRO",23:"ROJO",24:"NEGRO",25:"ROJO",26:"NEGRO",27:"ROJO",
    28:"NEGRO",29:"NEGRO",30:"ROJO",31:"NEGRO",32:"ROJO",33:"NEGRO",34:"ROJO",
    35:"NEGRO",36:"ROJO"
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
    "Azure Roulette": {
        "ws_key": 227,
        "chat_id": -1003835197023,
        "thread_id": 6,
        "color_data": COLOR_DATA,
        "betting_system": "dalembert",
        "min_prob_threshold": 0.52,
    },
}

WS_URL       = "wss://dga.pragmaticplaylive.net/ws"
CASINO_ID    = "ppcjd00000007254"
MAX_ATTEMPTS = 3
BASE_BET     = 0.10
VISIBLE      = 50

# ─── D'ALEMBERT ───────────────────────────────────────────────────────────────
class D_Alembert:
    def __init__(self, base):
        self.base=base; self.step=0; self.bankroll=0.0; self.max_step=20
    def current_bet(self): return round(self.base*(self.step+1),2)
    def win(self):
        bet=self.current_bet(); self.bankroll=round(self.bankroll+bet,2)
        if self.step>0: self.step-=1
        return bet
    def loss(self):
        bet=self.current_bet(); self.bankroll=round(self.bankroll-bet,2)
        if self.step>=self.max_step-1: self.step=0
        else: self.step+=1
        return bet

# ─── MARKOV CHAIN (ventana 100 giros) ─────────────────────────────────────────
class MarkovChainPredictor:
    def __init__(self, window=100, order=2):
        self.window=window; self.order=order; self.tc={}
    def update(self, spin_history):
        self.tc=defaultdict(lambda:defaultdict(int))
        recent=[s["real"] for s in spin_history[-self.window:] if s["real"]!="VERDE"]
        if len(recent)<self.order+1: return
        for i in range(len(recent)-self.order):
            st=tuple(recent[i:i+self.order]); nc=recent[i+self.order]
            if nc in ("ROJO","NEGRO"): self.tc[st][nc]+=1
    def predict(self, spin_history):
        recent=[s["real"] for s in spin_history if s["real"]!="VERDE"]
        if len(recent)<self.order: return None
        st=tuple(recent[-self.order:]); counts=dict(self.tc.get(st,{}))
        total=sum(counts.values())
        if total<5: return None
        return {"ROJO":counts.get("ROJO",0)/total,"NEGRO":counts.get("NEGRO",0)/total,"total":total}

# ─── ML PATTERN PREDICTOR (historial completo) ────────────────────────────────
class MLPatternPredictor:
    def __init__(self, pattern_length=4):
        self.pl=pattern_length; self.pc=defaultdict(lambda:defaultdict(int)); self._kl=0
    def add_spin(self, spin_history):
        nv=[s["real"] for s in spin_history if s["real"]!="VERDE"]
        cl=len(nv)
        if cl<=self._kl: return
        self._kl=cl
        if cl<self.pl+1: return
        i=cl-self.pl-1; pat=tuple(nv[i:i+self.pl]); nc=nv[i+self.pl]
        if nc in ("ROJO","NEGRO"): self.pc[pat][nc]+=1
    def predict(self, spin_history):
        nv=[s["real"] for s in spin_history if s["real"]!="VERDE"]
        if len(nv)<self.pl: return None
        pat=tuple(nv[-self.pl:]); counts=dict(self.pc.get(pat,{}))
        total=sum(counts.values())
        if total<3: return None
        return {"ROJO":counts.get("ROJO",0)/total,"NEGRO":counts.get("NEGRO",0)/total,"total":total}

# ─── ENSEMBLE ADAPTATIVO ──────────────────────────────────────────────────────
class AdaptiveEnsemble:
    W_INIT       = {"markov":0.40,"ml":0.40,"table":0.20}
    ALPHA        = 0.15
    UPDATE_EVERY = 10

    def __init__(self):
        self.weights=dict(self.W_INIT)
        self._correct={"markov":0,"ml":0,"table":0}
        self._total  ={"markov":0,"ml":0,"table":0}
        self._resolved=0

    @staticmethod
    def _ema_list(data, period):
        if len(data)<period: return [None]*len(data)
        mult=2/(period+1); out=[None]*(period-1); prev=sum(data[:period])/period; out.append(prev)
        for i in range(period,len(data)): prev=(data[i]*mult)+(prev*(1-mult)); out.append(prev)
        return out

    def unified_prob(self, color, markov_pred, ml_pred, table_prob,
                     orig_levels, inv_levels, spin_history):
        p_mk = markov_pred.get(color,0.50) if markov_pred else 0.50
        p_ml = ml_pred.get(color,0.50)     if ml_pred     else 0.50
        p_tb = table_prob
        w=self.weights
        p_raw = w["markov"]*p_mk + w["ml"]*p_ml + w["table"]*p_tb

        # Factor EMA (tendencia del nivel)
        levels = orig_levels if color=="ROJO" else inv_levels
        ema_factor=1.0
        if len(levels)>=20:
            ema20=self._ema_list(levels,20); li=len(levels)-1
            if ema20[li] is not None:
                diff=levels[li]-ema20[li]
                ema_factor=1.0+float(np.clip(diff/30.0,-0.08,0.08))

        # Factor S/R
        sr_factor=1.0
        if len(levels)>=30:
            sr=find_support_resistance(levels,30); cur=levels[-1]
            if color=="ROJO":
                if sr["resistance"] is not None and sr["resistance"]>cur:
                    gap=(sr["resistance"]-cur)/max(abs(sr["resistance"])+1,1)
                    sr_factor=1.0+float(np.clip(gap*0.3,-0.05,0.05))
            else:
                if sr["support"] is not None and sr["support"]<cur:
                    gap=(cur-sr["support"])/max(abs(cur)+1,1)
                    sr_factor=1.0+float(np.clip(gap*0.3,-0.05,0.05))

        return float(np.clip(p_raw*ema_factor*sr_factor,0.01,0.99))

    def dynamic_threshold(self, base_thr, spin_history, markov_pred, ml_pred, color):
        thr=base_thr
        recent=[s["real"] for s in spin_history[-20:] if s["real"]!="VERDE"]
        if len(recent)>=4:
            changes=sum(1 for i in range(1,len(recent)) if recent[i]!=recent[i-1])
            vol=changes/(len(recent)-1)
            thr+=(vol-0.5)*0.06
        streak=0
        for c in reversed(recent):
            if c==color: streak+=1
            else: break
        if streak>=4: thr+=0.02*min(streak-3,4)
        cm=markov_pred.get(color,0.5) if markov_pred else 0.5
        cml=ml_pred.get(color,0.5)    if ml_pred     else 0.5
        avg=(cm+cml)/2
        if avg>0.65: thr-=0.03
        elif avg<0.45: thr+=0.03
        return float(np.clip(thr,0.42,0.72))

    def register_outcome(self, color_bet, real_color, markov_pred, ml_pred, table_prob):
        if markov_pred:
            self._total["markov"]+=1
            self._correct["markov"]+=int(markov_pred.get(real_color,0)>0.5)
        if ml_pred:
            self._total["ml"]+=1
            self._correct["ml"]+=int(ml_pred.get(real_color,0)>0.5)
        self._total["table"]+=1
        self._correct["table"]+=int(color_bet==real_color)
        self._resolved+=1
        if self._resolved%self.UPDATE_EVERY==0: self._update_weights()

    def _update_weights(self):
        acc={k:self._correct[k]/self._total[k] if self._total[k]>0 else 0.5 for k in self.weights}
        tot=sum(acc.values()) or 1.0
        new_w={k:acc[k]/tot for k in acc}
        for k in self.weights:
            self.weights[k]=(1-self.ALPHA)*self.weights[k]+self.ALPHA*new_w[k]
        s=sum(self.weights.values())
        self.weights={k:v/s for k,v in self.weights.items()}
        logger.info(f"Ensemble weights → {self.weights}")

# ─── SISTEMA AMX V20 ──────────────────────────────────────────────────────────
class AMXSignalSystem:
    def __init__(self, mode="moderado"):
        self.mode=mode; self.last_signal_time=0; self.cooldown_seconds=8
        self.so_cooldown=None; self.ultimos_puntos=[]
        self.last_two_expected=deque(maxlen=2); self.last_two_colors=deque(maxlen=2)
    def update_streak(self, real, expected):
        if expected: self.last_two_expected.append(real==expected)
        self.last_two_colors.append(real)
    def calc_ema(self, data, period):
        if len(data)<period: return [None]*len(data)
        mult=2/(period+1); ema=[None]*(period-1); prev=sum(data[:period])/period; ema.append(prev)
        for i in range(period,len(data)): prev=(data[i]*mult)+(prev*(1-mult)); ema.append(prev)
        return ema
    def _base_check(self):
        ahora=time.time()
        if ahora-self.last_signal_time<self.cooldown_seconds: return False
        if self.so_cooldown and ahora-self.so_cooldown<8: return False
        return True
    def check_signal_tendencia(self, positions, color_data, cur_num, exp_color, thr):
        if len(positions)<20 or not self._base_check(): return None
        e4=self.calc_ema(positions,4); e8=self.calc_ema(positions,8); e20=self.calc_ema(positions,20)
        if any(v is None for v in [e4[-1],e8[-1],e20[-1],e4[-2],e8[-2],e20[-2]]): return None
        cp=positions[-1]
        cA=e4[-2]<=e20[-2] and e4[-1]>e20[-1]; s3=cp>e4[-1] and cp>e8[-1] and cp>e20[-1]
        cE=e8[-2]<=e20[-2] and e8[-1]>e20[-1]; nE=abs(cp-e4[-1])<=0.5
        do=len(self.last_two_expected)>=2 and all(self.last_two_expected)
        if not((cA or s3) or cE or (s3 and do) or (s3 and nE)): return None
        entry=next((e for e in color_data if e["id"]==cur_num),None)
        if not entry or entry["senal"]=="NO APOSTAR": return None
        prob=entry["rojo"] if exp_color=="ROJO" else entry["negro"]
        if entry["senal"]!=exp_color or prob<thr: return None
        return {"type":"SKRILL_2.0","mode":"tendencia","expected_color":exp_color,
                "probability":prob,"trigger_number":cur_num,
                "strength":"strong" if(cA or cE) else "moderate"}
    def check_signal_moderado(self, positions, color_data, cur_num, exp_color, thr):
        if len(positions)<20 or not self._base_check(): return None
        e4=self.calc_ema(positions,4); e8=self.calc_ema(positions,8); e20=self.calc_ema(positions,20)
        if any(v is None for v in [e4[-1],e8[-1],e20[-1],e8[-2],e20[-2]]): return None
        cE=e8[-2]<=e20[-2] and e8[-1]>e20[-1]; sE=positions[-1]>e4[-1] and positions[-1]>e8[-1]
        pV=False
        if len(positions)>=3:
            a,b,c=positions[-3],positions[-2],positions[-1]
            pV=b<a and b<c and abs(a-c)<=1 and c>a
        do=len(self.last_two_expected)>=2 and all(self.last_two_expected)
        cR=do and e4[-1]>e8[-1]>e20[-1] and sE
        if not(cE or pV or cR): return None
        entry=next((e for e in color_data if e["id"]==cur_num),None)
        if not entry or entry["senal"]=="NO APOSTAR": return None
        prob=entry["rojo"] if exp_color=="ROJO" else entry["negro"]
        if entry["senal"]!=exp_color or prob<thr: return None
        return {"type":"ALERTA_2.0","mode":"moderado","expected_color":exp_color,
                "probability":prob,"trigger_number":cur_num,
                "pattern":"V" if pV else "EMA_CROSS"}
    def register_signal_sent(self): self.last_signal_time=time.time()
    def register_so_failed(self): self.so_cooldown=time.time()

# ─── STATISTICS CON DESGLOSE POR INTENTO ──────────────────────────────────────
class Stats:
    def __init__(self):
        self.total=0; self.wins=0; self.losses=0
        self.wins_by_attempt={1:0,2:0,3:0}
        self.last_stats_at=0; self._h24=deque()
        self.batch_start_bankroll=None; self._wins_at_last_batch=0
        self._watt_at_last_batch={1:0,2:0,3:0}
    def record(self, is_win, bankroll, attempt_number=1):
        self.total+=1
        if is_win:
            self.wins+=1; att=max(1,min(3,attempt_number))
            self.wins_by_attempt[att]+=1
        else: self.losses+=1
        self._h24.append((time.time(),is_win,bankroll,attempt_number)); self._trim24()
    def _trim24(self):
        cutoff=time.time()-86400
        while self._h24 and self._h24[0][0]<cutoff: self._h24.popleft()
    def should_send_stats(self): return (self.total-self.last_stats_at)>=20
    def mark_stats_sent(self, bankroll):
        self.last_stats_at=self.total; self.batch_start_bankroll=bankroll
        self._wins_at_last_batch=self.wins; self._watt_at_last_batch=dict(self.wins_by_attempt)
    def _fmt_block(self, t, w, l, watt, bk):
        e=round(w/t*100,1) if t else 0.0
        bk_s=(f"+{bk:.2f}" if bk>=0 else f"{bk:.2f}")
        lines=[f"🈯️ T: {t} 📈 E: {e}%"]
        for att,icon in [(1,"1️⃣"),(2,"2️⃣"),(3,"3️⃣")]:
            ea=round(watt[att]/t*100,2) if t else 0.0
            lines.append(f"{icon} W: {watt[att]} --> E: {ea:.2f}%")
        el=round(l/t*100,2) if t else 0.0
        lines.append(f"🈲 L: {l} --> E: {el:.2f}%")
        lines.append(f"💰 Bankroll acumulado: {bk_s} usd")
        return "\n".join(lines)
    def batch_stats_text(self, bk):
        n=self.total-self.last_stats_at; w=self.wins-self._wins_at_last_batch; l=n-w
        watt={a:self.wins_by_attempt[a]-self._watt_at_last_batch.get(a,0) for a in(1,2,3)}
        bbk=round(bk-self.batch_start_bankroll,2) if self.batch_start_bankroll is not None else 0.0
        return self._fmt_block(n,w,l,watt,bbk)
    def h24_stats_text(self, bk):
        self._trim24(); t=len(self._h24); w=sum(1 for _,iw,_,_ in self._h24 if iw); l=t-w
        watt={1:0,2:0,3:0}
        for _,iw,_,att in self._h24:
            if iw: watt[max(1,min(3,att))]+=1
        bbk=round(self._h24[-1][2]-self._h24[0][2],2) if t>=2 else 0.0
        return self._fmt_block(t,w,l,watt,bbk)

# ─── SOPORTE/RESISTENCIA ──────────────────────────────────────────────────────
def find_support_resistance(levels, lookback=30):
    if len(levels)<lookback: return {'support':None,'resistance':None}
    r=levels[-lookback:]; sc=[]; rc=[]
    for i in range(2,len(r)-2):
        if r[i]<r[i-1] and r[i]<r[i-2] and r[i]<r[i+1] and r[i]<r[i+2]: sc.append(r[i])
        if r[i]>r[i-1] and r[i]>r[i-2] and r[i]>r[i+1] and r[i]>r[i+2]: rc.append(r[i])
    return {'support':sc[-1] if sc else None,'resistance':rc[-1] if rc else None}

# ─── CHART GENERATION ─────────────────────────────────────────────────────────
def generate_chart(levels, spin_history, bet_color, visible=VISIBLE,
                   unified_prob=None):
    """Eje Y ajustado a [min(últimos visible niveles)-1, max+1]."""
    arr=np.array(levels,dtype=float); n=len(arr)
    def cema(data,p):
        if len(data)<p: return np.full(len(data),np.nan)
        m=2/(p+1); out=np.full(len(data),np.nan); out[p-1]=np.mean(data[:p])
        for i in range(p,len(data)): out[i]=(data[i]-out[i-1])*m+out[i-1]
        return out
    e4=cema(arr,4); e8=cema(arr,8); e20=cema(arr,20)
    start=max(0,n-visible); sl=slice(start,n)
    x=np.arange(len(arr[sl])); hs=spin_history[start:]
    y=arr[sl]; e4s=e4[sl]; e8s=e8[sl]; e20s=e20[sl]

    is_r=bet_color=="ROJO"
    bg="#0b101f"; axbg="#0f1a2a"; gc="#1e2e48"
    lc="#e84040" if is_r else "#9090bb"
    tc="#ff8080" if is_r else "#b0b8d0"
    e4c="#ff9f43"; e8c="#48dbfb"; e20c="#1dd1a1"

    fig,ax=plt.subplots(figsize=(8,3.8),facecolor=bg); ax.set_facecolor(axbg)
    ax.fill_between(x,y,alpha=0.10,color=lc)
    ax.plot(x,y,color=lc,linewidth=0.8,zorder=3)
    ax.plot(x,e4s,color=e4c,linewidth=0.7,linestyle="--",label="EMA 4",zorder=4)
    ax.plot(x,e8s,color=e8c,linewidth=0.7,linestyle="--",label="EMA 8",zorder=4)
    ax.plot(x,e20s,color=e20c,linewidth=1.0,label="EMA 20",zorder=4)

    dc={"ROJO":"#e84040","NEGRO":"#aaaacc","VERDE":"#2ecc71"}
    for i,sp in enumerate(hs):
        ax.scatter(i,y[i],color=dc.get(sp["real"],"#fff"),s=22,zorder=5,
                   edgecolors="white",linewidths=0.3)

    # Ajuste eje Y: min-1 / max+1 de los datos visibles
    vy=y[~np.isnan(y)]
    if len(vy)>0:
        ax.set_ylim(float(np.min(vy))-1, float(np.max(vy))+1)

    # S/R
    sr=find_support_resistance(levels,30)
    sv=sr['support']; rv=sr['resistance']
    rc2="#e84040" if is_r else "#888888"
    sc2="#888888"  if is_r else "#e84040"
    if sv is not None:
        ax.axhline(y=sv,color=sc2,linestyle='--',linewidth=1.5,alpha=0.7)
        ax.text(x[-1],sv,f' S {sv:.1f}',color=sc2,fontsize=7,va='bottom',ha='right')
    if rv is not None:
        ax.axhline(y=rv,color=rc2,linestyle='--',linewidth=1.5,alpha=0.7)
        ax.text(x[-1],rv,f' R {rv:.1f}',color=rc2,fontsize=7,va='top',ha='right')

    ts=max(1,len(x)//8); tx=list(range(0,len(x),ts))
    tl=[str(hs[i]["number"]) if i<len(hs) else "" for i in tx]
    ax.set_xticks(tx); ax.set_xticklabels(tl,color="#8899bb",fontsize=7)
    ax.tick_params(axis='y',colors="#8899bb",labelsize=7)
    ax.tick_params(axis='x',colors="#8899bb",labelsize=7)
    ax.spines['bottom'].set_color(gc); ax.spines['left'].set_color(gc)
    ax.spines['top'].set_visible(False); ax.spines['right'].set_visible(False)
    ax.grid(axis='y',color=gc,linewidth=0.4,alpha=0.5)

    emoji="🔴" if is_r else "⚫️"
    pi=f" · P.Unif:{unified_prob*100:.0f}%" if unified_prob is not None else ""
    ax.set_title(f"{emoji} {'ROJO' if is_r else 'NEGRO'} — últimos {visible} giros · EMA 4/8/20{pi}",
                 color=tc,fontsize=8.5,pad=6)

    from matplotlib.lines import Line2D
    les=[
        Line2D([0],[0],color=lc,linewidth=0.8,label="Nivel"),
        Line2D([0],[0],color=e4c,linewidth=0.7,linestyle="--",label="EMA 4"),
        Line2D([0],[0],color=e8c,linewidth=0.7,linestyle="--",label="EMA 8"),
        Line2D([0],[0],color=e20c,linewidth=1.0,label="EMA 20"),
        Line2D([0],[0],marker='o',color='w',markerfacecolor='#e84040',markersize=5,label="Rojo"),
        Line2D([0],[0],marker='o',color='w',markerfacecolor='#aaaacc',markersize=5,label="Negro"),
        Line2D([0],[0],marker='o',color='w',markerfacecolor='#2ecc71',markersize=5,label="Verde"),
    ]
    if sv is not None: les.append(Line2D([0],[0],color=sc2,linestyle='--',linewidth=1.5,label='Soporte'))
    if rv is not None: les.append(Line2D([0],[0],color=rc2,linestyle='--',linewidth=1.5,label='Resistencia'))
    ax.legend(handles=les,loc="upper left",fontsize=6.5,facecolor="#0b101f",
              edgecolor=gc,labelcolor="white",framealpha=0.8,ncol=2)
    plt.tight_layout(pad=0.8)
    buf=io.BytesIO(); fig.savefig(buf,format="png",dpi=120,facecolor=bg)
    plt.close(fig); buf.seek(0); return buf

# ─── HELPERS ──────────────────────────────────────────────────────────────────
COLOR_EMOJI={"ROJO":"🔴","NEGRO":"⚫️","VERDE":"🟢"}

def build_sequence_line(spin_history, n):
    recent=spin_history[-n:] if len(spin_history)>=n else spin_history
    emojis=[COLOR_EMOJI.get(s["real"],"❓") for s in recent]
    return "🆔 Secuencia: "+" --> ".join(emojis) if emojis else "🆔 Secuencia: —"

_TG_MAX=5
def _tg_call(fn,*a,**kw):
    delay=2.0
    for att in range(1,_TG_MAX+1):
        try: return fn(*a,**kw)
        except Exception as e:
            err=str(e)
            if "retry after" in err.lower():
                try: wait=int(''.join(filter(str.isdigit,err)))+1
                except: wait=30
                logger.warning(f"Flood-wait {wait}s"); time.sleep(wait); continue
            logger.warning(f"TG error (att {att}/{_TG_MAX}): {e}")
            if att<_TG_MAX: time.sleep(delay); delay=min(delay*2,60)
            else: logger.error(f"TG failed: {e}"); return None

def tg_send_photo(chat_id,thread_id,buf,cap):
    buf.seek(0)
    msg=_tg_call(bot.send_photo,chat_id=chat_id,photo=buf,caption=cap,
                 parse_mode="HTML",message_thread_id=thread_id)
    return msg.message_id if msg else None
def tg_send_text(chat_id,thread_id,text):
    msg=_tg_call(bot.send_message,chat_id=chat_id,text=text,
                 parse_mode="HTML",message_thread_id=thread_id)
    return msg.message_id if msg else None
def tg_delete(chat_id,mid): _tg_call(bot.delete_message,chat_id=chat_id,message_id=mid)

# ─── ROULETTE ENGINE ──────────────────────────────────────────────────────────
class RouletteEngine:
    def __init__(self, name, cfg):
        self.name=name; self.ws_key=cfg["ws_key"]
        self.chat_id=cfg["chat_id"]; self.thread_id=cfg["thread_id"]
        self.color_data=cfg["color_data"]
        self.spin_history=[]; self.original_levels=[]; self.inverted_levels=[]
        self.last_nonzero_color=None; self.anti_block=set()

        self.signal_active=False; self.waiting_for_attempt=False
        self.waiting_attempt_number=0; self.skip_one_after_zero=False
        self.expected_color=None; self.bet_color=None
        self.attempts_left=0; self.total_attempts=0
        self.trigger_number=None; self.current_attempt_number=1

        self.signal_msg_ids=[]; self.waiting_msg_id=None
        self.bet_sys=D_Alembert(BASE_BET)
        self.consec_losses=0; self.recovery_active=False
        self.recovery_target=0.0; self.level1_bankroll=0.0; self.signal_is_level1=False

        self.amx_system=AMXSignalSystem(mode="moderado")
        self.min_prob_threshold=cfg.get("min_prob_threshold",0.48)
        self.markov=MarkovChainPredictor(window=100,order=2)
        self.ml_predictor=MLPatternPredictor(pattern_length=4)
        self.ensemble=AdaptiveEnsemble()
        self.stats=Stats(); self.ws=None; self.running=True

    def set_mode(self, mode):
        self.amx_system=AMXSignalSystem(mode=mode)
        logger.info(f"[{self.name}] Modo → {mode}")

    @staticmethod
    def _ema(data, period):
        if len(data)<period: return [None]*len(data)
        mult=2/(period+1); out=[None]*(period-1); prev=sum(data[:period])/period; out.append(prev)
        for i in range(period,len(data)): prev=(data[i]-prev)*mult+prev; out.append(prev)
        return out

    def get_entry(self, num):
        for e in self.color_data:
            if e["id"]==num: return e
        return None
    def get_signal(self, num):
        e=self.get_entry(num); return e["senal"] if e else None
    def get_prob(self, num, color):
        e=self.get_entry(num)
        if not e: return 0.0
        return e["rojo"] if color=="ROJO" else e["negro"]
    def _opp(self, c): return "NEGRO" if c=="ROJO" else "ROJO"

    def _uni_prob(self, color, trigger_num):
        mp=self.markov.predict(self.spin_history)
        ml=self.ml_predictor.predict(self.spin_history)
        tp=self.get_prob(trigger_num,color)
        return self.ensemble.unified_prob(color,mp,ml,tp,
               self.original_levels,self.inverted_levels,self.spin_history)

    def _dyn_thr(self, color):
        mp=self.markov.predict(self.spin_history)
        ml=self.ml_predictor.predict(self.spin_history)
        return self.ensemble.dynamic_threshold(
            self.min_prob_threshold,self.spin_history,mp,ml,color)

    def _check_retry(self, color, trigger_num):
        entry=self.get_entry(trigger_num)
        if not entry or entry["senal"]=="NO APOSTAR" or entry["senal"]!=color: return False
        p=self._uni_prob(color,trigger_num)
        if p<self._dyn_thr(color): return False
        levels=self.original_levels if color=="ROJO" else self.inverted_levels
        if len(levels)<20: return False
        ema20=self._ema(levels,20); li=len(levels)-1
        if ema20[li] is None or levels[li]<=ema20[li]: return False
        return True

    def _best_retry_color(self, trigger_num):
        same=self._check_retry(self.bet_color,trigger_num)
        opp=self._opp(self.bet_color); opok=self._check_retry(opp,trigger_num)
        if same and opok:
            ps=self._uni_prob(self.bet_color,trigger_num); po=self._uni_prob(opp,trigger_num)
            return self.bet_color if ps>=po else opp
        if same: return self.bet_color
        if opok: return opp
        return None

    def _detect_amx(self):
        if len(self.amx_system.ultimos_puntos)<20: return None
        cur=self.spin_history[-1]["number"] if self.spin_history else 0
        entry=self.get_entry(cur)
        if not entry or entry["senal"]=="NO APOSTAR": return None
        ec=entry["senal"]
        rc=[s["real"] for s in self.spin_history[-5:]]
        mom=0
        for c in reversed(rc):
            if c==ec: mom+=1
            elif c!="VERDE": break
        if mom<2: return None
        try:
            if self.amx_system.mode=="tendencia":
                return self.amx_system.check_signal_tendencia(
                    self.amx_system.ultimos_puntos,self.color_data,cur,ec,self.min_prob_threshold)
            else:
                return self.amx_system.check_signal_moderado(
                    self.amx_system.ultimos_puntos,self.color_data,cur,ec,self.min_prob_threshold)
        except Exception as e:
            logger.warning(f"[{self.name}] AMX err: {e}"); return None

    def should_activate(self):
        losses=self.consec_losses; ms=22+losses*2
        if len(self.spin_history)<ms: return None
        ln=self.spin_history[-1]["number"]; entry=self.get_entry(ln)
        if not entry or entry["senal"]=="NO APOSTAR": return None
        exp=entry["senal"]
        if len(self.original_levels)<20 or len(self.inverted_levels)<20: return None
        e4o=self._ema(self.original_levels,4); e8o=self._ema(self.original_levels,8)
        e20o=self._ema(self.original_levels,20)
        e4i=self._ema(self.inverted_levels,4); e8i=self._ema(self.inverted_levels,8)
        e20i=self._ema(self.inverted_levels,20)
        req=min(3+losses,13); li=len(self.original_levels)-1
        def chk(lvl,e20,e8,e4,idx):
            for off in range(req):
                i=idx-(req-1)+off
                if i<0 or i>=len(lvl) or i>=len(e20): return False
                if e20[i] is None or lvl[i]<=e20[i]: return False
                if losses>=2 and (i>=len(e8) or e8[i] is None or lvl[i]<=e8[i]): return False
                if losses>=4 and (i>=len(e4) or e4[i] is None or lvl[i]<=e4[i]): return False
            return True
        if exp=="ROJO" and chk(self.original_levels,e20o,e8o,e4o,li): return "ROJO"
        if exp=="NEGRO" and chk(self.inverted_levels,e20i,e8i,e4i,li): return "NEGRO"
        return None

    def determine_bet_color(self, expected):
        if len(self.spin_history)<20: return expected
        e20o=self._ema(self.original_levels,20); e20i=self._ema(self.inverted_levels,20)
        li=len(self.original_levels)-1
        if li<0 or li>=len(e20o) or li>=len(e20i): return expected
        if e20o[li] is None or e20i[li] is None: return expected
        ls=self.get_signal(self.spin_history[-1]["number"])
        if expected=="ROJO":
            return "NEGRO" if self.original_levels[li]<e20o[li] and ls=="NEGRO" else "ROJO"
        return "ROJO" if self.inverted_levels[li]<e20i[li] and ls=="ROJO" else "NEGRO"

    def _check_recovery(self):
        if not self.recovery_active: return
        if self.bet_sys.bankroll>=self.recovery_target:
            logger.info(f"[{self.name}] Recuperación OK!")
            self.consec_losses=0; self.recovery_active=False
            self.recovery_target=0.0; self.bet_sys.step=0

    def _upd_amx(self, color):
        last=self.amx_system.ultimos_puntos[-1] if self.amx_system.ultimos_puntos else 0
        d=1 if color=="ROJO" else (-1 if color=="NEGRO" else 0)
        self.amx_system.ultimos_puntos.append(last+d)
        if len(self.amx_system.ultimos_puntos)>300:
            self.amx_system.ultimos_puntos=self.amx_system.ultimos_puntos[-200:]

    def _chart(self, up=None):
        lvl=self.original_levels[:] if self.bet_color=="ROJO" else self.inverted_levels[:]
        return generate_chart(lvl,self.spin_history[:],self.bet_color,unified_prob=up)

    # ── Mensajes ──────────────────────────────────────────────────────────────
    def _signal_caption(self, trigger, attempt, up):
        bet=self.bet_sys.current_bet(); step=self.bet_sys.step+1
        ci="🔴" if self.bet_color=="ROJO" else "⚫️"
        pp=int(round(up*100))
        rec=" 🔄" if self.recovery_active else ""
        return (
            f"☑️☑️ <b>SEÑAL CONFIRMADA</b> ☑️☑️\n\n"
            f"🎰 <b>Juego: {self.name}</b>\n"
            f"👉 <b>Después de: {trigger} {ci}</b>\n"
            f"🎯 <b>Apostar a: {self.bet_color}</b> {ci}{rec}\n\n"
            f"🤖 <b>Probabilidad Unificada: {pp}%</b>\n"
            f"🌀 <i>D'Alembert paso {step} de 20</i>\n"
            f"📍 <i>Apuesta: {bet:.2f} usd</i>\n\n"
            f"♻️ <i>Intento {attempt}/{MAX_ATTEMPTS}</i>"
        )

    def _result_caption(self, number, real, won):
        icon=COLOR_EMOJI.get(real,"❓")
        bk=self.bet_sys.bankroll; bks=(f"+{bk:.2f}" if bk>=0 else f"{bk:.2f}")
        seq=build_sequence_line(self.spin_history, self.current_attempt_number)
        mark="✅" if won else "❌"
        return f"{seq}\n\n{mark} <b>RESULTADO: {number}</b> {icon}\n💰 <i>Bankroll Actual: {bks} usd</i>"

    def _send_signal(self, trigger, attempt):
        self.signal_is_level1=(self.bet_sys.step==0 and not self.recovery_active)
        if self.signal_is_level1: self.level1_bankroll=self.bet_sys.bankroll
        up=self._uni_prob(self.bet_color,trigger)
        cap=self._signal_caption(trigger,attempt,up)
        chart=self._chart(up=up)
        mid=tg_send_photo(self.chat_id,self.thread_id,chart,cap)
        if mid: self.signal_msg_ids.append(mid)
        logger.info(f"[{self.name}] Signal att={attempt} {self.bet_color} after={trigger} p={up:.2f}")

    def _send_waiting(self, att):
        for mid in self.signal_msg_ids: tg_delete(self.chat_id,mid)
        self.signal_msg_ids=[]
        if self.waiting_msg_id: tg_delete(self.chat_id,self.waiting_msg_id); self.waiting_msg_id=None
        ord_s="2°" if att==2 else "3°"
        cap=(f"⚠️ <b>Esperando condiciones para el {ord_s} intento</b>\n\n"
             f"🎰 <b>{self.name}</b>\n🔍 <i>Analizando 🔴 y ⚫️ cada giro...</i>")
        mid=tg_send_photo(self.chat_id,self.thread_id,self._chart(),cap)
        if mid: self.waiting_msg_id=mid
        logger.info(f"[{self.name}] Waiting att={att}")

    def _send_result(self, number, real, won):
        # Borrar TODOS los mensajes de señal del chat
        for mid in self.signal_msg_ids: tg_delete(self.chat_id,mid)
        self.signal_msg_ids=[]
        if self.waiting_msg_id: tg_delete(self.chat_id,self.waiting_msg_id); self.waiting_msg_id=None
        cap=self._result_caption(number,real,won)
        tg_send_photo(self.chat_id,self.thread_id,self._chart(),cap)
        logger.info(f"[{self.name}] Result {'WIN' if won else 'LOSS'} #{number} bk={self.bet_sys.bankroll:.2f}")

    def _check_stats(self):
        if not self.stats.should_send_stats(): return
        bk=self.bet_sys.bankroll
        b20=self.stats.batch_stats_text(bk); self.stats.mark_stats_sent(bk)
        b24=self.stats.h24_stats_text(bk)
        text=(f"👉🏼 <b>ESTADÍSTICAS 20 SEÑALES</b>\n{b20}\n\n"
              f"👉🏼 <b>ESTADÍSTICAS 24 HORAS</b>\n{b24}")
        tg_send_text(self.chat_id,self.thread_id,text)

    # ── Proceso principal ─────────────────────────────────────────────────────
    def process_number(self, number):
        real=REAL_COLOR_MAP.get(number,"VERDE")
        self.spin_history.append({"number":number,"real":real})
        if len(self.spin_history)>300: self.spin_history.pop(0)

        lo=self.original_levels[-1] if self.original_levels else 0
        li2=self.inverted_levels[-1] if self.inverted_levels else 0
        if number==0:
            p=self.last_nonzero_color
            self.original_levels.append(lo+(1 if p=="ROJO" else(-1 if p=="NEGRO" else 0)))
            self.inverted_levels.append(li2+(1 if p=="NEGRO" else(-1 if p=="ROJO" else 0)))
        else:
            self.original_levels.append(lo+(1 if real=="ROJO" else -1))
            self.inverted_levels.append(li2+(1 if real=="NEGRO" else -1))
            self.last_nonzero_color=real

        while len(self.original_levels)>len(self.spin_history): self.original_levels.pop(0)
        while len(self.inverted_levels)>len(self.spin_history): self.inverted_levels.pop(0)
        ml=min(len(self.original_levels),len(self.inverted_levels))
        self.original_levels=self.original_levels[-ml:]; self.inverted_levels=self.inverted_levels[-ml:]

        self._upd_amx(real)
        es=self.get_signal(number); self.amx_system.update_streak(real,es)
        self.markov.update(self.spin_history); self.ml_predictor.add_spin(self.spin_history)

        # ════════════ MÁQUINA DE ESTADOS ═══════════════════════════
        if self.signal_active:
            is_win=((self.bet_color=="ROJO" and real=="ROJO") or
                    (self.bet_color=="NEGRO" and real=="NEGRO"))
            if is_win:
                bet=self.bet_sys.win()
                mp=self.markov.predict(self.spin_history); ml2=self.ml_predictor.predict(self.spin_history)
                self.ensemble.register_outcome(self.bet_color,real,mp,ml2,self.get_prob(number,self.bet_color))
                self.stats.record(True,self.bet_sys.bankroll,self.current_attempt_number)
                self.signal_active=False; self._check_recovery()
                self._send_result(number,real,True); self._check_stats()
            else:
                self.attempts_left-=1; bet=self.bet_sys.loss()
                if self.attempts_left<=0:
                    mp=self.markov.predict(self.spin_history); ml2=self.ml_predictor.predict(self.spin_history)
                    self.ensemble.register_outcome(self.bet_color,real,mp,ml2,self.get_prob(number,self.bet_color))
                    self.consec_losses+=1
                    if self.consec_losses>=10:
                        self.consec_losses=0; self.recovery_active=False; self.recovery_target=0.0
                    else:
                        self.recovery_active=True; self.recovery_target=self.level1_bankroll+BASE_BET
                    self.stats.record(False,self.bet_sys.bankroll,self.current_attempt_number)
                    self.signal_active=False; self._send_result(number,real,False); self._check_stats()
                else:
                    att=MAX_ATTEMPTS-self.attempts_left+1; self.current_attempt_number=att
                    if real=="VERDE":
                        self.signal_active=False; self.waiting_for_attempt=True
                        self.waiting_attempt_number=att; self.skip_one_after_zero=True
                        self._send_waiting(att); return
                    chosen=self._best_retry_color(number)
                    if chosen:
                        self.bet_color=chosen; self.trigger_number=number
                        self._send_signal(number,att)
                    else:
                        self.signal_active=False; self.waiting_for_attempt=True
                        self.waiting_attempt_number=att; self._send_waiting(att)

        elif self.waiting_for_attempt:
            if real=="VERDE": self.skip_one_after_zero=True; return
            if self.skip_one_after_zero: self.skip_one_after_zero=False; return
            att=self.waiting_attempt_number; chosen=self._best_retry_color(number)
            if chosen:
                if self.waiting_msg_id: tg_delete(self.chat_id,self.waiting_msg_id); self.waiting_msg_id=None
                self.bet_color=chosen; self.trigger_number=number
                self.signal_active=True; self.waiting_for_attempt=False
                self.current_attempt_number=att; self._send_signal(number,att)

        else:
            self.signal_msg_ids=[]
            signal=self._detect_amx()
            if signal:
                c=signal["expected_color"]; p=self._uni_prob(c,signal["trigger_number"])
                if p<self._dyn_thr(c): signal=None
            if signal:
                self.signal_active=True; self.expected_color=signal["expected_color"]
                self.bet_color=signal["expected_color"]; self.attempts_left=MAX_ATTEMPTS
                self.total_attempts=MAX_ATTEMPTS; self.trigger_number=signal["trigger_number"]
                self.current_attempt_number=1; self._send_signal(signal["trigger_number"],1)
                self.amx_system.register_signal_sent()
            else:
                exp=self.should_activate()
                if exp:
                    p=self._uni_prob(exp,self.spin_history[-1]["number"])
                    if p>=self._dyn_thr(exp):
                        self.signal_active=True; self.expected_color=exp
                        self.bet_color=self.determine_bet_color(exp)
                        self.attempts_left=MAX_ATTEMPTS; self.total_attempts=MAX_ATTEMPTS
                        self.trigger_number=number; self.current_attempt_number=1
                        self._send_signal(number,1)

    # ── WebSocket ─────────────────────────────────────────────────────────────
    async def run_ws(self):
        rd=5
        while self.running:
            try:
                async with websockets.connect(WS_URL,ping_interval=30,ping_timeout=60,close_timeout=10) as ws:
                    self.ws=ws; rd=5; logger.info(f"[{self.name}] WS connected")
                    await ws.send(json.dumps({"type":"subscribe","casinoId":CASINO_ID,
                                              "currency":"USD","key":[self.ws_key]}))
                    async for msg in ws:
                        if not self.running: break
                        try: data=json.loads(msg)
                        except: continue
                        if "last20Results" in data and isinstance(data["last20Results"],list):
                            tmp=[]
                            for r in data["last20Results"]:
                                gid=r.get("gameId"); num=r.get("result")
                                if gid and num is not None:
                                    try: n=int(num)
                                    except: continue
                                    if 0<=n<=36 and gid not in self.anti_block:
                                        tmp.append((gid,n))
                                        if len(self.anti_block)>1000: self.anti_block.clear()
                                        self.anti_block.add(gid)
                            for _,n in reversed(tmp): self.process_number(n)
                        gid=data.get("gameId"); res=data.get("result")
                        if gid and res is not None:
                            try: n=int(res)
                            except: continue
                            if 0<=n<=36 and gid not in self.anti_block:
                                if len(self.anti_block)>1000: self.anti_block.clear()
                                self.anti_block.add(gid); self.process_number(n)
            except Exception as e:
                logger.warning(f"[{self.name}] WS err: {e}. Reconnect in {rd}s")
                await asyncio.sleep(rd); rd=min(rd*2,60)

# ─── FLASK ────────────────────────────────────────────────────────────────────
app=Flask(__name__)
@app.route("/")
def index(): return jsonify({"status":"ok","bot":"AMX V20.3","ts":time.time()})
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
            with urllib.request.urlopen(f"{url}/ping",timeout=10) as r:
                logger.info(f"Self-ping OK: {r.status}")
        except Exception as e: logger.warning(f"Self-ping failed: {e}")

# ─── COMANDOS TELEGRAM ────────────────────────────────────────────────────────
engines: dict = {}

@bot.message_handler(commands=['start','help'])
def cmd_start(msg):
    bot.reply_to(msg,(
        "<b>🎰 Roulette Bot - Sistema AMX V20.3</b>\n\n"
        "<b>Novedades V3:</b>\n"
        "• Probabilidad Unificada: Markov+ML+Tabla+EMA+S/R\n"
        "• Pesos adaptativos por backtesting online\n"
        "• Umbral dinámico por volatilidad y rachas\n"
        "• Gráfico eje Y escalado a resultados recientes\n"
        "• Resultado con gráfico + secuencia de emojis\n"
        "• Estadísticas por intento (1°/2°/3°/L)\n\n"
        "/moderado · /tendencia · /status · /reset · /help"
    ),parse_mode="HTML")

@bot.message_handler(commands=['moderado'])
def cmd_moderado(msg):
    ch=[n for n,e in engines.items() if e.amx_system.mode!="moderado" or (e.set_mode("moderado") or True)]
    for e in engines.values(): e.set_mode("moderado")
    bot.reply_to(msg,"✅ <b>Modo MODERADO activado</b>",parse_mode="HTML")

@bot.message_handler(commands=['tendencia'])
def cmd_tendencia(msg):
    for e in engines.values(): e.set_mode("tendencia")
    bot.reply_to(msg,"📈 <b>Modo TENDENCIA activado</b>",parse_mode="HTML")

@bot.message_handler(commands=['status'])
def cmd_status(msg):
    lines=["<b>📊 ESTADO</b>\n"]
    for name,engine in engines.items():
        mi="📈" if engine.amx_system.mode=="tendencia" else "📊"
        w={k:round(v*100,1) for k,v in engine.ensemble.weights.items()}
        if engine.signal_active:
            up=engine._uni_prob(engine.bet_color,engine.trigger_number or 0)
            thr=engine._dyn_thr(engine.bet_color)
            st=f"🟢 {engine.bet_color} att={engine.current_attempt_number}/{MAX_ATTEMPTS} p={up:.2f} thr={thr:.2f}"
        elif engine.waiting_for_attempt:
            st=f"⏳ Esperando intento {engine.waiting_attempt_number}/{MAX_ATTEMPTS}"
        else: st="⚪ Idle"
        lines.append(f"<b>{name}</b>: {mi} — {st}\n"
                     f"  <i>Pesos: Mk={w['markov']}% ML={w['ml']}% Tb={w['table']}%</i>")
    bot.reply_to(msg,"\n".join(lines),parse_mode="HTML")

@bot.message_handler(commands=['reset'])
def cmd_reset(msg):
    for e in engines.values(): e.stats=Stats(); e.ensemble=AdaptiveEnsemble()
    bot.reply_to(msg,"🔄 <b>Estadísticas y pesos reseteados</b>",parse_mode="HTML")

# ─── MAIN ─────────────────────────────────────────────────────────────────────
def run_flask():
    port=int(os.environ.get("PORT",10000))
    app.run(host="0.0.0.0",port=port,debug=False,use_reloader=False)

async def main():
    global engines
    engines={n:RouletteEngine(n,c) for n,c in ROULETTE_CONFIGS.items()}
    tasks=[asyncio.create_task(e.run_ws()) for e in engines.values()]
    tasks.append(asyncio.create_task(self_ping_loop()))
    threading.Thread(target=lambda:bot.polling(none_stop=True,interval=1,timeout=30),daemon=True).start()
    logger.info("🎰 Roulette Bot AMX V20.3 iniciado")
    await asyncio.gather(*tasks)

if __name__=="__main__":
    threading.Thread(target=run_flask,daemon=True).start()
    logger.info("Flask started.")
    try: asyncio.run(main())
    except KeyboardInterrupt: logger.info("Bot stopped.")
