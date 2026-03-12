"""
ボートレース予想Webアプリ - UIメイン（v3）複数会場対応
【v3 追加機能】
- 推奨買い目を5つ（本命・対抗・穴・注目・参考）まで表示
- 「期待値スコア」→ 的中確率(%)・公正オッズ・実オッズ・期待値 に変更
- 3連単オッズをリアルタイム取得して各買い目カードに表示
- 期待値 > 1.0 の買い目は強調表示（バリュー買い目）
"""
import sys
import os
import json
import pandas as pd
import numpy as np
import streamlit as st
import plotly.graph_objects as go
from datetime import date, datetime, timezone, timedelta
import time
import re
import uuid as _uuid_mod
from streamlit.components.v1 import html as _st_html
from concurrent.futures import ThreadPoolExecutor, as_completed

APP_VERSION = "v3.1-20260304"

_JST_TZ = timezone(timedelta(hours=9))
def _today_jst():
    return datetime.now(_JST_TZ).date()

sys.path.insert(0, os.path.dirname(__file__))
import config as _cfg
from config import VENUE_CONFIGS, get_venue_config
from race_scraper import (
    fetch_full_race_data, fetch_race_card,
    fetch_base_race_data, apply_extended_data, fetch_extended_player_data,
    fetch_deadline, fetch_odds_3t, fetch_odds_2tf, fetch_gamagori_taka,
    fetch_racer_kimarite, fetch_race_result, fetch_lady_racers,
    set_thread_venue,
)
from scorer import predict, get_wind_type
from result_tracker import save_prediction

# ── 結果キャッシュ（セッション喪失対策）──────────────────────────
_CACHE_DIR = os.path.join(os.path.dirname(__file__), "_cache")
os.makedirs(_CACHE_DIR, exist_ok=True)

# ── 実行時間の記録・推定 ─────────────────────────────────────────
_EXEC_TIMES_PATH = os.path.join(_CACHE_DIR, "execution_times.json")
_DEFAULT_TOTAL = 7.0  # 初回用デフォルト合計秒数

def _load_avg_total():
    try:
        with open(_EXEC_TIMES_PATH, "r") as f:
            history = json.load(f)
        if not history:
            return _DEFAULT_TOTAL
        return sum(history) / len(history)
    except (FileNotFoundError, json.JSONDecodeError):
        return _DEFAULT_TOTAL

def _save_exec_total(total_sec: float):
    try:
        with open(_EXEC_TIMES_PATH, "r") as f:
            history = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        history = []
    history.append(total_sec)
    history = history[-10:]
    with open(_EXEC_TIMES_PATH, "w") as f:
        json.dump(history, f)

def _fmt_remaining(sec):
    sec = max(0, round(sec))
    if sec < 60:
        return f"残り約{sec}秒"
    return f"残り約{sec // 60}分{sec % 60}秒"

def _cache_path(race_no, d_str, device_id=""):
    if device_id:
        return os.path.join(_CACHE_DIR, f"result_{d_str}_{race_no}_{device_id}.json")
    return os.path.join(_CACHE_DIR, f"result_{d_str}_{race_no}.json")

def _save_result_cache(race_no, d_str, result, weather, deadline, odds, odds_2t, taka, racer_km, race_result_data, lady_racers=None, device_id=""):
    """予想結果をファイルにキャッシュ（セッション切断対策）"""
    try:
        # scored_dfをJSON化可能に変換
        scored_list = result["scored_df"].to_dict(orient="records") if hasattr(result["scored_df"], "to_dict") else []
        scored_cols = list(result["scored_df"].columns) if hasattr(result["scored_df"], "columns") else []
        cache = {
            "scored_list": scored_list,
            "scored_cols": scored_cols,
            "recommendations": result.get("recommendations", []),
            "all_3t_candidates": result.get("all_3t_candidates", []),
            "focus_formation": result.get("focus_formation", {}),
            "tenkai_scenarios": result.get("tenkai_scenarios", []),
            "weather": weather,
            "deadline": deadline,
            "odds": odds,
            "odds_2t": odds_2t,
            "taka": taka,
            "racer_km": racer_km,
            "race_result": race_result_data,
            "race_no": race_no,
            "date_str": d_str,
            "lady_racers": list(lady_racers) if lady_racers else [],
        }
        with open(_cache_path(race_no, d_str, device_id), "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, default=str)
    except Exception:
        pass

def _load_result_cache(race_no, d_str, device_id=""):
    """キャッシュから予想結果を復元"""
    try:
        path = _cache_path(race_no, d_str, device_id)
        if not os.path.exists(path):
            return None
        with open(path, "r", encoding="utf-8") as f:
            cache = json.load(f)
        # scored_dfを復元
        scored_df = pd.DataFrame(cache["scored_list"], columns=cache["scored_cols"])
        result = {
            "scored_df": scored_df,
            "recommendations": cache.get("recommendations", []),
            "all_3t_candidates": cache.get("all_3t_candidates", []),
            "focus_formation": cache.get("focus_formation", {}),
            "tenkai_scenarios": cache.get("tenkai_scenarios", []),
        }
        return {
            "result": result,
            "weather": cache.get("weather"),
            "deadline": cache.get("deadline"),
            "odds": cache.get("odds"),
            "odds_2t": cache.get("odds_2t"),
            "taka": cache.get("taka"),
            "racer_km": cache.get("racer_km"),
            "race_result": cache.get("race_result"),
            "lady_racers": set(cache.get("lady_racers", [])),
        }
    except Exception:
        return None

st.set_page_config(page_title="競艇予想AI マクリザサ", page_icon="🔱", layout="centered", initial_sidebar_state="collapsed")

# ── デバイスID（localStorage で端末識別）────────────────────────────
# query_params に did が無い場合、JSで localStorage から取得/生成して
# query_params に付与 → 次回アクセス時に Python 側に渡る
_device_id = st.query_params.get("did", "")
if not _device_id:
    _new_did = _uuid_mod.uuid4().hex[:12]
    _device_id = _new_did  # JS成功まではフォールバック値を使用
    _st_html(f"""<script>
    (function(){{
        var k='_layerdon_did';
        var did=localStorage.getItem(k);
        if(!did){{ did='{_new_did}'; localStorage.setItem(k,did); }}
        var u=new URL(window.location);
        if(u.searchParams.get('did')!==did){{
            u.searchParams.set('did',did);
            window.location.replace(u.toString());
        }}
    }})();
    </script>""", height=0)

# ── 会場選択ページ ────────────────────────────────────────────────
# query_params に venue が無い場合、会場選択画面を表示してメインに進まない
if "venue" not in st.query_params:
    st.markdown("""
    <style>
        section[data-testid="stSidebar"],
        button[data-testid="stSidebarCollapsedControl"],
        button[data-testid="collapsedControl"] { display: none !important; }
    </style>
    """, unsafe_allow_html=True)

    st.markdown('''<div style="background:linear-gradient(135deg,#060e1f 0%,#0d2855 40%,#1a3a6b 70%,#0d2855 100%);
        padding:1.5rem;border-radius:12px;border:1px solid #1e5fa8;margin-top:2.5rem;text-align:center">
        <h1 style="color:#e8f4ff;font-size:1.3rem;letter-spacing:2px;margin:0">競艇予想AI マクリザサ</h1>
        <div style="color:#5a9fd4;font-size:0.65rem;letter-spacing:4px;margin-top:4px">― BOATRACE AI ―</div>
    </div>''', unsafe_allow_html=True)

    st.markdown("")
    st.markdown("##### レース場を選択してください")
    st.markdown("")

    st.markdown('<style>[data-testid="stButton"] button[kind="primary"] { padding: 2.5rem 1rem; font-size: 1.1rem; color: #0a1628 !important; font-weight: bold; }</style>', unsafe_allow_html=True)
    _venue_list = list(VENUE_CONFIGS.items())
    # 2列ずつ表示（狭い画面でも全会場が見えるように）
    for row_start in range(0, len(_venue_list), 2):
        row_items = _venue_list[row_start:row_start + 2]
        _vs_cols = st.columns(2)
        for i, (code, vcfg) in enumerate(row_items):
            with _vs_cols[i]:
                if st.button(f'{vcfg["short_name"]}\n{vcfg["en_name"]}', key=f"venue_sel_{code}", use_container_width=True, type="primary"):
                    st.query_params["venue"] = code
                    st.rerun()
    st.stop()

st.markdown("""
<style>
    /* ── ツールバー・フッター非表示 ── */
    header[data-testid="stHeader"] { display: none !important; }
    footer { display: none !important; visibility: hidden !important; height: 0 !important; }
    footer a { display: none !important; }
    [data-testid="stDecoration"] { display: none !important; }
    [data-testid="stStatusWidget"] { display: none !important; }
    .viewerBadge_container__r5tak { display: none !important; }
    .stApp > footer { display: none !important; }
    #MainMenu { display: none !important; }
    div[class*="StatusWidget"] { display: none !important; }
    a[href*="streamlit.io"] { display: none !important; }
    /* ── pills改行制御 ── */
    .st-key-race_no_pills_wrap div[data-testid="stPills"] > div { flex-wrap: wrap !important; }
    .st-key-race_no_pills_wrap div[data-testid="stPills"] > div > div:nth-child(6) { margin-right: 100% !important; }
    .st-key-race_no_pills_wrap div[data-testid="stPills"] > div > button:nth-child(6) { margin-right: 100% !important; }
    .st-key-date_pills_wrap div[data-testid="stPills"] > div { flex-wrap: wrap !important; }
    .st-key-date_pills_wrap div[data-testid="stPills"] > div > div:nth-child(3) { margin-right: 100% !important; }
    .st-key-date_pills_wrap div[data-testid="stPills"] > div > div:nth-child(4) { margin-right: 100% !important; }
    .st-key-date_pills_wrap div[data-testid="stPills"] > div > button:nth-child(3) { margin-right: 100% !important; }
    .st-key-date_pills_wrap div[data-testid="stPills"] > div > button:nth-child(4) { margin-right: 100% !important; }
    /* ── ベーススタイル ─────────────────────────────────── */
    .main-header { background: linear-gradient(135deg, #060e1f 0%, #0d2855 40%, #1a3a6b 70%, #0d2855 100%); padding: 1.2rem 1.2rem 1rem; border-radius: 12px; margin-bottom: 0.8rem; border: 1px solid #1e5fa8; margin-top: 2.5rem; position: relative; overflow: hidden; }
    .main-header::before { content: ''; position: absolute; top: 0; left: 0; right: 0; bottom: 0; background: radial-gradient(ellipse at 20% 80%, rgba(30,95,168,0.15) 0%, transparent 60%), radial-gradient(ellipse at 80% 20%, rgba(100,180,255,0.08) 0%, transparent 50%); pointer-events: none; }
    .main-header .logo-row { display: flex; align-items: center; gap: 12px; position: relative; z-index: 1; }
    .main-header .logo-icon { flex-shrink: 0; }
    .main-header h1 { color: #e8f4ff; margin: 0; font-size: 1.3rem; letter-spacing: 2px; }
    .main-header .logo-sub { color: #5a9fd4; font-size: 0.65rem; letter-spacing: 4px; text-transform: uppercase; margin-top: 2px; font-weight: 600; }
    .main-header .logo-wave { position: absolute; bottom: 0; left: 0; right: 0; height: 20px; z-index: 0; opacity: 0.15; }
    .bet-box { background: #1a2744; border-left: 5px solid #f0a500; padding: 0.9rem 1rem; margin: 0.4rem 0; border-radius: 6px; }
    .bet-box-value { background: #12301a; border-left: 5px solid #2ecc71; padding: 0.9rem 1rem; margin: 0.4rem 0; border-radius: 6px; }
    .bet-box-sub { background: #151f35; border-left: 3px solid #7ab8e8; padding: 0.9rem 1rem; margin: 0.4rem 0; border-radius: 6px; }
    .bet-combo { font-size: 1.8rem; font-weight: bold; color: #fff; letter-spacing: 3px; }
    .bet-combo-sub { font-size: 1.4rem; font-weight: bold; color: #cce0ff; letter-spacing: 2px; }
    .bet-label { color: #7ab8e8; font-size: 0.78rem; margin-top: 0.5rem; }
    .bet-value { color: #fff; font-size: 0.9rem; margin-bottom: 0.1rem; }
    .ev-positive { color: #2ecc71; font-weight: bold; }
    .ev-negative { color: #aaa; }
    .deadline-box { background: #1a2744; border: 1px solid #1e5fa8; border-radius: 8px; padding: 0.6rem 1rem; display: inline-block; margin-bottom: 0.8rem; }
    .deadline-urgent { border-color: #e05c5c !important; background: #2a1a1a !important; }
    .deadline-label { color: #7ab8e8; font-size: 0.8rem; margin-bottom: 0.1rem; }
    .deadline-time { font-size: 1.4rem; font-weight: bold; color: #fff; }
    .deadline-remaining { font-size: 0.85rem; margin-top: 0.2rem; }
    .deadline-remaining.urgent { color: #e05c5c; font-weight: bold; }
    .deadline-remaining.ok { color: #7ab8e8; }
    .stat-box { background: #1a2744; border: 1px solid #2a4a80; border-radius: 8px; padding: 0.8rem; margin: 0.3rem 0; }
    .odds-tag { display: inline-block; background: #0d3360; border-radius: 4px; padding: 1px 6px; font-size: 0.75rem; color: #7ab8e8; margin-right: 4px; }

    /* ── 天気グリッド（モバイル用3x2） ──────────────────── */
    .weather-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 6px; margin: 0.5rem 0; }
    .weather-item { background: #1a2744; border: 1px solid #2a4a80; border-radius: 8px; padding: 8px 6px; text-align: center; }
    .weather-item .label { color: #7ab8e8; font-size: 0.7rem; margin-bottom: 2px; }
    .weather-item .value { color: #fff; font-size: 1.05rem; font-weight: bold; }

    /* ── 買い目グループヘッダー ─────────────────────────── */
    .group-header { text-align: center; font-size: 1rem; font-weight: bold; padding-bottom: 4px; margin-bottom: 6px; }

    /* ── モバイルでもカラムを横並び維持 ──────────────────── */
    @media (max-width: 768px) {
        [data-testid="stHorizontalBlock"] {
            flex-wrap: nowrap !important;
            gap: 0.3rem !important;
        }
        [data-testid="stHorizontalBlock"] > [data-testid="stColumn"] {
            min-width: 0 !important;
            width: auto !important;
            flex: 1 1 0 !important;
        }
    }

    /* ── サイドバー非表示（設定はメインエリアのexpanderに移動済み） */
    section[data-testid="stSidebar"],
    button[data-testid="stSidebarCollapsedControl"],
    button[data-testid="collapsedControl"] { display: none !important; }

    /* ── モバイル最適化 ─────────────────────────────────── */
    @media (max-width: 768px) {
        /* Streamlitの余白を縮小 */
        .stMainBlockContainer { padding: 0.5rem 0.8rem !important; }

        /* 見出しサイズ調整 */
        .main-header h1 { font-size: 1.1rem; letter-spacing: 1px; }
        .main-header .logo-sub { font-size: 0.58rem; letter-spacing: 3px; }
        .main-header .logo-icon svg { width: 38px; height: 38px; }
        h3 { font-size: 0.95rem !important; }

        /* ボタンを押しやすく */
        .stButton > button { min-height: 48px !important; font-size: 1rem !important; }
        .stSelectbox, .stDateInput { font-size: 1rem !important; }

        /* データテーブルの横スクロール */
        .stDataFrame { overflow-x: auto !important;  }
        .stDataFrame table { font-size: 0.75rem !important; }
        .stDataFrame th, .stDataFrame td { padding: 4px 6px !important; white-space: nowrap !important; }

        /* Plotlyチャート高さ調整 */
        .stPlotlyChart { max-height: 320px; }

        /* 三連単オッズ一覧：モバイル横スクロール防止 */
        .odds-3t-wrap table th { font-size: 0.5rem !important; padding: 1px 1px !important; }
        .odds-3t-wrap table td { font-size: 0.5rem !important; padding: 1px 1px !important; }
        .odds-3t-wrap table td[style*="width:14px"] { width: 12px !important; min-width: 12px !important; }

        /* 展示データ：モバイル横スクロール防止 */
        .exhibit-wrap table th { font-size: 0.68rem !important; padding: 5px 3px !important; }
        .exhibit-wrap table td { font-size: 0.7rem !important; padding: 4px 3px !important; }

        /* 三連単予想：モバイル横スクロール防止 */
        .sanrentan-wrap table th { font-size: 0.7rem !important; padding: 3px 2px !important; }
        .sanrentan-wrap table td { font-size: 0.75rem !important; padding: 3px 2px !important; }
        .sanrentan-wrap table td .frame-badge { width: 18px !important; height: 18px !important; min-width: 18px !important; line-height: 18px !important; font-size: 0.7rem !important; }
    }
</style>
""", unsafe_allow_html=True)

st.markdown('''<div class="main-header">
  <div class="logo-row">
    <div class="logo-icon">
      <svg width="50" height="50" viewBox="0 0 100 100" xmlns="http://www.w3.org/2000/svg">
        <defs>
          <linearGradient id="trident-grad" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stop-color="#ffd700"/>
            <stop offset="100%" stop-color="#b8860b"/>
          </linearGradient>
          <linearGradient id="wave-grad" x1="0" y1="0" x2="1" y2="0">
            <stop offset="0%" stop-color="#4aa3ff" stop-opacity="0.9"/>
            <stop offset="100%" stop-color="#06b6d4" stop-opacity="0.6"/>
          </linearGradient>
          <radialGradient id="glow" cx="50%" cy="35%" r="45%">
            <stop offset="0%" stop-color="#ffd700" stop-opacity="0.2"/>
            <stop offset="100%" stop-color="#4aa3ff" stop-opacity="0"/>
          </radialGradient>
          <filter id="gold-glow">
            <feGaussianBlur stdDeviation="1.5" result="blur"/>
            <feMerge><feMergeNode in="blur"/><feMergeNode in="SourceGraphic"/></feMerge>
          </filter>
        </defs>
        <!-- 背景グロー -->
        <circle cx="50" cy="40" r="38" fill="url(#glow)"/>
        <!-- トライデント(三叉の槍) -->
        <g filter="url(#gold-glow)">
          <!-- 柄 -->
          <rect x="47" y="28" width="6" height="48" rx="2" fill="url(#trident-grad)"/>
          <!-- 中央の刃 -->
          <path d="M50 6 L45 24 L50 20 L55 24 Z" fill="url(#trident-grad)"/>
          <!-- 左の刃 -->
          <path d="M32 22 L34 18 L38 28 L42 26 L40 32 L47 30 L47 28 L38 30 L36 24 Z" fill="url(#trident-grad)"/>
          <!-- 右の刃 -->
          <path d="M68 22 L66 18 L62 28 L58 26 L60 32 L53 30 L53 28 L62 30 L64 24 Z" fill="url(#trident-grad)"/>
          <!-- 横棒 -->
          <rect x="38" y="28" width="24" height="4" rx="1.5" fill="url(#trident-grad)"/>
        </g>
        <!-- 波 -->
        <path d="M5 78 Q18 70, 32 78 Q46 86, 60 78 Q74 70, 88 78 Q96 83, 100 80" stroke="url(#wave-grad)" stroke-width="3" fill="none" opacity="0.8"/>
        <path d="M0 85 Q16 79, 34 85 Q52 91, 68 85 Q84 79, 100 85" stroke="#4aa3ff" stroke-width="2" fill="none" opacity="0.4"/>
        <path d="M8 92 Q28 87, 48 92 Q68 97, 88 92" stroke="#06b6d4" stroke-width="1.2" fill="none" opacity="0.25"/>
      </svg>
    </div>
    <div>
      <h1>競艇予想AI マクリザサ</h1>
      <div class="logo-sub">― BOATRACE AI ―</div>
    </div>
  </div>
  <svg class="logo-wave" viewBox="0 0 1200 30" preserveAspectRatio="none" xmlns="http://www.w3.org/2000/svg">
    <path d="M0 15 Q150 0, 300 15 Q450 30, 600 15 Q750 0, 900 15 Q1050 30, 1200 15 L1200 30 L0 30 Z" fill="#4aa3ff"/>
  </svg>
</div>''', unsafe_allow_html=True)

# ── モード切替（予想 / 出走表一覧）────────────────────────────────
st.markdown(
    """<style>
    div[data-testid="stRadio"] > div[role="radiogroup"] {
        flex-wrap: nowrap;
    }
    div[data-testid="stRadio"] > div[role="radiogroup"] > label {
        padding: 4px 16px; font-size: 0.9rem; white-space: nowrap;
    }
    </style>""",
    unsafe_allow_html=True,
)
if "running" not in st.session_state:
    st.session_state.running = False
_ui_disabled = st.session_state.running

# ── 会場設定（query_params から読み取り）─────────────────────────
_selected_venue_code = st.query_params.get("venue", "07")
set_thread_venue(_selected_venue_code)
_venue = get_venue_config(_selected_venue_code)

# ── 会場名 + 変更ボタン + モード切替 ──────────────────────────────
_hdr_cols = st.columns([3, 1])
with _hdr_cols[0]:
    st.markdown(
        f'<div style="font-size:1rem;font-weight:bold;color:#7ab8e8;padding:6px 0">'
        f'{_venue["short_name"]} ボートレース</div>',
        unsafe_allow_html=True,
    )
with _hdr_cols[1]:
    if st.button("会場変更", disabled=_ui_disabled, use_container_width=True):
        del st.query_params["venue"]
        st.rerun()

app_mode = st.pills(
    "モード", ["予想", "出走表一覧"],
    label_visibility="collapsed",
    key="app_mode",
    disabled=_ui_disabled,
    default="予想",
)

if not app_mode:
    app_mode = "予想"

if app_mode == "予想":
    # ══════════════════════════════════════════════════════════════
    #  予想モード（既存機能）
    # ══════════════════════════════════════════════════════════════

    # ── セッション初期化 ──────────────────────────────────────────────
    for key in ("result", "weather", "deadline", "race_no", "date_str", "odds", "odds_2t", "taka", "racer_km", "race_result", "lady_racers"):
        if key not in st.session_state:
            st.session_state[key] = None
    if "odds_last_refresh_time" not in st.session_state:
        st.session_state.odds_last_refresh_time = 0.0

    # ── レース間ナビゲーション（← 前R / 次R →）─────────────────────
    if "nav_race" not in st.session_state:
        st.session_state.nav_race = None  # None=通常, int=自動実行するレース番号

    def _go_prev_race():
        rno = st.session_state.race_no or 1
        if rno > 1:
            st.session_state.nav_race = rno - 1
            st.session_state.result = None  # 結果をクリアして再実行トリガー
            st.session_state.running = True
            st.session_state.show_settings = False

    def _go_next_race():
        rno = st.session_state.race_no or 1
        if rno < 12:
            st.session_state.nav_race = rno + 1
            st.session_state.result = None
            st.session_state.running = True
            st.session_state.show_settings = False

    # ── セッション喪失時のキャッシュ復元 ─────────────────────────────
    # Google Appなどのインアプリブラウザでセッションが切断された場合、
    # query_paramsのrace番号と日付からキャッシュを復元する
    if st.session_state.result is None and st.session_state.nav_race is None:
        _restore_race = st.query_params.get("race", "1")
        _restore_date = st.query_params.get("d") or _today_jst().strftime("%Y%m%d")
        _cached = _load_result_cache(_restore_race, _restore_date, _device_id)
        if _cached:
            st.session_state.result      = _cached["result"]
            st.session_state.weather     = _cached["weather"]
            st.session_state.deadline    = _cached["deadline"]
            st.session_state.race_no     = int(_restore_race)
            st.session_state.radio_race_no = int(_restore_race)
            st.session_state.date_str    = _restore_date
            st.session_state.odds        = _cached["odds"]
            st.session_state.odds_2t     = _cached["odds_2t"]
            st.session_state.taka        = _cached["taka"]
            st.session_state.racer_km    = _cached["racer_km"]
            st.session_state.race_result = _cached["race_result"]
            st.session_state.lady_racers = _cached.get("lady_racers", set())

    # ── 設定パネル表示フラグ ──────────────────────────────────────────
    if "show_settings" not in st.session_state:
        st.session_state.show_settings = True  # 初回は開いた状態
    if "_fetch_error" not in st.session_state:
        st.session_state._fetch_error = None

    # ── 停止ボタン用コールバック ────────────────────────────────────
    def _on_stop_click():
        st.session_state.running = False
        st.session_state.show_settings = True
        st.session_state.pop("_exec_race_no", None)
        st.session_state.pop("_exec_race_date", None)
        st.session_state.pop("_ui_flushed", None)

    # ── レース設定パネル（メインエリア） ─────────────────────────────
    # プレースホルダーを使い、予想実行後に即座にたたむ（st.rerun()不要）
    _settings_ph = st.empty()
    _form_ph = st.empty()

    _hide_settings = st.session_state.running or (
        not st.session_state.show_settings and st.session_state.result is not None
    )
    if _hide_settings:
        # 予想実行中 or 予想後で設定非表示: トグルボタンだけ表示
        _form_ph.empty()
        with _settings_ph.container():
            if st.session_state.running:
                _run_rno = st.session_state.get("_exec_race_no") or st.session_state.get("radio_race_no", 1)
                _run_date = st.session_state.get("_exec_race_date") or _today_jst()
                _run_date_str = _run_date.strftime("%Y年%m月%d日") if hasattr(_run_date, "strftime") else str(_run_date)
                st.markdown(
                    f'<div style="background:#1a2744;border:1px solid #1e5fa8;border-radius:8px;'
                    f'padding:0.7rem 1rem;margin-bottom:0.5rem;text-align:center">'
                    f'<span style="color:#7ab8e8;font-size:0.85rem">'
                    f'📡 予想を実行中…</span>'
                    f'<div style="margin-top:0.4rem">'
                    f'<span style="display:inline-block;width:22px;height:22px;'
                    f'border:3px solid rgba(122,184,232,0.3);border-top:3px solid #7ab8e8;'
                    f'border-radius:50%;animation:spin 1s linear infinite"></span></div>'
                    f'</div>'
                    f'<style>@keyframes spin {{from{{transform:rotate(0deg)}}to{{transform:rotate(360deg)}}}}</style>',
                    unsafe_allow_html=True,
                )
                st.button("⏹ 予想を停止", use_container_width=True, key="stop_btn_init",
                          on_click=_on_stop_click)
            else:
                st.button("▸ レース設定を開く", use_container_width=True,
                          on_click=lambda: st.session_state.update(show_settings=True))
        race_no = None
        race_date = None
        fetch_btn = False
    else:
        # 初回 or 設定表示中
        with _settings_ph.container():
            if st.session_state.result is not None:
                # 予想後: 閉じるボタンを表示
                st.button("▾ レース設定を閉じる", use_container_width=True,
                          on_click=lambda: st.session_state.update(show_settings=False))
            else:
                st.markdown(
                    '<div style="background:#1a2744;border:1px solid #1e5fa8;border-radius:8px;'
                    'padding:0.8rem 1rem;margin-bottom:0.5rem">'
                    '<span style="color:#7ab8e8;font-size:0.9rem;font-weight:bold">▾ レース設定</span>'
                    '</div>',
                    unsafe_allow_html=True,
                )
        with _form_ph.container():
            st.markdown(
                """<style>
                div[data-testid="stRadio"] > div[role="radiogroup"] {
                    flex-wrap: wrap;
                }
                div[data-testid="stRadio"] > div[role="radiogroup"] > label {
                    width: calc(25% - 1rem);
                }
                </style>""",
                unsafe_allow_html=True,
            )
            # session_state 初期化（初回のみ query_params から復元）
            if "radio_race_no" not in st.session_state:
                st.session_state.radio_race_no = max(1, min(12, int(st.query_params.get("race", 1))))
            with st.container(key="race_no_pills_wrap"):
                race_no = st.pills("レース番号", list(range(1, 13)), default=st.session_state.radio_race_no, disabled=_ui_disabled, key="radio_race_no_widget", format_func=lambda x: f"{x}R")
            # 選択値を session_state と query_params に反映
            st.session_state.radio_race_no = race_no
            if str(race_no) != st.query_params.get("race"):
                st.query_params["race"] = str(race_no)
            _date_options = [_today_jst() + timedelta(days=i) for i in range(-3, 4)]
            _date_labels = {d: d.strftime("%m/%d") + ("(今日)" if d == _today_jst() else "") for d in _date_options}
            if "pills_race_date" not in st.session_state:
                _d_param = st.query_params.get("d")
                if _d_param:
                    try:
                        st.session_state.pills_race_date = datetime.strptime(_d_param, "%Y%m%d").date()
                    except ValueError:
                        st.session_state.pills_race_date = _today_jst()
                else:
                    st.session_state.pills_race_date = _today_jst()
                if st.session_state.pills_race_date not in _date_options:
                    st.session_state.pills_race_date = _today_jst()
            with st.container(key="date_pills_wrap"):
                race_date = st.pills("開催日", _date_options, disabled=_ui_disabled, key="pills_race_date",
                                     format_func=lambda d: _date_labels[d])
            if race_date:
                _d_val = race_date.strftime("%Y%m%d")
                if _d_val != st.query_params.get("d"):
                    st.query_params["d"] = _d_val
            def _on_fetch_click():
                st.session_state.running = True
                st.session_state.show_settings = False
                st.session_state._exec_race_no = race_no
                st.session_state._exec_race_date = race_date
                st.session_state.result = None
            fetch_btn  = st.button("▶ 予想実行", type="primary", use_container_width=True, disabled=_ui_disabled,
                                   on_click=_on_fetch_click)
            # 前回実行時のエラーメッセージを表示
            if st.session_state._fetch_error:
                st.error(st.session_state._fetch_error)
                st.session_state._fetch_error = None

    # ── 予想実行（ナビゲーション経由の自動実行を含む）───────────────
    _nav_auto = False
    if st.session_state.nav_race is not None:
        race_no = st.session_state.nav_race
        # 日付復元: _exec_race_date → date_str → query_params → today
        _nav_date = st.session_state.get("_exec_race_date")
        if _nav_date is None and st.session_state.get("date_str"):
            try:
                _nav_date = datetime.strptime(st.session_state.date_str, "%Y%m%d").date()
            except (ValueError, TypeError):
                _nav_date = None
        if _nav_date is None:
            _d_qp = st.query_params.get("d")
            if _d_qp:
                try:
                    _nav_date = datetime.strptime(_d_qp, "%Y%m%d").date()
                except ValueError:
                    _nav_date = None
        race_date = _nav_date or _today_jst()
        st.session_state.nav_race = None
        st.session_state.radio_race_no = race_no
        st.session_state._exec_race_date = race_date
        st.query_params["race"] = str(race_no)
        st.query_params["d"] = race_date.strftime("%Y%m%d")
        _nav_auto = True

    # running フラグが立っている場合（設定パネルが非表示でも）予想を実行
    _run_from_state = False
    if st.session_state.running and not fetch_btn and not _nav_auto:
        race_no = st.session_state.get("_exec_race_no") or st.session_state.get("radio_race_no", 1)
        race_date = st.session_state.get("_exec_race_date") or _today_jst()
        _run_from_state = True

    if fetch_btn or _nav_auto or _run_from_state:
        # 設定パネルを即座に折りたたむ → 実行中表示に差し替え
        st.session_state.show_settings = False
        st.session_state.running = True
        _settings_ph.empty()
        _form_ph.empty()
        d_str = race_date.strftime("%Y%m%d")
        _run_date_fmt = race_date.strftime("%Y年%m月%d日") if hasattr(race_date, "strftime") else str(race_date)
        with _settings_ph.container():
            st.markdown(
                f'<div id="running-indicator" style="background:#1a2744;border:1px solid #1e5fa8;border-radius:8px;'
                f'padding:0.7rem 1rem;margin-bottom:0.5rem;text-align:center">'
                f'<span style="color:#7ab8e8;font-size:0.85rem">'
                f'📡 {_run_date_fmt} <b>{race_no}R</b> の予想を実行中…</span>'
                f'<div style="margin-top:0.4rem">'
                f'<span style="display:inline-block;width:22px;height:22px;'
                f'border:3px solid rgba(122,184,232,0.3);border-top:3px solid #7ab8e8;'
                f'border-radius:50%;animation:spin 1s linear infinite"></span></div>'
                f'</div>'
                f'<style>@keyframes spin {{from{{transform:rotate(0deg)}}to{{transform:rotate(360deg)}}}}</style>',
                unsafe_allow_html=True,
            )
            st.button("⏹ 予想を停止", use_container_width=True, key="stop_btn_exec",
                      on_click=_on_stop_click)

        # UI折りたたみをブラウザに反映してから予想を開始する
        if not st.session_state.get("_ui_flushed"):
            st.session_state._ui_flushed = True
            st.rerun()
        st.session_state.pop("_ui_flushed", None)

        # ── 同一レースの再予想キャッシュ判定 ──────────────────────
        # 選手詳細データ（コース別成績・直近成績・決まり手）は同一レースなら不変
        # → session_state にキャッシュして2回目以降は Phase 2 をスキップ
        _race_cache_key = f"_static_cache_{d_str}_{race_no}_{_selected_venue_code}"
        _has_static_cache = (
            _race_cache_key in st.session_state
            and st.session_state[_race_cache_key].get("ext_data")
            and st.session_state[_race_cache_key].get("racer_km") is not None
        )

        _avg_total = _load_avg_total()
        if _has_static_cache:
            _avg_total = _avg_total * 0.5  # キャッシュ利用時は推定時間を短縮
        _t_start = time.time()
        progress_bar = st.progress(0, text=f"⏳ データ取得を開始します...｜{_fmt_remaining(_avg_total)}")

        # ── Phase 1: 独立した全HTTPリクエストを一括並列実行 ──────────
        # fetch_base_race_data は内部で3並列（soup+beforeinfo+gamagori_time）
        # オッズ・結果など変動データは常に再取得
        # 女子選手・高橋アナ予想はキャッシュがあればスキップ
        with ThreadPoolExecutor(max_workers=10, initializer=set_thread_venue, initargs=(_selected_venue_code,)) as executor:
            f_data     = executor.submit(fetch_base_race_data, race_no, d_str)
            f_odds     = executor.submit(fetch_odds_3t, race_no, d_str, _selected_venue_code)
            f_odds_2tf = executor.submit(fetch_odds_2tf, race_no, d_str, _selected_venue_code)
            f_rresult  = executor.submit(fetch_race_result, race_no, d_str)
            # 日次固定データはキャッシュがあればスキップ
            f_taka = None
            if _venue.get("has_taka_yoso"):
                if _has_static_cache and "taka" in st.session_state[_race_cache_key]:
                    f_taka = None  # キャッシュ利用
                else:
                    f_taka = executor.submit(fetch_gamagori_taka, race_no, d_str)
            f_lady = None
            if _has_static_cache and "lady_racers" in st.session_state[_race_cache_key]:
                f_lady = None  # キャッシュ利用
            else:
                f_lady = executor.submit(fetch_lady_racers, d_str)

            phase1_map = {
                f_data:     "出走表・直前データ",
                f_odds:     "3連単オッズ",
                f_odds_2tf: "2連単オッズ",
                f_rresult:  "レース結果",
            }
            if f_lady is not None:
                phase1_map[f_lady] = "女子選手情報"
            if f_taka is not None:
                phase1_map[f_taka] = "高橋アナ予想"
            done_count = 0
            _phase2_tasks = 0 if _has_static_cache else 2
            total_tasks = len(phase1_map) + _phase2_tasks
            for future in as_completed(phase1_map):
                done_count += 1
                name = phase1_map[future]
                pct = int((done_count / total_tasks) * 60) if total_tasks > 0 else 60
                _remaining = max(0, _avg_total - (time.time() - _t_start))
                progress_bar.progress(pct, text=f"⏳ データ取得中... {name} 完了 ({done_count}/{total_tasks})｜{_fmt_remaining(_remaining)}")

            df_raw, weather, racelist_soup = f_data.result()
            odds     = f_odds.result()
            odds_2tf = f_odds_2tf.result()
            taka     = f_taka.result() if f_taka else (st.session_state[_race_cache_key].get("taka", {}) if _has_static_cache else {})
            race_result_data = f_rresult.result()
            lady_racers = f_lady.result() if f_lady else (st.session_state[_race_cache_key].get("lady_racers", set()) if _has_static_cache else set())

        # ── Phase 2: 出走表に依存するリクエストを並列実行 ──────────
        # deadline は soup を再利用（HTTPリクエスト不要）
        deadline = fetch_deadline(race_no, d_str, _soup=racelist_soup)
        if not df_raw.empty and "登録番号" in df_raw.columns:
            reg_nos = df_raw["登録番号"].tolist()
            # 展示進入マッピングを構築（進入コースがあれば枠番→進入コース）
            _course_map = None
            if "進入コース" in df_raw.columns:
                _cm = {}
                for _, _r in df_raw.iterrows():
                    _f = str(_r.get("枠番", ""))
                    _c = _r.get("進入コース")
                    if _f and pd.notna(_c):
                        _cm[_f] = int(_c)
                if _cm and _cm != {str(i): i for i in range(1, 7)}:
                    _course_map = _cm  # 枠番順と異なる場合のみ渡す

            if _has_static_cache:
                # ── キャッシュ利用: Phase 2 スキップ ──────────────
                ext_data = st.session_state[_race_cache_key]["ext_data"]
                racer_km = st.session_state[_race_cache_key]["racer_km"]
                _remaining = max(0, _avg_total - (time.time() - _t_start))
                progress_bar.progress(60, text=f"⚡ キャッシュ利用中（選手詳細データ）｜{_fmt_remaining(_remaining)}")
            else:
                # ── 初回: 選手詳細データを取得 ──────────────────
                _remaining = max(0, _avg_total - (time.time() - _t_start))
                progress_bar.progress(int((done_count / total_tasks) * 60), text=f"⏳ 選手詳細データ取得中... ({done_count}/{total_tasks})｜{_fmt_remaining(_remaining)}")
                with ThreadPoolExecutor(max_workers=2, initializer=set_thread_venue, initargs=(_selected_venue_code,)) as executor:
                    f_ext      = executor.submit(fetch_extended_player_data, reg_nos)
                    f_racer_km = executor.submit(fetch_racer_kimarite, race_no, d_str, df_raw, course_map=_course_map)

                    phase2_map = {f_ext: "選手コース別成績", f_racer_km: "選手別決まり手"}
                    for future in as_completed(phase2_map):
                        done_count += 1
                        name = phase2_map[future]
                        pct = int((done_count / total_tasks) * 60)
                        _remaining = max(0, _avg_total - (time.time() - _t_start))
                        progress_bar.progress(pct, text=f"⏳ 選手詳細データ取得中... {name} 完了 ({done_count}/{total_tasks})｜{_fmt_remaining(_remaining)}")

                    ext_data = f_ext.result()
                    racer_km = f_racer_km.result()

                # ── 静的データをキャッシュに保存 ──────────────────
                st.session_state[_race_cache_key] = {
                    "ext_data": ext_data,
                    "racer_km": racer_km,
                    "taka": taka,
                    "lady_racers": lady_racers,
                }

            df_raw = apply_extended_data(df_raw, ext_data)
        else:
            racer_km = fetch_racer_kimarite(race_no, d_str)

        odds_2t = odds_2tf.get("2連単", {})

        if not df_raw.empty:
            _remaining = max(0, _avg_total - (time.time() - _t_start))
            progress_bar.progress(70, text=f"🧠 AI予想を計算中...｜{_fmt_remaining(_remaining)}")
            try:
                result = predict(
                    df_raw, weather, race_no,
                    taka_data=taka, odds_dict=odds,
                    odds_2t=odds_2t,
                    racer_kimarite=racer_km,
                    deadline=deadline,
                    venue_code=_selected_venue_code,
                )
            except Exception as e:
                progress_bar.progress(100, text="❌ 予想計算エラー")
                time.sleep(1)
                progress_bar.empty()
                st.session_state.running = False
                st.session_state.show_settings = True
                st.session_state.result = None
                st.session_state.pop("_exec_race_no", None)
                st.session_state.pop("_exec_race_date", None)
                st.session_state._fetch_error = f"予想計算中にエラーが発生しました: {e}\nレース設定を変更して再度お試しください。"
                st.rerun()

            _remaining = max(0, _avg_total - (time.time() - _t_start))
            progress_bar.progress(90, text=f"💾 予想結果を保存中...｜{_fmt_remaining(_remaining)}")
            st.session_state.result      = result
            st.session_state.weather     = weather
            st.session_state.deadline    = deadline
            st.session_state.race_no     = race_no
            st.session_state.date_str    = d_str
            st.session_state.odds        = odds
            st.session_state.odds_2t     = odds_2t
            st.session_state.taka        = taka
            st.session_state.racer_km    = racer_km
            st.session_state.race_result = race_result_data
            st.session_state.lady_racers = lady_racers

            confidence = (
                result["scored_df"]["confidence"].iloc[0]
                if "confidence" in result["scored_df"].columns else "-"
            )
            try:
                save_prediction(d_str, race_no, result["recommendations"], weather, confidence)
            except Exception:
                pass  # 保存失敗しても予想表示は続行

            _save_exec_total(time.time() - _t_start)
            progress_bar.progress(100, text="✅ 予想完了！")
            progress_bar.empty()

            # オッズ自動更新のタイマーをリセット（リラン直後の再取得を防止）
            st.session_state.odds_last_refresh_time = time.time()

            # 結果をファイルキャッシュに保存（セッション切断対策）
            _save_result_cache(
                race_no, d_str, result, weather, deadline,
                odds, odds_2t, taka, racer_km, race_result_data, lady_racers,
                device_id=_device_id,
            )

            # 停止ボタン・実行中表示をJSで即座に非表示（rerun完了前のラグ防止）
            _st_html("""<script>
            (function(){
                var doc = window.parent.document;
                var ind = doc.getElementById('running-indicator');
                if(ind){
                    var wrap = ind.closest('[data-testid="stVerticalBlockBorderWrapper"]');
                    if(wrap) wrap.style.display='none';
                }
            })();
            </script>""", height=0)
            st.session_state.show_settings = False
            st.session_state.running = False
            st.session_state.pop("_exec_race_no", None)
            st.session_state.pop("_exec_race_date", None)
            st.rerun()

        else:
            progress_bar.empty()
            st.session_state.result = None
            st.session_state.running = False
            st.session_state.show_settings = True
            st.session_state.pop("_exec_race_no", None)
            st.session_state.pop("_exec_race_date", None)
            st.session_state._fetch_error = "データが取得できませんでした。開催時間外の可能性があります。レース設定を変更して再度お試しください。"
            st.rerun()

    # ── 安全リセット: 実行フラグが残っていたら解除 ────────────────────
    if st.session_state.running:
        st.session_state.running = False
    st.session_state.pop("_ui_flushed", None)

    # ── 結果表示 ─────────────────────────────────────────────────────
    # 結果がない場合は固定バーを除去
    if st.session_state.result is None:
        _st_html("""<script>
        (function(){
          var bar = window.parent.document.getElementById('fixed-deadline-bar');
          if(bar) bar.remove();
          var mainEl = window.parent.document.querySelector('section.main');
          if(mainEl) mainEl.style.paddingTop = '';
        })();
        </script>""", height=0)

    if st.session_state.result is not None:
        res    = st.session_state.result
        scored = res["scored_df"]
        odds   = st.session_state.odds or {}

        # ── 予想対象レース ヘッダー ──────────────────────────────────
        _rno_disp = st.session_state.race_no
        _dstr = st.session_state.date_str or ""
        _date_fmt = f"{_dstr[:4]}/{_dstr[4:6]}/{_dstr[6:]}" if len(_dstr) == 8 else _dstr
        _dl_time = st.session_state.deadline or "-"

        # カウントダウン用: 締切のJST日時を生成（レース日付を使用）
        _dl_dt = None
        if _dl_time != "-" and len(_dstr) == 8:
            try:
                _race_date = f"{_dstr[:4]}-{_dstr[4:6]}-{_dstr[6:]}"
                _dl_dt = datetime.strptime(
                    f"{_race_date} {_dl_time}", "%Y-%m-%d %H:%M"
                ).replace(tzinfo=_JST_TZ)
            except Exception:
                pass

        # 締め切り判定（JSTで比較）
        _is_past_deadline = False
        _remain_sec = 0
        if _dl_dt:
            try:
                _now_jst = datetime.now(_JST_TZ)
                _remain = _dl_dt - _now_jst
                _remain_sec = int(_remain.total_seconds())
                _is_past_deadline = _remain_sec <= 0
            except Exception:
                pass

        # ── 締め切り固定バー（常に画面上部に表示） ──────────────────
        _dl_iso = _dl_dt.isoformat() if _dl_dt else ""
        if _is_past_deadline:
            _fixed_deadline_status = '締め切り済み'
            _fixed_deadline_color = '#e05c5c'
        else:
            _fixed_deadline_status = ''
            _fixed_deadline_color = '#7ab8e8'

        _st_html(f"""
        <script>
        (function(){{
          var barId = 'fixed-deadline-bar';
          var existing = window.parent.document.getElementById(barId);
          if(existing) existing.remove();

          var bar = window.parent.document.createElement('div');
          bar.id = barId;
          bar.style.cssText = 'position:fixed;top:0;left:0;right:0;z-index:999999;' +
            'background:linear-gradient(135deg,#0a1628 0%,#0d2855 100%);' +
            'border-bottom:2px solid #f0a500;padding:4px 8px;' +
            'display:flex;flex-wrap:nowrap;align-items:center;justify-content:center;gap:6px;' +
            'white-space:nowrap;font-family:-apple-system,BlinkMacSystemFont,sans-serif;';

          var raceInfo = '<span style="color:#aac8e8;font-size:0.7rem">{_venue["short_name"]}</span>' +
            '<span style="color:#f0a500;font-size:0.85rem;font-weight:900;margin-left:4px">{_rno_disp}R</span>' +
            '<span style="color:#aac8e8;font-size:0.7rem;margin-left:3px">{_date_fmt}</span>';

          var deadlineInfo = '<span style="color:#ff9800;font-size:0.75rem">締切{_dl_time}</span>' +
            '<span id="fixed-countdown" style="color:{_fixed_deadline_color};font-weight:bold;font-size:0.75rem;margin-left:4px">{_fixed_deadline_status}</span>';

          bar.innerHTML = raceInfo + '<span style="color:#2a4a80;font-size:0.9rem">|</span>' + deadlineInfo;

          var mainEl = window.parent.document.querySelector('section.main');
          if(mainEl) {{
            mainEl.style.paddingTop = '36px';
          }}
          window.parent.document.body.appendChild(bar);

          var deadlineStr = "{_dl_iso}";
          var isPast = {'true' if _is_past_deadline else 'false'};
          if(deadlineStr && !isPast) {{
            var deadline = new Date(deadlineStr);
            var el = window.parent.document.getElementById('fixed-countdown');
            function update(){{
              var now = new Date();
              var diff = Math.floor((deadline - now) / 1000);
              if(diff <= 0){{
                el.textContent = '締め切り済み';
                el.style.color = '#e05c5c';
                bar.style.borderBottomColor = '#e05c5c';
                return;
              }}
              var m = Math.floor(diff / 60);
              var s = diff % 60;
              el.textContent = 'あと ' + m + '分' + String(s).padStart(2,'0') + '秒';
              if(m < 5){{
                el.style.color = '#e05c5c';
                bar.style.borderBottomColor = '#e05c5c';
              }} else {{
                el.style.color = '#7ab8e8';
                bar.style.borderBottomColor = '#f0a500';
              }}
            }}
            update();
            setInterval(update, 1000);
          }}
        }})();
        </script>
        """, height=0)

        st.markdown(
            f'<div style="background:linear-gradient(135deg,#0d2855,#1a3a6b);'
            f'border:2px solid #f0a500;border-radius:10px;padding:0.8rem 1rem;'
            f'margin-bottom:0.8rem;text-align:center">'
            f'<div style="color:#7ab8e8;font-size:0.75rem;letter-spacing:2px;margin-bottom:2px">PREDICTION TARGET</div>'
            f'<div style="display:flex;align-items:center;justify-content:center;gap:10px">'
            f'<span style="color:#f0a500;font-size:2rem;font-weight:900;letter-spacing:2px">{_rno_disp}R</span>'
            f'<span style="color:#fff;font-size:1rem">{_date_fmt}</span>'
            f'</div>'
            f'</div>',
            unsafe_allow_html=True,
        )

        # ── レース間ナビゲーションボタン ──────────────────────────────
        _nav_col1, _nav_col2, _nav_col3 = st.columns([1, 2, 1])
        with _nav_col1:
            st.button(
                f"◀ {_rno_disp - 1}R" if _rno_disp > 1 else "◀",
                use_container_width=True,
                disabled=(_rno_disp <= 1),
                on_click=_go_prev_race,
            )
        with _nav_col2:
            st.markdown(
                f'<div style="text-align:center;color:#7ab8e8;font-size:0.8rem;'
                f'line-height:2.4">{_rno_disp} / 12R</div>',
                unsafe_allow_html=True,
            )
        with _nav_col3:
            st.button(
                f"{_rno_disp + 1}R ▶" if _rno_disp < 12 else "▶",
                use_container_width=True,
                disabled=(_rno_disp >= 12),
                on_click=_go_next_race,
            )
        # ── 予想再実行ボタン ──────────────────────────────────────
        def _on_rerun_click():
            st.session_state.running = True
            st.session_state.show_settings = False
            st.session_state._exec_race_no = _rno_disp
            st.session_state._exec_race_date = st.session_state.get("_exec_race_date") or _today_jst()
            st.session_state.result = None
        st.button("🔄 予想を再実行", type="secondary", use_container_width=True,
                  on_click=_on_rerun_click)

        # ── 直前情報未取得の通知 ──────────────────────────────────────
        _has_chokuzen = (
            "展示タイム" in scored.columns
            and scored["展示タイム"].apply(lambda x: pd.notnull(x) and x > 0).any()
        )
        if not _has_chokuzen:
            st.markdown(
                '<div style="background:linear-gradient(135deg,#3a2000,#4a2800);'
                'border:1px solid #ff9800;border-radius:8px;padding:10px 14px;'
                'margin-bottom:10px;text-align:center">'
                '<span style="color:#ffb74d;font-size:0.95rem;font-weight:bold">'
                '⚠ 直前情報がまだ取得できていません</span>'
                '</div>',
                unsafe_allow_html=True,
            )

        # グレードバッジ + 気象メトリクス
        if st.session_state.weather:
            w = st.session_state.weather
            grade = w.get("grade", "一般")
            grade_colors = {"SG": "#e74c3c", "G1": "#f0a500", "G2": "#3498db", "G3": "#2ecc71", "一般": "#888"}
            grade_color = grade_colors.get(grade, "#888")
            grade_title = w.get("grade_title", "")
            final_badge = ' <span style="background:#e74c3c;color:#fff;padding:1px 8px;border-radius:10px;font-size:0.75rem;margin-left:6px">優勝戦</span>' if w.get("is_final") else ""
            st.markdown(
                f'<div style="margin-bottom:8px">'
                f'<span style="background:{grade_color};color:#fff;padding:2px 12px;border-radius:12px;'
                f'font-size:0.85rem;font-weight:bold">{grade}</span>{final_badge}'
                f'<span style="color:#aaa;font-size:0.8rem;margin-left:8px">{grade_title}</span>'
                f'</div>',
                unsafe_allow_html=True,
            )
            weather_items = [
                ("天気", w.get("天気", "-")),
                ("波高", w.get("波高", "0cm")),
                ("風向", (lambda d, t: d + f'<br><span style="font-size:0.7rem;color:#f0a500">({t})</span>' if t != "-" else d)(w.get("風向", "-"), get_wind_type(w.get("風向", "-"), _selected_venue_code))),
                ("風速", w.get("風速", "0m")),
                ("気温", w.get("気温", "-")),
                ("水温", w.get("水温", "-")),
                ("湿度", w.get("湿度", "-")),
                ("気圧", w.get("気圧", "-")),
            ]
            weather_html = '<div class="weather-grid">'
            for lbl, val in weather_items:
                weather_html += (
                    f'<div class="weather-item">'
                    f'<div class="label">{lbl}</div>'
                    f'<div class="value">{val}</div>'
                    f'</div>'
                )
            weather_html += '</div>'
            # 気象情報の注釈
            _note_style = 'color:#7ab8e8;font-size:0.7rem;margin-top:2px;'
            weather_html += f'<div style="{_note_style}">※天気・波高以外はリアルタイム情報</div>'
            if _dl_dt:
                _now_jst = datetime.now(_JST_TZ)
                _diff_sec = abs((_dl_dt - _now_jst).total_seconds())
                if _diff_sec > 3600:
                    weather_html += '<div style="color:#ff6e6e;font-size:0.75rem;margin-top:2px;font-weight:bold;">※締め切り前後1時間外のため気象条件は予想に含めていません</div>'
            # 安定板使用バッジ / 展示進入変化バッジ
            _badges_html = ""
            if w.get("安定板"):
                _badges_html += (
                    '<div style="margin-top:6px">'
                    '<span style="background:#e53935;color:#fff;padding:3px 14px;'
                    'border-radius:12px;font-size:0.85rem;font-weight:bold;'
                    'letter-spacing:1px">安定板使用</span>'
                    '<span style="color:#ff8a80;font-size:0.75rem;margin-left:8px">'
                    'イン有利傾向 / 展示タイム信頼性低下'
                    '</span></div>'
                )
            # 展示進入が枠番通りでない場合
            if "進入コース" in scored.columns:
                _entry_diff = False
                _course_to_boat = {}
                for _, _row in scored.iterrows():
                    _waku = int(_row.get("枠番", 0))
                    _course = _row.get("進入コース")
                    if pd.notna(_course) and int(_course) > 0:
                        _c = int(_course)
                        if _c != _waku:
                            _entry_diff = True
                        _course_to_boat[_c] = _waku
                    else:
                        _course_to_boat[_waku] = _waku
                if _entry_diff:
                    _entry_order = [str(_course_to_boat.get(c, c)) for c in range(1, 7)]
                    _entry_str = " - ".join(_entry_order)
                    _badges_html += (
                        '<div style="margin-top:6px">'
                        '<span style="background:#ff6f00;color:#fff;padding:3px 14px;'
                        'border-radius:12px;font-size:0.85rem;font-weight:bold;'
                        'letter-spacing:1px">進入変化</span>'
                        f'<span style="color:#ffab40;font-size:0.8rem;margin-left:8px">'
                        f'進入順: {_entry_str}</span></div>'
                    )
            if _badges_html:
                weather_html += _badges_html
            st.markdown(weather_html, unsafe_allow_html=True)

        st.markdown("---")

        # ── レース結果表示 ────────────────────────────────────────────
        _race_result = st.session_state.race_result
        if _race_result:
            _rno = st.session_state.race_no
            finishers = _race_result.get("着順", [])
            kimarite = _race_result.get("決まり手", "")
            payouts = _race_result.get("払戻", {})

            # 3連単の結果組番
            _result_3t_combo = ""
            if payouts.get("3連単"):
                _result_3t_combo = payouts["3連単"]["組番"].replace("-", "-")

            # 的中判定: 予想Top10に3連単結果が含まれているか
            _hit_rank = None
            all_3t = res.get("all_3t_candidates", [])
            if _result_3t_combo and all_3t:
                # 組番フォーマットを統一して比較 (例: "1-2-3")
                _norm_result = _result_3t_combo.replace("－", "-").replace("ー", "-")
                for i, cand in enumerate(all_3t[:10]):
                    _norm_cand = cand["買い目"].replace("－", "-").replace("ー", "-")
                    if _norm_result == _norm_cand:
                        _hit_rank = i + 1
                        break

            # 枠番カラー
            _FBG = {"1": "#fff", "2": "#000", "3": "#e74c3c",
                    "4": "#3498db", "5": "#f1c40f", "6": "#2ecc71"}
            _FFG = {"1": "#000", "2": "#fff", "3": "#fff",
                    "4": "#fff", "5": "#000", "6": "#fff"}

            # 的中バッジ
            if _hit_rank is not None:
                _hit_badge = (
                    f'<div style="background:linear-gradient(135deg,#ff6b00,#ff2d00);'
                    f'border-radius:12px;padding:10px 16px;margin-bottom:10px;text-align:center;'
                    f'animation:pulse 1.5s ease-in-out infinite">'
                    f'<span style="font-size:1.8rem;font-weight:900;color:#fff;'
                    f'text-shadow:0 0 20px rgba(255,255,255,0.6)">🎯 的中！</span>'
                    f'<span style="color:#ffe066;font-size:1rem;margin-left:10px;font-weight:bold">'
                    f'予想 {_hit_rank}位</span>'
                    f'</div>'
                    f'<style>@keyframes pulse{{'
                    f'0%,100%{{transform:scale(1)}}50%{{transform:scale(1.02)}}}}</style>'
                )
            else:
                _hit_badge = ""

            # 着順テーブル
            _finish_rows = ""
            for f in finishers:
                _fr = str(f["枠番"]) if f["枠番"] else "-"
                _bg = _FBG.get(_fr, "#555")
                _fg = _FFG.get(_fr, "#fff")
                _border = "border:1.5px solid #888;" if _fr == "1" else ""
                _frame_badge = (
                    f'<span style="display:inline-flex;align-items:center;justify-content:center;'
                    f'background:{_bg};color:{_fg};{_border}border-radius:4px;'
                    f'width:24px;height:24px;font-weight:bold;font-size:0.85rem">{_fr}</span>'
                )
                _rank_color = "#FFD700" if f["着"] == 1 else ("#C0C0C0" if f["着"] == 2 else ("#CD7F32" if f["着"] == 3 else "#7ab8e8"))
                _finish_rows += (
                    f'<tr style="border-bottom:1px solid rgba(30,95,168,0.3)">'
                    f'<td style="padding:5px 8px;text-align:center;color:{_rank_color};'
                    f'font-weight:bold;font-size:1rem">{f["着"]}</td>'
                    f'<td style="padding:5px 8px;text-align:center">{_frame_badge}</td>'
                    f'<td style="padding:5px 8px;color:#e8f4ff;font-size:0.9rem">{f["選手名"]}</td>'
                    f'<td style="padding:5px 8px;color:#7ab8e8;font-size:0.85rem;text-align:right">{f["レースタイム"]}</td>'
                    f'</tr>'
                )

            # 払戻金表示
            _payout_rows = ""
            for _bet_type in ("3連単", "3連複", "2連単", "2連複"):
                _p = payouts.get(_bet_type)
                if not _p:
                    continue
                if isinstance(_p, list):
                    _p = _p[0]
                # 組番の枠番をカラーバッジ化
                _combo_display = ""
                _raw_combo = _p["組番"]
                for ch in _raw_combo:
                    if ch.isdigit():
                        _cbg = _FBG.get(ch, "#555")
                        _cfgc = _FFG.get(ch, "#fff")
                        _cbdr = "border:1px solid #888;" if ch == "1" else ""
                        _combo_display += (
                            f'<span style="display:inline-block;width:20px;height:20px;'
                            f'line-height:20px;text-align:center;border-radius:3px;'
                            f'background:{_cbg};color:{_cfgc};{_cbdr}font-weight:bold;'
                            f'font-size:0.8rem;margin:0 1px">{ch}</span>'
                        )
                    else:
                        _combo_display += f'<span style="color:#7ab8e8;margin:0 2px">{ch}</span>'

                _payout_rows += (
                    f'<tr style="border-bottom:1px solid rgba(30,95,168,0.3)">'
                    f'<td style="padding:4px 8px;color:#7ab8e8;font-size:0.85rem;white-space:nowrap">{_bet_type}</td>'
                    f'<td style="padding:4px 8px">{_combo_display}</td>'
                    f'<td style="padding:4px 8px;color:#ffe066;font-weight:bold;font-size:0.95rem;'
                    f'text-align:right;white-space:nowrap">&yen;{_p["払戻金"]:,}</td>'
                    f'<td style="padding:4px 8px;color:#aaa;font-size:0.8rem;text-align:center">'
                    f'{_p["人気"]}番人気</td>'
                    f'</tr>'
                )

            st.markdown(
                f'{_hit_badge}'
                f'<div style="background:linear-gradient(135deg,#0d1f3c,#162d50);'
                f'border:2px solid #f0a500;border-radius:10px;padding:12px 14px;margin-bottom:12px">'
                f'<div style="display:flex;align-items:center;margin-bottom:8px">'
                f'<span style="font-size:1.2rem;margin-right:6px">🏁</span>'
                f'<span style="color:#f0a500;font-size:1.1rem;font-weight:bold">{_rno}R レース結果</span>'
                f'<span style="color:#7ab8e8;font-size:0.8rem;margin-left:auto">決まり手: '
                f'<b style="color:#ffe066">{kimarite}</b></span>'
                f'</div>'
                f'<table style="width:100%;border-collapse:collapse;margin-bottom:10px">'
                f'<tr style="border-bottom:2px solid #1e5fa8">'
                f'<th style="padding:4px 8px;color:#7ab8e8;font-size:0.8rem;text-align:center">着</th>'
                f'<th style="padding:4px 8px;color:#7ab8e8;font-size:0.8rem;text-align:center">枠</th>'
                f'<th style="padding:4px 8px;color:#7ab8e8;font-size:0.8rem;text-align:left">選手名</th>'
                f'<th style="padding:4px 8px;color:#7ab8e8;font-size:0.8rem;text-align:right">タイム</th>'
                f'</tr>{_finish_rows}</table>'
                f'<div style="border-top:1px solid #1e5fa8;padding-top:8px;margin-top:4px">'
                f'<div style="color:#7ab8e8;font-size:0.78rem;margin-bottom:4px">払戻金</div>'
                f'<table style="width:100%;border-collapse:collapse">'
                f'{_payout_rows}</table>'
                f'</div>'
                f'</div>',
                unsafe_allow_html=True,
            )
            st.markdown("---")

        # ── 3連単 全組番テーブル（確率順）【メイン予想 - 最上部表示】──

        @st.fragment
        def _render_3t_section():
            """3連単予想セクション（オッズ更新時はここだけ再描画）"""
            _hdr_col1, _hdr_col2 = st.columns([3, 1])
            with _hdr_col1:
                _lr_text = ""
                if st.session_state.odds_last_refresh_time > 0:
                    _lr = datetime.fromtimestamp(st.session_state.odds_last_refresh_time, tz=_JST_TZ)
                    _lr_text = f'<span style="color:#888;font-size:0.75rem;margin-left:12px;white-space:nowrap">最終更新: {_lr.strftime("%H:%M:%S")}</span>'
                st.markdown(f'<h3 style="display:flex;align-items:baseline;flex-wrap:nowrap;white-space:nowrap;font-size:1.1rem">🎯 3連単 予想{_lr_text}</h3>', unsafe_allow_html=True)
            with _hdr_col2:
                if st.session_state.race_no and st.session_state.date_str:
                    if st.button("オッズ更新", key="odds_refresh_btn", use_container_width=True):
                        _ar_rno = st.session_state.race_no
                        _ar_dstr = st.session_state.date_str
                        _ar_odds = fetch_odds_3t(_ar_rno, _ar_dstr, jycd=_selected_venue_code)
                        if _ar_odds:
                            st.session_state.odds = _ar_odds
                            _ar_3t = st.session_state.result.get("all_3t_candidates", [])
                            for _c in _ar_3t:
                                _actual = _ar_odds.get(_c["買い目"])
                                _c["実オッズ"] = _actual
                                if _actual is not None and _c["的中確率"] > 0:
                                    _c["期待値"] = (_c["的中確率"] / 100) * _actual
                                else:
                                    _c["期待値"] = None
                            st.session_state.result["all_3t_candidates"] = _ar_3t
                            for _rec in st.session_state.result.get("recommendations", []):
                                _rec_key = _rec.get("買い目", "")
                                _rec_actual = _ar_odds.get(_rec_key)
                                _rec["実オッズ"] = _rec_actual
                                if _rec_actual is not None and _rec.get("的中確率", 0) > 0:
                                    _rec["期待値"] = (_rec["的中確率"] / 100) * _rec_actual
                                else:
                                    _rec["期待値"] = None
                            st.session_state.odds_last_refresh_time = time.time()

            all_3t = st.session_state.result.get("all_3t_candidates", [])
            odds = st.session_state.odds or {}

            if not all_3t:
                st.warning("買い目候補がありません")
                return
            # レース結果の3連単組番を取得（的中マーク用）
            _rr = st.session_state.race_result
            _result_combo_3t = ""
            if _rr and _rr.get("払戻", {}).get("3連単"):
                _result_combo_3t = _rr["払戻"]["3連単"]["組番"].replace("－", "-").replace("ー", "-")

            def _calc_bet_alloc(candidates: list[dict], budget: int, min_return_ratio: float = 1.3) -> dict:
                """確率上位から連続で買い、トリガミなしで配分する。

                ルール:
                  1) 確率順で上から連続で買う（飛ばさない）
                  2) どの買い目が当たっても払戻 ≧ 投資総額（トリガミなし）
                  3) 最低配分 = ceil(投資総額 ÷ オッズ) を確保後、残りをEV加重
                  4) 点数は的中率増分2%以上 かつ トリガミなし配分が予算内に収まる範囲
                Returns: {combo_str: allocation_yen, ...}
                """
                import math
                _min_prob = 1.0  # 的中確率1%未満は買わない
                buyable = [c for c in candidates
                           if c.get("実オッズ") is not None and c.get("的中確率", 0) >= _min_prob]
                if not buyable:
                    return {}

                # ── 買える最大点数を決定（上限10点） ──
                # 最低払戻 ≧ 投資額×1.5 を保証する配分が予算内に収まる最大N点
                # → オッズが低い（本命決着型）→ 少点数
                # → オッズが高い（混戦型）→ 多点数
                _min_return_ratio = min_return_ratio
                final_n = 0
                for n in range(1, min(len(buyable), 10) + 1):
                    subset = buyable[:n]
                    min_total = sum(
                        math.ceil(budget * _min_return_ratio / c["実オッズ"] / 100) * 100
                        for c in subset
                    )
                    if min_total <= budget:
                        final_n = n
                    else:
                        break

                if final_n == 0:
                    # 1点も買えない場合（オッズが低すぎ）→ 1点だけ
                    if buyable:
                        return {buyable[0]["買い目"]: budget}
                    return {}

                selected = buyable[:final_n]

                # ── 配分: 最低払戻保証額を確保 + 残りをEV加重 ──
                alloc = {}
                for c in selected:
                    min_alloc = math.ceil(budget * _min_return_ratio / c["実オッズ"] / 100) * 100
                    alloc[c["買い目"]] = min_alloc

                remaining = budget - sum(alloc.values())

                if remaining >= 100:
                    # 残り予算をEV加重で上乗せ
                    weights = {}
                    for c in selected:
                        ev = c.get("期待値") or 0
                        weights[c["買い目"]] = max(ev, 0.1)
                    total_w = sum(weights.values())
                    for combo, w in weights.items():
                        extra = round(w / total_w * remaining / 100) * 100
                        if extra >= 100:
                            alloc[combo] += extra
                    # 端数超過の調整（最低保証額を守りつつ最大配分から削減）
                    _min_alloc = {c["買い目"]: math.ceil(budget * _min_return_ratio / c["実オッズ"] / 100) * 100
                                  for c in selected}
                    while sum(alloc.values()) > budget:
                        # 最低保証額を超えている中で最大の買い目から削減
                        reducible = {k: v for k, v in alloc.items() if v > _min_alloc.get(k, 100)}
                        if not reducible:
                            break
                        max_combo = max(reducible, key=lambda k: alloc[k])
                        alloc[max_combo] -= 100

                return alloc

            def _render_3t_table(rows: list[dict], table_id: str = "", result_combo: str = "",
                                 start_num: int = 1, alloc_map: dict | None = None) -> str:
                """3連単テーブルのHTML文字列を生成する。alloc_map があれば配分列を表示。"""
                _show_alloc = alloc_map and any(alloc_map.values())
                _th_style = ('background:#0d2855;color:#7ab8e8;padding:4px 3px;'
                             'font-size:0.7rem;border-bottom:2px solid #1e5fa8;white-space:nowrap;text-align:center')
                hdr = (
                    f'<tr>'
                    f'<th style="{_th_style};width:30px">#</th>'
                    f'<th style="{_th_style};width:{"22%" if _show_alloc else "28%"}">組番</th>'
                    f'<th style="{_th_style}">確率</th>'
                    f'<th style="{_th_style}">実ｵｯｽﾞ</th>'
                    f'<th style="{_th_style}">公正ｵｯｽﾞ</th>'
                    f'<th style="{_th_style}">期待値</th>'
                )
                if _show_alloc:
                    hdr += (f'<th style="{_th_style};color:#f0a030">配分</th>'
                            f'<th style="{_th_style};color:#4cda7c">払戻</th>')
                hdr += '</tr>'
                body = ""
                _FBG = {"1":"#fff","2":"#000","3":"#e74c3c","4":"#3498db","5":"#f1c40f","6":"#2ecc71"}
                _FFG = {"1":"#000","2":"#fff","3":"#fff","4":"#fff","5":"#000","6":"#fff"}

                # 購入ライン挿入位置の検出（配分がある最後の行の直後）
                _last_buy_idx = -1
                _any_buy = False
                if alloc_map:
                    for _ri, _rc in enumerate(rows):
                        if alloc_map.get(_rc.get("買い目", ""), 0) > 0:
                            _last_buy_idx = _ri
                            _any_buy = True

                for idx, c in enumerate(rows):
                    combo = c["買い目"]
                    prob = c["的中確率"]
                    fair = c["公正オッズ"]
                    actual = c.get("実オッズ")
                    ev = c.get("期待値")

                    # 的中判定
                    _is_hit = (result_combo and
                               combo.replace("－", "-").replace("ー", "-") == result_combo)

                    # 枠番バッジ
                    parts = combo.split("-")
                    nums_html = ""
                    for p in parts:
                        bg = _FBG.get(p, "#555")
                        fg = _FFG.get(p, "#fff")
                        nums_html += (
                            f'<span class="frame-badge" style="display:inline-block;width:18px;height:18px;'
                            f'min-width:18px;line-height:18px;text-align:center;border-radius:4px;'
                            f'background:{bg};color:{fg};font-weight:bold;font-size:0.7rem;'
                            f'margin:0 1px;flex-shrink:0">{p}</span>'
                        )
                    hit_html = ""
                    if _is_hit:
                        hit_html = (
                            '<span style="'
                            'background:linear-gradient(135deg,#ff6b00,#ff2d00);'
                            'color:#fff;font-weight:bold;font-size:0.6rem;padding:1px 4px;'
                            'line-height:1;border-radius:6px;white-space:nowrap;'
                            'margin-left:3px;flex-shrink:0">的中</span>'
                        )
                    combo_html = (
                        f'<div style="display:inline-flex;align-items:center;'
                        f'justify-content:center;gap:2px;'
                        f'height:22px;flex-wrap:nowrap">'
                        f'{nums_html}{hit_html}</div>'
                    )

                    if actual is not None:
                        actual_str = "999" if actual > 999 else f"{actual:.1f}"
                    else:
                        actual_str = "-"

                    if ev is not None:
                        if ev >= 1.0:
                            ev_str = f'<span style="color:#2ecc71;font-weight:bold">{ev:.2f}</span>'
                        else:
                            ev_str = f'<span style="color:#aaa">{ev:.2f}</span>'
                    else:
                        ev_str = '<span style="color:#555">-</span>'

                    # 配分
                    _alloc_yen = alloc_map.get(combo, 0) if alloc_map else 0

                    # 行背景
                    _is_zero = prob < 0.005
                    if _is_hit:
                        row_bg = "#3a1a0a"
                        row_border = "border-left:3px solid #ff6b00;"
                    elif _alloc_yen > 0:
                        row_bg = "#12301a"
                        row_border = "border-left:3px solid #2ecc71;"
                    elif ev is not None and ev >= 1.0:
                        row_bg = "#1a3020"
                        row_border = "border-left:3px solid #2ecc7166;"
                    elif _is_zero:
                        row_bg = "#12151e"
                        row_border = ""
                    else:
                        row_bg = "#1a2744" if idx % 2 == 0 else "#151f35"
                        row_border = ""

                    _row_num = start_num + idx
                    _zero_opacity = "opacity:0.35;" if _is_zero else ""
                    prob_str = f'<span style="color:#555">0%</span>' if _is_zero else f'{prob:.2f}%'
                    _td_base = "padding:3px 3px;vertical-align:middle;border-bottom:1px solid #2a4a80"
                    body += (
                        f'<tr style="background:{row_bg};{row_border}{_zero_opacity}height:32px">'
                        f'<td style="{_td_base};text-align:center;color:#7ab8e8;'
                        f'font-size:0.72rem;white-space:nowrap">{_row_num}</td>'
                        f'<td style="{_td_base};text-align:left">{combo_html}</td>'
                        f'<td style="{_td_base};text-align:right;color:#fff;'
                        f'font-size:0.78rem">{prob_str}</td>'
                        f'<td style="{_td_base};text-align:right;color:#ffe066;'
                        f'font-weight:bold;font-size:0.78rem">{actual_str}</td>'
                        f'<td style="{_td_base};text-align:right;color:#7ab8e8;'
                        f'font-size:0.78rem">{"999" if fair > 999 else f"{fair:.1f}"}</td>'
                        f'<td style="{_td_base};text-align:right;'
                        f'font-size:0.78rem">{ev_str}</td>'
                    )
                    if _show_alloc:
                        if _alloc_yen > 0:
                            _payout = int(_alloc_yen * actual) if actual is not None else 0
                            body += (
                                f'<td style="{_td_base};text-align:right;'
                                f'font-size:0.78rem;color:#ffe066;font-weight:bold">'
                                f'{_alloc_yen:,}</td>'
                                f'<td style="{_td_base};text-align:right;'
                                f'font-size:0.78rem;color:#2ecc71;font-weight:bold">'
                                f'{_payout:,}</td>'
                            )
                        else:
                            body += (
                                f'<td style="{_td_base};text-align:right;'
                                f'font-size:0.78rem;color:#555">-</td>'
                                f'<td style="{_td_base};text-align:right;'
                                f'font-size:0.78rem;color:#555">-</td>'
                            )
                    body += '</tr>'

                    # ── 購入ライン ─────────────────────
                    if _any_buy and idx == _last_buy_idx:
                        _ncols = 8 if _show_alloc else 6
                        body += (
                            f'<tr><td colspan="{_ncols}" style="padding:0;height:3px;'
                            f'background:linear-gradient(90deg,#2ecc71,#2ecc7100)">'
                            f'</td></tr>'
                        )

                return (
                    f'<div class="sanrentan-wrap" style="overflow-x:hidden;">'
                    f'<table style="border-collapse:collapse;width:100%;table-layout:fixed;'
                    f'background:#1a2744;border-radius:8px;overflow:hidden">'
                    f'{hdr}{body}</table></div>'
                )

            # ── 購入ライン分析 + 資金配分 ─────────────────────────────
            _has_ev = any(c.get("期待値") is not None for c in all_3t)
            _alloc_map = {}
            if _has_ev:
                # 予算スライダー
                if "buy_budget_slider" not in st.session_state:
                    st.session_state["buy_budget_slider"] = 1000
                st.slider(
                    "1R あたりの予算",
                    min_value=100, max_value=5000, step=100,
                    format="%d円", key="buy_budget_slider",
                )
                _budget = st.session_state["buy_budget_slider"]
                # 最低回収倍率スライダー
                if "min_return_ratio_slider" not in st.session_state:
                    st.session_state["min_return_ratio_slider"] = 1.3
                st.slider(
                    "最低回収倍率",
                    min_value=1.0, max_value=3.0, step=0.1,
                    format="%.1f倍", key="min_return_ratio_slider",
                )
                _min_return_ratio = st.session_state["min_return_ratio_slider"]
                _alloc_map = _calc_bet_alloc(all_3t, _budget, _min_return_ratio)

                if _alloc_map:
                    _total_invest = sum(_alloc_map.values())
                    # 配分加重の期待回収
                    _expected_return = 0
                    _bought = []
                    for c in all_3t:
                        _a = _alloc_map.get(c["買い目"], 0)
                        if _a > 0:
                            _bought.append(c)
                            if c.get("実オッズ") is not None:
                                _expected_return += c["的中確率"] / 100.0 * c["実オッズ"] * _a
                    _expected_roi = (_expected_return / _total_invest - 1) * 100 if _total_invest > 0 else 0
                    _hit_prob_any = (1 - np.prod([1 - c["的中確率"] / 100 for c in _bought])) * 100
                    _n_alloc = len(_alloc_map)

                    # 最低払戻額を計算
                    _min_payout = min(
                        _alloc_map.get(c["買い目"], 0) * c.get("実オッズ", 0)
                        for c in _bought if _alloc_map.get(c["買い目"], 0) > 0
                    )

                    # サマリーパネル
                    _roi_color = "#2ecc71" if _expected_roi > 0 else "#e74c3c"
                    _panel_html = (
                        f'<div style="background:linear-gradient(135deg,#0d2855,#1a3a6a);'
                        f'border:1px solid #2ecc71;border-radius:10px;padding:12px 16px;'
                        f'margin-bottom:12px">'
                        f'<div style="display:flex;align-items:center;gap:8px;margin-bottom:8px">'
                        f'<span style="font-size:1.1rem">💰</span>'
                        f'<span style="color:#2ecc71;font-weight:bold;font-size:0.95rem">'
                        f'推奨: 確率上位 {_n_alloc}点買い</span>'
                        f'<span style="color:#888;font-size:0.75rem">'
                        f'（最低{_min_return_ratio:.1f}倍回収配分）</span>'
                        f'</div>'
                        f'<div style="display:flex;flex-wrap:wrap;gap:6px 16px;font-size:0.8rem">'
                        f'<div><span style="color:#888">投資額:</span> '
                        f'<span style="color:#fff;font-weight:bold">{_total_invest:,}円</span></div>'
                        f'<div><span style="color:#888">期待回収:</span> '
                        f'<span style="color:#ffe066;font-weight:bold">{_expected_return:,.0f}円</span></div>'
                        f'<div><span style="color:#888">期待ROI:</span> '
                        f'<span style="color:{_roi_color};font-weight:bold">'
                        f'{"+" if _expected_roi > 0 else ""}{_expected_roi:.1f}%</span></div>'
                        f'<div><span style="color:#888">いずれか的中率:</span> '
                        f'<span style="color:#7ab8e8;font-weight:bold">{_hit_prob_any:.1f}%</span></div>'
                        f'<div><span style="color:#888">最低払戻:</span> '
                        f'<span style="color:#2ecc71;font-weight:bold">{_min_payout:,.0f}円</span></div>'
                        f'</div>'
                    )
                    _panel_html += '</div>'
                    st.markdown(_panel_html, unsafe_allow_html=True)

            top10 = all_3t[:10]
            rest = all_3t[10:]

            st.markdown(_render_3t_table(top10, result_combo=_result_combo_3t, alloc_map=_alloc_map), unsafe_allow_html=True)

            # 凡例
            _legend_items = '🟢 緑行 = 購入対象'
            if _result_combo_3t:
                _legend_items += '&nbsp;|&nbsp;🟠 橙行 = 的中'
            _legend_items += '&nbsp;|&nbsp;緑ライン = 購入ライン'
            _legend_items += ('&nbsp;|&nbsp;確率: Heneryモデル推定&nbsp;|&nbsp;'
                              '公正ｵｯｽﾞ: 理論適正倍率')
            st.markdown(
                f'<div style="font-size:0.75rem;color:#666;margin-top:6px">'
                f'{_legend_items}'
                f'</div>',
                unsafe_allow_html=True,
            )

            if rest:
                with st.expander(f"▼ 残り {len(rest)} 件を表示"):
                    st.markdown(_render_3t_table(rest, result_combo=_result_combo_3t, start_num=11), unsafe_allow_html=True)

        _render_3t_section()

        # ── FOCUS フォーメーション買い目（本命） ─────────────────────
        _focus = st.session_state.result.get("focus_formation", {})
        _focus_f = _focus.get("F", [])
        _focus_s = _focus.get("S", [])
        if _focus_f or _focus_s:
            st.markdown("---")
            st.markdown("### FOCUS", unsafe_allow_html=True)

            _FBG = {"1":"#fff","2":"#000","3":"#e74c3c","4":"#3498db","5":"#f1c40f","6":"#2ecc71"}
            _FFG = {"1":"#000","2":"#fff","3":"#fff","4":"#fff","5":"#000","6":"#fff"}

            def _focus_badge(num: str) -> str:
                bg = _FBG.get(num, "#555")
                fg = _FFG.get(num, "#fff")
                return (
                    f'<span style="display:inline-flex;align-items:center;justify-content:center;'
                    f'width:26px;height:26px;min-width:26px;border-radius:5px;'
                    f'background:{bg};color:{fg};font-weight:bold;font-size:0.9rem;'
                    f'margin:0 1px">{num}</span>'
                )

            _dash = (
                '<span style="color:#fff;margin:0 5px;font-size:1.2rem;'
                'font-weight:bold;line-height:1">&#8211;</span>'
            )
            _eq = (
                '<span style="color:#ffe066;margin:0 5px;font-size:1.2rem;'
                'font-weight:bold;line-height:1">=</span>'
            )

            def _count_pts(combo: str) -> int:
                return 2 if "=" in combo else 1

            # ── 2連単セクション ──
            _f_rows = ""
            for row in _focus_f:
                combo = row["買い目"]
                pts = _count_pts(combo)
                if "=" in combo:
                    a, b = combo.split("=")
                    badges = _focus_badge(a) + _eq + _focus_badge(b)
                else:
                    a, b = combo.split("-")
                    badges = _focus_badge(a) + _dash + _focus_badge(b)
                _f_rows += (
                    f'<div style="display:flex;align-items:center;justify-content:space-between;'
                    f'padding:6px 0;border-bottom:1px solid #1e3455">'
                    f'<div style="display:flex;align-items:center">{badges}</div>'
                    f'<span style="color:#888;font-size:0.7rem">{pts}点</span>'
                    f'</div>'
                )

            # ── 3連単セクション ──
            _s_rows = ""
            for row in _focus_s:
                combo = row["買い目"]
                pts = _count_pts(combo)
                if "=" in combo:
                    axis_part, third = combo.split("=")
                    first, second = axis_part.split("-")
                    badges = _focus_badge(first) + _dash + _focus_badge(second) + _eq + _focus_badge(third)
                else:
                    parts = combo.split("-")
                    first, second, third = parts[0], parts[1], parts[2] if len(parts) > 2 else "?"
                    badges = _focus_badge(first) + _dash + _focus_badge(second) + _dash + _focus_badge(third)
                _s_rows += (
                    f'<div style="display:flex;align-items:center;justify-content:space-between;'
                    f'padding:6px 0;border-bottom:1px solid #1e3455">'
                    f'<div style="display:flex;align-items:center">{badges}</div>'
                    f'<span style="color:#888;font-size:0.7rem">{pts}点</span>'
                    f'</div>'
                )

            _section_label = (
                'style="color:#7ab8e8;font-size:0.6rem;font-weight:bold;'
                'padding:2px 0;margin-top:4px;border-bottom:1px solid #2a4a80"'
            )

            st.markdown(
                f'<div style="background:#111a2e;border:1px solid #2a4a80;border-radius:10px;'
                f'padding:10px 14px;margin:8px 0">'
                f'<div {_section_label}>2連単</div>{_f_rows}'
                f'<div {_section_label}>3連単</div>{_s_rows}'
                f'</div>',
                unsafe_allow_html=True,
            )

        st.markdown("---")

        # ── 分析データテーブル ────────────────────────────────────────
        rno = st.session_state.race_no
        st.markdown(f"### 📋 {rno}R 分析データ")

        base_cols  = ["枠番", "選手名", "級別"]
        extra_cols = []
        for col in ["F回数", "体重"]:
            if col in scored.columns:
                extra_cols.append(col)
        base_cols += ["全国勝率", "当地勝率"]
        if "モーター2連率" in scored.columns:
            extra_cols.append("モーター2連率")
        if "スタートタイミング" in scored.columns:
            extra_cols.append("スタートタイミング")
        for col in ["コース別1着率", "直近平均着順"]:
            if col in scored.columns:
                extra_cols.append(col)
        # 展示情報は別テーブルに分離
        _has_exhibit = (
            "展示タイム" in scored.columns
            and scored["展示タイム"].apply(lambda x: pd.notnull(x) and x > 0).any()
        )
        display_cols = base_cols[:3] + extra_cols + base_cols[3:]
        seen = set()
        display_cols_unique = [
            c for c in display_cols
            if c in scored.columns and c not in seen and not seen.add(c)
        ]

        col_rename = {
            "進入コース":         "展示進入",
            "モーター2連率":      "M2連(%)",
            "スタートタイミング": "平均ST",
            "コース別1着率":      "C別1着(%)",
            "直近平均着順":       "直近平均着",
            "まわり足タイム":     "まわり足",
            "直線タイム":         "直線T",
            "一周タイム":         "一周T",
            "F回数":              "F",
        }
        display_df = scored[display_cols_unique].rename(columns=col_rename).copy()

        # 女子選手にハートマークを付与
        _lady_set = st.session_state.lady_racers or set()
        if _lady_set and "登録番号" in scored.columns:
            _lady_mask = scored["登録番号"].astype(str).isin(_lady_set)
            display_df.loc[_lady_mask, "選手名"] = '<span style="color:#ff0000;font-size:1.1em">♥</span> ' + display_df.loc[_lady_mask, "選手名"].astype(str)

        fmt = {"全国勝率": "{:.2f}", "当地勝率": "{:.2f}"}
        if "M2連(%)" in display_df.columns:
            fmt["M2連(%)"] = lambda x: f"{x:.1f}" if pd.notnull(x) and x > 0 else "-"
        if "平均ST" in display_df.columns:
            fmt["平均ST"] = lambda x: f"{x:.2f}" if pd.notnull(x) else "-"
        if "C別1着(%)" in display_df.columns:
            fmt["C別1着(%)"] = lambda x: f"{x:.1f}" if pd.notnull(x) and x > 0 else "-"
        if "直近平均着" in display_df.columns:
            fmt["直近平均着"] = lambda x: f"{x:.1f}" if pd.notnull(x) and x > 0 else "-"
        if "体重" in display_df.columns:
            fmt["体重"] = lambda x: f"{x:.1f}kg" if pd.notnull(x) and x > 0 else "-"
        if "F" in display_df.columns:
            fmt["F"] = lambda x: f"{int(x)}" if pd.notnull(x) and x > 0 else "0"

        # M2連(%) の良/悪ハイライト
        def _style_motor2(col):
            """モーター2連率: ≥40%=好調(赤), ≤25%=不調(青)"""
            styles = [""] * len(col)
            for i, v in enumerate(col):
                if pd.notnull(v) and v > 0:
                    if v >= 40:
                        styles[i] = "background-color: rgba(231,76,60,0.45); color: #fff; font-weight: bold"
                    elif v <= 25:
                        styles[i] = "background-color: rgba(52,152,219,0.45); color: #fff; font-weight: bold"
            return styles

        styler = display_df.style

        if "M2連(%)" in display_df.columns:
            styler = styler.apply(_style_motor2, subset=["M2連(%)"])

        # 女子選手の選手名セルをピンク色に
        if _lady_set and "登録番号" in scored.columns:
            _lady_mask_list = scored["登録番号"].astype(str).isin(_lady_set).tolist()
            def _style_lady_name(col):
                return ["color: #ff69b4" if _lady_mask_list[i] else "" for i in range(len(col))]
            styler = styler.apply(_style_lady_name, subset=["選手名"])

        # テーブル全体スタイル
        tbl_css = [
            {"selector": "", "props": [("border-collapse", "collapse"), ("width", "100%")]},
            {"selector": "th", "props": [
                ("background-color", "#1a2744"), ("color", "#e8f4ff"),
                ("padding", "8px 6px"), ("text-align", "center"),
                ("font-size", "0.78rem"), ("border-bottom", "2px solid #1e5fa8"),
                ("white-space", "nowrap"),
            ]},
            {"selector": "td", "props": [
                ("padding", "6px 4px"), ("text-align", "center"),
                ("color", "#e8f4ff"), ("border-bottom", "1px solid rgba(30,95,168,0.3)"),
                ("font-size", "0.8rem"), ("white-space", "nowrap"),
            ]},
        ]
        styler = styler.set_table_styles(tbl_css)

        styled = styler.format(fmt).hide(axis="index")
        tbl_html = styled.to_html()

        st.markdown(
            f'<div style="overflow-x:auto;">{tbl_html}</div>',
            unsafe_allow_html=True,
        )

        # ── 予測・ポイントテーブル ────────────────────────────────────
        st.markdown("---")
        st.markdown(f"### 🎯 {rno}R 予測・ポイント")

        prob_cols = ["枠番", "選手名"]
        if "win_prob" in scored.columns:
            prob_cols.append("win_prob")
        if "highlight_reason" in scored.columns:
            prob_cols.append("highlight_reason")

        prob_display_cols = [c for c in prob_cols if c in scored.columns]
        prob_df = scored[prob_display_cols].rename(columns={
            "win_prob": "1着確率(%)",
            "highlight_reason": "ポイント",
        }).copy()

        # 女子選手にハートマーク
        if _lady_set and "登録番号" in scored.columns:
            _lady_mask_p = scored["登録番号"].astype(str).isin(_lady_set)
            prob_df.loc[_lady_mask_p, "選手名"] = '<span style="color:#ff0000;font-size:1.1em">♥</span> ' + prob_df.loc[_lady_mask_p, "選手名"].astype(str)

        prob_fmt = {}
        if "1着確率(%)" in prob_df.columns:
            prob_fmt["1着確率(%)"] = "{:.1f}%"

        prob_styler = prob_df.style

        # 女子選手の選手名セルをピンク色に
        if _lady_set and "登録番号" in scored.columns:
            _lady_mask_p_list = scored["登録番号"].astype(str).isin(_lady_set).tolist()
            def _style_lady_name_p(col):
                return ["color: #ff69b4" if _lady_mask_p_list[i] else "" for i in range(len(col))]
            prob_styler = prob_styler.apply(_style_lady_name_p, subset=["選手名"])

        prob_styler = prob_styler.set_table_styles(tbl_css)
        prob_styled = prob_styler.format(prob_fmt).hide(axis="index")
        prob_tbl_html = prob_styled.to_html()

        # ポイント列を左寄せにする
        if "ポイント" in prob_df.columns:
            _point_idx = prob_df.columns.get_loc("ポイント")
            _uuid_prob = re.search(r'id="(T_[a-zA-Z0-9_]+)"', prob_tbl_html)
            if _uuid_prob:
                _tid_prob = _uuid_prob.group(1)
                _pcss = (
                    f"<style>"
                    f"#{_tid_prob} th.col_heading.level0.col{_point_idx},"
                    f"#{_tid_prob} td.col{_point_idx} {{"
                    f" text-align:left !important;"
                    f"}}</style>"
                )
                prob_tbl_html = _pcss + prob_tbl_html

        st.markdown(
            f'<div style="overflow-x:auto;">{prob_tbl_html}</div>',
            unsafe_allow_html=True,
        )

        # ── 展示データテーブル ──────────────────────────────────────
        if _has_exhibit:
            st.markdown("---")
            st.markdown(f"### 🚤 {rno}R 展示データ")

            # 展示テーブル用カラム構築
            ex_cols = ["枠番", "選手名"]
            ex_col_rename = {}
            if "進入コース" in scored.columns:
                ex_cols.append("進入コース")
                ex_col_rename["進入コース"] = "展示進入"
            ex_cols.append("展示タイム")
            for col in ["まわり足タイム", "直線タイム", "一周タイム"]:
                if col in scored.columns:
                    ex_cols.append(col)
            ex_cols.append("チルト")
            ex_col_rename.update({
                "まわり足タイム": "まわり足",
                "直線タイム":     "直線T",
                "一周タイム":     "一周T",
            })

            ex_display_cols = [c for c in ex_cols if c in scored.columns]
            ex_df = scored[ex_display_cols].rename(columns=ex_col_rename).copy()

            # 女子選手にハートマーク
            if _lady_set and "登録番号" in scored.columns:
                _lady_mask = scored["登録番号"].astype(str).isin(_lady_set)
                ex_df.loc[_lady_mask, "選手名"] = '<span style="color:#ff0000;font-size:1.1em">♥</span> ' + ex_df.loc[_lady_mask, "選手名"].astype(str)

            # フォーマット
            ex_fmt = {}
            ex_fmt["展示タイム"] = lambda x: f"{x:.2f}" if pd.notnull(x) and x > 0 else "未発表"
            ex_fmt["チルト"] = lambda x: f"{x:.1f}" if pd.notnull(x) else "-"
            if "まわり足" in ex_df.columns:
                ex_fmt["まわり足"] = lambda x: f"{x:.2f}" if pd.notnull(x) and x > 0 else "-"
            if "直線T" in ex_df.columns:
                ex_fmt["直線T"] = lambda x: f"{x:.2f}" if pd.notnull(x) and x > 0 else "-"
            if "一周T" in ex_df.columns:
                ex_fmt["一周T"] = lambda x: f"{x:.2f}" if pd.notnull(x) and x > 0 else "-"

            # 進入コースのフォーマット
            if "展示進入" in ex_df.columns:
                _waku_list = scored["枠番"].astype(int).tolist()
                def _fmt_shinnyuu(idx, x):
                    if pd.isnull(x) or int(x) <= 0:
                        return "-"
                    c = int(x)
                    w = _waku_list[idx]
                    return f"{c}" if c == w else f"{c}コース"
                ex_df["展示進入"] = [
                    _fmt_shinnyuu(i, v) for i, v in enumerate(ex_df["展示進入"])
                ]
                ex_fmt["展示進入"] = "{}"

            # 展示列の1位/2位ハイライト
            _EXHIBIT_COLS = ["展示進入", "展示タイム", "まわり足", "直線T", "一周T"]

            def _style_exhibit_rank(col):
                """展示列の1位(赤)・2位(黄)を色付け"""
                styles = [""] * len(col)
                valid = col[(col > 0) & col.notnull()]
                if len(valid) >= 2:
                    sorted_vals = valid.sort_values()
                    top1, top2 = sorted_vals.iloc[0], sorted_vals.iloc[1]
                    for i, v in enumerate(col):
                        if pd.notnull(v) and v > 0:
                            if v == top1:
                                styles[i] = "background-color: rgba(231,76,60,0.35); color: #fff; font-weight: bold"
                            elif v == top2:
                                styles[i] = "background-color: rgba(241,196,15,0.35); color: #fff; font-weight: bold"
                elif len(valid) == 1:
                    top1 = valid.iloc[0]
                    for i, v in enumerate(col):
                        if pd.notnull(v) and v > 0 and v == top1:
                            styles[i] = "background-color: rgba(231,76,60,0.35); color: #fff; font-weight: bold"
                return styles

            ex_styler = ex_df.style

            # 展示進入が枠番と異なるセルをオレンジでハイライト
            if "展示進入" in ex_df.columns:
                _shinnyuu_diff = ["コース" in str(v) for v in ex_df["展示進入"]]
                def _style_shinnyuu(col):
                    return [
                        "background-color: rgba(255,111,0,0.55); color: #fff; font-weight: bold; text-shadow: 0 0 4px rgba(255,160,0,0.7)"
                        if _shinnyuu_diff[i] else ""
                        for i in range(len(col))
                    ]
                ex_styler = ex_styler.apply(_style_shinnyuu, subset=["展示進入"])

            # 展示情報列に1位/2位ハイライト適用（展示進入はランキング対象外）
            _EXHIBIT_RANK_COLS = [c for c in _EXHIBIT_COLS if c != "展示進入"]
            for ecol in _EXHIBIT_RANK_COLS:
                if ecol in ex_df.columns:
                    ex_styler = ex_styler.apply(_style_exhibit_rank, subset=[ecol])

            # 女子選手の選手名セルをピンク色に
            if _lady_set and "登録番号" in scored.columns:
                _lady_mask_list = scored["登録番号"].astype(str).isin(_lady_set).tolist()
                def _style_lady_name_ex(col):
                    return ["color: #ff69b4" if _lady_mask_list[i] else "" for i in range(len(col))]
                ex_styler = ex_styler.apply(_style_lady_name_ex, subset=["選手名"])

            # テーブルスタイル（展示用：ヘッダー色を緑系に）
            ex_tbl_css = [
                {"selector": "", "props": [("border-collapse", "collapse"), ("width", "100%")]},
                {"selector": "th", "props": [
                    ("background-color", "#0a6e5c"), ("color", "#5dffe0"),
                    ("padding", "6px 4px"), ("text-align", "center"),
                    ("font-size", "0.72rem"), ("border-bottom", "2px solid #0d8a72"),
                    ("white-space", "nowrap"), ("font-weight", "bold"),
                ]},
                {"selector": "td", "props": [
                    ("padding", "5px 3px"), ("text-align", "center"),
                    ("color", "#e8f4ff"), ("border-bottom", "1px solid rgba(10,110,92,0.3)"),
                    ("font-size", "0.75rem"), ("white-space", "nowrap"),
                ]},
            ]
            ex_styler = ex_styler.set_table_styles(ex_tbl_css)

            ex_styled = ex_styler.format(ex_fmt).hide(axis="index")
            ex_tbl_html = ex_styled.to_html()

            st.markdown(
                f'<div class="exhibit-wrap" style="overflow-x:auto;">{ex_tbl_html}</div>',
                unsafe_allow_html=True,
            )

            # ── スタート展示図 ──────────────────────────────────────
            # ボート画像をbase64エンコードで読み込み
            import base64 as _b64
            from pathlib import Path as _Path
            _boat_img_dir = _Path(__file__).parent / "船画像"
            _boat_b64 = {}
            for _bno in range(1, 7):
                _bpath = _boat_img_dir / f"{_bno}.png"
                if _bpath.exists():
                    _boat_b64[str(_bno)] = _b64.b64encode(_bpath.read_bytes()).decode()
            _has_st_display = (
                "ST展示" in scored.columns
                and scored["ST展示"].apply(lambda x: isinstance(x, str) and len(x) > 0).any()
            )
            if "進入コース" in scored.columns and (
                scored["進入コース"].apply(lambda x: pd.notnull(x) and x > 0).any()
            ):
                _course_rows = []
                for _, _row in scored.iterrows():
                    _waku = str(int(_row["枠番"]))
                    _course = int(_row["進入コース"]) if pd.notna(_row.get("進入コース")) and int(_row.get("進入コース", 0)) > 0 else int(_waku)
                    _st_val = str(_row.get("ST展示", "")) if "ST展示" in scored.columns else ""
                    _course_rows.append({"waku": _waku, "course": _course, "st": _st_val})
                _course_rows.sort(key=lambda r: r["course"])

                def _parse_st_val(st_str):
                    if not st_str:
                        return None
                    s = st_str.strip()
                    is_f = s.upper().startswith("F")
                    if is_f:
                        s = s[1:]
                    try:
                        val = float(s)
                        return -val if is_f else val
                    except ValueError:
                        return None

                _st_vals = [_parse_st_val(r["st"]) for r in _course_rows]
                # スリット線の位置（left %）
                _slit_left = 72
                # 0.01秒あたりのオフセット（%）
                _px_per_001 = 0.8

                # --- HTML ---
                _sd_html = (
                    '<div style="background:#0c1a2e;border-radius:10px;padding:14px 0 10px 0;'
                    'margin-top:12px;position:relative;overflow:hidden;max-width:50%;">'
                    # ヘッダー
                    '<div style="display:flex;justify-content:space-between;align-items:center;'
                    'padding:0 16px;margin-bottom:6px;">'
                    '<span style="color:#8bb8d8;font-size:0.78rem;font-weight:bold;letter-spacing:1px;">スタート展示</span>'
                    '<span style="color:#4a6a8a;font-size:0.7rem;">ST</span>'
                    '</div>'
                    # ボート行ラッパー（スリット線の基準）
                    '<div style="position:relative;">'
                )

                for cr in _course_rows:
                    w = cr["waku"]
                    st_str = cr["st"]
                    st_sec = _parse_st_val(st_str)
                    is_flying = st_str.strip().upper().startswith("F") if st_str else False

                    # ボート位置: スリット線から絶対距離（0.01秒 = _px_per_001 %）
                    if st_sec is not None and _has_st_display:
                        if st_sec < 0:
                            boat_left = _slit_left + abs(st_sec) * 100 * _px_per_001
                        else:
                            boat_left = _slit_left - st_sec * 100 * _px_per_001
                        boat_left = max(2, min(95, boat_left))
                    else:
                        boat_left = _slit_left - 20

                    # ST表示
                    if st_str and _has_st_display:
                        if is_flying:
                            st_color = "#ff3333"
                            st_weight = "bold"
                            st_label = f"F{st_str.strip()[1:]}"
                        else:
                            st_color = "#c0d8f0"
                            st_weight = "600"
                            st_label = st_str.strip()
                    else:
                        st_color = "#4a6a8a"
                        st_weight = "normal"
                        st_label = "-"

                    # ボート画像
                    _b64_data = _boat_b64.get(w, "")
                    if _b64_data:
                        _boat_icon = (
                            f'<img src="data:image/png;base64,{_b64_data}" '
                            f'width="40" height="24" style="display:block;" />'
                        )
                    else:
                        _boat_icon = f'<span style="font-size:0.8rem;font-weight:bold;color:#fff;">{w}</span>'

                    _sd_html += (
                        f'<div style="display:flex;align-items:center;height:36px;'
                        f'border-bottom:1px solid rgba(40,70,110,0.25);position:relative;">'
                        # ボートアイコン（右端がboat_left%の位置に来るように配置）
                        f'<div style="position:absolute;left:{boat_left}%;top:50%;'
                        f'transform:translate(-100%,-50%);z-index:2;">{_boat_icon}</div>'
                        # ST値（右端固定）
                        f'<div style="position:absolute;right:14px;top:50%;transform:translateY(-50%);'
                        f'font-size:0.88rem;font-weight:{st_weight};color:{st_color};'
                        f'font-variant-numeric:tabular-nums;">{st_label}</div>'
                        f'</div>'
                    )

                # スリット線（縦線）- ボート行ラッパー内
                _sd_html += (
                    f'<div style="position:absolute;left:{_slit_left}%;top:0;bottom:0;'
                    f'width:3px;background:rgba(140,190,240,0.6);'
                    f'pointer-events:none;"></div>'
                )
                _sd_html += '</div>'  # ボート行ラッパー閉じ
                _sd_html += '</div>'  # 外側コンテナ閉じ
                st.markdown(_sd_html, unsafe_allow_html=True)

        st.markdown("---")

        # ── AI展開予想（1マーク）─────────────────────────────────────
        st.markdown("### 🌊 AI展開予想（1マーク）")
        st.caption("1マーク旋回時の展開をAIが予測。コース・ST・決まり手傾向・気象条件から算出")

        _tenkai_scenarios = res.get("tenkai_scenarios", [])
        if _tenkai_scenarios:
            _TK_BOAT_BADGE = {
                "①": ("1", "background:#fff;color:#000;border:1px solid #888"),
                "②": ("2", "background:#000;color:#fff"),
                "③": ("3", "background:#e74c3c;color:#fff"),
                "④": ("4", "background:#3498db;color:#fff"),
                "⑤": ("5", "background:#f1c40f;color:#000"),
                "⑥": ("6", "background:#2ecc71;color:#fff"),
            }
            _TK_FBG = {"1":"#fff","2":"#000","3":"#e74c3c","4":"#3498db","5":"#f1c40f","6":"#2ecc71"}
            _TK_FFG = {"1":"#000","2":"#fff","3":"#fff","4":"#fff","5":"#000","6":"#fff"}

            for _ti, _ts in enumerate(_tenkai_scenarios):
                stars = "★" * _ts["confidence"] + "☆" * (3 - _ts["confidence"])
                prob_pct = _ts["probability"] * 100
                frame = _ts.get("winner_frame", "1")

                if _ti == 0:
                    border_color = "#f0a500"
                    bg = "#1a2744"
                elif _ti == 1:
                    border_color = "#3498db"
                    bg = "#151f35"
                else:
                    border_color = "#2a4a80"
                    bg = "#12151e"

                fbg = _TK_FBG.get(frame, "#555")
                ffg = _TK_FFG.get(frame, "#fff")
                fborder = "border:1.5px solid #888;" if frame == "1" else ""
                frame_badge = (
                    f'<span style="display:inline-flex;align-items:center;justify-content:center;'
                    f'background:{fbg};color:{ffg};{fborder}border-radius:50%;'
                    f'width:24px;height:24px;font-weight:bold;font-size:0.85rem;margin-right:6px">'
                    f'{frame}</span>'
                )

                flow_html = _ts.get("flow", "")
                for _bm, (_bd, _bs) in _TK_BOAT_BADGE.items():
                    flow_html = flow_html.replace(
                        _bm,
                        f'<span style="display:inline-block;{_bs};border-radius:50%;'
                        f'width:1.4em;height:1.4em;text-align:center;line-height:1.4em;'
                        f'font-size:0.85em;font-weight:bold;margin:0 2px">{_bd}</span>'
                    )

                factors_html = ""
                for _fk in _ts.get("key_factors", []):
                    factors_html += (
                        f'<span style="display:inline-block;background:#0d3360;border-radius:4px;'
                        f'padding:1px 6px;font-size:0.7rem;color:#7ab8e8;margin:2px 2px 0 0">{_fk}</span>'
                    )

                st.markdown(
                    f'<div style="background:{bg};border-left:5px solid {border_color};'
                    f'padding:0.8rem 1rem;border-radius:6px;margin:6px 0">'
                    f'<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:6px">'
                    f'<div style="display:flex;align-items:center">'
                    f'{frame_badge}'
                    f'<span style="color:#ffe066;font-size:0.85rem;font-weight:bold">{_ts["title"]}</span>'
                    f'</div>'
                    f'<div style="text-align:right">'
                    f'<span style="color:#f0a500;font-size:0.85rem">{stars}</span>'
                    f'<span style="color:#7ab8e8;font-size:0.78rem;margin-left:6px">{prob_pct:.0f}%</span>'
                    f'</div>'
                    f'</div>'
                    f'<div style="color:#e8f4ff;font-size:0.85rem;line-height:1.6;margin-bottom:6px">'
                    f'{flow_html}</div>'
                    f'<div>{factors_html}</div>'
                    f'</div>',
                    unsafe_allow_html=True,
                )
        else:
            st.caption("展開予想を生成するのに十分なデータがありません")

        # ── 高橋アナ予想パネル（蒲郡のみ）───────────────────────────────
        taka = st.session_state.taka or {}
        if _venue.get("has_taka_yoso"):
            st.markdown("---")
            st.markdown("### 🎤 高橋アナの予想（蒲郡競艇公式サイト）")

        if _venue.get("has_taka_yoso") and taka.get("available"):
            tenkai = taka.get("tenkai", "")
            if tenkai and tenkai != "（入力中）":
                _BOAT_BADGE = {
                    "①": ("1", "background:#fff;color:#000;border:1px solid #888"),
                    "②": ("2", "background:#000;color:#fff"),
                    "③": ("3", "background:#e74c3c;color:#fff"),
                    "④": ("4", "background:#3498db;color:#fff"),
                    "⑤": ("5", "background:#f1c40f;color:#000"),
                    "⑥": ("6", "background:#2ecc71;color:#fff"),
                }
                tenkai_html = tenkai
                for mark, (digit, style) in _BOAT_BADGE.items():
                    tenkai_html = tenkai_html.replace(
                        mark,
                        f'<span style="display:inline-block;{style};'
                        f'border-radius:50%;width:1.2em;height:1.2em;text-align:center;'
                        f'line-height:1.2em;font-size:0.75em;font-weight:bold;margin:0 1px">{digit}</span>'
                    )
                tenkai_html = tenkai_html.replace("\n", "<br>")
                st.markdown(
                    f'<div style="background:#1a2744;border-left:4px solid #3498db;'
                    f'padding:0.8rem 1rem;border-radius:6px;margin-bottom:0.6rem">'
                    f'<div style="color:#7ab8e8;font-size:0.72rem;margin-bottom:4px">展開予想</div>'
                    f'<div style="color:#fff;font-size:0.85rem;line-height:1.7">{tenkai_html}</div>'
                    f'</div>',
                    unsafe_allow_html=True
                )
            elif tenkai == "（入力中）":
                st.info("高橋アナが現在入力中です。しばらく後に再取得してください。")
            else:
                st.caption("展開予想テキストは取得できませんでした")

            yoso_list = taka.get("yoso", [])
            if yoso_list:
                for y in yoso_list:
                    st.markdown(
                        f'<div style="background:#1a2744;border-left:4px solid #f0a500;'
                        f'padding:0.5rem 1rem;border-radius:6px;margin-bottom:4px">'
                        f'<span style="color:#7ab8e8;font-size:0.7rem">予想買い目</span>&nbsp;'
                        f'<b style="font-size:1.0rem;color:#ffe066">{y}</b>'
                        f'</div>',
                        unsafe_allow_html=True
                    )

            if True:  # 評価チャート（インデント維持）
                # ── 評価チャート（公式5×5グリッド再現）─────────────────
                positions = taka.get("chart_positions", {})
                if positions:
                    _BOAT_BG = {
                        "1": "#fff", "2": "#000", "3": "#e74c3c",
                        "4": "#3498db", "5": "#f1c40f", "6": "#2ecc71",
                    }
                    _BOAT_FG = {
                        "1": "#000", "2": "#fff", "3": "#fff",
                        "4": "#fff", "5": "#000", "6": "#fff",
                    }
                    # 5×5 グリッド構築 (row0=top=Y5, col0=left=X1)
                    grid: dict[tuple[int, int], list[tuple[str, str]]] = {}
                    for boat, pos in positions.items():
                        key = (pos["row"], pos["col"])
                        trend = pos.get("trend", "")
                        arrow = "↑" if trend == "up" else ("↓" if trend == "down" else "")
                        grid.setdefault(key, []).append((boat, arrow))

                    y_labels = ["5", "4", "3", "2", "1"]
                    chart_rows = ""
                    for r in range(5):
                        cells = ""
                        for c in range(5):
                            boats_in_cell = grid.get((r, c), [])
                            if boats_in_cell:
                                badges = ""
                                for b, arr in boats_in_cell:
                                    bg = _BOAT_BG.get(b, "#666")
                                    fg = _BOAT_FG.get(b, "#fff")
                                    border = "border:1.5px solid #888;" if b == "1" else ""
                                    arr_html = ""
                                    if arr:
                                        arr_color = "#ff4444" if arr == "↑" else "#4488ff"
                                        arr_html = (
                                            f'<span style="position:absolute;top:-5px;right:-4px;'
                                            f'font-size:0.65rem;color:{arr_color};font-weight:bold;'
                                            f'text-shadow:0 0 2px #000">{arr}</span>'
                                        )
                                    badges += (
                                        f'<span style="display:inline-block;position:relative;'
                                        f'margin:1px">'
                                        f'<span style="display:inline-flex;align-items:center;'
                                        f'justify-content:center;background:{bg};color:{fg};{border}'
                                        f'border-radius:50%;width:22px;height:22px;font-weight:bold;'
                                        f'font-size:0.72rem">{b}</span>'
                                        f'{arr_html}</span>'
                                    )
                                cells += f'<td style="text-align:center;padding:2px;min-width:30px">{badges}</td>'
                            else:
                                cells += '<td style="padding:2px;min-width:30px"></td>'
                        y_lbl = y_labels[r]
                        chart_rows += (
                            f'<tr><td style="color:#7ab8e8;font-size:0.7rem;padding-right:4px;'
                            f'text-align:right;vertical-align:middle">{y_lbl}</td>{cells}</tr>'
                        )

                    x_labels_row = '<tr><td></td>'
                    for x in range(1, 6):
                        x_labels_row += f'<td style="color:#7ab8e8;font-size:0.7rem;text-align:center">{x}</td>'
                    x_labels_row += '</tr>'

                    st.markdown(
                        f'<div style="background:#0e1a2e;border:1px solid #2a4a80;border-radius:8px;'
                        f'padding:8px 6px 4px 6px">'
                        f'<div style="color:#7ab8e8;font-size:0.75rem;text-align:center;margin-bottom:4px">'
                        f'評価チャート</div>'
                        f'<div style="display:flex;align-items:center">'
                        f'<div style="writing-mode:vertical-rl;color:#7ab8e8;font-size:0.65rem;'
                        f'letter-spacing:2px;margin-right:2px">ターンの雰囲気</div>'
                        f'<table style="border-collapse:collapse">{chart_rows}{x_labels_row}</table>'
                        f'</div>'
                        f'<div style="color:#7ab8e8;font-size:0.65rem;text-align:center;margin-top:2px">'
                        f'スリット付近の勢い →</div>'
                        f'</div>',
                        unsafe_allow_html=True
                    )

                # スリット順
                slit = taka.get("slit_order", [])
                if slit:
                    slit_badges = ""
                    for s in slit:
                        bg = _BOAT_BG.get(str(s), "#666") if positions else "#555"
                        fg = _BOAT_FG.get(str(s), "#fff") if positions else "#fff"
                        border = "border:1.5px solid #888;" if str(s) == "1" else ""
                        slit_badges += (
                            f'<span style="display:inline-flex;align-items:center;'
                            f'justify-content:center;background:{bg};color:{fg};{border}'
                            f'border-radius:50%;width:20px;height:20px;font-weight:bold;'
                            f'font-size:0.7rem;margin:0 1px">{s}</span>'
                        )
                    st.markdown(
                        f'<div style="background:#12301a;border-left:3px solid #2ecc71;'
                        f'padding:0.5rem 0.8rem;border-radius:6px;margin-top:0.6rem">'
                        f'<div style="color:#7ab8e8;font-size:0.7rem;margin-bottom:4px">スリット順</div>'
                        f'<div>{slit_badges}</div>'
                        f'</div>',
                        unsafe_allow_html=True
                    )

            # スコア反映注記
            if taka.get("chart_scores"):
                st.caption("※ 高橋アナ評価チャートはAIスコアに反映済みです")
        elif _venue.get("has_taka_yoso"):
            st.markdown(
                '<div style="background:#151f35;border:1px solid #2a4a80;border-radius:8px;'
                'padding:1rem;color:#666;text-align:center">'
                '高橋アナ予想は未発表またはデータ取得外です。<br>'
                '<small>レース直前（約1〜2時間前）に「予想実行」を再クリックすると取得できる場合があります</small>'
                '</div>',
                unsafe_allow_html=True
            )

        st.markdown("---")

        # ── 選手別決まり手 ────────────────────────────────────────────
        racer_km = st.session_state.racer_km or {}
        if racer_km:
            st.markdown("### 🎯 選手別決まり手（コース別）")
            if True:
                _KIMARITE_COLORS = {
                    "逃げ": "#e74c3c", "差し": "#3498db", "まくり": "#f1c40f",
                    "まくり差し": "#2ecc71", "抜き": "#9b59b6", "恵まれ": "#95a5a6",
                }
                for _, row in scored.iterrows():
                    frame = str(row["枠番"])
                    rk = racer_km.get(frame)
                    if not rk:
                        continue
                    name = row.get("選手名", "")
                    # 枠番色
                    _FRAME_BG = {"1": "#fff", "2": "#000", "3": "#e74c3c",
                                 "4": "#3498db", "5": "#f1c40f", "6": "#2ecc71"}
                    _FRAME_FG = {"1": "#000", "2": "#fff", "3": "#fff",
                                 "4": "#fff", "5": "#000", "6": "#fff"}
                    fbg = _FRAME_BG.get(frame, "#666")
                    ffg = _FRAME_FG.get(frame, "#fff")
                    fborder = "border:1.5px solid #888;" if frame == "1" else ""

                    # 決まり手バーを構築
                    bars_html = ""
                    for kt_name in ["逃げ", "差し", "まくり", "まくり差し", "抜き", "恵まれ"]:
                        pct = rk.get(kt_name, 0.0)
                        if pct <= 0:
                            continue
                        color = _KIMARITE_COLORS.get(kt_name, "#888")
                        bars_html += (
                            f'<div style="display:flex;align-items:center;margin:1px 0">'
                            f'<span style="color:#7ab8e8;font-size:0.7rem;width:55px;text-align:right;'
                            f'margin-right:6px">{kt_name}</span>'
                            f'<div style="flex:1;background:#1a2744;border-radius:3px;height:14px;overflow:hidden">'
                            f'<div style="width:{min(pct, 100):.1f}%;height:100%;background:{color};'
                            f'border-radius:3px"></div></div>'
                            f'<span style="color:#fff;font-size:0.72rem;width:42px;text-align:right;'
                            f'margin-left:6px">{pct:.1f}%</span>'
                            f'</div>'
                        )
                    # 1号艇: 負け視点の決まり手（差され・捲られ・捲られ差）
                    _MAKE_COLORS = {
                        "差され": "#e67e22", "捲られ": "#d35400", "捲られ差": "#e74c3c",
                    }
                    if frame == "1":
                        for kt_name in ["差され", "捲られ", "捲られ差"]:
                            pct = rk.get(kt_name, 0.0)
                            if pct <= 0:
                                continue
                            color = _MAKE_COLORS.get(kt_name, "#e67e22")
                            bars_html += (
                                f'<div style="display:flex;align-items:center;margin:1px 0">'
                                f'<span style="color:#e8a87a;font-size:0.7rem;width:55px;text-align:right;'
                                f'margin-right:6px">{kt_name}</span>'
                                f'<div style="flex:1;background:#2a1a0a;border-radius:3px;height:14px;overflow:hidden">'
                                f'<div style="width:{min(pct, 100):.1f}%;height:100%;background:{color};'
                                f'border-radius:3px"></div></div>'
                                f'<span style="color:#e8a87a;font-size:0.72rem;width:42px;text-align:right;'
                                f'margin-left:6px">{pct:.1f}%</span>'
                                f'</div>'
                            )

                    st.markdown(
                        f'<div style="background:#0e1a2e;border:1px solid #2a4a80;border-radius:6px;'
                        f'padding:8px 10px;margin:4px 0">'
                        f'<div style="display:flex;align-items:center;margin-bottom:4px">'
                        f'<span style="display:inline-flex;align-items:center;justify-content:center;'
                        f'background:{fbg};color:{ffg};{fborder}border-radius:50%;'
                        f'width:22px;height:22px;font-weight:bold;font-size:0.8rem;margin-right:6px">'
                        f'{frame}</span>'
                        f'<span style="color:#fff;font-weight:bold;font-size:0.85rem">{name}</span>'
                        f'</div>'
                        f'{bars_html}'
                        f'</div>',
                        unsafe_allow_html=True,
                    )
        else:
            st.markdown("### 🎯 選手別決まり手（コース別）")
            st.caption("選手別決まり手データを取得できませんでした")

        # ── 3連単オッズ一覧表（expander） ──────────────────────────
        st.markdown("---")

        @st.fragment
        def _render_odds_section():
            """3連単オッズ一覧セクション（オッズ更新時はここだけ再描画）"""
            _odds_cur = st.session_state.odds or {}
            if _odds_cur:
                _hdr_col1, _hdr_col2 = st.columns([3, 1])
                with _hdr_col1:
                    _lr2_text = ""
                    if st.session_state.odds_last_refresh_time > 0:
                        _lr2 = datetime.fromtimestamp(st.session_state.odds_last_refresh_time, tz=_JST_TZ)
                        _lr2_text = f'<span style="color:#888;font-size:0.75rem;margin-left:12px;white-space:nowrap">最終更新: {_lr2.strftime("%H:%M:%S")}</span>'
                    st.markdown(f'<h3 style="display:flex;align-items:baseline;flex-wrap:nowrap;white-space:nowrap;font-size:1.1rem">📊 3連単オッズ一覧{_lr2_text}</h3>', unsafe_allow_html=True)
                with _hdr_col2:
                    if st.session_state.race_no and st.session_state.date_str:
                        if st.button("オッズ更新", key="odds_refresh_btn2", use_container_width=True):
                            _ar_rno = st.session_state.race_no
                            _ar_dstr = st.session_state.date_str
                            _ar_odds = fetch_odds_3t(_ar_rno, _ar_dstr, jycd=_selected_venue_code)
                            if _ar_odds:
                                st.session_state.odds = _ar_odds
                                _odds_cur = _ar_odds
                                _ar_3t = st.session_state.result.get("all_3t_candidates", [])
                                for _c in _ar_3t:
                                    _actual = _ar_odds.get(_c["買い目"])
                                    _c["実オッズ"] = _actual
                                    if _actual is not None and _c["的中確率"] > 0:
                                        _c["期待値"] = (_c["的中確率"] / 100) * _actual
                                    else:
                                        _c["期待値"] = None
                                st.session_state.result["all_3t_candidates"] = _ar_3t
                                for _rec in st.session_state.result.get("recommendations", []):
                                    _rec_key = _rec.get("買い目", "")
                                    _rec_actual = _ar_odds.get(_rec_key)
                                    _rec["実オッズ"] = _rec_actual
                                    if _rec_actual is not None and _rec.get("的中確率", 0) > 0:
                                        _rec["期待値"] = (_rec["的中確率"] / 100) * _rec_actual
                                    else:
                                        _rec["期待値"] = None
                                st.session_state.odds_last_refresh_time = time.time()
                odds = _odds_cur
                _OBG = {
                    "1": "#fff", "2": "#000", "3": "#e74c3c",
                    "4": "#3498db", "5": "#f1c40f", "6": "#2ecc71",
                }
                _OFG = {
                    "1": "#000", "2": "#fff", "3": "#fff",
                    "4": "#fff", "5": "#000", "6": "#fff",
                }
                _rnames = {}
                for _, _rw in scored.iterrows():
                    _rnames[str(int(_rw["枠番"]))] = _rw.get("選手名", "")

                _boats = [1, 2, 3, 4, 5, 6]
                _CB = "border:1px solid #aaa;"

                # 各1着艇ごとの2着候補（昇順）
                _sec_map = {
                    f: sorted(b for b in _boats if b != f) for f in _boats
                }

                # ── ヘッダー行（列＝1着艇、各列 colspan=3） ──
                _hdr = '<tr>'
                for _b in _boats:
                    _bg = _OBG[str(_b)]
                    _fg = _OFG[str(_b)]
                    _nm = _rnames.get(str(_b), "")
                    # 選手名は姓のみ（2文字まで）
                    _nm_short = _nm.split()[0][:2] if _nm else ""
                    _hdr += (
                        f'<th colspan="3" style="{_CB}background:{_bg};'
                        f'color:{_fg};padding:2px 1px;font-size:0.58rem;'
                        f'text-align:center;font-weight:bold;'
                        f'white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:0">{_b}{_nm_short}</th>'
                    )
                _hdr += '</tr>'

                # オッズ値のランキング（低い順トップ2を算出）
                _all_odds_vals = sorted(set(v for v in odds.values() if v is not None and v > 0))
                _odds_top1 = _all_odds_vals[0] if len(_all_odds_vals) >= 1 else None
                _odds_top2 = _all_odds_vals[1] if len(_all_odds_vals) >= 2 else None

                # ── データ行（5グループ × 4サブ行 = 20行） ──
                # グループ = 2着艇インデックス、サブ行 = 3着艇インデックス
                _body = ''
                for _gi in range(5):
                    for _ri in range(4):
                        _tr = '<tr>'
                        for _f in _boats:
                            _s = _sec_map[_f][_gi]
                            _thirds = sorted(
                                b for b in _boats if b != _f and b != _s
                            )
                            _t = _thirds[_ri]
                            _combo = f"{_f}-{_s}-{_t}"

                            # 2着セル（グループ先頭のみ rowspan=4）
                            if _ri == 0:
                                _bg2 = _OBG[str(_s)]
                                _fg2 = _OFG[str(_s)]
                                _tr += (
                                    f'<td rowspan="4" style="{_CB}'
                                    f'background:{_bg2};color:{_fg2};'
                                    f'text-align:center;font-weight:bold;'
                                    f'font-size:0.6rem;padding:1px 1px;'
                                    f'width:14px;vertical-align:middle">'
                                    f'{_s}</td>'
                                )

                            # 3着セル
                            _bg3 = _OBG[str(_t)]
                            _fg3 = _OFG[str(_t)]
                            _tr += (
                                f'<td style="{_CB}background:{_bg3};'
                                f'color:{_fg3};text-align:center;'
                                f'font-weight:bold;font-size:0.6rem;'
                                f'padding:1px 1px;width:14px">{_t}</td>'
                            )

                            # オッズセル（1番人気=赤、2番人気=黄、999倍以上=紫で強調）
                            _oval = odds.get(_combo)
                            if _oval is not None:
                                # 999倍超は全て999表示
                                _ostr = "999" if _oval > 999 else f"{_oval:.0f}" if _oval >= 100 else f"{_oval:.1f}"
                                if _oval == _odds_top1:
                                    _ocss = (f'{_CB}background:#fff;'
                                             f'color:#e74c3c;text-align:right;'
                                             f'font-size:0.58rem;padding:1px 2px;'
                                             f'white-space:nowrap;font-weight:bold')
                                elif _oval == _odds_top2:
                                    _ocss = (f'{_CB}background:#fff;'
                                             f'color:#f39c12;text-align:right;'
                                             f'font-size:0.58rem;padding:1px 2px;'
                                             f'white-space:nowrap;font-weight:bold')
                                elif _oval >= 999:
                                    _ocss = (f'{_CB}background:#fff;'
                                             f'color:#8e44ad;text-align:right;'
                                             f'font-size:0.58rem;padding:1px 2px;'
                                             f'white-space:nowrap;font-weight:bold')
                                else:
                                    _ocss = (f'{_CB}background:#fff;'
                                             f'color:#000;text-align:right;'
                                             f'font-size:0.58rem;padding:1px 2px;'
                                             f'white-space:nowrap')
                                _tr += f'<td style="{_ocss}">{_ostr}</td>'
                            else:
                                _tr += (
                                    f'<td style="{_CB}background:#fff;'
                                    f'color:#999;text-align:right;'
                                    f'font-size:0.58rem;padding:1px 2px">'
                                    f'-</td>'
                                )

                        _tr += '</tr>'
                        _body += _tr

                _odds_tbl = (
                    f'<table style="border-collapse:collapse;width:100%;'
                    f'table-layout:fixed;background:#fff">{_hdr}{_body}</table>'
                )
                st.markdown(
                    f'<div class="odds-3t-wrap" style="overflow-x:hidden;'
                    f'">{_odds_tbl}</div>',
                    unsafe_allow_html=True,
                )

            else:
                st.markdown("### 📊 3連単オッズ一覧")
                st.info("3連単オッズはまだ公開されていません。締切が近づくと表示されます。")

        _render_odds_section()

        # ── 艇別パフォーマンスレーダーチャート（expander） ─────────
        st.markdown("---")
        st.markdown("### 📡 艇別パフォーマンスレーダーチャート")

        def _make_radar_chart(df_scored: pd.DataFrame) -> go.Figure:
            """scored DataFrameから艇別レーダーチャートを生成する。"""
            dims = [
                ("当地勝率",   "当地勝率",   False),
                ("当地2連率",  "当地2連率",  False),
                ("展示タイム", "展示タイム", True),
                ("モーター2連率", "モーター",  False),
                ("スタートタイミング", "ST速さ", True),
                ("全国勝率",   "全国勝率",    False),
                ("コース別1着率", "C別1着率", False),
                ("直近平均着順", "好調度", True),    # 低いほど好調→反転
            ]

            # 各次元の有効値を0-100正規化（欠損値は除外して正規化）
            # 競艇公式カラー: 1白, 2黒, 3赤, 4青, 5黄, 6緑
            boat_colors = ["#cccccc", "#666666", "#e74c3c", "#2563eb", "#d4a017", "#2ecc71"]
            radar_dims: list[str] = []
            radar_data: dict[str, list[float]] = {}  # frame -> normalized values per dim

            frames = df_scored["枠番"].astype(str).values
            for col, label, invert in dims:
                if col not in df_scored.columns:
                    continue
                vals = []
                for v in df_scored[col].values:
                    try:
                        fv = float(v)
                        vals.append(fv if not np.isnan(fv) else 0.0)
                    except (TypeError, ValueError):
                        vals.append(0.0)

                # 有効値（>0）のみで正規化範囲を決定
                valid_vals = [v for v in vals if v > 0]
                if len(valid_vals) < 2:
                    continue  # 有効データが2件未満の次元はスキップ
                vmin, vmax = min(valid_vals), max(valid_vals)
                rng = (vmax - vmin) or 1.0
                FLOOR = 25  # 最低値を25に底上げ（視覚的な誇張を緩和）
                normed = [((v - vmin) / rng * (100 - FLOOR) + FLOOR if v > 0 else -1.0) for v in vals]
                if invert:
                    normed = [(100 - x if x >= 0 else -1.0) for x in normed]

                # 欠損艇は 50 に補正
                for i in range(len(normed)):
                    if normed[i] < 0:
                        normed[i] = 50.0

                radar_dims.append(label)
                for i, f in enumerate(frames):
                    radar_data.setdefault(f, []).append(normed[i])

            if not radar_dims:
                return go.Figure()

            fig = go.Figure()
            for i, frame in enumerate(frames):
                values = radar_data.get(frame, [50.0] * len(radar_dims))
                # 閉じた図形にするために先頭要素を末尾に追加
                color = boat_colors[i % len(boat_colors)]
                fig.add_trace(go.Scatterpolar(
                    r=values + [values[0]],
                    theta=radar_dims + [radar_dims[0]],
                    fill="toself",
                    fillcolor=color,
                    line=dict(color=color, width=2.5),
                    opacity=0.4,
                    name=f"{frame}号艇",
                ))

            fig.update_layout(
                polar=dict(
                    radialaxis=dict(visible=True, range=[0, 100], showticklabels=False),
                    angularaxis=dict(
                        tickfont=dict(size=9, color="#7ab8e8"),
                        rotation=90,
                        direction="clockwise",
                    ),
                    bgcolor="#0e1a2e",
                ),
                showlegend=True,
                legend=dict(
                    font=dict(color="#e8f4ff", size=10),
                    bgcolor="rgba(14,26,46,0.8)",
                    orientation="h",
                    yanchor="top",
                    y=-0.05,
                    xanchor="center",
                    x=0.5,
                ),
                paper_bgcolor="#0e1a2e",
                margin=dict(l=80, r=80, t=50, b=60),
                height=420,
            )
            return fig

        radar_fig = _make_radar_chart(scored)
        if radar_fig.data:
            st.plotly_chart(radar_fig, use_container_width=True, config={"displayModeBar": False})
        else:
            st.caption("レーダーチャートを表示するのに十分なデータがありません")

        st.markdown("---")
        # ── 予想パラメータ一覧 ────────────────────────────────────────
        with st.expander("📐 予想に使用しているパラメータ一覧", expanded=False):
            _W = VENUE_CONFIGS[_selected_venue_code]["score_weights"]
            _G = VENUE_CONFIGS[_selected_venue_code]["settings"]

            param_groups = [
                ("コース・勝率", [
                    ("コース基礎確率", f'{_W["course_base"]}'),
                    ("全国勝率の重み", f'{_W["win_rate"]}'),
                    ("当地勝率の重み", f'{_W["local_win_rate"]}'),
                    ("全国2連率の重み", f'{_W["nat2_rate"]}'),
                    ("当地2連率の重み", f'{_W["loc2_rate"]}'),
                    ("コース別1着率の重み", f'{_W["course_win_rate"]}'),
                ]),
                ("展示・機力", [
                    ("展示タイム偏差の重み", f'{_W["exhibit_time"]}'),
                    ("展示1位ボーナス", f'{_W["exhibit_top_bonus"]}'),
                    ("モーター2連率の重み", f'{_W["motor2_rate"]}'),
                    ("ボート2連率の重み", f'{_W["boat2_rate"]}'),
                    ("まわり足タイムの重み", f'{_W["mawari_time"]}'),
                    ("直線タイムの重み", f'{_W["chokusen_time"]}'),
                    ("一周タイムの重み", f'{_W["lap_time"]}'),
                    ("ターン巧者ボーナス", f'{_W["turn_master_bonus"]}'),
                ]),
                ("スタート", [
                    ("STの重み", f'{_W["st_weight"]}'),
                    ("フライングペナルティ", f'{_W["st_fly_penalty"]}'),
                    ("F持ちペナルティ", f'{_W["fl_f_penalty"]}'),
                    ("L持ちペナルティ", f'{_W["fl_l_penalty"]}'),
                ]),
                ("気象・コンディション", [
                    ("ナイター補正（1号艇）", f'{_W["night_boost"]}'),
                    ("静水面イン加点", f'{_W["calm_in_boost"]}'),
                    ("カドまくり補正（4号艇）", f'{_W["kado_boost"]}'),
                    ("まくり差し補正（3号艇）", f'{_W["makuri_sashi"]}'),
                    ("体重（静水面）ボーナス", f'{_W["weight_calm"]}'),
                    ("体重（荒天）ボーナス", f'{_W["weight_rough"]}'),
                    ("無風判定閾値", f'{_G["calm_wind_threshold"]}m'),
                    ("ナイター開始R", f'{_G["night_race_start"]}R以降'),
                    ("安定板インコース加点", f'{_W.get("stabilizer_in_boost", 3.0)}'),
                    ("安定板スコア均等化率", f'{_W.get("stabilizer_equalize", 0.15)}'),
                    ("安定板展示タイム割引率", f'{_W.get("stabilizer_et_discount", 0.6)}'),
                ]),
                ("モデル・予想生成", [
                    ("直近成績モメンタムの重み", f'{_W["momentum"]}'),
                    ("優勝戦イン強化", f'{_W["grade_final_boost"]}'),
                    ("高橋アナ予想ブースト", f'{_W["taka_boost"]}'),
                    ("選手別決まり手適合度の重み", f'{_W.get("racer_kimarite_weight", 3.0)}'),
                    ("決まり手連動2着補正", f'{_W.get("kimarite_placement_weight", 0.5)}'),
                    ("ソフトマックス温度", f'{_W.get("individual_temp", 15.0)}'),
                    ("Henery gamma", f'{_W["henery_gamma"]}'),
                    ("期待値閾値（穴選定）", f'{_W["ev_threshold"]}'),
                    ("穴候補の確率下限", f'{_W.get("ana_min_prob", 0.5)}%'),
                    ("穴候補の公正オッズ上限", f'{_W.get("ana_max_fair_odds", 80)}'),
                ]),
            ]

            for group_name, params in param_groups:
                st.markdown(
                    f'<div style="color:#f0a500;font-size:0.85rem;font-weight:bold;'
                    f'margin-top:0.6rem;margin-bottom:0.3rem">{group_name}</div>',
                    unsafe_allow_html=True,
                )
                for name, value in params:
                    st.markdown(
                        f'<div style="background:#1a2744;border-left:3px solid #2a4a80;'
                        f'padding:4px 10px;margin:2px 0;border-radius:4px;font-size:0.8rem">'
                        f'<span style="color:#7ab8e8">{name}</span>'
                        f'<span style="color:#fff;float:right">{value}</span>'
                        f'</div>',
                        unsafe_allow_html=True,
                    )


else:
    # ══════════════════════════════════════════════════════════════
    #  出走表一覧モード
    # ══════════════════════════════════════════════════════════════

    # セッション初期化（出走表一覧用）
    if "shutsusou_data" not in st.session_state:
        st.session_state.shutsusou_data = None
    if "shutsusou_date" not in st.session_state:
        st.session_state.shutsusou_date = None
    if "_shutsusou_error" not in st.session_state:
        st.session_state._shutsusou_error = None

    # 枠番カラー
    _WAKU_COLORS = {
        1: ("#fff", "#000"),    # 白
        2: ("#000", "#fff"),    # 黒
        3: ("#e03030", "#fff"), # 赤
        4: ("#2060d0", "#fff"), # 青
        5: ("#d0c020", "#000"), # 黄
        6: ("#20a040", "#fff"), # 緑
    }

    def _on_stop_shutsusou_click():
        st.session_state.running = False
        st.session_state.pop("_exec_shutsusou_date", None)

    _shutsusou_settings_ph = st.empty()
    _shutsusou_form_ph = st.empty()

    _hide_shutsusou_settings = st.session_state.running

    if _hide_shutsusou_settings:
        _shutsusou_form_ph.empty()
        _s_run_date = st.session_state.get("_exec_shutsusou_date")
        _s_date_str = _s_run_date.strftime("%Y年%m月%d日") if _s_run_date and hasattr(_s_run_date, "strftime") else ""
        with _shutsusou_settings_ph.container():
            st.markdown(
                f'<div style="background:#1a2744;border:1px solid #1e5fa8;border-radius:8px;'
                f'padding:0.7rem 1rem;margin-bottom:0.5rem;text-align:center">'
                f'<span style="color:#7ab8e8;font-size:0.85rem">'
                f'📡 出走表一覧を取得中…</span>'
                f'<div style="margin-top:0.4rem">'
                f'<span style="display:inline-block;width:22px;height:22px;'
                f'border:3px solid rgba(122,184,232,0.3);border-top:3px solid #7ab8e8;'
                f'border-radius:50%;animation:spin 1s linear infinite"></span></div>'
                f'</div>'
                f'<style>@keyframes spin {{from{{transform:rotate(0deg)}}to{{transform:rotate(360deg)}}}}</style>',
                unsafe_allow_html=True,
            )
            st.button("⏹ 取得を停止", use_container_width=True, key="stop_btn_shutsusou_init",
                      on_click=_on_stop_shutsusou_click)
        shutsusou_date = _s_run_date
        fetch_shutsusou = False
    else:
        with _shutsusou_settings_ph.container():
            st.markdown(
                '<div style="background:#1a2744;border:1px solid #1e5fa8;border-radius:8px;'
                'padding:0.8rem 1rem;margin-bottom:0.5rem">'
                '<span style="color:#7ab8e8;font-size:0.9rem;font-weight:bold">出走表一覧</span>'
                '</div>',
                unsafe_allow_html=True,
            )
        with _shutsusou_form_ph.container():
            _d_param_s = st.query_params.get("d")
            if _d_param_s:
                try:
                    _default_date_s = datetime.strptime(_d_param_s, "%Y%m%d").date()
                except ValueError:
                    _default_date_s = _today_jst()
            else:
                _default_date_s = _today_jst()

            _date_options_s = [_today_jst() + timedelta(days=i) for i in range(-3, 4)]
            _date_labels_s = {d: d.strftime("%m/%d") + ("(今日)" if d == _today_jst() else "") for d in _date_options_s}
            _default_pill_s = _default_date_s if _default_date_s in _date_options_s else _today_jst()
            shutsusou_date = st.pills("開催日", _date_options_s, default=_default_pill_s, key="shutsusou_date_input", disabled=_ui_disabled,
                                      format_func=lambda d: _date_labels_s[d])

            def _on_shutsusou_click():
                st.session_state.running = True
                st.session_state._exec_shutsusou_date = shutsusou_date
            fetch_shutsusou = st.button("▶ 出走表を取得", type="primary", use_container_width=True, disabled=_ui_disabled,
                                        on_click=_on_shutsusou_click)
            # 前回実行時のエラーメッセージを表示
            if st.session_state._shutsusou_error:
                st.error(st.session_state._shutsusou_error)
                st.session_state._shutsusou_error = None

    # running フラグが立っている場合（設定パネルが非表示でも）取得を実行
    _shutsusou_run_from_state = False
    if st.session_state.running and not fetch_shutsusou:
        shutsusou_date = st.session_state.get("_exec_shutsusou_date")
        if shutsusou_date is not None:
            _shutsusou_run_from_state = True

    if fetch_shutsusou or _shutsusou_run_from_state:
        _shutsusou_form_ph.empty()
        if _shutsusou_run_from_state:
            _shutsusou_settings_ph.empty()
            _s_date_str2 = shutsusou_date.strftime("%Y年%m月%d日") if hasattr(shutsusou_date, "strftime") else ""
            with _shutsusou_settings_ph.container():
                st.markdown(
                    f'<div style="background:#1a2744;border:1px solid #1e5fa8;border-radius:8px;'
                    f'padding:0.7rem 1rem;margin-bottom:0.5rem;text-align:center">'
                    f'<span style="color:#7ab8e8;font-size:0.85rem">'
                    f'📡 {_s_date_str2} の出走表一覧を取得中…</span>'
                    f'<div style="margin-top:0.4rem">'
                    f'<span style="display:inline-block;width:22px;height:22px;'
                    f'border:3px solid rgba(122,184,232,0.3);border-top:3px solid #7ab8e8;'
                    f'border-radius:50%;animation:spin 1s linear infinite"></span></div>'
                    f'</div>'
                    f'<style>@keyframes spin {{from{{transform:rotate(0deg)}}to{{transform:rotate(360deg)}}}}</style>',
                    unsafe_allow_html=True,
                )
                st.button("⏹ 取得を停止", use_container_width=True, key="stop_btn_shutsusou_exec",
                          on_click=_on_stop_shutsusou_click)
        d_str_s = shutsusou_date.strftime("%Y%m%d")
        progress = st.progress(0, text="⏳ 全レースの出走表を取得中...")

        all_race_data = {}
        import requests as _requests
        _session = _requests.Session()
        _session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"})
        with ThreadPoolExecutor(max_workers=12, initializer=set_thread_venue, initargs=(_selected_venue_code,)) as executor:
            futures = {
                executor.submit(fetch_race_card, rno, d_str_s, session=_session): rno
                for rno in range(1, 13)
            }
            done = 0
            for future in as_completed(futures):
                rno = futures[future]
                done += 1
                progress.progress(
                    int(done / 12 * 100),
                    text=f"⏳ 出走表取得中... {done}/12レース完了",
                )
                try:
                    df = future.result()
                    if not df.empty:
                        all_race_data[rno] = df
                except Exception:
                    pass
        _session.close()

        if all_race_data:
            progress.progress(100, text="✅ 出走表取得完了")
            time.sleep(0.3)
            progress.empty()

            st.session_state.shutsusou_data = all_race_data
            st.session_state.shutsusou_date = d_str_s
            st.session_state.running = False
            st.session_state.pop("_exec_shutsusou_date", None)
            st.rerun()
        else:
            progress.empty()
            st.session_state.shutsusou_data = None
            st.session_state.shutsusou_date = None
            st.session_state.running = False
            st.session_state.pop("_exec_shutsusou_date", None)
            st.session_state._shutsusou_error = "出走表を取得できませんでした。該当日にレースが開催されていない可能性があります。日付を変更して再度お試しください。"
            st.rerun()

    # ── 安全リセット: 実行フラグが残っていたら解除 ────────────────────
    if st.session_state.running:
        st.session_state.running = False

    # ── 出走表表示 ─────────────────────────────────────────────
    if st.session_state.shutsusou_data:
        _sd = st.session_state.shutsusou_date or ""
        _sd_fmt = f"{_sd[:4]}/{_sd[4:6]}/{_sd[6:]}" if len(_sd) == 8 else _sd
        st.markdown(
            f'<div style="color:#7ab8e8;font-size:0.85rem;margin-bottom:0.5rem">'
            f'📅 {_sd_fmt} {_venue["short_name"]}ボートレース 全レース出走表</div>',
            unsafe_allow_html=True,
        )

        for rno in range(1, 13):
            df = st.session_state.shutsusou_data.get(rno)
            if df is None:
                continue

            # レース番号ヘッダ
            st.markdown(
                f'<div style="color:#fff;font-weight:bold;font-size:0.95rem;'
                f'margin-top:0.8rem;margin-bottom:0.3rem;padding:4px 8px;'
                f'background:#1a3a6a;border-radius:4px">{rno}R</div>',
                unsafe_allow_html=True,
            )
            # HTMLテーブルで表示
            html_rows = []
            for _, row in df.iterrows():
                waku = int(row.get("枠番", 0))
                bg, fg = _WAKU_COLORS.get(waku, ("#555", "#fff"))
                name = row.get("選手名", "")
                rank = row.get("級別", "")
                nw = row.get("全国勝率", "-")
                lw = row.get("当地勝率", "-")
                m2 = row.get("モーター2連率", "-")
                b2 = row.get("ボート2連率", "-")
                st_val = row.get("スタートタイミング", "-")

                nw = f"{nw:.2f}" if isinstance(nw, (int, float)) and nw is not None else "-"
                lw = f"{lw:.2f}" if isinstance(lw, (int, float)) and lw is not None else "-"
                m2 = f"{m2:.1f}" if isinstance(m2, (int, float)) and m2 is not None else "-"
                b2 = f"{b2:.1f}" if isinstance(b2, (int, float)) and b2 is not None else "-"
                st_val = f"{st_val:.2f}" if isinstance(st_val, (int, float)) and st_val is not None else "-"

                html_rows.append(
                    f'<tr>'
                    f'<td style="background:{bg};color:{fg};text-align:center;'
                    f'font-weight:bold;width:22px;border-radius:4px">{waku}</td>'
                    f'<td style="color:#fff;font-weight:bold;padding-left:4px;white-space:nowrap">{name}</td>'
                    f'<td style="color:#7ab8e8;text-align:center;font-size:0.7rem">{rank}</td>'
                    f'<td style="color:#cce0ff;text-align:center">{nw}</td>'
                    f'<td style="color:#cce0ff;text-align:center">{lw}</td>'
                    f'<td style="color:#cce0ff;text-align:center">{m2}</td>'
                    f'<td style="color:#cce0ff;text-align:center">{b2}</td>'
                    f'<td style="color:#cce0ff;text-align:center">{st_val}</td>'
                    f'</tr>'
                )

            _th = ('style="color:#7ab8e8;font-size:0.6rem;padding:3px 1px;'
                   'text-align:center;white-space:nowrap;line-height:1.2"')
            _sub = 'style="display:block;font-size:0.5rem;color:#5a9ad0;font-weight:normal"'
            table_html = (
                '<table style="width:100%;border-collapse:collapse;font-size:0.78rem;margin:0">'
                '<thead><tr style="border-bottom:1px solid #2a4a80">'
                f'<th {_th}>枠</th>'
                f'<th {_th}>選手名</th>'
                f'<th {_th}>級別</th>'
                f'<th {_th}>全国<span {_sub}>勝率</span></th>'
                f'<th {_th}>当地<span {_sub}>勝率</span></th>'
                f'<th {_th}>ﾓｰﾀ<span {_sub}>2連率</span></th>'
                f'<th {_th}>ﾎﾞｰﾄ<span {_sub}>2連率</span></th>'
                f'<th {_th}>ST</th>'
                '</tr></thead>'
                '<tbody>' + ''.join(html_rows) + '</tbody></table>'
            )
            st.markdown(table_html, unsafe_allow_html=True)

# ── フッター（バージョン情報）────────────────────────────────────
st.markdown("---")
_has_result = "あり" if st.session_state.get("result") is not None else "なし"
st.markdown(
    f'<div style="text-align:center;color:#555;font-size:0.7rem;padding:0.5rem 0">'
    f'{APP_VERSION} | 結果: {_has_result}'
    f'</div>',
    unsafe_allow_html=True,
)
