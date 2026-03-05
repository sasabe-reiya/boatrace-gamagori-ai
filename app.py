"""
蒲郡ボートレース予想Webアプリ - UIメイン（v3）
【v3 追加機能】
- 推奨買い目を5つ（本命・対抗・穴・注目・参考）まで表示
- 「期待値スコア」→ 的中確率(%)・公正オッズ・実オッズ・期待値 に変更
- 3連単オッズをリアルタイム取得して各買い目カードに表示
- 期待値 > 1.0 の買い目は強調表示（バリュー買い目）
"""
import sys
import os
import hashlib
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

sys.path.insert(0, os.path.dirname(__file__))
from config import JYNAME
from race_scraper import (
    fetch_full_race_data, fetch_race_card,
    fetch_base_race_data, apply_extended_data, fetch_extended_player_data,
    fetch_deadline, fetch_odds_3t, fetch_odds_2tf, fetch_gamagori_taka,
    fetch_racer_kimarite, fetch_race_result, fetch_lady_racers,
)
from scorer import predict
from result_tracker import save_prediction

# ── 結果キャッシュ（セッション喪失対策）──────────────────────────
_CACHE_DIR = os.path.join(os.path.dirname(__file__), "_cache")
os.makedirs(_CACHE_DIR, exist_ok=True)

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

st.set_page_config(page_title="競艇予想AI レイヤドン", page_icon="🔱", layout="centered", initial_sidebar_state="collapsed")

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

# ── パスワード認証 ──────────────────────────────────────────────────
APP_PASSWORD = "sasabe"
_AUTH_TOKEN = hashlib.sha256(APP_PASSWORD.encode()).hexdigest()[:16]

def check_password():
    """パスワード認証。正しいパスワードが入力されるまでアプリを表示しない。
    認証後はURLにトークンを付与し、リロードしても再入力不要にする。"""
    if st.session_state.get("authenticated"):
        return True

    # URLのトークンで自動認証（リロード時）
    if st.query_params.get("token") == _AUTH_TOKEN:
        st.session_state["authenticated"] = True
        return True

    st.markdown(
        '<div style="max-width:400px;margin:80px auto;text-align:center">'
        '<h2 style="color:#e8f4ff;white-space:nowrap;font-size:1.4rem">🔱 競艇予想AI レイヤドン</h2>'
        '<p style="color:#5a9fd4;font-size:0.6rem;letter-spacing:4px;margin-top:-8px">― GAMAGORI BOATRACE ―</p>'
        '<p style="color:#7ab8e8">アクセスにはパスワードが必要です</p>'
        '</div>',
        unsafe_allow_html=True,
    )

    with st.form("login_form"):
        password = st.text_input("パスワード", type="password", placeholder="パスワードを入力")
        submitted = st.form_submit_button("ログイン", use_container_width=True, type="primary")

    if submitted:
        if password == APP_PASSWORD:
            st.session_state["authenticated"] = True
            st.query_params["token"] = _AUTH_TOKEN
            st.rerun()
        else:
            st.error("パスワードが正しくありません")

    return False

if not check_password():
    st.stop()

st.markdown("""
<style>
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
    .weather-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 6px; margin: 0.5rem 0; }
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
        h3 { font-size: 1.1rem !important; }

        /* ボタンを押しやすく */
        .stButton > button { min-height: 48px !important; font-size: 1rem !important; }
        .stSelectbox, .stDateInput { font-size: 1rem !important; }

        /* データテーブルの横スクロール */
        .stDataFrame { overflow-x: auto !important;  }
        .stDataFrame table { font-size: 0.75rem !important; }
        .stDataFrame th, .stDataFrame td { padding: 4px 6px !important; white-space: nowrap !important; }

        /* Plotlyチャート高さ調整 */
        .stPlotlyChart { max-height: 320px; }
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
      <h1>競艇予想AI レイヤドン</h1>
      <div class="logo-sub">― GAMAGORI BOATRACE ―</div>
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

app_mode = st.radio(
    "モード", ["予想", "出走表一覧"], horizontal=True,
    label_visibility="collapsed",
    key="app_mode",
    disabled=_ui_disabled,
)

if app_mode == "予想":
    # ══════════════════════════════════════════════════════════════
    #  予想モード（既存機能）
    # ══════════════════════════════════════════════════════════════

    # ── セッション初期化 ──────────────────────────────────────────────
    for key in ("result", "weather", "deadline", "race_no", "date_str", "odds", "odds_2t", "taka", "racer_km", "race_result", "lady_racers"):
        if key not in st.session_state:
            st.session_state[key] = None

    # ── レース間ナビゲーション（← 前R / 次R →）─────────────────────
    if "nav_race" not in st.session_state:
        st.session_state.nav_race = None  # None=通常, int=自動実行するレース番号

    def _go_prev_race():
        rno = st.session_state.race_no or 1
        if rno > 1:
            st.session_state.nav_race = rno - 1
            st.session_state.result = None  # 結果をクリアして再実行トリガー
            st.session_state.running = True

    def _go_next_race():
        rno = st.session_state.race_no or 1
        if rno < 12:
            st.session_state.nav_race = rno + 1
            st.session_state.result = None
            st.session_state.running = True

    # ── セッション喪失時のキャッシュ復元 ─────────────────────────────
    # Google Appなどのインアプリブラウザでセッションが切断された場合、
    # query_paramsのrace番号と日付からキャッシュを復元する
    if st.session_state.result is None:
        _restore_race = st.query_params.get("race", "1")
        _restore_date = date.today().strftime("%Y%m%d")
        _cached = _load_result_cache(_restore_race, _restore_date, _device_id)
        if _cached:
            st.session_state.result      = _cached["result"]
            st.session_state.weather     = _cached["weather"]
            st.session_state.deadline    = _cached["deadline"]
            st.session_state.race_no     = int(_restore_race)
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

    # ── レース設定パネル（メインエリア） ─────────────────────────────
    # プレースホルダーを使い、予想実行後に即座にたたむ（st.rerun()不要）
    _settings_ph = st.empty()

    if st.session_state.result is not None and not st.session_state.show_settings:
        # 予想後で設定非表示: トグルボタンだけ表示
        with _settings_ph.container():
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
            _saved_race = max(1, min(12, int(st.query_params.get("race", 1))))
            race_no   = st.radio("レース番号", list(range(1, 13)), index=_saved_race - 1, horizontal=True, format_func=lambda x: f"{x}R", disabled=_ui_disabled)
            if str(race_no) != st.query_params.get("race"):
                st.query_params["race"] = str(race_no)
            _d_param = st.query_params.get("d")
            if _d_param:
                try:
                    _default_date = datetime.strptime(_d_param, "%Y%m%d").date()
                except ValueError:
                    _default_date = date.today()
            else:
                _default_date = date.today()
            race_date = st.date_input("開催日", _default_date, disabled=_ui_disabled)
            fetch_btn  = st.button("▶ 予想実行", type="primary", use_container_width=True, disabled=_ui_disabled,
                                   on_click=lambda: st.session_state.update(running=True))

    # ── 予想実行（ナビゲーション経由の自動実行を含む）───────────────
    _nav_auto = False
    if st.session_state.nav_race is not None:
        race_no = st.session_state.nav_race
        race_date = date.today()
        st.session_state.nav_race = None
        st.query_params["race"] = str(race_no)
        _nav_auto = True

    if fetch_btn or _nav_auto:
        d_str = race_date.strftime("%Y%m%d")
        progress_bar = st.progress(0, text="⏳ データ取得を開始します...")

        # ── Phase 1: 独立した全HTTPリクエストを一括並列実行 ──────────
        # fetch_base_race_data は内部で3並列（soup+beforeinfo+gamagori_time）
        # その他5リクエストを同時に走らせる
        with ThreadPoolExecutor(max_workers=10) as executor:
            f_data     = executor.submit(fetch_base_race_data, race_no, d_str)
            f_odds     = executor.submit(fetch_odds_3t, race_no, d_str)
            f_odds_2tf = executor.submit(fetch_odds_2tf, race_no, d_str)
            f_taka     = executor.submit(fetch_gamagori_taka, race_no, d_str)
            f_rresult  = executor.submit(fetch_race_result, race_no, d_str)
            f_lady     = executor.submit(fetch_lady_racers, d_str)

            phase1_map = {
                f_data:     "出走表・直前データ",
                f_odds:     "3連単オッズ",
                f_odds_2tf: "2連単オッズ",
                f_taka:     "高橋アナ予想",
                f_rresult:  "レース結果",
                f_lady:     "女子選手情報",
            }
            done_count = 0
            total_tasks = 8  # Phase1: 6 + Phase2: 2
            for future in as_completed(phase1_map):
                done_count += 1
                name = phase1_map[future]
                pct = int((done_count / total_tasks) * 60)
                progress_bar.progress(pct, text=f"⏳ データ取得中... {name} 完了 ({done_count}/{total_tasks})")

            df_raw, weather, racelist_soup = f_data.result()
            odds     = f_odds.result()
            odds_2tf = f_odds_2tf.result()
            taka     = f_taka.result()
            race_result_data = f_rresult.result()
            lady_racers = f_lady.result()

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
            progress_bar.progress(int((done_count / total_tasks) * 60), text=f"⏳ 選手詳細データ取得中... ({done_count}/{total_tasks})")
            with ThreadPoolExecutor(max_workers=2) as executor:
                f_ext      = executor.submit(fetch_extended_player_data, reg_nos)
                f_racer_km = executor.submit(fetch_racer_kimarite, race_no, d_str, df_raw, course_map=_course_map)

                phase2_map = {f_ext: "選手コース別成績", f_racer_km: "選手別決まり手"}
                for future in as_completed(phase2_map):
                    done_count += 1
                    name = phase2_map[future]
                    pct = int((done_count / total_tasks) * 60)
                    progress_bar.progress(pct, text=f"⏳ 選手詳細データ取得中... {name} 完了 ({done_count}/{total_tasks})")

                ext_data = f_ext.result()
                racer_km = f_racer_km.result()

            df_raw = apply_extended_data(df_raw, ext_data)
        else:
            racer_km = fetch_racer_kimarite(race_no, d_str)

        odds_2t = odds_2tf.get("2連単", {})

        if not df_raw.empty:
            progress_bar.progress(70, text="🧠 AI予想を計算中...")
            try:
                result = predict(
                    df_raw, weather, race_no,
                    taka_data=taka, odds_dict=odds,
                    odds_2t=odds_2t,
                    racer_kimarite=racer_km,
                )
            except Exception as e:
                progress_bar.progress(100, text="❌ 予想計算エラー")
                time.sleep(1)
                progress_bar.empty()
                st.session_state.running = False
                st.error(f"予想計算中にエラーが発生しました: {e}")
                st.stop()

            progress_bar.progress(90, text="💾 予想結果を保存中...")
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

            progress_bar.progress(100, text="✅ 予想完了！")
            time.sleep(0.5)
            progress_bar.empty()

            # 結果をファイルキャッシュに保存（セッション切断対策）
            _save_result_cache(
                race_no, d_str, result, weather, deadline,
                odds, odds_2t, taka, racer_km, race_result_data, lady_racers,
                device_id=_device_id,
            )

            # 設定パネルをたたんで再描画
            st.session_state.show_settings = False
            st.session_state.running = False
            st.rerun()

        else:
            progress_bar.progress(100, text="❌ データ取得失敗")
            time.sleep(1)
            progress_bar.empty()
            st.session_state.running = False
            st.error("データが取得できませんでした。開催時間外の可能性があります。")

    # ── 安全リセット: 実行フラグが残っていたら解除 ────────────────────
    if st.session_state.running:
        st.session_state.running = False

    # ── 結果表示 ─────────────────────────────────────────────────────
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
        _JST = timezone(timedelta(hours=9))
        _dl_dt = None
        if _dl_time != "-" and len(_dstr) == 8:
            try:
                _race_date = f"{_dstr[:4]}-{_dstr[4:6]}-{_dstr[6:]}"
                _dl_dt = datetime.strptime(
                    f"{_race_date} {_dl_time}", "%Y-%m-%d %H:%M"
                ).replace(tzinfo=_JST)
            except Exception:
                pass

        # 締め切り判定（JSTで比較）
        _is_past_deadline = False
        _remain_sec = 0
        if _dl_dt:
            try:
                _now_jst = datetime.now(_JST)
                _remain = _dl_dt - _now_jst
                _remain_sec = int(_remain.total_seconds())
                _is_past_deadline = _remain_sec <= 0
            except Exception:
                pass

        if _is_past_deadline:
            _deadline_html = (
                f'<div style="color:#e05c5c;font-size:0.85rem;margin-top:4px;font-weight:bold">'
                f'締め切り済み</div>'
            )
        else:
            _deadline_html = (
                f'<div style="color:#ff9800;font-size:0.85rem;margin-top:4px">'
                f'⏰ 締切 {_dl_time}</div>'
            )

        st.markdown(
            f'<div style="background:linear-gradient(135deg,#0d2855,#1a3a6b);'
            f'border:2px solid #f0a500;border-radius:10px;padding:0.8rem 1rem;'
            f'margin-bottom:0.8rem;text-align:center">'
            f'<div style="color:#7ab8e8;font-size:0.75rem;letter-spacing:2px;margin-bottom:2px">PREDICTION TARGET</div>'
            f'<div style="display:flex;align-items:center;justify-content:center;gap:10px">'
            f'<span style="color:#f0a500;font-size:2rem;font-weight:900;letter-spacing:2px">{_rno_disp}R</span>'
            f'<span style="color:#fff;font-size:1rem">{_date_fmt}</span>'
            f'</div>'
            f'{_deadline_html}'
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
        # カウントダウン（JS自動更新）
        if _dl_dt and not _is_past_deadline:
            try:
                _dl_iso = _dl_dt.isoformat()
                _st_html(f"""
                <div id="countdown-box" style="text-align:center;margin-top:-6px;margin-bottom:4px">
                  <span id="countdown-text" style="font-weight:bold;font-size:0.85rem;color:#7ab8e8"></span>
                </div>
                <script>
                (function(){{
                  var deadline = new Date("{_dl_iso}");
                  var el = document.getElementById("countdown-text");
                  var box = document.getElementById("countdown-box");
                  function update(){{
                    var now = new Date();
                    var diff = Math.floor((deadline - now) / 1000);
                    if(diff <= 0){{
                      el.textContent = "締め切り済み";
                      el.style.color = "#e05c5c";
                      return;
                    }}
                    var m = Math.floor(diff / 60);
                    var s = diff % 60;
                    el.textContent = "あと " + m + "分" + String(s).padStart(2,"0") + "秒";
                    el.style.color = m < 5 ? "#e05c5c" : "#7ab8e8";
                  }}
                  update();
                  setInterval(update, 1000);
                }})();
                </script>
                """, height=30)
            except Exception:
                pass

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
                ("気温", w.get("気温", "-")),
                ("水温", w.get("水温", "-")),
                ("風速", w.get("風速", "0m")),
                ("風向", w.get("風向", "-")),
                ("波高", w.get("波高", "0cm")),
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
                _entry_parts = []
                for _, _row in scored.iterrows():
                    _waku = int(_row.get("枠番", 0))
                    _course = _row.get("進入コース")
                    if pd.notna(_course) and int(_course) > 0 and int(_course) != _waku:
                        _entry_diff = True
                    _entry_parts.append(str(int(_course)) if pd.notna(_course) and int(_course) > 0 else str(_waku))
                if _entry_diff:
                    _entry_str = " - ".join(_entry_parts)
                    _badges_html += (
                        '<div style="margin-top:6px">'
                        '<span style="background:#ff6f00;color:#fff;padding:3px 14px;'
                        'border-radius:12px;font-size:0.85rem;font-weight:bold;'
                        'letter-spacing:1px">進入変化</span>'
                        f'<span style="color:#ffab40;font-size:0.8rem;margin-left:8px">'
                        f'展示進入: {_entry_str}</span></div>'
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
                        _cfg = _FFG.get(ch, "#fff")
                        _cbdr = "border:1px solid #888;" if ch == "1" else ""
                        _combo_display += (
                            f'<span style="display:inline-block;width:20px;height:20px;'
                            f'line-height:20px;text-align:center;border-radius:3px;'
                            f'background:{_cbg};color:{_cfg};{_cbdr}font-weight:bold;'
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

        # ── 3連単 全組番テーブル（確率順）【メイン予想 - 最上部表示】──
        _3t_hdr_col1, _3t_hdr_col2 = st.columns([3, 1])
        with _3t_hdr_col1:
            st.markdown("### 🎯 3連単 予想")
        with _3t_hdr_col2:
            _odds_refresh = st.button("🔄 オッズ更新", use_container_width=True)

        # オッズ更新処理（予想は再計算せず、オッズと期待値だけ再取得）
        if _odds_refresh and st.session_state.race_no and st.session_state.date_str:
            _ref_rno = st.session_state.race_no
            _ref_dstr = st.session_state.date_str
            with st.spinner("オッズを更新中..."):
                _new_odds = fetch_odds_3t(_ref_rno, _ref_dstr)
            if _new_odds:
                st.session_state.odds = _new_odds
                odds = _new_odds
                # all_3t_candidates の期待値を再計算
                _updated_3t = res.get("all_3t_candidates", [])
                for _c in _updated_3t:
                    _combo_key = _c["買い目"]
                    _actual = _new_odds.get(_combo_key)
                    _c["実オッズ"] = _actual
                    if _actual is not None and _c["的中確率"] > 0:
                        _c["期待値"] = (_c["的中確率"] / 100) * _actual
                    else:
                        _c["期待値"] = None
                res["all_3t_candidates"] = _updated_3t
                st.session_state.result = res
                st.toast("オッズを更新しました")
            else:
                st.warning("オッズの取得に失敗しました")

        all_3t = res.get("all_3t_candidates", [])
        if not all_3t:
            st.warning("買い目候補がありません")
        else:
            # レース結果の3連単組番を取得（的中マーク用）
            _rr = st.session_state.race_result
            _result_combo_3t = ""
            if _rr and _rr.get("払戻", {}).get("3連単"):
                _result_combo_3t = _rr["払戻"]["3連単"]["組番"].replace("－", "-").replace("ー", "-")

            def _render_3t_table(rows: list[dict], table_id: str = "", result_combo: str = "", start_num: int = 1) -> str:
                """3連単テーブルのHTML文字列を生成する。"""
                _th_style = ('background:#0d2855;color:#7ab8e8;padding:8px 10px;'
                             'font-size:0.82rem;border-bottom:2px solid #1e5fa8;white-space:nowrap')
                hdr = (
                    f'<tr>'
                    f'<th style="{_th_style};text-align:center;width:30px">#</th>'
                    f'<th style="{_th_style};text-align:center">組番</th>'
                    f'<th style="{_th_style};text-align:right">確率</th>'
                    f'<th style="{_th_style};text-align:right">実ｵｯｽﾞ</th>'
                    f'<th style="{_th_style};text-align:right">公正ｵｯｽﾞ</th>'
                    f'<th style="{_th_style};text-align:right">期待値</th>'
                    f'</tr>'
                )
                body = ""
                _FBG = {"1":"#fff","2":"#000","3":"#e74c3c","4":"#3498db","5":"#f1c40f","6":"#2ecc71"}
                _FFG = {"1":"#000","2":"#fff","3":"#fff","4":"#fff","5":"#000","6":"#fff"}
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
                            f'<span style="display:inline-block;width:22px;height:22px;'
                            f'min-width:22px;line-height:22px;text-align:center;border-radius:4px;'
                            f'background:{bg};color:{fg};font-weight:bold;font-size:0.82rem;'
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

                    actual_str = f"{actual:.1f}" if actual is not None else "-"

                    if ev is not None:
                        if ev >= 1.0:
                            ev_str = f'<span style="color:#2ecc71;font-weight:bold">{ev:.2f}</span>'
                        else:
                            ev_str = f'<span style="color:#aaa">{ev:.2f}</span>'
                    else:
                        ev_str = '<span style="color:#555">-</span>'

                    # 行背景
                    _is_zero = prob < 0.005
                    if _is_hit:
                        row_bg = "#3a1a0a"
                        row_border = "border-left:3px solid #ff6b00;"
                    elif ev is not None and ev >= 1.0:
                        row_bg = "#12301a"
                        row_border = "border-left:3px solid #2ecc71;"
                    elif _is_zero:
                        row_bg = "#12151e"
                        row_border = ""
                    else:
                        row_bg = "#1a2744" if idx % 2 == 0 else "#151f35"
                        row_border = ""

                    _row_num = start_num + idx
                    _zero_opacity = "opacity:0.35;" if _is_zero else ""
                    prob_str = f'<span style="color:#555">0%</span>' if _is_zero else f'{prob:.2f}%'
                    _td_base = "padding:6px 10px;vertical-align:middle;border-bottom:1px solid #2a4a80"
                    body += (
                        f'<tr style="background:{row_bg};{row_border}{_zero_opacity}height:36px">'
                        f'<td style="{_td_base};text-align:center;color:#7ab8e8;'
                        f'font-size:0.78rem">{_row_num}</td>'
                        f'<td style="{_td_base};text-align:left">{combo_html}</td>'
                        f'<td style="{_td_base};text-align:right;color:#fff;'
                        f'font-size:0.85rem">{prob_str}</td>'
                        f'<td style="{_td_base};text-align:right;color:#ffe066;'
                        f'font-weight:bold;font-size:0.85rem">{actual_str}</td>'
                        f'<td style="{_td_base};text-align:right;color:#7ab8e8;'
                        f'font-size:0.85rem">{fair:.1f}</td>'
                        f'<td style="{_td_base};text-align:right;'
                        f'font-size:0.85rem">{ev_str}</td>'
                        f'</tr>'
                    )
                return (
                    f'<div style="overflow-x:auto;">'
                    f'<table style="border-collapse:collapse;width:100%;'
                    f'background:#1a2744;border-radius:8px;overflow:hidden">'
                    f'{hdr}{body}</table></div>'
                )

            top10 = all_3t[:10]
            rest = all_3t[10:]

            st.markdown(_render_3t_table(top10, result_combo=_result_combo_3t), unsafe_allow_html=True)

            # 凡例
            _legend_items = '🟢 緑行 = 期待値 ≧ 1.0（バリュー買い目）'
            if _result_combo_3t:
                _legend_items += '&nbsp;|&nbsp;🟠 橙行 = 的中'
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

        st.markdown("---")

        # ── 分析データテーブル ────────────────────────────────────────
        rno = st.session_state.race_no
        st.markdown(f"#### 📋 {rno}R 分析データ")

        base_cols  = ["枠番", "選手名", "級別"]
        extra_cols = []
        for col in ["F回数", "体重"]:
            if col in scored.columns:
                extra_cols.append(col)
        base_cols += ["全国勝率", "蒲郡勝率"]
        if "モーター2連率" in scored.columns:
            extra_cols.append("モーター2連率")
        if "スタートタイミング" in scored.columns:
            extra_cols.append("スタートタイミング")
        for col in ["コース別1着率", "直近平均着順"]:
            if col in scored.columns:
                extra_cols.append(col)
        # 展示情報をグループ化（展示進入・展示タイム・まわり足・直線T・一周T）
        # 展示タイムが全て未発表のときは進入コースも表示しない
        _has_exhibit = (
            "展示タイム" in scored.columns
            and scored["展示タイム"].apply(lambda x: pd.notnull(x) and x > 0).any()
        )
        if "進入コース" in scored.columns and _has_exhibit:
            base_cols.append("進入コース")
        base_cols += ["展示タイム"]
        for col in ["まわり足タイム", "直線タイム", "一周タイム"]:
            if col in scored.columns:
                base_cols.append(col)
        base_cols += ["チルト", "win_prob", "highlight_reason"]

        display_cols = base_cols[:3] + extra_cols + base_cols[3:]
        seen = set()
        display_cols_unique = [
            c for c in display_cols
            if c in scored.columns and c not in seen and not seen.add(c)
        ]

        col_rename = {
            "win_prob":          "1着確率(%)",
            "highlight_reason":  "ポイント",
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
            display_df.loc[_lady_mask, "選手名"] = "♥ " + display_df.loc[_lady_mask, "選手名"].astype(str)

        fmt = {"1着確率(%)": "{:.1f}%", "全国勝率": "{:.2f}", "蒲郡勝率": "{:.2f}"}
        if "M2連(%)" in display_df.columns:
            fmt["M2連(%)"] = lambda x: f"{x:.1f}" if pd.notnull(x) and x > 0 else "-"
        if "平均ST" in display_df.columns:
            fmt["平均ST"] = lambda x: f"{x:.2f}" if pd.notnull(x) else "-"
        fmt["展示タイム"] = lambda x: f"{x:.2f}" if pd.notnull(x) and x > 0 else "未発表"
        fmt["チルト"]    = lambda x: f"{x:.1f}" if pd.notnull(x) else "-"
        if "C別1着(%)" in display_df.columns:
            fmt["C別1着(%)"] = lambda x: f"{x:.1f}" if pd.notnull(x) and x > 0 else "-"
        if "直近平均着" in display_df.columns:
            fmt["直近平均着"] = lambda x: f"{x:.1f}" if pd.notnull(x) and x > 0 else "-"
        if "まわり足" in display_df.columns:
            fmt["まわり足"] = lambda x: f"{x:.2f}" if pd.notnull(x) and x > 0 else "-"
        if "直線T" in display_df.columns:
            fmt["直線T"] = lambda x: f"{x:.2f}" if pd.notnull(x) and x > 0 else "-"
        if "一周T" in display_df.columns:
            fmt["一周T"] = lambda x: f"{x:.2f}" if pd.notnull(x) and x > 0 else "-"
        if "体重" in display_df.columns:
            fmt["体重"] = lambda x: f"{x:.1f}kg" if pd.notnull(x) and x > 0 else "-"
        if "F" in display_df.columns:
            fmt["F"] = lambda x: f"{int(x)}" if pd.notnull(x) and x > 0 else "0"
        if "展示進入" in display_df.columns:
            # 枠番と異なる進入コースを「枠→コース」形式で表示
            _waku_list = scored["枠番"].astype(int).tolist()
            def _fmt_shinnyuu(idx, x):
                if pd.isnull(x) or int(x) <= 0:
                    return "-"
                c = int(x)
                w = _waku_list[idx]
                return f"{w}→{c}" if c != w else f"{c}"
            display_df["展示進入"] = [
                _fmt_shinnyuu(i, v) for i, v in enumerate(display_df["展示進入"])
            ]
            fmt["展示進入"] = "{}"

        # 展示情報の列名リスト
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

        styler = display_df.style

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

        if "M2連(%)" in display_df.columns:
            styler = styler.apply(_style_motor2, subset=["M2連(%)"])

        # 展示進入が枠番と異なるセルをオレンジでハイライト
        if "展示進入" in display_df.columns:
            _shinnyuu_diff = [
                "→" in str(v) for v in display_df["展示進入"]
            ]
            def _style_shinnyuu(col):
                return [
                    "background-color: rgba(255,111,0,0.55); color: #fff; font-weight: bold; text-shadow: 0 0 4px rgba(255,160,0,0.7)"
                    if _shinnyuu_diff[i] else ""
                    for i in range(len(col))
                ]
            styler = styler.apply(_style_shinnyuu, subset=["展示進入"])

        # 展示情報列に1位/2位ハイライト適用（展示進入はランキング対象外）
        _EXHIBIT_RANK_COLS = [c for c in _EXHIBIT_COLS if c != "展示進入"]
        for ecol in _EXHIBIT_RANK_COLS:
            if ecol in display_df.columns:
                styler = styler.apply(_style_exhibit_rank, subset=[ecol])

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

        # 展示列ヘッダーの色を変更（テーブルUUIDを取得しCSS IDセレクタで上書き）
        _exhibit_idxs = [display_df.columns.get_loc(c) for c in _EXHIBIT_COLS if c in display_df.columns]
        _uuid_m = re.search(r'id="(T_[a-zA-Z0-9_]+)"', tbl_html)
        if _uuid_m and _exhibit_idxs:
            _tid = _uuid_m.group(1)
            _sel = ", ".join(f"#{_tid}_level0_col{i}" for i in _exhibit_idxs)
            _ecss = (
                f"<style>{_sel} {{"
                f" background-color:#0a6e5c !important;"
                f" color:#5dffe0 !important;"
                f" font-weight:bold !important;"
                f"}}</style>"
            )
            tbl_html = _ecss + tbl_html

        # ポイント列を左寄せにする
        if "ポイント" in display_df.columns:
            _point_idx = display_df.columns.get_loc("ポイント")
            _uuid_m2 = re.search(r'id="(T_[a-zA-Z0-9_]+)"', tbl_html)
            if _uuid_m2:
                _tid2 = _uuid_m2.group(1)
                _pcss = (
                    f"<style>"
                    f"#{_tid2} td.col{_point_idx} {{"
                    f" text-align:left !important;"
                    f"}}</style>"
                )
                tbl_html = _pcss + tbl_html

        st.markdown(
            f'<div style="overflow-x:auto;">{tbl_html}</div>',
            unsafe_allow_html=True,
        )

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

        # ── 高橋アナ予想パネル ────────────────────────────────────────
        taka = st.session_state.taka or {}
        st.markdown("### 🎤 高橋アナの予想（蒲郡競艇公式サイト）")

        if taka.get("available"):
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
                        f'border-radius:50%;width:1.4em;height:1.4em;text-align:center;'
                        f'line-height:1.4em;font-size:0.85em;font-weight:bold;margin:0 2px">{digit}</span>'
                    )
                tenkai_html = tenkai_html.replace("\n", "<br>")
                st.markdown(
                    f'<div style="background:#1a2744;border-left:4px solid #3498db;'
                    f'padding:0.8rem 1rem;border-radius:6px;margin-bottom:0.6rem">'
                    f'<div style="color:#7ab8e8;font-size:0.8rem;margin-bottom:4px">展開予想</div>'
                    f'<div style="color:#fff;line-height:1.8">{tenkai_html}</div>'
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
                        f'<span style="color:#7ab8e8;font-size:0.78rem">予想買い目</span>&nbsp;'
                        f'<b style="font-size:1.2rem;color:#ffe066">{y}</b>'
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
                                        f'border-radius:50%;width:26px;height:26px;font-weight:bold;'
                                        f'font-size:0.8rem">{b}</span>'
                                        f'{arr_html}</span>'
                                    )
                                cells += f'<td style="text-align:center;padding:3px;min-width:36px">{badges}</td>'
                            else:
                                cells += '<td style="padding:3px;min-width:36px"></td>'
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
                            f'border-radius:50%;width:22px;height:22px;font-weight:bold;'
                            f'font-size:0.75rem;margin:0 2px">{s}</span>'
                        )
                    st.markdown(
                        f'<div style="background:#12301a;border-left:3px solid #2ecc71;'
                        f'padding:0.5rem 0.8rem;border-radius:6px;margin-top:0.6rem">'
                        f'<div style="color:#7ab8e8;font-size:0.78rem;margin-bottom:4px">スリット順</div>'
                        f'<div>{slit_badges}</div>'
                        f'</div>',
                        unsafe_allow_html=True
                    )

            # スコア反映注記
            if taka.get("chart_scores"):
                st.caption("※ 高橋アナ評価チャートはAIスコアに反映済みです")
        else:
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
            st.markdown("#### 🎯 選手別決まり手（コース別）")
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
            st.markdown("#### 🎯 選手別決まり手（コース別）")
            st.caption("選手別決まり手データを取得できませんでした")

        # ── 3連単オッズ一覧表（expander） ──────────────────────────
        if odds:
            st.markdown("#### 📊 3連単オッズ一覧")
            if True:
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
                    _hdr += (
                        f'<th colspan="3" style="{_CB}background:{_bg};'
                        f'color:{_fg};padding:5px 4px;font-size:0.72rem;'
                        f'text-align:center;font-weight:bold;'
                        f'white-space:nowrap">{_b}.{_nm}</th>'
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
                                    f'font-size:0.78rem;padding:2px 4px;'
                                    f'width:20px;vertical-align:middle">'
                                    f'{_s}</td>'
                                )

                            # 3着セル
                            _bg3 = _OBG[str(_t)]
                            _fg3 = _OFG[str(_t)]
                            _tr += (
                                f'<td style="{_CB}background:{_bg3};'
                                f'color:{_fg3};text-align:center;'
                                f'font-weight:bold;font-size:0.78rem;'
                                f'padding:2px 4px;width:20px">{_t}</td>'
                            )

                            # オッズセル（1番人気=赤、2番人気=黄、999倍以上=紫で強調）
                            _oval = odds.get(_combo)
                            if _oval is not None:
                                _ostr = f"{_oval:.1f}"
                                if _oval == _odds_top1:
                                    _ocss = (f'{_CB}background:#fff;'
                                             f'color:#e74c3c;text-align:right;'
                                             f'font-size:0.78rem;padding:2px 6px;'
                                             f'white-space:nowrap;font-weight:bold')
                                elif _oval == _odds_top2:
                                    _ocss = (f'{_CB}background:#fff;'
                                             f'color:#f39c12;text-align:right;'
                                             f'font-size:0.78rem;padding:2px 6px;'
                                             f'white-space:nowrap;font-weight:bold')
                                elif _oval >= 999:
                                    _ocss = (f'{_CB}background:#fff;'
                                             f'color:#8e44ad;text-align:right;'
                                             f'font-size:0.78rem;padding:2px 6px;'
                                             f'white-space:nowrap;font-weight:bold')
                                else:
                                    _ocss = (f'{_CB}background:#fff;'
                                             f'color:#000;text-align:right;'
                                             f'font-size:0.78rem;padding:2px 6px;'
                                             f'white-space:nowrap')
                                _tr += f'<td style="{_ocss}">{_ostr}</td>'
                            else:
                                _tr += (
                                    f'<td style="{_CB}background:#fff;'
                                    f'color:#999;text-align:right;'
                                    f'font-size:0.78rem;padding:2px 6px">'
                                    f'-</td>'
                                )

                        _tr += '</tr>'
                        _body += _tr

                _odds_tbl = (
                    f'<table style="border-collapse:collapse;width:100%;'
                    f'background:#fff">{_hdr}{_body}</table>'
                )
                st.markdown(
                    f'<div style="overflow-x:auto;'
                    f'">{_odds_tbl}</div>',
                    unsafe_allow_html=True,
                )

        # ── 艇別パフォーマンスレーダーチャート（expander） ─────────
        st.markdown("#### 📡 艇別パフォーマンスレーダーチャート")

        def _make_radar_chart(df_scored: pd.DataFrame) -> go.Figure:
            """scored DataFrameから艇別レーダーチャートを生成する。"""
            dims = [
                ("蒲郡勝率",   "蒲郡勝率",   False),
                ("蒲郡2連率",  "蒲郡2連率",  False),
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
                        tickfont=dict(size=11, color="#7ab8e8"),
                        rotation=90,
                        direction="clockwise",
                    ),
                    bgcolor="#0e1a2e",
                ),
                showlegend=True,
                legend=dict(font=dict(color="#e8f4ff"), bgcolor="rgba(14,26,46,0.8)"),
                paper_bgcolor="#0e1a2e",
                margin=dict(l=20, r=20, t=20, b=20),
                height=340,
            )
            return fig

        radar_fig = _make_radar_chart(scored)
        if radar_fig.data:
            st.plotly_chart(radar_fig, use_container_width=True, config={"staticPlot": True})
        else:
            st.caption("レーダーチャートを表示するのに十分なデータがありません")

        # ── 予想パラメータ一覧 ────────────────────────────────────────
        with st.expander("📐 予想に使用しているパラメータ一覧", expanded=False):
            from config import SCORE_WEIGHTS as _W, GAMAGORI_SETTINGS as _G

            param_groups = [
                ("コース・勝率", [
                    ("コース基礎確率", f'{_W["course_base"]}'),
                    ("全国勝率の重み", f'{_W["win_rate"]}'),
                    ("蒲郡勝率の重み", f'{_W["local_win_rate"]}'),
                    ("全国2連率の重み", f'{_W["nat2_rate"]}'),
                    ("蒲郡2連率の重み", f'{_W["loc2_rate"]}'),
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

    # 枠番カラー
    _WAKU_COLORS = {
        1: ("#fff", "#000"),    # 白
        2: ("#000", "#fff"),    # 黒
        3: ("#e03030", "#fff"), # 赤
        4: ("#2060d0", "#fff"), # 青
        5: ("#d0c020", "#000"), # 黄
        6: ("#20a040", "#fff"), # 緑
    }

    st.markdown(
        '<div style="background:#1a2744;border:1px solid #1e5fa8;border-radius:8px;'
        'padding:0.8rem 1rem;margin-bottom:0.5rem">'
        '<span style="color:#7ab8e8;font-size:0.9rem;font-weight:bold">出走表一覧</span>'
        '</div>',
        unsafe_allow_html=True,
    )

    _d_param_s = st.query_params.get("d")
    if _d_param_s:
        try:
            _default_date_s = datetime.strptime(_d_param_s, "%Y%m%d").date()
        except ValueError:
            _default_date_s = date.today()
    else:
        _default_date_s = date.today()

    shutsusou_date = st.date_input("開催日", _default_date_s, key="shutsusou_date_input", disabled=_ui_disabled)
    fetch_shutsusou = st.button("▶ 出走表を取得", type="primary", use_container_width=True, disabled=_ui_disabled,
                                on_click=lambda: st.session_state.update(running=True))

    if fetch_shutsusou:
        d_str_s = shutsusou_date.strftime("%Y%m%d")
        progress = st.progress(0, text="⏳ 全レースの出走表を取得中...")

        all_race_data = {}
        import requests as _requests
        _session = _requests.Session()
        _session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"})
        with ThreadPoolExecutor(max_workers=12) as executor:
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

        progress.progress(100, text="✅ 出走表取得完了")
        time.sleep(0.3)
        progress.empty()

        st.session_state.shutsusou_data = all_race_data
        st.session_state.shutsusou_date = d_str_s
        st.session_state.running = False

    # ── 安全リセット: 実行フラグが残っていたら解除 ────────────────────
    if st.session_state.running:
        st.session_state.running = False

    # ── 出走表表示 ─────────────────────────────────────────────
    if st.session_state.shutsusou_data:
        _sd = st.session_state.shutsusou_date or ""
        _sd_fmt = f"{_sd[:4]}/{_sd[4:6]}/{_sd[6:]}" if len(_sd) == 8 else _sd
        st.markdown(
            f'<div style="color:#7ab8e8;font-size:0.85rem;margin-bottom:0.5rem">'
            f'📅 {_sd_fmt} 蒲郡ボートレース 全レース出走表</div>',
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
                lw = row.get("蒲郡勝率", "-")
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
                    f'font-weight:bold;width:30px;border-radius:4px">{waku}</td>'
                    f'<td style="color:#fff;font-weight:bold;padding-left:8px">{name}'
                    f' <span style="color:#7ab8e8;font-size:0.75rem">{rank}</span></td>'
                    f'<td style="color:#cce0ff;text-align:right">{nw}</td>'
                    f'<td style="color:#cce0ff;text-align:right">{lw}</td>'
                    f'<td style="color:#cce0ff;text-align:right">{m2}</td>'
                    f'<td style="color:#cce0ff;text-align:right">{b2}</td>'
                    f'<td style="color:#cce0ff;text-align:right">{st_val}</td>'
                    f'</tr>'
                )

            table_html = (
                '<table style="width:100%;border-collapse:collapse;font-size:0.85rem;margin:0">'
                '<thead><tr style="border-bottom:1px solid #2a4a80">'
                '<th style="color:#7ab8e8;font-size:0.7rem;padding:4px 2px">枠</th>'
                '<th style="color:#7ab8e8;font-size:0.7rem;padding:4px 2px;text-align:left">選手名</th>'
                '<th style="color:#7ab8e8;font-size:0.7rem;padding:4px 2px;text-align:right">全国勝率</th>'
                '<th style="color:#7ab8e8;font-size:0.7rem;padding:4px 2px;text-align:right">当地勝率</th>'
                '<th style="color:#7ab8e8;font-size:0.7rem;padding:4px 2px;text-align:right">ﾓｰﾀｰ2連率</th>'
                '<th style="color:#7ab8e8;font-size:0.7rem;padding:4px 2px;text-align:right">ﾎﾞｰﾄ2連率</th>'
                '<th style="color:#7ab8e8;font-size:0.7rem;padding:4px 2px;text-align:right">ST</th>'
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
