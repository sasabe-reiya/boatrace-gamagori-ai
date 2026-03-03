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
import pandas as pd
import numpy as np
import streamlit as st
import plotly.graph_objects as go
from datetime import date, datetime
import time
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.dirname(__file__))
from config import JYNAME
import streamlit.components.v1 as components
from race_scraper import (
    fetch_full_race_data,
    fetch_deadline, fetch_odds_3t, fetch_odds_2tf, fetch_gamagori_taka,
    fetch_venue_kimarite, fetch_racer_kimarite,
)
from scorer import predict
from result_tracker import save_prediction

st.set_page_config(page_title="舟券錬金術 - 蒲郡", page_icon="⚗️", layout="centered", initial_sidebar_state="expanded")

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
        '<h2 style="color:#e8f4ff;white-space:nowrap;font-size:1.4rem">⚗️ 蒲郡 舟券錬金術</h2>'
        '<p style="color:#5a9fd4;font-size:0.6rem;letter-spacing:4px;margin-top:-8px">GAMAGORI ALCHEMIST</p>'
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
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no, viewport-fit=cover">
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

    /* ── モバイル最適化 ─────────────────────────────────── */
    @media (max-width: 768px) {
        /* Streamlitの余白を縮小 */
        .stMainBlockContainer { padding: 0.5rem 0.8rem !important; }
        section[data-testid="stSidebar"] { min-width: 260px !important; }

        /* 見出しサイズ調整 */
        .main-header h1 { font-size: 1.1rem; letter-spacing: 1px; }
        .main-header .logo-sub { font-size: 0.58rem; letter-spacing: 3px; }
        .main-header .logo-icon svg { width: 38px; height: 38px; }
        h3 { font-size: 1.1rem !important; }

        /* ボタンを押しやすく */
        .stButton > button { min-height: 48px !important; font-size: 1rem !important; }
        .stSelectbox, .stDateInput { font-size: 1rem !important; }

        /* データテーブルの横スクロール */
        .stDataFrame { overflow-x: auto !important; -webkit-overflow-scrolling: touch; }
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
      <svg width="46" height="46" viewBox="0 0 100 100" xmlns="http://www.w3.org/2000/svg">
        <defs>
          <linearGradient id="flask-fill" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stop-color="#f0a500" stop-opacity="0.9"/>
            <stop offset="50%" stop-color="#e08600" stop-opacity="0.8"/>
            <stop offset="100%" stop-color="#2ecc71" stop-opacity="0.6"/>
          </linearGradient>
          <linearGradient id="flask-glass" x1="0" y1="0" x2="1" y2="1">
            <stop offset="0%" stop-color="#4aa3ff" stop-opacity="0.4"/>
            <stop offset="100%" stop-color="#1e5fa8" stop-opacity="0.2"/>
          </linearGradient>
          <linearGradient id="wave1" x1="0" y1="0" x2="1" y2="0">
            <stop offset="0%" stop-color="#4aa3ff" stop-opacity="0.8"/>
            <stop offset="100%" stop-color="#1e5fa8" stop-opacity="0.3"/>
          </linearGradient>
        </defs>
        <!-- フラスコ本体 -->
        <path d="M38 18 L38 40 L20 72 Q18 78, 24 80 L76 80 Q82 78, 80 72 L62 40 L62 18" fill="url(#flask-glass)" stroke="#5cb8ff" stroke-width="1.5"/>
        <!-- フラスコ口 -->
        <rect x="35" y="12" width="30" height="8" rx="3" fill="#0d2855" stroke="#5cb8ff" stroke-width="1"/>
        <!-- 液体 -->
        <path d="M28 62 Q40 56, 50 62 Q60 68, 72 62 L76 80 Q82 78, 80 72 L76 80 L24 80 Q18 78, 20 72 Z" fill="url(#flask-fill)" opacity="0.8"/>
        <!-- 泡 -->
        <circle cx="40" cy="65" r="2.5" fill="#f0a500" opacity="0.6"/>
        <circle cx="55" cy="60" r="1.8" fill="#2ecc71" opacity="0.5"/>
        <circle cx="48" cy="58" r="1.5" fill="#ffe066" opacity="0.7"/>
        <circle cx="60" cy="66" r="2" fill="#f0a500" opacity="0.4"/>
        <!-- 蒸気・キラキラ -->
        <path d="M44 12 Q42 5, 44 0" stroke="#ffe066" stroke-width="1" fill="none" opacity="0.6"/>
        <path d="M50 12 Q50 3, 52 -2" stroke="#f0a500" stroke-width="1.2" fill="none" opacity="0.7"/>
        <path d="M56 12 Q58 5, 56 0" stroke="#ffe066" stroke-width="1" fill="none" opacity="0.5"/>
        <!-- キラキラ -->
        <g opacity="0.8">
          <line x1="12" y1="30" x2="18" y2="30" stroke="#ffe066" stroke-width="1"/>
          <line x1="15" y1="27" x2="15" y2="33" stroke="#ffe066" stroke-width="1"/>
        </g>
        <g opacity="0.6">
          <line x1="82" y1="22" x2="88" y2="22" stroke="#ffe066" stroke-width="1"/>
          <line x1="85" y1="19" x2="85" y2="25" stroke="#ffe066" stroke-width="1"/>
        </g>
        <g opacity="0.5">
          <line x1="78" y1="50" x2="82" y2="50" stroke="#4aa3ff" stroke-width="0.8"/>
          <line x1="80" y1="48" x2="80" y2="52" stroke="#4aa3ff" stroke-width="0.8"/>
        </g>
        <!-- 波（下部） -->
        <path d="M0 85 Q12 80, 25 85 Q38 90, 50 85 Q62 80, 75 85 Q88 90, 100 85 L100 100 L0 100 Z" fill="url(#wave1)" opacity="0.25"/>
      </svg>
    </div>
    <div>
      <h1>蒲郡 舟券錬金術</h1>
      <div class="logo-sub">GAMAGORI ALCHEMIST</div>
    </div>
  </div>
  <svg class="logo-wave" viewBox="0 0 1200 30" preserveAspectRatio="none" xmlns="http://www.w3.org/2000/svg">
    <path d="M0 15 Q150 0, 300 15 Q450 30, 600 15 Q750 0, 900 15 Q1050 30, 1200 15 L1200 30 L0 30 Z" fill="#4aa3ff"/>
  </svg>
</div>''', unsafe_allow_html=True)

# ── セッション初期化 ──────────────────────────────────────────────
for key in ("result", "weather", "deadline", "race_no", "date_str", "odds", "odds_2t", "odds_2f", "taka", "racer_km", "kimarite"):
    if key not in st.session_state:
        st.session_state[key] = None
if "sidebar_open" not in st.session_state:
    st.session_state.sidebar_open = True

# ── サイドバー ────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### ⚙️ レース設定")
    race_no   = st.radio("レース番号", list(range(1, 13)), index=0, horizontal=False, format_func=lambda x: f"{x}R")
    race_date = st.date_input("開催日", date.today())
    fetch_btn  = st.button("🔄 予想実行", type="primary", use_container_width=True)

# 予想実行ボタン押下時にサイドバーを閉じるフラグをセット
if fetch_btn:
    st.session_state.sidebar_open = False

# ── モバイル: CSSでサイドバーの表示/非表示を制御 ──────────────────
if st.session_state.sidebar_open:
    st.markdown("""
    <style>
    @media (max-width: 768px) {
        /* サイドバーを強制表示 */
        section[data-testid="stSidebar"] {
            transform: none !important;
            width: 85vw !important;
            max-width: 300px !important;
            min-width: 260px !important;
            margin-left: 0 !important;
            left: 0 !important;
            z-index: 999999 !important;
        }
        section[data-testid="stSidebar"][aria-expanded="false"] {
            transform: none !important;
            width: 85vw !important;
            max-width: 300px !important;
            min-width: 260px !important;
            margin-left: 0 !important;
        }
        section[data-testid="stSidebar"] > div:first-child {
            width: 100% !important;
        }
    }
    </style>
    """, unsafe_allow_html=True)

# ── 予想実行 ─────────────────────────────────────────────────────
if fetch_btn:
    d_str = race_date.strftime("%Y%m%d")
    progress_bar = st.progress(0, text="⏳ データ取得を開始します...")

    # 独立した7つのデータ取得を並列実行
    with ThreadPoolExecutor(max_workers=7) as executor:
        f_data     = executor.submit(fetch_full_race_data, race_no, d_str, True)
        f_deadline = executor.submit(fetch_deadline, race_no, d_str)
        f_odds     = executor.submit(fetch_odds_3t, race_no, d_str)
        f_odds_2tf = executor.submit(fetch_odds_2tf, race_no, d_str)
        f_taka     = executor.submit(fetch_gamagori_taka, race_no, d_str)
        f_kimarite = executor.submit(fetch_venue_kimarite)
        f_racer_km = executor.submit(fetch_racer_kimarite, race_no, d_str)

        futures_map = {
            f_data:     "出走表・直前データ",
            f_deadline: "締切時刻",
            f_odds:     "3連単オッズ",
            f_odds_2tf: "2連単/複オッズ",
            f_taka:     "高橋アナ予想",
            f_kimarite: "決まり手データ",
            f_racer_km: "選手別決まり手",
        }
        done_count = 0
        total_tasks = len(futures_map)
        for future in as_completed(futures_map):
            done_count += 1
            name = futures_map[future]
            pct = int((done_count / total_tasks) * 60)
            progress_bar.progress(pct, text=f"⏳ データ取得中... {name} 完了 ({done_count}/{total_tasks})")

        df_raw, weather = f_data.result()
        deadline = f_deadline.result()
        odds     = f_odds.result()
        odds_2tf = f_odds_2tf.result()
        taka     = f_taka.result()
        kimarite_raw = f_kimarite.result()
        racer_km = f_racer_km.result()

    odds_2t = odds_2tf.get("2連単", {})
    odds_2f = odds_2tf.get("2連複", {})

    # 決まり手データ: スクレイピング失敗時はデフォルト値にフォールバック
    from config import GAMAGORI_KIMARITE_DEFAULT
    kimarite = kimarite_raw if kimarite_raw else GAMAGORI_KIMARITE_DEFAULT

    if not df_raw.empty:
        progress_bar.progress(70, text="🧠 AI予想を計算中...")
        result = predict(
            df_raw, weather, race_no,
            taka_data=taka, odds_dict=odds,
            odds_2t=odds_2t, odds_2f=odds_2f,
            kimarite_data=kimarite,
            racer_kimarite=racer_km,
        )

        progress_bar.progress(90, text="💾 予想結果を保存中...")
        st.session_state.result   = result
        st.session_state.weather  = weather
        st.session_state.deadline = deadline
        st.session_state.race_no  = race_no
        st.session_state.date_str = d_str
        st.session_state.odds     = odds
        st.session_state.odds_2t  = odds_2t
        st.session_state.odds_2f  = odds_2f
        st.session_state.taka     = taka
        st.session_state.racer_km = racer_km
        st.session_state.kimarite = kimarite

        confidence = (
            result["scored_df"]["confidence"].iloc[0]
            if "confidence" in result["scored_df"].columns else "-"
        )
        save_prediction(d_str, race_no, result["recommendations"], weather, confidence)

        progress_bar.progress(100, text="✅ 予想完了！")
        time.sleep(1)
        progress_bar.empty()

        # サイドバーを閉じて結果表示（rerunでCSS強制表示が消える）
        st.rerun()

    else:
        progress_bar.progress(100, text="❌ データ取得失敗")
        time.sleep(1)
        progress_bar.empty()
        st.error("データが取得できませんでした。開催時間外の可能性があります。")

# ── 結果表示 ─────────────────────────────────────────────────────
if st.session_state.result is not None:
    res    = st.session_state.result
    scored = res["scored_df"]
    odds   = st.session_state.odds or {}

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
        st.markdown(weather_html, unsafe_allow_html=True)

    st.markdown("---")

    # ── 締め切り時刻 ──────────────────────────────────────────────
    rno = st.session_state.race_no
    deadline = st.session_state.deadline or "-"
    remaining_html = ""
    urgent = False

    if deadline != "-":
        try:
            now   = datetime.now()
            dl_dt = datetime.strptime(f"{now.strftime('%Y-%m-%d')} {deadline}", "%Y-%m-%d %H:%M")
            diff_sec = (dl_dt - now).total_seconds()
            if diff_sec < 0:
                remaining_html = '<span class="deadline-remaining ok">締切済み</span>'
            else:
                diff_min = int(diff_sec // 60)
                diff_s   = int(diff_sec % 60)
                urgent   = diff_min < 5
                css_cls  = "urgent" if urgent else "ok"
                remaining_html = (
                    f'<span class="deadline-remaining {css_cls}">'
                    f'あと {diff_min}分{diff_s:02d}秒</span>'
                )
        except Exception:
            pass

    box_cls = "deadline-box deadline-urgent" if urgent else "deadline-box"
    st.markdown(
        f'<div class="{box_cls}">'
        f'<div class="deadline-label">⏰ 締切予定時刻</div>'
        f'<div class="deadline-time">{deadline}</div>'
        f'{remaining_html}'
        f'</div>',
        unsafe_allow_html=True
    )

    # ── 分析データテーブル ────────────────────────────────────────
    st.markdown(f"### 📋 {rno}R 分析データ")

    base_cols  = ["枠番", "選手名", "級別"]
    extra_cols = []
    if "進入コース" in scored.columns:
        extra_cols.append("進入コース")
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
    # 展示情報をグループ化（展示タイム・まわり足・直線T・一周T）
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
        "進入コース":         "進入",
        "モーター2連率":      "M2連(%)",
        "スタートタイミング": "ST",
        "コース別1着率":      "C別1着(%)",
        "直近平均着順":       "直近平均着",
        "まわり足タイム":     "まわり足",
        "直線タイム":         "直線T",
        "一周タイム":         "一周T",
        "F回数":              "F",
    }
    display_df = scored[display_cols_unique].rename(columns=col_rename)

    fmt = {"1着確率(%)": "{:.1f}%", "全国勝率": "{:.2f}", "蒲郡勝率": "{:.2f}"}
    if "M2連(%)" in display_df.columns:
        fmt["M2連(%)"] = lambda x: f"{x:.1f}" if pd.notnull(x) and x > 0 else "-"
    if "ST" in display_df.columns:
        fmt["ST"] = lambda x: f"{x:.2f}" if pd.notnull(x) else "-"
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

    def style_row(row):
        is_max = row["1着確率(%)"] == display_df["1着確率(%)"].max()
        return ["background-color: rgba(240,165,0,0.15)" if is_max else ""] * len(row)

    # 展示情報の列名リスト
    _EXHIBIT_COLS = ["展示タイム", "まわり足", "直線T", "一周T"]

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

    styler = display_df.style.apply(style_row, axis=1)

    # 展示情報列に1位/2位ハイライト適用
    for ecol in _EXHIBIT_COLS:
        if ecol in display_df.columns:
            styler = styler.apply(_style_exhibit_rank, subset=[ecol])

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

    st.markdown(
        f'<div style="overflow-x:auto;-webkit-overflow-scrolling:touch">{tbl_html}</div>',
        unsafe_allow_html=True,
    )

    # ── 蒲郡コース別決まり手 ────────────────────────────────────────
    venue_km = st.session_state.kimarite or {}
    if venue_km:
        with st.expander("🏟️ 蒲郡コース別決まり手（直近3ヶ月）", expanded=False):
            _VK_COLORS = {
                "逃げ": "#e74c3c", "差し": "#3498db", "まくり": "#f1c40f",
                "まくり差し": "#2ecc71", "抜き": "#9b59b6", "恵まれ": "#95a5a6",
            }
            _VK_FRAME_BG = {"1": "#fff", "2": "#000", "3": "#e74c3c",
                            "4": "#3498db", "5": "#f1c40f", "6": "#2ecc71"}
            _VK_FRAME_FG = {"1": "#000", "2": "#fff", "3": "#fff",
                            "4": "#fff", "5": "#000", "6": "#fff"}
            for course in sorted(venue_km.keys()):
                km = venue_km[course]
                cs = str(course)
                fbg = _VK_FRAME_BG.get(cs, "#666")
                ffg = _VK_FRAME_FG.get(cs, "#fff")
                fborder = "border:1.5px solid #888;" if cs == "1" else ""
                first_rate = km.get("1着率", 0.0)

                bars_html = ""
                for kt_name in ["逃げ", "差し", "まくり", "まくり差し", "抜き", "恵まれ"]:
                    pct = km.get(kt_name, 0.0)
                    if pct <= 0:
                        continue
                    color = _VK_COLORS.get(kt_name, "#888")
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

                st.markdown(
                    f'<div style="background:#0e1a2e;border:1px solid #2a4a80;border-radius:6px;'
                    f'padding:8px 10px;margin:4px 0">'
                    f'<div style="display:flex;align-items:center;margin-bottom:4px">'
                    f'<span style="display:inline-flex;align-items:center;justify-content:center;'
                    f'background:{fbg};color:{ffg};{fborder}border-radius:50%;'
                    f'width:22px;height:22px;font-weight:bold;font-size:0.8rem;margin-right:6px">'
                    f'{cs}</span>'
                    f'<span style="color:#fff;font-weight:bold;font-size:0.85rem">{cs}コース</span>'
                    f'<span style="color:#7ab8e8;font-size:0.7rem;margin-left:auto">'
                    f'1着率 {first_rate:.1f}%</span>'
                    f'</div>'
                    f'{bars_html}'
                    f'</div>',
                    unsafe_allow_html=True,
                )

    # ── 選手別決まり手 ────────────────────────────────────────────
    racer_km = st.session_state.racer_km or {}
    if racer_km:
        with st.expander("🎯 選手別決まり手（コース別）", expanded=False):
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
                num = rk.get("レース数", 0)
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

                st.markdown(
                    f'<div style="background:#0e1a2e;border:1px solid #2a4a80;border-radius:6px;'
                    f'padding:8px 10px;margin:4px 0">'
                    f'<div style="display:flex;align-items:center;margin-bottom:4px">'
                    f'<span style="display:inline-flex;align-items:center;justify-content:center;'
                    f'background:{fbg};color:{ffg};{fborder}border-radius:50%;'
                    f'width:22px;height:22px;font-weight:bold;font-size:0.8rem;margin-right:6px">'
                    f'{frame}</span>'
                    f'<span style="color:#fff;font-weight:bold;font-size:0.85rem">{name}</span>'
                    f'<span style="color:#7ab8e8;font-size:0.7rem;margin-left:auto">'
                    f'{num}走</span>'
                    f'</div>'
                    f'{bars_html}'
                    f'</div>',
                    unsafe_allow_html=True,
                )
    else:
        with st.expander("🎯 選手別決まり手（コース別）", expanded=False):
            st.caption("選手別決まり手データを取得できませんでした")

    # ── 買い目カード（3グループ × 3点） ─────────────────────────
    st.markdown("### 🎯 推奨3連単")
    recs = res["recommendations"]
    if not recs:
        st.warning("買い目候補がありません")
    else:
        # ── カード描画ヘルパー ───────────────────────────────────
        def _render_bet_card(r: dict, odds_dict: dict, accent: str):
            """1枚の買い目カードを描画する。accent は枠線色のHEX。"""
            combo       = r["買い目"]
            hit_prob    = r["的中確率"]
            fair_odds   = r["公正オッズ"]
            bet_type    = r["タイプ"]
            actual_odds = odds_dict.get(combo)

            ev = None
            if actual_odds is not None:
                ev = round(actual_odds * (hit_prob / 100), 2)

            if ev is not None and ev >= 1.0:
                bg, border = "#12301a", "#2ecc71"
            else:
                bg, border = "#1a2744", accent

            odds_html = ""
            if actual_odds is not None:
                ev_cls = "ev-positive" if (ev is not None and ev >= 1.0) else "ev-negative"
                ev_str = f'<span class="{ev_cls}">×{ev:.2f}</span>' if ev is not None else ""
                odds_html = (
                    f'<div class="bet-label" style="margin-top:5px">'
                    f'<span class="odds-tag">実オッズ</span>'
                    f'<b style="color:#ffe066">{actual_odds:.1f}倍</b>'
                    f'&nbsp;{ev_str}'
                    f'</div>'
                )
            else:
                odds_html = (
                    '<div class="bet-label" style="margin-top:5px;color:#555">オッズ未公開</div>'
                )

            st.markdown(
                f'<div style="background:{bg};border-left:4px solid {border};'
                f'padding:0.7rem 0.9rem;margin:0.3rem 0;border-radius:6px">'
                f'<div style="color:#aaa;font-size:0.75rem">{bet_type}</div>'
                f'<div style="font-size:1.6rem;font-weight:bold;color:#fff;letter-spacing:3px">{combo}</div>'
                f'<div class="bet-label" style="margin-top:4px">'
                f'<span class="odds-tag">的中確率</span>{hit_prob:.2f}%'
                f'&nbsp;&nbsp;<span class="odds-tag">公正オッズ</span>{fair_odds:.1f}倍'
                f'</div>'
                f'{odds_html}'
                f'</div>',
                unsafe_allow_html=True,
            )

        # ── 買い目グループ: 本命 → 対抗 → 穴（縦積み） ──────
        GROUP_INFO = [
            ("本命", "#f0a500"),   # 黄金
            ("対抗", "#3498db"),   # 青
            ("穴",   "#e74c3c"),   # 赤
        ]
        group_recs = {g: [r for r in recs if r.get("グループ") == g] for g, _ in GROUP_INFO}

        for grp, color in GROUP_INFO:
            cards = group_recs.get(grp, [])
            if not cards:
                continue
            st.markdown(
                f'<div class="group-header" style="color:{color};'
                f'border-bottom:2px solid {color}">{grp}</div>',
                unsafe_allow_html=True,
            )
            for r in cards:
                _render_bet_card(r, odds, color)

        # 凡例
        st.markdown(
            '<div style="font-size:0.75rem; color:#666; margin-top:8px">'
            '🟢 緑枠 = 期待値 &gt; 1.0（バリュー買い目）&nbsp;|&nbsp;'
            '的中確率: Heneryモデル推定確率&nbsp;|&nbsp;'
            '公正オッズ: 確率から算出した理論的な適正倍率'
            '</div>',
            unsafe_allow_html=True,
        )

    # ── 2連単・2連複 推奨買い目 【v3新規 + オッズ対応】 ─────────────
    recs_2ren = res.get("recommendations_2ren", {})
    nitan  = recs_2ren.get("2連単", [])
    nifuku = recs_2ren.get("2連複", [])
    if nitan or nifuku:
        st.markdown("### 🎲 2連単・2連複 推奨")
        st.caption("Heneryモデルによる2連系推奨買い目（上位5点）")

        def _render_2ren_card(r2: dict, accent: str):
            """2連系の1枚のカードを描画する。"""
            combo     = r2["買い目"]
            hit_prob  = r2["的中確率"]
            fair_odds = r2["公正オッズ"]
            actual    = r2.get("実オッズ")
            ev        = r2.get("期待値")

            if ev is not None and ev >= 1.0:
                bg, border = "#12301a", "#2ecc71"
            else:
                bg, border = "#1a2744", accent

            odds_html = ""
            if actual is not None:
                ev_cls = "ev-positive" if (ev is not None and ev >= 1.0) else "ev-negative"
                ev_str = f'<span class="{ev_cls}">×{ev:.2f}</span>' if ev is not None else ""
                odds_html = (
                    f'<div style="margin-top:3px">'
                    f'<span class="odds-tag">実オッズ</span>'
                    f'<b style="color:#ffe066">{actual:.1f}倍</b>'
                    f'&nbsp;{ev_str}'
                    f'</div>'
                )

            st.markdown(
                f'<div style="background:{bg};border-left:3px solid {border};'
                f'padding:0.4rem 0.8rem;margin:0.2rem 0;border-radius:6px">'
                f'<span style="font-size:1.2rem;font-weight:bold;color:#fff;letter-spacing:2px">{combo}</span>'
                f'&nbsp;&nbsp;<span style="color:#7ab8e8;font-size:0.78rem">'
                f'{hit_prob:.2f}%</span>'
                f'&nbsp;<span style="color:#aaa;font-size:0.78rem">'
                f'({fair_odds:.1f}倍)</span>'
                f'{odds_html}'
                f'</div>',
                unsafe_allow_html=True,
            )

        if nitan:
            st.markdown(
                '<div class="group-header" style="color:#f0a500;'
                'border-bottom:2px solid #f0a500">2連単</div>',
                unsafe_allow_html=True,
            )
            for r2 in nitan:
                _render_2ren_card(r2, "#f0a500")
        if nifuku:
            st.markdown(
                '<div class="group-header" style="color:#3498db;'
                'border-bottom:2px solid #3498db;margin-top:0.8rem">2連複</div>',
                unsafe_allow_html=True,
            )
            for r2 in nifuku:
                _render_2ren_card(r2, "#3498db")

    st.markdown("---")

    # ── 艇別パフォーマンスレーダーチャート ──────────────────────
    st.markdown("### 📡 艇別パフォーマンスレーダーチャート")
    st.caption("各艇の指標をレーダー表示（スコアはレース内相対値）")

    def _make_radar_chart(df_scored: pd.DataFrame) -> go.Figure:
        """scored DataFrameから艇別レーダーチャートを生成する。"""
        dims = [
            ("蒲郡勝率",   "蒲郡勝率",   False),
            ("蒲郡2連率",  "蒲郡2連率",  False),
            ("展示タイム", "展示タイム", True),
            ("モーター2連率", "モーター",  False),
            ("スタートタイミング", "ST速さ", True),
            ("win_prob",   "1着確率",    False),
            ("コース別1着率", "C別1着率", False),
            ("直近平均着順", "好調度", True),    # 低いほど好調→反転
        ]

        # 各次元の有効値を0-100正規化
        # 競艇公式カラー: 1白, 2黒, 3赤, 4青, 5黄, 6緑
        boat_colors = ["#ffffff", "#000000", "#e74c3c", "#2563eb", "#f1c40f", "#2ecc71"]
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

            vmin, vmax = min(vals), max(vals)
            rng = (vmax - vmin) or 1.0
            normed = [(v - vmin) / rng * 100 for v in vals]
            if invert:
                normed = [100 - x for x in normed]

            # ゼロ値（欠損）艇は 50 に補正
            valid_mask = [v > 0 for v in vals]
            if any(valid_mask):
                for i in range(len(normed)):
                    if not valid_mask[i]:
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
                angularaxis=dict(tickfont=dict(size=11)),
                bgcolor="#0e1726",
            ),
            showlegend=True,
            legend=dict(font=dict(color="#ccc"), bgcolor="rgba(0,0,0,0)"),
            paper_bgcolor="#0e1726",
            margin=dict(l=20, r=20, t=20, b=20),
            height=340,
        )
        return fig

    radar_fig = _make_radar_chart(scored)
    if radar_fig.data:
        st.plotly_chart(radar_fig, use_container_width=True)
    else:
        st.caption("レーダーチャートを表示するのに十分なデータがありません")

    st.markdown("---")

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
            ]),
            ("モデル・予想生成", [
                ("直近成績モメンタムの重み", f'{_W["momentum"]}'),
                ("優勝戦イン強化", f'{_W["grade_final_boost"]}'),
                ("高橋アナ予想ブースト", f'{_W["taka_boost"]}'),
                ("選手別決まり手適合度の重み", f'{_W.get("racer_kimarite_weight", 3.0)}'),
                ("Henery gamma", f'{_W["henery_gamma"]}'),
                ("期待値閾値（穴選定）", f'{_W["ev_threshold"]}'),
                ("展開連動パターン強度", f'{_W.get("tenkai_rendo_strength", 0.0)}'),
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
