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
import pandas as pd
import numpy as np
import streamlit as st
import plotly.graph_objects as go
from datetime import date, datetime

sys.path.insert(0, os.path.dirname(__file__))
from config import JYNAME
from race_scraper import (
    fetch_full_race_data, generate_sample_data,
    fetch_deadline, fetch_odds_3t, fetch_odds_2tf, fetch_gamagori_taka,
    fetch_race_result,
)
from scorer import predict
from result_tracker import (
    save_prediction, record_result,
    get_accuracy_stats, get_recent_predictions,
)
from backtester import (
    load_backtest_data, collect_historical_data,
    optimize_from_backtest, apply_to_config, WEIGHT_KEYS,
)

st.set_page_config(page_title="蒲郡競艇AI予想", page_icon="🚤", layout="centered")

st.markdown("""
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
<style>
    /* ── ベーススタイル ─────────────────────────────────── */
    .main-header { background: linear-gradient(135deg, #0a1628 0%, #1a3a6b 50%, #0d2855 100%); padding: 1rem; border-radius: 10px; margin-bottom: 0.8rem; border: 1px solid #1e5fa8; }
    .main-header h1 { color: #e8f4ff; margin: 0; font-size: 1.3rem; }
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
        .main-header h1 { font-size: 1.15rem; }
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

st.markdown('<div class="main-header"><h1>🚤 蒲郡ボートレース予想システム</h1></div>', unsafe_allow_html=True)

# ── セッション初期化 ──────────────────────────────────────────────
for key in ("result", "weather", "deadline", "race_no", "date_str", "odds", "odds_2t", "odds_2f", "taka"):
    if key not in st.session_state:
        st.session_state[key] = None

# ── サイドバー ────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### ⚙️ レース設定")
    race_no   = st.selectbox("レース番号", list(range(1, 13)), index=0)
    race_date = st.date_input("開催日", date.today())
    use_sample = st.checkbox("サンプルデータを使用")
    use_extended = st.checkbox("拡張データ取得（コース別成績・直近成績）", value=False,
                               help="選手個別のコース別1着率・直近10走の成績を取得します（追加で数秒かかります）")
    fetch_btn  = st.button("🔄 予想実行", type="primary", use_container_width=True)

    st.markdown("---")

    # ── 結果自動取得セクション ──────────────────────────────────
    st.markdown("### 📝 レース結果取得")
    st.caption("レース終了後に結果を自動取得して的中率を記録します")
    r_date = st.date_input("結果日付", date.today(), key="r_date")
    r_race = st.selectbox("レース番号", list(range(1, 13)), key="r_race")
    if st.button("🔍 結果を自動取得", use_container_width=True, type="primary"):
        d_str_r = r_date.strftime("%Y%m%d")
        with st.spinner("結果を取得中..."):
            res_data = fetch_race_result(r_race, d_str_r)
        if res_data is None:
            st.warning("結果を取得できませんでした。レースが終了していないか、データ未公開の可能性があります。")
        else:
            actual = record_result(
                d_str_r, r_race,
                res_data["1着"], res_data["2着"], res_data["3着"]
            )
            combo = actual["三連単"]
            if actual["hit"]:
                st.success(f"✅ 的中！ {combo}（{', '.join(actual['hit'])}）")
            else:
                st.info(f"❌ 不的中: {combo}")

    st.markdown("---")

    # ── 的中率統計 ───────────────────────────────────────────────
    st.markdown("### 📊 累積的中率")
    stats = get_accuracy_stats()
    if stats["total"] == 0:
        st.caption("結果がまだ記録されていません")
    else:
        s_col1, s_col2 = st.columns(2)
        s_col1.metric("記録済みレース", stats["total"])
        s_col2.metric("3連単的中", stats["hits"])
        st.metric("的中率", f"{stats['hit_rate']:.1f}%")

        if stats["by_type"]:
            st.caption("▼ タイプ別")
            for t, v in stats["by_type"].items():
                st.markdown(
                    f'<div class="stat-box">'
                    f'<b>{t}</b>: {v["hit"]}/{v["total"]}件 '
                    f'({v["rate"]:.1f}%)</div>',
                    unsafe_allow_html=True
                )

        if stats["by_confidence"]:
            st.caption("▼ 信頼度別")
            for c_key, v in stats["by_confidence"].items():
                st.markdown(
                    f'<div class="stat-box">'
                    f'信頼度 <b>{c_key}</b>: {v["hit"]}/{v["total"]}件 '
                    f'({v["rate"]:.1f}%)</div>',
                    unsafe_allow_html=True
                )

    st.markdown("---")

    # ── ML最適化セクション ─────────────────────────────────────────
    st.markdown("### 🤖 ML最適化")
    bt_data = load_backtest_data()
    if bt_data:
        dates = sorted(set(e["date"] for e in bt_data))
        st.caption(f"バックテストデータ: {len(bt_data)}レース ({len(dates)}日分)")
        st.caption(f"期間: {dates[0]} 〜 {dates[-1]}")
    else:
        st.caption("バックテストデータなし（先にデータを収集してください）")

    with st.expander("📥 過去データ収集", expanded=False):
        bt_days = st.number_input("収集日数", 7, 90, 30, key="bt_days")
        bt_delay = st.number_input("リクエスト間隔(秒)", 0.5, 5.0, 1.0, step=0.5, key="bt_delay")
        if st.button("過去データ収集開始", use_container_width=True):
            with st.status("データ収集中...", expanded=True) as status:
                prog_text = st.empty()
                prog_bar = st.progress(0)

                def _on_collect_progress(current, total, msg):
                    if total > 0:
                        prog_bar.progress(min(current / total, 1.0))
                    prog_text.text(msg)

                collect_historical_data(
                    days=int(bt_days), delay=float(bt_delay),
                    progress_callback=_on_collect_progress,
                )
                status.update(label="収集完了！", state="complete")
            st.rerun()

    if bt_data and len(bt_data) >= 10:
        with st.expander("🧠 重み最適化", expanded=False):
            bt_iter = st.number_input("イテレーション数", 50, 1000, 300, key="bt_iter")
            if st.button("最適化実行", use_container_width=True, type="primary"):
                with st.status("最適化中...", expanded=True) as status:
                    opt_text = st.empty()
                    opt_bar = st.progress(0)

                    def _on_opt_progress(current, total, msg):
                        if total > 0:
                            opt_bar.progress(min(current / total, 1.0))
                        opt_text.text(msg)

                    ml_res = optimize_from_backtest(
                        max_iter=int(bt_iter),
                        progress_callback=_on_opt_progress,
                    )
                    status.update(label="最適化完了！", state="complete")
                if ml_res:
                    st.session_state.ml_result = ml_res

        if "ml_result" in st.session_state and st.session_state.ml_result:
            r = st.session_state.ml_result
            st.metric(
                "スコア改善",
                f"{r['hit_rate']:.4f}",
                delta=f"{r['improvement']:+.4f}",
            )
            bd = r.get("baseline_details", {})
            od = r.get("optimized_details", {})
            if bd and od:
                st.caption(
                    f"3連単的中: {bd['hits']}→{od['hits']}件 / "
                    f"1着率: {bd['top1_rate']:.1f}%→{od['top1_rate']:.1f}%"
                )
            if st.button("✅ config.pyに反映", type="primary", use_container_width=True):
                apply_to_config(r["optimized_weights"])
                st.success("config.py を更新しました！次回予想から新しい重みが適用されます。")
                st.session_state.ml_result = None

# ── 予想実行 ─────────────────────────────────────────────────────
if fetch_btn:
    with st.spinner("データを取得中..."):
        d_str = race_date.strftime("%Y%m%d")
        if use_sample:
            df_raw, weather = generate_sample_data(race_no)
            deadline = "19:44"
            odds     = {}
            odds_2t  = {}
            odds_2f  = {}
            taka     = {"available": False}
        else:
            df_raw, weather = fetch_full_race_data(race_no, d_str, extended=use_extended)
            deadline = fetch_deadline(race_no, d_str)
            odds     = fetch_odds_3t(race_no, d_str)
            odds_2tf = fetch_odds_2tf(race_no, d_str)
            odds_2t  = odds_2tf.get("2連単", {})
            odds_2f  = odds_2tf.get("2連複", {})
            taka     = fetch_gamagori_taka(race_no, d_str)

        if not df_raw.empty:
            result = predict(
                df_raw, weather, race_no,
                taka_data=taka, odds_dict=odds,
                odds_2t=odds_2t, odds_2f=odds_2f,
            )
            st.session_state.result   = result
            st.session_state.weather  = weather
            st.session_state.deadline = deadline
            st.session_state.race_no  = race_no
            st.session_state.date_str = d_str
            st.session_state.odds     = odds
            st.session_state.odds_2t  = odds_2t
            st.session_state.odds_2f  = odds_2f
            st.session_state.taka     = taka

            confidence = (
                result["scored_df"]["confidence"].iloc[0]
                if "confidence" in result["scored_df"].columns else "-"
            )
            save_prediction(d_str, race_no, result["recommendations"], weather, confidence)

            odds_msg_parts = []
            if odds:
                odds_msg_parts.append(f"3連単 {len(odds)}通り")
            if odds_2t:
                odds_msg_parts.append(f"2連単 {len(odds_2t)}通り")
            if odds_2f:
                odds_msg_parts.append(f"2連複 {len(odds_2f)}通り")
            if odds_msg_parts:
                st.success(f"オッズ取得: {' / '.join(odds_msg_parts)}")
            else:
                st.info("オッズはまだ未公開か取得できませんでした（締切前に再実行すると表示されます）")
            if taka and taka.get("available"):
                boats_str = "・".join(taka.get("yoso_boats", [])) or "—"
                st.success(f"高橋アナ予想取得！ 予想艇: {boats_str}")
            else:
                st.info("高橋アナ予想は未公開またはデータ取得外です（レース直前に再実行すると反映されます）")
        else:
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

    # ── 分析データテーブル ────────────────────────────────────────
    rno = st.session_state.race_no

    # モバイル向け: 主要列のみのコンパクト表示をデフォルト、全列は折りたたみ
    mobile_cols_raw = ["枠番", "選手名", "全国勝率", "蒲郡勝率", "展示タイム", "win_prob"]
    mobile_cols = [c for c in mobile_cols_raw if c in scored.columns]

    mobile_rename = {"win_prob": "1着確率(%)", "展示タイム": "展示T"}
    mobile_df = scored[mobile_cols].rename(columns=mobile_rename)
    mobile_fmt = {"1着確率(%)": "{:.1f}%", "全国勝率": "{:.2f}", "蒲郡勝率": "{:.2f}"}
    if "展示T" in mobile_df.columns:
        mobile_fmt["展示T"] = lambda x: f"{x:.2f}" if pd.notnull(x) and x > 0 else "-"

    st.markdown(f"### 📋 {rno}R 分析データ")

    def style_prob_row(row, df):
        is_max = row["1着確率(%)"] == df["1着確率(%)"].max()
        return ["background-color: rgba(240,165,0,0.15)" if is_max else ""] * len(row)

    styled_mobile = mobile_df.style.apply(style_prob_row, df=mobile_df, axis=1).format(mobile_fmt)
    st.dataframe(styled_mobile, use_container_width=True, hide_index=True)

    # 全データは折りたたみの中に
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
    base_cols += ["展示タイム", "チルト"]
    if "スタートタイミング" in scored.columns:
        extra_cols.append("スタートタイミング")
    for col in ["まわり足タイム", "直線タイム", "一周タイム", "コース別1着率", "直近平均着順"]:
        if col in scored.columns:
            extra_cols.append(col)
    base_cols += ["win_prob", "highlight_reason"]

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

    with st.expander("📊 全データ表示", expanded=False):
        def style_row(row):
            is_max = row["1着確率(%)"] == display_df["1着確率(%)"].max()
            return ["background-color: rgba(240,165,0,0.15)" if is_max else ""] * len(row)

        styled = display_df.style.apply(style_row, axis=1).format(fmt)
        st.dataframe(styled, use_container_width=True, hide_index=True)

    # ── 締め切り時刻 ──────────────────────────────────────────────
    st.markdown("### 🎯 推奨3連単")
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

    # ── 買い目カード（3グループ × 3点） ─────────────────────────
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

    # ── 直近予想履歴 ──────────────────────────────────────────────
    with st.expander("📜 直近予想履歴（直近10件）", expanded=False):
        recent = get_recent_predictions(10)
        if not recent:
            st.caption("履歴がありません")
        else:
            rows = []
            for e in recent:
                actual  = e.get("actual") or {}
                hit_str = (
                    "的中 " + ", ".join(actual.get("hit", []))
                    if actual.get("hit")
                    else ("不的中" if actual else "未記録")
                )
                top_rec = e["recommendations"][0]["買い目"] if e.get("recommendations") else "-"
                rows.append({
                    "日付":     e["date"],
                    "R":        e["race_no"],
                    "本命買い目": top_rec,
                    "実結果":   actual.get("三連単", "-"),
                    "判定":     hit_str,
                    "信頼度":   e.get("confidence", "-"),
                })
            st.dataframe(pd.DataFrame(rows), use_container_width=True)
