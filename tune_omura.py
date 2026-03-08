"""
大村競艇場 専用チューニングスクリプト

【使い方】
1. データ収集:        python tune_omura.py collect --days 30
2. チューニング:      python tune_omura.py tune
3. 収集＋チューニング: python tune_omura.py all --days 30

処理が重くならないよう、収集は5日単位のバッチに分割して実行する。
"""
import json
import sys
import os
import time
import random
import argparse
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.optimize import minimize

PROJECT_DIR = Path(__file__).parent
sys.path.insert(0, str(PROJECT_DIR))

OMURA_BACKTEST_FILE = PROJECT_DIR / "backtest_data_omura.json"
OMURA_TUNE_RESULT_FILE = PROJECT_DIR / "_omura_tune_result.json"
VENUE_CODE = "24"
BATCH_SIZE_DAYS = 5  # 5日単位でバッチ処理（サーバー負荷軽減）


# ────────────────────────────────────────────────────────────────
# データ永続化
# ────────────────────────────────────────────────────────────────

def load_omura_data() -> list[dict]:
    if OMURA_BACKTEST_FILE.exists():
        try:
            return json.loads(OMURA_BACKTEST_FILE.read_text(encoding="utf-8"))
        except Exception:
            return []
    return []


def _save_omura_data(data: list[dict]) -> None:
    data.sort(key=lambda x: (x["date"], x["race_no"]))
    OMURA_BACKTEST_FILE.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


# ────────────────────────────────────────────────────────────────
# 1. 過去データ収集（バッチ分割）
# ────────────────────────────────────────────────────────────────

def collect_omura_data(days: int = 30, delay: float = 1.0) -> list[dict]:
    """
    大村の過去 days 日分のレースデータを BATCH_SIZE_DAYS 日ずつ収集する。
    """
    from race_scraper import (
        fetch_race_card, fetch_before_info, fetch_race_result,
        set_thread_venue,
    )

    # 大村の会場コードをセット
    set_thread_venue(VENUE_CODE)

    def _normalize_result(raw_result):
        """fetch_race_result の返り値を backtester 互換形式に変換する。"""
        if raw_result is None:
            return None
        # 旧形式（"1着", "2着", "3着", "三連単" あり）ならそのまま
        if "1着" in raw_result and "三連単" in raw_result:
            return raw_result
        # 新形式（"着順" リスト）→ 旧形式に変換
        finishers = raw_result.get("着順", [])
        if len(finishers) < 3:
            return None
        # 着順でソートして1〜3着を取得
        finishers_sorted = sorted(finishers, key=lambda x: x.get("着", 99))
        f1 = finishers_sorted[0].get("枠番")
        f2 = finishers_sorted[1].get("枠番")
        f3 = finishers_sorted[2].get("枠番")
        if not all([f1, f2, f3]):
            return None
        return {
            "1着": f1, "2着": f2, "3着": f3,
            "三連単": f"{f1}-{f2}-{f3}",
        }

    existing = load_omura_data()
    existing_keys = {(e["date"], e["race_no"]) for e in existing}
    today = datetime.now()

    total_new = 0
    total_batches = (days + BATCH_SIZE_DAYS - 1) // BATCH_SIZE_DAYS

    for batch_idx in range(total_batches):
        start_day = batch_idx * BATCH_SIZE_DAYS + 1
        end_day = min((batch_idx + 1) * BATCH_SIZE_DAYS, days)

        print(f"\n{'='*60}")
        print(f"バッチ {batch_idx+1}/{total_batches}: "
              f"{start_day}〜{end_day}日前を処理中")
        print(f"{'='*60}")

        batch_new = []
        for d in range(start_day, end_day + 1):
            target_date = today - timedelta(days=d)
            date_str = target_date.strftime("%Y%m%d")
            date_display = target_date.strftime("%Y/%m/%d")
            day_count = 0
            day_has_races = True  # R1がなければ日ごとスキップ

            for race_no in range(1, 13):
                if not day_has_races:
                    break

                if (date_str, race_no) in existing_keys:
                    continue

                print(f"  {date_display} R{race_no}: 取得中...", end="", flush=True)

                try:
                    time.sleep(delay)
                    card_df = fetch_race_card(race_no, date_str)
                    if card_df.empty:
                        if race_no == 1:
                            print(f" → 開催なし（{date_display}スキップ）")
                            day_has_races = False
                        else:
                            print(" → データなし")
                        continue

                    time.sleep(delay)
                    ex_df, weather = fetch_before_info(race_no, date_str)

                    if not ex_df.empty:
                        merged = pd.merge(card_df, ex_df, on="枠番", how="left")
                    else:
                        merged = card_df
                        merged["展示タイム"] = None
                        merged["チルト"] = 0.0
                        merged["体重"] = None
                        merged["周回タイム"] = None

                    time.sleep(delay)
                    result = _normalize_result(
                        fetch_race_result(race_no, date_str))
                    if result is None:
                        print(" → 結果なし")
                        continue

                    records = merged.where(merged.notna(), None).to_dict("records")
                    entry = {
                        "date": date_str,
                        "race_no": race_no,
                        "venue": VENUE_CODE,
                        "race_data": records,
                        "weather": weather,
                        "actual": result,
                    }
                    batch_new.append(entry)
                    existing_keys.add((date_str, race_no))
                    day_count += 1
                    print(f" → OK ({result.get('三連単', '?')})")

                except Exception as e:
                    print(f" → エラー: {e}")
                    continue

            print(f"  [{date_display}] {day_count} レース取得")

        # バッチごとに保存（中断耐性）
        if batch_new:
            existing.extend(batch_new)
            _save_omura_data(existing)
            total_new += len(batch_new)
            print(f"\n  バッチ {batch_idx+1} 保存: +{len(batch_new)} 件 "
                  f"(累計 {len(existing)} 件)")

        # バッチ間でやや長めに待機（サーバー負荷軽減）
        if batch_idx < total_batches - 1:
            print(f"  次のバッチまで3秒待機...")
            time.sleep(3)

    print(f"\n[collect] 完了: 合計 {len(existing)} レース (新規 {total_new} 件)")
    return existing


# ────────────────────────────────────────────────────────────────
# 2. バックテスト実行
# ────────────────────────────────────────────────────────────────

def run_backtest(data, weight_overrides=None, calc_trifecta=False):
    """大村用: 指定データに対してスコアリングし指標を返す。"""
    from scorer import calculate_scores, generate_recommendations
    from config import VENUE_CONFIGS

    W = dict(VENUE_CONFIGS[VENUE_CODE]["score_weights"])

    if weight_overrides:
        for k, v in weight_overrides.items():
            W[k] = v

    # 一時的にVENUE_CONFIGSを上書き
    original_w = dict(VENUE_CONFIGS[VENUE_CODE]["score_weights"])
    VENUE_CONFIGS[VENUE_CODE]["score_weights"] = W

    try:
        ll = 0.0
        top1_hits = 0
        tri_hits = 0
        total = 0
        b1_probs, b1_wins, winner_probs = [], [], []

        for entry in data:
            actual = entry.get("actual", {})
            winner = str(actual.get("1着", ""))
            if not winner:
                continue
            df = pd.DataFrame(entry["race_data"])
            weather = entry.get("weather", {})
            race_no = entry.get("race_no", 1)
            try:
                scored = calculate_scores(df, weather, race_no,
                                          venue_code=VENUE_CODE)
            except Exception:
                continue
            if "win_prob" not in scored.columns:
                continue

            total += 1
            frames = scored["枠番"].astype(str).values
            probs = scored["win_prob"].values / 100.0

            for i, f in enumerate(frames):
                if f == "1":
                    b1_probs.append(probs[i])
                    b1_wins.append(1 if winner == "1" else 0)
                    break

            wp = 0.0
            for i, f in enumerate(frames):
                if f == winner:
                    wp = probs[i]
                    break
            winner_probs.append(wp)
            ll += np.log(max(wp, 1e-9))

            if frames[np.argmax(probs)] == winner:
                top1_hits += 1

            if calc_trifecta:
                try:
                    actual_2 = str(actual.get("2着", ""))
                    actual_3 = str(actual.get("3着", ""))
                    recs = generate_recommendations(
                        scored, venue_code=VENUE_CODE)
                    if isinstance(recs, tuple):
                        recs = recs[0]
                    combo = f"{winner}-{actual_2}-{actual_3}"
                    if any(r["買い目"] == combo for r in recs):
                        tri_hits += 1
                except Exception:
                    pass

        if total == 0:
            return {"total": 0, "avg_ll": -99, "top1_rate": 0,
                    "tri_rate": 0, "b1_pred": 0, "b1_actual": 0,
                    "b1_gap": 0, "calibration": []}

        b1p = np.array(b1_probs)
        b1w = np.array(b1_wins)
        cal = []
        for lo, hi in [(0, 30), (30, 40), (40, 50), (50, 60), (60, 70), (70, 100)]:
            mask = (b1p * 100 >= lo) & (b1p * 100 < hi)
            if mask.sum() > 0:
                cal.append({
                    "range": f"{lo}-{hi}%", "count": int(mask.sum()),
                    "predicted": round(b1p[mask].mean() * 100, 1),
                    "actual": round(b1w[mask].mean() * 100, 1),
                    "gap": round((b1w[mask].mean() - b1p[mask].mean()) * 100, 1),
                })

        return {
            "total": total,
            "avg_ll": round(ll / max(total, 1), 4),
            "top1_rate": round(top1_hits / max(total, 1) * 100, 2),
            "tri_rate": round(tri_hits / max(total, 1) * 100, 2) if calc_trifecta else None,
            "b1_pred": round(b1p.mean() * 100, 2),
            "b1_actual": round(b1w.mean() * 100, 2),
            "b1_gap": round((b1p.mean() - b1w.mean()) * 100, 2),
            "calibration": cal,
        }
    finally:
        VENUE_CONFIGS[VENUE_CODE]["score_weights"] = original_w


# ────────────────────────────────────────────────────────────────
# 3. チューニング（Nelder-Mead 最適化）
# ────────────────────────────────────────────────────────────────

TUNE_PARAMS = [
    "individual_temp",
    "win_rate", "local_win_rate", "nat2_rate", "loc2_rate",
    "exhibit_time", "exhibit_top_bonus",
    "night_boost", "calm_in_boost", "st_weight",
    "motor2_rate", "boat2_rate",
    "course_win_rate", "momentum",
    "mawari_time", "chokusen_time", "turn_master_bonus",
    "fl_f_penalty",
    "kado_boost", "makuri_sashi",
    "rank_a1", "rank_a2",
    "stabilizer_in_boost", "stabilizer_equalize",
    "prob_cap",
]


def tune_omura():
    """大村の過去データでパラメータチューニングを実行する。"""
    from config import VENUE_CONFIGS

    all_data = load_omura_data()
    if len(all_data) < 20:
        print(f"[tune] データが {len(all_data)} 件しかありません（最低20件必要）")
        return None

    W = dict(VENUE_CONFIGS[VENUE_CODE]["score_weights"])

    # サンプル分割: 80件で最適化、全件で検証
    random.seed(42)
    sample_size = min(100, len(all_data))
    SAMPLE = random.sample(all_data, sample_size)

    print(f"全データ: {len(all_data)} レース / 最適化サンプル: {sample_size} レース")

    # ── Step 1: 現状確認 ──
    print("\n" + "=" * 60)
    print(f"現在のモデル - 全{len(all_data)}レース")
    print("=" * 60)
    cur = run_backtest(all_data, calc_trifecta=True)
    print(f"  LL={cur['avg_ll']:.4f}  Top1={cur['top1_rate']:.1f}%  "
          f"3連単={cur['tri_rate']:.1f}%")
    print(f"  1号艇: 予測={cur['b1_pred']:.1f}% 実際={cur['b1_actual']:.1f}% "
          f"差={cur['b1_gap']:+.1f}%")
    for b in cur["calibration"]:
        print(f"    {b['range']:>8s}: n={b['count']:3d}  "
              f"予測={b['predicted']:5.1f}%  実際={b['actual']:5.1f}%  "
              f"差={b['gap']:+5.1f}%")

    # ── Step 2: individual_temp グリッドサーチ ──
    print("\n" + "=" * 60)
    print(f"individual_temp グリッドサーチ ({sample_size}レース)")
    print("=" * 60)
    best_ll, best_temp = -9999, 5.0
    for t in [3, 5, 7, 9, 10, 12, 15, 20, 25, 30]:
        r = run_backtest(SAMPLE, {"individual_temp": float(t)})
        mark = " <-- BEST" if r["avg_ll"] > best_ll else ""
        if r["avg_ll"] > best_ll:
            best_ll, best_temp = r["avg_ll"], t
        print(f"  temp={t:3d}  LL={r['avg_ll']:.4f}  "
              f"Top1={r['top1_rate']:5.1f}%  "
              f"1号艇={r['b1_pred']:5.1f}%(実際{r['b1_actual']:.1f}%){mark}")
    print(f"  => 最適 temp = {best_temp}")

    # ── Step 3: Nelder-Mead 全パラメータ最適化 ──
    print("\n" + "=" * 60)
    print(f"Nelder-Mead最適化 ({sample_size}レース, 初期temp={best_temp})")
    print("=" * 60)

    x0 = [W.get(k, 1.0) for k in TUNE_PARAMS]
    x0[0] = float(best_temp)

    bounds = []
    for k in TUNE_PARAMS:
        if k == "individual_temp":
            bounds.append((3.0, 50.0))
        elif k == "st_weight":
            bounds.append((5.0, 80.0))
        elif k == "prob_cap":
            bounds.append((50.0, 85.0))
        elif k in ("rank_a1", "rank_a2"):
            bounds.append((0.0, 10.0))
        elif k in ("stabilizer_in_boost",):
            bounds.append((0.0, 10.0))
        elif k in ("stabilizer_equalize",):
            bounds.append((0.0, 0.5))
        else:
            bounds.append((0.01, 15.0))

    n_eval = [0]

    def objective(x):
        ov = dict(zip(TUNE_PARAMS, x))
        r = run_backtest(SAMPLE, ov)
        # 対数尤度を最大化 + 1号艇キャリブレーション誤差を小さく
        cost = -r["avg_ll"] + abs(r["b1_gap"]) * 0.005
        n_eval[0] += 1
        if n_eval[0] % 50 == 0:
            print(f"  [{n_eval[0]:4d}] LL={r['avg_ll']:.4f} "
                  f"Top1={r['top1_rate']:.1f}% "
                  f"1号艇gap={r['b1_gap']:+.1f}%")
        return cost

    res = minimize(objective, x0, method="Nelder-Mead",
                   options={"maxiter": 600, "xatol": 0.05,
                            "fatol": 0.0005, "adaptive": True})
    opt = {k: round(v, 3) for k, v in zip(TUNE_PARAMS, res.x)}
    print(f"  完了 ({n_eval[0]} evaluations)")

    # ── Step 4: 全データで最終検証 ──
    print("\n" + "=" * 60)
    print(f"最終検証（全{len(all_data)}レース・3連単含む）")
    print("=" * 60)
    final = run_backtest(all_data, opt, calc_trifecta=True)
    print(f"  LL={final['avg_ll']:.4f}  Top1={final['top1_rate']:.1f}%  "
          f"3連単={final['tri_rate']:.1f}%")
    print(f"  1号艇: 予測={final['b1_pred']:.1f}% 実際={final['b1_actual']:.1f}% "
          f"差={final['b1_gap']:+.1f}%")
    for b in final["calibration"]:
        print(f"    {b['range']:>8s}: n={b['count']:3d}  "
              f"予測={b['predicted']:5.1f}%  実際={b['actual']:5.1f}%  "
              f"差={b['gap']:+5.1f}%")

    # ── 比較 ──
    print("\n" + "=" * 60)
    print("Before vs After")
    print("=" * 60)
    print(f"  対数尤度    : {cur['avg_ll']:.4f} -> {final['avg_ll']:.4f} "
          f"({final['avg_ll']-cur['avg_ll']:+.4f})")
    print(f"  Top-1的中率 : {cur['top1_rate']:.1f}% -> {final['top1_rate']:.1f}%")
    print(f"  3連単的中率 : {cur['tri_rate']:.1f}% -> {final['tri_rate']:.1f}%")
    print(f"  1号艇予測   : {cur['b1_pred']:.1f}% -> {final['b1_pred']:.1f}% "
          f"(実際={final['b1_actual']:.1f}%)")

    # ── パラメータ差分 ──
    print("\n  パラメータ変更:")
    for k, v in opt.items():
        old = W.get(k, "-")
        if isinstance(old, (int, float)):
            print(f"    {k:30s}: {old:8.3f} -> {v:8.3f}  ({v-old:+.3f})")

    # ── 保存 ──
    out = {
        "venue": VENUE_CODE,
        "venue_name": "大村",
        "optimized_weights": opt,
        "before": cur,
        "after": final,
        "n_races": len(all_data),
        "timestamp": datetime.now().isoformat(),
    }
    OMURA_TUNE_RESULT_FILE.write_text(
        json.dumps(out, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"\n結果を {OMURA_TUNE_RESULT_FILE} に保存しました")

    # config.py に反映するためのコードスニペットを出力
    print("\n" + "=" * 60)
    print("config.py に反映するには以下を _OMURA_SCORE_WEIGHTS.update に追加:")
    print("=" * 60)
    print("_OMURA_SCORE_WEIGHTS.update({")
    for k, v in opt.items():
        print(f'    "{k}": {v},')
    print("})")

    return out


# ────────────────────────────────────────────────────────────────
# CLI
# ────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="大村競艇場 データ収集＆チューニング")
    parser.add_argument(
        "command", choices=["collect", "tune", "all"],
        help="collect=データ収集, tune=チューニング, all=両方")
    parser.add_argument(
        "--days", type=int, default=30,
        help="収集する日数 (デフォルト: 30)")
    parser.add_argument(
        "--delay", type=float, default=1.0,
        help="リクエスト間隔秒 (デフォルト: 1.0)")
    args = parser.parse_args()

    if args.command in ("collect", "all"):
        collect_omura_data(days=args.days, delay=args.delay)

    if args.command in ("tune", "all"):
        tune_omura()


if __name__ == "__main__":
    main()
