"""
ベイズモデルのパラメータチューニング（高速版）

最適化ループ: 80レースサンプルで高速評価
最終検証: 全240レースで精度確認
"""
import json
import sys
import os
import random
import numpy as np
import pandas as pd
from scipy.optimize import minimize

sys.path.insert(0, os.path.dirname(__file__))
from scorer import calculate_scores, generate_recommendations
from config import SCORE_WEIGHTS

# ── データ読み込み ────────────────────────────────────────────────
with open(os.path.join(os.path.dirname(__file__), "backtest_data.json"), encoding="utf-8") as f:
    ALL_DATA = json.load(f)

random.seed(42)
SAMPLE = random.sample(ALL_DATA, 80)
print(f"全データ: {len(ALL_DATA)}レース / 最適化サンプル: {len(SAMPLE)}レース")


def run_backtest(data, weight_overrides=None, calc_trifecta=False):
    """指定データに対してスコアリングし指標を返す。"""
    original = {}
    if weight_overrides:
        for k, v in weight_overrides.items():
            original[k] = SCORE_WEIGHTS.get(k)
            SCORE_WEIGHTS[k] = v
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
                scored = calculate_scores(df, weather, race_no)
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
                    recs, _ = generate_recommendations(scored)
                    combo = f"{winner}-{actual_2}-{actual_3}"
                    if any(r["買い目"] == combo for r in recs):
                        tri_hits += 1
                except Exception:
                    pass

        b1p = np.array(b1_probs)
        b1w = np.array(b1_wins)
        cal = []
        for lo, hi in [(0, 30), (30, 40), (40, 50), (50, 60), (60, 100)]:
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
        if weight_overrides:
            for k, v in original.items():
                if v is not None:
                    SCORE_WEIGHTS[k] = v
                elif k in SCORE_WEIGHTS:
                    del SCORE_WEIGHTS[k]


if __name__ == "__main__":
    # ── Step 1: 現状確認 ──
    print("=" * 60)
    print("現在のモデル（ベイズ結合・temp=15）- 全240レース")
    print("=" * 60)
    cur = run_backtest(ALL_DATA, calc_trifecta=True)
    print(f"  LL={cur['avg_ll']:.4f}  Top1={cur['top1_rate']:.1f}%  3連単={cur['tri_rate']:.1f}%")
    print(f"  1号艇: 予測={cur['b1_pred']:.1f}% 実際={cur['b1_actual']:.1f}% 差={cur['b1_gap']:+.1f}%")
    for b in cur["calibration"]:
        print(f"    {b['range']:>8s}: n={b['count']:3d}  予測={b['predicted']:5.1f}%  実際={b['actual']:5.1f}%  差={b['gap']:+5.1f}%")

    # ── Step 2: individual_temp グリッドサーチ（80レース） ──
    print("\n" + "=" * 60)
    print("individual_temp グリッドサーチ (80レース)")
    print("=" * 60)
    best_ll, best_temp = -9999, 15.0
    for t in [5, 7, 9, 10, 11, 12, 13, 14, 15, 17, 20, 25, 30]:
        r = run_backtest(SAMPLE, {"individual_temp": float(t)})
        mark = " <-- BEST" if r["avg_ll"] > best_ll else ""
        if r["avg_ll"] > best_ll:
            best_ll, best_temp = r["avg_ll"], t
        print(f"  temp={t:3d}  LL={r['avg_ll']:.4f}  Top1={r['top1_rate']:5.1f}%  1号艇={r['b1_pred']:5.1f}%(実際{r['b1_actual']:.1f}%){mark}")
    print(f"  => 最適 temp = {best_temp}")

    # ── Step 3: 全ウェイト最適化（80レース） ──
    print("\n" + "=" * 60)
    print(f"Nelder-Mead最適化 (80レース, 初期temp={best_temp})")
    print("=" * 60)

    params = [
        "individual_temp",
        "win_rate", "local_win_rate", "nat2_rate", "loc2_rate",
        "exhibit_time", "exhibit_top_bonus",
        "night_boost", "calm_in_boost", "st_weight",
        "motor2_rate", "boat2_rate",
        "course_win_rate", "momentum",
        "mawari_time", "chokusen_time", "turn_master_bonus",
        "fl_f_penalty",
    ]
    x0 = [SCORE_WEIGHTS.get(k, 1.0) for k in params]
    x0[0] = float(best_temp)

    bounds = []
    for k in params:
        if k == "individual_temp": bounds.append((5.0, 50.0))
        elif k == "st_weight": bounds.append((5.0, 80.0))
        else: bounds.append((0.01, 15.0))

    n_eval = [0]
    def objective(x):
        ov = dict(zip(params, x))
        r = run_backtest(SAMPLE, ov)
        cost = -r["avg_ll"] + abs(r["b1_gap"]) * 0.005
        n_eval[0] += 1
        if n_eval[0] % 50 == 0:
            print(f"  [{n_eval[0]:4d}] LL={r['avg_ll']:.4f} Top1={r['top1_rate']:.1f}% 1号艇gap={r['b1_gap']:+.1f}%")
        return cost

    res = minimize(objective, x0, method="Nelder-Mead",
                   options={"maxiter": 500, "xatol": 0.05, "fatol": 0.0005, "adaptive": True})
    opt = {k: round(v, 3) for k, v in zip(params, res.x)}
    print(f"  完了 ({n_eval[0]} evaluations)")

    # ── Step 4: 全240レースで最終検証 ──
    print("\n" + "=" * 60)
    print("最終検証（全240レース・3連単含む）")
    print("=" * 60)
    final = run_backtest(ALL_DATA, opt, calc_trifecta=True)
    print(f"  LL={final['avg_ll']:.4f}  Top1={final['top1_rate']:.1f}%  3連単={final['tri_rate']:.1f}%")
    print(f"  1号艇: 予測={final['b1_pred']:.1f}% 実際={final['b1_actual']:.1f}% 差={final['b1_gap']:+.1f}%")
    for b in final["calibration"]:
        print(f"    {b['range']:>8s}: n={b['count']:3d}  予測={b['predicted']:5.1f}%  実際={b['actual']:5.1f}%  差={b['gap']:+5.1f}%")

    # ── 比較 ──
    print("\n" + "=" * 60)
    print("Before vs After")
    print("=" * 60)
    print(f"  対数尤度    : {cur['avg_ll']:.4f} -> {final['avg_ll']:.4f} ({final['avg_ll']-cur['avg_ll']:+.4f})")
    print(f"  Top-1的中率 : {cur['top1_rate']:.1f}% -> {final['top1_rate']:.1f}%")
    print(f"  3連単的中率 : {cur['tri_rate']:.1f}% -> {final['tri_rate']:.1f}%")
    print(f"  1号艇予測   : {cur['b1_pred']:.1f}% -> {final['b1_pred']:.1f}% (実際={final['b1_actual']:.1f}%)")

    # ── パラメータ差分 ──
    print("\n  パラメータ変更:")
    for k, v in opt.items():
        old = SCORE_WEIGHTS.get(k, "-")
        if isinstance(old, (int, float)):
            print(f"    {k:25s}: {old:8.3f} -> {v:8.3f}  ({v-old:+.3f})")

    # ── 保存 ──
    out = {"optimized_weights": opt, "before": cur, "after": final}
    out_path = os.path.join(os.path.dirname(__file__), "_bayes_tune_result.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"\n結果を {out_path} に保存しました")
