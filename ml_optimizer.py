"""
機械学習による SCORE_WEIGHTS 自動最適化モジュール

【使い方】
1. バックテストデータ（推奨）:
     python backtester.py collect --days 30
     python backtester.py optimize

2. prediction_log.json からの最適化（従来方式）:
     python ml_optimizer.py

   ※ バックテストデータがある場合は自動的にそちらを優先使用

【アルゴリズム】
- scipy.optimize.minimize (Nelder-Mead) を使用
- 目的関数: 予想の上位買い目が実際の結果とどれだけ一致するかの負の一致率
- 各重みパラメータの範囲制約あり
"""
import json
import sys
from pathlib import Path

import numpy as np

# パス設定
PROJECT_DIR = Path(__file__).parent
sys.path.insert(0, str(PROJECT_DIR))

LOG_FILE = PROJECT_DIR / "prediction_log.json"


def load_results() -> list[dict]:
    """prediction_log.json から結果記録済みエントリを読み込む。"""
    if not LOG_FILE.exists():
        print("[ml] prediction_log.json が見つかりません")
        return []
    try:
        data = json.loads(LOG_FILE.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"[ml] ログ読み込みエラー: {e}")
        return []

    # 結果が記録されているエントリのみ
    return [e for e in data if e.get("actual") is not None and e["actual"].get("三連単")]


def evaluate_weights(weight_vector: np.ndarray, results: list[dict]) -> float:
    """
    prediction_log のエントリを使って重みの品質を評価する。
    注意: この方式は保存時の推奨買い目との照合のみで、再スコアリングは行わない。
    バックテストデータが使える場合は backtester.py の方が正確。
    """
    keys = [
        "win_rate", "local_win_rate", "nat2_rate", "loc2_rate",
        "exhibit_time", "exhibit_top_bonus", "night_boost", "calm_in_boost",
        "kado_boost", "makuri_sashi", "st_weight", "motor2_rate",
        "boat2_rate", "taka_boost",
        "fl_f_penalty", "weight_calm", "course_win_rate",
        "momentum", "lap_time",
        "mawari_time", "chokusen_time", "turn_master_bonus",
    ]

    w_clamped = np.clip(weight_vector, 0.0, 50.0)

    from config import SCORE_WEIGHTS
    old_weights = {k: SCORE_WEIGHTS.get(k) for k in keys}
    for i, k in enumerate(keys):
        if i < len(w_clamped):
            SCORE_WEIGHTS[k] = float(w_clamped[i])

    hits = 0
    total = 0
    top9_hits = 0

    for entry in results:
        actual_combo = entry["actual"]["三連単"]
        recs = entry.get("recommendations", [])
        if not recs:
            continue

        rec_combos = {r["買い目"] for r in recs}
        total += 1
        if actual_combo in rec_combos:
            hits += 1
            for r in recs:
                if r["買い目"] == actual_combo:
                    grp = r.get("グループ", "")
                    if grp == "本命":
                        top9_hits += 3
                    elif grp == "対抗":
                        top9_hits += 2
                    elif grp == "穴":
                        top9_hits += 1

    for k, v in old_weights.items():
        if v is not None:
            SCORE_WEIGHTS[k] = v

    if total == 0:
        return 0.0

    hit_rate = hits / total
    bonus = top9_hits / (total * 3)
    return hit_rate + bonus * 0.3


def optimize(max_iter: int = 200) -> dict:
    """
    SCORE_WEIGHTS を最適化する。
    バックテストデータがあればそちらを優先使用。
    """
    # バックテストデータを優先
    try:
        from backtester import load_backtest_data, optimize_from_backtest
        bt_data = load_backtest_data()
        if len(bt_data) >= 10:
            print(f"[ml] バックテストデータ ({len(bt_data)} レース) を使用して最適化します")
            return optimize_from_backtest(max_iter=max_iter)
    except ImportError:
        pass

    # フォールバック: prediction_log.json
    results = load_results()
    if len(results) < 10:
        print(f"[ml] 結果データが {len(results)} 件しかありません（最低10件必要）")
        print("[ml] ヒント: python backtester.py collect --days 30 で過去データを収集できます")
        return {}

    print(f"[ml] {len(results)} 件の結果データで最適化を開始...")

    from config import SCORE_WEIGHTS as W

    keys = [
        "win_rate", "local_win_rate", "nat2_rate", "loc2_rate",
        "exhibit_time", "exhibit_top_bonus", "night_boost", "calm_in_boost",
        "kado_boost", "makuri_sashi", "st_weight", "motor2_rate",
        "boat2_rate", "taka_boost",
        "fl_f_penalty", "weight_calm", "course_win_rate",
        "momentum", "lap_time",
        "mawari_time", "chokusen_time", "turn_master_bonus",
    ]
    x0 = np.array([W.get(k, 1.0) for k in keys])

    baseline_score = evaluate_weights(x0, results)
    print(f"[ml] 現在の重みでのスコア: {baseline_score:.4f}")

    try:
        from scipy.optimize import minimize

        def objective(x):
            return -evaluate_weights(x, results)

        res = minimize(
            objective,
            x0,
            method="Nelder-Mead",
            options={
                "maxiter": max_iter,
                "xatol": 0.01,
                "fatol": 0.001,
                "adaptive": True,
            },
        )
        best_x = np.clip(res.x, 0.0, 50.0)
        best_score = -res.fun
        print(f"[ml] 最適化完了: スコア {best_score:.4f} (改善 {best_score - baseline_score:+.4f})")

    except ImportError:
        print("[ml] scipy が未インストールのため、ランダム探索にフォールバックします")
        best_x = x0.copy()
        best_score = baseline_score

        rng = np.random.default_rng(42)
        for iteration in range(max_iter):
            noise = rng.uniform(-0.2, 0.2, size=len(x0))
            candidate = np.clip(x0 * (1.0 + noise), 0.0, 50.0)
            score = evaluate_weights(candidate, results)
            if score > best_score:
                best_score = score
                best_x = candidate.copy()
                print(f"  [iter {iteration}] 改善: {score:.4f}")

        print(f"[ml] 探索完了: スコア {best_score:.4f} (改善 {best_score - baseline_score:+.4f})")

    optimized = {k: round(float(best_x[i]), 3) for i, k in enumerate(keys)}
    improvement = best_score - baseline_score

    return {
        "optimized_weights": optimized,
        "hit_rate":          round(best_score, 4),
        "baseline":          round(baseline_score, 4),
        "improvement":       round(improvement, 4),
    }


def apply_to_config(optimized_weights: dict) -> None:
    """最適化された重みを config.py に書き込む。"""
    import re

    config_path = PROJECT_DIR / "config.py"
    content = config_path.read_text(encoding="utf-8")

    for key, value in optimized_weights.items():
        pattern = f'"{key}":\\s*[\\d.]+,'
        match = re.search(pattern, content)
        if match:
            line_start = content.rfind("\n", 0, match.start()) + 1
            line_end = content.find("\n", match.end())
            full_line = content[line_start:line_end]
            comment_idx = full_line.find("#")
            comment = full_line[comment_idx:] if comment_idx >= 0 else ""

            indent = "    "
            new_full = f'{indent}"{key}":{" " * max(1, 17 - len(key))}{value},'
            if comment:
                new_full += f"    {comment}"

            content = content[:line_start] + new_full + content[line_end:]

    config_path.write_text(content, encoding="utf-8")
    print(f"[ml] config.py を更新しました")


# ────────────────────────────────────────────────────────────────
# CLI エントリポイント
# ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("蒲郡競艇AI - SCORE_WEIGHTS 自動最適化")
    print("=" * 60)

    result = optimize()
    if not result:
        print("\n最適化を実行できませんでした。")
        print("python backtester.py collect --days 30 で過去データを収集してください。")
        sys.exit(1)

    print("\n" + "=" * 60)
    print("最適化結果")
    print("=" * 60)
    print(f"ベースラインスコア: {result['baseline']}")
    print(f"最適化後スコア:     {result['hit_rate']}")
    print(f"改善幅:             {result['improvement']:+.4f}")
    print()
    print("最適化された重み:")
    for k, v in result["optimized_weights"].items():
        print(f"  {k:.<25s} {v:.3f}")

    print()
    answer = input("config.py に反映しますか？ (y/n): ").strip().lower()
    if answer == "y":
        apply_to_config(result["optimized_weights"])
        print("完了！次回の予想実行から新しい重みが適用されます。")
    else:
        print("反映をスキップしました。上記の値を手動で config.py に設定できます。")
