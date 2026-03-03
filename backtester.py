"""
過去レース自動バックテスト＆ML最適化モジュール

【使い方】
1. データ収集:  python backtester.py collect --days 30
2. 最適化:      python backtester.py optimize
3. 両方一括:    python backtester.py all --days 30

boatrace.jp から過去の蒲郡レースデータを自動収集し、
各重みパラメータで再スコアリング → 的中率を評価 → 最適な重みを探索する。
"""
import json
import sys
import time
import argparse
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_DIR = Path(__file__).parent
sys.path.insert(0, str(PROJECT_DIR))

BACKTEST_FILE = PROJECT_DIR / "backtest_data.json"


# ────────────────────────────────────────────────────────────────
# データ永続化
# ────────────────────────────────────────────────────────────────

def load_backtest_data() -> list[dict]:
    """バックテストデータを読み込む。"""
    if BACKTEST_FILE.exists():
        try:
            return json.loads(BACKTEST_FILE.read_text(encoding="utf-8"))
        except Exception:
            return []
    return []


def _save_backtest_data(data: list[dict]) -> None:
    data.sort(key=lambda x: (x["date"], x["race_no"]))
    BACKTEST_FILE.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


# ────────────────────────────────────────────────────────────────
# 1. 過去データ収集
# ────────────────────────────────────────────────────────────────

def collect_historical_data(
    days: int = 30,
    delay: float = 1.0,
    progress_callback=None,
) -> list[dict]:
    """
    過去 days 日分の蒲郡レースデータを収集する。

    Parameters
    ----------
    days     : 遡る日数
    delay    : リクエスト間隔（秒）。サーバー負荷軽減のため最低0.5秒推奨。
    progress_callback : (current, total, message) を受け取るコールバック

    Returns
    -------
    全バックテストデータのリスト
    """
    from race_scraper import fetch_race_card, fetch_before_info, fetch_race_result

    existing = load_backtest_data()
    existing_keys = {(e["date"], e["race_no"]) for e in existing}

    today = datetime.now()
    new_entries = []
    total_steps = days * 12
    current_step = 0

    for d in range(1, days + 1):
        target_date = today - timedelta(days=d)
        date_str = target_date.strftime("%Y%m%d")
        date_display = target_date.strftime("%Y/%m/%d")

        day_count = 0
        for race_no in range(1, 13):
            current_step += 1

            if (date_str, race_no) in existing_keys:
                if progress_callback:
                    progress_callback(
                        current_step, total_steps,
                        f"{date_display} R{race_no}: スキップ（収集済み）",
                    )
                continue

            msg = f"{date_display} R{race_no}: 取得中..."
            if progress_callback:
                progress_callback(current_step, total_steps, msg)
            else:
                print(f"  {msg}", end="", flush=True)

            # ── 出走表取得 ──
            time.sleep(delay)
            card_df = fetch_race_card(race_no, date_str)
            if card_df.empty:
                if not progress_callback:
                    print(" → データなし")
                continue

            # ── 直前情報取得 ──
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

            # ── レース結果取得 ──
            time.sleep(delay)
            result = fetch_race_result(race_no, date_str)
            if result is None:
                if not progress_callback:
                    print(" → 結果なし")
                continue

            # ── 保存用データ構築 ──
            # DataFrameをJSON化可能にする（NaN → None）
            records = merged.where(merged.notna(), None).to_dict("records")
            entry = {
                "date": date_str,
                "race_no": race_no,
                "race_data": records,
                "weather": weather,
                "actual": result,
            }
            new_entries.append(entry)
            day_count += 1

            if not progress_callback:
                print(f" → OK ({result['三連単']})")

        # 日ごとに途中保存（中断耐性）
        if new_entries:
            all_data = existing + new_entries
            _save_backtest_data(all_data)

        if not progress_callback:
            print(f"[backtest] {date_display}: {day_count} レース取得")

    all_data = existing + new_entries
    _save_backtest_data(all_data)

    total = len(all_data)
    new = len(new_entries)
    if progress_callback:
        progress_callback(total_steps, total_steps, f"完了: 合計 {total} レース (新規 {new} 件)")
    else:
        print(f"\n[backtest] 合計 {total} レース (新規 {new} 件)")

    return all_data


# ────────────────────────────────────────────────────────────────
# 2. 重みの評価（バックテスト）
# ────────────────────────────────────────────────────────────────

def evaluate_weights_backtest(
    weight_vector: np.ndarray,
    races: list[dict],
    keys: list[str],
) -> float:
    """
    バックテストデータに対して重みベクトルを評価する。

    各レースで:
      1. 重みを適用して再スコアリング
      2. 推奨買い目を生成
      3. 実際の結果と照合
    → 的中率 + 上位グループ的中ボーナス の複合スコアを返す
    """
    from config import SCORE_WEIGHTS
    from scorer import calculate_scores, generate_recommendations

    # 重みを一時的に変更
    w_clamped = np.clip(weight_vector, 0.0, 50.0)
    old_weights = {k: SCORE_WEIGHTS.get(k) for k in keys}
    for i, k in enumerate(keys):
        if i < len(w_clamped):
            SCORE_WEIGHTS[k] = float(w_clamped[i])

    hits = 0
    total = 0
    group_bonus = 0

    for race in races:
        try:
            df = pd.DataFrame(race["race_data"])
            weather = race["weather"]
            race_no = race["race_no"]
            actual = race["actual"]

            if not actual or "三連単" not in actual:
                continue

            # スコアリング（taka_dataなし、odds_dictなし）
            scored = calculate_scores(df, weather, race_no)
            recs = generate_recommendations(scored)

            actual_combo = actual["三連単"]
            rec_combos = {r["買い目"] for r in recs}

            total += 1
            if actual_combo in rec_combos:
                hits += 1
                for r in recs:
                    if r["買い目"] == actual_combo:
                        grp = r.get("グループ", "")
                        if grp == "本命":
                            group_bonus += 3
                        elif grp == "対抗":
                            group_bonus += 2
                        elif grp == "穴":
                            group_bonus += 1
        except Exception:
            continue

    # 重みを元に戻す
    for k, v in old_weights.items():
        if v is not None:
            SCORE_WEIGHTS[k] = v

    if total == 0:
        return 0.0

    hit_rate = hits / total
    bonus = group_bonus / (total * 3)
    return hit_rate + bonus * 0.3


# ────────────────────────────────────────────────────────────────
# 3. 最適化
# ────────────────────────────────────────────────────────────────

WEIGHT_KEYS = [
    "win_rate", "local_win_rate", "nat2_rate", "loc2_rate",
    "exhibit_time", "exhibit_top_bonus", "night_boost", "calm_in_boost",
    "kado_boost", "makuri_sashi", "st_weight", "motor2_rate",
    "boat2_rate", "taka_boost",
    "fl_f_penalty", "weight_calm", "course_win_rate",
    "momentum", "lap_time",
    "mawari_time", "chokusen_time", "turn_master_bonus",
]


def optimize_from_backtest(
    max_iter: int = 300,
    progress_callback=None,
) -> dict:
    """
    バックテストデータを使って SCORE_WEIGHTS を最適化する。

    Returns
    -------
    {"optimized_weights": dict, "hit_rate": float, "baseline": float,
     "improvement": float, "n_races": int, "details": dict}
    """
    races = load_backtest_data()
    if len(races) < 10:
        msg = f"バックテストデータが {len(races)} 件しかありません（最低10件必要）"
        if progress_callback:
            progress_callback(0, 1, msg)
        else:
            print(f"[ml] {msg}")
        return {}

    if progress_callback:
        progress_callback(0, max_iter, f"{len(races)} レースで最適化開始...")
    else:
        print(f"[ml] {len(races)} レースのバックテストデータで最適化を開始...")

    from config import SCORE_WEIGHTS as W

    x0 = np.array([W.get(k, 1.0) for k in WEIGHT_KEYS])

    # ── 現在の重みでのベースラインスコア ──
    baseline_score = evaluate_weights_backtest(x0, races, WEIGHT_KEYS)
    if progress_callback:
        progress_callback(0, max_iter, f"ベースライン: {baseline_score:.4f}")
    else:
        print(f"[ml] 現在の重みでのスコア: {baseline_score:.4f}")

    # ── ベースライン詳細（的中レース一覧） ──
    baseline_details = _evaluate_details(x0, races)

    # ── 最適化実行 ──
    try:
        from scipy.optimize import minimize

        iteration_count = [0]
        best_so_far = [baseline_score]

        def objective(x):
            score = -evaluate_weights_backtest(x, races, WEIGHT_KEYS)
            iteration_count[0] += 1
            if -score > best_so_far[0]:
                best_so_far[0] = -score
                if progress_callback:
                    progress_callback(
                        iteration_count[0], max_iter,
                        f"改善: {-score:.4f} (iter {iteration_count[0]})",
                    )
                else:
                    print(f"  [iter {iteration_count[0]}] 改善: {-score:.4f}")
            elif iteration_count[0] % 50 == 0:
                if progress_callback:
                    progress_callback(
                        iteration_count[0], max_iter,
                        f"探索中 (iter {iteration_count[0]}, best={best_so_far[0]:.4f})",
                    )
                else:
                    print(f"  [iter {iteration_count[0]}] best={best_so_far[0]:.4f}")
            return score

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

    except ImportError:
        if progress_callback:
            progress_callback(0, max_iter, "scipy未インストール → ランダム探索")
        else:
            print("[ml] scipy未インストール → ランダム探索にフォールバック")

        best_x = x0.copy()
        best_score = baseline_score
        rng = np.random.default_rng(42)

        for iteration in range(max_iter):
            # 各重みを±30%の範囲でランダムに摂動
            noise = rng.uniform(-0.3, 0.3, size=len(x0))
            candidate = np.clip(x0 * (1.0 + noise), 0.0, 50.0)
            score = evaluate_weights_backtest(candidate, races, WEIGHT_KEYS)
            if score > best_score:
                best_score = score
                best_x = candidate.copy()
                if progress_callback:
                    progress_callback(iteration, max_iter, f"改善: {score:.4f}")
                else:
                    print(f"  [iter {iteration}] 改善: {score:.4f}")
            elif iteration % 50 == 0:
                if progress_callback:
                    progress_callback(iteration, max_iter, f"探索中 (best={best_score:.4f})")

    improvement = best_score - baseline_score
    msg = f"最適化完了: {best_score:.4f} (改善 {improvement:+.4f})"
    if progress_callback:
        progress_callback(max_iter, max_iter, msg)
    else:
        print(f"\n[ml] {msg}")

    optimized = {k: round(float(best_x[i]), 3) for i, k in enumerate(WEIGHT_KEYS)}

    # ── 最適化後の詳細 ──
    optimized_details = _evaluate_details(best_x, races)

    return {
        "optimized_weights": optimized,
        "original_weights":  {k: round(W.get(k, 1.0), 3) for k in WEIGHT_KEYS},
        "hit_rate":          round(best_score, 4),
        "baseline":          round(baseline_score, 4),
        "improvement":       round(improvement, 4),
        "n_races":           len(races),
        "baseline_details":  baseline_details,
        "optimized_details": optimized_details,
    }


def _evaluate_details(weight_vector: np.ndarray, races: list[dict]) -> dict:
    """
    重みベクトルでの詳細な評価結果を返す。
    """
    from config import SCORE_WEIGHTS
    from scorer import calculate_scores, generate_recommendations

    w_clamped = np.clip(weight_vector, 0.0, 50.0)
    old_weights = {k: SCORE_WEIGHTS.get(k) for k in WEIGHT_KEYS}
    for i, k in enumerate(WEIGHT_KEYS):
        if i < len(w_clamped):
            SCORE_WEIGHTS[k] = float(w_clamped[i])

    total = 0
    hits = 0
    honmei_hits = 0
    taiko_hits = 0
    ana_hits = 0
    top1_in_recs = 0  # 1着艇が推奨に含まれていた回数

    for race in races:
        try:
            df = pd.DataFrame(race["race_data"])
            weather = race["weather"]
            race_no = race["race_no"]
            actual = race["actual"]
            if not actual or "三連単" not in actual:
                continue

            scored = calculate_scores(df, weather, race_no)
            recs = generate_recommendations(scored)
            actual_combo = actual["三連単"]
            actual_1st = str(actual.get("1着", ""))

            total += 1

            # 1着艇が推奨買い目の1着に含まれているか
            rec_1st_boats = {r["1着艇"] for r in recs}
            if actual_1st in rec_1st_boats:
                top1_in_recs += 1

            # 3連単的中チェック
            for r in recs:
                if r["買い目"] == actual_combo:
                    hits += 1
                    grp = r.get("グループ", "")
                    if grp == "本命":
                        honmei_hits += 1
                    elif grp == "対抗":
                        taiko_hits += 1
                    elif grp == "穴":
                        ana_hits += 1
                    break
        except Exception:
            continue

    # 重みを元に戻す
    for k, v in old_weights.items():
        if v is not None:
            SCORE_WEIGHTS[k] = v

    return {
        "total":        total,
        "hits":         hits,
        "hit_rate":     round(hits / total * 100, 2) if total > 0 else 0.0,
        "honmei_hits":  honmei_hits,
        "taiko_hits":   taiko_hits,
        "ana_hits":     ana_hits,
        "top1_rate":    round(top1_in_recs / total * 100, 2) if total > 0 else 0.0,
    }


# ────────────────────────────────────────────────────────────────
# 4. config.py への書き込み
# ────────────────────────────────────────────────────────────────

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
    parser = argparse.ArgumentParser(description="蒲郡競艇AI バックテスト＆最適化")
    parser.add_argument(
        "command",
        choices=["collect", "optimize", "all", "status"],
        help="collect: データ収集, optimize: 最適化, all: 両方実行, status: データ状況確認",
    )
    parser.add_argument("--days", type=int, default=30, help="収集日数（デフォルト30日）")
    parser.add_argument("--delay", type=float, default=1.0, help="リクエスト間隔（秒）")
    parser.add_argument("--iter", type=int, default=300, help="最適化イテレーション数")
    args = parser.parse_args()

    print("=" * 60)
    print("蒲郡競艇AI - バックテスト＆ML最適化")
    print("=" * 60)

    if args.command == "status":
        data = load_backtest_data()
        if not data:
            print("バックテストデータなし。collect コマンドでデータを収集してください。")
        else:
            dates = sorted(set(e["date"] for e in data))
            print(f"レース数:   {len(data)}")
            print(f"日数:       {len(dates)}")
            print(f"期間:       {dates[0]} 〜 {dates[-1]}")
            # 現在の重みでの的中率
            from config import SCORE_WEIGHTS as W
            x0 = np.array([W.get(k, 1.0) for k in WEIGHT_KEYS])
            details = _evaluate_details(x0, data)
            print(f"現在の的中率: {details['hit_rate']:.2f}% ({details['hits']}/{details['total']})")
            print(f"  本命: {details['honmei_hits']}件, 対抗: {details['taiko_hits']}件, 穴: {details['ana_hits']}件")
            print(f"1着艇的中率: {details['top1_rate']:.1f}%")
        sys.exit(0)

    if args.command in ("collect", "all"):
        print(f"\n--- データ収集 ({args.days}日分) ---")
        collect_historical_data(days=args.days, delay=args.delay)

    if args.command in ("optimize", "all"):
        print(f"\n--- 最適化 (max {args.iter} iterations) ---")
        result = optimize_from_backtest(max_iter=args.iter)
        if not result:
            print("\n最適化を実行できませんでした。")
            print("先に collect コマンドでデータを収集してください。")
            sys.exit(1)

        print("\n" + "=" * 60)
        print("最適化結果")
        print("=" * 60)
        print(f"レース数:       {result['n_races']}")
        print(f"ベースライン:   {result['baseline']}")
        print(f"最適化後:       {result['hit_rate']}")
        print(f"改善幅:         {result['improvement']:+.4f}")

        bd = result["baseline_details"]
        od = result["optimized_details"]
        print(f"\n  的中率:  {bd['hit_rate']:.2f}% → {od['hit_rate']:.2f}%")
        print(f"  本命:    {bd['honmei_hits']} → {od['honmei_hits']}")
        print(f"  対抗:    {bd['taiko_hits']} → {od['taiko_hits']}")
        print(f"  穴:      {bd['ana_hits']} → {od['ana_hits']}")
        print(f"  1着率:   {bd['top1_rate']:.1f}% → {od['top1_rate']:.1f}%")

        print("\n重みの変更:")
        for k in WEIGHT_KEYS:
            old = result["original_weights"][k]
            new = result["optimized_weights"][k]
            diff = new - old
            arrow = "↑" if diff > 0.01 else ("↓" if diff < -0.01 else "→")
            print(f"  {k:.<25s} {old:>7.3f} {arrow} {new:>7.3f} ({diff:+.3f})")

        print()
        answer = input("config.py に反映しますか？ (y/n): ").strip().lower()
        if answer == "y":
            apply_to_config(result["optimized_weights"])
            print("完了！次回の予想実行から新しい重みが適用されます。")
        else:
            print("反映をスキップしました。上記の値を手動で config.py に設定できます。")
