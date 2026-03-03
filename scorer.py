"""
蒲郡競艇場専用スコアリングエンジン【強化版 v3】

【v3 改善点】
1. F/L回数ペナルティ（スタート慎重化リスク反映）
2. 体重×気象条件の相互作用スコア
3. 選手コース別1着率によるスコアリング
4. 直近成績モメンタム（好調/不調の検出）
5. 蒲郡独自展示タイム（一周・まわり足・直線によるターン巧者判定）
6. レースグレード補正（SG/G1は均等化、優勝戦はイン強化）
7. Heneryモデル（修正Harville）で2・3着確率を改善
8. 期待値ベースの穴買い目選定
9. 2連単・2連複推奨買い目の生成
"""
import re
from itertools import permutations

import numpy as np
import pandas as pd

from config import GAMAGORI_SETTINGS as G, SCORE_WEIGHTS as W


# ────────────────────────────────────────────────────────────────
# ユーティリティ
# ────────────────────────────────────────────────────────────────

def _to_float(val, default=0.0) -> float:
    try:
        if isinstance(val, str):
            val = re.sub(r"[^\d.\-]", "", val)
        return float(val)
    except (ValueError, TypeError):
        return default


def _parse_wind_speed(wind_str) -> float:
    if isinstance(wind_str, (int, float)):
        return float(wind_str)
    m = re.search(r"[\d.]+", str(wind_str))
    return float(m.group()) if m else 0.0


def _parse_wave_height(wave_str) -> float:
    if isinstance(wave_str, (int, float)):
        return float(wave_str)
    m = re.search(r"[\d.]+", str(wave_str))
    return float(m.group()) if m else 0.0


def _parse_temp(temp_str) -> float:
    if isinstance(temp_str, (int, float)):
        return float(temp_str)
    m = re.search(r"[\d.]+", str(temp_str))
    return float(m.group()) if m else 15.0


def _safe_col(df, col):
    """DataFrameから列を安全に取得。なければNone配列を返す。"""
    if col in df.columns:
        return np.array([_to_float(v) for v in df[col].values])
    return None


# ────────────────────────────────────────────────────────────────
# 蒲郡コース特性定数
# ────────────────────────────────────────────────────────────────

GAMAGORI_COURSE_WIN_RATE = [0.555, 0.095, 0.115, 0.095, 0.085, 0.055]

WIND_COURSE_EFFECT = {
    "北":     {0: +2.5, 1: +1.0, 2: -0.5, 3: -1.0, 4: -1.5, 5: -2.0},
    "北北東":  {0: +2.0, 1: +0.8, 2: -0.3, 3: -0.8, 4: -1.2, 5: -1.8},
    "北東":   {0: +1.5, 1: +0.5, 2:  0.0, 3: -0.5, 4: -1.0, 5: -1.5},
    "東北東":  {0: +1.0, 1: +0.3, 2: +0.5, 3:  0.0, 4: -0.5, 5: -1.0},
    "東":     {0:  0.0, 1: -0.5, 2: +1.0, 3: +1.5, 4: +0.5, 5: -0.5},
    "東南東":  {0: -0.5, 1: -0.5, 2: +0.8, 3: +1.5, 4: +0.8, 5:  0.0},
    "南東":   {0: -1.0, 1: -0.5, 2: +0.5, 3: +1.5, 4: +1.0, 5: +0.5},
    "南南東":  {0: -1.5, 1: -0.5, 2: +0.5, 3: +1.5, 4: +1.2, 5: +0.8},
    "南":     {0: -2.0, 1: -0.5, 2: +0.5, 3: +1.0, 4: +1.5, 5: +1.0},
    "南南西":  {0: -1.5, 1: -0.3, 2: +0.3, 3: +0.8, 4: +1.2, 5: +1.0},
    "南西":   {0: -1.0, 1:  0.0, 2: +0.3, 3: +0.5, 4: +0.8, 5: +0.8},
    "西南西":  {0: -0.5, 1: +0.5, 2: +0.5, 3: +0.3, 4: +0.5, 5: +0.5},
    "西":     {0:  0.0, 1: +1.0, 2: +0.8, 3:  0.0, 4:  0.0, 5:  0.0},
    "西北西":  {0: +0.5, 1: +1.0, 2: +0.5, 3: -0.3, 4: -0.3, 5: -0.5},
    "北西":   {0: +1.0, 1: +1.0, 2:  0.0, 3: -0.5, 4: -0.8, 5: -1.0},
    "北北西":  {0: +2.0, 1: +0.8, 2: -0.3, 3: -0.8, 4: -1.2, 5: -1.5},
    "-":      {0:  0.0, 1:  0.0, 2:  0.0, 3:  0.0, 4:  0.0, 5:  0.0},
}


def _tilt_score(tilt: float, course_idx: int) -> float:
    if course_idx == 0:
        return tilt * 1.5
    elif course_idx == 1:
        return tilt * 0.5
    elif course_idx == 2:
        return -tilt * 1.2
    elif course_idx == 3:
        return -tilt * 1.5
    elif course_idx == 4:
        return -tilt * 0.8
    else:
        return -tilt * 0.3


# ────────────────────────────────────────────────────────────────
# コアスコアリング関数
# ────────────────────────────────────────────────────────────────

def calculate_scores(
    df: pd.DataFrame,
    weather: dict,
    race_no: int,
    taka_data: dict | None = None,
) -> pd.DataFrame:
    df = df.copy()
    n = len(df)
    if n == 0:
        return df

    # ── Step 1: コースベース（進入コースがあれば優先して使用） ──────
    if "進入コース" in df.columns:
        course_positions = []
        for v in df["進入コース"].values:
            try:
                pos = int(v) - 1
                course_positions.append(max(0, min(pos, 5)))
            except (TypeError, ValueError):
                course_positions.append(len(course_positions))
        base = [GAMAGORI_COURSE_WIN_RATE[pos] * 100 for pos in course_positions]
    else:
        base = [r * 100 for r in GAMAGORI_COURSE_WIN_RATE[:n]]
        course_positions = list(range(n))
    scores = np.array(base, dtype=float)

    # ── Step 2: 勝率・2連率複合補正 ──────────────────────────────
    win_rates   = df["全国勝率"].apply(_to_float).values
    local_rates = df["蒲郡勝率"].apply(_to_float).values

    if "全国2連率" in df.columns:
        nat2_rates = df["全国2連率"].apply(_to_float).values
    else:
        nat2_rates = win_rates * 2.5

    if "蒲郡2連率" in df.columns:
        loc2_rates = df["蒲郡2連率"].apply(_to_float).values
    else:
        loc2_rates = local_rates * 2.5

    mean_wr  = win_rates.mean() + 1e-9
    mean_lr  = local_rates.mean() + 1e-9
    mean_n2  = nat2_rates.mean() + 1e-9
    mean_l2  = loc2_rates.mean() + 1e-9

    score_wr  = (win_rates   / mean_wr  - 1.0) * W["win_rate"]
    score_lr  = (local_rates / mean_lr  - 1.0) * W["local_win_rate"]
    score_n2  = (nat2_rates  / mean_n2  - 1.0) * W.get("nat2_rate", 1.5)
    score_l2  = (loc2_rates  / mean_l2  - 1.0) * W.get("loc2_rate", 2.5)
    scores   += score_wr + score_lr + score_n2 + score_l2

    # ── Step 3: 展示タイム（絶対差 + 相対偏差の複合） ────────────
    exhibit_times = df["展示タイム"].apply(_to_float).values
    tilts         = df["チルト"].apply(_to_float).values
    valid_mask    = exhibit_times > 0

    if valid_mask.sum() >= 2:
        et_valid = exhibit_times[valid_mask]
        mean_et  = et_valid.mean()
        std_et   = et_valid.std() + 1e-9
        best_et  = et_valid.min()

        for i in range(n):
            if not valid_mask[i]:
                continue
            et_z = -(exhibit_times[i] - mean_et) / std_et
            scores[i] += et_z * W["exhibit_time"]

            gap_from_best = exhibit_times[i] - best_et
            if gap_from_best == 0.0:
                scores[i] += W.get("exhibit_top_bonus", 2.0)
            elif gap_from_best <= 0.05:
                scores[i] += W.get("exhibit_top_bonus", 2.0) * 0.5

            scores[i] += _tilt_score(tilts[i], course_positions[i])

        for i in range(n):
            if exhibit_times[i] == best_et and tilts[i] < -0.5:
                if course_positions[i] == 2:
                    scores[i] += W.get("makuri_sashi", 2.0)
                if course_positions[i] == 3:
                    scores[i] += W.get("kado_boost", 2.0)

    # ── Step 3b: スタートタイミング（ST）補正 ────────────────────
    st_raw = df["スタートタイミング"].values if "スタートタイミング" in df.columns else [None] * n
    st_numeric = np.full(n, np.nan)
    for i, v in enumerate(st_raw):
        if v is None:
            continue
        try:
            st_numeric[i] = float(v)
        except (TypeError, ValueError):
            pass

    valid_sts = st_numeric[~np.isnan(st_numeric) & (st_numeric >= 0)]
    if len(valid_sts) >= 2:
        mean_st = valid_sts.mean()
        for i in range(n):
            if np.isnan(st_numeric[i]):
                continue
            if st_numeric[i] < 0:
                scores[i] -= W.get("st_fly_penalty", 5.0)
            else:
                st_diff = mean_st - st_numeric[i]
                scores[i] += st_diff * W.get("st_weight", 25.0)

    # ── Step 3c: モーター2連率・ボート2連率補正 ──────────────────
    if "モーター2連率" in df.columns:
        motor2 = np.array([_to_float(v) for v in df["モーター2連率"].values])
        valid_m2 = motor2[motor2 > 0]
        if len(valid_m2) >= 2:
            mean_m2 = valid_m2.mean() + 1e-9
            for i in range(n):
                if motor2[i] > 0:
                    scores[i] += (motor2[i] / mean_m2 - 1.0) * W.get("motor2_rate", 3.0)

    if "ボート2連率" in df.columns:
        boat2 = np.array([_to_float(v) for v in df["ボート2連率"].values])
        valid_b2 = boat2[boat2 > 0]
        if len(valid_b2) >= 2:
            mean_b2 = valid_b2.mean() + 1e-9
            for i in range(n):
                if boat2[i] > 0:
                    scores[i] += (boat2[i] / mean_b2 - 1.0) * W.get("boat2_rate", 1.5)

    # ── Step 3d: F/L回数ペナルティ 【v3新規】 ────────────────────
    f_counts = _safe_col(df, "F回数")
    l_counts = _safe_col(df, "L回数")
    if f_counts is not None:
        for i in range(n):
            if f_counts[i] >= 1:
                # F持ち: スタート慎重化 → 本来のST能力を発揮できない
                scores[i] -= W.get("fl_f_penalty", 5.0) * f_counts[i]
    if l_counts is not None:
        for i in range(n):
            if l_counts[i] >= 1:
                scores[i] -= W.get("fl_l_penalty", 3.0) * l_counts[i]

    # ── Step 3e: 体重×気象相互作用 【v3新規】 ─────────────────────
    weights = _safe_col(df, "体重")
    wind_speed = _parse_wind_speed(weather.get("風速", 0))
    wave_height = _parse_wave_height(weather.get("波高", "0cm"))
    if weights is not None:
        valid_w = weights[weights > 0]
        if len(valid_w) >= 2:
            mean_w = valid_w.mean()
            is_rough = wave_height >= 5 or wind_speed >= 4
            is_calm_water = wave_height <= 2 and wind_speed <= 2
            for i in range(n):
                if weights[i] <= 0:
                    continue
                w_diff = weights[i] - mean_w
                if is_calm_water and w_diff < -1.0:
                    # 静水面で軽量 → 有利
                    scores[i] += abs(w_diff) * 0.3 * W.get("weight_calm", 1.5)
                elif is_rough and w_diff > 1.0:
                    # 荒天で重い → 安定
                    scores[i] += abs(w_diff) * 0.2 * W.get("weight_rough", 1.5)
                elif is_rough and w_diff < -1.5:
                    # 荒天で軽い → 不利
                    scores[i] -= abs(w_diff) * 0.15

    # ── Step 3f: 蒲郡独自展示タイム（一周・まわり足・直線） ────────
    # まわり足タイム: ターン力の指標（最重要）
    mawari_times = _safe_col(df, "まわり足タイム")
    if mawari_times is not None:
        valid_mw = mawari_times[mawari_times > 0]
        if len(valid_mw) >= 2:
            mean_mw = valid_mw.mean()
            std_mw  = valid_mw.std() + 1e-9
            best_mw = valid_mw.min()
            for i in range(n):
                if mawari_times[i] <= 0:
                    continue
                mw_z = -(mawari_times[i] - mean_mw) / std_mw
                scores[i] += mw_z * W.get("mawari_time", 2.0)
                if mawari_times[i] == best_mw:
                    scores[i] += 1.0

    # 直線タイム: 伸び足の指標
    chokusen_times = _safe_col(df, "直線タイム")
    if chokusen_times is not None:
        valid_ch = chokusen_times[chokusen_times > 0]
        if len(valid_ch) >= 2:
            mean_ch = valid_ch.mean()
            std_ch  = valid_ch.std() + 1e-9
            best_ch = valid_ch.min()
            for i in range(n):
                if chokusen_times[i] <= 0:
                    continue
                ch_z = -(chokusen_times[i] - mean_ch) / std_ch
                scores[i] += ch_z * W.get("chokusen_time", 1.5)
                if chokusen_times[i] == best_ch:
                    scores[i] += 0.5

    # 一周タイム: 総合力の指標（データがある場合のみ）
    lap_times = _safe_col(df, "一周タイム")
    if lap_times is not None:
        valid_lt = lap_times[lap_times > 0]
        if len(valid_lt) >= 2:
            mean_lt = valid_lt.mean()
            std_lt  = valid_lt.std() + 1e-9
            best_lt = valid_lt.min()
            for i in range(n):
                if lap_times[i] <= 0:
                    continue
                lt_z = -(lap_times[i] - mean_lt) / std_lt
                scores[i] += lt_z * W.get("lap_time", 1.5)
                if lap_times[i] == best_lt:
                    scores[i] += 1.0

    # ターン巧者判定: 直線は遅いがまわり足が速い → 差し/まくり差し向き
    if (mawari_times is not None and chokusen_times is not None
            and sum(mawari_times > 0) >= 2 and sum(chokusen_times > 0) >= 2):
        for i in range(n):
            if mawari_times[i] <= 0 or chokusen_times[i] <= 0:
                continue
            mw_rank = sum(1 for j in range(n) if mawari_times[j] > 0 and mawari_times[j] < mawari_times[i])
            ch_rank = sum(1 for j in range(n) if chokusen_times[j] > 0 and chokusen_times[j] < chokusen_times[i])
            if mw_rank < ch_rank - 1 and course_positions[i] >= 1:
                # ターン巧者: まわり足順位 >> 直線順位 → 差し/まくり差し有利
                scores[i] += W.get("turn_master_bonus", 1.5)

    # ── Step 4: 風向×コース特性の複合補正 ────────────────────────
    wind_dir = weather.get("風向", "-")
    is_calm  = wind_speed <= G["calm_wind_threshold"]

    if not is_calm and wind_dir in WIND_COURSE_EFFECT:
        effect = WIND_COURSE_EFFECT[wind_dir]
        wind_multiplier = min(wind_speed / 3.0, 3.0)
        for i in range(n):
            scores[i] += effect.get(course_positions[i], 0.0) * wind_multiplier

    if is_calm:
        if local_rates[0] >= local_rates.mean():
            scores[0] += W["calm_in_boost"]
        scores[0] += 1.5

    # ── Step 5: 波高補正 ─────────────────────────────────────────
    if wave_height >= 10:
        for i in range(n):
            scores[i] += (course_positions[i] - 2.5) * (wave_height / 20.0)
    elif wave_height <= 2:
        scores[0] += 1.0

    # ── Step 6: 水温補正（機力差） ───────────────────────────────
    water_temp = _parse_temp(weather.get("水温", "15℃"))
    if water_temp <= 10.0:
        top_wr_idx = int(np.argmax(local_rates))
        scores[top_wr_idx] += 1.5
    elif water_temp >= 25.0:
        if valid_mask.sum() >= 2:
            best_et_idx = int(np.argmin(exhibit_times[valid_mask]))
            scores[best_et_idx] += 1.0

    # ── Step 7: ナイター補正 ──────────────────────────────────────
    is_night = race_no >= G["night_race_start"]
    if is_night:
        scores[0] += W["night_boost"]
        scores[1] -= 0.5

    # ── Step 8: 級別ボーナス ─────────────────────────────────────
    RANK_BONUS = {"A1": 3.5, "A2": 1.5, "B1": 0.0, "B2": -2.0}
    for i, rank in enumerate(df["級別"].values):
        bonus = RANK_BONUS.get(str(rank).strip(), 0.0)
        if str(rank).strip() == "A1" and course_positions[i] >= 3:
            bonus += 1.0
        scores[i] += bonus

    # ── Step 8b: コース別1着率 【v3新規】 ─────────────────────────
    course_wr = _safe_col(df, "コース別1着率")
    if course_wr is not None:
        valid_cwr = course_wr[course_wr > 0]
        if len(valid_cwr) >= 2:
            mean_cwr = valid_cwr.mean() + 1e-9
            for i in range(n):
                if course_wr[i] > 0:
                    scores[i] += (course_wr[i] / mean_cwr - 1.0) * W.get("course_win_rate", 3.5)

    # ── Step 8c: 直近成績モメンタム 【v3新規】 ───────────────────
    recent_avg = _safe_col(df, "直近平均着順")
    recent_wr  = _safe_col(df, "直近勝率")
    if recent_avg is not None and recent_wr is not None:
        valid_ra = recent_avg[recent_avg > 0]
        if len(valid_ra) >= 2:
            mean_ra = valid_ra.mean()
            for i in range(n):
                if recent_avg[i] <= 0:
                    continue
                # 平均着順が良い（低い）ほどプラス
                momentum = (mean_ra - recent_avg[i]) * 0.8
                # 直近勝率ボーナス
                if recent_wr is not None and recent_wr[i] > 0:
                    momentum += (recent_wr[i] / 100.0 - 0.15) * 2.0
                scores[i] += momentum * W.get("momentum", 2.5)

    # ── Step 9: レース番号・グレード特性補正 【v3拡張】 ────────────
    grade    = weather.get("grade", "一般")
    is_final = weather.get("is_final", False)

    # グレード補正: 高グレードほど実力拮抗 → スコア均等化
    grade_factors = {"SG": 0.80, "G1": 0.85, "G2": 0.90, "G3": 0.95, "一般": 1.0}
    gf = grade_factors.get(grade, 1.0)
    if gf < 1.0:
        mean_score = scores.mean()
        scores = scores * gf + mean_score * (1.0 - gf)

    # 優勝戦: イン逃げ率が特に高い
    if is_final:
        scores[0] += W.get("grade_final_boost", 2.0)

    # レース番号特性
    if race_no <= 3:
        for i, rank in enumerate(df["級別"].values):
            if str(rank).strip() == "A1":
                scores[i] += 1.0
    elif race_no >= 10:
        scores = scores * 0.9 + scores.mean() * 0.1

    # ── Step 10: 高橋アナ予想ブースト ────────────────────────────
    if taka_data and taka_data.get("available") and taka_data.get("chart_scores"):
        frames_str = df["枠番"].astype(str).values
        cs = taka_data["chart_scores"]
        taka_boost = W.get("taka_boost", 3.0)
        for i, f in enumerate(frames_str):
            if f in cs:
                scores[i] += taka_boost * cs[f]

    # ── Step 11: ソフトマックスで確率変換 ────────────────────────
    temperature = 6.0
    exp_s = np.exp((scores - scores.max()) / temperature)
    probs = (exp_s / exp_s.sum()) * 100.0

    # ── 信頼度スコア計算 ─────────────────────────────────────────
    top_prob = probs.max()
    confidence = _calc_confidence(
        top_prob, exhibit_times, local_rates, wind_speed, wave_height,
        grade, course_wr, recent_avg,
    )

    # ── ハイライト理由テキスト生成 ────────────────────────────────
    reasons = _build_reasons(
        df, weather, race_no, scores, probs,
        is_calm, is_night, wind_dir, wind_speed,
        wave_height, water_temp, course_positions,
    )

    df["score"]            = scores.round(2)
    df["win_prob"]         = probs.round(1)
    df["_raw_prob"]        = probs
    df["highlight_reason"] = reasons
    df["confidence"]       = confidence

    return df


def _calc_confidence(
    top_prob, exhibit_times, local_rates, wind_speed, wave_height,
    grade="一般", course_wr=None, recent_avg=None,
) -> str:
    score = 0

    if top_prob >= 50: score += 3
    elif top_prob >= 40: score += 2
    elif top_prob >= 30: score += 1

    valid_et = exhibit_times[exhibit_times > 0]
    if len(valid_et) == 6: score += 2
    elif len(valid_et) >= 4: score += 1

    if len(valid_et) >= 2:
        et_range = valid_et.max() - valid_et.min()
        if et_range >= 0.2: score += 2
        elif et_range >= 0.1: score += 1

    lr_range = local_rates.max() - local_rates.min()
    if lr_range >= 2.0: score += 1

    if wind_speed >= 5: score -= 2
    elif wind_speed >= 3: score -= 1
    if wave_height >= 10: score -= 2
    elif wave_height >= 5: score -= 1

    # 【v3追加】コース別成績がある→データ精度UP
    if course_wr is not None and sum(1 for v in course_wr if v > 0) >= 4:
        score += 1

    # 【v3追加】直近成績がある→トレンド把握
    if recent_avg is not None and sum(1 for v in recent_avg if v > 0) >= 4:
        score += 1

    # 【v3追加】高グレードは荒れやすい
    if grade in ("SG", "G1"):
        score -= 1

    if score >= 8: return "S（非常に高い）"
    elif score >= 6: return "A（高い）"
    elif score >= 3: return "B（普通）"
    else: return "C（低い）"


def _build_reasons(
    df, weather, race_no, scores, probs,
    is_calm, is_night, wind_dir, wind_speed,
    wave_height, water_temp, course_positions,
):
    reasons = []
    n = len(df)
    tilts         = df["チルト"].apply(_to_float).values
    exhibit_times = df["展示タイム"].apply(_to_float).values
    local_rates   = df["蒲郡勝率"].apply(_to_float).values
    valid_et      = exhibit_times[exhibit_times > 0]
    best_et       = valid_et.min() if len(valid_et) > 0 else -1

    loc2 = df["蒲郡2連率"].apply(_to_float).values if "蒲郡2連率" in df.columns else local_rates * 2.5

    st_vals = np.full(n, np.nan)
    if "スタートタイミング" in df.columns:
        for i, v in enumerate(df["スタートタイミング"].values):
            try:
                st_vals[i] = float(v) if v is not None else np.nan
            except (TypeError, ValueError):
                pass
    valid_sts = st_vals[~np.isnan(st_vals) & (st_vals >= 0)]
    mean_st   = valid_sts.mean() if len(valid_sts) >= 2 else None

    motor2 = np.zeros(n)
    if "モーター2連率" in df.columns:
        motor2 = np.array([_to_float(v) for v in df["モーター2連率"].values])

    f_counts = _safe_col(df, "F回数")
    l_counts = _safe_col(df, "L回数")
    course_wr = _safe_col(df, "コース別1着率")
    recent_avg = _safe_col(df, "直近平均着順")
    mawari_times = _safe_col(df, "まわり足タイム")
    chokusen_times = _safe_col(df, "直線タイム")
    lap_times_1shu = _safe_col(df, "一周タイム")
    recent_wr_arr = _safe_col(df, "直近勝率")

    for i in range(n):
        r = []
        ci = course_positions[i] if i < len(course_positions) else i

        # コース特性
        if ci == 0:
            if is_calm:
                r.append("無風→イン安定")
            elif wind_dir in ["北", "北北東", "北東"] and wind_speed >= 2:
                r.append(f"向い風({wind_speed}m)→逃げ有利")
            elif wind_dir in ["南", "南南東", "南東"] and wind_speed >= 3:
                r.append(f"追い風({wind_speed}m)→イン注意")
            if is_night:
                r.append("ナイター→イン補正")

        if ci == 1 and wind_dir in ["西", "西北西", "北西"] and wind_speed >= 2:
            r.append(f"横風({wind_dir})→差し有利")

        if ci in [2, 3] and wind_dir in ["東", "東南東", "南東", "南", "南南西"] and wind_speed >= 2:
            r.append(f"{'まくり差し' if ci==2 else 'カドまくり'}条件")

        # 展示タイム
        if exhibit_times[i] == best_et and best_et > 0:
            r.append(f"展示1位({exhibit_times[i]:.2f}s)")
        elif best_et > 0 and exhibit_times[i] > 0:
            gap = exhibit_times[i] - best_et
            if gap <= 0.05:
                r.append(f"展示2位圏({exhibit_times[i]:.2f}s)")

        # チルト
        if tilts[i] < -0.5:
            r.append(f"チルト{tilts[i]}→まくり志向")
        elif tilts[i] > 0.5:
            r.append(f"チルト+{tilts[i]}→逃げ/差し志向")

        # 勝率
        if local_rates[i] == local_rates.max() and local_rates[i] > 0:
            r.append(f"蒲郡勝率1位({local_rates[i]:.2f})")
        if loc2[i] == loc2.max() and loc2[i] > 0:
            r.append(f"蒲郡2連率1位({loc2[i]:.1f}%)")

        # 荒れ条件
        if wave_height >= 10 and ci >= 3:
            r.append(f"高波({wave_height}cm)→外枠有利")

        # 水温
        if water_temp <= 10.0 and local_rates[i] == local_rates.max():
            r.append(f"低水温({water_temp}℃)→機力差注目")

        # スタートタイミング
        if not np.isnan(st_vals[i]):
            if st_vals[i] < 0:
                r.append(f"F注意(ST={st_vals[i]:.2f})")
            elif mean_st is not None:
                diff = mean_st - st_vals[i]
                if diff >= 0.03:
                    r.append(f"ST速い({st_vals[i]:.2f}s)")
                elif diff <= -0.03:
                    r.append(f"ST遅め({st_vals[i]:.2f}s)")

        # モーター2連率
        valid_m2 = motor2[motor2 > 0]
        if len(valid_m2) >= 2 and motor2[i] > 0:
            mean_m2 = valid_m2.mean()
            if motor2[i] >= mean_m2 * 1.10:
                r.append(f"モーター優秀({motor2[i]:.1f}%)")
            elif motor2[i] <= mean_m2 * 0.85:
                r.append(f"モーター劣勢({motor2[i]:.1f}%)")

        # まわり足タイム（ターン力）
        if mawari_times is not None and mawari_times[i] > 0:
            valid_mw = mawari_times[mawari_times > 0]
            if len(valid_mw) >= 2:
                if mawari_times[i] == valid_mw.min():
                    r.append(f"まわり足1位({mawari_times[i]:.2f}s)")
                elif mawari_times[i] <= sorted(valid_mw)[min(1, len(valid_mw)-1)]:
                    r.append(f"まわり足2位圏({mawari_times[i]:.2f}s)")

        # 直線タイム（伸び足）
        if chokusen_times is not None and chokusen_times[i] > 0:
            valid_ch = chokusen_times[chokusen_times > 0]
            if len(valid_ch) >= 2:
                if chokusen_times[i] == valid_ch.min():
                    r.append(f"直線1位({chokusen_times[i]:.2f}s)")

        # ターン巧者判定
        if (mawari_times is not None and chokusen_times is not None
                and mawari_times[i] > 0 and chokusen_times[i] > 0):
            valid_mw2 = mawari_times[mawari_times > 0]
            valid_ch2 = chokusen_times[chokusen_times > 0]
            if len(valid_mw2) >= 2 and len(valid_ch2) >= 2:
                mw_rank = sum(1 for j in range(n) if mawari_times[j] > 0 and mawari_times[j] < mawari_times[i])
                ch_rank = sum(1 for j in range(n) if chokusen_times[j] > 0 and chokusen_times[j] < chokusen_times[i])
                if mw_rank < ch_rank - 1 and ci >= 1:
                    r.append("ターン巧者")

        # 【v3追加】F/L回数
        if f_counts is not None and f_counts[i] >= 1:
            r.append(f"F{int(f_counts[i])}持ち→ST慎重")
        if l_counts is not None and l_counts[i] >= 1:
            r.append(f"L{int(l_counts[i])}持ち")

        # 【v3追加】コース別1着率
        if course_wr is not None and course_wr[i] > 0:
            valid_cwr = course_wr[course_wr > 0]
            if len(valid_cwr) >= 2 and course_wr[i] == valid_cwr.max():
                r.append(f"コース別1着率トップ({course_wr[i]:.1f}%)")
            elif len(valid_cwr) >= 2 and course_wr[i] >= 40:
                r.append(f"コース別1着率高({course_wr[i]:.1f}%)")

        # 【v3追加】直近成績モメンタム
        if recent_avg is not None and recent_avg[i] > 0:
            if recent_avg[i] <= 2.0:
                r.append(f"絶好調(直近平均{recent_avg[i]:.1f}着)")
            elif recent_avg[i] <= 2.5:
                r.append(f"好調(直近平均{recent_avg[i]:.1f}着)")
            elif recent_avg[i] >= 4.5:
                r.append(f"不調(直近平均{recent_avg[i]:.1f}着)")

        reasons.append(" / ".join(r) if r else "—")
    return reasons


# ────────────────────────────────────────────────────────────────
# Henery モデル（修正 Harville）
# ────────────────────────────────────────────────────────────────

def _henery_joint_prob(ability: np.ndarray, i: int, j: int, k: int, gamma: float) -> float:
    """
    Heneryモデルによる3連単結合確率 P(i=1着, j=2着, k=3着) を返す。

    標準Harville: P(j=2着|i=1着) = a[j] / (Σa - a[i])
    Henery修正:   P(j=2着|i=1着) = a[j]^γ / Σ_{m≠i} a[m]^γ
      γ < 1 → 2,3着の確率がより均等になる（人気薄の2着が出やすくなる）
    """
    n = len(ability)
    total = ability.sum() + 1e-12

    # 1着確率（通常のHarville）
    p1 = ability[i] / total

    # 2着確率（Henery修正）: γ乗した能力値で条件付き確率
    a_gamma = ability ** gamma
    rem_1 = sum(a_gamma[m] for m in range(n) if m != i) + 1e-12
    p2_cond = a_gamma[j] / rem_1

    # 3着確率（Henery修正）
    rem_2 = sum(a_gamma[m] for m in range(n) if m != i and m != j) + 1e-12
    p3_cond = a_gamma[k] / rem_2

    return p1 * p2_cond * p3_cond


# ────────────────────────────────────────────────────────────────
# 推奨買い目生成（強化版 v3）
# ────────────────────────────────────────────────────────────────

def generate_recommendations(
    df_scored: pd.DataFrame,
    odds_dict: dict | None = None,
) -> list[dict]:
    """
    Henery結合確率で3連単スコアを計算。
    本命3点・対抗3点は確率順、穴3点は期待値ベースで選定。
    """
    if df_scored.empty or "win_prob" not in df_scored.columns:
        return []

    if "_raw_prob" in df_scored.columns:
        ability = df_scored["_raw_prob"].values / 100.0
    else:
        ability = df_scored["win_prob"].values / 100.0
    frames = df_scored["枠番"].astype(str).values
    n      = len(ability)
    gamma  = W.get("henery_gamma", 0.85)

    candidates = []
    for i in range(n):
        for j in range(n):
            if j == i:
                continue
            for k in range(n):
                if k == i or k == j:
                    continue
                joint = _henery_joint_prob(ability, i, j, k, gamma)
                combo = f"{frames[i]}-{frames[j]}-{frames[k]}"

                # 期待値計算（オッズがある場合）
                actual_odds = odds_dict.get(combo) if odds_dict else None
                ev = round(actual_odds * (joint), 2) if actual_odds is not None else None

                candidates.append({
                    "買い目":    combo,
                    "総合スコア": round(joint * 1000, 3),
                    "的中確率":  round(joint * 100, 2),
                    "公正オッズ": round(1.0 / joint, 1) if joint > 1e-9 else 9999.0,
                    "1着艇":    frames[i],
                    "2着艇":    frames[j],
                    "3着艇":    frames[k],
                    "1着確率":  round(ability[i] / (ability.sum() + 1e-12) * 100, 1),
                    "期待値":   ev,
                })

    candidates.sort(key=lambda x: x["総合スコア"], reverse=True)

    # ── 本命 (top 3) ─────────────────────────────────────────────
    SUB = ["①", "②", "③"]
    honmei = candidates[:3]
    for i, c in enumerate(honmei):
        c["グループ"] = "本命"
        c["タイプ"]   = f"本命{SUB[i]}"

    # ── 対抗 (4〜6位) ────────────────────────────────────────────
    taiko = candidates[3:6]
    for i, c in enumerate(taiko):
        c["グループ"] = "対抗"
        c["タイプ"]   = f"対抗{SUB[i]}"

    # ── 穴 (期待値ベース選定) 【v3改良】 ─────────────────────────
    # オッズがある場合: 期待値が最も高い買い目を優先
    # オッズがない場合: 従来の1着艇多様化ロジック
    top6 = honmei + taiko
    top6_combos = {c["買い目"] for c in top6}
    rest = [c for c in candidates[6:] if c["買い目"] not in top6_combos]

    ev_threshold = W.get("ev_threshold", 1.0)
    if odds_dict and any(c.get("期待値") is not None for c in rest):
        # 期待値ベース: EV > threshold の中で最も高いものを選定
        rest_with_ev = [c for c in rest if c.get("期待値") is not None]
        rest_with_ev.sort(key=lambda x: x["期待値"], reverse=True)
        rest_no_ev = [c for c in rest if c.get("期待値") is None]
        # EV > threshold を優先、足りなければ通常順
        high_ev = [c for c in rest_with_ev if c["期待値"] >= ev_threshold]
        low_ev  = [c for c in rest_with_ev if c["期待値"] < ev_threshold]
        ana_pool = high_ev + low_ev + rest_no_ev
    else:
        # 従来ロジック: 1着艇の多様化
        top6_first = {c["1着艇"] for c in top6}
        ana_diff = [c for c in rest if c["1着艇"] not in top6_first]
        ana_same = [c for c in rest if c["1着艇"] in top6_first]
        ana_pool = ana_diff + ana_same

    ana = ana_pool[:3]
    for i, c in enumerate(ana):
        c["グループ"] = "穴"
        c["タイプ"]   = f"穴{SUB[i]}"

    return honmei + taiko + ana


# ────────────────────────────────────────────────────────────────
# 2連単・2連複推奨買い目生成 【v3新規】
# ────────────────────────────────────────────────────────────────

def generate_2ren_recommendations(
    df_scored: pd.DataFrame,
    odds_2t: dict | None = None,
    odds_2f: dict | None = None,
) -> dict:
    """
    2連単・2連複の推奨買い目を生成する。

    Parameters
    ----------
    odds_2t : 2連単実オッズ {"1-2": 10.6, ...} or None
    odds_2f : 2連複実オッズ {"1=2": 4.9, ...} or None

    Returns
    -------
    {
        "2連単": [{"買い目": "1-2", "的中確率": 25.0, "公正オッズ": 4.0, "実オッズ": 10.6, "期待値": 2.65}, ...],
        "2連複": [{"買い目": "1=2", "的中確率": 35.0, "公正オッズ": 2.9, "実オッズ": 4.9, "期待値": 1.72}, ...],
    }
    """
    if df_scored.empty or "win_prob" not in df_scored.columns:
        return {"2連単": [], "2連複": []}

    if "_raw_prob" in df_scored.columns:
        ability = df_scored["_raw_prob"].values / 100.0
    else:
        ability = df_scored["win_prob"].values / 100.0
    frames = df_scored["枠番"].astype(str).values
    n      = len(ability)
    total  = ability.sum() + 1e-12
    gamma  = W.get("henery_gamma", 0.85)

    # 2連単: Heneryモデルで P(i=1着, j=2着)
    nitan = []
    for i in range(n):
        p1 = ability[i] / total
        a_gamma = ability ** gamma
        rem = sum(a_gamma[m] for m in range(n) if m != i) + 1e-12
        for j in range(n):
            if j == i:
                continue
            p2_cond = a_gamma[j] / rem
            joint = p1 * p2_cond
            combo = f"{frames[i]}-{frames[j]}"
            actual_odds = odds_2t.get(combo) if odds_2t else None
            ev = round(actual_odds * joint, 2) if actual_odds is not None else None
            nitan.append({
                "買い目":    combo,
                "的中確率":  round(joint * 100, 2),
                "公正オッズ": round(1.0 / joint, 1) if joint > 1e-9 else 9999.0,
                "実オッズ":  actual_odds,
                "期待値":   ev,
            })
    nitan.sort(key=lambda x: x["的中確率"], reverse=True)

    # 2連複: P(i=1着,j=2着) + P(j=1着,i=2着)
    seen = set()
    nifuku = []
    for i in range(n):
        for j in range(i + 1, n):
            key = tuple(sorted([i, j]))
            if key in seen:
                continue
            seen.add(key)
            p1 = ability[i] / total
            p2 = ability[j] / total
            a_gamma = ability ** gamma
            rem_i = sum(a_gamma[m] for m in range(n) if m != i) + 1e-12
            rem_j = sum(a_gamma[m] for m in range(n) if m != j) + 1e-12
            joint = p1 * (a_gamma[j] / rem_i) + p2 * (a_gamma[i] / rem_j)
            lo, hi = sorted([frames[i], frames[j]])
            combo = f"{lo}={hi}"
            actual_odds = odds_2f.get(combo) if odds_2f else None
            ev = round(actual_odds * joint, 2) if actual_odds is not None else None
            nifuku.append({
                "買い目":    combo,
                "的中確率":  round(joint * 100, 2),
                "公正オッズ": round(1.0 / joint, 1) if joint > 1e-9 else 9999.0,
                "実オッズ":  actual_odds,
                "期待値":   ev,
            })
    nifuku.sort(key=lambda x: x["的中確率"], reverse=True)

    return {
        "2連単": nitan[:5],
        "2連複": nifuku[:5],
    }


# ────────────────────────────────────────────────────────────────
# メイン呼び出しインターフェース
# ────────────────────────────────────────────────────────────────

def predict(
    df: pd.DataFrame,
    weather: dict,
    race_no: int,
    taka_data: dict | None = None,
    odds_dict: dict | None = None,
    odds_2t: dict | None = None,
    odds_2f: dict | None = None,
) -> dict:
    scored = calculate_scores(df, weather, race_no, taka_data=taka_data)
    recs   = generate_recommendations(scored, odds_dict=odds_dict)
    recs_2ren = generate_2ren_recommendations(scored, odds_2t=odds_2t, odds_2f=odds_2f)

    top_boat   = scored.sort_values("win_prob", ascending=False).iloc[0]
    wind_speed = _parse_wind_speed(weather.get("風速", "?"))
    is_night   = race_no >= G["night_race_start"]
    confidence = scored["confidence"].iloc[0] if "confidence" in scored.columns else "-"
    grade      = weather.get("grade", "一般")

    summary_lines = [
        f"📍 蒲郡 {race_no}R 予想サマリ",
        f"🏆 グレード: {grade}" + (" / 優勝戦" if weather.get("is_final") else ""),
        f"💨 風速: {weather.get('風速','?')} / 風向: {weather.get('風向','?')} / 天気: {weather.get('天気','?')}",
        f"🌡 気温: {weather.get('気温','?')} / 水温: {weather.get('水温','?')} / 波高: {weather.get('波高','?')}",
        f"{'🌙 ナイターレース' if is_night else '☀️ デイレース'}",
        f"🎯 予想信頼度: {confidence}",
        f"🥇 最有力: {top_boat['枠番']}号艇 {top_boat.get('選手名','')} （推定勝率 {top_boat['win_prob']:.1f}%）",
        "",
        "🎯 推奨3連単:",
    ]
    for i, r in enumerate(recs, 1):
        ev_str = f"  期待値: {r['期待値']:.2f}" if r.get("期待値") is not None else ""
        summary_lines.append(
            f"  {i}. {r['買い目']}  [{r['タイプ']}]"
            f"  的中確率: {r['的中確率']:.2f}%  公正オッズ: {r['公正オッズ']:.1f}倍{ev_str}"
        )

    return {
        "scored_df":          scored,
        "recommendations":    recs,
        "recommendations_2ren": recs_2ren,
        "summary":            "\n".join(summary_lines),
    }
