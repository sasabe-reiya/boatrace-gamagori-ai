from __future__ import annotations
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
from datetime import datetime, timedelta
from itertools import permutations

import numpy as np
import pandas as pd

from config import VENUE_CONFIGS, get_venue_params, DEFAULT_VENUE


def _is_weather_reliable(deadline: str | None) -> bool:
    """締め切り時刻の前後1時間以内なら気象データを信頼して予想に反映する。"""
    if not deadline or deadline == "-":
        return False
    try:
        now = datetime.now()
        hh, mm = map(int, deadline.split(":"))
        dl_time = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
        return abs((now - dl_time).total_seconds()) <= 3600
    except (ValueError, TypeError):
        return False


def _neutralize_weather(weather: dict) -> dict:
    """気象データが信頼できない場合、予想に影響しないニュートラルな値にする。"""
    neutral = dict(weather)
    neutral["風速"] = "0m"
    neutral["風向"] = "-"
    neutral["波高"] = "0cm"
    neutral["水温"] = "15℃"
    return neutral


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
# コース特性定数（会場設定から動的に取得）
# ────────────────────────────────────────────────────────────────

def _get_course_win_rate(venue_code: str = DEFAULT_VENUE):
    """指定会場のコース別1着率を返す。"""
    stats = VENUE_CONFIGS[venue_code]["course_stats"]
    return [stats[c]["1着"] / 100.0 for c in range(1, 7)]

# 後方互換用（直接参照している箇所用のフォールバック）
GAMAGORI_COURSE_WIN_RATE = [0.555, 0.095, 0.115, 0.095, 0.085, 0.055]

# セオリー: 向かい風→1マークで減速→まくり決まりやすい→ダッシュ有利
#           追い風→スピード出る→先マイしやすい→イン有利

# 蒲郡の地理: 北東の風=追い風, 南西の風=向かい風
_WIND_EFFECT_GAMAGORI = {
    "北":     {0: +1.5, 1: +0.5, 2: -0.3, 3: -0.8, 4: -1.2, 5: -1.0},
    "北北東":  {0: +1.8, 1: +0.5, 2: -0.5, 3: -1.2, 4: -1.5, 5: -1.0},
    "北東":   {0: +2.0, 1: +0.5, 2: -0.5, 3: -1.5, 4: -1.5, 5: -1.0},
    "東北東":  {0: +1.5, 1: +0.5, 2: -0.5, 3: -1.5, 4: -1.2, 5: -0.8},
    "東":     {0: +0.5, 1: +0.5, 2: -0.8, 3: -1.5, 4: -0.8, 5:  0.0},
    "東南東":  {0:  0.0, 1: -0.3, 2: -0.5, 3:  0.0, 4: +0.5, 5: +0.5},
    "南東":   {0: -0.5, 1: -0.5, 2:  0.0, 3: +0.5, 4: +0.8, 5: +1.0},
    "南南東":  {0: -1.0, 1: -0.5, 2: +0.3, 3: +0.8, 4: +1.0, 5: +1.5},
    "南":     {0: -1.5, 1: -0.8, 2: +0.3, 3: +0.8, 4: +1.2, 5: +1.5},
    "南南西":  {0: -1.8, 1: -0.8, 2: +0.5, 3: +1.2, 4: +1.5, 5: +1.8},
    "南西":   {0: -2.0, 1: -1.0, 2: +0.5, 3: +1.5, 4: +1.5, 5: +2.0},
    "西南西":  {0: -1.5, 1: -0.8, 2: +0.3, 3: +0.8, 4: +1.2, 5: +1.5},
    "西":     {0: -0.5, 1: -1.0, 2: -0.5, 3:  0.0, 4: +0.5, 5: +0.5},
    "西北西":  {0:  0.0, 1: -0.5, 2: -0.5, 3: -0.3, 4: -0.5, 5: -0.3},
    "北西":   {0: +0.5, 1:  0.0, 2: -0.3, 3: -0.5, 4: -0.8, 5: -0.5},
    "北北西":  {0: +1.0, 1: +0.3, 2: -0.3, 3: -0.8, 4: -1.0, 5: -0.8},
    "-":      {0:  0.0, 1:  0.0, 2:  0.0, 3:  0.0, 4:  0.0, 5:  0.0},
}

# 住之江の地理: 北の風=追い風, 南の風=向かい風
_WIND_EFFECT_SUMINOE = {
    "北":     {0: +2.0, 1: +0.5, 2: -0.5, 3: -1.0, 4: -1.5, 5: -1.0},
    "北北東":  {0: +1.5, 1: +0.5, 2: -0.5, 3: -1.0, 4: -1.2, 5: -0.8},
    "北東":   {0: +1.0, 1: +0.5, 2: -0.5, 3: -1.0, 4: -0.8, 5: -0.5},
    "東北東":  {0: +0.5, 1: +0.5, 2: -0.8, 3: -0.5, 4: -0.5, 5:  0.0},
    "東":     {0:  0.0, 1: +0.5, 2: -1.0, 3: -0.5, 4:  0.0, 5: +0.5},
    "東南東":  {0: -0.5, 1: -0.3, 2: -0.5, 3:  0.0, 4: +0.5, 5: +1.0},
    "南東":   {0: -1.0, 1: -0.5, 2:  0.0, 3: +0.5, 4: +1.0, 5: +1.5},
    "南南東":  {0: -1.5, 1: -0.8, 2: +0.3, 3: +0.8, 4: +1.2, 5: +1.8},
    "南":     {0: -2.0, 1: -1.0, 2: +0.5, 3: +1.0, 4: +1.5, 5: +2.0},
    "南南西":  {0: -1.5, 1: -0.8, 2: +0.3, 3: +0.8, 4: +1.2, 5: +1.5},
    "南西":   {0: -1.0, 1: -0.5, 2:  0.0, 3: +0.5, 4: +1.0, 5: +1.5},
    "西南西":  {0: -0.5, 1: -0.3, 2: -0.5, 3:  0.0, 4: +0.5, 5: +1.0},
    "西":     {0:  0.0, 1: -0.5, 2: -1.0, 3: -0.5, 4:  0.0, 5: +0.5},
    "西北西":  {0: +0.5, 1: -0.5, 2: -0.5, 3: -0.3, 4: -0.5, 5: -0.5},
    "北西":   {0: +1.0, 1:  0.0, 2: -0.3, 3: -0.5, 4: -0.8, 5: -0.8},
    "北北西":  {0: +1.5, 1: +0.3, 2: -0.3, 3: -0.8, 4: -1.2, 5: -1.0},
    "-":      {0:  0.0, 1:  0.0, 2:  0.0, 3:  0.0, 4:  0.0, 5:  0.0},
}

# 大村の地理: 南西の風=追い風, 北東の風=向かい風
_WIND_EFFECT_OMURA = {
    "北":     {0: -1.5, 1: -0.5, 2: +0.3, 3: +0.8, 4: +1.0, 5: +0.8},
    "北北東":  {0: -1.8, 1: -0.8, 2: +0.5, 3: +1.0, 4: +1.2, 5: +1.0},
    "北東":   {0: -2.0, 1: -1.0, 2: +0.5, 3: +1.2, 4: +1.5, 5: +1.0},
    "東北東":  {0: -1.8, 1: -0.8, 2: +0.5, 3: +1.0, 4: +1.2, 5: +1.0},
    "東":     {0: -0.5, 1: -0.5, 2:  0.0, 3: +0.5, 4: +0.8, 5: +1.0},
    "東南東":  {0: -0.5, 1: -0.3, 2: -0.5, 3:  0.0, 4: +0.5, 5: +0.5},
    "南東":   {0:  0.0, 1: -0.5, 2: -0.5, 3: -0.3, 4: -0.5, 5: -0.3},
    "南南東":  {0: +0.5, 1:  0.0, 2: -0.3, 3: -0.5, 4: -0.8, 5: -0.5},
    "南":     {0: +1.5, 1: +0.5, 2: -0.3, 3: -0.8, 4: -1.0, 5: -0.8},
    "南南西":  {0: +1.8, 1: +0.8, 2: -0.5, 3: -1.0, 4: -1.2, 5: -1.0},
    "南西":   {0: +2.0, 1: +0.5, 2: -0.5, 3: -1.0, 4: -1.5, 5: -1.0},
    "西南西":  {0: +1.5, 1: +0.8, 2: -0.3, 3: -0.8, 4: -1.2, 5: -0.8},
    "西":     {0: +0.5, 1: -0.5, 2: -0.3, 3: -0.5, 4: -0.8, 5: -0.5},
    "西北西":  {0:  0.0, 1: -0.5, 2: -0.5, 3: -0.3, 4: -0.5, 5: -0.3},
    "北西":   {0: -0.5, 1: -0.3, 2: -0.5, 3:  0.0, 4: +0.5, 5: +0.5},
    "北北西":  {0: -1.0, 1: -0.5, 2:  0.0, 3: +0.5, 4: +0.8, 5: +0.8},
    "-":      {0:  0.0, 1:  0.0, 2:  0.0, 3:  0.0, 4:  0.0, 5:  0.0},
}

def _get_wind_effect(venue_code: str = DEFAULT_VENUE) -> dict:
    """指定会場の風向×コース効果テーブルを返す。"""
    if venue_code == "12":
        return _WIND_EFFECT_SUMINOE
    if venue_code == "24":
        return _WIND_EFFECT_OMURA
    return _WIND_EFFECT_GAMAGORI

# 後方互換
WIND_COURSE_EFFECT = _WIND_EFFECT_GAMAGORI

# ── 風向→風種別（追い風/向かい風/右横風/左横風）分類 ──────────────
# 各会場のコース主軸に基づき16方位を4分類する。
# 蒲郡: コース主軸=北東（追い風方向）、住之江: コース主軸=北
_WIND_TYPE_GAMAGORI = {
    "北": "追い風", "北北東": "追い風", "北東": "追い風", "東北東": "追い風",
    "東": "右横風", "東南東": "右横風", "南東": "右横風",
    "南南東": "向かい風", "南": "向かい風", "南南西": "向かい風", "南西": "向かい風", "西南西": "向かい風",
    "西": "左横風", "西北西": "左横風", "北西": "左横風",
    "北北西": "追い風",
}
_WIND_TYPE_SUMINOE = {
    "北西": "追い風", "北北西": "追い風", "北": "追い風", "北北東": "追い風",
    "北東": "右横風", "東北東": "右横風", "東": "右横風", "東南東": "右横風",
    "南東": "向かい風", "南南東": "向かい風", "南": "向かい風", "南南西": "向かい風",
    "南西": "左横風", "西南西": "左横風", "西": "左横風", "西北西": "左横風",
}
# 大村: 追い風=南西から吹く風（ボート進行方向=北東）
_WIND_TYPE_OMURA = {
    "南西": "追い風", "西南西": "追い風", "南": "追い風", "南南西": "追い風",
    "北西": "左横風", "西北西": "左横風", "西": "左横風",
    "北北西": "向かい風", "北東": "向かい風", "東北東": "向かい風", "北": "向かい風", "北北東": "向かい風",
    "南東": "右横風", "東南東": "右横風", "東": "右横風", "南南東": "右横風",
}

def get_wind_type(wind_dir: str, venue_code: str = DEFAULT_VENUE) -> str:
    """指定会場に応じて風向から風種別（追い風/向かい風/右横風/左横風）を返す。"""
    if wind_dir == "-":
        return "-"
    _tables = {"12": _WIND_TYPE_SUMINOE, "24": _WIND_TYPE_OMURA}
    table = _tables.get(venue_code, _WIND_TYPE_GAMAGORI)
    return table.get(wind_dir, "-")


# ────────────────────────────────────────────────────────────────
# 決まり手連動着順補正マトリクス 【v6新規】
# CONDITIONAL_PLACEMENT_MATRIX[勝者コース(1-indexed)][決まり手]
#   -> {2着候補コース(0-indexed): 倍率}
# 倍率: 1.0=変更なし, >1.0=2着に来やすい, <1.0=来にくい
#
# 値の根拠: backtest_data.json 240レース (蒲郡2026年実績) から算出
#   ratio = P(2着=Y|1着=X) / P(2着=Y) をベイズ縮小 (prior_n=10)
#   決まり手別データ未取得のため全決まり手共通値を使用
# ────────────────────────────────────────────────────────────────

# 決まり手ごとに同一値を適用するヘルパー
def _uniform_kimarite(multipliers: dict, kimarite_list: list) -> dict:
    return {k: dict(multipliers) for k in kimarite_list}

CONDITIONAL_PLACEMENT_MATRIX = {
    1: _uniform_kimarite(  # 1コースが勝つ場合 (n=122)
        {1: 1.37, 2: 1.13, 3: 1.11, 4: 0.96, 5: 0.98},
        ["逃げ", "差し"],
    ),
    2: _uniform_kimarite(  # 2コースが勝つ場合 (n=29)
        {0: 0.92, 2: 1.49, 3: 1.42, 4: 1.05, 5: 1.58},
        ["差し", "まくり"],
    ),
    3: _uniform_kimarite(  # 3コースが勝つ場合 (n=33)
        {0: 2.80, 1: 1.16, 3: 0.38, 4: 0.77, 5: 0.63},
        ["まくり", "まくり差し", "差し"],
    ),
    4: _uniform_kimarite(  # 4コースが勝つ場合 (n=26)
        {0: 1.90, 1: 0.48, 2: 0.76, 4: 2.00, 5: 1.71},
        ["まくり", "まくり差し", "差し"],
    ),
    5: _uniform_kimarite(  # 5コースが勝つ場合 (n=14)
        {0: 1.23, 1: 1.02, 2: 1.14, 3: 1.23, 5: 0.42},
        ["まくり", "まくり差し"],
    ),
    6: _uniform_kimarite(  # 6コースが勝つ場合 (n=16)
        {0: 1.38, 1: 0.66, 2: 1.06, 3: 1.63, 4: 0.68},
        ["まくり", "まくり差し"],
    ),
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
    racer_kimarite: dict | None = None,
    venue_code: str = DEFAULT_VENUE,
) -> pd.DataFrame:
    df = df.copy()
    n = len(df)
    if n == 0:
        return df

    # 会場固有パラメータをローカルに取得（グローバル状態に依存しない）
    G, _course_stats, W, _jycd, _jyname = get_venue_params(venue_code)

    # ── Step 1: コース位置の決定（ベイズモデル） ────────────────────
    # コース有利度は事前確率(prior)として分離し、個人スコアとベイズ結合する。
    # これにより、1号艇の選手が弱い場合に勝率が適切に下がるようになる。
    if "進入コース" in df.columns:
        course_positions = []
        for v in df["進入コース"].values:
            try:
                pos = int(v) - 1
                course_positions.append(max(0, min(pos, 5)))
            except (TypeError, ValueError):
                course_positions.append(len(course_positions))
    else:
        course_positions = list(range(n))
    _cwr = _get_course_win_rate(venue_code)
    course_probs = np.array([_cwr[pos] for pos in course_positions])
    scores = np.zeros(n, dtype=float)

    # ── Step 2: 勝率・2連率複合補正 ──────────────────────────────
    win_rates   = df["全国勝率"].apply(_to_float).values
    local_rates = df["当地勝率"].apply(_to_float).values

    if "全国2連率" in df.columns:
        nat2_rates = df["全国2連率"].apply(_to_float).values
    else:
        nat2_rates = win_rates * 2.5

    if "当地2連率" in df.columns:
        loc2_rates = df["当地2連率"].apply(_to_float).values
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

    # 安定板使用時は展示タイムの信頼性が低下するため重みを削減
    _stabilizer_et_factor = W.get("stabilizer_et_discount", 0.6) if weather.get("安定板", False) else 1.0

    if valid_mask.sum() >= 2:
        et_valid = exhibit_times[valid_mask]
        mean_et  = et_valid.mean()
        std_et   = et_valid.std() + 1e-9
        best_et  = et_valid.min()

        for i in range(n):
            if not valid_mask[i]:
                continue
            et_z = -(exhibit_times[i] - mean_et) / std_et
            scores[i] += et_z * W["exhibit_time"] * _stabilizer_et_factor

            gap_from_best = exhibit_times[i] - best_et
            if gap_from_best == 0.0:
                scores[i] += W.get("exhibit_top_bonus", 2.0) * _stabilizer_et_factor
            elif gap_from_best <= 0.05:
                scores[i] += W.get("exhibit_top_bonus", 2.0) * 0.5 * _stabilizer_et_factor

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
                st_score = st_diff * W.get("st_weight", 25.0)
                st_score = max(-3.0, min(3.0, st_score))  # v11: 極端なST差の影響をクランプ
                scores[i] += st_score

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
    _mw_weight = W.get("mawari_time", 1.2)
    if mawari_times is not None and _mw_weight > 0:
        valid_mw = mawari_times[mawari_times > 0]
        if len(valid_mw) >= 2:
            mean_mw = valid_mw.mean()
            std_mw  = valid_mw.std() + 1e-9
            for i in range(n):
                if mawari_times[i] <= 0:
                    continue
                mw_z = -(mawari_times[i] - mean_mw) / std_mw
                scores[i] += mw_z * _mw_weight

    # 直線タイム: 伸び足の指標
    chokusen_times = _safe_col(df, "直線タイム")
    _ch_weight = W.get("chokusen_time", 1.5)
    if chokusen_times is not None and _ch_weight > 0:
        valid_ch = chokusen_times[chokusen_times > 0]
        if len(valid_ch) >= 2:
            mean_ch = valid_ch.mean()
            std_ch  = valid_ch.std() + 1e-9
            for i in range(n):
                if chokusen_times[i] <= 0:
                    continue
                ch_z = -(chokusen_times[i] - mean_ch) / std_ch
                scores[i] += ch_z * _ch_weight

    # 一周タイム: 総合力の指標（v11: まわり足+直線と三重計上のため、重み0で無効化推奨）
    lap_times = _safe_col(df, "一周タイム")
    _lt_weight = W.get("lap_time", 0.0)
    if lap_times is not None and _lt_weight > 0:
        valid_lt = lap_times[lap_times > 0]
        if len(valid_lt) >= 2:
            mean_lt = valid_lt.mean()
            std_lt  = valid_lt.std() + 1e-9
            for i in range(n):
                if lap_times[i] <= 0:
                    continue
                lt_z = -(lap_times[i] - mean_lt) / std_lt
                scores[i] += lt_z * _lt_weight

    # ターン巧者判定: 直線は遅いがまわり足が速い → 差し/まくり差し向き
    # v11: 段階化（順位差に応じてボーナスを按分）
    if (mawari_times is not None and chokusen_times is not None
            and sum(mawari_times > 0) >= 2 and sum(chokusen_times > 0) >= 2):
        _tm_weight = W.get("turn_master_bonus", 2.0)
        for i in range(n):
            if mawari_times[i] <= 0 or chokusen_times[i] <= 0:
                continue
            mw_rank = sum(1 for j in range(n) if mawari_times[j] > 0 and mawari_times[j] < mawari_times[i])
            ch_rank = sum(1 for j in range(n) if chokusen_times[j] > 0 and chokusen_times[j] < chokusen_times[i])
            rank_gap = ch_rank - mw_rank
            if rank_gap >= 2 and course_positions[i] >= 1:
                # v11: 順位差2=50%, 3=75%, 4+=100%のボーナスに段階化
                ratio = min(1.0, (rank_gap - 1) / 3.0)
                scores[i] += _tm_weight * ratio

    # ── Step 4: 風向×コース特性の複合補正 ────────────────────────
    wind_dir = weather.get("風向", "-")
    is_calm  = wind_speed <= G["calm_wind_threshold"]

    wind_effect_table = _get_wind_effect(venue_code)
    if not is_calm and wind_dir in wind_effect_table:
        effect = wind_effect_table[wind_dir]
        wind_multiplier = min(wind_speed / 3.0, 3.0)
        for i in range(n):
            scores[i] += effect.get(course_positions[i], 0.0) * wind_multiplier

    if is_calm:
        if local_rates[0] >= local_rates.mean():
            scores[0] += W["calm_in_boost"]
        # v8: ハードコード加点を廃止（コース事前確率が静水面効果を包含）

    # ── Step 5: 波高補正 ─────────────────────────────────────────
    if wave_height >= 10:
        for i in range(n):
            scores[i] += (course_positions[i] - 2.5) * (wave_height / 20.0)
    elif wave_height <= 2:
        pass  # v8: ハードコード加点を廃止（コース事前確率が低波効果を包含）

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
    # v8で night_boost=0 に設定済み（ベイズpriorがイン有利を反映）
    # v11: 2コースへのハードコードペナルティ(-0.5)も削除
    is_night = race_no >= G["night_race_start"]
    if is_night and W["night_boost"] > 0:
        scores[0] += W["night_boost"]

    # ── Step 8: 級別ボーナス（v11: SCORE_WEIGHTSから取得） ────────
    RANK_BONUS = {
        "A1": W.get("rank_a1", 3.0),
        "A2": W.get("rank_a2", 1.2),
        "B1": W.get("rank_b1", 0.0),
        "B2": W.get("rank_b2", -1.5),
    }
    for i, rank in enumerate(df["級別"].values):
        bonus = RANK_BONUS.get(str(rank).strip(), 0.0)
        if str(rank).strip() == "A1" and course_positions[i] >= 3:
            bonus += 0.5  # v11: 1.0→0.5 外枠A1追加ボーナス縮小
        scores[i] += bonus

    # ── Step 8b: コース別1着率 【v3新規 → コース基準正規化 + log圧縮】
    # インコースの1着率が高いのは当然なので、各コースの全体平均1着率を
    # 基準にして「そのコースの平均と比べてどれだけ優秀か」で評価する。
    # 外コースは基準値が極端に低いため、比率が爆発しないようlog圧縮する。
    course_wr = _safe_col(df, "コース別1着率")
    if course_wr is not None:
        _cwr_base = _get_course_win_rate(venue_code)  # コース別の全体平均1着率 (0-1)
        valid_count = 0
        normalized = np.zeros(n)
        for i in range(n):
            if course_wr[i] > 0:
                base = _cwr_base[course_positions[i]] * 100.0  # %換算
                ratio = course_wr[i] / (base + 1e-9)
                # log圧縮: ratio=2.0 → +0.69, ratio=5.0 → +1.61, ratio=0.5 → -0.69
                normalized[i] = np.log(max(ratio, 0.01))
                valid_count += 1
        if valid_count >= 2:
            for i in range(n):
                if course_wr[i] > 0:
                    scores[i] += normalized[i] * W.get("course_win_rate", 3.5)

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

    # ── Step 8d: 選手別決まり手適合度 【v5新規】 ─────────────────
    # 各選手のコース別決まり手傾向と、蒲郡のコース特性を比較して
    # 得意決まり手が合致する選手にボーナスを付与する。
    if racer_kimarite:
        # コース別の主要決まり手パターン（各コースで有効な決まり手とその基準ウェイト）
        _COURSE_KIMARITE_WEIGHT = {
            1: {"逃げ": 1.0},
            2: {"差し": 0.6, "まくり": 0.4},
            3: {"まくり差し": 0.5, "まくり": 0.3, "差し": 0.2},
            4: {"まくり": 0.5, "まくり差し": 0.3, "差し": 0.2},
            5: {"まくり差し": 0.6, "まくり": 0.3, "差し": 0.1},
            6: {"まくり差し": 0.5, "まくり": 0.4, "差し": 0.1},
        }
        frames_str = df["枠番"].astype(str).values
        rk_weight = W.get("racer_kimarite_weight", 3.0)

        for i in range(n):
            frame = frames_str[i]
            rk = racer_kimarite.get(frame)
            if not rk:
                continue

            ci = course_positions[i] + 1  # 1-indexed
            patterns = _COURSE_KIMARITE_WEIGHT.get(ci, {})
            if not patterns:
                continue

            # 適合度 = 選手の得意決まり手率 × コースパターン重み の加重和
            fit_score = 0.0
            for kimarite_name, weight in patterns.items():
                racer_pct = rk.get(kimarite_name, 0.0) / 100.0
                fit_score += racer_pct * weight

            # fit_score は 0.0〜1.0。0.5を基準に偏差でスコア調整。
            scores[i] += (fit_score - 0.3) * rk_weight

    # ── Step 8e: 安定板使用補正 【v9新規】 ─────────────────────────
    # 安定板装着時の影響:
    #   - ターンが安定 → インコースが逃げやすい（1コース有利度UP）
    #   - 全艇の最高速が低下 → 機力差が縮小（スコア均等化）
    #   - 展示タイムの信頼性が低下（展示後に装着するケースあり）
    #   - 荒天条件との複合でさらにイン有利
    is_stabilizer = weather.get("安定板", False)
    if is_stabilizer:
        stab_in_boost = W.get("stabilizer_in_boost", 3.0)
        stab_equalize = W.get("stabilizer_equalize", 0.15)

        # イン有利強化: 1コースにボーナス、外枠にペナルティ
        for i in range(n):
            if course_positions[i] == 0:
                scores[i] += stab_in_boost
            elif course_positions[i] == 1:
                scores[i] += stab_in_boost * 0.2
            elif course_positions[i] >= 3:
                scores[i] -= stab_in_boost * 0.15 * (course_positions[i] - 2)

        # スコア均等化: 機力差が縮小するため個人スコアの差を圧縮
        mean_score = scores.mean()
        scores = scores * (1.0 - stab_equalize) + mean_score * stab_equalize

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

    # ── Step 11: ベイズ結合で確率変換 ─────────────────────────────
    # 個人スコア → ソフトマックスで個人力確率に変換
    ind_temp = W.get("individual_temp", 15.0)
    ind_exp = np.exp((scores - scores.max()) / ind_temp)
    ind_prob = ind_exp / ind_exp.sum()

    # コース有利度（事前確率）× 個人力（尤度）→ 事後確率
    combined = course_probs * ind_prob
    probs = (combined / combined.sum()) * 100.0

    # 確率キャップ: 1着確率の上限を制限し、超過分を他艇に再分配
    prob_cap = W.get("prob_cap", 70.0)
    if probs.max() > prob_cap:
        excess = probs.max() - prob_cap
        top_idx = int(np.argmax(probs))
        others_sum = probs.sum() - probs[top_idx]
        probs[top_idx] = prob_cap
        if others_sum > 0:
            probs[:top_idx] += probs[:top_idx] / others_sum * excess
            probs[top_idx+1:] += probs[top_idx+1:] / others_sum * excess

    # ── 信頼度スコア計算 ─────────────────────────────────────────
    top_prob = probs.max()
    confidence = _calc_confidence(
        top_prob, exhibit_times, local_rates, wind_speed, wave_height,
        grade, course_wr, recent_avg,
        is_stabilizer=weather.get("安定板", False),
    )

    # ── ハイライト理由テキスト生成 ────────────────────────────────
    reasons = _build_reasons(
        df, weather, race_no, scores, probs,
        is_calm, is_night, wind_dir, wind_speed,
        wave_height, water_temp, course_positions,
        racer_kimarite=racer_kimarite,
        venue_code=venue_code,
    )

    df["score"]            = scores.round(2)
    df["win_prob"]         = probs.round(1)
    df["_raw_prob"]        = probs
    df["highlight_reason"] = reasons
    df["confidence"]       = confidence
    df["_course_pos"]      = course_positions

    return df


def _calc_confidence(
    top_prob, exhibit_times, local_rates, wind_speed, wave_height,
    grade="一般", course_wr=None, recent_avg=None, is_stabilizer=False,
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

    # 【v9追加】安定板使用時は展示データの信頼性低下
    if is_stabilizer:
        score -= 1

    if score >= 8: return "S（非常に高い）"
    elif score >= 6: return "A（高い）"
    elif score >= 3: return "B（普通）"
    else: return "C（低い）"


def _build_reasons(
    df, weather, race_no, scores, probs,
    is_calm, is_night, wind_dir, wind_speed,
    wave_height, water_temp, course_positions,
    racer_kimarite=None,
    venue_code: str = DEFAULT_VENUE,
):
    reasons = []
    n = len(df)
    tilts         = df["チルト"].apply(_to_float).values
    exhibit_times = df["展示タイム"].apply(_to_float).values
    local_rates   = df["当地勝率"].apply(_to_float).values
    valid_et      = exhibit_times[exhibit_times > 0]
    best_et       = valid_et.min() if len(valid_et) > 0 else -1

    loc2 = df["当地2連率"].apply(_to_float).values if "当地2連率" in df.columns else local_rates * 2.5

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

    is_stabilizer = weather.get("安定板", False)

    for i in range(n):
        r = []
        ci = course_positions[i] if i < len(course_positions) else i

        # 安定板使用時のコース別理由
        if is_stabilizer:
            if ci == 0:
                r.append("安定板→イン有利強化")
            elif ci >= 4:
                r.append("安定板→外枠不利")

        # コース特性（風向効果テーブルから追い風/向かい風を自動判定）
        _wt = _get_wind_effect(venue_code)
        _wind_in_eff = _wt.get(wind_dir, {}).get(0, 0.0)
        if ci == 0:
            if is_calm:
                r.append("無風→イン安定")
            elif _wind_in_eff < -0.5 and wind_speed >= 2:
                r.append(f"向い風({wind_speed}m)→まくり注意")
            elif _wind_in_eff > 0.5 and wind_speed >= 2:
                r.append(f"追い風({wind_speed}m)→逃げ有利")
            if is_night:
                r.append("ナイター→イン補正")

        if ci == 1 and abs(_wind_in_eff) <= 0.5 and _wt.get(wind_dir, {}).get(1, 0.0) > 0 and wind_speed >= 2:
            r.append(f"横風({wind_dir})→差し有利")

        if ci in [2, 3] and _wind_in_eff < -0.5 and wind_speed >= 2:
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
            r.append(f"当地勝率1位({local_rates[i]:.2f})")
        if loc2[i] == loc2.max() and loc2[i] > 0:
            r.append(f"当地2連率1位({loc2[i]:.1f}%)")

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

        # 【v3追加】コース別1着率（コース平均比で評価）
        if course_wr is not None and course_wr[i] > 0:
            _cwr_base_r = _get_course_win_rate(venue_code)
            base_pct = _cwr_base_r[course_positions[i]] * 100.0
            ratio = course_wr[i] / (base_pct + 1e-9)
            if ratio >= 1.3:
                r.append(f"コース別1着率◎(平均比{ratio:.0%})")
            elif ratio >= 1.15:
                r.append(f"コース別1着率○(平均比{ratio:.0%})")

        # 【v3追加】直近成績モメンタム
        if recent_avg is not None and recent_avg[i] > 0:
            if recent_avg[i] <= 2.0:
                r.append(f"絶好調(直近平均{recent_avg[i]:.1f}着)")
            elif recent_avg[i] <= 2.5:
                r.append(f"好調(直近平均{recent_avg[i]:.1f}着)")
            elif recent_avg[i] >= 4.5:
                r.append(f"不調(直近平均{recent_avg[i]:.1f}着)")

        # 【v5追加】選手別決まり手
        if racer_kimarite:
            frames_str = df["枠番"].astype(str).values
            rk = racer_kimarite.get(frames_str[i])
            if rk and rk.get("レース数", 0) >= 5:
                # 最も得意な決まり手を表示
                kimarite_items = [
                    (rk.get("逃げ", 0), "逃げ"), (rk.get("差し", 0), "差し"),
                    (rk.get("まくり", 0), "まくり"), (rk.get("まくり差し", 0), "まくり差し"),
                ]
                best_kt = max(kimarite_items, key=lambda x: x[0])
                if best_kt[0] >= 30:
                    r.append(f"得意:{best_kt[1]}({best_kt[0]:.0f}%/{rk['レース数']}走)")

        reasons.append(" / ".join(r) if r else "—")
    return reasons


# ────────────────────────────────────────────────────────────────
# Henery モデル（修正 Harville）
# ────────────────────────────────────────────────────────────────

def _henery_joint_prob(
    ability: np.ndarray, i: int, j: int, k: int,
    gamma: float,
    a_gamma_override: np.ndarray | None = None,
) -> float:
    """
    Heneryモデルによる3連単結合確率 P(i=1着, j=2着, k=3着) を返す。

    標準Harville: P(j=2着|i=1着) = a[j] / (Σa - a[i])
    Henery修正:   P(j=2着|i=1着) = a[j]^γ / Σ_{m≠i} a[m]^γ
      γ < 1 → 2,3着の確率がより均等になる（人気薄の2着が出やすくなる）

    a_gamma_override: 決まり手連動補正済みの ability^γ 配列。
      渡された場合、2着・3着の条件付き確率をこの配列で計算する。
    """
    n = len(ability)
    total = ability.sum() + 1e-12

    # 1着確率（通常のHarville）
    p1 = ability[i] / total

    # 2着確率（Henery修正）: γ乗した能力値で条件付き確率
    a_gamma = a_gamma_override if a_gamma_override is not None else ability ** gamma
    rem_1 = sum(a_gamma[m] for m in range(n) if m != i) + 1e-12
    p2_cond = a_gamma[j] / rem_1

    # 3着確率（Henery修正）
    rem_2 = sum(a_gamma[m] for m in range(n) if m != i and m != j) + 1e-12
    p3_cond = a_gamma[k] / rem_2

    return p1 * p2_cond * p3_cond


def _adjusted_ability_for_winner(
    ability: np.ndarray,
    winner_idx: int,
    gamma: float,
    course_positions: list[int],
    racer_kimarite: dict | None,
    frames: np.ndarray,
    venue_code: str = DEFAULT_VENUE,
) -> np.ndarray:
    """
    決まり手連動着順補正 【v6】

    勝者の決まり手傾向に基づいて、2着以降の各艇の ability^γ を調整する。
    例: 3号艇がまくり傾向 → 4号艇の ability^γ を引き上げ、1,2号艇を引き下げ。

    Returns: 調整済み ability^γ 配列（shape: (n,)）
    """
    W = VENUE_CONFIGS[venue_code]["score_weights"]
    a_gamma = ability ** gamma

    if not racer_kimarite:
        return a_gamma

    winner_course = course_positions[winner_idx] + 1  # 1-indexed
    winner_frame = str(frames[winner_idx])
    rk = racer_kimarite.get(winner_frame)
    if not rk:
        return a_gamma

    course_patterns = CONDITIONAL_PLACEMENT_MATRIX.get(winner_course)
    if not course_patterns:
        return a_gamma

    w = W.get("kimarite_placement_weight", 0.5)
    if w <= 0:
        return a_gamma

    n = len(ability)
    adjusted = a_gamma.copy()

    for m in range(n):
        if m == winner_idx:
            continue
        runner_up_course = course_positions[m]  # 0-indexed

        # 各決まり手の割合で加重平均した倍率を計算
        total_pct = 0.0
        weighted_mult = 0.0
        for kimarite_name, mult_dict in course_patterns.items():
            pct = rk.get(kimarite_name, 0.0)  # 0-100%
            if pct <= 0:
                continue
            mult = mult_dict.get(runner_up_course, 1.0)
            weighted_mult += pct * mult
            total_pct += pct

        if total_pct > 0:
            raw_mult = weighted_mult / total_pct
            # weight で 1.0 とブレンド (0.5 → 半分の強さで適用)
            final_mult = 1.0 + (raw_mult - 1.0) * w
            adjusted[m] *= final_mult

    return adjusted


# ────────────────────────────────────────────────────────────────
# 推奨買い目生成
# ────────────────────────────────────────────────────────────────

def generate_recommendations(
    df_scored: pd.DataFrame,
    odds_dict: dict | None = None,
    course_positions: list[int] | None = None,
    racer_kimarite: dict | None = None,
    venue_code: str = DEFAULT_VENUE,
) -> list[dict]:
    """
    Henery結合確率で3連単スコアを計算。
    本命3点・対抗3点は確率順、穴3点は期待値ベースで選定。
    【v6】決まり手連動着順補正を適用。
    """
    W = VENUE_CONFIGS[venue_code]["score_weights"]
    if df_scored.empty or "win_prob" not in df_scored.columns:
        return [], []

    if "_raw_prob" in df_scored.columns:
        ability = df_scored["_raw_prob"].values / 100.0
    else:
        ability = df_scored["win_prob"].values / 100.0
    frames = df_scored["枠番"].astype(str).values
    n      = len(ability)
    gamma  = W.get("henery_gamma", 0.85)

    # 【v6】各勝者ごとの決まり手連動補正済み ability^γ を事前計算
    cp = course_positions or list(range(n))
    adj_cache = {}
    for i in range(n):
        adj_cache[i] = _adjusted_ability_for_winner(
            ability, i, gamma, cp, racer_kimarite, frames,
            venue_code=venue_code,
        )

    candidates = []
    for i in range(n):
        a_adj = adj_cache[i]
        for j in range(n):
            if j == i:
                continue
            for k in range(n):
                if k == i or k == j:
                    continue
                joint = _henery_joint_prob(ability, i, j, k, gamma,
                                          a_gamma_override=a_adj)

                combo = f"{frames[i]}-{frames[j]}-{frames[k]}"

                # 期待値計算（オッズがある場合）
                actual_odds = odds_dict.get(combo) if odds_dict else None
                ev = round(actual_odds * (joint), 2) if actual_odds is not None else None

                candidates.append({
                    "買い目":    combo,
                    "総合スコア": round(joint * 1000, 3),
                    "的中確率":  round(joint * 100, 2),
                    "公正オッズ": round(1.0 / joint, 1) if joint > 1e-9 else 9999.0,
                    "実オッズ":  actual_odds,
                    "1着艇":    frames[i],
                    "2着艇":    frames[j],
                    "3着艇":    frames[k],
                    "1着確率":  round(ability[i] / (ability.sum() + 1e-12) * 100, 1),
                    "期待値":   ev,
                })

    candidates.sort(key=lambda x: x["総合スコア"], reverse=True)

    # ── 本命 (top 4) ─────────────────────────────────────────────
    SUB = ["①", "②", "③", "④"]
    honmei = candidates[:4]
    for i, c in enumerate(honmei):
        c["グループ"] = "本命"
        c["タイプ"]   = f"本命{SUB[i]}"

    # ── 対抗 (5〜8位) ────────────────────────────────────────────
    taiko = candidates[4:8]
    for i, c in enumerate(taiko):
        c["グループ"] = "対抗"
        c["タイプ"]   = f"対抗{SUB[i]}"

    # ── 穴 (期待値ベース選定) 【v3改良】 ─────────────────────────
    # オッズがある場合: 期待値が最も高い買い目を優先
    # オッズがない場合: 従来の1着艇多様化ロジック
    top8 = honmei + taiko
    top8_combos = {c["買い目"] for c in top8}
    ana_min_prob = W.get("ana_min_prob", 0.5)  # 穴候補の的中確率下限(%)
    ana_max_fair_odds = W.get("ana_max_fair_odds", 80)  # 公正オッズ上限
    rest = [c for c in candidates[8:]
            if c["買い目"] not in top8_combos
            and c["的中確率"] >= ana_min_prob
            and c["公正オッズ"] <= ana_max_fair_odds]

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
        top8_first = {c["1着艇"] for c in top8}
        ana_diff = [c for c in rest if c["1着艇"] not in top8_first]
        ana_same = [c for c in rest if c["1着艇"] in top8_first]
        ana_pool = ana_diff + ana_same

    ana = ana_pool[:4]
    for i, c in enumerate(ana):
        c["グループ"] = "穴"
        c["タイプ"]   = f"穴{SUB[i]}"

    selected = honmei + taiko + ana
    return selected, candidates


# ────────────────────────────────────────────────────────────────
# 2連単推奨買い目生成 【v3新規】
# ────────────────────────────────────────────────────────────────

def generate_2ren_recommendations(
    df_scored: pd.DataFrame,
    odds_2t: dict | None = None,
    course_positions: list[int] | None = None,
    racer_kimarite: dict | None = None,
    venue_code: str = DEFAULT_VENUE,
) -> dict:
    """
    2連単の推奨買い目を生成する。
    【v6】決まり手連動着順補正を適用。

    Parameters
    ----------
    odds_2t : 2連単実オッズ {"1-2": 10.6, ...} or None
    """
    W = VENUE_CONFIGS[venue_code]["score_weights"]
    if df_scored.empty or "win_prob" not in df_scored.columns:
        return {"2連単": []}

    if "_raw_prob" in df_scored.columns:
        ability = df_scored["_raw_prob"].values / 100.0
    else:
        ability = df_scored["win_prob"].values / 100.0
    frames = df_scored["枠番"].astype(str).values
    n      = len(ability)
    total  = ability.sum() + 1e-12
    gamma  = W.get("henery_gamma", 0.85)

    # 【v6】各勝者ごとの決まり手連動補正済み ability^γ を事前計算
    cp = course_positions or list(range(n))
    adj_cache = {}
    for i in range(n):
        adj_cache[i] = _adjusted_ability_for_winner(
            ability, i, gamma, cp, racer_kimarite, frames,
            venue_code=venue_code,
        )

    # 2連単: Heneryモデルで P(i=1着, j=2着)
    nitan = []
    for i in range(n):
        p1 = ability[i] / total
        a_adj = adj_cache[i]
        rem = sum(a_adj[m] for m in range(n) if m != i) + 1e-12
        for j in range(n):
            if j == i:
                continue
            p2_cond = a_adj[j] / rem
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

    return {
        "2連単": nitan[:5],
    }


# ────────────────────────────────────────────────────────────────
# 展開予想（1マーク）
# ────────────────────────────────────────────────────────────────

_CIRCLED = ["①", "②", "③", "④", "⑤", "⑥"]


def generate_tenkai_prediction(
    df_scored: pd.DataFrame,
    weather: dict,
    racer_kimarite: dict | None = None,
    race_no: int = 1,
    venue_code: str = DEFAULT_VENUE,
) -> list[dict]:
    """1マーク旋回時の展開シナリオを2〜4パターン生成する。"""
    G, _cs, W, _jycd, _jyname = get_venue_params(venue_code)
    n = len(df_scored)
    if n < 6:
        return []

    # ── データ抽出 ──────────────────────────────────────────────
    course_positions = df_scored["_course_pos"].tolist() if "_course_pos" in df_scored.columns else list(range(n))
    frames     = df_scored["枠番"].astype(str).values
    names      = df_scored["選手名"].values if "選手名" in df_scored.columns else [""] * n
    win_probs  = df_scored["_raw_prob"].values if "_raw_prob" in df_scored.columns else df_scored["win_prob"].values
    st_raw     = df_scored["スタートタイミング"].values if "スタートタイミング" in df_scored.columns else [None] * n
    tilts      = np.array([_to_float(v) for v in df_scored["チルト"].values])
    mawari     = np.array([_to_float(v) for v in (df_scored["まわり足タイム"].values if "まわり足タイム" in df_scored.columns else [0]*n)])
    chokusen   = np.array([_to_float(v) for v in (df_scored["直線タイム"].values if "直線タイム" in df_scored.columns else [0]*n)])

    # ST値のパース
    st_vals = np.full(n, np.nan)
    for i, v in enumerate(st_raw):
        try:
            if v is not None:
                st_vals[i] = float(v)
        except (TypeError, ValueError):
            pass
    valid_sts = st_vals[~np.isnan(st_vals) & (st_vals >= 0)]
    mean_st = valid_sts.mean() if len(valid_sts) >= 2 else None

    # コース→DataFrame行インデックスのマッピング
    course_to_idx = {}
    for i in range(n):
        course_to_idx[course_positions[i]] = i

    # 気象情報
    wind_dir   = str(weather.get("風向", "-"))
    wind_speed = _parse_wind_speed(weather.get("風速", 0))
    is_stabilizer = weather.get("安定板", False)
    is_final   = weather.get("is_final", False)
    grade      = weather.get("grade", "一般")

    # まわり足・直線のランキング（0=最速）
    valid_mw = mawari[mawari > 0]
    valid_ch = chokusen[chokusen > 0]

    def _mawari_rank(idx):
        if mawari[idx] <= 0 or len(valid_mw) < 2:
            return 99
        return int(sum(1 for j in range(n) if mawari[j] > 0 and mawari[j] < mawari[idx]))

    def _chokusen_rank(idx):
        if chokusen[idx] <= 0 or len(valid_ch) < 2:
            return 99
        return int(sum(1 for j in range(n) if chokusen[j] > 0 and chokusen[j] < chokusen[idx]))

    # ── イン逃げ確率 ────────────────────────────────────────────
    c1_idx = course_to_idx.get(0)
    if c1_idx is None:
        return []

    nige_score = _get_course_win_rate(venue_code)[0]  # 当地の基礎イン逃げ率

    # ST優位性
    if mean_st is not None and not np.isnan(st_vals[c1_idx]):
        c1_st = st_vals[c1_idx]
        st_adv = (mean_st - c1_st)  # 正=平均より速い
        nige_score += st_adv * W.get("tenkai_st_factor", 2.0)

    # 風向効果
    wind_eff = _get_wind_effect(venue_code).get(wind_dir, {}).get(0, 0.0)
    wind_mult = min(wind_speed / 3.0, 2.0)
    nige_score += wind_eff * wind_mult * W.get("tenkai_wind_factor", 0.05)

    # 安定板
    if is_stabilizer:
        nige_score += W.get("tenkai_stabilizer_boost", 0.08)

    # 選手の逃げ率 / 脆弱性
    c1_frame = frames[c1_idx]
    c1_rk = (racer_kimarite or {}).get(c1_frame, {})
    c1_nige_pct = c1_rk.get("逃げ", 55.0) / 100.0
    nige_score *= (0.6 + 0.4 * c1_nige_pct / 0.555)

    c1_vulnerability = (c1_rk.get("差され", 0) + c1_rk.get("捲られ", 0) + c1_rk.get("捲られ差", 0)) / 100.0
    nige_score *= max(0.5, 1.0 - c1_vulnerability * 0.3)

    # 優勝戦 / グレード / ナイター
    if is_final:
        nige_score += 0.05
    if grade in ("SG", "G1"):
        nige_score -= 0.03
    if race_no >= G["night_race_start"]:
        nige_score += 0.02

    nige_score = max(0.15, min(0.85, nige_score))

    nige_factors = []
    if mean_st is not None and not np.isnan(st_vals[c1_idx]):
        diff = mean_st - st_vals[c1_idx]
        if diff > 0.02:
            nige_factors.append(f"ST速い({st_vals[c1_idx]:.2f}s)")
        elif diff < -0.02:
            nige_factors.append(f"ST遅め({st_vals[c1_idx]:.2f}s)")
    if wind_eff > 0 and wind_speed >= 2:
        nige_factors.append(f"{wind_dir}→イン有利")
    elif wind_eff < 0 and wind_speed >= 2:
        nige_factors.append(f"{wind_dir}→イン不利")
    if is_stabilizer:
        nige_factors.append("安定板→イン有利")
    if c1_nige_pct >= 0.60:
        nige_factors.append(f"逃げ率{c1_nige_pct*100:.0f}%")
    if is_final:
        nige_factors.append("優勝戦→イン強化")

    # ── 外枠攻めシナリオ ─────────────────────────────────────────
    attack_scenarios = []
    kw = W.get("tenkai_kimarite_weight", 0.4)
    ww = W.get("tenkai_winprob_weight", 0.3)

    for cp in range(1, 6):  # コース2〜6
        idx = course_to_idx.get(cp)
        if idx is None:
            continue
        frame = frames[idx]
        name = str(names[idx]).split()[0] if names[idx] else ""
        rk = (racer_kimarite or {}).get(frame, {})
        _wp_raw = win_probs[idx]
        boat_wp = 0.0 if (np.isnan(_wp_raw) if isinstance(_wp_raw, float) else False) else _wp_raw / 100.0

        # ── 差し ──
        if cp in [1, 2, 3]:  # コース2,3,4
            sashi_base = rk.get("差し", 0.0) / 100.0
            sashi_score = sashi_base * kw + boat_wp * ww
            if _mawari_rank(idx) <= 1:
                sashi_score += 0.05
            if cp == 1:  # コース2は差しの本場
                sashi_score *= 1.3
            # 1号艇のSTが遅いと差しチャンス
            if mean_st is not None and not np.isnan(st_vals[c1_idx]):
                if st_vals[c1_idx] > mean_st + 0.01:
                    sashi_score *= 1.2
            # 横風→コース2差し有利
            _wt_tenkai = _get_wind_effect(venue_code)
            _w_in = _wt_tenkai.get(wind_dir, {}).get(0, 0.0)
            if cp == 1 and abs(_w_in) <= 0.5 and _wt_tenkai.get(wind_dir, {}).get(1, 0.0) > 0 and wind_speed >= 2:
                sashi_score *= 1.15

            if sashi_score > 0.02:
                factors = []
                if sashi_base >= 0.20:
                    factors.append(f"差し率{sashi_base*100:.0f}%")
                if _mawari_rank(idx) <= 1:
                    factors.append("まわり足上位")
                if mean_st is not None and not np.isnan(st_vals[c1_idx]) and st_vals[c1_idx] > mean_st + 0.01:
                    factors.append("イン凹み")
                attack_scenarios.append({
                    "type": "差し", "course": cp + 1, "idx": idx,
                    "frame": frame, "name": name, "score": sashi_score,
                    "factors": factors,
                })

        # ── まくり ──
        if cp >= 2:  # コース3〜6
            makuri_base = rk.get("まくり", 0.0) / 100.0
            makuri_score = makuri_base * kw + boat_wp * (ww * 0.7)
            if _chokusen_rank(idx) <= 1:
                makuri_score += 0.06
            if not np.isnan(st_vals[idx]) if idx < len(st_vals) else False:
                if mean_st is not None and (mean_st - st_vals[idx]) > 0.02:
                    makuri_score *= 1.2
            if tilts[idx] < -0.5:
                makuri_score *= 1.15
            _w_in_makuri = _get_wind_effect(venue_code).get(wind_dir, {}).get(0, 0.0)
            if _w_in_makuri < -0.5 and wind_speed >= 2:
                makuri_score *= 1.1
            if cp == 3:  # カドまくり
                makuri_score *= 1.2

            if makuri_score > 0.02:
                factors = []
                if makuri_base >= 0.15:
                    factors.append(f"まくり率{makuri_base*100:.0f}%")
                if _chokusen_rank(idx) <= 1:
                    factors.append("直線上位")
                if tilts[idx] < -0.5:
                    factors.append(f"チルト{tilts[idx]:.1f}")
                if cp == 3:
                    factors.append("カド位置")
                attack_scenarios.append({
                    "type": "まくり", "course": cp + 1, "idx": idx,
                    "frame": frame, "name": name, "score": makuri_score,
                    "factors": factors,
                })

        # ── まくり差し ──
        if cp >= 2:  # コース3〜6
            ms_base = rk.get("まくり差し", 0.0) / 100.0
            ms_score = ms_base * kw + boat_wp * (ww * 0.7)
            if _mawari_rank(idx) <= 1:
                ms_score += 0.07
            if cp in [2, 4]:  # コース3, 5がまくり差し好位置
                ms_score *= 1.15

            if ms_score > 0.02:
                factors = []
                if ms_base >= 0.15:
                    factors.append(f"まくり差し率{ms_base*100:.0f}%")
                if _mawari_rank(idx) <= 1:
                    factors.append("まわり足上位")
                attack_scenarios.append({
                    "type": "まくり差し", "course": cp + 1, "idx": idx,
                    "frame": frame, "name": name, "score": ms_score,
                    "factors": factors,
                })

    # ── 全シナリオ統合・正規化 ────────────────────────────────────
    c1_name = str(names[c1_idx]).split()[0] if names[c1_idx] else ""

    # attack_scenarios が空の場合、閾値なしで全候補から最良を選ぶ
    if not attack_scenarios:
        _all_candidates = []
        for cp in range(1, 6):
            idx = course_to_idx.get(cp)
            if idx is None:
                continue
            frame = frames[idx]
            name = str(names[idx]).split()[0] if names[idx] else ""
            rk = (racer_kimarite or {}).get(frame, {})
            _wp = win_probs[idx]
            boat_wp = 0.0 if (np.isnan(_wp) if isinstance(_wp, float) else False) else _wp / 100.0

            # コースに応じた代表的な決まり手でスコア計算
            if cp in [1, 2, 3]:
                sashi_base = rk.get("差し", 0.0) / 100.0
                score = sashi_base * kw + boat_wp * ww
                if np.isnan(score) or score < 0.001:
                    score = 0.001
                _all_candidates.append({
                    "type": "差し", "course": cp + 1, "idx": idx,
                    "frame": frame, "name": name, "score": score,
                    "factors": [],
                })
            if cp >= 2:
                makuri_base = rk.get("まくり", 0.0) / 100.0
                score = makuri_base * kw + boat_wp * (ww * 0.7)
                if np.isnan(score) or score < 0.001:
                    score = 0.001
                _all_candidates.append({
                    "type": "まくり", "course": cp + 1, "idx": idx,
                    "frame": frame, "name": name, "score": score,
                    "factors": [],
                })
        if _all_candidates:
            _all_candidates.sort(key=lambda x: x["score"], reverse=True)
            # 上位2件をフォールバックとして追加
            attack_scenarios = _all_candidates[:2]

    all_scenarios = [{
        "type": "イン逃げ", "course": 1, "frame": c1_frame,
        "name": c1_name, "score": nige_score, "factors": nige_factors,
    }]
    all_scenarios.extend(attack_scenarios)

    total = sum(s["score"] for s in all_scenarios)
    if total <= 0:
        return []
    for s in all_scenarios:
        s["probability"] = s["score"] / total

    all_scenarios.sort(key=lambda x: x["probability"], reverse=True)

    # 上位2〜4件を選択（最低2件は必ず含める）
    min_prob = W.get("tenkai_min_scenario_prob", 0.08)
    result = all_scenarios[:2]
    for s in all_scenarios[2:4]:
        if s["probability"] >= min_prob:
            result.append(s)

    # ── テキスト生成 ──────────────────────────────────────────────
    # 最強チャレンジャー（イン逃げ以外で最高確率）
    top_challenger = None
    for s in all_scenarios:
        if s["type"] != "イン逃げ":
            top_challenger = s
            break

    for s in result:
        prob = s["probability"]
        # 信頼度
        if prob >= 0.40:
            s["confidence"] = 3
        elif prob >= 0.20:
            s["confidence"] = 2
        else:
            s["confidence"] = 1

        c_mark = _CIRCLED[int(s["frame"]) - 1]
        c1_mark = _CIRCLED[int(c1_frame) - 1]

        # タイトル
        if s["type"] == "イン逃げ":
            if prob >= 0.50:
                s["title"] = "イン逃げ (鉄板)"
            elif prob >= 0.35:
                s["title"] = "イン逃げ (本線)"
            else:
                s["title"] = "イン逃げ (やや不安)"
        else:
            s["title"] = f"{c_mark}{s['name']}の{s['type']}"

        # 展開の流れテキスト
        if s["type"] == "イン逃げ":
            if not top_challenger or nige_score >= 0.55:
                s["flow"] = f"{c1_mark}{c1_name}がスタート決めて先マイ。そのまま押し切る鉄板レース"
            else:
                ch = top_challenger
                ch_mark = _CIRCLED[int(ch["frame"]) - 1]
                if ch["type"] == "まくり":
                    s["flow"] = f"{c1_mark}{c1_name}が先マイ。{ch_mark}{ch['name']}がまくりに来るが{c1_mark}粘って逃げ切り"
                elif ch["type"] == "差し":
                    s["flow"] = f"{c1_mark}{c1_name}がスタート決めて逃げ。{ch_mark}{ch['name']}が差しを狙うも届かず"
                else:
                    s["flow"] = f"{c1_mark}{c1_name}が先マイ。{ch_mark}{ch['name']}のまくり差しを振り切る"
        elif s["type"] == "差し":
            s["flow"] = f"{c1_mark}がやや遅れ、{c_mark}{s['name']}が内を差して浮上。{c1_mark}は2着争いへ"
        elif s["type"] == "まくり":
            s["flow"] = f"{c_mark}{s['name']}がスタート決めてまくり一撃。{c1_mark}は抵抗するも外から飲み込まれる"
        elif s["type"] == "まくり差し":
            s["flow"] = f"{c_mark}{s['name']}がまくり差しで{c1_mark}の内を突く。間を割って先頭に立つ"

        s["winner_frame"] = s["frame"]
        s["key_factors"] = s.get("factors", [])[:4]

    # 不要キーを除去
    for s in result:
        for k in ("score", "idx", "factors"):
            s.pop(k, None)

    return result


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
    racer_kimarite: dict | None = None,
    deadline: str | None = None,
    venue_code: str = DEFAULT_VENUE,
) -> dict:
    # 会場固有パラメータをローカルに取得
    G, _cs, W, _jycd, _jyname = get_venue_params(venue_code)

    # 締め切り前後1時間以内でなければ気象条件をニュートラル化
    weather_reliable = _is_weather_reliable(deadline)
    scoring_weather = weather if weather_reliable else _neutralize_weather(weather)

    scored = calculate_scores(df, scoring_weather, race_no, taka_data=taka_data, racer_kimarite=racer_kimarite, venue_code=venue_code)

    # 【v6】進入コース情報を取得し、決まり手連動補正に渡す
    course_positions = scored["_course_pos"].tolist() if "_course_pos" in scored.columns else None

    recs, all_3t = generate_recommendations(
        scored, odds_dict=odds_dict,
        course_positions=course_positions,
        racer_kimarite=racer_kimarite,
        venue_code=venue_code,
    )
    recs_2ren = generate_2ren_recommendations(
        scored, odds_2t=odds_2t,
        course_positions=course_positions,
        racer_kimarite=racer_kimarite,
        venue_code=venue_code,
    )

    # 【v10】展開予想シナリオ生成
    tenkai_scenarios = generate_tenkai_prediction(
        scored, scoring_weather,
        racer_kimarite=racer_kimarite,
        race_no=race_no,
        venue_code=venue_code,
    )

    top_boat   = scored.sort_values("win_prob", ascending=False).iloc[0]
    wind_speed = _parse_wind_speed(weather.get("風速", "?"))
    is_night   = race_no >= G["night_race_start"]
    confidence = scored["confidence"].iloc[0] if "confidence" in scored.columns else "-"
    grade      = weather.get("grade", "一般")

    weather_note = "" if weather_reliable else "⏰ 気象データ未反映（締切1時間以上前）"
    is_stabilizer = weather.get("安定板", False)
    summary_lines = [
        f"📍 {_jyname} {race_no}R 予想サマリ",
        f"🏆 グレード: {grade}" + (" / 優勝戦" if weather.get("is_final") else ""),
        f"💨 風速: {weather.get('風速','?')} / 風向: {weather.get('風向','?')} / 天気: {weather.get('天気','?')}"
        + (f"  ({weather_note})" if weather_note else ""),
        f"🌡 気温: {weather.get('気温','?')} / 水温: {weather.get('水温','?')} / 波高: {weather.get('波高','?')}",
        f"{'🌙 ナイターレース' if is_night else '☀️ デイレース'}"
        + (" / ⚠️ 安定板使用（イン有利・展示信頼性低下）" if is_stabilizer else ""),
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
        "all_3t_candidates":  all_3t,
        "recommendations_2ren": recs_2ren,
        "tenkai_scenarios":   tenkai_scenarios,
        "summary":            "\n".join(summary_lines),
    }
