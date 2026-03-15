from __future__ import annotations

# ── 会場設定 ──────────────────────────────────────────────────────
# 対応会場を VENUE_CONFIGS に定義。会場設定は get_venue_params() で取得する。

# boatrace.jp ベースURL
BASE_URL = "https://www.boatrace.jp"
RACE_CARD_URL = f"{BASE_URL}/owpc/pc/race/racelist"
BEFORE_INFO_URL = f"{BASE_URL}/owpc/pc/race/beforeinfo"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}

# ── 会場共通のスコアリング重みテンプレート ──────────────────────────
# 各会場で上書き可能。上書きしないキーはこのデフォルト値が使われる。
_SCORE_WEIGHTS_BASE = {
    "course_base":       [56.4, 10.5, 13.8, 12.1, 5.2, 1.9],
    "win_rate":         1.5,
    "local_win_rate":   0.858,
    "nat2_rate":        3.5,
    "loc2_rate":        0.481,
    "exhibit_time":     0.745,
    "exhibit_top_bonus": 2.5,
    "night_boost":      0.0,
    "calm_in_boost":    0.0,
    "kado_boost":       0.747,
    "makuri_sashi":     2.5,
    "st_weight":        36.216,
    "st_fly_penalty":    5.0,
    "motor2_rate":      3.0,
    "boat2_rate":       0.872,
    "taka_boost":       1.62,
    "fl_f_penalty":     2.677,
    "fl_l_penalty":      3.0,
    "weight_calm":      0.607,
    "weight_rough":      1.5,
    "course_win_rate":  2.0,
    "momentum":         2.136,
    "lap_time":         0.0,
    "mawari_time":      1.2,
    "chokusen_time":    1.5,
    "turn_master_bonus": 2.0,
    "grade_final_boost": 2.0,
    "henery_gamma":      0.85,
    "ev_threshold":      1.3,
    "ana_min_prob":      1.0,
    "ana_max_fair_odds": 80,
    "racer_kimarite_weight": 3.0,
    "individual_temp": 5.0,
    "prob_cap": 70.0,
    "kimarite_placement_weight": 0.5,
    "stabilizer_in_boost":    3.0,
    "stabilizer_equalize":    0.15,
    "stabilizer_et_discount": 0.6,
    "nikkan_boost":     0.0,   # 日刊スポーツ記者予想ブースト（尼崎のみ有効）
    "rank_a1":          3.0,
    "rank_a2":          1.2,
    "rank_b1":          0.0,
    "rank_b2":         -1.5,
    "tenkai_st_factor":          2.0,
    "tenkai_wind_factor":        0.05,
    "tenkai_stabilizer_boost":   0.08,
    "tenkai_kimarite_weight":    0.4,
    "tenkai_winprob_weight":     0.3,
    "tenkai_min_scenario_prob":  0.08,
}

# ══════════════════════════════════════════════════════════════════
#  蒲郡
# ══════════════════════════════════════════════════════════════════
_GAMAGORI_SETTINGS = {
    "night_race_start":       9,
    "calm_wind_threshold":    2.5,
    "base_kado_rate":         18.5,
    "base_makuri_sashi_rate": 14.2,
    "night_nige_boost":       1.08,
}

_GAMAGORI_COURSE_STATS = {
    1: {"1着": 56.4, "2着": 16.1, "3着":  9.2, "4着":  7.7, "5着":  5.0, "6着":  4.3},
    2: {"1着": 10.5, "2着": 24.2, "3着": 19.7, "4着": 16.0, "5着": 16.1, "6着": 12.0},
    3: {"1着": 13.8, "2着": 22.5, "3着": 17.7, "4着": 18.0, "5着": 14.6, "6着": 12.1},
    4: {"1着": 12.1, "2着": 18.1, "3着": 21.5, "4着": 19.5, "5着": 16.2, "6着": 11.4},
    5: {"1着":  5.2, "2着": 14.3, "3着": 19.4, "4着": 20.5, "5着": 23.5, "6着": 15.7},
    6: {"1着":  1.9, "2着":  4.9, "3着": 12.8, "4着": 18.2, "5着": 23.7, "6着": 36.9},
}

_GAMAGORI_SCORE_WEIGHTS = dict(_SCORE_WEIGHTS_BASE)
# 蒲郡固有の上書きがあればここで
# _GAMAGORI_SCORE_WEIGHTS["taka_boost"] = 1.62  # 高橋アナ予想は蒲郡のみ

# ══════════════════════════════════════════════════════════════════
#  住之江（デイ）
# ══════════════════════════════════════════════════════════════════
_SUMINOE_SETTINGS = {
    "night_race_start":       99,   # デイレースなのでナイター補正なし
    "calm_wind_threshold":    2.5,
    "base_kado_rate":         17.0,
    "base_makuri_sashi_rate": 13.5,
    "night_nige_boost":       1.0,  # ナイター補正なし
}

# 住之江 進入コース別成績（2023.12〜2024.12 集計 ※公式データに基づく概算値）
_SUMINOE_COURSE_STATS = {
    1: {"1着": 54.5, "2着": 17.2, "3着":  9.5, "4着":  7.8, "5着":  5.5, "6着":  4.5},
    2: {"1着": 12.3, "2着": 23.0, "3着": 18.5, "4着": 16.5, "5着": 15.8, "6着": 12.9},
    3: {"1着": 12.8, "2着": 21.0, "3着": 18.2, "4着": 18.5, "5着": 15.5, "6着": 13.0},
    4: {"1着": 12.5, "2着": 18.5, "3着": 20.0, "4着": 19.0, "5着": 16.5, "6着": 12.5},
    5: {"1着":  5.8, "2着": 14.8, "3着": 19.0, "4着": 20.0, "5着": 22.5, "6着": 16.9},
    6: {"1着":  2.1, "2着":  5.5, "3着": 14.8, "4着": 18.2, "5着": 24.2, "6着": 34.2},
}

_SUMINOE_SCORE_WEIGHTS = dict(_SCORE_WEIGHTS_BASE)
_SUMINOE_SCORE_WEIGHTS.update({
    "course_base":       [54.5, 12.3, 12.8, 12.5, 5.8, 2.1],  # 住之江1着率
    "taka_boost":        0.0,   # 高橋アナ予想は蒲郡のみ
    "night_boost":       0.0,
    "calm_in_boost":     0.0,
})

# ══════════════════════════════════════════════════════════════════
#  大村（ナイター）
# ══════════════════════════════════════════════════════════════════
_OMURA_SETTINGS = {
    "night_race_start":       9,   # ナイターレース（蒲郡と同様）
    "calm_wind_threshold":    2.5,
    "base_kado_rate":         16.0,   # 大村はイン逃げが強くカド率は低め
    "base_makuri_sashi_rate": 12.0,
    "night_nige_boost":       1.10,   # ナイター＋静水面でイン有利
}

# 大村 進入コース別成績（公式データに基づく概算値）
_OMURA_COURSE_STATS = {
    1: {"1着": 62.0, "2着": 14.5, "3着":  8.0, "4着":  6.5, "5着":  4.5, "6着":  3.5},
    2: {"1着":  9.5, "2着": 22.5, "3着": 19.0, "4着": 17.5, "5着": 17.0, "6着": 13.5},
    3: {"1着": 10.5, "2着": 21.0, "3着": 18.5, "4着": 18.0, "5着": 16.0, "6着": 15.0},
    4: {"1着": 11.0, "2着": 19.0, "3着": 20.5, "4着": 19.0, "5着": 16.5, "6着": 13.0},
    5: {"1着":  5.5, "2着": 15.5, "3着": 19.5, "4着": 20.0, "5着": 22.5, "6着": 16.0},
    6: {"1着":  1.5, "2着":  7.5, "3着": 14.5, "4着": 19.0, "5着": 23.5, "6着": 33.0},
}

_OMURA_SCORE_WEIGHTS = dict(_SCORE_WEIGHTS_BASE)
_OMURA_SCORE_WEIGHTS.update({
    "course_base":       [62.0, 9.5, 10.5, 11.0, 5.5, 1.5],  # 大村1着率
    "taka_boost":        0.0,   # 高橋アナ予想は蒲郡のみ
    # ── 大村チューニング結果 (155レース, 2026-01-27〜2026-03-03) ──
    "individual_temp":   6.073,
    "win_rate":          1.597,
    "local_win_rate":    0.768,
    "nat2_rate":         3.93,
    "loc2_rate":         0.451,
    "exhibit_time":      0.86,
    "exhibit_top_bonus": 3.185,
    "night_boost":       0.0,
    "calm_in_boost":     0.0,
    "st_weight":         31.074,
    "motor2_rate":       3.289,
    "boat2_rate":        0.871,
    "course_win_rate":   1.914,
    "momentum":          2.047,
    "mawari_time":       1.147,
    "chokusen_time":     1.43,
    "turn_master_bonus": 1.925,
    "fl_f_penalty":      2.577,
    "kado_boost":        0.715,
    "makuri_sashi":      2.353,
    "rank_a1":           1.788,
    "rank_a2":           1.425,
    "stabilizer_in_boost": 3.421,
    "stabilizer_equalize": 0.139,
    "prob_cap":          70.0,
})

# ══════════════════════════════════════════════════════════════════
#  尼崎（デイ）
# ══════════════════════════════════════════════════════════════════
_AMAGASAKI_SETTINGS = {
    "night_race_start":       99,   # デイレースなのでナイター補正なし
    "calm_wind_threshold":    2.5,
    "base_kado_rate":         17.5,
    "base_makuri_sashi_rate": 13.0,
    "night_nige_boost":       1.0,  # ナイター補正なし
}

# 尼崎 進入コース別成績（2025.12〜2026.02 集計 ※公式サイトに基づく）
_AMAGASAKI_COURSE_STATS = {
    1: {"1着": 61.8, "2着": 14.1, "3着":  9.0, "4着":  6.8, "5着":  4.6, "6着":  3.5},
    2: {"1着": 10.1, "2着": 27.8, "3着": 17.6, "4着": 15.9, "5着": 17.9, "6着": 10.4},
    3: {"1着": 10.0, "2着": 23.0, "3着": 21.0, "4着": 20.3, "5着": 12.1, "6着": 13.4},
    4: {"1着": 11.2, "2着": 14.8, "3着": 21.0, "4着": 20.7, "5着": 18.3, "6着": 13.6},
    5: {"1着":  5.8, "2着": 16.9, "3着": 19.9, "4着": 17.0, "5着": 22.1, "6着": 18.1},
    6: {"1着":  2.1, "2着":  4.8, "3着": 12.8, "4着": 20.4, "5着": 24.8, "6着": 34.8},
}

_AMAGASAKI_SCORE_WEIGHTS = dict(_SCORE_WEIGHTS_BASE)
_AMAGASAKI_SCORE_WEIGHTS.update({
    "course_base":       [61.8, 10.1, 10.0, 11.2, 5.8, 2.1],  # 尼崎1着率
    "taka_boost":        0.0,   # 高橋アナ予想は蒲郡のみ
    # ── 尼崎チューニング結果 (263レース, 2026-02-13〜2026-03-14) ──
    "individual_temp":   7.095,
    "win_rate":          1.503,
    "local_win_rate":    0.84,
    "nat2_rate":         3.433,
    "loc2_rate":         0.477,
    "exhibit_time":      0.746,
    "exhibit_top_bonus": 2.497,
    "night_boost":       0.0,
    "calm_in_boost":     0.0,
    "st_weight":         36.806,
    "motor2_rate":       2.993,
    "boat2_rate":        0.883,
    "course_win_rate":   1.999,
    "momentum":          2.146,
    "mawari_time":       1.198,
    "chokusen_time":     1.524,
    "turn_master_bonus": 2.018,
    "fl_f_penalty":      2.735,
    "kado_boost":        0.754,
    "makuri_sashi":      2.508,
    "rank_a1":           2.995,
    "rank_a2":           1.219,
    "stabilizer_in_boost": 2.942,
    "stabilizer_equalize": 0.15,
    "prob_cap":          69.369,
    "nikkan_boost":      2.0,   # 日刊スポーツ記者予想コンピ指数ブースト
})

# ── 会場マスタ ──────────────────────────────────────────────────
VENUE_CONFIGS = {
    "07": {
        "code": "07",
        "name": "蒲郡",
        "short_name": "蒲郡",
        "en_name": "GAMAGORI BOATRACE",
        "settings": _GAMAGORI_SETTINGS,
        "course_stats": _GAMAGORI_COURSE_STATS,
        "score_weights": _GAMAGORI_SCORE_WEIGHTS,
        "has_original_exhibit": True,    # 蒲郡独自展示タイム（一周・まわり足・直線）
        "has_taka_yoso": True,           # 高橋アナ予想
        "has_iot_weather": True,         # IoTリアルタイム気象API
        "official_site": "https://www.gamagori-kyotei.com/asp/gamagori/kyogi/kyogihtml",
    },
    "12": {
        "code": "12",
        "name": "住之江",
        "short_name": "住之江",
        "en_name": "SUMINOE BOATRACE",
        "settings": _SUMINOE_SETTINGS,
        "course_stats": _SUMINOE_COURSE_STATS,
        "score_weights": _SUMINOE_SCORE_WEIGHTS,
        "has_original_exhibit": True,     # 住之江独自展示タイム（一周・まわり足）
        "has_taka_yoso": False,
        "has_iot_weather": False,
        "has_official_weather": True,    # 住之江公式サイト気象データ
        "official_site": "https://www.boatrace-suminoe.jp/asp/suminoe/kyogi/kyogihtml",
    },
    "24": {
        "code": "24",
        "name": "大村",
        "short_name": "大村",
        "en_name": "OMURA BOATRACE",
        "settings": _OMURA_SETTINGS,
        "course_stats": _OMURA_COURSE_STATS,
        "score_weights": _OMURA_SCORE_WEIGHTS,
        "has_original_exhibit": True,     # 大村公式サイト展示タイム (omurakyotei.jp)
        "has_taka_yoso": False,
        "has_iot_weather": False,
        "has_official_weather": True,     # 大村公式サイト気象データ
        "official_site": "https://omurakyotei.jp",
    },
    "13": {
        "code": "13",
        "name": "尼崎",
        "short_name": "尼崎",
        "en_name": "AMAGASAKI BOATRACE",
        "settings": _AMAGASAKI_SETTINGS,
        "course_stats": _AMAGASAKI_COURSE_STATS,
        "score_weights": _AMAGASAKI_SCORE_WEIGHTS,
        "has_original_exhibit": False,    # 独自展示タイムなし（boatrace.jpのデータを使用）
        "has_taka_yoso": False,
        "has_nikkan_yoso": True,          # 日刊スポーツ記者予想（コンピ指数）
        "has_iot_weather": False,
        "has_official_weather": True,     # 尼崎公式サイト気象データ
        "official_site": "https://www.boatrace-amagasaki.jp",
    },
}

# ── デフォルト会場コード ─────────────────────────────────────────
DEFAULT_VENUE = "07"

# ── 後方互換用（backtester / tune_bayes 等の単体スクリプト向け） ──
# Webアプリ（app.py / scorer.py）では使用しない。
JYCD = DEFAULT_VENUE
JYNAME = "蒲郡"
GAMAGORI_SETTINGS = _GAMAGORI_SETTINGS
GAMAGORI_COURSE_STATS = _GAMAGORI_COURSE_STATS
SCORE_WEIGHTS = dict(_GAMAGORI_SCORE_WEIGHTS)


def get_venue_config(jycd: str | None = None) -> dict:
    """指定会場（省略時はデフォルト会場）の設定辞書を返す。"""
    code = jycd or DEFAULT_VENUE
    return VENUE_CONFIGS[code]


def get_venue_params(jycd: str | None = None) -> tuple:
    """会場固有の (settings, course_stats, score_weights, jycd, jyname) を返す。

    グローバル変数を介さずスレッドセーフに会場設定を取得できる。
    """
    code = jycd or DEFAULT_VENUE
    cfg = VENUE_CONFIGS.get(code)
    if cfg is None:
        raise ValueError(f"未対応の会場コード: {code}")
    return (
        cfg["settings"],
        cfg["course_stats"],
        dict(cfg["score_weights"]),
        cfg["code"],
        cfg["name"],
    )
