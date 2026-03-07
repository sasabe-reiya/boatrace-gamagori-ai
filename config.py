# ── 会場設定 ──────────────────────────────────────────────────────
# 対応会場を VENUE_CONFIGS に定義。アクティブ会場は set_venue() で切り替える。

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
    "course_win_rate":  2.915,
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
#  蒲郡（ナイター）
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


# ── 会場マスタ ──────────────────────────────────────────────────
VENUE_CONFIGS = {
    "07": {
        "code": "07",
        "name": "蒲郡（ナイター）",
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
    "04": {
        "code": "04",
        "name": "住之江",
        "short_name": "住之江",
        "en_name": "SUMINOE BOATRACE",
        "settings": _SUMINOE_SETTINGS,
        "course_stats": _SUMINOE_COURSE_STATS,
        "score_weights": _SUMINOE_SCORE_WEIGHTS,
        "has_original_exhibit": False,
        "has_taka_yoso": False,
        "has_iot_weather": False,
        "official_site": None,
    },
}

# ── アクティブ会場（後方互換用グローバル変数）─────────────────────
# app.py から set_venue() で切り替え。デフォルトは蒲郡。
JYCD = "07"
JYNAME = "蒲郡（ナイター）"
GAMAGORI_SETTINGS = _GAMAGORI_SETTINGS
GAMAGORI_COURSE_STATS = _GAMAGORI_COURSE_STATS
SCORE_WEIGHTS = dict(_GAMAGORI_SCORE_WEIGHTS)

def set_venue(jycd: str):
    """アクティブ会場を切り替え、グローバル変数を更新する。"""
    global JYCD, JYNAME, GAMAGORI_SETTINGS, GAMAGORI_COURSE_STATS, SCORE_WEIGHTS
    cfg = VENUE_CONFIGS.get(jycd)
    if cfg is None:
        raise ValueError(f"未対応の会場コード: {jycd}")
    JYCD = cfg["code"]
    JYNAME = cfg["name"]
    GAMAGORI_SETTINGS = cfg["settings"]
    GAMAGORI_COURSE_STATS = cfg["course_stats"]
    SCORE_WEIGHTS = dict(cfg["score_weights"])


def get_venue_config(jycd: str | None = None) -> dict:
    """指定会場（省略時はアクティブ会場）の設定辞書を返す。"""
    code = jycd or JYCD
    return VENUE_CONFIGS[code]
