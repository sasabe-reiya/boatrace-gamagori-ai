# 競艇場コード
JYCD = "07"  # 蒲郡
JYNAME = "蒲郡（ナイター）"

# boatrace.jp ベースURL
BASE_URL = "https://www.boatrace.jp"
RACE_CARD_URL = f"{BASE_URL}/owpc/pc/race/racelist"
BEFORE_INFO_URL = f"{BASE_URL}/owpc/pc/race/beforeinfo"

# 蒲郡専用ロジック定数
GAMAGORI_SETTINGS = {
    "night_race_start":       9,
    "calm_wind_threshold":    3.0,
    "base_kado_rate":         18.5,
    "base_makuri_sashi_rate": 14.2,
    "night_nige_boost":       1.08,
}

# スコアリング重みパラメータ（全キーを1つにまとめる）
SCORE_WEIGHTS = {
    "course_base":       [35, 18, 13, 11, 9, 7],  # 1〜6コースの基礎確率(%)
    "win_rate":          0.6,    # 全国勝率の重み
    "local_win_rate":    1.4,    # 蒲郡勝率の重み
    "nat2_rate":         1.5,    # 全国2連率の重み
    "loc2_rate":         2.5,    # 蒲郡2連率の重み
    "exhibit_time":      2.0,    # 展示タイム偏差の重み
    "exhibit_top_bonus": 2.0,    # 展示1位ボーナス
    "night_boost":       4.0,    # ナイター補正加点（1号艇）
    "calm_in_boost":     3.5,    # 風弱→イン加点
    "kado_boost":        2.5,    # カドまくり補正（4号艇）
    "makuri_sashi":      1.8,    # まくり差し補正（3号艇）
    "st_weight":        25.0,    # STの重み（0.01秒差 = 0.25点）
    "st_fly_penalty":    5.0,    # フライング(-ST)ペナルティ
    "motor2_rate":       3.0,    # モーター2連率の重み
    "boat2_rate":        1.5,    # ボート2連率の重み
    "taka_boost":        3.0,    # 高橋アナ予想ブースト（1着予想に加点）
    # ── v3 追加パラメータ ──
    "fl_f_penalty":      5.0,    # F持ちペナルティ（スタート慎重化）
    "fl_l_penalty":      3.0,    # L持ちペナルティ
    "weight_calm":       1.5,    # 体重軽い選手の静水面ボーナス
    "weight_rough":      1.5,    # 体重重い選手の荒天ボーナス
    "course_win_rate":   3.5,    # コース別1着率の重み
    "momentum":          2.5,    # 直近成績モメンタムの重み
    "lap_time":          1.5,    # 一周タイムの重み
    "mawari_time":       2.0,    # まわり足タイムの重み（ターン力）
    "chokusen_time":     1.5,    # 直線タイムの重み（伸び足）
    "turn_master_bonus": 1.5,    # ターン巧者ボーナスの重み
    "grade_final_boost": 2.0,    # 優勝戦イン強化
    "henery_gamma":      0.85,   # Heneryモデルγ（<1 で2,3着均等化）
    "ev_threshold":      1.0,    # 期待値閾値（穴買い目選定用）
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}
