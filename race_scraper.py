"""
boatrace.jp から蒲郡の出走表・直前情報をスクレイピングするモジュール。
【v3 追加機能】
- F/L回数・登録番号の取得
- 体重・展示周回タイムの取得
- 選手コース別成績・直近成績（拡張データ）の取得
- レースグレード検出
"""
import re
import warnings
import pandas as pd
import requests
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

try:
    from config import BASE_URL, JYCD, HEADERS
except ImportError:
    BASE_URL = "https://www.boatrace.jp"
    JYCD = "07"
    HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

def _get_soup(url: str, params: dict) -> BeautifulSoup | None:
    try:
        resp = requests.get(url, params=params, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        resp.encoding = "utf-8"
        return BeautifulSoup(resp.text, "lxml")
    except Exception as e:
        print(f"[scraper] リクエスト失敗: {e}")
        return None

def _to_float(s):
    if s is None: return None
    clean_s = re.sub(r'[\s\t\n\r,]', '', str(s))
    if not clean_s or clean_s == '-': return None
    if clean_s.startswith('.'): clean_s = '0' + clean_s
    match = re.search(r'[-+]?\d*\.\d+|\d+', clean_s)
    return float(match.group()) if match else None


def _zenkaku_to_frame(s: str) -> str:
    mapping = {"１": "1", "２": "2", "３": "3", "４": "4", "５": "5", "６": "6"}
    return mapping.get(s.strip(), s.strip())



# ─────────────────────────────────────────────
# 1. 出走表取得
# ─────────────────────────────────────────────
def fetch_race_card(race_no: int, date_str: str | None = None) -> pd.DataFrame:
    if date_str is None: date_str = datetime.now().strftime("%Y%m%d")
    url = f"{BASE_URL}/owpc/pc/race/racelist"
    params = {"jcd": JYCD, "hd": date_str, "rno": race_no}
    soup = _get_soup(url, params)
    if not soup: return pd.DataFrame()

    tbody_list = soup.find_all("tbody", class_=re.compile("is-fs12"))
    rows = []

    for tbody in tbody_list:
        frame_td = tbody.find("td", class_=re.compile("is-boatColor"))
        if not frame_td: continue
        frame_no = _zenkaku_to_frame(frame_td.get_text())

        name_div = tbody.find("div", class_=re.compile("is-fs18"))
        player_name = name_div.get_text(strip=True).replace('\u3000', '') if name_div else ""

        # 登録番号: 選手名リンクまたは周辺テキストから取得
        reg_no = None
        if name_div:
            name_link = name_div.find("a")
            if name_link and name_link.get("href"):
                reg_m = re.search(r'toession=(\d+)', name_link["href"])
                if reg_m:
                    reg_no = reg_m.group(1)
        if not reg_no:
            # フォールバック: tbody内の4桁数字で登録番号らしきものを探す
            for a_tag in tbody.find_all("a"):
                href = a_tag.get("href", "")
                reg_m = re.search(r'toession=(\d+)', href)
                if reg_m:
                    reg_no = reg_m.group(1)
                    break

        rank = ""
        rank_info = tbody.find("div", class_=re.compile("is-fs11"))
        if rank_info:
            rank_match = re.search(r'([AB][12])', rank_info.get_text())
            if rank_match: rank = rank_match.group(1)

        # is-lineH2 セルが5つある構造:
        #   [0] F回数|L回数|平均ST
        #   [1] 全国勝率|全国2連率|全国3連率
        #   [2] 蒲郡勝率|蒲郡2連率|蒲郡3連率
        #   [3] モーター番号|モーター2連率|モーター3連率
        #   [4] ボート番号|ボート2連率|ボート3連率
        line_tds = tbody.find_all("td", class_=re.compile("is-lineH2"))
        nw, n2, lw, l2, st = [None]*5
        motor_no, motor2, boat_no, boat2 = None, None, None, None
        f_count, l_count = 0, 0

        if len(line_tds) > 0:
            st_text = line_tds[0].get_text(separator="|")
            parts_fl = [v.strip() for v in st_text.split("|") if v.strip()]
            st = next((_to_float(v) for v in parts_fl if "." in v), None)
            # F/L回数: 整数値を順番に取得（構造: F回数, L回数, 平均ST）
            int_vals_fl = []
            for v in parts_fl:
                if re.fullmatch(r'\d+', v.strip()):
                    int_vals_fl.append(int(v.strip()))
            if len(int_vals_fl) >= 2:
                f_count = int_vals_fl[0]
                l_count = int_vals_fl[1]
            elif len(int_vals_fl) == 1:
                f_count = int_vals_fl[0]
        if len(line_tds) > 1:
            vals = [_to_float(v) for v in line_tds[1].get_text(separator="|").split("|") if _to_float(v) is not None]
            if len(vals) >= 1: nw = vals[0]  # 全国勝率
            if len(vals) >= 2: n2 = vals[1]  # 全国2連率
        if len(line_tds) > 2:
            vals = [_to_float(v) for v in line_tds[2].get_text(separator="|").split("|") if _to_float(v) is not None]
            if len(vals) >= 1: lw = vals[0]  # 蒲郡勝率
            if len(vals) >= 2: l2 = vals[1]  # 蒲郡2連率
        if len(line_tds) > 3:
            vals = [_to_float(v) for v in line_tds[3].get_text(separator="|").split("|") if _to_float(v) is not None]
            if len(vals) >= 1 and vals[0] is not None:
                motor_no = int(vals[0])      # モーター番号
            if len(vals) >= 2: motor2 = vals[1]  # モーター2連率
        if len(line_tds) > 4:
            vals = [_to_float(v) for v in line_tds[4].get_text(separator="|").split("|") if _to_float(v) is not None]
            if len(vals) >= 1 and vals[0] is not None:
                boat_no = int(vals[0])       # ボート番号
            if len(vals) >= 2: boat2 = vals[1]   # ボート2連率

        # ── フォールバック: クラスなし <td> スキャン ─────────────────
        # is-lineH2 のインデックス3/4 が取れなかった場合、
        # 番号(<td>34</td>) + 率(<td>37.80</td>) の連続パターンを探す
        if motor2 is None:
            _SKIP = re.compile(r"is-(?:lineH2|boatColor|fs18|fs11|fs14|fs12)")
            _cands: list[tuple[float, bool]] = []
            for _td in tbody.find_all("td"):
                _cls = " ".join(_td.get("class", []))
                if _td.get("class") and _SKIP.search(_cls):
                    continue
                _t = _td.get_text(strip=True)
                if re.fullmatch(r'\d{1,3}', _t):
                    _v = _to_float(_t)
                    if _v is not None:
                        _cands.append((_v, False))
                elif re.fullmatch(r'\d{1,3}\.\d{1,2}', _t):
                    _v = _to_float(_t)
                    if _v is not None:
                        _cands.append((_v, True))

            _pairs: list[tuple[int, float]] = []
            _ci = 0
            while _ci < len(_cands) - 1:
                v1, d1 = _cands[_ci]
                v2, d2 = _cands[_ci + 1]
                if (not d1 and d2 and 1 <= v1 <= 99 and 0 < v2 <= 100):
                    _pairs.append((int(v1), v2))
                    _ci += 2
                else:
                    _ci += 1

            if len(_pairs) >= 2:
                motor_no, motor2 = _pairs[-2]
                boat_no,  boat2  = _pairs[-1]
            elif len(_pairs) == 1:
                motor_no, motor2 = _pairs[0]

        rows.append({
            "枠番": frame_no, "選手名": player_name, "級別": rank,
            "登録番号": reg_no,
            "F回数": f_count, "L回数": l_count,
            "全国勝率": nw, "全国2連率": n2, "蒲郡勝率": lw, "蒲郡2連率": l2,
            "スタートタイミング": st,
            "モーター番号": motor_no, "モーター2連率": motor2,
            "ボート番号": boat_no,  "ボート2連率":  boat2,
        })
    return pd.DataFrame(rows)


# ─────────────────────────────────────────────
# 2. 直前情報取得 (風向・展示タイム)
# ─────────────────────────────────────────────
def fetch_before_info(race_no: int, date_str: str | None = None) -> tuple[pd.DataFrame, dict]:
    if date_str is None: date_str = datetime.now().strftime("%Y%m%d")
    url = f"{BASE_URL}/owpc/pc/race/beforeinfo"
    params = {"jcd": JYCD, "hd": date_str, "rno": race_no}
    soup = _get_soup(url, params)

    weather = {"天気": "-", "気温": "-", "水温": "-", "風速": "0m", "風向": "-", "波高": "0cm"}
    if not soup: return pd.DataFrame(), weather

    # ── 気象解析 ──────────────────────────────────
    # 実際のHTML構造:
    #   weather1_bodyUnit is-direction       → 気温 (is-direction11 等のpタグを含む)
    #   weather1_bodyUnit is-weather         → 天気 (is-weather1 等のpタグを含む)
    #   weather1_bodyUnit is-wind            → 風速
    #   weather1_bodyUnit is-windDirection   → 風向 (is-wind9 等のpタグを含む)
    #   weather1_bodyUnit is-waterTemperature→ 水温
    #   weather1_bodyUnit is-wave            → 波高
    # データは <span class="weather1_bodyUnitLabelData"> から取得する

    # 方位コンパス is-direction{N} のマッピング
    # 実測: is-direction11 = 北（コンパスのNが左下表示）
    # オフセット+6: (n-1+6) % 16 = (n+5) % 16
    # 検証: n=11 → (10+6)%16=0 → 北 ✓
    #       n=1  → (0+6)%16=6  → 南東
    _DIR_BASE = ['北','北北東','北東','東北東','東','東南東','南東','南南東',
                 '南','南南西','南西','西南西','西','西北西','北西','北北西']
    COMPASS_DIR_MAP = {n: _DIR_BASE[(n - 1 + 6) % 16] for n in range(1, 17)}
    # 確認: {11:北, 12:北北東, 13:北東, ..., 10:北北西}

    WEATHER_MAP = {
        1:"晴れ", 2:"曇り", 3:"雨", 4:"雪"
    }

    def _get_label_data(unit) -> str:
        """weather1_bodyUnitLabelData spanの値を返す"""
        span = unit.find("span", class_="weather1_bodyUnitLabelData")
        return span.get_text(strip=True) if span else ""

    def _get_p_class_number(unit, prefix: str) -> int | None:
        """<p class="is-{prefix}{N}"> のNを返す"""
        p = unit.find("p", class_=re.compile(rf"is-{prefix}\d+"))
        if p:
            m = re.search(rf"is-{prefix}(\d+)", " ".join(p.get("class", [])))
            if m:
                return int(m.group(1))
        return None

    weather_units = soup.find_all("div", class_="weather1_bodyUnit")
    for unit in weather_units:
        classes = unit.get("class", [])

        if "is-direction" in classes:
            # 方位コンパスユニット → 気温データ + 風向を取得
            # ★ 風向: is-direction{N} の番号を COMPASS_DIR_MAP で変換
            n = _get_p_class_number(unit, "direction")
            if n is not None:
                weather["風向"] = COMPASS_DIR_MAP.get(n, "-")
            # 気温: LabelData spanから取得
            weather["気温"] = _get_label_data(unit)

        elif "is-weather" in classes:
            # 天気ユニット: <p class="is-weather1"> の番号で判定
            n = _get_p_class_number(unit, "weather")
            if n is not None:
                weather["天気"] = WEATHER_MAP.get(n, "-")
            else:
                # フォールバック: LabelTitleのテキスト
                title = unit.find("span", class_="weather1_bodyUnitLabelTitle")
                if title:
                    t = title.get_text(strip=True)
                    if "晴" in t: weather["天気"] = "晴れ"
                    elif "曇" in t: weather["天気"] = "曇り"
                    elif "雨" in t: weather["天気"] = "雨"
                    elif "雪" in t: weather["天気"] = "雪"

        elif "is-windDirection" in classes:
            # is-wind17 = 無風（1〜16の範囲外の特別値）→ 風向を"-"に上書き
            # それ以外は方位コンパス(is-direction)を正とするので無視
            p = unit.find("p", class_=re.compile(r"is-wind\d+"))
            if p:
                m = re.search(r"is-wind(\d+)", " ".join(p.get("class", [])))
                if m and int(m.group(1)) == 17:
                    weather["風向"] = "-"

        elif "is-wind" in classes:
            # 風速ユニット（is-windDirection より後に判定すること）
            weather["風速"] = _get_label_data(unit)

        elif "is-waterTemperature" in classes:
            # 水温ユニット
            weather["水温"] = _get_label_data(unit)

        elif "is-wave" in classes:
            # 波高ユニット
            weather["波高"] = _get_label_data(unit)

    # ── 展示タイム解析 ───────────────────────────
    # テーブルの行順 = 進入コース順（1行目=1コース、2行目=2コース…）
    # 実際のtd構造（データ行 10セル）:
    #   [0]枠番 [1]写真 [2]選手名 [3]体重 [4]展示タイム [5]チルト
    #   [6]プロペラ [7]部品交換 [8]前走成績 [9](空)
    # ※ boatrace.jp に周回展示タイムの列は存在しない
    rows = []
    tables = soup.find_all("table")
    for tbl in tables:
        if "展示タイム" in tbl.get_text():
            course_idx = 0  # 進入コース番号（1始まり）
            for tr in tbl.find_all("tr"):
                tds = tr.find_all("td")
                if len(tds) >= 6:
                    f_no = _zenkaku_to_frame(tds[0].get_text())
                    weight = _to_float(tds[3].get_text())
                    et = _to_float(tds[4].get_text())
                    tilt = _to_float(tds[5].get_text())
                    if f_no in ["1", "2", "3", "4", "5", "6"]:
                        course_idx += 1
                        rows.append({
                            "枠番":       f_no,
                            "体重":       weight,
                            "展示タイム":  et,
                            "チルト":     tilt,
                            "進入コース":  course_idx,
                            "周回タイム":  None,
                        })
            break

    return pd.DataFrame(rows), weather


# ─────────────────────────────────────────────
# 3. 統合
# ─────────────────────────────────────────────
def fetch_full_race_data(
    race_no: int,
    date_str: str | None = None,
    extended: bool = False,
) -> tuple[pd.DataFrame, dict]:
    """
    出走表＋直前情報を統合して返す。

    extended=True の場合、選手個別のコース別成績・直近成績・レースグレードも取得する。
    weather dict に "grade", "is_final", "grade_title" キーが追加される。
    DataFrame に "コース別1着率", "直近平均着順", "直近勝率" 列が追加される。
    """
    card_df = fetch_race_card(race_no, date_str)
    if card_df.empty:
        return pd.DataFrame(), {}

    ex_df, weather = fetch_before_info(race_no, date_str)
    if not ex_df.empty:
        final_df = pd.merge(card_df, ex_df, on="枠番", how="left")
    else:
        final_df = card_df
        final_df["展示タイム"] = None
        final_df["チルト"] = 0.0
        final_df["体重"] = None
        final_df["周回タイム"] = None

    # ── 蒲郡公式サイト：オリジナル展示タイム取得 ──────
    gama_time_df = fetch_gamagori_time(race_no, date_str)
    if not gama_time_df.empty:
        final_df = pd.merge(final_df, gama_time_df, on="枠番", how="left")
    else:
        final_df["展示タイム_gama"] = None
        final_df["一周タイム"] = None
        final_df["まわり足タイム"] = None
        final_df["直線タイム"] = None

    # ── レースグレード検出 ────────────────────────
    grade_info = fetch_race_grade(race_no, date_str)
    weather["grade"] = grade_info["grade"]
    weather["is_final"] = grade_info["is_final"]
    weather["grade_title"] = grade_info["title"]

    # ── 拡張データ取得（選手コース別成績・直近成績） ──
    if extended and "登録番号" in final_df.columns:
        reg_nos = final_df["登録番号"].tolist()
        ext_data = fetch_extended_player_data(reg_nos)

        # コース別1着率: 現在の進入コースに対応する1着率を紐付け
        course_win_rates = []
        for _, row in final_df.iterrows():
            rno = row.get("登録番号")
            course = str(int(row["進入コース"])) if pd.notna(row.get("進入コース")) else row["枠番"]
            cs = ext_data["course_stats"].get(rno, {})
            cw = cs.get(course, {}).get("win_rate")
            course_win_rates.append(cw)
        final_df["コース別1着率"] = course_win_rates

        # 直近成績: 平均着順と直近勝率（1着の割合）
        avg_results = []
        recent_win_rates = []
        for _, row in final_df.iterrows():
            rno = row.get("登録番号")
            recent = ext_data["recent_results"].get(rno, [])
            if recent:
                avg_results.append(round(sum(recent) / len(recent), 2))
                recent_win_rates.append(
                    round(sum(1 for r in recent if r == 1) / len(recent) * 100, 1)
                )
            else:
                avg_results.append(None)
                recent_win_rates.append(None)
        final_df["直近平均着順"] = avg_results
        final_df["直近勝率"] = recent_win_rates

    return final_df, weather


# ─────────────────────────────────────────────
# 4. 3連単オッズ取得
# ─────────────────────────────────────────────
def fetch_odds_3t(race_no: int, date_str: str | None = None) -> dict:
    """
    boatrace.jp の odds3t ページから3連単オッズを取得する。

    Returns
    -------
    {"1-2-3": 12.3, "1-2-4": 8.5, ...} の dict。
    締め切り前はリアルタイムオッズ、取得失敗時は空 dict。

    テーブル構造（実測）:
      横型 6カラム構成。ヘッダー行が [1着=1 名前 名前 | 1着=2 名前 名前 | ... | 1着=6 名前 名前]
      データ行は colspan展開後に 18列: グループi (i=0..5) の列 [3*i]=2着, [3*i+1]=3着, [3*i+2]=オッズ
      2着セルは rowspan=4（1つの2着に対して3着が4通り）。
      高倍率(例:1893)は整数表示（小数点なし）になる点に注意。
    """
    if date_str is None:
        date_str = datetime.now().strftime("%Y%m%d")

    url = f"{BASE_URL}/owpc/pc/race/odds3t"
    params = {"jcd": JYCD, "hd": date_str, "rno": race_no}
    soup = _get_soup(url, params)
    if not soup:
        return {}

    odds_dict = {}
    try:
        # 最も行数が多いテーブルを対象にする
        all_tables = soup.find_all("table")
        target_tbl = max(all_tables, key=lambda t: len(t.find_all("tr")), default=None)
        if target_tbl is None or len(target_tbl.find_all("tr")) < 10:
            return {}

        rows = target_tbl.find_all("tr")

        # rowspan/colspan を考慮してテーブルを仮想グリッドに展開
        grid: dict = {}
        for r_idx, tr in enumerate(rows):
            c_idx = 0
            for td in tr.find_all(["td", "th"]):
                while (r_idx, c_idx) in grid:
                    c_idx += 1
                text    = td.get_text(strip=True)
                rowspan = int(td.get("rowspan", 1))
                colspan = int(td.get("colspan", 1))
                for dr in range(rowspan):
                    for dc in range(colspan):
                        grid[(r_idx + dr, c_idx + dc)] = text
                c_idx += colspan

        if not grid:
            return {}

        max_row = max(r for r, c in grid.keys()) + 1
        max_col = max(c for r, c in grid.keys()) + 1

        # ── ヘッダー行(row=0)から1着艇番リストを取得 ──────────────
        # ヘッダー: [艇番1, 名前1, 名前1, 艇番2, 名前2, 名前2, ...]
        # 艇番は3列おきの位置 (0, 3, 6, 9, 12, 15) にある
        header_boats: list[int] = []
        for c in range(0, max_col, 3):
            val = grid.get((0, c), "").strip()
            if re.match(r"^[1-6]$", val):
                header_boats.append(int(val))
        if len(header_boats) != 6:
            header_boats = list(range(1, 7))  # フォールバック

        # ── データ行(row=1〜)をグループ単位でパース ──────────────
        # グループ i (i=0..5): col=[3i]=2着, [3i+1]=3着, [3i+2]=オッズ
        for r in range(1, max_row):
            for i, first_boat in enumerate(header_boats):
                cs = i * 3  # column_start
                if cs + 2 >= max_col:
                    break

                second_str = grid.get((r, cs),     "").strip()
                third_str  = grid.get((r, cs + 1), "").strip()
                odds_str   = grid.get((r, cs + 2), "").strip()

                # 艇番チェック
                if not (re.match(r"^[1-6]$", second_str) and
                        re.match(r"^[1-6]$", third_str)):
                    continue

                second = int(second_str)
                third  = int(third_str)

                # 3艇すべて異なることを確認
                if len({first_boat, second, third}) != 3:
                    continue

                # オッズ: 整数(高倍率)・小数どちらも受け付ける
                odds_m = re.match(r"^(\d+(?:\.\d+)?)$", odds_str)
                if not odds_m:
                    continue
                try:
                    odds_val = float(odds_m.group(1))
                except ValueError:
                    continue

                odds_dict[f"{first_boat}-{second}-{third}"] = odds_val

    except Exception as e:
        print(f"[scraper] 3連単オッズ取得失敗: {e}")

    return odds_dict


# ─────────────────────────────────────────────
# 4b. 2連単・2連複オッズ取得
# ─────────────────────────────────────────────
def fetch_odds_2tf(race_no: int, date_str: str | None = None) -> dict:
    """
    boatrace.jp の odds2tf ページから2連単・2連複オッズを取得する。
    同一ページに両方のテーブルが存在する。

    Returns
    -------
    {
        "2連単": {"1-2": 10.6, "1-3": 15.2, ...},  # 30通り
        "2連複": {"1=2": 4.9, "1=3": 13.5, ...},    # 15通り
    }
    取得失敗時は空dict。
    """
    if date_str is None:
        date_str = datetime.now().strftime("%Y%m%d")

    url = f"{BASE_URL}/owpc/pc/race/odds2tf"
    params = {"jcd": JYCD, "hd": date_str, "rno": race_no}
    soup = _get_soup(url, params)
    if not soup:
        return {"2連単": {}, "2連複": {}}

    nitan_odds = {}
    nifuku_odds = {}

    try:
        def _parse_odds_val(text: str) -> float | None:
            """オッズ文字列を数値に変換（整数表示・小数表示の両対応）"""
            clean = re.sub(r'[\s,]', '', text)
            if not clean or clean == '-':
                return None
            m = re.match(r'^(\d+(?:\.\d+)?)$', clean)
            if m:
                return float(m.group(1))
            return None

        # ページ内の title7_mainLabel からラベル直後のテーブルを特定
        # HTML構造: <div class="title7"><h3><span class="title7_mainLabel">2連単オッズ</span>
        # テーブルはラベルの兄弟要素にあるため find_next("table") で取得
        nitan_tbl = None
        nifuku_tbl = None
        for label in soup.find_all("span", class_="title7_mainLabel"):
            text = label.get_text(strip=True)
            if "2連単" in text:
                nitan_tbl = label.find_next("table")
            elif "2連複" in text:
                nifuku_tbl = label.find_next("table")

        # ── 2連単パース ──────────────────────────────────────────
        # 12列: グループg(0-5) = col[2g]=2着艇番, col[2g+1]=オッズ
        # 1着艇 = ヘッダーのg+1号艇
        if nitan_tbl:
            tbody = nitan_tbl.find("tbody")
            if tbody:
                for tr in tbody.find_all("tr"):
                    tds = tr.find_all("td")
                    if len(tds) < 12:
                        continue
                    for g in range(6):
                        first = g + 1
                        boat_td = tds[2 * g]
                        odds_td = tds[2 * g + 1]
                        if "is-disabled" in " ".join(odds_td.get("class", [])):
                            continue
                        second_str = boat_td.get_text(strip=True)
                        if not re.match(r'^[1-6]$', second_str):
                            continue
                        second = int(second_str)
                        if first == second:
                            continue
                        odds_val = _parse_odds_val(odds_td.get_text(strip=True))
                        if odds_val is not None:
                            nitan_odds[f"{first}-{second}"] = odds_val

        # ── 2連複パース ──────────────────────────────────────────
        # 下三角行列: row r の 2着艇 = r+2, 有効列は g+1 < r+2 のみ
        if nifuku_tbl:
            tbody = nifuku_tbl.find("tbody")
            if tbody:
                for r_idx, tr in enumerate(tbody.find_all("tr")):
                    second_boat = r_idx + 2
                    if second_boat > 6:
                        break
                    tds = tr.find_all("td")
                    if len(tds) < 2:
                        continue
                    for g in range(6):
                        first_boat = g + 1
                        if first_boat >= second_boat:
                            break
                        if 2 * g + 1 >= len(tds):
                            break
                        odds_td = tds[2 * g + 1]
                        if "is-disabled" in " ".join(odds_td.get("class", [])):
                            continue
                        odds_val = _parse_odds_val(odds_td.get_text(strip=True))
                        if odds_val is not None:
                            nifuku_odds[f"{first_boat}={second_boat}"] = odds_val

    except Exception as e:
        print(f"[scraper] 2連単/2連複オッズ取得失敗: {e}")

    return {"2連単": nitan_odds, "2連複": nifuku_odds}


# ─────────────────────────────────────────────
# 5. 締め切り時刻取得
# ─────────────────────────────────────────────
def fetch_deadline(race_no: int, date_str: str | None = None) -> str:
    """
    racelist ページから指定レースの締切予定時刻を取得して返す。
    取得できない場合は "-" を返す。

    HTML構造（実測）:
      締切予定時刻ラベルと時刻が同一 tr 内に並んでいる:
      <tr>
        <td colspan="2">締切予定時刻</td>  ← インデックス0
        <td>15:25</td>   ← 1R (インデックス1)
        <td>15:52</td>   ← 2R (インデックス2)
        ...
        <td>20:41</td>   ← 12R (インデックス12)
      </tr>
      → race_no 番目のtd（0始まりで race_no）を取得
    """
    if date_str is None:
        date_str = datetime.now().strftime("%Y%m%d")

    url = f"{BASE_URL}/owpc/pc/race/racelist"
    params = {"jcd": JYCD, "hd": date_str, "rno": 1}
    soup = _get_soup(url, params)
    if not soup:
        return "-"

    try:
        deadline_td = soup.find("td", string=re.compile("締切予定時刻"))
        if not deadline_td:
            return "-"

        # 同じtr内の全td取得
        parent_tr = deadline_td.find_parent("tr")
        if not parent_tr:
            return "-"

        all_tds = parent_tr.find_all("td")
        # all_tds[0] = 「締切予定時刻」ラベル (colspan=2)
        # all_tds[1] = 1R, all_tds[2] = 2R, ... all_tds[12] = 12R
        idx = race_no  # race_no=1 → all_tds[1]
        if idx < len(all_tds):
            deadline = all_tds[idx].get_text(strip=True)
            if re.match(r"\d{1,2}:\d{2}", deadline):
                return deadline

        return "-"
    except Exception as e:
        print(f"[scraper] 締切時刻取得失敗: {e}")
        return "-"


# ─────────────────────────────────────────────
# 6. レース結果自動取得
# ─────────────────────────────────────────────
def fetch_race_result(race_no: int, date_str: str | None = None) -> dict | None:
    """
    boatrace.jp のレース結果ページから1〜3着の艇番を取得する。

    レース未終了・データ未公開の場合は None を返す。

    Returns
    -------
    {"1着": 1, "2着": 3, "3着": 2, "三連単": "1-3-2"} or None
    """
    if date_str is None:
        date_str = datetime.now().strftime("%Y%m%d")

    url = f"{BASE_URL}/owpc/pc/race/raceresult"
    params = {"jcd": JYCD, "hd": date_str, "rno": race_no}
    soup = _get_soup(url, params)
    if not soup:
        return None

    try:
        # ── 方法A: tbody の is-boatColor td を着順に読む ────────────
        # raceresult の tbody は着順順に並ぶ前提
        boat_order: list[int] = []
        for tbody in soup.find_all("tbody"):
            td = tbody.find("td", class_=re.compile(r"is-boatColor"))
            if td:
                bn = _to_float(td.get_text(strip=True))
                if bn is not None and 1 <= bn <= 6:
                    boat_order.append(int(bn))
            if len(boat_order) >= 3:
                break

        if len(boat_order) >= 3:
            return {
                "1着": boat_order[0],
                "2着": boat_order[1],
                "3着": boat_order[2],
                "三連単": f"{boat_order[0]}-{boat_order[1]}-{boat_order[2]}",
            }

        # ── 方法B: テーブル行の (着順, 艇番) パターンを探す ─────────
        seen_ranks: set[str] = set()
        ranked_boats: list[int] = []
        for table in soup.find_all("table"):
            for row in table.find_all("tr"):
                cells = [c.get_text(strip=True) for c in row.find_all(["td", "th"])]
                if not cells:
                    continue
                rank_str = cells[0]
                if rank_str in {"1", "2", "3", "4", "5", "6"} and rank_str not in seen_ranks:
                    for cell in cells[1:5]:
                        bn = _to_float(cell)
                        if bn is not None and 1 <= bn <= 6:
                            ranked_boats.append(int(bn))
                            seen_ranks.add(rank_str)
                            break
            if len(ranked_boats) >= 3:
                break

        if len(ranked_boats) >= 3:
            return {
                "1着": ranked_boats[0],
                "2着": ranked_boats[1],
                "3着": ranked_boats[2],
                "三連単": f"{ranked_boats[0]}-{ranked_boats[1]}-{ranked_boats[2]}",
            }

        # ── 方法C: 払い戻しテキストから3連単を逆算 ─────────────────
        page_text = soup.get_text(" ")
        m = re.search(r'3連単\D{0,5}([1-6])-([1-6])-([1-6])', page_text)
        if m:
            a, b, c = int(m.group(1)), int(m.group(2)), int(m.group(3))
            return {"1着": a, "2着": b, "3着": c, "三連単": f"{a}-{b}-{c}"}

    except Exception as e:
        print(f"[scraper] レース結果取得失敗: {e}")

    return None


# ─────────────────────────────────────────────
# 7. レースグレード検出
# ─────────────────────────────────────────────
def fetch_race_grade(race_no: int, date_str: str | None = None) -> dict:
    """
    racelist ページからレースグレードと節情報を検出する。

    Returns
    -------
    {"grade": "一般"|"G3"|"G2"|"G1"|"SG", "is_final": bool, "title": str}
    """
    if date_str is None:
        date_str = datetime.now().strftime("%Y%m%d")

    result = {"grade": "一般", "is_final": False, "title": ""}

    url = f"{BASE_URL}/owpc/pc/race/racelist"
    params = {"jcd": JYCD, "hd": date_str, "rno": race_no}
    soup = _get_soup(url, params)
    if not soup:
        return result

    # グレード: ページタイトルやheading要素から検出
    page_text = soup.get_text()
    title_tag = soup.find("h2", class_=re.compile(r"heading2"))
    if title_tag:
        result["title"] = title_tag.get_text(strip=True)

    # グレード判定（優先度順）
    grade_patterns = [
        (r"SG|グランプリ|クラシック|オールスター|メモリアル|ダービー|チャレンジカップ", "SG"),
        (r"G[Ⅰ1I]|GI|周年記念|高松宮記念|ダイヤモンドカップ", "G1"),
        (r"G[Ⅱ2II]|GII|モーターボート大賞|レディースチャレンジカップ", "G2"),
        (r"G[Ⅲ3III]|GIII|オールレディース|マスターズリーグ|企業杯", "G3"),
    ]
    for pattern, grade in grade_patterns:
        if re.search(pattern, page_text):
            result["grade"] = grade
            break

    # 優勝戦・準優勝戦検出
    if re.search(r"優勝戦|ファイナル", page_text):
        result["is_final"] = True
    elif race_no == 12:
        # 最終レースはメインレースの可能性が高い
        result["is_final"] = True

    return result


# ─────────────────────────────────────────────
# 8. 選手コース別成績取得
# ─────────────────────────────────────────────
def fetch_player_course_stats(reg_no: str) -> dict:
    """
    boatrace.jp の選手コース別成績ページからデータを取得する。

    Returns
    -------
    {
        "1": {"starts": 50, "win_rate": 62.0, "rentai_rate": 78.0},
        "2": {"starts": 30, "win_rate": 13.3, "rentai_rate": 40.0},
        ...
    }
    """
    if not reg_no:
        return {}

    url = f"{BASE_URL}/owpc/pc/data/racersearch/course"
    params = {"toban": reg_no}
    soup = _get_soup(url, params)
    if not soup:
        return {}

    stats = {}
    try:
        # コース別成績テーブルを探す（3連対率テーブル優先）
        tables = soup.find_all("table")
        # 3連対率テーブルを優先（進入率テーブルより先にチェック）
        tables_sorted = sorted(tables, key=lambda t: "3連対" in t.get_text(), reverse=True)
        for tbl in tables_sorted:
            tbl_text = tbl.get_text()
            if "コース" not in tbl_text:
                continue
            for tr in tbl.find_all("tr"):
                # コース番号は <th> にある場合と <td> にある場合がある
                ths = tr.find_all("th")
                tds = tr.find_all("td")

                course = None
                # <th> からコース番号を探す
                for th in ths:
                    th_text = th.get_text(strip=True)
                    course_m = re.fullmatch(r'([1-6])', th_text)
                    if course_m:
                        course = course_m.group(1)
                        break
                # <td> の先頭からも探す（旧レイアウト対応）
                if not course and tds:
                    first_text = tds[0].get_text(strip=True)
                    course_m = re.fullmatch(r'([1-6])', first_text)
                    if course_m:
                        course = course_m.group(1)
                        tds = tds[1:]  # コース番号セルを除外

                if not course:
                    continue

                # <td> から数値を収集
                nums = []
                for td in tds:
                    v = _to_float(td.get_text(strip=True))
                    if v is not None:
                        nums.append(v)

                if len(nums) >= 3:
                    stats[course] = {
                        "starts":      int(nums[0]),
                        "win_rate":    nums[1],     # 1着率 (%)
                        "rentai_rate": nums[2],     # 2連対率 (%)
                    }
                elif len(nums) >= 1:
                    # 3連対率のみ表示の場合
                    stats[course] = {
                        "starts":      0,
                        "win_rate":    nums[0],     # 3連対率 (%)
                        "rentai_rate": nums[0],
                    }
            if stats:
                break
    except Exception as e:
        print(f"[scraper] コース別成績取得失敗 ({reg_no}): {e}")

    return stats


# ─────────────────────────────────────────────
# 9. 選手直近成績取得
# ─────────────────────────────────────────────
def fetch_player_recent_results(reg_no: str, n: int = 10) -> list[int]:
    """
    boatrace.jp の過去3節成績ページから直近N走の着順リストを返す。

    Returns
    -------
    [1, 3, 2, 1, 4, ...] （最新が先頭）
    空リストの場合はデータ取得失敗
    """
    if not reg_no:
        return []

    # back3 ページ（過去3節成績）から個別レース着順を取得
    url = f"{BASE_URL}/owpc/pc/data/racersearch/back3"
    params = {"toban": reg_no}
    soup = _get_soup(url, params)
    if not soup:
        return []

    # 全角→半角マッピング
    zen2han = {"１": "1", "２": "2", "３": "3", "４": "4", "５": "5", "６": "6"}

    results: list[int] = []
    try:
        # 着順は raceresult リンク付きの <a> タグ内に全角数字で記載
        for a_tag in soup.find_all("a", href=re.compile(r"raceresult")):
            text = a_tag.get_text(strip=True)
            # 全角数字を半角に変換
            converted = zen2han.get(text, text)
            if re.fullmatch(r'[1-6]', converted):
                results.append(int(converted))
                if len(results) >= n:
                    break
    except Exception as e:
        print(f"[scraper] 直近成績取得失敗 ({reg_no}): {e}")

    return results[:n]


# ─────────────────────────────────────────────
# 10. 拡張選手データの並列取得
# ─────────────────────────────────────────────
def fetch_extended_player_data(reg_nos: list[str]) -> dict:
    """
    複数選手のコース別成績・直近成績を並列で取得する。

    Parameters
    ----------
    reg_nos : 登録番号リスト（枠番順）

    Returns
    -------
    {
        "course_stats":    {"登録番号": {コース別成績dict}, ...},
        "recent_results":  {"登録番号": [着順list], ...},
    }
    """
    course_stats = {}
    recent_results = {}

    valid_reg_nos = [r for r in reg_nos if r]
    if not valid_reg_nos:
        return {"course_stats": {}, "recent_results": {}}

    def _fetch_course(rno):
        return rno, fetch_player_course_stats(rno)

    def _fetch_recent(rno):
        return rno, fetch_player_recent_results(rno)

    try:
        with ThreadPoolExecutor(max_workers=6) as executor:
            # コース別成績と直近成績を全選手分並列取得
            futures = []
            for rno in valid_reg_nos:
                futures.append(executor.submit(_fetch_course, rno))
                futures.append(executor.submit(_fetch_recent, rno))

            for future in as_completed(futures):
                try:
                    key, data = future.result(timeout=20)
                    if isinstance(data, dict):
                        course_stats[key] = data
                    elif isinstance(data, list):
                        recent_results[key] = data
                except Exception:
                    pass
    except Exception as e:
        print(f"[scraper] 拡張データ並列取得失敗: {e}")

    return {
        "course_stats":   course_stats,
        "recent_results": recent_results,
    }


# ─────────────────────────────────────────────
# 11. 蒲郡公式サイト：オリジナル展示タイム取得
# ─────────────────────────────────────────────

GAMAGORI_SITE = "https://www.gamagori-kyotei.com/asp/gamagori/kyogi/kyogihtml"


def fetch_gamagori_time(race_no: int, date_str: str | None = None) -> pd.DataFrame:
    """
    蒲郡競艇公式サイトのオリジナル展示ページから展示タイムデータを取得する。

    データソース: time/time{YYYYMMDD}07{RR}.htm
    boatrace.jp にない蒲郡独自計測データ（一周・まわり足・直線）を含む。

    テーブル構造（1テーブル、ヘッダー+6行）:
      [0]予想 [1]枠番 [2]選手名 [3]級別 [4]モーター素性 [5]出足
      [6]伸び [7]回り足 [8]展示タイム [9]一周 [10]まわり足 [11]直線 [12]コメント

    Returns
    -------
    DataFrame with columns: 枠番, 展示タイム_gama, 一周タイム, まわり足タイム, 直線タイム
    空DataFrameの場合はデータ取得失敗。
    """
    if date_str is None:
        date_str = datetime.now().strftime("%Y%m%d")

    path = f"time/time{date_str}{JYCD}{race_no:02d}.htm"
    html = _fetch_gamagori_html(path)
    if not html:
        return pd.DataFrame()

    soup = BeautifulSoup(html, "lxml")
    tbl = soup.find("table")
    if not tbl:
        return pd.DataFrame()

    rows = []
    for tr in tbl.find_all("tr")[1:]:  # ヘッダーをスキップ
        tds = tr.find_all("td")
        if len(tds) < 12:
            continue
        waku = tds[1].get_text(strip=True)
        if waku not in ("1", "2", "3", "4", "5", "6"):
            continue

        tenji = _to_float(tds[8].get_text())
        isshu = _to_float(tds[9].get_text())
        mawari = _to_float(tds[10].get_text())
        chokusen = _to_float(tds[11].get_text())

        rows.append({
            "枠番":          waku,
            "展示タイム_gama": tenji,
            "一周タイム":     isshu,
            "まわり足タイム": mawari,
            "直線タイム":     chokusen,
        })

    return pd.DataFrame(rows)


# ─────────────────────────────────────────────
# 12. 蒲郡公式サイト：高橋アナ予想取得
# ─────────────────────────────────────────────


def _fetch_gamagori_html(path: str) -> str | None:
    """蒲郡競艇公式サイトから生テキストを取得（エンコーディング自動検出）"""
    url = f"{GAMAGORI_SITE}/{path}"
    hdrs = {**HEADERS, "Referer": "https://www.gamagori-kyotei.com/"}
    try:
        r = requests.get(url, headers=hdrs, timeout=15)
        r.raise_for_status()
        for enc in ("utf-8", "shift_jis", "cp932", "euc-jp"):
            try:
                return r.content.decode(enc)
            except UnicodeDecodeError:
                continue
        return r.text
    except Exception as e:
        print(f"[gamagori] 取得失敗 {url}: {e}")
        return None


def _parse_gamagori_js_func(js_text: str, func_name: str, race_no: int) -> dict[str, str]:
    """
    蒲郡サイトのJSファイルから、特定関数・特定レース番号の
    (艇番 → 値) マッピングを抽出する。

    JS関数の構造:
      function funcXxx( argRaceNum, argTei ) {
          if( strRaceNum === '1'){
              if( strTei === '1'){ strX = 'val1' }
              else if( strTei === '2'){ strX = 'val2' }
              ...
          } else if( strRaceNum === '2'){ ... }
      }

    Returns
    -------
    {"1": "val1", "2": "val2", ...}
    """
    # 関数の開始位置を特定
    func_start = js_text.find(f"function {func_name}")
    if func_start < 0:
        return {}

    # 対象レースブロックの位置を特定
    race_marker = f"strRaceNum === '{race_no}'"
    pos = js_text.find(race_marker, func_start)
    if pos < 0:
        return {}

    # 次のレースブロック or 関数末尾でブロックを区切る
    next_marker = f"strRaceNum === '{race_no + 1}'"
    next_pos = js_text.find(next_marker, pos + 1)
    if next_pos < 0:
        # 最終レース (12R) の場合: "}else{" か適当な終端を使う
        next_pos = js_text.find("}else{", pos + len(race_marker))
        if next_pos < 0:
            next_pos = pos + 3000  # フォールバック: 3000文字以内を対象

    block = js_text[pos:next_pos]

    # ブロック内から (strTei === 'N') { strXxx = 'value' } パターンを抽出
    result: dict[str, str] = {}
    for m in re.finditer(
        r"strTei\s*===\s*'([1-6])'\s*\)\s*\{\s*\w+\s*=\s*'([^']*)'",
        block,
    ):
        result[m.group(1)] = m.group(2)

    return result


def fetch_gamagori_taka(race_no: int, date_str: str | None = None) -> dict:
    """
    蒲郡競艇公式サイトの高橋アナ予想ページから予想データを取得する。

    データソース: takahashi/takahashi{YYYYMMDD}07{RR}.htm
      - chart div: 5×5評価チャート（X=スリット付近の勢い, Y=ターンの雰囲気）
      - hyoka div: 展開予想テキスト
      - start div: コース進入・スリット・ST情報

    chart div から各艇の (col, row) 座標を読み取り、
    chart_score = (col+1)/5 * 0.5 + (5-row)/5 * 0.5 + trend補正
    で 0〜1 のスコアを算出する。

    「展示後に更新します」等の場合は未公開と判定し available=False を返す。

    Returns
    -------
    {
        "available":    bool,
        "tenkai":       str,              # 展開予想テキスト
        "yoso":         list[str],        # （予約）
        "yoso_boats":   list[str],        # hyoka画像順の予想艇番リスト
        "chart_scores": dict[str, float], # 艇番→チャートスコア(0〜1)
        "chart_positions": dict,          # 艇番→{col,row,trend} チャートXY座標
        "ana":          str,              # 穴艇情報（または ""）
        "slit_order":   list[int],        # スリット通過順
    }
    """
    if date_str is None:
        date_str = datetime.now().strftime("%Y%m%d")

    result: dict = {
        "available":    False,
        "tenkai":       "",
        "yoso":         [],
        "yoso_boats":   [],
        "chart_scores": {},
        "chart_positions": {},
        "ana":          "",
        "slit_order":   [],
    }

    # ── 高橋アナ予想HTMLページを取得 ──────────────────────────────
    path = f"takahashi/takahashi{date_str}07{race_no:02d}.htm"
    html = _fetch_gamagori_html(path)
    if not html:
        print(f"[gamagori] 高橋アナページ取得失敗: {path}")
        return result

    soup = BeautifulSoup(html, "lxml")

    # ── hyoka div: 未公開判定 + 展開予想テキスト ──────────────────
    hyoka = soup.find("div", id="hyoka")
    if hyoka:
        hyoka_text = hyoka.get_text(strip=True)
        if re.search(r"更新します|準備中|しばらくお待ち", hyoka_text):
            return result
        # 予想艇順: taka_{N}S.png 画像の並び順（高橋アナの実際の予想ランク）
        yoso_boats: list[str] = []
        for img in hyoka.find_all("img"):
            m_s = re.search(r"taka_(\d)S", img.get("src", ""))
            if m_s:
                yoso_boats.append(m_s.group(1))
        if yoso_boats:
            result["yoso_boats"] = yoso_boats

        # 展開予想テキスト: <img alt="2"> → ② のように丸数字に変換
        _CIRCLE = {"1": "①", "2": "②", "3": "③", "4": "④", "5": "⑤", "6": "⑥"}
        p_tag = hyoka.find("p")
        if p_tag:
            parts: list[str] = []
            for elem in p_tag.children:
                if isinstance(elem, str):
                    parts.append(elem)
                elif elem.name == "br":
                    parts.append("\n")
                elif elem.name == "img":
                    alt = elem.get("alt", "")
                    parts.append(_CIRCLE.get(alt, alt))
            result["tenkai"] = "".join(parts).strip()

    # ── chart div: 5×5評価チャートからXY座標を読み取る ────────────
    # テーブル構造: 5行×5列、各セルにボート画像 taka_{N}M.png
    #   X軸 (col 0→4): スリット付近の勢い（右ほど高い）
    #   Y軸 (row 0→4): ターンの雰囲気（上ほど高い）
    #   _up / _down サフィックス: 勢いの上昇/下降トレンド
    chart = soup.find("div", id="chart")
    if not chart:
        return result

    boat_positions: dict[str, dict] = {}  # {艇番: {col, row, trend}}
    for r_idx, tr in enumerate(chart.find_all("tr")):
        for c_idx, td in enumerate(tr.find_all("td")):
            for img in td.find_all("img"):
                src = img.get("src", "")
                m = re.search(r"taka_(\d)M", src)
                if not m:
                    continue
                boat = m.group(1)
                trend = ""
                if "_up" in src:
                    trend = "up"
                elif "_down" in src:
                    trend = "down"
                boat_positions[boat] = {
                    "col": c_idx, "row": r_idx, "trend": trend,
                }

    if not boat_positions:
        return result

    # chart_score 算出: X成分 + Y成分 + トレンド補正 → 0〜1
    chart_scores: dict[str, float] = {}
    for boat, pos in boat_positions.items():
        x_norm = (pos["col"] + 1) / 5.0   # 1〜5 → 0.2〜1.0
        y_norm = (5 - pos["row"]) / 5.0   # row0=1.0, row4=0.2
        score = x_norm * 0.5 + y_norm * 0.5
        if pos["trend"] == "up":
            score += 0.05
        elif pos["trend"] == "down":
            score -= 0.05
        score = max(0.0, min(1.0, score))
        chart_scores[boat] = round(score, 3)

    result["chart_scores"] = chart_scores
    result["chart_positions"] = boat_positions
    result["available"] = True

    # ── start div: スリット通過順を取得 ───────────────────────────
    start_div = soup.find("div", id="start")
    if start_div:
        for tr in start_div.find_all("tr"):
            for td in tr.find_all("td"):
                if "slit" in " ".join(td.get("class", [])):
                    slit_img = td.find("img")
                    if slit_img:
                        sm = re.search(r"focus_b(\d)", slit_img.get("src", ""))
                        if sm:
                            boat_no = int(sm.group(1))
                            if boat_no not in result["slit_order"]:
                                result["slit_order"].append(boat_no)

    return result


def generate_sample_data(race_no: int = 1) -> tuple[pd.DataFrame, dict]:
    import random
    random.seed(race_no)
    sample_sts    = [0.12, 0.15, 0.10, 0.18, 0.14, 0.20]
    sample_motor  = [62.5, 45.0, 55.3, 38.2, 71.0, 50.8]
    sample_boat   = [55.0, 48.5, 60.1, 42.0, 53.3, 49.7]
    sample_weight = [52.0, 54.5, 51.0, 53.0, 50.5, 55.0]
    sample_lap    = [6.82, 6.90, 6.85, 6.95, 6.78, 6.92]
    sample_isshu  = [37.50, 37.80, 37.40, 38.00, 37.60, 37.90]  # 一周タイム
    sample_mawari = [5.10, 5.30, 5.05, 5.45, 5.15, 5.35]        # まわり足タイム
    sample_choku  = [6.35, 6.50, 6.40, 6.55, 6.30, 6.48]        # 直線タイム
    sample_cwr    = [65.0, 12.5, 15.0, 18.0, 8.0, 5.0]  # コース別1着率
    sample_recent = [[1,2,1,3,1,2,1,1,3,2],  # 選手1: 好調
                     [3,4,2,3,5,2,4,3,2,3],  # 選手2: 普通
                     [2,1,3,2,1,3,2,1,2,3],  # 選手3: やや好調
                     [4,5,3,4,6,3,5,4,3,4],  # 選手4: 不調
                     [1,1,2,1,2,1,3,1,1,2],  # 選手5: 絶好調
                     [3,4,5,3,4,5,4,3,4,5]]  # 選手6: 不調
    ranks = ["A1", "B1", "A1", "B2", "A2", "B1"]
    data = [
        {
            "枠番": str(i), "選手名": f"選手{i}", "級別": ranks[i-1],
            "登録番号": f"400{i}",
            "F回数": 1 if i == 4 else 0,
            "L回数": 0,
            "全国勝率": 6.0 + random.uniform(-1, 1),
            "全国2連率": 38.0 + random.uniform(-5, 5),
            "蒲郡勝率": 5.5 + random.uniform(-1, 1),
            "蒲郡2連率": 35.0 + random.uniform(-5, 5),
            "スタートタイミング": sample_sts[i - 1],
            "展示タイム": 6.7 + random.uniform(-0.1, 0.1),
            "チルト": 0.0 if i <= 2 else (-0.5 if i <= 4 else 0.5),
            "進入コース": i,
            "体重": sample_weight[i - 1],
            "周回タイム": sample_lap[i - 1],
            "一周タイム": sample_isshu[i - 1],
            "まわり足タイム": sample_mawari[i - 1],
            "直線タイム": sample_choku[i - 1],
            "モーター番号": 60 + i, "モーター2連率": sample_motor[i - 1],
            "ボート番号":  40 + i, "ボート2連率":  sample_boat[i - 1],
            "コース別1着率": sample_cwr[i - 1],
            "直近平均着順": round(sum(sample_recent[i-1]) / len(sample_recent[i-1]), 2),
            "直近勝率": round(sum(1 for r in sample_recent[i-1] if r == 1) / len(sample_recent[i-1]) * 100, 1),
        }
        for i in range(1, 7)
    ]
    weather = {
        "天気": "晴れ", "気温": "12.0℃", "水温": "11.0℃",
        "風速": "1m", "風向": "北", "波高": "1cm",
        "grade": "一般", "is_final": False, "grade_title": "",
    }
    return pd.DataFrame(data), weather
