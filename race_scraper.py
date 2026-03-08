from __future__ import annotations
"""
boatrace.jp から出走表・直前情報をスクレイピングするモジュール（複数会場対応）。
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
    import config as _cfg
    from config import BASE_URL, HEADERS
except ImportError:
    import types as _types
    _cfg = _types.SimpleNamespace(JYCD="07", get_venue_config=lambda jycd=None: {"has_iot_weather": True, "has_original_exhibit": True, "has_taka_yoso": True})
    BASE_URL = "https://www.boatrace.jp"
    HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}


def _jycd():
    """アクティブ会場コードを返す（config.set_venue() で動的に切り替わる）。"""
    return _cfg.JYCD

def _get_soup(url: str, params: dict, session: requests.Session | None = None) -> BeautifulSoup | None:
    try:
        _req = session if session else requests
        resp = _req.get(url, params=params, headers=HEADERS, timeout=15)
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


def _fetch_racelist_soup(race_no: int, date_str: str, session: requests.Session | None = None) -> BeautifulSoup | None:
    """racelist ページの BeautifulSoup を返す（soup 共有用）。"""
    url = f"{BASE_URL}/owpc/pc/race/racelist"
    params = {"jcd": _jycd(), "hd": date_str, "rno": race_no}
    return _get_soup(url, params, session=session)


# ─────────────────────────────────────────────
# 1. 出走表取得
# ─────────────────────────────────────────────
def fetch_race_card(race_no: int, date_str: str | None = None, _soup: BeautifulSoup | None = None, session: requests.Session | None = None) -> pd.DataFrame:
    if date_str is None: date_str = datetime.now().strftime("%Y%m%d")
    soup = _soup if _soup is not None else _fetch_racelist_soup(race_no, date_str, session=session)
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
                reg_m = re.search(r'toban=(\d+)', name_link["href"])
                if reg_m:
                    reg_no = reg_m.group(1)
        if not reg_no:
            # フォールバック: tbody内の4桁数字で登録番号らしきものを探す
            for a_tag in tbody.find_all("a"):
                href = a_tag.get("href", "")
                reg_m = re.search(r'toban=(\d+)', href)
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
        #   [2] 当地勝率|当地2連率|蒲郡3連率
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
            if len(vals) >= 1: lw = vals[0]  # 当地勝率
            if len(vals) >= 2: l2 = vals[1]  # 当地2連率
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
            "全国勝率": nw, "全国2連率": n2, "当地勝率": lw, "当地2連率": l2,
            "スタートタイミング": st,
            "モーター番号": motor_no, "モーター2連率": motor2,
            "ボート番号": boat_no,  "ボート2連率":  boat2,
        })
    return pd.DataFrame(rows)


# ─────────────────────────────────────────────
# 1.5  蒲郡競艇リアルタイム気象データ取得 (IoT API)
# ─────────────────────────────────────────────
_GAMAGORI_IOT_URL = "https://vy9ytyar04.execute-api.ap-northeast-1.amazonaws.com/wdsxkey"
_GAMAGORI_IOT_KEY = "RNMO2YNAgy1drxuUyVnUUaECvRUprycxEbJuL9oe"
# Wd1m: 0=無風, 1=北北東, 2=北東, ..., 15=北北西, 16=北
_WD1M_MAP = {
    0: "-", 1: "北北東", 2: "北東", 3: "東北東", 4: "東",
    5: "東南東", 6: "南東", 7: "南南東", 8: "南",
    9: "南南西", 10: "南西", 11: "西南西", 12: "西",
    13: "西北西", 14: "北西", 15: "北北西", 16: "北",
}

def fetch_gamagori_weather() -> dict | None:
    """蒲郡競艇公式サイトのIoT APIからリアルタイム気象データを取得する。
    失敗時は None を返す。"""
    try:
        resp = requests.post(_GAMAGORI_IOT_URL, json={
            "tablename": "RKS_Iot_DB_Monitor_Short2",
            "clientid": "devid007",
            "timeval": "0",
        }, headers={"x-api-key": _GAMAGORI_IOT_KEY}, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        items = data.get("Items", [])
        if not items:
            return None
        m = items[0]

        def _float(key):
            v = m.get(key)
            if v is None: return None
            try: return float(v)
            except (ValueError, TypeError): return None

        # 1分平均風速・風向
        wind_speed = _float("Sm1m")
        wind_dir_num = _float("Wd1m")
        wind_dir = _WD1M_MAP.get(int(wind_dir_num), "-") if wind_dir_num is not None else "-"

        temp       = _float("Ta")
        water_temp = _float("Tr")
        humidity   = _float("Ua")
        pressure   = _float("Pa")

        return {
            "風速": f"{wind_speed:.1f}m" if wind_speed is not None else "-",
            "風向": wind_dir,
            "気温": f"{temp:.1f}℃" if temp is not None else "-",
            "水温": f"{water_temp:.1f}℃" if water_temp is not None else "-",
            "湿度": f"{int(round(humidity))}%" if humidity is not None else "-",
            "気圧": f"{int(round(pressure))}hPa" if pressure is not None else "-",
        }
    except Exception:
        return None


# ─────────────────────────────────────────────
# 1.6  住之江競艇リアルタイム気象データ取得（公式サイト）
# ─────────────────────────────────────────────
_SUMINOE_WEATHER_URL = "https://www.boatrace-suminoe.jp/asp/kyogi/12/pc/sub_inf.htm"

def fetch_suminoe_weather() -> dict | None:
    """住之江競艇公式サイトから気象データを取得する。
    失敗時は None を返す。"""
    try:
        resp = requests.get(_SUMINOE_WEATHER_URL, headers=HEADERS, timeout=10)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, "html.parser")

        weather_div = soup.find("div", id="weather")
        if not weather_div:
            return None

        # テーブルから th/td ペアを読み取る
        ths = [th.get_text(strip=True) for th in weather_div.find_all("th")]
        tds = weather_div.find_all("td")
        if len(ths) < 6 or len(tds) < 6:
            return None

        data = {}
        for i, th in enumerate(ths):
            td = tds[i]
            # 風向セルは <br><span>(追い風)</span> を含むので最初のテキストノードだけ取る
            if th == "風向":
                # td内の直接テキスト（"北" 等）を取得（spanの中身は除外）
                text_parts = []
                for child in td.children:
                    if isinstance(child, str):
                        t = child.strip()
                        if t:
                            text_parts.append(t)
                    elif child.name != "span" and child.name != "br":
                        text_parts.append(child.get_text(strip=True))
                data[th] = text_parts[0] if text_parts else td.get_text(strip=True)
            else:
                data[th] = td.get_text(strip=True)

        return {
            "天気": data.get("天候", "-"),
            "風向": data.get("風向", "-"),
            "風速": data.get("風速", "-"),
            "波高": data.get("波高", "-"),
            "気温": data.get("気温", "-"),
            "水温": data.get("水温", "-"),
        }
    except Exception:
        return None


# ─────────────────────────────────────────────
# 2. 直前情報取得 (風向・展示タイム)
# ─────────────────────────────────────────────
def fetch_before_info(race_no: int, date_str: str | None = None) -> tuple[pd.DataFrame, dict]:
    if date_str is None: date_str = datetime.now().strftime("%Y%m%d")
    url = f"{BASE_URL}/owpc/pc/race/beforeinfo"
    params = {"jcd": _jycd(), "hd": date_str, "rno": race_no}
    soup = _get_soup(url, params)

    weather = {"天気": "-", "気温": "-", "水温": "-", "風速": "-", "風向": "-", "波高": "-", "湿度": "-", "気圧": "-", "安定板": False}
    if not soup: return pd.DataFrame(), weather

    # ── 安定板使用検出 ──────────────────────────────
    page_text = soup.get_text()
    if "安定板使用" in page_text:
        weather["安定板"] = True

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
            _wave = _get_label_data(unit)
            weather["波高"] = _wave if _wave else "-"

    # ── 蒲郡公式リアルタイム気象で上書き（蒲郡のみ）──────────────
    if _cfg.get_venue_config().get("has_iot_weather"):
        gw = fetch_gamagori_weather()
        if gw:
            for k in ("風速", "風向", "気温", "水温", "湿度", "気圧"):
                weather[k] = gw[k]

    # ── 住之江公式リアルタイム気象で上書き（住之江のみ）──────────
    if _cfg.get_venue_config().get("has_official_weather"):
        sw = fetch_suminoe_weather()
        if sw:
            for k in ("天気", "風速", "風向", "波高", "気温", "水温"):
                weather[k] = sw[k]

    # ── 展示進入コース・ST展示タイム解析 ──────────────────────────
    # table1_boatImage1 セクションにスタート展示の進入コース順が表示される
    # 上から順にコース1, コース2, ... コース6
    # 各div内の table1_boatImage1Number に枠番が記載
    # 各div内の table1_boatImage1Time にST展示タイムが記載
    course_map = {}  # {枠番文字列: 進入コース番号}
    st_display = {}  # {枠番文字列: ST展示タイム文字列} (例: ".02", "F.03")
    boat_divs = soup.find_all("div", class_="table1_boatImage1")
    for course_idx, div in enumerate(boat_divs, start=1):
        num_span = div.find("span", class_=re.compile(r"table1_boatImage1Number"))
        if num_span:
            f_no = num_span.get_text(strip=True)
            if f_no in ["1", "2", "3", "4", "5", "6"]:
                course_map[f_no] = course_idx
                # ST展示タイム取得
                time_span = div.find("span", class_=re.compile(r"table1_boatImage1Time"))
                if time_span:
                    st_text = time_span.get_text(strip=True)
                    if st_text:
                        st_display[f_no] = st_text

    # ── 展示タイム解析 ───────────────────────────
    # テーブルの行順 = 枠番順（1号艇、2号艇、...）
    # 実際のtd構造（データ行 10セル）:
    #   [0]枠番 [1]写真 [2]選手名 [3]体重 [4]展示タイム [5]チルト
    #   [6]プロペラ [7]部品交換 [8]前走成績 [9](空)
    rows = []
    tables = soup.find_all("table")
    for tbl in tables:
        if "展示タイム" in tbl.get_text():
            for tr in tbl.find_all("tr"):
                tds = tr.find_all("td")
                if len(tds) >= 6:
                    f_no = _zenkaku_to_frame(tds[0].get_text())
                    weight = _to_float(tds[3].get_text())
                    et = _to_float(tds[4].get_text())
                    tilt = _to_float(tds[5].get_text())
                    if f_no in ["1", "2", "3", "4", "5", "6"]:
                        rows.append({
                            "枠番":       f_no,
                            "体重":       weight,
                            "展示タイム":  et,
                            "チルト":     tilt,
                            "進入コース":  course_map.get(f_no, int(f_no)),
                            "ST展示":     st_display.get(f_no, ""),
                            "周回タイム":  None,
                        })
            break

    return pd.DataFrame(rows), weather


def _submit_original_exhibit(executor, venue_cfg: dict, race_no: int, date_str: str):
    """会場に応じたオリジナル展示タイム取得タスクを submit する。"""
    if not venue_cfg.get("has_original_exhibit"):
        return None
    code = venue_cfg.get("code", "")
    if code == "12":
        return executor.submit(fetch_suminoe_time, race_no, date_str)
    if code == "24":
        return executor.submit(fetch_omura_time, race_no, date_str)
    return executor.submit(fetch_gamagori_time, race_no, date_str)


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
    if date_str is None:
        date_str = datetime.now().strftime("%Y%m%d")

    # ── Phase 1: 独立した HTTPリクエストを並列実行 ──────
    # racelist soup → 出走表 + グレード（同一ページから2つ抽出）
    # beforeinfo   → 展示タイム + 気象
    # 会場独自展示データ（対応会場のみ）
    _venue = _cfg.get_venue_config()
    with ThreadPoolExecutor(max_workers=3) as executor:
        f_soup  = executor.submit(_fetch_racelist_soup, race_no, date_str)
        f_before = executor.submit(fetch_before_info, race_no, date_str)
        f_exhibit = _submit_original_exhibit(executor, _venue, race_no, date_str)

        racelist_soup = f_soup.result()
        ex_df, weather = f_before.result()
        gama_time_df = f_exhibit.result() if f_exhibit else pd.DataFrame()

    # ── 出走表をsoupからパース（HTTPリクエストなし） ──────
    card_df = fetch_race_card(race_no, date_str, _soup=racelist_soup)
    if card_df.empty:
        return pd.DataFrame(), {}

    if not ex_df.empty:
        final_df = pd.merge(card_df, ex_df, on="枠番", how="left")
    else:
        final_df = card_df
        final_df["展示タイム"] = None
        final_df["チルト"] = 0.0
        final_df["体重"] = None
        final_df["周回タイム"] = None

    # ── 会場独自展示タイム ──────
    if not gama_time_df.empty:
        final_df = pd.merge(final_df, gama_time_df, on="枠番", how="left")
    else:
        final_df["展示タイム_gama"] = None
        final_df["一周タイム"] = None
        final_df["まわり足タイム"] = None
        final_df["直線タイム"] = None

    # ── グレード検出（同じsoupを再利用、HTTPリクエストなし）──
    grade_info = fetch_race_grade(race_no, date_str, _soup=racelist_soup)
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


def fetch_base_race_data(
    race_no: int,
    date_str: str | None = None,
) -> tuple[pd.DataFrame, dict, BeautifulSoup | None]:
    """
    出走表＋直前情報を統合して返す（拡張データなし・Phase 1のみ）。
    racelist soup も返すので、呼び出し側で deadline 解析や extended 処理に再利用できる。
    """
    if date_str is None:
        date_str = datetime.now().strftime("%Y%m%d")

    _venue = _cfg.get_venue_config()
    with ThreadPoolExecutor(max_workers=3) as executor:
        f_soup   = executor.submit(_fetch_racelist_soup, race_no, date_str)
        f_before = executor.submit(fetch_before_info, race_no, date_str)
        f_exhibit = _submit_original_exhibit(executor, _venue, race_no, date_str)

        racelist_soup = f_soup.result()
        ex_df, weather = f_before.result()
        gama_time_df = f_exhibit.result() if f_exhibit else pd.DataFrame()

    card_df = fetch_race_card(race_no, date_str, _soup=racelist_soup)
    if card_df.empty:
        return pd.DataFrame(), {}, racelist_soup

    if not ex_df.empty:
        final_df = pd.merge(card_df, ex_df, on="枠番", how="left")
    else:
        final_df = card_df
        final_df["展示タイム"] = None
        final_df["チルト"] = 0.0
        final_df["体重"] = None
        final_df["周回タイム"] = None

    if not gama_time_df.empty:
        final_df = pd.merge(final_df, gama_time_df, on="枠番", how="left")
    else:
        final_df["展示タイム_gama"] = None
        final_df["一周タイム"] = None
        final_df["まわり足タイム"] = None
        final_df["直線タイム"] = None

    grade_info = fetch_race_grade(race_no, date_str, _soup=racelist_soup)
    weather["grade"] = grade_info["grade"]
    weather["is_final"] = grade_info["is_final"]
    weather["grade_title"] = grade_info["title"]

    return final_df, weather, racelist_soup


def apply_extended_data(final_df: pd.DataFrame, ext_data: dict) -> pd.DataFrame:
    """
    fetch_extended_player_data() の結果を DataFrame に適用する。
    （HTTPリクエストなし、データ結合のみ）
    """
    if "登録番号" not in final_df.columns:
        return final_df

    course_win_rates = []
    for _, row in final_df.iterrows():
        rno = row.get("登録番号")
        course = str(int(row["進入コース"])) if pd.notna(row.get("進入コース")) else row["枠番"]
        cs = ext_data["course_stats"].get(rno, {})
        cw = cs.get(course, {}).get("win_rate")
        course_win_rates.append(cw)
    final_df["コース別1着率"] = course_win_rates

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

    return final_df


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
    params = {"jcd": _jycd(), "hd": date_str, "rno": race_no}
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
    params = {"jcd": _jycd(), "hd": date_str, "rno": race_no}
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

        # ── 2連複パース（改良版） ──────────────────────────────────
        # 方式A: 2連単と同じ12列構造（boat_number/odds ペア）を試す
        # 方式B: 下三角行列（行ラベル + オッズ列）のフォールバック
        if nifuku_tbl:
            tbody = nifuku_tbl.find("tbody")
            if not tbody:
                tbody = nifuku_tbl

            # 方式A: 12列構造
            parsed_a = {}
            for tr in tbody.find_all("tr"):
                tds = tr.find_all("td")
                if len(tds) < 12:
                    continue
                for g in range(6):
                    boat_td = tds[2 * g]
                    odds_td = tds[2 * g + 1]
                    if "is-disabled" in " ".join(odds_td.get("class", [])):
                        continue
                    boat_text = boat_td.get_text(strip=True)
                    if not re.match(r'^[1-6]$', boat_text):
                        continue
                    boat_num = int(boat_text)
                    other_num = g + 1
                    if boat_num == other_num:
                        continue
                    odds_val = _parse_odds_val(odds_td.get_text(strip=True))
                    if odds_val is not None:
                        a, b = sorted([boat_num, other_num])
                        parsed_a[f"{a}={b}"] = odds_val

            if len(parsed_a) >= 10:
                nifuku_odds = parsed_a
            else:
                # 方式B: 下三角行列
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

    # ── 整合性チェック: 2連複オッズ < 2連単オッズ であるべき ───
    # 同じペアで比較して逆転している場合、ラベルが入れ替わっている
    if nitan_odds and nifuku_odds:
        swap_count = 0
        check_count = 0
        for key_f, odds_f in nifuku_odds.items():
            a, b = key_f.split("=")
            key_t = f"{a}-{b}"
            if key_t in nitan_odds:
                check_count += 1
                if odds_f > nitan_odds[key_t]:
                    swap_count += 1
        if check_count >= 3 and swap_count > check_count * 0.6:
            print(f"[scraper] 2連単/2連複オッズ逆転検出 ({swap_count}/{check_count}) → ラベル入替")
            nitan_odds, nifuku_odds = nifuku_odds, nitan_odds
            # キー形式も修正（"=" ↔ "-"）
            new_nitan = {}
            for k, v in nitan_odds.items():
                a, b = k.split("=") if "=" in k else k.split("-")
                new_nitan[f"{a}-{b}"] = v
            new_nifuku = {}
            for k, v in nifuku_odds.items():
                a, b = k.split("-") if "-" in k else k.split("=")
                lo, hi = sorted([a, b])
                new_nifuku[f"{lo}={hi}"] = v
            nitan_odds = new_nitan
            nifuku_odds = new_nifuku

    return {"2連単": nitan_odds, "2連複": nifuku_odds}


# ─────────────────────────────────────────────
# 5. 締め切り時刻取得
# ─────────────────────────────────────────────
def fetch_deadline(race_no: int, date_str: str | None = None, _soup: BeautifulSoup | None = None) -> str:
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

    soup = _soup
    if not soup:
        url = f"{BASE_URL}/owpc/pc/race/racelist"
        params = {"jcd": _jycd(), "hd": date_str, "rno": 1}
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
# 5b. 女子選手判定（raceindex の is-lady クラス）
# ─────────────────────────────────────────────
def fetch_lady_racers(date_str: str | None = None) -> set[str]:
    """raceindex ページから女子選手の登録番号セットを返す。"""
    if date_str is None:
        date_str = datetime.now().strftime("%Y%m%d")
    soup = _get_soup(f"{BASE_URL}/owpc/pc/race/raceindex",
                     {"jcd": _jycd(), "hd": date_str})
    if not soup:
        return set()
    lady_set: set[str] = set()
    for lady_div in soup.find_all("div", class_="is-lady"):
        td = lady_div.find_parent("td")
        if not td:
            continue
        a_tag = td.find("a", href=re.compile(r"toban=(\d+)"))
        if a_tag:
            m = re.search(r"toban=(\d+)", a_tag["href"])
            if m:
                lady_set.add(m.group(1))
    return lady_set


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
    params = {"jcd": _jycd(), "hd": date_str, "rno": race_no}
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
def fetch_race_grade(race_no: int, date_str: str | None = None, _soup: BeautifulSoup | None = None) -> dict:
    """
    racelist ページからレースグレードと節情報を検出する。

    Returns
    -------
    {"grade": "一般"|"G3"|"G2"|"G1"|"SG", "is_final": bool, "title": str}
    """
    if date_str is None:
        date_str = datetime.now().strftime("%Y%m%d")

    result = {"grade": "一般", "is_final": False, "title": ""}

    soup = _soup if _soup is not None else _fetch_racelist_soup(race_no, date_str)
    if not soup:
        return result

    # タイトル取得
    title_tag = soup.find("h2", class_=re.compile(r"heading2"))
    if title_tag:
        result["title"] = title_tag.get_text(strip=True)

    # グレード判定: heading2_title の is-* クラスから検出（ナビ誤マッチ防止）
    grade_div = soup.find("div", class_=re.compile(r"heading2_title"))
    if grade_div:
        cls_str = " ".join(grade_div.get("class", []))
        grade_class_map = {
            "is-SG": "SG", "is-G1": "G1", "is-PG1": "G1",
            "is-G2": "G2", "is-G3": "G3",
        }
        for cls_key, grade_val in grade_class_map.items():
            if cls_key.lower() in cls_str.lower():
                result["grade"] = grade_val
                break
        # is-ippan or no match → default "一般"

    # フォールバック: title16 テキストからも判定
    if result["grade"] == "一般":
        t16 = soup.find("h3", class_=re.compile(r"title16"))
        if t16:
            t16_text = t16.get_text(strip=True)
            fallback_patterns = [
                (r"ＳＧ|SG", "SG"),
                (r"Ｇ[Ⅰ１1]|G[Ⅰ1]", "G1"),
                (r"Ｇ[Ⅱ２2]|G[Ⅱ2]", "G2"),
                (r"Ｇ[Ⅲ３3]|G[Ⅲ3]", "G3"),
            ]
            for pattern, grade_val in fallback_patterns:
                if re.search(pattern, t16_text):
                    result["grade"] = grade_val
                    break

    # 優勝戦・準優勝戦検出（title16テキストから限定検索）
    race_label = ""
    t16_tag = soup.find("h3", class_=re.compile(r"title16"))
    if t16_tag:
        race_label = t16_tag.get_text(strip=True)
    if re.search(r"優勝戦|ファイナル", race_label):
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
        # コース別3連対率テーブルから1着率・2着率・3着率を取得
        # 実際のHTML構造: 各コース行の <td> 内にCSSバーチャートがあり、
        #   is-progress span の style="width: X%" が各着順率を表す
        #   (1着率=is-progress1, 2着率=is-progress2, 3着率=is-progress3)
        #   テキストラベルは3連対率(合計)のみ表示
        tables = soup.find_all("table")
        for tbl in tables:
            tbl_text = tbl.get_text()
            if "3連対" not in tbl_text or "コース" not in tbl_text:
                continue
            for tr in tbl.find_all("tr"):
                ths = tr.find_all("th")
                course = None
                for th in ths:
                    th_text = th.get_text(strip=True)
                    course_m = re.fullmatch(r'([1-6])', th_text)
                    if course_m:
                        course = course_m.group(1)
                        break
                if not course:
                    continue

                # CSSバーチャートから1着率・2着率・3着率を抽出
                win_rate = None
                rentai_rate = None
                progress_spans = tr.find_all("span", class_="is-progress")
                rates = []
                for span in progress_spans:
                    style = span.get("style", "")
                    m = re.search(r'width:\s*([\d.]+)%', style)
                    if m:
                        rates.append(float(m.group(1)))
                if rates:
                    win_rate = rates[0]                              # 1着率
                    rentai_rate = sum(rates[:2]) if len(rates) >= 2 else win_rate  # 2連対率

                # フォールバック: テキストラベルから3連対率を取得
                rentai3 = None
                label = tr.find("span", class_="table1_progress2Label")
                if label:
                    rentai3 = _to_float(label.get_text(strip=True))

                if win_rate is not None:
                    stats[course] = {
                        "starts":      0,
                        "win_rate":    win_rate,
                        "rentai_rate": rentai_rate or win_rate,
                    }
                elif rentai3 is not None:
                    # CSSから取れなかった場合は3連対率をフォールバック
                    stats[course] = {
                        "starts":      0,
                        "win_rate":    rentai3,
                        "rentai_rate": rentai3,
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

    path = f"time/time{date_str}{_jycd()}{race_no:02d}.htm"
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
# 11b. 住之江公式サイト：オリジナル展示タイム取得
# ─────────────────────────────────────────────

SUMINOE_EXHIBIT_URL = "https://www.boatrace-suminoe.jp/asp/kyogi/12/pc"


def fetch_suminoe_time(race_no: int, date_str: str | None = None) -> pd.DataFrame:
    """
    住之江競艇公式サイトのオリジナル展示ページから展示タイムデータを取得する。

    データソース: /asp/kyogi/12/pc/st02{RR}.htm
    boatrace.jp にない住之江独自計測データ（一周・まわり足）を含む。

    テーブル構造（rowspan=2の2行構成×6艇）:
      [0]枠 [1]選手名 [2]体重 [3]チルト [4]展示 [5]一周 [6]まわり足

    Returns
    -------
    DataFrame with columns: 枠番, 展示タイム_gama, 一周タイム, まわり足タイム, 直線タイム
    ※ 直線タイムは住之江では計測なしのため常にNone。
    空DataFrameの場合はデータ取得失敗。
    """
    url = f"{SUMINOE_EXHIBIT_URL}/st02{race_no:02d}.htm"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        resp.encoding = "utf-8"
    except Exception as e:
        print(f"[suminoe] 展示データ取得失敗 {url}: {e}")
        return pd.DataFrame()

    soup = BeautifulSoup(resp.text, "lxml")
    tbl = soup.find("table", class_="table_solo")
    if not tbl:
        return pd.DataFrame()

    rows = []
    for tbody in tbl.find_all("tbody"):
        trs = tbody.find_all("tr")
        if not trs:
            continue
        first_tr = trs[0]
        tds = first_tr.find_all("td")
        if len(tds) < 7:
            continue

        # td[0]=枠番(waku01等), td[1]=選手名, td[2]=体重, td[3]=チルト, td[4]=展示, td[5]=一周, td[6]=まわり足
        waku_td = tds[0]
        waku_class = waku_td.get("class", [])
        waku = None
        for cls in waku_class:
            m = re.search(r'waku0?(\d)', cls)
            if m:
                waku = m.group(1)
                break
        if not waku:
            waku = waku_td.get_text(strip=True)
        if waku not in ("1", "2", "3", "4", "5", "6"):
            continue

        tenji = _to_float(tds[4].get_text())
        isshu = _to_float(tds[5].get_text())
        mawari = _to_float(tds[6].get_text())

        rows.append({
            "枠番":          waku,
            "展示タイム_gama": tenji,
            "一周タイム":     isshu,
            "まわり足タイム": mawari,
            "直線タイム":     None,
        })

    return pd.DataFrame(rows)


# ─────────────────────────────────────────────
# 11c. 大村公式サイト：オリジナル展示タイム取得
# ─────────────────────────────────────────────

OMURA_EXHIBIT_URL = "https://omurakyotei.jp/yosou/include/new_top_iframe_chokuzen_2.php"


def fetch_omura_time(race_no: int, date_str: str | None = None) -> pd.DataFrame:
    """
    大村競艇公式サイト (omurakyotei.jp) の直前情報iframeから展示タイムデータを取得する。

    データソース: /yosou/include/new_top_iframe_chokuzen_2.php?day=YYYYMMDD&race=RR

    テーブル構造（th2列 + td9列）:
      th: [0]枠番 [1]選手名
      td: [0]ST [1]展示T [2]一周 [3]回り足 [4]直線 [5]チルト [6]部品交換 [7]スタート [8]展示評価

    Returns
    -------
    DataFrame with columns: 枠番, 展示タイム_gama, 一周タイム, まわり足タイム, 直線タイム
    空DataFrameの場合はデータ取得失敗。
    """
    if date_str is None:
        date_str = datetime.now().strftime("%Y%m%d")

    url = f"{OMURA_EXHIBIT_URL}?day={date_str}&race={race_no:02d}"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        resp.encoding = "utf-8"
    except Exception as e:
        print(f"[omura] 展示データ取得失敗 {url}: {e}")
        return pd.DataFrame()

    soup = BeautifulSoup(resp.text, "lxml")
    tbl = soup.find("table")
    if not tbl:
        return pd.DataFrame()

    # テーブル構造: 各艇は th(tei1〜tei6) + td9列
    # td: [0]ST [1]展示T [2]一周 [3]回り足 [4]直線 [5]チルト [6]部品交換 [7]スタート [8]評価
    rows = []
    for tr in tbl.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 5:
            continue

        # 枠番をth要素のCSSクラスから取得 (tei1〜tei6)
        th = tr.find("th")
        waku = None
        if th:
            for cls in th.get("class", []):
                m = re.search(r'tei(\d)', cls)
                if m:
                    waku = m.group(1)
                    break
        if waku not in ("1", "2", "3", "4", "5", "6"):
            continue

        tenji    = _to_float(tds[1].get_text())
        isshu    = _to_float(tds[2].get_text())
        mawari   = _to_float(tds[3].get_text())
        chokusen = _to_float(tds[4].get_text())

        rows.append({
            "枠番":          waku,
            "展示タイム_gama": tenji,
            "一周タイム":     isshu,
            "まわり足タイム": mawari,
            "直線タイム":     chokusen,
        })

    if rows:
        print(f"[omura] 展示タイム取得成功: {len(rows)}艇")
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
    path = f"takahashi/takahashi{date_str}{_jycd()}{race_no:02d}.htm"
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
            "当地勝率": 5.5 + random.uniform(-1, 1),
            "当地2連率": 35.0 + random.uniform(-5, 5),
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


# ─────────────────────────────────────────────
# 14. 選手別決まり手データ取得（kyoteibiyori.com）
# ─────────────────────────────────────────────
_KYOTEI_BASE = "https://kyoteibiyori.com"
_KYOTEI_HEADERS = {
    "User-Agent": HEADERS["User-Agent"],
    "Referer": "https://kyoteibiyori.com/",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "X-Requested-With": "XMLHttpRequest",
}


def _parse_henko_html(html: str, course: int) -> dict | None:
    """
    request_racer_henko.php の HTML レスポンスを解析し、
    指定コースの決まり手データを返す（直近6ヶ月データ使用）。

    HTML テーブル構造（新概念データ）:
        各グループはラベル行（shusso_title / shusso_title_make クラス）で始まり、
        続く2行がデータ行（1行目=直近1年, 2行目=直近6ヶ月）。
        Group 1: 逃げ/逃し — コース1-2のみ（2値）
        Group 2: 差され/差し — 全6コース
        Group 3: 捲られ/捲り — 全6コース
        Group 4: 捲られ差/捲り差し — 全6コース
    各グループのデータ配列:
        [0]=コース1, [1]=コース2, ... [5]=コース6
    コース1: 逃げ(勝ち視点), 差され・捲られ・捲られ差(負け視点)
    コース2-6: 差し・捲り・捲り差し(勝ち視点)
    """
    soup = BeautifulSoup(html, "html.parser")
    rows = soup.find_all("tr")
    if len(rows) < 7:
        return None

    def _extract_pcts(tr_tag) -> list[float]:
        """1行の全TDセルからパーセンテージ数値をリストで抽出。"""
        pcts = []
        for c in tr_tag.find_all("td"):
            text = c.get_text(strip=True).replace("%", "")
            try:
                pcts.append(float(text))
            except ValueError:
                pass
        return pcts

    def _is_label_row(tr_tag) -> bool:
        """shusso_title / shusso_title_make クラスを含むラベル行か判定。"""
        for td in tr_tag.find_all("td"):
            cls = td.get("class") or []
            if "shusso_title" in cls or "shusso_title_make" in cls:
                return True
        return False

    # ラベル行を検出して各グループの6ヶ月データを取得
    # 各グループ: ラベル行 → 1年データ行 → 6ヶ月データ行
    group_data_6m = []
    for i, row in enumerate(rows):
        if _is_label_row(row) and i + 2 < len(rows):
            data_6m = _extract_pcts(rows[i + 2])  # 2行後 = 6ヶ月データ
            if data_6m:
                group_data_6m.append(data_6m)

    if len(group_data_6m) < 4:
        print(f"[scraper] 決まり手HTML解析: グループ数不足 ({len(group_data_6m)}/4)")
        return None

    g1_6m = group_data_6m[0]  # 逃げ/逃し（2値: コース1, コース2）
    g2_6m = group_data_6m[1]  # 差され/差し（6値: コース1-6）
    g3_6m = group_data_6m[2]  # 捲られ/捲り（6値: コース1-6）
    g4_6m = group_data_6m[3]  # 捲られ差/捲り差し（6値: コース1-6）

    if course == 1:
        nige = g1_6m[0] if len(g1_6m) > 0 else 0.0
        sasare = g2_6m[0] if len(g2_6m) > 0 else 0.0
        makurare = g3_6m[0] if len(g3_6m) > 0 else 0.0
        makurare_sashi = g4_6m[0] if len(g4_6m) > 0 else 0.0
        return {
            "逃げ": nige,
            "差し": 0.0,
            "まくり": 0.0,
            "まくり差し": 0.0,
            "抜き": 0.0,
            "恵まれ": 0.0,
            "差され": sasare,
            "捲られ": makurare,
            "捲られ差": makurare_sashi,
        }
    else:
        idx = course - 1  # 0-indexed: コース2→idx=1, コース3→idx=2, ...
        sashi = g2_6m[idx] if len(g2_6m) > idx else 0.0
        makuri = g3_6m[idx] if len(g3_6m) > idx else 0.0
        makuri_sashi = g4_6m[idx] if len(g4_6m) > idx else 0.0
        return {
            "逃げ": 0.0,
            "差し": sashi,
            "まくり": makuri,
            "まくり差し": makuri_sashi,
            "抜き": 0.0,
            "恵まれ": 0.0,
        }


def fetch_racer_kimarite(
    race_no: int,
    date_str: str,
    df_race_card: pd.DataFrame | None = None,
    course_map: dict | None = None,
) -> dict:
    """
    kyoteibiyori.com の新概念データ API から選手別・枠別の決まり手データを取得する。

    各選手のコース（進入コース）における直近6ヶ月の決まり手分布
    （逃げ/差し/まくり/まくり差し）をパーセンテージで返す。

    Parameters
    ----------
    race_no : レース番号 (1-12)
    date_str : 日付文字列 "YYYYMMDD"
    df_race_card : 出走表DataFrame（登録番号を含む）。Noneの場合は内部で取得する。
    course_map : 展示進入マッピング {枠番文字列: 進入コース番号(1-6)}。
                 Noneの場合は枠番=コースとして扱う。

    Returns
    -------
    {
        "1": {"逃げ": 45.2, "差し": 0.0, "まくり": 0.0, "まくり差し": 0.0,
              "抜き": 0.0, "恵まれ": 0.0, "レース数": 0},
        "2": {...}, ... "6": {...}
    }
    空辞書の場合はデータ取得失敗。
    """
    import json

    # 出走表から登録番号を取得
    if df_race_card is None or df_race_card.empty:
        df_race_card = fetch_race_card(race_no, date_str)
    if df_race_card.empty or "登録番号" not in df_race_card.columns:
        print("[scraper] 選手別決まり手: 出走表に登録番号なし")
        return {}

    result = {}

    def _fetch_one(frame_no: str, player_no: str, course_no: int):
        """1選手分の決まり手データを取得する。"""
        try:
            reqdata = json.dumps({
                "mode": 0,
                "player_no": str(player_no),
                "grade": 1,  # 1=総合
            })
            resp = requests.post(
                f"{_KYOTEI_BASE}/racer/request_racer_henko.php",
                data={"data": reqdata},
                headers={
                    **_KYOTEI_HEADERS,
                    "Referer": f"{_KYOTEI_BASE}/racer/racer_no/{player_no}",
                },
                timeout=10,
            )
            if resp.status_code != 200:
                return frame_no, None

            data = _parse_henko_html(resp.text, course_no)
            return frame_no, data
        except Exception as e:
            print(f"[scraper] 選手別決まり手取得エラー ({frame_no}号艇): {e}")
            return frame_no, None

    # 6選手を並列で取得
    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = []
        for _, row in df_race_card.iterrows():
            frame_no = str(row.get("枠番", ""))
            player_no = row.get("登録番号")
            if frame_no and player_no:
                # 展示進入マッピングがあればそのコース、なければ枠番=コース
                course_no = int(course_map[frame_no]) if course_map and frame_no in course_map else int(frame_no)
                futures.append(executor.submit(_fetch_one, frame_no, str(player_no), course_no))

        for future in as_completed(futures):
            frame_no, data = future.result()
            if data is not None:
                result[frame_no] = data

    if result:
        cm_info = " (展示進入反映)" if course_map else ""
        print(f"[scraper] 選手別決まり手: {len(result)}名分取得成功{cm_info}")
    else:
        print("[scraper] 選手別決まり手: データ取得失敗（フォールバックなし）")

    return result


# ─────────────────────────────────────────────
# レース結果取得
# ─────────────────────────────────────────────
def fetch_race_result(race_no: int, date_str: str | None = None) -> dict | None:
    """レース結果を取得する。レースが終了していない場合は None を返す。

    Returns:
        dict with keys:
            - "着順": list of dict (着, 枠番, 選手名, レースタイム)
            - "決まり手": str
            - "払戻": dict (勝式 -> {"組番": str, "払戻金": int, "人気": int})
        or None if race not finished.
    """
    if date_str is None:
        date_str = datetime.now().strftime("%Y%m%d")

    url = f"{BASE_URL}/owpc/pc/race/raceresult"
    params = {"jcd": _jycd(), "hd": date_str, "rno": race_no}
    soup = _get_soup(url, params)
    if not soup:
        return None

    # ── 着順テーブル (最初の table.is-w495) ──
    result_tables = soup.find_all("table", class_="is-w495")
    if not result_tables:
        return None

    result_tbl = result_tables[0]
    headers = [th.get_text(strip=True) for th in result_tbl.find_all("th")]
    if "着" not in headers:
        return None

    finishers = []
    for tbody in result_tbl.find_all("tbody"):
        tr = tbody.find("tr")
        if not tr:
            continue
        tds = tr.find_all("td")
        if len(tds) < 3:
            continue
        rank_text = tds[0].get_text(strip=True)
        rank_map = {"１": 1, "２": 2, "３": 3, "４": 4, "５": 5, "６": 6}
        rank = rank_map.get(rank_text)
        if rank is None:
            m = re.search(r'\d+', rank_text)
            rank = int(m.group()) if m else None
        if rank is None:
            continue

        frame_text = tds[1].get_text(strip=True)
        frame = int(frame_text) if frame_text.isdigit() else None

        name_span = tds[2].find("span", class_=re.compile("is-fs18"))
        player_name = name_span.get_text(strip=True).replace('\u3000', '') if name_span else ""

        race_time = tds[3].get_text(strip=True) if len(tds) > 3 else ""

        finishers.append({
            "着": rank, "枠番": frame,
            "選手名": player_name, "レースタイム": race_time,
        })

    if not finishers:
        return None

    # ── 決まり手 ──
    kimarite = ""
    for tbl in soup.find_all("table"):
        ths = [th.get_text(strip=True) for th in tbl.find_all("th")]
        if "決まり手" in ths:
            td = tbl.find("td")
            if td:
                kimarite = td.get_text(strip=True)
            break

    # ── 払戻金テーブル ──
    payouts = {}
    for tbl in soup.find_all("table", class_="is-w495"):
        ths = [th.get_text(strip=True) for th in tbl.find_all("th")]
        if "勝式" not in ths or "払戻金" not in ths:
            continue
        current_type = None
        for tbody in tbl.find_all("tbody"):
            for tr in tbody.find_all("tr"):
                tds = tr.find_all("td")
                if not tds:
                    continue
                first_text = tds[0].get_text(strip=True)
                if first_text in ("3連単", "3連複", "2連単", "2連複", "拡連複", "単勝", "複勝"):
                    current_type = first_text
                    combo_td = tds[1] if len(tds) > 1 else None
                    payout_td = tds[2] if len(tds) > 2 else None
                    ninki_td = tds[3] if len(tds) > 3 else None
                else:
                    combo_td = tds[0] if len(tds) > 0 else None
                    payout_td = tds[1] if len(tds) > 1 else None
                    ninki_td = tds[2] if len(tds) > 2 else None

                if not current_type or not combo_td:
                    continue

                numbers = combo_td.find_all("span", class_=re.compile("numberSet1_number"))
                separators = combo_td.find_all("span", class_="numberSet1_text")
                if not numbers:
                    continue
                num_strs = [n.get_text(strip=True) for n in numbers]
                sep_strs = [s.get_text(strip=True) for s in separators]
                combo = ""
                for i, n in enumerate(num_strs):
                    combo += n
                    if i < len(sep_strs):
                        combo += sep_strs[i]
                if not combo:
                    continue

                payout_val = 0
                if payout_td:
                    pt = re.sub(r'[¥\\,\s]', '', payout_td.get_text(strip=True))
                    m = re.search(r'\d+', pt)
                    payout_val = int(m.group()) if m else 0

                ninki_val = 0
                if ninki_td:
                    m = re.search(r'\d+', ninki_td.get_text(strip=True))
                    ninki_val = int(m.group()) if m else 0

                if payout_val > 0:
                    if current_type in payouts and current_type in ("拡連複", "複勝"):
                        if not isinstance(payouts[current_type], list):
                            payouts[current_type] = [payouts[current_type]]
                        payouts[current_type].append({
                            "組番": combo, "払戻金": payout_val, "人気": ninki_val,
                        })
                    else:
                        payouts[current_type] = {
                            "組番": combo, "払戻金": payout_val, "人気": ninki_val,
                        }
        break

    return {
        "着順": finishers,
        "決まり手": kimarite,
        "払戻": payouts,
    }
