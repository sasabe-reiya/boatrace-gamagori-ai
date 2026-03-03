"""
予想ログ・的中率計測モジュール

【機能】
- save_prediction   : 予想実行時に買い目と気象情報をJSONに保存
- record_result     : レース結果（着順）を記録し的中チェックを実行
- get_accuracy_stats: 累積的中率を集計して返す
- get_recent_predictions: 直近N件の予想履歴を返す
"""
import json
from pathlib import Path

LOG_FILE = Path(__file__).parent / "prediction_log.json"


# ────────────────────────────────────────────────────────────────
# 内部ユーティリティ
# ────────────────────────────────────────────────────────────────

def _load() -> list:
    if LOG_FILE.exists():
        try:
            return json.loads(LOG_FILE.read_text(encoding="utf-8"))
        except Exception:
            return []
    return []


def _save(log: list) -> None:
    LOG_FILE.write_text(
        json.dumps(log, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )


# ────────────────────────────────────────────────────────────────
# 公開 API
# ────────────────────────────────────────────────────────────────

def save_prediction(date_str: str, race_no: int,
                    recommendations: list, weather: dict,
                    confidence: str = "-") -> None:
    """
    予想結果を保存する。同日・同レースの既存エントリは上書きする。

    Parameters
    ----------
    date_str        : "YYYYMMDD" 形式の日付文字列
    race_no         : レース番号 (1〜12)
    recommendations : generate_recommendations() の戻り値リスト
    weather         : 気象情報 dict
    confidence      : 信頼度ラベル ("S"/"A"/"B"/"C")
    """
    log = _load()
    # 同日・同レースの既存エントリを削除してから追加（上書き）
    log = [e for e in log
           if not (e["date"] == date_str and e["race_no"] == race_no)]
    log.append({
        "date":            date_str,
        "race_no":         race_no,
        "recommendations": recommendations,
        "weather":         weather,
        "confidence":      confidence,
        "actual":          None,
    })
    log.sort(key=lambda x: (x["date"], x["race_no"]))
    _save(log)


def record_result(date_str: str, race_no: int,
                  result_1: int, result_2: int, result_3: int) -> dict:
    """
    実際の着順を記録し、買い目の的中チェックを行う。

    Returns
    -------
    actual dict: {"1着", "2着", "3着", "三連単", "hit": [タイプ] or None}
    """
    log = _load()
    actual_combo = f"{result_1}-{result_2}-{result_3}"
    actual = {
        "1着": str(result_1),
        "2着": str(result_2),
        "3着": str(result_3),
        "三連単": actual_combo,
        "hit": None,
    }

    for entry in log:
        if entry["date"] == date_str and entry["race_no"] == race_no:
            # 的中チェック: 推奨買い目と実際の3連単を照合
            hits = [
                rec["タイプ"]
                for rec in entry.get("recommendations", [])
                if rec.get("買い目") == actual_combo
            ]
            actual["hit"] = hits if hits else None
            entry["actual"] = actual
            break
    else:
        # 予想エントリが無い日の結果でも記録できるようにする
        log.append({
            "date":            date_str,
            "race_no":         race_no,
            "recommendations": [],
            "weather":         {},
            "confidence":      "-",
            "actual":          actual,
        })

    log.sort(key=lambda x: (x["date"], x["race_no"]))
    _save(log)
    return actual


def get_accuracy_stats() -> dict:
    """
    累積的中率を集計して返す。

    Returns
    -------
    {
      "total"   : 結果記録済みレース数,
      "hits"    : 3連単的中数（いずれかの買い目が的中）,
      "hit_rate": 的中率 (%),
      "by_type" : {"本命": {"total", "hit", "rate"}, ...},
      "by_confidence": {"S": {...}, "A": {...}, ...},
    }
    """
    log = _load()
    judged = [e for e in log if e.get("actual") is not None]
    total  = len(judged)

    if total == 0:
        return {
            "total": 0, "hits": 0, "hit_rate": 0.0,
            "by_type": {}, "by_confidence": {},
        }

    hits = sum(1 for e in judged if e["actual"].get("hit"))

    # タイプ別（本命・対抗・穴）
    type_stats: dict = {}
    for entry in judged:
        hit_types = entry["actual"].get("hit") or []
        for rec in entry.get("recommendations", []):
            t = rec.get("タイプ", "")
            if t not in type_stats:
                type_stats[t] = {"total": 0, "hit": 0}
            type_stats[t]["total"] += 1
            if t in hit_types:
                type_stats[t]["hit"] += 1

    by_type = {
        t: {
            "total": v["total"],
            "hit":   v["hit"],
            "rate":  round(v["hit"] / v["total"] * 100, 1) if v["total"] > 0 else 0.0,
        }
        for t, v in type_stats.items()
    }

    # 信頼度別
    conf_stats: dict = {}
    for entry in judged:
        c = entry.get("confidence", "-")
        # 先頭1文字をキーにする（"S（非常に高い）" → "S"）
        key = str(c)[0] if c and c != "-" else "?"
        if key not in conf_stats:
            conf_stats[key] = {"total": 0, "hit": 0}
        conf_stats[key]["total"] += 1
        if entry["actual"].get("hit"):
            conf_stats[key]["hit"] += 1

    by_confidence = {
        k: {
            "total": v["total"],
            "hit":   v["hit"],
            "rate":  round(v["hit"] / v["total"] * 100, 1) if v["total"] > 0 else 0.0,
        }
        for k, v in sorted(conf_stats.items())
    }

    return {
        "total":          total,
        "hits":           hits,
        "hit_rate":       round(hits / total * 100, 1),
        "by_type":        by_type,
        "by_confidence":  by_confidence,
    }


def get_recent_predictions(n: int = 10) -> list:
    """直近 n 件の予想エントリを新しい順で返す。"""
    log = _load()
    return sorted(log, key=lambda x: (x["date"], x["race_no"]), reverse=True)[:n]
