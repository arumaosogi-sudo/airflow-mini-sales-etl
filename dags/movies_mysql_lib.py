"""
movies_mysql_lib — ฟังก์ชันแปลงข้อมูลหนัง (MongoDB document) ให้อยู่ในรูปที่โหลดลง MySQL ได้

ขั้นตอน:
  1. parse_document(text)       TXT (JSON จริง หรือ JSON ที่หลุด quote/colon) -> dict
  2. normalize_extended_json()  แปลง {"$oid"}, {"$date"}, {"$numberLong"} ของ MongoDB -> ค่าปกติ
  3. flatten_movie()            document ซ้อนชั้น -> แถวตาราง movies + ตารางลูก (genres, cast, ...)

รันเดี่ยวๆ ก็ได้:  python movies_mysql_lib.py input.txt output.json
"""

from __future__ import annotations

import json
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

# ---------------------------------------------------------------------------
# 1. parse: TXT -> dict
# ---------------------------------------------------------------------------
_STRUCT_OPEN = re.compile(r"^(\S+)\s+([\[{])$")       # key {   /  key [
_KEY_VALUE = re.compile(r"^(\S+)\s+(.*)$")             # key value


def _coerce_scalar(raw: str):
    """แปลง string เป็น number/bool/null ถ้าหน้าตาเป็นอย่างนั้น ไม่งั้นคืน string"""
    if raw in ("null", "None"):
        return None
    if raw == "true":
        return True
    if raw == "false":
        return False
    if re.fullmatch(r"-?\d+", raw):
        return int(raw)
    if re.fullmatch(r"-?\d+\.\d+", raw):
        return float(raw)
    return raw


def parse_loose_document(text: str) -> dict:
    """
    parser สำหรับ JSON ที่ถูกถอด "..." และ : ออก (เช่น copy มาจาก MongoDB Compass แบบ plain text)
    อาศัยว่า 1 บรรทัด = 1 ค่า เสมอ  จึง parse ทีละบรรทัดด้วย stack
    """
    root: dict = {}
    stack: list = [root]          # container ที่กำลังเติมค่า (dict หรือ list)

    # ตัด { ... } ชั้นนอกสุดของ document ทิ้ง (root คือ dict อยู่แล้ว)
    body = text.strip()
    if body.startswith("{") and body.endswith("}"):
        body = body[1:-1]

    for line in body.splitlines():
        line = line.strip()
        if not line:
            continue
        line = line.rstrip(",")   # ตัด , ท้ายบรรทัดทิ้ง

        # ปิด block
        if line in ("}", "]"):
            stack.pop()
            continue

        cur = stack[-1]

        # เปิด block:  key {   หรือ  key [   หรือ (ใน list)  {
        if line in ("{", "["):
            new = {} if line == "{" else []
            cur.append(new)
            stack.append(new)
            continue
        m = _STRUCT_OPEN.match(line)
        if m and isinstance(cur, dict):
            key, bracket = m.groups()
            new = {} if bracket == "{" else []
            cur[key] = new
            stack.append(new)
            continue

        # ค่าใน list: ทั้งบรรทัดคือ 1 item
        if isinstance(cur, list):
            cur.append(_coerce_scalar(line))
            continue

        # key value  (แยกที่ช่องว่างแรก)
        m = _KEY_VALUE.match(line)
        if not m:
            raise ValueError(f"parse error at line: {line!r}")
        key, value = m.groups()
        cur[key] = _coerce_scalar(value)

    if len(stack) != 1:
        raise ValueError("unbalanced brackets in loose document")
    return root


def parse_document(text: str) -> dict:
    """ลอง json.loads ก่อน ถ้าไม่ผ่านค่อยใช้ loose parser"""
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return parse_loose_document(text)


# ---------------------------------------------------------------------------
# 2. normalize: MongoDB Extended JSON -> ค่าปกติ
# ---------------------------------------------------------------------------
_TIME_NO_COLON = re.compile(r"^(\d{4}-\d{2}-\d{2})[T ](\d{2})(\d{2})(\d{2})(\.\d+)?(Z?)$")
_URL_NO_SLASH = re.compile(r"^https?m\.media-amazon\.comimagesM(.+)$")


def _repair_lost_characters(key: str, value):
    """
    ไฟล์ที่หลุด : และ / จะทำให้เวลา/URL เพี้ยน  ซ่อมเฉพาะ pattern ที่รู้แน่ๆ:
      2015-08-13 002759.177  -> 2015-08-13 00:27:59.177
      httpsm.media-amazon.comimagesMxxx.jpg -> https://m.media-amazon.com/images/M/xxx.jpg
    """
    if not isinstance(value, str):
        return value
    m = _TIME_NO_COLON.match(value)
    if m:
        d, hh, mm, ss, frac, z = m.groups()
        sep = "T" if "T" in value else " "
        return f"{d}{sep}{hh}:{mm}:{ss}{frac or ''}{z}"
    if key == "poster":
        m = _URL_NO_SLASH.match(value)
        if m:
            return f"https://m.media-amazon.com/images/M/{m.group(1)}"
    return value


def _mongo_date_to_iso(value) -> str | None:
    """{"$date": "...ISO..."} หรือ {"$date": {"$numberLong": "ms"}} -> 'YYYY-MM-DD HH:MM:SS'"""
    if isinstance(value, dict) and "$numberLong" in value:
        ms = int(value["$numberLong"])
        return (EPOCH + timedelta(milliseconds=ms)).strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, (int, float)):
        return (EPOCH + timedelta(milliseconds=int(value))).strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, str):
        v = _repair_lost_characters("", value).replace("Z", "").replace("T", " ")
        return v[:19]
    return None


def normalize_extended_json(obj, parent_key: str = ""):
    """เดินทั้ง tree: แทน {"$oid"} -> str, {"$date"} -> iso string, {"$numberLong"} -> int, ซ่อม string"""
    if isinstance(obj, dict):
        if "$oid" in obj:
            return str(obj["$oid"])
        if "$date" in obj:
            return _mongo_date_to_iso(obj["$date"])
        if "$numberLong" in obj:
            return int(obj["$numberLong"])
        return {k: normalize_extended_json(v, k) for k, v in obj.items()}
    if isinstance(obj, list):
        return [normalize_extended_json(v, parent_key) for v in obj]
    return _repair_lost_characters(parent_key, obj)


def txt_to_json(src: Path, dst: Path) -> dict:
    """อ่านไฟล์ TXT -> parse -> normalize -> เขียน JSON สะอาด  คืน dict ที่ได้"""
    doc = normalize_extended_json(parse_document(src.read_text(encoding="utf-8")))
    dst.parent.mkdir(parents=True, exist_ok=True)
    dst.write_text(json.dumps(doc, ensure_ascii=False, indent=2), encoding="utf-8")
    return doc


# ---------------------------------------------------------------------------
# 3. flatten: document -> rows สำหรับ MySQL
# ---------------------------------------------------------------------------
def _get(d: dict, *path, default=None):
    for p in path:
        if not isinstance(d, dict) or p not in d:
            return default
        d = d[p]
    return d


def _dt(value: str | None):
    """'2015-08-13 00:27:59.177000000' -> datetime (ตัด fractional ให้เหลือ 6 หลัก)"""
    if not value:
        return None
    value = value.replace("T", " ").replace("Z", "")
    if "." in value:
        head, frac = value.split(".", 1)
        value = f"{head}.{frac[:6]}"
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f")
    return datetime.strptime(value[:19], "%Y-%m-%d %H:%M:%S")


def flatten_movie(doc: dict) -> dict:
    """
    คืน {"movie": {...1 แถว...}, "genres": [...], "cast": [...], "directors": [...],
         "languages": [...], "countries": [...]}
    """
    mongo_id = doc["_id"]
    released = doc.get("released")
    movie = {
        "mongo_id": mongo_id,
        "title": doc.get("title"),
        "year": doc.get("year"),
        "type": doc.get("type"),
        "rated": doc.get("rated"),
        "runtime": doc.get("runtime"),
        "released": released[:10] if released else None,
        "plot": doc.get("plot"),
        "fullplot": doc.get("fullplot"),
        "poster": doc.get("poster"),
        "num_mflix_comments": doc.get("num_mflix_comments"),
        "lastupdated": _dt(doc.get("lastupdated")),
        "imdb_id": _get(doc, "imdb", "id"),
        "imdb_rating": _get(doc, "imdb", "rating"),
        "imdb_votes": _get(doc, "imdb", "votes"),
        "awards_wins": _get(doc, "awards", "wins"),
        "awards_nominations": _get(doc, "awards", "nominations"),
        "awards_text": _get(doc, "awards", "text"),
        "tomatoes_viewer_rating": _get(doc, "tomatoes", "viewer", "rating"),
        "tomatoes_viewer_num_reviews": _get(doc, "tomatoes", "viewer", "numReviews"),
        "tomatoes_viewer_meter": _get(doc, "tomatoes", "viewer", "meter"),
        "tomatoes_critic_rating": _get(doc, "tomatoes", "critic", "rating"),
        "tomatoes_critic_num_reviews": _get(doc, "tomatoes", "critic", "numReviews"),
        "tomatoes_critic_meter": _get(doc, "tomatoes", "critic", "meter"),
        "tomatoes_fresh": _get(doc, "tomatoes", "fresh"),
        "tomatoes_rotten": _get(doc, "tomatoes", "rotten"),
        "tomatoes_last_updated": _dt(_get(doc, "tomatoes", "lastUpdated")),
    }

    def rows(field: str, col: str):
        return [{"mongo_id": mongo_id, "position": i + 1, col: v}
                for i, v in enumerate(doc.get(field) or [])]

    return {
        "movie": movie,
        "genres": rows("genres", "genre"),
        "cast": rows("cast", "actor"),
        "directors": rows("directors", "director"),
        "languages": rows("languages", "language"),
        "countries": rows("countries", "country"),
    }


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit("usage: python movies_mysql_lib.py <input.txt> <output.json>")
    result = txt_to_json(Path(sys.argv[1]), Path(sys.argv[2]))
    print(json.dumps(result, ensure_ascii=False, indent=2))
