"""
movies_lib — แปลง MongoDB document (JSON) ของ sample_mflix.movies ให้เป็นแถวสำหรับตาราง SQL

ใช้ได้ 2 ทาง:
  - จาก DAG (movies_json_etl.py) เพื่อโหลดเข้า PostgreSQL ด้วย PostgresHook
  - จาก scripts/convert_movies.py เพื่อสร้างไฟล์ .sql (INSERT statements) ไว้ดู/ส่ง
"""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path


# ---------------------------------------------------------------------------
# แปลงชนิดพิเศษของ MongoDB Extended JSON
# ---------------------------------------------------------------------------
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def parse_mongo_date(value):
    """
    รับได้ทั้ง 3 รูปแบบ:
      {"$date": {"$numberLong": "-2085523200000"}}   -> ms นับจาก 1970 (ติดลบ = ก่อน 1970)
      {"$date": "2015-08-08T19:16:10.000Z"}          -> ISO string
      None
    คืน datetime (UTC) หรือ None
    """
    if not value:
        return None
    inner = value.get("$date", value) if isinstance(value, dict) else value
    if isinstance(inner, dict) and "$numberLong" in inner:
        return _EPOCH + timedelta(milliseconds=int(inner["$numberLong"]))
    if isinstance(inner, str):
        return datetime.fromisoformat(inner.replace("Z", "+00:00"))
    return None


def parse_lastupdated(value):
    """'2015-08-13 00:27:59.177000000' (นาโนวินาที 9 หลัก) -> datetime (Python รับได้แค่ 6 หลัก)"""
    if not value:
        return None
    if "." in value:
        head, frac = value.split(".")
        value = f"{head}.{frac[:6]}"
    return datetime.fromisoformat(value)


# ---------------------------------------------------------------------------
# document -> rows
# ---------------------------------------------------------------------------
def document_to_rows(doc: dict) -> dict:
    """
    รับ 1 document คืน dict ของแถวที่จะ insert:
      {"movies": {...1 แถว...}, "movie_genres": [...], "movie_cast": [...], ...}
    ใช้ .get() ทุกที่เพราะ document ของ Mongo ไม่บังคับให้มีทุก field (schemaless)
    """
    awards = doc.get("awards") or {}
    imdb = doc.get("imdb") or {}
    tomatoes = doc.get("tomatoes") or {}
    viewer = tomatoes.get("viewer") or {}
    critic = tomatoes.get("critic") or {}

    movie_id = doc["_id"]["$oid"]
    released = parse_mongo_date(doc.get("released"))

    movie_row = {
        "movie_id": movie_id,
        "title": doc.get("title"),
        "type": doc.get("type"),
        "year": doc.get("year"),
        "rated": doc.get("rated"),
        "runtime_min": doc.get("runtime"),
        "released": released.date() if released else None,
        "plot": doc.get("plot"),
        "fullplot": doc.get("fullplot"),
        "poster_url": doc.get("poster"),
        "num_mflix_comments": doc.get("num_mflix_comments"),
        "lastupdated": parse_lastupdated(doc.get("lastupdated")),
        "awards_wins": awards.get("wins"),
        "awards_nominations": awards.get("nominations"),
        "awards_text": awards.get("text"),
        "imdb_id": imdb.get("id"),
        "imdb_rating": imdb.get("rating"),
        "imdb_votes": imdb.get("votes"),
        "tomatoes_viewer_rating": viewer.get("rating"),
        "tomatoes_viewer_reviews": viewer.get("numReviews"),
        "tomatoes_viewer_meter": viewer.get("meter"),
        "tomatoes_critic_rating": critic.get("rating"),
        "tomatoes_critic_reviews": critic.get("numReviews"),
        "tomatoes_critic_meter": critic.get("meter"),
        "tomatoes_fresh": tomatoes.get("fresh"),
        "tomatoes_rotten": tomatoes.get("rotten"),
        "tomatoes_lastupdated": parse_mongo_date(tomatoes.get("lastUpdated")),
    }

    # array -> ตารางลูก (บางอันเก็บลำดับด้วยเพราะลำดับมีความหมาย เช่น นักแสดงนำ)
    return {
        "movies": movie_row,
        "movie_genres": [{"movie_id": movie_id, "genre": g} for g in doc.get("genres", [])],
        "movie_cast": [
            {"movie_id": movie_id, "position": i, "actor_name": a}
            for i, a in enumerate(doc.get("cast", []), start=1)
        ],
        "movie_directors": [
            {"movie_id": movie_id, "position": i, "director_name": d}
            for i, d in enumerate(doc.get("directors", []), start=1)
        ],
        "movie_languages": [{"movie_id": movie_id, "language": l} for l in doc.get("languages", [])],
        "movie_countries": [{"movie_id": movie_id, "country": c} for c in doc.get("countries", [])],
    }


def load_documents(folder: str) -> list[dict]:
    """อ่านทุกไฟล์ .json ในโฟลเดอร์ (เรียงชื่อไฟล์) คืน list ของ document"""
    docs = []
    for path in sorted(Path(folder).glob("*.json")):
        with open(path, encoding="utf-8") as f:
            docs.append(json.load(f))
    return docs


# ---------------------------------------------------------------------------
# rows -> SQL text  (สำหรับสร้างไฟล์ .sql ให้คนอ่าน)
# ---------------------------------------------------------------------------
def sql_literal(value) -> str:
    """แปลงค่า Python เป็น literal ของ SQL (escape ' ด้วยการซ้ำเป็น '')"""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def rows_to_insert_sql(table: str, rows: list[dict]) -> str:
    if not rows:
        return ""
    cols = list(rows[0].keys())
    values = ",\n".join("    (" + ", ".join(sql_literal(r[c]) for c in cols) + ")" for r in rows)
    return f"INSERT INTO {table} ({', '.join(cols)}) VALUES\n{values};\n"


def documents_to_sql(docs: list[dict]) -> str:
    """รวมทุก document เป็นสคริปต์ SQL เดียว (ลบของเดิมก่อนแล้ว insert ใหม่ = idempotent)"""
    tables = ["movies", "movie_genres", "movie_cast", "movie_directors", "movie_languages", "movie_countries"]
    collected = {t: [] for t in tables}
    for doc in docs:
        rows = document_to_rows(doc)
        collected["movies"].append(rows["movies"])
        for t in tables[1:]:
            collected[t].extend(rows[t])

    ids = ", ".join(sql_literal(r["movie_id"]) for r in collected["movies"])
    parts = [
        "-- generated by movies_lib.documents_to_sql — อย่าแก้มือ",
        "BEGIN;",
        f"DELETE FROM movies WHERE movie_id IN ({ids});   -- ตารางลูกหายตามด้วย ON DELETE CASCADE",
        "",
    ]
    parts += [rows_to_insert_sql(t, collected[t]) for t in tables if collected[t]]
    parts.append("COMMIT;")
    return "\n".join(parts)
