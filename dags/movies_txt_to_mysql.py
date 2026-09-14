"""
movies_txt_to_mysql  —  TXT (MongoDB document, non-SQL)  ->  JSON  ->  MySQL

ลำดับ task:
    convert_txt_to_json -> create_tables -> load_movies -> quality_check -> notify_done
        (Python)             (SQL/MySQL)     (Python)      (SQL check)     (Python)

ต้นทาง : include/data/movies/raw/*.txt   (document จาก MongoDB — บางไฟล์ JSON หลุด quote/colon)
กลางทาง: include/data/movies/json/*.json (JSON สะอาด แปลง $oid/$date แล้ว)
ปลายทาง: MySQL db "movies" — ตาราง movies + movie_genres/cast/directors/languages/countries

idempotent: ก่อน insert หนังเรื่องใด จะ DELETE แถวเดิมของ mongo_id นั้น (ตารางลูกหายตามด้วย FK CASCADE)
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import (
    SQLCheckOperator,
    SQLExecuteQueryOperator,
)
from airflow.providers.mysql.hooks.mysql import MySqlHook

# movies_mysql_lib.py อยู่ในโฟลเดอร์ dags/ เดียวกัน  Airflow ใส่ dags/ ไว้ใน sys.path ให้อยู่แล้ว
from movies_mysql_lib import flatten_movie, txt_to_json

CONN_ID = "mysql_movies"                                   # ตั้งใน docker-compose (AIRFLOW_CONN_MYSQL_MOVIES)
RAW_DIR = Path("/opt/airflow/include/data/movies/raw")     # path ข้างใน container
JSON_DIR = Path("/opt/airflow/include/data/movies/json")


# ---------------------------------------------------------------------------
# Python callables
# ---------------------------------------------------------------------------
def convert_txt_to_json() -> list[str]:
    """อ่านทุก .txt ใน raw/ -> เขียน .json ใน json/  คืนรายชื่อไฟล์ json ที่สร้าง (ผ่าน XCom)"""
    outputs = []
    for src in sorted(RAW_DIR.glob("*.txt")):
        dst = JSON_DIR / (src.stem + ".json")
        doc = txt_to_json(src, dst)
        print(f"{src.name:28s} -> {dst.name:28s}  _id={doc['_id']}  title={doc['title']!r}")
        outputs.append(str(dst))
    if not outputs:
        raise FileNotFoundError(f"no .txt files found in {RAW_DIR}")
    return outputs


_CHILD_TABLES = {
    # key ใน flatten_movie()  ->  (ชื่อตาราง, ชื่อคอลัมน์ค่า)
    "genres": ("movie_genres", "genre"),
    "cast": ("movie_cast", "actor"),
    "directors": ("movie_directors", "director"),
    "languages": ("movie_languages", "language"),
    "countries": ("movie_countries", "country"),
}


def load_movies(ti) -> int:
    """อ่าน JSON ทุกไฟล์ -> flatten -> insert ลง MySQL ใน transaction เดียว  คืนจำนวนหนังที่โหลด"""
    import json

    json_files = ti.xcom_pull(task_ids="convert_txt_to_json")
    conn = MySqlHook(mysql_conn_id=CONN_ID).get_conn()
    cur = conn.cursor()
    loaded = 0
    try:
        for path in json_files:
            doc = json.loads(Path(path).read_text(encoding="utf-8"))
            rows = flatten_movie(doc)
            movie = rows["movie"]

            # idempotent: ลบของเดิม (ตารางลูกหายตาม ON DELETE CASCADE)
            cur.execute("DELETE FROM movies WHERE mongo_id = %s", (movie["mongo_id"],))

            cols = ", ".join(movie.keys())
            marks = ", ".join(["%s"] * len(movie))
            cur.execute(f"INSERT INTO movies ({cols}) VALUES ({marks})", list(movie.values()))

            for key, (table, col) in _CHILD_TABLES.items():
                if rows[key]:
                    cur.executemany(
                        f"INSERT INTO {table} (mongo_id, position, {col}) VALUES (%s, %s, %s)",
                        [(r["mongo_id"], r["position"], r[col]) for r in rows[key]],
                    )
            loaded += 1
            print(f"loaded {movie['title']!r} ({movie['mongo_id']}): "
                  + ", ".join(f"{k}={len(rows[k])}" for k in _CHILD_TABLES))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
    return loaded


def notify_done(ti) -> None:
    loaded = ti.xcom_pull(task_ids="load_movies")
    hook = MySqlHook(mysql_conn_id=CONN_ID)
    print("=" * 60)
    print(f"movies loaded this run : {loaded}")
    for table in ["movies", *[t for t, _ in _CHILD_TABLES.values()]]:
        n = hook.get_first(f"SELECT COUNT(*) FROM {table}")[0]
        print(f"{table:18s}: {n} rows")
    print("-" * 60)
    for title, year, rating in hook.get_records(
        "SELECT title, year, imdb_rating FROM movies ORDER BY year"
    ):
        print(f"{year}  {title:30s}  imdb {rating}")
    print("=" * 60)


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------
default_args = {
    "owner": "oly",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="movies_txt_to_mysql",
    description="MongoDB document TXT -> JSON -> MySQL (normalized)",
    start_date=datetime(2026, 9, 1),
    schedule=None,                       # รันเมื่อกด Trigger เท่านั้น (ไฟล์ test ไม่ได้มาใหม่ทุกวัน)
    catchup=False,
    default_args=default_args,
    template_searchpath=["/opt/airflow/include"],
    tags=["demo", "etl", "mysql", "json"],
) as dag:

    convert = PythonOperator(
        task_id="convert_txt_to_json",
        python_callable=convert_txt_to_json,
    )

    create_tables = SQLExecuteQueryOperator(
        task_id="create_tables",
        conn_id=CONN_ID,
        sql="sql/mysql/create_movies_tables.sql",
        split_statements=True,           # ไฟล์มีหลาย CREATE TABLE ต้องให้แยกส่งทีละคำสั่ง
        return_last=False,
    )

    load = PythonOperator(
        task_id="load_movies",
        python_callable=load_movies,
    )

    quality_check = SQLCheckOperator(
        task_id="quality_check",
        conn_id=CONN_ID,
        sql="sql/mysql/quality_check_movies.sql",
    )

    done = PythonOperator(
        task_id="notify_done",
        python_callable=notify_done,
    )

    convert >> create_tables >> load >> quality_check >> done
