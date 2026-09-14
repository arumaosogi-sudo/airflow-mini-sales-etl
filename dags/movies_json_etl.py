"""
Movies JSON ETL  —  MongoDB document (JSON, NoSQL)  ->  PostgreSQL (normalized tables)

    create_movie_tables -> load_movies -> export_sql_file -> quality_check_movies
          (SQL)           (Python)         (Python)              (SQL check)

- อ่านทุกไฟล์ใน include/data/movies/*.json  (1 ไฟล์ = 1 document ของ MongoDB)
- movies_lib แปลง document -> แถวของ 6 ตาราง (object ซ้อน -> คอลัมน์, array -> ตารางลูก)
- โหลดด้วย PostgresHook.insert_rows ใน transaction เดียว (ลบของเดิมก่อน = idempotent)
- เขียนไฟล์ include/output/movies_insert.sql ไว้เป็นหลักฐาน/ส่งต่อ
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLCheckOperator, SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from movies_lib import document_to_rows, documents_to_sql, load_documents   # dags/ อยู่ใน sys.path อยู่แล้ว

CONN_ID = "postgres_dw"
JSON_DIR = "/opt/airflow/include/data/movies"
SQL_OUT = "/opt/airflow/include/output/movies_insert.sql"

CHILD_TABLES = ["movie_genres", "movie_cast", "movie_directors", "movie_languages", "movie_countries"]


def load_movies() -> int:
    """JSON -> rows -> PostgreSQL. คืนจำนวน document ที่โหลด"""
    docs = load_documents(JSON_DIR)
    print(f"found {len(docs)} json documents in {JSON_DIR}")

    hook = PostgresHook(postgres_conn_id=CONN_ID)
    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            for doc in docs:
                rows = document_to_rows(doc)
                movie = rows["movies"]

                # idempotent: ลบ movie เดิม (ตารางลูกหายตาม CASCADE) แล้วใส่ใหม่
                cur.execute("DELETE FROM movies WHERE movie_id = %s", (movie["movie_id"],))

                cols = list(movie.keys())
                cur.execute(
                    f"INSERT INTO movies ({', '.join(cols)}) VALUES ({', '.join(['%s'] * len(cols))})",
                    [movie[c] for c in cols],
                )
                for table in CHILD_TABLES:
                    for r in rows[table]:
                        c = list(r.keys())
                        cur.execute(
                            f"INSERT INTO {table} ({', '.join(c)}) VALUES ({', '.join(['%s'] * len(c))})",
                            [r[k] for k in c],
                        )
                print(f"loaded '{movie['title']}' ({movie['movie_id']}) "
                      f"genres={len(rows['movie_genres'])} cast={len(rows['movie_cast'])}")
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return len(docs)


def export_sql_file() -> str:
    """สร้างไฟล์ .sql (INSERT statements) จาก JSON เดียวกัน ไว้เป็นเอกสาร"""
    docs = load_documents(JSON_DIR)
    Path(SQL_OUT).parent.mkdir(parents=True, exist_ok=True)
    Path(SQL_OUT).write_text(documents_to_sql(docs), encoding="utf-8")
    print(f"wrote {SQL_OUT}")
    return SQL_OUT


with DAG(
    dag_id="movies_json_etl",
    description="MongoDB JSON documents -> PostgreSQL normalized tables",
    start_date=datetime(2026, 9, 1),
    schedule=None,                      # รันเมื่อกด Trigger เท่านั้น (ข้อมูลไม่ได้มาเป็นรอบ)
    catchup=False,
    default_args={"owner": "oly", "retries": 1, "retry_delay": timedelta(minutes=1)},
    template_searchpath=["/opt/airflow/include"],
    tags=["demo", "nosql", "json", "sql"],
) as dag:

    create_tables = SQLExecuteQueryOperator(
        task_id="create_movie_tables",
        conn_id=CONN_ID,
        sql="sql/create_movie_tables.sql",
    )

    load = PythonOperator(task_id="load_movies", python_callable=load_movies)

    export_sql = PythonOperator(task_id="export_sql_file", python_callable=export_sql_file)

    quality = SQLCheckOperator(
        task_id="quality_check_movies",
        conn_id=CONN_ID,
        sql="""
            SELECT
                COUNT(*) > 0                                                    AS has_movies,
                COUNT(*) FILTER (WHERE title IS NULL OR year IS NULL) = 0       AS required_fields_present,
                -- ทุกหนังต้องมีอย่างน้อย 1 genre และ 1 director
                NOT EXISTS (SELECT 1 FROM movies m
                            WHERE NOT EXISTS (SELECT 1 FROM movie_genres g WHERE g.movie_id = m.movie_id)) AS every_movie_has_genre,
                NOT EXISTS (SELECT 1 FROM movies m
                            WHERE NOT EXISTS (SELECT 1 FROM movie_directors d WHERE d.movie_id = m.movie_id)) AS every_movie_has_director
            FROM movies;
        """,
    )

    create_tables >> load >> export_sql >> quality
