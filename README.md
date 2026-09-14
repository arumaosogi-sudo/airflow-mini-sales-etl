# Non-SQL → SQL ด้วย Apache Airflow (Mini ETL)

โปรเจกต์ตัวอย่างขนาดเล็กเพื่อแสดงพื้นฐาน Data Engineering: แปลงข้อมูลที่**ไม่ใช่ SQL**
ให้กลายเป็นตารางในฐานข้อมูล SQL โดยใช้ **Apache Airflow 3** จัดลำดับงาน และรันทั้งหมดบน **Docker**

มี 2 pipeline:

| DAG | ต้นทาง (non-SQL) | ปลายทาง (SQL) | จุดที่แสดง |
|---|---|---|---|
| `sales_etl` | CSV ยอดขาย | **PostgreSQL** `raw_sales` → `daily_summary` | ELT, pandas clean, SQL aggregate, schedule รายวัน |
| `movies_txt_to_mysql` | **MongoDB document (TXT)** | **MySQL** `movies` + ตารางลูก 5 ตาราง | TXT → JSON → NoSQL → relational: ซ่อม JSON, แบน object ซ้อน, แตก array, แปลง `$oid`/`$date` |

## Tech stack

| ส่วน | เครื่องมือ |
|---|---|
| Orchestration | Apache Airflow 3.3.1 (LocalExecutor) |
| Database | PostgreSQL 16 (pipeline 1), MySQL 8.4 (pipeline 2) |
| Transform | pandas 2.2 / Python + SQL |
| Runtime | Docker Desktop + WSL2 (Windows 11) |

## โครงสร้างโปรเจกต์

```
.
├── docker-compose.yaml            # Airflow + PostgreSQL x2 (metadata / warehouse) + MySQL
├── Dockerfile                     # image ทางการของ Airflow + pandas
├── requirements-airflow.txt
├── .env                           # AIRFLOW_UID ฯลฯ (ไม่มีรหัสลับ)
├── dags/
│   ├── sales_etl.py               # DAG 1: CSV -> PostgreSQL (5 task)
│   ├── movies_txt_to_mysql.py     # DAG 2: TXT -> JSON -> MySQL (5 task)
│   └── movies_mysql_lib.py        # ตัวแปลง TXT/JSON -> dict -> rows (รันเดี่ยวๆ ได้)
└── include/
    ├── data/
    │   ├── sales.csv              # ต้นทาง DAG 1 (31 แถว มี order ซ้ำ 1 แถวโดยตั้งใจ)
    │   └── movies/
    │       ├── raw/*.txt          # ต้นทาง DAG 2 (document จาก MongoDB — โจทย์ test)
    │       └── json/*.json        # ผลกลางทางที่ DAG 2 สร้าง (JSON สะอาด)
    └── sql/
        ├── create_tables.sql              # PostgreSQL
        ├── transform_daily_summary.sql    # PostgreSQL
        ├── quality_check.sql              # PostgreSQL
        └── mysql/
            ├── create_movies_tables.sql   # MySQL
            └── quality_check_movies.sql   # MySQL
```

---

## Pipeline 1: `sales_etl` — CSV → PostgreSQL → daily summary

```
include/data/sales.csv ──extract+clean (pandas)──▶ raw_sales ──transform (SQL)──▶ daily_summary
        (CSV)                                     (PostgreSQL)                    (PostgreSQL)
                                                                                       │
                                                                                 quality_check (SQL)
                                                                                       │
                                                                                  notify_done
```

| # | task_id | Operator | ทำอะไร |
|---|---|---|---|
| 1 | `create_tables` | `SQLExecuteQueryOperator` | สร้าง `raw_sales`, `daily_summary` ถ้ายังไม่มี |
| 2 | `extract_load_raw` | `PythonOperator` | อ่าน CSV → แปลงวันที่, ตัด order ซ้ำ, คำนวณ `amount` → `TRUNCATE` + `to_sql()` ลง `raw_sales` |
| 3 | `transform_summary` | `SQLExecuteQueryOperator` | `TRUNCATE daily_summary` แล้ว `INSERT … SELECT … GROUP BY date, product` |
| 4 | `quality_check` | `SQLCheckOperator` | เช็ค: มีแถว, ยอดไม่ติดลบ, key ไม่ null, ยอดรวม summary = ยอดรวม raw |
| 5 | `notify_done` | `PythonOperator` | ดึงจำนวนแถวจาก XCom + query สรุปลง log |

- **Schedule:** ทุกวัน 06:00 (`0 6 * * *`) และกด Trigger รันเองได้
- **Idempotent:** ทุกขั้นล้างก่อนใส่ใหม่ → รันซ้ำกี่ครั้งข้อมูลไม่ซ้ำ

Data model:

```
raw_sales                          daily_summary
-----------------------            ---------------------------
order_id    INT  PK                sale_date    DATE  PK ┐
order_date  DATE                   product      TEXT  PK ┘
product     TEXT                   category     TEXT
category    TEXT                   total_orders INT
qty         INT                    total_qty    INT
unit_price  NUMERIC(10,2)          total_amount NUMERIC(12,2)
amount      NUMERIC(12,2)
loaded_at   TIMESTAMP
```

ผลลัพธ์:

```
 sale_date  | qty | amount
------------+-----+---------
 2026-09-08 |   9 |  545.00
 2026-09-09 |  11 |  765.00
 2026-09-10 |  11 |  685.00
 2026-09-11 |  14 |  955.00
 2026-09-12 |  18 | 1175.00
```

---

## Pipeline 2: `movies_txt_to_mysql` — MongoDB document (TXT) → JSON → MySQL

ต้นทางคือ document จาก MongoDB (`sample_mflix.movies`) ที่ได้มาเป็นไฟล์ `.txt` 2 ไฟล์
ซึ่งเป็น **schemaless / nested / มี array** และไฟล์หนึ่ง**ไม่ใช่ JSON ที่ถูกต้อง** (ถูกถอด `"` `:` `/` ออก):

```
dataset_test_1.txt (เสีย)              datasets_test_2.txt (JSON ถูกต้อง)
  _id {                                  "_id": { "$oid": "573a13…" },
    $oid 573a1390f29313caabcd42e8        "released": { "$date": { "$numberLong": "-1895097600000" } },
  },                                     "genres": ["Short", "Drama"],
  released {                             "imdb": { "rating": 6.6, "votes": 1375, "id": 832 },
    $date {                              ...
      $numberLong -2085523200000
    }
  },
  lastupdated 2015-08-13 002759.177000000      ← เวลาหาย ":"
  poster httpsm.media-amazon.comimagesM…       ← URL หาย "://" และ "/"
```

```
raw/*.txt ──convert_txt_to_json──▶ json/*.json ──load_movies──▶ movies (1 แถว/หนัง)
              (Python: parse+repair+normalize)      (Python)      ├─ movie_genres     (1 แถว/genre, เก็บ position)
                                                                  ├─ movie_cast
                                                                  ├─ movie_directors
                                                                  ├─ movie_languages
                                                                  └─ movie_countries
                                                                        │
                                                                  quality_check (SQL)  →  notify_done
```

| # | task_id | Operator | ทำอะไร |
|---|---|---|---|
| 1 | `convert_txt_to_json` | `PythonOperator` | อ่านทุก `raw/*.txt` → ถ้า `json.loads` ไม่ผ่านใช้ parser ซ่อม → แปลง `$oid`/`$date`/`$numberLong` → เขียน `json/*.json` |
| 2 | `create_tables` | `SQLExecuteQueryOperator` | สร้าง `movies` + ตารางลูก 5 ตาราง (FK + `ON DELETE CASCADE`) ใน MySQL |
| 3 | `load_movies` | `PythonOperator` | อ่าน JSON → flatten → `DELETE` หนังเดิม → `INSERT` ทุกตารางใน transaction เดียว |
| 4 | `quality_check` | `SQLCheckOperator` | มีแถว, title ไม่ว่าง, imdb_rating อยู่ใน 0–10, ทุกหนังมี genre, ไม่มี cast กำพร้า |
| 5 | `notify_done` | `PythonOperator` | นับแถวทุกตาราง + list หนังลง log |

กฎการแปลง NoSQL → SQL ที่ใช้ (`dags/movies_mysql_lib.py`):

| ใน MongoDB | ใน MySQL | เหตุผล |
|---|---|---|
| field ธรรมดา (`title`, `year`) | คอลัมน์ | ตรงตัว |
| object ซ้อน (`imdb.rating`, `tomatoes.viewer.meter`) | คอลัมน์ `imdb_rating`, `tomatoes_viewer_meter` | ความสัมพันธ์ 1:1 ไม่ต้องแยกตาราง |
| array (`cast`, `genres`, …) | ตารางลูก + FK + `position` | 1:N — SQL ไม่ควรเก็บ list ในช่องเดียว และรักษาลำดับเดิมไว้ |
| `{"$oid": "…"}` | `CHAR(24)` primary key | ObjectId เป็น hex 24 ตัว |
| `{"$date": {"$numberLong": "-2085523200000"}}` | `DATE` | ms นับจาก 1970 (ติดลบ = ก่อน 1970 → 1903-12-01) |
| `{"$date": "2015-08-08T19:16:10.000Z"}` | `DATETIME` | ISO string |
| field ที่ไม่มีใน document (เช่น `poster`, `tomatoes.critic`) | `NULL` | schemaless — ใช้ `.get()` ทุก field |

รันตัวแปลง TXT → JSON เดี่ยวๆ โดยไม่ต้องเปิด Airflow:

```bash
python dags/movies_mysql_lib.py include/data/movies/raw/dataset_test_1.txt out.json
```

ผลลัพธ์ใน MySQL:

```sql
SELECT m.title, m.year, m.released, m.imdb_rating,
       (SELECT GROUP_CONCAT(genre ORDER BY position) FROM movie_genres g WHERE g.mongo_id = m.mongo_id) AS genres,
       (SELECT COUNT(*) FROM movie_cast c WHERE c.mongo_id = m.mongo_id) AS n_cast
FROM movies m ORDER BY m.year;
```
```
title                    year  released    imdb_rating  genres         n_cast
The Great Train Robbery  1903  1903-12-01  7.4          Short,Western  4
A Corner in Wheat        1909  1909-12-13  6.6          Short,Drama    4
```

---

## วิธีรัน

ต้องมี Docker Desktop (เปิดอยู่) + WSL2

```bash
docker compose up -d --build
```

รอ ~1–2 นาที แล้วเปิด <http://localhost:8080> (user `airflow` / pass `airflow`)
→ เปิดสวิตช์ DAG → กด ▶ Trigger → ดู Grid/Graph view

ดูข้อมูล:

```bash
docker compose exec warehouse psql -U etl -d warehouse -c "SELECT * FROM daily_summary ORDER BY sale_date, product;"
```

```bash
docker compose exec mysql mysql -uetl -petl movies -e "SELECT mongo_id, title, year, released, imdb_rating FROM movies;"
```

หรือต่อด้วย DBeaver:
- PostgreSQL: `localhost:5433` user `etl` pass `etl` db `warehouse`
- MySQL: `localhost:3306` user `etl` pass `etl` db `movies`

ปิดระบบ:

```bash
docker compose down
```

(เพิ่ม `-v` ถ้าต้องการลบข้อมูลใน DB ทั้งหมดเพื่อเริ่มใหม่)

## สิ่งที่โปรเจกต์นี้แสดง

- แนวคิด **ELT**: โหลด raw ก่อน แล้ว transform ด้วย SQL ใน DB
- การแยกชั้นข้อมูล raw / summary
- **Data quality check** เป็นส่วนหนึ่งของ pipeline (ไม่ใช่เช็คมือ)
- **Idempotency** และ retry อัตโนมัติ
- การแปลง **NoSQL (MongoDB document) → relational schema**: repair / flatten / normalize / type mapping
- ใช้ SQL database 2 ค่าย (PostgreSQL, MySQL) ผ่าน Airflow Connection โดยโค้ด DAG ไม่ผูกกับรหัสผ่าน
- Reproducible environment ด้วย Docker Compose — clone แล้ว `up` ได้เลย
