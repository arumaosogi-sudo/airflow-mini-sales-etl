# Non-SQL → SQL ด้วย Apache Airflow (Mini ETL)

โปรเจกต์ตัวอย่างขนาดเล็กเพื่อแสดงพื้นฐาน Data Engineering: แปลงข้อมูลที่**ไม่ใช่ SQL**
ให้กลายเป็นตารางใน **PostgreSQL** โดยใช้ **Apache Airflow 3** จัดลำดับงาน และรันทั้งหมดบน **Docker**

มี 2 pipeline:

| DAG | ต้นทาง (non-SQL) | ปลายทาง (SQL) | จุดที่แสดง |
|---|---|---|---|
| `sales_etl` | CSV ยอดขาย | `raw_sales` → `daily_summary` | ELT, pandas clean, SQL aggregate, schedule รายวัน |
| `movies_json_etl` | **MongoDB document (JSON)** | `movies` + ตารางลูก 5 ตาราง | NoSQL → relational: แบน object ซ้อน, แตก array, แปลง `$oid`/`$date` |

## Pipeline 1: `sales_etl` — CSV → daily summary

```
include/data/sales.csv ──extract+clean (pandas)──▶ raw_sales ──transform (SQL)──▶ daily_summary
        (CSV)                                     (PostgreSQL)                    (PostgreSQL)
                                                                                       │
                                                                                 quality_check (SQL)
                                                                                       │
                                                                                  notify_done
```

## Tech stack

| ส่วน | เครื่องมือ |
|---|---|
| Orchestration | Apache Airflow 3.3.1 (LocalExecutor) |
| Database | PostgreSQL 16 |
| Transform | pandas 2.2 (clean) + SQL (aggregate) |
| Runtime | Docker Desktop + WSL2 (Windows 11) |

## โครงสร้างโปรเจกต์

```
.
├── docker-compose.yaml            # Airflow + PostgreSQL x2 (metadata / warehouse)
├── Dockerfile                     # image ทางการของ Airflow + pandas
├── requirements-airflow.txt
├── .env                           # AIRFLOW_UID ฯลฯ (ไม่มีรหัสลับ)
├── dags/
│   ├── sales_etl.py               # DAG 1: CSV -> PostgreSQL (5 task)
│   ├── movies_json_etl.py         # DAG 2: MongoDB JSON -> PostgreSQL (4 task)
│   └── movies_lib.py              # ตัวแปลง document -> rows / -> SQL text (ใช้ร่วมกับ scripts/)
├── scripts/
│   └── convert_movies.py          # รันเดี่ยวไม่ต้องมี Airflow: JSON -> include/output/movies_insert.sql
└── include/
    ├── data/
    │   ├── sales.csv              # ต้นทาง DAG 1 (31 แถว มี order ซ้ำ 1 แถวโดยตั้งใจ)
    │   └── movies/*.json          # ต้นทาง DAG 2 (MongoDB Extended JSON, 1 ไฟล์ = 1 document)
    ├── output/movies_insert.sql   # ผลลัพธ์ที่ DAG 2 สร้าง: INSERT statements
    └── sql/
        ├── create_tables.sql
        ├── transform_daily_summary.sql
        ├── quality_check.sql
        └── create_movie_tables.sql
```

## DAG: `sales_etl`

| # | task_id | Operator | ทำอะไร |
|---|---|---|---|
| 1 | `create_tables` | `SQLExecuteQueryOperator` | สร้าง `raw_sales`, `daily_summary` ถ้ายังไม่มี |
| 2 | `extract_load_raw` | `PythonOperator` | อ่าน CSV → แปลงวันที่, ตัด order ซ้ำ, คำนวณ `amount` → `TRUNCATE` + `to_sql()` ลง `raw_sales` |
| 3 | `transform_summary` | `SQLExecuteQueryOperator` | `TRUNCATE daily_summary` แล้ว `INSERT … SELECT … GROUP BY date, product` |
| 4 | `quality_check` | `SQLCheckOperator` | เช็ค: มีแถว, ยอดไม่ติดลบ, key ไม่ null, ยอดรวม summary = ยอดรวม raw |
| 5 | `notify_done` | `PythonOperator` | ดึงจำนวนแถวจาก XCom + query สรุปลง log |

- **Schedule:** ทุกวัน 06:00 (`0 6 * * *`) และกด Trigger รันเองได้
- **Idempotent:** ทุกขั้นล้างก่อนใส่ใหม่ → รันซ้ำกี่ครั้งข้อมูลไม่ซ้ำ
- **Connection:** `postgres_dw` ตั้งผ่าน env `AIRFLOW_CONN_POSTGRES_DW` ใน compose (ไม่ต้องตั้งใน UI)

## Data model

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

## Pipeline 2: `movies_json_etl` — MongoDB JSON → PostgreSQL

ต้นทางคือ document จาก MongoDB (`sample_mflix.movies`) ซึ่งเป็น **schemaless / nested / มี array**
เป้าหมายคือทำให้อยู่ในรูป **relational** ที่ query ด้วย SQL ได้

```
include/data/movies/*.json ──movies_lib.document_to_rows──▶ movies (1 แถว/หนัง)
   {                                                       ├─ movie_genres     (1 แถว/genre)
     "_id": {"$oid": ...},        ── $oid ──▶ TEXT PK       ├─ movie_cast       (เก็บ position = ลำดับใน array)
     "released": {"$date":        ── ms epoch ──▶ DATE      ├─ movie_directors
        {"$numberLong": "-2085523200000"}},                 ├─ movie_languages
     "genres": ["Short","Western"],  ── array ──▶ ตารางลูก  └─ movie_countries
     "imdb": {"rating": 7.4, ...},   ── object ──▶ imdb_rating, imdb_votes, imdb_id
     ...
   }
```

| # | task_id | ทำอะไร |
|---|---|---|
| 1 | `create_movie_tables` | สร้าง `movies` + ตารางลูก 5 ตาราง (FK + `ON DELETE CASCADE`) |
| 2 | `load_movies` | อ่านทุก `.json` → แปลง → `DELETE` หนังเดิม → `INSERT` ทุกตารางใน transaction เดียว |
| 3 | `export_sql_file` | เขียน `include/output/movies_insert.sql` (INSERT statements) จาก JSON เดียวกัน |
| 4 | `quality_check_movies` | มีแถว, `title`/`year` ไม่ null, ทุกหนังมีอย่างน้อย 1 genre และ 1 director |

กฎการแปลง NoSQL → SQL ที่ใช้:

| ใน MongoDB | ใน PostgreSQL | เหตุผล |
|---|---|---|
| field ธรรมดา (`title`, `year`) | คอลัมน์ | ตรงตัว |
| object ซ้อน (`imdb.rating`) | คอลัมน์ `imdb_rating` | ความสัมพันธ์ 1:1 ไม่ต้องแยกตาราง |
| array (`cast`, `genres`) | ตารางลูก + FK | 1:N — SQL ไม่ควรเก็บ list ในช่องเดียว |
| `{"$oid": "..."}` | `TEXT` primary key | ObjectId เป็น hex 24 ตัว |
| `{"$date": {"$numberLong": "-2085523200000"}}` | `DATE` | ms นับจาก 1970 (ติดลบ = ก่อน 1970 → 1903-12-01) |
| field ที่ไม่มีใน document (เช่น `poster`) | `NULL` | schemaless — ใช้ `.get()` ทุก field |

รันตัวแปลงเดี่ยวๆ โดยไม่ต้องเปิด Airflow (ได้ไฟล์ .sql เหมือนกัน):

```bash
python scripts/convert_movies.py
```

ตัวอย่าง query หลังโหลด:

```sql
SELECT m.title, m.year, m.released, m.imdb_rating,
       string_agg(DISTINCT g.genre, ', ') AS genres
FROM movies m JOIN movie_genres g USING (movie_id)
GROUP BY m.movie_id ORDER BY m.year;
```
```
          title          | year |  released  | imdb_rating |     genres
-------------------------+------+------------+-------------+----------------
 The Great Train Robbery | 1903 | 1903-12-01 |         7.4 | Short, Western
 A Corner in Wheat       | 1909 | 1909-12-13 |         6.6 | Drama, Short
```

## วิธีรัน

ต้องมี Docker Desktop (เปิดอยู่) + WSL2

```bash
docker compose up -d --build
```

รอ ~1–2 นาที แล้วเปิด <http://localhost:8080> (user `airflow` / pass `airflow`)
→ เปิดสวิตช์ DAG `sales_etl` และ `movies_json_etl` → กด ▶ Trigger → ดู Grid/Graph view

ดูข้อมูลใน warehouse:

```bash
docker compose exec warehouse psql -U etl -d warehouse -c "SELECT * FROM daily_summary ORDER BY sale_date, product;"
```

หรือต่อด้วย DBeaver/pgAdmin: `localhost:5433` user `etl` pass `etl` db `warehouse`

ปิดระบบ:

```bash
docker compose down
```

(เพิ่ม `-v` ถ้าต้องการลบข้อมูลใน DB ทั้งหมดเพื่อเริ่มใหม่)

## ผลลัพธ์ตัวอย่าง

```
 sale_date  | qty | amount
------------+-----+---------
 2026-09-08 |   9 |  545.00
 2026-09-09 |  11 |  765.00
 2026-09-10 |  11 |  685.00
 2026-09-11 |  14 |  955.00
 2026-09-12 |  18 | 1175.00
```

log ของ `extract_load_raw`:
```
read 31 rows from /opt/airflow/include/data/sales.csv
dropped 1 duplicate order_id rows
loaded 30 rows into raw_sales
```

## สิ่งที่โปรเจกต์นี้แสดง

- แนวคิด **ELT**: โหลด raw ก่อน แล้ว transform ด้วย SQL ใน DB
- การแยกชั้นข้อมูล raw / summary
- **Data quality check** เป็นส่วนหนึ่งของ pipeline (ไม่ใช่เช็คมือ)
- **Idempotency** และ retry อัตโนมัติ
- การแปลง **NoSQL (MongoDB document) → relational schema**: flatten / normalize / type mapping
- Reproducible environment ด้วย Docker Compose — clone แล้ว `up` ได้เลย
