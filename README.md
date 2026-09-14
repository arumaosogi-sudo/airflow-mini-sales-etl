# Mini Sales ETL — CSV (non-SQL) → PostgreSQL → Daily Summary ด้วย Apache Airflow

โปรเจกต์ตัวอย่างขนาดเล็กเพื่อแสดงพื้นฐาน Data Engineering:
อ่านไฟล์ยอดขาย **CSV** (ข้อมูลที่ไม่ใช่ SQL) → ทำความสะอาดด้วย **pandas** →
โหลดลง **PostgreSQL** → สรุปยอดรายวันด้วย **SQL** → ตรวจคุณภาพข้อมูล
โดยทั้งหมดถูกจัดลำดับและตั้งเวลาด้วย **Apache Airflow 3** และรันบน **Docker**

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
│   └── sales_etl.py               # DAG หลัก 5 task
└── include/
    ├── data/sales.csv             # ข้อมูลต้นทาง (31 แถว มี order ซ้ำ 1 แถวโดยตั้งใจ)
    └── sql/
        ├── create_tables.sql
        ├── transform_daily_summary.sql
        └── quality_check.sql
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

## วิธีรัน

ต้องมี Docker Desktop (เปิดอยู่) + WSL2

```bash
docker compose up -d --build
```

รอ ~1–2 นาที แล้วเปิด <http://localhost:8080> (user `airflow` / pass `airflow`)
→ เปิดสวิตช์ DAG `sales_etl` → กด ▶ Trigger → ดู Grid/Graph view

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
- Reproducible environment ด้วย Docker Compose — clone แล้ว `up` ได้เลย
