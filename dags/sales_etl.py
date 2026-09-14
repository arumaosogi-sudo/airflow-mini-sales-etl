"""
Mini Sales ETL  —  CSV (non-SQL)  ->  PostgreSQL  ->  Daily Summary

ลำดับ task:
    create_tables -> extract_load_raw -> transform_summary -> quality_check -> notify_done
       (SQL)          (Python+pandas)         (SQL)            (SQL check)      (Python)

แนวคิด ELT: โหลดข้อมูลดิบลง DB ก่อน (raw_sales) แล้วค่อยใช้ SQL สรุป (daily_summary)
ทุก task รันซ้ำได้ (idempotent): TRUNCATE ก่อน INSERT เสมอ
"""

from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import (
    SQLCheckOperator,
    SQLExecuteQueryOperator,
)
from airflow.providers.postgres.hooks.postgres import PostgresHook

# ---------------------------------------------------------------------------
# ค่าคงที่
# ---------------------------------------------------------------------------
CONN_ID = "postgres_dw"                                  # ตั้งไว้ใน docker-compose (AIRFLOW_CONN_POSTGRES_DW)
CSV_PATH = "/opt/airflow/include/data/sales.csv"         # path "ข้างใน container" (mount จาก ./include)


# ---------------------------------------------------------------------------
# Python functions ที่ PythonOperator จะเรียก
# (โค้ดหนักๆ เช่นอ่านไฟล์/ต่อ DB ต้องอยู่ในฟังก์ชันเท่านั้น
#  เพราะตัวไฟล์ DAG ถูก dag-processor รันซ้ำทุก ~30 วิเพื่ออ่านโครงสร้าง)
# ---------------------------------------------------------------------------
def extract_load_raw() -> int:
    """Extract: อ่าน CSV -> Clean ด้วย pandas -> Load ลง raw_sales. คืนจำนวนแถวที่โหลด"""
    import pandas as pd
    from sqlalchemy import text

    # ---- Extract ----
    df = pd.read_csv(CSV_PATH)
    print(f"read {len(df)} rows from {CSV_PATH}")

    # ---- Clean / Transform เบื้องต้น ----
    df["order_date"] = pd.to_datetime(df["order_date"]).dt.date   # string -> date
    before = len(df)
    df = df.drop_duplicates(subset=["order_id"])                  # ตัด order ซ้ำ (CSV มีซ้ำจริง 1 แถว)
    print(f"dropped {before - len(df)} duplicate order_id rows")
    df["amount"] = df["qty"] * df["unit_price"]                   # คำนวณยอดต่อแถว

    # ---- Load ----
    engine = PostgresHook(postgres_conn_id=CONN_ID).get_sqlalchemy_engine()
    with engine.begin() as conn:                                  # transaction เดียว: ล้าง + ใส่ใหม่
        conn.execute(text("TRUNCATE TABLE raw_sales"))
        df.to_sql("raw_sales", conn, if_exists="append", index=False)

    print(f"loaded {len(df)} rows into raw_sales")
    return len(df)                                                # ค่า return -> XCom ให้ task อื่นดึงไปใช้


def notify_done(ti) -> None:
    """สรุปผลลง log: ดึงจำนวนแถว raw จาก XCom และนับแถวใน daily_summary"""
    raw_rows = ti.xcom_pull(task_ids="extract_load_raw")

    hook = PostgresHook(postgres_conn_id=CONN_ID)
    summary_rows = hook.get_first("SELECT COUNT(*) FROM daily_summary")[0]
    grand_total = hook.get_first("SELECT SUM(total_amount) FROM daily_summary")[0]

    print("=" * 50)
    print(f"raw_sales rows     : {raw_rows}")
    print(f"daily_summary rows : {summary_rows}")
    print(f"grand total amount : {grand_total:,.2f} THB")
    print("=" * 50)


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
default_args = {
    "owner": "oly",
    "retries": 1,                          # พังแล้วลองใหม่ 1 ครั้ง
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="sales_etl",
    description="CSV -> PostgreSQL -> daily summary (Mini ETL demo)",
    start_date=datetime(2026, 9, 1),
    schedule="0 6 * * *",                  # ทุกวัน 06:00 (cron)  — กด Trigger รันเองได้ตลอด
    catchup=False,                         # ไม่ย้อนรันวันที่ผ่านมา
    default_args=default_args,
    template_searchpath=["/opt/airflow/include"],   # ให้ sql="sql/xxx.sql" หาไฟล์เจอ
    tags=["demo", "etl", "sql"],
) as dag:

    create_tables = SQLExecuteQueryOperator(
        task_id="create_tables",
        conn_id=CONN_ID,
        sql="sql/create_tables.sql",
    )

    load_raw = PythonOperator(
        task_id="extract_load_raw",
        python_callable=extract_load_raw,
    )

    transform_summary = SQLExecuteQueryOperator(
        task_id="transform_summary",
        conn_id=CONN_ID,
        sql="sql/transform_daily_summary.sql",
    )

    quality_check = SQLCheckOperator(
        task_id="quality_check",
        conn_id=CONN_ID,
        sql="sql/quality_check.sql",
    )

    done = PythonOperator(
        task_id="notify_done",
        python_callable=notify_done,
    )

    # ลำดับการรัน:  A >> B  อ่านว่า "A เสร็จก่อน แล้วค่อย B"
    create_tables >> load_raw >> transform_summary >> quality_check >> done
