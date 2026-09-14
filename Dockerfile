# ต่อยอดจาก image ทางการของ Airflow แล้วติดตั้ง library ที่ DAG ของเราใช้ (pandas)
FROM apache/airflow:3.3.1
COPY requirements-airflow.txt /requirements-airflow.txt
RUN pip install --no-cache-dir -r /requirements-airflow.txt
