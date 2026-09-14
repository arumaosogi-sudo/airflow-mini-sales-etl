-- สร้างตารางถ้ายังไม่มี (รันซ้ำได้ไม่พัง)

-- ตารางข้อมูลดิบ: 1 แถว = 1 บรรทัดใน CSV (หลัง clean ด้วย pandas แล้ว)
CREATE TABLE IF NOT EXISTS raw_sales (
    order_id    INTEGER PRIMARY KEY,
    order_date  DATE          NOT NULL,
    product     TEXT          NOT NULL,
    category    TEXT          NOT NULL,
    qty         INTEGER       NOT NULL,
    unit_price  NUMERIC(10,2) NOT NULL,
    amount      NUMERIC(12,2) NOT NULL,   -- qty * unit_price (คำนวณใน pandas)
    loaded_at   TIMESTAMP     NOT NULL DEFAULT NOW()
);

-- ตารางสรุป: ยอดขายต่อวัน ต่อสินค้า
CREATE TABLE IF NOT EXISTS daily_summary (
    sale_date     DATE          NOT NULL,
    product       TEXT          NOT NULL,
    category      TEXT          NOT NULL,
    total_orders  INTEGER       NOT NULL,
    total_qty     INTEGER       NOT NULL,
    total_amount  NUMERIC(12,2) NOT NULL,
    PRIMARY KEY (sale_date, product)
);
