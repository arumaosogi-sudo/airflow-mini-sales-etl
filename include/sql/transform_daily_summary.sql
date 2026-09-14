-- Transform ด้วย SQL: raw_sales -> daily_summary
-- ทำให้ idempotent ด้วยการล้างตารางสรุปก่อนแล้วสร้างใหม่จาก raw ทั้งหมด
-- (รันกี่ครั้งผลก็เท่าเดิม ไม่มีแถวซ้ำ)

TRUNCATE TABLE daily_summary;

INSERT INTO daily_summary (sale_date, product, category, total_orders, total_qty, total_amount)
SELECT
    order_date               AS sale_date,
    product,
    category,
    COUNT(*)                 AS total_orders,
    SUM(qty)                 AS total_qty,
    SUM(amount)              AS total_amount
FROM raw_sales
GROUP BY order_date, product, category
ORDER BY order_date, product;
