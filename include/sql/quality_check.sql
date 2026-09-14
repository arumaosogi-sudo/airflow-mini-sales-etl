-- Data quality check: ต้องคืน 1 แถว และทุกคอลัมน์ต้องเป็น TRUE
-- (SQLCheckOperator จะทำให้ task fail ถ้าค่าใดเป็น FALSE / 0 / NULL)
SELECT
    COUNT(*) > 0                                                     AS has_rows,
    COUNT(*) FILTER (WHERE total_amount < 0) = 0                     AS no_negative_amount,
    COUNT(*) FILTER (WHERE sale_date IS NULL OR product IS NULL) = 0 AS no_null_keys,
    (SELECT SUM(amount) FROM raw_sales) = SUM(total_amount)          AS totals_match_raw
FROM daily_summary;
