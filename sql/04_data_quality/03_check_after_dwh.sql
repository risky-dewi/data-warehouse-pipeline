-- ============================================================
-- CHECKPOINT 3 — After Data Warehouse Layer
-- File   : 03_check_after_dwh.sql
-- When   : Run AFTER 02_dwh.sql completes
--          Run BEFORE 03_mart.sql starts
-- Goal   : Verify star schema was built correctly
--          If any check fails → fix DWH, do NOT proceed to mart
-- How    : All checks labelled PASS/FAIL
--          Overall status at the end tells you if it's safe to continue
-- ============================================================


-- ─────────────────────────────────────────────────────────────
-- SECTION 1 — DIMENSION TABLE ROW COUNT CHECKS
-- ─────────────────────────────────────────────────────────────

SELECT
    check_name,
    actual,
    expected,
    CASE WHEN actual = expected THEN 'PASS' ELSE 'FAIL' END AS status
FROM (
    -- dim_date: 2019-01-01 to 2023-12-31 = 1,826 days
    SELECT 'dim_date row count' AS check_name,
           COUNT(*) AS actual, 1826 AS expected
    FROM dw.dim_date

    UNION ALL

    SELECT 'dim_location row count',  COUNT(*), 13   FROM dw.dim_location
    UNION ALL
    -- dim_user: 9,156 real + 1 sentinel
    SELECT 'dim_user row count (incl sentinel)', COUNT(*), 9157 FROM dw.dim_user
    UNION ALL
    SELECT 'dim_product row count',   COUNT(*), 66   FROM dw.dim_product
    UNION ALL
    SELECT 'dim_payment_method row count', COUNT(*), 6 FROM dw.dim_payment_method
    UNION ALL
    SELECT 'dim_voucher row count',   COUNT(*), 4    FROM dw.dim_voucher
) t
ORDER BY status DESC, check_name;


-- ─────────────────────────────────────────────────────────────
-- SECTION 2 — FACT TABLE ROW COUNT CHECKS
-- ─────────────────────────────────────────────────────────────

SELECT
    check_name,
    actual,
    expected,
    CASE WHEN actual = expected THEN 'PASS' ELSE 'FAIL' END AS status
FROM (
    -- fact_sessions = all stg_sessions (real + ghost)
    SELECT 'fact_sessions row count' AS check_name,
           COUNT(*) AS actual, 89626 AS expected
    FROM dw.fact_sessions

    UNION ALL

    -- fact_events = all events
    SELECT 'fact_events row count',
           COUNT(*), 565527
    FROM dw.fact_events

    UNION ALL

    -- fact_transaction_items = all valid items
    SELECT 'fact_transaction_items row count',
           COUNT(*), 59520
    FROM dw.fact_transaction_items
) t
ORDER BY status DESC, check_name;


-- ─────────────────────────────────────────────────────────────
-- SECTION 3 — SENTINEL AND SPECIAL ROW CHECKS
-- ─────────────────────────────────────────────────────────────

SELECT
    check_name,
    actual,
    expected,
    CASE WHEN actual = expected THEN 'PASS' ELSE 'FAIL' END AS status
FROM (
    -- Sentinel user must exist in dim_user
    SELECT 'dim_user has sentinel (user_key=-1)' AS check_name,
           COUNT(*) AS actual, 1 AS expected
    FROM dw.dim_user WHERE user_key = -1

    UNION ALL

    -- Sentinel must have Unknown labels not NULL
    SELECT 'sentinel age_group = Unknown',
           COUNT(*), 1
    FROM dw.dim_user
    WHERE user_key = -1 AND age_group = 'Unknown' AND gender = 'Unknown'

    UNION ALL

    -- dim_traffic must have Untracked row for ghost sessions
    SELECT 'dim_traffic has Untracked row',
           COUNT(*), 1
    FROM dw.dim_traffic
    WHERE traffic_source = 'Untracked' AND traffic_medium = 'Untracked'

    UNION ALL

    -- fact_sessions ghost rows must point to sentinel user
    SELECT 'fact_sessions ghost sessions have user_key=-1',
           COUNT(*), 6140
    FROM dw.fact_sessions WHERE user_key = -1

    UNION ALL

    -- dim_date must cover full range 2019-2023
    SELECT 'dim_date covers 2019-01-01',
           COUNT(*), 1
    FROM dw.dim_date WHERE full_date = '2019-01-01'

    UNION ALL

    SELECT 'dim_date covers 2023-12-31',
           COUNT(*), 1
    FROM dw.dim_date WHERE full_date = '2023-12-31'
) t
ORDER BY status DESC, check_name;


-- ─────────────────────────────────────────────────────────────
-- SECTION 4 — REVENUE AND METRIC CORRECTNESS
-- Critical: these are the numbers stakeholders will see in reports
-- ─────────────────────────────────────────────────────────────

SELECT
    check_name,
    actual,
    expected,
    CASE WHEN actual = expected THEN 'PASS' ELSE 'FAIL' END AS status
FROM (
    -- All completed transactions must have revenue = total_amount
    SELECT 'completed transactions revenue = total_amount' AS check_name,
           COUNT(*) AS actual, 0 AS expected
    FROM dw.fact_transactions
    WHERE status = 'completed' AND revenue <> total_amount

    UNION ALL

    -- All canceled transactions must have revenue = 0
    SELECT 'canceled transactions revenue = 0',
           COUNT(*), 0
    FROM dw.fact_transactions
    WHERE status = 'canceled' AND revenue <> 0

    UNION ALL

    -- is_completed flag must match status
    SELECT 'is_completed flag correct',
           COUNT(*), 0
    FROM dw.fact_transactions
    WHERE (status = 'completed' AND is_completed <> 1)
       OR (status = 'canceled'  AND is_completed <> 0)

    UNION ALL

    -- is_canceled flag must match status
    SELECT 'is_canceled flag correct',
           COUNT(*), 0
    FROM dw.fact_transactions
    WHERE (status = 'canceled'  AND is_canceled <> 1)
       OR (status = 'completed' AND is_canceled <> 0)

    UNION ALL

    -- No total_amount <= 0
    SELECT 'fact_transactions total_amount > 0',
           COUNT(*), 0
    FROM dw.fact_transactions WHERE total_amount <= 0

    UNION ALL

    -- fact_transaction_items: amount = qty * price
    SELECT 'fact_transaction_items amount = qty * price',
           COUNT(*), 0
    FROM dw.fact_transaction_items
    WHERE product_amount <> product_qty * product_price
) t
ORDER BY status DESC, check_name;


-- ─────────────────────────────────────────────────────────────
-- SECTION 5 — REFERENTIAL INTEGRITY CHECKS
-- Verify all foreign keys resolve correctly
-- ─────────────────────────────────────────────────────────────

SELECT
    check_name,
    actual,
    expected,
    CASE WHEN actual = expected THEN 'PASS' ELSE 'FAIL' END AS status
FROM (
    -- All fact_sessions date_keys must exist in dim_date
    SELECT 'fact_sessions date_key → dim_date' AS check_name,
           COUNT(*) AS actual, 0 AS expected
    FROM dw.fact_sessions fs
    LEFT JOIN dw.dim_date d ON fs.date_key = d.date_key
    WHERE d.date_key IS NULL

    UNION ALL

    -- All fact_sessions user_keys must exist in dim_user
    SELECT 'fact_sessions user_key → dim_user',
           COUNT(*), 0
    FROM dw.fact_sessions fs
    LEFT JOIN dw.dim_user du ON fs.user_key = du.user_key
    WHERE du.user_key IS NULL

    UNION ALL

    -- All fact_sessions traffic_keys must exist in dim_traffic
    SELECT 'fact_sessions traffic_key → dim_traffic',
           COUNT(*), 0
    FROM dw.fact_sessions fs
    LEFT JOIN dw.dim_traffic dt ON fs.traffic_key = dt.traffic_key
    WHERE dt.traffic_key IS NULL

    UNION ALL

    -- All fact_transactions date_keys must exist in dim_date
    SELECT 'fact_transactions date_key → dim_date',
           COUNT(*), 0
    FROM dw.fact_transactions ft
    LEFT JOIN dw.dim_date d ON ft.date_key = d.date_key
    WHERE d.date_key IS NULL

    UNION ALL

    -- All fact_transactions payment_method_keys must resolve
    SELECT 'fact_transactions payment_method_key → dim_payment_method',
           COUNT(*), 0
    FROM dw.fact_transactions ft
    LEFT JOIN dw.dim_payment_method dpm ON ft.payment_method_key = dpm.payment_method_key
    WHERE dpm.payment_method_key IS NULL

    UNION ALL

    -- All fact_transactions voucher_keys must resolve
    SELECT 'fact_transactions voucher_key → dim_voucher',
           COUNT(*), 0
    FROM dw.fact_transactions ft
    LEFT JOIN dw.dim_voucher dv ON ft.voucher_key = dv.voucher_key
    WHERE dv.voucher_key IS NULL

    UNION ALL

    -- All fact_transaction_items product_keys must resolve
    SELECT 'fact_transaction_items product_key → dim_product',
           COUNT(*), 0
    FROM dw.fact_transaction_items fti
    LEFT JOIN dw.dim_product dp ON fti.product_key = dp.product_key
    WHERE dp.product_key IS NULL

    UNION ALL

    -- All fact_events date_keys must exist in dim_date
    SELECT 'fact_events date_key → dim_date',
           COUNT(*), 0
    FROM dw.fact_events fe
    LEFT JOIN dw.dim_date d ON fe.date_key = d.date_key
    WHERE d.date_key IS NULL

    UNION ALL

    -- All fact_events user_keys must exist in dim_user
    SELECT 'fact_events user_key → dim_user',
           COUNT(*), 0
    FROM dw.fact_events fe
    LEFT JOIN dw.dim_user du ON fe.user_key = du.user_key
    WHERE du.user_key IS NULL
) t
ORDER BY status DESC, check_name;


-- ─────────────────────────────────────────────────────────────
-- SECTION 6 — PRIMARY KEY UNIQUENESS CHECKS
-- ─────────────────────────────────────────────────────────────

SELECT
    check_name,
    actual,
    expected,
    CASE WHEN actual = expected THEN 'PASS' ELSE 'FAIL' END AS status
FROM (
    SELECT 'dim_date no duplicate date_key' AS check_name,
           COUNT(*) AS actual, 0 AS expected
    FROM (SELECT date_key FROM dw.dim_date GROUP BY date_key HAVING COUNT(*) > 1) t

    UNION ALL

    SELECT 'dim_user no duplicate user_key',
           COUNT(*), 0
    FROM (SELECT user_key FROM dw.dim_user GROUP BY user_key HAVING COUNT(*) > 1) t

    UNION ALL

    SELECT 'fact_sessions no duplicate session_key',
           COUNT(*), 0
    FROM (SELECT session_key FROM dw.fact_sessions GROUP BY session_key HAVING COUNT(*) > 1) t

    UNION ALL

    SELECT 'fact_transactions no duplicate transaction_key',
           COUNT(*), 0
    FROM (SELECT transaction_key FROM dw.fact_transactions GROUP BY transaction_key HAVING COUNT(*) > 1) t

    UNION ALL

    SELECT 'fact_events no duplicate event_key',
           COUNT(*), 0
    FROM (SELECT event_key FROM dw.fact_events GROUP BY event_key HAVING COUNT(*) > 1) t

    UNION ALL

    SELECT 'fact_transaction_items no duplicate item_key',
           COUNT(*), 0
    FROM (SELECT transaction_item_key FROM dw.fact_transaction_items GROUP BY transaction_item_key HAVING COUNT(*) > 1) t
) t
ORDER BY status DESC, check_name;


-- ─────────────────────────────────────────────────────────────
-- SECTION 7 — DIM_DATE ATTRIBUTE CHECKS
-- Verify calendar attributes are computed correctly
-- ─────────────────────────────────────────────────────────────

SELECT
    check_name,
    actual,
    expected,
    CASE WHEN actual = expected THEN 'PASS' ELSE 'FAIL' END AS status
FROM (
    -- 2022-12-31 is Saturday → day_of_week = 6 (ISODOW)
    SELECT 'dim_date day_of_week ISODOW Saturday=6' AS check_name,
           day_of_week AS actual, 6 AS expected
    FROM dw.dim_date WHERE full_date = '2022-12-31'

    UNION ALL

    -- 2022-12-31 is weekend
    SELECT 'dim_date is_weekend for Saturday',
           CASE WHEN is_weekend THEN 1 ELSE 0 END, 1
    FROM dw.dim_date WHERE full_date = '2022-12-31'

    UNION ALL

    -- 2022-01-03 is Monday → not weekend
    SELECT 'dim_date is_weekend for Monday = FALSE',
           CASE WHEN is_weekend THEN 1 ELSE 0 END, 0
    FROM dw.dim_date WHERE full_date = '2022-01-03'

    UNION ALL

    -- No NULL month_name
    SELECT 'dim_date no NULL month_name',
           COUNT(*), 0
    FROM dw.dim_date WHERE month_name IS NULL

    UNION ALL

    -- No NULL day_name
    SELECT 'dim_date no NULL day_name',
           COUNT(*), 0
    FROM dw.dim_date WHERE day_name IS NULL
) t
ORDER BY status DESC, check_name;


-- ─────────────────────────────────────────────────────────────
-- SECTION 8 — OVERALL STATUS SUMMARY
-- Run this last — gives a single go/no-go decision
-- ─────────────────────────────────────────────────────────────

SELECT
    -- Dimension counts
    (SELECT COUNT(*) FROM dw.dim_date)                              AS dim_date_rows,
    (SELECT COUNT(*) FROM dw.dim_user)                              AS dim_user_rows,
    (SELECT COUNT(*) FROM dw.dim_traffic)                           AS dim_traffic_rows,

    -- Fact counts
    (SELECT COUNT(*) FROM dw.fact_sessions)                         AS fact_sessions_rows,
    (SELECT COUNT(*) FROM dw.fact_events)                           AS fact_events_rows,
    (SELECT COUNT(*) FROM dw.fact_transactions)                     AS fact_transactions_rows,
    (SELECT COUNT(*) FROM dw.fact_transaction_items)                AS fact_items_rows,

    -- Revenue integrity
    (SELECT SUM(revenue) FROM dw.fact_transactions)                 AS total_revenue,
    (SELECT SUM(is_completed) FROM dw.fact_transactions)            AS completed_orders,
    (SELECT SUM(is_canceled)  FROM dw.fact_transactions)            AS canceled_orders,

    -- Critical checks
    (SELECT COUNT(*) FROM dw.dim_user WHERE user_key = -1)          AS sentinel_exists,
    (SELECT COUNT(*) FROM dw.fact_transactions
     WHERE status = 'completed' AND revenue <> total_amount)        AS revenue_mismatch,
    (SELECT COUNT(*) FROM dw.fact_sessions fs
     LEFT JOIN dw.dim_date d ON fs.date_key = d.date_key
     WHERE d.date_key IS NULL)                                      AS orphan_date_keys,

    -- Final verdict
    CASE
        WHEN (SELECT COUNT(*) FROM dw.fact_sessions) = 89626
         AND (SELECT COUNT(*) FROM dw.fact_events)  = 565527
         AND (SELECT COUNT(*) FROM dw.fact_transaction_items) = 59520
         AND (SELECT COUNT(*) FROM dw.dim_user WHERE user_key = -1) = 1
         AND (SELECT COUNT(*) FROM dw.fact_transactions
              WHERE status = 'completed' AND revenue <> total_amount) = 0
         AND (SELECT COUNT(*) FROM dw.fact_sessions fs
              LEFT JOIN dw.dim_date d ON fs.date_key = d.date_key
              WHERE d.date_key IS NULL) = 0
        THEN 'DWH OK — SAFE TO PROCEED TO MART'
        ELSE 'DWH HAS ISSUES — DO NOT PROCEED TO MART'
    END AS overall_status;
