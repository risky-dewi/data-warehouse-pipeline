-- ============================================================
-- DATA QUALITY CHECKS
-- File   : 04_data_quality.sql
-- Purpose: Validate data integrity across all layers
-- Run    : After all layers have been loaded
-- How    : All checks should return 0 rows or expected values
--          Any non-zero result = issue that needs investigation
-- ============================================================


-- ─────────────────────────────────────────────────────────────
-- SECTION 1 — ROW COUNT CHECKS
-- Expected: counts match source CSV row counts
-- ─────────────────────────────────────────────────────────────

SELECT 'row_count_check' AS check_type, *
FROM (
    SELECT 'raw.voucher'           AS table_name, COUNT(*) AS row_count, 4       AS expected FROM raw.voucher
    UNION ALL
    SELECT 'raw.products',                         COUNT(*),              66              FROM raw.products
    UNION ALL
    SELECT 'raw.locations',                        COUNT(*),              13              FROM raw.locations
    UNION ALL
    SELECT 'raw.payment_methods',                  COUNT(*),              6               FROM raw.payment_methods
    UNION ALL
    SELECT 'raw.users',                            COUNT(*),              9156            FROM raw.users
    UNION ALL
    SELECT 'raw.sessions',                         COUNT(*),              83486           FROM raw.sessions
    UNION ALL
    SELECT 'raw.events',                           COUNT(*),              565527          FROM raw.events
    UNION ALL
    SELECT 'raw.transaction_items',                COUNT(*),              59520           FROM raw.transaction_items
    -- raw.transactions: update expected count based on your CSV
    UNION ALL
    SELECT 'raw.transactions',                     COUNT(*),              19173           FROM raw.transactions
) counts
WHERE row_count <> expected;
-- Expected result: 0 rows
-- If any row appears: that table has wrong number of rows


-- ─────────────────────────────────────────────────────────────
-- SECTION 2 — DUPLICATE PRIMARY KEY CHECKS
-- Expected: all return 0 rows
-- ─────────────────────────────────────────────────────────────

-- Check: duplicate sessions_id in raw
SELECT 'duplicate_sessions_id' AS check_name, sessions_id, COUNT(*) AS cnt
FROM raw.sessions
GROUP BY sessions_id
HAVING COUNT(*) > 1;

-- Check: duplicate event_id in raw
SELECT 'duplicate_event_id' AS check_name, event_id, COUNT(*) AS cnt
FROM raw.events
GROUP BY event_id
HAVING COUNT(*) > 1;

-- Check: duplicate transactions_id in raw
SELECT 'duplicate_transactions_id' AS check_name, transactions_id, COUNT(*) AS cnt
FROM raw.transactions
GROUP BY transactions_id
HAVING COUNT(*) > 1;

-- Check: duplicate transaction_items_id in raw
SELECT 'duplicate_transaction_items_id' AS check_name, transaction_items_id, COUNT(*) AS cnt
FROM raw.transaction_items
GROUP BY transaction_items_id
HAVING COUNT(*) > 1;

-- Check: duplicate user_id in raw
SELECT 'duplicate_user_id' AS check_name, user_id, COUNT(*) AS cnt
FROM raw.users
GROUP BY user_id
HAVING COUNT(*) > 1;


-- ─────────────────────────────────────────────────────────────
-- SECTION 3 — NULL CHECKS ON KEY COLUMNS
-- Expected: all return 0
-- ─────────────────────────────────────────────────────────────

SELECT 'null_key_columns' AS check_type, *
FROM (
    SELECT 'raw.sessions.sessions_id'              AS column_name, COUNT(*) AS null_count FROM raw.sessions         WHERE sessions_id IS NULL
    UNION ALL
    SELECT 'raw.events.event_id',                                  COUNT(*)               FROM raw.events            WHERE event_id IS NULL
    UNION ALL
    SELECT 'raw.events.sessions_id',                               COUNT(*)               FROM raw.events            WHERE sessions_id IS NULL
    UNION ALL
    SELECT 'raw.transactions.transactions_id',                     COUNT(*)               FROM raw.transactions      WHERE transactions_id IS NULL
    UNION ALL
    SELECT 'raw.transactions.sessions_id',                         COUNT(*)               FROM raw.transactions      WHERE sessions_id IS NULL
    UNION ALL
    SELECT 'raw.transaction_items.transaction_items_id',           COUNT(*)               FROM raw.transaction_items WHERE transaction_items_id IS NULL
    UNION ALL
    SELECT 'raw.transaction_items.transactions_id',                COUNT(*)               FROM raw.transaction_items WHERE transactions_id IS NULL
    UNION ALL
    SELECT 'raw.users.user_id',                                    COUNT(*)               FROM raw.users             WHERE user_id IS NULL
) nulls
WHERE null_count > 0;
-- Expected result: 0 rows


-- ─────────────────────────────────────────────────────────────
-- SECTION 4 — BUSINESS RULE VALIDATION
-- Expected: all return 0 rows unless noted
-- ─────────────────────────────────────────────────────────────

-- Check: product_amount must equal product_qty * product_price
SELECT 'amount_calc_mismatch' AS check_name,
       transactions_id,
       transaction_items_id,
       product_qty,
       product_price,
       product_amount,
       product_qty * product_price AS calculated_amount
FROM raw.transaction_items
WHERE product_amount <> product_qty * product_price;
-- Expected: 0 rows

-- Check: no negative or zero product quantities
SELECT 'invalid_product_qty' AS check_name, COUNT(*) AS cnt
FROM raw.transaction_items
WHERE product_qty <= 0;
-- Expected: 0

-- Check: no negative or zero prices
SELECT 'invalid_product_price' AS check_name, COUNT(*) AS cnt
FROM raw.transaction_items
WHERE product_price <= 0;
-- Expected: 0

-- Check: no negative or zero total amounts
SELECT 'invalid_total_amount' AS check_name, COUNT(*) AS cnt
FROM raw.transactions
WHERE total_amount <= 0;
-- Expected: 0

-- Check: only valid status values
SELECT 'invalid_status' AS check_name, status, COUNT(*) AS cnt
FROM raw.transactions
WHERE status NOT IN ('completed', 'canceled')
GROUP BY status;
-- Expected: 0 rows

-- Check: only known event types
SELECT 'unknown_event_type' AS check_name, event, COUNT(*) AS cnt
FROM raw.events
WHERE event NOT IN (
    'productview', 'addtocart', 'viewcart',
    'chooseaddress', 'choosedelivery', 'changepaymentmethod',
    'checkout', 'complete', 'cancel'
)
GROUP BY event;
-- Expected: 0 rows


-- ─────────────────────────────────────────────────────────────
-- SECTION 5 — REFERENTIAL INTEGRITY CHECKS
-- ─────────────────────────────────────────────────────────────

-- Check: transaction_items with no matching transaction
SELECT 'orphan_transaction_items' AS check_name, COUNT(*) AS cnt
FROM raw.transaction_items ti
WHERE ti.transactions_id NOT IN (SELECT transactions_id FROM raw.transactions);
-- Expected: 0

-- Check: transactions with no matching session (known issue — documented)
SELECT 'orphan_transactions_no_session' AS check_name, COUNT(*) AS cnt
FROM raw.transactions t
WHERE t.sessions_id NOT IN (SELECT sessions_id FROM raw.sessions);
-- Expected: 6,140 (known — ghost sessions, handled by backfill in staging)

-- Check: sessions with no matching user
SELECT 'orphan_sessions_no_user' AS check_name, COUNT(*) AS cnt
FROM raw.sessions s
WHERE s.user_id NOT IN (SELECT user_id FROM raw.users);
-- Expected: 0

-- Check: transaction_items with unknown product
SELECT 'unknown_product_in_items' AS check_name, COUNT(*) AS cnt
FROM raw.transaction_items ti
WHERE ti.product_id NOT IN (SELECT product_id FROM raw.products);
-- Expected: 0

-- Check: transactions with unknown payment method
SELECT 'unknown_payment_method' AS check_name, COUNT(*) AS cnt
FROM raw.transactions t
WHERE t.payment_method_id NOT IN (SELECT payment_method_id FROM raw.payment_methods);
-- Expected: 0


-- ─────────────────────────────────────────────────────────────
-- SECTION 6 — STAGING LAYER CHECKS
-- ─────────────────────────────────────────────────────────────

-- Check: stg_sessions total = real sessions + ghost sessions
SELECT 'stg_sessions_count' AS check_name,
       COUNT(*)                                    AS total,
       SUM(CASE WHEN is_ghost = FALSE THEN 1 END)  AS real_sessions,
       SUM(CASE WHEN is_ghost = TRUE  THEN 1 END)  AS ghost_sessions
FROM staging.stg_sessions;
-- Expected: total = 89626, real = 83486, ghost = 6140

-- Check: sentinel user exists in stg_users
SELECT 'sentinel_user_exists' AS check_name, COUNT(*) AS cnt
FROM staging.stg_users
WHERE user_id = -1 AND is_sentinel = TRUE;
-- Expected: 1

-- Check: no NULL session_date in stg_sessions
SELECT 'null_session_date' AS check_name, COUNT(*) AS cnt
FROM staging.stg_sessions
WHERE session_date IS NULL;
-- Expected: 0

-- Check: traffic_name column is not string 'nan'
-- (pandas NaN should have been converted to proper NULL)
SELECT 'traffic_name_nan_string' AS check_name, COUNT(*) AS cnt
FROM staging.stg_sessions
WHERE traffic_name = 'nan';
-- Expected: 0

-- Check: age_group coverage in stg_users (no Millennila typo)
SELECT age_group, COUNT(*) AS cnt
FROM staging.stg_users
WHERE is_sentinel = FALSE
GROUP BY age_group
ORDER BY age_group;
-- Expected: Gen Z (Teen), Gen Z (Adult), Millennial, Gen X, Boomer


-- ─────────────────────────────────────────────────────────────
-- SECTION 7 — DATA WAREHOUSE LAYER CHECKS
-- ─────────────────────────────────────────────────────────────

-- Check: dim_user has sentinel row
SELECT 'sentinel_in_dim_user' AS check_name, COUNT(*) AS cnt
FROM dw.dim_user
WHERE user_key = -1;
-- Expected: 1

-- Check: dim_traffic has Untracked row for ghost sessions
SELECT 'untracked_in_dim_traffic' AS check_name, COUNT(*) AS cnt
FROM dw.dim_traffic
WHERE traffic_source = 'Untracked' AND traffic_medium = 'Untracked';
-- Expected: 1

-- Check: all fact_sessions rows have a valid date_key
SELECT 'fact_sessions_null_date_key' AS check_name, COUNT(*) AS cnt
FROM dw.fact_sessions
WHERE date_key IS NULL;
-- Expected: 0

-- Check: fact_transactions revenue is correct
-- All completed rows must have revenue = total_amount
SELECT 'revenue_mismatch' AS check_name, COUNT(*) AS cnt
FROM dw.fact_transactions
WHERE status = 'completed' AND revenue <> total_amount;
-- Expected: 0

-- Check: fact_transactions revenue for canceled must be 0
SELECT 'canceled_revenue_nonzero' AS check_name, COUNT(*) AS cnt
FROM dw.fact_transactions
WHERE status = 'canceled' AND revenue <> 0;
-- Expected: 0

-- Check: dim_date covers all transaction dates
SELECT 'transaction_dates_missing_in_dim_date' AS check_name, COUNT(*) AS cnt
FROM dw.fact_transactions ft
LEFT JOIN dw.dim_date d ON ft.date_key = d.date_key
WHERE d.date_key IS NULL;
-- Expected: 0


-- ─────────────────────────────────────────────────────────────
-- SECTION 8 — MART LAYER SANITY CHECKS
-- ─────────────────────────────────────────────────────────────

-- Check: monthly_revenue total matches fact_transactions
SELECT
    'revenue_mart_vs_fact' AS check_name,
    (SELECT SUM(total_revenue) FROM mart.monthly_revenue)   AS mart_revenue,
    (SELECT SUM(revenue)       FROM dw.fact_transactions)   AS fact_revenue,
    (SELECT SUM(total_revenue) FROM mart.monthly_revenue) =
    (SELECT SUM(revenue)       FROM dw.fact_transactions)   AS is_match;
-- Expected: is_match = TRUE

-- Check: no sentinel user in customer_segment
SELECT 'sentinel_in_customer_segment' AS check_name, COUNT(*) AS cnt
FROM mart.customer_segment
WHERE age_group = 'Unknown' AND gender = 'Unknown';
-- Expected: 0

-- Check: user_cohort has no NULL cohort_month
SELECT 'null_cohort_month' AS check_name, COUNT(*) AS cnt
FROM mart.user_cohort
WHERE cohort_month IS NULL;
-- Expected: 0

-- Check: funnel steps decrease monotonically
-- (each step should have <= sessions than the previous step)
SELECT
    step1_product_view,
    step2_add_to_cart,
    step3_view_cart,
    step7_checkout,
    completed_sessions,
    CASE
        WHEN step1_product_view >= step2_add_to_cart
         AND step2_add_to_cart  >= step3_view_cart
         AND step3_view_cart    >= step7_checkout
         AND step7_checkout     >= completed_sessions
        THEN 'MONOTONIC - OK'
        ELSE 'NOT MONOTONIC - INVESTIGATE'
    END AS funnel_integrity
FROM mart.funnel_analysis;
-- Expected: MONOTONIC - OK


-- ─────────────────────────────────────────────────────────────
-- SECTION 9 — SUMMARY DASHBOARD
-- Run this for a quick overall health check
-- ─────────────────────────────────────────────────────────────

SELECT
    (SELECT COUNT(*) FROM raw.sessions)                         AS raw_sessions,
    (SELECT COUNT(*) FROM raw.transactions)                     AS raw_transactions,
    (SELECT COUNT(*) FROM staging.stg_sessions)                 AS stg_sessions,
    (SELECT COUNT(*) FROM staging.stg_sessions WHERE is_ghost)  AS ghost_sessions_backfilled,
    (SELECT COUNT(*) FROM dw.dim_user)                          AS dim_users_incl_sentinel,
    (SELECT COUNT(*) FROM dw.fact_sessions)                     AS fact_sessions,
    (SELECT COUNT(*) FROM dw.fact_transactions)                 AS fact_transactions,
    (SELECT COUNT(*) FROM dw.fact_transaction_items)            AS fact_items,
    (SELECT COUNT(*) FROM dw.fact_events)                       AS fact_events,
    (SELECT SUM(revenue) FROM dw.fact_transactions)             AS total_revenue,
    (SELECT SUM(is_completed) FROM dw.fact_transactions)        AS completed_orders,
    (SELECT SUM(is_canceled)  FROM dw.fact_transactions)        AS canceled_orders;
