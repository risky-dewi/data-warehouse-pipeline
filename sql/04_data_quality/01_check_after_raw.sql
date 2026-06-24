-- ============================================================
-- CHECKPOINT 1 — After Raw Data Load
-- File   : 01_check_after_load.sql
-- When   : Run AFTER 00_load_raw.py completes
--          Run BEFORE 01_staging.sql starts
-- Goal   : Verify raw loaded data is valid before transformation
--          If any check fails → fix raw/load process, do NOT proceed to staging
-- How    : All checks labelled PASS/FAIL
--          Expected result column tells you what the correct value is
-- ============================================================


-- ─────────────────────────────────────────────────────────────
-- SECTION 1 — CRITICAL ID NULL CHECKS
-- Verify important identifiers are not NULL
-- ─────────────────────────────────────────────────────────────

SELECT
    check_name,
    actual,
    expected,
    CASE WHEN actual = expected THEN 'PASS' ELSE 'FAIL' END AS status
FROM (
    SELECT 'raw.sessions no NULL sessions_id' AS check_name,
           COUNT(*) AS actual,
           0 AS expected
    FROM raw.sessions
    WHERE sessions_id IS NULL

    UNION ALL

    SELECT 'raw.transactions no NULL transactions_id',
           COUNT(*),
           0
    FROM raw.transactions
    WHERE transactions_id IS NULL

    UNION ALL

    SELECT 'raw.transaction_items no NULL transaction_items_id',
           COUNT(*),
           0
    FROM raw.transaction_items
    WHERE transaction_items_id IS NULL

    UNION ALL

    SELECT 'raw.products no NULL product_id',
           COUNT(*),
           0
    FROM raw.products
    WHERE product_id IS NULL

    UNION ALL

    SELECT 'raw.users no NULL user_id',
           COUNT(*),
           0
    FROM raw.users
    WHERE user_id IS NULL
) t
ORDER BY status DESC, check_name;


-- ─────────────────────────────────────────────────────────────
-- SECTION 2 — DUPLICATE KEY CHECKS
-- Verify primary identifier candidates are unique
-- ─────────────────────────────────────────────────────────────

SELECT
    check_name,
    actual,
    expected,
    CASE WHEN actual = expected THEN 'PASS' ELSE 'FAIL' END AS status
FROM (
    SELECT 'raw.sessions duplicate sessions_id' AS check_name,
           COUNT(*) AS actual,
           0 AS expected
    FROM (
        SELECT sessions_id
        FROM raw.sessions
        WHERE sessions_id IS NOT NULL
        GROUP BY sessions_id
        HAVING COUNT(*) > 1
    ) dup

    UNION ALL

    SELECT 'raw.transactions duplicate transactions_id',
           COUNT(*),
           0
    FROM (
        SELECT transactions_id
        FROM raw.transactions
        WHERE transactions_id IS NOT NULL
        GROUP BY transactions_id
        HAVING COUNT(*) > 1
    ) dup

    UNION ALL

    SELECT 'raw.transaction_items duplicate transaction_items_id',
           COUNT(*),
           0
    FROM (
        SELECT transaction_items_id
        FROM raw.transaction_items
        WHERE transaction_items_id IS NOT NULL
        GROUP BY transaction_items_id
        HAVING COUNT(*) > 1
    ) dup

    UNION ALL

    SELECT 'raw.products duplicate product_id',
           COUNT(*),
           0
    FROM (
        SELECT product_id
        FROM raw.products
        WHERE product_id IS NOT NULL
        GROUP BY product_id
        HAVING COUNT(*) > 1
    ) dup

    UNION ALL

    SELECT 'raw.users duplicate user_id',
           COUNT(*),
           0
    FROM (
        SELECT user_id
        FROM raw.users
        WHERE user_id IS NOT NULL
        GROUP BY user_id
        HAVING COUNT(*) > 1
    ) dup
) t
ORDER BY status DESC, check_name;


-- ─────────────────────────────────────────────────────────────
-- SECTION 3 — TRANSACTION ITEM BUSINESS RULE CHECKS
-- Verify basic amount calculation and numeric validity
-- ─────────────────────────────────────────────────────────────

SELECT
    check_name,
    actual,
    expected,
    CASE WHEN actual = expected THEN 'PASS' ELSE 'FAIL' END AS status
FROM (
    SELECT 'raw.transaction_items amount = qty * price' AS check_name,
           COUNT(*) AS actual,
           0 AS expected
    FROM raw.transaction_items
    WHERE product_amount <> product_qty * product_price

    UNION ALL

    SELECT 'raw.transaction_items product_qty > 0',
           COUNT(*),
           0
    FROM raw.transaction_items
    WHERE product_qty <= 0

    UNION ALL

    SELECT 'raw.transaction_items product_price > 0',
           COUNT(*),
           0
    FROM raw.transaction_items
    WHERE product_price <= 0

    UNION ALL

    SELECT 'raw.transaction_items product_amount > 0',
           COUNT(*),
           0
    FROM raw.transaction_items
    WHERE product_amount <= 0
) t
ORDER BY status DESC, check_name;


-- ─────────────────────────────────────────────────────────────
-- SECTION 4 — TRANSACTION BASIC VALIDITY CHECKS
-- Verify transaction values are usable before staging
-- ─────────────────────────────────────────────────────────────

SELECT
    check_name,
    actual,
    expected,
    CASE WHEN actual = expected THEN 'PASS' ELSE 'FAIL' END AS status
FROM (
    SELECT 'raw.transactions total_amount > 0' AS check_name,
           COUNT(*) AS actual,
           0 AS expected
    FROM raw.transactions
    WHERE total_amount <= 0

    UNION ALL

    SELECT 'raw.transactions no NULL sessions_id',
           COUNT(*),
           0
    FROM raw.transactions
    WHERE sessions_id IS NULL

    UNION ALL

    SELECT 'raw.transactions no NULL payment_method_id',
           COUNT(*),
           0
    FROM raw.transactions
    WHERE payment_method_id IS NULL

    UNION ALL

    SELECT 'raw.transactions no NULL status',
           COUNT(*),
           0
    FROM raw.transactions
    WHERE status IS NULL

    UNION ALL

    SELECT 'raw.transactions no NULL timestamp',
           COUNT(*),
           0
    FROM raw.transactions
    WHERE transactions_timestamps IS NULL
) t
ORDER BY status DESC, check_name;


-- ─────────────────────────────────────────────────────────────
-- SECTION 5 — RAW REFERENTIAL SANITY CHECKS
-- Verify raw relationships are mostly resolvable before staging
-- Note: Missing sessions from transactions may be handled later as ghost sessions
-- ─────────────────────────────────────────────────────────────

SELECT
    check_name,
    actual,
    expected,
    CASE WHEN actual = expected THEN 'PASS' ELSE 'FAIL' END AS status
FROM (
    SELECT 'transaction_items product_id exists in raw.products' AS check_name,
           COUNT(*) AS actual,
           0 AS expected
    FROM raw.transaction_items ti
    LEFT JOIN raw.products p
        ON ti.product_id = p.product_id
    WHERE p.product_id IS NULL

    UNION ALL

    SELECT 'transaction_items transactions_id exists in raw.transactions',
           COUNT(*),
           0
    FROM raw.transaction_items ti
    LEFT JOIN raw.transactions t
        ON ti.transactions_id = t.transactions_id
    WHERE t.transactions_id IS NULL

    UNION ALL

    SELECT 'transactions payment_method_id exists in raw.payment_methods',
           COUNT(*),
           0
    FROM raw.transactions t
    LEFT JOIN raw.payment_methods pm
        ON t.payment_method_id = pm.payment_method_id
    WHERE pm.payment_method_id IS NULL

    UNION ALL

    SELECT 'transactions voucher_id exists in raw.voucher when not NULL',
           COUNT(*),
           0
    FROM raw.transactions t
    LEFT JOIN raw.voucher v
        ON t.voucher_id = v.voucher_id
    WHERE t.voucher_id IS NOT NULL
      AND v.voucher_id IS NULL

    UNION ALL

    SELECT 'users locations_id exists in raw.locations',
           COUNT(*),
           0
    FROM raw.users u
    LEFT JOIN raw.locations l
        ON u.locations_id = l.locations_id
    WHERE u.locations_id IS NOT NULL
      AND l.locations_id IS NULL
) t
ORDER BY status DESC, check_name;