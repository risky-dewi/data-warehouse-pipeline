-- ============================================================
-- CHECKPOINT 2 — After Staging Layer
-- File   : 02_check_after_staging.sql
-- When   : Run AFTER 01_staging.sql completes
--          Run BEFORE 02_dwh.sql starts
-- Goal   : Verify all staging transformations are correct
--          If any check fails → fix staging, do NOT proceed to DWH
-- How    : All checks labelled PASS/FAIL
--          Expected result column tells you what the correct value is
-- ============================================================


-- ─────────────────────────────────────────────────────────────
-- SECTION 1 — ROW COUNT CHECKS
-- Verify staging tables have the correct number of rows
-- ─────────────────────────────────────────────────────────────

SELECT
    check_name,
    actual,
    expected,
    CASE WHEN actual = expected THEN 'PASS' ELSE 'FAIL' END AS status
FROM (
    SELECT 'stg_voucher row count'          AS check_name, COUNT(*) AS actual, 4       AS expected FROM staging.stg_voucher
    UNION ALL
    SELECT 'stg_products row count',                       COUNT(*),           66              FROM staging.stg_products
    UNION ALL
    SELECT 'stg_locations row count',                      COUNT(*),           13              FROM staging.stg_locations
    UNION ALL
    SELECT 'stg_payment_methods row count',                COUNT(*),           6               FROM staging.stg_payment_methods
    UNION ALL
    -- stg_users = 9,156 real users + 1 sentinel row
    SELECT 'stg_users row count (incl sentinel)',          COUNT(*),           9157            FROM staging.stg_users
    UNION ALL
    SELECT 'stg_events row count',                         COUNT(*),           565527          FROM staging.stg_events
    UNION ALL
    SELECT 'stg_transaction_items row count',              COUNT(*),           59520           FROM staging.stg_transaction_items
    UNION ALL
    -- stg_sessions = 83,486 real + 6,140 ghost backfilled
    SELECT 'stg_sessions total row count',                 COUNT(*),           89626           FROM staging.stg_sessions
) t
ORDER BY status DESC, check_name;


-- ─────────────────────────────────────────────────────────────
-- SECTION 2 — GHOST SESSION BACKFILL VERIFICATION
-- Verify the backfill logic produced exactly the right rows
-- ─────────────────────────────────────────────────────────────

SELECT
    check_name,
    actual,
    expected,
    CASE WHEN actual = expected THEN 'PASS' ELSE 'FAIL' END AS status
FROM (
    SELECT 'stg_sessions real (is_ghost=FALSE)' AS check_name,
           COUNT(*) AS actual, 83486 AS expected
    FROM staging.stg_sessions WHERE is_ghost = FALSE

    UNION ALL

    SELECT 'stg_sessions ghost (is_ghost=TRUE)',
           COUNT(*), 6140
    FROM staging.stg_sessions WHERE is_ghost = TRUE

    UNION ALL

    -- Ghost sessions must all have user_id = -1
    SELECT 'ghost sessions all have user_id = -1',
           COUNT(*), 6140
    FROM staging.stg_sessions WHERE is_ghost = TRUE AND user_id = -1

    UNION ALL

    -- Ghost sessions must have traffic = Untracked
    SELECT 'ghost sessions traffic = Untracked',
           COUNT(*), 6140
    FROM staging.stg_sessions
    WHERE is_ghost = TRUE
      AND traffic_source = 'Untracked'
      AND traffic_medium = 'Untracked'

    UNION ALL

    -- Ghost sessions must have a valid session_date (inferred from events)
    SELECT 'ghost sessions with NULL session_date (must be 0)',
           COUNT(*), 0
    FROM staging.stg_sessions WHERE is_ghost = TRUE AND session_date IS NULL

    UNION ALL

    -- No sessions_id should appear in both real and ghost
    SELECT 'duplicate sessions_id between real and ghost (must be 0)',
           COUNT(*), 0
    FROM (
        SELECT sessions_id FROM staging.stg_sessions
        GROUP BY sessions_id HAVING COUNT(*) > 1
    ) dupes
) t
ORDER BY status DESC, check_name;


-- ─────────────────────────────────────────────────────────────
-- SECTION 3 — SENTINEL USER VERIFICATION
-- ─────────────────────────────────────────────────────────────

SELECT
    check_name,
    actual,
    expected,
    CASE WHEN actual = expected THEN 'PASS' ELSE 'FAIL' END AS status
FROM (
    -- Sentinel row must exist
    SELECT 'sentinel user_id=-1 exists' AS check_name,
           COUNT(*) AS actual, 1 AS expected
    FROM staging.stg_users WHERE user_id = -1

    UNION ALL

    -- Sentinel must be flagged
    SELECT 'sentinel is_sentinel=TRUE',
           COUNT(*), 1
    FROM staging.stg_users WHERE user_id = -1 AND is_sentinel = TRUE

    UNION ALL

    -- Sentinel must have NULL registration_date
    SELECT 'sentinel registration_date is NULL',
           COUNT(*), 1
    FROM staging.stg_users WHERE user_id = -1 AND registration_date IS NULL

    UNION ALL

    -- Sentinel age_group must be Unknown
    SELECT 'sentinel age_group = Unknown',
           COUNT(*), 1
    FROM staging.stg_users WHERE user_id = -1 AND age_group = 'Unknown'

    UNION ALL

    -- Real users must NOT be flagged as sentinel
    SELECT 'real users is_sentinel=FALSE (count)',
           COUNT(*), 9156
    FROM staging.stg_users WHERE is_sentinel = FALSE
) t
ORDER BY status DESC, check_name;


-- ─────────────────────────────────────────────────────────────
-- SECTION 4 — TYPE CASTING VERIFICATION
-- Verify dates were correctly converted from VARCHAR
-- ─────────────────────────────────────────────────────────────

SELECT
    check_name,
    actual,
    expected,
    CASE WHEN actual = expected THEN 'PASS' ELSE 'FAIL' END AS status
FROM (
    -- No NULL session_date in any row (both real and ghost)
    SELECT 'stg_sessions no NULL session_date' AS check_name,
           COUNT(*) AS actual, 0 AS expected
    FROM staging.stg_sessions WHERE session_date IS NULL

    UNION ALL

    -- No NULL event_date in stg_events
    SELECT 'stg_events no NULL event_date',
           COUNT(*), 0
    FROM staging.stg_events WHERE event_date IS NULL

    UNION ALL

    -- No NULL registration_date for real users
    SELECT 'stg_users no NULL registration_date (real users only)',
           COUNT(*), 0
    FROM staging.stg_users WHERE is_sentinel = FALSE AND registration_date IS NULL

    UNION ALL

    -- No NULL transaction_timestamp
    SELECT 'stg_transactions no NULL transaction_timestamp',
           COUNT(*), 0
    FROM staging.stg_transactions WHERE transaction_timestamp IS NULL

    UNION ALL

    -- session_date must be within valid range 2019-2023
    SELECT 'stg_sessions date out of range (must be 0)',
           COUNT(*), 0
    FROM staging.stg_sessions
    WHERE session_date < '2019-01-01' OR session_date > '2023-12-31'
) t
ORDER BY status DESC, check_name;


-- ─────────────────────────────────────────────────────────────
-- SECTION 5 — NULL HANDLING VERIFICATION
-- Verify traffic NULLs were replaced with 'Unknown'
-- ─────────────────────────────────────────────────────────────

SELECT
    check_name,
    actual,
    expected,
    CASE WHEN actual = expected THEN 'PASS' ELSE 'FAIL' END AS status
FROM (
    -- No NULL traffic_medium in real sessions (should be 'Unknown')
    SELECT 'real sessions no NULL traffic_medium' AS check_name,
           COUNT(*) AS actual, 0 AS expected
    FROM staging.stg_sessions
    WHERE is_ghost = FALSE AND traffic_medium IS NULL

    UNION ALL

    -- No NULL traffic_source in real sessions
    SELECT 'real sessions no NULL traffic_source',
           COUNT(*), 0
    FROM staging.stg_sessions
    WHERE is_ghost = FALSE AND traffic_source IS NULL

    UNION ALL

    -- traffic_name should never be the string 'nan' (pandas artifact)
    SELECT 'stg_sessions traffic_name not string nan',
           COUNT(*), 0
    FROM staging.stg_sessions WHERE traffic_name = 'nan'

    UNION ALL

    -- 14,682 real sessions should have traffic_medium = Unknown
    SELECT 'real sessions with traffic_medium = Unknown',
           COUNT(*), 14682
    FROM staging.stg_sessions
    WHERE is_ghost = FALSE AND traffic_medium = 'Unknown'

    UNION ALL

    -- voucher_id must have no NULLs (defaulted to 4 = No Discounts)
    SELECT 'stg_transactions no NULL voucher_id',
           COUNT(*), 0
    FROM staging.stg_transactions WHERE voucher_id IS NULL
) t
ORDER BY status DESC, check_name;


-- ─────────────────────────────────────────────────────────────
-- SECTION 6 — BUSINESS RULE VERIFICATION
-- ─────────────────────────────────────────────────────────────

SELECT
    check_name,
    actual,
    expected,
    CASE WHEN actual = expected THEN 'PASS' ELSE 'FAIL' END AS status
FROM (
    -- Amount must equal qty * price
    SELECT 'stg_transaction_items amount = qty * price' AS check_name,
           COUNT(*) AS actual, 0 AS expected
    FROM staging.stg_transaction_items
    WHERE product_amount <> product_qty * product_price

    UNION ALL

    -- No zero or negative qty
    SELECT 'stg_transaction_items qty > 0',
           COUNT(*), 0
    FROM staging.stg_transaction_items WHERE product_qty <= 0

    UNION ALL

    -- No zero or negative price
    SELECT 'stg_transaction_items price > 0',
           COUNT(*), 0
    FROM staging.stg_transaction_items WHERE product_price <= 0

    UNION ALL

    -- Status must only be completed or canceled
    SELECT 'stg_transactions valid status only',
           COUNT(*), 0
    FROM staging.stg_transactions
    WHERE status NOT IN ('completed', 'canceled')

    UNION ALL

    -- age_group must not contain typo 'Millennila'
    SELECT 'stg_users no Millennila typo',
           COUNT(*), 0
    FROM staging.stg_users WHERE age_group = 'Millennila'

    UNION ALL

    -- All 5 valid age_groups must exist (Millennial spelled correctly)
    SELECT 'stg_users Millennial spelled correctly',
           COUNT(*), 1
    FROM (
        SELECT DISTINCT age_group FROM staging.stg_users
        WHERE age_group = 'Millennial'
    ) t
) t
ORDER BY status DESC, check_name;


-- ─────────────────────────────────────────────────────────────
-- SECTION 7 — SUMMARY
-- Quick overall result — run this last
-- ─────────────────────────────────────────────────────────────

WITH all_checks AS (
    SELECT CASE WHEN COUNT(*) = 4       THEN 'PASS' ELSE 'FAIL' END AS voucher_ok       FROM staging.stg_voucher
    -- add all from above...
)
SELECT
    (SELECT COUNT(*) FROM staging.stg_sessions)             AS stg_sessions_total,
    (SELECT COUNT(*) FROM staging.stg_sessions WHERE is_ghost = FALSE) AS real_sessions,
    (SELECT COUNT(*) FROM staging.stg_sessions WHERE is_ghost = TRUE)  AS ghost_sessions,
    (SELECT COUNT(*) FROM staging.stg_users WHERE user_id = -1)        AS sentinel_exists,
    (SELECT COUNT(*) FROM staging.stg_users WHERE age_group = 'Millennila') AS typo_count,
    (SELECT COUNT(*) FROM staging.stg_sessions WHERE traffic_name = 'nan')  AS nan_string_count,
    (SELECT COUNT(*) FROM staging.stg_transaction_items
     WHERE product_amount <> product_qty * product_price)              AS amount_mismatch_count,
    CASE
        WHEN (SELECT COUNT(*) FROM staging.stg_sessions) = 89626
         AND (SELECT COUNT(*) FROM staging.stg_users WHERE user_id = -1) = 1
         AND (SELECT COUNT(*) FROM staging.stg_users WHERE age_group = 'Millennila') = 0
         AND (SELECT COUNT(*) FROM staging.stg_sessions WHERE traffic_name = 'nan') = 0
         AND (SELECT COUNT(*) FROM staging.stg_transaction_items
              WHERE product_amount <> product_qty * product_price) = 0
        THEN 'STAGING OK — SAFE TO PROCEED TO DWH'
        ELSE 'STAGING HAS ISSUES — DO NOT PROCEED TO DWH'
    END AS overall_status;
