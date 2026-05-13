CREATE SCHEMA IF NOT EXISTS mart;

-- Management
-- MART 1 Monthly Revenue and Order Summary
-- Purpose : Monitor revenue, growth and total order

CREATE OR REPLACE VIEW mart.monthly_revenue AS 
SELECT
	d.year,
	d.month,
	d.month_name,
	COUNT(ft.transaction_key) AS total_orders,
	SUM(ft.is_completed) AS completed_orders,
	SUM(ft.is_canceled) AS canceled_orders,
	ROUND(100.0*SUM(ft.is_canceled)/NULLIF(COUNT(*),0),2) AS cancellation_rate_pct,
	SUM(ft.revenue) AS total_revenue,
	ROUND(AVG(total_amount) FILTER (WHERE status='completed'),0) AS avg_order_value
FROM dw.fact_transactions ft
INNER JOIN dw.dim_date d ON ft.date_key = d.date_key
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year,d.month;

-- PRODUCT
-- MART 2 Product Performance
-- Purpose: Monitor top and low selling products 
CREATE OR REPLACE VIEW mart.product_performance AS
SELECT
	dp.product_category,
	dp.product_name,
	COUNT(DISTINCT fti.transaction_key) AS total_orders,
	SUM(fti. product_qty) AS total_units_sold,
	SUM(fti.product_amount) FILTER (WHERE status='completed') AS total_revenue,
	ROUND(AVG(fti.product_price),0) AS avg_price
FROM dw.fact_transaction_items fti
INNER JOIN dw.dim_product dp ON fti.product_key = dp.product_key
GROUP BY dp.product_category, dp.product_name
ORDER BY total_revenue DESC;

-- MART 3: Customer Segmentation Summary
-- Purpose:

CREATE OR REPLACE VIEW mart.customer_segment AS 
SELECT 
	du.age_group,
	du.gender,
	COALESCE(du.location, 'Unknown') AS location,
	COUNT(DISTINCT ft.user_key) AS total_user,
	COUNT(ft.transaction_key) AS total_orders,
	SUM(ft.revenue) AS total_revenue,
	ROUND(SUM(ft.revenue)/NULLIF(COUNT(DISTINCT ft.user_key),0),0) AS revenue_per_customer
FROM dw.fact_transactions ft 
INNER JOIN dw.dim_user du ON ft.user_key = du.user_key
WHERE ft.status = 'completed'
GROUP BY du.age_group, du.gender, du.location
ORDER BY total_revenue DESC;

-- MART 4: Funnel Analysis (Conversion Funnel)
-- Purpose:

CREATE OR REPLACE VIEW mart.funnel_analysis AS
WITH funnel_steps AS (
	SELECT
		session_key,
		MAX(CASE WHEN event = 'productview' THEN 1 ELSE 0 END) AS did_productview,
		MAX(CASE WHEN event = 'addtocart' THEN 1 ELSE 0 END) AS did_addtocart,
		MAX(CASE WHEN event = 'viewcart' THEN 1 ELSE 0 END) AS did_viewcart,
		MAX(CASE WHEN event = 'chooseaddress' THEN 1 ELSE 0 END) AS did_chooseaddress,
		MAX(CASE WHEN event = 'choosedelivery' THEN 1 ELSE 0 END) AS did_choosedelivery,
		MAX(CASE WHEN event = 'changepaymentmethod' THEN 1 ELSE 0 END) AS did_changepayment,
		MAX(CASE WHEN event = 'checkout' THEN 1 ELSE 0 END) AS did_checkout,
		MAX(CASE WHEN event = 'complete' THEN 1 ELSE 0 END) AS did_complete,
		MAX(CASE WHEN event = 'cancel' THEN 1 ELSE 0 END) AS did_cancel
	FROM dw.fact_events fe
	GROUP BY session_key
)
SELECT
	SUM(did_productview) AS step1_product_view,
	SUM(did_addtocart) AS step2_add_to_cart,
	SUM(did_viewcart) AS step3_view_cart,
	SUM(did_chooseaddress) AS step4_choose_address,
	SUM(did_choosedelivery) AS step5_choose_delivery,
	SUM(did_changepayment) AS step6_choose_payment,
	SUM(did_checkout) AS step7_checkout,
	SUM(did_complete) AS completed_sessions,
	SUM(did_cancel) AS canceled_sessions,
	COUNT(*) AS total_sessions,
	ROUND(100.0*SUM(did_checkout)/NULLIF(SUM(did_productview),0),2) AS view_to_checkout_rate_pct,
	ROUND(100.0*SUM(did_complete)/NULLIF(SUM(did_checkout),0),2) AS checkout_to_complete_rate_pct,
	ROUND(100.0*SUM(did_cancel)/NULLIF(SUM(did_checkout),0),2) AS checkout_to_cancelled_rate_pct
FROM funnel_steps;

-- MART 5: Segmented Funnel Analysis (Conversion Funnel)
-- Purpose:

CREATE OR REPLACE VIEW mart.funnel_by_segment AS
WITH funnel_steps AS (
	SELECT
		fe.session_key,
		fe.user_key,
		MAX(CASE WHEN event = 'productview' THEN 1 ELSE 0 END) AS did_productview,
		MAX(CASE WHEN event = 'addtocart' THEN 1 ELSE 0 END) AS did_addtocart,
		MAX(CASE WHEN event = 'viewcart' THEN 1 ELSE 0 END) AS did_viewcart,
		MAX(CASE WHEN event = 'chooseaddress' THEN 1 ELSE 0 END) AS did_chooseaddress,
		MAX(CASE WHEN event = 'choosedelivery' THEN 1 ELSE 0 END) AS did_choosedelivery,
		MAX(CASE WHEN event = 'changepaymentmethod' THEN 1 ELSE 0 END) AS did_changepayment,
		MAX(CASE WHEN event = 'checkout' THEN 1 ELSE 0 END) AS did_checkout,
		MAX(CASE WHEN event = 'complete' THEN 1 ELSE 0 END) AS did_complete,
		MAX(CASE WHEN event = 'cancel' THEN 1 ELSE 0 END) AS did_cancel
	FROM dw.fact_events fe
	INNER JOIN dw.fact_sessions fs ON fe.session_key = fs.session_key  
	GROUP BY fe.session_key, fe.user_key
)
SELECT
	du.age_group,
	du.gender,
	COALESCE(du.location, 'Unknown') AS location,
	COUNT(*) AS total_sessions,
	SUM(did_productview) AS step1_product_view,
	SUM(did_addtocart) AS step2_add_to_cart,
	SUM(did_viewcart) AS step3_view_cart,
	SUM(did_chooseaddress) AS step4_choose_address,
	SUM(did_choosedelivery) AS step5_choose_delivery,
	SUM(did_changepayment) AS step6_choose_payment,
	SUM(did_checkout) AS step7_checkout,
	SUM(did_complete) AS completed_sessions,
	SUM(did_cancel) AS canceled_sessions,
	ROUND(100.0*SUM(did_checkout)/NULLIF(SUM(did_productview),0),2) AS view_to_checkout_rate_pct,
	ROUND(100.0*SUM(did_complete)/NULLIF(SUM(did_checkout),0),2) AS checkout_to_complete_rate_pct,
	ROUND(100.0*SUM(did_cancel)/NULLIF(SUM(did_checkout),0),2) AS checkout_to_cancelled_rate_pct
FROM funnel_steps fs
INNER JOIN dw.dim_user du ON fs.user_key = du.user_key
GROUP BY du.age_group, du.gender, du.location;


-- MART 6: Traffic Source Performance
-- Performance: 

CREATE OR REPLACE VIEW mart.traffic_performance AS
WITH trx AS (
	SELECT
		session_key,
		SUM(revenue) AS revenue,
		COUNT(*) AS total_transactions
	FROM dw.fact_transactions
	GROUP BY session_key
)
SELECT
	dt.traffic_source,
	dt.traffic_medium,
	COUNT(DISTINCT fs.user_key) AS unique_users,
	COUNT(DISTINCT fs.session_key) AS total_sessions,
	SUM(COALESCE(trx.total_transactions,0)) AS total_transactions,
	SUM(COALESCE(trx.revenue,0)) AS total_revenue,
	ROUND(SUM(COALESCE(trx.revenue,0))/NULLIF(COUNT(DISTINCT fs.session_key),0),0) AS revenue_per_session
FROM dw.fact_sessions fs
INNER JOIN dw.dim_traffic dt
	ON fs.traffic_key = dt.traffic_key
LEFT JOIN trx
	ON fs.session_key = trx.session_key
GROUP BY dt.traffic_source, dt.traffic_medium
ORDER BY total_revenue DESC;

-- MART 7: Voucher Usage Impact
-- Performance: 

CREATE OR REPLACE VIEW mart.voucher_impact AS 
SELECT
	dv.voucher_type,
	COUNT(ft.transaction_key) AS total_orders,
	SUM(ft.is_completed) AS completed_orders,
	SUM(ft.revenue) AS total_revenue,
	ROUND(AVG(total_amount) FILTER (WHERE status='completed'),0) AS avg_order_value,
	ROUND(100.0*SUM(ft.is_canceled)/NULLIF(COUNT(*),0),2) AS cancellation_rate_pct
FROM dw.fact_transactions ft
INNER JOIN dw.dim_voucher dv ON ft.voucher_key = dv.voucher_key
GROUP BY dv.voucher_type
ORDER BY total_revenue DESC;

-- MART 8: Payment Method Preference
-- Performance: 

CREATE OR REPLACE VIEW mart.payment_preference AS
SELECT
    dpm.payment_method,
    COUNT(ft.transaction_key)                        AS total_orders,
    SUM(ft.revenue)                                  AS total_revenue,
    ROUND(100.0 * COUNT(ft.transaction_key)
          / SUM(COUNT(*)) OVER (), 2)                AS order_share_pct,
    ROUND(100.0 * SUM(ft.revenue)
          / SUM(SUM(ft.revenue)) OVER (), 2)                AS revenue_share_pct
FROM dw.fact_transactions ft
JOIN dw.dim_payment_method dpm ON ft.payment_method_key = dpm.payment_method_key
WHERE ft.status = 'completed'
GROUP BY dpm.payment_method
ORDER BY total_orders DESC;

-- MART 9: User Cohort (by Registration Month)
-- Performance: 

CREATE OR REPLACE VIEW mart.user_cohort AS 
SELECT
	CAST(DATE_TRUNC('month', du.registration_date) AS DATE) AS cohort_month,
	COUNT(DISTINCT du.user_key) AS registered_users,
	COUNT(ft.transaction_key) AS total_orders,
	SUM(ft.revenue) AS total_revenue,
	ROUND(SUM(ft.revenue)/NULLIF(COUNT(DISTINCT du.user_key), 0), 0) AS ltv_per_usr
FROM dw.dim_user du
LEFT JOIN dw.fact_transactions ft ON du.user_key = ft.user_key
GROUP BY DATE_TRUNC('month', du.registration_date)
ORDER BY cohort_month;