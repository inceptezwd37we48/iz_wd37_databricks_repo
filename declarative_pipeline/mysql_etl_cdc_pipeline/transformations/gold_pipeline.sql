

CREATE OR REFRESH MATERIALIZED VIEW retail.gold.orders_current AS
SELECT
  order_id,
  customer_id,
  order_status,
  order_amount,
  order_ts,
  updated_at,
  __START_AT AS status_effective_from
FROM retail.silver.orders
WHERE __END_AT IS NULL;


CREATE OR REFRESH MATERIALIZED VIEW retail.gold.daily_revenue (
  CONSTRAINT revenue_non_negative EXPECT (total_revenue >= 0) ON VIOLATION FAIL UPDATE
) AS
SELECT
  date(order_ts)   AS order_date,
  order_status,
  count(*)         AS order_count,
  sum(order_amount) AS total_revenue
FROM retail.gold.orders_current
GROUP BY date(order_ts), order_status;


CREATE OR REFRESH MATERIALIZED VIEW retail.gold.order_status_history AS
SELECT
  order_id,
  customer_id,
  order_status,
  order_amount,
  __START_AT AS effective_from,
  __END_AT   AS effective_to,
  __END_AT IS NULL AS is_current
FROM retail.silver.orders
ORDER BY order_id, effective_from;


CREATE OR REFRESH MATERIALIZED VIEW retail.gold.customer_ltv AS
SELECT
  c.customer_id,
  c.first_name,
  c.last_name,
  c.city,
  c.country,
  count(o.order_id)      AS lifetime_orders,
  coalesce(sum(o.order_amount), 0) AS lifetime_value
FROM retail.silver.customers c
LEFT JOIN retail.gold.orders_current o
  ON o.customer_id = c.customer_id AND o.order_status != 'CANCELLED'
GROUP BY c.customer_id, c.first_name, c.last_name, c.city, c.country;