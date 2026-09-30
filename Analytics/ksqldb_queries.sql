-- Register processed orders as a stream
CREATE STREAM processed_orders (
    order_id              INT,
    user_id               VARCHAR,
    item                  VARCHAR,
    amount                DOUBLE,
    category              VARCHAR,
    customer_name         VARCHAR,
    customer_tier         VARCHAR,
    discount              DOUBLE,
    amount_after_discount DOUBLE,
    processed_at          VARCHAR
) WITH (
    KAFKA_TOPIC  = 'processed-orders',
    VALUE_FORMAT = 'JSON',
    PARTITIONS   = 3
);

-- Running total per user
CREATE TABLE user_lifetime_stats
    WITH (KAFKA_TOPIC = 'user-lifetime-stats', VALUE_FORMAT = 'JSON')
AS SELECT
    user_id,
    customer_name,
    customer_tier,
    COUNT(*)                  AS total_orders,
    SUM(amount_after_discount) AS lifetime_spend,
    AVG(amount_after_discount) AS avg_order_value,
    MAX(amount_after_discount) AS largest_order
FROM processed_orders
GROUP BY user_id, customer_name, customer_tier
EMIT CHANGES;

-- Revenue per category per minute
CREATE TABLE category_revenue
    WITH (KAFKA_TOPIC = 'category-revenue', VALUE_FORMAT = 'JSON')
AS SELECT
    category,
    COUNT(*)                   AS orders,
    SUM(amount_after_discount) AS revenue,
    WINDOWSTART                AS window_start,
    WINDOWEND                  AS window_end
FROM processed_orders
WINDOW TUMBLING (SIZE 1 MINUTE)
GROUP BY category
EMIT CHANGES;

-- High value orders stream (for alerting)
CREATE STREAM high_value_orders
    WITH (KAFKA_TOPIC = 'high-value-orders', VALUE_FORMAT = 'JSON')
AS SELECT *
FROM processed_orders
WHERE amount_after_discount > 5.0
EMIT CHANGES;