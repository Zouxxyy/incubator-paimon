-- source
CREATE TEMPORARY TABLE datagen_orders
(
  order_name        STRING
  ,order_user_id     BIGINT
  ,order_shop_id     BIGINT
  ,order_product_id  BIGINT
  ,order_fee         DECIMAL(20, 2)
  ,order_create_time TIMESTAMP(3)
  ,order_update_time TIMESTAMP(3)
  ,order_state       INT
)
WITH (
    'connector' = 'datagen',
    'rows-per-second' = '5000000',
    'fields.order_user_id.kind' = 'random',
    'fields.order_user_id.min' = '1',
    'fields.order_user_id.max' = '100000',
    'fields.order_shop_id.kind' = 'random',
    'fields.order_shop_id.min' = '1',
    'fields.order_shop_id.max' = '10000',
    'fields.order_product_id.kind' = 'random',
    'fields.order_product_id.min' = '1',
    'fields.order_product_id.max' = '1000',
    'fields.order_fee.kind' = 'random',
    'fields.order_fee.min' = '0.1',
    'fields.order_fee.max' = '10000.0',
    'fields.order_state.kind' = 'random',
    'fields.order_state.min' = '1',
    'fields.order_state.max' = '5'
)
;

-- __SINK_DDL_BEGIN__

CREATE TABLE IF NOT EXISTS ${SINK_NAME} (
    order_id          STRING,
    order_name        STRING,
    order_user_id     BIGINT,
    order_shop_id     BIGINT,
    order_product_id  BIGINT,
    order_fee         DECIMAL(20, 2),
    order_create_time TIMESTAMP(3),
    order_update_time TIMESTAMP(3),
    order_state       INT
) WITH (
    ${DDL_TEMPLATE}
);

-- __SINK_DDL_END__

INSERT INTO ${SINK_NAME}
SELECT
  UUID() AS order_id,
  order_name,
  order_user_id,
  order_shop_id,
  order_product_id,
  order_fee,
  NOW() AS order_create_time,
  NOW() AS order_update_time,
  order_state
FROM datagen_orders;
