SELECT order_id,
       customer_id,
       order_status,
       order_approved_at::date,
       order_purchase_timestamp::date AS order_purchase_date,
       order_delivered_carrier_date::date,
       order_delivered_customer_date::date,
       order_estimated_delivery_date::date
FROM orders