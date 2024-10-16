select order_id, order_item_id, product_id, seller_id, shipping_limit_date::date, price, freight_value
from order_items