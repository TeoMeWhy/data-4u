select   
    order_id as idOrder,
    order_item_id as idOrderItem,
    product_id as idProduct,
    seller_id as idSeller,
    shipping_limit_date as dtShippingLimit,
    price as vlPrice,
    freight_value as vlFreight

from bronze_olist.olist_order_items_dataset