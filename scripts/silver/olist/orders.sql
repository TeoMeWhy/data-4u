select   
    order_id as idOrder,
    customer_id as idCustomer,
    order_status as descStatus,
    order_purchase_timestamp as dtPurchase,
    order_approved_at as dtApproved,
    order_delivered_carrier_date as dtDeliveredCarrier,
    order_delivered_customer_date as dtDeliveredCustomer,
    order_estimated_delivery_date as dtEstimatedDelivered

from bronze_olist.olist_orders_dataset