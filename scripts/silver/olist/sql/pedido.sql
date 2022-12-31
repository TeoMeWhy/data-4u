select
  order_id as idPedido,
  customer_id as idCliente,
  order_status as descSituacao,
  to_timestamp(order_purchase_timestamp) as dtPedido,
  to_timestamp(order_approved_at) as dtAprovado,
  to_timestamp(order_delivered_carrier_date) as dtEnvio,
  to_timestamp(order_delivered_customer_date) as dtEntregue,
  to_timestamp(order_estimated_delivery_date) as dtEstimativaEntrega

from bronze_olist.orders