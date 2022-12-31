select
    order_id as idPedido,
    order_item_id as idPedidoItem,
    product_id as idProduto,
    seller_id as idVendedor,
    to_timestamp(shipping_limit_date) as dtLimiteEnvio,
    float(price) as vlPreco,
    float(freight_value) as vlFrete

from bronze_olist.order_items