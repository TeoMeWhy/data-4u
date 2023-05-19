select
  order_id as idPedido,
  int(payment_sequential) as idPagamentoPedido,
  payment_type as descTipoPagamento,
  int(payment_installments) as nrParcelas,
  float(payment_value) as vlPagamento

from bronze.olist.olist_order_payments_dataset