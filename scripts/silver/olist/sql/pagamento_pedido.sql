select
  order_id as idPedido,
  int(payment_sequential) as idPagamentoPedido,
  payment_type as descTipoPagamento,
  int(payment_installments) as nrPacelas,
  float(payment_value) as vlPagamento

from bronze_olist.order_payments