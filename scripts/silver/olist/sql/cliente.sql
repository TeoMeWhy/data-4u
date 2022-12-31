select
  customer_id as idCliente,
  customer_unique_id as idClienteUnico,
  customer_zip_code_prefix as codCep,
  customer_city as descCidade,
  customer_state as descUF

from bronze_olist.customers