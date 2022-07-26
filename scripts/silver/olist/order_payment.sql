select
   
    order_id as idOrder,
    payment_sequential as nrSequential,
    payment_type as descType,
    payment_installments as nrInstallments,
    payment_value as vlPayment

from bronze_olist.olist_order_payments_dataset