select
    customer_id as idCustomer,
    customer_unique_id as idUniqueCustomer,
    customer_zip_code_prefix as nrZipPrefix,
    customer_city as descCity,
    customer_state as descState

from bronze_olist.olist_customers_dataset