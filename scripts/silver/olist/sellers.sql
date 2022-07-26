select
    seller_id as idSeller,
    seller_zip_code_prefix as nrZipPrefix,
    seller_city as descCity,
    seller_state as descState

from bronze_olist.olist_sellers_dataset