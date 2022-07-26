select
    geolocation_zip_code_prefix as nrZipPrefix,
    geolocation_lat as nrLat,
    geolocation_lng as nrLong,
    geolocation_city as descCity,
    geolocation_state as descState

from bronze_olist.olist_geolocation_dataset