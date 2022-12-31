select 
  geolocation_zip_code_prefix as codCep,
  float(geolocation_lat) as nrLatitude,
  float(geolocation_lng) as nrLongitude,
  geolocation_city as descCidade,
  geolocation_state as descUF

from bronze_olist.geolocation