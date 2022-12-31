select
  product_id as idProduto,
  product_category_name as descCategoria,
  int(product_name_lenght) as nrTamanhoNome,
  int(product_description_lenght) as nrTamanhoDescricao,
  int(product_photos_qty) as nrFotos,
  int(product_weight_g) as vlPesoGramas,
  int(product_length_cm) as vlComprimentoCm,
  int(product_height_cm) as vlAlturaCm,
  int(product_width_cm) as vlLarguraCm

from bronze_olist.products