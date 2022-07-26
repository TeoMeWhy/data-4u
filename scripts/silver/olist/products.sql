select
    product_id as idProduct,
    product_category_name as descCategoryName,
    product_name_lenght as nrNameLength,
    product_description_lenght as nrDescriptionLength,
    product_photos_qty as nrPhotos,
    product_weight_g as vlWeightGramas,
    product_length_cm as vlLengthCm,
    product_height_cm as vlHeightCm,
    product_width_cm as vlWidthCm

from bronze_olist.olist_products_dataset