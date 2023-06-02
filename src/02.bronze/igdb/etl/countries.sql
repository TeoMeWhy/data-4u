SELECT
    alpha-2 AS alpha_2,
    alpha-3 AS alpha_3,
    country-code AS country_code,
    intermediate-region AS intermediate_region,
    intermediate-region-code AS intermediate_region-code,
    iso_3166-2 AS iso_3166_2,
    name AS name,
    region AS region,
    region-code AS region_code,
    sub-region AS sub_region,
    sub-region-code AS sub_region_code,
    NOW () dt_ingestion
FROM {table}