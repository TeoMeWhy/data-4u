SELECT id AS idCollection,
       explode(games) AS idGame

FROM bronze.igdb.collections
WHERE games IS NOT null