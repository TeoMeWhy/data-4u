SELECT country AS idCountry,
       from_unixtime(created_at) AS dtCreated,
       description AS descDescription,
       id AS idCompany,
       name AS descName,
       parent AS idParentCompany,
       slug AS descSlug,
       from_unixtime(start_date) AS dtStart

FROM bronze.igdb.companies