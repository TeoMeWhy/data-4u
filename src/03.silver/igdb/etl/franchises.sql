SELECT from_unixtime(created_at) AS dtCreated,
       id AS idFranchise,
       name AS descName,
       slug AS descSlug,
       from_unixtime(updated_at) AS dtUpdated,
       url AS urlFranchise
       
FROM bronze.igdb.franchises