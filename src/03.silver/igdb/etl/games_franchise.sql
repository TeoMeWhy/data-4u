SELECT 
      id AS idGame,
      explode(franchises) AS idFranchise

FROM bronze.igdb.games
WHERE franchises IS NOT null