WITH tb_company AS (

  SELECT idCompany,
         dtRef

  FROM feature_store.igdb_company_general
  WHERE dtRef <= '2022-05-01'
  AND qtdeDaysLastRelease <= 365 * 3
  AND qtdeDaysFirstRelease > 365
  AND qtdeGamesInvolved > 1

),

tb_games AS (

  SELECT t1.idGame,
        t1.dtRelease,
        -- t1.vlRatingTotal,
        -- t1.qtdeRatingTotal,
        t2.idCompany
        
  FROM silver.igdb.games AS t1

  LEFT JOIN silver.igdb.involved_companies AS t2
  ON t1.idGame = t2.idGame

  WHERE t1.vlRatingTotal >= 80
  AND t1.qtdeRatingTotal >= 30
  AND (t2.flDeveloper = TRUE OR t2.flPublisher = TRUE)

),

tb_join AS (

  SELECT t1.*,
        t2.idGame,
        t2.dtRelease       

  FROM tb_company AS t1

  LEFT JOIN tb_games AS t2
  ON t1.idCompany = t2.idCompany
  AND t1.dtRef < t2.dtRelease
  AND t2.dtRelease <= (date(t1.dtRef) + INTERVAL 1 YEAR)

  ORDER BY t1.idCompany, t1.dtRef

),

tb_final AS (
    SELECT idCompany,
          dtRef,
          max(CASE WHEN idGame IS NOT null THEN 1 ELSE 0 END) AS flSuccessGame

    FROM tb_join

    GROUP BY idCompany, dtRef
    ORDER BY idCompany, dtRef

)

select *
from tb_final