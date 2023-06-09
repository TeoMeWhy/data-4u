WITH tb_all AS (

    SELECT t1.idCompany,
           t1.idGame,
           t1.flDeveloper,
           t1.flPorting,
           t1.flPublisher,
           t1.flSupporting,
           t2.dtRelease,
           t2.dtRelease,
           t2.vlRatingExternal,
           t2.qtdeRatingExternal,
           t2.vlRatingIGDB,
           t2.qtdeRatingIGDB,
           t2.vlRatingTotal,
           t2.qtdeRatingTotal,
           t2.qtdeFollows

    FROM silver.igdb.involved_companies AS t1

    LEFT JOIN silver.igdb.games AS t2
    ON t1.idGame = t2.idGame

    WHERE t2.dtRelease < '{date}'
    AND t2.dtRelease >= add_months('{date}', -12)
)

SELECT idCompany,
       '{date}' AS dtRef,
       count( distinct idGame) AS qtdeGamesInvolved01Years,
       count(distinct CASE WHEN flDeveloper = TRUE THEN idGame END) AS qtdeGamesDeveloper01Years,
       count(distinct CASE WHEN flPorting = TRUE THEN idGame END) AS qtdeGamesPorting01Years,
       count(distinct CASE WHEN flPublisher = TRUE THEN idGame END) AS qtdeGamesPublisher01Years,
       count(distinct CASE WHEN flSupporting = TRUE THEN idGame END) AS qtdeGamesSupporting01Years,
       max(date_diff('{date}', dtRelease)) AS qtdeDaysFirstRelease01Years,
       min(date_diff('{date}', dtRelease)) AS qtdeDaysLastRelease01Years,
       coalesce(avg(vlRatingExternal),0) AS avgRatingExternal01Years ,
       coalesce(avg(qtdeRatingExternal),0) AS avgVotesRatingExternal01Years,
       coalesce(sum(qtdeRatingExternal),0) AS TotalVotesRatingExternal01Years,
       coalesce(avg(vlRatingIGDB),0) AS avgRatingIGDB01Years,
       coalesce(avg(qtdeRatingIGDB),0) AS avgVotesRatingIGDB01Years,
       coalesce(sum(qtdeRatingIGDB),0) AS TotalVotesRatingIGDB01Years,
       coalesce(avg(vlRatingTotal),0) AS avgRatingTotal01Years,
       coalesce(avg(qtdeRatingTotal),0) AS avgVotesRatingTotal01Years,
       coalesce(sum(qtdeRatingTotal),0) AS TotalVotesRatingTotal01Years,
       coalesce(avg(qtdeFollows),0) AS avgFollows01Years,
       coalesce(sum(qtdeFollows),0) AS TotalFollows01Years

FROM tb_all

GROUP BY idCompany, dtRef