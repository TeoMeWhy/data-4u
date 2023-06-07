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

)

SELECT idCompany,
       '{date}' AS dtRef,
       count( distinct idGame) AS qtdeGamesInvolved,
       count(distinct CASE WHEN flDeveloper = TRUE THEN idGame END) AS qtdeGamesDeveloper,
       count(distinct CASE WHEN flPorting = TRUE THEN idGame END) AS qtdeGamesPorting,
       count(distinct CASE WHEN flPublisher = TRUE THEN idGame END) AS qtdeGamesPublisher,
       count(distinct CASE WHEN flSupporting = TRUE THEN idGame END) AS qtdeGamesSupporting,
       max(date_diff('{date}', dtRelease)) AS qtdeDaysFirstRelease,
       min(date_diff('{date}', dtRelease)) AS qtdeDaysLastRelease,
       coalesce(avg(vlRatingExternal),0) AS avgRatingExternal ,
       coalesce(avg(qtdeRatingExternal),0) AS avgVotesRatingExternal,
       coalesce(sum(qtdeRatingExternal),0) AS TotalVotesRatingExternal,
       coalesce(avg(vlRatingIGDB),0) AS avgRatingIGDB,
       coalesce(avg(qtdeRatingIGDB),0) AS avgVotesRatingIGDB,
       coalesce(sum(qtdeRatingIGDB),0) AS TotalVotesRatingIGDB,
       coalesce(avg(vlRatingTotal),0) AS avgRatingTotal,
       coalesce(avg(qtdeRatingTotal),0) AS avgVotesRatingTotal,
       coalesce(sum(qtdeRatingTotal),0) AS TotalVotesRatingTotal,
       coalesce(avg(qtdeFollows),0) AS avgFollows,
       coalesce(sum(qtdeFollows),0) AS TotalFollows 

FROM tb_all

GROUP BY idCompany, dtRef