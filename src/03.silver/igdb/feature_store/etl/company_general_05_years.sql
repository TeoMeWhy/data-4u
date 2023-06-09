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
    AND t2.dtRelease >= date('{date}') - INTERVAL 5 year
)

SELECT idCompany,
       '{date}' AS dtRef,
       count( distinct idGame) AS qtdeGamesInvolved05Years,
       count(distinct CASE WHEN flDeveloper = TRUE THEN idGame END) AS qtdeGamesDeveloper05Years,
       count(distinct CASE WHEN flPorting = TRUE THEN idGame END) AS qtdeGamesPorting05Years,
       count(distinct CASE WHEN flPublisher = TRUE THEN idGame END) AS qtdeGamesPublisher05Years,
       count(distinct CASE WHEN flSupporting = TRUE THEN idGame END) AS qtdeGamesSupporting05Years,
       max(date_diff('{date}', dtRelease)) AS qtdeDaysFirstRelease05Years,
       min(date_diff('{date}', dtRelease)) AS qtdeDaysLastRelease05Years,
       coalesce(avg(vlRatingExternal),0) AS avgRatingExternal05Years ,
       coalesce(avg(qtdeRatingExternal),0) AS avgVotesRatingExternal05Years,
       coalesce(sum(qtdeRatingExternal),0) AS TotalVotesRatingExternal05Years,
       coalesce(avg(vlRatingIGDB),0) AS avgRatingIGDB05Years,
       coalesce(avg(qtdeRatingIGDB),0) AS avgVotesRatingIGDB05Years,
       coalesce(sum(qtdeRatingIGDB),0) AS TotalVotesRatingIGDB05Years,
       coalesce(avg(vlRatingTotal),0) AS avgRatingTotal05Years,
       coalesce(avg(qtdeRatingTotal),0) AS avgVotesRatingTotal05Years,
       coalesce(sum(qtdeRatingTotal),0) AS TotalVotesRatingTotal05Years,
       coalesce(avg(qtdeFollows),0) AS avgFollows05Years,
       coalesce(sum(qtdeFollows),0) AS TotalFollows05Years 

FROM tb_all

GROUP BY idCompany, dtRef