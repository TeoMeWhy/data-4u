SELECT 
       id AS idCompanyGame,
       company AS idCompany,
       game AS idGame,
       developer AS flDeveloper,
       porting AS flPorting,
       publisher AS flPublisher,
       supporting AS flSupporting,
       from_unixtime(created_at) AS dtCreated,
       from_unixtime(updated_at) AS dtUpdated

FROM bronze.igdb.involved_companies

ORDER BY game