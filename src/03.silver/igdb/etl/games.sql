SELECT aggregated_rating AS vlRatingExternal,
       aggregated_rating_count AS qtdeRatingExternal,
       CASE
            WHEN category = 0 THEN 'main_game'
            WHEN category = 1 THEN 'dlc_addon'
            WHEN category = 2 THEN 'expansion'
            WHEN category = 3 THEN 'bundle'
            WHEN category = 4 THEN 'standalone_expansion'
            WHEN category = 5 THEN 'mod'
            WHEN category = 6 THEN 'episode'
            WHEN category = 7 THEN 'season'
            WHEN category = 8 THEN 'remake'
            WHEN category = 9 THEN 'remaster'
            WHEN category = 10 THEN 'expanded_game'
            WHEN category = 11 THEN 'port'
            WHEN category = 12 THEN 'fork'
            WHEN category = 13 THEN 'pack'
            WHEN category = 14 THEN 'update'
            ELSE 'missing_category'
        END AS descCategory,

        collection AS idCollection,
        cover AS idCover,
        from_unixtime(created_at) AS dtCreated,
        from_unixtime(first_release_date) AS dtRelease,
        follows AS qtdeFollows,
        franchise AS descMainFranchise,
        id AS idGame,
        name AS descGameName,
        parent_game AS idParentGame,
        rating AS vlRatingIGDB,
        rating_count AS qtdeRatingIGDB,
        slug AS descSlug,
        total_rating AS vlRatingTotal,
        total_rating_count AS qtdeRatingTotal,
        from_unixtime(updated_at) AS dtUpdated,
        url AS urlGame,
        version_parent AS idParentVersionGame,
        version_title AS descVersionTitle

FROM bronze.igdb.games

ORDER BY id