SELECT abbreviation AS descAbbreviation,
       alternative_name AS descAlternativeName,
       CASE
          WHEN category = 1 THEN 'console'
          WHEN category = 2 THEN 'arcade'
          WHEN category = 3 THEN 'platform'
          WHEN category = 4 THEN 'operating_system'
          WHEN category = 5 THEN 'portable_console'
          WHEN category = 6 THEN 'computer'
          ELSE 'missing category'
       END AS descCategory,
       from_unixtime(created_at) AS dtCreated,
       generation AS nrGeneration,
       id AS idPlatform,
       name AS descName,
       platform_family AS idPlatformFamily,
       slug AS descSlug,
       from_unixtime(updated_at) AS dtUpdated

FROM bronze.igdb.platforms