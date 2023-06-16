SELECT id AS idCharacter,
       name AS descName,
       description AS descDescription,
       from_unixtime(created_at) AS dtCreated,
       from_unixtime(updated_at) AS dtUpdated,
       CASE
           WHEN int(gender) = 0 THEN 'Male'
           WHEN int(gender) = 1 THEN 'Female'
           WHEN int(gender) = 2 THEN 'Other'
           ELSE 'Missing'
       END AS descGender,
       CASE
           WHEN int(species) = 1 THEN 'Human'
           WHEN int(species) = 2 THEN 'Alien'
           WHEN int(species) = 3 THEN 'Animal'
           WHEN int(species) = 4 THEN 'Android'
           WHEN int(species) = 5 THEN 'Unknown'
           ELSE 'Missing'
       END AS descSpecie

FROM bronze.igdb.characters