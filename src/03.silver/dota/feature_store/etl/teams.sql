SELECT
        t1.dtReference,
        t1.idTeam,

        t1.idTeam AS idTeamRadiant,
        t1.descTeamName AS descTeamNameRadiant,
        t1.descTeamTag AS descTeamTagRadiant,

        t1.idTeam AS idTeamDire,
        t1.descTeamName AS descTeamNameDire,
        t1.descTeamTag AS descTeamTagDire,

        coalesce(avg(t2.nrFrequency),0) AS avgFrequency180,
        coalesce(min(t2.nrFrequency),0) AS minFrequency180,
        coalesce(max(t2.nrFrequency),0) AS maxFrequency180,
        coalesce(maxFrequency180/minFrequency180,0) AS txMinMaxFrequency180,

        coalesce(avg(t2.avgWin),0) AS avgWin180,
        coalesce(min(t2.avgWin),0) AS minWin180,
        coalesce(max(t2.avgWin),0) AS maxWin180,
        coalesce(maxWin180/minWin180,0) AS txMinMaxWin180,

        coalesce(avg(t2.avgCampsStacked),0) AS avgCampsStacked180,
        coalesce(min(t2.avgCampsStacked),0) AS minCampsStacked180,
        coalesce(max(t2.avgCampsStacked),0) AS maxCampsStacked180,
        coalesce(maxCampsStacked180/minCampsStacked180,0) AS txMinMaxCampsStacked180,

        coalesce(avg(t2.avgCreepsStacked),0) AS avgCreepsStacked180,
        coalesce(min(t2.avgCreepsStacked),0) AS minCreepsStacked180,
        coalesce(max(t2.avgCreepsStacked),0) AS maxCreepsStacked180,
        coalesce(maxCreepsStacked180/minCreepsStacked180,0) AS txMinMaxCreepsStacked180,

        coalesce(avg(t2.avgKills),0) AS avgKills180,
        coalesce(min(t2.avgKills),0) AS minKills180,
        coalesce(max(t2.avgKills),0) AS maxKills180,
        coalesce(maxKills180/minKills180,0) AS txMinMaxKills180,

        coalesce(avg(t2.avgAssist),0) AS avgAssist180,
        coalesce(min(t2.avgAssist),0) AS minAssist180,
        coalesce(max(t2.avgAssist),0) AS maxAssist180,
        coalesce(maxAssist180/minAssist180,0) AS txMinMaxAssist180,

        coalesce(avg(t2.avgDeaths),0) AS avgDeaths180,
        coalesce(min(t2.avgDeaths),0) AS minDeaths180,
        coalesce(max(t2.avgDeaths),0) AS maxDeaths180,
        coalesce(maxDeaths180/minDeaths180,0) AS txMinMaxDeaths180,

        coalesce(avg(t2.avgDenies),0) AS avgDenies180,
        coalesce(min(t2.avgDenies),0) AS minDenies180,
        coalesce(max(t2.avgDenies),0) AS maxDenies180,
        coalesce(maxDenies180/minDenies180,0) AS txMinMaxDenies180,

        coalesce(avg(t2.avgFirstbloodClaimed),0) AS avgFirstbloodClaimed180,
        coalesce(min(t2.avgFirstbloodClaimed),0) AS minFirstbloodClaimed180,
        coalesce(max(t2.avgFirstbloodClaimed),0) AS maxFirstbloodClaimed180,
        coalesce(maxFirstbloodClaimed180/minFirstbloodClaimed180,0) AS txMinMaxFirstbloodClaimed180,

        coalesce(avg(t2.avgGold),0) AS avgGold180,
        coalesce(min(t2.avgGold),0) AS minGold180,
        coalesce(max(t2.avgGold),0) AS maxGold180,
        coalesce(maxGold180/minGold180,0) AS txMinMaxGold180,

        coalesce(avg(t2.avgGoldMinute),0) AS avgGoldMinute180,
        coalesce(min(t2.avgGoldMinute),0) AS minGoldMinute180,
        coalesce(max(t2.avgGoldMinute),0) AS maxGoldMinute180,
        coalesce(maxGoldMinute180/minGoldMinute180,0) AS txMinMaxGoldMinute180,

        coalesce(avg(t2.avgGoldSpent),0) AS avgGoldSpent180,
        coalesce(min(t2.avgGoldSpent),0) AS minGoldSpent180,
        coalesce(max(t2.avgGoldSpent),0) AS maxGoldSpent180,
        coalesce(maxGoldSpent180/minGoldSpent180,0) AS txMinMaxGoldSpent180,

        coalesce(avg(t2.avgHeroDamage),0) AS avgHeroDamage180,
        coalesce(min(t2.avgHeroDamage),0) AS minHeroDamage180,
        coalesce(max(t2.avgHeroDamage),0) AS maxHeroDamage180,
        coalesce(maxHeroDamage180/minHeroDamage180,0) AS txMinMaxHeroDamage180,

        coalesce(avg(t2.avgHeroHealing),0) AS avgHeroHealing180,
        coalesce(min(t2.avgHeroHealing),0) AS minHeroHealing180,
        coalesce(max(t2.avgHeroHealing),0) AS maxHeroHealing180,
        coalesce(maxHeroHealing180/minHeroHealing180,0) AS txMinMaxHeroHealing180,

        coalesce(avg(t2.avgLastHits),0) AS avgLastHits180,
        coalesce(min(t2.avgLastHits),0) AS minLastHits180,
        coalesce(max(t2.avgLastHits),0) AS maxLastHits180,
        coalesce(maxLastHits180/minLastHits180,0) AS txMinMaxLastHits180,

        coalesce(avg(t2.avgLevel),0) AS avgLevel180,
        coalesce(min(t2.avgLevel),0) AS minLevel180,
        coalesce(max(t2.avgLevel),0) AS maxLevel180,
        coalesce(maxLevel180/minLevel180,0) AS txMinMaxLevel180,

        coalesce(avg(t2.avgNetWorth),0) AS avgNetWorth180,
        coalesce(min(t2.avgNetWorth),0) AS minNetWorth180,
        coalesce(max(t2.avgNetWorth),0) AS maxNetWorth180,
        coalesce(maxNetWorth180/minNetWorth180,0) AS txMinMaxNetWorth180,

        coalesce(avg(t2.avgRoshansKilled),0) AS avgRoshansKilled180,
        coalesce(min(t2.avgRoshansKilled),0) AS minRoshansKilled180,
        coalesce(max(t2.avgRoshansKilled),0) AS maxRoshansKilled180,
        coalesce(maxRoshansKilled180/minRoshansKilled180,0) AS txMinMaxRoshansKilled180,

        coalesce(avg(t2.avgRunePicks),0) AS avgRunePicks180,
        coalesce(min(t2.avgRunePicks),0) AS minRunePicks180,
        coalesce(max(t2.avgRunePicks),0) AS maxRunePicks180,
        coalesce(maxRunePicks180/minRunePicks180,0) AS txMinMaxRunePicks180,

        coalesce(avg(t2.avgStunsSec),0) AS avgStunsSec180,
        coalesce(min(t2.avgStunsSec),0) AS minStunsSec180,
        coalesce(max(t2.avgStunsSec),0) AS maxStunsSec180,
        coalesce(maxStunsSec180/minStunsSec180,0) AS txMinMaxStunsSec180,

        coalesce(avg(t2.avgtTeamfightParticipation),0) AS avgtTeamfightParticipation180,
        coalesce(min(t2.avgtTeamfightParticipation),0) AS mintTeamfightParticipation180,
        coalesce(max(t2.avgtTeamfightParticipation),0) AS maxtTeamfightParticipation180,
        coalesce(maxtTeamfightParticipation180/mintTeamfightParticipation180,0) AS txMinMaxtTeamfightParticipation180,

        coalesce(avg(t2.avgTowerDamage),0) AS avgTowerDamage180,
        coalesce(min(t2.avgTowerDamage),0) AS minTowerDamage180,
        coalesce(max(t2.avgTowerDamage),0) AS maxTowerDamage180,
        coalesce(maxTowerDamage180/minTowerDamage180,0) AS txMinMaxTowerDamage180,

        coalesce(avg(t2.avgTowerKilled),0) AS avgTowerKilled180,
        coalesce(min(t2.avgTowerKilled),0) AS minTowerKilled180,
        coalesce(max(t2.avgTowerKilled),0) AS maxTowerKilled180,
        coalesce(maxTowerKilled180/minTowerKilled180,0) AS txMinMaxTowerKilled180,

        coalesce(avg(t2.avgXpMinute),0) AS avgXpMinute180,
        coalesce(min(t2.avgXpMinute),0) AS minXpMinute180,
        coalesce(max(t2.avgXpMinute),0) AS maxXpMinute180,
        coalesce(maxXpMinute180/minXpMinute180,0) AS txMinMaxXpMinute180,

        coalesce(avg(t2.avgTotalGold),0) AS avgTotalGold180,
        coalesce(min(t2.avgTotalGold),0) AS minTotalGold180,
        coalesce(max(t2.avgTotalGold),0) AS maxTotalGold180,
        coalesce(maxTotalGold180/minTotalGold180,0) AS txMinMaxTotalGold180,

        coalesce(avg(t2.avgTotalXp),0) AS avgTotalXp180,
        coalesce(min(t2.avgTotalXp),0) AS minTotalXp180,
        coalesce(max(t2.avgTotalXp),0) AS maxTotalXp180,
        coalesce(maxTotalXp180/minTotalXp180,0) AS txMinMaxTotalXp180,

        coalesce(avg(t2.avgKillsMinute),0) AS avgKillsMinute180,
        coalesce(min(t2.avgKillsMinute),0) AS minKillsMinute180,
        coalesce(max(t2.avgKillsMinute),0) AS maxKillsMinute180,
        coalesce(maxKillsMinute180/minKillsMinute180,0) AS txMinMaxKillsMinute180,

        coalesce(avg(t2.avgKDA),0) AS avgKDA180,
        coalesce(min(t2.avgKDA),0) AS minKDA180,
        coalesce(max(t2.avgKDA),0) AS maxKDA180,
        coalesce(maxKDA180/minKDA180,0) AS txMinMaxKDA180,

        coalesce(avg(t2.avgNeutralKills),0) AS avgNeutralKills180,
        coalesce(min(t2.avgNeutralKills),0) AS minNeutralKills180,
        coalesce(max(t2.avgNeutralKills),0) AS maxNeutralKills180,
        coalesce(maxNeutralKills180/minNeutralKills180,0) AS txMinMaxNeutralKills180,

        coalesce(avg(t2.avgTowerKills),0) AS avgTowerKills180,
        coalesce(min(t2.avgTowerKills),0) AS minTowerKills180,
        coalesce(max(t2.avgTowerKills),0) AS maxTowerKills180,
        coalesce(maxTowerKills180/minTowerKills180,0) AS txMinMaxTowerKills180,

        coalesce(avg(t2.avgCourierKills),0) AS avgCourierKills180,
        coalesce(min(t2.avgCourierKills),0) AS minCourierKills180,
        coalesce(max(t2.avgCourierKills),0) AS maxCourierKills180,
        coalesce(maxCourierKills180/minCourierKills180,0) AS txMinMaxCourierKills180,

        coalesce(avg(t2.avgLaneKills),0) AS avgLaneKills180,
        coalesce(min(t2.avgLaneKills),0) AS minLaneKills180,
        coalesce(max(t2.avgLaneKills),0) AS maxLaneKills180,
        coalesce(maxLaneKills180/minLaneKills180,0) AS txMinMaxLaneKills180,

        coalesce(avg(t2.avgHeroKills),0) AS avgHeroKills180,
        coalesce(min(t2.avgHeroKills),0) AS minHeroKills180,
        coalesce(max(t2.avgHeroKills),0) AS maxHeroKills180,
        coalesce(maxHeroKills180/minHeroKills180,0) AS txMinMaxHeroKills180,

        coalesce(avg(t2.avgObserverKills),0) AS avgObserverKills180,
        coalesce(min(t2.avgObserverKills),0) AS minObserverKills180,
        coalesce(max(t2.avgObserverKills),0) AS maxObserverKills180,
        coalesce(maxObserverKills180/minObserverKills180,0) AS txMinMaxObserverKills180,

        coalesce(avg(t2.avgSentryKills),0) AS avgSentryKills180,
        coalesce(min(t2.avgSentryKills),0) AS minSentryKills180,
        coalesce(max(t2.avgSentryKills),0) AS maxSentryKills180,
        coalesce(maxSentryKills180/minSentryKills180,0) AS txMinMaxSentryKills180,

        coalesce(avg(t2.avgRoshansKills),0) AS avgRoshansKills180,
        coalesce(min(t2.avgRoshansKills),0) AS minRoshansKills180,
        coalesce(max(t2.avgRoshansKills),0) AS maxRoshansKills180,
        coalesce(maxRoshansKills180/minRoshansKills180,0) AS txMinMaxRoshansKills180,

        coalesce(avg(t2.avgNecronomiconKills),0) AS avgNecronomiconKills180,
        coalesce(min(t2.avgNecronomiconKills),0) AS minNecronomiconKills180,
        coalesce(max(t2.avgNecronomiconKills),0) AS maxNecronomiconKills180,
        coalesce(maxNecronomiconKills180/minNecronomiconKills180,0) AS txMinMaxNecronomiconKills180,

        coalesce(avg(t2.avgAncientKills),0) AS avgAncientKills180,
        coalesce(min(t2.avgAncientKills),0) AS minAncientKills180,
        coalesce(max(t2.avgAncientKills),0) AS maxAncientKills180,
        coalesce(maxAncientKills180/minAncientKills180,0) AS txMinMaxAncientKills180,

        coalesce(avg(t2.avgBuybackCount),0) AS avgBuybackCount180,
        coalesce(min(t2.avgBuybackCount),0) AS minBuybackCount180,
        coalesce(max(t2.avgBuybackCount),0) AS maxBuybackCount180,
        coalesce(maxBuybackCount180/minBuybackCount180,0) AS txMinMaxBuybackCount180,

        coalesce(avg(t2.avgObserverUses),0) AS avgObserverUses180,
        coalesce(min(t2.avgObserverUses),0) AS minObserverUses180,
        coalesce(max(t2.avgObserverUses),0) AS maxObserverUses180,
        coalesce(maxObserverUses180/minObserverUses180,0) AS txMinMaxObserverUses180,

        coalesce(avg(t2.avgSentryUses),0) AS avgSentryUses180,
        coalesce(min(t2.avgSentryUses),0) AS minSentryUses180,
        coalesce(max(t2.avgSentryUses),0) AS maxSentryUses180,
        coalesce(maxSentryUses180/minSentryUses180,0) AS txMinMaxSentryUses180,

        coalesce(avg(t2.avgLaneEfficiency),0) AS avgLaneEfficiency180,
        coalesce(min(t2.avgLaneEfficiency),0) AS minLaneEfficiency180,
        coalesce(max(t2.avgLaneEfficiency),0) AS maxLaneEfficiency180,
        coalesce(maxLaneEfficiency180/minLaneEfficiency180,0) AS txMinMaxLaneEfficiency180,

        coalesce(avg(t2.avgPurchaseTps),0) AS avgPurchaseTps180,
        coalesce(min(t2.avgPurchaseTps),0) AS minPurchaseTps180,
        coalesce(max(t2.avgPurchaseTps),0) AS maxPurchaseTps180,
        coalesce(maxPurchaseTps180/minPurchaseTps180,0) AS txMinMaxPurchaseTps180,

        coalesce(avg(t3.nrFrequency),0)AS avgFrequency90,
        coalesce(min(t3.nrFrequency),0)AS minFrequency90,
        coalesce(max(t3.nrFrequency),0)AS maxFrequency90,
        coalesce(maxFrequency90/minFrequency90,0) AS txMinMaxFrequency90,

        coalesce(avg(t3.avgWin),0)AS avgWin90,
        coalesce(min(t3.avgWin),0)AS minWin90,
        coalesce(max(t3.avgWin),0)AS maxWin90,
        coalesce(maxWin90/minWin90,0) AS txMinMaxWin90,

        coalesce(avg(t3.avgCampsStacked),0)AS avgCampsStacked90,
        coalesce(min(t3.avgCampsStacked),0)AS minCampsStacked90,
        coalesce(max(t3.avgCampsStacked),0)AS maxCampsStacked90,
        coalesce(maxCampsStacked90/minCampsStacked90,0) AS txMinMaxCampsStacked90,

        coalesce(avg(t3.avgCreepsStacked),0)AS avgCreepsStacked90,
        coalesce(min(t3.avgCreepsStacked),0)AS minCreepsStacked90,
        coalesce(max(t3.avgCreepsStacked),0)AS maxCreepsStacked90,
        coalesce(maxCreepsStacked90/minCreepsStacked90,0) AS txMinMaxCreepsStacked90,

        coalesce(avg(t3.avgKills),0)AS avgKills90,
        coalesce(min(t3.avgKills),0)AS minKills90,
        coalesce(max(t3.avgKills),0)AS maxKills90,
        coalesce(maxKills90/minKills90,0) AS txMinMaxKills90,

        coalesce(avg(t3.avgAssist),0)AS avgAssist90,
        coalesce(min(t3.avgAssist),0)AS minAssist90,
        coalesce(max(t3.avgAssist),0)AS maxAssist90,
        coalesce(maxAssist90/minAssist90,0) AS txMinMaxAssist90,

        coalesce(avg(t3.avgDeaths),0)AS avgDeaths90,
        coalesce(min(t3.avgDeaths),0)AS minDeaths90,
        coalesce(max(t3.avgDeaths),0)AS maxDeaths90,
        coalesce(maxDeaths90/minDeaths90,0) AS txMinMaxDeaths90,

        coalesce(avg(t3.avgDenies),0)AS avgDenies90,
        coalesce(min(t3.avgDenies),0)AS minDenies90,
        coalesce(max(t3.avgDenies),0)AS maxDenies90,
        coalesce(maxDenies90/minDenies90,0) AS txMinMaxDenies90,

        coalesce(avg(t3.avgFirstbloodClaimed),0)AS avgFirstbloodClaimed90,
        coalesce(min(t3.avgFirstbloodClaimed),0)AS minFirstbloodClaimed90,
        coalesce(max(t3.avgFirstbloodClaimed),0)AS maxFirstbloodClaimed90,
        coalesce(maxFirstbloodClaimed90/minFirstbloodClaimed90,0) AS txMinMaxFirstbloodClaimed90,

        coalesce(avg(t3.avgGold),0)AS avgGold90,
        coalesce(min(t3.avgGold),0)AS minGold90,
        coalesce(max(t3.avgGold),0)AS maxGold90,
        coalesce(maxGold90/minGold90,0) AS txMinMaxGold90,

        coalesce(avg(t3.avgGoldMinute),0)AS avgGoldMinute90,
        coalesce(min(t3.avgGoldMinute),0)AS minGoldMinute90,
        coalesce(max(t3.avgGoldMinute),0)AS maxGoldMinute90,
        coalesce(maxGoldMinute90/minGoldMinute90,0) AS txMinMaxGoldMinute90,

        coalesce(avg(t3.avgGoldSpent),0)AS avgGoldSpent90,
        coalesce(min(t3.avgGoldSpent),0)AS minGoldSpent90,
        coalesce(max(t3.avgGoldSpent),0)AS maxGoldSpent90,
        coalesce(maxGoldSpent90/minGoldSpent90,0) AS txMinMaxGoldSpent90,

        coalesce(avg(t3.avgHeroDamage),0)AS avgHeroDamage90,
        coalesce(min(t3.avgHeroDamage),0)AS minHeroDamage90,
        coalesce(max(t3.avgHeroDamage),0)AS maxHeroDamage90,
        coalesce(maxHeroDamage90/minHeroDamage90,0) AS txMinMaxHeroDamage90,

        coalesce(avg(t3.avgHeroHealing),0)AS avgHeroHealing90,
        coalesce(min(t3.avgHeroHealing),0)AS minHeroHealing90,
        coalesce(max(t3.avgHeroHealing),0)AS maxHeroHealing90,
        coalesce(maxHeroHealing90/minHeroHealing90,0) AS txMinMaxHeroHealing90,

        coalesce(avg(t3.avgLastHits),0)AS avgLastHits90,
        coalesce(min(t3.avgLastHits),0)AS minLastHits90,
        coalesce(max(t3.avgLastHits),0)AS maxLastHits90,
        coalesce(maxLastHits90/minLastHits90,0) AS txMinMaxLastHits90,

        coalesce(avg(t3.avgLevel),0)AS avgLevel90,
        coalesce(min(t3.avgLevel),0)AS minLevel90,
        coalesce(max(t3.avgLevel),0)AS maxLevel90,
        coalesce(maxLevel90/minLevel90,0) AS txMinMaxLevel90,

        coalesce(avg(t3.avgNetWorth),0)AS avgNetWorth90,
        coalesce(min(t3.avgNetWorth),0)AS minNetWorth90,
        coalesce(max(t3.avgNetWorth),0)AS maxNetWorth90,
        coalesce(maxNetWorth90/minNetWorth90,0) AS txMinMaxNetWorth90,

        coalesce(avg(t3.avgRoshansKilled),0)AS avgRoshansKilled90,
        coalesce(min(t3.avgRoshansKilled),0)AS minRoshansKilled90,
        coalesce(max(t3.avgRoshansKilled),0)AS maxRoshansKilled90,
        coalesce(maxRoshansKilled90/minRoshansKilled90,0) AS txMinMaxRoshansKilled90,

        coalesce(avg(t3.avgRunePicks),0)AS avgRunePicks90,
        coalesce(min(t3.avgRunePicks),0)AS minRunePicks90,
        coalesce(max(t3.avgRunePicks),0)AS maxRunePicks90,
        coalesce(maxRunePicks90/minRunePicks90,0) AS txMinMaxRunePicks90,

        coalesce(avg(t3.avgStunsSec),0)AS avgStunsSec90,
        coalesce(min(t3.avgStunsSec),0)AS minStunsSec90,
        coalesce(max(t3.avgStunsSec),0)AS maxStunsSec90,
        coalesce(maxStunsSec90/minStunsSec90,0) AS txMinMaxStunsSec90,

        coalesce(avg(t3.avgtTeamfightParticipation),0)AS avgtTeamfightParticipation90,
        coalesce(min(t3.avgtTeamfightParticipation),0)AS mintTeamfightParticipation90,
        coalesce(max(t3.avgtTeamfightParticipation),0)AS maxtTeamfightParticipation90,
        coalesce(maxtTeamfightParticipation90/mintTeamfightParticipation90,0) AS txMinMaxtTeamfightParticipation90,

        coalesce(avg(t3.avgTowerDamage),0)AS avgTowerDamage90,
        coalesce(min(t3.avgTowerDamage),0)AS minTowerDamage90,
        coalesce(max(t3.avgTowerDamage),0)AS maxTowerDamage90,
        coalesce(maxTowerDamage90/minTowerDamage90,0) AS txMinMaxTowerDamage90,

        coalesce(avg(t3.avgTowerKilled),0)AS avgTowerKilled90,
        coalesce(min(t3.avgTowerKilled),0)AS minTowerKilled90,
        coalesce(max(t3.avgTowerKilled),0)AS maxTowerKilled90,
        coalesce(maxTowerKilled90/minTowerKilled90,0) AS txMinMaxTowerKilled90,

        coalesce(avg(t3.avgXpMinute),0)AS avgXpMinute90,
        coalesce(min(t3.avgXpMinute),0)AS minXpMinute90,
        coalesce(max(t3.avgXpMinute),0)AS maxXpMinute90,
        coalesce(maxXpMinute90/minXpMinute90,0) AS txMinMaxXpMinute90,

        coalesce(avg(t3.avgTotalGold),0)AS avgTotalGold90,
        coalesce(min(t3.avgTotalGold),0)AS minTotalGold90,
        coalesce(max(t3.avgTotalGold),0)AS maxTotalGold90,
        coalesce(maxTotalGold90/minTotalGold90,0) AS txMinMaxTotalGold90,

        coalesce(avg(t3.avgTotalXp),0)AS avgTotalXp90,
        coalesce(min(t3.avgTotalXp),0)AS minTotalXp90,
        coalesce(max(t3.avgTotalXp),0)AS maxTotalXp90,
        coalesce(maxTotalXp90/minTotalXp90,0) AS txMinMaxTotalXp90,

        coalesce(avg(t3.avgKillsMinute),0)AS avgKillsMinute90,
        coalesce(min(t3.avgKillsMinute),0)AS minKillsMinute90,
        coalesce(max(t3.avgKillsMinute),0)AS maxKillsMinute90,
        coalesce(maxKillsMinute90/minKillsMinute90,0) AS txMinMaxKillsMinute90,

        coalesce(avg(t3.avgKDA),0)AS avgKDA90,
        coalesce(min(t3.avgKDA),0)AS minKDA90,
        coalesce(max(t3.avgKDA),0)AS maxKDA90,
        coalesce(maxKDA90/minKDA90,0) AS txMinMaxKDA90,

        coalesce(avg(t3.avgNeutralKills),0)AS avgNeutralKills90,
        coalesce(min(t3.avgNeutralKills),0)AS minNeutralKills90,
        coalesce(max(t3.avgNeutralKills),0)AS maxNeutralKills90,
        coalesce(maxNeutralKills90/minNeutralKills90,0) AS txMinMaxNeutralKills90,

        coalesce(avg(t3.avgTowerKills),0)AS avgTowerKills90,
        coalesce(min(t3.avgTowerKills),0)AS minTowerKills90,
        coalesce(max(t3.avgTowerKills),0)AS maxTowerKills90,
        coalesce(maxTowerKills90/minTowerKills90,0) AS txMinMaxTowerKills90,

        coalesce(avg(t3.avgCourierKills),0)AS avgCourierKills90,
        coalesce(min(t3.avgCourierKills),0)AS minCourierKills90,
        coalesce(max(t3.avgCourierKills),0)AS maxCourierKills90,
        coalesce(maxCourierKills90/minCourierKills90,0) AS txMinMaxCourierKills90,

        coalesce(avg(t3.avgLaneKills),0)AS avgLaneKills90,
        coalesce(min(t3.avgLaneKills),0)AS minLaneKills90,
        coalesce(max(t3.avgLaneKills),0)AS maxLaneKills90,
        coalesce(maxLaneKills90/minLaneKills90,0) AS txMinMaxLaneKills90,

        coalesce(avg(t3.avgHeroKills),0)AS avgHeroKills90,
        coalesce(min(t3.avgHeroKills),0)AS minHeroKills90,
        coalesce(max(t3.avgHeroKills),0)AS maxHeroKills90,
        coalesce(maxHeroKills90/minHeroKills90,0) AS txMinMaxHeroKills90,

        coalesce(avg(t3.avgObserverKills),0)AS avgObserverKills90,
        coalesce(min(t3.avgObserverKills),0)AS minObserverKills90,
        coalesce(max(t3.avgObserverKills),0)AS maxObserverKills90,
        coalesce(maxObserverKills90/minObserverKills90,0) AS txMinMaxObserverKills90,

        coalesce(avg(t3.avgSentryKills),0)AS avgSentryKills90,
        coalesce(min(t3.avgSentryKills),0)AS minSentryKills90,
        coalesce(max(t3.avgSentryKills),0)AS maxSentryKills90,
        coalesce(maxSentryKills90/minSentryKills90,0) AS txMinMaxSentryKills90,

        coalesce(avg(t3.avgRoshansKills),0)AS avgRoshansKills90,
        coalesce(min(t3.avgRoshansKills),0)AS minRoshansKills90,
        coalesce(max(t3.avgRoshansKills),0)AS maxRoshansKills90,
        coalesce(maxRoshansKills90/minRoshansKills90,0) AS txMinMaxRoshansKills90,

        coalesce(avg(t3.avgNecronomiconKills),0)AS avgNecronomiconKills90,
        coalesce(min(t3.avgNecronomiconKills),0)AS minNecronomiconKills90,
        coalesce(max(t3.avgNecronomiconKills),0)AS maxNecronomiconKills90,
        coalesce(maxNecronomiconKills90/minNecronomiconKills90,0) AS txMinMaxNecronomiconKills90,

        coalesce(avg(t3.avgAncientKills),0)AS avgAncientKills90,
        coalesce(min(t3.avgAncientKills),0)AS minAncientKills90,
        coalesce(max(t3.avgAncientKills),0)AS maxAncientKills90,
        coalesce(maxAncientKills90/minAncientKills90,0) AS txMinMaxAncientKills90,

        coalesce(avg(t3.avgBuybackCount),0)AS avgBuybackCount90,
        coalesce(min(t3.avgBuybackCount),0)AS minBuybackCount90,
        coalesce(max(t3.avgBuybackCount),0)AS maxBuybackCount90,
        coalesce(maxBuybackCount90/minBuybackCount90,0) AS txMinMaxBuybackCount90,

        coalesce(avg(t3.avgObserverUses),0)AS avgObserverUses90,
        coalesce(min(t3.avgObserverUses),0)AS minObserverUses90,
        coalesce(max(t3.avgObserverUses),0)AS maxObserverUses90,
        coalesce(maxObserverUses90/minObserverUses90,0) AS txMinMaxObserverUses90,

        coalesce(avg(t3.avgSentryUses),0)AS avgSentryUses90,
        coalesce(min(t3.avgSentryUses),0)AS minSentryUses90,
        coalesce(max(t3.avgSentryUses),0)AS maxSentryUses90,
        coalesce(maxSentryUses90/minSentryUses90,0) AS txMinMaxSentryUses90,

        coalesce(avg(t3.avgLaneEfficiency),0)AS avgLaneEfficiency90,
        coalesce(min(t3.avgLaneEfficiency),0)AS minLaneEfficiency90,
        coalesce(max(t3.avgLaneEfficiency),0)AS maxLaneEfficiency90,
        coalesce(maxLaneEfficiency90/minLaneEfficiency90,0) AS txMinMaxLaneEfficiency90,

        coalesce(avg(t3.avgPurchaseTps),0)AS avgPurchaseTps90,
        coalesce(min(t3.avgPurchaseTps),0)AS minPurchaseTps90,
        coalesce(max(t3.avgPurchaseTps),0)AS maxPurchaseTps90,
        coalesce(maxPurchaseTps90/minPurchaseTps90,0) AS txMinMaxPurchaseTps90,

        coalesce(avg(t4.nrFrequency),0)AS avgFrequency30,
        coalesce(min(t4.nrFrequency),0)AS minFrequency30,
        coalesce(max(t4.nrFrequency),0)AS maxFrequency30,
        coalesce(maxFrequency30/minFrequency30,0) AS txMinMaxFrequency30,

        coalesce(avg(t4.avgWin),0)AS avgWin30,
        coalesce(min(t4.avgWin),0)AS minWin30,
        coalesce(max(t4.avgWin),0)AS maxWin30,
        coalesce(maxWin30/minWin30,0) AS txMinMaxWin30,

        coalesce(avg(t4.avgCampsStacked),0)AS avgCampsStacked30,
        coalesce(min(t4.avgCampsStacked),0)AS minCampsStacked30,
        coalesce(max(t4.avgCampsStacked),0)AS maxCampsStacked30,
        coalesce(maxCampsStacked30/minCampsStacked30,0) AS txMinMaxCampsStacked30,

        coalesce(avg(t4.avgCreepsStacked),0)AS avgCreepsStacked30,
        coalesce(min(t4.avgCreepsStacked),0)AS minCreepsStacked30,
        coalesce(max(t4.avgCreepsStacked),0)AS maxCreepsStacked30,
        coalesce(maxCreepsStacked30/minCreepsStacked30,0) AS txMinMaxCreepsStacked30,

        coalesce(avg(t4.avgKills),0)AS avgKills30,
        coalesce(min(t4.avgKills),0)AS minKills30,
        coalesce(max(t4.avgKills),0)AS maxKills30,
        coalesce(maxKills30/minKills30,0) AS txMinMaxKills30,

        coalesce(avg(t4.avgAssist),0)AS avgAssist30,
        coalesce(min(t4.avgAssist),0)AS minAssist30,
        coalesce(max(t4.avgAssist),0)AS maxAssist30,
        coalesce(maxAssist30/minAssist30,0) AS txMinMaxAssist30,

        coalesce(avg(t4.avgDeaths),0)AS avgDeaths30,
        coalesce(min(t4.avgDeaths),0)AS minDeaths30,
        coalesce(max(t4.avgDeaths),0)AS maxDeaths30,
        coalesce(maxDeaths30/minDeaths30,0) AS txMinMaxDeaths30,

        coalesce(avg(t4.avgDenies),0)AS avgDenies30,
        coalesce(min(t4.avgDenies),0)AS minDenies30,
        coalesce(max(t4.avgDenies),0)AS maxDenies30,
        coalesce(maxDenies30/minDenies30,0) AS txMinMaxDenies30,

        coalesce(avg(t4.avgFirstbloodClaimed),0)AS avgFirstbloodClaimed30,
        coalesce(min(t4.avgFirstbloodClaimed),0)AS minFirstbloodClaimed30,
        coalesce(max(t4.avgFirstbloodClaimed),0)AS maxFirstbloodClaimed30,
        coalesce(maxFirstbloodClaimed30/minFirstbloodClaimed30,0) AS txMinMaxFirstbloodClaimed30,

        coalesce(avg(t4.avgGold),0)AS avgGold30,
        coalesce(min(t4.avgGold),0)AS minGold30,
        coalesce(max(t4.avgGold),0)AS maxGold30,
        coalesce(maxGold30/minGold30,0) AS txMinMaxGold30,

        coalesce(avg(t4.avgGoldMinute),0)AS avgGoldMinute30,
        coalesce(min(t4.avgGoldMinute),0)AS minGoldMinute30,
        coalesce(max(t4.avgGoldMinute),0)AS maxGoldMinute30,
        coalesce(maxGoldMinute30/minGoldMinute30,0) AS txMinMaxGoldMinute30,

        coalesce(avg(t4.avgGoldSpent),0)AS avgGoldSpent30,
        coalesce(min(t4.avgGoldSpent),0)AS minGoldSpent30,
        coalesce(max(t4.avgGoldSpent),0)AS maxGoldSpent30,
        coalesce(maxGoldSpent30/minGoldSpent30,0) AS txMinMaxGoldSpent30,

        coalesce(avg(t4.avgHeroDamage),0)AS avgHeroDamage30,
        coalesce(min(t4.avgHeroDamage),0)AS minHeroDamage30,
        coalesce(max(t4.avgHeroDamage),0)AS maxHeroDamage30,
        coalesce(maxHeroDamage30/minHeroDamage30,0) AS txMinMaxHeroDamage30,

        coalesce(avg(t4.avgHeroHealing),0)AS avgHeroHealing30,
        coalesce(min(t4.avgHeroHealing),0)AS minHeroHealing30,
        coalesce(max(t4.avgHeroHealing),0)AS maxHeroHealing30,
        coalesce(maxHeroHealing30/minHeroHealing30,0) AS txMinMaxHeroHealing30,

        coalesce(avg(t4.avgLastHits),0)AS avgLastHits30,
        coalesce(min(t4.avgLastHits),0)AS minLastHits30,
        coalesce(max(t4.avgLastHits),0)AS maxLastHits30,
        coalesce(maxLastHits30/minLastHits30,0) AS txMinMaxLastHits30,

        coalesce(avg(t4.avgLevel),0)AS avgLevel30,
        coalesce(min(t4.avgLevel),0)AS minLevel30,
        coalesce(max(t4.avgLevel),0)AS maxLevel30,
        coalesce(maxLevel30/minLevel30,0) AS txMinMaxLevel30,

        coalesce(avg(t4.avgNetWorth),0)AS avgNetWorth30,
        coalesce(min(t4.avgNetWorth),0)AS minNetWorth30,
        coalesce(max(t4.avgNetWorth),0)AS maxNetWorth30,
        coalesce(maxNetWorth30/minNetWorth30,0) AS txMinMaxNetWorth30,

        coalesce(avg(t4.avgRoshansKilled),0)AS avgRoshansKilled30,
        coalesce(min(t4.avgRoshansKilled),0)AS minRoshansKilled30,
        coalesce(max(t4.avgRoshansKilled),0)AS maxRoshansKilled30,
        coalesce(maxRoshansKilled30/minRoshansKilled30,0) AS txMinMaxRoshansKilled30,

        coalesce(avg(t4.avgRunePicks),0)AS avgRunePicks30,
        coalesce(min(t4.avgRunePicks),0)AS minRunePicks30,
        coalesce(max(t4.avgRunePicks),0)AS maxRunePicks30,
        coalesce(maxRunePicks30/minRunePicks30,0) AS txMinMaxRunePicks30,

        coalesce(avg(t4.avgStunsSec),0)AS avgStunsSec30,
        coalesce(min(t4.avgStunsSec),0)AS minStunsSec30,
        coalesce(max(t4.avgStunsSec),0)AS maxStunsSec30,
        coalesce(maxStunsSec30/minStunsSec30,0) AS txMinMaxStunsSec30,

        coalesce(avg(t4.avgtTeamfightParticipation),0)AS avgtTeamfightParticipation30,
        coalesce(min(t4.avgtTeamfightParticipation),0)AS mintTeamfightParticipation30,
        coalesce(max(t4.avgtTeamfightParticipation),0)AS maxtTeamfightParticipation30,
        coalesce(maxtTeamfightParticipation30/mintTeamfightParticipation30,0) AS txMinMaxtTeamfightParticipation30,

        coalesce(avg(t4.avgTowerDamage),0)AS avgTowerDamage30,
        coalesce(min(t4.avgTowerDamage),0)AS minTowerDamage30,
        coalesce(max(t4.avgTowerDamage),0)AS maxTowerDamage30,
        coalesce(maxTowerDamage30/minTowerDamage30,0) AS txMinMaxTowerDamage30,

        coalesce(avg(t4.avgTowerKilled),0)AS avgTowerKilled30,
        coalesce(min(t4.avgTowerKilled),0)AS minTowerKilled30,
        coalesce(max(t4.avgTowerKilled),0)AS maxTowerKilled30,
        coalesce(maxTowerKilled30/minTowerKilled30,0) AS txMinMaxTowerKilled30,

        coalesce(avg(t4.avgXpMinute),0)AS avgXpMinute30,
        coalesce(min(t4.avgXpMinute),0)AS minXpMinute30,
        coalesce(max(t4.avgXpMinute),0)AS maxXpMinute30,
        coalesce(maxXpMinute30/minXpMinute30,0) AS txMinMaxXpMinute30,

        coalesce(avg(t4.avgTotalGold),0)AS avgTotalGold30,
        coalesce(min(t4.avgTotalGold),0)AS minTotalGold30,
        coalesce(max(t4.avgTotalGold),0)AS maxTotalGold30,
        coalesce(maxTotalGold30/minTotalGold30,0) AS txMinMaxTotalGold30,

        coalesce(avg(t4.avgTotalXp),0)AS avgTotalXp30,
        coalesce(min(t4.avgTotalXp),0)AS minTotalXp30,
        coalesce(max(t4.avgTotalXp),0)AS maxTotalXp30,
        coalesce(maxTotalXp30/minTotalXp30,0) AS txMinMaxTotalXp30,

        coalesce(avg(t4.avgKillsMinute),0)AS avgKillsMinute30,
        coalesce(min(t4.avgKillsMinute),0)AS minKillsMinute30,
        coalesce(max(t4.avgKillsMinute),0)AS maxKillsMinute30,
        coalesce(maxKillsMinute30/minKillsMinute30,0) AS txMinMaxKillsMinute30,

        coalesce(avg(t4.avgKDA),0)AS avgKDA30,
        coalesce(min(t4.avgKDA),0)AS minKDA30,
        coalesce(max(t4.avgKDA),0)AS maxKDA30,
        coalesce(maxKDA30/minKDA30,0) AS txMinMaxKDA30,

        coalesce(avg(t4.avgNeutralKills),0)AS avgNeutralKills30,
        coalesce(min(t4.avgNeutralKills),0)AS minNeutralKills30,
        coalesce(max(t4.avgNeutralKills),0)AS maxNeutralKills30,
        coalesce(maxNeutralKills30/minNeutralKills30,0) AS txMinMaxNeutralKills30,

        coalesce(avg(t4.avgTowerKills),0)AS avgTowerKills30,
        coalesce(min(t4.avgTowerKills),0)AS minTowerKills30,
        coalesce(max(t4.avgTowerKills),0)AS maxTowerKills30,
        coalesce(maxTowerKills30/minTowerKills30,0) AS txMinMaxTowerKills30,

        coalesce(avg(t4.avgCourierKills),0)AS avgCourierKills30,
        coalesce(min(t4.avgCourierKills),0)AS minCourierKills30,
        coalesce(max(t4.avgCourierKills),0)AS maxCourierKills30,
        coalesce(maxCourierKills30/minCourierKills30,0) AS txMinMaxCourierKills30,

        coalesce(avg(t4.avgLaneKills),0)AS avgLaneKills30,
        coalesce(min(t4.avgLaneKills),0)AS minLaneKills30,
        coalesce(max(t4.avgLaneKills),0)AS maxLaneKills30,
        coalesce(maxLaneKills30/minLaneKills30,0) AS txMinMaxLaneKills30,

        coalesce(avg(t4.avgHeroKills),0)AS avgHeroKills30,
        coalesce(min(t4.avgHeroKills),0)AS minHeroKills30,
        coalesce(max(t4.avgHeroKills),0)AS maxHeroKills30,
        coalesce(maxHeroKills30/minHeroKills30,0) AS txMinMaxHeroKills30,

        coalesce(avg(t4.avgObserverKills),0)AS avgObserverKills30,
        coalesce(min(t4.avgObserverKills),0)AS minObserverKills30,
        coalesce(max(t4.avgObserverKills),0)AS maxObserverKills30,
        coalesce(maxObserverKills30/minObserverKills30,0) AS txMinMaxObserverKills30,

        coalesce(avg(t4.avgSentryKills),0)AS avgSentryKills30,
        coalesce(min(t4.avgSentryKills),0)AS minSentryKills30,
        coalesce(max(t4.avgSentryKills),0)AS maxSentryKills30,
        coalesce(maxSentryKills30/minSentryKills30,0) AS txMinMaxSentryKills30,

        coalesce(avg(t4.avgRoshansKills),0)AS avgRoshansKills30,
        coalesce(min(t4.avgRoshansKills),0)AS minRoshansKills30,
        coalesce(max(t4.avgRoshansKills),0)AS maxRoshansKills30,
        coalesce(maxRoshansKills30/minRoshansKills30,0) AS txMinMaxRoshansKills30,

        coalesce(avg(t4.avgNecronomiconKills),0)AS avgNecronomiconKills30,
        coalesce(min(t4.avgNecronomiconKills),0)AS minNecronomiconKills30,
        coalesce(max(t4.avgNecronomiconKills),0)AS maxNecronomiconKills30,
        coalesce(maxNecronomiconKills30/minNecronomiconKills30,0) AS txMinMaxNecronomiconKills30,

        coalesce(avg(t4.avgAncientKills),0)AS avgAncientKills30,
        coalesce(min(t4.avgAncientKills),0)AS minAncientKills30,
        coalesce(max(t4.avgAncientKills),0)AS maxAncientKills30,
        coalesce(maxAncientKills30/minAncientKills30,0) AS txMinMaxAncientKills30,

        coalesce(avg(t4.avgBuybackCount),0)AS avgBuybackCount30,
        coalesce(min(t4.avgBuybackCount),0)AS minBuybackCount30,
        coalesce(max(t4.avgBuybackCount),0)AS maxBuybackCount30,
        coalesce(maxBuybackCount30/minBuybackCount30,0) AS txMinMaxBuybackCount30,

        coalesce(avg(t4.avgObserverUses),0)AS avgObserverUses30,
        coalesce(min(t4.avgObserverUses),0)AS minObserverUses30,
        coalesce(max(t4.avgObserverUses),0)AS maxObserverUses30,
        coalesce(maxObserverUses30/minObserverUses30,0) AS txMinMaxObserverUses30,

        coalesce(avg(t4.avgSentryUses),0)AS avgSentryUses30,
        coalesce(min(t4.avgSentryUses),0)AS minSentryUses30,
        coalesce(max(t4.avgSentryUses),0)AS maxSentryUses30,
        coalesce(maxSentryUses30/minSentryUses30,0) AS txMinMaxSentryUses30,

        coalesce(avg(t4.avgLaneEfficiency),0)AS avgLaneEfficiency30,
        coalesce(min(t4.avgLaneEfficiency),0)AS minLaneEfficiency30,
        coalesce(max(t4.avgLaneEfficiency),0)AS maxLaneEfficiency30,
        coalesce(maxLaneEfficiency30/minLaneEfficiency30,0) AS txMinMaxLaneEfficiency30,

        coalesce(avg(t4.avgPurchaseTps),0)AS avgPurchaseTps30,
        coalesce(min(t4.avgPurchaseTps),0)AS minPurchaseTps30,
        coalesce(max(t4.avgPurchaseTps),0)AS maxPurchaseTps30,
        coalesce(maxPurchaseTps30/minPurchaseTps30,0) AS txMinMaxPurchaseTps30

FROM feature_store.dota_teams_player_0 AS t1

LEFT JOIN feature_store.dota_players_180 AS t2
ON t1.dtReference = t2.dtReference
AND t1.idPlayer = t2.idPlayer

LEFT JOIN feature_store.dota_players_90 AS t3
ON t1.dtReference = t3.dtReference
AND t1.idPlayer = t3.idPlayer

LEFT JOIN feature_store.dota_players_30 AS t4
ON t1.dtReference = t4.dtReference
AND t1.idPlayer = t4.idPlayer

WHERE t1.dtReference = '{date}'

GROUP BY t1.dtReference, t1.idTeam, t1.descTeamName, t1.descTeamTag
ORDER BY t1.idTeam, t1.dtReference