-- Databricks notebook source
-- DBTITLE 1,Censo Escolar
GRANT USAGE ON DATABASE `bronze_censo_escolar` TO `twitch`;
GRANT SELECT ON DATABASE `bronze_censo_escolar` TO `twitch`;
GRANT READ_METADATA ON DATABASE `bronze_censo_escolar` TO `twitch`;

-- COMMAND ----------

-- DBTITLE 1,ENEM
GRANT USAGE ON DATABASE `bronze_enem` TO `twitch`;
GRANT SELECT ON DATABASE `bronze_enem` TO `twitch`;
GRANT READ_METADATA ON DATABASE `bronze_enem` TO `twitch`;

-- COMMAND ----------

-- DBTITLE 1,Gamers Club
GRANT USAGE ON DATABASE `bronze_gc` TO `twitch`;
GRANT SELECT ON DATABASE `bronze_gc` TO `twitch`;
GRANT READ_METADATA ON DATABASE `bronze_gc` TO `twitch`;

GRANT USAGE ON DATABASE `silver_gc` TO `twitch`;
GRANT SELECT ON DATABASE `silver_gc` TO `twitch`;
GRANT READ_METADATA ON DATABASE `silver_gc` TO `twitch`;

-- COMMAND ----------

-- DBTITLE 1,Olist
GRANT USAGE ON DATABASE `bronze.olist` TO `twitch`;
GRANT SELECT ON DATABASE `bronze.olist` TO `twitch`;
GRANT READ_METADATA ON DATABASE `bronze.olist` TO `twitch`;

GRANT USAGE ON DATABASE `silver.olist` TO `twitch`;
GRANT SELECT ON DATABASE `silver.olist` TO `twitch`;
GRANT READ_METADATA ON DATABASE `silver.olist` TO `twitch`;

-- COMMAND ----------

-- DBTITLE 1,TSE
GRANT USAGE ON DATABASE `bronze_tse` TO `twitch`;
GRANT SELECT ON DATABASE `bronze_tse` TO `twitch`;
GRANT READ_METADATA ON DATABASE `bronze_tse` TO `twitch`;

GRANT USAGE ON DATABASE `silver_tse` TO `twitch`;
GRANT SELECT ON DATABASE `silver_tse` TO `twitch`;
GRANT READ_METADATA ON DATABASE `silver_tse` TO `twitch`;
