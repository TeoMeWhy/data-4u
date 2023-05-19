-- Databricks notebook source

-- COMMAND ----------

-- DBTITLE 1,Olist
GRANT USAGE ON DATABASE `bronze.olist` TO `twitch`;
GRANT SELECT ON DATABASE `bronze.olist` TO `twitch`;
GRANT READ_METADATA ON DATABASE `bronze.olist` TO `twitch`;

GRANT USAGE ON DATABASE `silver.olist` TO `twitch`;
GRANT SELECT ON DATABASE `silver.olist` TO `twitch`;
GRANT READ_METADATA ON DATABASE `silver.olist` TO `twitch`;

