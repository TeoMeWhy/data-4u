-- Databricks notebook source
-- DBTITLE 1,Censo Escolar
GRANT USAGE ON DATABASE `bronze_censo_escolar` TO `apoiadores`;
GRANT SELECT ON DATABASE `bronze_censo_escolar` TO `apoiadores`;
GRANT READ_METADATA ON DATABASE `bronze_censo_escolar` TO `apoiadores`;

-- COMMAND ----------

-- DBTITLE 1,ENEM
GRANT USAGE ON DATABASE `bronze_enem` TO `apoiadores`;
GRANT SELECT ON DATABASE `bronze_enem` TO `apoiadores`;
GRANT READ_METADATA ON DATABASE `bronze_enem` TO `apoiadores`;

-- COMMAND ----------

-- DBTITLE 1,Gamers Club
GRANT USAGE ON DATABASE `bronze_gc` TO `apoiadores`;
GRANT SELECT ON DATABASE `bronze_gc` TO `apoiadores`;
GRANT READ_METADATA ON DATABASE `bronze_gc` TO `apoiadores`;

GRANT USAGE ON DATABASE `silver_gc` TO `apoiadores`;
GRANT SELECT ON DATABASE `silver_gc` TO `apoiadores`;
GRANT READ_METADATA ON DATABASE `silver_gc` TO `apoiadores`;

-- COMMAND ----------

-- DBTITLE 1,Olist
GRANT USAGE ON DATABASE `bronze_olist` TO `apoiadores`;
GRANT SELECT ON DATABASE `bronze_olist` TO `apoiadores`;
GRANT READ_METADATA ON DATABASE `bronze_olist` TO `apoiadores`;

GRANT USAGE ON DATABASE `silver_olist` TO `apoiadores`;
GRANT SELECT ON DATABASE `silver_olist` TO `apoiadores`;
GRANT READ_METADATA ON DATABASE `silver_olist` TO `apoiadores`;

-- COMMAND ----------

-- DBTITLE 1,TSE
GRANT USAGE ON DATABASE `bronze_tse` TO `apoiadores`;
GRANT SELECT ON DATABASE `bronze_tse` TO `apoiadores`;
GRANT READ_METADATA ON DATABASE `bronze_tse` TO `apoiadores`;

GRANT USAGE ON DATABASE `silver_tse` TO `apoiadores`;
GRANT SELECT ON DATABASE `silver_tse` TO `apoiadores`;
GRANT READ_METADATA ON DATABASE `silver_tse` TO `apoiadores`;
