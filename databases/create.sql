-- Databricks notebook source
-- DBTITLE 1,DataSUS
CREATE DATABASE IF NOT EXISTS bronze.datasus;
CREATE DATABASE IF NOT EXISTS silver.datasus;

-- COMMAND ----------

-- DBTITLE 1,Dota
CREATE DATABASE IF NOT EXISTS bronze.dota;
CREATE DATABASE IF NOT EXISTS silver.dota;

-- COMMAND ----------

-- DBTITLE 1,IBGE
CREATE DATABASE IF NOT EXISTS bronze.ibge;
CREATE DATABASE IF NOT EXISTS silver.ibge;

-- COMMAND ----------

-- DBTITLE 1,IGDB
CREATE DATABASE IF NOT EXISTS bronze.igdb;
CREATE DATABASE IF NOT EXISTS silver.igdb;

-- COMMAND ----------

-- DBTITLE 1,LinuxTips
CREATE DATABASE IF NOT EXISTS bronze.linuxtips;
CREATE DATABASE IF NOT EXISTS silver.pizza_query;

-- COMMAND ----------

-- DBTITLE 1,Olist
CREATE DATABASE IF NOT EXISTS bronze.olist;
CREATE DATABASE IF NOT EXISTS silver.olist;
