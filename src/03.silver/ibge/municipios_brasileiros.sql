-- Databricks notebook source
DROP TABLE IF EXISTS silver.ibge.municipios_brasileiros;

CREATE TABLE IF NOT EXISTS silver.ibge.municipios_brasileiros AS (

  SELECT *
  FROM bronze.ibge.municipios_brasileiros

);
