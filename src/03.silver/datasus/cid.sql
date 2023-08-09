-- Databricks notebook source
DROP TABLE IF EXISTS silver.datasus.cid;

CREATE TABLE IF NOT EXISTS silver.datasus.cid AS (
  SELECT *
  FROM bronze.datasus.cid
);
