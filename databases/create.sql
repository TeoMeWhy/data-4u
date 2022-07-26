-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS bronze_censo_escolar LOCATION "/mnt/datalake/bronze/censo_escolar";
CREATE DATABASE IF NOT EXISTS bronze_enem LOCATION "/mnt/datalake/bronze/enem";
CREATE DATABASE IF NOT EXISTS bronze_gc LOCATION "/mnt/datalake/bronze/gc";
CREATE DATABASE IF NOT EXISTS bronze_olist LOCATION "/mnt/datalake/bronze/olist";
CREATE DATABASE IF NOT EXISTS silver_olist LOCATION "/mnt/datalake/silver/olist";
