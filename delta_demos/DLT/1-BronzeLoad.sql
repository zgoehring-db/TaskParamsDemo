-- Databricks notebook source
CREATE LIVE TABLE encounters_etl 
AS 
SELECT * FROM demo_omop_database.encounters


-- COMMAND ----------

CREATE LIVE TABLE T1_ETL
AS 
  SELECT
    CL1.id encounter_id,
    CL1.patient,
    CL1.encounterclass,
    CL1.start VISIT_START_DATE,
    CL1.stop VISIT_END_DATE
  FROM
    live.encounters_etl CL1
  WHERE
    CL1.encounterclass in ('emergency', 'urgent')
