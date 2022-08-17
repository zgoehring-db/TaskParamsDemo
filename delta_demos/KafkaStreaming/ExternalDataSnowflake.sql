
-- Create and select table  
CREATE DATABASE IF NOT EXISTS DEMO;
USE DEMO;

-- create/use a schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS rac_schema;
USE schema rac_schema; 

CREATE OR REPLACE FILE FORMAT rac_csv_file
TYPE = CSV
field_delimiter = ','
COMMENT = 'DEMO_CSV_FILE_FORMAT';



-- Create External Stage 
-- sp=racwdlme&st=2022-08-17T03:18:09Z&se=2023-01-01T12:18:09Z&spr=https&sv=2021-06-08&sr=c&sig=Acij%2FNhe1%2FiXEuVBvXscT%2B8KWLWVFymSGHBT5%2B%2F6cCU%3D
CREATE OR REPLACE STAGE rac_ext_stage_demo
URL = 'azure://racadlsgen2.blob.core.windows.net/snowflakedemo'
//STORAGE_INTEGRATION = rac_storage_integration  
CREDENTIALS = (AZURE_SAS_TOKEN = 'sp=racwdlme&st=2022-08-17T03:18:09Z&se=2023-01-01T12:18:09Z&spr=https&sv=2021-06-08&sr=c&sig=Acij%2FNhe1%2FiXEuVBvXscT%2B8KWLWVFymSGHBT5%2B%2F6cCU%3D')
FILE_FORMAT = rac_csv_file
;


list @rac_ext_stage_demo;


CREATE OR REPLACE TABLE  FactProductInventory 
AS SELECT t.$1, t.$2, t.$3
FROM @rac_ext_stage_demo 
//FILE_FORMAT = (format_name = 'rac_csv_file');
(file_format => 'rac_csv_file', pattern=>'.*csv') t



