-- create warehouse, database and schema
CREATE WAREHOUSE STORE_WAREHOUSE;
CREATE DATABASE STORE_DB;
CREATE SCHEMA STORE_SCHEMA;


-- using database and schema
USE DATABASE STORE_DB;
USE SCHEMA STORE_SCHEMA;


-- create schema of table stores
CREATE OR REPLACE TABLE STORE_DB.STORE_SCHEMA.STORES (
        "STORE" INTEGER,
        "TYPE" CHAR,
        "SIZE" INTEGER
    );


SELECT *
FROM STORES;


-- Create two stages
CREATE OR REPLACE STAGE "STORE_DB"."STORE_SCHEMA"."RAW_STAGE1" COPY_OPTIONS = (ON_ERROR = 'skip_file');
CREATE OR REPLACE STAGE "STORE_DB"."STORE_SCHEMA"."RAW_STAGE2" COPY_OPTIONS = (ON_ERROR = 'skip_file');


-- show all stages
SHOW STAGES;


-- list contents of stages
LIST @RAW_STAGE1;
LIST @RAW_STAGE2;


-- Upload files to stages from filesystem using snowsql client CLI, commands are:
-- note: features and sales table are uploaded to @RAW_STAGE1 and stores is uploaded to @RAW_STAGE2
-- PUT file://<path_to_datasets>/features.csv @RAW_STAGE1
-- PUT file://<path_to_datasets>/sales.csv @RAW_STAGE1
-- PUT file://<path_to_datasets>/stores.csv @RAW_STAGE2


-- create tables from added files not using select
COPY INTO STORE_DB.STORE_SCHEMA.STORES
FROM @RAW_STAGE2 / stores.csv.gz file_format = (format_name = CSV_FF) on_error = 'continue';
-- using csv file format here


-- create tables from added files using select
CREATE OR REPLACE TABLE STORE_DB.STORE_SCHEMA.FEATURES AS
SELECT T.$1 STORE,
    T.$2 DATE,
    T.$3 TEMPERATURE,
    T.$4 FUEL_PRICE,
    T.$5 MARKDOWN1,
    T.$6 MARKDOWN2,
    T.$7 MARKDOWN3,
    T.$8 MARKDOWN4,
    T.$9 MARKDOWN5,
    T.$10 CPI,
    T.$11 UNEMPLOYMENT,
    T.$12 IS_HOLIDAY
FROM @RAW_STAGE1 / features.csv.gz (file_format => CSV_FF) T;


CREATE OR REPLACE TABLE STORE_DB.STORE_SCHEMA.SALES AS
SELECT T.$1 STORE,
    T.$2 DEPARTMENT,
    T.$3 DATE,
    T.$4 WEEKLY_SALES,
    T.$5 IS_HOLIDAY
FROM @RAW_STAGE1 / sales.csv.gz (file_format => CSV_FF) T;


-- view tables of store_db
SELECT *
FROM STORE_DB.STORE_SCHEMA.STORES;
SELECT *
FROM STORE_DB.STORE_SCHEMA.FEATURES;
SELECT *
FROM STORE_DB.STORE_SCHEMA.SALES;


-- Create views
-- a view to see those sales data where the record is a holiday and weekly sales >= 30000
CREATE VIEW STORE_DB.STORE_SCHEMA.HOLIDAY_SALES_VIEW AS
SELECT *
FROM STORE_DB.STORE_SCHEMA.SALES S
WHERE S.IS_HOLIDAY = 'TRUE'
    AND S.WEEKLY_SALES >= 30000;

--- see all views
SHOW VIEWS;

-- a view can be treated like a table
SELECT *
FROM HOLIDAY_SALES_VIEW;


-- a view to see total sales of each store by summing up its weekly sales records
CREATE VIEW STORE_DB.STORE_SCHEMA.STORE_TOTAL_SALES_VIEW AS
SELECT S.STORE,
    SUM(S.WEEKLY_SALES) TOTAL_SALES
FROM SALES S
GROUP BY S.STORE
ORDER BY S.STORE;


SELECT *
FROM STORE_TOTAL_SALES_VIEW;


-- a view to join store table with sales table using inner join
CREATE VIEW STORES_SALES_JOIN_VIEW AS
SELECT ST.STORE,
    ST.TYPE,
    ST.SIZE,
    SA.DEPARTMENT,
    SA.DATE,
    SA.WEEKLY_SALES,
    SA.IS_HOLIDAY
FROM STORES ST
    INNER JOIN SALES SA ON ST.STORE = SA.STORE;
SELECT *
FROM STORES_SALES_JOIN_VIEW;

-- TASKS --
-- create scheduled tasks in the database
-- create a table TEMPERATURE that stores random temperatures in it
CREATE OR REPLACE TABLE STORE_DB.STORE_SCHEMA.TEMPERATURE (
        "DATETIME" DATETIME,
        "TEMPERATURE" FLOAT
    );

-- create a task that adds random temperatures in temperatures table every minute
CREATE OR REPLACE TASK ADD_LATEST_TEMPERATURE 
WAREHOUSE = STORE_WAREHOUSE 
SCHEDULE = 'USING CRON */1 * * * * Asia/Kathmandu' -- use cron job to run the task every minute in Asia/Kathmandu timezone
AS
INSERT INTO STORE_DB.STORE_SCHEMA.TEMPERATURE
SELECT CURRENT_TIMESTAMP,
    UNIFORM(
        10::float,
        40::float,
        random()
    );
-- UNIFORM() generates a uniformly random number given the range


-- show tasks
SHOW TASKS;


-- all created tasks are initially in suspended state
-- alter the state of task (make it 'resume' from 'suspend')
ALTER TASK ADD_LATEST_TEMPERATURE RESUME;


-- see the contents of table temperature
SELECT *
FROM STORE_DB.STORE_SCHEMA.TEMPERATURE;