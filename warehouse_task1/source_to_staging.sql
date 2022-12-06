-- public schema is the schema that is treated as source (landing db)
-- staging schema is the schema treated as staging layer
-- core schema is the schema that is treated as core layer


------------------ CREATE INITIAL TABLE SALES ------------------
CREATE OR REPLACE TABLE PUBLIC.SALES
(
  "DATE" DATE,
  "DAY" INTEGER,
  "MONTH" STRING,
  "YEAR" INTEGER,
  "CUSTOMER_AGE" INTEGER,
  "AGE_GROUP" STRING,
  "CUSTOMER_GENDER" CHAR(1),
  "COUNTRY" STRING,
  "STATE" STRING,
  "PRODUCT_CATEGORY" STRING,
  "SUB_CATEGORY" STRING,
  "PRODUCT" STRING,
  "ORDER_QUANTITY" INTEGER,
  "UNIT_COST" FLOAT,
  "UNIT_PRICE" FLOAT,
  "PROFIT" FLOAT,
  "COST" FLOAT,
  "REVENUE" FLOAT
);


----------------------- SEE IF THE TABLE IS CREATED -------------------------
SELECT * FROM PUBLIC.SALES;

----------------------- UPLOAD DATA TO YOUR STAGE FROM SNOWSQL-CLIENT ---------------------
-- PUT file://<path_to_data>/sales.csv @RAW_INPUT_STAGE;


----------------------- LIST FILES FROM THE STAGE --------------------------
LIST @RAW_INPUT_STAGE;


------------------------ CREATE FILE FORMAT -------------------------
CREATE OR REPLACE file format BIKE_STORE_DB.PUBLIC.CSV_FF
  type = csv
  record_delimiter = '\n'
  field_delimiter = ','
  skip_header = 1
  null_if = ('NULL', 'null')
  empty_field_as_null = true
  FIELD_OPTIONALLY_ENCLOSED_BY = '0x22';
-- 0x22 is " quotation mark


----------------------- COPY STAGED CSV FILE TO TABLE --------------------------
-- you should be in public schema
COPY INTO BIKE_STORE_DB.PUBLIC.SALES
FROM @RAW_INPUT_STAGE/sales.csv.gz
file_format = (format_name = CSV_FF)
on_error = 'continue';


---------------------- SEE THE CONTENTS OF SALES TABLE --------------------------
SELECT * FROM PUBLIC.SALES;



---------------------- CREATE TABLE SALES IN STAGING LAYER --------------------------
CREATE OR REPLACE TABLE STAGING.SALES
(
  "DATE" DATE,
  "DAY" INTEGER,
  "MONTH" STRING,
  "YEAR" INTEGER,
  "CUSTOMER_AGE" INTEGER,
  "AGE_GROUP" STRING,
  "CUSTOMER_GENDER" CHAR(1),
  "COUNTRY" STRING,
  "STATE" STRING,
  "PRODUCT_CATEGORY" STRING,
  "SUB_CATEGORY" STRING,
  "PRODUCT" STRING,
  "ORDER_QUANTITY" INTEGER,
  "UNIT_COST" FLOAT,
  "UNIT_PRICE" FLOAT,
  "PROFIT" FLOAT,
  "COST" FLOAT,
  "REVENUE" FLOAT
);


-------------------- SEE IF THE TABLE IS CREATED PROPERLY ----------------------------
SELECT * FROM STAGING.SALES;

-------------------- LOAD DATA FROM PUBLIC.SALES TO STAGING.SALES ------------------------
-- do not perform this step here, it is performed by stored procedure
-- load all data initially
INSERT INTO STAGING.SALES
SELECT * FROM PUBLIC.SALES;


----------------------- TEST INCREMENTAL LOADING --------------------------
-- incremental loading procedure is stored in stored_procedures worksheet
-- add new dummy record into public schema sales table
INSERT INTO BIKE_STORE_DB.PUBLIC.SALES
VALUES('2022-11-26' ,26,'November',2022,19,'Youth (<25)','M','Canada','British Columbia','Accessories','Bike Racks','Hitch Rack - 4-Bike',8,45,120,590,360,950);

-- count the number of records after new data is loaded into public.sales table
SELECT COUNT(*) FROM BIKE_STORE_DB.PUBLIC.SALES; -- 113037
SELECT COUNT(*) FROM BIKE_STORE_DB.STAGING.SALES; -- 113036 (we have already loaded incrementally)

-- now call the incremental loading procedure and then check the number of records in staging.sales table
CALL BIKE_STORE_DB.STAGING.LOAD_STAGING();
SELECT COUNT(*) FROM BIKE_STORE_DB.STAGING.SALES; -- 113037 (which means that incremental loading is working)
