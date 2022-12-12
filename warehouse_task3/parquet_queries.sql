---------------- WORKING WITH PARQUET FILES -------------------
---------------- CREATE DATABASE AND SCHEMA -------------------
CREATE WAREHOUSE PARQUET_WH;
CREATE DATABASE PARQUET_DB;
CREATE SCHEMA PARQUET_SCHEMA;

---------------- EXAMPLE DATA FORMAT -----------------
//{
//  "continent": "Europe",
//  "country": {
//    "city": [
//      "Paris",
//      "Nice",
//      "Marseilles",
//      "Cannes"
//    ],
//    "name": "France"
//  }
//}

---------------- CREATE TABLE TO LOAD DATA ------------------
CREATE OR REPLACE TABLE CITIES (
  CONTINENT STRING DEFAULT NULL,
  COUNTRY STRING DEFAULT NULL,
  CITY STRING DEFAULT NULL
);

---------------- CREATE PARQUET FILE FORMAT -----------------
CREATE OR REPLACE FILE FORMAT PARQUET_FF
    TYPE = 'parquet';
    
---------------- CREATE STAGE THAT STORES PARQUET FILES -------------------
CREATE OR REPLACE STAGE RAW_STAGE
FILE_FORMAT = PARQUET_FF;


---------------- PUT SAMPLE PARQUET FILE INTO RAW_STAGE USING SNOWSQL CLIENT ------------------
-- PUT FILE://<PATH_TO_FILE>/filename.parquet

---------------- LIST FILES OF RAW STAGE ----------------
LIST @RAW_STAGE;


---------------- SEE THE CONTENTS OF PARQUET FILE -------------------
SELECT
*
FROM @RAW_STAGE/cities.parquet;

---------------- SEE NESTED ELEMENTS -----------------
SELECT
$1:continent::string continent,
$1:country:name::string country,
$1:country:city::variant city
FROM @RAW_STAGE/cities.parquet;

---------------- CREATE TABLE TO STORE RAW_DATA --------------
CREATE TABLE RAW_PARQUET_T
(
  RAW_DATA VARIANT
);

---------------- COPY RAW RECORD INTO TABLE ----------------
COPY INTO RAW_PARQUET_T
FROM @RAW_STAGE/cities.parquet
FILE_FORMAT = (TYPE = 'parquet')
ON_ERROR = 'continue';

---------------- SEE CONTENTS OF TABLE ----------------
SELECT * FROM RAW_PARQUET_T;

---------------- USE LATERAL FLATTEN TO LOAD INTO CITIES TABLE ---------------
INSERT INTO CITIES
SELECT
RAW_DATA:continent::string CONTINENT,
RAW_DATA:country:name::string COUNTRY,
VALUE::string CITY
FROM RAW_PARQUET_T,
LATERAL FLATTEN(INPUT => RAW_DATA:country:city)
WHERE CONTINENT IS NOT NULL;

SELECT * FROM CITIES;
