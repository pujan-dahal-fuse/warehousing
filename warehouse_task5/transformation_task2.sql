---------------------- USE WAREHOUSE AND DATABASE -------------------
USE WAREHOUSE PRACTICE_WAREHOUSE;
USE DATABASE PRACTICE_DB;
CREATE SCHEMA TRANSFORMATION_TASK;
CREATE STAGE RAW_STAGE;

---------------------- CREATE CSV FILE FORMAT -------------------
CREATE OR REPLACE FILE FORMAT CSV_FF
  type = csv
  record_delimiter = '\n'
  field_delimiter = ','
  skip_header = 1
  null_if = ('NULL', 'null')
  empty_field_as_null = true;
  
-------------- UPLOAD TO RAW STAGE FROM DEVICE -----------------
-- put file://<path_to_csv>/filename.csv @RAW_STAGE;

------------- LIST CONTENTS OF STAGE --------------
LIST @RAW_STAGE;

------------- CREATE TABLE TO STORE DATA ----------------
SELECT
$1,$2,$3,$4,$5,$6,$7,$8,$9,$10
FROM @RAW_STAGE/All_data.csv.gz;

CREATE OR REPLACE TABLE SONGS
(
  STATION_ID INT NOT NULL,
  SONG_ID INT NOT NULL,
  BREAKOUT_NAME VARCHAR NOT NULL,
  DATE STRING NOT NULL,
  T1 INT DEFAULT NULL,
  T2 INT DEFAULT NULL,
  T3 INT DEFAULT NULL,
  T4 INT DEFAULT NULL,
  T5 INT DEFAULT NULL,
  T6 INT DEFAULT NULL
);

SELECT * FROM SONGS;

------------- COPY INTO TABLE ------------------
COPY INTO SONGS
FROM @RAW_STAGE/All_data.csv.gz
FILE_FORMAT = CSV_FF
ON_ERROR = 'CONTINUE';

------------- SEE THE CONTENTS OF TABLE --------------
SELECT * FROM SONGS;



------------- QUERY TO GET NECESSARY RESULT ---------------
SELECT 
STATION_ID, 
SONG_ID, 
SUM(T1) T1, 
SUM(T2) T2, 
SUM(T3) T3, 
SUM(T4) T4, 
SUM(T5) T5, 
SUM(T6) T6
FROM SONGS
GROUP BY STATION_ID, SONG_ID;

------------- USER DEFINED FUNCTION(UDFT) TO RETURN VALUES FOR GIVEN STATION_ID AND SONG_ID --------------
CREATE OR REPLACE FUNCTION station_song_values(st_id int, sng_id int)
RETURNS TABLE(station_id int, song_id int, t1 int, t2 int, t3 int, t4 int, t5 int, t6 int)
AS
$$
SELECT 
STATION_ID, 
SONG_ID, 
SUM(T1) T1, 
SUM(T2) T2, 
SUM(T3) T3, 
SUM(T4) T4, 
SUM(T5) T5, 
SUM(T6) T6
FROM SONGS
WHERE STATION_ID = ST_ID AND SONG_ID = SNG_ID
GROUP BY STATION_ID, SONG_ID
$$
;

SELECT * from table(station_song_values(3322022, 1214548025));