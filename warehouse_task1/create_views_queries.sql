-------------- RETRIEVE RAW DATA BY JOINING CORE TABLES ----------------
-------------- SEE CONTENTS OF TABLES ---------------
SELECT * FROM PUBLIC.SALES;
SELECT * FROM BIKE_STORE_DB.STAGING.SALES;
SELECT * FROM BIKE_STORE_DB.CORE.DATE_DIM;
SELECT * FROM BIKE_STORE_DB.CORE.LOCATION_DIM;
SELECT * FROM BIKE_STORE_DB.CORE.CUSTOMER_DIM;
SELECT * FROM BIKE_STORE_DB.CORE.PRODUCT_DIM;
SELECT * FROM BIKE_STORE_DB.CORE.SALES_FACT ORDER BY SALES_ID;


USE WAREHOUSE BIKE_WAREHOUSE;
USE DATABASE BIKE_STORE_DB;
USE SCHEMA CORE;

------------- JOIN FACT AND DIMENSION TABLES APPROPRIATELY TO OBTAIN RAW DATA AND CREATE VIEW -----------------
CREATE OR REPLACE VIEW BIKE_STORE_DB.CORE.RAW_DATA_VIEW
AS
SELECT
DD.DATE,
DD.DAY_OF_MON DAY,
DD.MONTH_NAME MONTH,
DD.YEAR,
CD.AGE CUSTOMER_AGE,
CD.AGE_GROUP,
CD.GENDER CUSTOMER_GENDER,
LD.COUNTRY,
LD.STATE,
PD.CATEGORY PRODUCT_CATEGORY,
PD.SUB_CATEGORY,
PD.PRODUCT,
SF.ORDER_QUANTITY,
SF.UNIT_COST,
SF.UNIT_PRICE,
SF.PROFIT,
SF.COST,
SF.REVENUE
FROM SALES_FACT SF
INNER JOIN DATE_DIM DD ON SF.DATE_FK = DD.DATE_ID
INNER JOIN CUSTOMER_DIM CD ON SF.CUSTOMER_TYPE_FK = CD.CUSTOMER_TYPE_ID
INNER JOIN LOCATION_DIM LD ON SF.LOCATION_FK = LD.LOCATION_ID
INNER JOIN PRODUCT_DIM PD ON SF.PRODUCT_FK = PD.PRODUCT_ID;


-------------- SEE THE CONTENTS OF THE VIEW ---------------
SELECT * FROM RAW_DATA_VIEW;



------------- VIEWS THAT PERFORM ANALYSIS ON DATA ---------------
-- VIEW 1
-- CALCULATE TOTAL REVENUE COST PROFIT FOR EACH YEAR
CREATE OR REPLACE VIEW BIKE_STORE_DB.CORE.TOTAL_SALES_YEAR_VIEW
AS
SELECT
DD.YEAR,
SUM(SF.COST) TOTAL_COST,
SUM(SF.REVENUE) TOTAL_REVENUE,
SUM(SF.PROFIT) TOTAL_PROFIT
FROM SALES_FACT SF
INNER JOIN DATE_DIM DD
ON SF.DATE_FK = DD.DATE_ID
GROUP BY DD.YEAR;

SELECT * FROM TOTAL_SALES_YEAR_VIEW;


-- VIEW 2
-- TOP 5 STORE LOCATIONS WHICH HAD HIGHEST AVERAGE PROFIT PER ORDER IN YEAR 2014
CREATE OR REPLACE VIEW BIKE_STORE_DB.CORE.STORES_WITH_HIGHEST_AVG_PROFIT
AS
SELECT
DD.YEAR,
LD.COUNTRY,
LD.STATE,
SUM(SF.PROFIT)/COUNT(1) AVG_PROFIT
FROM SALES_FACT SF
INNER JOIN LOCATION_DIM LD ON SF.LOCATION_FK = LD.LOCATION_ID
INNER JOIN DATE_DIM DD ON SF.DATE_FK = DD.DATE_ID
WHERE DD.YEAR = 2014
GROUP BY 1, 2, 3
ORDER BY 4 DESC
LIMIT 5;

SELECT * FROM STORES_WITH_HIGHEST_AVG_PROFIT;

-- VIEW 3
-- PRODUCTS WITH MAXIMUM QUANTITIES ORDERED EACH YEAR
CREATE OR REPLACE VIEW PRODUCT_MAX_SOLD_VIEW
AS
SELECT YEAR, PRODUCT, TOTAL_ORDERED FROM
(SELECT
DD.YEAR,
PD.PRODUCT,
SUM(SF.ORDER_QUANTITY) AS TOTAL_ORDERED,
RANK() OVER(PARTITION BY DD.YEAR ORDER BY SUM(SF.ORDER_QUANTITY) DESC) RNK
FROM SALES_FACT SF
INNER JOIN PRODUCT_DIM PD ON SF.PRODUCT_FK = PD.PRODUCT_ID
INNER JOIN DATE_DIM DD ON SF.DATE_FK = DD.DATE_ID
GROUP BY 1, 2
)
WHERE RNK = 1
ORDER BY 1;


SELECT * FROM PRODUCT_MAX_SOLD_VIEW;
