---------------------- STORED PROCEDURES ----------------------
-- stored procedure to incrementally load staging schema with data from public schema
CREATE OR REPLACE PROCEDURE BIKE_STORE_DB.STAGING.LOAD_STAGING()
RETURNS STRING NOT NULL
LANGUAGE JAVASCRIPT
AS
$$  
    
    // sql query to load data later than the max date
    var load_staging_query = 'INSERT INTO BIKE_STORE_DB.STAGING.SALES';
    load_staging_query += '\n   SELECT * FROM BIKE_STORE_DB.PUBLIC.SALES SA';
    load_staging_query += '\n   WHERE SA.DATE > (SELECT MAX(DATE) FROM BIKE_STORE_DB.STAGING.SALES)';
    
    // convert into statement
    stmt = snowflake.createStatement({sqlText: load_staging_query});
    
    try{
        var rs = stmt.execute();
        return("Inserted data into staging sales table");
    }
    catch(err){
        return("Error!! Could not insert data into staging sales table");
    }
    
$$
;


-- stored procedure to incrementally load tables of core layer
CREATE OR REPLACE PROCEDURE BIKE_STORE_DB.CORE.INCREMENTALLY_LOAD_CORE()
RETURNS STRING NOT NULL
LANGUAGE JAVASCRIPT
AS
$$
    // all sql queries as strings
    // customer dimension table
    var query1 = `MERGE INTO BIKE_STORE_DB.CORE.CUSTOMER_DIM CT
                  USING
                        (SELECT DISTINCT CUSTOMER_AGE, AGE_GROUP, CUSTOMER_GENDER FROM BIKE_STORE_DB.STAGING.SALES ORDER BY CUSTOMER_AGE) CS
                  ON
                        CS.CUSTOMER_AGE = CT.AGE AND CS.AGE_GROUP = CT.AGE_GROUP AND CS.CUSTOMER_GENDER = CT.GENDER
                  WHEN NOT MATCHED THEN
                        INSERT ("AGE", "AGE_GROUP", "GENDER") VALUES (CS.CUSTOMER_AGE, CS.AGE_GROUP, CS.CUSTOMER_GENDER)`;

    // location dimension table
    var query2 = `MERGE INTO BIKE_STORE_DB.CORE.LOCATION_DIM LT
                   USING
                        (SELECT DISTINCT COUNTRY, STATE FROM BIKE_STORE_DB.STAGING.SALES ORDER BY COUNTRY, STATE) LS
                   ON
                        LS.COUNTRY = LT.COUNTRY AND LS.STATE = LT.STATE
                   WHEN NOT MATCHED THEN
                        INSERT ("COUNTRY", "STATE") VALUES (LS.COUNTRY, LS.STATE)`;

    // product dimension table
    var query3 = `MERGE INTO BIKE_STORE_DB.CORE.PRODUCT_DIM PT
                    USING
                        (SELECT DISTINCT PRODUCT, PRODUCT_CATEGORY, SUB_CATEGORY FROM BIKE_STORE_DB.STAGING.SALES ORDER BY PRODUCT, PRODUCT_CATEGORY, SUB_CATEGORY) PS \
                    ON
                        PT.PRODUCT = PS.PRODUCT AND PT.CATEGORY = PS.PRODUCT_CATEGORY AND PT.SUB_CATEGORY = PS.SUB_CATEGORY
                    WHEN NOT MATCHED THEN
                        INSERT ("PRODUCT", "CATEGORY", "SUB_CATEGORY") VALUES (PS.PRODUCT, PS.PRODUCT_CATEGORY, PS.SUB_CATEGORY)`;

    // sales fact table
    var query4 = `INSERT INTO BIKE_STORE_DB.CORE.SALES_FACT("DATE_FK", "CUSTOMER_TYPE_FK", "LOCATION_FK", "PRODUCT_FK", "ORDER_QUANTITY", "UNIT_COST", "UNIT_PRICE", "REVENUE", "COST", "PROFIT") \
                     SELECT
                        EXTRACT (YEAR FROM S.DATE)*10000 + EXTRACT(MONTH FROM S.DATE)*100 + EXTRACT(DAY FROM S.DATE) AS DATE_FK,
                        C.CUSTOMER_TYPE_ID AS CUSTOMER_TYPE_FK,
                        L.LOCATION_ID AS LOCATION_FK,
                        P.PRODUCT_ID AS PRODUCT_FK,
                        S.ORDER_QUANTITY,
                        S.UNIT_COST,
                        S.UNIT_PRICE,
                        S.REVENUE,
                        S.COST,
                        S.PROFIT
                    FROM BIKE_STORE_DB.STAGING.SALES S
                    LEFT JOIN BIKE_STORE_DB.CORE.CUSTOMER_DIM C
                        ON C.AGE = S.CUSTOMER_AGE AND C.GENDER = S.CUSTOMER_GENDER
                    LEFT JOIN BIKE_STORE_DB.CORE.LOCATION_DIM L
                        ON L.COUNTRY = S.COUNTRY AND L.STATE = S.STATE
                    LEFT JOIN BIKE_STORE_DB.CORE.PRODUCT_DIM P
                        ON P.PRODUCT = S.PRODUCT AND P.CATEGORY = S.PRODUCT_CATEGORY AND P.SUB_CATEGORY = S.SUB_CATEGORY
                    WHERE S.DATE > (SELECT T2.DATE
                                    FROM (SELECT MAX(DATE_FK) AS MAX_DATE_FK FROM BIKE_STORE_DB.CORE.SALES_FACT) T1
                                    INNER JOIN BIKE_STORE_DB.CORE.DATE_DIM T2
                                        ON T1.MAX_DATE_FK = T2.DATE_ID) -- selecting max date by joining sales fact with date dim table
                    ORDER BY DATE_FK`;

    // create statements
    var stmt1 = snowflake.createStatement({
        sqlText: query1
    });
    var stmt2 = snowflake.createStatement({
        sqlText: query2
    });
    var stmt3 = snowflake.createStatement({
        sqlText: query3
    });
    var stmt4 = snowflake.createStatement({
        sqlText: query4
    });
    try {
        // result sets
        var rs1 = stmt1.execute();
        var rs2 = stmt2.execute();
        var rs3 = stmt3.execute();
        var rs4 = stmt4.execute();

        // return the strings
        rs1.next();
        rs2.next();
        rs3.next();
        rs4.next();

        var return_data = 'Customer Dim Inserted: ' + rs1.getColumnValue(1) + ', Location Dim Inserted: ' + rs2.getColumnValue(1) + ', Product Dim Inserted: ' + rs3.getColumnValue(1) + ', Sales Fact Inserted: ' + rs4.getColumnValue(1);
        return return_data;
    } catch (err) {
        return ("Error!! Could not load tables in core");
    }
$$
;

-- since there is no data in sales_fact table initially incremental loading doesn't work
-- so we introduce dummy data in it, so that the incremental loading procedure works fine
INSERT INTO BIKE_STORE_DB.CORE.SALES_FACT("DATE_FK", "CUSTOMER_TYPE_FK", "LOCATION_FK", "PRODUCT_FK", "ORDER_QUANTITY", "UNIT_COST", "UNIT_PRICE", "REVENUE", "COST", "PROFIT")
VALUES ('20000101', 1, 1, 1, 0, 0, 0, 0, 0, 0);

-- also for sales table in staging layer dummy data
INSERT INTO BIKE_STORE_DB.STAGING.SALES("DATE", "DAY", "MONTH", "YEAR", "CUSTOMER_AGE", "AGE_GROUP", "CUSTOMER_GENDER", "COUNTRY", "STATE", "PRODUCT_CATEGORY", "SUB_CATEGORY", "PRODUCT", "ORDER_QUANTITY", "UNIT_COST", "UNIT_PRICE", "PROFIT", "COST", "REVENUE")
VALUES('2000-01-01', 1, 'January', 2000, 19, 'Youth (<25)', 'M', 'Canada', 'British Columbia', 'Accessories', 'Bike Racks', 'Hitch Rack -4-Bike', 0, 0, 0, 0, 0, 0);

-- call the stored procedure for incrementally loading core
-- this step is performed by complete_etl procedure
CALL BIKE_STORE_DB.CORE.INCREMENTALLY_LOAD_CORE();

-- complete ETL stored procedure
CREATE OR REPLACE PROCEDURE BIKE_STORE_DB.CORE.COMPLETE_ETL()
RETURNS STRING NOT NULL
LANGUAGE JAVASCRIPT
AS
$$
    // queries
    var query1 = 'CALL BIKE_STORE_DB.STAGING.LOAD_STAGING()';
    var query2 = 'CALL BIKE_STORE_DB.CORE.INCREMENTALLY_LOAD_CORE()';
    
    // statements
    var stmt1 = snowflake.createStatement({sqlText: query1});
    var stmt2 = snowflake.createStatement({sqlText: query2});
    
    // execute statements
    try {
        var rs1 = stmt1.execute();
        var rs2 = stmt2.execute();
        rs1.next();
        rs2.next();
        return  '[ETL Complete]' + rs2.getColumnValue(1);
    }
    catch(err){
        return 'ETL failure';
    }
$$
;

-- call the complete ETL stored procedure to load any new data that is written to staging schema
CALL BIKE_STORE_DB.CORE.COMPLETE_ETL();


------------- CHECK IF ALL TABLES ARE LOADED ---------------
SELECT * FROM BIKE_STORE_DB.STAGING.SALES;
SELECT * FROM BIKE_STORE_DB.CORE.DATE_DIM;
SELECT * FROM BIKE_STORE_DB.CORE.LOCATION_DIM;
SELECT * FROM BIKE_STORE_DB.CORE.CUSTOMER_DIM;
SELECT * FROM BIKE_STORE_DB.CORE.PRODUCT_DIM;
SELECT * FROM BIKE_STORE_DB.CORE.SALES_FACT;



------------- TESTING INCREMENTAL LOAD ----------------
INSERT INTO BIKE_STORE_DB.PUBLIC.SALES
VALUES('2022-11-26' ,26,'November',2022,19,'Youth (<25)','M','Canada','British Columbia','Accessories','Bike Racks','Hitch Rack - 4-Bike',8,45,120,590,360,950);

CALL BIKE_STORE_DB.CORE.COMPLETE_ETL();

-- check the contents of the tables