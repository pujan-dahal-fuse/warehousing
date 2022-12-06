-- STORED PROCEDURES
-- store procedure to count the number of rows in any table
CREATE OR REPLACE PROCEDURE STORE_DB.STORE_SCHEMA.GET_ROW_COUNT(table_name VARCHAR)
RETURNS FLOAT NOT NULL
LANGUAGE JAVASCRIPT
AS
$$
    var row_count = 0

    // SQL statement to get the count of rows in the table
    var query = 'select count(*) from ' + TABLE_NAME;

    // create a snowflake sql statement from the above query in the form of string
    var statement = snowflake.createStatement({sqlText: query})

    // execute the statement
    var result_set = statement.e    xecute()

    // get the row count, the first row of table in its first column contains the row count
    result_set.next();
    row_count = result_set.getColumnValue(1)

    return row_count;
$$
;

CALL GET_ROW_COUNT('FEATURES'); -- 8190
CALL GET_ROW_COUNT('SALES'); -- 421570
CALL GET_ROW_COUNT('STORES'); -- 45


-- stored procedure can be overloaded i.e. same named stored procedure can be used with different number of parameters
-- stored procedure to count the number of rows in a table given a certain condition
CREATE OR REPLACE PROCEDURE STORE_DB.STORE_SCHEMA.GET_ROW_COUNT(table_name VARCHAR, condition VARCHAR)
RETURNS FLOAT NOT NULL
LANGUAGE JAVASCRIPT
AS
$$
    var row_count = 0

    // SQL statement to get the count of rows in the table
    var query = 'select count(*) from ' + TABLE_NAME + ' where ' + CONDITION;

    // create a snowflake sql statement from the above query in the form of string
    var statement = snowflake.createStatement({sqlText: query})

    // try to execute the statement, given condition may raise an error
    try {
        var result_set = statement.execute()
        
        // get the row count, the first row of table in its first column contains the row count
        result_set.next();
        row_count = result_set.getColumnValue(1)

        return row_count;
    }
    catch(err) {
        // on error return NaN (not a number)
        return NaN;
    }
$$
;

CALL GET_ROW_COUNT('FEATURES', 'TEMPERATURE <20'); -- 129

-- to see the list of procedures we have
SHOW PROCEDURES;

-- stored procedure to delete records of a store from table SALES and STORES
CREATE OR REPLACE PROCEDURE DELETE_STORE_RECORDS(store_id varchar)
RETURNS VARCHAR NOT NULL
LANGUAGE JAVASCRIPT
AS
$$
    // query variable to store the sql statement in the form of string
    var query1 = 'DELETE FROM SALES WHERE STORE = ' + STORE_ID;
    var query2 = 'DELETE FROM STORES WHERE STORE = ' + STORE_ID;

    // sql statements
    var stmt1 = snowflake.createStatement({sqlText: query1})
    var stmt2 = snowflake.createStatement({sqlText: query2})

    try {
        // execute the statements
        var rs1 = stmt1.execute();
        var rs2 = stmt2.execute();
        return 'Store record with id ' + STORE_ID + ' successfully deleted';
    }
    catch(err){
        result = 'Failed! Code: '+ err.code;
        result += '\nMessage: ' + err.message;
        return result;
    }
$$
;

-- delete store records of store 1
CALL DELETE_STORE_RECORDS('1');

-- see if the results are actually deleted
SELECT * FROM SALES;
SELECT * FROM STORES;


-- stored procedure for features table to update markdown1 column with 'temp > 85' or 'temp < 85' or 'temp = 85' according to given temperature
-- further update markdown2 column with 'fuel_price > 3.5', 'fuel_price < 3.5' or 'fuel_price = 3.5' according to unemployment column
-- and UPDATE markdown3 column 'HOLIDAY', 'NO HOLIDAY' according to IS_HOLIDAY column
CREATE OR REPLACE PROCEDURE STORE_DB.STORE_SCHEMA.UPDATE_FEATURES(table_name VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    // sql query
    var query1 = 'SELECT * FROM ' + TABLE_NAME;
    // sql statement
    var stmt1 = snowflake.createStatement({sqlText: query1});
    // execute the statement
    var result_set = stmt1.execute();
    
    while(result_set.next()){
        var date = result_set.getColumnValue(2);
        var temperature = result_set.getColumnValue(3);
        var fuel_price = result_set.getColumnValue(4);
        var is_holiday  = result_set.getColumnValue(12);
        
        var update_markdown1 = '';
        var update_markdown2 = '';
        var update_markdown3 = '';
        
        if (temperature > 85){
            update_markdown1 = "'temperature > 85'";
        }
        else if (temperature < 85){
            update_markdown1 = "'temperature < 85'";
        }
        else {
            update_markdown1 = "'temperature = 85'";
        }
        
        if (fuel_price > 3.5){
            update_markdown2 = "'fuel_price > 3.5'";
        }
        else if (fuel_price < 3.5){
            update_markdown2 = "'fuel_price < 3.5'";
        }
        else {
            update_markdown2 = "'fuel_price = 3.5'";
        }
        
        if (is_holiday == "TRUE") {
            update_markdown3 = "'HOLIDAY'";
        }
        else {
            update_markdown3 = "'NO HOLIDAY'";
        }
        
        // update the markdowns in this statement, record is identified by date field
        var query2 = 'UPDATE ' + TABLE_NAME;
        query2 += '\n   SET MARKDOWN1 = ' + update_markdown1 + ',';
        query2 += '\n       MARKDOWN2 = ' + update_markdown2 + ',';
        query2 += '\n       MARKDOWN3 = ' + update_markdown3;
        query2 += "\n   WHERE DATE = '" + date + "'";
        
        // sql statement
        var stmt2 = snowflake.createStatement({sqlText: query2});
        
        // execute sql statement
        var rs2 = stmt2.execute();
    }
    return 'Successfully updated '+ TABLE_NAME + ' table';
$$
;

CALL UPDATE_FEATURES('FEATURES');   

SELECT * FROM FEATURES;


-- stored procedure to convert given string to timestamp
CREATE OR REPLACE PROCEDURE STORE_DB.STORE_SCHEMA.STRING_TO_TIMESTAMP_LTZ(TSV VARCHAR)
RETURNS TIMESTAMP_LTZ
LANGUAGE JAVASCRIPT
AS
$$
    // sql query
    var query = "SELECT '" + TSV + "'::TIMESTAMP_LTZ";
    // sql statement
    var stmt = snowflake.createStatement({sqlText: query});
    
    // execute the statement
    var result_set = stmt.execute();
    
    // Retrieve the TIMESTAMP_LTZ and store it in sf_date variable
    result_set.next()
    var sf_date = result_set.getColumnValue(1);
    
    return sf_date;
$$
;

CALL STRING_TO_TIMESTAMP_LTZ('2022-11-20 16:00:00');


-- User defined functions
-- a user defined function to convert temperature in celsius to farenheit
CREATE OR REPLACE FUNCTION TEMPERATURE_IN_FARENHEIT(temperature DOUBLE)
RETURNS DOUBLE
LANGUAGE JAVASCRIPT
AS
$$
    return (TEMPERATURE * 9/5) + 32
$$
;

-- apply the udf to features table
SELECT 
    TEMPERATURE AS CELSIUS,
    TEMPERATURE_IN_FARENHEIT(TEMPERATURE) AS FARENHEIT 
FROM STORE_DB.STORE_SCHEMA.FEATURES;

