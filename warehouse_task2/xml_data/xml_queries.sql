-------------------- WORKING WITH XML DATA --------------------
-------------------- CREATE DATABASE AND SCHEMA ------------------
CREATE WAREHOUSE XML_WAREHOUSE;
CREATE DATABASE XML_DB;
CREATE SCHEMA XML_SCHEMA;
CREATE STAGE RAW_STAGE;

------------------- UPLOAD FILE TO STAGE FROM DEVICE USING SNOWSQL CLIENT -------------------
-- put file://<path_to_file>/file.xml @RAW_STAGE

------------------- CREATE TABLE TO STORE RAW XML DATA ---------------------
CREATE OR REPLACE TABLE BOOKS_XML_T
(
  RAW_DATA VARIANT
);

------------------- COPY INTO TABLE THE FILE IN STAGE ---------------------
COPY INTO BOOKS_XML_T
FROM @RAW_STAGE/books.xml.gz
FILE_FORMAT = (TYPE = XML)
ON_ERROR = 'CONTINUE';

------------------- SEE IF THE DATA IS LOADED --------------------
SELECT * FROM BOOKS_XML_T;

-- "@" is used to access name
-- "$" is used to access value
-- "@attr" is used to access value of named attribute

------------------- ACCESS ATTRIBUTE OF AN ELEMENT --------------------
SELECT RAW_DATA:"@" FROM BOOKS_XML_T; -- catalog
-- this sql command helps to obtain the attribute of root element

------------------ ACCESS CONTENTS OF ELEMENT --------------------
SELECT RAW_DATA:"$" FROM BOOKS_XML_T;
-- obtain the contents of root element of XML document 
-- "@" gives attribute and "$" gives value


------------------ ACCESSING NESTED ELEMENTS --------------------
SELECT
RAW_DATA:"$"[0]."@id" -- returns id of first nested book
FROM BOOKS_XML_T;

SELECT
RAW_DATA:"$"[1]."@id" -- returns id of second nested book
FROM BOOKS_XML_T;


----------------- USING XMLGET --------------------
-- XMLGET function returns the XML element of NAME at index INDEX if provided directly under the XML. If optional index is not
-- provided, then the default value is 0, and the first element of name NAME is returned as XML with all children nodes.
SELECT 
XMLGET(RAW_DATA, 'book')
FROM BOOKS_XML_T; -- returns book at index 0

SELECT
XMLGET(RAW_DATA, 'book', 2)
FROM BOOKS_XML_T; -- returns book at index 2

SELECT
XMLGET(RAW_DATA, 'book', 2):"$"
FROM BOOKS_XML_T; -- returns contents of book at index 2

SELECT
XMLGET(RAW_DATA, 'book', 3):"@"
FROM BOOKS_XML_T; -- returns name of book element at index 3 which is "book"

-------------------- XMLGET WITH LATERAL FLATTEN ----------------
SELECT
XMLGET(VALUE, 'title'):"$"::STRING AS BOOK_TITLE
FROM BOOKS_XML_T,
LATERAL FLATTEN(INPUT => RAW_DATA:"$");


-------------------- COMPLEX QUERYING ---------------------
SELECT
VALUE:"@id"::STRING AS BOOK_ID,
XMLGET(VALUE, 'title'):"$"::STRING AS TITLE,
XMLGET(VALUE, 'genre'):"$"::STRING AS GENRE,
XMLGET(VALUE, 'author'):"$"::STRING AS AUTHOR,
XMLGET(VALUE, 'price'):"$"::FLOAT AS PRICE,
TO_DATE(XMLGET(VALUE, 'publish_date'):"$"::STRING) AS PUBLISH_DATE,
XMLGET(VALUE, 'description'):"$"::STRING AS DESCRIPTION,
VALUE
FROM BOOKS_XML_T,
LATERAL FLATTEN(INPUT => RAW_DATA:"$");
