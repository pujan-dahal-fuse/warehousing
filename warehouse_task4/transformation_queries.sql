--------------------- ASSIGNMENT ------------------------
--Transform Table 1 into Table 2 and vice versa
--if number or email is repeated, they shouldn't repeat on the transformed table
--
--Table1
--
--Name|Email|number 
--AP|ap@gmail.com|9840449184
--AP|apaaa@gmail.com|9840373649
--AP|apppp@gmail.com|9840128367
--BC|bc@gmail.com|9840337266
--BC|cb@gmail.com|9840337266
--BC|bcb@gmail.com|9840333323
--BD|bd@gmail.com|9840098779
--BD|db@gmail.com|9840336387
--BD|bdbd@gmail.com|9840388273
--BA|ba@gmail.com|9840456789
--BA|bababa@gmail.com|9840456789
--BA|bababa@gmail.com|9840377689
--
--
-- 
--
--Table2
--Name|Email|number
--AP | ap@gmail.com,apaaa@gmail.com,apppp@gmail.com | 9840449184,9840373649,9840128367
--
--BC | bc@gmail.com,cb@gmail.com,bcb@gmail.com | 9840337266,9840337266,9840333323
--
--BD | bd@gmail.com,db@gmail.com,bdbd@gmail.com | 9840098779,9840336387,9840388273
--
--BA | ba@gmail.com,bababa@gmail.com | 9840456789,9840377689


------------------- CREATE SCHEMA -------------------------
create schema test_schema;


-- create table to store person records
create table test_schema.person
(
	name varchar(255),
	email varchar(255),
	number varchar(20)
);

-- see the contents of table
select * from test_schema.person;

-- insert multiple records into person table
insert into test_schema.person(name, email, number)
values
('AP', 'ap@gmail.com', '9840449184'),
('AP', 'apaaa@gmail.com', '9840373649'),
('AP', 'apppp@gmail.com', '9840128367'),
('BC', 'bc@gmail.com', '9840337266'),
('BC', 'cb@gmail.com', '9840333323'),
('BD', 'bd@gmail.com', '9840098779'),
('BD', 'db@gmail.com', '9840336387'),
('BD', 'bdbd@gmail.com', '9840388273'),
('BA', 'ba@gmail.com', '9840456789'),
('BA', 'bababa@gmail.com', '9840456789'),
('BA', 'bababa@gmail.com', '9840377689');


-- array_agg aggregates grouped elements into array
-- array_to_string converts arrays into string with given separator
-- required query
select 
name, 
array_to_string(array_agg(distinct email), ', ') email, 
array_to_string(array_agg(distinct number), ', ') number
from test_schema.person
group by name;


-- create person_agg table
create table test_schema.person_agg
(
	name varchar(255),
	email varchar(255),
	number varchar(255)
);

-- insert records into person_agg table
insert into test_schema.person_agg(name, email, number)
select 
name, 
array_to_string(array_agg(distinct email), ', ') email, 
array_to_string(array_agg(distinct number), ', ') number
from test_schema.person
group by name;

-- see the contents of table
select * from test_schema.person_agg;


---------- VICE VERSA TASK ------------
-- string_to_array converst string to array with provided separator
-- unnest explodes array into multiple rows
select 
name, 
unnest(string_to_array(email, ', ')) email,
unnest(string_to_array(number, ', ')) number
from test_schema.person_agg;
