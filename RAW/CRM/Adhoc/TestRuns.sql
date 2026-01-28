use role ACCOUNTADMIN;

truncate table RAW.CRM.INFINITY_DOWNLOAD;

select * from RAW.CRM.INFINITY_DOWNLOAD limit 10;

--  remove @RAW.CRM.INFINITY;

list @RAW.CRM.INFINITY;

--============================= TEST RUNS ============================--
CALL RAW.CRM.sp_load_infinity_download();

select *  from RAW.CRM.INFINITY_DOWNLOAD  limit 10; 

DELETE FROM RAW.CRM.INFINITY_DOWNLOAD WHERE TRY_CAST(rowid AS NUMBER) IS NULL;
select count(*)  from RAW.CRM.INFINITY_DOWNLOAD ;

show stages;

create stage RAW.CRM.INFINITY;

show schemas like 'D%' ;

show network policies;

--delete first 10 rows of the table
DELETE FROM RAW.CRM.Infinity_Download
WHERE rowId IN (
    SELECT rowId
    FROM RAW.CRM.Infinity_Download
    ORDER BY rowId
    LIMIT 6001
);

 SELECT
    dialledPhoneNumber,
    srcPhoneNumber,
    destPhoneNumber,
    num
FROM RAW.CRM.Infinity_Download;

use role accountadmin;

CALL SYSTEM$START_USER_EMAIL_VERIFICATION('LINDIAN.THOMAS');

SHOW USERS;

--ASK STUART TO RUN THIS ONE BELOW TO VERIFY HIS EMAIL ----------------------------------------
CALL SYSTEM$START_USER_EMAIL_VERIFICATION('stuart.saunders@bigmotoringworld.co.uk');

ALTER NOTIFICATION INTEGRATION DATA_ALERTS_EMAIL_INT
SET ALLOWED_RECIPIENTS = ('lindian.thomas@bigmotoringworld.co.uk', 'stuart.saunders@bigmotoringworld.co.uk');

DESC NOTIFICATION INTEGRATION DATA_ALERTS_EMAIL_INT;

---------------------------------------------------------------------------------------------------------


CREATE NOTIFICATION INTEGRATION DATA_ALERTS_EMAIL_INT
  TYPE = EMAIL
  ENABLED = TRUE
  ALLOWED_RECIPIENTS = (
    'lindian.thomas@bigmotoringworld.co.uk',
    'stuart.saunders@bigmotoringworld.co.uk'
  );

 
 GRANT USAGE
 ON INTEGRATION DATA_ALERTS_EMAIL_INT
 TO ROLE ACCOUNTADMIN;
 

 SHOW NOTIFICATION INTEGRATIONS;

 DESC NOTIFICATION INTEGRATION DATA_ALERTS_EMAIL_INT;


 CALL SYSTEM$SEND_EMAIL(
  'DATA_ALERTS_EMAIL_INT',
  'lindian.thomas@bigmotoringworld.co.uk',
  'Test',
  'Test'
);
DESC NOTIFICATION INTEGRATION DATA_ALERTS_EMAIL_INT;

ALTER NOTIFICATION INTEGRATION DATA_ALERTS_EMAIL_INT
SET ALLOWED_RECIPIENTS = ('lindian.thomas@bigmotoringworld.co.uk', 'stuart.saunders@bigmotoringworld.co.uk');
ALTER NOTIFICATION INTEGRATION DATA_ALERTS_EMAIL_INT
SET ALLOWED_RECIPIENTS = ('lindian.thomas@bigmotoringworld.co.uk', 'stuart.saunders@bigmotoringworld.co.uk');

USE ROLE ACCOUNTADMIN;
ALTER NOTIFICATION INTEGRATION DATA_ALERTS_EMAIL_INT
SET ALLOWED_RECIPIENTS = ('lindian.thomas@bigmotoringworld.co.uk');


USE DATABASE RAW;
USE SCHEMA CRM;
show schemas;


DESC NOTIFICATION INTEGRATION DATA_ALERTS_EMAIL_INT;

CALL SYSTEM$SEND_EMAIL(
  'DATA_ALERTS_EMAIL_INT',
  'lindian.thomas@bigmotoringworld.co.uk',
  'Test',
  'Test'
);



select * from  reporting.details.CLAIMS_IMPORT limit 10;


list @RAW.CRM.INFINITY;  