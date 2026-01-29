use role accountadmin;
/*
This is just a demo of how the Big YTD data load would work. It is a template for the other loads
where we take an attachment to an email and ingest the attachment contents into a Snowflake table
or file
Merge OK. 
 */

 --1. Clear out the Destination file for demo purposes
truncate table reporting.details.BIGMW1_DAILY_REPORT;
 
--2. Confirm the table is empty
 select * from reporting.details.BIGMW1_DAILY_REPORT;

--3. Clear Stage 
remove @reporting.details.REPORTS;

list @reporting.details.REPORTS;

--3. Ask Stuart to go to Power Automate 

/*/
https://make.powerautomate.com/environments/Default-81f4a483-668f-47fa-a36e-cb76157c74f0/flows/8920b67f-1a84-49e0-9d84-b9ede638ffc3/details

--4. Go to sharepoint
https://bigmotoringworldcouk.sharepoint.com/sites/DataAnalytics/Test/Forms/AllItems.aspx

C:\Users\lindian.thomas\Documents\ForDemo

--4. Sharepoint location of the file to be ingested


--- send the email with the attachment
scrip to reun after sharefolder checks C:\Users\lindian.thomas\Documents\Snowflake\Reporting\scripts 
*/ 
--5. Now run the stored procedure to load the data from the email attachment into Snowflake

call reporting.details.sp_load_bigmw1_daily_report();

select * from reporting.details.BIGMW1_DAILY_REPORT; --2026-01-26 02:36:45.836

select * from reporting.details.BIGMW1_DAILY_REPORT where customer = 'Lindian'; 

DELETE FROM reporting.details.BIGMW1_DAILY_REPORT
WHERE CLAIM_NO IN (
  SELECT CLAIM_NO
  FROM reporting.details.BIGMW1_DAILY_REPORT
  LIMIT 756
);

