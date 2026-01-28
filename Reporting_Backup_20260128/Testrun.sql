
use database REPORTING;
use schema REPORTING.Details;


Truncate table reporting.details.CLAIMS_IMPORT;

call reporting.details.sp_load_big_paid_2025_ytd();

select * from reporting.details.CLAIMS_IMPORT; --2026-01-26 02:36:45.836


show tasks; 


remove @reporting.details.REPORTS;

list @reporting.details.REPORTS;