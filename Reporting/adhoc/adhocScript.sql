-- Get 1 sample row from each table
SELECT * FROM reporting.details.SALES_REPORTS_DETAILED_SALES LIMIT 1;

SELECT * FROM reporting.details.CLAIMS_IMPORT LIMIT 1;

SELECT * FROM reporting.details.bmw_commission_site_comm LIMIT 1;

SELECT * FROM reporting.details.BLACKHORSE_COMMISSION_DATA LIMIT 1;

SELECT * FROM reporting.details.BMW_COMMISSION_RECONCILIATION 
where code is not null; 

SELECT * FROM reporting.details.BIGMW1_DAILY_REPORT LIMIT 1;

SELECT * FROM RAW.CRM.Infinity_Download LIMIT 1;

SELECT
    inf.*,
    big.*
FROM RAW.CRM.Infinity_Download inf
JOIN reporting.details.BIGMW1_DAILY_REPORT big
    ON inf.VID = big.VRN
-- or try inf.VREF = big.VRN if that is the correct mapping
LIMIT 10;


