-- Test script for SALES_REPORTS_DETAILED_SALES ETL

-- Use the correct role and warehouse
use role accountadmin;
use warehouse compute_wh;

-- 1. Clear out the destination table for demo purposes
TRUNCATE TABLE reporting.details.SALES_REPORTS_DETAILED_SALES;

-- 2. Confirm the table is empty
SELECT * FROM reporting.details.SALES_REPORTS_DETAILED_SALES;

-- 3. Clear the stage
REMOVE @reporting.details.REPORTS;

list @reporting.details.REPORTS;

-- 4. Upload your test file to the stage (do this via SnowSQL or the web UI)
-- Example SnowSQL command:
-- snowsql -c your_connection -q "PUT 'file://C:/Temp/2026.01 - SALES_REPORTS_DETAILED_SALES.csv' @reporting.details.REPORTS AUTO_COMPRESS=false"

-- 5. Run the stored procedure to load the data
CALL reporting.details.sp_sales_reports_detailed_sales();

-- 6. Check the results
SELECT * FROM reporting.details.SALES_REPORTS_DETAILED_SALES;

-- 7. Optionally, run the procedure again to confirm idempotency (should report no new/updated rows)
CALL reporting.details.sp_sales_reports_detailed_sales();
