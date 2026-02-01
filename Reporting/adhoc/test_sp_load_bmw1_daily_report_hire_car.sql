-- Test script for sp_load_bmw1_daily_report_hire_car

-- 1. Truncate the target table to start fresh
tRUNCATE TABLE reporting.details.bigmw1_daily_report_HIRE_CAR;

-- 2. Remove all files from the Snowflake stage (optional, if you want to clear the stage)
REMOVE @reporting.details.REPORTS;

-- 3. List files in the stage to confirm the .csv is present
LIST @reporting.details.REPORTS;

-- 4. Call the stored procedure to load the data
CALL reporting.details.sp_load_bmw1_daily_report_hire_car();

-- 5. Check row count after load
SELECT COUNT(*) AS after_count FROM reporting.details.bigmw1_daily_report_HIRE_CAR;

-- 6. Optionally, view the most recent rows loaded (adjust LIMIT as needed)
SELECT * FROM reporting.details.bigmw1_daily_report_HIRE_CAR;

truncate table reporting.details.bigmw1_daily_report_hire_car_stage_audit;

select * from reporting.details.bigmw1_daily_report_hire_car_stage_audit; 

-- 7. Check for duplicates (should be zero rows)
SELECT JOB_NUMBER, VRN, COUNT(*)
FROM reporting.details.bigmw1_daily_report_HIRE_CAR
GROUP BY JOB_NUMBER, VRN
HAVING COUNT(*) > 1;
