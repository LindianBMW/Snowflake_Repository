-- Snowflake Task to call reporting.details.sp_load_big_paid_2025_ytd() at 8am daily
CREATE OR REPLACE TASK reporting.details.task_load_big_paid_2025_ytd
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 8 * * * Europe/London'
AS
  CALL reporting.details.sp_load_big_paid_2025_ytd();
 
ALTER TASK reporting.details.task_load_big_paid_2025_ytd RESUME;

