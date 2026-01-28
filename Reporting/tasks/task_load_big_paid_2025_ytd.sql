-- Snowflake Task to call reporting.details.sp_load_big_paid_2025_ytd() at 8am daily
CREATE OR REPLACE TASK reporting.details.task_load_big_paid_2025_ytd
  WAREHOUSE = COMPUTW_WH
  SCHEDULE = 'USING CRON 0 8 * * * Europe/London'
AS
  CALL reporting.details.sp_load_big_paid_2025_ytd();
-- Replace <YOUR_WAREHOUSE_NAME> with your actual Snowflake warehouse name before running.

ALTER TASK reporting.details.task_load_big_paid_2025_ytd RESUME;