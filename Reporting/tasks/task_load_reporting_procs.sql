
CREATE OR REPLACE TASK reporting.details.task_load_big_paid_2025_ytd
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 8 * * * Europe/London'
AS
  CALL reporting.details.sp_load_big_paid_2025_ytd();

-- Task for BIGMW1 Daily Report
CREATE OR REPLACE TASK reporting.details.task_load_bigmw1_daily_report
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 8 * * * Europe/London'
AS
  CALL reporting.details.sp_load_bigmw1_daily_report();

-- Task for Blackhorse Commission Data
CREATE OR REPLACE TASK reporting.details.task_load_blackhorse_commission_data
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 8 * * * Europe/London'
AS
  CALL reporting.details.sp_blackhorse_commission_data();

-- Resume all tasks
ALTER TASK reporting.details.task_load_big_paid_2025_ytd RESUME;
ALTER TASK reporting.details.task_load_bigmw1_daily_report RESUME;
ALTER TASK reporting.details.task_load_blackhorse_commission_data RESUME;
