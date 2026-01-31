
truncate table reporting.details.BMW_COMMISSION_RECONCILIATION;

REMOVE @reporting.details.REPORTS;

list @reporting.details.REPORTS;

-- 1. Optionally, check current row count before load
SELECT COUNT(*) AS before_count FROM reporting.details.BMW_COMMISSION_RECONCILIATION;

-- 2. Call the stored procedure
CALL reporting.details.sp_load_bmw_commission_reconciliation();

select *  from reporting.details.BMW_COMMISSION_RECONCILIATION;

-- 3. Check row count after load
SELECT COUNT(*) AS after_count FROM reporting.details.BMW_COMMISSION_RECONCILIATION;

-- 4. Optionally, view the most recent rows loaded (adjust LIMIT as needed)
SELECT * FROM reporting.details.BMW_COMMISSION_RECONCILIATION ORDER BY code;;

-- 5. Check for duplicates (should be zero rows)
SELECT CODE, SITE_NAME, STATEMENT_TOTAL, COUNT(*)
FROM reporting.details.BMW_COMMISSION_RECONCILIATION
GROUP BY CODE, SITE_NAME, STATEMENT_TOTAL
HAVING COUNT(*) > 1;
