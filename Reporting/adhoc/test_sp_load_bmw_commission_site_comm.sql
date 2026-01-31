
truncate table reporting.details.BMW_COMMISSION_RECONCILIATION;

REMOVE @reporting.details.REPORTS;

list @reporting.details.REPORTS;

drop table  reporting.details.bmw_commission_site_comm_stage_audit;
SELECT COUNT(*) AS before_count FROM reporting.details.bmw_commission_site_comm;

-- 2. Call the stored procedure
CALL reporting.details.sp_load_bmw_commission_site_comm();

-- 3. Check row count after load
SELECT COUNT(*) AS after_count FROM reporting.details.bmw_commission_site_comm;

-- 4. Optionally, view the most recent rows loaded (adjust LIMIT as needed)
SELECT * FROM reporting.details.bmw_commission_site_comm;

-- 5. Check for duplicates (should be zero rows)
SELECT AGREEMENT_NUMBER, TRANS_TYPE_CODE, THIRD_PARTY_ID_CODE, COUNT(*)
FROM reporting.details.bmw_commission_site_comm
GROUP BY AGREEMENT_NUMBER, TRANS_TYPE_CODE, THIRD_PARTY_ID_CODE
HAVING COUNT(*) > 1;
