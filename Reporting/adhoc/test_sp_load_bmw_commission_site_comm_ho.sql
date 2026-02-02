truncate table reporting.details.bmw_commission_site_comm_ho;
truncate table reporting.details.bmw_commission_site_comm_stage_audit;
REMOVE @reporting.details.REPORTS;
list @reporting.details.REPORTS;

-- 1. Call the stored procedure
CALL reporting.details.sp_load_bmw_commission_site_comm_HO();

-- 2. Check row count after load
SELECT COUNT(*) AS after_count FROM reporting.details.bmw_commission_site_comm_HO;

-- 3. Optionally, view the most recent rows loaded (adjust LIMIT as needed)
select * from reporting.details.bmw_commission_site_comm_ho;

-- 4. Check audit table
SELECT * FROM reporting.details.bmw_commission_site_comm_stage_audit;

-- 5. Check for duplicates (should be zero rows)
SELECT AGREEMENT_NUMBER, TRANS_TYPE_CODE, THIRD_PARTY_ID_CODE, COUNT(*)
FROM reporting.details.bmw_commission_site_comm
GROUP BY AGREEMENT_NUMBER, TRANS_TYPE_CODE, THIRD_PARTY_ID_CODE
HAVING COUNT(*) > 1;

select * from reporting.details.bmw_commission_site_comm_ho_stage_audit;
