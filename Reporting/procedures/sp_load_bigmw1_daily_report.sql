
use role  accountadmin;

CREATE OR REPLACE PROCEDURE reporting.details.sp_load_bigmw1_daily_report()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
/*
  Loads bigmw1_daily report from stage @reporting.details.REPORTS into reporting.details.BIGMW1_DAILY_REPORT (raw VARCHAR).
  Enhanced for success/error messaging and notification. 
  Before Merge
*/
try {
    // Drop temp table if it exists
    var drop_stage_sql = `DROP TABLE IF EXISTS temp_bigmw1_daily_report_staging;`;
    var stmt_drop_stage = snowflake.createStatement({sqlText: drop_stage_sql});
    stmt_drop_stage.execute();

    // Create temp staging table (all VARCHAR)
    var create_stage_sql = `
        CREATE TEMPORARY TABLE temp_bigmw1_daily_report_staging (
            DUMMY_COL VARCHAR,
            DATE VARCHAR,
            PARTNER VARCHAR,
            SUB_ACCOUNT VARCHAR,
            SCHEME VARCHAR,
            JOB_NUMBER VARCHAR,
            VRN VARCHAR,
            VEHICLE_MAKE VARCHAR,
            VEHICLE_MODEL VARCHAR,
            REPORTED_SYMPTOM VARCHAR,
            FAULT_DESCRIPTION VARCHAR,
            FAULT_CAUSE VARCHAR,
            FAULT_ACTION VARCHAR,
            RESOURCE_TYPE VARCHAR,
            JOB_TYPE_NAME VARCHAR,
            TOW_DESTINATION VARCHAR,
            TOW_DISTANCE VARCHAR,
            SERVICE_BREAKDOWN_COUNT VARCHAR
        );
    `;
    var stmt_create = snowflake.createStatement({sqlText: create_stage_sql});
    stmt_create.execute();

    // Load data into staging table
    var copy_command = `
        COPY INTO temp_bigmw1_daily_report_staging (
            DUMMY_COL, DATE, PARTNER, SUB_ACCOUNT, SCHEME, JOB_NUMBER, VRN, VEHICLE_MAKE, VEHICLE_MODEL, 
            REPORTED_SYMPTOM, FAULT_DESCRIPTION, FAULT_CAUSE, FAULT_ACTION, RESOURCE_TYPE, 
            JOB_TYPE_NAME, TOW_DESTINATION, TOW_DISTANCE, SERVICE_BREAKDOWN_COUNT
        )
        FROM @reporting.details.REPORTS
        PATTERN = 'bigmw1_daily report\\.csv'
        FILE_FORMAT = (
            TYPE = 'CSV',
            SKIP_HEADER = 1,
            FIELD_DELIMITER = ',',
            FIELD_OPTIONALLY_ENCLOSED_BY = '\"',
            TRIM_SPACE = FALSE
        )
        ON_ERROR = 'ABORT_STATEMENT';
    `;
    var stmt_copy = snowflake.createStatement({sqlText: copy_command});
    stmt_copy.execute();


        // Delete rows with invalid date format from temp table (allow DD-MON-YY and DD MON YYYY)
        var delete_invalid_date_sql = `
                DELETE FROM temp_bigmw1_daily_report_staging
                WHERE TRY_TO_DATE(DATE, 'DD-MON-YY') IS NULL
                    AND TRY_TO_DATE(DATE, 'DD MON YYYY') IS NULL;
        `;
        var stmt_delete_invalid = snowflake.createStatement({sqlText: delete_invalid_date_sql});
        stmt_delete_invalid.execute();

    // Get row count before insert
    var count_before_sql = `SELECT COUNT(*) FROM reporting.details.BIGMW1_DAILY_REPORT;`;
    var stmt_count_before = snowflake.createStatement({sqlText: count_before_sql});
    var rs_before = stmt_count_before.execute();
    rs_before.next();
    var count_before = rs_before.getColumnValue(1);

    // Insert only new rows into main table (exclude by JOB_NUMBER+VRN)
    var insert_command = `
            INSERT INTO reporting.details.BIGMW1_DAILY_REPORT (
                DATE, PARTNER, SUB_ACCOUNT, SCHEME, JOB_NUMBER, VRN, VEHICLE_MAKE, VEHICLE_MODEL, 
                REPORTED_SYMPTOM, FAULT_DESCRIPTION, FAULT_CAUSE, FAULT_ACTION, RESOURCE_TYPE, 
                JOB_TYPE_NAME, TOW_DESTINATION, TOW_DISTANCE, SERVICE_BREAKDOWN_COUNT
            )
            SELECT
                CASE
                    WHEN TRY_TO_DATE(DATE, 'DD-MON-YY') IS NOT NULL THEN TO_VARCHAR(TRY_TO_DATE(DATE, 'DD-MON-YY'), 'DD Mon YYYY')
                    WHEN TRY_TO_DATE(DATE, 'DD MON YYYY') IS NOT NULL THEN TO_VARCHAR(TRY_TO_DATE(DATE, 'DD MON YYYY'), 'DD Mon YYYY')
                    ELSE DATE
                END AS DATE,
                PARTNER, SUB_ACCOUNT, SCHEME, JOB_NUMBER, VRN, VEHICLE_MAKE, VEHICLE_MODEL, 
                REPORTED_SYMPTOM, FAULT_DESCRIPTION, FAULT_CAUSE, FAULT_ACTION, RESOURCE_TYPE, 
                JOB_TYPE_NAME, TOW_DESTINATION, TOW_DISTANCE, SERVICE_BREAKDOWN_COUNT
            FROM temp_bigmw1_daily_report_staging s
            WHERE NOT EXISTS (
                SELECT 1 FROM reporting.details.BIGMW1_DAILY_REPORT t 
                WHERE t.JOB_NUMBER = s.JOB_NUMBER AND t.VRN = s.VRN
            );
    `;
    var stmt_insert = snowflake.createStatement({sqlText: insert_command});
    stmt_insert.execute();

    // Get row count after insert
    var count_after_sql = `SELECT COUNT(*) FROM reporting.details.BIGMW1_DAILY_REPORT;`;
    var stmt_count_after = snowflake.createStatement({sqlText: count_after_sql});
    var rs_after = stmt_count_after.execute();
    rs_after.next();
    var count_after = rs_after.getColumnValue(1);

    var rows_added = count_after - count_before;

    // Drop staging table
    var drop_command = `DROP TABLE IF EXISTS temp_bigmw1_daily_report_staging;`;
    var stmt_drop = snowflake.createStatement({sqlText: drop_command});
    stmt_drop.execute();

    // Notify
    var subject, msg, returnMsg;
    if (rows_added > 0) {
        subject = 'BIGMW1 Daily Report Load: Rows Loaded';
        msg = `SUCCESS: Loaded ${rows_added} new row(s) into BIGMW1_DAILY_REPORT from @reporting.details.REPORTS.`;
        returnMsg = msg;
    } else {
        subject = 'BIGMW1 Daily Report Load: No New Rows';
        msg = 'Load ran successfully: No new rows loaded; data is up to date and no duplicates found.';
        returnMsg = msg;
    }
    var success_email_command = `call system$send_email(
        'DATA_ALERTS_EMAIL_INT',
        'Lindian.thomas@bigmotoringworld.co.uk,stuart.saunders@bigmotoringworld.co.uk',
        '${subject}',
        '${msg}'
    );`;
    var stmt_success_email = snowflake.createStatement({sqlText: success_email_command});
    stmt_success_email.execute();

    var allow_recipients_command = `ALTER NOTIFICATION INTEGRATION DATA_ALERTS_EMAIL_INT SET ALLOWED_RECIPIENTS = ('lindian.thomas@bigmotoringworld.co.uk', 'stuart.saunders@bigmotoringworld.co.uk');`;
    var stmt_allow_recipients = snowflake.createStatement({sqlText: allow_recipients_command});
    stmt_allow_recipients.execute();

    return returnMsg;
} catch (err) {
    try {
        var email_command = `call system$send_email(
            'DATA_ALERTS_EMAIL_INT',
            'Lindian.thomas@bigmotoringworld.co.uk,stuart.saunders@bigmotoringworld.co.uk',
            'BIGMW1 Daily Report Load Failed',
            'Error message: ${err.message}'
        );`;
        var stmt_email = snowflake.createStatement({sqlText: email_command});
        stmt_email.execute();

        var allow_recipients_command = `ALTER NOTIFICATION INTEGRATION DATA_ALERTS_EMAIL_INT SET ALLOWED_RECIPIENTS = ('lindian.thomas@bigmotoringworld.co.uk', 'stuart.saunders@bigmotoringworld.co.uk');`;
        var stmt_allow_recipients = snowflake.createStatement({sqlText: allow_recipients_command});
        stmt_allow_recipients.execute();
    } catch (emailErr) {
        // Ignore email error
    }
    return 'ERROR in sp_load_bigmw1_daily_report: ' + err.message;
}
$$;