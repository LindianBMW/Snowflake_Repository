use role accountadmin;

CREATE OR REPLACE PROCEDURE reporting.details.sp_load_bmw1_daily_report_hire_car()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
/*
  Loads hire car report from stage @reporting.details.REPORTS into reporting.details.bigmw1_daily_report_HIRE_CAR (raw VARCHAR).
  Enhanced for success/error messaging and notification.
  Expects .csv file with columns:
  Date, Sub Account, Scheme, Job Number, VRN, Vehicle Make, Vehicle Model, Hire Car

 select * from  reporting.details.bigmw1_daily_report_HIRE_CAR; 
*/
try {
    // Drop temp table if it exists
    var drop_stage_sql = `DROP TABLE IF EXISTS temp_hire_car_report_staging;`;
    var stmt_drop_stage = snowflake.createStatement({sqlText: drop_stage_sql});
    stmt_drop_stage.execute();

    // Create temp staging table (all VARCHAR, no DUMMY_COL)
    var create_stage_sql = `
        CREATE TEMPORARY TABLE temp_hire_car_report_staging (
            DATE VARCHAR,
            SUB_ACCOUNT VARCHAR,
            SCHEME VARCHAR,
            JOB_NUMBER VARCHAR,
            VRN VARCHAR,
            VEHICLE_MAKE VARCHAR,
            VEHICLE_MODEL VARCHAR,
            HIRE_CAR VARCHAR
        );
    `;
    var stmt_create = snowflake.createStatement({sqlText: create_stage_sql});
    stmt_create.execute();

    // Load data into staging table
    var copy_command = `
        COPY INTO temp_hire_car_report_staging (
            DATE, SUB_ACCOUNT, SCHEME, JOB_NUMBER, VRN, VEHICLE_MAKE, VEHICLE_MODEL, HIRE_CAR
        )
        FROM @reporting.details.REPORTS
        PATTERN = 'bigmw1_daily report hire car\\.csv'
        FILE_FORMAT = (
            TYPE = 'CSV',
            SKIP_HEADER = 1,
            FIELD_DELIMITER = ',',
            FIELD_OPTIONALLY_ENCLOSED_BY = '"',
            TRIM_SPACE = FALSE
        )
        ON_ERROR = 'ABORT_STATEMENT';
    `;
    var stmt_copy = snowflake.createStatement({sqlText: copy_command});
    stmt_copy.execute();

    // Store all staged data in a permanent audit table (can be deleted later)
    var create_perm_sql = `
        CREATE TABLE IF NOT EXISTS reporting.details.bigmw1_daily_report_hire_car_stage_audit (
            DATE VARCHAR,
            SUB_ACCOUNT VARCHAR,
            SCHEME VARCHAR,
            JOB_NUMBER VARCHAR,
            VRN VARCHAR,
            VEHICLE_MAKE VARCHAR,
            VEHICLE_MODEL VARCHAR,
            HIRE_CAR VARCHAR
        );
    `;
    var stmt_create_perm = snowflake.createStatement({sqlText: create_perm_sql});
    stmt_create_perm.execute();

    var insert_perm_sql = `
        INSERT INTO reporting.details.bigmw1_daily_report_hire_car_stage_audit (
            DATE, SUB_ACCOUNT, SCHEME, JOB_NUMBER, VRN, VEHICLE_MAKE, VEHICLE_MODEL, HIRE_CAR
        )
        SELECT
            DATE, SUB_ACCOUNT, SCHEME, JOB_NUMBER, VRN, VEHICLE_MAKE, VEHICLE_MODEL, HIRE_CAR
        FROM temp_hire_car_report_staging;
    `;
    var stmt_insert_perm = snowflake.createStatement({sqlText: insert_perm_sql});
    stmt_insert_perm.execute();

    // Store all staged data in a permanent table for audit/archive (can be deleted later)
    var create_perm_sql = `
        CREATE TABLE IF NOT EXISTS reporting.details.bigmw1_daily_report_hire_car_stage_audit (
            DATE VARCHAR,
            SUB_ACCOUNT VARCHAR,
            SCHEME VARCHAR,
            JOB_NUMBER VARCHAR,
            VRN VARCHAR,
            VEHICLE_MAKE VARCHAR,
            VEHICLE_MODEL VARCHAR,
            HIRE_CAR VARCHAR
        );
    `;
    var stmt_create_perm = snowflake.createStatement({sqlText: create_perm_sql});
    stmt_create_perm.execute();

    var insert_perm_sql = `
        INSERT INTO reporting.details.bigmw1_daily_report_hire_car_stage_audit (
            DATE, SUB_ACCOUNT, SCHEME, JOB_NUMBER, VRN, VEHICLE_MAKE, VEHICLE_MODEL, HIRE_CAR
        )
        SELECT
            s.DATE,
            s.SUB_ACCOUNT,
            s.SCHEME,
            s.JOB_NUMBER,
            s.VRN,
            s.VEHICLE_MAKE,
            s.VEHICLE_MODEL,
            s.HIRE_CAR
        FROM (
            SELECT
                DATE as DATE,
                SUB_ACCOUNT as SUB_ACCOUNT,
                SCHEME as SCHEME,
                JOB_NUMBER as JOB_NUMBER,
                VRN as VRN,
                VEHICLE_MAKE as VEHICLE_MAKE,
                VEHICLE_MODEL as VEHICLE_MODEL,
                HIRE_CAR as HIRE_CAR
            FROM temp_hire_car_report_staging
        ) s;
    `;
    var stmt_insert_perm = snowflake.createStatement({sqlText: insert_perm_sql});
    stmt_insert_perm.execute();

    // Delete rows with invalid date format from temp table (allow DD-MON-YY and DD MON YYYY)
    var delete_invalid_date_sql = `
            DELETE FROM temp_hire_car_report_staging
            WHERE TRY_TO_DATE(DATE, 'DD-MON-YY') IS NULL
                AND TRY_TO_DATE(DATE, 'DD MON YYYY') IS NULL;
    `;
    var stmt_delete_invalid = snowflake.createStatement({sqlText: delete_invalid_date_sql});
    stmt_delete_invalid.execute();

    // Get row count before insert
    var count_before_sql = `SELECT COUNT(*) FROM reporting.details.bigmw1_daily_report_HIRE_CAR;`;
    var stmt_count_before = snowflake.createStatement({sqlText: count_before_sql});
    var rs_before = stmt_count_before.execute();
    rs_before.next();
    var count_before = rs_before.getColumnValue(1);

    // Insert only new rows into main table (exclude by JOB_NUMBER+VRN)
    var insert_command = `
            INSERT INTO reporting.details.bigmw1_daily_report_HIRE_CAR (
                DATE, SUB_ACCOUNT, SCHEME, JOB_NUMBER, VRN, VEHICLE_MAKE, VEHICLE_MODEL, HIRE_CAR
            )
            SELECT
                CASE
                    WHEN TRY_TO_DATE(staging.DATE, 'DD-MON-YY') IS NOT NULL THEN TO_VARCHAR(TRY_TO_DATE(staging.DATE, 'DD-MON-YY'), 'DD Mon YYYY')
                    WHEN TRY_TO_DATE(staging.DATE, 'DD MON YYYY') IS NOT NULL THEN TO_VARCHAR(TRY_TO_DATE(staging.DATE, 'DD MON YYYY'), 'DD Mon YYYY')
                    ELSE staging.DATE
                END AS DATE,
                staging.SUB_ACCOUNT, staging.SCHEME, staging.JOB_NUMBER, staging.VRN, staging.VEHICLE_MAKE, staging.VEHICLE_MODEL, staging.HIRE_CAR
            FROM temp_hire_car_report_staging staging
            WHERE NOT EXISTS (
                SELECT 1 FROM reporting.details.bigmw1_daily_report_HIRE_CAR t 
                WHERE t.JOB_NUMBER = staging.JOB_NUMBER AND t.VRN = staging.VRN
            );
    `;
    var stmt_insert = snowflake.createStatement({sqlText: insert_command});
    stmt_insert.execute();

    // Get row count after insert
    var count_after_sql = `SELECT COUNT(*) FROM reporting.details.bigmw1_daily_report_HIRE_CAR;`;
    var stmt_count_after = snowflake.createStatement({sqlText: count_after_sql});
    var rs_after = stmt_count_after.execute();
    rs_after.next();
    var count_after = rs_after.getColumnValue(1);

    var rows_added = count_after - count_before;

    // Drop staging table
    var drop_command = `DROP TABLE IF EXISTS temp_hire_car_report_staging;`;
    var stmt_drop = snowflake.createStatement({sqlText: drop_command});
    stmt_drop.execute();

    // Notify
    var subject, msg, returnMsg;
    if (rows_added > 0) {
        subject = 'Hire Car Report Load: Rows Loaded';
        msg = `SUCCESS: Loaded ${rows_added} new row(s) into bigw1_daily_report_HIRE_CAR from @reporting.details.REPORTS.`;
        returnMsg = msg;
    } else {
        subject = 'Hire Car Report Load: No New Rows';
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
            'Hire Car Report Load Failed',
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
    return 'ERROR in sp_load_hire_car_report: ' + err.message;
}
$$;

select * from  reporting.details.bigmw1_daily_report_HIRE_CAR; 

call reporting.details.sp_load_bmw1_daily_report_hire_car();