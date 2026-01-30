use role accountadmin;

CREATE OR REPLACE PROCEDURE reporting.details.sp_load_bmw_commission_site_comm()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
/*
  Loads BMW commission site comm data from stage @reporting.details.REPORTS into reporting.details.bmw_commission_site_comm (all VARCHAR).
  Skips first 1 row of the CSV (header is on row 2).
  Enhanced for success/error messaging and notification.
*/
try {
    // Drop temp table if it exists
    var drop_stage_sql = `DROP TABLE IF EXISTS temp_bmw_commission_site_comm_staging;`;
    var stmt_drop_stage = snowflake.createStatement({sqlText: drop_stage_sql});
    stmt_drop_stage.execute();

    // Create temp staging table (all VARCHAR)
    var create_stage_sql = `
        CREATE TEMPORARY TABLE temp_bmw_commission_site_comm_staging (
            TRANSACTION_CREATION_DATE_TIME VARCHAR,
            AGREEMENT_NUMBER VARCHAR,
            TRANS_TYPE_CODE VARCHAR,
            TRANS_TYPE_DESC VARCHAR,
            THIRD_PARTY_ID_CODE VARCHAR,
            TRADING_NAME VARCHAR,
            DEBIT_VALUE VARCHAR,
            CREDIT_VALUE VARCHAR,
            ADVANCE VARCHAR,
            CUSTOMER_NAME VARCHAR,
            REGISTRATION_PLATE VARCHAR,
            COMPONENT_LIVE_DATE VARCHAR,
            PRODUCT VARCHAR,
            CREDIT_PERCENT VARCHAR
        );
    `;
    var stmt_create = snowflake.createStatement({sqlText: create_stage_sql});
    stmt_create.execute();

    // Load data into staging table, skip first row
    var copy_command = `
        COPY INTO temp_bmw_commission_site_comm_staging (
            TRANSACTION_CREATION_DATE_TIME, AGREEMENT_NUMBER, TRANS_TYPE_CODE, TRANS_TYPE_DESC, THIRD_PARTY_ID_CODE, TRADING_NAME, DEBIT_VALUE, CREDIT_VALUE, ADVANCE, CUSTOMER_NAME, REGISTRATION_PLATE, COMPONENT_LIVE_DATE, PRODUCT, CREDIT_PERCENT
        )
        FROM @reporting.details.REPORTS
        PATTERN = 'b.m.w commission statement site comm.*\\.csv'
        FILE_FORMAT = (
            TYPE = 'CSV',
            SKIP_HEADER = 1,
            FIELD_DELIMITER = ',',
            FIELD_OPTIONALLY_ENCLOSED_BY = '"',
            TRIM_SPACE = TRUE
        )
        ON_ERROR = 'ABORT_STATEMENT';
    `;
    var stmt_copy = snowflake.createStatement({sqlText: copy_command});
    stmt_copy.execute();

    // Get row count before insert
    var count_before_sql = `SELECT COUNT(*) FROM reporting.details.bmw_commission_site_comm;`;
    var stmt_count_before = snowflake.createStatement({sqlText: count_before_sql});
    var rs_before = stmt_count_before.execute();
    rs_before.next();
    var count_before = rs_before.getColumnValue(1);

    // Insert only new rows into main table (exclude by AGREEMENT_NUMBER+TRANS_TYPE_CODE+THIRD_PARTY_ID_CODE)
    var insert_command = `
            INSERT INTO reporting.details.bmw_commission_site_comm (
                TRANSACTION_CREATION_DATE_TIME, AGREEMENT_NUMBER, TRANS_TYPE_CODE, TRANS_TYPE_DESC, THIRD_PARTY_ID_CODE, TRADING_NAME, DEBIT_VALUE, CREDIT_VALUE, ADVANCE, CUSTOMER_NAME, REGISTRATION_PLATE, COMPONENT_LIVE_DATE, PRODUCT, CREDIT_PERCENT
            )
            SELECT
                TRANSACTION_CREATION_DATE_TIME, AGREEMENT_NUMBER, TRANS_TYPE_CODE, TRANS_TYPE_DESC, THIRD_PARTY_ID_CODE, TRADING_NAME, DEBIT_VALUE, CREDIT_VALUE, ADVANCE, CUSTOMER_NAME, REGISTRATION_PLATE, COMPONENT_LIVE_DATE, PRODUCT, CREDIT_PERCENT
            FROM temp_bmw_commission_site_comm_staging s
            WHERE NOT EXISTS (
                SELECT 1 FROM reporting.details.bmw_commission_site_comm t 
                WHERE t.AGREEMENT_NUMBER = s.AGREEMENT_NUMBER AND t.TRANS_TYPE_CODE = s.TRANS_TYPE_CODE AND t.THIRD_PARTY_ID_CODE = s.THIRD_PARTY_ID_CODE
            );
    `;
    var stmt_insert = snowflake.createStatement({sqlText: insert_command});
    stmt_insert.execute();

    // Get row count after insert
    var count_after_sql = `SELECT COUNT(*) FROM reporting.details.bmw_commission_site_comm;`;
    var stmt_count_after = snowflake.createStatement({sqlText: count_after_sql});
    var rs_after = stmt_count_after.execute();
    rs_after.next();
    var count_after = rs_after.getColumnValue(1);

    var rows_added = count_after - count_before;

    // Drop staging table
    var drop_command = `DROP TABLE IF EXISTS temp_bmw_commission_site_comm_staging;`;
    var stmt_drop = snowflake.createStatement({sqlText: drop_command});
    stmt_drop.execute();

    // Notify
    var subject, msg, returnMsg;
    if (rows_added > 0) {
        subject = 'BMW Commission Site Comm: Rows Loaded';
        msg = `SUCCESS: Loaded ${rows_added} new row(s) into bmw_commission_site_comm from @reporting.details.REPORTS.`;
        returnMsg = msg;
    } else {
        subject = 'BMW Commission Site Comm: No New Rows';
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
            'BMW Commission Site Comm Load Failed',
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
    return 'ERROR in sp_load_bmw_commission_site_comm: ' + err.message;
}
$$;
