use role accountadmin;

CREATE OR REPLACE PROCEDURE reporting.details.sp_load_bmw_commission_reconciliation()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
/*
    Loads BMW commission reconciliation from stage @reporting.details.REPORTS into reporting.details.BMW_COMMISSION_RECONCILIATION (all VARCHAR).
*/
try {
    // Drop temp table if it exists
    var drop_stage_sql = `DROP TABLE IF EXISTS temp_bmw_commission_reconciliation_staging;`;
    var stmt_drop_stage = snowflake.createStatement({sqlText: drop_stage_sql});
    stmt_drop_stage.execute();

    // Create temp staging table (all VARCHAR)
    var create_stage_sql = `
        CREATE TEMPORARY TABLE temp_bmw_commission_reconciliation_staging (
            CODE VARCHAR,
            SITE_NAME VARCHAR,
            DEBIT VARCHAR,
            CREDIT VARCHAR,
            STATEMENT_TOTAL VARCHAR,
            ACTION_REQUIRED VARCHAR
        );
    `;
    var stmt_create = snowflake.createStatement({sqlText: create_stage_sql});
    stmt_create.execute();

    // Load data into staging table, skip first 5 rows
    var copy_command = `
        COPY INTO temp_bmw_commission_reconciliation_staging (
            CODE, SITE_NAME, DEBIT, CREDIT, STATEMENT_TOTAL, ACTION_REQUIRED
        )
        FROM @reporting.details.REPORTS
        PATTERN = 'b.m.w commission statement reconciliation.*\\.csv'
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
    var count_before_sql = `SELECT COUNT(*) FROM reporting.details.BMW_COMMISSION_RECONCILIATION;`;
    var stmt_count_before = snowflake.createStatement({sqlText: count_before_sql});
    var rs_before = stmt_count_before.execute();
    rs_before.next();
    var count_before = rs_before.getColumnValue(1);

    // Insert only new rows into main table (exclude by CODE+SITE_NAME+STATEMENT_TOTAL)
    var insert_command = `
            INSERT INTO reporting.details.BMW_COMMISSION_RECONCILIATION (
                CODE, SITE_NAME, DEBIT, CREDIT, STATEMENT_TOTAL, ACTION_REQUIRED
            )
            SELECT
                CODE, SITE_NAME, DEBIT, CREDIT, STATEMENT_TOTAL, ACTION_REQUIRED
            FROM temp_bmw_commission_reconciliation_staging s
            WHERE NOT EXISTS (
                SELECT 1 FROM reporting.details.BMW_COMMISSION_RECONCILIATION t 
                WHERE t.CODE = s.CODE AND t.SITE_NAME = s.SITE_NAME AND t.STATEMENT_TOTAL = s.STATEMENT_TOTAL
            );
    `;
    var stmt_insert = snowflake.createStatement({sqlText: insert_command});
    stmt_insert.execute();

    // Get row count after insert
    var count_after_sql = `SELECT COUNT(*) FROM reporting.details.BMW_COMMISSION_RECONCILIATION;`;
    var stmt_count_after = snowflake.createStatement({sqlText: count_after_sql});
    var rs_after = stmt_count_after.execute();
    rs_after.next();
    var count_after = rs_after.getColumnValue(1);

    var rows_added = count_after - count_before;

    // Delete rows where CODE does not follow the format D704667 (D followed by 6 digits)
    var delete_bad_code_sql = `
        DELETE FROM reporting.details.BMW_COMMISSION_RECONCILIATION
        WHERE CODE IS NULL OR CODE NOT RLIKE '^D[0-9]{6}$';
    `;
    var stmt_delete_bad_code = snowflake.createStatement({sqlText: delete_bad_code_sql});
    stmt_delete_bad_code.execute();

    // Drop staging table
    var drop_command = `DROP TABLE IF EXISTS temp_bmw_commission_reconciliation_staging;`;
    var stmt_drop = snowflake.createStatement({sqlText: drop_command});
    stmt_drop.execute();

    // Notify
    var subject, msg, returnMsg;
    if (rows_added > 0) {
        subject = 'BMW Commission Reconciliation: Rows Loaded';
        msg = `SUCCESS: Loaded ${rows_added} new row(s) into BMW_COMMISSION_RECONCILIATION from @reporting.details.REPORTS.`;
        returnMsg = msg;
    } else {
        subject = 'BMW Commission Reconciliation: No New Rows';
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
            'BMW Commission Reconciliation Load Failed',
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
    return 'ERROR in sp_load_bmw_commission_reconciliation: ' + err.message;
}
$$;