
CREATE OR REPLACE PROCEDURE reporting.details.sp_load_big_paid_2025_ytd()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
/*
  Enhanced to provide success/error messaging and email notification, following RAW.CRM.sp_load_infinity_download pattern.
*/
try {
    // Drop temp table if it exists
    var drop_stage_sql = `DROP TABLE IF EXISTS temp_claims_import_staging;`;
    var stmt_drop_stage = snowflake.createStatement({sqlText: drop_stage_sql});
    stmt_drop_stage.execute();

    // Create temp staging table
    var create_stage_sql = `CREATE TEMPORARY TABLE temp_claims_import_staging LIKE reporting.details.CLAIMS_IMPORT;`;
    var stmt_create = snowflake.createStatement({sqlText: create_stage_sql});
    stmt_create.execute();

    // Load data into staging table
    var copy_command = `
        COPY INTO temp_claims_import_staging (
            PAID, IN_DATE, POLICY, CUSTOMER, PUR_DATE, EXP_DATE, MAKE, MODEL, REG_NO,
            STATUS, CATEGORY, CODE, FAULT, CAT_2, CODE_2, FLT2, CAT_3, CODE_3, FLT3,
            FLT_DATE, ASSESSOR, QT_PARTS, QT_LABOUR, AU_PARTS, AU_LABOUR,
            ASSESSOR_DUPLICATE, VAT, NETT, GROSS, EXCESS, REJECTION_SAVING, CONT_PCT,
            CHQ_DATE, NAME, MONTHS, "TYPE", LIM, CLAIM_NO, SALE_MILEAGE, CLAIM_MILEAGE,
            AUTH_BY, CONTACT, AUTH_TOTAL, COSTS_QUOTED, APPROVAL_EMAIL, FIRST_REG,
            ACTUAL_PURCHASE
        )
        FROM @reporting.details.REPORTS
        PATTERN = '.*\\.csv'
        FILE_FORMAT = (
            TYPE = 'CSV',
            SKIP_HEADER = 1,
            FIELD_DELIMITER = ',',
            FIELD_OPTIONALLY_ENCLOSED_BY = '\"',
            TRIM_SPACE = FALSE,
            DATE_FORMAT = 'AUTO',
            TIME_FORMAT = 'AUTO',
            TIMESTAMP_FORMAT = 'AUTO'
        )
        ON_ERROR = 'ABORT_STATEMENT';
    `;
    var stmt_copy = snowflake.createStatement({sqlText: copy_command});
    stmt_copy.execute();

    // Insert only new rows into the main table (exclude duplicates by CLAIM_NO)
    var insert_command = `
        INSERT INTO reporting.details.CLAIMS_IMPORT (
            PAID, IN_DATE, POLICY, CUSTOMER, PUR_DATE, EXP_DATE, MAKE, MODEL, REG_NO,
            STATUS, CATEGORY, CODE, FAULT, CAT_2, CODE_2, FLT2, CAT_3, CODE_3, FLT3,
            FLT_DATE, ASSESSOR, QT_PARTS, QT_LABOUR, AU_PARTS, AU_LABOUR,
            ASSESSOR_DUPLICATE, VAT, NETT, GROSS, EXCESS, REJECTION_SAVING, CONT_PCT,
            CHQ_DATE, NAME, MONTHS, "TYPE", LIM, CLAIM_NO, SALE_MILEAGE, CLAIM_MILEAGE,
            AUTH_BY, CONTACT, AUTH_TOTAL, COSTS_QUOTED, APPROVAL_EMAIL, FIRST_REG,
            ACTUAL_PURCHASE
        )
        SELECT
            PAID, IN_DATE, POLICY, CUSTOMER, PUR_DATE, EXP_DATE, MAKE, MODEL, REG_NO,
            STATUS, CATEGORY, CODE, FAULT, CAT_2, CODE_2, FLT2, CAT_3, CODE_3, FLT3,
            FLT_DATE, ASSESSOR, QT_PARTS, QT_LABOUR, AU_PARTS, AU_LABOUR,
            ASSESSOR_DUPLICATE, VAT, NETT, GROSS, EXCESS, REJECTION_SAVING, CONT_PCT,
            CHQ_DATE, NAME, MONTHS, "TYPE", LIM, CLAIM_NO, SALE_MILEAGE, CLAIM_MILEAGE,
            AUTH_BY, CONTACT, AUTH_TOTAL, COSTS_QUOTED, APPROVAL_EMAIL, FIRST_REG,
            ACTUAL_PURCHASE
        FROM temp_claims_import_staging s
        WHERE NOT EXISTS (
            SELECT 1 FROM reporting.details.CLAIMS_IMPORT t WHERE t.CLAIM_NO = s.CLAIM_NO
        );
    `;

    // Get row count before insert
    var count_before_sql = `SELECT COUNT(*) FROM reporting.details.CLAIMS_IMPORT;`;
    var stmt_count_before = snowflake.createStatement({sqlText: count_before_sql});
    var rs_before = stmt_count_before.execute();
    rs_before.next();
    var count_before = rs_before.getColumnValue(1);

    var stmt_insert = snowflake.createStatement({sqlText: insert_command});
    stmt_insert.execute();

    // Get row count after insert
    var count_after_sql = `SELECT COUNT(*) FROM reporting.details.CLAIMS_IMPORT;`;
    var stmt_count_after = snowflake.createStatement({sqlText: count_after_sql});
    var rs_after = stmt_count_after.execute();
    rs_after.next();
    var count_after = rs_after.getColumnValue(1);

    var rows_added = count_after - count_before;

    // Drop the temp staging table
    var drop_command = `DROP TABLE IF EXISTS temp_claims_import_staging;`;
    var stmt_drop = snowflake.createStatement({sqlText: drop_command});
    stmt_drop.execute();

    // Send success email notification
    var subject, body, returnMsg;
    if (rows_added > 0) {
        subject = 'Big Paid 2025 YTD Load: Rows Loaded';
        body = `SUCCESS: Loaded ${rows_added} new row(s) into Snowflake table reporting.details.CLAIMS_IMPORT from stage @reporting.details.REPORTS.`;
        returnMsg = 'SUCCESS: Loaded ' + rows_added + ' new row(s) into Snowflake table reporting.details.CLAIMS_IMPORT from stage @reporting.details.REPORTS.';
    } else {
        subject = 'Big Paid 2025 YTD Load: No New Rows';
        body = 'Load successfully ran: No new rows were loaded into reporting.details.CLAIMS_IMPORT. No duplicates were found.';
        returnMsg = 'INFO: No new rows loaded.';
    }
    var success_email_command = `call system$send_email(
        'DATA_ALERTS_EMAIL_INT',
        'Lindian.thomas@bigmotoringworld.co.uk,stuart.saunders@bigmotoringworld.co.uk',
        '${subject}',
        '${body}'
    );`;
    var stmt_success_email = snowflake.createStatement({sqlText: success_email_command});
    stmt_success_email.execute();

     return returnMsg;
} catch (err) {
    // Attempt to send error email
    try {
        var email_command = `call system$send_email(
            'DATA_ALERTS_EMAIL_INT',
            'Lindian.thomas@bigmotoringworld.co.uk,stuart.saunders@bigmotoringworld.co.uk',
            'Big Paid 2025 YTD Load Failed',
            'Error message: ${err.message}'
        );`;
        var stmt_email = snowflake.createStatement({sqlText: email_command});
        stmt_email.execute();

        // Ensure both recipients are allowed in the notification integration
        var allow_recipients_command = `ALTER NOTIFICATION INTEGRATION DATA_ALERTS_EMAIL_INT SET ALLOWED_RECIPIENTS = ('lindian.thomas@bigmotoringworld.co.uk', 'stuart.saunders@bigmotoringworld.co.uk');`;
        var stmt_allow_recipients = snowflake.createStatement({sqlText: allow_recipients_command});
        stmt_allow_recipients.execute();
    } catch (emailErr) {
        // If email fails, ignore
    }
    return 'ERROR in sp_load_big_paid_2025_ytd: ' + err.message;
}
$$;
 