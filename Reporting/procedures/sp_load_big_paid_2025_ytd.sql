

CREATE OR REPLACE PROCEDURE reporting.details.sp_load_big_paid_2025_ytd()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
/*
Procedure: reporting.details.sp_load_big_paid_2025_ytd
Author: Lindian Thomas
Date: January 2026

Description:
Loads and upserts claim data from a staged CSV file into the CLAIMS_IMPORT table.
- Inserts new claims (by CLAIM_NO)
- Updates existing claims only if any column value has changed
- Sends email notifications on success or error, including counts of inserted and updated rows
*/
try {
    //  before merge
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
        PATTERN = '.*big paid 2025 ytd\\.csv'
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


    // Upsert (merge) rows from staging into the main table based on CLAIM_NO
    var merge_command = `
        MERGE INTO reporting.details.CLAIMS_IMPORT t
        USING temp_claims_import_staging s
        ON t.CLAIM_NO = s.CLAIM_NO
        WHEN MATCHED AND (
            t.PAID IS DISTINCT FROM s.PAID OR
            t.IN_DATE IS DISTINCT FROM s.IN_DATE OR
            t.POLICY IS DISTINCT FROM s.POLICY OR
            t.CUSTOMER IS DISTINCT FROM s.CUSTOMER OR
            t.PUR_DATE IS DISTINCT FROM s.PUR_DATE OR
            t.EXP_DATE IS DISTINCT FROM s.EXP_DATE OR
            t.MAKE IS DISTINCT FROM s.MAKE OR
            t.MODEL IS DISTINCT FROM s.MODEL OR
            t.REG_NO IS DISTINCT FROM s.REG_NO OR
            t.STATUS IS DISTINCT FROM s.STATUS OR
            t.CATEGORY IS DISTINCT FROM s.CATEGORY OR
            t.CODE IS DISTINCT FROM s.CODE OR
            t.FAULT IS DISTINCT FROM s.FAULT OR
            t.CAT_2 IS DISTINCT FROM s.CAT_2 OR
            t.CODE_2 IS DISTINCT FROM s.CODE_2 OR
            t.FLT2 IS DISTINCT FROM s.FLT2 OR
            t.CAT_3 IS DISTINCT FROM s.CAT_3 OR
            t.CODE_3 IS DISTINCT FROM s.CODE_3 OR
            t.FLT3 IS DISTINCT FROM s.FLT3 OR
            t.FLT_DATE IS DISTINCT FROM s.FLT_DATE OR
            t.ASSESSOR IS DISTINCT FROM s.ASSESSOR OR
            t.QT_PARTS IS DISTINCT FROM s.QT_PARTS OR
            t.QT_LABOUR IS DISTINCT FROM s.QT_LABOUR OR
            t.AU_PARTS IS DISTINCT FROM s.AU_PARTS OR
            t.AU_LABOUR IS DISTINCT FROM s.AU_LABOUR OR
            t.ASSESSOR_DUPLICATE IS DISTINCT FROM s.ASSESSOR_DUPLICATE OR
            t.VAT IS DISTINCT FROM s.VAT OR
            t.NETT IS DISTINCT FROM s.NETT OR
            t.GROSS IS DISTINCT FROM s.GROSS OR
            t.EXCESS IS DISTINCT FROM s.EXCESS OR
            t.REJECTION_SAVING IS DISTINCT FROM s.REJECTION_SAVING OR
            t.CONT_PCT IS DISTINCT FROM s.CONT_PCT OR
            t.CHQ_DATE IS DISTINCT FROM s.CHQ_DATE OR
            t.NAME IS DISTINCT FROM s.NAME OR
            t.MONTHS IS DISTINCT FROM s.MONTHS OR
            t."TYPE" IS DISTINCT FROM s."TYPE" OR
            t.LIM IS DISTINCT FROM s.LIM OR
            t.SALE_MILEAGE IS DISTINCT FROM s.SALE_MILEAGE OR
            t.CLAIM_MILEAGE IS DISTINCT FROM s.CLAIM_MILEAGE OR
            t.AUTH_BY IS DISTINCT FROM s.AUTH_BY OR
            t.CONTACT IS DISTINCT FROM s.CONTACT OR
            t.AUTH_TOTAL IS DISTINCT FROM s.AUTH_TOTAL OR
            t.COSTS_QUOTED IS DISTINCT FROM s.COSTS_QUOTED OR
            t.APPROVAL_EMAIL IS DISTINCT FROM s.APPROVAL_EMAIL OR
            t.FIRST_REG IS DISTINCT FROM s.FIRST_REG OR
            t.ACTUAL_PURCHASE IS DISTINCT FROM s.ACTUAL_PURCHASE
        ) THEN UPDATE SET
            t.PAID = s.PAID,
            t.IN_DATE = s.IN_DATE,
            t.POLICY = s.POLICY,
            t.CUSTOMER = s.CUSTOMER,
            t.PUR_DATE = s.PUR_DATE,
            t.EXP_DATE = s.EXP_DATE,
            t.MAKE = s.MAKE,
            t.MODEL = s.MODEL,
            t.REG_NO = s.REG_NO,
            t.STATUS = s.STATUS,
            t.CATEGORY = s.CATEGORY,
            t.CODE = s.CODE,
            t.FAULT = s.FAULT,
            t.CAT_2 = s.CAT_2,
            t.CODE_2 = s.CODE_2,
            t.FLT2 = s.FLT2,
            t.CAT_3 = s.CAT_3,
            t.CODE_3 = s.CODE_3,
            t.FLT3 = s.FLT3,
            t.FLT_DATE = s.FLT_DATE,
            t.ASSESSOR = s.ASSESSOR,
            t.QT_PARTS = s.QT_PARTS,
            t.QT_LABOUR = s.QT_LABOUR,
            t.AU_PARTS = s.AU_PARTS,
            t.AU_LABOUR = s.AU_LABOUR,
            t.ASSESSOR_DUPLICATE = s.ASSESSOR_DUPLICATE,
            t.VAT = s.VAT,
            t.NETT = s.NETT,
            t.GROSS = s.GROSS,
            t.EXCESS = s.EXCESS,
            t.REJECTION_SAVING = s.REJECTION_SAVING,
            t.CONT_PCT = s.CONT_PCT,
            t.CHQ_DATE = s.CHQ_DATE,
            t.NAME = s.NAME,
            t.MONTHS = s.MONTHS,
            t."TYPE" = s."TYPE",
            t.LIM = s.LIM,
            t.SALE_MILEAGE = s.SALE_MILEAGE,
            t.CLAIM_MILEAGE = s.CLAIM_MILEAGE,
            t.AUTH_BY = s.AUTH_BY,
            t.CONTACT = s.CONTACT,
            t.AUTH_TOTAL = s.AUTH_TOTAL,
            t.COSTS_QUOTED = s.COSTS_QUOTED,
            t.APPROVAL_EMAIL = s.APPROVAL_EMAIL,
            t.FIRST_REG = s.FIRST_REG,
            t.ACTUAL_PURCHASE = s.ACTUAL_PURCHASE
        WHEN NOT MATCHED THEN INSERT (
            PAID, IN_DATE, POLICY, CUSTOMER, PUR_DATE, EXP_DATE, MAKE, MODEL, REG_NO,
            STATUS, CATEGORY, CODE, FAULT, CAT_2, CODE_2, FLT2, CAT_3, CODE_3, FLT3,
            FLT_DATE, ASSESSOR, QT_PARTS, QT_LABOUR, AU_PARTS, AU_LABOUR,
            ASSESSOR_DUPLICATE, VAT, NETT, GROSS, EXCESS, REJECTION_SAVING, CONT_PCT,
            CHQ_DATE, NAME, MONTHS, "TYPE", LIM, CLAIM_NO, SALE_MILEAGE, CLAIM_MILEAGE,
            AUTH_BY, CONTACT, AUTH_TOTAL, COSTS_QUOTED, APPROVAL_EMAIL, FIRST_REG,
            ACTUAL_PURCHASE
        ) VALUES (
            s.PAID, s.IN_DATE, s.POLICY, s.CUSTOMER, s.PUR_DATE, s.EXP_DATE, s.MAKE, s.MODEL, s.REG_NO,
            s.STATUS, s.CATEGORY, s.CODE, s.FAULT, s.CAT_2, s.CODE_2, s.FLT2, s.CAT_3, s.CODE_3, s.FLT3,
            s.FLT_DATE, s.ASSESSOR, s.QT_PARTS, s.QT_LABOUR, s.AU_PARTS, s.AU_LABOUR,
            s.ASSESSOR_DUPLICATE, s.VAT, s.NETT, s.GROSS, s.EXCESS, s.REJECTION_SAVING, s.CONT_PCT,
            s.CHQ_DATE, s.NAME, s.MONTHS, s."TYPE", s.LIM, s.CLAIM_NO, s.SALE_MILEAGE, s.CLAIM_MILEAGE,
            s.AUTH_BY, s.CONTACT, s.AUTH_TOTAL, s.COSTS_QUOTED, s.APPROVAL_EMAIL, s.FIRST_REG,
            s.ACTUAL_PURCHASE
        );
    `;

    // Count new inserts (CLAIM_NO in staging not in target)
    var count_inserts_sql = `
        SELECT COUNT(*) FROM temp_claims_import_staging s
        LEFT JOIN reporting.details.CLAIMS_IMPORT t ON t.CLAIM_NO = s.CLAIM_NO
        WHERE t.CLAIM_NO IS NULL
    `;
    var stmt_count_inserts = snowflake.createStatement({sqlText: count_inserts_sql});
    var rs_count_inserts = stmt_count_inserts.execute();
    rs_count_inserts.next();
    var num_inserts = rs_count_inserts.getColumnValue(1);

    // Count real updates (CLAIM_NO exists and at least one column is different)
    var count_updates_sql = `
        SELECT COUNT(*) FROM temp_claims_import_staging s
        JOIN reporting.details.CLAIMS_IMPORT t ON t.CLAIM_NO = s.CLAIM_NO
        WHERE
            t.PAID IS DISTINCT FROM s.PAID OR
            t.IN_DATE IS DISTINCT FROM s.IN_DATE OR
            t.POLICY IS DISTINCT FROM s.POLICY OR
            t.CUSTOMER IS DISTINCT FROM s.CUSTOMER OR
            t.PUR_DATE IS DISTINCT FROM s.PUR_DATE OR
            t.EXP_DATE IS DISTINCT FROM s.EXP_DATE OR
            t.MAKE IS DISTINCT FROM s.MAKE OR
            t.MODEL IS DISTINCT FROM s.MODEL OR
            t.REG_NO IS DISTINCT FROM s.REG_NO OR
            t.STATUS IS DISTINCT FROM s.STATUS OR
            t.CATEGORY IS DISTINCT FROM s.CATEGORY OR
            t.CODE IS DISTINCT FROM s.CODE OR
            t.FAULT IS DISTINCT FROM s.FAULT OR
            t.CAT_2 IS DISTINCT FROM s.CAT_2 OR
            t.CODE_2 IS DISTINCT FROM s.CODE_2 OR
            t.FLT2 IS DISTINCT FROM s.FLT2 OR
            t.CAT_3 IS DISTINCT FROM s.CAT_3 OR
            t.CODE_3 IS DISTINCT FROM s.CODE_3 OR
            t.FLT3 IS DISTINCT FROM s.FLT3 OR
            t.FLT_DATE IS DISTINCT FROM s.FLT_DATE OR
            t.ASSESSOR IS DISTINCT FROM s.ASSESSOR OR
            t.QT_PARTS IS DISTINCT FROM s.QT_PARTS OR
            t.QT_LABOUR IS DISTINCT FROM s.QT_LABOUR OR
            t.AU_PARTS IS DISTINCT FROM s.AU_PARTS OR
            t.AU_LABOUR IS DISTINCT FROM s.AU_LABOUR OR
            t.ASSESSOR_DUPLICATE IS DISTINCT FROM s.ASSESSOR_DUPLICATE OR
            t.VAT IS DISTINCT FROM s.VAT OR
            t.NETT IS DISTINCT FROM s.NETT OR
            t.GROSS IS DISTINCT FROM s.GROSS OR
            t.EXCESS IS DISTINCT FROM s.EXCESS OR
            t.REJECTION_SAVING IS DISTINCT FROM s.REJECTION_SAVING OR
            t.CONT_PCT IS DISTINCT FROM s.CONT_PCT OR
            t.CHQ_DATE IS DISTINCT FROM s.CHQ_DATE OR
            t.NAME IS DISTINCT FROM s.NAME OR
            t.MONTHS IS DISTINCT FROM s.MONTHS OR
            t."TYPE" IS DISTINCT FROM s."TYPE" OR
            t.LIM IS DISTINCT FROM s.LIM OR
            t.SALE_MILEAGE IS DISTINCT FROM s.SALE_MILEAGE OR
            t.CLAIM_MILEAGE IS DISTINCT FROM s.CLAIM_MILEAGE OR
            t.AUTH_BY IS DISTINCT FROM s.AUTH_BY OR
            t.CONTACT IS DISTINCT FROM s.CONTACT OR
            t.AUTH_TOTAL IS DISTINCT FROM s.AUTH_TOTAL OR
            t.COSTS_QUOTED IS DISTINCT FROM s.COSTS_QUOTED OR
            t.APPROVAL_EMAIL IS DISTINCT FROM s.APPROVAL_EMAIL OR
            t.FIRST_REG IS DISTINCT FROM s.FIRST_REG OR
            t.ACTUAL_PURCHASE IS DISTINCT FROM s.ACTUAL_PURCHASE
    `;
    var stmt_count_updates = snowflake.createStatement({sqlText: count_updates_sql});
    var rs_count_updates = stmt_count_updates.execute();
    rs_count_updates.next();
    var num_updates = rs_count_updates.getColumnValue(1);

    // Now run the MERGE
    var stmt_merge = snowflake.createStatement({sqlText: merge_command});
    stmt_merge.execute();

    // Drop the temp staging table
    var drop_command = `DROP TABLE IF EXISTS temp_claims_import_staging;`;
    var stmt_drop = snowflake.createStatement({sqlText: drop_command});
    stmt_drop.execute();

    // Send success email notification
    var subject, body, returnMsg;
    if (num_inserts > 0 || num_updates > 0) {
        subject = 'Big Paid 2025 YTD Load: Rows Loaded or Updated';
        body = `SUCCESS: ${num_inserts} row(s) inserted, ${num_updates} row(s) updated in reporting.details.CLAIMS_IMPORT from stage @reporting.details.REPORTS.`;
        returnMsg = 'SUCCESS: ' + num_inserts + ' row(s) inserted, ' + num_updates + ' row(s) updated in reporting.details.CLAIMS_IMPORT from stage @reporting.details.REPORTS.';
    } else {
        subject = 'Big Paid 2025 YTD Load: No Changes';
        body = 'Load successfully ran: No rows were inserted or updated in reporting.details.CLAIMS_IMPORT. The data is up to date.';
        returnMsg = 'INFO: No rows inserted or updated. The data is up to date.';
    }
    var success_email_command = `call system$send_email(
        'DATA_ALERTS_EMAIL_INT',
        'Lindian.thomas@bigmotoringworld.co.uk,stuart.saunders@bigmotoringworld.co.uk',
        '${subject}',
        '${body}'
    );`;
    var stmt_success_email = snowflake.createStatement({sqlText: success_email_command});
    stmt_success_email.execute();

    // Ensure both recipients are allowed in the notification integration
    var allow_recipients_command = `ALTER NOTIFICATION INTEGRATION DATA_ALERTS_EMAIL_INT SET ALLOWED_RECIPIENTS = ('lindian.thomas@bigmotoringworld.co.uk', 'stuart.saunders@bigmotoringworld.co.uk');`;
    var stmt_allow_recipients = snowflake.createStatement({sqlText: allow_recipients_command});
    stmt_allow_recipients.execute();

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
 