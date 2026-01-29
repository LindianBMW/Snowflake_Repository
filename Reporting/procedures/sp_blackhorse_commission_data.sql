CREATE OR REPLACE PROCEDURE reporting.details.sp_blackhorse_commission_data()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
/*
Procedure: reporting.details.sp_blackhorse_commission_data
Author: Lindian Thomas
Date: January 2026

Description:
Loads and upserts Blackhorse commission data from a staged CSV file into the BLACKHORSE_COMMISSION_DATA table.
- Inserts new rows (by TransactionId)
- Updates existing rows only if any column value has changed
- Sends email notifications on success or error, including counts of inserted and updated rows
- Designed for robust, idempotent data loads from external sources
*/
try {
    // Drop temp table if it exists
    var drop_stage_sql = `DROP TABLE IF EXISTS temp_blackhorse_commission_staging;`;
    var stmt_drop_stage = snowflake.createStatement({sqlText: drop_stage_sql});
    stmt_drop_stage.execute();

    // Create temp staging table
    var create_stage_sql = `
        CREATE TEMPORARY TABLE temp_blackhorse_commission_staging (
            Indicator VARCHAR,
            TransactionId VARCHAR,
            Date VARCHAR,
            Reference VARCHAR,
            Tooltip VARCHAR,
            Agreement VARCHAR,
            Customer VARCHAR,
            RegNo VARCHAR,
            Amount VARCHAR,
            VATRate VARCHAR,
            VATAmount VARCHAR,
            Total VARCHAR,
            DealerNumber VARCHAR,
            Vehicle VARCHAR,
            YearOfRegistration VARCHAR,
            BalanceFinanced VARCHAR,
            CustomerRate VARCHAR,
            Period VARCHAR,
            OriginalCommission VARCHAR,
            Scheme VARCHAR,
            DateIncepted VARCHAR,
            DateFunded VARCHAR,
            ActualPayments VARCHAR,
            DebitBackDesc VARCHAR,
            DebitEffectiveDate VARCHAR,
            RuleDescription VARCHAR,
            RuleAmendment VARCHAR,
            ProtectionRule VARCHAR,
            RuleEffectiveDate VARCHAR,
            RuleExpiryDate VARCHAR
        );
    `;
    var stmt_create = snowflake.createStatement({sqlText: create_stage_sql});
    stmt_create.execute();

    // Load data into staging table
    var copy_command = `
        COPY INTO temp_blackhorse_commission_staging
        FROM @reporting.details.REPORTS
        PATTERN = '.*blackhorse commission data\\.csv'
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

    // Count new inserts (TransactionId in staging not in target)
    var count_inserts_sql = `
        SELECT COUNT(*) FROM temp_blackhorse_commission_staging s
        LEFT JOIN reporting.details.BLACKHORSE_COMMISSION_DATA t
        ON t.TransactionId = s.TransactionId
        WHERE t.TransactionId IS NULL
    `;
    var stmt_count_inserts = snowflake.createStatement({sqlText: count_inserts_sql});
    var rs_count_inserts = stmt_count_inserts.execute();
    rs_count_inserts.next();
    var num_inserts = rs_count_inserts.getColumnValue(1);

    // Count real updates (TransactionId exists and at least one column is different)
    var count_updates_sql = `
        SELECT COUNT(*) FROM temp_blackhorse_commission_staging s
        JOIN reporting.details.BLACKHORSE_COMMISSION_DATA t
        ON t.TransactionId = s.TransactionId
        WHERE
            t.Indicator IS DISTINCT FROM s.Indicator OR
            t.Date IS DISTINCT FROM s.Date OR
            t.Reference IS DISTINCT FROM s.Reference OR
            t.Tooltip IS DISTINCT FROM s.Tooltip OR
            t.Agreement IS DISTINCT FROM s.Agreement OR
            t.Customer IS DISTINCT FROM s.Customer OR
            t.RegNo IS DISTINCT FROM s.RegNo OR
            t.Amount IS DISTINCT FROM s.Amount OR
            t.VATRate IS DISTINCT FROM s.VATRate OR
            t.VATAmount IS DISTINCT FROM s.VATAmount OR
            t.Total IS DISTINCT FROM s.Total OR
            t.DealerNumber IS DISTINCT FROM s.DealerNumber OR
            t.Vehicle IS DISTINCT FROM s.Vehicle OR
            t.YearOfRegistration IS DISTINCT FROM s.YearOfRegistration OR
            t.BalanceFinanced IS DISTINCT FROM s.BalanceFinanced OR
            t.CustomerRate IS DISTINCT FROM s.CustomerRate OR
            t.Period IS DISTINCT FROM s.Period OR
            t.OriginalCommission IS DISTINCT FROM s.OriginalCommission OR
            t.Scheme IS DISTINCT FROM s.Scheme OR
            t.DateIncepted IS DISTINCT FROM s.DateIncepted OR
            t.DateFunded IS DISTINCT FROM s.DateFunded OR
            t.ActualPayments IS DISTINCT FROM s.ActualPayments OR
            t.DebitBackDesc IS DISTINCT FROM s.DebitBackDesc OR
            t.DebitEffectiveDate IS DISTINCT FROM s.DebitEffectiveDate OR
            t.RuleDescription IS DISTINCT FROM s.RuleDescription OR
            t.RuleAmendment IS DISTINCT FROM s.RuleAmendment OR
            t.ProtectionRule IS DISTINCT FROM s.ProtectionRule OR
            t.RuleEffectiveDate IS DISTINCT FROM s.RuleEffectiveDate OR
            t.RuleExpiryDate IS DISTINCT FROM s.RuleExpiryDate
    `;
    var stmt_count_updates = snowflake.createStatement({sqlText: count_updates_sql});
    var rs_count_updates = stmt_count_updates.execute();
    rs_count_updates.next();
    var num_updates = rs_count_updates.getColumnValue(1);

    // MERGE (upsert) rows from staging into the main table based on TransactionId
    var merge_command = `
        MERGE INTO reporting.details.BLACKHORSE_COMMISSION_DATA t
        USING temp_blackhorse_commission_staging s
        ON t.TransactionId = s.TransactionId
        WHEN MATCHED AND (
            t.Indicator IS DISTINCT FROM s.Indicator OR
            t.Date IS DISTINCT FROM s.Date OR
            t.Reference IS DISTINCT FROM s.Reference OR
            t.Tooltip IS DISTINCT FROM s.Tooltip OR
            t.Agreement IS DISTINCT FROM s.Agreement OR
            t.Customer IS DISTINCT FROM s.Customer OR
            t.RegNo IS DISTINCT FROM s.RegNo OR
            t.Amount IS DISTINCT FROM s.Amount OR
            t.VATRate IS DISTINCT FROM s.VATRate OR
            t.VATAmount IS DISTINCT FROM s.VATAmount OR
            t.Total IS DISTINCT FROM s.Total OR
            t.DealerNumber IS DISTINCT FROM s.DealerNumber OR
            t.Vehicle IS DISTINCT FROM s.Vehicle OR
            t.YearOfRegistration IS DISTINCT FROM s.YearOfRegistration OR
            t.BalanceFinanced IS DISTINCT FROM s.BalanceFinanced OR
            t.CustomerRate IS DISTINCT FROM s.CustomerRate OR
            t.Period IS DISTINCT FROM s.Period OR
            t.OriginalCommission IS DISTINCT FROM s.OriginalCommission OR
            t.Scheme IS DISTINCT FROM s.Scheme OR
            t.DateIncepted IS DISTINCT FROM s.DateIncepted OR
            t.DateFunded IS DISTINCT FROM s.DateFunded OR
            t.ActualPayments IS DISTINCT FROM s.ActualPayments OR
            t.DebitBackDesc IS DISTINCT FROM s.DebitBackDesc OR
            t.DebitEffectiveDate IS DISTINCT FROM s.DebitEffectiveDate OR
            t.RuleDescription IS DISTINCT FROM s.RuleDescription OR
            t.RuleAmendment IS DISTINCT FROM s.RuleAmendment OR
            t.ProtectionRule IS DISTINCT FROM s.ProtectionRule OR
            t.RuleEffectiveDate IS DISTINCT FROM s.RuleEffectiveDate OR
            t.RuleExpiryDate IS DISTINCT FROM s.RuleExpiryDate
        ) THEN UPDATE SET
            Indicator = s.Indicator,
            Date = s.Date,
            Reference = s.Reference,
            Tooltip = s.Tooltip,
            Agreement = s.Agreement,
            Customer = s.Customer,
            RegNo = s.RegNo,
            Amount = s.Amount,
            VATRate = s.VATRate,
            VATAmount = s.VATAmount,
            Total = s.Total,
            DealerNumber = s.DealerNumber,
            Vehicle = s.Vehicle,
            YearOfRegistration = s.YearOfRegistration,
            BalanceFinanced = s.BalanceFinanced,
            CustomerRate = s.CustomerRate,
            Period = s.Period,
            OriginalCommission = s.OriginalCommission,
            Scheme = s.Scheme,
            DateIncepted = s.DateIncepted,
            DateFunded = s.DateFunded,
            ActualPayments = s.ActualPayments,
            DebitBackDesc = s.DebitBackDesc,
            DebitEffectiveDate = s.DebitEffectiveDate,
            RuleDescription = s.RuleDescription,
            RuleAmendment = s.RuleAmendment,
            ProtectionRule = s.ProtectionRule,
            RuleEffectiveDate = s.RuleEffectiveDate,
            RuleExpiryDate = s.RuleExpiryDate
        WHEN NOT MATCHED THEN INSERT (
            Indicator, TransactionId, Date, Reference, Tooltip, Agreement, Customer, RegNo, Amount, VATRate, VATAmount, Total, DealerNumber, Vehicle, YearOfRegistration, BalanceFinanced, CustomerRate, Period, OriginalCommission, Scheme, DateIncepted, DateFunded, ActualPayments, DebitBackDesc, DebitEffectiveDate, RuleDescription, RuleAmendment, ProtectionRule, RuleEffectiveDate, RuleExpiryDate
        ) VALUES (
            s.Indicator, s.TransactionId, s.Date, s.Reference, s.Tooltip, s.Agreement, s.Customer, s.RegNo, s.Amount, s.VATRate, s.VATAmount, s.Total, s.DealerNumber, s.Vehicle, s.YearOfRegistration, s.BalanceFinanced, s.CustomerRate, s.Period, s.OriginalCommission, s.Scheme, s.DateIncepted, s.DateFunded, s.ActualPayments, s.DebitBackDesc, s.DebitEffectiveDate, s.RuleDescription, s.RuleAmendment, s.ProtectionRule, s.RuleEffectiveDate, s.RuleExpiryDate
        );
    `;
    var stmt_merge = snowflake.createStatement({sqlText: merge_command});
    stmt_merge.execute();

    // Drop staging table
    var drop_command = `DROP TABLE IF EXISTS temp_blackhorse_commission_staging;`;
    var stmt_drop = snowflake.createStatement({sqlText: drop_command});
    stmt_drop.execute();

    // Notify
    var subject, body, returnMsg;
    if (num_inserts > 0 || num_updates > 0) {
        subject = 'Blackhorse Commission Data Load: Rows Loaded or Updated';
        body = `SUCCESS: ${num_inserts} row(s) inserted, ${num_updates} row(s) updated in BLACKHORSE_COMMISSION_DATA from @reporting.details.REPORTS.`;
        returnMsg = body;
    } else {
        subject = 'Blackhorse Commission Data Load: No Changes';
        body = 'Load ran successfully: No rows were inserted or updated; data is up to date.';
        returnMsg = body;
    }
    var success_email_command = `call system$send_email(
        'DATA_ALERTS_EMAIL_INT',
        'Lindian.thomas@bigmotoringworld.co.uk,stuart.saunders@bigmotoringworld.co.uk',
        '${subject}',
        '${body}'
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
            'Blackhorse Commission Data Load Failed',
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
    return 'ERROR in sp_blackhorse_commission_data: ' + err.message;
}
$$;