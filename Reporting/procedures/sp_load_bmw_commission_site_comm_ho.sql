use role accountadmin;


CREATE OR REPLACE PROCEDURE reporting.details.sp_load_bmw_commission_site_comm_HO()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
/*
    Loads BMW commission site comm from stage @reporting.details.REPORTS into reporting.details.bmw_commission_site_comm_ho (all VARCHAR).
  Expects .csv file with columns:
    Transaction Creation Date Time, Agreement Number, Trans Type Code, Trans Type Desc, Third Party Id Code, Trading Name, Debit Value, Credt Value, Advance, Customer Name, Registration Plate, Component Live Date, Product, Credit %
*/
try {
        // Get row count before insert
        var count_before_sql = `SELECT COUNT(*) FROM reporting.details.bmw_commission_site_comm_ho;`;
        var stmt_count_before = snowflake.createStatement({sqlText: count_before_sql});
        var rs_before = stmt_count_before.execute();
        rs_before.next();
        var count_before = rs_before.getColumnValue(1);

    // Drop temp table if it exists
    var drop_stage_sql = `DROP TABLE IF EXISTS temp_bmw_commission_site_comm_staging;`;
    var stmt_drop_stage = snowflake.createStatement({sqlText: drop_stage_sql});
    stmt_drop_stage.execute();

    // Create temp staging table
    var create_stage_sql = `
        CREATE TEMPORARY TABLE temp_bmw_commission_site_comm_staging (
            "Transaction Creation Date Time" VARCHAR,
            "Agreement Number" VARCHAR,
            "Trans Type Code" VARCHAR,
            "Trans Type Desc" VARCHAR,
            "Third Party Id Code" VARCHAR,
            "Trading Name" VARCHAR,
            "Debit Value" VARCHAR,
            "Credt Value" VARCHAR,
            "Advance" VARCHAR,
            "Customer Name" VARCHAR,
            "Registration Plate" VARCHAR,
            "Component Live Date" VARCHAR,
            "Product" VARCHAR,
            "Credit %" VARCHAR
        );
    `;
    var stmt_create = snowflake.createStatement({sqlText: create_stage_sql});
    stmt_create.execute();

    // Load data into staging table
    var copy_command = `
        COPY INTO temp_bmw_commission_site_comm_staging (
            "Transaction Creation Date Time", "Agreement Number", "Trans Type Code", "Trans Type Desc", "Third Party Id Code", "Trading Name", "Debit Value", "Credt Value", "Advance", "Customer Name", "Registration Plate", "Component Live Date", "Product", "Credit %"
        )
        FROM @reporting.details.REPORTS
        PATTERN = '.*b\.m\.w commission statement reconciliation  comm ho\.csv'
        FILE_FORMAT = (
            TYPE = 'CSV',
            SKIP_HEADER = 1,
            FIELD_DELIMITER = '\t',
            TRIM_SPACE = TRUE
        )
        ON_ERROR = 'ABORT_STATEMENT';
    `;
    var stmt_copy = snowflake.createStatement({sqlText: copy_command});
    stmt_copy.execute();

    // Diagnostic: Check if temp table is loaded
    var check_temp_sql = `SELECT COUNT(*) FROM temp_bmw_commission_site_comm_staging;`;
    var stmt_check_temp = snowflake.createStatement({sqlText: check_temp_sql});
    var rs_check_temp = stmt_check_temp.execute();
    rs_check_temp.next();
    var temp_count = rs_check_temp.getColumnValue(1);
    if (temp_count == 0) {
        return 'ERROR: No rows loaded into temp_bmw_commission_site_comm_staging. Check if the file exists, matches the pattern, and is not empty.';
    }

    // Store all staged data in a permanent audit table
    var create_perm_sql = `
        CREATE TABLE IF NOT EXISTS reporting.details.bmw_commission_site_comm_ho_stage_audit (
            "Transaction Creation Date Time" VARCHAR,
            "Agreement Number" VARCHAR,
            "Trans Type Code" VARCHAR,
            "Trans Type Desc" VARCHAR,
            "Third Party Id Code" VARCHAR,
            "Trading Name" VARCHAR,
            "Debit Value" VARCHAR,
            "Credt Value" VARCHAR,
            "Advance" VARCHAR,
            "Customer Name" VARCHAR,
            "Registration Plate" VARCHAR,
            "Component Live Date" VARCHAR,
            "Product" VARCHAR,
            "Credit %" VARCHAR
        );
    `;
    var stmt_create_perm = snowflake.createStatement({sqlText: create_perm_sql});
    stmt_create_perm.execute();

    var insert_perm_sql = `
        INSERT INTO reporting.details.bmw_commission_site_comm_ho_stage_audit (
            "Transaction Creation Date Time", "Agreement Number", "Trans Type Code", "Trans Type Desc", "Third Party Id Code", "Trading Name", "Debit Value", "Credt Value", "Advance", "Customer Name", "Registration Plate", "Component Live Date", "Product", "Credit %"
        )
        SELECT
            "Transaction Creation Date Time", "Agreement Number", "Trans Type Code", "Trans Type Desc", "Third Party Id Code", "Trading Name", "Debit Value", "Credt Value", "Advance", "Customer Name", "Registration Plate", "Component Live Date", "Product", "Credit %"
        FROM temp_bmw_commission_site_comm_staging;
    `;
    var stmt_insert_perm = snowflake.createStatement({sqlText: insert_perm_sql});
    stmt_insert_perm.execute();

    // Insert only new rows into main table (exclude by AGREEMENT_NUMBER, TRANS_TYPE_CODE, THIRD_PARTY_ID_CODE)
    var insert_command = `
        INSERT INTO reporting.details.bmw_commission_site_comm_ho (
            "Transaction Creation Date Time", "Agreement Number", "Trans Type Code", "Trans Type Desc", "Third Party Id Code", "Trading Name", "Debit Value", "Credt Value", "Advance", "Customer Name", "Registration Plate", "Component Live Date", "Product", "Credit %"
        )
        SELECT
            staging."Transaction Creation Date Time", staging."Agreement Number", staging."Trans Type Code", staging."Trans Type Desc", staging."Third Party Id Code", staging."Trading Name", staging."Debit Value", staging."Credt Value", staging."Advance", staging."Customer Name", staging."Registration Plate", staging."Component Live Date", staging."Product", staging."Credit %"
        FROM temp_bmw_commission_site_comm_staging staging
        WHERE NOT EXISTS (
            SELECT 1 FROM reporting.details.bmw_commission_site_comm_ho t 
            WHERE t."Agreement Number" = staging."Agreement Number" AND t."Trans Type Code" = staging."Trans Type Code" AND t."Third Party Id Code" = staging."Third Party Id Code"
        );
    `;
    var stmt_insert = snowflake.createStatement({sqlText: insert_command});
    stmt_insert.execute();

    // Get row count after insert
    var count_after_sql = `SELECT COUNT(*) FROM reporting.details.bmw_commission_site_comm_ho;`;
    var stmt_count_after = snowflake.createStatement({sqlText: count_after_sql});
    var rs_after = stmt_count_after.execute();
    rs_after.next();
    var count_after = rs_after.getColumnValue(1);

    var rows_added = count_after - count_before;

    // Drop staging table
    var drop_command = `DROP TABLE IF EXISTS temp_bmw_commission_site_comm_staging;`;
    var stmt_drop = snowflake.createStatement({sqlText: drop_command});
    stmt_drop.execute();

    return `SUCCESS: Loaded ${rows_added} new row(s) into reporting.details.bmw_commission_site_comm_ho.`;
} catch (err) {
    return 'ERROR in sp_load_bmw_commission_site_comm_HO: ' + err.message;
}
$$;
