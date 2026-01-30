use role accountadmin;

CREATE OR REPLACE PROCEDURE reporting.details.sp_sales_reports_detailed_sales()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
/*
  Loads SALES_REPORTS_DETAILED_SALES from stage @reporting.details.REPORTS into reporting.details.SALES_REPORTS_DETAILED_SALES (all VARCHAR).
  Robust ETL: temp staging, deduplication, audit column, notification.
  Column names: all lowercase, underscores, no spaces.
*/
try {

    // Drop temp table if it exists
    var drop_stage_sql = `DROP TABLE IF EXISTS temp_sales_reports_detailed_sales_staging;`;
    var stmt_drop_stage = snowflake.createStatement({sqlText: drop_stage_sql});
    stmt_drop_stage.execute();

    // Create temp staging table (all VARCHAR, new column names)
    var create_stage_sql = `
        CREATE TEMPORARY TABLE temp_sales_reports_detailed_sales_staging (
            deal_date VARCHAR,
            invoice_date VARCHAR,
            stocknumber VARCHAR,
            reg VARCHAR,
            dateinstock VARCHAR,
            make VARCHAR,
            model VARCHAR,
            mileage VARCHAR,
            fuel VARCHAR,
            transmission VARCHAR,
            colour VARCHAR,
            bodystyle VARCHAR,
            trim VARCHAR,
            description VARCHAR,
            dis VARCHAR,
            dod VARCHAR,
            vatq VARCHAR,
            vin VARCHAR,
            cost VARCHAR,
            date_of_purchase VARCHAR,
            date_of_registration VARCHAR,
            retail_marginvat VARCHAR,
            vat VARCHAR,
            auction_costs VARCHAR,
            extprepfee VARCHAR,
            extsalefee VARCHAR,
            asking_price VARCHAR,
            sales_person VARCHAR,
            location VARCHAR,
            destination VARCHAR,
            buyer VARCHAR,
            recon VARCHAR,
            admin_fee VARCHAR,
            gap_payment_plan VARCHAR,
            gap VARCHAR,
            warranty_12 VARCHAR,
            warranty_24 VARCHAR,
            warranty_36 VARCHAR,
            warranty_taxi_12 VARCHAR,
            warranty_taxi_24 VARCHAR,
            warranty_taxi_36 VARCHAR,
            warranty_ev_12 VARCHAR,
            warranty_ev_24 VARCHAR,
            warranty_ev_36 VARCHAR,
            warranty_ev_taxi_12 VARCHAR,
            warranty_ev_taxi_24 VARCHAR,
            warranty_ev_taxi_36 VARCHAR,
            warrany_term VARCHAR,
            shine_12 VARCHAR,
            shine_24 VARCHAR,
            shine_36 VARCHAR,
            shine_48 VARCHAR,
            graphene VARCHAR,
            tyre_alloy VARCHAR,
            finance_commission VARCHAR,
            total_profit VARCHAR,
            status VARCHAR,
            boughtfrom VARCHAR,
            boughtfromspecific VARCHAR,
            sold_to VARCHAR,
            suplementary_invoices VARCHAR,
            invoice_number VARCHAR,
            siv VARCHAR,
            costs VARCHAR,
            profit_of_extras VARCHAR,
            px_reg VARCHAR,
            px_mileage VARCHAR,
            px_make VARCHAR,
            px_model VARCHAR,
            px_date_of_registration VARCHAR,
            px_siv VARCHAR,
            px2_reg VARCHAR,
            px2_mileage VARCHAR,
            px2_make VARCHAR,
            px2_model VARCHAR,
            px2_date_of_registration VARCHAR,
            px2_siv VARCHAR,
            cap_id VARCHAR,
            latest_cap_clean VARCHAR,
            engine_size VARCHAR,
            customer_postcode VARCHAR,
            chassis_profit VARCHAR,
            admin_fee_retail VARCHAR,
            gap_payment_plan_retail VARCHAR,
            gap_retail VARCHAR,
            warranty_12_retail VARCHAR,
            warranty_24_retail VARCHAR,
            warranty_36_retail VARCHAR,
            warranty_taxi_12_retail VARCHAR,
            warranty_taxi_24_retail VARCHAR,
            warranty_taxi_36_retail VARCHAR,
            warranty_ev_12_retail VARCHAR,
            warranty_ev_24_retail VARCHAR,
            warranty_ev_36_retail VARCHAR,
            warranty_ev_taxi_12_retail VARCHAR,
            warranty_ev_taxi_24_retail VARCHAR,
            warranty_ev_taxi_36_retail VARCHAR,
            shine_12_retail VARCHAR,
            shine_24_retail VARCHAR,
            shine_36_retail VARCHAR,
            shine_48_retail VARCHAR,
            gard_x_retail VARCHAR,
            tyre_alloy_retail VARCHAR,
            warranty_length VARCHAR,
            finance_amount VARCHAR,
            ltv VARCHAR,
            has_active_manufacturer_warranty VARCHAR,
            calculated_recon VARCHAR,
            retail VARCHAR,
            gemini_soldto_ref VARCHAR,
            deposit_type VARCHAR,
            online_deposit VARCHAR,
            autotrader_percent VARCHAR,
            autotrader_retail_rating VARCHAR,
            days_to_deliver VARCHAR,
            days_to_prep VARCHAR,
            delivery_costs VARCHAR,
            purchasing_costs VARCHAR,
            other_costs VARCHAR,
            deal_id VARCHAR,
            recon_workshop_parts VARCHAR,
            recon_workshop_labour VARCHAR,
            recon_bodyshop_parts VARCHAR,
            recon_bodyshop_labour VARCHAR,
            recon_other_parts VARCHAR,
            recon_other_labour VARCHAR,
            appraisal_hours_est VARCHAR,
            days_on_site VARCHAR,
            days_at_prep VARCHAR,
            days_waiting_delivery VARCHAR,
            days_to_deal VARCHAR,
            days_to_handout VARCHAR,
            days_to_invoice VARCHAR,
            supp_gap VARCHAR,
            supp_warranty_12 VARCHAR,
            supp_warranty_24 VARCHAR,
            supp_warranty_36 VARCHAR,
            supp_shine_12 VARCHAR,
            supp_shine_24 VARCHAR,
            supp_shine_36 VARCHAR,
            supp_shine_48 VARCHAR,
            supp_gard_x VARCHAR,
            supp_tyre_alloy VARCHAR,
            supp_admin_fee VARCHAR,
            customerid VARCHAR,
            funder VARCHAR,
            full_commission VARCHAR,
            deposit_contribution_vat_m VARCHAR,
            deposit_contribution_vat_q VARCHAR,
            finance_type VARCHAR,
            dashboard_commission VARCHAR,
            finance_company VARCHAR,
            most_recent_price_change_by VARCHAR
        );
    `;
    var stmt_create = snowflake.createStatement({sqlText: create_stage_sql});
    stmt_create.execute();

    // Load data into staging table
    var copy_command = `
        COPY INTO temp_sales_reports_detailed_sales_staging (
            deal_date,invoice_date,stocknumber,reg,dateinstock,make,model,mileage,fuel,transmission,colour,bodystyle,trim,description,dis,dod,vatq,vin,cost,date_of_purchase,date_of_registration,retail_marginvat,vat,auction_costs,extprepfee,extsalefee,asking_price,sales_person,location,destination,buyer,recon,admin_fee,gap_payment_plan,gap,warranty_12,warranty_24,warranty_36,warranty_taxi_12,warranty_taxi_24,warranty_taxi_36,warranty_ev_12,warranty_ev_24,warranty_ev_36,warranty_ev_taxi_12,warranty_ev_taxi_24,warranty_ev_taxi_36,warrany_term,shine_12,shine_24,shine_36,shine_48,graphene,tyre_alloy,finance_commission,total_profit,status,boughtfrom,boughtfromspecific,sold_to,suplementary_invoices,invoice_number,siv,costs,profit_of_extras,px_reg,px_mileage,px_make,px_model,px_date_of_registration,px_siv,px2_reg,px2_mileage,px2_make,px2_model,px2_date_of_registration,px2_siv,cap_id,latest_cap_clean,engine_size,customer_postcode,chassis_profit,admin_fee_retail,gap_payment_plan_retail,gap_retail,warranty_12_retail,warranty_24_retail,warranty_36_retail,warranty_taxi_12_retail,warranty_taxi_24_retail,warranty_taxi_36_retail,warranty_ev_12_retail,warranty_ev_24_retail,warranty_ev_36_retail,warranty_ev_taxi_12_retail,warranty_ev_taxi_24_retail,warranty_ev_taxi_36_retail,shine_12_retail,shine_24_retail,shine_36_retail,shine_48_retail,gard_x_retail,tyre_alloy_retail,warranty_length,finance_amount,ltv,has_active_manufacturer_warranty,calculated_recon,retail,gemini_soldto_ref,deposit_type,online_deposit,autotrader_percent,autotrader_retail_rating,days_to_deliver,days_to_prep,delivery_costs,purchasing_costs,other_costs,deal_id,recon_workshop_parts,recon_workshop_labour,recon_bodyshop_parts,recon_bodyshop_labour,recon_other_parts,recon_other_labour,appraisal_hours_est,days_on_site,days_at_prep,days_waiting_delivery,days_to_deal,days_to_handout,days_to_invoice,supp_gap,supp_warranty_12,supp_warranty_24,supp_warranty_36,supp_shine_12,supp_shine_24,supp_shine_36,supp_shine_48,supp_gard_x,supp_tyre_alloy,supp_admin_fee,customerid,funder,full_commission,deposit_contribution_vat_m,deposit_contribution_vat_q,finance_type,dashboard_commission,finance_company,most_recent_price_change_by
        )
        FROM @reporting.details.REPORTS
        PATTERN = 'sales reports detailed-sales  big dashboard.*\\.csv'
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

    // Check row count in staging table after COPY INTO
    var count_stage_sql = `SELECT COUNT(*) FROM temp_sales_reports_detailed_sales_staging;`;
    var stmt_count_stage = snowflake.createStatement({sqlText: count_stage_sql});
    var rs_stage = stmt_count_stage.execute();
    rs_stage.next();
    var count_stage = rs_stage.getColumnValue(1);
    if (count_stage == 0) {
        return 'No data loaded into staging table. Check file format, header, and column mapping.';
    }

    // Get row count before insert
    var count_before_sql = `SELECT COUNT(*) FROM reporting.details.SALES_REPORTS_DETAILED_SALES;`;
    var stmt_count_before = snowflake.createStatement({sqlText: count_before_sql});
    var rs_before = stmt_count_before.execute();
    rs_before.next();
    var count_before = rs_before.getColumnValue(1);

    // Insert only new rows into main table (dedupe by stocknumber+reg+invoice_date)
    var insert_command = `
            INSERT INTO reporting.details.SALES_REPORTS_DETAILED_SALES (
                deal_date,invoice_date,stocknumber,reg,dateinstock,make,model,mileage,fuel,transmission,colour,bodystyle,trim,description,dis,dod,vatq,vin,cost,date_of_purchase,date_of_registration,retail_marginvat,vat,auction_costs,extprepfee,extsalefee,asking_price,sales_person,location,destination,buyer,recon,admin_fee,gap_payment_plan,gap,warranty_12,warranty_24,warranty_36,warranty_taxi_12,warranty_taxi_24,warranty_taxi_36,warranty_ev_12,warranty_ev_24,warranty_ev_36,warranty_ev_taxi_12,warranty_ev_taxi_24,warranty_ev_taxi_36,warrany_term,shine_12,shine_24,shine_36,shine_48,graphene,tyre_alloy,finance_commission,total_profit,status,boughtfrom,boughtfromspecific,sold_to,suplementary_invoices,invoice_number,siv,costs,profit_of_extras,px_reg,px_mileage,px_make,px_model,px_date_of_registration,px_siv,px2_reg,px2_mileage,px2_make,px2_model,px2_date_of_registration,px2_siv,cap_id,latest_cap_clean,engine_size,customer_postcode,chassis_profit,admin_fee_retail,gap_payment_plan_retail,gap_retail,warranty_12_retail,warranty_24_retail,warranty_36_retail,warranty_taxi_12_retail,warranty_taxi_24_retail,warranty_taxi_36_retail,warranty_ev_12_retail,warranty_ev_24_retail,warranty_ev_36_retail,warranty_ev_taxi_12_retail,warranty_ev_taxi_24_retail,warranty_ev_taxi_36_retail,shine_12_retail,shine_24_retail,shine_36_retail,shine_48_retail,gard_x_retail,tyre_alloy_retail,warranty_length,finance_amount,ltv,has_active_manufacturer_warranty,calculated_recon,retail,gemini_soldto_ref,deposit_type,online_deposit,autotrader_percent,autotrader_retail_rating,days_to_deliver,days_to_prep,delivery_costs,purchasing_costs,other_costs,deal_id,recon_workshop_parts,recon_workshop_labour,recon_bodyshop_parts,recon_bodyshop_labour,recon_other_parts,recon_other_labour,appraisal_hours_est,days_on_site,days_at_prep,days_waiting_delivery,days_to_deal,days_to_handout,days_to_invoice,supp_gap,supp_warranty_12,supp_warranty_24,supp_warranty_36,supp_shine_12,supp_shine_24,supp_shine_36,supp_shine_48,supp_gard_x,supp_tyre_alloy,supp_admin_fee,customerid,funder,full_commission,deposit_contribution_vat_m,deposit_contribution_vat_q,finance_type,dashboard_commission,finance_company,most_recent_price_change_by,audit_insert_ts
            )
            SELECT
                deal_date,invoice_date,stocknumber,reg,dateinstock,make,model,mileage,fuel,transmission,colour,bodystyle,trim,description,dis,dod,vatq,vin,cost,date_of_purchase,date_of_registration,retail_marginvat,vat,auction_costs,extprepfee,extsalefee,asking_price,sales_person,location,destination,buyer,recon,admin_fee,gap_payment_plan,gap,warranty_12,warranty_24,warranty_36,warranty_taxi_12,warranty_taxi_24,warranty_taxi_36,warranty_ev_12,warranty_ev_24,warranty_ev_36,warranty_ev_taxi_12,warranty_ev_taxi_24,warranty_ev_taxi_36,warrany_term,shine_12,shine_24,shine_36,shine_48,graphene,tyre_alloy,finance_commission,total_profit,status,boughtfrom,boughtfromspecific,sold_to,suplementary_invoices,invoice_number,siv,costs,profit_of_extras,px_reg,px_mileage,px_make,px_model,px_date_of_registration,px_siv,px2_reg,px2_mileage,px2_make,px2_model,px2_date_of_registration,px2_siv,cap_id,latest_cap_clean,engine_size,customer_postcode,chassis_profit,admin_fee_retail,gap_payment_plan_retail,gap_retail,warranty_12_retail,warranty_24_retail,warranty_36_retail,warranty_taxi_12_retail,warranty_taxi_24_retail,warranty_taxi_36_retail,warranty_ev_12_retail,warranty_ev_24_retail,warranty_ev_36_retail,warranty_ev_taxi_12_retail,warranty_ev_taxi_24_retail,warranty_ev_taxi_36_retail,shine_12_retail,shine_24_retail,shine_36_retail,shine_48_retail,gard_x_retail,tyre_alloy_retail,warranty_length,finance_amount,ltv,has_active_manufacturer_warranty,calculated_recon,retail,gemini_soldto_ref,deposit_type,online_deposit,autotrader_percent,autotrader_retail_rating,days_to_deliver,days_to_prep,delivery_costs,purchasing_costs,other_costs,deal_id,recon_workshop_parts,recon_workshop_labour,recon_bodyshop_parts,recon_bodyshop_labour,recon_other_parts,recon_other_labour,appraisal_hours_est,days_on_site,days_at_prep,days_waiting_delivery,days_to_deal,days_to_handout,days_to_invoice,supp_gap,supp_warranty_12,supp_warranty_24,supp_warranty_36,supp_shine_12,supp_shine_24,supp_shine_36,supp_shine_48,supp_gard_x,supp_tyre_alloy,supp_admin_fee,customerid,funder,full_commission,deposit_contribution_vat_m,deposit_contribution_vat_q,finance_type,dashboard_commission,finance_company,most_recent_price_change_by,CURRENT_TIMESTAMP
            FROM temp_sales_reports_detailed_sales_staging s
            WHERE NOT EXISTS (
                SELECT 1 FROM reporting.details.SALES_REPORTS_DETAILED_SALES t 
                WHERE t.stocknumber = s.stocknumber AND t.reg = s.reg AND t.invoice_date = s.invoice_date
            );
    `;
    var stmt_insert = snowflake.createStatement({sqlText: insert_command});
    stmt_insert.execute();

    // Get row count after insert
    var count_after_sql = `SELECT COUNT(*) FROM reporting.details.SALES_REPORTS_DETAILED_SALES;`;
    var stmt_count_after = snowflake.createStatement({sqlText: count_after_sql});
    var rs_after = stmt_count_after.execute();
    rs_after.next();
    var count_after = rs_after.getColumnValue(1);

    var rows_added = count_after - count_before;

    // Drop staging table
    var drop_command = `DROP TABLE IF EXISTS temp_sales_reports_detailed_sales_staging;`;
    var stmt_drop = snowflake.createStatement({sqlText: drop_command});
    stmt_drop.execute();

    // Notify
    var subject, msg, returnMsg;
    if (rows_added > 0) {
        subject = 'SALES_REPORTS_DETAILED_SALES Load: Rows Loaded';
        msg = `SUCCESS: Loaded ${rows_added} new row(s) into SALES_REPORTS_DETAILED_SALES from @reporting.details.REPORTS.`;
        returnMsg = msg;
    } else {
        subject = 'SALES_REPORTS_DETAILED_SALES Load: No New Rows';
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
            'SALES_REPORTS_DETAILED_SALES Load Failed',
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
    return 'ERROR in sp_sales_reports_detailed_sales: ' + err.message;
}
$$;