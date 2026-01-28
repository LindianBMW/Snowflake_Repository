


CREATE OR REPLACE PROCEDURE RAW.CRM.sp_load_infinity_download()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
/*
------------------------------------------------------------
    Procedure: RAW.CRM.sp_load_infinity_download
    Author: Lindian Thomas,  January 2026
    Description:
        This procedure loads Infinity data from staged CSV files into the main Infinity_Download table.
        - It removes duplicate rows based on rowId.
        - Sensitive columns (phone numbers, postcode, IP) are hashed for privacy.
        - If any error occurs, an email notification is sent to Lindian Thomas.
        - All steps are logged and errors are handled gracefully.
        Use this procedure to automate and secure the ingestion of Infinity call tracking data.

    To add additional email receipients in this proc 
                'Lindian.thomas@bigmotoringworld.co.uk, stuart.saunders@bigmotoringworld.co.uk',   
    Plus do this 
    CALL SYSTEM$START_USER_EMAIL_VERIFICATION('stuart.saunders@bigmotoringworld.co.uk');

    ALTER NOTIFICATION INTEGRATION DATA_ALERTS_EMAIL_INT
    SET ALLOWED_RECIPIENTS = ('lindian.thomas@bigmotoringworld.co.uk', 'stuart.saunders@bigmotoringworld.co.uk');

    DESC NOTIFICATION INTEGRATION DATA_ALERTS_EMAIL_INT; 
-----------------------------------------------------------
*/
try {
     
    // Drop temp table if it already exists
    var drop_stage_sql = `DROP TABLE IF EXISTS temp_infinity_download_staging;`;
    var stmt_drop_stage = snowflake.createStatement({sqlText: drop_stage_sql});
    stmt_drop_stage.execute();

    var create_stage_sql = `CREATE TEMPORARY TABLE temp_infinity_download_staging LIKE RAW.CRM.Infinity_Download;`;
    var stmt_create = snowflake.createStatement({sqlText: create_stage_sql});
    stmt_create.execute();

    var copy_command = `
        COPY INTO temp_infinity_download_staging (
            rowId,
            triggerDatetime,
            igrp,
            dgrp,
            ch,
            src,
            act,
            algo,
            attr,
            vref,
            href,
            num,
            term,
            vid,
            t,
            goal,
            srcHash,
            new,
            pageTitle,
            pub,
            segment,
            segmentGroupId,
            dom,
            ref,
            network,
            matchRef,
            matchType,
            campaign,
            adGroup,
            adRef,
            keywordRef,
            dialledPhoneNumber,
            srcPhoneNumber,
            destPhoneNumber,
            callDuration,
            bridgeDuration,
            ringTime,
            ivrDuration,
            queueDuration,
            operatorRef,
            ivrRef,
            dialplanRef,
            callRating,
            callState,
            callDirection,
            callStage,
            operatorRealm,
            telcoCode,
            numType,
            rec,
            transcriptionConfidence,
            totalKeywordScore,
            totalOperatorKeywordScore,
            totalContactKeywordScore,
            operatorPositiveKeywordCount,
            operatorNeutralKeywordCount,
            operatorNegativeKeywordCount,
            contactPositiveKeywordCount,
            contactNeutralKeywordCount,
            contactNegativeKeywordCount,
            callPciDataChecked,
            callPciDataFound,
            callSsnDataChecked,
            callSsnDataFound,
            callPiiDataChecked,
            callPiiDataFound,
            callKeywordSpotting,
            callTranscription,
            whois,
            ip,
            ua,
            country,
            city,
            continent,
            res,
            lat,
            long,
            region,
            postcode,
            area,
            spider,
            host,
            visitorType,
            sfWhoRef,
            visitorPageCount,
            visitorGoalCount,
            visitorCallCount,
            visitorFirstDatetime,
            landingPageId,
            conversionPageId,
            chName,
            chType,
            segmentName,
            segmentRef,
            orgId,
            segmentGroupName,
            notes,
            leadScore
        )
        FROM @RAW.CRM.Infinity
        PATTERN = '.*\\.csv'
        FILE_FORMAT = (
            TYPE = 'CSV',
            SKIP_HEADER = 1,
            FIELD_DELIMITER = ',',
            FIELD_OPTIONALLY_ENCLOSED_BY = '"',
            TRIM_SPACE = FALSE,
            DATE_FORMAT = 'AUTO',
            TIME_FORMAT = 'AUTO',
            TIMESTAMP_FORMAT = 'AUTO'
        )
        ON_ERROR = 'ABORT_STATEMENT';
    `;
    var stmt_copy = snowflake.createStatement({sqlText: copy_command});
    stmt_copy.execute();

    var delete_non_numeric_command = `
        DELETE FROM temp_infinity_download_staging
        WHERE TRY_CAST(rowid AS NUMBER) IS NULL;
    `;
    var stmt_delete = snowflake.createStatement({sqlText: delete_non_numeric_command});
    stmt_delete.execute();

    // Insert only new rows into the main table
    var insert_command = `
        INSERT INTO RAW.CRM.Infinity_Download (
            rowId,
            triggerDatetime,
            igrp,
            dgrp,
            ch,
            src,
            act,
            algo,
            attr,
            vref,
            href,
            num,
            term,
            vid,
            t,
            goal,
            srcHash,
            new,
            pageTitle,
            pub,
            segment,
            segmentGroupId,
            dom,
            ref,
            network,
            matchRef,
            matchType,
            campaign,
            adGroup,
            adRef,
            keywordRef,
            dialledPhoneNumber,
            srcPhoneNumber,
            destPhoneNumber,
            callDuration,
            bridgeDuration,
            ringTime,
            ivrDuration,
            queueDuration,
            operatorRef,
            ivrRef,
            dialplanRef,
            callRating,
            callState,
            callDirection,
            callStage,
            operatorRealm,
            telcoCode,
            numType,
            rec,
            transcriptionConfidence,
            totalKeywordScore,
            totalOperatorKeywordScore,
            totalContactKeywordScore,
            operatorPositiveKeywordCount,
            operatorNeutralKeywordCount,
            operatorNegativeKeywordCount,
            contactPositiveKeywordCount,
            contactNeutralKeywordCount,
            contactNegativeKeywordCount,
            callPciDataChecked,
            callPciDataFound,
            callSsnDataChecked,
            callSsnDataFound,
            callPiiDataChecked,
            callPiiDataFound,
            callKeywordSpotting,
            callTranscription,
            whois,
            ip,
            ua,
            country,
            city,
            continent,
            res,
            lat,
            long,
            region,
            postcode,
            area,
            spider,
            host,
            visitorType,
            sfWhoRef,
            visitorPageCount,
            visitorGoalCount,
            visitorCallCount,
            visitorFirstDatetime,
            landingPageId,
            conversionPageId,
            chName,
            chType,
            segmentName,
            segmentRef,
            orgId,
            segmentGroupName,
            notes,
            leadScore
        )
        SELECT
            rowId,
            triggerDatetime,
            igrp,
            dgrp,
            ch,
            src,
            act,
            algo,
            attr,
            vref,
            href,
            SHA2(num, 256) AS num,
            term,
            vid,
            t,
            goal,
            srcHash,
            new,
            pageTitle,
            pub,
            segment,
            segmentGroupId,
            dom,
            ref,
            network,
            matchRef,
            matchType,
            campaign,
            adGroup,
            adRef,
            keywordRef,
            SHA2(dialledPhoneNumber, 256) AS dialledPhoneNumber,
            SHA2(srcPhoneNumber, 256) AS srcPhoneNumber,
            SHA2(destPhoneNumber, 256) AS destPhoneNumber,
            callDuration,
            bridgeDuration,
            ringTime,
            ivrDuration,
            queueDuration,
            operatorRef,
            ivrRef,
            dialplanRef,
            callRating,
            callState,
            callDirection,
            callStage,
            operatorRealm,
            telcoCode,
            numType,
            rec,
            transcriptionConfidence,
            totalKeywordScore,
            totalOperatorKeywordScore,
            totalContactKeywordScore,
            operatorPositiveKeywordCount,
            operatorNeutralKeywordCount,
            operatorNegativeKeywordCount,
            contactPositiveKeywordCount,
            contactNeutralKeywordCount,
            contactNegativeKeywordCount,
            callPciDataChecked,
            callPciDataFound,
            callSsnDataChecked,
            callSsnDataFound,
            callPiiDataChecked,
            callPiiDataFound,
            callKeywordSpotting,
            callTranscription,
            whois,
            SHA2(ip, 256) AS ip,
            ua,
            country,
            city,
            continent,
            res,
            lat,
            long,
            region,
            SHA2(postcode, 256) AS postcode,
            area,
            spider,
            host,
            visitorType,
            sfWhoRef,
            visitorPageCount,
            visitorGoalCount,
            visitorCallCount,
            visitorFirstDatetime,
            landingPageId,
            conversionPageId,
            chName,
            chType,
            segmentName,
            segmentRef,
            orgId,
            segmentGroupName,
            notes,
            leadScore
        FROM temp_infinity_download_staging s
        WHERE NOT EXISTS (
            SELECT 1 FROM RAW.CRM.Infinity_Download t WHERE t.rowId = s.rowId
        );
    `;

    // Get row count before insert
    var count_before_sql = `SELECT COUNT(*) FROM RAW.CRM.Infinity_Download;`;
    var stmt_count_before = snowflake.createStatement({sqlText: count_before_sql});
    var rs_before = stmt_count_before.execute();
    rs_before.next();
    var count_before = rs_before.getColumnValue(1);

    var stmt_insert = snowflake.createStatement({sqlText: insert_command});
    stmt_insert.execute();

    // Get row count after insert
    var count_after_sql = `SELECT COUNT(*) FROM RAW.CRM.Infinity_Download;`;
    var stmt_count_after = snowflake.createStatement({sqlText: count_after_sql});
    var rs_after = stmt_count_after.execute();
    rs_after.next();
    var count_after = rs_after.getColumnValue(1);

    var rows_added = count_after - count_before;

    // Drop the temp staging table
    var drop_command = `DROP TABLE IF EXISTS temp_infinity_download_staging;`;
    var stmt_drop = snowflake.createStatement({sqlText: drop_command});
    stmt_drop.execute();

    // Send success email notification
    var subject, body, returnMsg;
    if (rows_added > 0) {
        subject = 'Infinity data load: rows loaded';
        body = `SUCCESS: Loaded ${rows_added} new row(s) into RAW.CRM.Infinity_Download from stage RAW.CRM.Infinity.`;
        returnMsg = 'SUCCESS: Loaded ' + rows_added + ' new row(s) into RAW.CRM.Infinity_Download from stage RAW.CRM.Infinity.';
    } else {
        subject = 'Infinity data load: no new rows';
        body = 'Infinity Data Load successfully ran: No new rows were loaded into RAW.CRM.Infinity_Download. The data is up to date and no duplicates were found.';
        returnMsg = 'INFO: No new rows loaded. The data is up to date.';
    }
    var success_email_command = `call system$send_email(
        'DATA_ALERTS_EMAIL_INT',
        'Lindian.thomas@bigmotoringworld.co.uk',
        '${subject}',
        '${body}'
    );`;
    var stmt_success_email = snowflake.createStatement({sqlText: success_email_command});
    stmt_success_email.execute();
    return returnMsg;
} catch (err) {
    // Attempt to send error email (using DATA_ALERTS_EMAIL_INT integration)
    try {
        var email_command = `call system$send_email(
            'DATA_ALERTS_EMAIL_INT',
            'Lindian.thomas@bigmotoringworld.co.uk', 
            'Infinity load failed',
            'Error message: ${err.message}'
        );`;
        var stmt_email = snowflake.createStatement({sqlText: email_command});
        stmt_email.execute();
    } catch (emailErr) {
        // If email fails, ignore
    }
    return 'ERROR in sp_load_infinity_download: ' + err.message;
}
$$;

 