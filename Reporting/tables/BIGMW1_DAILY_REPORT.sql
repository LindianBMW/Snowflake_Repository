-- Table definition, all columns as VARCHAR for raw data ingestion

use role accountadmin;
CREATE OR REPLACE TABLE reporting.details.BIGMW1_DAILY_REPORT (
    date VARCHAR,
    partner VARCHAR,
    sub_account VARCHAR,
    scheme VARCHAR,
    job_number VARCHAR,
    vrn VARCHAR,
    vehicle_make VARCHAR,
    vehicle_model VARCHAR,
    reported_symptom VARCHAR,
    fault_description VARCHAR,
    fault_cause VARCHAR,
    fault_action VARCHAR,
    resource_type VARCHAR,
    job_type_name VARCHAR,
    tow_destination VARCHAR,
    tow_distance VARCHAR,
    service_breakdown_count VARCHAR,
    audit_insert_ts TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP
);
 