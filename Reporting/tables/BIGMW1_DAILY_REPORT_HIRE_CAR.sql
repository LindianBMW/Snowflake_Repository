use role accountadmin;

DROP TABLE reporting.details.bigw1_daily_report_HIRE_CAR;

CREATE OR REPLACE TABLE reporting.details.bigmw1_daily_report_HIRE_CAR (
    DATE VARCHAR,
    SUB_ACCOUNT VARCHAR,
    SCHEME VARCHAR,
    JOB_NUMBER VARCHAR,
    VRN VARCHAR,
    VEHICLE_MAKE VARCHAR,
    VEHICLE_MODEL VARCHAR,
    HIRE_CAR VARCHAR
);
