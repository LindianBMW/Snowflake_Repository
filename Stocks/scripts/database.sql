
use role accountadmin;

use database raw;
use schema stock_audit;


show tables; in stock_audit;

select * from RAW.stock_audit.STOCK_AUDIT_2025_08_10 limit 10; 

show taBLES IN RAW.stock_audit;

select * from  analytics.dna.stock_audit order by date desc;

select * from 
=====================================================================================
models/stg_stock_audit_union.sql

{# -- Jinja macro section -- #}
{% set source_tables = run_query(
    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'STOCK_AUDIT' AND table_catalog = 'RAW' AND table_name ILIKE 'STOCK_AUDIT_%' ORDER BY table_name DESC"
).columns[0].values() %}

{# -- Begin output SQL -- #}
{% for t in source_tables %}
    SELECT
        MARGIN_VAT,
        ASKING_PRICE,
        INTEREST_RATE,
        TRANSMISSION,
        CAP_CLEAN,
        BUYER,
        CHASSIS_NUMBER,
        COST,
        COLOUR,
        STOCK_NUMBER,
        DAYS_IN_STOCK,
        REGISTRATION,
        MAKE_MODEL,
        DATE_REGD,
        GROSS_COST,
        CURRENT_SIV,
        BOUGHT_FROM_GROUP,
        STATUS,
        MILEAGE,
        DAYS_ON_DISPLAY,
        REAL_DAYS_ON_DISPLAY,
        VAT_QUALIFYING,
        AUTO_TRADER_RANK,
        CHASSIS_PROFIT,
        RETAIL_VAT,
        BOUGHT_FROM,
        FUEL,
        AUTO_TRADER_RANKING,
        DATE_IN_STOCK,
        AUTO_TRADER_PRICE_,
        RECON,
        LOCATION,
        TO_DATE(REGEXP_SUBSTR('{{ t }}', '[0-9]{4}_[0-9]{2}_[0-9]{2}'), 'YYYY_MM_DD') AS DATE
    FROM RAW.STOCK_AUDIT.{{ t }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %}

=====================================================================================
models/analytics_dna_stock_audit.sql
      
{{ config(materialized='incremental', unique_key=['stock_number','date']) }}

SELECT
    MARGIN_VAT,
    ASKING_PRICE,
    INTEREST_RATE,
    TRANSMISSION,
    CAP_CLEAN,
    BUYER,
    CHASSIS_NUMBER,
    COST,
    COLOUR,
    STOCK_NUMBER,
    DAYS_IN_STOCK,
    REGISTRATION,
    MAKE_MODEL,
    DATE_REGD,
    GROSS_COST,
    CURRENT_SIV,
    BOUGHT_FROM_GROUP,
    STATUS,
    MILEAGE,
    DAYS_ON_DISPLAY,
    REAL_DAYS_ON_DISPLAY,
    VAT_QUALIFYING,
    AUTO_TRADER_RANK,
    CHASSIS_PROFIT,
    RETAIL_VAT,
    BOUGHT_FROM,
    FUEL,
    AUTO_TRADER_RANKING,
    DATE_IN_STOCK,
    AUTO_TRADER_PRICE_,
    RECON,
    LOCATION,
    DATE -- this comes from the staging union model
FROM {{ ref('stg_stock_audit_union') }}
WHERE date NOT IN (
  SELECT DISTINCT date FROM {{ this }}
)

=====================================================================================

sources.yml

version: 2

sources:
  - name: stock_audit
    database: RAW
    schema: STOCK_AUDIT
    description: >
      Contains daily snapshot tables for vehicle stock audit (table names: STOCK_AUDIT_YYYY_MM_DD).

--==================================================================================

version: 2

models:
  - name: stg_stock_audit_union
    description: >
      Union of all stock audit snapshot tables from RAW.STOCK_AUDIT.
    columns:
      - name: margin_vat
        description: "Margin VAT"
      - name: asking_price
      - name: interest_rate
      - name: transmission
      - name: cap_clean
      - name: buyer
      - name: chassis_number
      - name: cost
      - name: colour
      - name: stock_number
      - name: days_in_stock
      - name: registration
      - name: make_model
      - name: date_regd
      - name: gross_cost
      - name: current_siv
      - name: bought_from_group
      - name: status
      - name: mileage
      - name: days_on_display
      - name: real_days_on_display
      - name: vat_qualifying
      - name: auto_trader_rank
      - name: chassis_profit
      - name: retail_vat
      - name: bought_from
      - name: fuel
      - name: auto_trader_ranking
      - name: date_in_stock
      - name: auto_trader_price_
      - name: recon
      - name: location
      - name: date
        description: "Snapshot date extracted from source table name"

  - name: analytics_dna_stock_audit
    description: >
      The incrementally built table with only unloaded snapshots.
    columns:
      - name: margin_vat
      - name: asking_price
      - name: interest_rate
      - name: transmission
      - name: cap_clean
      - name: buyer
      - name: chassis_number
      - name: cost
      - name: colour
      - name: stock_number
      - name: days_in_stock
      - name: registration
      - name: make_model
      - name: date_regd
      - name: gross_cost
      - name: current_siv
      - name: bought_from_group
      - name: status
      - name: mileage
      - name: days_on_display
      - name: real_days_on_display
      - name: vat_qualifying
      - name: auto_trader_rank
      - name: chassis_profit
      - name: retail_vat
      - name: bought_from
      - name: fuel
      - name: auto_trader_ranking
      - name: date_in_stock
      - name: auto_trader_price_
      - name: recon
      - name: location
      - name: date
        description: "Snapshot date"
select distinct date from analytics.dna.stock_audit order by date desc;

select count(distinct stock_number) from analytics.dna.stock_audit order by stock_number;
--44266;

select count(*) from analytics.dna.stock_audit;

DESC TABLE RAW.stock_audit.STOCK_AUDIT_2025_08_10;

DESC TABLE analytics.dna.stock_audit;


select * from  raw.stock_audit.information_schema.tables;


SELECT
  stock_number,
  date,
  COUNT(*) AS record_count
FROM analytics.dna.stock_audit
GROUP BY stock_number, date
having COUNT(*) > 1
ORDER BY date DESC, stock_number;

--Good  
SELECT 
    table_schema, 
    table_name 
FROM 
    information_schema.tables
WHERE 
    table_schema = 'STOCK_AUDIT'  -- <--- Schema name in all caps, unless created lowercase with quotes
    AND table_catalog = 'RAW'     -- <--- Database name in all caps
    AND table_name ILIKE 'STOCK_AUDIT_%'
ORDER BY 
    table_name DESC;

SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'STOCK_AUDIT'
  AND table_catalog = 'RAW'
  AND table_name ILIKE 'STOCK_AUDIT_%'
ORDER BY table_name DESC;



CREATE TABLE raw.STOCK_AUDIT.STOCK_AUDIT_2026_02_02 CLONE raw.STOCK_AUDIT.STOCK_AUDIT_2025_12_06;;


SELECT * FROM  raw.STOCK_AUDIT.STOCK_AUDIT_2026_02_02;

--backup table analytics.dna.stock_audit
CREATE TABLE analytics.dna.stock_audit_backup_20260202 CLONE analytics.dna.stock_audit;

use role accountadmin;

CREATE OR REPLACE TABLE analytics.dna.stock_audit CLONE analytics.dna.stock_audit_backup_20260202;

select * from analytics.dna.stock_audit order by date desc limit 10; 

SELECT *
FROM ANALYTICS.DNA.STOCK_AUDIT
WHERE DATE IN ('2025-02-02'); 

QUALIFY ROW_NUMBER() OVER (PARTITION BY DATE ORDER BY STOCK_NUMBER) = 1;

SELECT *
FROM ANALYTICS.DNA.STOCK_AUDIT;



WHERE DATE IN ('2026-02-02', '2025-12-6')
QUALIFY ROW_NUMBER() OVER (PARTITION BY DATE ORDER BY STOCK_NUMBER) = 1;


--last date 09-12-2025
select * from analytics.dna.stock_audit_backup_20260202 order by date desc limit 10; 

SELECT * FROM RAW.INFORMATION_SCHEMA.TABLES; 

SELECT table_name 
FROM RAW.information_schema.tables 
WHERE table_schema = 'STOCK_AUDIT' 
  AND table_catalog = 'RAW' 
  AND table_name ILIKE 'STOCK_AUDIT_%' 
ORDER BY table_name DESC;




-- See what columns exist in the view
SELECT * 
FROM STAGING.dbt_lthomas.stg_stock_audit_union 
LIMIT 1;

USE ROLE ACCOUNTADMIN;

SELECT DATE, COUNT(*) 
FROM STAGING.dbt_lthomas.stg_stock_audit_union 
GROUP BY DATE
ORDER BY DATE DESC;

-- Find where the view was created
SELECT 
    table_catalog AS database_name,
    table_schema AS schema_name,
    table_name,
    created AS created_at
FROM information_schema.views
WHERE table_name = 'STG_STOCK_AUDIT_UNION'
ORDER BY created DESC;

SELECT DATE, COUNT(*) 
FROM RAW.DBT_LTHOMAS.STG_STOCK_AUDIT_UNION 
GROUP BY DATE
ORDER BY DATE DESC;

SELECT DATE, COUNT(*) 
FROM RAW.DBT_LTHOMAS.STG_STOCK_AUDIT_UNION 
GROUP BY DATE
ORDER BY DATE DESC;

-- See all columns in the view
SELECT * 
FROM RAW.DBT_LTHOMAS.STG_STOCK_AUDIT_UNION 
LIMIT 0;

SELECT column_name 
FROM RAW.information_schema.columns 
WHERE table_name = 'STG_STOCK_AUDIT_UNION' 
  AND table_schema = 'DBT_LTHOMAS'
  AND table_catalog = 'RAW'
ORDER BY ordinal_position;

SELECT column_name 
FROM RAW.information_schema.columns 
WHERE table_name = 'STG_STOCK_AUDIT_UNION' 
  AND table_schema = 'DBT_LTHOMAS'
  AND table_catalog = 'RAW'
ORDER BY ordinal_position;

SELECT DATE, COUNT(*) 
FROM RAW.DBT_LTHOMAS.STG_STOCK_AUDIT_UNION 
GROUP BY DATE
ORDER BY DATE DESC;


SELECT DATE, COUNT(*) 
FROM ANALYTICS.DBT_LTHOMAS.analytics_dna_stock_audit 
GROUP BY DATE
ORDER BY DATE DESC;


SELECT DATE, COUNT(*) as row_count
FROM ANALYTICS.DNA.STOCK_AUDIT
GROUP BY DATE
ORDER BY DATE DESC;


SELECT DATE, COUNT(*) 
FROM RAW.DBT_LTHOMAS.STG_STOCK_AUDIT_UNION 
WHERE DATE = '2026-02-02'
GROUP BY DATE;


-- See all dates in the final table
SELECT DATE, COUNT(*) 
FROM ANALYTICS.DNA.STOCK_AUDIT 
GROUP BY DATE
ORDER BY DATE DESC
LIMIT 20;


SELECT *  
FROM ANALYTICS.DNA.STOCK_AUDIT 
ORDER BY DATE DESC
LIMIT 20;

SELECT DATE, COUNT(*) 
FROM ANALYTICS.DNA.STOCK_AUDIT 
GROUP BY DATE
ORDER BY DATE DESC
LIMIT 20;

SELECT DATE, COUNT(*) 
FROM ANALYTICS.DNA.STOCK_AUDIT 
GROUP BY DATE
ORDER BY DATE DESC
LIMIT 20;

SELECT DATE, COUNT(*) 
FROM ANALYTICS.DNA.STOCK_AUDIT 
GROUP BY DATE
ORDER BY DATE DESC
LIMIT 20;

--dbt run --select analytics_dna_stock_audit

SELECT DATE, COUNT(*) 
FROM ANALYTICS.DNA.STOCK_AUDIT 
WHERE DATE = '2026-02-02'
GROUP BY DATE;

SELECT DATE, COUNT(*) 
FROM RAW.DBT_LTHOMAS.STG_STOCK_AUDIT_UNION 
WHERE DATE = '2026-02-02'
GROUP BY DATE;

SELECT DATE, COUNT(*) 
FROM ANALYTICS.DNA.STOCK_AUDIT 
GROUP BY DATE
ORDER BY DATE DESC
LIMIT 10;

SELECT DATE, COUNT(*) 
FROM ANALYTICS.DNA.STOCK_AUDIT 
GROUP BY DATE
ORDER BY DATE DESC
LIMIT 10;

SELECT DATE, COUNT(*) 
FROM RAW.DBT_LTHOMAS.STG_STOCK_AUDIT_UNION
WHERE date NOT IN (
  SELECT DISTINCT date FROM ANALYTICS.DNA.STOCK_AUDIT
)
GROUP BY DATE
ORDER BY DATE;

-- Check destination columns
SELECT column_name
FROM ANALYTICS.INFORMATION_SCHEMA.COLUMNS
WHERE table_schema = 'DNA'
  AND table_name = 'STOCK_AUDIT' and column_name ='SOURCE_TABLE'
ORDER BY ordinal_position;

SELECT column_name
FROM RAW.INFORMATION_SCHEMA.COLUMNS
WHERE table_schema = 'DBT_LTHOMAS'
  AND table_name = 'STG_STOCK_AUDIT_UNION'
ORDER BY ordinal_position;

SELECT column_name
FROM ANALYTICS.INFORMATION_SCHEMA.COLUMNS
WHERE table_schema = 'DNA'
  AND table_name = 'STOCK_AUDIT'  and column_name ='SOURCE_TABLE'
ORDER BY ordinal_position;

SELECT DATE, COUNT(*) 
FROM ANALYTICS.DNA.STOCK_AUDIT 
WHERE DATE = '2026-02-02'
GROUP BY DATE;

SELECT DATE, COUNT(*) 
FROM ANALYTICS.dbt_lthomas_DNA.STOCK_AUDIT 
WHERE DATE = '2026-02-02'
GROUP BY DATE;


/*Verification queries */

SELECT DATE, COUNT(*) 
FROM ANALYTICS.DNA.STOCK_AUDIT 
WHERE DATE = '2026-02-02'
GROUP BY DATE;

SELECT DATE, COUNT(*) 
FROM ANALYTICS.DNA.STOCK_AUDIT 
GROUP BY DATE
ORDER BY DATE DESC
LIMIT 10;

show schemas in database raw;