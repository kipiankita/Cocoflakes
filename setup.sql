USE ROLE ACCOUNTADMIN;

-- Create the Role
CREATE ROLE IF NOT EXISTS data_engineer;

-- Grant Account-level privileges
GRANT CREATE DATABASE ON ACCOUNT TO ROLE data_engineer;
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE data_engineer;

-- Grant access to snowflake.account_usage and cost data
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE data_engineer;
GRANT DATABASE ROLE SNOWFLAKE.USAGE_VIEWER TO ROLE data_engineer;

-- Grant the role to sysadmin to maintain the hierarchy
GRANT ROLE data_engineer TO ROLE SYSADMIN;

grant role data_analyst to user CHANDANR;

CREATE OR REPLACE RESOURCE MONITOR COCO_MONITOR
  WITH CREDIT_QUOTA = 15
  FREQUENCY = DAILY
  START_TIMESTAMP = IMMEDIATELY
  TRIGGERS
    ON 80 PERCENT DO NOTIFY              
    ON 100 PERCENT DO SUSPEND            
    ON 110 PERCENT DO SUSPEND_IMMEDIATE; 

ALTER WAREHOUSE COCO_WH SET RESOURCE_MONITOR = COCO_MONITOR;

-- Switch to the new role for the rest of the execution
USE ROLE data_engineer;


-- Create Databases
CREATE DATABASE IF NOT EXISTS PROD_DATA;
CREATE DATABASE IF NOT EXISTS LDW;
CREATE DATABASE IF NOT EXISTS DELETED_TABLE;

-- Create Schemas in PROD_DATA
CREATE SCHEMA IF NOT EXISTS PROD_DATA.ANALYSIS;
CREATE SCHEMA IF NOT EXISTS PROD_DATA.DM;
CREATE SCHEMA IF NOT EXISTS PROD_DATA.STG;
CREATE SCHEMA IF NOT EXISTS PROD_DATA.WEBHOOKS;

-- Create Schemas in LDW
CREATE SCHEMA IF NOT EXISTS LDW.RID_DM;
CREATE SCHEMA IF NOT EXISTS LDW.VEH_DM;

-- Create Schemas in DELETED_TABLE
CREATE SCHEMA IF NOT EXISTS DELETED_TABLE.PUBLIC_DATA;



USE ROLE ACCOUNTADMIN;

-- 1. Create the Resource Monitor with a 25-credit hard limit
CREATE OR REPLACE RESOURCE MONITOR POC_MONITOR
  WITH CREDIT_QUOTA = 25
  FREQUENCY = DAILY
  START_TIMESTAMP = IMMEDIATELY
  TRIGGERS
    ON 80 PERCENT DO NOTIFY              -- Sends an email warning at 20 credits
    ON 100 PERCENT DO SUSPEND            -- Stops accepting new queries at 25 credits
    ON 110 PERCENT DO SUSPEND_IMMEDIATE; -- Kills the actively running generation at 27.5 credits

-- 2. Tie the monitor to your specific warehouse
-- MAKE SURE TO REPLACE 'YOUR_POC_WAREHOUSE' WITH YOUR ACTUAL WAREHOUSE NAME
ALTER WAREHOUSE COMPUTE_WH SET RESOURCE_MONITOR = POC_MONITOR;

-- 3. Switch back to your engineering role to run the actual data load
USE ROLE data_engineer;

-- Remember to scale up your warehouse first!
ALTER WAREHOUSE COMPUTE_WH SET WAREHOUSE_SIZE = '2X-LARGE';

CREATE OR REPLACE PROCEDURE GENERATE_EBIKE_POC_TABLES()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    -- Cursor to loop through all 7 schemas
    schema_list CURSOR FOR SELECT column1 AS schema_name FROM VALUES 
        ('PROD_DATA.ANALYSIS'), 
        ('PROD_DATA.DM'), 
        ('PROD_DATA.STG'), 
        ('PROD_DATA.WEBHOOKS'), 
        ('LDW.RID_DM'), 
        ('LDW.VEH_DM'), 
        ('DELETED_TABLE.PUBLIC_DATA');
        
    target_table VARCHAR;
    row_count NUMBER;
    sql_ddl VARCHAR;
    sql_dml VARCHAR;
    i INTEGER;
BEGIN
    -- Loop through each schema
    FOR s IN schema_list DO
        
        -- Create 20 tables per schema
        FOR i IN 1 TO 20 DO
            
            -- Dynamic table naming (e.g., PROD_DATA.STG.EBIKE_SIM_1)
            target_table := s.schema_name || '.EBIKE_SIM_' || i::VARCHAR;
            
            -- Enforce the Size Distribution Logic
            IF (i <= 2) THEN
                -- 10% Colossal (>10GB)
                row_count := 250000000; 
            ELSEIF (i <= 4) THEN
                -- 10% Medium (200-500MB)
                row_count := 5000000;   
            ELSEIF (i <= 8) THEN
                -- 20% Large (1-3GB)
                row_count := 30000000;  
            ELSE
                -- 60% Huge (>5GB)
                row_count := 100000000; 
            END IF;

            -- Build the DDL for a generic E-bike Telemetry/Event Table
            sql_ddl := 'CREATE OR REPLACE TABLE ' || target_table || ' (
                RECORD_ID VARCHAR DEFAULT UUID_STRING(),
                ENTITY_ID VARCHAR,
                EVENT_TIMESTAMP TIMESTAMP_NTZ,
                METRIC_VALUE_1 NUMBER(10,2),
                METRIC_VALUE_2 NUMBER(10,2),
                STATUS_CODE VARCHAR
            )';
            
            -- Build the DML to insert 12 years of randomized historical data
            -- 12 years = approx 105,120 hours. We spread the timestamps over this range.
            -- FIXED: Using UNIFORM to safely generate decimals up to 1000.00
            sql_dml := 'INSERT INTO ' || target_table || ' 
                (ENTITY_ID, EVENT_TIMESTAMP, METRIC_VALUE_1, METRIC_VALUE_2, STATUS_CODE)
            SELECT 
                ''ENT_'' || UNIFORM(1, 100000, RANDOM()),
                DATEADD(hour, -UNIFORM(1, 105120, RANDOM()), CURRENT_TIMESTAMP()),
                UNIFORM(0, 100000, RANDOM()) / 100.0, 
                UNIFORM(0, 100000, RANDOM()) / 100.0,
                DECODE(UNIFORM(1, 4, RANDOM()), 1, ''ACTIVE'', 2, ''IDLE'', 3, ''MAINTENANCE'', 4, ''DECOMMISSIONED'')
            FROM TABLE(GENERATOR(ROWCOUNT => ' || row_count || '))';

            -- Execute the statements dynamically
            EXECUTE IMMEDIATE sql_ddl;
            EXECUTE IMMEDIATE sql_dml;
            
        END FOR;
    END FOR;
    
    RETURN 'Success: 140 tables generated across 7 schemas with the requested size distribution.';
END;
$$;

CALL GENERATE_EBIKE_POC_TABLES();

-- Scale it back down immediately after completion to save credits
ALTER WAREHOUSE COMPUTE_WH SET WAREHOUSE_SIZE = 'XSMALL';



USE ROLE data_engineer;

USE ROLE data_engineer;



CREATE OR REPLACE PROCEDURE GENERATE_EBIKE_POC_VIEWS()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    s_name VARCHAR;
    sql_ddl VARCHAR;
    
    rand_schema_1 VARCHAR;
    rand_schema_2 VARCHAR;
    rand_idx_1 INTEGER;
    rand_idx_2 INTEGER;
    rand_days INTEGER;
    
    schema_idx INTEGER;
    i INTEGER;
BEGIN
    
    -- ==========================================
    -- PASS 1: LEVEL 0 VIEWS (Directly on Base Tables)
    -- ==========================================
    FOR schema_idx IN 1 TO 7 DO
        s_name := CASE schema_idx 
            WHEN 1 THEN 'PROD_DATA.ANALYSIS' WHEN 2 THEN 'PROD_DATA.DM' WHEN 3 THEN 'PROD_DATA.STG' 
            WHEN 4 THEN 'PROD_DATA.WEBHOOKS' WHEN 5 THEN 'LDW.RID_DM' WHEN 6 THEN 'LDW.VEH_DM' WHEN 7 THEN 'DELETED_TABLE.PUBLIC_DATA' 
        END;
        
        FOR i IN 1 TO 8 DO
            SELECT DECODE(UNIFORM(1, 7, RANDOM()), 1, 'PROD_DATA.ANALYSIS', 2, 'PROD_DATA.DM', 3, 'PROD_DATA.STG', 4, 'PROD_DATA.WEBHOOKS', 5, 'LDW.RID_DM', 6, 'LDW.VEH_DM', 7, 'DELETED_TABLE.PUBLIC_DATA'), UNIFORM(1, 20, RANDOM()), UNIFORM(1, 1600, RANDOM())
            INTO :rand_schema_1, :rand_idx_1, :rand_days;
            
            sql_ddl := 'CREATE OR REPLACE VIEW ' || s_name || '.VW_L0_SIM_' || i::VARCHAR || ' AS 
                        SELECT RECORD_ID, ENTITY_ID, EVENT_TIMESTAMP, METRIC_VALUE_1, STATUS_CODE 
                        FROM ' || rand_schema_1 || '.EBIKE_SIM_' || rand_idx_1::VARCHAR || ' 
                        WHERE EVENT_TIMESTAMP >= DATEADD(day, -' || rand_days::VARCHAR || ', CURRENT_TIMESTAMP())';
            EXECUTE IMMEDIATE sql_ddl;
        END FOR;
    END FOR;

    -- ==========================================
    -- PASS 2: LEVEL 1 VIEWS (1 Level Nested - On top of L0)
    -- ==========================================
    FOR schema_idx IN 1 TO 7 DO
        s_name := CASE schema_idx 
            WHEN 1 THEN 'PROD_DATA.ANALYSIS' WHEN 2 THEN 'PROD_DATA.DM' WHEN 3 THEN 'PROD_DATA.STG' 
            WHEN 4 THEN 'PROD_DATA.WEBHOOKS' WHEN 5 THEN 'LDW.RID_DM' WHEN 6 THEN 'LDW.VEH_DM' WHEN 7 THEN 'DELETED_TABLE.PUBLIC_DATA' 
        END;
        
        FOR i IN 1 TO 8 DO
            SELECT DECODE(UNIFORM(1, 7, RANDOM()), 1, 'PROD_DATA.ANALYSIS', 2, 'PROD_DATA.DM', 3, 'PROD_DATA.STG', 4, 'PROD_DATA.WEBHOOKS', 5, 'LDW.RID_DM', 6, 'LDW.VEH_DM', 7, 'DELETED_TABLE.PUBLIC_DATA'),
                   DECODE(UNIFORM(1, 7, RANDOM()), 1, 'PROD_DATA.ANALYSIS', 2, 'PROD_DATA.DM', 3, 'PROD_DATA.STG', 4, 'PROD_DATA.WEBHOOKS', 5, 'LDW.RID_DM', 6, 'LDW.VEH_DM', 7, 'DELETED_TABLE.PUBLIC_DATA'),
                   UNIFORM(1, 8, RANDOM()), UNIFORM(1, 8, RANDOM())
            INTO :rand_schema_1, :rand_schema_2, :rand_idx_1, :rand_idx_2;
            
            sql_ddl := 'CREATE OR REPLACE VIEW ' || s_name || '.VW_L1_SIM_' || i::VARCHAR || ' AS 
                        SELECT A.ENTITY_ID, MAX(A.METRIC_VALUE_1) AS MAX_METRIC, COUNT(A.RECORD_ID) AS EVENT_COUNT 
                        FROM ' || rand_schema_1 || '.VW_L0_SIM_' || rand_idx_1::VARCHAR || ' A
                        JOIN ' || rand_schema_2 || '.VW_L0_SIM_' || rand_idx_2::VARCHAR || ' B 
                          ON A.ENTITY_ID = B.ENTITY_ID
                        GROUP BY A.ENTITY_ID';
            EXECUTE IMMEDIATE sql_ddl;
        END FOR;
    END FOR;

    -- ==========================================
    -- PASS 3: LEVEL 2 VIEWS (2 Levels Nested - On top of L1)
    -- ==========================================
    FOR schema_idx IN 1 TO 7 DO
        s_name := CASE schema_idx 
            WHEN 1 THEN 'PROD_DATA.ANALYSIS' WHEN 2 THEN 'PROD_DATA.DM' WHEN 3 THEN 'PROD_DATA.STG' 
            WHEN 4 THEN 'PROD_DATA.WEBHOOKS' WHEN 5 THEN 'LDW.RID_DM' WHEN 6 THEN 'LDW.VEH_DM' WHEN 7 THEN 'DELETED_TABLE.PUBLIC_DATA' 
        END;
        
        FOR i IN 1 TO 16 DO
            SELECT DECODE(UNIFORM(1, 7, RANDOM()), 1, 'PROD_DATA.ANALYSIS', 2, 'PROD_DATA.DM', 3, 'PROD_DATA.STG', 4, 'PROD_DATA.WEBHOOKS', 5, 'LDW.RID_DM', 6, 'LDW.VEH_DM', 7, 'DELETED_TABLE.PUBLIC_DATA'), UNIFORM(1, 8, RANDOM()), UNIFORM(1, 10, RANDOM())
            INTO :rand_schema_1, :rand_idx_1, :rand_days; 
            
            sql_ddl := 'CREATE OR REPLACE VIEW ' || s_name || '.VW_L2_SIM_' || i::VARCHAR || ' AS 
                        SELECT ENTITY_ID, MAX_METRIC, EVENT_COUNT, 
                               (MAX_METRIC / NULLIF(EVENT_COUNT, 0)) AS PERFORMANCE_RATIO 
                        FROM ' || rand_schema_1 || '.VW_L1_SIM_' || rand_idx_1::VARCHAR || '
                        WHERE EVENT_COUNT > ' || rand_days::VARCHAR;
            EXECUTE IMMEDIATE sql_ddl;
        END FOR;
    END FOR;

    -- ==========================================
    -- PASS 4: LEVEL 3 VIEWS (3 Levels Nested - On top of L2)
    -- ==========================================
    FOR schema_idx IN 1 TO 7 DO
        s_name := CASE schema_idx 
            WHEN 1 THEN 'PROD_DATA.ANALYSIS' WHEN 2 THEN 'PROD_DATA.DM' WHEN 3 THEN 'PROD_DATA.STG' 
            WHEN 4 THEN 'PROD_DATA.WEBHOOKS' WHEN 5 THEN 'LDW.RID_DM' WHEN 6 THEN 'LDW.VEH_DM' WHEN 7 THEN 'DELETED_TABLE.PUBLIC_DATA' 
        END;
        
        FOR i IN 1 TO 8 DO
            SELECT DECODE(UNIFORM(1, 7, RANDOM()), 1, 'PROD_DATA.ANALYSIS', 2, 'PROD_DATA.DM', 3, 'PROD_DATA.STG', 4, 'PROD_DATA.WEBHOOKS', 5, 'LDW.RID_DM', 6, 'LDW.VEH_DM', 7, 'DELETED_TABLE.PUBLIC_DATA'), UNIFORM(1, 16, RANDOM())
            INTO :rand_schema_1, :rand_idx_1;
            
            sql_ddl := 'CREATE OR REPLACE VIEW ' || s_name || '.VW_L3_SIM_' || i::VARCHAR || ' AS 
                        SELECT ENTITY_ID, PERFORMANCE_RATIO, 
                               IFF(PERFORMANCE_RATIO > 50, ''HIGH_PERFORMER'', ''STANDARD'') AS ENTITY_TIER 
                        FROM ' || rand_schema_1 || '.VW_L2_SIM_' || rand_idx_1::VARCHAR || '
                        WHERE MAX_METRIC IS NOT NULL';
            EXECUTE IMMEDIATE sql_ddl;
        END FOR;
    END FOR;

    RETURN 'Success: All 280 Views created! 8 L0, 8 L1, 16 L2, and 8 L3 views generated in each of the 7 schemas.';
END;
$$;

CALL GENERATE_EBIKE_POC_VIEWS();




create database ssnowflake;
create schema ssnowflake.account_usage;

ALTER WAREHOUSE COMPUTE_WH SET WAREHOUSE_SIZE = '2X-LARGE';

-- 2. Generate 18.25 Million Rows (Exactly ~25k queries/day for 730 days)
CREATE OR REPLACE TABLE SSNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY AS
WITH RAW_GENERATOR AS (
    SELECT 
        -- Ensures even distribution of exactly 25,000 queries across every single day for 2 years
        DATEADD(minute, -(MOD(SEQ4(), 730) * 1440) - UNIFORM(1, 1440, RANDOM()), CURRENT_TIMESTAMP()) AS START_TIME,
        UNIFORM(1, 100, RANDOM()) AS Q_DIST_PCT,       
        UNIFORM(1, 7, RANDOM()) AS SCHEMA_IDX,         
        UNIFORM(1, 3, RANDOM()) AS OBJ_TYPE_IDX        
    FROM TABLE(GENERATOR(ROWCOUNT => 18250000))
),
ROUTED_QUERIES AS (
    SELECT 
        *,
        -- RIGGING THE ARCHIVAL RULES (Forcing the 60/20/15/5 outcome)
        CASE 
            WHEN Q_DIST_PCT <= 5 THEN UNIFORM(3, 4, RANDOM())     -- 5%: Routed to Medium tables -> No Archive / >5 Years
            WHEN Q_DIST_PCT <= 30 THEN UNIFORM(5, 8, RANDOM())    -- 25%: Routed to Large tables -> Archive > 2 Years
            WHEN Q_DIST_PCT <= 65 THEN UNIFORM(9, 14, RANDOM())   -- 70%: Routed to Huge/Colossal -> Archive > 6 Months
            ELSE UNIFORM(15, 20, RANDOM()) 
        END AS TABLE_IDX,
        
        -- RIGGING THE LOOKBACK DAYS
        CASE 
            WHEN Q_DIST_PCT <= 5 THEN UNIFORM(2500, 2900, RANDOM()) -- 7-8 years lookback
            WHEN Q_DIST_PCT <= 30 THEN UNIFORM(365, 730, RANDOM())  -- 1-2 years lookback
            ELSE UNIFORM(240, 300, RANDOM())                        -- 8-10 months lookback
        END AS LOOKBACK_DAYS
    FROM RAW_GENERATOR
)
SELECT 
    UUID_STRING() AS QUERY_ID,
    
    -- Dynamically routing to the correct POC databases
    'SELECT COUNT(RECORD_ID), MAX(METRIC_VALUE_1) FROM ' || 
        CASE SCHEMA_IDX 
            WHEN 1 THEN 'PROD_DATA.ANALYSIS' 
            WHEN 2 THEN 'PROD_DATA.DM' 
            WHEN 3 THEN 'PROD_DATA.STG' 
            WHEN 4 THEN 'PROD_DATA.WEBHOOKS' 
            WHEN 5 THEN 'LDW.RID_DM' 
            WHEN 6 THEN 'LDW.VEH_DM' 
            ELSE 'DELETED_TABLE.PUBLIC_DATA' 
        END || '.' ||
        CASE 
            WHEN OBJ_TYPE_IDX = 1 THEN 'EBIKE_SIM_' || TABLE_IDX::VARCHAR
            WHEN OBJ_TYPE_IDX = 2 THEN 'VW_L0_SIM_' || (MOD(TABLE_IDX, 8) + 1)::VARCHAR 
            ELSE 'VW_L2_SIM_' || (MOD(TABLE_IDX, 16) + 1)::VARCHAR 
        END ||
        ' WHERE EVENT_TIMESTAMP >= DATEADD(day, -' || LOOKBACK_DAYS::VARCHAR || ', CURRENT_TIMESTAMP());' 
    AS QUERY_TEXT,
    
    MD5(SCHEMA_IDX::VARCHAR || '_' || OBJ_TYPE_IDX::VARCHAR || '_' || TABLE_IDX::VARCHAR) AS QUERY_PARAMETERIZED_HASH,
    
    -- FIXED DATABASE MAPPING
    CASE SCHEMA_IDX 
        WHEN 1 THEN 'PROD_DATA' WHEN 2 THEN 'PROD_DATA' WHEN 3 THEN 'PROD_DATA' WHEN 4 THEN 'PROD_DATA'
        WHEN 5 THEN 'LDW' WHEN 6 THEN 'LDW' 
        ELSE 'DELETED_TABLE'
    END AS DATABASE_NAME,
    
    UNIFORM(10000, 99999, RANDOM()) AS DATABASE_ID,
    
    CASE SCHEMA_IDX 
        WHEN 1 THEN 'ANALYSIS' WHEN 2 THEN 'DM' WHEN 3 THEN 'STG' WHEN 4 THEN 'WEBHOOKS' 
        WHEN 5 THEN 'RID_DM' WHEN 6 THEN 'VEH_DM' 
        ELSE 'PUBLIC_DATA' 
    END AS SCHEMA_NAME,
    
    UNIFORM(1000, 9999, RANDOM()) AS SCHEMA_ID,
    'SELECT' AS QUERY_TYPE,
    UNIFORM(100000, 999999, RANDOM()) AS SESSION_ID,
    'ANALYST_SIMULATOR' AS USER_NAME,
    DECODE(UNIFORM(1, 4, RANDOM()), 1, 'ANALYST_ROLE', 2, 'BI_TOOL_ROLE', 3, 'DATA_ENG_ROLE', 4, 'SYSADMIN') AS ROLE_NAME,
    'SUCCESS' AS EXECUTION_STATUS,
    NULL::VARCHAR AS ERROR_CODE,
    NULL::VARCHAR AS ERROR_MESSAGE,
    START_TIME,
    DATEADD(millisecond, UNIFORM(100, 15000, RANDOM()), START_TIME) AS END_TIME,
    UNIFORM(100, 15000, RANDOM()) AS TOTAL_ELAPSED_TIME,
    UNIFORM(1000, 5000000, RANDOM()) AS BYTES_SCANNED,
    UNIFORM(0, 100, RANDOM()) AS PERCENTAGE_SCANNED_FROM_CACHE,
    0 AS BYTES_WRITTEN,
    UNIFORM(10, 50000, RANDOM()) AS BYTES_WRITTEN_TO_RESULT,
    UNIFORM(10, 50000, RANDOM()) AS BYTES_READ_FROM_RESULT,
    UNIFORM(1, 5000, RANDOM()) AS ROWS_PRODUCED,
    UNIFORM(10, 500, RANDOM()) AS COMPILATION_TIME,
    UNIFORM(50, 14000, RANDOM()) AS EXECUTION_TIME,
    0 AS QUEUED_PROVISIONING_TIME,
    0 AS QUEUED_REPAIR_TIME,
    0 AS QUEUED_OVERLOAD_TIME,
    0 AS TRANSACTION_BLOCKED_TIME,
    0 AS OUTBOUND_DATA_TRANSFER_CLOUD,
    0 AS OUTBOUND_DATA_TRANSFER_REGION,
    0 AS OUTBOUND_DATA_TRANSFER_BYTES,
    0 AS INBOUND_DATA_TRANSFER_CLOUD,
    0 AS INBOUND_DATA_TRANSFER_REGION,
    0 AS INBOUND_DATA_TRANSFER_BYTES,
    0 AS LIST_EXTERNAL_FILES_TIME,
    UNIFORM(0, 100, RANDOM()) / 100.0 AS CREDITS_USED_CLOUD_SERVICES,
    '8.4.2' AS RELEASE_VERSION,
    0 AS EXTERNAL_FUNCTION_TOTAL_INVOCATIONS,
    0 AS EXTERNAL_FUNCTION_TOTAL_SENT_ROWS,
    0 AS EXTERNAL_FUNCTION_TOTAL_RECEIVED_ROWS,
    0 AS EXTERNAL_FUNCTION_TOTAL_SENT_BYTES,
    0 AS EXTERNAL_FUNCTION_TOTAL_RECEIVED_BYTES,
    UNIFORM(0, 100, RANDOM()) AS QUERY_LOAD_PERCENT,
    FALSE AS IS_CLIENT_GENERATED_STATEMENT,
    0 AS QUERY_ACCELERATION_BYTES_SCANNED,
    0 AS QUERY_ACCELERATION_PARTITIONS_SCANNED,
    0 AS QUERY_ACCELERATION_UPPER_LIMIT_SCALE_FACTOR,
    UNIFORM(100, 999, RANDOM()) AS WAREHOUSE_ID,
    'COMPUTE_WH' AS WAREHOUSE_NAME,
    'X-Large' AS WAREHOUSE_SIZE,
    'STANDARD' AS WAREHOUSE_TYPE,
    1 AS CLUSTER_NUMBER,
    'COCO_POC_SIMULATION' AS QUERY_TAG,
    0 AS EXECUTION_POOLED_BYTES
FROM ROUTED_QUERIES;

ALTER WAREHOUSE COMPUTE_WH SET WAREHOUSE_SIZE = 'XSMALL';

select * from SSNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY limit 100;