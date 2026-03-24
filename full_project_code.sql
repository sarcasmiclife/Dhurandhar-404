-- ============================================================
-- HOSPITAL DATA PIPELINE
-- Layers: RAW → VALIDATED → CLEAN (Star Schema)
-- Features: Incremental load, deduplication, data validation
-- ============================================================


-- ============================================================
-- SECTION 1: DATABASE, WAREHOUSE & SCHEMA SETUP
-- ============================================================

-- Create the main database for the hospital data pipeline
create or replace database hospital_db;

-- Grant database-level privileges to workspace admin role
GRANT USAGE ON DATABASE hospital_db TO ROLE WORKSPACE_ADMIN_ROLE;
GRANT CREATE SCHEMA ON DATABASE hospital_db TO ROLE WORKSPACE_ADMIN_ROLE;

-- Grant access on all existing schemas
GRANT USAGE ON ALL SCHEMAS IN DATABASE hospital_db TO ROLE WORKSPACE_ADMIN_ROLE;
GRANT CREATE TABLE, CREATE VIEW, CREATE STAGE, CREATE FILE FORMAT
  ON ALL SCHEMAS IN DATABASE hospital_db TO ROLE WORKSPACE_ADMIN_ROLE;

-- Grant access on any future schemas automatically
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE hospital_db TO ROLE WORKSPACE_ADMIN_ROLE;
GRANT CREATE TABLE, CREATE VIEW, CREATE STAGE, CREATE FILE FORMAT
  ON FUTURE SCHEMAS IN DATABASE hospital_db TO ROLE WORKSPACE_ADMIN_ROLE;

-- Grant DML privileges on all existing tables
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES
  ON ALL TABLES IN DATABASE hospital_db TO ROLE WORKSPACE_ADMIN_ROLE;

-- Grant DML privileges on any future tables automatically
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES
  ON FUTURE TABLES IN DATABASE hospital_db TO ROLE WORKSPACE_ADMIN_ROLE;

-- Grant SELECT on existing and future views
GRANT SELECT ON ALL VIEWS IN DATABASE hospital_db TO ROLE WORKSPACE_ADMIN_ROLE;
GRANT SELECT ON FUTURE VIEWS IN DATABASE hospital_db TO ROLE WORKSPACE_ADMIN_ROLE;

GRANT USAGE ON DATABASE HOSPITAL_db TO ROLE WORKSPACE_ADMIN_ROLE;

-- Create a virtual warehouse for compute
create or replace warehouse compute_wh
WITH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60       -- suspend after 60 seconds of inactivity
    AUTO_RESUME = TRUE;     -- auto-resume when a query arrives


-- Create the three pipeline layer schemas: RAW → VALIDATED → CLEAN
CREATE OR REPLACE SCHEMA HOSPITAL_db.RAW;
CREATE OR REPLACE SCHEMA HOSPITAL_db.VALIDATED;
CREATE OR REPLACE SCHEMA HOSPITAL_db.CLEAN;


-- ============================================================
-- SECTION 2: RAW LAYER — TABLE DEFINITIONS
-- All columns stored as VARCHAR to preserve source data as-is.
-- Metadata columns (file_name, load_timestamp) track lineage.
-- ============================================================

-- Fact: Patient visit records (all fields as strings in raw)
CREATE OR REPLACE TABLE RAW.RAW_PATIENT_VISITS (
    VISIT_ID            VARCHAR(50),
    PATIENT_ID          VARCHAR(50),
    DOCTOR_ID           VARCHAR(50),
    DEPARTMENT_ID       VARCHAR(50),
    VISIT_DATE          VARCHAR(50),      -- kept as string in raw layer
    CONSULTATION_FEE    varchar(50),
    MEDICINE_COST       varchar(50),
    LAB_COST            varchar(50),
    TOTAL_BILL          varchar(50),
    -- metadata columns
    file_name           varchar(50),
    load_timestamp    timestamp
);

-- Fact: Diagnosis records linked to visits
CREATE OR REPLACE TABLE RAW.RAW_DIAGNOSIS (
    DIAGNOSIS_ID        VARCHAR(50),
    VISIT_ID            VARCHAR(50),
    DIAGNOSIS           VARCHAR(255),
    NOTES               VARCHAR(500),
    file_name           varchar(50),
    load_timestamp    timestamp
);

-- Fact: Billing records linked to visits
CREATE OR REPLACE TABLE RAW.RAW_BILLING (
    BILL_ID             VARCHAR(50),
    VISIT_ID            VARCHAR(50),
    BILL_AMOUNT         NUMBER(10,2),
    BILL_DATE           VARCHAR(50),      -- kept as string in raw layer
     file_name           varchar(50),
    load_timestamp    timestamp
);

-- Dimension: Patient master data
CREATE OR REPLACE TABLE RAW.RAW_DIM_PATIENT (
    PATIENT_ID          VARCHAR(50),
    FULL_NAME           VARCHAR(255),
    CITY                VARCHAR(100),
      file_name           varchar(50),
    load_timestamp    timestamp
);

-- Dimension: Doctor master data
CREATE OR REPLACE TABLE RAW.RAW_DIM_DOCTOR (
    DOCTOR_ID           VARCHAR(50),
    DOCTOR_NAME         VARCHAR(255),
    DEPARTMENT          VARCHAR(100),
     file_name           varchar(50),
    load_timestamp    timestamp
);


-- ============================================================
-- SECTION 3: FILE FORMAT & STAGE FOR CSV INGESTION
-- ============================================================

-- CSV file format: skip header, handle quotes, trim spaces, treat empty/null strings as NULL
CREATE OR REPLACE FILE FORMAT RAW.CSV_FILE_FORMAT
TYPE = 'CSV'
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
TRIM_SPACE = TRUE
EMPTY_FIELD_AS_NULL = TRUE
NULL_IF = ('NULL', 'null', '');

-- Internal stage using the CSV file format above
CREATE OR REPLACE STAGE RAW.RAW_STAGE
FILE_FORMAT = RAW.CSV_FILE_FORMAT;

-- List files uploaded to the stage (verify uploads)
list@RAW_STAGE;


-- ============================================================
-- SECTION 4: COPY INTO RAW TABLES (Data Ingestion)
-- Loads CSV files from internal stage into raw tables.
-- METADATA$FILENAME captures source file for lineage tracking.
-- ON_ERROR = 'CONTINUE' skips bad rows instead of failing.
-- ============================================================

-- Load patient visits from CSV into raw table
COPY INTO RAW.RAW_PATIENT_VISITS
FROM (
    SELECT
        $1,  -- VISIT_ID
        $2,  -- PATIENT_ID
        $3,  -- DOCTOR_ID
        $4,  -- DEPARTMENT_ID
        $5,  -- VISIT_DATE
        $6,  -- CONSULTATION_FEE
        $7,  -- MEDICINE_COST
        $8,  -- LAB_COST
        $9,  -- TOTAL_BILL
        METADATA$FILENAME,       -- source file name for lineage
        CURRENT_TIMESTAMP()      -- load timestamp
    FROM @RAW.raw_stage/RAW_PATIENT_VISITS
)
FILE_FORMAT = (FORMAT_NAME = RAW.CSV_FILE_FORMAT)
ON_ERROR = 'CONTINUE';

-- Load diagnosis records from CSV
COPY INTO RAW.RAW_DIAGNOSIS (
    DIAGNOSIS_ID,
    VISIT_ID,
    DIAGNOSIS,
    NOTES,
    file_name,
    load_timestamp
)
FROM (
    SELECT 
        $1 AS DIAGNOSIS_ID,
        $2 AS VISIT_ID,
        $3 AS DIAGNOSIS,
        $4 AS NOTES,
        METADATA$FILENAME AS file_name,
        CURRENT_TIMESTAMP() AS load_timestamp
    FROM @raw_stage/RAW_DIAGNOSIS.csv
)
FILE_FORMAT = (FORMAT_NAME = RAW.CSV_FILE_FORMAT)
ON_ERROR = 'CONTINUE';

-- Load billing records from CSV (cast BILL_AMOUNT to decimal during load)
COPY INTO RAW.RAW_BILLING
FROM (
    SELECT
        $1::VARCHAR(50)        AS BILL_ID,
        $2::VARCHAR(50)        AS VISIT_ID,
        TRY_TO_DECIMAL($3,10,2) AS BILL_AMOUNT,
        $4::VARCHAR(50)        AS BILL_DATE,
        METADATA$FILENAME::VARCHAR(50) AS file_name,
        CURRENT_TIMESTAMP()    AS load_timestamp
    FROM @raw_stage/RAW_BILLING.csv
    (FILE_FORMAT => 'RAW.CSV_FILE_FORMAT')
)
FORCE = TRUE
ON_ERROR = 'CONTINUE';
select * from raw_billing;

-- Load patient dimension from CSV
COPY INTO RAW.RAW_DIM_PATIENT (
    PATIENT_ID,
    FULL_NAME,
    CITY,
    file_name,
    load_timestamp
)
FROM (
    SELECT
        $1 AS PATIENT_ID,
        $2 AS FULL_NAME,
        $3 AS CITY,
        METADATA$FILENAME AS file_name,
        CURRENT_TIMESTAMP() AS load_timestamp
    FROM @raw_stage/RAW_DIM_PATIENT.csv
    (FILE_FORMAT => RAW.CSV_FILE_FORMAT)
)
FORCE = TRUE
ON_ERROR = 'CONTINUE';

-- Load doctor dimension from CSV
COPY INTO RAW.RAW_DIM_DOCTOR (
    DOCTOR_ID,
    DOCTOR_NAME,
    DEPARTMENT,
    file_name,
    load_timestamp
)
FROM (
    SELECT
        $1 AS DOCTOR_ID,
        $2 AS DOCTOR_NAME,
        $3 AS DEPARTMENT,
        METADATA$FILENAME AS file_name,
        CURRENT_TIMESTAMP() AS load_timestamp
    FROM @raw_stage/RAW_DIM_DOCTOR.csv
    (FILE_FORMAT => RAW.CSV_FILE_FORMAT)
)
FORCE = TRUE
ON_ERROR = 'CONTINUE';

-- Verify stage contents after load
list@raw_stage;

-- Quick check on raw patient visits data
select * from raw.RAW_PATIENT_VISITS;

-- Truncate if needed for re-load
truncate table raw.raw_patient_visits;


-- ============================================================
-- SECTION 5: VALIDATED LAYER — TABLE DEFINITIONS
-- Data types are enforced (DATE, NUMBER).
-- NOT NULL constraints on key columns.
-- RECORD_STATUS tracks validation outcome (VALID/INVALID/BAD).
-- ============================================================

use schema validated;

-- Validated patient visits with typed columns and computed totals
CREATE OR REPLACE TABLE VALIDATED.VALIDATED_PATIENT_VISITS (
    VISIT_ID             VARCHAR(50)    NOT NULL,
    PATIENT_ID           VARCHAR(50)    NOT NULL,
    DOCTOR_ID            VARCHAR(50),
    DEPARTMENT_ID        VARCHAR(50),
    VISIT_DATE           DATE,              -- standardized from raw string
    CONSULTATION_FEE     NUMBER(10,2),
    MEDICINE_COST        NUMBER(10,2),
    LAB_COST             NUMBER(10,2),
    TOTAL_CHARGES        NUMBER(10,2),      -- sum of fee + medicine + lab
    BILLING_STATUS       VARCHAR(20),       -- VALID or INVALID (negative amounts)
    RECORD_STATUS        VARCHAR(30),       -- VALID / DUPLICATE / BAD
    SOURCE_FILE_NAME     VARCHAR(255),
    LOAD_TS              TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Validated patient dimension
CREATE OR REPLACE TABLE VALIDATED.VALIDATED_DIM_PATIENT (
    PATIENT_ID           VARCHAR(50)    NOT NULL,
    FULL_NAME            VARCHAR(255)   NOT NULL,
    CITY                 VARCHAR(100),
    RECORD_STATUS        VARCHAR(30),   -- VALID / INVALID
    SOURCE_FILE_NAME     VARCHAR(255),
    LOAD_TS              TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Validated doctor dimension
CREATE OR REPLACE TABLE VALIDATED.VALIDATED_DIM_DOCTOR (
    DOCTOR_ID            VARCHAR(50)    NOT NULL,
    DOCTOR_NAME          VARCHAR(255)   NOT NULL,
    DEPARTMENT           VARCHAR(100),
    RECORD_STATUS        VARCHAR(30),   -- VALID / INVALID
    SOURCE_FILE_NAME     VARCHAR(255),
    LOAD_TS              TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Validated billing records with standardized date
CREATE OR REPLACE TABLE VALIDATED.VALIDATED_BILLING (
    BILL_ID              VARCHAR(50)    NOT NULL,
    VISIT_ID             VARCHAR(50)    NOT NULL,
    BILL_AMOUNT          NUMBER(10,2),
    BILL_DATE            DATE,              -- standardized from raw string
    BILLING_STATUS       VARCHAR(20),       -- VALID / INVALID (negative amounts)
    RECORD_STATUS        VARCHAR(30),       -- VALID / BAD
    SOURCE_FILE_NAME     VARCHAR(255),
    LOAD_TS              TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Validated diagnosis records
CREATE OR REPLACE TABLE VALIDATED.VALIDATED_DIAGNOSIS (
    DIAGNOSIS_ID         VARCHAR(50)    NOT NULL,
    VISIT_ID             VARCHAR(50)    NOT NULL,
    DIAGNOSIS            VARCHAR(255)   NOT NULL,
    NOTES                VARCHAR(500),
    RECORD_STATUS        VARCHAR(30),   -- VALID / INVALID / BAD
    SOURCE_FILE_NAME     VARCHAR(255),
    LOAD_TS              TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Quarantine table: rows that failed validation (e.g., missing patient ID)
CREATE OR REPLACE TABLE VALIDATED.BAD_PATIENT_DATA (
    VISIT_ID             VARCHAR(50),
    PATIENT_ID           VARCHAR(50),
    DOCTOR_ID            VARCHAR(50),
    DEPARTMENT_ID        VARCHAR(50),
    VISIT_DATE_RAW       VARCHAR(50),       -- original raw date string
    VISIT_DATE_STD       DATE,              -- attempted standardized date
    CONSULTATION_FEE     NUMBER(10,2),
    MEDICINE_COST        NUMBER(10,2),
    LAB_COST             NUMBER(10,2),
    TOTAL_CHARGES        NUMBER(10,2),
    ERROR_REASON         VARCHAR(255),      -- why the row was rejected
    RECORD_STATUS        VARCHAR(30),
    SOURCE_FILE_NAME     VARCHAR(255),
    LOAD_TS              TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);


-- ============================================================
-- SECTION 6: VALIDATED LAYER — MERGE STATEMENTS
-- Pattern: Incremental load + Deduplication + NULL safety
--   - Incremental: only process rows with LOAD_TIMESTAMP > MAX(LOAD_TS) in target
--   - Deduplication: ROW_NUMBER() keeps only the latest row per primary key
--   - NULL safety: WHERE filters exclude NULL/empty primary key values
-- ============================================================

use schema validated;

-- -------------------------------------------------------
-- MERGE: RAW_DIM_PATIENT → VALIDATED_DIM_PATIENT
-- Dedup on PATIENT_ID, keep latest by LOAD_TIMESTAMP
-- -------------------------------------------------------
MERGE INTO VALIDATED.VALIDATED_DIM_PATIENT T
USING (
    SELECT PATIENT_ID, FULL_NAME, CITY, RECORD_STATUS, SOURCE_FILE_NAME, LOAD_TS
    FROM (
        SELECT
            TRIM(PATIENT_ID) AS PATIENT_ID,
            TRIM(FULL_NAME) AS FULL_NAME,
            TRIM(CITY) AS CITY,
            'VALID' AS RECORD_STATUS,
            FILE_NAME AS SOURCE_FILE_NAME,
            LOAD_TIMESTAMP AS LOAD_TS,
            -- Deduplication: rank rows per PATIENT_ID, latest first
            ROW_NUMBER() OVER (PARTITION BY TRIM(PATIENT_ID) ORDER BY LOAD_TIMESTAMP DESC) AS RN
        FROM RAW.RAW_DIM_PATIENT
        WHERE PATIENT_ID IS NOT NULL          -- NULL safety: skip rows with missing PK
          AND TRIM(PATIENT_ID) <> ''
          AND FULL_NAME IS NOT NULL
          AND TRIM(FULL_NAME) <> ''
          -- Incremental: only process new rows since last load
          AND LOAD_TIMESTAMP > COALESCE(
                (SELECT MAX(LOAD_TS) FROM VALIDATED.VALIDATED_DIM_PATIENT),
                '1900-01-01'::TIMESTAMP)
    ) DEDUPED
    WHERE RN = 1                              -- Keep only the latest row per PATIENT_ID
) S
ON T.PATIENT_ID = S.PATIENT_ID
WHEN MATCHED THEN UPDATE SET
    T.FULL_NAME = S.FULL_NAME,
    T.CITY = S.CITY,
    T.RECORD_STATUS = S.RECORD_STATUS,
    T.SOURCE_FILE_NAME = S.SOURCE_FILE_NAME,
    T.LOAD_TS = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    PATIENT_ID, FULL_NAME, CITY, RECORD_STATUS, SOURCE_FILE_NAME, LOAD_TS
)
VALUES (
    S.PATIENT_ID, S.FULL_NAME, S.CITY, S.RECORD_STATUS, S.SOURCE_FILE_NAME, CURRENT_TIMESTAMP()
);

-- -------------------------------------------------------
-- MERGE: RAW_DIM_DOCTOR → VALIDATED_DIM_DOCTOR
-- Dedup on DOCTOR_ID, keep latest by LOAD_TIMESTAMP
-- -------------------------------------------------------
MERGE INTO VALIDATED.VALIDATED_DIM_DOCTOR T
USING (
    SELECT DOCTOR_ID, DOCTOR_NAME, DEPARTMENT, RECORD_STATUS, SOURCE_FILE_NAME, LOAD_TS
    FROM (
        SELECT
            TRIM(DOCTOR_ID) AS DOCTOR_ID,
            TRIM(DOCTOR_NAME) AS DOCTOR_NAME,
            TRIM(DEPARTMENT) AS DEPARTMENT,
            'VALID' AS RECORD_STATUS,
            FILE_NAME AS SOURCE_FILE_NAME,
            LOAD_TIMESTAMP AS LOAD_TS,
            -- Deduplication: rank rows per DOCTOR_ID, latest first
            ROW_NUMBER() OVER (PARTITION BY TRIM(DOCTOR_ID) ORDER BY LOAD_TIMESTAMP DESC) AS RN
        FROM RAW.RAW_DIM_DOCTOR
        WHERE DOCTOR_ID IS NOT NULL           -- NULL safety
          AND TRIM(DOCTOR_ID) <> ''
          AND DOCTOR_NAME IS NOT NULL
          AND TRIM(DOCTOR_NAME) <> ''
          -- Incremental: only process new rows since last load
          AND LOAD_TIMESTAMP > COALESCE(
                (SELECT MAX(LOAD_TS) FROM VALIDATED.VALIDATED_DIM_DOCTOR),
                '1900-01-01'::TIMESTAMP)
    ) DEDUPED
    WHERE RN = 1                              -- Keep only the latest row per DOCTOR_ID
) S
ON T.DOCTOR_ID = S.DOCTOR_ID
WHEN MATCHED THEN UPDATE SET
    T.DOCTOR_NAME = S.DOCTOR_NAME,
    T.DEPARTMENT = S.DEPARTMENT,
    T.RECORD_STATUS = S.RECORD_STATUS,
    T.SOURCE_FILE_NAME = S.SOURCE_FILE_NAME,
    T.LOAD_TS = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    DOCTOR_ID, DOCTOR_NAME, DEPARTMENT, RECORD_STATUS, SOURCE_FILE_NAME, LOAD_TS
)
VALUES (
    S.DOCTOR_ID, S.DOCTOR_NAME, S.DEPARTMENT, S.RECORD_STATUS, S.SOURCE_FILE_NAME, CURRENT_TIMESTAMP()

);


-- ============================================================
-- SECTION 7: VALIDATION UDF — FN_VALIDATE_PATIENT_VISITS
-- Returns an OBJECT with:
--   VISIT_DATE     → standardized date (DD-MM-YYYY or YYYY-MM-DD)
--   LOOKUP_STATUS  → checks for missing patient/doctor/department
--   TOTAL_CHARGES  → sum of consultation + medicine + lab costs
--   STATUS         → BAD (missing patient), INVALID (negative fees), or VALID
-- ============================================================

CREATE OR REPLACE FUNCTION VALIDATED.FN_VALIDATE_PATIENT_VISITS(
    P_PATIENT_ID STRING,
    P_DOCTOR_ID STRING,
    P_DEPARTMENT_ID STRING,
    P_VISIT_DATE STRING,
    P_CONSULTATION_FEE STRING,
    P_MEDICINE_COST STRING,
    P_LAB_COST STRING
)
RETURNS OBJECT
LANGUAGE SQL
AS
$$
OBJECT_CONSTRUCT(

    /* 1. Fix Date Format — try DD-MM-YYYY first, then YYYY-MM-DD */
    'VISIT_DATE',
        COALESCE(
            TRY_TO_DATE(P_VISIT_DATE, 'DD-MM-YYYY'),
            TRY_TO_DATE(P_VISIT_DATE, 'YYYY-MM-DD')
        ),

    /* 2. Lookup Check — verify required IDs are present */
    'LOOKUP_STATUS',
        CASE
            WHEN P_PATIENT_ID IS NULL OR TRIM(P_PATIENT_ID) = '' THEN 'MISSING_PATIENT'
            WHEN P_DOCTOR_ID IS NULL OR TRIM(P_DOCTOR_ID) = '' THEN 'MISSING_DOCTOR'
            WHEN P_DEPARTMENT_ID IS NULL OR TRIM(P_DEPARTMENT_ID) = '' THEN 'MISSING_DEPARTMENT'
            ELSE 'OK'
        END,

    /* 3. Calculate Total Charges — sum all cost components, treat NULLs as 0 */
    'TOTAL_CHARGES',
        COALESCE(TRY_TO_DECIMAL(P_CONSULTATION_FEE,10,2),0)
      + COALESCE(TRY_TO_DECIMAL(P_MEDICINE_COST,10,2),0)
      + COALESCE(TRY_TO_DECIMAL(P_LAB_COST,10,2),0),

    /* 4. Final Validation Status */
    'STATUS',
        CASE
            /* Missing Patient ID → route to BAD_PATIENT_DATA table */
            WHEN P_PATIENT_ID IS NULL OR TRIM(P_PATIENT_ID) = ''
                THEN 'BAD'

            /* Negative billing amounts → mark as INVALID but still load */
            WHEN TRY_TO_DECIMAL(P_CONSULTATION_FEE,10,2) < 0
              OR TRY_TO_DECIMAL(P_MEDICINE_COST,10,2) < 0
              OR TRY_TO_DECIMAL(P_LAB_COST,10,2) < 0
                THEN 'INVALID'

            ELSE 'VALID'
        END
)
$$;


-- ============================================================
-- SECTION 8: BAD DATA QUARANTINE
-- Rows where STATUS = 'BAD' (missing patient ID) go here
-- for manual review and correction.
-- ============================================================

INSERT INTO VALIDATED.BAD_PATIENT_DATA (
    VISIT_ID,
    PATIENT_ID,
    DOCTOR_ID,
    DEPARTMENT_ID,
    VISIT_DATE_RAW,
    VISIT_DATE_STD,
    CONSULTATION_FEE,
    MEDICINE_COST,
    LAB_COST,
    TOTAL_CHARGES,
    ERROR_REASON,
    RECORD_STATUS,
    SOURCE_FILE_NAME,
    LOAD_TS
)
SELECT
    R.VISIT_ID,
    R.PATIENT_ID,
    R.DOCTOR_ID,
    R.DEPARTMENT_ID,
    R.VISIT_DATE,                              -- original raw date string
    FN_VALIDATE_PATIENT_VISITS(                -- standardized date via UDF
        R.PATIENT_ID,
        R.DOCTOR_ID,
        R.DEPARTMENT_ID,
        R.VISIT_DATE,
        R.CONSULTATION_FEE,
        R.MEDICINE_COST,
        R.LAB_COST
    ):"VISIT_DATE"::DATE,
    TRY_TO_DECIMAL(R.CONSULTATION_FEE,10,2),
    TRY_TO_DECIMAL(R.MEDICINE_COST,10,2),
    TRY_TO_DECIMAL(R.LAB_COST,10,2),
    FN_VALIDATE_PATIENT_VISITS(                -- calculated total via UDF
        R.PATIENT_ID,
        R.DOCTOR_ID,
        R.DEPARTMENT_ID,
        R.VISIT_DATE,
        R.CONSULTATION_FEE,
        R.MEDICINE_COST,
        R.LAB_COST
    ):"TOTAL_CHARGES"::NUMBER(10,2),
    'Missing Patient ID',                      -- error reason
    'BAD',
    R.FILE_NAME,
    CURRENT_TIMESTAMP()
FROM RAW.RAW_PATIENT_VISITS R
WHERE FN_VALIDATE_PATIENT_VISITS(              -- only rows where UDF returns 'BAD'
        R.PATIENT_ID,
        R.DOCTOR_ID,
        R.DEPARTMENT_ID,
        R.VISIT_DATE,
        R.CONSULTATION_FEE,
        R.MEDICINE_COST,
        R.LAB_COST
      ):"STATUS"::STRING = 'BAD';


-- -------------------------------------------------------
-- MERGE: RAW_PATIENT_VISITS → VALIDATED_PATIENT_VISITS
-- Uses UDF for date standardization, total calculation, and status
-- Dedup on VISIT_ID, incremental by LOAD_TIMESTAMP
-- -------------------------------------------------------
MERGE INTO VALIDATED.VALIDATED_PATIENT_VISITS T
USING (
    SELECT
        VISIT_ID,
        PATIENT_ID,
        DOCTOR_ID,
        DEPARTMENT_ID,
        VISIT_DATE,
        CONSULTATION_FEE,
        MEDICINE_COST,
        LAB_COST,
        TOTAL_CHARGES,
        BILLING_STATUS,
        RECORD_STATUS,
        SOURCE_FILE_NAME,
        LOAD_TS
    FROM (
        SELECT
            TRIM(R.VISIT_ID) AS VISIT_ID,
            TRIM(R.PATIENT_ID) AS PATIENT_ID,
            TRIM(R.DOCTOR_ID) AS DOCTOR_ID,
            TRIM(R.DEPARTMENT_ID) AS DEPARTMENT_ID,
            -- Standardize visit date using validation UDF
            FN_VALIDATE_PATIENT_VISITS(
                R.PATIENT_ID,
                R.DOCTOR_ID,
                R.DEPARTMENT_ID,
                R.VISIT_DATE,
                R.CONSULTATION_FEE,
                R.MEDICINE_COST,
                R.LAB_COST
            ):"VISIT_DATE"::DATE AS VISIT_DATE,
            TRY_TO_DECIMAL(R.CONSULTATION_FEE,10,2) AS CONSULTATION_FEE,
            TRY_TO_DECIMAL(R.MEDICINE_COST,10,2) AS MEDICINE_COST,
            TRY_TO_DECIMAL(R.LAB_COST,10,2) AS LAB_COST,
            -- Calculate total charges using validation UDF
            FN_VALIDATE_PATIENT_VISITS(
                R.PATIENT_ID,
                R.DOCTOR_ID,
                R.DEPARTMENT_ID,
                R.VISIT_DATE,
                R.CONSULTATION_FEE,
                R.MEDICINE_COST,
                R.LAB_COST
            ):"TOTAL_CHARGES"::NUMBER(10,2) AS TOTAL_CHARGES,
            -- Determine billing status: INVALID if UDF says so, else VALID
            CASE
                WHEN FN_VALIDATE_PATIENT_VISITS(
                        R.PATIENT_ID,
                        R.DOCTOR_ID,
                        R.DEPARTMENT_ID,
                        R.VISIT_DATE,
                        R.CONSULTATION_FEE,
                        R.MEDICINE_COST,
                        R.LAB_COST
                     ):"STATUS"::STRING = 'INVALID'
                THEN 'INVALID'
                ELSE 'VALID'
            END AS BILLING_STATUS,
            'VALIDATED' AS RECORD_STATUS,
            R.FILE_NAME AS SOURCE_FILE_NAME,
            CURRENT_TIMESTAMP() AS LOAD_TS,
            -- Deduplication: rank rows per VISIT_ID, latest first
            ROW_NUMBER() OVER (PARTITION BY TRIM(R.VISIT_ID) ORDER BY R.LOAD_TIMESTAMP DESC) AS RN
        FROM RAW.RAW_PATIENT_VISITS R
        WHERE R.VISIT_ID IS NOT NULL           -- NULL safety
          AND TRIM(R.VISIT_ID) <> ''
          AND R.PATIENT_ID IS NOT NULL
          AND TRIM(R.PATIENT_ID) <> ''
          -- Exclude BAD rows (those go to BAD_PATIENT_DATA table)
          AND FN_VALIDATE_PATIENT_VISITS(
                R.PATIENT_ID,
                R.DOCTOR_ID,
                R.DEPARTMENT_ID,
                R.VISIT_DATE,
                R.CONSULTATION_FEE,
                R.MEDICINE_COST,
                R.LAB_COST
              ):"STATUS"::STRING <> 'BAD'
          -- Incremental: only process new rows since last load
          AND R.LOAD_TIMESTAMP > COALESCE(
                (SELECT MAX(LOAD_TS) FROM VALIDATED.VALIDATED_PATIENT_VISITS),
                '1900-01-01'::TIMESTAMP)
    ) DEDUPED
    WHERE RN = 1                               -- Keep only the latest row per VISIT_ID
) S
ON T.VISIT_ID = S.VISIT_ID
WHEN MATCHED THEN UPDATE SET
    T.PATIENT_ID = S.PATIENT_ID,
    T.DOCTOR_ID = S.DOCTOR_ID,
    T.DEPARTMENT_ID = S.DEPARTMENT_ID,
    T.VISIT_DATE = S.VISIT_DATE,
    T.CONSULTATION_FEE = S.CONSULTATION_FEE,
    T.MEDICINE_COST = S.MEDICINE_COST,
    T.LAB_COST = S.LAB_COST,
    T.TOTAL_CHARGES = S.TOTAL_CHARGES,
    T.BILLING_STATUS = S.BILLING_STATUS,
    T.RECORD_STATUS = S.RECORD_STATUS,
    T.SOURCE_FILE_NAME = S.SOURCE_FILE_NAME,
    T.LOAD_TS = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    VISIT_ID,
    PATIENT_ID,
    DOCTOR_ID,
    DEPARTMENT_ID,
    VISIT_DATE,
    CONSULTATION_FEE,
    MEDICINE_COST,
    LAB_COST,
    TOTAL_CHARGES,
    BILLING_STATUS,
    RECORD_STATUS,
    SOURCE_FILE_NAME,
    LOAD_TS
)
VALUES (
    S.VISIT_ID,
    S.PATIENT_ID,
    S.DOCTOR_ID,
    S.DEPARTMENT_ID,
    S.VISIT_DATE,
    S.CONSULTATION_FEE,
    S.MEDICINE_COST,
    S.LAB_COST,
    S.TOTAL_CHARGES,
    S.BILLING_STATUS,
    S.RECORD_STATUS,
    S.SOURCE_FILE_NAME,
    S.LOAD_TS
);


-- -------------------------------------------------------
-- MERGE: RAW_BILLING → VALIDATED_BILLING
-- Standardizes BILL_DATE, flags negative amounts as INVALID
-- Dedup on BILL_ID, incremental by LOAD_TIMESTAMP
-- -------------------------------------------------------
MERGE INTO VALIDATED.VALIDATED_BILLING T
USING (
    SELECT BILL_ID, VISIT_ID, BILL_AMOUNT, BILL_DATE, BILLING_STATUS, RECORD_STATUS, SOURCE_FILE_NAME, LOAD_TS
    FROM (
        SELECT
            TRIM(BILL_ID) AS BILL_ID,
            TRIM(VISIT_ID) AS VISIT_ID,
            BILL_AMOUNT,
            -- Standardize date: try DD-MM-YYYY first, then YYYY-MM-DD
            COALESCE(
                TRY_TO_DATE(BILL_DATE, 'DD-MM-YYYY'),
                TRY_TO_DATE(BILL_DATE, 'YYYY-MM-DD')
            ) AS BILL_DATE,
            -- Flag negative amounts as INVALID
            CASE
                WHEN BILL_AMOUNT < 0 THEN 'INVALID'
                ELSE 'VALID'
            END AS BILLING_STATUS,
            'VALIDATED' AS RECORD_STATUS,
            FILE_NAME AS SOURCE_FILE_NAME,
            CURRENT_TIMESTAMP() AS LOAD_TS,
            -- Deduplication: rank rows per BILL_ID, latest first
            ROW_NUMBER() OVER (PARTITION BY TRIM(BILL_ID) ORDER BY LOAD_TIMESTAMP DESC) AS RN
        FROM RAW.RAW_BILLING
        WHERE BILL_ID IS NOT NULL              -- NULL safety
          AND TRIM(BILL_ID) <> ''
          AND VISIT_ID IS NOT NULL
          AND TRIM(VISIT_ID) <> ''
          -- Incremental: only process new rows since last load
          AND LOAD_TIMESTAMP > COALESCE(
                (SELECT MAX(LOAD_TS) FROM VALIDATED.VALIDATED_BILLING),
                '1900-01-01'::TIMESTAMP)
    ) DEDUPED
    WHERE RN = 1                               -- Keep only the latest row per BILL_ID
) S
ON T.BILL_ID = S.BILL_ID
WHEN MATCHED THEN UPDATE SET
    T.VISIT_ID = S.VISIT_ID,
    T.BILL_AMOUNT = S.BILL_AMOUNT,
    T.BILL_DATE = S.BILL_DATE,
    T.BILLING_STATUS = S.BILLING_STATUS,
    T.RECORD_STATUS = S.RECORD_STATUS,
    T.SOURCE_FILE_NAME = S.SOURCE_FILE_NAME,
    T.LOAD_TS = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    BILL_ID, VISIT_ID, BILL_AMOUNT, BILL_DATE,
    BILLING_STATUS, RECORD_STATUS, SOURCE_FILE_NAME, LOAD_TS
)
VALUES (
    S.BILL_ID, S.VISIT_ID, S.BILL_AMOUNT, S.BILL_DATE,
    S.BILLING_STATUS, S.RECORD_STATUS, S.SOURCE_FILE_NAME, S.LOAD_TS
);

-- -------------------------------------------------------
-- MERGE: RAW_DIAGNOSIS → VALIDATED_DIAGNOSIS
-- Filters out rows with NULL/empty DIAGNOSIS (NOT NULL constraint)
-- Dedup on DIAGNOSIS_ID, incremental by LOAD_TIMESTAMP
-- -------------------------------------------------------
MERGE INTO VALIDATED.VALIDATED_DIAGNOSIS T
USING (
    SELECT DIAGNOSIS_ID, VISIT_ID, DIAGNOSIS, NOTES, RECORD_STATUS, SOURCE_FILE_NAME, LOAD_TS
    FROM (
        SELECT
            TRIM(DIAGNOSIS_ID) AS DIAGNOSIS_ID,
            TRIM(VISIT_ID) AS VISIT_ID,
            TRIM(DIAGNOSIS) AS DIAGNOSIS,
            TRIM(NOTES) AS NOTES,
            -- Mark rows with empty diagnosis as INVALID
            CASE
                WHEN DIAGNOSIS IS NULL OR TRIM(DIAGNOSIS) = '' THEN 'INVALID'
                ELSE 'VALID'
            END AS RECORD_STATUS,
            FILE_NAME AS SOURCE_FILE_NAME,
            CURRENT_TIMESTAMP() AS LOAD_TS,
            -- Deduplication: rank rows per DIAGNOSIS_ID, latest first
            ROW_NUMBER() OVER (PARTITION BY TRIM(DIAGNOSIS_ID) ORDER BY LOAD_TIMESTAMP DESC) AS RN
        FROM RAW.RAW_DIAGNOSIS
        WHERE DIAGNOSIS_ID IS NOT NULL         -- NULL safety for NOT NULL columns
          AND TRIM(DIAGNOSIS_ID) <> ''
          AND VISIT_ID IS NOT NULL
          AND TRIM(VISIT_ID) <> ''
          AND DIAGNOSIS IS NOT NULL
          AND TRIM(DIAGNOSIS) <> ''
          -- Incremental: only process new rows since last load
          AND LOAD_TIMESTAMP > COALESCE(
                (SELECT MAX(LOAD_TS) FROM VALIDATED.VALIDATED_DIAGNOSIS),
                '1900-01-01'::TIMESTAMP)
    ) DEDUPED
    WHERE RN = 1                               -- Keep only the latest row per DIAGNOSIS_ID
) S
ON T.DIAGNOSIS_ID = S.DIAGNOSIS_ID
WHEN MATCHED THEN UPDATE SET
    T.VISIT_ID = S.VISIT_ID,
    T.DIAGNOSIS = S.DIAGNOSIS,
    T.NOTES = S.NOTES,
    T.RECORD_STATUS = S.RECORD_STATUS,
    T.SOURCE_FILE_NAME = S.SOURCE_FILE_NAME,
    T.LOAD_TS = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    DIAGNOSIS_ID, VISIT_ID, DIAGNOSIS, NOTES,
    RECORD_STATUS, SOURCE_FILE_NAME, LOAD_TS
)
VALUES (
    S.DIAGNOSIS_ID, S.VISIT_ID, S.DIAGNOSIS, S.NOTES,
    S.RECORD_STATUS, S.SOURCE_FILE_NAME, S.LOAD_TS
);

-- Verify validated layer data
SELECT * FROM VALIDATED.VALIDATED_DIM_PATIENT;
SELECT * FROM VALIDATED.VALIDATED_DIM_DOCTOR;
SELECT * FROM VALIDATED.VALIDATED_PATIENT_VISITS;
SELECT * FROM VALIDATED.VALIDATED_BILLING;
SELECT * FROM VALIDATED.VALIDATED_DIAGNOSIS;
SELECT * FROM VALIDATED.BAD_PATIENT_DATA;


-- ============================================================
-- SECTION 9: CLEAN LAYER — STAR SCHEMA (Dimensional Model)
-- DIM_PATIENT and DIM_DOCTOR have surrogate keys (AUTOINCREMENT).
-- FACT_PATIENT_VISITS joins to dimensions via surrogate keys.
-- ============================================================

use schema clean;

-- Dimension: Patient with surrogate key (PATIENT_KEY)
CREATE OR REPLACE TABLE CLEAN.DIM_PATIENT (
    PATIENT_KEY          NUMBER AUTOINCREMENT START 1 INCREMENT 1,  -- surrogate key
    PATIENT_ID           VARCHAR(50) NOT NULL,                      -- natural key
    FULL_NAME            VARCHAR(255),
    CITY                 VARCHAR(100),
    SOURCE_FILE_NAME     VARCHAR(255),
    LOAD_TS              TIMESTAMP,
    CONSTRAINT PK_DIM_PATIENT PRIMARY KEY (PATIENT_KEY)
);

-- Dimension: Doctor with surrogate key (DOCTOR_KEY)
CREATE OR REPLACE TABLE CLEAN.DIM_DOCTOR (
    DOCTOR_KEY           NUMBER AUTOINCREMENT START 1 INCREMENT 1,  -- surrogate key
    DOCTOR_ID            VARCHAR(50) NOT NULL,                      -- natural key
    DOCTOR_NAME          VARCHAR(255),
    DEPARTMENT           VARCHAR(100),
    SOURCE_FILE_NAME     VARCHAR(255),
    LOAD_TS              TIMESTAMP,
    CONSTRAINT PK_DIM_DOCTOR PRIMARY KEY (DOCTOR_KEY)
);

-- Fact: Patient visits linked to dimensions via surrogate keys
CREATE OR REPLACE TABLE CLEAN.FACT_PATIENT_VISITS (
    VISIT_KEY            NUMBER AUTOINCREMENT START 1 INCREMENT 1,  -- surrogate key
    VISIT_ID             VARCHAR(50) NOT NULL,                      -- natural key
    PATIENT_KEY          NUMBER,            -- FK → DIM_PATIENT
    DOCTOR_KEY           NUMBER,            -- FK → DIM_DOCTOR
    DEPARTMENT_KEY       NUMBER,
    VISIT_DATE           DATE,
    TOTAL_CHARGES        NUMBER(10,2),
    SOURCE_FILE_NAME     VARCHAR(255),
    LOAD_TS              TIMESTAMP,
    CONSTRAINT PK_FACT_PATIENT_VISITS PRIMARY KEY (VISIT_KEY)
);


-- ============================================================
-- SECTION 10: CLEAN LAYER — MERGE STATEMENTS
-- Loads validated data into star schema dimensions and fact table.
-- Only VALID records are promoted to clean layer.
-- ============================================================

-- -------------------------------------------------------
-- MERGE: VALIDATED_DIM_PATIENT → CLEAN.DIM_PATIENT
-- Only loads records with RECORD_STATUS = 'VALID'
-- -------------------------------------------------------
MERGE INTO CLEAN.DIM_PATIENT T
USING (
    SELECT
        PATIENT_ID,
        FULL_NAME,
        CITY,
        SOURCE_FILE_NAME
    FROM VALIDATED.VALIDATED_DIM_PATIENT
    WHERE RECORD_STATUS = 'VALID'              -- only promote valid records
) S
ON T.PATIENT_ID = S.PATIENT_ID
WHEN MATCHED THEN UPDATE SET
    T.FULL_NAME = S.FULL_NAME,
    T.CITY = S.CITY,
    T.SOURCE_FILE_NAME = S.SOURCE_FILE_NAME,
    T.LOAD_TS = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    PATIENT_ID,
    FULL_NAME,
    CITY,
    SOURCE_FILE_NAME,
    LOAD_TS
)
VALUES (
    S.PATIENT_ID,
    S.FULL_NAME,
    S.CITY,
    S.SOURCE_FILE_NAME,
    CURRENT_TIMESTAMP()
);

-- -------------------------------------------------------
-- MERGE: VALIDATED_DIM_DOCTOR → CLEAN.DIM_DOCTOR
-- Only loads records with RECORD_STATUS = 'VALID'
-- -------------------------------------------------------
MERGE INTO CLEAN.DIM_DOCTOR T
USING (
    SELECT
        DOCTOR_ID,
        DOCTOR_NAME,
        DEPARTMENT,
        SOURCE_FILE_NAME
    FROM VALIDATED.VALIDATED_DIM_DOCTOR
    WHERE RECORD_STATUS = 'VALID'              -- only promote valid records
) S
ON T.DOCTOR_ID = S.DOCTOR_ID
WHEN MATCHED THEN UPDATE SET
    T.DOCTOR_NAME = S.DOCTOR_NAME,
    T.DEPARTMENT = S.DEPARTMENT,
    T.SOURCE_FILE_NAME = S.SOURCE_FILE_NAME,
    T.LOAD_TS = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    DOCTOR_ID,
    DOCTOR_NAME,
    DEPARTMENT,
    SOURCE_FILE_NAME,
    LOAD_TS
)
VALUES (
    S.DOCTOR_ID,
    S.DOCTOR_NAME,
    S.DEPARTMENT,
    S.SOURCE_FILE_NAME,
    CURRENT_TIMESTAMP()
);

-- -------------------------------------------------------
-- MERGE: VALIDATED_PATIENT_VISITS → CLEAN.FACT_PATIENT_VISITS
-- Joins to DIM_PATIENT and DIM_DOCTOR to resolve surrogate keys.
-- Only loads records with RECORD_STATUS = 'VALIDATED'.
-- -------------------------------------------------------
MERGE INTO CLEAN.FACT_PATIENT_VISITS T
USING (
    SELECT
        V.VISIT_ID,
        P.PATIENT_KEY,                         -- surrogate key from DIM_PATIENT
        D.DOCTOR_KEY,                          -- surrogate key from DIM_DOCTOR
        V.VISIT_DATE,
        V.TOTAL_CHARGES,
        V.SOURCE_FILE_NAME
    FROM VALIDATED.VALIDATED_PATIENT_VISITS V
    LEFT JOIN CLEAN.DIM_PATIENT P              -- resolve PATIENT_ID → PATIENT_KEY
        ON V.PATIENT_ID = P.PATIENT_ID
    LEFT JOIN CLEAN.DIM_DOCTOR D               -- resolve DOCTOR_ID → DOCTOR_KEY
        ON V.DOCTOR_ID = D.DOCTOR_ID
    WHERE V.RECORD_STATUS = 'VALIDATED'        -- only promote validated records
) S
ON T.VISIT_ID = S.VISIT_ID
WHEN MATCHED THEN UPDATE SET
    T.PATIENT_KEY = S.PATIENT_KEY,
    T.DOCTOR_KEY = S.DOCTOR_KEY,
    T.VISIT_DATE = S.VISIT_DATE,
    T.TOTAL_CHARGES = S.TOTAL_CHARGES,
    T.SOURCE_FILE_NAME = S.SOURCE_FILE_NAME,
    T.LOAD_TS = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    VISIT_ID,
    PATIENT_KEY,
    DOCTOR_KEY,
    VISIT_DATE,
    TOTAL_CHARGES,
    SOURCE_FILE_NAME,
    LOAD_TS
)
VALUES (
    S.VISIT_ID,
    S.PATIENT_KEY,
    S.DOCTOR_KEY,
    S.VISIT_DATE,
    S.TOTAL_CHARGES,
    S.SOURCE_FILE_NAME,
    CURRENT_TIMESTAMP()
);

-- Drop unused column from fact table
ALTER TABLE CLEAN.FACT_PATIENT_VISITS drop COLUMN DEPARTMENT_KEY;

-- Verify clean layer data
SELECT * FROM CLEAN.FACT_PATIENT_VISITS;


-- ============================================================
-- SECTION 11: KPI QUERIES — Business Analytics
-- ============================================================

-- -------------------------------------------------------
-- KPI 1: Doctor Productivity — visits and revenue per doctor
-- -------------------------------------------------------
SELECT
    D.DOCTOR_NAME,
    COUNT(F.VISIT_ID) AS TOTAL_VISITS,
    SUM(F.TOTAL_CHARGES) AS TOTAL_REVENUE
FROM CLEAN.FACT_PATIENT_VISITS F
JOIN CLEAN.DIM_DOCTOR D
    ON F.DOCTOR_KEY = D.DOCTOR_KEY
GROUP BY D.DOCTOR_NAME
ORDER BY TOTAL_VISITS DESC, TOTAL_REVENUE DESC;

-- -------------------------------------------------------
-- KPI 2: Average Revenue Per Visit by Doctor
-- -------------------------------------------------------
SELECT
    D.DOCTOR_NAME, AVG(TOTAL_CHARGES) AS AVG_REVENUE_PER_VISIT
FROM CLEAN.FACT_PATIENT_VISITS F
JOIN CLEAN.DIM_DOCTOR D 
ON D.DOCTOR_ID = F.DOCTOR_KEY
GROUP BY DOCTOR_NAME;

-- -------------------------------------------------------
-- KPI 3: High Billing Alerts — visits exceeding $1500
-- -------------------------------------------------------
SELECT *
FROM CLEAN.FACT_PATIENT_VISITS
WHERE TOTAL_CHARGES > 1500
ORDER BY TOTAL_CHARGES DESC;

-- -------------------------------------------------------
-- KPI 4: Department Performance — visits and revenue by department
-- -------------------------------------------------------
SELECT
    D.DEPARTMENT,
    COUNT(F.VISIT_ID) AS TOTAL_VISITS,
    SUM(F.TOTAL_CHARGES) AS TOTAL_REVENUE
FROM CLEAN.FACT_PATIENT_VISITS F
JOIN CLEAN.DIM_DOCTOR D
    ON F.DOCTOR_KEY = D.DOCTOR_KEY
GROUP BY D.DEPARTMENT
ORDER BY TOTAL_REVENUE DESC;     -- add LIMIT 3 for top performing departments

-- -------------------------------------------------------
-- KPI 5: Repetitive Patients — patients with most visits
-- -------------------------------------------------------
SELECT
    P.FULL_NAME,
    COUNT(F.VISIT_KEY) AS TOTAL_VISITS
FROM CLEAN.DIM_PATIENT P 
JOIN CLEAN.FACT_PATIENT_VISITS F 
    ON P.PATIENT_KEY = F.PATIENT_KEY 
GROUP BY P.FULL_NAME
ORDER BY TOTAL_VISITS DESC;