-- ===============================================================================
-- 0. CLEANUP (OPTIONAL - USE WITH CAUTION)
-- ===============================================================================
-- Uncomment this line if you want to perform a full reset of the environment.
-- DROP DATABASE IF EXISTS SPAIN_HOUSING_DB;

-- ===============================================================================
-- 1. INITIAL CONFIGURATION (DATABASE, WAREHOUSE, SCHEMA)
-- ===============================================================================
USE ROLE ACCOUNTADMIN; -- Or the role with sufficient privileges

CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WITH WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

CREATE DATABASE IF NOT EXISTS SPAIN_HOUSING_DB;
USE DATABASE SPAIN_HOUSING_DB;

CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS ANALYTICS;

-- ===============================================================================
-- 2. FILE FORMATS
-- ===============================================================================
CREATE OR REPLACE FILE FORMAT RAW.PARQUET_FORMAT
    TYPE = 'PARQUET'
    COMPRESSION = 'SNAPPY'
    BINARY_AS_TEXT = FALSE; 

-- ===============================================================================
-- 3. S3 INTEGRATION (EXTERNAL STAGE)
-- ===============================================================================
-- IMPORTANT: Replace 'YOUR_ACCESS_KEY' and 'YOUR_SECRET_KEY' with your actual AWS credentials.

CREATE OR REPLACE STAGE RAW.S3_LAKE_STAGE
    URL = 's3://spain-housing-datalake/raw/'
    CREDENTIALS = (AWS_KEY_ID='YOUR_ACCESS_KEY' AWS_SECRET_KEY='YOUR_SECRET_KEY')
    FILE_FORMAT = RAW.PARQUET_FORMAT;

-- ===============================================================================
-- 4. RAW TABLES (DDL) - ENGLISH NAMING CONVENTION
-- ===============================================================================

-- 4.1. POPULATION (INE)
-- S3 Prefix: raw/population/
CREATE OR REPLACE TABLE RAW.RAW_POPULATION (
    municipality_residence VARCHAR,  -- Was: municipio_residencia
    sex VARCHAR,                     -- Was: sexo
    period INT,                      -- Was: periodo
    total INT,                       -- Was: total
    __LOADED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- 4.2. REAL ESTATE VALUATIONS (FOMENTO)
-- S3 Prefix: raw/valuations/
CREATE OR REPLACE TABLE RAW.RAW_VALUATIONS (
    province VARCHAR,                -- Was: provincia
    municipality VARCHAR,            -- Was: municipio
    avg_value_m2 FLOAT,              -- Was: valor_medio_m2
    total_appraisals INT,            -- Was: numero_tasaciones
    __LOADED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- 4.3. INCOME (AEAT)
-- S3 Prefix: raw/income/
CREATE OR REPLACE TABLE RAW.RAW_INCOME (
    municipality_code_ine VARCHAR,   -- Was: cod_municipio_ine
    municipality VARCHAR,            -- Was: municipio
    avg_gross_income INT,            -- Was: renta_bruta_media
    avg_disposable_income INT,       -- Was: renta_disponible_media
    total_declarations INT,          -- Was: numero_declaraciones
    __LOADED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- ===============================================================================
-- 5. LOAD EXAMPLES (COPY INTO)
-- ===============================================================================
/*
COPY INTO RAW.RAW_POPULATION(municipality_residence, sex, period, total)
FROM (
    SELECT 
        $1:Municipio_de_residencia::VARCHAR, 
        $1:Sexo::VARCHAR, 
        $1:Periodo::INT, 
        $1:Total::INT
    FROM @RAW.S3_LAKE_STAGE/population/
)
PATTERN='.*population_INE_raw.parquet';

COPY INTO RAW.RAW_VALUATIONS(province, municipality, avg_value_m2, total_appraisals)
FROM (
    SELECT 
        $1:Provincia::VARCHAR,
        $1:Municipio::VARCHAR,
        $1:VALOR_MEDIO_M2_EUROS::FLOAT,
        $1:NUMERO_TASACIONES_TOTAL::INT
    FROM @RAW.S3_LAKE_STAGE/valuations/
)
PATTERN='.*valuations_Fomento_raw.parquet';

COPY INTO RAW.RAW_INCOME(municipality_code_ine, municipality, avg_gross_income, avg_disposable_income, total_declarations)
FROM (
    SELECT 
        $1:COD_MUNICIPIO_INE::VARCHAR,
        $1:MUNICIPIO::VARCHAR,
        $1:RENTA_BRUTA_MEDIA_EUROS::INT,
        $1:RENTA_DISPONIBLE_MEDIA_EUROS::INT,
        $1:NUMERO_DECLARACIONES_TOTAL::INT
    FROM @RAW.S3_LAKE_STAGE/income/
)
PATTERN='.*aeat_2023_raw.parquet';
*/
