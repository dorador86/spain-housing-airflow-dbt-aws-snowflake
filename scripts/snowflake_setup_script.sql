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
    BINARY_AS_TEXT = FALSE; -- Important for complex data types

-- ===============================================================================
-- 3. S3 INTEGRATION (EXTERNAL STAGE)
-- ===============================================================================
-- IMPORTANT: For this PoC, we use direct credentials.
-- In production environments, using a STORAGE INTEGRATION (IAM Role) is recommended.
-- Replace 'YOUR_ACCESS_KEY' and 'YOUR_SECRET_KEY' with your actual AWS credentials.

CREATE OR REPLACE STAGE RAW.S3_LAKE_STAGE
    URL = 's3://spain-housing-datalake/raw/'
    CREDENTIALS = (AWS_KEY_ID='YOUR_ACCESS_KEY' AWS_SECRET_KEY='YOUR_SECRET_KEY')
    FILE_FORMAT = RAW.PARQUET_FORMAT;

-- Verify if files are visible (DAGs must have run beforehand)
-- LIST @RAW.S3_LAKE_STAGE;

-- ===============================================================================
-- 4. RAW TABLES (DDL)
-- ===============================================================================
-- We take advantage of Snowflake's ability to query Parquet directly,
-- but for robustness, we define explicit schemas.

-- 4.1. POPULATION (INE)
CREATE OR REPLACE TABLE RAW.RAW_POBLACION (
    municipio_residencia VARCHAR,
    sexo VARCHAR,
    periodo INT,
    total INT,
    __LOADED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- 4.2. REAL ESTATE VALUATIONS (FOMENTO)
CREATE OR REPLACE TABLE RAW.RAW_TASACION (
    provincia VARCHAR,
    municipio VARCHAR,
    valor_medio_m2 FLOAT,  -- Was float32 in Parquet
    numero_tasaciones INT,
    __LOADED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- 4.3. INCOME (AEAT)
CREATE OR REPLACE TABLE RAW.RAW_INGRESOS (
    cod_municipio_ine VARCHAR,
    municipio VARCHAR,
    renta_bruta_media INT,
    renta_disponible_media INT,
    numero_declaraciones INT,
    __LOADED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- ===============================================================================
-- 5. LOAD EXAMPLES (COPY INTO) - To be orchestrated by Airflow/dbt
-- ===============================================================================
/*
COPY INTO RAW.RAW_POBLACION(municipio_residencia, sexo, periodo, total)
FROM (
    SELECT 
        $1:Municipio_de_residencia::VARCHAR, 
        $1:Sexo::VARCHAR, 
        $1:Periodo::INT, 
        $1:Total::INT
    FROM @RAW.S3_LAKE_STAGE/population/
)
PATTERN='.*population_INE_raw.parquet';

COPY INTO RAW.RAW_TASACION(provincia, municipio, valor_medio_m2, numero_tasaciones)
FROM (
    SELECT 
        $1:Provincia::VARCHAR,
        $1:Municipio::VARCHAR,
        $1:VALOR_MEDIO_M2_EUROS::FLOAT,
        $1:NUMERO_TASACIONES_TOTAL::INT
    FROM @RAW.S3_LAKE_STAGE/valuations/
)
PATTERN='.*valuations_Fomento_raw.parquet';

COPY INTO RAW.RAW_INGRESOS(cod_municipio_ine, municipio, renta_bruta_media, renta_disponible_media, numero_declaraciones)
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
