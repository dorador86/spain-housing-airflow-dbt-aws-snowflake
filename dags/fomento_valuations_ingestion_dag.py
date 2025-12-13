from __future__ import annotations

import datetime
import pandas as pd
import polars as pl
import requests
import io

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# --- Configuration ---
S3_BUCKET = "spain-housing-datalake"
S3_CONN_ID = "aws_s3_conn"
S3_KEY = "raw/valuations/valuations_Fomento_raw.parquet"
VALUATIONS_URL = "https://apps.fomento.gob.es/boletinonline2/sedal/35103500.XLS"

def ingest_and_upload_fomento_valuations(s3_bucket: str, s3_key: str, aws_conn_id: str, url: str):
    """
    Downloads, cleans, and converts the Fomento Valuations XLS to Parquet in memory,
    then uploads the binary Parquet data directly to S3 using S3Hook.
    """
    print(f"Starting RAW download and ingest from: {url}")

    try:
        # 1. Download the content
        response = requests.get(url)
        response.raise_for_status()
        excel_data = io.BytesIO(response.content)

        # 2. Read the specific sheet (Last sheet)
        # We need xlrd for .xls files (older Excel format used by Fomento)
        all_sheets = pd.read_excel(excel_data, sheet_name=None, engine='xlrd')
        latest_sheet_name = list(all_sheets.keys())[-1]
        print(f"Reading data from the last sheet: {latest_sheet_name}")

        # Read Excel with dtype=str for numeric columns to prevent auto-conversion
        # This is CRITICAL: Pandas might interpret Spanish format (1.569,8) incorrectly
        # We need raw string values to process them correctly
        df_pd = pd.read_excel(
            excel_data,
            sheet_name=latest_sheet_name,
            header=14,             # Excel row 15 (index 14)
            usecols="B:C,F:F,J:J", # Select columns: B, C, F, J
            dtype=str,             # Read ALL columns as strings to preserve original format
            engine='xlrd'
        )

        # 3. Data Cleaning and Pre-processing
        # Rename columns 'Unnamed: 5' -> 'VALOR_MEDIO_M2_EUROS' and 'Unnamed: 9' -> 'NUMERO_TASACIONES_TOTAL'
        df_pd = df_pd.rename(columns={
            'Unnamed: 5': 'VALOR_MEDIO_M2_EUROS',
            'Unnamed: 9': 'NUMERO_TASACIONES_TOTAL'
        }, errors='ignore')

        # Cleanup: Remove rows where both Province and Municipio are NaN
        df_pd = df_pd.dropna(subset=['Provincia', 'Municipio'], how='all')

        # Cleanup: Convert Spanish number format to standard format
        # Spanish format: 1.569,8 (thousands separator = '.', decimal separator = ',')
        # Standard format: 1569.8 (no thousands separator, decimal separator = '.')
        for col in ['VALOR_MEDIO_M2_EUROS', 'NUMERO_TASACIONES_TOTAL']:
             df_pd[col] = (df_pd[col]
                          .astype(str)
                          .str.replace('.', '', regex=False)      # Remove thousands separator
                          .str.replace(',', '.', regex=False)     # Convert decimal separator
                          .replace('n.r.', '', regex=False))      # Handle 'no reportado'

        # 4. Convert to Polars and Type Casting
        df_pl = pl.DataFrame(df_pd)
        
        # Forward Fill 'Provincia'
        df_pl = df_pl.with_columns(
            pl.col("Provincia").fill_null(strategy="forward")
        )

        # Rename to English to Match Snowflake Schema
        df_pl = df_pl.rename({
            'Provincia': 'province',
            'Municipio': 'municipality',
            'VALOR_MEDIO_M2_EUROS': 'avg_value_m2',
            'NUMERO_TASACIONES_TOTAL': 'total_appraisals'
        })

        # Cast types and filter null counts
        # strict=False allows "empty" strings (from n.r. removal) to become Nulls
        df_pl = (
            df_pl
            .with_columns([
                pl.col("avg_value_m2").cast(pl.Float64, strict=False), 
                pl.col("total_appraisals").cast(pl.Int32, strict=False), 
                pl.lit(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")).alias("__LOADED_AT")
            ])
            .filter(pl.col('total_appraisals').is_not_null())
        )

        print(f"✅ Data cleaning complete. Total records: {len(df_pl)}")

        # 5. Convert Polars DataFrame to Parquet format in memory
        buffer = io.BytesIO()
        df_pl.write_parquet(buffer, compression="snappy")
        buffer.seek(0)

        # 6. Upload to S3 using the S3Hook
        s3_hook = S3Hook(aws_conn_id=aws_conn_id)

        print(f"Starting upload to S3://{s3_bucket}/{s3_key}")

        s3_hook.load_file_obj(
            file_obj=buffer,
            key=s3_key,
            bucket_name=s3_bucket,
            replace=True,
        )

        print(f"✅ File uploaded successfully to S3://{s3_bucket}/{s3_key}")

    except Exception as e:
        print(f"❌ Error during processing or upload: {e}")
        raise

with DAG(
    dag_id="fomento_valuations_to_s3_raw_ingestion",
    schedule="0 2 * * 1",  # Run every Monday at 2:00 AM
    start_date=datetime.datetime(2024, 1, 1),
    catchup=False,
    tags=["raw", "ingestion", "fomento", "valuations"],
    doc_md=__doc__,
) as dag:

    ingest_and_upload_task = PythonOperator(
        task_id="ingest_and_upload_data_to_s3",
        python_callable=ingest_and_upload_fomento_valuations,
        op_kwargs={
            "s3_bucket": S3_BUCKET,
            "s3_key": S3_KEY,
            "aws_conn_id": S3_CONN_ID,
            "url": VALUATIONS_URL,
        },
    )

    # Task: Load data from S3 to Snowflake
    copy_into_snowflake = SQLExecuteQueryOperator(
        task_id="copy_into_snowflake",
        conn_id="snowflake_conn",
        sql=f"""
            TRUNCATE TABLE RAW.RAW_VALUATIONS;
            COPY INTO RAW.RAW_VALUATIONS
            FROM @RAW.S3_LAKE_STAGE/valuations/
            PATTERN='.*valuations_Fomento_raw.parquet'
            FILE_FORMAT = (TYPE = 'PARQUET')
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            ON_ERROR = 'CONTINUE';
        """,
    )

    ingest_and_upload_task >> copy_into_snowflake
