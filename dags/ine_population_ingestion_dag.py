# File: dags/ine_population_ingestion_dag.py

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
S3_CONN_ID = "aws_s3_conn"          # ID de la Conexión AWS S3 configurada en Airflow
S3_KEY = "raw/population/population_INE_raw.parquet"
INE_POBLACION_URL = "https://www.ine.es/jaxi/files/tpx/csv_bdsc/55200.csv"


def ingest_and_upload_ine_population(s3_bucket: str, s3_key: str, aws_conn_id: str, url: str):
    """
    Downloads, cleans, and converts the INE population CSV to Parquet in memory, 
    then uploads the binary Parquet data directly to S3 using S3Hook.
    This function is defined inside the DAG file for simplicity.
    """
    print(f"Starting RAW download and ingest from: {url}")
    
    try:
        # 1. Download the content
        response = requests.get(url)
        response.raise_for_status() 

        # Use BytesIO and the typical Latin-1 encoding for INE CSVs
        csv_content = io.StringIO(response.content.decode('latin-1'))
        
        # 2. Read RAW data using Pandas
        df_pd = pd.read_csv(csv_content, sep=';')
        
        # 3. Minimal Cleaning and Pre-processing
        # 3. Minimal Cleaning and Pre-processing
        # Rename columns to match Snowflake Table Schema (English)
        df_pd = df_pd.rename(columns={
            'ï»¿Municipio de residencia': 'municipality_residence',
            'Municipio de residencia': 'municipality_residence',
            'Sexo': 'sex',
            'Periodo': 'period',
            'Total': 'total'
        }, errors='ignore')

        df_pd = df_pd.drop(columns=['Unnamed: 4'], errors='ignore')
        
        # Clean numeric column (using new name)
        df_pd['total'] = df_pd['total'].astype(str).str.replace('.', '', regex=False)
        
        # 4. Convert to Polars and Type Casting
        df_pl = pl.DataFrame(df_pd)
        df_pl = df_pl.with_columns(
            pl.col("total").cast(pl.Int32, strict=False)
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
        
    except requests.exceptions.HTTPError as e:
        print(f"❌ HTTP Error during file download: {e}. Check the URL.")
        raise
    except Exception as e:
        print(f"❌ Error during processing or upload: {e}")
        raise


with DAG(
    dag_id="ine_population_to_s3_raw_ingestion",
    schedule="0 2 * * 1",  # Run every Monday at 2:00 AM
    start_date=datetime.datetime(2024, 1, 1),
    catchup=False,
    tags=["raw", "ingestion", "ine", "population"],
    doc_md=__doc__,
) as dag:
    
    # Task: Download, clean, convert, and upload data directly to S3
    ingest_and_upload_task = PythonOperator(
        task_id="ingest_and_upload_data_to_s3",
        python_callable=ingest_and_upload_ine_population,
        op_kwargs={
            "s3_bucket": S3_BUCKET,
            "s3_key": S3_KEY,
            "aws_conn_id": S3_CONN_ID,
            "url": INE_POBLACION_URL,
        },
    )

    # Task: Load data from S3 to Snowflake
    copy_into_snowflake = SQLExecuteQueryOperator(
        task_id="copy_into_snowflake",
        conn_id="snowflake_conn",
        sql=f"""
            TRUNCATE TABLE RAW.RAW_POPULATION;
            COPY INTO RAW.RAW_POPULATION
            FROM @RAW.S3_LAKE_STAGE/population/
            PATTERN='.*population_INE_raw.parquet'
            FILE_FORMAT = (TYPE = 'PARQUET')
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            ON_ERROR = 'CONTINUE';
        """,
    )

    ingest_and_upload_task >> copy_into_snowflake