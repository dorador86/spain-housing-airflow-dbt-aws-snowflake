from __future__ import annotations

import datetime
import pandas as pd
import polars as pl
import requests
import io

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

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

        df_pd = pd.read_excel(
            excel_data,
            sheet_name=latest_sheet_name,
            header=14,             # Excel row 15 (index 14)
            usecols="B:C,F:F,J:J", # Select columns: B, C, F, J
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

        # Cleanup: Remove thousands separators ('.') and handle 'n.r.'
        for col in ['VALOR_MEDIO_M2_EUROS', 'NUMERO_TASACIONES_TOTAL']:
             df_pd[col] = df_pd[col].astype(str).str.replace('.', '', regex=False).replace('n.r.', '', regex=False)

        # 4. Convert to Polars and Type Casting
        df_pl = pl.DataFrame(df_pd)
        
        # Forward Fill 'Provincia'
        df_pl = df_pl.with_columns(
            pl.col("Provincia").fill_null(strategy="forward")
        )

        # Cast types and filter null counts
        # strict=False allows "empty" strings (from n.r. removal) to become Nulls
        df_pl = (
            df_pl
            .with_columns([
                pl.col("VALOR_MEDIO_M2_EUROS").cast(pl.Float32, strict=False), 
                pl.col("NUMERO_TASACIONES_TOTAL").cast(pl.Int32, strict=False), 
            ])
            .filter(pl.col('NUMERO_TASACIONES_TOTAL').is_not_null())
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
