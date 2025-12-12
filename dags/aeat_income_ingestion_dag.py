from __future__ import annotations

import datetime
import io
import time
import pandas as pd
import polars as pl
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# --- Configuration ---
S3_BUCKET = "spain-housing-datalake"
S3_CONN_ID = "aws_s3_conn"
S3_KEY = "raw/income/aeat_2023_raw.parquet"
AEAT_PAGE_URL = "https://sede.agenciatributaria.gob.es/AEAT/Contenidos_Comunes/La_Agencia_Tributaria/Estadisticas/Publicaciones/sites/irpfmunicipios/2023/jrubikf6b1df8fe9d9e1b349886f03cf541e3fd2e65dd05.html"

# Service name defined in docker-compose.yml
SELENIUM_REMOTE_URL = "http://remote-chromedriver:4444/wd/hub"

def ingest_and_upload_aeat_income(s3_bucket: str, s3_key: str, aws_conn_id: str, url: str):
    """
    Scrapes the AEAT Income data using a Remote WebDriver (Selenium container),
    cleans the data, converts to Parquet, and uploads to S3.
    """
    print(f"Starting RAW scrape for AEAT Income from: {url}")
    
    # Configure Remote WebDriver Options
    options = webdriver.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--headless") # Headless is recommended for stability even in remote container
    options.add_argument("--window-size=1920,1080")

    print(f"Connecting to Remote WebDriver at: {SELENIUM_REMOTE_URL}")
    
    driver = webdriver.Remote(
        command_executor=SELENIUM_REMOTE_URL,
        options=options
    )
    
    try:
        driver.get(url)
        
        # 1. Wait until the main table is loaded
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.ID, "table01"))
        )
        print("Page loaded. Starting iterative expansion...")

        # 2. ITERATIVE EXPANSION logic
        MAX_ATTEMPTS = 5 
        attempt = 0
        
        while attempt < MAX_ATTEMPTS:
            # Note: We find elements by XPATH looking for the 'expand.gif' image
            expand_buttons = driver.find_elements(By.XPATH, "//table[@id='table01']//img[contains(@src, 'expand.gif')]")
            
            if not expand_buttons:
                print(f"Found no more expansion buttons. Table fully deployed.")
                break
                
            print(f"Attempt {attempt + 1}: Found {len(expand_buttons)} levels to expand.")
            
            for button in expand_buttons:
                try:
                    # Click parent link of the image
                    parent_link = button.find_element(By.XPATH, "..")
                    parent_link.click()
                    time.sleep(0.1) # Small pause
                except Exception:
                    pass
            
            time.sleep(2) 
            attempt += 1

        print("Data extraction after expansion...")
        
        # 3. Get HTML and Parse with Pandas
        # Configure thousands and decimal separators to correctly parse Spanish numbers correctly
        html_content = driver.page_source
        tables = pd.read_html(
            io.StringIO(html_content), 
            attrs={'id': 'table01'}, 
            header=None,
            thousands='.',
            decimal=','
        )
        
        if not tables:
             raise ValueError("Pandas could not find the table 'table01' in the scraped HTML.")
             
        df_pd = tables[0].copy()
        
        # 4. Data Cleaning
        # Keep rows starting from the 'Total' row (index 2 in raw scrape usually)
        df_pd = df_pd.iloc[2:].copy() 
        
        df_pd.columns = ['MUNICIPIO_O_COMUNIDAD', 'COL1', 'NUMERO_DECLARACIONES', 'NUMERO_HABITANTES', 
                         'COL4', 'COL5', 'RENTA_BRUTA_MEDIA_EUROS', 'COL7', 'RENTA_DISPONIBLE_MEDIA_EUROS', 'COL9']
        
        # Remove the actual 'Total' row which is now at the top
        df_pd = df_pd.iloc[1:].reset_index(drop=True) 
        
        # Filter for rows that look like municipalities (INE code pattern "-12345")
        df_pd = df_pd[df_pd['MUNICIPIO_O_COMUNIDAD'].astype(str).str.contains(r'-\d{5}')].copy()
        
        # Extract metadata
        df_pd['COD_MUNICIPIO_INE'] = df_pd['MUNICIPIO_O_COMUNIDAD'].astype(str).str.extract(r'-(\d{5})')
        df_pd['MUNICIPIO'] = df_pd['MUNICIPIO_O_COMUNIDAD'].astype(str).str.split('-').str[0].str.strip()

        # Clean numeric strings (Removed manual cleaning as pd.read_html now handles it)
        # However, we ensure they are numeric just in case there are dirty values
        for col in ['RENTA_BRUTA_MEDIA_EUROS', 'RENTA_DISPONIBLE_MEDIA_EUROS', 'NUMERO_DECLARACIONES']:
             df_pd[col] = pd.to_numeric(df_pd[col], errors='coerce').fillna(0)

        # 5. Convert to Polars and Rename to English (Match Snowflake Schema)
        df_pl = pl.DataFrame(df_pd[[
            'COD_MUNICIPIO_INE', 
            'MUNICIPIO', 
            'RENTA_BRUTA_MEDIA_EUROS', 
            'RENTA_DISPONIBLE_MEDIA_EUROS', 
            'NUMERO_DECLARACIONES'
        ]])
        
        df_pl = df_pl.rename({
            'COD_MUNICIPIO_INE': 'municipality_code_ine',
            'MUNICIPIO': 'municipality',
            'RENTA_BRUTA_MEDIA_EUROS': 'avg_gross_income',
            'RENTA_DISPONIBLE_MEDIA_EUROS': 'avg_disposable_income',
            'NUMERO_DECLARACIONES': 'total_declarations'
        })
        
        df_pl = df_pl.with_columns([
            pl.col('avg_gross_income').cast(pl.Int32, strict=False),
            pl.col('avg_disposable_income').cast(pl.Int32, strict=False),
            pl.col('total_declarations').cast(pl.Int32, strict=False),
            pl.lit(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")).alias("__LOADED_AT")
        ])
        
        print(f"✅ Data cleaning complete. Total records: {len(df_pl)}")
        
        # 6. Upload to S3
        buffer = io.BytesIO()
        df_pl.write_parquet(buffer, compression="snappy")
        buffer.seek(0)
        
        s3_hook = S3Hook(aws_conn_id=aws_conn_id)
        print(f"Starting upload to S3://{s3_bucket}/{s3_key}")
        
        s3_hook.load_file_obj(
            file_obj=buffer,
            key=s3_key,
            bucket_name=s3_bucket,
            replace=True,
        )
        print(f"✅ File uploaded successfully to S3.")

    except Exception as e:
        print(f"❌ Error during AEAT Income ingest: {e}")
        raise
    finally:
        # 8. Quit the browser
        if 'driver' in locals():
            driver.quit()

with DAG(
    dag_id="aeat_income_to_s3_raw_ingestion",
    schedule="0 2 * * 1",  # Run every Monday at 2:00 AM
    start_date=datetime.datetime(2024, 1, 1),
    catchup=False,
    tags=["raw", "ingestion", "aeat", "income", "selenium"],
    doc_md=__doc__,
) as dag:

    ingest_task = PythonOperator(
        task_id="ingest_and_upload_aeat_data",
        python_callable=ingest_and_upload_aeat_income,
        op_kwargs={
            "s3_bucket": S3_BUCKET,
            "s3_key": S3_KEY,
            "aws_conn_id": S3_CONN_ID,
            "url": AEAT_PAGE_URL,
        },
    )

    # Task: Load data from S3 to Snowflake
    copy_into_snowflake = SQLExecuteQueryOperator(
        task_id="copy_into_snowflake",
        conn_id="snowflake_conn",
        sql=f"""
            TRUNCATE TABLE RAW.RAW_INCOME;
            COPY INTO RAW.RAW_INCOME
            FROM @RAW.S3_LAKE_STAGE/income/
            PATTERN='.*aeat_2023_raw.parquet'
            FILE_FORMAT = (TYPE = 'PARQUET')
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            ON_ERROR = 'CONTINUE';
        """,
    )

    ingest_task >> copy_into_snowflake
