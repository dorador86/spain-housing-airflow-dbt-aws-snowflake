# 🇪🇸 Spain Housing Market Analysis Data Pipeline

[![Airflow](https://img.shields.io/badge/Airflow-2.7+-017CEE?logo=apache-airflow&style=for-the-badge)](https://airflow.apache.org/)
[![dbt](https://img.shields.io/badge/dbt-Core-FF694B?logo=dbt&style=for-the-badge)](https://www.getdbt.com/)
[![Snowflake](https://img.shields.io/badge/Snowflake-Data%20Cloud-29B5E8?logo=snowflake&style=for-the-badge)](https://www.snowflake.com/)
[![AWS](https://img.shields.io/badge/AWS-S3-232F3E?logo=amazon-aws&style=for-the-badge)](https://aws.amazon.com/s3/)
[![Docker](https://img.shields.io/badge/Docker-Container-2496ED?logo=docker&style=for-the-badge)](https://www.docker.com/)
[![Python](https://img.shields.io/badge/Python-3.10-3776AB?logo=python&style=for-the-badge)](https://www.python.org/)

An End-to-End **Advanced Data Engineering Project** that integrates widely dispersed government data sources to calculate the **Housing Affordability Tension Index** across Spanish municipalities.

This project demonstrates a production-grade ETL/ELT architecture using modern Data Stack tools to solve a real-world economic analysis problem.

---

## 🏗️ Architecture

```mermaid
graph LR
    subgraph "Sources (Extraction)"
        AEAT[AEAT Income<br/>Web Scraper] -->|Selenium/Pandas| S3
        FOMENTO[Fomento Valuations<br/>Excel Files] -->|Polars| S3
        INE[INE Census<br/>API/CSV] -->|Python| S3
    end

    subgraph "Data Lake & Warehouse (Load)"
        S3[AWS S3<br/>Raw Data Lake] -->|Copy Into| SNOW[Snowflake<br/>Raw Layer]
        SNOW -->|dbt| DBT_L[Staging Layer]
        DBT_L -->|dbt| MART[Marts Layer<br/>Transformed Stats]
    end

    subgraph "Orchestration & Quality"
        AIRFLOW[Apache Airflow] -->|Trigger| AEAT
        AIRFLOW -->|Trigger| FOMENTO
        AIRFLOW -->|Trigger| INE
        AIRFLOW -->|SQLCheck| SNOW
        AIRFLOW -->|dbt run/test| DBT_L
    end

    style AEAT fill:#f9f,stroke:#333
    style FOMENTO fill:#f9f,stroke:#333
    style INE fill:#f9f,stroke:#333
    style SNOW fill:#6baed6,stroke:#333
    style AIRFLOW fill:#ffcc00,stroke:#333
```

---

## 🎯 Project Overview & Business Logic

### The Problem
Housing affordability is a critical issue in Spain. Data, however, is siloed:
*   **Income Data**: Hidden behind complex interactive portals (Tax Agency).
*   **Housing Prices**: Locked in unstructured Excel files from the Ministry of Development.
*   **Demographics**: Distinct datasets from the National Statistics Institute.

### The Solution: Tension Index
This pipeline unifies these sources to calculate the **Tension Index**:

$$ \text{Tension Index} = \frac{\text{Housing Price (€/m²)}}{\text{Avg. Disposable Income (€)}} \times 100 $$

*   **Metric**: Higher values (>20) indicate severe stress on affordability.

---

## ⚙️ Key Technical Features

### 1. Advanced Ingestion (Extraction)
*   **Selenium Web Scraper**: Bypassed simple `requests` limitations to scrape dynamic JS-rendered tables from the Tax Agency (AEAT), handling recursive navigation.
*   **Polars & Pandas**: Optimized processing of Excel files with Spanish number formats (handling locale-specific decimals `,` vs `.`).
*   **Raw Data Lake**: Used **AWS S3** as an intermediate Raw storage layer (Parquet format) for auditability.

### 2. Robust Transformation (dbt + Snowflake)
*   **Municipality Normalization**: Implemented complex SQL logic to align mismatched municipality names across sources (e.g., handling *"Coruña, A"* vs *"A Coruña"* vs *"Coruña (A)"*).
*   **Materialization strategy**: Used `view` for Staging and `table` for Marts for optimal performance/cost.
*   **Business Logic**: Calculated derived metrics like *Gross vs Disposable Income Gap*.

### 3. Data Quality & Governance (Defense in Depth)
A multi-layered approach to ensure data trust:
*   **Ingestion Layer**: `SQLCheckOperator` in Airflow immediately blocks negative prices or unlikely outliers (>50k €/m²).
*   **Transformation Layer**: **dbt tests** validate referential integrity, uniqueness keys, and acceptable value ranges.
*   **Docs**: Auto-generated dbt lineage documentation.

---

## 📊 Sample Insights (Results)

*Top 5 Municipalities by Housing Market Tension (Sample Output)*

| Rank | Municipality | Province | Housing Price (€/m²) | Avg. Income (€) | **Tension Index** |
|------|--------------|----------|----------------------|-----------------|-------------------|
| 1    | Ibiza        | Baleares | 5,850                | 16,300          | **35.89**         |
| 2    | Santa Eulària| Baleares | 5,200                | 15,900          | **32.70**         |
| 3    | Marbella     | Málaga   | 4,120                | 14,800          | **27.84**         |
| 4    | San Sebastián| Guipúzcoa| 5,100                | 22,500          | **22.66**         |
| 5    | Madrid (City)| Madrid   | 3,900                | 19,800          | **19.69**         |

*(Data simulated for demonstration typical values)*

---

## 🛠️ Stack & Technologies

*   **Orchestration**: Apache Airflow (Dockerized)
*   **Compute/Transformation**: dbt Core + Snowflake (ELT pattern)
*   **Cloud Storage**: AWS S3
*   **Containerization**: Docker & Docker Compose
*   **Languages**: Python (3.10), SQL, Jinja
*   **Libraries**: Polars, Pandas, Selenium, Airflow-Providers

---

## 🚀 Setup & Usage

1.  **Clone the Repository**
    ```bash
    git clone https://github.com/your-username/spain-housing-data-pipeline.git
    cd spain-housing-data-pipeline
    ```

2.  **Environment Setup**
    Create a `.env` file with your credentials:
    ```env
    AIRFLOW_UID=50000
    AWS_ACCESS_KEY_ID=your_key
    AWS_SECRET_ACCESS_KEY=your_secret
    SNOWFLAKE_ACCOUNT=your_account
    SNOWFLAKE_USER=your_user
    SNOWFLAKE_PASSWORD=your_password
    ```

3.  **Run with Docker**
    ```bash
    docker-compose up -d --build
    ```

4.  **Access Airflow UI**
    *   Go to `http://localhost:8080`
    *   Login: `airflow`/`airflow`
    *   Trigger `dbt_transformation_dag` to run the full pipeline.

---

## 📂 Project Structure

```bash
├── dags/
│   ├── aeat_income_ingestion_dag.py    # Selenium Scraper DAG
│   ├── fomento_valuations_dag.py       # Excel Processing DAG
│   └── dbt_transformation_dag.py       # dbt Runner & Docs DAG
├── dbt/
│   ├── models/
│   │   ├── staging/                    # Cleaning & Standardization
│   │   └── marts/                      # Business Logic & Joins
│   ├── tests/                          # Data Quality Tests
│   └── dbt_project.yml
├── docker-compose.yml                  # Airflow + Selenium Infrastructure
└── requirements.txt
```

---

## 👨‍💻 Author

**Victor** - Data Engineer  
[LinkedIn](https://linkedin.com/in/your-profile) | [Portfolio](https://your-portfolio.com)