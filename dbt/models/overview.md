{% docs __overview__ %}

# Spain Housing Market Analysis 🇪🇸🏠

Welcome to the documentation for the Spain Housing Market data pipeline. This project integrates data from multiple government sources to analyze housing affordability across Spanish municipalities.

## 📊 Data Sources

1.  **AEAT (Tax Agency)**: Average gross and disposable income per municipality.
2.  **Fomento (Ministry of Development)**: Housing appraisal values (price per m²).
3.  **INE (National Statistics Institute)**: Population census data.

## 🧮 Key Metrics

### Tension Index
The core metric of this project is the **Tension Index**, which measures the housing affordability stress.

$$
\text{Tension Index} = \frac{\text{Housing Price (€/m²)}}{\text{Avg Disposable Income (€)}} \times 100
$$

*   **High values** (> 20) indicate low affordability (expensive housing relative to income).
*   **Low values** (< 10) indicate high affordability.

## 🛠️ Pipeline Architecture

1.  **Airflow**: Orchestrates ingestion from sources (XLS, CSV) to S3 and loads Raw data into Snowflake.
2.  **dbt**: Transforms raw data using SQL models:
    *   `Staging`: Cleans and standardizes raw data (renaming, casting).
    *   `Marts`: Joins datasets and calculates business metrics (Facts).
3.  **Snowflake**: Cloud Data Warehouse storage.

## 📞 Contact

Maintained by the Data Engineering Team.

{% enddocs %}
