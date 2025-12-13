# dbt Testing & Documentation Guide

## 📋 Overview

This guide explains how to run dbt tests and generate documentation for the Spain Housing Market project.

## 🚀 Quick Start

### 1. Install dbt Packages

First, install the required dbt packages (dbt_utils for advanced tests):

```bash
cd dbt
dbt deps
```

This will install `dbt_utils` which provides advanced testing macros.

### 2. Run dbt Tests

Execute all data quality tests:

```bash
# Run all tests
dbt test

# Run tests for specific models
dbt test --select stg_income
dbt test --select fct_housing_market

# Run tests for staging layer
dbt test --select staging.*

# Run tests for marts layer
dbt test --select marts.*
```

### 3. Generate Documentation

Generate interactive documentation with data lineage:

```bash
# Generate documentation
dbt docs generate

# Serve documentation locally (opens in browser at http://localhost:8080)
dbt docs serve
```

## 📊 Test Coverage

### Staging Layer Tests

**stg_income:**
- ✅ `municipality_code`: not_null, unique
- ✅ `avg_gross_income`: range validation (0-200,000€)
- ✅ `avg_disposable_income`: range validation (0-150,000€)
- ✅ `total_declarations`: not_null, positive values

**stg_population:**
- ✅ `municipality_code`: not_null
- ✅ `population_count`: not_null, positive values
- ✅ `sex`: accepted_values check (only 'Ambos sexos')

**stg_valuations:**
- ✅ `province`: not_null
- ✅ `municipality_name`: not_null
- ✅ `avg_value_m2`: range validation (0-50,000€/m²)
- ✅ `total_appraisals`: not_null, positive values

### Marts Layer Tests

**fct_housing_market:**
- ✅ `municipality_code`: not_null, unique
- ✅ `tension_index`: range validation (0-1,000)
- ✅ Business logic: disposable_income ≤ gross_income
- ✅ All metrics: not_null, range validations

## 🎯 Test Severity Levels

- **error**: Test failure stops the pipeline (critical issues)
- **warn**: Test failure logs a warning but continues (data quality alerts)

## 📈 Expected Test Results

When you run `dbt test`, you should see output like:

```
Completed successfully

Done. PASS=25 WARN=0 ERROR=0 SKIP=0 TOTAL=25
```

If you see WARN or ERROR, review the specific test failures:

```bash
# See detailed test results
dbt test --store-failures
```

## 🔍 Documentation Features

The generated documentation includes:

1. **Data Lineage Graph**: Visual representation of data flow
2. **Model Descriptions**: Documentation for each table/view
3. **Column Details**: Data types, descriptions, and tests
4. **Test Results**: Pass/fail status for each test
5. **SQL Code**: Source code for each model

## 🐳 Running in Docker (Airflow)

To run tests in your Airflow environment:

```bash
# Enter the Airflow container
docker exec -it <airflow-container-id> bash

# Navigate to dbt directory
cd /opt/airflow/dbt

# Install packages and run tests
dbt deps
dbt test
```

## 📝 Adding New Tests

To add tests to a new model:

1. Open the appropriate `schema.yml` file
2. Add your model under `models:`
3. Define column-level and model-level tests
4. Run `dbt test` to validate

Example:

```yaml
models:
  - name: my_new_model
    columns:
      - name: my_column
        tests:
          - not_null
          - unique
```

## 🔗 Useful Commands

```bash
# Compile SQL without running
dbt compile

# Run models and tests together
dbt build

# Run specific test
dbt test --select test_name

# Debug connection
dbt debug
```

## 📚 Resources

- [dbt Testing Documentation](https://docs.getdbt.com/docs/build/tests)
- [dbt_utils Package](https://github.com/dbt-labs/dbt-utils)
- [dbt Documentation](https://docs.getdbt.com/docs/collaborate/documentation)
