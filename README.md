# FoodLens Data Platform #

An end-to-end AWS data platform that ingests food product data from the Open Food Facts API, validates and transforms it using PySpark on AWS Glue, stores curated datasets in an S3 data lake, and exposes analytics-ready tables through Amazon Athena.

The platform follows a Bronze → Silver → Gold architecture with automated data-quality validation, quarantine handling, CI/CD testing, and cloud-native analytics.

---

## What the pipeline does

1. Pulls product data from the Open Food Facts API
2. Saves the original response in an S3 Bronze layer
3. Uses an AWS Glue PySpark job to clean and validate the data
4. Sends valid records to Silver and invalid records to quarantine
5. Builds Gold summary tables
6. Registers the Gold tables in the AWS Glue Data Catalog
7. Queries the final tables with Amazon Athena
8. Runs tests and code checks through GitHub Actions

---

## Architecture

```text
                    Open Food Facts API
                             │
                             ▼
                   Python Ingestion Script
                             │
                             ▼
                      Amazon S3 Bronze
                       Raw JSON Files
                             │
                             ▼
                    AWS Glue (PySpark ETL)
                             │
         ┌───────────────────┴───────────────────┐
         ▼                                       ▼
 Amazon S3 Silver                        S3 Quarantine
 Clean Parquet                     Invalid Records + Reason
         │
         ▼
 AWS Glue Gold Transform
         │
         ▼
 Amazon S3 Gold
 Analytics Tables
         │
         ▼
 AWS Glue Catalog
         │
         ▼
 Amazon Athena
 SQL Analytics
         │
         ▼
 Streamlit Dashboard
```

---

## Tech Stack

<p align="center">
  <img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python">
  <img src="https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white" alt="PySpark">
  <img src="https://img.shields.io/badge/Amazon%20S3-569A31?style=for-the-badge&logo=amazons3&logoColor=white" alt="Amazon S3">
  <img src="https://img.shields.io/badge/AWS%20Glue-8C4FFF?style=for-the-badge&logo=amazonwebservices&logoColor=white" alt="AWS Glue">
  <img src="https://img.shields.io/badge/Amazon%20Athena-232F3E?style=for-the-badge&logo=amazonwebservices&logoColor=white" alt="Amazon Athena">
  <img src="https://img.shields.io/badge/Parquet-50ABF1?style=for-the-badge&logo=apacheparquet&logoColor=white" alt="Parquet">
  <img src="https://img.shields.io/badge/GitHub%20Actions-2088FF?style=for-the-badge&logo=githubactions&logoColor=white" alt="GitHub Actions">
</p>

---

## Data layers

### Bronze

Bronze stores the original API response and basic run metadata.

```text
s3://<bucket-name>/bronze/
  ingestion_date=YYYY-MM-DD/
    run_id=<run-id>/
      products.json
      metadata.json
```

Keeping the raw response makes it possible to rerun the transformation without calling the API again.

### Silver

Silver contains product records that passed validation.

```text
s3://<bucket-name>/silver/
  processing_date=YYYY-MM-DD/
    run_id=<run-id>/
      part-*.snappy.parquet
```

Main fields include barcode, product name, brand, category, country, ingredients, allergens, nutrition grade, nutrition values, timestamps, and run ID.

### Quarantine

Records that fail validation are kept instead of being deleted.

```text
s3://<bucket-name>/quarantine/
  processing_date=YYYY-MM-DD/
    run_id=<run-id>/
      part-*.snappy.parquet
```

Example rejection reasons:

```text
missing_barcode
missing_product_name
invalid_energy_kcal_100g
invalid_fat_100g
invalid_sugars_100g
invalid_proteins_100g
invalid_salt_100g
```

### Gold

Gold contains analytics-ready datasets optimized for reporting and Athena queries.

```text
s3://<bucket-name>/gold/
  fact_products/
  brand_summary/
  category_summary/
```

The three Gold tables are:

- **fact_products:** cleaned product-level records for detailed analysis
- **brand_summary:** product counts and average nutrition values by brand
- **category_summary:** product counts and average nutrition values by category

---

## Data-quality rules

Rather than dropping malformed records, FoodLens isolates them in a dedicated quarantine layer together with a rejection reason. This enables downstream auditing, replayability, and monitoring while preserving complete ingestion history.

Validation includes:

- Missing barcode
- Missing product name
- Duplicate barcode detection
- Invalid nutrition values
- Negative numeric fields

---

## Example run report

Each pipeline run creates a JSON report.

```json
{
  "run_id": "20260718T195354Z",
  "status": "success",
  "source": "open_food_facts",
  "category": "chocolates",
  "extracted_record_count": 2000,
  "silver_record_count": 1894,
  "quarantined_record_count": 106,
  "acceptance_rate": 94.7,
  "brand_summary_count": 612,
  "nutrition_grade_summary_count": 5,
  "gold_table_count": 3,
  "quarantine_breakdown": {
    "missing_product_name": 71,
    "invalid_energy_kcal_100g": 23,
    "missing_barcode": 12
  }
}
```

---

## Repository structure

```text
foodlens-data-platform/
├── .github/
│   └── workflows/
│       └── ci.yml
├── dashboard/
├── data/
│   └── samples/
├── docs/
├── glue_jobs/
│   ├── bronze_to_silver.py
│   └── silver_to_gold.py
├── reports/
├── src/
│   ├── api_client.py
│   ├── reporting.py
│   ├── schemas.py
│   └── transform.py
├── tests/
├── run_pipeline.py
├── requirements.txt
├── pytest.ini
├── .env.example
└── README.md
```

---

## Configuration

```bash
cp .env.example .env
```

Example:

```env
OPEN_FOOD_FACTS_BASE_URL=https://world.openfoodfacts.org
OPEN_FOOD_FACTS_CATEGORY=chocolates
OPEN_FOOD_FACTS_PAGE_SIZE=100
OPEN_FOOD_FACTS_MAX_PAGES=20
OPEN_FOOD_FACTS_USER_AGENT=FoodLensDataPlatform/1.0

BRONZE_ROOT=data/bronze
SILVER_ROOT=data/silver
GOLD_ROOT=data/gold
QUARANTINE_ROOT=data/quarantine
REPORT_ROOT=reports

AWS_REGION=us-east-1
S3_BUCKET_NAME=<your-private-bucket-name>
PUBLISH_TO_S3=true

GLUE_DATABASE_NAME=foodlens_gold
GLUE_JOB_NAME=foodlens-pyspark-job
ATHENA_OUTPUT_LOCATION=s3://<your-private-bucket-name>/athena-results/
```

Do not store AWS access keys in this file.

---

## Run locally

```bash
conda create -n foodlens python=3.12 -y
conda activate foodlens
python -m pip install -r requirements.txt
python run_pipeline.py
```

---

## Testing

```bash
python -m pytest -v
python -m pytest tests/test_pipeline_integration.py -v
ruff check src tests run_pipeline.py
```

GitHub Actions runs these checks automatically on pushes and pull requests.

---

## AWS setup

- **Amazon S3:** stores Bronze, Silver, quarantine, Gold, reports, and Athena results
- **AWS Glue:** runs the PySpark transformation
- **Glue Data Catalog:** stores table definitions
- **Amazon Athena:** queries the Gold tables
- **CloudWatch:** stores job logs
- **IAM:** gives the Glue job access to only the resources it needs

---

## Pipeline Outputs

Every pipeline execution produces:

- Bronze raw API snapshot
- Silver validated Parquet dataset
- Quarantine dataset with rejection reasons
- Three Gold analytical tables
- Pipeline execution report
- CloudWatch execution logs
- Athena-queryable datasets

---

## Example Athena query

```sql
SELECT
    brand,
    product_count,
    average_sugars_100g,
    average_energy_kcal_100g
FROM foodlens_gold.brand_summary
ORDER BY product_count DESC
LIMIT 20;
```

---

## Scheduling and monitoring

The Glue jobs can be executed on demand through the AWS Glue console. Scheduled execution through EventBridge or Glue triggers is planned.

GitHub Actions runs linting and automated tests on pushes and pull requests.

---

## Main engineering lessons I learned from building this pipeline

- Save raw data before transforming it so failed jobs can be replayed
- Keep invalid records with a reason instead of deleting them
- Use explicit schemas for inconsistent API data
- Use JSON for raw data and Parquet for cleaned analytical data
- Separate code testing from scheduled production runs
- Treat retries as protection from temporary failures, not as a scaling strategy
- Choose partitions based on how the data will be queried
- Track record counts and rejection reasons for every run

---

## Future improvements

- Event-driven ingestion using Amazon EventBridge
- Infrastructure as Code with Terraform
- Data versioning using Apache Iceberg
- Great Expectations for advanced data-quality testing
- Incremental loading and change detection
- Amazon QuickSight dashboard with scheduled refresh
- Automated CloudWatch alarms for pipeline failures
