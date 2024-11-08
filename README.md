# predictit_de_project

Task list:

- Script to ingest data into S3 (done)
- Integration b/w Snowflake and AWS (done)
- Staging in snowflake (done)
- Extracting bronze data into silver tables (including unnesting XML data) (done)

- Land gold data back in snowflake?
- Orchestrate whole thing in Airflow
    - First the data ingestion
    - then transformation within Snowflake. Maybe use Snowflake tasks?


# PredictIt Data Engineering Pipeline and Analytics Project

### Steps:

1) Configure S3 buckets
2) Configure storage integration between AWS S3 buckets and 