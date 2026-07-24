YouTube Data Analytics Pipeline using AWS
Overview

This project implements an end-to-end YouTube Data Analytics pipeline on AWS. It ingests YouTube data, processes it through multiple ETL stages using AWS Glue, validates the transformed data, and stores analytics-ready datasets in Amazon S3. Finally, the processed data is visualized using a lightweight dashboard built with DuckDB, Streamlit, and Plotly.
Architecture

YouTube Dataset
       │
       ▼
Amazon S3 (Bronze)
       │
       ▼
AWS Glue ETL
       │
       ▼
Amazon S3 (Silver)
       │
       ▼
Data Cleaning & Validation
       │
       ▼
Amazon S3 (Gold)
       │
       ▼
DuckDB
       │
       ▼
Streamlit Dashboard

Technologies Used

    AWS S3
    AWS Glue (PySpark)
    Amazon Athena (optional for querying)
    Python
    DuckDB
    Streamlit
    Plotly
    Pandas
    Git & GitHub

Data Pipeline
Bronze Layer

    Stores raw YouTube data.
    Immutable copy of the source dataset.

Silver Layer

    Cleans and transforms raw data.
    Standardizes schema.
    Removes duplicates.
    Handles invalid records.

Gold Layer

    Business-ready analytical datasets.
    Includes:
        Channel Analysis
        Daily Analysis

Data Validation

Validation checks include:

    Required columns
    Duplicate records
    Null values
    Negative metrics
    Likes greater than views
    Comments greater than views
    Missing partition columns

Only validated data is promoted to the Gold layer.
Dashboard Features

    KPI Cards
        Total Views
        Total Videos
        Total Likes
        Total Channels
    Top Channels by Views
    Likes vs Comments Analysis
    Daily View Trends
    Daily Upload Analysis
    Interactive Data Table

Project Structure

project/
│
├── glue/
│   ├── bronze_to_silver.py
│   ├── silver_cleaning.py
│   └── validation_gold.py
│
├── dashboard/
│   ├── dashboard.py
│   ├── channel_analysis/
│   └── daily_analysis/
│
├── README.md
└── REPORT.md

Running the Dashboard

Install dependencies:

pip install streamlit duckdb plotly pandas pyarrow

Start the dashboard:

streamlit run dashboard.py

Open:

http://localhost:8501

Future Improvements

    Automated Glue Workflows
    Event-driven processing with AWS Lambda
    Amazon QuickSight integration
    AI-powered natural language analytics
    Real-time streaming pipeline using Amazon Kinesis

Author

Developed as an end-to-end AWS Data Engineering portfolio project demonstrating cloud ETL, data validation, analytics engineering, and interactive visualization.

The following is a more formal report suitable for submission or inclusion in your portfolio.
