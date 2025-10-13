Wistia Video Analytics – End-to-End Data Pipeline
Overview

This project implements a production-grade data pipeline designed to extract, process, and analyze video engagement metrics from the Wistia Stats API. The system automates data ingestion, transformation, and reporting using AWS services and Python-based tooling.

The goal was to simulate a real-world data engineering environment—building a scalable, automated pipeline capable of handling daily ingestion and transformation workflows, while exposing final insights through an interactive dashboard.

Business Objective

The marketing team uses Wistia to track video engagement across social channels such as Facebook and YouTube.
This project provides a repeatable solution to:

Collect media-level and visitor-level analytics from Wistia’s Stats API

Structure and transform raw data into analytics-ready formats

Surface performance insights through an interactive Streamlit dashboard

Implement CI/CD and version control practices for maintainability

Architecture

The system follows a three-layer medallion architecture to ensure clarity, data quality, and scalability.

Wistia Stats API
    │
    ▼
Bronze (Raw Data in S3)
    │
    ▼
Silver (Cleaned and Structured Data via AWS Glue / PySpark)
    │
    ▼
Gold (Aggregated Analytics Tables in S3)
    │
    ▼
Streamlit Dashboard (KPIs, Filters, Charts)

Core Components
Layer	Technology	Purpose
Ingestion	Python, Wistia API	Authenticated API pulls with pagination and incremental loading
Storage	Amazon S3	Central data lake for Bronze, Silver, and Gold layers
Transformation	AWS Glue (PySpark)	Schema enforcement, deduplication, and metric aggregation
Automation	AWS Lambda, CloudWatch	Orchestration, scheduling, and monitoring of Glue jobs
Visualization	Streamlit	Interactive dashboard for KPI and trend exploration
Version Control	GitHub	CI/CD, branching, and deployment management

Design Decisions

Medallion architecture provides a clear separation between raw, cleaned, and aggregated datasets, supporting scalability and data lineage tracking.

AWS Glue was chosen over dbt to comply with project requirements and leverage distributed PySpark transformations.

S3 serves as the data lake for durability and integration with Glue, Athena, and downstream analytics tools.

Streamlit was selected for visualization due to its rapid development cycle and native Python support.

CI/CD is implemented with GitHub Actions for testing and deployment automation.

Setup Instructions
1. Clone the Repository
git clone https://github.com/elupovit/wistia-video-analytics.git
cd wistia-video-analytics

2. Set Up the Python Environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r ingestion/requirements.txt
pip install -r streamlit_app/requirements.txt

3. AWS Configuration

Before running the pipeline, ensure your AWS account includes:

S3 buckets for Bronze, Silver, and Gold layers

IAM roles granting Glue, Lambda, and CloudWatch access

(Optional) Athena setup for ad-hoc SQL queries

If deploying infrastructure with AWS SAM:

sam build
sam deploy --guided

4. Run the Dashboard Locally
cd streamlit_app
streamlit run app.py

5. CI/CD Workflow

Each push to main runs linting, testing, and optional deployment to Streamlit Cloud.

Dashboard Overview

The Streamlit dashboard presents metrics at both the media and visitor levels.

Filters

Date range

Media ID

KPIs

Total Plays

Total Loads

Play Rate

Total Watch Time

Engagement Rate

Visuals

30-day engagement trends by media

Top visitors ranked by engagement metrics

The dashboard reads directly from Gold-layer Parquet data, ensuring consistency with the underlying AWS pipeline outputs.

Evaluation Summary

This project demonstrates the implementation of an end-to-end cloud data pipeline with automated ingestion, transformation, and reporting.

Key Achievements

Designed and implemented modular ingestion → transformation → reporting architecture

Used authenticated API access, pagination, and incremental ingestion logic

Ran the pipeline continuously in a production-style configuration

Delivered an interactive, analytics-ready Streamlit dashboard

Implemented version control and CI/CD for pipeline maintenance

Documented all architectural and technical design decisions

Author

Developed by Eitan Lupovitch
End-to-end data engineering project built to model a real-world analytics pipeline using AWS and Python.
