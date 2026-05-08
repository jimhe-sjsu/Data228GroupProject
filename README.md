# NYC Taxi Demand Prediction — End-to-End Big Data System

## Overview

This project builds a complete **Big Data + Machine Learning pipeline** to predict hourly taxi demand across New York City pickup zones using NYC Taxi & Limousine Commission (TLC) data.

The system processes large-scale trip data (2023–2025), performs distributed data cleaning and aggregation using Apache Spark, stores structured tables using Apache Iceberg, trains machine learning models, and serves predictions through an interactive Streamlit dashboard and LLM agent.

---

## Project Goal

Predict:

> **trip_count = number of trips for a given pickup zone and hour**

This enables zone-level demand forecasting across NYC for different time periods.

---

## Dataset

- **Source:** NYC TLC Trip Records  
  https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- **Data Types:** Yellow Taxi + HVFHV  
- **Format:** Monthly Parquet files  
- **Time Range:** 2023–2025  

### Data Scale

| Stage | Size |
|------|------|
| Raw Data | ~18 GB |
| Cleaned Data | ~14 GB |
| ML Dataset | ~7 million rows |

---

## Tech Stack

| Layer | Tools |
|------|------|
| Storage | HDFS |
| Processing | Apache Spark |
| Orchestration | Apache Airflow |
| Table Format | Apache Iceberg |
| ML Models | Random Forest, Linear Regression |
| Serving | DuckDB |
| Frontend | Streamlit |
| AI Interface | LLM Agent |

---


---

## Data Pipeline

### 1. Data Ingestion
- Downloads TLC monthly parquet files
- Stores raw data in HDFS
- Handles Yellow Taxi and HVFHV separately

---

### 2. Spark Data Processing

#### Cleaning
- Remove invalid timestamps
- Filter unrealistic distance/duration values
- Handle missing/null values
- Normalize schema across datasets

#### Feature Engineering
- pickup_hour, pickup_day_of_week
- pickup_month, is_weekend
- trip_duration, speed
- source_id (Yellow vs HVFHV)

---

### 3. Aggregation (Iceberg)

- Group data by:
  - pickup zone (PULocationID)
  - time (hour, day, month)
- Generate:
  - EDA tables
  - ML training dataset

---

## Iceberg Tables

### ML Table

```sql
CREATE TABLE nyc.taxi_demand_ml (
  source_id INT,
  source_name STRING,
  PULocationID BIGINT,
  pickup_year INT,
  pickup_month INT,
  pickup_day_of_week INT,
  pickup_hour INT,
  is_weekend BOOLEAN,
  trip_count BIGINT
);
