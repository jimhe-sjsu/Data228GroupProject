# Dataset Description

## Dataset Name
NYC Yellow Taxi Trip Data (2023)

## Source
NYC Taxi and Limousine Commission (TLC)

- https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- https://data.cityofnewyork.us/Transportation/2023-Yellow-Taxi-Trip-Data/4b4i-vvec/about_data

## Dataset Overview
This project uses the 2023 NYC Yellow Taxi dataset, which contains large-scale trip-level records of taxi operations in New York City. The data is stored in monthly Parquet files and includes temporal, spatial, and trip-related attributes.

Each record represents a single taxi trip.

## Big Data Justification

This dataset satisfies the **Volume** characteristic of Big Data:

- Dataset size exceeds 5GB
- Contains millions of trip records
- High temporal granularity

Processing this dataset requires:

- **HDFS** for distributed storage
- **Apache Spark** for parallel processing

Velocity and Variety are not primary dimensions since the dataset is batch and structured.

## Relevance to Project Goal

The goal of this project is to predict:

→ **Hourly taxi demand per NYC taxi zone**

The dataset enables this by providing:

- Timestamp data → extract hour, weekday features
- Location IDs → spatial aggregation by zone
- Trip-level data → aggregation into **zone × hour demand**

## Key Attributes

- tpep_pickup_datetime
- tpep_dropoff_datetime
- PULocationID
- DOLocationID
- trip_distance
- fare_amount
- passenger_count

## Data Processing Pipeline

1. Ingestion into HDFS
2. Data cleaning (invalid trips, null values)
3. Feature engineering (time-based features)
4. Aggregation into **zone × hour demand**
5. Storage in MySQL for analysis and modeling

## Dataset Complexity

- Large-scale data volume
- Spatial variability across taxi zones
- Temporal demand patterns (hourly, daily)
- Data quality challenges (missing values, outliers)
