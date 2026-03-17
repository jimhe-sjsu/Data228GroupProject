-- ─────────────────────────────────────────────────────────────
-- create_tables.sql
-- MySQL DDL for the NYC Taxi cleaned data target table.
-- Run once before the first pipeline execution:
--     mysql -u root -p nyc_taxi < /workspace/sql/create_tables.sql
-- ─────────────────────────────────────────────────────────────

CREATE DATABASE IF NOT EXISTS nyc_taxi;
USE nyc_taxi;

DROP TABLE IF EXISTS nyc_taxi_cleaned;

CREATE TABLE nyc_taxi_cleaned (
    VendorID                  BIGINT,
    tpep_pickup_datetime      DATETIME,
    tpep_dropoff_datetime     DATETIME,
    passenger_count           DOUBLE,
    trip_distance             DOUBLE,
    RatecodeID                DOUBLE,
    PULocationID              BIGINT,
    DOLocationID              BIGINT,
    payment_type              BIGINT,
    fare_amount               DOUBLE,
    tip_amount                DOUBLE,
    total_amount              DOUBLE,
    congestion_surcharge      DOUBLE,
    airport_fee               DOUBLE,
    trip_duration_mins        DOUBLE,
    pickup_hour               INT,
    pickup_day_of_week        INT,
    pickup_month              INT,
    is_weekend                BOOLEAN,
    speed_mph                 DOUBLE,
    payment_type_label        VARCHAR(50),
    ratecode_label            VARCHAR(50),
    vendor_label              VARCHAR(50),
    day_name                  VARCHAR(20),

    INDEX idx_pickup_dt (tpep_pickup_datetime),
    INDEX idx_pu_location (PULocationID),
    INDEX idx_do_location (DOLocationID),
    INDEX idx_pickup_month (pickup_month)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
