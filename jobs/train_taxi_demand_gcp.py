import argparse
import gc

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor, LinearRegression, RandomForestRegressor
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from xgboost.spark import SparkXGBRegressor


XGBOOST_MAX_TRAIN_ROWS = None


def parse_args():
    parser = argparse.ArgumentParser(description="Train Spark MLlib taxi-demand models on Dataproc.")
    parser.add_argument("--input", required=True, help="Input taxi-demand Parquet path, for example gs://bucket/data/taxi_demand_ml_parquet")
    parser.add_argument("--output", required=True, help="Output directory, for example gs://bucket/output/taxi_demand_training")
    parser.add_argument("--demo-mode", action="store_true", help="Use every 14th modeling day for a quick smoke test.")
    parser.add_argument("--demo-date-stride-days", type=int, default=14)
    parser.add_argument("--shuffle-partitions", type=int, default=48)
    return parser.parse_args()


def create_spark(shuffle_partitions):
    return (
        SparkSession.builder
        .appName("DATA228 Taxi Demand Spark MLlib Tuning")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.default.parallelism", str(shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def date_from_year_month_day(year_col, month_col, day_col):
    return F.to_date(
        F.concat_ws(
            "-",
            year_col.cast("string"),
            F.format_string("%02d", month_col),
            F.format_string("%02d", day_col),
        )
    )


def build_source_agnostic_demand(input_df):
    select_columns = [
        F.col("PULocationID").cast("long").alias("PULocationID"),
        F.col("pickup_year").cast("int").alias("pickup_year"),
        F.col("pickup_month").cast("int").alias("pickup_month"),
        F.col("pickup_day_of_week").cast("int").alias("pickup_day_of_week"),
        F.col("pickup_hour").cast("int").alias("pickup_hour"),
        F.col("is_weekend").cast("boolean").alias("is_weekend"),
        F.col("trip_count").cast("double").alias("trip_count"),
    ]

    if "pickup_date" in input_df.columns:
        select_columns.append(F.to_date("pickup_date").alias("pickup_date"))
    if "pickup_day" in input_df.columns:
        select_columns.append(F.col("pickup_day").cast("int").alias("pickup_day"))

    base_df = input_df.select(*select_columns)

    if "pickup_day" not in base_df.columns:
        base_df = base_df.withColumn("pickup_day", F.dayofmonth("pickup_date"))
    if "pickup_date" not in base_df.columns:
        base_df = base_df.withColumn(
            "pickup_date",
            date_from_year_month_day(F.col("pickup_year"), F.col("pickup_month"), F.col("pickup_day")),
        )

    return (
        base_df
        .dropna(subset=[
            "PULocationID",
            "pickup_date",
            "pickup_year",
            "pickup_month",
            "pickup_day",
            "pickup_day_of_week",
            "pickup_hour",
            "is_weekend",
            "trip_count",
        ])
        .groupBy(
            "PULocationID",
            "pickup_date",
            "pickup_year",
            "pickup_month",
            "pickup_day",
            "pickup_day_of_week",
            "pickup_hour",
            "is_weekend",
        )
        .agg(F.sum("trip_count").alias("trip_count"))
        .withColumn("month_index", F.col("pickup_year") * F.lit(12) + F.col("pickup_month"))
        .withColumn("PULocationID_string", F.col("PULocationID").cast("string"))
        .withColumn("is_weekend_int", F.col("is_weekend").cast("int"))
    )


def select_modeling_dates(demand_df, demo_mode, demo_date_stride_days):
    date_df = demand_df.select(
        "pickup_date",
        "pickup_year",
        "pickup_month",
        "pickup_day",
        "pickup_day_of_week",
        "is_weekend",
        "is_weekend_int",
        "month_index",
    ).distinct()

    if demo_mode:
        date_df = date_df.filter(F.pmod(F.dayofyear("pickup_date"), F.lit(demo_date_stride_days)) == F.lit(1))

    return date_df


def build_complete_location_hour_demand(spark, positive_demand_df, demo_mode, demo_date_stride_days, shuffle_partitions):
    locations_df = positive_demand_df.select("PULocationID", "PULocationID_string").distinct()
    dates_df = select_modeling_dates(positive_demand_df, demo_mode, demo_date_stride_days)
    hours_df = spark.range(0, 24).select(F.col("id").cast("int").alias("pickup_hour"))

    grid_df = locations_df.crossJoin(dates_df).crossJoin(hours_df)
    demand_values_df = positive_demand_df.select("PULocationID", "pickup_date", "pickup_hour", "trip_count")

    return (
        grid_df
        .join(demand_values_df, ["PULocationID", "pickup_date", "pickup_hour"], "left")
        .withColumn("trip_count", F.coalesce(F.col("trip_count"), F.lit(0.0)))
        .repartition(shuffle_partitions, "pickup_year", "pickup_month")
    )


def build_history_tables(demand_df):
    location_month = (
        demand_df
        .groupBy("PULocationID", "month_index")
        .agg(F.avg("trip_count").alias("month_location_avg"))
    )
    location_window = (
        Window
        .partitionBy("PULocationID")
        .orderBy("month_index")
        .rowsBetween(Window.unboundedPreceding, -1)
    )
    location_history = (
        location_month
        .withColumn("location_avg", F.avg("month_location_avg").over(location_window))
        .select("PULocationID", "month_index", "location_avg")
    )

    location_hour_month = (
        demand_df
        .groupBy("PULocationID", "pickup_hour", "month_index")
        .agg(
            F.avg("trip_count").alias("month_location_hour_avg"),
            F.avg(F.when(F.col("trip_count") == 0, 1.0).otherwise(0.0)).alias("month_location_hour_zero_rate"),
        )
    )
    location_hour_window = (
        Window
        .partitionBy("PULocationID", "pickup_hour")
        .orderBy("month_index")
        .rowsBetween(Window.unboundedPreceding, -1)
    )
    location_hour_history = (
        location_hour_month
        .withColumn("location_hour_avg", F.avg("month_location_hour_avg").over(location_hour_window))
        .withColumn("location_hour_zero_rate", F.avg("month_location_hour_zero_rate").over(location_hour_window))
        .select("PULocationID", "pickup_hour", "month_index", "location_hour_avg", "location_hour_zero_rate")
    )

    location_dow_hour_month = (
        demand_df
        .groupBy("PULocationID", "pickup_day_of_week", "pickup_hour", "month_index")
        .agg(F.avg("trip_count").alias("month_location_dow_hour_avg"))
    )
    location_dow_hour_window = (
        Window
        .partitionBy("PULocationID", "pickup_day_of_week", "pickup_hour")
        .orderBy("month_index")
        .rowsBetween(Window.unboundedPreceding, -1)
    )
    location_dow_hour_history = (
        location_dow_hour_month
        .withColumn("location_dow_hour_avg", F.avg("month_location_dow_hour_avg").over(location_dow_hour_window))
        .select("PULocationID", "pickup_day_of_week", "pickup_hour", "month_index", "location_dow_hour_avg")
    )

    previous_month_location_hour = (
        location_hour_month
        .select(
            "PULocationID",
            "pickup_hour",
            (F.col("month_index") + F.lit(1)).alias("month_index"),
            F.col("month_location_hour_avg").alias("previous_month_location_hour_avg"),
        )
    )

    hour_month = (
        demand_df
        .groupBy("pickup_hour", "month_index")
        .agg(F.avg("trip_count").alias("month_hour_avg"))
    )
    hour_window = (
        Window
        .partitionBy("pickup_hour")
        .orderBy("month_index")
        .rowsBetween(Window.unboundedPreceding, -1)
    )
    hour_history = (
        hour_month
        .withColumn("hour_avg", F.avg("month_hour_avg").over(hour_window))
        .select("pickup_hour", "month_index", "hour_avg")
    )

    return {
        "location_history": location_history,
        "location_hour_history": location_hour_history,
        "location_dow_hour_history": location_dow_hour_history,
        "previous_month_location_hour": previous_month_location_hour,
        "hour_history": hour_history,
    }


def add_historical_features(input_df, history_tables):
    return (
        input_df
        .join(history_tables["location_history"], ["PULocationID", "month_index"], "left")
        .join(history_tables["location_hour_history"], ["PULocationID", "pickup_hour", "month_index"], "left")
        .join(history_tables["location_dow_hour_history"], ["PULocationID", "pickup_day_of_week", "pickup_hour", "month_index"], "left")
        .join(history_tables["previous_month_location_hour"], ["PULocationID", "pickup_hour", "month_index"], "left")
        .join(history_tables["hour_history"], ["pickup_hour", "month_index"], "left")
        .withColumn("location_avg", F.coalesce("location_avg", "hour_avg", F.lit(0.0)))
        .withColumn("location_hour_avg", F.coalesce("location_hour_avg", "location_avg", "hour_avg", F.lit(0.0)))
        .withColumn("location_dow_hour_avg", F.coalesce("location_dow_hour_avg", "location_hour_avg", "location_avg", "hour_avg", F.lit(0.0)))
        .withColumn(
            "previous_month_location_hour_avg",
            F.coalesce("previous_month_location_hour_avg", "location_hour_avg", "location_avg", "hour_avg", F.lit(0.0)),
        )
        .withColumn("location_hour_zero_rate", F.coalesce("location_hour_zero_rate", F.lit(1.0)))
        .drop("hour_avg")
    )


def evaluate_predictions(predictions):
    metric_row = (
        predictions
        .select(
            F.col("label").cast("double").alias("label"),
            F.col("prediction").cast("double").alias("prediction"),
        )
        .where(F.col("label").isNotNull() & F.col("prediction").isNotNull())
        .select(
            "label",
            "prediction",
            F.pow(F.col("label") - F.col("prediction"), 2).alias("squared_error"),
            F.abs(F.col("label") - F.col("prediction")).alias("absolute_error"),
            F.pow(F.col("label"), 2).alias("label_squared"),
        )
        .agg(
            F.count("*").alias("row_count"),
            F.sum("squared_error").alias("sse"),
            F.sum("absolute_error").alias("absolute_error_sum"),
            F.sum("label").alias("label_sum"),
            F.sum("label_squared").alias("label_squared_sum"),
        )
        .first()
    )

    row_count = metric_row["row_count"] or 0
    if row_count == 0:
        raise ValueError("No rows available for model evaluation")

    sse = float(metric_row["sse"] or 0.0)
    absolute_error_sum = float(metric_row["absolute_error_sum"] or 0.0)
    label_sum = float(metric_row["label_sum"] or 0.0)
    label_squared_sum = float(metric_row["label_squared_sum"] or 0.0)

    rmse = (sse / row_count) ** 0.5
    mae = absolute_error_sum / row_count
    sst = label_squared_sum - ((label_sum * label_sum) / row_count)
    r2 = 0.0 if sst <= 0 else 1.0 - (sse / sst)

    return {"rmse": rmse, "mae": mae, "r2": r2, "row_count": row_count}


def params_text(params):
    return ", ".join("{}={}".format(key, params[key]) for key in sorted(params))


def make_estimator(model_name, params):
    if model_name == "LinearRegression":
        return LinearRegression(
            featuresCol="features",
            labelCol="label",
            predictionCol="prediction",
            maxIter=params["maxIter"],
            regParam=params["regParam"],
            elasticNetParam=params["elasticNetParam"],
        )
    if model_name == "RandomForestRegressor":
        return RandomForestRegressor(
            featuresCol="features",
            labelCol="label",
            predictionCol="prediction",
            numTrees=params["numTrees"],
            maxDepth=params["maxDepth"],
            minInstancesPerNode=params["minInstancesPerNode"],
            subsamplingRate=params.get("subsamplingRate", 0.8),
            featureSubsetStrategy=params.get("featureSubsetStrategy", "sqrt"),
            maxMemoryInMB=params.get("maxMemoryInMB", 512),
            seed=42,
        )
    if model_name == "GBTRegressor":
        return GBTRegressor(
            featuresCol="features",
            labelCol="label",
            predictionCol="prediction",
            maxIter=params["maxIter"],
            maxDepth=params["maxDepth"],
            minInstancesPerNode=params["minInstancesPerNode"],
            subsamplingRate=params.get("subsamplingRate", 0.7),
            maxMemoryInMB=params.get("maxMemoryInMB", 512),
            seed=42,
        )
    if model_name == "SparkXGBRegressor":
        return SparkXGBRegressor(
            features_col="features",
            label_col="label",
            prediction_col="prediction",
            objective="reg:squarederror",
            num_workers=params.get("num_workers", 1),
            n_estimators=params["n_estimators"],
            max_depth=params["max_depth"],
            learning_rate=params["learning_rate"],
            subsample=params.get("subsample", 0.8),
            colsample_bytree=params.get("colsample_bytree", 0.8),
            max_bin=params.get("max_bin", 64),
            verbosity=0,
            random_state=42,
        )
    raise ValueError("Unknown model: {}".format(model_name))


def balanced_model_configs():
    return [
        ("LinearRegression", "linear_default", {"maxIter": 30, "regParam": 0.0, "elasticNetParam": 0.0}),
        ("LinearRegression", "linear_l2", {"maxIter": 30, "regParam": 0.01, "elasticNetParam": 0.0}),
        ("LinearRegression", "linear_elastic_net", {"maxIter": 30, "regParam": 0.01, "elasticNetParam": 0.5}),
        ("RandomForestRegressor", "rf_small", {"numTrees": 20, "maxDepth": 5, "minInstancesPerNode": 5}),
        ("RandomForestRegressor", "rf_medium", {"numTrees": 30, "maxDepth": 6, "minInstancesPerNode": 8}),
        ("RandomForestRegressor", "rf_deeper", {"numTrees": 40, "maxDepth": 8, "minInstancesPerNode": 10}),
        ("GBTRegressor", "gbt_small", {"maxIter": 10, "maxDepth": 2, "minInstancesPerNode": 20}),
        ("GBTRegressor", "gbt_medium", {"maxIter": 15, "maxDepth": 3, "minInstancesPerNode": 25}),
        ("GBTRegressor", "gbt_deeper", {"maxIter": 20, "maxDepth": 4, "minInstancesPerNode": 30}),
        ("SparkXGBRegressor", "xgb_tiny", {"n_estimators": 10, "max_depth": 2, "learning_rate": 0.08}),
        ("SparkXGBRegressor", "xgb_small", {"n_estimators": 20, "max_depth": 3, "learning_rate": 0.08}),
        ("SparkXGBRegressor", "xgb_fast_learning", {"n_estimators": 20, "max_depth": 3, "learning_rate": 0.12}),
        ("SparkXGBRegressor", "xgb_depth4", {"n_estimators": 20, "max_depth": 4, "learning_rate": 0.08}),
    ]


def main():
    args = parse_args()
    spark = create_spark(args.shuffle_partitions)
    spark.sparkContext.setLogLevel("WARN")
    output_path = args.output.rstrip("/")

    raw_df = spark.read.parquet(args.input)
    required_columns = {
        "PULocationID",
        "pickup_year",
        "pickup_month",
        "pickup_day_of_week",
        "pickup_hour",
        "is_weekend",
        "trip_count",
    }
    missing_columns = sorted(required_columns - set(raw_df.columns))
    if missing_columns:
        raise ValueError("Missing required columns: {}".format(missing_columns))

    positive_demand_df = build_source_agnostic_demand(raw_df)
    demand_df = build_complete_location_hour_demand(
        spark,
        positive_demand_df,
        args.demo_mode,
        args.demo_date_stride_days,
        args.shuffle_partitions,
    )
    history_tables = build_history_tables(demand_df)
    dataset = add_historical_features(demand_df, history_tables).withColumn("label", F.col("trip_count"))

    train_df = dataset.filter(F.col("pickup_year").isin([2023, 2024])).persist()
    test_df = dataset.filter(F.col("pickup_year") == 2025).persist()
    print("Train rows: {:,}".format(train_df.count()))
    print("Test rows: {:,}".format(test_df.count()))

    numeric_feature_cols = [
        "pickup_month",
        "pickup_day_of_week",
        "pickup_hour",
        "is_weekend_int",
        "location_avg",
        "location_hour_avg",
        "location_dow_hour_avg",
        "previous_month_location_hour_avg",
        "location_hour_zero_rate",
    ]

    preprocessing_pipeline = Pipeline(stages=[
        StringIndexer(inputCol="PULocationID_string", outputCol="PULocationID_index", handleInvalid="keep"),
        OneHotEncoder(inputCols=["PULocationID_index"], outputCols=["PULocationID_ohe"], handleInvalid="keep"),
        VectorAssembler(inputCols=numeric_feature_cols + ["PULocationID_ohe"], outputCol="features", handleInvalid="skip"),
    ])
    feature_model = preprocessing_pipeline.fit(train_df)

    keep_columns = [
        "PULocationID",
        "pickup_date",
        "pickup_year",
        "pickup_month",
        "pickup_day",
        "pickup_day_of_week",
        "pickup_hour",
        "trip_count",
        "label",
        "location_dow_hour_avg",
        "features",
    ]
    train_features_df = feature_model.transform(train_df).select(keep_columns).persist()
    test_features_df = feature_model.transform(test_df).select(keep_columns).persist()
    train_feature_count = train_features_df.count()
    test_feature_count = test_features_df.count()
    print("Train feature rows: {:,}".format(train_feature_count))
    print("Test feature rows: {:,}".format(test_feature_count))

    xgb_train_features_df = train_features_df
    xgb_train_feature_count = train_feature_count
    if XGBOOST_MAX_TRAIN_ROWS and train_feature_count > XGBOOST_MAX_TRAIN_ROWS:
        sample_fraction = min(1.0, XGBOOST_MAX_TRAIN_ROWS / float(train_feature_count))
        xgb_train_features_df = (
            train_features_df
            .sample(withReplacement=False, fraction=sample_fraction, seed=42)
            .limit(XGBOOST_MAX_TRAIN_ROWS)
            .persist()
        )
        xgb_train_feature_count = xgb_train_features_df.count()
        print(
            "SparkXGBRegressor train rows capped at {:,} from {:,} full training rows".format(
                xgb_train_feature_count,
                train_feature_count,
            )
        )

    baseline_metrics = evaluate_predictions(
        test_features_df.withColumn("prediction", F.col("location_dow_hour_avg")).select("label", "prediction")
    )
    evaluation_rows = [{
        "model": "NaiveHistoricalAverage",
        "config": "baseline",
        "params": "location_dow_hour_avg",
        "selection": "baseline",
        "train_row_count": 0,
        "model_artifact": "",
        **baseline_metrics,
    }]

    best_trainable_row = None
    best_trainable_model = None
    for model_name, config_name, params in balanced_model_configs():
        text = params_text(params)
        print("Training {} ({}) with {}...".format(model_name, config_name, text))
        estimator = make_estimator(model_name, params)
        fit_df = xgb_train_features_df if model_name == "SparkXGBRegressor" else train_features_df
        fit_row_count = xgb_train_feature_count if model_name == "SparkXGBRegressor" else train_feature_count
        model = estimator.fit(fit_df)
        predictions = model.transform(test_features_df).select("label", "prediction")
        metrics = evaluate_predictions(predictions)
        model_artifact = "models/{}_pipeline".format(config_name)
        PipelineModel(stages=feature_model.stages + [model]).write().overwrite().save(output_path + "/" + model_artifact)
        evaluation_row = {
            "model": model_name,
            "config": config_name,
            "params": text,
            "selection": "trainable",
            "train_row_count": fit_row_count,
            "model_artifact": model_artifact,
            **metrics,
        }
        evaluation_rows.append(evaluation_row)
        if best_trainable_row is None or evaluation_row["rmse"] < best_trainable_row["rmse"]:
            best_trainable_row = evaluation_row
            best_trainable_model = model
        else:
            del model
        print("{} {}: RMSE={:.4f}, MAE={:.4f}, R2={:.4f}".format(model_name, config_name, metrics["rmse"], metrics["mae"], metrics["r2"]))
        del predictions
        gc.collect()

    metrics_df = spark.createDataFrame(evaluation_rows)
    metrics_df.orderBy("rmse").show(truncate=False)
    metrics_df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path + "/metrics_csv")
    metrics_df.coalesce(1).write.mode("overwrite").json(output_path + "/metrics_json")

    trainable_rows = [row for row in evaluation_rows if row["selection"] == "trainable"]
    if best_trainable_model is None:
        raise RuntimeError("No trainable model completed successfully")
    best_pipeline_model = PipelineModel(stages=feature_model.stages + [best_trainable_model])

    best_trainable_model.write().overwrite().save(output_path + "/best_model")
    best_pipeline_model.write().overwrite().save(output_path + "/best_pipeline_model")

    per_model_best_rows = []
    for model_name in sorted({row["model"] for row in trainable_rows}):
        model_rows = [row for row in trainable_rows if row["model"] == model_name]
        per_model_best_rows.append(min(model_rows, key=lambda row: row["rmse"]))

    spark.createDataFrame([best_trainable_row]).coalesce(1).write.mode("overwrite").json(output_path + "/best_model_summary")
    spark.createDataFrame(per_model_best_rows).coalesce(1).write.mode("overwrite").json(output_path + "/model_summaries")
    print("Best trainable model: {} ({})".format(best_trainable_row["model"], best_trainable_row["config"]))
    print("Output written to: {}".format(output_path))
    spark.stop()


if __name__ == "__main__":
    main()
