import argparse
import csv
import logging
import os
import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("analyze_taxi_demand_ml")


DEFAULT_HDFS_URI = "hdfs://namenode:9000"
DEFAULT_WAREHOUSE = "hdfs://namenode:9000/user/data/warehouse"
DEFAULT_TABLE_PATH = "/user/data/warehouse/taxi_demand_ml"
DEFAULT_TABLE = "nyc.taxi_demand_ml"
DEFAULT_OUTPUT_DIR = "/workspace/output/taxi_demand_ml_analysis"


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Inspect the taxi_demand_ml Iceberg table, export summary CSVs, "
            "create charts, and write a balanced 200-row sample CSV."
        )
    )
    parser.add_argument(
        "--hdfs-uri",
        default=DEFAULT_HDFS_URI,
        help="HDFS URI used to qualify absolute HDFS paths.",
    )
    parser.add_argument(
        "--warehouse",
        default=DEFAULT_WAREHOUSE,
        help="Iceberg Hadoop catalog warehouse path.",
    )
    parser.add_argument(
        "--table-path",
        default=DEFAULT_TABLE_PATH,
        help="Iceberg table location to inspect. Absolute /user paths are treated as HDFS paths.",
    )
    parser.add_argument(
        "--table",
        default=DEFAULT_TABLE,
        help="Fallback Iceberg catalog table name.",
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help="Local output directory for reports, CSV files, and charts.",
    )
    parser.add_argument(
        "--source-one-id",
        type=int,
        default=1,
        help="Source id used for the first 100 sample rows.",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=100,
        help="Rows to sample from source-one-id and from all other sources.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for repeatable sampling.",
    )
    return parser.parse_args()


def create_spark_session(args):
    return (
        SparkSession.builder
        .appName("Analyze Taxi Demand ML")
        .config("spark.sql.catalog.nyc", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.nyc.type", "hadoop")
        .config("spark.sql.catalog.nyc.warehouse", args.warehouse)
        .getOrCreate()
    )


def qualify_hdfs_path(path, hdfs_uri):
    if path.startswith("hdfs://"):
        return path
    if path.startswith("/"):
        return hdfs_uri.rstrip("/") + path
    return path


def list_hdfs_files(spark, path):
    jvm = spark._jvm
    conf = spark._jsc.hadoopConfiguration()
    hdfs_path = jvm.org.apache.hadoop.fs.Path(path)
    fs = hdfs_path.getFileSystem(conf)

    if not fs.exists(hdfs_path):
        raise FileNotFoundError("HDFS path does not exist: {}".format(path))

    rows = []
    file_iter = fs.listFiles(hdfs_path, True)
    while file_iter.hasNext():
        status = file_iter.next()
        file_path = str(status.getPath())
        rows.append(
            {
                "path": file_path,
                "size_bytes": int(status.getLen()),
                "modified_at": datetime.fromtimestamp(
                    status.getModificationTime() / 1000.0
                ).isoformat(timespec="seconds"),
                "is_parquet": str(file_path.endswith(".parquet")).lower(),
                "is_metadata": str("/metadata/" in file_path).lower(),
            }
        )
    return sorted(rows, key=lambda row: row["path"])


def load_table(spark, table_path, table_name):
    try:
        logger.info("Reading Iceberg table from path: %s", table_path)
        return spark.read.format("iceberg").load(table_path)
    except Exception as path_exc:
        logger.warning("Path load failed; falling back to catalog table %s", table_name)
        logger.warning("Path load error: %s", path_exc)
        return spark.read.table(table_name)


def ensure_output_dirs(output_dir):
    charts_dir = os.path.join(output_dir, "charts")
    os.makedirs(charts_dir, exist_ok=True)
    return charts_dir


def write_csv(path, rows, fieldnames):
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row.get(name) for name in fieldnames})


def row_to_dict(row):
    return {key: row[key] for key in row.asDict(recursive=True)}


def collect_dicts(df):
    return [row_to_dict(row) for row in df.collect()]


def numeric_value(value):
    if value is None:
        return 0.0
    return float(value)


def write_bar_svg(path, title, labels, values, x_label="", width=980, height=560):
    margin_left = 220
    margin_right = 40
    margin_top = 70
    margin_bottom = 70
    plot_width = width - margin_left - margin_right
    bar_height = 24
    gap = 12
    needed_height = margin_top + margin_bottom + len(labels) * (bar_height + gap)
    height = max(height, needed_height)
    max_value = max([numeric_value(value) for value in values] + [1.0])

    parts = [
        '<svg xmlns="http://www.w3.org/2000/svg" width="{0}" height="{1}" viewBox="0 0 {0} {1}">'.format(width, height),
        '<rect width="100%" height="100%" fill="#ffffff"/>',
        '<text x="{0}" y="34" font-family="Arial" font-size="24" font-weight="700" fill="#1f2933">{1}</text>'.format(margin_left, title),
    ]

    for idx, (label, value) in enumerate(zip(labels, values)):
        value = numeric_value(value)
        y = margin_top + idx * (bar_height + gap)
        bar_width = 0 if max_value == 0 else (value / max_value) * plot_width
        parts.extend(
            [
                '<text x="{0}" y="{1}" font-family="Arial" font-size="13" text-anchor="end" fill="#334155">{2}</text>'.format(
                    margin_left - 14, y + 17, escape_xml(str(label))
                ),
                '<rect x="{0}" y="{1}" width="{2:.2f}" height="{3}" rx="2" fill="#2563eb"/>'.format(
                    margin_left, y, bar_width, bar_height
                ),
                '<text x="{0}" y="{1}" font-family="Arial" font-size="12" fill="#0f172a">{2:,.0f}</text>'.format(
                    margin_left + min(bar_width + 8, plot_width - 80), y + 17, value
                ),
            ]
        )

    if x_label:
        parts.append(
            '<text x="{0}" y="{1}" font-family="Arial" font-size="13" fill="#475569">{2}</text>'.format(
                margin_left, height - 28, escape_xml(x_label)
            )
        )
    parts.append("</svg>")
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(parts))


def write_line_svg(path, title, rows, x_col, y_col, series_col, width=980, height=560):
    margin_left = 80
    margin_right = 150
    margin_top = 70
    margin_bottom = 80
    plot_width = width - margin_left - margin_right
    plot_height = height - margin_top - margin_bottom
    colors = ["#2563eb", "#dc2626", "#059669", "#7c3aed", "#ea580c"]

    x_values = sorted({row[x_col] for row in rows})
    series_values = sorted({row[series_col] for row in rows})
    y_max = max([numeric_value(row[y_col]) for row in rows] + [1.0])
    x_index = {value: idx for idx, value in enumerate(x_values)}
    denom = max(len(x_values) - 1, 1)

    def point(row):
        x = margin_left + (x_index[row[x_col]] / denom) * plot_width
        y = margin_top + plot_height - (numeric_value(row[y_col]) / y_max) * plot_height
        return "{:.2f},{:.2f}".format(x, y)

    parts = [
        '<svg xmlns="http://www.w3.org/2000/svg" width="{0}" height="{1}" viewBox="0 0 {0} {1}">'.format(width, height),
        '<rect width="100%" height="100%" fill="#ffffff"/>',
        '<text x="{0}" y="34" font-family="Arial" font-size="24" font-weight="700" fill="#1f2933">{1}</text>'.format(margin_left, title),
        '<line x1="{0}" y1="{1}" x2="{2}" y2="{1}" stroke="#94a3b8"/>'.format(margin_left, margin_top + plot_height, margin_left + plot_width),
        '<line x1="{0}" y1="{1}" x2="{0}" y2="{2}" stroke="#94a3b8"/>'.format(margin_left, margin_top, margin_top + plot_height),
    ]

    for tick in range(5):
        value = y_max * tick / 4
        y = margin_top + plot_height - (value / y_max) * plot_height
        parts.extend(
            [
                '<line x1="{0}" y1="{1:.2f}" x2="{2}" y2="{1:.2f}" stroke="#e2e8f0"/>'.format(margin_left, y, margin_left + plot_width),
                '<text x="{0}" y="{1:.2f}" font-family="Arial" font-size="11" text-anchor="end" fill="#475569">{2:,.0f}</text>'.format(margin_left - 8, y + 4, value),
            ]
        )

    for idx, x_value in enumerate(x_values):
        if len(x_values) > 16 and idx % max(len(x_values) // 12, 1) != 0:
            continue
        x = margin_left + (idx / denom) * plot_width
        parts.append(
            '<text x="{0:.2f}" y="{1}" font-family="Arial" font-size="11" text-anchor="middle" fill="#475569">{2}</text>'.format(
                x, height - 44, escape_xml(str(x_value))
            )
        )

    for series_idx, series_value in enumerate(series_values):
        color = colors[series_idx % len(colors)]
        series_rows = sorted(
            [row for row in rows if row[series_col] == series_value],
            key=lambda row: x_index[row[x_col]],
        )
        if not series_rows:
            continue
        points = " ".join(point(row) for row in series_rows)
        legend_y = margin_top + series_idx * 24
        parts.extend(
            [
                '<polyline fill="none" stroke="{0}" stroke-width="3" points="{1}"/>'.format(color, points),
                '<rect x="{0}" y="{1}" width="14" height="14" fill="{2}"/>'.format(width - margin_right + 25, legend_y - 11, color),
                '<text x="{0}" y="{1}" font-family="Arial" font-size="13" fill="#334155">{2}</text>'.format(
                    width - margin_right + 46, legend_y, escape_xml(str(series_value))
                ),
            ]
        )

    parts.append("</svg>")
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(parts))


def escape_xml(value):
    return (
        value.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def write_markdown_report(path, table_path, table_name, file_rows, schema_rows, summary_rows, output_files):
    total_size = sum(int(row["size_bytes"]) for row in file_rows)
    parquet_files = [row for row in file_rows if row["is_parquet"] == "true"]

    lines = [
        "# taxi_demand_ml Analysis",
        "",
        "Generated at: {}".format(datetime.now().isoformat(timespec="seconds")),
        "",
        "## Source",
        "",
        "- HDFS table path: `{}`".format(table_path),
        "- Fallback catalog table: `{}`".format(table_name),
        "- Files found: {:,}".format(len(file_rows)),
        "- Parquet data files found: {:,}".format(len(parquet_files)),
        "- Total file size: {:,} bytes".format(total_size),
        "",
        "## Schema",
        "",
        "| Column | Type |",
        "|---|---|",
    ]

    for row in schema_rows:
        lines.append("| {} | {} |".format(row["column"], row["type"]))

    lines.extend(["", "## Source Summary", "", "| Source ID | Source Name | Rows | Total Trips | Avg Trips Per Row | Distinct Pickup Locations |", "|---:|---|---:|---:|---:|---:|"])
    for row in summary_rows:
        lines.append(
            "| {source_id} | {source_name} | {row_count:,} | {total_trip_count:,} | {avg_trip_count:.2f} | {distinct_pickup_locations:,} |".format(
                source_id=row.get("source_id"),
                source_name=row.get("source_name"),
                row_count=int(row.get("row_count") or 0),
                total_trip_count=int(row.get("total_trip_count") or 0),
                avg_trip_count=float(row.get("avg_trip_count") or 0),
                distinct_pickup_locations=int(row.get("distinct_pickup_locations") or 0),
            )
        )

    lines.extend(["", "## Outputs", ""])
    for output_file in output_files:
        lines.append("- `{}`".format(output_file))

    with open(path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")


def build_outputs(df, file_rows, args):
    output_dir = args.output_dir
    charts_dir = ensure_output_dirs(output_dir)

    schema_rows = [
        {"column": field.name, "type": field.dataType.simpleString()}
        for field in df.schema.fields
    ]
    write_csv(
        os.path.join(output_dir, "schema.csv"),
        schema_rows,
        ["column", "type"],
    )

    write_csv(
        os.path.join(output_dir, "file_inventory.csv"),
        file_rows,
        ["path", "size_bytes", "modified_at", "is_parquet", "is_metadata"],
    )

    source_summary_df = (
        df.groupBy("source_id", "source_name")
        .agg(
            F.count("*").alias("row_count"),
            F.sum("trip_count").cast("long").alias("total_trip_count"),
            F.avg("trip_count").alias("avg_trip_count"),
            F.min("trip_count").alias("min_trip_count"),
            F.max("trip_count").alias("max_trip_count"),
            F.countDistinct("PULocationID").alias("distinct_pickup_locations"),
            F.min("pickup_year").alias("min_pickup_year"),
            F.max("pickup_year").alias("max_pickup_year"),
        )
        .orderBy("source_id")
    )
    source_summary = collect_dicts(source_summary_df)
    write_csv(
        os.path.join(output_dir, "source_summary.csv"),
        source_summary,
        [
            "source_id",
            "source_name",
            "row_count",
            "total_trip_count",
            "avg_trip_count",
            "min_trip_count",
            "max_trip_count",
            "distinct_pickup_locations",
            "min_pickup_year",
            "max_pickup_year",
        ],
    )

    hourly_rows = collect_dicts(
        df.groupBy("source_name", "pickup_hour")
        .agg(F.sum("trip_count").cast("long").alias("total_trip_count"))
        .orderBy("source_name", "pickup_hour")
    )
    write_csv(
        os.path.join(output_dir, "hourly_demand.csv"),
        hourly_rows,
        ["source_name", "pickup_hour", "total_trip_count"],
    )

    monthly_rows = collect_dicts(
        df.groupBy("source_name", "pickup_year", "pickup_month")
        .agg(F.sum("trip_count").cast("long").alias("total_trip_count"))
        .withColumn("year_month", F.format_string("%04d-%02d", F.col("pickup_year"), F.col("pickup_month")))
        .orderBy("year_month", "source_name")
        .select("source_name", "year_month", "total_trip_count")
    )
    write_csv(
        os.path.join(output_dir, "monthly_demand.csv"),
        monthly_rows,
        ["source_name", "year_month", "total_trip_count"],
    )

    top_pickup_rows = collect_dicts(
        df.groupBy("source_name", "PULocationID")
        .agg(F.sum("trip_count").cast("long").alias("total_trip_count"))
        .orderBy(F.desc("total_trip_count"))
        .limit(20)
    )
    write_csv(
        os.path.join(output_dir, "top_pickup_locations.csv"),
        top_pickup_rows,
        ["source_name", "PULocationID", "total_trip_count"],
    )

    top_weekday_rows = collect_dicts(
        df.groupBy("source_name", "pickup_day_of_week")
        .agg(F.sum("trip_count").cast("long").alias("total_trip_count"))
        .orderBy("source_name", "pickup_day_of_week")
    )
    write_csv(
        os.path.join(output_dir, "weekday_demand.csv"),
        top_weekday_rows,
        ["source_name", "pickup_day_of_week", "total_trip_count"],
    )

    write_bar_svg(
        os.path.join(charts_dir, "source_total_trips.svg"),
        "Total Trips by Source",
        [str(row["source_name"]) for row in source_summary],
        [row["total_trip_count"] for row in source_summary],
        "total trip_count",
    )
    write_line_svg(
        os.path.join(charts_dir, "hourly_demand_by_source.svg"),
        "Hourly Demand by Source",
        hourly_rows,
        "pickup_hour",
        "total_trip_count",
        "source_name",
    )
    write_line_svg(
        os.path.join(charts_dir, "monthly_demand_by_source.svg"),
        "Monthly Demand by Source",
        monthly_rows,
        "year_month",
        "total_trip_count",
        "source_name",
    )
    write_bar_svg(
        os.path.join(charts_dir, "top_pickup_locations.svg"),
        "Top Pickup Locations by Total Trips",
        [
            "{} / PU {}".format(row["source_name"], row["PULocationID"])
            for row in top_pickup_rows
        ],
        [row["total_trip_count"] for row in top_pickup_rows],
        "total trip_count",
    )
    sample_columns = df.columns
    sample_source_one = (
        df.filter(F.col("source_id") == args.source_one_id)
        .orderBy(F.rand(args.seed))
        .limit(args.sample_size)
        .withColumn("sample_group", F.lit("source_{}".format(args.source_one_id)))
    )
    sample_other = (
        df.filter(F.col("source_id") != args.source_one_id)
        .orderBy(F.rand(args.seed + 1))
        .limit(args.sample_size)
        .withColumn("sample_group", F.lit("other_sources"))
    )
    sample_rows = collect_dicts(
        sample_source_one.unionByName(sample_other).select(["sample_group"] + sample_columns)
    )
    sample_path = os.path.join(output_dir, "sample_200_rows.csv")
    write_csv(sample_path, sample_rows, ["sample_group"] + sample_columns)

    output_files = [
        os.path.join(output_dir, name)
        for name in [
            "analysis_report.md",
            "file_inventory.csv",
            "schema.csv",
            "source_summary.csv",
            "hourly_demand.csv",
            "monthly_demand.csv",
            "weekday_demand.csv",
            "top_pickup_locations.csv",
            "sample_200_rows.csv",
        ]
    ]
    output_files.extend(
        [
            os.path.join(charts_dir, name)
            for name in [
                "source_total_trips.svg",
                "hourly_demand_by_source.svg",
                "monthly_demand_by_source.svg",
                "top_pickup_locations.svg",
            ]
        ]
    )

    write_markdown_report(
        os.path.join(output_dir, "analysis_report.md"),
        qualify_hdfs_path(args.table_path, args.hdfs_uri),
        args.table,
        file_rows,
        schema_rows,
        source_summary,
        output_files,
    )

    return output_files


def main():
    args = parse_args()
    table_path = qualify_hdfs_path(args.table_path, args.hdfs_uri)
    spark = create_spark_session(args)

    try:
        os.makedirs(args.output_dir, exist_ok=True)
        file_rows = list_hdfs_files(spark, table_path)
        df = load_table(spark, table_path, args.table)

        required_columns = [
            "source_id",
            "source_name",
            "PULocationID",
            "pickup_year",
            "pickup_month",
            "pickup_hour",
            "pickup_day_of_week",
            "trip_count",
        ]
        missing = [col for col in required_columns if col not in df.columns]
        if missing:
            raise RuntimeError("taxi_demand_ml is missing column(s): {}".format(", ".join(missing)))

        row_count = df.count()
        if row_count == 0:
            raise RuntimeError("taxi_demand_ml has no rows")

        output_files = build_outputs(df, file_rows, args)

        print("\n===== TAXI DEMAND ML ANALYSIS COMPLETE =====")
        print("Rows analyzed: {:,}".format(row_count))
        print("HDFS files inspected: {:,}".format(len(file_rows)))
        print("Output directory: {}".format(args.output_dir))
        print("Sample CSV: {}".format(os.path.join(args.output_dir, "sample_200_rows.csv")))
        print("Report: {}".format(os.path.join(args.output_dir, "analysis_report.md")))
        print("Charts:")
        for output_file in output_files:
            if output_file.endswith(".svg"):
                print(" - {}".format(output_file))

    except Exception as exc:
        logger.error("Analysis failed: %s", exc)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
