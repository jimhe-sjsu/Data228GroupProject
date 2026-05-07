import argparse
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Tuple

from pyspark.sql import SparkSession


TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
DATASETS = {
    "yellow": {
        "label": "Yellow Taxi",
        "prefix": "yellow_tripdata",
        "download_dir": Path("/tmp/yellow_taxi_downloads"),
        "hdfs_dir": "hdfs://namenode:9000/user/data/nyc_taxi/raw/yellow_taxi",
    },
    "fhvhv": {
        "label": "High Volume FHV",
        "prefix": "fhvhv_tripdata",
        "download_dir": Path("/tmp/fhvhv_downloads"),
        "hdfs_dir": "hdfs://namenode:9000/user/data/nyc_taxi/raw/hvfhv",
    },
}


def dataset_config(dataset: str) -> dict:
    if dataset not in DATASETS:
        raise ValueError(f"Unsupported dataset: {dataset}")
    return DATASETS[dataset]


def raw_file_name(dataset: str, year: int, month: int) -> str:
    prefix = dataset_config(dataset)["prefix"]
    return f"{prefix}_{year}-{month:02d}.parquet"


def month_url(dataset: str, year: int, month: int) -> str:
    return f"{TLC_BASE_URL}/{raw_file_name(dataset, year, month)}"


def year_months(start_year: int, end_year: int) -> List[Tuple[int, int]]:
    return [(year, month) for year in range(start_year, end_year + 1) for month in range(1, 13)]


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("ingest_tlc_tripdata")
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
        .getOrCreate()
    )


def hdfs_filesystem(spark: SparkSession, hdfs_path: str):
    path = spark._jvm.org.apache.hadoop.fs.Path(hdfs_path)
    return path.getFileSystem(spark._jsc.hadoopConfiguration())


def hdfs_file_has_data(spark: SparkSession, hdfs_path: str) -> bool:
    path = spark._jvm.org.apache.hadoop.fs.Path(hdfs_path)
    fs = hdfs_filesystem(spark, hdfs_path)
    return fs.exists(path) and fs.isFile(path) and fs.getFileStatus(path).getLen() > 0


def make_hdfs_dir(spark: SparkSession, hdfs_dir: str) -> None:
    path = spark._jvm.org.apache.hadoop.fs.Path(hdfs_dir)
    fs = hdfs_filesystem(spark, hdfs_dir)
    fs.mkdirs(path)


def download_file(dataset: str, year: int, month: int, download_dir: Path) -> Tuple[Path, bool]:
    download_dir.mkdir(parents=True, exist_ok=True)
    filename = raw_file_name(dataset, year, month)
    local_path = download_dir / filename

    if local_path.exists() and local_path.stat().st_size > 0:
        print(f"Local file exists, skipping download: {local_path}")
        return local_path, False

    url = month_url(dataset, year, month)
    print(f"Downloading {url}")
    try:
        urllib.request.urlretrieve(url, local_path)
    except urllib.error.HTTPError as exc:
        if exc.code in (403, 404):
            print(f"Remote file unavailable, skipping: {url}")
            return local_path, False
        raise
    return local_path, True


def upload_to_hdfs(spark: SparkSession, local_path: Path, hdfs_dir: str, overwrite: bool) -> None:
    hdfs_target = f"{hdfs_dir.rstrip('/')}/{local_path.name}"
    j_path = spark._jvm.org.apache.hadoop.fs.Path
    fs = hdfs_filesystem(spark, hdfs_target)
    target_path = j_path(hdfs_target)

    if fs.exists(target_path):
        target_len = fs.getFileStatus(target_path).getLen()
        if not overwrite and target_len > 0:
            print(f"HDFS file exists, skipping upload: {hdfs_target}")
            return
        if not overwrite and target_len == 0:
            print(f"Removing zero-byte HDFS placeholder: {hdfs_target}")
            fs.delete(target_path, False)

    print(f"Uploading {local_path} -> {hdfs_target}")
    fs.mkdirs(target_path.getParent())
    fs.copyFromLocalFile(False, overwrite, j_path(local_path.resolve().as_uri()), target_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Download NYC TLC monthly parquet files into HDFS.")
    parser.add_argument("--dataset", choices=sorted(DATASETS.keys()), default="yellow")
    parser.add_argument("--start-year", type=int, default=2018)
    parser.add_argument("--end-year", type=int, default=2026)
    parser.add_argument("--hdfs-dir", default=None)
    parser.add_argument("--download-dir", type=Path, default=None)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--keep-local", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if args.start_year > args.end_year:
        raise ValueError("--start-year must be less than or equal to --end-year")
    if args.workers < 1:
        raise ValueError("--workers must be at least 1")

    dataset_cfg = dataset_config(args.dataset)
    hdfs_dir = args.hdfs_dir or dataset_cfg["hdfs_dir"]
    download_dir = args.download_dir or dataset_cfg["download_dir"]

    spark = create_spark_session()
    try:
        months = year_months(args.start_year, args.end_year)
        make_hdfs_dir(spark, hdfs_dir)

        if not args.overwrite:
            months_to_download = []
            for year, month in months:
                hdfs_target = f"{hdfs_dir.rstrip('/')}/{raw_file_name(args.dataset, year, month)}"
                if hdfs_file_has_data(spark, hdfs_target):
                    print(f"HDFS file exists, skipping: {hdfs_target}")
                else:
                    months_to_download.append((year, month))
            months = months_to_download

        print(
            f"Ingesting {len(months)} {dataset_cfg['label']} monthly parquet files "
            f"from {args.start_year} to {args.end_year} into {hdfs_dir}"
        )

        if args.dry_run:
            for year, month in months:
                print(f"Would ingest {month_url(args.dataset, year, month)}")
            return

        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = {
                executor.submit(download_file, args.dataset, year, month, download_dir): (year, month)
                for year, month in months
            }

            for future in as_completed(futures):
                year, month = futures[future]
                try:
                    local_path, downloaded_now = future.result()
                    if not local_path.exists() or local_path.stat().st_size == 0:
                        continue
                    upload_to_hdfs(spark, local_path, hdfs_dir, args.overwrite)
                    if downloaded_now and not args.keep_local:
                        try:
                            local_path.unlink()
                        except FileNotFoundError:
                            pass
                except Exception as exc:
                    print(f"Failed {raw_file_name(args.dataset, year, month)}: {exc}")
                    raise

        print("Ingest complete.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
