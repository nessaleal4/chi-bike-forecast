import requests
import zipfile
import os
import polars as pl

DIVVY_S3 = "https://divvy-tripdata.s3.amazonaws.com"


dtypes = {
    "ride_id": pl.String,
    "rideable_type": pl.String,
    "started_at": pl.Datetime,
    "ended_at": pl.Datetime,
    "start_station_name": pl.String,
    "start_station_id": pl.String,
    "end_station_name": pl.String,
    "end_station_id": pl.String,
    "start_lat": pl.Float64,
    "start_lng": pl.Float64,
    "end_lat": pl.Float64,
    "end_lng": pl.Float64,
    "member_casual": pl.String,
}


def download_file(file: str):
    resp = requests.get(f"{DIVVY_S3}/{file}")
    with open(f"../data/raw/{file}", "wb") as f:
        f.write(resp.content)


def download_all_months(start=(2020, 4), end=(2020, 4)):
    cur_year = start[0]
    cur_month = start[1]

    end_year = end[0]
    end_month = end[1]

    while (cur_year < end_year) or (
        (cur_year == end_year) and (cur_month <= end_month)
    ):
        print(cur_month, cur_year)
        download_file(f"{cur_year}{cur_month:02}-divvy-tripdata.zip")
        if cur_month % 12 == 0:
            cur_month = 0
            cur_year += 1
        cur_month += 1


def extract_zips(folder="../data/raw"):
    for file_name in os.listdir(folder):
        if file_name.endswith(".zip"):
            with zipfile.ZipFile(f"{folder}/{file_name}", "r") as zip_ref:
                zip_ref.extractall(folder)
            os.remove(f"{folder}/{file_name}")


def merge_csvs_to_parquet(folder="../data/raw", finished_file="../data/finished"):
    data_frames = []
    for file_name in os.listdir(folder):
        if file_name.endswith(".csv"):
            df = pl.scan_csv(f"{folder}/{file_name}", dtypes=dtypes)
            data_frames.append(df)

    pl.concat(data_frames).sink_parquet(finished_file)

    for file_name in os.listdir(folder):
        if file_name.endswith(".csv"):
            os.remove(f"{folder}/{file_name}")


if __name__ == "__main__":
    download_all_months((2020, 4), (2024, 3))
    extract_zips()
    merge_csvs_to_parquet(finished_file="../data/finished/divvy-tripdata.parquet")
