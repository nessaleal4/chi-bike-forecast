import duckdb


if __name__ == "__main__":
    duckdb.execute(
        """INSTALL spatial; LOAD spatial;
        COPY (
            SELECT
                *,
                ST_POINT(start_lng, start_lat) AS start_pnt,
                ST_POINT(end_lng, end_lat) AS end_pnt,
                ST_Distance(start_pnt, end_pnt) AS trip_dist
            FROM read_parquet('data/finished/divvy-tripdata.parquet')
        )
        TO 'data/finished/divvy-tripdata-with-points.parquet'
        (FORMAT 'parquet');"""
    )

    duckdb.execute(
        """
        COPY (
            SELECT
                EXTRACT(HOUR FROM started_at) AS hour,
                EXTRACT(DAY FROM started_at) AS day,
                EXTRACT(MONTH FROM started_at) AS month,
                EXTRACT(YEAR FROM started_at) AS year,
                COUNT(*) AS count
            FROM
                read_parquet('data/finished/divvy-tripdata-with-points.parquet')
            GROUP BY
                EXTRACT(HOUR FROM started_at),
                EXTRACT(DAY FROM started_at),
                EXTRACT(MONTH FROM started_at),
                EXTRACT(YEAR FROM started_at)
        )
        TO 'data/processed/divvy-hourly-count.parquet'
        (FORMAT 'parquet');"""
    )

    duckdb.execute(
        """
        COPY (
            SELECT
                EXTRACT(DAY FROM started_at) AS day,
                EXTRACT(MONTH FROM started_at) AS month,
                EXTRACT(YEAR FROM started_at) AS year,
                COUNT(*) AS count
            FROM
                read_parquet('data/finished/divvy-tripdata-with-points.parquet')
            GROUP BY
                EXTRACT(DAY FROM started_at),
                EXTRACT(MONTH FROM started_at),
                EXTRACT(YEAR FROM started_at)
        )
        TO 'data/processed/divvy-daily-count.parquet'
        (FORMAT 'parquet');"""
    )

    duckdb.execute(
        """
        COPY (
            SELECT
                EXTRACT(MONTH FROM started_at) AS month,
                EXTRACT(YEAR FROM started_at) AS year,
                COUNT(*) AS count
            FROM
                read_parquet('data/finished/divvy-tripdata-with-points.parquet')
            GROUP BY
                EXTRACT(MONTH FROM started_at),
                EXTRACT(YEAR FROM started_at)
        )
        TO 'data/processed/divvy-monthly-count.parquet'
        (FORMAT 'parquet');"""
    )
