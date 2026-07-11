import os
import polars as pl
from loguru import logger
from datetime import datetime


class WeatherELTPipeline:
    """
    Manages the File-Based Medallion Architecture (Bronze -> Silver -> Gold)
    using Polars and Parquet storage for high scalability and zero-cloud cost.
    """

    def __init__(self, base_data_dir: str = "data"):
        self.base_dir = base_data_dir
        self.bronze_dir = os.path.join(base_data_dir, "bronze")
        self.silver_dir = os.path.join(base_data_dir, "silver")
        self.gold_dir = os.path.join(base_data_dir, "gold")

        # Ensure data lake directories exist locally or inside GitHub container
        os.makedirs(self.bronze_dir, exist_ok=True)
        os.makedirs(self.silver_dir, exist_ok=True)
        os.makedirs(self.gold_dir, exist_ok=True)

    # ---------------------------------------------------------
    # BRONZE LAYER (RAW DATA)
    # ---------------------------------------------------------
    def load_bronze_layer(self, df_raw: pl.DataFrame) -> None:
        """Dumps raw Polars DataFrame directly into Bronze Parquet file."""
        logger.info("Ingesting raw API logs into Bronze Layer...")

        # Add metadata auditing column seamlessly
        df_raw = df_raw.with_columns(pl.lit(datetime.now()).alias("extracted_at"))

        # Target path for bronze snapshot
        target_path = os.path.join(self.bronze_dir, "raw_weather.parquet")

        # Idempotent write: Overwrite or create fresh snapshot
        df_raw.write_parquet(target_path)
        logger.success(f"Bronze layer saved successfully at: {target_path}")

    # ---------------------------------------------------------
    # SILVER LAYER (CLEANED & CONFORMED)
    # ---------------------------------------------------------
    def process_silver_layer(self) -> None:
        """Incrementally cleans, dedupes, and casts data types from Bronze to Silver."""
        logger.info("Processing Silver layer transformations...")

        bronze_path = os.path.join(self.bronze_dir, "raw_weather.parquet")
        if not os.path.exists(bronze_path):
            raise FileNotFoundError(
                "Bronze source file missing. Aborting Silver stage."
            )

        df_bronze = pl.read_parquet(bronze_path)

        # Apply Data Quality & Data Conforming rules
        df_cleaned = df_bronze.with_columns(
            [
                pl.col("location").cast(pl.Utf8),
                pl.col("date").cast(pl.Datetime),
                pl.col("temperature_2m").cast(pl.Float32),
                pl.col("relative_humidity_2m").cast(pl.Float32),
                pl.col("precipitation").cast(pl.Float32),
                pl.col("precipitation_probability").cast(pl.Float32),
                pl.col("weather_code").cast(
                    pl.Int32
                ),  # Cast WMO code from Float to Int
            ]
        )

        # Enforce Idempotency: Deduplicate rows based on location and date window
        df_cleaned = df_cleaned.unique(subset=["location", "date"], keep="last")

        # Save to Silver Layer
        silver_path = os.path.join(self.silver_dir, "cleaned_weather.parquet")
        df_cleaned.write_parquet(silver_path)
        logger.success(f"Silver layer cleaned and saved at: {silver_path}")

    # ---------------------------------------------------------
    # GOLD LAYER (STAR SCHEMA AGGREGATION)
    # ---------------------------------------------------------
    def process_gold_layer(self) -> None:
        """Transforms conformed Silver data into analytical Star Schema Parquet files."""
        logger.info("Processing Gold layer dimensional aggregation...")

        silver_path = os.path.join(self.silver_dir, "cleaned_weather.parquet")
        if not os.path.exists(silver_path):
            raise FileNotFoundError("Silver source file missing. Aborting Gold stage.")

        df_silver = pl.read_parquet(silver_path)

        # Populate Dim Location
        df_dim_location = df_silver.select(
            pl.col("location").alias("location_name")
        ).unique()
        # Generate surrogate integer key via row numbers
        df_dim_location = df_dim_location.with_row_index(name="location_key", offset=1)
        df_dim_location.write_parquet(
            os.path.join(self.gold_dir, "dim_location.parquet")
        )

        # Populate Dim Date
        df_dim_date = df_silver.select(pl.col("date")).unique()
        df_dim_date = df_dim_date.with_columns(
            [
                pl.col("date").dt.strftime("%Y%m%d").cast(pl.Int32).alias("date_key"),
                pl.col("date").dt.date().alias("full_date"),
                pl.col("date").dt.weekday().alias("day_of_week"),
                pl.col("date").dt.strftime("%A").alias("day_name"),
                pl.col("date").dt.month().alias("month_actual"),
                pl.col("date").dt.strftime("%B").alias("month_name"),
                pl.col("date").dt.year().alias("year_actual"),
                pl.col("date").dt.quarter().alias("quarter_actual"),
            ]
        ).unique(subset=["date_key"])
        df_dim_date.write_parquet(os.path.join(self.gold_dir, "dim_date.parquet"))

        # Populate Fact Weather Hourly (Mapping Dimension Keys)
        # Join with dim_location and dim_date to fetch the surrogate keys
        df_fact_hourly = df_silver.join(
            df_dim_location, left_on="location", right_on="location_name", how="inner"
        )
        df_fact_hourly = df_fact_hourly.with_columns(
            pl.col("date").dt.strftime("%Y%m%d").cast(pl.Int32).alias("date_key")
        )

        df_fact_hourly_final = df_fact_hourly.select(
            [
                pl.col("date_key"),
                pl.col("location_key"),
                pl.col("date").alias("timestamp_utc"),
                pl.col("temperature_2m"),
                pl.col("relative_humidity_2m"),
                pl.col("precipitation"),
                pl.col("precipitation_probability"),
                pl.col("weather_code"),
            ]
        ).unique(subset=["location_key", "timestamp_utc"])
        df_fact_hourly_final.write_parquet(
            os.path.join(self.gold_dir, "fact_weather_hourly.parquet")
        )

        # Populate Fact Weather Daily Summary (Analytical Business Aggregate)
        # Truncate timestamp to date level for aggregation
        df_daily_agg = (
            df_fact_hourly_final.group_by(["date_key", "location_key"])
            .agg(
                [
                    pl.col("temperature_2m").max().alias("max_temperature"),
                    pl.col("temperature_2m").min().alias("min_temperature"),
                    pl.col("temperature_2m").mean().round(2).alias("avg_temperature"),
                    pl.col("precipitation").sum().round(2).alias("total_precipitation"),
                    pl.col("precipitation_probability")
                    .max()
                    .alias("max_precipitation_probability"),
                ]
            )
            .sort(["date_key", "location_key"])
        )
        df_daily_agg.write_parquet(
            os.path.join(self.gold_dir, "fact_weather_daily_summary.parquet")
        )

        logger.success("Gold layer Star Schema files (.parquet) successfully compiled.")
