import pandas as pd
from loguru import logger
from src.db_manager import PostgresManager


class WeatherELTPipeline:
    """
    Manages the Medallion Architecture data flow (Bronze -> Silver -> Gold).
    Requires an active PostgresManager instance to execute database operations.
    """

    def __init__(self, db: PostgresManager, bronze_table: str, silver_table: str):
        self.db = db
        self.bronze_table = bronze_table
        self.silver_table = silver_table

    # ---------------------------------------------------------
    # BRONZE LAYER (RAW DATA)
    # ---------------------------------------------------------
    def load_bronze_layer(self, df: pd.DataFrame) -> None:
        """Dumps raw DataFrame directly into the Bronze layer."""
        logger.info(
            f"Loading {len(df)} records into Bronze layer ({self.bronze_table})..."
        )

        data_to_insert = [tuple(row) for row in df.itertuples(index=False)]

        self.db.execute_query(
            f"""
            CREATE TABLE IF NOT EXISTS {self.bronze_table} (
                id SERIAL PRIMARY KEY,
                location VARCHAR,
                date TIMESTAMPTZ,
                temperature_2m FLOAT,
                relative_humidity_2m FLOAT,
                precipitation FLOAT,
                precipitation_probability FLOAT,
                weather_code FLOAT,
                extracted_at TIMESTAMP DEFAULT now(),
                UNIQUE (date)
            );
        """
        )

        upsert_query = f"""
            INSERT INTO {self.bronze_table} 
            (location, date, temperature_2m, relative_humidity_2m, precipitation, precipitation_probability, weather_code)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date) 
            DO UPDATE SET 
                temperature_2m = EXCLUDED.temperature_2m,
                relative_humidity_2m = EXCLUDED.relative_humidity_2m,
                precipitation = EXCLUDED.precipitation,
                precipitation_probability = EXCLUDED.precipitation_probability,
                weather_code = EXCLUDED.weather_code,
                extracted_at = now();
        """
        self.db.executemany_records(upsert_query, data_to_insert)

        logger.success("Bronze load complete.")

    # ---------------------------------------------------------
    # SILVER LAYER (CLEANED & CONFORMED)
    # ---------------------------------------------------------
    def process_silver_layer(self) -> None:
        """Incrementally cleans and standardizes data from Bronze to Silver."""
        logger.info(f"Processing Silver layer ({self.silver_table})...")

        self.db.execute_query(
            f"""
            CREATE TABLE IF NOT EXISTS {self.silver_table} (
                id SERIAL PRIMARY KEY,
                location VARCHAR(100) NOT NULL,
                date TIMESTAMPTZ NOT NULL,
                temperature_2m NUMERIC(5,2),
                relative_humidity_2m NUMERIC(5,2),
                precipitation NUMERIC(6,2),
                precipitation_probability NUMERIC(5,2),
                weather_code INT,
                transformed_at TIMESTAMPTZ DEFAULT now(),
                UNIQUE (location, date)
            );
        """
        )

        checkpoint_date = self.db.fetch_one(
            f"SELECT COALESCE(MAX(date), '1970-01-01'::TIMESTAMPTZ) FROM {self.silver_table}"
        )[0]

        sql_query = f"""
            WITH new_raw_data AS (
                SELECT 
                    location, date,
                    temperature_2m::NUMERIC(5,2) AS temperature_2m,
                    relative_humidity_2m::NUMERIC(5,2) AS relative_humidity_2m,
                    precipitation::NUMERIC(6,2) AS precipitation,
                    precipitation_probability::NUMERIC(5,2) AS precipitation_probability,
                    weather_code::INT AS weather_code
                FROM {self.bronze_table}
                WHERE date > %s
            )
            INSERT INTO {self.silver_table} 
                (location, date, temperature_2m, relative_humidity_2m, precipitation, precipitation_probability, weather_code)
            SELECT * FROM new_raw_data
            ON CONFLICT (location, date) 
            DO UPDATE SET 
                temperature_2m = EXCLUDED.temperature_2m,
                relative_humidity_2m = EXCLUDED.relative_humidity_2m,
                precipitation = EXCLUDED.precipitation,
                precipitation_probability = EXCLUDED.precipitation_probability,
                weather_code = EXCLUDED.weather_code,
                transformed_at = now();
        """
        self.db.execute_query(sql_query, (checkpoint_date,))
        rows_affected = self.db.cur.rowcount

        if rows_affected > 0:
            logger.success(
                f"Silver processing complete. {rows_affected} rows updated/inserted."
            )
        else:
            logger.info("Silver layer is already up-to-date.")

    # ---------------------------------------------------------
    # GOLD LAYER (STAR SCHEMA AGGREGATION)
    # ---------------------------------------------------------
    def process_gold_layer(self) -> None:
        """Builds the analytical Star Schema incrementally."""
        logger.info("Processing Gold layer (Star Schema)...")

        self.db.execute_query("CREATE SCHEMA IF NOT EXISTS gold;")

        self._initialize_gold_schema()
        self._upsert_dimensions()
        self._upsert_fact_hourly()
        self._upsert_fact_daily_summary()

        logger.success("Gold processing complete.")

    def _initialize_gold_schema(self) -> None:
        """Creates the DDL for the dimensional model."""
        self.db.execute_query(
            """
            CREATE TABLE IF NOT EXISTS gold.dim_location (
                location_key SERIAL PRIMARY KEY,
                location_name VARCHAR(100) NOT NULL UNIQUE
            );
        """
        )
        self.db.execute_query(
            """
            CREATE TABLE IF NOT EXISTS gold.dim_date (
                date_key INT PRIMARY KEY,
                full_date DATE NOT NULL UNIQUE,
                day_of_week INT NOT NULL,
                day_name VARCHAR(10) NOT NULL,
                month_actual INT NOT NULL,
                month_name VARCHAR(10) NOT NULL,
                year_actual INT NOT NULL,
                quarter_actual INT NOT NULL
            );
        """
        )
        self.db.execute_query(
            """
            CREATE TABLE IF NOT EXISTS gold.fact_weather_hourly (
                fact_hourly_id SERIAL PRIMARY KEY,
                date_key INT NOT NULL REFERENCES gold.dim_date(date_key),
                location_key INT NOT NULL REFERENCES gold.dim_location(location_key),
                timestamp_utc TIMESTAMPTZ NOT NULL,
                temperature_2m NUMERIC(5,2),
                relative_humidity_2m NUMERIC(5,2),
                precipitation NUMERIC(6,2),
                precipitation_probability NUMERIC(5,2),
                weather_code INT,
                CONSTRAINT uq_fact_hourly UNIQUE (location_key, timestamp_utc)
            );
        """
        )
        self.db.execute_query(
            """
            CREATE TABLE IF NOT EXISTS gold.fact_weather_daily_summary (
                fact_daily_id SERIAL PRIMARY KEY,
                date_key INT NOT NULL REFERENCES gold.dim_date(date_key),
                location_key INT NOT NULL REFERENCES gold.dim_location(location_key),
                max_temperature NUMERIC(5,2),
                min_temperature NUMERIC(5,2),
                avg_temperature NUMERIC(5,2),
                total_precipitation NUMERIC(6,2),
                max_precipitation_probability NUMERIC(5,2),
                CONSTRAINT uq_fact_daily UNIQUE (location_key, date_key)
            );
        """
        )
        self.db.execute_query(
            "CREATE INDEX IF NOT EXISTS idx_fact_hourly_date ON gold.fact_weather_hourly(date_key);"
        )
        self.db.execute_query(
            "CREATE INDEX IF NOT EXISTS idx_fact_daily_date ON gold.fact_weather_daily_summary(date_key);"
        )

    def _upsert_dimensions(self) -> None:
        """Populates Location and Date dimensions from Silver."""
        self.db.execute_query(
            f"""
            INSERT INTO gold.dim_location (location_name)
            SELECT DISTINCT location FROM {self.silver_table}
            ON CONFLICT (location_name) DO NOTHING;
        """
        )

        self.db.execute_query(
            f"""
            INSERT INTO gold.dim_date (
                date_key, full_date, day_of_week, day_name, 
                month_actual, month_name, year_actual, quarter_actual
            )
            SELECT DISTINCT
                TO_CHAR(date, 'YYYYMMDD')::INT AS date_key,
                date::DATE AS full_date,
                EXTRACT(ISODOW FROM date)::INT AS day_of_week,
                TRIM(TO_CHAR(date, 'Day')) AS day_name,
                EXTRACT(MONTH FROM date)::INT AS month_actual,
                TRIM(TO_CHAR(date, 'Month')) AS month_name,
                EXTRACT(YEAR FROM date)::INT AS year_actual,
                EXTRACT(QUARTER FROM date)::INT AS quarter_actual
            FROM {self.silver_table}
            ON CONFLICT (date_key) DO NOTHING;
        """
        )

    def _upsert_fact_hourly(self) -> None:
        """Populates the granular hourly fact table incrementally."""
        checkpoint = self.db.fetch_one(
            "SELECT COALESCE(MAX(timestamp_utc), '1970-01-01'::TIMESTAMPTZ) FROM gold.fact_weather_hourly"
        )[0]

        self.db.execute_query(
            f"""
            WITH source_delta AS (
                SELECT * FROM {self.silver_table} WHERE date > %s
            )
            INSERT INTO gold.fact_weather_hourly (
                date_key, location_key, timestamp_utc, temperature_2m, 
                relative_humidity_2m, precipitation, precipitation_probability, weather_code
            )
            SELECT 
                d.date_key, l.location_key, s.date, s.temperature_2m, 
                s.relative_humidity_2m, s.precipitation, s.precipitation_probability, s.weather_code
            FROM source_delta s
            JOIN gold.dim_location l ON s.location = l.location_name
            JOIN gold.dim_date d ON s.date::DATE = d.full_date
            ON CONFLICT (location_key, timestamp_utc) 
            DO UPDATE SET
                temperature_2m = EXCLUDED.temperature_2m,
                relative_humidity_2m = EXCLUDED.relative_humidity_2m,
                precipitation = EXCLUDED.precipitation,
                precipitation_probability = EXCLUDED.precipitation_probability,
                weather_code = EXCLUDED.weather_code;
        """,
            (checkpoint,),
        )

    def _upsert_fact_daily_summary(self) -> None:
        """Calculates and stores daily aggregates incrementally."""
        checkpoint = self.db.fetch_one(
            """
            SELECT COALESCE(MAX(d.full_date), '1970-01-01'::DATE) 
            FROM gold.fact_weather_daily_summary f 
            JOIN gold.dim_date d ON f.date_key = d.date_key
        """
        )[0]

        self.db.execute_query(
            """
            WITH aggregated_daily AS (
                SELECT 
                    date_key,           -- FIXED: Dipindah ke posisi pertama
                    location_key,       -- FIXED: Dipindah ke posisi kedua
                    MAX(temperature_2m) AS max_temp,
                    MIN(temperature_2m) AS min_temp,
                    AVG(temperature_2m)::NUMERIC(5,2) AS avg_temp,
                    SUM(precipitation)::NUMERIC(6,2) AS total_precip,
                    MAX(precipitation_probability) AS max_prob
                FROM gold.fact_weather_hourly
                WHERE timestamp_utc::DATE >= %s
                GROUP BY date_key, location_key
            )
            INSERT INTO gold.fact_weather_daily_summary (
                date_key, location_key, max_temperature, min_temperature, 
                avg_temperature, total_precipitation, max_precipitation_probability
            )
            SELECT * FROM aggregated_daily
            ON CONFLICT (location_key, date_key) 
            DO UPDATE SET
                max_temperature = EXCLUDED.max_temperature,
                min_temperature = EXCLUDED.min_temperature,
                avg_temperature = EXCLUDED.avg_temperature,
                total_precipitation = EXCLUDED.total_precipitation,
                max_precipitation_probability = EXCLUDED.max_precipitation_probability;
        """,
            (checkpoint,),
        )
