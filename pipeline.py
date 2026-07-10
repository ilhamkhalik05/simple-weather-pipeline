from loguru import logger
from src.extract import fetch_weather_data, process_weather_response
from src.db_manager import PostgresManager
from src.pipeline_manager import WeatherELTPipeline
from src.config.db_config import get_db_config

BRONZE_TABLE = "bronze.raw_weather"
SILVER_TABLE = "silver.cleaned_weather"


def main():
    logger.info("--- Starting Daily Weather ELT Pipeline ---")

    try:
        raw_response = fetch_weather_data()
        df_weather = process_weather_response(raw_response)

        db_config = get_db_config()

        # Dependency Injection: We pass the DB context into our pipeline logic
        with PostgresManager(db_config) as db:
            elt_pipeline = WeatherELTPipeline(db, BRONZE_TABLE, SILVER_TABLE)

            # Execute Medallion Architecture Flow
            elt_pipeline.load_bronze_layer(df_weather)
            elt_pipeline.process_silver_layer()
            elt_pipeline.process_gold_layer()

        logger.success("--- Pipeline Execution Finished Successfully ---")

    except Exception as e:
        logger.critical(f"Pipeline execution aborted due to a critical error: {e}")


if __name__ == "__main__":
    main()
