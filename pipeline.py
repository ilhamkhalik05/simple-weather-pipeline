from loguru import logger
from src.extract import fetch_weather_data, process_weather_response
from src.pipeline_manager import WeatherELTPipeline

# Will act as a file-based data lake for our weather data
OUTPUT_DATA_DIR = "data"


def main():
    logger.info("--- Starting Daily Weather ELT Pipeline ---")

    try:
        raw_response = fetch_weather_data()
        df_weather = process_weather_response(raw_response)

        elt_pipeline = WeatherELTPipeline(OUTPUT_DATA_DIR)

        # Execute Medallion Architecture Flow
        elt_pipeline.load_bronze_layer(df_weather)
        elt_pipeline.process_silver_layer()
        elt_pipeline.process_gold_layer()

        logger.success("--- Pipeline Execution Finished Successfully ---")

    except Exception as e:
        logger.critical(f"Pipeline execution aborted due to a critical error: {e}")


if __name__ == "__main__":
    main()
