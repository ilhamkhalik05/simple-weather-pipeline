# AI Agent Guide for Weather Pipeline (AGENTS.md)

## 1. System Overview
This repository contains an automated Data Engineering ETL (Extract, Transform, Load) pipeline for weather data. 
- **Architecture:** It utilizes the **Medallion Data Architecture** (Bronze, Silver, Gold).
- **Core Language:** Python 3.12.
- **Data Format:** Parquet (Columnar storage).

## 2. Codebase Map
When modifying or debugging this project, refer to the following directory structure:

*   **Entry Point:** `pipeline.py` is the main script that orchestrates the ETL process.
*   **Source Code (`src/`):**
    *   `src/extract.py`: Handles data extraction (likely from a weather API).
    *   `src/pipeline_manager.py`: Manages the data flow between Bronze, Silver, and Gold states.
*   **Data Storage (`data/`):**
    *   `data/bronze/`: Contains `raw_weather.parquet`.
    *   `data/silver/`: Contains `cleaned_weather.parquet`.
    *   `data/gold/`: Contains dimension and fact tables (`dim_date`, `dim_location`, `fact_weather_daily_summary`, `fact_weather_hourly`).
*   **Infrastructure:** 
    *   `Dockerfile` & `docker-compose.yaml`: Defines the containerized environment.
    *   `requirements.txt`: Python dependencies.
*   **CI/CD (`.github/workflows/`):**
    *   `daily-run.yml`: Cron job or scheduled automation for the pipeline.
    *   `test.yml`: Automated test runner triggered on PRs/pushes.

## 3. Execution & Environment Rules
*   **Containerization First:** Always prefer running scripts within the provided Docker context using `docker-compose up` or standard `docker build` commands.
*   **Dependencies:** If adding a new Python package, update `requirements.txt`.
*   **Data Manipulation:** Do not modify `.parquet` files directly. Changes to data schemas must be implemented in the transformation logic within `src/pipeline_manager.py`.

## 4. Testing Guidelines
*   **Framework:** `pytest`.
*   **Location:** All tests are located in the `tests/` directory (`test_extract.py`, `test_pipeline_medallion.py`).
*   **Rule:** Before proposing any code changes, ensure you instruct the user to run tests to validate that the Medallion pipeline stages remain intact.