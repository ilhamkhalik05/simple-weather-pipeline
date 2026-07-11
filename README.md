# 🌦️ Weather Data Pipeline with Medallion Architecture

> A modern ELT weather data pipeline built with **Python**, **Polars**, and **Medallion Architecture**, transforming raw weather API responses into analytics-ready datasets using an efficient local data lake.

![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=for-the-badge&logo=python)
![Polars](https://img.shields.io/badge/Polars-DataFrame-blue?style=for-the-badge)
![Parquet](https://img.shields.io/badge/Storage-Parquet-green?style=for-the-badge)
![Architecture](https://img.shields.io/badge/Architecture-Medallion-orange?style=for-the-badge)

---

# 📖 Overview

This project demonstrates how a production-inspired **ELT (Extract, Load, Transform)** pipeline can be implemented using a lightweight local environment without requiring cloud infrastructure.

The pipeline retrieves hourly weather forecasts from the **Open-Meteo API**, stores immutable raw data in a local data lake, performs data quality transformations, and produces analytical datasets following the **Bronze → Silver → Gold** Medallion Architecture.

The entire pipeline is powered by **Polars**, providing high-performance DataFrame operations while storing datasets in the efficient **Apache Parquet** format.

---

# ✨ Features

- 📡 Extract weather forecast data from Open-Meteo API
- ⚡ High-performance data processing using **Polars**
- 🏗️ Medallion Architecture implementation
- 💾 Local file-based Data Lake
- 📦 Apache Parquet storage
- 🔁 Idempotent pipeline execution
- 🧹 Data cleaning & schema enforcement
- 📊 Star Schema generation for analytics
- 📝 Comprehensive logging with Loguru
- ✅ Unit testing with Pytest

---

# 🏛️ Pipeline Architecture

```text
                Open-Meteo API
                      │
                Extract Weather
                      │
                      ▼
          ┌────────────────────┐
          │    Bronze Layer    │
          │ Raw Weather Data   │
          └────────────────────┘
                      │
                Data Cleaning
                Type Casting
                Deduplication
                      │
                      ▼
          ┌────────────────────┐
          │    Silver Layer    │
          │ Cleaned Dataset    │
          └────────────────────┘
                      │
                Aggregation
            Star Schema Modeling
                      │
                      ▼
          ┌────────────────────┐
          │     Gold Layer     │
          │ Analytics Ready    │
          └────────────────────┘
```

---

# 🔄 Pipeline Flow

## 1. Extract

The pipeline connects to the Open-Meteo API and downloads hourly weather data for Jakarta.

Collected variables include:

- Temperature
- Relative Humidity
- Precipitation
- Precipitation Probability
- Weather Code

To improve reliability, the extraction layer implements:

- HTTP request retry
- Response caching
- Structured logging

---

## 2. Bronze Layer

Purpose:

> Preserve raw data exactly as received.

Actions:

- Convert API response into Polars DataFrame
- Append extraction timestamp
- Store immutable snapshot as Parquet

Output:

```
data/
└── bronze/
    └── raw_weather.parquet
```

---

## 3. Silver Layer

Purpose:

> Produce trusted, standardized datasets.

Transformations:

- Data type normalization
- Timestamp conversion
- Schema enforcement
- Duplicate removal
- Data quality improvements

Output:

```
data/
└── silver/
    └── cleaned_weather.parquet
```

---

## 4. Gold Layer

Purpose:

> Build analytics-ready datasets.

Generated tables:

### Dimension Tables

- `dim_location.parquet`
- `dim_date.parquet`

### Fact Tables

- `fact_weather_hourly.parquet`
- `fact_weather_daily_summary.parquet`

The Gold layer follows a simplified **Star Schema**, making the datasets suitable for dashboards, BI tools, and downstream analytics.

---

# 📂 Project Structure

```text
weather-pipeline/
│
├── pipeline.py
├── requirements.txt
│
├── src/
│   ├── extract.py
│   ├── pipeline_manager.py
│   └── __init__.py
│
├── tests/
│   ├── test_extract.py
│   └── conftest.py
│
├── data/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
└── .github/
    └── workflows/
```

---

# 🛠️ Tech Stack

| Category | Technology |
|-----------|------------|
| Language | Python |
| Data Processing | Polars |
| Storage | Apache Parquet |
| API | Open-Meteo |
| Logging | Loguru |
| Testing | Pytest |
| Data Architecture | Medallion Architecture |
| Pipeline Pattern | ELT |

---

# 🚀 Getting Started

## 1. Clone Repository

```bash
git clone https://github.com/ilhamkhalik05/weather-pipeline.git

cd weather-pipeline
```

---

## 2. Create Virtual Environment

Linux / macOS

```bash
python3 -m venv .venv

source .venv/bin/activate
```

Windows

```powershell
python -m venv .venv

.venv\Scripts\activate
```

---

## 3. Install Dependencies

```bash
pip install -r requirements.txt
```

---

## 4. Run the Pipeline

```bash
python pipeline.py
```

If everything runs successfully, the following directory structure will be generated automatically:

```text
data/

├── bronze/
│   └── raw_weather.parquet
│
├── silver/
│   └── cleaned_weather.parquet
│
└── gold/
    ├── dim_date.parquet
    ├── dim_location.parquet
    ├── fact_weather_hourly.parquet
    └── fact_weather_daily_summary.parquet
```

---

# 🧪 Running Tests

Execute the test suite with:

```bash
pytest
```

---

# 📊 Output Datasets

| Dataset | Description |
|----------|-------------|
| raw_weather.parquet | Raw API response stored in Bronze |
| cleaned_weather.parquet | Cleaned and standardized weather data |
| dim_location.parquet | Location dimension table |
| dim_date.parquet | Date dimension table |
| fact_weather_hourly.parquet | Hourly weather fact table |
| fact_weather_daily_summary.parquet | Daily aggregated weather metrics |

---

# 💡 Engineering Highlights

This project showcases several practical data engineering concepts:

- ELT pipeline design
- Medallion Architecture
- Star Schema modeling
- File-based Data Lake
- Idempotent data processing
- Data quality enforcement
- High-performance DataFrame processing with Polars
- Efficient columnar storage using Parquet
- Modular pipeline organization
- Production-style logging

---

# 🔮 Future Improvements

Potential enhancements include:

- Docker support
- Apache Airflow orchestration
- DuckDB analytical layer
- Delta Lake / Iceberg support
- Multi-city ingestion
- Historical backfill
- Data validation with Great Expectations
- Dashboard integration (Power BI / Tableau)
- Cloud object storage (Amazon S3 / Google Cloud Storage)
- CI/CD deployment pipeline

---

# 🤝 Contributing

Contributions, suggestions, and discussions are always welcome.

Feel free to fork the repository, open an issue, or submit a pull request.

---