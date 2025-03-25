# Ship Weather ETL

This project uses Python and Apache Airflow to extract, transform, and load weather data for a specific ship to an S3 bucket.

## Overview

The ETL process consists of the following steps:
1. Extract the current location of a ship using web scraping on the Vessel Finder app.
2. Query the OpenWeather API for the current weather forecast based on the ship's location.
3. Load the processed data into an S3 bucket.

## Technologies Used

- Python
- Apache Airflow (for orchestration)
- Beautiful Soup (for web scraping)
- Pandas (for data manipulation)
- Requests (for API calls)
