# Ship-Weather-ETL
ETL script using Python and Apache Airflow for extracting weather data for a specific ship and loading to an S3 bucket.  

This script uses Python and Apache Airflow (Orchestration) to extract, transform and load weather data for a specific ship to an S3 bucket. It initially returns the current location of a ship (based on the ship number) by using webscraping on the vessel finder app. Based on the previously returned location of the ship, it then queries the OpenWeather Api for the current weather forecast. 

Libraries used:

Apache Airflow
Beautiful soup
Pandas
Requests 

