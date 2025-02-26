"""
@author: senyalol
"""


from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.http.sensors.http import HttpSensor
import json
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.python import PythonOperator
import pandas as pd
import urllib.request
from bs4 import BeautifulSoup



IMO = "XXXX"  #Replace with the ship MMSI number

hdr = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
       'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
       'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
       'Accept-Encoding': 'none',
       'Accept-Language': 'en-US,en;q=0.8',
       'Connection': 'keep-alive'}


#This function uses bs4 to scrape data based on ship IMO number and returns current city for the ship
def scrape_location():
        url = r'https://www.vesselfinder.com/en/vessels/details/' + IMO
        req = urllib.request.Request(url, None, hdr)
        with urllib.request.urlopen(req) as response:
            the_page = response.read()
        parsed_html = BeautifulSoup(the_page,"lxml")
        try:            
            location = parsed_html.find_all("a", {"class": "_npNa"})
            city = str(location[0].string)
        except:
            print("last port not available")
        return city
        
#print(scrape_location())
    
city = scrape_location()




#Transform weather data and save locally to CSV or S3 bucket based on previously returned city

def transform_load_data(task_instance):
    data = task_instance.xcom_pull(task_ids="extract_weather_data")
    city = scrape_location()
    weather_description = data["weather"][0]['description']
    
    temp_farenheit = data["main"]["temp"]
    feels_like_farenheit = data["main"]["feels_like"]
    min_temp_farenheit = data["main"]["temp_min"]
    max_temp_farenheit = data["main"]["temp_max"]
    pressure = data["main"]["pressure"]
    humidity = data["main"]["humidity"]
    wind_speed = data["wind"]["speed"]
    time_of_record = datetime.utcfromtimestamp(data['dt'] + data['timezone'])
    sunrise_time = datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone'])
    sunset_time = datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone'])

    transformed_data = {"City": city,
                        "Description": weather_description,
                        "Temperature (F)": temp_farenheit,
                        "Feels Like (F)": feels_like_farenheit,
                        "Minimun Temp (F)":min_temp_farenheit,
                        "Maximum Temp (F)": max_temp_farenheit,
                        "Pressure": pressure,
                        "Humidty": humidity,
                        "Wind Speed": wind_speed,
                        "Time of Record": time_of_record,
                        "Sunrise (Local Time)":sunrise_time,
                        "Sunset (Local Time)": sunset_time,
                        "MMSI": IMO                        
                        }
    transformed_data_list = [transformed_data]
    df_data = pd.DataFrame(transformed_data_list)
   

    now = datetime.now()
    dt_string = now.strftime("%d%m%Y%H%M%S")
    dt_string = IMO + dt_string
    df_data.to_csv(f"{dt_string}.csv", index=False)
    df_data.to_csv(f"s3://XXXX/{dt_string}.csv", index=False) #XXXX replace with bucket name


##Default arguments for airflow to start###
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 8),
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}


###Weather_DAG###
with DAG('weather_dag',
        default_args=default_args,
        schedule_interval = '@daily',
        catchup=False) as dag:

###Check if weatherAPI is ready ###
        is_weather_api_ready = HttpSensor(
        task_id ='is_weather_api_ready',
        http_conn_id='weathermap_api',
        endpoint='/data/2.5/weather?q={city}&APPID=XXXX&units=metric' #XXXX replace with the api key from openweather
        )

###Extract weather data ###
        extract_weather_data = HttpOperator(
        task_id = 'extract_weather_data',
        http_conn_id = 'weathermap_api',
        endpoint='/data/2.5/weather?q={city}&APPID=XXXX&units=metric', #XXXX replace with the api key from openweather
        method = 'GET',
        response_filter= lambda r: json.loads(r.text),
        log_response=True
        )
###transform_load_wetaher_data ###
        transform_load_weather_data = PythonOperator(
        task_id= 'transform_load_weather_data',
        python_callable=transform_load_data
        )
##Dag for scraping
        scrape_location_data = PythonOperator(
        task_id= 'scrape_location_data',
        python_callable=scrape_location
        )






        ###Define DAG workflow 
        is_weather_api_ready >> scrape_location_data >> extract_weather_data >> transform_load_weather_data
        
