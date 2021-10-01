""" Gets weather data from NOAA API and stores in S3 bucket """
import os
import json
import configparser
from io import StringIO
import requests
import boto3

def get_weather_data(req_years, weather_types, api_token, aws_bucket):
    """
    Loops through two lists containing weather data types
    and years to complete a string and make request to NOAA's API.

    Stores one json file for each request.
    """

    try:
        for datatype in weather_types:
            for year in req_years:
                req_string = "https://www.ncdc.noaa.gov/cdo-web/api/v2/data?" \
                        f"datasetid=GHCND&datatypeid={datatype}&limit=1000&" \
                        "stationid=GHCND:USW00094728&" \
                        f"startdate={year}-01-01&enddate={year}-12-31&" \
                        "units=standard&" \
                        "includemetadata=False"

                response = requests.get(req_string, headers={'token':api_token})
                weather_data = response.json()["results"]

                object_name = f"weather/{datatype}_{year}.json"

                io = StringIO()
                json.dump(weather_data, io)

                s3_client = boto3.client('s3')

                s3_client.put_object(Body=io.getvalue(), Bucket=aws_bucket, Key=object_name)

        return s3_client
    except requests.RequestException as exception:
        return exception

if __name__ == "__main__":

    ROOT_DIR, af_directory = os.path.split(os.environ['AIRFLOW_HOME'])
    CONFIG_PATH = os.path.join(ROOT_DIR, 'tokens.ini')

    config = configparser.ConfigParser()
    config.read(CONFIG_PATH)
    bucket = config.get('AWS', 'Bucket')
    noaa_token = config.get('WEATHER', 'Token')

    weather_data_types = ["TMIN", "TMAX", "PRCP", "SNOW", "SNWD"]
    years = list(range(2014, 2019))

    get_weather_data(years, weather_data_types, noaa_token, bucket)
    