"""cleans/transforms NOAA weather data and stores processed data in S3 """
from io import StringIO
import pandas as pd
import boto3
from airflow.models import Variable

def transform_weather_data(bucket):
    """
    Collects all weather data JSON files from S3 and concats into one data frame.
    Drops columns and create a pivot
    Stores processed file in S3 bucket
    """

    resource = boto3.resource('s3')
    my_bucket = resource.Bucket(bucket)
    prefix_objs = list(my_bucket.objects.filter(Prefix="weather/"))

    df_list = [pd.read_json(obj.get()['Body']) for obj in prefix_objs]
    weather_df = pd.concat(df_list)

    weather_df["date"] = pd.to_datetime(weather_df["date"], format='%Y/%m/%d')
    weather_df = weather_df.drop(columns=['station', 'attributes'])

    weather_df.rename(columns={"date":"record_date"}, inplace=True)

    weather_df = weather_df.pivot(index="record_date", columns="datatype", values="value")

    new_col_name = {"PRCP":"precipitation",
                    "SNOW":"snowfall", "SNWD":"snow_depth",
                    "TMAX":"max_temp", "TMIN":"min_temp"}

    weather_df.rename(columns=new_col_name, inplace=True)

    csv_buffer = StringIO()
    weather_df.to_csv(csv_buffer)

    s3_client = boto3.client('s3')
    s3_client.put_object(Body=csv_buffer.getvalue(), Bucket=bucket,
                         Key="processed/dim_weather/dim_weather.csv")

if __name__ == "__main__":

    aws_bucket = Variable.get('aws_bucket')
    transform_weather_data(aws_bucket)
