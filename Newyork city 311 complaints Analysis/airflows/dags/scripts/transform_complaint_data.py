""" Transform 311 complaint data and stores in S3 bucket """
from io import StringIO
from datetime import datetime
import pandas as pd
import boto3
from airflow.models import Variable

def get_time_data(column, bucket_name):
    """
	Using the created date column from the complaints dataframe to
	extract date and time values. Saves csv file containing time dataframe
	in S3.
	"""

    time_df = pd.DataFrame(column)

    time_df.drop_duplicates(subset='created_date', keep="first", inplace=True)

    time_df['year'] = time_df['created_date'].dt.year
    time_df['month'] = time_df['created_date'].dt.month
    time_df['day'] = time_df['created_date'].dt.day
    time_df['hour'] = time_df['created_date'].dt.hour
    time_df['minute'] = time_df['created_date'].dt.minute

    time_df['weekday'] = time_df['created_date'].dt.weekday
    time_df['quarter'] = time_df['created_date'].dt.quarter
    time_df['day_of_year'] = time_df['created_date'].dt.dayofyear

    time_df.set_index("created_date", inplace=True)

    time_csv_buffer = StringIO()
    time_df.to_csv(time_csv_buffer)
    s3_client = boto3.client('s3')
    s3_client.put_object(Body=time_csv_buffer.getvalue(), Bucket=bucket_name,
                         Key="processed/dim_time/dim_time.csv")


def transform_complaint_data(path, bucket_name):
    """
    Tranforms 311 complaint data by using specific colums,
    updating community districts values and updates column names.
    """
    community_districts = {
        "0 Unspecified":000,
        "01 BRONX":101,
        "01 BROOKLYN":201,
        "01 MANHATTAN":301,
        "01 QUEENS":401,
        "01 STATEN ISLAND":501,
        "02 BRONX":102,
        "02 BROOKLYN":202,
        "02 MANHATTAN":302,
        "02 QUEENS":402,
        "02 STATEN ISLAND":502,
        "03 BRONX":103,
        "03 BROOKLYN":203,
        "03 MANHATTAN":303,
        "03 QUEENS":403,
        "03 STATEN ISLAND":503,
        "04 BRONX":104,
        "04 BROOKLYN":204,
        "04 MANHATTAN":304,
        "04 QUEENS":404,
        "05 BRONX":105,
        "05 BROOKLYN":205,
        "05 MANHATTAN":305,
        "05 QUEENS":405,
        "06 BRONX":106,
        "06 BROOKLYN":206,
        "06 MANHATTAN":306,
        "06 QUEENS":406,
        "07 BRONX":107,
        "07 BROOKLYN":207,
        "07 MANHATTAN":307,
        "07 QUEENS":407,
        "08 BRONX":108,
        "08 BROOKLYN":208,
        "08 MANHATTAN":308,
        "08 QUEENS":408,
        "09 BRONX":109,
        "09 BROOKLYN":209,
        "09 MANHATTAN":309,
        "09 QUEENS":409,
        "10 BRONX":110,
        "10 BROOKLYN":210,
        "10 MANHATTAN":310,
        "10 QUEENS":410,
        "11 BRONX":111,
        "11 BROOKLYN":211,
        "11 MANHATTAN":311,
        "11 QUEENS":411,
        "12 BRONX":112,
        "12 BROOKLYN":212,
        "12 MANHATTAN":312,
        "12 QUEENS":412,
        "13 BROOKLYN":213,
        "13 QUEENS":413,
        "14 BROOKLYN":214,
        "14 QUEENS":414,
        "15 BROOKLYN":215,
        "16 BROOKLYN":216,
        "17 BROOKLYN":217,
        "18 BROOKLYN":218,
        "26 BRONX":100,
        "27 BRONX":100,
        "28 BRONX":100,
        "55 BROOKLYN":200,
        "56 BROOKLYN":200,
        "64 MANHATTAN":300,
        "80 QUEENS":400,
        "81 QUEENS":400,
        "82 QUEENS":400,
        "83 QUEENS":400,
        "84 QUEENS":400,
        "95 STATEN ISLAND":500,
        "Unspecified BRONX":100,
        "Unspecified BROOKLYN":200,
        "Unspecified MANHATTAN":300,
        "Unspecified QUEENS":400,
        "Unspecified STATEN ISLAND":500
        }

    use_cols = ['Unique Key', 'Created Date', 'Agency', 'Agency Name',
                'Complaint Type', 'Descriptor', 'Location Type', 'Incident Zip',
                'Incident Address', 'Street Name', 'City', 'Status', 'Community Board', 'Borough',
                'Latitude', 'Longitude', 'Location']

    dtype_dict = {'Borough':'category', 'Status':'category',
                  'City':'category', 'Location Type':'category', 'Agency':'category',
                  'Agency Name':'category', 'Complaint Type':'category',
                  'Descriptor':'category', 'Incident Zip':"object"}

    custom_date_parser = lambda x: datetime.strptime(x, '%m/%d/%Y %I:%M:%S %p')

    reader = pd.read_csv(path, usecols=use_cols, dtype=dtype_dict, iterator=True,
                         parse_dates=['Created Date'], date_parser=custom_date_parser,
                         chunksize=100000)

    complaint_df = pd.concat(reader, ignore_index=True, copy=False)

    complaint_df.columns = complaint_df.columns.str.strip().str.lower().str.replace(' ', '_') \
                                        .str.replace('(', '').str.replace(')', '')

    complaint_df['incident_date'] = complaint_df['created_date'].dt.date

    complaint_df.community_board.replace(community_districts, inplace=True)

    complaint_df.set_index("unique_key", inplace=True)

    complaints_csv_buffer = StringIO()
    complaint_df.to_csv(complaints_csv_buffer)
    s3_client = boto3.client('s3')
    s3_client.put_object(Body=complaints_csv_buffer.getvalue(), Bucket=bucket_name,
                         Key="processed/fact_complaints/fact_complaints.csv")

    get_time_data(complaint_df['created_date'], bucket_name)

if __name__ == "__main__":

    aws_bucket = Variable.get('aws_bucket')
    FILE_OBJ = "311_complaints/311_Service_Requests_from_2010_to_Present.csv"
    filepath = f"s3://{aws_bucket}/{FILE_OBJ}"

    transform_complaint_data(filepath, aws_bucket)
	