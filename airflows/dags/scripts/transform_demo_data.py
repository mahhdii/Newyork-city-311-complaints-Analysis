""" Transform demographics data and stores in S3 bucket """
from io import StringIO
import boto3
import pandas as pd
from airflow.models import Variable

def transform_demo_data(path, bucket_name):
    """
    Transforms demographic dataframe by updating community
    districts and eliminates unnecessary data from
    demographics spreadsheets.
    """
    community_districts = {
        101:"Mott Haven/Melrose",
        102:"Hunts Point/Longwood",
        103:"Morrisania/Crotona",
        104:"Highbridge/Concourse",
        105:"Fordham/University Heights",
        106:"Belmont/East Tremont",
        107:"Kingsbridge Hghts/Bedford",
        108:"Riverdale/Fieldston",
        109:"Parkchester/Soundview",
        110:"Throgs Neck/Co-op City",
        111:"Morris Park/Bronxdale",
        112:"Williamsbridge/Baychester",
        201:"Greenpoint/Williamsburg",
        202:"Fort Greene/Brooklyn Heights",
        203:"Bedford Stuyvesant",
        204:"Bushwick",
        205:"East New York/Starrett City",
        206:"Park Slope/Carroll Gardens",
        207:"Sunset Park",
        208:"Crown Heights",
        209:"S. Crown Heights/Prospect Heights",
        210:"Bay Ridge/Dyker Heights",
        211:"Bensonhurst",
        212:"Borough Park",
        213:"Coney Island",
        214:"Flatbush/Midwood",
        215:"Sheepshead Bay",
        216:"Brownsville",
        217:"East Flatbush",
        218:"Flatlands/Canarsie",
        301:"Financial District",
        302:"Greenwich Village/Soho",
        303:"Lower East Side/Chinatown",
        304:"Clinton/Chelsea",
        305:"Midtown",
        306:"Stuyvesant Town/Turtle Bay",
        307:"Upper West Side",
        308:"Upper East Side",
        309:"Morningside Heights/Hamilton Heights",
        310:"Central Harlem",
        311:"East Harlem",
        312:"Washington Heights/Inwood",
        401:"Astoria",
        402:"Woodside/Sunnyside",
        403:"Jackson Heights",
        404:"Elmhurst/Corona",
        405:"Ridgewood/Maspeth",
        406:"Rego Park/Forest Hills",
        407:"Flushing/Whitestone",
        408:"Hillcrest/Fresh Meadows",
        409:"Kew Gardens/Woodhaven",
        410:"South Ozone Park/Howard Beach",
        411:"Bayside/Little Neck",
        412:"Jamaica/Hollis",
        413:"Queens Village",
        414:"Rockaway/Broad Channel",
        501:"St. George/Stapleton",
        502:"South Beach/Willowbrook",
        503:"Tottenville/Great Kills"
    }


    columns = ["Region ID", "Year", "pop_num", "pop_pov_65p_pct", "pop_pov_pct",
               "pop_race_asian_pct", "pop_race_black_pct", "pop_race_div_idx",
               "pop_race_hisp_pct", "pop_race_white_pct", "pop16_unemp_pct"]

    demo_df = pd.read_excel(path, sheet_name="Data", usecols=columns)

    years = [2014, 2015, 2016, 2017, 2018]
    region = list(range(101, 504))

    demo_df = demo_df .loc[(demo_df["Region ID"].isin(region)) & (demo_df["Year"].isin(years))]
    demo_df = demo_df[demo_df.pop_num.notna()]
    demo_df.pop_num = demo_df.pop_num.astype("int")

    codes_df = pd.DataFrame.from_dict(community_districts, orient='index', columns=["region_name"])


    demo_df = demo_df.merge(codes_df, left_on="Region ID", right_index=True)

    demo_df.columns = demo_df.columns.str.strip().str.lower().str.replace(' ', '_') \
                    .str.replace('(', '').str.replace(')', '')

    demo_df = demo_df.set_index(["region_id", "year"])

    csv_buffer = StringIO()
    demo_df.to_csv(csv_buffer)

    s3_client = boto3.client('s3')
    s3_client.put_object(Body=csv_buffer.getvalue(), Bucket=bucket_name,
                         Key="processed/dim_demographics/dim_demographics.csv")

if __name__ == "__main__":

    aws_bucket = Variable.get('aws_bucket')
    FILE_OBJ = 'demographics/Neighorhood_Indicators_CoreDataDownload_2020-06-30.xlsx'

    filepath = f"s3://{aws_bucket}/{FILE_OBJ}"

    transform_demo_data(filepath, aws_bucket)
