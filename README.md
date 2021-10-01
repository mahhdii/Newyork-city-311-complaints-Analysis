# Data Engineering Capstone Project

## Scope of Work
In a hypothetical situation, the Mayor of New York City has requested the city's analytics team present their office with a report detailing trends in the city's 311 complaints in effort to properly allocate the city's resources. The Mayor is curious how potential demographic changes in different parts of the city may have impacted different types of complaints received. Since New York City experiences four seasons, the Mayor also wonders if there are any cyclical issues that can use some workflow improvement which may result in savings for the city.

This project creates a data pipeline using Apache Airflow to extract, transform and load the requested datasets into a data warehouse in Amazon Redshift for the analytics team to perform their analysis.

## Data Description & Sources
* NYC 311 Compliants: The City of New York publishes complaints made to 311 via their [Open Data portal](https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2010-to-Present/erm2-nwe9). 311 complaints are updated daily. A csv file containing all complaints from 2014 to 2018 was downloaded from here. The dataset contains columns such as complaint type, city agency respoding to complaint, location (including address, neighborhood and community district information) and longtitude/latitude coordinates. 

* NOAA Weather API: The weather data is gathering from the [National Oceanic and Atmospheric Administration's API](https://www.ncdc.noaa.gov/cdo-web/webservices/v2). API calls returns json data with the max/min temperature, snowfall, snow depth and precipitation measures for each day. In this project, we ping and receive data from the station located in Central Park, Manhattan.


* NYU Furman Center: As per the center's [website](https://furmancenter.org/), presents essential data and analysis about the state of New York City’s housing and neighborhoods to those involved in land use, real estate development, community economic development, housing, urban economics, and urban policy. The center published a [spreadsheet](https://furmancenter.org/coredata/userguide/data-downloads) containing neighborhood indicators organized by different geographic identifiers. We use this spreadsheet get population, race and unemployement figures for the years between 2014 and 2018.


Further details and analysis of the 311 [complaints](notebooks/311_NYC_Complaint_Data.ipynb), NOAA [weather](notebooks/NWS_Weather_Data_EDA.ipynb) and [demographic](notebooks/NYC_Neighborhood_Demographics_EDA.ipynb) datasets can be found in their respective Jupyter notebooks.

<br>

## Final model:

![data model](images/ERDiagram.png)

<br>

## Tools and Technologies

* Apache Airflow - To schedule and monitor our data pipeline.
* AWS S3 - For storage of raw and processed datasets.
* AWS Redshift - Used to create and store the data warehouse.
* Pandas - Used to process and transform datasets. If datasets were bigger, using Apache Spark would be better suited. 

## Airflow Pipeline

![pipeline img](images/data_pipeline.png)

## Pipeline
The task order in the snippet below are the following:

1. Call NOAA Weather API and save result as a JSON file in S3. Upload 311 complaints and NYC demographics data into S3.

2. Transform data and store processed results in S3.

3. Copy transformed files into their respective tables in AWS Redshift. 

4. Complete data quality checks


## Scenarios
* Write a description of how you would approach the problem differently under the following scenarios:

 1. Q: The data was increased by 100x.

    A: An Amazon EMR cluster can be spun up with Apache Spark installed to process the increase in data easily prior to being stored on S3.

 2. Q:The data populates a dashboard that must be updated on a daily basis by 7am every day.

    A: The airflow tasks can be scheduled to run every morning before 7am to ensure the data is up to date and ready to be consumed.

 3. Q: The database needed to be accessed by 100+ people. 

    A: Amazon Redshift by default allows up to 500 connections therefore 100+ people connecting to the database wouldn't be an issue.

