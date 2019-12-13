# Data Pipelines with ** Airflow **

## Overview

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring you into the project and expect you to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

## Database & Pipelines
- There are 2 datasets :- Songs and Logs. These are loaded on AWS S3.
- In order to create an efficient database, one fact table and four dimension tables were created:
	- Fact: songplays
    - Dimesnsion : songs,artists,time and users
- A pipeline is designed to transfer data into 2 staging tables.
- The next pipeline transfers data from the staging tables to the above created tables.

## Use of Airflow
[Airflow](https://airflow.apache.org/) is configured and customized to make the whole process more efficient. Four different operators were built.

## Installation
Easy installation of Airflow [here](https://airflow.apache.org/start.html)

