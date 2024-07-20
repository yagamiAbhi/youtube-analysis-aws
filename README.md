# youtube-analysis-aws

## Project Summary: YouTube Data Pipeline and Analysis on AWS

**Data Ingestion and Cataloging:**

* Uploaded YouTube dataset to S3 using AWS CLI script.
* Crawled data using AWS Glue Crawler.
* Created a Data Catalog for data discovery and lineage tracking.
* Queried the raw data using Amazon Athena for initial exploration.

**Data Transformation:**

* **JSON Files:**
    * Developed an AWS Lambda function to transform JSON files to a desired format.
    * Converted the transformed data to Parquet format for efficient storage and querying.
    * Stored the cleaned and formatted Parquet files in a separate S3 bucket ("cleaned").
    * Used Amazon Athena to verify the successful transformation of JSON data.
* **CSV Files:**
    * Created an AWS Glue notebook to transform and partition CSV files by region.
    * Converted the CSV files to Parquet format for improved performance.
    * Stored the partitioned Parquet files in the same S3 bucket ("cleaned").

**Data Orchestration and Joining:**

* Implemented an AWS Lambda trigger to automatically execute the Lambda function when new files arrive in the S3 bucket.
* Designed an AWS Glue ETL job to:
    * Join the transformed JSON and CSV datasets based on specific criteria.
    * Store the final joined data in a dedicated S3 bucket for visualization.

**Data Visualization:**

* Loaded the joined data set into Amazon QuickSight.
* Developed interactive dashboards for data analysis and insights generation.

**Key Points:**

* This project demonstrates a complete data pipeline on AWS, encompassing data ingestion, transformation, orchestration, and visualization.
* It highlights the use of various AWS services like S3, Glue, Lambda, Athena, and QuickSight for data management and analytics.


**Dataset Used**
This Kaggle dataset contains statistics (CSV files) on daily popular YouTube videos over the course of many months. There are up to 200 trending videos published every day for many locations. The data for each region is in its own file. The video title, channel title, publication time, tags, views, likes and dislikes, description, and comment count are among the items included in the data. A category_id field, which differs by area, is also included in the JSON file linked to the region.

* https://www.kaggle.com/datasets/datasnaek/youtube-new
