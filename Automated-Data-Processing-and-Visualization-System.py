#!/usr/bin/env python
# coding: utf-8

# In[12]:


import os
import pandas as pd
import psycopg2
import boto3
import logging
import plotly.express as px
from sqlalchemy import create_engine
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# AWS S3 Configurations
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
S3_BUCKET_NAME = "your-s3-bucket"

def extract_data(file_path):
    """ Extract data from CSV file with error handling """
    try:
        df = pd.read_csv(file_path, on_bad_lines='skip')  # Skip bad lines
        logging.info("Data extracted successfully")
        return df
    except Exception as e:
        logging.error(f"Error extracting data: {e}")
        return None

def clean_data(df):
    """ Clean the data by removing duplicates and null values """
    if df.empty:
        logging.warning("Dataset is empty, skipping cleaning.")
        return df
    df.drop_duplicates(inplace=True)
    df.dropna(inplace=True)
    logging.info("Data cleaned successfully")
    return df

def load_to_s3(df, file_name):
    """ Load cleaned data to AWS S3 """
    try:
        s3 = boto3.client('s3', 
                          aws_access_key_id=AWS_ACCESS_KEY, 
                          aws_secret_access_key=AWS_SECRET_KEY, 
                          region_name="us-east-1")  # Adjust your AWS region

        df.to_csv(file_name, index=False)
        s3.upload_file(file_name, S3_BUCKET_NAME, file_name)
        logging.info("Data uploaded to S3 successfully")
    except Exception as e:
        logging.error(f"Error uploading to S3: {e}")

def transform_with_spark():
    """ Perform transformations using Apache Spark """
    try:
        spark = SparkSession.builder.appName("ETL Pipeline").getOrCreate()
        spark.conf.set("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY)
        spark.conf.set("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY)

        df = spark.read.csv(f"s3://{S3_BUCKET_NAME}/cleaned_data.csv", header=True, inferSchema=True)
        
        if "date" not in df.columns:
            logging.error("Column 'date' not found in dataset")
            return

        df = df.withColumn("processed_date", col("date").cast("date"))
        df.show()
        logging.info("Transformation with Spark completed")
    except Exception as e:
        logging.error(f"Error in Spark transformation: {e}")

def load_to_database(df, db_url):
    """ Load transformed data into AWS """
    try:
        engine = create_engine(db_url)
        with engine.connect() as connection:
            df.to_sql("processed_data", connection, if_exists='replace', index=False)
        logging.info("Data loaded into database successfully")
    except Exception as e:
        logging.error(f"Database connection error: {e}")

def visualize_data(df):
    """ Create a dashboard using Plotly """
    try:
        if "date" not in df.columns or "value" not in df.columns:
            logging.error("Required columns 'date' or 'value' missing for visualization")
            return
        
        fig = px.scatter(df, x="date", y="value", title="Data Trends")
        fig.show()
        logging.info("Data visualization generated successfully")
    except Exception as e:
        logging.error(f"Error generating visualization: {e}")

if __name__ == "__main__":
    raw_data = extract_data(r"C:\Users\Dias\Downloads\urbandict-word-defs.csv\raw_data.csv")
    if raw_data is not None:
        cleaned_data = clean_data(raw_data)
        load_to_s3(cleaned_data, "cleaned_data.csv")
        transform_with_spark()
        load_to_database(cleaned_data, "postgresql://user:password@host:port/dbname")
        visualize_data(cleaned_data)


# In[ ]:





# In[ ]:




