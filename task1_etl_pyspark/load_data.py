import boto3
import os
import configparser
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import functions as f
import json 

my_bucket = 's3a://dataminded-academy-capstone-resources/raw/open_aq/'

#os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages=org.apache.hadoop:hadoop-aws:3.1.2 pyspark-shell"

spark_conf= SparkConf()
spark_conf.set("spark.jars.packages","org.apache.hadoop-aws:3.1.2,net.snowflake:spark-snowflake_2.12:2.9.0-spark_3.1,net.snowflake:snowflake-jdbc:3.13.3")
spark_conf.set("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
spark_conf.set("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")

spark = SparkSession.builder.config(conf= spark_conf).getOrCreate()

df=spark.read.json(my_bucket)

# Flatten the data
df = df.select('*','coordinates.*','date.*').drop('coordinates','date')

# Change the data types
df = df.withColumn("local", f.to_timestamp(f.col("local")))
df = df.withColumn("utc", f.to_timestamp(f.col("utc")))
df.show()
print(df.dtypes)

# Retrieve secrets for snowflake: 
secret = 'snowflake/capstone/login'
session = boto3.session.Session()
s3_client = session.client('secretsmanager')
retrieved_secret = s3_client.get_secret_value(SecretId=secret)["SecretString"]

retrieved_secret = json.loads(retrieved_secret)

sfOptions = {
    "sfURL":retrieved_secret['URL']+".snowflakecomputing.com",
    "sfWarehouse": retrieved_secret['WAREHOUSE'],
    "sfUser": retrieved_secret['USER_NAME'],
    "sfRole": retrieved_secret['ROLE'],
    "sfPassword": retrieved_secret['PASSWORD'],
    "sfDatabase": retrieved_secret['DATABASE'],
    "sfSchema": 'NELE'
}

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
df.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable","airquality").mode("overwrite").save()
