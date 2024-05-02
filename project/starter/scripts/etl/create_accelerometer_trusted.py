import sys
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from awsglue.dynamicframe import DynamicFrame  # Import DynamicFrame correctly

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = SparkSession.builder.config("spark.sql.broadcastTimeout", "36000").getOrCreate()
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load the data from Glue Catalog
accel_data = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing")
customer_data = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted")

# Convert to Spark DataFrame for easier operations
df_accel = accel_data.toDF()
df_customer = customer_data.toDF()

# Assuming 'user' in df_accel matches 'email' in df_customer
df_customer = df_customer.withColumnRenamed("email", "user")

# Join dataframes to filter accelerometer data
df_joined = df_accel.join(df_customer, "user", "inner")

# Filter out only records with non-null 'shareWithResearchAsOfDate'
df_filtered = df_joined.filter(col("shareWithResearchAsOfDate").isNotNull())

# Convert back to DynamicFrame
filtered_dynamic_frame = DynamicFrame.fromDF(df_filtered, glueContext, "filtered_dynamic_frame")

# Write out the data to S3 in parquet format
glueContext.write_dynamic_frame.from_options(
    frame = filtered_dynamic_frame,
    connection_type = "s3",
    connection_options = {"path": "s3://udacity-stedi/customer/curated/"},
    format = "parquet"
)

job.commit()
