import sys
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim  # Ensure functions are imported correctly

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = SparkSession.builder.getOrCreate()
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load the data from Glue Catalog
customer_data = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted")
accel_data = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted")

# Convert to Spark DataFrame for easier operations and alias them
df_customer = customer_data.toDF().alias("cust")
df_accel = accel_data.toDF().alias("accel")

# Data cleaning for consistent joining
df_customer = df_customer.withColumn("email", lower(trim(col("email"))))
df_accel = df_accel.withColumn("user", lower(trim(col("user"))))

# Join dataframes to find customers with accelerometer data, using cleaned and aliased columns
df_joined = df_customer.join(df_accel, df_customer["email"] == df_accel["user"], "inner")

# Filter for customers who agreed to share data for research
df_filtered = df_joined.filter(col("cust.shareWithResearchAsOfDate").isNotNull())

# Select distinct customer entries
df_distinct = df_filtered.select("cust.*").distinct()

# Convert back to DynamicFrame
curated_customers = DynamicFrame.fromDF(df_distinct, glueContext, "curated_customers")

# Write out the data to a new S3 location in parquet format
glueContext.write_dynamic_frame.from_options(
    frame = curated_customers,
    connection_type = "s3",
    connection_options = {"path": "s3://udacity-stedi/customer/curated/"},
    format = "parquet"
)

job.commit()