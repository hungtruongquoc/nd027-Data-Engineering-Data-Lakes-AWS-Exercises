import sys
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = SparkSession.builder.getOrCreate()
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load the data from Glue Catalog
customer_data = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted")
accel_data = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted")

# Convert to Spark DataFrame for easier operations
df_customer = customer_data.toDF().alias("cust")
df_accel = accel_data.toDF().alias("accel")

# Join to find customers with accelerometer data
df_joined = df_customer.join(df_accel, df_customer.email == df_accel.user, "inner")

# Filter for customers who agreed to share data for research, specify the DataFrame for clarity
df_filtered = df_joined.filter(col("cust.shareWithResearchAsOfDate").isNotNull())

# Select distinct customer entries
df_distinct = df_filtered.select("serialNumber", "customerName", "email", "phone", "birthday", "registrationDate", "lastUpdateDate").distinct()

# Convert back to DynamicFrame
curated_customers = DynamicFrame.fromDF(df_distinct, glueContext, "curated_customers")

# Write out the data to a new S3 location in parquet format
glueContext.write_dynamic_frame.from_options(
    frame = curated_customers,
    connection_type = "s3",
    connection_options = {"path": "s3://your-bucket-name/customers_curated/"},
    format = "parquet"
)

job.commit()