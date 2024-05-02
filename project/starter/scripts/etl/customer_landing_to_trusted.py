import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Data source: the Glue table for customer_landing
datasource0 = glueContext.create_dynamic_frame.from_catalog(
    database = "stedi",
    table_name = "customer_landing",
    transformation_ctx = "datasource0"
)

# Filter records where shareWithResearchAsOfDate is not null
filtered_frame = Filter.apply(frame = datasource0, f = lambda x: x["shareWithResearchAsOfDate"] is not None)

# Write out the data to S3 in parquet format
sink = glueContext.write_dynamic_frame.from_options(
    frame = filtered_frame,
    connection_type = "s3",
    connection_options = {"path": "s3://udacity-stedi/customer/trusted/"},
    format = "parquet",
    transformation_ctx = "sink"
)

job.commit()
