import sys
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load data from the Step Trainer Landing Zone
step_trainer_data = glueContext.create_dynamic_frame.from_catalog(
    database = "stedi",
    table_name = "step_trainer_landing",
    transformation_ctx = "step_trainer_data"
)

# Load curated customer data
customers_curated = glueContext.create_dynamic_frame.from_catalog(
    database = "stedi",
    table_name = "customer_curated",
    transformation_ctx = "customer_curated"
)

# Convert DynamicFrames to DataFrames for joins
df_step_trainer = step_trainer_data.toDF()
df_customers_curated = customers_curated.toDF()

# Join on the serialNumber and filter by customers who have agreed and have accelerometer data
df_trusted = df_step_trainer.join(df_customers_curated, "serialNumber").distinct()

# Convert back to DynamicFrame
trusted_dynamic_frame = DynamicFrame.fromDF(df_trusted, glueContext, "trusted_dynamic_frame")

# Write the data back to S3 with overwrite mode
sink = glueContext.write_dynamic_frame.from_options(
    frame = trusted_dynamic_frame,
    connection_type = "s3",
    connection_options = {
        "path": "s3://udacity-stedi/step_trainer/trusted/",
        "mode": "overwrite"
    },
    format = "parquet",
    transformation_ctx = "sink"
)

job.commit()
