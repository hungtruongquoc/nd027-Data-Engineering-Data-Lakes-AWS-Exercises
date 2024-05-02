import sys
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load data from the Trusted Zone tables
step_trainer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database = "stedi",
    table_name = "step_trainer_trusted",
    transformation_ctx = "step_trainer_trusted"
)

accelerometer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database = "stedi",
    table_name = "accelerometer_trusted",
    transformation_ctx = "accelerometer_trusted"
)

# Convert to DataFrames
df_step_trainer = step_trainer_trusted.toDF()
df_accelerometer = accelerometer_trusted.toDF()

# Join on serialNumber and timestamp
df_joined = df_step_trainer.join(df_accelerometer, (df_step_trainer.serialNumber == df_accelerometer.serialNumber) &
                                 (df_step_trainer.sensorReadingTime == df_accelerometer.timeStamp))

# Select required fields and perform any necessary transformations/aggregations
df_ml_curated = df_joined.select(
    df_step_trainer.serialNumber,
    df_step_trainer.sensorReadingTime,
    df_step_trainer.distanceFromObject,
    df_accelerometer.x,
    df_accelerometer.y,
    df_accelerometer.z
).distinct()

# Convert back to DynamicFrame
ml_curated_dynamic_frame = DynamicFrame.fromDF(df_ml_curated, glueContext, "ml_curated_dynamic_frame")

# Write the data back to S3 with overwrite mode
sink = glueContext.write_dynamic_frame.from_options(
    frame = ml_curated_dynamic_frame,
    connection_type = "s3",
    connection_options = {
        "path": "s3://udacity-stedi/machine_learning/curated/",
        "mode": "overwrite"
    },
    format = "parquet",
    transformation_ctx = "sink"
)

job.commit()
