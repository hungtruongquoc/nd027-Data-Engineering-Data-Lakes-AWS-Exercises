import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node accelerometer
accelerometer_node1714634234592 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="accelerometer_node1714634234592")

# Script generated for node customer
customer_node1714634526092 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_curated", transformation_ctx="customer_node1714634526092")

# Script generated for node step_trainer
step_trainer_node1714634190294 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="step_trainer_node1714634190294")

# Script generated for node SQL Query
SqlQuery0 = '''
select * from s inner join a on s.sensorReadingTime = a.timestamp 
inner join cus on cus.email = a.user 
where cus.sharewithresearchasofdate is not null
'''
SQLQuery_node1714634257976 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"s":step_trainer_node1714634190294, "a":accelerometer_node1714634234592, "cus":customer_node1714634526092}, transformation_ctx = "SQLQuery_node1714634257976")

# Script generated for node Amazon S3
AmazonS3_node1714634692095 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1714634257976, connection_type="s3", format="glueparquet", connection_options={"path": "s3://udacity-stedi/machine_learning/curated/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1714634692095")

job.commit()