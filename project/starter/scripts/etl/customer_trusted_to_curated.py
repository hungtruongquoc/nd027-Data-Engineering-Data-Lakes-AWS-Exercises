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

# Script generated for node customer_trusted
customer_trusted_node1714635359611 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="customer_trusted_node1714635359611")

# Script generated for node acc_trusted
acc_trusted_node1714635417461 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="acc_trusted_node1714635417461")

# Script generated for node SQL Query
SqlQuery0 = '''
select distinct cust.* 
from cust inner join acce on cust.email = acce.user where cust.sharewithresearchasofdate is not null
'''
SQLQuery_node1714635432200 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"acce":acc_trusted_node1714635417461, "cust":customer_trusted_node1714635359611}, transformation_ctx = "SQLQuery_node1714635432200")

# Script generated for node Amazon S3
AmazonS3_node1714635530866 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1714635432200, connection_type="s3", format="glueparquet", connection_options={"path": "s3://udacity-stedi/customer/curated/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1714635530866")

job.commit()