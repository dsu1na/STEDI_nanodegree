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


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node accelerometer landing
accelerometerlanding_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="accelerometerlanding_node1",
)

# Script generated for node customer trusted
customertrusted_node1692723364492 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="customertrusted_node1692723364492",
)

# Script generated for node Join accelerometer and customer data
Joinaccelerometerandcustomerdata_node1692723499343 = Join.apply(
    frame1=accelerometerlanding_node1,
    frame2=customertrusted_node1692723364492,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Joinaccelerometerandcustomerdata_node1692723499343",
)

# Script generated for node SQL Query to filter accelerometer readings when consent is in place
SqlQuery0 = """
select 
user,
timestamp,
x,
y,
z
from myDataSource
WHERE timestamp >= sharewithresearchasofdate
"""
SQLQuerytofilteraccelerometerreadingswhenconsentisinplace_node1692723859169 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"myDataSource": Joinaccelerometerandcustomerdata_node1692723499343},
    transformation_ctx="SQLQuerytofilteraccelerometerreadingswhenconsentisinplace_node1692723859169",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=SQLQuerytofilteraccelerometerreadingswhenconsentisinplace_node1692723859169,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-das-nanod/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()