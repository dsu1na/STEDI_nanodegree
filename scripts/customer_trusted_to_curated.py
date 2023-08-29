import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node accelerometer trusted
accelerometertrusted_node1692849128699 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometertrusted_node1692849128699",
)

# Script generated for node customer trusted
customertrusted_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="customertrusted_node1",
)

# Script generated for node join
join_node1692849240442 = Join.apply(
    frame1=customertrusted_node1,
    frame2=accelerometertrusted_node1692849128699,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="join_node1692849240442",
)

# Script generated for node Drop Fields
DropFields_node1692849297373 = DropFields.apply(
    frame=join_node1692849240442,
    paths=["user", "timestamp", "x", "y", "z"],
    transformation_ctx="DropFields_node1692849297373",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1692849838752 = DynamicFrame.fromDF(
    DropFields_node1692849297373.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1692849838752",
)

# Script generated for node customer curated
customercurated_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1692849838752,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-das-nanod/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="customercurated_node3",
)

job.commit()