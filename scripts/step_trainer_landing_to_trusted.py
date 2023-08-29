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

# Script generated for node Amazon S3
AmazonS3_node1693326110849 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_curated",
    transformation_ctx="AmazonS3_node1693326110849",
)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-das-nanod/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Join
Join_node1693326181060 = Join.apply(
    frame1=S3bucket_node1,
    frame2=AmazonS3_node1693326110849,
    keys1=["serialNumber"],
    keys2=["serialnumber"],
    transformation_ctx="Join_node1693326181060",
)

# Script generated for node Drop Fields
DropFields_node1693326308160 = DropFields.apply(
    frame=Join_node1693326181060,
    paths=[
        "serialnumber",
        "birthday",
        "sharewithpublicasofdate",
        "sharewithresearchasofdate",
        "registrationdate",
        "customername",
        "sharewithfriendsasofdate",
        "email",
        "lastupdatedate",
        "phone",
    ],
    transformation_ctx="DropFields_node1693326308160",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1693326342678 = DynamicFrame.fromDF(
    DropFields_node1693326308160.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1693326342678",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1693326342678,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-das-nanod/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()