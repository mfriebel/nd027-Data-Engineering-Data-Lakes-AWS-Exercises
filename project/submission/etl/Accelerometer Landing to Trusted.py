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

# Script generated for node Accelerometer Landing Node
AccelerometerLandingNode_node1702650902317 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="stedi",
        table_name="accelerometer_landing",
        transformation_ctx="AccelerometerLandingNode_node1702650902317",
    )
)

# Script generated for node Customer Trusted
CustomerTrusted_node1702651065236 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1702651065236",
)

# Script generated for node Join
Join_node1702651186049 = Join.apply(
    frame1=AccelerometerLandingNode_node1702650902317,
    frame2=CustomerTrusted_node1702651065236,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1702651186049",
)

# Script generated for node Drop Fields
DropFields_node1702658109415 = DropFields.apply(
    frame=Join_node1702651186049,
    paths=[
        "serialNumber",
        "birthDay",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "customerName",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithFriendsAsOfDate",
        "shareWithPublicAsOfDate",
    ],
    transformation_ctx="DropFields_node1702658109415",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1702658117560 = DynamicFrame.fromDF(
    DropFields_node1702658109415.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1702658117560",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1702651218854 = glueContext.getSink(
    path="s3://udc-lake-house/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AccelerometerTrusted_node1702651218854",
)
AccelerometerTrusted_node1702651218854.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="accelerometer_trusted"
)
AccelerometerTrusted_node1702651218854.setFormat("json")
AccelerometerTrusted_node1702651218854.writeFrame(DropDuplicates_node1702658117560)
job.commit()
