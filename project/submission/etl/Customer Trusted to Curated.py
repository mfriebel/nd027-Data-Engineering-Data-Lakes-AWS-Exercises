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

# Script generated for node Customer Trusted
CustomerTrusted_node1702658704278 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1702658704278",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1702658760673 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node1702658760673",
)

# Script generated for node Join
Join_node1702658795525 = Join.apply(
    frame1=CustomerTrusted_node1702658704278,
    frame2=AccelerometerTrusted_node1702658760673,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1702658795525",
)

# Script generated for node Drop Fields
DropFields_node1702658950214 = DropFields.apply(
    frame=Join_node1702658795525,
    paths=["z", "user", "y", "x", "timestamp"],
    transformation_ctx="DropFields_node1702658950214",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1702658970124 = DynamicFrame.fromDF(
    DropFields_node1702658950214.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1702658970124",
)

# Script generated for node Customer Curated
CustomerCurated_node1702658995124 = glueContext.getSink(
    path="s3://udc-lake-house/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerCurated_node1702658995124",
)
CustomerCurated_node1702658995124.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_curated"
)
CustomerCurated_node1702658995124.setFormat("json")
CustomerCurated_node1702658995124.writeFrame(DropDuplicates_node1702658970124)
job.commit()
