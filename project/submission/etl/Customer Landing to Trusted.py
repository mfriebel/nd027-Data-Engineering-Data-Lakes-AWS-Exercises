import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Landing
CustomerLanding_node1702644170621 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udc-lake-house/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="CustomerLanding_node1702644170621",
)

# Script generated for node Share with Research
SharewithResearch_node1702644710374 = Filter.apply(
    frame=CustomerLanding_node1702644170621,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="SharewithResearch_node1702644710374",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1702644915025 = glueContext.getSink(
    path="s3://udc-lake-house/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerTrusted_node1702644915025",
)
CustomerTrusted_node1702644915025.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_trusted"
)
CustomerTrusted_node1702644915025.setFormat("json")
CustomerTrusted_node1702644915025.writeFrame(SharewithResearch_node1702644710374)
job.commit()
