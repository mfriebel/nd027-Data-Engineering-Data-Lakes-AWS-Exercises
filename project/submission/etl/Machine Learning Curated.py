import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs


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

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1702885791220 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node1702885791220",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1702885746704 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="steptrainer_trusted",
    transformation_ctx="StepTrainerTrusted_node1702885746704",
)

# Script generated for node SQL Query
SqlQuery1883 = """
select st.sensorreadingtime, st.serialnumber, st.distancefromobject, 
ac.timestamp, ac.user, ac.x, ac.y, ac.z
from steptrainer_trusted as st
join accelerometer_trusted as ac
on st.sensorreadingtime = ac.timestamp
"""
SQLQuery_node1702981678072 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1883,
    mapping={
        "steptrainer_trusted": StepTrainerTrusted_node1702885746704,
        "accelerometer_trusted": AccelerometerTrusted_node1702885791220,
    },
    transformation_ctx="SQLQuery_node1702981678072",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1702886091086 = DynamicFrame.fromDF(
    SQLQuery_node1702981678072.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1702886091086",
)

# Script generated for node Machine Learning Curated
MachineLearningCurated_node1702886605362 = glueContext.getSink(
    path="s3://udc-lake-house/step_trainer/machine_learning/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="MachineLearningCurated_node1702886605362",
)
MachineLearningCurated_node1702886605362.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="machine_learning_curated"
)
MachineLearningCurated_node1702886605362.setFormat("json")
MachineLearningCurated_node1702886605362.writeFrame(DropDuplicates_node1702886091086)
job.commit()
