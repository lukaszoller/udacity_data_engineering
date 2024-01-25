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

# Script generated for node step_trainer_landing
step_trainer_landing_node1706216815753 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://borti-stedi/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="step_trainer_landing_node1706216815753",
)

# Script generated for node customer_trusted
customer_trusted_node1705660072172 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://borti-stedi/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="customer_trusted_node1705660072172",
)

# Script generated for node SQL Query
SqlQuery0 = """
select * 
from step
inner join cus on step.serialnumber = cus.serialnumber
"""
SQLQuery_node1706216830327 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "step": step_trainer_landing_node1706216815753,
        "cus": customer_trusted_node1705660072172,
    },
    transformation_ctx="SQLQuery_node1706216830327",
)

# Script generated for node Amazon S3
AmazonS3_node1705660470302 = glueContext.getSink(
    path="s3://borti-stedi/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1705660470302",
)
AmazonS3_node1705660470302.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="step_trainer_trusted"
)
AmazonS3_node1705660470302.setFormat("json")
AmazonS3_node1705660470302.writeFrame(SQLQuery_node1706216830327)
job.commit()
