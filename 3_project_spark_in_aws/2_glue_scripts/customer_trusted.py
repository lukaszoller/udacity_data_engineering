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

# Script generated for node Amazon S3
AmazonS3_node1705660072172 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://borti-stedi/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1705660072172",
)

# Script generated for node SQL Query
SqlQuery0 = """
SELECT *
FROM customer_landing
WHERE sharewithresearchasofdate != 0;
"""
SQLQuery_node1705863318475 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"customer_landing": AmazonS3_node1705660072172},
    transformation_ctx="SQLQuery_node1705863318475",
)

# Script generated for node Amazon S3
AmazonS3_node1705660470302 = glueContext.getSink(
    path="s3://borti-stedi/customer/trusted/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1705660470302",
)
AmazonS3_node1705660470302.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_trusted"
)
AmazonS3_node1705660470302.setFormat("json")
AmazonS3_node1705660470302.writeFrame(SQLQuery_node1705863318475)
job.commit()
