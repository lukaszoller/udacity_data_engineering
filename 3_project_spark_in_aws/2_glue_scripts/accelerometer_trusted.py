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

# Script generated for node customer_trusted
customer_trusted_node1705855456887 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://borti-stedi/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="customer_trusted_node1705855456887",
)

# Script generated for node accelerometer_landing
accelerometer_landing_node1705660072172 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://borti-stedi/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_landing_node1705660072172",
)

# Script generated for node SQL Query
SqlQuery0 = """
select * FROM accelerometer_landing a INNER JOIN customer_trusted c ON a.user = c.email;
"""
SQLQuery_node1705855259671 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "customer_trusted": customer_trusted_node1705855456887,
        "accelerometer_landing": accelerometer_landing_node1705660072172,
    },
    transformation_ctx="SQLQuery_node1705855259671",
)

# Script generated for node Amazon S3
AmazonS3_node1705660470302 = glueContext.getSink(
    path="s3://borti-stedi/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1705660470302",
)
AmazonS3_node1705660470302.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="accelerometer_trusted"
)
AmazonS3_node1705660470302.setFormat("json")
AmazonS3_node1705660470302.writeFrame(SQLQuery_node1705855259671)
job.commit()
