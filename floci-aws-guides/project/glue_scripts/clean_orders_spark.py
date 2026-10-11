"""Glue Spark (glueetl) variant for real AWS. Not executed by Floci."""
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME", "SOURCE_PATH", "TARGET_PATH"])
sc = SparkContext()
glue = GlueContext(sc)
spark = glue.spark_session
job = Job(glue)
job.init(args["JOB_NAME"], args)

df = spark.read.option("header", True).csv(args["SOURCE_PATH"])
clean = (df.dropDuplicates(["order_id"])
           .withColumn("amount", F.col("amount").cast("double"))
           .filter(F.col("amount") > 0))
clean.write.mode("overwrite").option("header", True).csv(args["TARGET_PATH"])
job.commit()
