import sys
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions

# Initialize Glue Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Parameters from Glue Job
args = getResolvedOptions(sys.argv, ['SOURCE_S3_PATH', 'TARGET_S3_PATH'])

# Load Data from S3
df = spark.read.json(args['SOURCE_S3_PATH'])

# Transform Data
df = df.withColumn("normalized_feature1", col("feature1") / 10.0)\
       .withColumn("normalized_feature2", col("feature2") / 10.0)\
       .withColumn("is_high_label", when(col("label") == 1, True).otherwise(False))

# Write Transformed Data to S3
df.write.mode("overwrite").json(args['TARGET_S3_PATH'])
