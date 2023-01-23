import os

from pyspark.sql import SparkSession

SPARK_HOME = '/export/server/spark'
PYSPARK_PYTHON = '/root/anaconda3/envs/pyspark_env/bin/python'
# 导入路径
os.environ['SPARK_HOME'] = SPARK_HOME
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("test_spark_dataframe") \
    .getOrCreate()

df = spark.createDataFrame([('赵四', 1), ('董宇辉', 19)], ['name', 'age'])
df.printSchema()
df.show(truncate=False)
