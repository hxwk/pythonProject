# 2-服务器路径
import os

from pyspark.sql import SparkSession

SPARK_HOME = '/export/server/spark'
PYSPARK_PYTHON = '/root/anaconda3/envs/pyspark_env/bin/python'
# 导入路径
os.environ['SPARK_HOME'] = SPARK_HOME
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON

spark: SparkSession = SparkSession.builder.master('local[2]').appName('test').getOrCreate()

# -   es.nodes = 'up01:9200'
# -   es.resource = 'my_index/_doc'
# -   es.index.read.missing.as.empty = 'yes'
# -   es.read.field.include = 'id, birthday'
esDF = spark.read \
    .format("es") \
    .option("es.nodes", "up01:9200") \
    .option("es.resource", "tfec_tbl_users/_doc") \
    .option("es.index.read.missing.as.empty", "yes") \
    .option("es.read.field.include", "id, birthday") \
    .load()

# -   es.nodes = 'up01:9200'
# -   es.resource = 'my_index/_doc'
# -   es.mapping.id = 'id'
# -   es.mapping.name = 'userId:userId,tagsId:tagsId'
# -   es.write.operation = 'upsert'
# -   mode = 'append'

esDF.write \
    .format("es") \
    .option("es.nodes", "up01:9200") \
    .option("es.resource", "mytest_1212/_doc") \
    .option("es.mapping.id", "id") \
    .option("es.mapping.name", "userId:userId,tagsId:tagsId") \
    .option("es.write.operation", "upsert") \
    .mode('append') \
    .save()
