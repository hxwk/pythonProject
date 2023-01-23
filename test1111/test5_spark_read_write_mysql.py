#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Project ：pythonProject 
@File    ：test5_spark_read_write_mysql.py
@Author  ：itcast
@Date    ：2022/12/14 7:23 
'''
import os

from pyspark.sql import SparkSession

# 2-服务器路径
SPARK_HOME = '/export/server/spark'
PYSPARK_PYTHON = '/root/anaconda3/envs/pyspark_env/bin/python'
# 导入路径
os.environ['SPARK_HOME'] = SPARK_HOME
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON

spark: SparkSession = SparkSession.builder.master('local[2]').appName('test').getOrCreate()
jdbcDF = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://up01:3306/tags") \
    .option("dbtable", "tbl_basic_tag") \
    .option("user", "root") \
    .option("password", "123456") \
    .load()
jdbcDF.printSchema()
jdbcDF.show(truncate=False)