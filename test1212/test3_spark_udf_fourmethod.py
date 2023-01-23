#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Project ：pythonProject 
@File    ：test3_spark_udf_fourmethod.py
@Author  ：itcast
@Date    ：2022/12/15 6:01 
'''
import os

from pyspark.sql import SparkSession, functions as F

SPARK_HOME = '/export/server/spark'
PYSPARK_PYTHON = '/root/anaconda3/envs/pyspark_env/bin/python'
# 导入路径
os.environ['SPARK_HOME'] = SPARK_HOME
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON

spark: SparkSession = SparkSession.builder.master('local[2]').appName('test').getOrCreate()

df = spark.createDataFrame([(7, 'jack', 58), (8, 'Rose', 48)], ['id', 'name', 'age'])


@F.udf
def nameToUpper(name: str):
    return name.upper()


df.select(df.name, nameToUpper(df.name)).show(truncate=False)


# age年龄字段增加1岁
@F.udf(returnType=IntegerType())
def aggAddOne(age):
    return age + 1


df.select(df.age, aggAddOne(df.age)).show(truncate=False)


# 求用户名长度
def nameLengthUDF(name):
    return len(name)

F.udf(nameLengthUDF,returnType=IntegerType())


df.select(df.name, nameLengthUDF(df.name)).show(truncate=False)
