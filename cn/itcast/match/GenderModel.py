#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Project ：pythonProject 
@File    ：GenderModel.py
@Author  ：itcast
@Date    ：2022/12/15 6:53 
'''
from cn.itcast.bean.ESMeta import ruleToESMeta
from pyspark.sql import SparkSession, functions as F


@F.udf
def genderToTagsId(gender):
    return fiveDict[str(gender)]

@F.udf
def mergeDf(newTagsId, oldTagsId):
    if newTagsId is None:
        return oldTagsId
    if oldTagsId is None:
        return newTagsId
    oldTagsIdList = str(oldTagsId).split(',')
    oldTagsIdList.append(str(newTagsId))
    return ",".join(set(oldTagsIdList))


if __name__ == '__main__':
    """
    0.准备Spark开发环境
    1.读取MySQL中的数据【优化：只读四级标签和对应的五级标签】
    2.读取和年龄段标签相关的4级标签rule并解析
    3.根据解析出的rule读取es数据
    4.读取和年龄段标签相关的5级标签(根据4级标签的id作为pid查询)
    5.将esDF和fiveDS进行匹配计算, 得到的数据继续处理:
      5.1.将fiveDF转为map, 方便后续自定义UDF操作
      5.2.使用单表 + UDF完成esDF和fiveDS的匹配
    6.查询ES中的oldDF
    7.合并newDF和oldDF
    8.将最终结果写到ES
    """
    SPARK_HOME = '/export/server/spark'
    PYSPARK_PYTHON = '/root/anaconda3/envs/pyspark_env/bin/python'
    # 导入路径
    os.environ['SPARK_HOME'] = SPARK_HOME
    os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON

    spark: SparkSession = SparkSession.builder.master('local[2]').appName('AgeModel').getOrCreate()
    # 1.读取mysql的业务标签规则数据
    url = "jdbc:mysql://up01:3306/tfec_tags?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&useSSL=false"
    mysqlDf = spark \
        .read \
        .format('jdbc') \
        .option('url', url) \
        .option('dbtable', 'tbl_basic_tag') \
        .load()
    # 2.读取和年龄段标签相关的4级标签rule并解析
    fourTagsId = 4
    fourDf = mysqlDf.where(f'id ={fourTagsId}').select(mysqlDf.rule)
    fourDF = mysqlDf.where(f'id={fourTagsId}').select(mysqlDf.rule)
    # 第二种写法
    # mysqlDf.where('id = 14').select(mysqlDf['rule'])

    # 3. 根据 4级标签 rule ，作为 es 元数据信息，查询 es 源索引表数据
    fourRuleStr = fourDF.rdd.map(lambda row: row.rule).collect()[0]
    esMeta = ruleToESMeta(fourRuleStr)
    print(esMeta)

    esDF = spark \
        .read \
        .format('es') \
        .option('es.nodes', esMeta.esNodes) \
        .option('es.resource', esMeta.esIndex) \
        .option('es.read.field.include', esMeta.selectFields) \
        .load()
    esDF.printSchema()
    esDF.show(truncate=False)

    # 4.  从mysql业务标签规则数据中过滤出5级标签数据
    fiveDF = mysqlDf.where(f'pid = {fourTagsId}').select(mysqlDf.id, mysqlDf.rule)

    fiveDF.printSchema()
    fiveDF.show(truncate=False)
    # 5.  根据标签规则，实现标签匹配逻辑,返回newDF
    # 5.1 把fiveDF转换为dict
    fiveDict = fiveDF.rdd.map(lambda row: (row.rule, row.id)).collectAsMap()
    # 5.2 查询esDF的数据，使用udf函数，传入gender的值，返回五级标签id => dict的value
    newDF = esDF.select(esDF.id.alias('userId'), genderToTagsId(esDF.gender).alias('tagsId'))
    # 6.  读取旧的标签结果数据,返回oldDF
    oldDF = spark \
        .read \
        .format('es') \
        .option('es.nodes', esMeta.esNodes) \
        .option('es.resource', 'tfec_userprofile_result') \
        .option('es.read.field.include', 'userId,tagsId') \
        .load()
    oldDF.printSchema()
    oldDF.show(truncate=False)
    # 7.  根据用户id相同，进行新旧标签合并处理，返回resultDF
    # 7.1. 使用left join实现新旧df的合并
    # 7.2. 已经根据userId相同合并新旧标签df的数据集，再使用udf函数合并新旧标签id
    resultDF = newDF \
        .join(other=oldDF,
              on=newDF.userId == oldDF.userId,
              how='left'
              ).select(newDF.userId, mergeDf(newDF.tagsId, oldDF.tagsId).alias('tagsId'))
    resultDF\
        .write\
        .format('es')\
        .option('es.nodes', esMeta.esNodes)\
        .option('es.resource', 'tfec_userprofile_result/_doc') \
        .option('es.mapping.id', 'userId') \
        .option('es.write.operation', 'upsert') \
        .mode('append')\
        .save()