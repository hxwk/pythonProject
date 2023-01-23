#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Project ：pythonProject 
@File    ：AgeModel.py
@Author  ：itcast
@Date    ：2022/12/15 6:20 
'''
import os

from pyspark.sql import SparkSession, functions as F

from cn.itcast.bean.ESMeta import ruleToESMeta

if __name__ == '__main__':
    """
    0.创建spark环境，获得SparkSession对象
	1.读取mysql的业务标签规则数据
	2.从mysql业务标签规则数据中过滤出4级标签数据
	3.根据4级标签rule，作为es元数据信息，查询es源索引表数据
	4.从mysql业务标签规则数据中过滤出5级标签数据
	5.根据标签规则，实现标签匹配逻辑
	6.把标签计算结果写入到es结果索引表中
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

    fourTagsId = 14
    # 2.过滤出 4级 标签数据
    fourDF = mysqlDf.where(f'id={fourTagsId}').select(mysqlDf.rule)
    # 第二种写法
    # mysqlDf.where('id = 14').select(mysqlDf['rule'])

    # 3. 根据 4级标签 rule ，作为 es 元数据信息，查询 es 源索引表数据
    fourRuleStr = fourDF.rdd.map(lambda row: row.rule).collect()[0]
    esMeta = ruleToESMeta(fourRuleStr)
    print(esMeta)

    # spark 读取指定的 es 源表数据
    esDf = spark \
        .read \
        .format('es') \
        .option('es.nodes', esMeta.esNodes) \
        .option('es.resource', esMeta.esIndex) \
        .option('es.read.field.include', esMeta.selectFields) \
        .load()

    # 4. 从 mysql 业务标签规则数据中过滤出 5 级标签数据
    fiveDF = mysqlDf.where(f'pid={fourTagsId}').select(mysqlDf.id, mysqlDf.rule)

    # 5. 根据标签规则，实现标签匹配逻辑
    esDF2 = esDf.select(esDf.id, F.regexp_replace(F.substring(mysqlDf.birthday, 1, 10), '-', '').alias('birth'))
    fiveDF2 = fiveDF.select(
        fiveDF.id,
        F.split(fiveDF.rule, '-')[0].alias('start'),
        F.split(fiveDF.rule, '-')[1].alias('end')
    )

    result = esDF2 \
        .join(fiveDF2) \
        .where(
        esDf2.birth
            .between(fiveDF2.start, fiveDF2.end)
    ) \
        .select(esDf2.id.alias('userId'), fiveDF2.id.alias('tagsId').cast('string'))
    # 写入到 es
    result.write\
        .format('es')\
        .option('es.nodes', esMeta.esNodes)\
        .option('es.resource', 'tfec_userprofile_result/_doc') \
        .option("es.mapping.id", "userId") \
        .option("es.write.operation", "upsert") \
        .mode('append') \
        .save()