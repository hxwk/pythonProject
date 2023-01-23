#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Project ：pythonProject 
@File    ：AbstractModelBase.py
@Author  ：itcast
@Date    ：2022/12/17 6:46 
'''

SPARK_HOME = '/export/server/spark'
PYSPARK_PYTHON = '/root/anaconda3/envs/pyspark_env/bin/python'
# 导入路径
os.environ['SPARK_HOME'] = SPARK_HOME
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON


@F.udf
def mergeToTagsId(newTagId, oldTagId):
    if newTagId == None:
        return oldTagId
    if oldTagId == None:
        return newTagId
    tagList = oldTagId.split(',')
    tagList.append(newTagId)
    return ','.join(set(tagList))


class AbstractModelBase(metaclass=ABCMeta):
    """
    0.  创建spark环境，获得SparkSession对象
    1.  读取mysql的业务标签规则数据
    2.  从mysql业务标签规则数据中过滤出4级标签数据 => 4级标签id需要传入
    3.  根据4级标签rule，作为es元数据信息，查询es源索引表数据
    4.  从mysql业务标签规则数据中过滤出5级标签数据 => 4级标签id需要传入
    5.  根据标签规则，实现标签匹配逻辑,返回newDF => 需要在子类实现匹配、统计、挖掘逻辑
    6.  读取旧的标签结果数据,返回oldDF
    7.  根据用户id相同，进行新旧标签合并处理，返回resultDF
    8.  把标签计算结果写入到es结果索引表中
    """

    def __init__(self, taskName):
        self.__taskName = taskName
        self.spark = SparkSession.builder \
            .master('local[2]') \
            .appName('gender_model') \
            .config('spark.sql.shuffle.partitions', '4') \
            .getOrCreate()

        # 设置打印日志的级别
        self.spark.sparkContext.setLogLevel('WARN')

    def __readMysqlTagData(self, fourTagsId):
        jdbcDF: DataFrame = self.spark.read \
            .format("jdbc") \
            .option("url", "jdbc:mysql://up01:3306/tfec_tags?characterEncoding=utf8") \
            .option("query", f'select id, rule,pid from tbl_basic_tag where id={fourTagsId} or pid = {fourTagsId}') \
            .option("user", "root") \
            .option("password", "123456") \
            .load()
        return jdbcDF

    @abstractmethod
    def getFourTagsId(self):
        pass

    def __readFourRuleData(self, mysqlDF, fourTagsId):
        fourDF = mysqlDF.where(f'id ={fourTagsId}').select(mysqlDF.rule)
        return fourDF

    def __getEsMetaFromFourRule(self, fourDF):
        fourRuleStr = fourDF.rdd.map(lambda row: row.rule).collect()[0]
        # 	3.根据4级标签rule，作为es元数据信息，查询es源索引表数据
        esMeta: ESMeta = strToESMeta(fourRuleStr)
        return esMeta

    def __readEsWithEsMeta(self, esMeta):
        esDF: DataFrame = spark.read \
            .format("es") \
            .option("es.nodes", esMeta.esNodes) \
            .option("es.index.auto.create", "yes") \
            .option("es.resource", esMeta.esIndex) \
            .option("es.read.field.include", esMeta.selectFields) \
            .load()
        return esDF

    def __readFiveRuleData(self, mysqlDF, fourTagsId):
        fiveRuleDF: DataFrame = jdbcDF.where(f'pid={fourTagsId}').select(jdbcDF.id, jdbcDF.rule)
        fiveRuleDF.show(truncate=False)
        return fiveRuleDF

    @abstractmethod
    def compute(self, esDF, fiveDF):
        pass

    def __readOldTagsResultData(self, esMeta):
        oldDF: DataFrame = spark.read \
            .format("es") \
            .option("es.nodes", esMeta.esNodes) \
            .option("es.index.auto.create", "yes") \
            .option("es.resource", 'tfec_userprofile_result') \
            .option("es.read.field.include", 'userId, tagsId') \
            .load()
        return oldDF

    def __mergeNewDFAndOldDF(self, newDF, oldDF):
        resultDF = newDF.join(
            other=oldDF,
            on=newDF.userId == oldDF.userId,
            how='left'
        ).select(
            newDF.userId,
            mergeToTagsId(newDF.tagsId, oldDF.tagsId).alias('tagsId')
        )
        return resultDF

    def __writeResultDFToES(self, resultDF, esMeta):
        resultDF.write \
            .format("es") \
            .option("es.nodes", esMeta.esNodes) \
            .option("es.write.operation", 'upsert') \
            .option("es.mapping.id", 'userId') \
            .option("es.resource", "tfec_userprofile_result") \
            .mode("append") \
            .save()

    def execute(self):
        fourTagsId = self.getFourTagsId()
        mysqlDF = self.__readMysqlTagData(fourTagsId)
        fourDF = self.__readFourRuleData(mysqlDF, fourTagsId)
        esMeta = self.__getEsMetaFromFourRule()
        esDF = self.__readEsWithEsMeta(esMeta)
        fiveDF = self.__readFiveRuleData(mysqlDF, fourTagsId)
        newDF = self.compute(esDF, fiveDF)
        try:
            oldDF = self.__readOldTagsResultData(esMeta)
            resultDF = self.__mergeNewDFAndOldDF(newDF, oldDF)
            self.__writeResultDFToES(resultDF, esMeta)
        except Exception:
            self.__writeResultDFToES(newDF, esMeta)
        logging.warning('标签数据写入父类完毕！')