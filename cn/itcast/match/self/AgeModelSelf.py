#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Project ：pythonProject 
@File    ：AgeModelSelf.py
@Author  ：itcast
@Date    ：2022/12/17 7:32 
'''
from cn.itcast.base.AbstractModelBase import AbstractModelBase


class AgeModelSelf(AbstractModelBase):

    def getFourTagsId(self):
        return 14

    def compute(self, esDF, fiveDF):
        esDF2: DataFrame = esDF.select(esDF.id,
                                       F.regexp_replace(F.substring(esDF.birthday, 1, 10), '-', '').alias('birth'))

        fiveRuleDF: DataFrame = jdbcDF.where(f'pid={fourTagsId}').select(jdbcDF.id, jdbcDF.rule)
        fiveRuleDF.show(truncate=False)
        # 	5.根据标签规则，实现标签匹配逻辑
        ## 将一个字段转换成两个字段
        fiveRuleDF2: DataFrame = fiveRuleDF.select(
            fiveRuleDF.id,
            F.split(fiveRuleDF.rule, '-')[0].alias('startDate'),
            F.split(fiveRuleDF.rule, '-')[1].alias('endDate')
        )

        resultDF: DataFrame = esDF2.join(
            fiveRuleDF2,
        ).where(esDF2.birth.between(fiveRuleDF2.startDate, fiveRuleDF2.endDate)) \
            .select(esDF2.id.alias('userId'), fiveRuleDF2.id.cast('string').alias('tagsId'))
        resultDF.show(truncate=False)
        return resultDF

if __name__ == '__main__':
    ageModel = AgeModelSelf('AgeModelSelf')
    ageModel.execute()
