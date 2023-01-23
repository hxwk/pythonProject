#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Project ：pythonProject 
@File    ：ESMeta.py
@Author  ：itcast
@Date    ：2022/12/15 6:36 
'''
from dataclasses import dataclass

def ruleToESMeta(ruleStr):
    ruleList = ruleStr.split('##')
    ruleDict = {}
    for kv in ruleList:
        ruleDict[kv.split('=')[0]] = kv.split('=')[1]
    return ESMeta(**ruleDict)


@dataclass
class ESMeta:
    """
      dataclass是定义一个为数据类，帮助对象快速实现初始化属性和重写打印对象内容的方法
    """
    inType:str
    esNodes:str
    esIndex:str
    esType:str
    selectFields:str