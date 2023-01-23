#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Project ：pythonProject 
@File    ：test4_meta_str_obj.py
@Author  ：itcast
@Date    ：2022/12/14 7:13 
'''
from dataclasses import dataclass


@dataclass
class ESMeta:
    inType: str
    esNodes: str
    esIndex: str
    esType: str
    selectFields: str

    def __str__(self):
        return f'ESMeta(inType={self.inType},esNodes={self.esNodes},esIndex={self.esIndex},esType={self.esType},selectFields={self.selectFields})'


def splitToMeta(es: str) -> ESMeta:
    es_list = es.split("##")
    es_map = {}
    for kv in es_list:
        keyValue = kv.split('=')
        es_map[keyValue[0]] = keyValue[1]
    return ESMeta(**es_map)


if __name__ == '__main__':
    es_str = 'inType=Elasticsearch##esNodes=up01:9200##esIndex=tfec_tbl_users##esType=_doc##selectFields=id,gender'
    esMeta: ESMeta = splitToMeta(es_str)
    print(esMeta)
