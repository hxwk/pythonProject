#!/usr/bin/env python

# -*- coding: UTF-8 -*-
'''
@Project ：pythonProject 
@File    ：test5_merge_tags.py
@Author  ：itcast
@Date    ：2022/12/15 7:13 
'''
if __name__ == '__main__':
    newTagsId = '5'
    oldTagsId = '5,17'
    oldTagsIdList = oldTagsId.split(',')
    oldTagsIdList.append(newTagsId)
    print(oldTagsIdList)
    print(','.join(oldTagsIdList))
    print(','.join(set(oldTagsIdList)))
    print(str('5').split(','))
