#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Project ：pythonProject 
@File    ：Doctor.py
@Author  ：itcast
@Date    ：2022/12/17 6:40 
'''
from test1214.abcmeta2.Human import Human


class Doctor(Human):

    def sleep(self):
        print('每天睡觉5个小时')

    def acid(self):
        print('每天核酸1000次！')

if __name__ == '__main__':
    d = Human
    d.execute()

    doc = Doctor()
    doc.execute()