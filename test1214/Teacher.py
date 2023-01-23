#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Project ：pythonProject 
@File    ：Teacher.py
@Author  ：itcast
@Date    ：2022/12/17 6:35 
'''
from test1214.abcmeta2.Human import Human


class Teacher(Human):

    def sleep(self):
        print('每天睡7个小时...')

if __name__ == '__main__':
    t = Human()
    t.execute()