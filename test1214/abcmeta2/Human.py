#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Project ：pythonProject 
@File    ：Human.py
@Author  ：itcast
@Date    ：2022/12/17 6:28 
'''
from abc import ABCMeta, abstractmethod


class Human(metaclass=ABCMeta):
    """
    abc 是 python 的内置模块，帮助实现父类和子类定义
    功能与 class clsName(object) 定义父类是一样的
    唯一区别：提示功能更强
    """

    def eat(self):
        print('一日三餐')

    def drink(self):
        print('每天喝够 5L 水...')

    @abstractmethod
    def sleep(self):
        pass

    def execute(self):
        """
        组装方法，封装到一起
        :return:
        """
        self.eat()
        self.drink()
        self.sleep()