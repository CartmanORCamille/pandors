#!/usr/bin/python
# -*- encoding: utf-8 -*-
'''
@Time    :   2021/03/04 14:42:51
@Author  :   Camille
@Version :   1.0
'''


import logging
import time
import json
import redis
import datetime
import os


class BaseLogs():
    """
        @logName:       types_datetime
        @callerPath:    caller function path
    """
    def __init__(self, logName, callerPath='.\\'):
        if not logName:
            todays = datetime.date.today()
            self.logName = '{}{}'.format(todays, '.log')
        self.logName = logName
        self.callerPath = callerPath
        # The main log folder path.
        self.callerLogsPath = '{}{}'.format(self.callerPath , r'\logs')
        # Default log name.
        self.baseLogDir()
    
    def baseLogDir(self):
        """
            Complete the main log folder creation requirements.
        """
        if not os.path.exists(self.callerLogsPath):
            os.makedirs(self.callerLogsPath)

    def subLogDir(self, subLogPath):
        """
            Complete other log folder creation requirements.
        """
        os.makedirs('{}{}{}'.format(self.callerPath, '\\', subLogPath))

    def logHandler(self, logName=None, w_logName=None):

        # Create the log.
        logPath = '{}{}{}'.format(self.callerLogsPath, '\\', self.logName)
        fileHandler = logging.FileHandler(logPath, 'a', encoding='utf-8')
        # The logs format.
        fmt = logging.Formatter(fmt='%(asctime)s - %(name)s - %(levelname)s - %(module)s: %(message)s')
        fileHandler.setFormatter(fmt)
        # Use the log. Write to self.logName.
        # Default log name: today.
        if w_logName:
            logger = logging.Logger(w_logName)    
        logger = logging.Logger(logPath)
        logger.addHandler(fileHandler)
        return logger
        

class BasicLogs(BaseLogs):
    
    @staticmethod
    def handler(logName=None):
        logsObj = BaseLogs(logName)
        return logsObj


class ChangeRedis():
    """ 自定义redis模块 """
    def __init__(self, redisInfo) -> None:
        """初始化模块，基础参数。

        Args:
            redisInfo (dict): redis配置信息。
            
        使用原生redis库调用redisPointer()，其他函数为自定义函数。
        """
        try:
            self.redisObj = redis.Redis(
                host = redisInfo.get('host'),
                port = 6379,
                password = redisInfo.get('password'),
                decode_responses = True
            )
        except Exception as e:
            # 行数，错误
            print('redis连接函数：意料之外的错误。')
            print(e.__traceback__.tb_lineno, e)
            # 日志记录 - 待补充
            pass

    def redisPointer(self):
        # 返回redis指针给其他函数调用，原生redis库
        return self.redisObj

    def setAddData(self, setName, setList: list):
        """set格式插入数据

        Args:
            setName (string): set集合名称
            setList (list): 插入的数据列表
        """
        if len(setList) == 1:
            self.redisObj.sadd(setName, setList[0])
        elif len(setList) > 1:
            for i in setList:
                self.redisObj.sadd(setName, i)
        else:
            raise ValueError('ERROR: redis set 传入数据有误。')


class PrettyCode():
    
    @staticmethod
    def loadingConfigJson(file: str) -> dict:
        with open(file, 'r', encoding='utf-8') as f:
            config = json.load(f)
        return config

    @staticmethod
    def prettyPrint(msg, level='INFO'):
        nowTime = time.strftime('%d-%m-%Y %H:%M:%S', time.localtime())
        prettyMsg = '[{}] {} - {}'.format(nowTime, level, msg)
        print(prettyMsg)
        return prettyMsg


if __name__ == "__main__":
    obj = BasicLogs.handler(logName='abc.log')
    obj.logHandler().warning
    pass