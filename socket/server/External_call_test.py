#!/usr/bin/python
# -*- encoding: utf-8 -*-
'''
@Time    :   2021/03/12 10:45:54
@Author  :   LI DIANKAI 
@Version :   1.0
@Contact :   lidiankai@kingsoft.com
'''


from server import UdpInfo, Troy

class TaskInfo(UdpInfo):

    @staticmethod
    def ipList():
        # return list
        ipList = ['10.11.163.179']
        return ipList

    @staticmethod
    def disCommands():
        # return dict
        taskDict = {
            'ipconfig': 100,
            'gpedit.msc': 80,
        }
        return taskDict

    @staticmethod
    def key():
        # return str
        key = 'AC9198'
        return key


if __name__ == '__main__':
    
    serverConnectObj = Troy()
    serverConnectObj.udpSendInstructions(TaskInfo)