#!/usr/bin/python
# -*- encoding: utf-8 -*-
'''
@Time    :   2021/03/16 10:35:28
@Author  :   Camille
@Version :   0.7beta
'''


from concurrent import futures
import socket
from sys import argv
import time
import threading
import struct
import uuid
import os, subprocess
from concurrent.futures import thread, ThreadPoolExecutor
import queue
from auxiliaryTools import PrettyCode, ChangeRedis


class Client():
    def __init__(self) -> None:
        socket.setdefaulttimeout(5)
        self.config = Client._loadingConfig()
        self.maternalIpInfo = None
        self.tcpClientSocket = None

        # 实例化redis
        self.redisObj = ChangeRedis(self.config.get('redisConfig'))
        PrettyCode.prettyPrint('redis server 连接成功。')

        # 线程配置
        self.event = threading.Event()
        self.lock = threading.Lock()
        self.tcpOnline = queue.Queue(1)

        # 任务池
        self.taskPool = ThreadPoolExecutor(max_workers=10)

        # 报文信息（任务汇报）
        self.initializationTaskInfo = {
            'kp': None,
            'code': None,
            'working': None,
            'complete': [],
            'oncall': []
        }

    def recvMsg(self) -> None:
        # udp
        self.udpClientSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        while 1:
            if self.maternalIpInfo:
                # TCP创建完成后才能拿到地址
                self.udpClientSocket.bind(self.maternalIpInfo)
                break
            continue
        PrettyCode.prettyPrint('UDP对象创建成功。')
        # 永久等待信息下发
        self.udpClientSocket.settimeout(None)
        while 1:
            data = self.udpClientSocket.recvfrom(1024)
            recvMsg = data[0].decode('utf-8')
            if recvMsg:
                msg = '数据已接收：{}\n'.format(recvMsg)
                PrettyCode.prettyPrint(msg)

                # 判断信息类型
                if recvMsg.startswith('AC'):
                    # redis任务编码信息
                    tips = '开始执行任务，任务编号: {}'.format(msg)
                    PrettyCode.prettyPrint(tips)
                    # 执行任务
                    self._performOrderRedis(recvMsg)
                else:
                    # cmd指令
                    self._performOrderCMD(recvMsg)
                    recvMsg
                continue
        self.udpClientSocket.close()

    def sendMsg(self, msg) -> None:
        """构建报头

        Args:
            msg (str): 发送的信息。

        Raises:
            e: 预料之外的错误。
        """
        while 1:
            msg = str(msg)
            
            try:
                if not self.tcpClientSocket:
                    break
                # 加锁
                self.lock.acquire()
                msgPack = struct.pack('i', len(msg))
                
                self.tcpClientSocket.send(msgPack)
                self.tcpClientSocket.send(msg.encode('utf-8'))
                PrettyCode.prettyPrint('发送成功。')
                # 释放锁
                self.lock.release()
                if 'keepAlive' not in msg:
                    # 判断是普通心跳包还是其他信息
                    break
                # 需要间隔，不然会粘包，服务端接收数据出错
                time.sleep(5)     
                
            except socket.timeout as timeoutE:
                # 释放锁
                self.lock.release()
                PrettyCode.prettyPrint('发送超时，正在尝试重新发送。', 'ERROR')
                continue
            except Exception as e:
                # 释放锁
                self.lock.release()
                errorMsg = '{}{}'.format(self._errorCheck(e), '，现在重启TCP。')                
                PrettyCode.prettyPrint(errorMsg ,'ERROR')
                # 清空TCP客户端连接
                self.tcpClientSocket = None
                raise e
            
    def makeInfoMsg(self, taskStatus: dict = {}) -> str:
        # 构建报文，default = 'keepAlive'
        if not taskStatus:
            taskStatus = {'kp': 'ADH18/keepAlive', }
        msgList = []
        for key, value in taskStatus.items():
            msgList.append(str(value))
                
        msg = '/'.join(msgList)
        return msg

    def TCPConnect(self) -> None:
        while 1:
            # tcp
            if self.tcpOnline.empty():
                # 离线状态
                
                tcpClientSocket = socket.socket()
                PrettyCode.prettyPrint('TCP对象创建成功。')
                # 重连次数
                nOfRec = 0
                # 连接服务器异常处理
                while 1:
                    recingMsg = '正在连接服务器中 {}'.format(nOfRec)
                    PrettyCode.prettyPrint(recingMsg)
                    try:
                        hostIP = self.config.get('serverConfig').get('host')
                        tcpClientSocket.connect((hostIP, 11451))
                        # 获取与套接字关联的本地协议地址
                        self.maternalIpInfo = (tcpClientSocket.getsockname()[0], 6655)
                        break
                    except:
                        nOfRec += 1
                        continue
                self.tcpOnline.put('ONLINE')
                # 连接成功，event为True
                self.event.set()
                PrettyCode.prettyPrint('服务器连接成功。')
                self.tcpClientSocket = tcpClientSocket

            time.sleep(10)

    def heartbeat(self) -> None:
        while 1:
            # 循环做异常判断检测用
            if not self.tcpClientSocket:
                break
            # 普通心跳包
            msg = self.makeInfoMsg()
            try:
                # 函数内层会进入循环
                # 普通心跳包持续发送
                self.sendMsg(msg)
            except Exception as e:
                # 心跳逻辑层异常
                errorMsg = '[hb Error]意料之外的错误，将关闭本次TCP连接。错误信息：{} - {}'.format(e, e.__traceback__.tb_lineno)
                PrettyCode.prettyPrint(errorMsg, 'ERROR')
                break
            
        # 心跳进程结束
        if self.tcpClientSocket:
            self.tcpClientSocket.close()

    @staticmethod
    def performOrderResult(worker):
        worker.add_done_callback(worker.result)
        while 1:
            if worker.done():
                result = worker.result()
                return result
            time.sleep(1)

    def _performOrderCMD(self, order: str) -> None:
        """执行CMD命令函数

        Args:
            order (str): CMD命令
        """
        self.lock.acquire()
        worker = self.taskPool.submit(self.taskExecuteCMD, order, )
        self.lock.release()
        result = Client.performOrderResult(worker)
        msg = '{} - 任务完成。'.format(order)
        PrettyCode.prettyPrint(msg)

    def _performOrderRedis(self, taskId: str, standardEnd=True) -> None:
        """执行Redis命令函数

        Args:
            taskId (str): 任务编号
            standardEnd (bool, optional): 执行模式. Defaults to True.
        """

        # 获取任务列表，从优先级最高到最低 -> (任务，优先级)
        taskBook = self.redisObj.redisPointer().zrange(taskId, 0, -1, withscores=True, desc=True)
        if taskBook:
            PrettyCode.prettyPrint('任务获取成功。')
            initializationTaskInfo = self.initializationTaskInfo
            initializationTaskInfo['kp'] = 'ADH27I/keepGotIt'
            initializationTaskInfo['code'] = taskId
            # 添加任务至未完成列表并上传到redis
            initializationTaskInfo['complete'] = []
            initializationTaskInfo['oncall'] = [i[0] for i in taskBook]

            # 发送讯息已经接收到任务，即将开始执行
            taskInfo = self.makeInfoMsg(initializationTaskInfo)
            print('接收报文', taskInfo)
            self.sendMsg(taskInfo)
        
        else:
            PrettyCode.prettyPrint('任务获取失败。')
            raise ValueError('任务获取失败。')
        

        # 开始执行任务
        for task in taskBook:
            # 上锁
            self.lock.acquire()
            msg = '开始执行 - {}'.format(task[0])
            PrettyCode.prettyPrint(msg)
            # 向线程池提交任务 -> (任务，优先级)
            worker = self.taskPool.submit(self.taskExecuteCMD, task[0], )
            self.lock.release()

            # 发送执行报文
            initializationTaskInfo['kp'] = 'ADH27II/keepGotIt'
            initializationTaskInfo['working'] = task[0]
            taskInfo = self.makeInfoMsg(initializationTaskInfo)
            self.sendMsg(taskInfo)
            print('执行报文', taskInfo)
            worker.add_done_callback(worker.result)
            result = Client.performOrderResult(worker)

            # 发送任务执行完成报文
            initializationTaskInfo['kp'] = 'ADH27III/keepGotIt'
            taskStatusDict = self._taskReportMsgComplete(initializationTaskInfo, task[0])
            taskInfo = self.makeInfoMsg(taskStatusDict)
            print('完成报文', taskInfo)
            self.sendMsg(taskInfo)

            msg = '{} - 任务完成。'.format(task[0])
            PrettyCode.prettyPrint(msg)

            time.sleep(5)

        return True
        
    def taskExecuteCMD(self, task):
        """任务执行函数

        Args:
            task (str): 任务执行命令
        """
        try:
            self.lock.acquire()
            msg = '正在执行 - {}'.format(task)
            PrettyCode.prettyPrint(msg)
            executor = subprocess.Popen(task, shell=True, stdout=subprocess.PIPE)
            result = executor.stdout.read().decode('gbk')
            self.lock.release()
            return result
        except Exception as e:
            raise e
    
    def daemonlogic(self, existsInfo: dict):
        # 守护进程
        while 1:
            for tName, tFunc in existsInfo.items():
                if tName not in str(threading.enumerate()):
                    # 监测离线
                    if tName == 'heartbeta':
                        # tcpOnline此时为空，即代表offline
                        self.tcpOnline.get()
                        # 如果连接成功则event为true, wait立即返回；如果服务器离线event则改为false，开始阻塞并等待event重新变成true
                        # 这里进入离线监测了，代表此时TCP已经离线，则设置event为false
                        self.event.clear()
                        self.event.wait()
                        
                    tFunc().start()
            time.sleep(10)

    def _taskReportMsgComplete(self, info: dict, task: str):
        # 当任务执行完后更新信息
        for key in info.keys():
            if key == 'working':
                info[key] = None
            elif key == 'complete':
                info.get(key).append(task)
            elif key == 'oncall':
                info.get(key).pop(0)
            elif key == 'kp' or key == 'code':
                # 报头标识符忽略不作处理
                pass
            else:
                # 异常数据
                raise ValueError('异常数据')
        return info
        
    def _taskReport(self, code, func):
        # 结果信息情况汇报（需要采集客户端信息通道）
        report = {
            'identifier': code,
            'kp': 'keepRogerThat',
            'systemInfoTask': func()
        }
        msg = self.makeInfoMsg(report)
        self.sendMsg(msg)

    @staticmethod
    def _loadingConfig():
        # 配置文件
        return PrettyCode.loadingConfigJson(r'config.json')

    @staticmethod
    def _errorCheck(errorInfo):
        # 异常情况分析，给出合理错误结果
        if str(errorInfo).startswith('[WinError 10054]'):
            # 远程主机强迫关闭了一个现有的连接
            return '服务器离线'
        else:
            return '意料之外的错误'

    @staticmethod
    def _getClientSystemInfo():
        # 获取系统信息
        hostname = socket.gethostname()
        localAddrs = socket.getaddrinfo(hostname, None)
        localAddrsIPV4 = [ip[4][0] for ip in localAddrs if ':' not in ip[4][0]]

        # 获取mac地址
        macUUID = uuid.UUID(int=uuid.getnode()).hex[-12:]
        macAddress = '-'.join(macUUID[i: i + 2] for i in range(0, 11, 2))

        localInfo = {
            'hostname': hostname,
            'localAddrsIPV4': localAddrsIPV4,
            'MACAddress': macAddress,
        }
        return localInfo

    def _daemonThread(self, existsInfo: dict) -> thread:
        daemonThread = threading.Thread(target=self.daemonlogic, name='daemonThread', args=(existsInfo, ))
        daemonThread.setDaemon(True)
        return daemonThread      

    def _hbControl(self):
        # 激活心跳
        return threading.Thread(target=self.heartbeat, name='heartbeta')

    def _dataReportControl(self, method):
        # 数据信息汇报
        if method == 'get_system':
            self._taskReport('ADH56', self._getClientSystemInfo)
        

    def _dataReport():
        pass

    def _recvMsgControl(self):
        # 接收信息
        return threading.Thread(target=self.recvMsg, name='recvMsg')

    def dispatch(self):
        threadInfoDict = {
            'heartbeta': self._hbControl,
            'recvMsg': self._recvMsgControl,
        }
        tPool = ThreadPoolExecutor(max_workers=10)

        # 如果此时event为false即代表server已经成功连上，当event为true时，即开始以下线程
        self.event.wait()
        self._recvMsgControl().start()
        self._hbControl().start()
        self._daemonThread(threadInfoDict).start()
        # 发送在线设备信息
        # dataReport = self.taskPool.submit(self._dataReportControl, 'get_system', )

        time.sleep(2)

        # if dataReport.done():
        #    PrettyCode.prettyPrint('主机信息上传完成。')
        
        

    def main(self):
        threading.Thread(target=self.TCPConnect, name='TCPConnect').start()
        threading.Thread(target=self.dispatch, name='dispatch').start()

def testTask():
    pass

if __name__ == "__main__":
    mole = Client()
    mole.main()
    # mole.performOrder('AC131')