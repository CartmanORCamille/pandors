#!/usr/bin/python
# -*- encoding: utf-8 -*-
'''
@Time    :   2021/03/16 10:34:57
@Author  :   Camille
@Version :   0.7beta
'''


import ast
import json
from logging import fatal
import socket
import struct
import time
from enum import Enum
from threading import Thread, enumerate
from auxiliaryTools import PrettyCode, ChangeRedis, BasicLogs
from concurrent.futures import ThreadPoolExecutor


class Troy():
    def __init__(self):
        # 初始化
        self.config= None
        self._loadingConfig()
        # 重传超时时间
        self.rto = 3

        # 实例化日志
        logName = 'troy_{}.log'.format(time.strftime('%S_%M_%H_%d_%m_%Y'))
        self.logObj = BasicLogs.handler(logName=logName)
        # log使用方法：self.logObj.logHandler()

        # 实例化redis
        self.redisObj = ChangeRedis(self.config.get('redisConfig'))
        PrettyCode.prettyPrint('redis server 连接成功。')
        self.logObj.logHandler().info('redis server started successfully.')

        # rcc计时器线程
        self.rccTimerWorker = ThreadPoolExecutor(max_workers=100)

    def existsCheck(self, moles):
        """存活主机检测

        Args:
            moles (list): 指定主机列表。

        Returns:
            list: 离线主机列表
        """
        offline = []
        for mole in moles:
            if mole not in self.redisObj.redisPointer().smembers('exists'):
                offline.append(mole)
        return offline

    def udpSendInstructions(self, udpScheduleArgs: object = None) -> None:
        """udp发包主函数

        Args:
            udpScheduleArgs (object, optional): UDP调用模式参数，如果为None则进入控制台模式，如果为object则进入任务模式. Defaults to None.
        """
        
        udpSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        PrettyCode.prettyPrint('UDP服务器启动成功。')
        PrettyCode.prettyPrint('udp服务器进入监听。')
        self.logObj.logHandler().info('UDP server started successfully.')
        while 1:
            if udpScheduleArgs:
                # specified (list, optional): 指定主机列表. Defaults to None.
                # dispatchCommands (dict, optional): 调度命令任务集. Defaults to None.
                # key (str, optional): 任务集名（编号）. Defaults to None.
                specified, dispatchCommands, key = udpScheduleArgs.ipList(), udpScheduleArgs.disCommands(), udpScheduleArgs.key()
                assert isinstance(specified, list), '错误的类型数据，请传入list。'
                assert isinstance(dispatchCommands, dict), '错误的类型数据，请传入dict。'
                assert isinstance(key, str), '错误的类型数据，请传入str。'
            else:
                specified, dispatchCommands, key = 0, 0 ,0
            msg = None
            # 获取所有存活主机信息
            moles = [i for i in self.redisObj.redisPointer().smembers('exists')]
            
            if dispatchCommands and key:
                # 是否为调度模式下发判断
                if self.redisObj.redisPointer().exists(key):
                    # 判断该任务是否已经上传
                    PrettyCode.prettyPrint('此任务已经上传。', 'warning')
                    self.logObj.logHandler().warning('upload repetitive tasks.')
                    break

                # 上传任务
                try:
                    msg = self._udpSendDispatchMode(key, dispatchCommands)
                except Exception as e:
                    errorMsg = '{}{}'.format(e, e.__traceback__.tb_lineno)
                    # 记录logs - 意料之外的错误
                    self.logObj.logHandler().error(errorMsg)
                    continue
            else:
                # CMD控制台模式下发
                try:
                    # 获取用户输入值
                    PrettyCode.prettyPrint('进入CMD指令模式。')
                    self.logObj.logHandler().info('Enter CMD.')
                    msg = self._udpSendCmdMode()
                    if msg == 'exit()':
                        self.logObj.logHandler().info('Exit CMD.')
                        # 退出控制台下发指令，关闭UDP服务器 `exit()`
                        break
                except Exception as e:
                    eMsg = (e.__traceback__.tb_lineno, e)
                    self.logObj.logHandler().error(eMsg)
                    continue
            # 下发信息
            try:
                if not specified:
                    # 下发所有存活客户端
                    if not moles:
                        # 如果没有存活主机
                        PrettyCode.prettyPrint('无存活主机接受信息。', 'warning')
                        self.logObj.logHandler().warning('No online host.')
                        continue
                    PrettyCode.prettyPrint('执行下发所有存活客户端。')
                    self._udpSenderAllSurvive(udpSocket, msg, moles, key)
                else:
                    # 下发指定客户端
                    assert isinstance(specified, list), '错误的类型数据，请传入list。'
                    PrettyCode.prettyPrint('执行下发指定客户端。')
                    self._udpSenderSpecified(udpSocket, msg, specified, key)
                    PrettyCode.prettyPrint('执行下发指定客户端函数结束。退出udp服务器。')
                    break
            except Exception as e:
                PrettyCode.prettyPrint('下发失败 - 意料之外的原因。', 'ERROR')
                print(e, e.__traceback__.tb_lineno)
                continue
        # udp函数关闭
        PrettyCode.prettyPrint('udp下发函数关闭。或exit()生效。')
        self.logObj.logHandler().warning('UDP server close.')

    def heartbeatMonitoring(self):
        # 心跳监测函数 - 功能函数
        tcpSocket = socket.socket()
        localIP = self.config.get('localConfig').get('ip')
        tcpSocket.bind((localIP, 11451))
        tcpSocket.listen(32)
        PrettyCode.prettyPrint('TCP服务器启动成功。')
        PrettyCode.prettyPrint('客户端心跳进入监听。')
        self.logObj.logHandler().info('TCP server started successfully.')

        while 1:
            conn, addr = tcpSocket.accept()
            # 子客户端超时设定
            conn.settimeout(60)
            while 1:
                data = self._tcpRecverInstructions(conn, addr)
                # 信息为空就判断此客户端连接已经断开并删除redis存活列表的对应地址
                if not data:
                    # 当机器离线时的处理
                    msg = '{} - offline.'.format(addr[0])
                    # 离线记录日志
                    self.logObj.logHandler().warning(msg)
                    # redis删除信息
                    self.redisObj.redisPointer().srem('exists', addr[0])

                    needDel = []
                    for i in self.config.get('offlineDelFields').get('system'):
                        # 需要删除的字段名
                        field = '{}_{}'.format(addr[0], i)
                        needDel.append(field)
                    
                    # 删除信息字段
                    for dels in needDel:
                        self.redisObj.redisPointer().delete(dels)
                    PrettyCode.prettyPrint('{} - 断开连接。'.format(addr[0]))
                    break

                """当信息不为空时：
                    1. 打印（记录日志）存活信息
                    2. 查询redis存活列表是否存在此地址，如果存在就跳过，如果不存在（新主机）就添加
                """
                try:
                    data = self.taskStatus(data = data.decode('utf-8'), addr = addr)
                except Exception as e:
                    # 数据异常
                    errorMsg = '{} - {}'.format(e, e.__traceback__.tb_lineno)
                    self.logObj.logHandler().error(errorMsg)
                    warningMsg = '数据异常，主动 {} 切断连接，开始等待其他连接。'.format(addr[0])
                    PrettyCode.prettyPrint(warningMsg)
                    self.logObj.logHandler().warning('Data anomalies, {} actively cut off the connection.'.format(addr[0]))
                    conn.close()
                    break

                # 判断心跳信息 - keepAlive
                if data == 'keepAlive':
                    # redis
                    if not addr[0] in self.redisObj.redisPointer().smembers('exists'):
                        # redis未查询到记录，添加新主机
                        self.redisObj.setAddData('exists', [addr[0]])
                        self.logObj.logHandler().info('Online: {}'.format(addr[0]))
                        PrettyCode.prettyPrint('已上线：{}'.format(addr[0]))
                    PrettyCode.prettyPrint('心跳信息：{} - {}'.format(addr[0], data))
                
                elif data == 'keepGotIt':
                    # 判断任务报文处理 - keepGotIt
                    self.logObj.logHandler().info('{} - Message processing returned successfully.'.format(addr[0]))
                    PrettyCode.prettyPrint('任务报文信息：{} - {}'.format(addr[0], data))
                
                elif data == 'keepRogerthat':
                    # 判断数据提交情况 - keepRogerthat
                    self.logObj.logHandler().info('{} - Data submission returned successfully.'.format(addr[0]))
                    PrettyCode.prettyPrint('数据处理信息：{} - {}'.format(addr[0], data))

                else:
                    # 异常情况 - 非异常数据错误（代码错误）
                    self.logObj.logHandler().error('There are some unexpected situations, the error function: heartbeatMonitoring(self) error report specific location: determine that the heartbeat information has entered the else.')
                    continue
                
        self.logObj.logHandler().warning('TCP server close.')
        PrettyCode.prettyPrint('TCP SERVER 退出。', 'warning')

    def taskStatus(self, data, addr: str) -> str:
        # 任务状态监控
        addr = addr[0]
        recvDataDict = json.loads(data)
        dataCode = recvDataDict.get('flag')

        flagDispatch = {
            'ADH18': self._ADH18,
            'ADH27': self._ADH27,
            'ADH33': self._ADH33,
            'ADH56': self._ADH56,
        }

        try:
            # 执行对应函数
            msg = flagDispatch.get(dataCode)(recvDataDict, (addr, ))
            return msg
        except Exception as e:
            self.logObj.logHandler().error('Unexpected error: ', e, e.__traceback__.tb_lineno)
            raise ValueError('未识别的报文代码。')

    def _udpSendDispatchMode(self, key: str, dispatchCommands: dict) -> str:
        """调度指令，传入命令上传redis，传入后下发key至客户端标记开始

        Args:
            key (str): task ID
            dispatchCommands (dict): task

        Returns:
            str: task ID
        """

        try:
            self.redisObj.redisPointer().zadd(key, dispatchCommands)
            PrettyCode.prettyPrint('任务已成功更新。')
        except Exception as e:
            errorMsg = '{} - {}'.format(e, e.__traceback__.tb_lineno)
            self.logObj.logHandler().error(errorMsg)
        return key

    def _udpSendCmdMode(self):
        # 控制台下发（只能单发）
        order = str(input('\rINPUT: say something...\n'))
        return order

    def _udpSenderAllSurvive(self, udpSocket, msg: str, moles: list, taskId: str):
        """udp发包操作函数（所有存活客户端），只做发送操作。

        Args:
            msg (str): 需要发送的信息。
            moles (list): 需要发送的地址。
            taskId (str): 任务ID。
        """
        for mole in moles:
            udpSocket.sendto(msg.encode('utf-8'), (mole, 6655))
        result = '{} - 下发完成 - 所有存活主机。'.format(mole)
        self.logObj.logHandler().info('{} - Delivery is complete (all online hosts).'.format(mole))

        # 上传应答表
        self._responseForm(taskId, moles)

        PrettyCode.prettyPrint(result)

    def _udpSenderSpecified(self, udpSocket, msg: str, specifyTheMole: list, taskId: str) -> None:
        """udp发包操作函数（指定客户端），只做发送操作。

        Args:
            msg (str): 需要发送的信息。
            specifyTheMole (list): 主机信息。
            taskId (str): 任务ID。
        """
        # 获取离线列表
        offline = self.existsCheck(specifyTheMole)
        
        for mole in specifyTheMole:
            # 检查指定客户端是否存活
            if mole not in offline:
                udpSocket.sendto(msg.encode('utf-8'), (mole, 6655))
                result = '{} - 下发完成 - 指定主机。'.format(mole)
                self.logObj.logHandler().info('{} - Delivery completed (specify host).'.format(mole))

                # 上传应答表
                self._responseForm(taskId, specifyTheMole)

                PrettyCode.prettyPrint(result)
            else:
                result = '{} 下发失败 - 主机离线。'.format(mole)
                self.logObj.logHandler().info('{} - Delivery failed (specify host).'.format(mole))
                PrettyCode.prettyPrint(result, 'ERROR')

    def _responseForm(self, taskId: str, moles: list) -> None:
        # 上传接收命令的主机到redis表
        field = '{}_response'.format(taskId)
        self.redisObj.redisPointer().sadd(field, *moles)
        msg = 'The task {} answer form has been logged'.format(taskId)
        self.logObj.logHandler().info(msg)
        PrettyCode.prettyPrint(msg)

    def _tcpRecverInstructions(self, conn, addr):
        """tcp收包函数

        Args:
            conn (socket object): 客户端socket对象
            addr (tuple): 客户端ip端口信息元组

        Returns:
            byte: 接收的信息
        """
        # 获取报头，内容是传送数据总长度，如果接受到空数据就返回None
        try:
            headerPack = conn.recv(4)
        except:
            return None
        if not headerPack:
            # 无报头，异常信息
            return None
        # 解析传送数据总长度
        dataLength = struct.unpack('i', headerPack)[0]
        # 记录长度
        recvDataLen = 0
        # 接收数据
        recvData = b''

        while recvDataLen < dataLength:
            # 循环取出数据
            data = conn.recv(4096)
            recvDataLen += len(data)
            recvData += data
        return recvData
        
    def _loadingConfig(self):
        """读取配置文件
            用户 -> list
        """
        # 配置文件
        self.config = PrettyCode.loadingConfigJson(r'.\config.json')
    
    def _ADH18(self, recvDataDict: dict, *args, **kwargs) -> str:
        ack = recvDataDict.get('ACK')
        if ack != 'keepAlive':
            # 验证报文信息
            self.logObj.logHandler().error('abnormal data.')
            raise ValueError('异常数据。')
        return ack

    def _ADH27(self, recvDataDict: dict, *args, **kwargs) -> str:
        """任务报文处理

        Args:
            recvDataDict (Dict): 任务信息
            addr (str): 客户端地址

        Raises:
            e: 意料之外的错误

        Returns:
            str: 报文码
        """
        # 获取数据
        self.logObj.logHandler().info('ADH27 function starts processing.')
        addr = args[0][0]
        phase = recvDataDict.get('phase')

        if not recvDataDict.get('working') and recvDataDict.get('complete') and recvDataDict.get('oncall'):
            # 当前客户端没有任务
            taskId, currentTask, completeTask, notCompleteTask = None, None, None, None
            # 可能是异常数据，提交了ADH27编号但没任务信息
            self.logObj.logHandler().warning('It may be abnormal data. ADH27 number is submitted but no task information.')
            
        else:
            # 当前客户端有任务
            try:
                taskId = recvDataDict.get('code')
                currentTask = recvDataDict.get('working')
                completeTask = recvDataDict.get('complete')
                notCompleteTask = recvDataDict.get('oncall')
                
            except Exception as e:
                errorMsg = '{} - {}'.format(e, e.__traceback__.tb_lineno)
                self.logObj.logHandler().error('There was an error assigning the task dictionary to the ADH27 function.')
                self.logObj.logHandler().error(errorMsg)

            if phase == 1:
                # 1阶段初始化未完成任务列表
                listField = '{}_{}_oncall'.format(taskId, addr)
                for task in notCompleteTask:
                    self.redisObj.redisPointer().lpush(listField, task)
                msg = '{} - 任务已经成功接收。'.format(addr)
                PrettyCode.prettyPrint(msg)
            
            else:
                statusFields = []
                # HASH KEY.
                if taskId:
                    for i in ['working', 'complete', 'oncall']:
                        statusFields.append(self._makeFieldsName(taskId, i))

                if phase == 2:
                    """任务类型
                        currentTask(str)
                        completeTask(list)
                        notCompleteTask(list)
                    """
                    # 任务状态检测
                    if currentTask:
                        # 置换正在进行的任务
                        self.redisObj.redisPointer().hset(statusFields[0], addr, currentTask)
                        tips = '当前任务已置换 {}'.format(currentTask)
                        self.logObj.logHandler().info('Successful update: task in progress.')
                        PrettyCode.prettyPrint(tips)

                if phase == 3:
                    if completeTask:
                        # 已完成的任务 hash -> list
                        self.statusOfWork(addr, completeTask, taskId, 'complete', statusFields[1])
                        self.logObj.logHandler().info('Successful update: completed tasks.')

                    if notCompleteTask:
                        # 未完成的任务 hash -> list
                        self.statusOfWork(addr, completeTask, taskId, 'oncall', statusFields[2])
                        self.logObj.logHandler().info('Successful update: unfinished tasks.')

                    if not notCompleteTask and not currentTask:
                        # 所有任务已完成
                        self.statusOfWork(addr, completeTask, taskId, 'oncall', statusFields[2])
                        self.logObj.logHandler().info('All tasks have been completed.')

        return 'keepGotIt'

    def _ADH33(self, recvDataDict: dict, *args, **kwargs) -> bool:
        """应答包处理函数

        Args:
            recvDataDict (dict): 应答数据。

        Returns:
            bool: 应答处理状态
        """
        rcc = recvDataDict.get('rcc')
        taskId = recvDataDict.get('taskId', default=None)
        addr = args[0][0]

        if rcc != 1 or not taskId:
            # 主动异常情况，需要对此设备重新下发指令
            return False
        else:
            # 正确应答，删除名单
            field = '{}_response'.format(taskId)
            self.redisObj.redisPointer().srem(field, addr)
            return True

    def _ADH56(self, info: str, *args, **kwargs) -> None:
        # 系统信息处理
        addr = args[0]
        self.logObj.logHandler().info('ADH56 function starts processing.')
        localInfo = ast.literal_eval(info[-1])
        # 信息入库
        systemInfoHashFields = '{}_{}'.format(addr, 'systemInfo')
        systemInfoIPListFields = '{}_{}'.format(addr, 'systemInfoIPV4')
        for key, value in localInfo.items():
            # redis系统信息字段名
            if isinstance(value, list):
                # IPV4 list
                for ip in value:
                    self.redisObj.redisPointer().lpush(systemInfoIPListFields, ip)
            else:
                # another info
                self.redisObj.redisPointer().sadd(systemInfoHashFields, value)
        PrettyCode.prettyPrint('ADH56任务处理完成。')
        return 'keepRogerthat'

    def _makeFieldsName(self, taskId, status, *args, **kwargs):
        # redis 任务状态字段
        field = '{}_{}'.format(taskId, status)
        return field

    def rccTimer(self, taskId: str):
        # 线程计时器
        hookTime = time.time()
        field = '{}_response'.format
        while 1:
            # 阻塞等待应答包，等待时间内不检查名单
            nowTime = time.time()
            # 检查是否还存在应答名单
            responseTab = self.redisObj.redisPointer().smembers(field)
            rtoCheck = nowTime - hookTime
            if rtoCheck >= self.rto:
                # 超时
                if responseTab:
                # 还存在没有接收到报文的主机 -> 重发
                    self._udpSenderSpecified()
                    break
            
            time.sleep(1)

    def statusOfWork(self, addr: str, tasks: list, taskId: str, statusListField: str, statusHashField: str, *args, **kwargs) -> str:
        """报文任务字段信息处理模块

        Args:
            addr (str): 客户端IP地址
            tasks (list): 任务组
            taskId (str): 任务ID
            statusListField (str): 状态（list字段）
            statusHashField (str): 状态（hash字段）
            complete (bool, optional): 完成/未完成任务模式. Defaults to True.

        Returns:
            str: 格式化后的list名字
        """
        # list: ip_status: task task task
        listField = '{}_{}_{}'.format(taskId, addr, statusListField)
        
        # 获取当前调用状态的储存的任务信息 complete/oncall
        allTasks = self.redisObj.redisPointer().lrange(listField, 0, -1)
        # 最后一个任务结束判断，redis oncall只剩最后一个，发来的报文 oncall 内已经全部执行完了
        if len(allTasks) == 1 and not tasks:
            self.redisObj.redisPointer().lrem(listField, 1, tasks[0])

        # 获取当前存入的新任务信息
        
        if statusListField == 'complete':
            for nTask in tasks:
                '''目前传入的都是completeTask，即当前已完成的任务
                    complete:
                        nTask -> 已完成的任务
                        如果当前任务在当前获取的allTasks中，即pss；如果不在则添加
                    oncall:
                        nTask -> 已完成的任务
                        如果当前任务在获取的allTasks中，即删除；
                '''
                if nTask not in allTasks:
                    # redis已完成任务列表中没有此次传递完成任务列表的该项任务
                    getattr(self, '_statusOfWorkCompleted')(listField, nTask)
                    tips = '{} 已添加完成列表。'.format(nTask)
                    self.logObj.logHandler().info('ADDED: {}'.format(nTask))
                    PrettyCode.prettyPrint(tips)

        elif statusListField == 'oncall':
            for nTask in tasks:
                if nTask in allTasks:
                    # 此次传递已完成任务列表的该项任务(nTask)在redis未完成任务列表中（该单项任务已经完成），删除notcomplete列表元素
                    getattr(self, '_statusOfWorkNotCompleted')(listField, nTask)
                    tips = '{} 已删除未完成列表。'.format(nTask)
                    self.logObj.logHandler().info('DELETED: {}'.format(nTask))
                    PrettyCode.prettyPrint(tips)

            if sorted(tasks) == sorted(allTasks):
                # 任务全部完成判断 -> 未完成任务列表为空，已完成任务列表等于所有任务列表
                # 删除complete列表和hash保存项
                completeListField = '{}_{}_{}'.format(taskId, addr, 'complete')
                self.redisObj.redisPointer().hdel('{}_complete'.format(taskId), addr)
                self.redisObj.redisPointer().delete(completeListField)

                # 删除working hash
                self.redisObj.redisPointer().hdel('{}_working'.format(taskId), addr)

                msg = '{} - All tasks have been completed'.format(addr)
                PrettyCode.prettyPrint(msg)
                self.logObj.logHandler().info(msg)
                return 1
        
        # 为hash添加列表
        if not self.redisObj.redisPointer().exists(statusHashField):
            self.redisObj.redisPointer().hset(statusHashField, addr, listField)
            
        return listField

    def _statusOfWorkCompleted(self, listField, nTask):
        # 完成任务：旧任务表里没有，新任务表有，就添加（已完成）
        self.redisObj.redisPointer().rpush(listField, nTask)

    def _statusOfWorkNotCompleted(self, listField, nTask):
        # 未完成任务：旧任务表里有，新任务表没有，就删除（已完成）
        self.redisObj.redisPointer().lrem(listField, 1, nTask)

    def funcDaemon(self, threadSchedule: dict) -> None:
        """守护进程逻辑

        Args:
            threadSchedule (dict): 需要持续监控的线程字典。
        """
        while 1:
            for tName, tFunc in threadSchedule.items():
                if tName not in str(enumerate()):
                    # 监测离线
                    self.logObj.logHandler().warning('Process terminated: {}.'.format(tName))
                    tFunc.start()
                    msg = '{} 进程已重启。'.format(tName)
                    self.logObj.logHandler().warning('Process restarted: {}'.format(tName))
                    PrettyCode.prettyPrint(msg)
            time.sleep(10)

    def _daemonControl(self) -> Thread:
        # 守护进程
        threadInfo = {
            'heartbeatThread': self._tcpControl()
        }
        daemonControl = Thread(target=self.funcDaemon, name='daemon', args=(threadInfo, ))
        daemonControl.setDaemon(True)
        return daemonControl

    def _rccControl(self) -> Thread:
        rccThread = Thread(target=self.rccTimer, name='rccTimer')
        return rccThread

    def _tcpControl(self) -> Thread:
        # TCP线程控制
        tcpThread = Thread(target=self.heartbeatMonitoring, name='heartbeatThread')
        return tcpThread

    def udpControl(self, udpScheduleArgs=None) -> Thread:
        # UDP线程控制
        udpThread = Thread(target=self.udpSendInstructions, name='udpSender', args=(udpScheduleArgs, ))
        return udpThread

    def main(self, udpCmd=False) -> None:
        """主函数
            启动静默函数：TCP
        """
        self._tcpControl().start()
        self.logObj.logHandler().info('Process started: {}'.format('TCP Control.'))
        self._daemonControl().start()
        self.logObj.logHandler().info('Process started: {}'.format('Daemon Control.'))
        if udpCmd:
            self.udpControl().start()
            self.logObj.logHandler().info('Process started: {}'.format('UDP Control.'))


class UdpInfo():
    """UDP参数传入继承类
    """

    @staticmethod
    def ipList():
        # return list
        raise NotImplementedError('`ipList()` must be implemented.')

    @staticmethod
    def disCommands():
        # return dict
        # key: task, value: level
        raise NotImplementedError('`disCommands()` must be implemented.')

    @staticmethod
    def key():
        # return str
        raise NotImplementedError('`key()` must be implemented.')


if __name__ == "__main__":
    troy = Troy()
    troy.main(udpCmd=1)