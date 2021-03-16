#!/usr/bin/python
# -*- encoding: utf-8 -*-
'''
@Time    :   2021/03/16 10:34:57
@Author  :   Camille
@Version :   0.7beta
'''


import ast
import socket
import struct
import time
from threading import Thread, enumerate
from auxiliaryTools import PrettyCode, ChangeRedis, BasicLogs


class Troy():
    def __init__(self):
        # 初始化
        self.config, self.instruction= None, None
        self._loadingConfig()

        # 实例化日志
        logName = 'troy_{}.log'.format(time.strftime('%S_%M_%H_%d_%m_%Y'))
        self.logObj = BasicLogs.handler(logName=logName)
        # log使用方法：self.logObj.logHandler()

        # 实例化redis
        self.redisObj = ChangeRedis(self.config.get('redisConfig'))
        PrettyCode.prettyPrint('redis server 连接成功。')
        self.logObj.logHandler().info('redis server started successfully.')

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
                    self._udpSenderAllSurvive(udpSocket, msg, moles)
                else:
                    # 下发指定客户端
                    assert isinstance(specified, list), '错误的类型数据，请传入list。'
                    PrettyCode.prettyPrint('执行下发指定客户端。')
                    self._udpSenderSpecified(udpSocket, msg, specified)
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
                    print(e, e.__traceback__.tb_lineno)
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
                    self.logObj.logHandler().error('There are some unexpected situations, the error function: heartbeatMonitoring(self) error report specific location: determine that the heartbeat information has entered the else')
                    continue
                
        self.logObj.logHandler().warning('TCP server close.')
        PrettyCode.prettyPrint('TCP SERVER 退出。', 'warning')
    
    def taskStatus(self, data: str, addr: str) -> str:
        # 任务状态监控
        addr = addr[0]
        recvDataList = data.split('/')
        standardField = {'keepAlive', 'keepRogerThat', 'keepGotIt'}
        if not standardField & set(recvDataList):
            self.logObj.logHandler().error('abnormal data.')
            raise ValueError('异常数据。')
        # 获取报文代码
        dataCode = recvDataList.pop(0)
        
        try:
            if dataCode == 'ADH56':
                # 数据汇报信息
                msg = self._ADH56(recvDataList, addr)
                return msg
            elif dataCode.startswith('ADH27'):
                # 任务状态处理
                phase = dataCode.split('7')[-1]
                msg = self._ADH27(recvDataList, addr, phase)
                return msg
            elif dataCode == 'ADH18':
                return 'keepAlive'
            else:
                raise ValueError('未识别的报文代码。')
        except Exception as e:
            self.logObj.logHandler().error('Unexpected error: ', e, e.__traceback__.tb_lineno)
            raise ValueError('异常数据。')

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
            print(e, e.__traceback__.tb_lineno)
        return key

    def _udpSendCmdMode(self):
        # 控制台下发（只能单发）
        msg = None
        while 1:
            order = str(input('\rINPUT: say something...\n'))
            # 获取指令字典信息
            msg = self.instruction.get(order, 0)
            if not msg:
                PrettyCode.prettyPrint('此命令不存在。', 'ERROR')
                continue
            break
        return msg

    def _udpSenderAllSurvive(self, udpSocket, msg: str, moles: list):
        """udp发包操作函数（所有存活客户端），只做发送操作。

        Args:
            msg (str): 需要发送的信息。
            moles (list): 需要发送的地址。
        """
        for mole in moles:
            udpSocket.sendto(msg.encode('utf-8'), (mole, 6655))
        result = '{} - 下发完成 - 所有存活主机。'.format(mole)
        self.logObj.logHandler().info('{} - Delivery is complete (all online hosts).'.format(mole))
        PrettyCode.prettyPrint(result)

    def _udpSenderSpecified(self, udpSocket, msg: str, specifyTheMole: list) -> None:
        """udp发包操作函数（指定客户端），只做发送操作。

        Args:
            msg (str): 需要发送的信息。
        """
        # 获取离线列表
        offline = self.existsCheck(specifyTheMole)
        
        for mole in specifyTheMole:
            # 检查指定客户端是否存活
            if mole not in offline:
                udpSocket.sendto(msg.encode('utf-8'), (mole, 6655))
                result = '{} - 下发完成 - 指定主机。'.format(mole)
                self.logObj.logHandler().info('{} - Delivery completed (specify host).'.format(mole))
                PrettyCode.prettyPrint(result)
            else:
                result = '{} 下发失败 - 主机离线。'.format(mole)
                self.logObj.logHandler().info('{} - Delivery failed (specify host).'.format(mole))
                PrettyCode.prettyPrint(result, 'ERROR')

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
            指令 -> dict
            用户 -> list
        """
        # 配置文件
        self.config = PrettyCode.loadingConfigJson(r'.\config.json')
        # 控制台指令字典
        self.instruction = PrettyCode.loadingConfigJson(r'.\instruction.json')
        
    def _ADH56(self, info: str, addr):
        # 系统信息处理
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

    def _ADH27(self, recvDataList: list, addr: str, phase: str) -> str:
        """任务报文处理

        Args:
            recvDataList (list): 任务信息
            addr (str): 客户端地址
            phase (str): 传输阶段

        Raises:
            e: 意料之外的错误

        Returns:
            str: 报文码
        """
        # 获取数据
        self.logObj.logHandler().info('ADH27 function starts processing.')
        # 数据结构 - keepAlive/任务编号/目前正在进行的任务/已完成的任务/未完成的任务
        if len(recvDataList) == 1:
            # 当前客户端没有任务
            taskId, currentTask, completeTask, notCompleteTask = None, None, None, None
            # 可能是异常数据，提交了ADH27编号但没任务信息
            self.logObj.logHandler().warning('It may be abnormal data. ADH27 number is submitted but no task information.')
        else:
            # 当前客户端有任务
            try:
                msgCode, taskId, currentTask, completeTask, notCompleteTask = [i for i in recvDataList]
                print(msgCode, taskId, type(currentTask), completeTask, notCompleteTask)
                if completeTask or notCompleteTask:
                    if currentTask == 'None':
                        currentTask = ast.literal_eval(currentTask)
                    completeTask = ast.literal_eval(completeTask)
                    notCompleteTask = ast.literal_eval(notCompleteTask)
                    
            except Exception as e:
                print(e, e.__traceback__.tb_lineno)
                raise e

            if phase == 'I':
                # I阶段初始化未完成任务列表
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

                if phase == 'II':
                    # 任务状态检测
                    if currentTask:
                        # 置换正在进行的任务
                        self.redisObj.redisPointer().hset(statusFields[0], addr, currentTask)
                        tips = '当前任务已置换 {}'.format(currentTask)
                        self.logObj.logHandler().info('Successful update: task in progress.')
                        PrettyCode.prettyPrint(tips)

                if phase == 'III':
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
                        print('aaa')
                        self.statusOfWork(addr, completeTask, taskId, 'oncall', statusFields[2])
                        self.logObj.logHandler().info('All tasks have been completed.')


        return msgCode
            
    def _makeFieldsName(self, taskId, status, *args, **kwargs):
        # redis 任务状态字段
        field = '{}_{}'.format(taskId, status)
        return field

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
        for nTask in tasks:
            '''
                complete:
                    nTask -> 已完成的任务
                    如果当前任务在当前获取的allTasks中，即pss；如果不在则添加
                oncall:
                    nTask -> 已完成的任务
                    如果当前任务在获取的allTasks中，即删除；
            '''
            if statusListField == 'complete':
                if nTask not in allTasks:
                    # redis已完成任务列表中没有此次传递完成任务列表的该项任务
                    getattr(self, '_statusOfWorkCompleted')(listField, nTask)
                    tips = '{} 已添加完成列表。'.format(nTask)
                    self.logObj.logHandler().info('ADDED: {}'.format(nTask))
                    PrettyCode.prettyPrint(tips)
            elif statusListField == 'oncall':
                if nTask in allTasks:
                    print(tasks)
                    # 此次传递已完成任务列表的该项任务在redis未完成任务列表中（该任务已经完成）
                    getattr(self, '_statusOfWorkNotCompleted')(listField, nTask)
                    tips = '{} 已删除未完成列表。'.format(nTask)
                    self.logObj.logHandler().info('DELETED: {}'.format(nTask))
                    PrettyCode.prettyPrint(tips)
        
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
    # task = UdpInfo()
    # UdpInfo.ipList()
    
    # PrettyCode.prettyPrint('hi')