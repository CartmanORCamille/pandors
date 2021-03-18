#!/usr/bin/python
# -*- encoding: utf-8 -*-
'''
@Time    :   2021/03/18 11:19:27
@Author  :   Camille
@Version :   1.0
'''


import hashlib
import os
import base64
import json
from Crypto import Cipher
from Crypto import PublicKey
from Crypto.Cipher import AES, PKCS1_v1_5
from Crypto import Random
from Crypto.PublicKey import RSA


class AntLockFile():
    def __init__(self) -> None:
        self.mode = AES.MODE_EAX
        self.salt = '@Dtx4Q4,)?eUi9z{cc^({J8>3E|[c[Vf[{_ussqm#s%t*aB6B#rbnWcpMZt&+y8)'
        self.integrityChecks()

    def integrityChecks(self):
        publicKeyExists = os.path.exists(r'public.pem')
        if not publicKeyExists:
            raise FileNotFoundError('The public key is missing.')

    @staticmethod
    def readFile(file):
        with open(file, 'rb') as f:
            # 待加密文件
            text = f.read()

        with open(r'public.pem', 'rb') as f:
            RSA_publicKey = f.read()

        return text, RSA_publicKey

    def randomAESKey(self):
        # 随机一个AES对称秘钥
        AES_Key = Random.get_random_bytes(32)
        return AES_Key

    def encryFile(self, file):
        aes_key = self.randomAESKey()
        text, RSA_publicKey = self.readFile(file)
        # 加密数据
        AES_worker = AES.new(aes_key, self.mode)
        encryed_text, tag = AES_worker.encrypt_and_digest(text)

        # 加密AES KEY
        rsaKeyObj = RSA.importKey(RSA_publicKey)
        rsa_worker = PKCS1_v1_5.new(rsaKeyObj)

        e_aes_key = rsa_worker.encrypt(aes_key)
        e_aes_key_base64 = base64.b64encode(e_aes_key)

        # 检验密文完整性
        encryed_text_hash = self.integrityCheck(encryed_text)

        return (encryed_text, tag, AES_worker.nonce, ), (e_aes_key_base64, ), (file, encryed_text_hash)

    def integrityCheck(self, text):
        # 完整性校验 md5
        integrityObj = hashlib.md5(self.salt.encode())
        if isinstance(text, bytes):
            integrityObj.update(text)
        else:
            integrityObj.update(text.encode())

        hashInfo = integrityObj.hexdigest()
        return hashInfo

    def changeFileInfo(self, package: dict):
        
        file, fileHash = package.get('file')
        text, tag, nonce = package.get('e_TextInfo')

        # 加密文件
        with open(file, 'wb') as f:
            f.write(nonce)
            f.write(tag)
            f.write(text)

        # 文件添加后缀
        newName = '{}.ant'.format(file)
        os.rename(file, newName)

    def registerFileInfo(self, package: dict):
        
        
        # for value in package.values():
        #     print(value, type(value))
        with open(r'key.json', 'a') as f:
            json.dump(package, f, indent=4)

    def dispatch(self, file):
        e_TextInfo, e_Key, file = self.encryFile(file)

        # 修改函数信息
        package = {
            'e_TextInfo': e_TextInfo,
            'e_Key': e_Key,
            'file': file,
        }
        # 登记函数信息
        registerInfo = {
            'file': file[0],
            'eHash': file[-1],
            'eKey': str(e_Key[0])
        }
        
        self.changeFileInfo(package)
        self.registerFileInfo(registerInfo)
        
if __name__ == "__main__":
    eObj = AntLockFileA()
    eObj.dispatch(r'de.txt')






    