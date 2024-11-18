# from https://github.com/qinhy/singleton-key-value-storage.git
import base64
import hashlib
import math
import os
import re
import sqlite3
import threading
import queue
import time
import uuid
import fnmatch
import json
import unittest
import urllib
import urllib.parse
from urllib.parse import urlparse

try:
    from .Storage import SingletonKeyValueStorage,AbstractStorageController
except Exception as e:
    from Storage import SingletonKeyValueStorage,AbstractStorageController


def try_if_error(func):
    try:
        func()
    except Exception as e:
        print(e)
        return e

# self checking
redis_back     = try_if_error(lambda:__import__('redis')) is None

if redis_back:
    import redis
    class SingletonRedisStorage:
        _instance = None
        _meta = {}

        def __new__(cls, redis_URL=None):# redis://127.0.0.1:6379
            if cls._instance is not None and cls._meta.get('redis_URL',None)==redis_URL:
                return cls._instance
            
            if redis_URL is None: raise ValueError('redis_URL must not be None at first time (redis://127.0.0.1:6379)')
            
            if cls._instance is not None and cls._meta.get('redis_URL',None)!=redis_URL:
                cls._instance.client.close()
                print(f'warnning: instance changed to url {redis_URL}')

            url:urllib.parse.ParseResult = urlparse(redis_URL)
            cls._instance = super(SingletonRedisStorage, cls).__new__(cls)                        
            cls._instance.uuid = uuid.uuid4()
            cls._instance.client = redis.Redis(host=url.hostname, port=url.port, db=0, decode_responses=True)
            cls._meta['redis_URL'] = redis_URL

            return cls._instance

        def __init__(self, redis_URL=None):#redis://127.0.0.1:6379
            self.uuid:str = self.uuid
            self.client:redis.Redis = self.client

    class SingletonRedisStorageController(AbstractStorageController):
        def __init__(self, model: SingletonRedisStorage):
            self.model:SingletonRedisStorage = model

        def exists(self, key: str)->bool:
            return self.model.client.exists(key)

        def set(self, key: str, value: dict):
            self.model.client.set(key, json.dumps(value))

        def get(self, key: str)->dict:
            res = self.model.client.get(key)
            if res: res = json.loads(res)
            return res

        def delete(self, key: str):
            self.model.client.delete(key)

        def keys(self, pattern: str='*')->list[str]:            
            try:
                res = self.model.client.keys(pattern)
            except Exception as e:
                res = []
            return res

SingletonKeyValueStorage.backs['redis']=lambda *args,**kwargs:SingletonRedisStorageController(SingletonRedisStorage(*args,**kwargs)) if redis_back else None

