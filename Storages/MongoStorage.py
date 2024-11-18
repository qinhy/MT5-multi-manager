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
mongo_back     = try_if_error(lambda:__import__('pymongo')) is None

if mongo_back:
    from pymongo import MongoClient, database, collection
    
    class SingletonMongoDBStorage:
        _instance = None
        _meta = {}
        
        def __new__(cls, mongo_URL: str = "mongodb://127.0.0.1:27017/", 
                        db_name: str = "SingletonDB", collection_name: str = "store"):            
            same_url = cls._meta.get('mongo_URL',None)==mongo_URL
            same_db = cls._meta.get('db_name',None)==db_name
            same_col = cls._meta.get('collection_name',None)==collection_name

            if not (same_url and same_db and same_col):
                cls._instance = None

            if cls._instance is None:
                cls._instance = super(SingletonMongoDBStorage, cls).__new__(cls)
                cls._instance.uuid = uuid.uuid4()
                client = MongoClient(mongo_URL)
                cls._instance.db = client.get_database(db_name)
                cls._instance.collection = cls._instance.db.get_collection(collection_name)
                cls._instance._meta = dict(mongo_URL=mongo_URL,db_name=db_name,collection_name=collection_name)
            return cls._instance

        def __init__(self, mongo_URL: str = "mongodb://127.0.0.1:27017/", 
                        db_name: str = "SingletonDB", collection_name: str = "store"):
            self.uuid: str = self.uuid
            self.db:database.Database = self.db
            self.collection:collection.Collection = self.collection

    class SingletonMongoDBStorageController(AbstractStorageController):
        
        def __init__(self, model: SingletonMongoDBStorage):
            self.model: SingletonMongoDBStorage = model

        def _ID_KEY(self):return '_id'

        def exists(self, key: str)->bool:
            return self.model.collection.find_one({self._ID_KEY(): key}) is not None

        def set(self, key: str, value: dict):
            self.model.collection.update_one({self._ID_KEY(): key}, {"$set": value}, upsert=True)

        def get(self, key: str)->dict:
            res = self.model.collection.find_one({self._ID_KEY(): key})            
            if res: del res['_id']
            return res

        def delete(self, key: str):
            self.model.collection.delete_one({self._ID_KEY(): key})

        def keys(self, pattern: str = '*')->list[str]:
            regex = '^'+pattern.replace('*', '.*')
            return [doc['_id'] for doc in self.model.collection.find({self._ID_KEY(): {"$regex": regex}})]

SingletonKeyValueStorage.backs['mongodb']=lambda *args,**kwargs:SingletonMongoDBStorageController(SingletonMongoDBStorage(*args,**kwargs)) if mongo_back else None
