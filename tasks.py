
import os
import random
import time
from fastapi import FastAPI
from pymongo import MongoClient
from Manager import BookAction, MT5CopyLastRatesAction,MT5Manager,MT5Account,Book, MT5Rates

######################################### Celery connect to local rabbitmq and db sqlite backend
os.environ.setdefault('CELERY_TASK_SERIALIZER', 'json')

from celery import Celery
from celery.result import AsyncResult
from celery.app import task as Task
mongo_URL = 'mongodb://localhost:27017'
# celery_app = Celery('tasks', broker = 'amqp://localhost', backend = mongo_URL+'/tasks')
celery_app = Celery('tasks', broker = 'redis://localhost:6379/0', backend = 'redis://localhost:6379/0')

# mt5manager = MT5Manager().get_singleton()
# MT5Manager().get_singleton().add_terminal()    
class CeleryTask:
    
    @staticmethod
    @celery_app.task(bind=True)
    def revoke(t:Task, task_id: str):
        """Method to revoke a task."""
        return CeleryTask.celery_app.control.revoke(task_id, terminate=True)

    @staticmethod
    @celery_app.task(bind=True)
    def add_terminal(t:Task, broker: str, path: str):
        return MT5Manager().get_singleton().add_terminal(broker,path)
    
    # @staticmethod
    # @celery_app.task(bind=True)
    # def book_action(t:Task,action_name:str,acc:MT5Account,book:Book,tp,sl):
    #     ba = BookAction(acc,Book()).change_run(action_name,{})
    #     return MT5Manager().get_singleton().do(ba)
    
    @staticmethod
    @celery_app.task(bind=True)
    def account_info(t:Task,acc:MT5Account):
        ba = BookAction(acc,Book()).change_run('account_info',{})
        return MT5Manager().get_singleton().do(ba)

    @staticmethod
    @celery_app.task(bind=True)
    def get_books(t:Task, acc:MT5Account):
        ba = BookAction(acc, Book()).change_run('getBooks',{})
        res:list[Book] = MT5Manager().get_singleton().do(ba)
        tbs = {f'{b.symbol}-{b.price_open}-{b.volume}':b.model_dump() for b in res}
        return tbs

    @staticmethod
    @celery_app.task(bind=True)
    def book_send(t:Task,acc:MT5Account,book:Book):
        book = Book(**book)
        ba = BookAction(acc,book.as_plan()).change_run('send',{})
        res = MT5Manager().get_singleton().do(ba)
        return book.model_dump()

    @staticmethod
    @celery_app.task(bind=True)
    def book_close(t:Task,acc:MT5Account,book:Book):
        ba = BookAction(acc,book).change_run('close',{})
        res = MT5Manager().get_singleton().do(ba)
        if hasattr(res,'model_dump'):
            res = res.model_dump()
        return res

    @staticmethod
    @celery_app.task(bind=True)
    def book_changeP(t:Task,acc:MT5Account,book:Book,p):
        book = Book(**book)
        ba = BookAction(acc,book).change_run('close',dict(p=p))
        res = MT5Manager().get_singleton().do(ba)
        return book.model_dump()

    @staticmethod
    @celery_app.task(bind=True)
    def book_changeTS(t:Task,acc:MT5Account,book:Book,tp,sl):
        book = Book(**book)
        ba = BookAction(acc,book).change_run('changeTS',dict(tp=tp,sl=sl))
        res = MT5Manager().get_singleton().do(ba)
        return book.model_dump()
    
    @staticmethod
    @celery_app.task(bind=True)
    def rates_copy(t:Task,acc:MT5Account,
                   symbol:str,timeframe:str,count:int,debug:bool=False):
        ba = MT5CopyLastRatesAction(acc)
        rates:MT5Rates = MT5Manager().get_singleton().do(ba,
                    symbol=symbol,timeframe=timeframe,count=count,debug=debug)
        return rates.model_dump()


######################################### Create FastAPI app instance

class RESTapi:
    api = FastAPI()

    @staticmethod
    @api.post("/terminals/add")
    def add_terminal(broker: str, path: str):
        """Endpoint to add a terminal to MT5."""
        task = CeleryTask.add_terminal.delay(broker, path)
        return {'task_id': task.id}
    
    @staticmethod
    @api.get("/terminals/")
    def add_terminal():
        return { k:[i.exe_path for i in v] for k,v in MT5Manager().get_singleton().terminals.items()}        

    @staticmethod
    @api.post("/books/")
    def get_books(acc: MT5Account):
        """Endpoint to get books for a given MT5 account."""
        task = CeleryTask.get_books.delay(acc.model_dump())
        return {'task_id': task.id}

    @staticmethod
    @api.post("/account/info")
    def account_info(acc: MT5Account):
        """Endpoint to fetch account information."""
        task = CeleryTask.account_info.delay(acc.model_dump())
        return {'task_id': task.id}

    @staticmethod
    @api.post("/books/send")
    def book_send(acc: MT5Account, book: Book):
        """Endpoint to send a book."""
        task = CeleryTask.book_send.delay(acc.model_dump(), book.model_dump())
        return {'task_id': task.id}

    @staticmethod
    @api.post("/books/close")
    def book_close(acc: MT5Account, book: Book):
        """Endpoint to close a book."""
        task = CeleryTask.book_close.delay(acc.model_dump(), book.model_dump())
        return {'task_id': task.id}

    @staticmethod
    @api.post("/books/change-price")
    def book_change_price(acc: MT5Account, book: Book, p: float):
        """Endpoint to change the price of a book."""
        task = CeleryTask.book_changeP.delay(acc.model_dump(), book.model_dump(), p)
        return {'task_id': task.id}

    @staticmethod
    @api.post("/books/change-tp-sl")
    def book_change_tp_sl(acc: MT5Account, book: Book, tp: float, sl: float):
        """Endpoint to change tp sl values of a book."""
        task = CeleryTask.book_changeTS.delay(acc.model_dump(), book.model_dump(), tp, sl)
        return {'task_id': task.id}

    @staticmethod
    @api.get("/tasks/status/{task_id}")
    def task_status(task_id: str):
        """Endpoint to check the status of a task."""
        client = MongoClient(mongo_URL)
        db = client.get_database('tasks')
        collection = db.get_collection('celery_taskmeta')
        res = collection.find_one({'_id': task_id})
        if res: del res['_id']
        return res
        # return {"task_id": task.id, "status": task.status, "result": task.result}


class MockRESTapi:
    api = FastAPI()

    @staticmethod
    @api.post("/terminals/add")
    async def add_terminal(broker: str, path: str):
        """Mock Endpoint to add a terminal to MT5."""
        return {'task_id': 'mock_task_id'}

    @staticmethod
    @api.get("/terminals/")
    async def get_terminals():
        """Mock Endpoint to get list of terminals."""
        terminals_mock = {
            "broker1": ["path1.exe", "path2.exe"],
            "broker2": ["path3.exe"]
        }
        return terminals_mock

    @staticmethod
    @api.post("/books/")
    async def get_books(acc: MT5Account):
        """Mock Endpoint to get books for a given MT5 account."""
        return {'task_id': 'mock_task_id'}

    @staticmethod
    @api.post("/account/info")
    async def account_info(acc: MT5Account):
        """Mock Endpoint to fetch account information."""
        return {'task_id': 'mock_task_id'}

    @staticmethod
    @api.post("/books/send")
    async def book_send(acc: MT5Account, book: Book):
        """Mock Endpoint to send a book."""
        return {'task_id': 'mock_task_id'}

    @staticmethod
    @api.post("/books/close")
    async def book_close(acc: MT5Account, book: Book):
        """Mock Endpoint to close a book."""
        return {'task_id': 'mock_task_id'}

    @staticmethod
    @api.post("/books/change-price")
    async def book_change_price(acc: MT5Account, book: Book, p: float):
        """Mock Endpoint to change the price of a book."""
        return {'task_id': 'mock_task_id'}

    @staticmethod
    @api.post("/books/change-tp-sl")
    async def book_change_tp_sl(acc: MT5Account, book: Book, tp: float, sl: float):
        """Mock Endpoint to change TP/SL values of a book."""
        return {'task_id': 'mock_task_id'}

    @staticmethod
    @api.get("/tasks/status/{task_id}")
    async def task_status(task_id: str):
        """Mock Endpoint to check the status of a task."""
        # Simulate a database response for task status
        states = ['PENDING','STARTED','SUCCESS','FAILURE','RETRY','REVOKED']
        status = random.choice(states)
        task_status_mock = {
            "task_id": task_id,
            "status": status,
            "result": {"message": f"Task is {status.lower()}"}
        }
        time.sleep(1)
        return task_status_mock
