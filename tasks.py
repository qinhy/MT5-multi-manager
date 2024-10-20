
import os
import random
import time
from fastapi import FastAPI
from pymongo import MongoClient
from Manager import BookService, MT5CopyLastRatesService,MT5Manager,MT5Account,Book

######################################### Celery connect to local rabbitmq and db sqlite backend
os.environ.setdefault('CELERY_TASK_SERIALIZER', 'json')

from celery import Celery
from celery.result import AsyncResult
from celery.app import task as Task
mongo_URL = 'mongodb://localhost:27017'
celery_app = Celery('tasks', broker = 'amqp://localhost', backend = mongo_URL+'/tasks')
# celery_app = Celery('tasks', broker = 'redis://localhost:6379/0', backend = 'redis://localhost:6379/0')

class CeleryTask:
    api = FastAPI()
    
    @staticmethod
    @celery_app.task(bind=True)
    def revoke(t:Task, task_id: str):
        """Method to revoke a task."""
        return celery_app.control.revoke(task_id, terminate=True)
    @staticmethod
    @api.get("/tasks/stop/{task_id}")
    def api_stop_task(task_id: str):
        task = CeleryTask.revoke.delay(task_id)
        return {'task_id': task.id}


    @staticmethod
    @celery_app.task(bind=True)
    def add_terminal(t:Task, broker: str, path: str):
        return MT5Manager().get_singleton().add_terminal(broker,path)    
    @staticmethod
    @api.post("/terminals/add")
    def api_add_terminal(broker: str, path: str):
        """Endpoint to add a terminal to MT5."""
        task = CeleryTask.add_terminal.delay(broker, path)
        return {'task_id': task.id}
    

    
    @staticmethod
    @celery_app.task(bind=True)
    def get_terminal(t:Task):
        print(MT5Manager().get_singleton().terminals)
        return { k:[i.exe_path for i in v] for k,v in MT5Manager().get_singleton().terminals.items()}
    @staticmethod
    @api.get("/terminals/")
    def api_get_terminal():
        task = CeleryTask.get_terminal.delay()
        return {'task_id': task.id}
    
    @staticmethod
    @celery_app.task(bind=True)
    def book_action(t: Task, acc: MT5Account, book: Book, action: str,**kwargs):
        """
        Generic task for performing book-related actions.
        
        :param acc: MT5Account object
        :param book: Book object
        :param action: The action to perform (e.g., 'getBooks', 'send', 'close', 'changeP', 'changeTS')
        :param kwargs: Additional parameters for the action (e.g., price, tp, sl)
        :return: The result of the action
        """

        model = BookService.Model.build(acc,book,action in ['send'])
        ba = BookService.Action(model)
        res = ba.change_run(action, kwargs)()
        
        if action == 'getBooks':
            return {f'{b.symbol}-{b.price_open}-{b.volume}-{b.ticket}': b.model_dump() for b in res.ret.books}
        elif action in ['send', 'changeP', 'changeTS', 'account_info']:
            return res.ret.books[0].model_dump()
        else:
            return res.model_dump()
        
    @staticmethod
    @api.get("/accounts/info")
    def api_account_info(acc: MT5Account):
        """Endpoint to fetch account information."""
        task = CeleryTask.book_action.delay(acc.model_dump(), Book().model_dump(), action='account_info')
        return {'task_id': task.id}

    @staticmethod
    @api.get("/books/")
    def api_get_books(acc: MT5Account):
        """Endpoint to get books for a given MT5 account."""
        task = CeleryTask.book_action.delay(acc.model_dump(), Book().model_dump(), action='getBooks')
        return {'task_id': task.id}


    @staticmethod
    @api.post("/books/send")
    def api_book_send(acc: MT5Account, book: Book):
        """Endpoint to send a book."""
        task = CeleryTask.book_action.delay(acc.model_dump(), book.model_dump(), action='send')
        return {'task_id': task.id}


    @staticmethod
    @api.post("/books/close")
    def api_book_close(acc: MT5Account, book: Book):
        """Endpoint to close a book."""
        task = CeleryTask.book_action.delay(acc.model_dump(), book.model_dump(), action='close')
        return {'task_id': task.id}


    @staticmethod
    @api.post("/books/change/price")
    def api_book_change_price(acc: MT5Account, book: Book, p: float):
        """Endpoint to change the price of a book."""
        task = CeleryTask.book_action.delay(acc.model_dump(), book.model_dump(), action='changeP', p=p)
        return {'task_id': task.id}


    @staticmethod
    @api.post("/books/change/tpsl")
    def api_book_change_tp_sl(acc: MT5Account, book: Book, tp: float, sl: float):
        """Endpoint to change tp sl values of a book."""
        task = CeleryTask.book_action.delay(acc.model_dump(), book.model_dump(), action='changeTS', tp=tp, sl=sl)
        return {'task_id': task.id}

    
    @staticmethod
    @celery_app.task(bind=True)
    def rates_copy(t:Task,acc:MT5Account,
                   symbol:str,timeframe:str,count:int,debug:bool=False):
        model = MT5CopyLastRatesService.Model.build(acc)
        act = MT5CopyLastRatesService.Action(model)
        res = act(symbol=symbol,timeframe=timeframe,count=count,debug=debug)
        return res.ret.model_dump()
    
    @staticmethod
    @api.get("/rates/")
    def api_rates_copy(acc: MT5Account, symbol: str, timeframe: str, count: int, debug: bool = False):
        """
        Endpoint to copy rates for a given MT5 account, symbol, timeframe, and count.
        
        :param acc: MT5Account object
        :param symbol: The symbol for which to copy rates
        :param timeframe: The timeframe for which to copy rates
        :param count: The number of rates to copy
        :param debug: Optional flag to enable debugging
        :return: Task ID of the Celery task
        """
        task = CeleryTask.rates_copy.delay(acc.model_dump(), symbol, timeframe, count, debug)
        return {'task_id': task.id}

        
    @staticmethod
    @api.get("/tasks/status/{task_id}")
    def api_task_status(task_id: str):
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
    @api.get("/books/")
    async def get_books(acc: MT5Account):
        """Mock Endpoint to get books for a given MT5 account."""
        return {'task_id': 'mock_task_id'}

    @staticmethod
    @api.get("/accounts/info")
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
    @api.post("/books/change/price")
    async def book_change_price(acc: MT5Account, book: Book, p: float):
        """Mock Endpoint to change the price of a book."""
        return {'task_id': 'mock_task_id'}

    @staticmethod
    @api.post("/books/change/tpsl")
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
