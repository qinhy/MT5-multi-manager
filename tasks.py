
from fastapi import FastAPI, HTTPException
from Manager import MT5Manager,MT5Account,MT5Action,Book
class GetBooks(MT5Action):
    def run(acc:MT5Account):
        tbs = {f'{b.symbol}-{b.price_open}-{b.volume}':b.model_dump() for b in Book().getBooks()}
        return tbs

######################################### Celery connect to local rabbitmq and db sqlite backend

from celery import Celery
from celery.app import task as Task
celery_app = Celery('tasks', broker = 'amqp://localhost', backend = 'db+sqlite:///db.sqlite3')     
mt5manager = MT5Manager()
# mt5manager.add_terminal()    
class CeleryTask:
    
    @staticmethod
    @celery_app.task(bind=True)
    def revoke(t:Task, task_id: str):
        """Method to revoke a task."""
        return CeleryTask.celery_app.control.revoke(task_id, terminate=True)

    @staticmethod
    @celery_app.task(bind=True)
    def get_books(t:Task, acc:MT5Account):
        gtb = GetBooks(acc)  
        mt5manager.do(gtb)
        return mt5manager.results[gtb.uuid][0]

    @staticmethod
    @celery_app.task(bind=True)
    def book_send(t:Task,acc:MT5Account):
        pass

    @staticmethod
    @celery_app.task(bind=True)
    def book_close(t:Task,acc:MT5Account):
        pass

    @staticmethod
    @celery_app.task(bind=True)
    def book_changeP(t:Task,acc:MT5Account,p):
        pass

    @staticmethod
    @celery_app.task(bind=True)
    def book_changeTS(t:Task,acc:MT5Account,tp,sl):
        pass

    @staticmethod
    @celery_app.task(bind=True)
    def account_info(t:Task,acc:MT5Account):
        pass


######################################### Create FastAPI app instance

class RESTapi:   
    api = FastAPI()

    @staticmethod
    @api.post("/books/")
    def get_books(acc:MT5Account):
        task = CeleryTask.get_books.delay(acc)
        return {'id':task.id}

    @staticmethod
    @api.get("/books/{name}")
    def book_send(acc:MT5Account):
        task = CeleryTask.book_send.delay(acc)
        return {'id':task.id}
    
    @staticmethod
    @api.get("/books/{name}")
    def book_close(acc:MT5Account):
        task = CeleryTask.book_close.delay(acc)
        return {'id':task.id}
    
    @staticmethod
    @api.get("/books/{name}")
    def book_changeP(acc:MT5Account,p):
        task = CeleryTask.book_changeP.delay(acc,p)
        return {'id':task.id}
    
    @staticmethod
    @api.get("/books/{name}")
    def book_changeTS(acc:MT5Account,tp,sl):
        task = CeleryTask.book_changeTS.delay(acc,tp,sl)
        return {'id':task.id}
    
    @staticmethod
    @api.get("/accounts/")
    def account_info(acc:MT5Account):
        task = CeleryTask.account_info.delay(acc)
        return {'id':task.id}