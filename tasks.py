
from celery import Celery
from time import sleep

from fastapi import FastAPI
from celery.result import AsyncResult

######################################### Celery connect to local rabbitmq and db sqlite backend
app = Celery('tasks', broker = 'amqp://localhost', backend = 'db+sqlite:///db.sqlite3')

@app.task #register task into celery app via decorator
def get_hello(name):
    sleep(10) #simulation for long running task
    retval = f"Hello {name}"
    return retval

@app.task
def book_send(self):
    pass

@app.task
def book_close(self):
    pass

@app.task
def book_changeP(self,p):
    pass

@app.task
def book_changeTS(self,tp,sl):
    pass

@app.task
def getBooks(self):
    pass

@app.task
def account_info(self):
    pass


######################################### Create FastAPI app instance
api = FastAPI()

@api.get("/books/{name}")
def book_send(self):
    pass

@api.get("/books/{name}")
def book_close(self):
    pass

@api.get("/books/{name}")
def book_changeP(self,p):
    pass

@api.get("/books/{name}")
def book_changeTS(self,tp,sl):
    pass

@api.get("/books/")
def getBooks(self):
    pass

@api.get("/accounts/")
def account_info(self):
    pass

@api.get("/accounts/{id}")
def account_info(self):
    pass
