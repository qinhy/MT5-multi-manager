from celery import Celery
from time import sleep

#connect to local rabbitmq and db sqlite backend
app = Celery('tasks', broker = 'amqp://localhost', backend = 'db+sqlite:///db.sqlite3')

@app.task #register task into celery app via decorator
def get_hello(name):
    sleep(10) #simulation for long running task
    retval = f"Hello {name}"
    return retval