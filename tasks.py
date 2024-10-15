import os
from fastapi import FastAPI
from pymongo import MongoClient
from celery import Celery, Task

# Set environment variable for Celery task serialization
os.environ.setdefault('CELERY_TASK_SERIALIZER', 'json')

# MongoDB and Celery configurations
MONGO_URL = 'mongodb://localhost:27017'
MONGO_DB = 'tasks'
CELERY_META_COLLECTION = 'celery_taskmeta'
CELERY_BROKER_URL = 'amqp://localhost'

# Initialize Celery app with broker and backend
celery_app = Celery('tasks', broker=CELERY_BROKER_URL, backend=f'{MONGO_URL}/{MONGO_DB}')

class CeleryTask:
    @staticmethod
    @celery_app.task(bind=True)
    def revoke(self, task_id: str):
        """Revokes a Celery task."""
        return self.app.control.revoke(task_id, terminate=True)

class RESTapi:
    # Initialize FastAPI app
    app = FastAPI()

    @app.get("/tasks/stop/{task_id}")
    def task_stop(task_id: str):
        """Endpoint to stop a Celery task by task_id."""
        task = CeleryTask.revoke.delay(task_id=task_id)
        return {'task_id': task.id}

    @app.get("/tasks/status/{task_id}")
    def task_status(task_id: str):
        """Endpoint to check the status of a Celery task by task_id."""
        client = MongoClient(MONGO_URL)
        db = client[MONGO_DB]
        collection = db[CELERY_META_COLLECTION]
        task_info = collection.find_one({'_id': task_id})
        
        if task_info:
            del task_info['_id']  # Remove internal MongoDB ID for clarity
        return task_info
