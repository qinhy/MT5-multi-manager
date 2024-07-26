## need RabbitMQ sqlite for celery

## to run
- celery -A tasks worker -l info --concurrency=2 -P threads
- celery -A tasks flower --url_prefix=flower --port=50002
- uvicorn tasks:api