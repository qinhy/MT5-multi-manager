import os
import requests
from User.UserModel import Model4User, UsersStore

os.environ.setdefault('CELERY_TASK_SERIALIZER', 'json')

# INVITE_CODE = os.environ.get('APP_INVITE_CODE', '123')
# SECRET_KEY = os.environ.get('APP_SECRET_KEY', secrets.token_urlsafe(32))

# for app back {redis | mongodbrabbitmq}
APP_BACK_END = os.getenv('APP_BACK_END', 'mongodbrabbitmq')  # Defaulting to a common local endpoint
APP_INVITE_CODE = os.getenv('APP_INVITE_CODE', '123')  # Replace with appropriate default
APP_SECRET_KEY = os.getenv('APP_SECRET_KEY', 'super_secret_key')  # Caution: replace with a strong key in production

# Constants with clear definitions
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv('ACCESS_TOKEN_EXPIRE_MINUTES', 30))  # Use environment variable with a default fallback
SESSION_DURATION = ACCESS_TOKEN_EXPIRE_MINUTES * 60
UVICORN_PORT = int(os.getenv('UVICORN_PORT', 8000))  # Using an environment variable fallback
FLOWER_PORT = int(os.getenv('UVICORN_PORT', 5555))  # Using an environment variable fallback
# External service URLs with sensible defaults
RABBITMQ_URL = os.getenv('RABBITMQ_URL', 'localhost:15672')
RABBITMQ_USER = os.getenv('RABBITMQ_USER', 'guest')
RABBITMQ_PASSWORD = os.getenv('RABBITMQ_PASSWORD', 'guest')

MONGO_URL = os.getenv('MONGO_URL', 'mongodb://localhost:27017')
MONGO_DB = os.getenv('MONGO_DB', 'tasks')

CELERY_META = os.getenv('CELERY_META', 'celery_taskmeta')
CELERY_RABBITMQ_BROKER = os.getenv('CELERY_RABBITMQ_BROKER', 'amqp://localhost')

# Redis URL configuration with fallback
REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379/0')

# Handle external IP fetching gracefully with error handling
try:
    EX_IP = requests.get('https://v4.ident.me/').text
except requests.RequestException:
    EX_IP = '127.0.0.1'  # Fallback to a default value if the request fails


USER_DB = UsersStore()
if APP_BACK_END=='redis':
    USER_DB.redis_backend(redis_URL=REDIS_URL)
elif APP_BACK_END=='mongodbrabbitmq':
    USER_DB.mongo_backend(mongo_URL=MONGO_URL)
else:
    raise ValueError(f'no back end of {APP_BACK_END}')

username:str = 'root'
hashed_password:str = Model4User.User.hash_password('root')
full_name:str = 'root'
email:str = 'root@root.com'
role:str = 'root'

if USER_DB.find_user_by_email(email) is None:
    USER_DB.add_new_user(username,hashed_password,full_name,email,role)

# Debugging logs (optional, for development)
# if __name__ == "__main__":
#     print(f"APP_BACK_END: {APP_BACK_END}")
#     print(f"APP_INVITE_CODE: {APP_INVITE_CODE}")
#     print(f"APP_SECRET_KEY: [HIDDEN]")
#     print(f"ALGORITHM: {ALGORITHM}")
#     print(f"ACCESS_TOKEN_EXPIRE_MINUTES: {ACCESS_TOKEN_EXPIRE_MINUTES}")
#     print(f"SESSION_DURATION: {SESSION_DURATION}")
#     print(f"UVICORN_PORT: {UVICORN_PORT}")
#     print(f"EX_IP: {EX_IP}")
#     print(f"RABBITMQ_URL: {RABBITMQ_URL}")
#     print(f"MONGO_URL: {MONGO_URL}")
#     print(f"MONGO_DB: {MONGO_DB}")
#     print(f"CELERY_META: {CELERY_META}")
#     print(f"CELERY_RABBITMQ_BROKER: {CELERY_RABBITMQ_BROKER}")
#     print(f"REDIS_URL: {REDIS_URL}")
