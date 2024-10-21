
from multiprocessing import shared_memory
import celery
import celery.states
import numpy as np
from pydantic import BaseModel, Field
from pymongo import MongoClient

# Constants
mongo_URL = 'mongodb://localhost:27017'
mongo_DB = 'tasks'
celery_META = 'celery_taskmeta'
celery_broker = 'amqp://localhost'

# Function to get a document by task_id
def get_tasks_collection():
    # Reusable MongoDB client setup
    client = MongoClient(mongo_URL)
    db = client.get_database(mongo_DB)
    collection = db.get_collection(celery_META)
    return collection#.find_one({'_id': task_id})

def set_task_started(task_id):
    collection = get_tasks_collection()
    return collection.update_one({'_id': task_id},
                {'$set': {'status': celery.states.STARTED}},upsert=True)

def set_task_revoked(task_id):
    collection = get_tasks_collection()
    # Update the status of the task to 'REVOKED'
    update_result = collection.update_one({'_id': task_id}, {'$set': {'status': 'REVOKED'}})
    if update_result.matched_count > 0:
        res = collection.find_one({'_id': task_id})
    else:
        res = {'error': 'Task not found'}    
    return res

class CommonIO:
    class Base:            
        def write(self,data):
            raise ValueError("[CommonIO.Reader]: This is Reader can not write")
        def read(self):
            raise ValueError("[CommonIO.Writer]: This is Writer can not read") 
        def close(self):
            raise ValueError("[CommonIO.Base]: not implemented")            
    class Reader(Base):
        def read(self):
            raise ValueError("[CommonIO.Reader]: not implemented")      
    class Writer(Base):
        def write(self,data):
            raise ValueError("[CommonIO.Writer]: not implemented")

class CommonStreamIO(CommonIO):
    class Base(CommonIO.Base):
        def write(self, data, metadata={}):
            raise ValueError("[CommonStreamIO.Reader]: This is Reader can not write")
        
        def read(self):
            raise ValueError("[CommonStreamIO.Writer]: This is Writer can not read") 
        
        def __iter__(self):
            return self

        def __next__(self):
            return self.read()
        
        def stop(self):
            raise ValueError("[StreamWriter]: not implemented")
        
    class StreamReader(CommonIO.Reader, Base):
        def read(self):
            return super().read(),{}
        
    class StreamWriter(CommonIO.Writer, Base):
        def write(self, data, metadata={}):
            raise ValueError("[StreamWriter]: not implemented")

class GeneralSharedMemoryIO(CommonIO):
    class Base(CommonIO.Base, BaseModel):
        shm_name: str = Field(..., description="The name of the shared memory segment")
        create: bool = Field(default=False, description="Flag indicating whether to create or attach to shared memory")
        shm_size: int = Field(..., description="The size of the shared memory segment in bytes")
        
        _shm:shared_memory.SharedMemory
        _buffer:memoryview

        def __init__(self, **kwargs):
            # Initialize Pydantic BaseModel to validate and set fields
            super().__init__(**kwargs)
            
            # Initialize shared memory with the validated size and sanitized name
            self._shm = shared_memory.SharedMemory(name=self.shm_name, create=self.create, size=self.shm_size)
            self._buffer = memoryview(self._shm.buf)  # View into the shared memory buffer
                
        def close(self):
            """Detach from the shared memory."""
            # Release the memoryview before closing the shared memory
            if hasattr(self,'_buffer') and self._buffer is not None:
                self._buffer.release()
                del self._buffer
            if hasattr(self,'_shm'):
                self._shm.close()  # Detach from shared memory

        def __del__(self):
            self.close()

    class Reader(CommonIO.Reader, Base):
        def read(self, size: int = None) -> bytes:
            """Read binary data from shared memory."""
            if size is None or size > self.shm_size:
                size = self.shm_size  # Read the whole buffer by default
            return bytes(self._buffer[:size])  # Convert memoryview to bytes
  
    class Writer(CommonIO.Writer, Base):
        def write(self, data: bytes):
            """Write binary data to shared memory."""
            if len(data) > self.shm_size:
                raise ValueError(f"Data size exceeds shared memory size ({len(data)} > {self.shm_size})")
            
            # Write the binary data to shared memory
            self._buffer[:len(data)] = data
        
        def close(self):
            super().close()
            if hasattr(self,'_shm'):
                self._shm.unlink()  # Unlink (remove) the shared memory segment after writing
    
    def reader(self, shm_name: str, shm_size: int):
        """Helper function to create a Reader instance."""
        return GeneralSharedMemoryIO.Reader(shm_name=shm_name, create=False, shm_size=shm_size)
    
    def writer(self, shm_name: str, shm_size: int):
        """Helper function to create a Writer instance."""
        return GeneralSharedMemoryIO.Writer(shm_name=shm_name, create=True, shm_size=shm_size)
             
class NumpyUInt8SharedMemoryIO(GeneralSharedMemoryIO):
    class Base(GeneralSharedMemoryIO.Base):
        array_shape: tuple = Field(..., description="Shape of the NumPy array to store in shared memory")        
        _dtype: np.dtype = np.uint8        
        def __init__(self, **kwargs):
            kwargs['shm_size'] = np.prod(kwargs['array_shape']) * np.dtype(np.uint8).itemsize
            super().__init__(**kwargs)  # Initialize Pydantic BaseModel and validate fields
        
    class Reader(GeneralSharedMemoryIO.Reader, Base):
        def read(self) -> np.ndarray:
            """Read binary data from shared memory and return it as a NumPy array."""
            # Read the binary data from the shared memory buffer
            binary_data = super().read(size=self.shm_size)
            # Convert the binary data into a NumPy array with the original shape and dtype
            return np.frombuffer(binary_data, dtype=self._dtype).reshape(self.array_shape)
    
    class Writer(GeneralSharedMemoryIO.Writer, Base):
        def write(self, data: np.ndarray):
            """Write NumPy int8 array to shared memory."""
            # Ensure the data being written is a NumPy array with the correct shape and dtype
            if data.shape != self.array_shape:
                raise ValueError(f"Data shape {data.shape} does not match expected shape {self.array_shape}.")
            if data.dtype != self._dtype:
                raise ValueError(f"Data type {data.dtype} does not match expected type {self._dtype}.")
            # Write the NumPy array to shared memory as binary data
            super().write(data.tobytes())

    def reader(self, shm_name: str, array_shape: tuple):
        """Helper function to create a Reader instance for reading NumPy int8 arrays."""
        return NumpyUInt8SharedMemoryIO.Reader(shm_name=shm_name, create=False, array_shape=array_shape,shm_size=1)
    
    def writer(self, shm_name: str, array_shape: tuple):
        """Helper function to create a Writer instance for writing NumPy int8 arrays."""
        return NumpyUInt8SharedMemoryIO.Writer(shm_name=shm_name, create=True, array_shape=array_shape,shm_size=1)

try:
    import redis   

    class RedisIO(CommonIO):
        class Base(CommonIO.Base, BaseModel):
            redis_host: str = Field(default='localhost', description="The Redis server hostname")
            redis_port: int = Field(default=6379, description="The Redis server port")
            redis_db: int = Field(default=0, description="The Redis database index")
            _redis_client:redis.Redis = None
            
            def __init__(self, **kwargs):
                # Initialize BaseModel to validate and set fields
                super().__init__(**kwargs)
                
                # Create Redis connection
                self._redis_client = redis.Redis(host=self.redis_host, port=self.redis_port, db=self.redis_db)
            
            def close(self):
                # Optionally close Redis connections if needed
                del self._redis_client
        
        class Reader(CommonIO.Reader, Base):
            key: str
            
            def read(self):
                """Read binary data from Redis."""
                data = self._redis_client.get(self.key)
                if data is None:
                    raise ValueError(f"No data found for key: {self.key}")
                return data  # Returning the raw binary data stored under the key
        
        class Writer(CommonIO.Writer, Base):
            key: str
            
            def write(self, data: bytes):
                """Write binary data to Redis."""
                if not isinstance(data, bytes):
                    raise ValueError("Data must be in binary format (bytes)")
                self._redis_client.set(self.key, data)  # Store binary data under the given key

        def reader(self, key: str, redis_host: str = 'localhost', redis_port: int = 6379, redis_db: int = 0):
            """Helper function to create a Reader instance."""
            return RedisIO.Reader(key=key, redis_host=redis_host, redis_port=redis_port, redis_db=redis_db)

        def writer(self, key: str, redis_host: str = 'localhost', redis_port: int = 6379, redis_db: int = 0):
            """Helper function to create a Writer instance."""
            return RedisIO.Writer(key=key, redis_host=redis_host, redis_port=redis_port, redis_db=redis_db)

except Exception as e:
    print('No redis support')

class ServiceOrientedArchitecture:
    class Model(BaseModel):
        task_id:str = 'NULL'
        class Param(BaseModel):
            pass
        class Args(BaseModel):
            pass
        class Return(BaseModel):
            pass

        param:Param = Param()
        args:Args = Args()
        ret:Return = Return()
    class Action:
        def __init__(self, model):
            if isinstance(model, dict):
                nones = [k for k,v in model.items() if v is None]
                for i in nones:del model[i]
                model = ServiceOrientedArchitecture.Model(**model)
            self.model: ServiceOrientedArchitecture.Model = model

        def __call__(self, *args, **kwargs):
            set_task_started(self.model.task_id)            
            return self.model

        
########################## test 
def test_NumpyUInt8SharedMemoryIO():
    # Initialize the mock shared memory IO
    shm_io = NumpyUInt8SharedMemoryIO()

    # Create a writer for a specific shared memory segment
    writer = shm_io.writer(shm_name="numpy_uint8_shm", array_shape=(10, 10))    
    print(writer.model_dump())

    # Create some sample data to write
    data = np.random.randint(0, 256, size=(10, 10), dtype=np.uint8)

    # Write the NumPy int8 array to shared memory
    writer.write(data)

    # Now create a reader for the same shared memory segment
    reader = shm_io.reader(shm_name="numpy_uint8_shm", array_shape=(10, 10))

    # Read the data back from shared memory
    data_read = reader.read()

    print(data_read)

    # Validate that the data matches
    assert np.array_equal(data, data_read), "The data read from shared memory does not match the written data"
    
    # Close the reader
    reader.close()
    writer.close()

    return "Test passed!"

def test_redisIO():
    # Initialize RedisIO for writing
    redis_io = RedisIO()

    # Create a writer for a specific Redis key
    writer = redis_io.writer(key="binary_data_key")
    print(writer.model_dump())
    # Create a reader for the same Redis key
    reader = redis_io.reader(key="binary_data_key")

    # Write binary data to Redis
    data = b"Hello, this is a binary message stored in Redis!"
    writer.write(data)

    # Read the binary data from Redis
    data_read = reader.read()
    print(data_read)  # Outputs: b"Hello, this is a binary message stored in Redis!"

    # Close the reader (not necessary for Redis, but to maintain CommonIO consistency)
    reader.close()
    writer.close()

# test_NumpyUInt8SharedMemoryIO()
# test_redisIO()