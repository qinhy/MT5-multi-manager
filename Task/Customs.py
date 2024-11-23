import threading
import time
import cv2
import numpy as np
from pydantic import BaseModel, Field
from .Basic import NumpyUInt8SharedMemoryStreamIO, ServiceOrientedArchitecture

class Fibonacci(ServiceOrientedArchitecture):
    class Model(ServiceOrientedArchitecture.Model):
        
        class Param(BaseModel):
            mode: str = 'fast'
            def is_fast(self):
                return self.mode=='fast'
        class Args(BaseModel):
            n: int = 1
        class Return(BaseModel):
            n: int = -1

        param:Param = Param()
        args:Args
        ret:Return = Return()

    class Action(ServiceOrientedArchitecture.Action):
        def __init__(self, model):
            # Ensure model is a Fibonacci instance, even if a dict is passed
            if isinstance(model, dict):
                model = Fibonacci.Model(**model)            
            self.model: Fibonacci.Model = model

        def __call__(self, *args, **kwargs):
            stop_flag = super().__call__(*args, **kwargs)
            n = self.model.args.n
            if n <= 1:
                self.model.ret.n = n
            else:
                if self.model.param.is_fast():
                    a, b = 0, 1
                    for _ in range(2, n + 1):
                        a, b = b, a + b
                    res = b
                else:
                    def fib_r(n):
                        if stop_flag.is_set(): return 0
                        return(fib_r(n-1) + fib_r(n-2))                    
                    res = fib_r(n)
                self.model.ret.n = res
            return self.model

class CvCameraSharedMemoryService:
    class Model(ServiceOrientedArchitecture.Model):        
        class Param(BaseModel):
            stream_key: str = Field(default='camera:0', description="The name of the stream")
            array_shape: tuple = Field(default=(480, 640), description="Shape of the NumPy array to store in shared memory")
            mode:str='write'
            _writer:NumpyUInt8SharedMemoryStreamIO.Writer=None
            _reader:NumpyUInt8SharedMemoryStreamIO.Reader=None
            
            def is_write(self):
                return self.mode=='write'
            
            def writer(self):
                if self._writer is None:
                    self._writer = NumpyUInt8SharedMemoryStreamIO.writer(
                        self.stream_key,self.array_shape)
                return self._writer

            def reader(self):
                if self._reader is None:
                    self._reader = NumpyUInt8SharedMemoryStreamIO.reader(
                        self.stream_key,self.array_shape)
                return self._reader

        class Args(BaseModel):
            camera:int = 0

        param:Param = Param()
        args:Args = Args()
        ret:str = 'NO_NEED_INPUT'
        
        
        def set_param(self, stream_key="camera_shm", array_shape=(480, 640), mode = 'write'):
            self.param = CvCameraSharedMemoryService.Model.Param(
                            mode=mode,stream_key=stream_key,array_shape=array_shape)
            return self

        def set_args(self, camera=0):
            self.args = CvCameraSharedMemoryService.Model.Args(camera=camera)
            return self
        
    class Action(ServiceOrientedArchitecture.Action):
        def __init__(self, model):
            if isinstance(model, dict):
                nones = [k for k,v in model.items() if v is None]
                for i in nones:del model[i]
                model = CvCameraSharedMemoryService.Model(**model)
            self.model: CvCameraSharedMemoryService.Model = model
        

        def cv2_VideoCapture_gen(self,src,writer:NumpyUInt8SharedMemoryStreamIO.Writer
                                 ,stop_flag:threading.Event):
            # Open the camera using OpenCV
            cap = cv2.VideoCapture(src)
            if not cap.isOpened():
                raise ValueError(f"Unable to open camera {src}")
            print("writing")
            while not stop_flag.is_set():
                ret, frame = cap.read()
                if not ret:
                    print("Failed to grab frame")
                    continue
                # Convert the frame to grayscale and resize to match shared memory size
                gray_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
                resized_frame = cv2.resize(gray_frame, (writer.array_shape[1], writer.array_shape[0]))
                yield resized_frame


        def cv2_Random_gen(self, stop_flag:threading.Event, array_shape=(7680, 4320), fps=30):
            # Define common resolutions
            # resolutions = {
            #     '8k': (7680, 4320),
            #     '4k': (3840, 2160),
            #     '1080p': (1920, 1080),
            #     '720p': (1280, 720),
            #     '480p': (640, 480)
            # }

            # # Get the resolution or default to 4k if size is not found
            # resolution = resolutions.get(size, resolutions['4k'])

            frame_time = 1.0 / fps  # Time per frame in seconds
            random_frame:np.ndarray = np.random.randint(0, 256, array_shape, dtype=np.uint8)

            while not stop_flag.is_set():
                # Generate a random grayscale frame (values between 0-255)
                # random_frame = np.random.randint(0, 256, (resolution[1], resolution[0]), dtype=np.uint8)
                
                # # Resize frame to match writer array shape
                # resized_frame = cv2.resize(random_frame, (writer.array_shape[1], writer.array_shape[0]))

                # Yield the random frame
                yield random_frame

                # Sleep to control frame rate
                # time.sleep(frame_time)

        def __call__(self, *args, **kwargs):
            stop_flag = super().__call__(*args, **kwargs)

            if self.model.param.is_write():
                writer = self.model.param.writer()

                frame_gen = self.cv2_Random_gen(stop_flag,writer.array_shape)
                start_time = time.time()
                # Add FPS text to the frame
                position = (10, 30)  # Top-left corner
                font = cv2.FONT_HERSHEY_SIMPLEX
                font_scale = 1
                color = (255, 255, 255)  # White color (use (0, 0, 0) for black on a grayscale image)
                thickness = 2
                for i,frame in enumerate(frame_gen):
                    if i%10000==0:
                        fps = 10000/(time.time()-start_time+1e-5)
                        print(f"FPS: {fps:.2f} , {writer.id}")
                        start_time = time.time()
                        
                    # Put text on the frame (converted to 3-channel to allow color text if needed)
                    # text = f"FPS: {fps:.2f}"
                    # frame_with_text = cv2.putText(frame, text, position, font, font_scale, color, thickness, cv2.LINE_AA)

                    writer.write(frame)

            else:
                # reading
                reader = self.model.param.reader()
                title = f'Shared Memory Reader Frame:{reader.id}'
                print("reading")
                while not stop_flag.is_set():
                    # Read the frame from shared memory
                    frame,_ = reader.read()
                    if frame is None:
                        print("No frame read from shared memory.")
                        continue

                    # Display the frame
                    cv2.imshow(title, frame)

                    if cv2.waitKey(1) & 0xFF == ord('q'):
                        break

                cv2.destroyAllWindows()
            
            return self.model

# def camera_writer_process(camera_service_model):
#     action = CvCameraSharedMemoryService.Action(camera_service_model)
#     action()  # Start capturing and writing to shared memory

# if __name__ == "__main__":
#     from multiprocessing import Process
#     camera_service_model = CvCameraSharedMemoryService.Model(
#         ).set_param(stream_key="camera:0", array_shape=(480, 640)
#         ).set_args(camera=0)
    
#     print(camera_service_model)    
#     # Start the writer process in a separate process

#     writer_process = Process(target=camera_writer_process,args=(camera_service_model.model_dump(),))
#     writer_process.start()

#     # Allow the writer some time to initialize and start capturing frames
#     time.sleep(5)

#     # Start the reader process
#     camera_service_model = CvCameraSharedMemoryService.Model(
#         ).set_param(mode='read',stream_key="camera:0", array_shape=(480, 640))
#     action = CvCameraSharedMemoryService.Action(camera_service_model)
#     action()

#     # Wait for the writer process to finish (if needed)
#     writer_process.join()
