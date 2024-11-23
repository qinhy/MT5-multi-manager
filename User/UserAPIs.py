
from Config import APP_INVITE_CODE, APP_SECRET_KEY, ALGORITHM, ACCESS_TOKEN_EXPIRE_MINUTES, USER_DB
from .UserModel import Model4User, text2hash2base32Str

from pydantic import BaseModel, EmailStr, Field
from fastapi.security import OAuth2PasswordRequestForm
from fastapi.responses import HTMLResponse, FileResponse, StreamingResponse, RedirectResponse
from fastapi import APIRouter, Depends, FastAPI, HTTPException, status, Request
from jose import JWTError, jwt
from datetime import datetime, timedelta, timezone
from io import BytesIO
import json
import uuid
import pyotp
import qrcode
import os

router = APIRouter()
#######################################################################################
class UserModels:
    class User(Model4User.User):
        pass
        # username:str
        # full_name: str
        # role:str = 'user'
        # hashed_password:str # text2hash2base64Str(password),
        # email:str
        # disabled: bool=False

    class RegisterRequest(BaseModel):
        username: str
        full_name: str
        email: str
        password: str
        invite_code: str

    class EditUserRequest(BaseModel):
        # username: str
        full_name: str
        # email: str
        new_password: str
        is_remove: bool
        password: str

    class PayloadModel(BaseModel):
        role:str = 'user'
        email: EmailStr = Field(..., description="The email address of the user")
        exp: datetime = Field(..., description="The expiration time of the token as a datetime object")
        
        def is_root(self):return self.role == 'root'

    class SessionModel(BaseModel):
        token_type: str = 'bearer'
        app_access_token: str
        user_uuid: str
        exp: datetime = Field(..., description="The expiration time of the token as a datetime object")

        def model_dump_json_dict(self)->dict:
            return json.loads(self.model_dump_json())
        
# AuthService for managing authentication
class AuthService:
    @staticmethod
    def create_access_token(email: str, expires_delta: timedelta = None, role:str = 'user'):
        expire = datetime.now(timezone.utc) + (expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
        payload = UserModels.PayloadModel(email=email,exp=expire,role=role)
        return jwt.encode(payload.model_dump(), APP_SECRET_KEY, algorithm=ALGORITHM),payload

    @staticmethod
    async def get_current_payload(request: Request): 
        try:
            session = UserModels.SessionModel(**request.session)
        except Exception as e:            
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid authentication credentials")
                  
        try:            
            payload = UserModels.PayloadModel(**jwt.decode(session.app_access_token, APP_SECRET_KEY, algorithms=[ALGORITHM]))
        except JWTError:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid authentication credentials")
        
        # Expiration check
        if datetime.now(timezone.utc) > payload.exp:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token has expired")

        return payload

    @staticmethod
    async def get_current_user(request: Request):
        payload = await AuthService.get_current_payload(request)
        user = USER_DB.find_user_by_email(payload.email)
        if not user or user.disabled:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found or inactive")
        
        return user

    @staticmethod
    async def get_current_root_payload(request: Request): 
        payload = await AuthService.get_current_payload(request)
        if not payload.is_root():
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User is not root")
        return payload

    @staticmethod
    def generate_otp_secret(email: str) -> str:
        secret = pyotp.random_base32()
        user = USER_DB.find_user_by_email(email)
        if user:
            USER_DB.update_user_otp_secret(email, secret)
        return secret

    @staticmethod
    def generate_otp_qr_code(user:UserModels.User) -> str:
        secret = text2hash2base32Str(user.email)
        totp = pyotp.TOTP(secret)
        uri = totp.provisioning_uri(name=user.email, issuer_name="APP")
        qr = qrcode.QRCode(version=1, error_correction=qrcode.constants.ERROR_CORRECT_L, box_size=10, border=4)
        qr.add_data(uri)
        qr.make(fit=True)
        img = qr.make_image(fill='black', back_color='white')
        buf = BytesIO()
        img.save(buf, format="PNG")
        # img_b64 = b64encode(buf.getvalue()).decode('utf-8')
        # return f"data:image/png;base64,{img_b64}"
        img.save(buf, format="PNG")
        buf.seek(0)
        return buf
    
    @staticmethod
    async def verify_otp(otp_code:str, email:str) -> bool:
        secret = text2hash2base32Str(email)
        if not secret: return False
        totp = pyotp.TOTP(secret)
        return totp.verify(otp_code)
    
    @staticmethod    
    def generate_session(user:UserModels.User):
        access_token,payload = AuthService.create_access_token(email=user.email,role=user.role)
        data = UserModels.SessionModel(app_access_token=access_token,user_uuid=user.get_id(),
                                       exp=payload.exp).model_dump_json_dict()
        return data
    
    @staticmethod
    def generate_login_qr(uid):
        # Generate a unique token or code for login, this could be based on user session or a one-time code
        uri = f"/qr?token={uid}"
        
        # Generate QR Code
        qr = qrcode.QRCode(version=1, error_correction=qrcode.constants.ERROR_CORRECT_L, box_size=10, border=4)
        qr.add_data(uri)
        qr.make(fit=True)
        img = qr.make_image(fill='black', back_color='white')

        # Save image to buffer
        buf = BytesIO()
        img.save(buf, format="PNG")
        buf.seek(0)
        return buf

    
# OAuth routes
class OAuthRoutes:
    @staticmethod
    @router.get("/register", response_class=HTMLResponse)
    async def get_register_page():
        return FileResponse(os.path.join(os.path.dirname(__file__), 'data', 'templates', "register.html"))

    @staticmethod
    @router.get("/login", response_class=HTMLResponse)
    async def get_login_page():
        return FileResponse(os.path.join(os.path.dirname(__file__), 'data', 'templates', "login.html"))

    @staticmethod
    @router.get("/edit", response_class=HTMLResponse)
    async def get_edit_page(current_user: UserModels.User = Depends(AuthService.get_current_payload)):
        return FileResponse(os.path.join(os.path.dirname(__file__), 'data', 'templates', "edit.html"))
    
    @staticmethod
    @router.get("/", response_class=HTMLResponse)
    async def read_home(current_user: UserModels.User = Depends(AuthService.get_current_payload)):
        return FileResponse(os.path.join(os.path.dirname(__file__), 'data', 'templates', "edit.html"))

    @staticmethod
    @router.post("/register")
    async def register_user(request: UserModels.RegisterRequest):

        # TODO
        # 1. **Email Format Validation**:
        # 2. **Password Strength Validation**:
        # 3. **Checking for Missing Fields**:
        # 4. **Duplicate Username/Email Checks**:
        # 5. **Invite Code Expiration**:
        # 6. **Validation for Other Fields**:
        # 7. **Rate Limiting**:
        # 8. **Cross-Site Request Forgery (CSRF) Tokens**:

        data = request.model_dump()
        
        if data.pop('invite_code') != APP_INVITE_CODE:
            raise HTTPException(status_code=status.HTTP_406_NOT_ACCEPTABLE, detail="Invalid invite code")

        if USER_DB.find_user_by_email(request.email) is not None:            
            raise HTTPException(status_code=400, detail="Username already exists")
        
        data['hashed_password'] = UserModels.User.hash_password(data.pop('password'))
        USER_DB.add_new_user(**data)
        return {"status": "success", "message": "User registered successfully"}

    @staticmethod
    @router.post("/token")
    def get_token(form_data: OAuth2PasswordRequestForm = Depends(), request: Request = None):
        email = form_data.username
        user = USER_DB.find_user_by_email(email)        
        if user is None  or not user.check_password(form_data.password):
            raise HTTPException(status_code=400, detail="Incorrect username or password")
        
        data = AuthService.generate_session(user)
        request.session.update(data)        
        return data
        
    
    @staticmethod
    @router.post("/edit")
    async def edit_user_info(request: UserModels.EditUserRequest, current_user: UserModels.User = Depends(AuthService.get_current_user)):
        if not current_user.check_password(request.password):
            raise HTTPException(status_code=400, detail="Incorrect password")

        try:
            new_password = request.new_password or request.password
            current_user.get_controller().update(
                full_name=request.full_name,
                hashed_password=UserModels.User.hash_password(new_password),
            )
            return {"status": "success", "message": "User info updated successfully"}
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to update user info: {e}")

    @staticmethod
    @router.post("/remove")
    async def remove_account(request: UserModels.EditUserRequest, current_user: UserModels.User = Depends(AuthService.get_current_user)):
        if not current_user.check_password(request.password):
            raise HTTPException(status_code=400, detail="Incorrect password")

        try:
            current_user.get_controller().delete()
            return {"status": "success", "message": "User account removed successfully"}
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to remove account: {e}")


    @staticmethod
    @router.get("/me")
    async def read_users_me(current_user: UserModels.User = Depends(AuthService.get_current_user)):
        return dict(**current_user.model_dump(),uuid=current_user.get_id())

    @staticmethod
    @router.get("/session")
    def read_session(request: Request, current_user: UserModels.User = Depends(AuthService.get_current_user)):
        session = UserModels.SessionModel(**request.session)
        return dict(**current_user.model_dump(),app_access_token=session.app_access_token,
                    timeout=session.exp-datetime.now(timezone.utc))
    
    @router.get("/otp/qr")
    async def get_otp_qr(current_user: UserModels.User = Depends(AuthService.get_current_user)):
        return StreamingResponse(AuthService.generate_otp_qr_code(current_user), media_type="image/png")
    
    @router.get("/qr")
    async def get_login_qr(request: Request = None):
        return RedirectResponse(f'/qr/{str(uuid.uuid4())}')
    
    @router.get("/qr/{uid}")
    async def get_login_qr(uid:str, request: Request = None):
        try:
            uid = str(uuid.UUID(uid))
        except Exception as e:
            return RedirectResponse(f'/{uid}')
        
        try:
            user = await AuthService.get_current_user(request)
            return RedirectResponse('/')
        except Exception as e:
            pass

        data = USER_DB.get(uid)
        if data is None or len(data)==0:
            USER_DB.set(uid,{})
            return StreamingResponse(AuthService.generate_login_qr(uid), media_type="image/png")
        else:
            session = UserModels.SessionModel(**data)
            user = USER_DB.find(session.user_uuid)
            data = AuthService.generate_session(user)
            request.session.update(data)
            USER_DB.delete(uid)
            return RedirectResponse('/')
            # return {"status": "success", "message": "Logged in successfully"}
    
    @router.get("/qr/login/{uid}")
    async def login_qr(uid:str, request: Request, current_user: UserModels.User = Depends(AuthService.get_current_user)):
        session = UserModels.SessionModel(**request.session)
        print(uid)
        if USER_DB.exists(uid):
            USER_DB.set(uid,session.model_dump_json_dict())
            return {"status": "success", "message": "Logged in successfully"}
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid code")
    
    @router.get("/otp/login/{email}/{code}")
    async def get_otp_token(code:str,email:str, request: Request = None):
        con = await AuthService.verify_otp(code,email)
        if not con: 
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid authentication credentials")
        user = USER_DB.find_user_by_email(email)
        data = AuthService.generate_session(user)
        request.session.update(data)        
        return data

    @router.get("/icon/{icon_name}", response_class=HTMLResponse)
    async def read_icon(icon_name: str, current_user: UserModels.User = Depends(AuthService.get_current_payload)):
        return FileResponse(os.path.join(os.path.dirname(__file__), 'data', 'icon', icon_name))

    @staticmethod
    @router.get("/logout")
    async def logout(request: Request):
        request.session.clear()
        return {"status": "logged out"}

# app = FastAPI(title="app")
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=[ '*',],
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )
# app.add_middleware(SessionMiddleware, secret_key=SECRET_KEY, max_age=SESSION_DURATION)

# app.include_router(router, prefix="", tags=["users"])
