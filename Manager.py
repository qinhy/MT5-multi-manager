from threading import Lock
import random
from typing import Dict, List

import MetaTrader5 as mt5
from pydantic import BaseModel, SecretStr

class MT5Account(BaseModel):
    # exe_path:str ="path/to/your/terminal64.exe"
    account_id: int = None # for @mt5_class_operation
    password: SecretStr = '' # for @mt5_class_operation
    account_server: SecretStr = '' # for @mt5_class_operation

    def is_valid(self):
        if self.account_id is None:raise ValueError('account_id is not set')
        if self.password.get_secret_value() == '':raise ValueError('password is not set')
        if self.account_server.get_secret_value() == '':raise ValueError('account_server is not set')
        return True


class MT5Action:
    #     timeout 
    #     retry_times on timeout
    #     retry_times on error
    
    def __init__(self,account:MT5Account, retry_times_on_error=3) -> None:       
        # do set_account at first
        self._account:MT5Account = account
        self.retry_times_on_error = retry_times_on_error

    def set_account(self,account_id,password,account_server):
        # do this at first
        self._account.account_id=account_id
        self._account.password=password
        self._account.account_server=account_server
        return self
    
    def _run(self):
        try:
            self.run()
            self.on_end()
        except Exception as e:
            print(e)
            self._on_error(e)

    def run(self):
        print('do your action at here with mt5')
    
    def on_error(self):
        pass
        # print('not implement')

    def _on_error(self):
        self.retry_times_on_error-=1
        if self.retry_times_on_error>0:
            self._run()
        else:
            self.on_error()

    def on_end(self):
        pass
        # print('not implement')

class MT5Manager:
    class TerminalLock:
        def __init__(self,exe_path="path/to/your/terminal64.exe"):
            self.exe_path=exe_path
            self._lock = Lock()
        def acquire(self):
            # print("acquired", self)
            self._lock.acquire()
        def release(self):
            # print("released", self)
            self._lock.release()
        def __enter__(self):
            self.acquire()
        def __exit__(self, type, value, traceback):
            self.release()

    def __init__(self) -> None:
        self.terminals:Dict[str,List[MT5Manager.TerminalLock]] = {
            'XMTrading':[ MT5Manager.TerminalLock(exe_path="path/to/your/terminal64.exe") ],}
    # {
    #     'TitanFX':[
    #         MT5Manager.TerminalLock(exe_path="path/to/your/terminal64.exe")
    #     ],
    #     'XMTrading':[],
    # }
    def add_terminal(self, account_server='XMTrading', exe_path="path/to/your/terminal64.exe"):
        self.terminals.get(account_server,[]).append(exe_path)
    
    def _get_terminal_lock(self, account_server='XMTrading'):
        broker = account_server.split('-')[0]
        t_locks = self.terminals.get(broker,[])
        if len(t_locks)==0:raise ValueError('the broker is not support!')
        return random.choice(t_locks)

    def do(self, action:MT5Action):
        # m = Manager()
        # m.addExe

        # m.do(
        #     new Action class do some thing
        #     timeout 
        #     retry_times on timeout
        #     retry_times on error
        # )
        
        # get lock
        l = self._get_terminal_lock(action._account.account_server.get_secret_value())
        try:
            l.acquire()
            if not mt5.initialize(path=l.exe_path):
                raise ValueError(f"Failed to initialize MT5 for executable path: {l.exe_path}")
            
            if action._account is None:raise ValueError('_account is not set')
            action._account.is_valid()
            account = action._account
    
            if not mt5.login(account.account_id,
                             password=account.password.get_secret_value(),
                             server=account.account_server.get_secret_value()):                
                raise ValueError(f"Failed to log in with account ID: {account.account_id}")
            
            action._run()
        finally:
            mt5.shutdown()  # Ensure shutdown is called even if an error occurs
            l.release()
        # release lock

class AccountInfo(MT5Action):
    def run():
        # Example operation: Getting account information
        account_info = mt5.account_info()
        print(account_info)
        if account_info is None:
            return "Failed to get account info"
        else:
            return account_info