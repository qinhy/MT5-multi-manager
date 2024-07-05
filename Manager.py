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

class MT5Action:
    #     timeout 
    #     retry_times on timeout
    #     retry_times on error
    
    def __init__(self) -> None:       
        # do set_account at first
        self._account:MT5Account = None

    def set_account(self,account_id,password,account_server):
        # do this at first
        self._account.account_id=account_id
        self._account.password=password
        self._account.account_server=account_server
        return self
    
    def run(self):
        print('do your action at here with mt5')
    
    def on_error(self):
        pass

    def on_end(self):
        pass

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
            if action._account.account_id is None:raise ValueError('_account.account_id is not set')
            if action._account.password.get_secret_value() == '':raise ValueError('_account.password is not set')
            if action._account.account_server.get_secret_value() == '':raise ValueError('_account.account_server is not set')

            # Extract parameters from kwargs or use default values
            logind = dict(account_id=action._account.account_id,
                          password=action._account.password.get_secret_value(),
                          account_server=action._account.account_server.get_secret_value())
            
            if all(list(logind.values())) and not mt5.login(logind['account_id'], 
                                                            password=logind['password'],
                                                            server=logind['account_server']):                
                raise ValueError(f"Failed to log in with account ID: {logind['account_id']}")
            
            action.run()
        finally:
            mt5.shutdown()  # Ensure shutdown is called even if an error occurs
            l.release()
        # release lock