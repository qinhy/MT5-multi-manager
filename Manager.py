from threading import Lock
import random
import time
from typing import Any, Dict, List

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
    
    def on_error(self,e):
        pass
        # print('not implement')

    def _on_error(self,e):
        self.retry_times_on_error-=1
        if self.retry_times_on_error>0:
            time.sleep(1)
            self._run()
        else:
            self.on_error(e)

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
        self.terminals:Dict[str,List[MT5Manager.TerminalLock]] = {}
    # {
    #     'TitanFX':[
    #         MT5Manager.TerminalLock(exe_path="path/to/your/terminal64.exe")
    #     ],
    #     'XMTrading':[],
    # }
    def add_terminal(self, account_server='XMTrading', exe_path="path/to/your/terminal64.exe"):
        if account_server not in self.terminals:self.terminals[account_server]=[]
        self.terminals.get(account_server,[]).append(
            MT5Manager.TerminalLock(exe_path=exe_path))
    
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

class Book(BaseModel):
    class Controller(BaseModel):        
        @staticmethod
        def _try(func):
            try:
                func()
                return True
            except Exception as e:
                print(e)
                return False
        @staticmethod
        def send(book):
            book:Book = book
            print('common function is not implemented')
        @staticmethod
        def close(book):
            book:Book = book
            print('common function is not implemented')
        @staticmethod
        def changeP(book):
            book:Book = book
            print('common function is not implemented')
        @staticmethod
        def changeST(book):
            book:Book = book
            print('common function is not implemented')

        class Null(BaseModel):
            type:str = 'Null'
            def send(self,book):
                raise ValueError(f'This is a {self.type} state')
            def close(self,book):
                raise ValueError(f'This is a {self.type} state')
            def changeP(self,book):
                raise ValueError(f'This is a {self.type} state')
            def changeST(self,book):
                raise ValueError(f'This is a {self.type} state')

        class Plan(BaseModel):
            type:str = 'Plan'
            def send(self,book):
                res = Book.Controller._try(lambda:Book.Controller.send(book))
                return res, ( Book.Controller.Order() if res else Book.Controller.Plan() )
            def close(self,book):
                raise ValueError('This is just a Plan')
            def changeP(self,book):
                res = Book.Controller._try(lambda:Book.Controller.changeP(book))
                return res, Book.Controller.Plan()
            def changeST(self,book):
                res = Book.Controller._try(lambda:Book.Controller.changeST(book))
                return res, Book.Controller.Plan()
            
        class Order(BaseModel):
            type:str = 'Order'
            def send(self,book):
                raise ValueError('This is a sended Order')
            def close(self,book):
                res = Book.Controller._try(lambda:Book.Controller.close(book))
                return res, ( Book.Controller.Null() if res else Book.Controller.Order() )
            def changeP(self,book):
                res = Book.Controller._try(lambda:Book.Controller.changeP(book))
                return res, Book.Controller.Order()
            def changeST(self,book):
                res = Book.Controller._try(lambda:Book.Controller.changeST(book))
                return res, Book.Controller.Order()

        class Position(BaseModel):
            type:str = 'Position'
            def send(self,book):
                raise ValueError('This is a exists Position')
            def close(self,book):
                res = Book.Controller._try(lambda:Book.Controller.close(book))
                return res, ( Book.Controller.Null() if res else Book.Controller.Position() )
            def changeP(self,book):
                raise ValueError('This is a exists Position')
            def changeST(self,book):
                res = Book.Controller._try(lambda:Book.Controller.changeST(book))
                return res, Book.Controller.Position()
            
    controller: Controller.Plan = Controller.Plan()
    symbol: str = ''
    sl: float = 0.0
    tp: float = 0.0
    price_open: float = 0.0
    volume: float = 0.0

    _book: Any = None# mt5_order_position
    _is_order: bool = False
    _is_position: bool = False
    _ticket: int = 0
    _type: str = ''
    _swap: int = 0

    def getBooks(self):
        return [ Book().set_mt5_book(book=op) for op in mt5.orders_get()+mt5.positions_get() ]
    
    def account_info(self):
        # Example operation: Getting account information
        account_info = mt5.account_info()
        if account_info is None:
            return "Failed to get account info"
        else:
            return account_info
        
    def set_mt5_book(self,book):
        self._book = book
        self.symbol = self._book.symbol
        self.sl = self._book.sl
        self.tp = self._book.tp
        self.price_open = self._book.price_open
        self._ticket = self._book.ticket
        self._type = self._book.type
        self._is_order=False
        self._is_position=False
        self._swap = 0
        
        if self._book.__class__.__name__ == "TradeOrder" : 
            self._is_order=True
            self.controller = Book.Controller.Order()
        elif self._book.__class__.__name__ == "TradePositio": 
            self._is_position=True
            self.controller = Book.Controller.Position()
        if hasattr(self._book,'volume_current'):
            self._is_order=True
            self.volume=self._book.volume_current
        elif hasattr(self._book,'volume'):
            self._is_position=True
            self.volume=self._book.volume
            self._swap = self._book.swap
        else:
            raise 'Unkown type!'
        
        self._book = ''
        return self

    def isBuy(self):
        if self._is_order: 
                return self._type in [mt5.ORDER_TYPE_BUY,mt5.ORDER_TYPE_BUY_LIMIT ,
                                      mt5.ORDER_TYPE_BUY_STOP ,mt5.ORDER_TYPE_BUY_STOP_LIMIT]
        elif self._is_position: 
                return self._type == mt5.POSITION_TYPE_BUY
        return True
    
    def sendRequest(self, request):    
        result=mt5.order_send(request)    
        if result is None or result.retcode != mt5.TRADE_RETCODE_DONE:
            print('send request failed',result)
            return False
        return True

    def changeOrderTPSL(self, tp=0.0,sl=0.0):
        request = {
            "action": mt5.TRADE_ACTION_MODIFY,
            "order": self._ticket,
            "price": self.price_open,
            "tp": tp,
            "sl": sl
        }
        return self.sendRequest(request)

    def changePositionTPSL(self, tp=0.0,sl=0.0):
        request = {
            "action": mt5.TRADE_ACTION_SLTP,
            "position": self._ticket,
            "symbol": self.symbol,
            "tp": tp,
            "sl": sl
        }
        return self.sendRequest(request)

    def changeTPSL(self, tp=0.0,sl=0.0):
        if self._is_order: 
            return self.changeOrderTPSL(tp,sl)
        elif self._is_position: 
            return self.changePositionTPSL(tp,sl)
        return False

    def close(self):
        if self._is_order:
            self._close_order()
        elif self._is_position:
            self._close_position()
    
    def _close_position(self):
        #https://www.mql5.com/ja/docs/constants/structures/mqltraderequest
        if self._type == mt5.ORDER_TYPE_BUY:
            price = mt5.symbol_info_tick(self.symbol).bid
            type_tmp = mt5.ORDER_TYPE_SELL
        elif self._type == mt5.ORDER_TYPE_SELL:
            price = mt5.symbol_info_tick(self.symbol).ask
            type_tmp = mt5.ORDER_TYPE_BUY
        else:
            raise ValueError('unknow position type(nor buy or sell) error.')

        deviation=20
        request={
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": self.symbol,
            "volume": self.volume,
            "type": type_tmp,
            "position": self._ticket,
            "price": price,
            "deviation": deviation,
            "magic": 901000,
            "comment": "script close",
            "type_time": mt5.ORDER_TIME_GTC,
            #"type_filling": mt5.ORDER_FILLING_IOC,
        }
        return mt5.order_send(request)

    def _close_order(self):
        #https://www.mql5.com/en/forum/365968
        request = {
            "action": mt5.TRADE_ACTION_REMOVE,
            "order": self._ticket,
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_IOC,
        }
        return mt5.order_send(request)

    def _make_order(self, profit_risk_ratio: float=None):
        # ProfitRiskRatio = self._ProfitRiskRatio
        # Determine order type and calculate stop loss based on parameters
        going_long = self.tp > self.price_open
        current_price_info = mt5.symbol_info_tick(self.symbol)
        if current_price_info is None:
            return f"Error getting current price for {self.symbol}"

        if going_long:
            current_price = current_price_info.ask
            order_type = mt5.ORDER_TYPE_BUY_STOP if self.price_open > current_price else mt5.ORDER_TYPE_BUY_LIMIT
        else:
            current_price = current_price_info.bid
            order_type = mt5.ORDER_TYPE_SELL_STOP if self.price_open < current_price else mt5.ORDER_TYPE_SELL_LIMIT

        if profit_risk_ratio is not None:
            self.sl = self.price_open + (self.price_open - self.tp) / profit_risk_ratio

        digitsnum = mt5.symbol_info(self.symbol).digits
        self.price_open,self.sl,self.tp = list(map(lambda x:round(x*10**digitsnum)/10**digitsnum,
                                                        [self.price_open,self.sl,self.tp]))
        # Prepare trade request
        deviation=20
        request = {
            "action": mt5.TRADE_ACTION_PENDING,
            "symbol": self.symbol,
            "volume": self._volume,
            "type": order_type,
            "price": self.price_open,
            "sl": self.sl,
            "tp": self.tp,
            "deviation": deviation,
            "magic": 901000,
            "comment": "auto order",
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_RETURN,
        }

        result = mt5.order_send(request)
        if result.retcode != mt5.TRADE_RETCODE_DONE:
            return f"order_send failed, retcode={result.retcode}"
        else:
            return "Trade successful"
     

manager = MT5Manager()
manager.add_terminal('xxxx',"xxxx/xxxx/xxxx/terminal64.exe")
book = Book()
action = MT5Action(MT5Account(account_id='xxxx',
                        password='xxxx',
                        account_server='xxxx',))
action.run = lambda :print(book.account_info())
manager.do(action)