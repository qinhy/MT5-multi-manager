import os
from threading import Lock
import random
import time
import uuid
from typing import Any, Dict, List

from .Basic import ServiceOrientedArchitecture
try:
    import MetaTrader5 as mt5
except Exception as e:
    print(e)
from pydantic import BaseModel, model_validator


class MT5Account(BaseModel):
    account_id: int = None  # for @mt5_class_operation
    password: str = ''
    account_server: str = ''

    def is_valid(self):
        return self.account_id is None or self.password=='' or self.account_server==''

class MT5Action:
    def __init__(self, account: MT5Account, retry_times_on_error=3) -> None:
        self.uuid = uuid.uuid4()
        self._account: MT5Account = account
        self.retry_times_on_error = retry_times_on_error

    def set_account(self, account_id, password, account_server):
        self._account.account_id = account_id
        self._account.password = password
        self._account.account_server = account_server
        return self

    def run_action(self,*args, **kwargs):
        while self.retry_times_on_error > 1:
            try:
                res = self.run()
                self.on_end(res)
                return res
            except Exception as e:
                print(f"Error occurred: {e}")
                self.retry_times_on_error -= 1
                time.sleep(1)
        
        res = self.run(*args, **kwargs)
        self.on_end(res)
        return res

    def run(self,*args, **kwargs):
        print("Executing action with MT5")

    def on_end(self, res):
        print("Action completed successfully")

class MT5Manager:
    # statics for singleton
    _uuid = uuid.uuid4()
    _results:Dict[str,List[Any]] = {}
    _terminals:Dict[str,set] = {}
    _is_singleton = True
    _meta = {}

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

        
    def __init__(self,id=None,results=None,terminals=None,is_singleton=None):
        self.uuid = uuid.uuid4() if id is None else id
        self.results:Dict[str,List[Any]] = None if results is None else results
        self.terminals:Dict[str,set[MT5Manager.TerminalLock]] = None if terminals is None else terminals
        self.is_singleton:bool = False if is_singleton is None else is_singleton
    # {
    #     'TitanFX':[
    #         MT5Manager.TerminalLock(exe_path="path/to/your/terminal64.exe")
    #     ],
    #     'XMTrading':[ MT5Manager.TerminalLock(exe_path="path/to/your/terminal64.exe") ],
    # }
    
    def get_singleton(self):
        return self.__class__(self._uuid,self._results,self._terminals,self._is_singleton)

    def add_terminal(self, account_server='XMTrading', exe_path="path/to/your/terminal64.exe"):
        # if not os.path.exists(exe_path):
        #     raise ValueError(f'No such file of {exe_path}')
        if account_server not in self.terminals:
            self.terminals[account_server] = set()
        if exe_path not in set([i.exe_path for i in self.terminals[account_server]]):
            self.terminals[account_server].add(MT5Manager.TerminalLock(exe_path=exe_path))

    def _get_terminal_lock(self, account_server='XMTrading'):
        broker = account_server.split('-')[0]
        t_locks = self.terminals.get(broker, set())
        if len(t_locks)==0:
            raise ValueError('The broker is not supported!')
        return random.choice(list(t_locks))
    
    def do(self, action: MT5Action, *args, **kwargs):
        terminal_lock = self._get_terminal_lock(action._account.account_server)
        
        try:
            terminal_lock.acquire()
            
            # Initialize the MT5 terminal
            if not mt5.initialize(path=terminal_lock.exe_path):
                raise ValueError(f"Failed to initialize MT5 for executable path: {terminal_lock.exe_path}")
            
            # Validate account information
            account = action._account
            if not account:
                raise ValueError('_account is not set')
            account.is_valid()
            
            # Check if we are already logged in with the correct account
            current_account = mt5.account_info()
            if str(current_account.login) != str(account.account_id):
                if not mt5.login(account.account_id, password=account.password, server=account.account_server):
                    raise ValueError(f"Failed to log in with account ID: {account.account_id}")
            
            # Execute the action and store the result
            if action.uuid not in self.results:
                self.results[action.uuid] = []
            
            action_result = action.run_action(*args, **kwargs)
            self.results[action.uuid].append(action_result)
            
            return action_result

        except Exception as e:
            raise e

        finally:
            terminal_lock.release()

class Book(BaseModel):
    class Controller(BaseModel):

        class Null(BaseModel):
            type:str = 'Null'
            def send(self,book):
                raise ValueError(f'This is a {self.type} state')
            def close(self,book):
                raise ValueError(f'This is a {self.type} state')
            def changeP(self,book,p):
                raise ValueError(f'This is a {self.type} state')
            def changeTS(self,book,tp,sl):
                raise ValueError(f'This is a {self.type} state')

        class Plan(Null):
            type:str = 'Plan'
            def send(self,book):
                book:Book = book
                res = book._make_order()
                book.state = Book.Controller.Order() if res else Book.Controller.Plan()
            def close(self,book):
                raise ValueError('This is just a Plan')
            def changeP(self,book,p):
                book:Book = book
                book.price_open = p
            def changeTS(self,book,tp,sl):
                book:Book = book
                book.tp,book.sl=tp,sl
            
        class Order(Null):
            type:str = 'Order'
            def send(self,book):
                raise ValueError('This is a exists Order')
            def close(self,book):
                book:Book = book
                res = book._close_order()
                if res : book.state = Book.Controller.Null()
            def changeP(self,book,p):
                book:Book = book
                res = book._changeOrderP(p)
                if res : book.price_open = p

            def changeTS(self,book,tp,sl):
                book:Book = book
                res = book._changeOrderTPSL(tp,sl)
                if res : book.tp,book.sl=tp,sl

        class Position(Null):
            type:str = 'Position'
            def send(self,book):
                raise ValueError('This is a exists Position')
            def close(self,book):
                book:Book = book
                res = book._close_position()
                if res : book.state = Book.Controller.Null()
            def changeP(self,book,p):
                raise ValueError('This is a exists Position, can not change price open')
            def changeTS(self,book,tp,sl):
                book:Book = book
                res = book._changePositionTPSL(tp,sl)
                if res : book.tp,book.sl=tp,sl

    @model_validator(mode='before')
    def check_state_type(cls, values:dict):
        state_data = values.get('state')
        if isinstance(state_data, dict):
            type_map = {
                'Null':cls.Controller.Null,
                'Plan':cls.Controller.Plan,
                'Order':cls.Controller.Order,
                'Position':cls.Controller.Position,
            }
            state_class = type_map.get(state_data.get('type', 'NULL'), cls.Controller.Null)
            values['state'] = state_class(**state_data)
        return values
    
    state: Controller.Null = Controller.Plan()
    symbol: str = ''
    sl: float = 0.0
    tp: float = 0.0
    price_open: float = -1.0
    volume: float = -1.0
    magic:int = 901000
    ticket: int = -1
    is_order: bool = False
    is_position: bool = False
    acc_info: dict = {}

    _book: Any = None# mt5_order_position
    _type: str = ''
    _swap: int = 0

    def as_plan(self):
        self.state = Book.Controller.Plan()
        return self
    
    def send(self):
        self.state.send(self)
        return self
    def close(self):
        self.state.close(self)
        return self
    def changeP(self,p):
        self.state.changeP(self,p)
        return self
    def changeTS(self,tp,sl):
        self.state.changeTS(self,tp,sl)
        return self

    def getBooks(self):
        return [ Book().set_mt5_book(book=op) for op in mt5.orders_get()+mt5.positions_get() ]
    
    def account_info(self):
        # Example operation: Getting account information
        account_info = mt5.account_info()
        if account_info is None:
            raise ValueError("Failed to get account info")
        else:
            return Book(acc_info=account_info._asdict())
        
    def set_mt5_book(self,book):
        self._book = book
        self.symbol = self._book.symbol
        self.sl = self._book.sl
        self.tp = self._book.tp
        self.price_open = self._book.price_open
        self.ticket = self._book.ticket
        self._type = self._book.type
        self.is_order=False
        self.is_position=False
        self._swap = 0
        
        if self._book.__class__.__name__ == "TradeOrder" : 
            self.is_order=True
            self.state = Book.Controller.Order()
        elif self._book.__class__.__name__ == "TradePosition": 
            self.is_position=True
            self.state = Book.Controller.Position()
        if hasattr(self._book,'volume_current'):
            self.is_order=True
            self.volume=self._book.volume_current
        elif hasattr(self._book,'volume'):
            self.is_position=True
            self.volume=self._book.volume
            self._swap = self._book.swap
        else:
            raise 'Unkown type!'
        
        self._book = ''
        return self

    def isBuy(self):
        if self.is_order: 
                return self._type in [mt5.ORDER_TYPE_BUY,mt5.ORDER_TYPE_BUY_LIMIT ,
                                      mt5.ORDER_TYPE_BUY_STOP ,mt5.ORDER_TYPE_BUY_STOP_LIMIT]
        elif self.is_position: 
                return self._type == mt5.POSITION_TYPE_BUY
        return True
    
    def _sendRequest(self, request):    
        result=mt5.order_send(request)
        if result is None or result.retcode != mt5.TRADE_RETCODE_DONE:
            raise ValueError(f'Send request failed: {result}')
            # return False
        
        if result.__class__.__name__ == "OrderSendResult" :
            self.ticket = result.order
            self.is_order=True
            self.state = Book.Controller.Order()

        return True
    
    def _changeOrderP(self, p, auto_tpsl=True):
        if auto_tpsl:
            tp = self.tp + self.price_open - p
            sl = self.sl + self.price_open - p
        else:
            tp,sl = self.tp, self.sl
        request = {
            "action": mt5.TRADE_ACTION_MODIFY,
            "order": self.ticket,
            "price": p,
            "tp": tp,
            "sl": sl
        }
        if self._sendRequest(request):
            if auto_tpsl: self.tp, self.sl = tp,sl
            return True
        else:
            return False
        
    def _changeOrderTPSL(self, tp=0.0,sl=0.0):
        request = {
            "action": mt5.TRADE_ACTION_MODIFY,
            "order": self.ticket,
            "price": self.price_open,
            "tp": tp,
            "sl": sl
        }
        print(request)
        return self._sendRequest(request)

    def _changePositionTPSL(self, tp=0.0,sl=0.0):
        request = {
            "action": mt5.TRADE_ACTION_SLTP,
            "position": self.ticket,
            "symbol": self.symbol,
            "tp": tp,
            "sl": sl
        }
        return self._sendRequest(request)

    def _changeTPSL(self, tp=0.0,sl=0.0):
        if self.is_order: 
            return self._changeOrderTPSL(tp,sl)
        elif self.is_position: 
            return self._changePositionTPSL(tp,sl)
        return False
    
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
            "position": self.ticket,
            "price": price,
            "deviation": deviation,
            "magic": self.magic,
            "comment": "script close",
            "type_time": mt5.ORDER_TIME_GTC,
            #"type_filling": mt5.ORDER_FILLING_IOC,
        }
        return self._sendRequest(request)

    def _close_order(self):
        #https://www.mql5.com/en/forum/365968
        request = {
            "action": mt5.TRADE_ACTION_REMOVE,
            "order": self.ticket,
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_IOC,
        }
        return self._sendRequest(request)

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
            "volume": self.volume,
            "type": order_type,
            "price": self.price_open,
            "sl": self.sl,
            "tp": self.tp,
            "deviation": deviation,
            "magic": self.magic,
            "comment": "auto order",
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_RETURN,
        }
        return self._sendRequest(request)
    
class BookService(ServiceOrientedArchitecture):
    class Model(ServiceOrientedArchitecture.Model):
        class Param(BaseModel):
            account:MT5Account
            book:Book

        class Args(BaseModel):
            p:float=-1.0 #price
            tp:float=0.0
            sl:float=0.0

        class Return(BaseModel):
            books:list[Book] = []
        
        param:Param
        args:Args = Args()
        ret:Return = Return()

        @staticmethod
        def build(acc:MT5Account,book:Book,plan=False):
            if isinstance(acc, dict):
                acc = MT5Account(**acc)
            if isinstance(book, dict):
                book = Book(**book)
            if plan:book = book.as_plan()
            param = BookService.Model.Param(account=acc,book=book)
            return BookService.Model(param=param)
        
    class Action(MT5Action, ServiceOrientedArchitecture.Action):
        def __call__(self, *args, **kwargs):
            super().__call__(*args, **kwargs)
            res = MT5Manager().get_singleton().do(self)
            if isinstance(res,Book):
                res = [res]
            self.model.ret.books = res
            return self.model
        
        def __init__(self, model=None):
            if isinstance(model, dict):
                # Remove keys with None values from the dictionary
                nones = [k for k, v in model.items() if v is None]
                for i in nones:
                    del model[i]
                # Initialize the model as an instance of BookService.Model
                model = BookService.Model(**model)
            # Store the model instance
            self.model: BookService.Model = model
            account = self.model.param.account

            super().__init__(account)
            self.book = self.model.param.book

        def change_run(self, func_name, kwargs):
            self.model.args = BookService.Model.Args(**kwargs)
            self.book_run = lambda: getattr(self.book, func_name)(**kwargs)
            return self

        def run(self):
            # tbs = {f'{b.symbol}-{b.price_open}-{b.volume}':b.model_dump() for b in Book().getBooks()}
            return self.book_run()
        

# @descriptions('Retrieve MT5 last N bars data in MetaTrader 5 terminal.',
#             # account='MT5Account object for login.',
#             # symbol='Financial instrument name (e.g., EURUSD).',
#             # timeframe='Timeframe from which the bars are requested. {M1, H1, ...}',
#             # # start_pos='Index of the first bar to retrieve.',
#             # count='Number of bars to retrieve.'
#             )
class MT5CopyLastRatesService:
    class Model(ServiceOrientedArchitecture.Model):
        class Param(BaseModel):
            account: MT5Account = None
        
        class Args(BaseModel):
            symbol: str = "null"
            timeframe: str = "H1"
            count: int = 10
            debug: bool = False
            retry_times_on_error: int = 3

        class Return(BaseModel):
            symbol: str = "null"
            timeframe: str = "H1"
            count: int = 10
            rates: list = None
            digitsnum: int = 0
            error: tuple = None
            header: str='```{symbol} {count} Open, High, Low, Close (OHLC) data points for the {timeframe} timeframe\n{join_formatted_rates}\n```'

            def __str__(self):
                if self.rates is None:
                    return f"Error: {self.error}"

                if self.digitsnum > 0:
                    n = self.digitsnum
                    formatted_rates = [
                        f'{r[1]:.{n}f}\n{r[2]:.{n}f}\n{r[3]:.{n}f}\n{r[4]:.{n}f}\n'
                        for r in self.rates
                    ]
                else:
                    formatted_rates = [
                        f'{int(r[1])}\n{int(r[2])}\n{int(r[3])}\n{int(r[4])}\n'
                        for r in self.rates
                    ]

                # Join the formatted rates into a single string
                join_formatted_rates = '\n'.join(formatted_rates)

                # Use the customizable header format to return the final output
                return self.header.format(
                    symbol=self.symbol,
                    count=self.count,
                    timeframe=self.timeframe,
                    join_formatted_rates=join_formatted_rates
                )

        # Set default instances for Param, Args, and Return to enable easy initialization
        param: Param = Param()
        args: Args = Args()
        ret: Return = Return()

        @staticmethod
        def build(acc:MT5Account):
            if isinstance(acc, dict):
                acc = MT5Account(**acc)
            param = MT5CopyLastRatesService.Model.Param(account=acc)
            return MT5CopyLastRatesService.Model(param=param)

    class Action(MT5Action, ServiceOrientedArchitecture.Action):
        _start_pos = 0
        _digitsnum = {
            'AUDJPY': 3, 'CADJPY': 3, 'CHFJPY': 3, 'CNHJPY': 3, 'EURJPY': 3,
            'GBPJPY': 3, 'USDJPY': 3, 'NZDJPY': 3, 'XAUJPY': 0, 'JPN225': 1, 'US500': 1
        }

        def __init__(self, model=None):
            # Remove None values from the model if it's a dictionary
            if isinstance(model, dict):
                model = {k: v for k, v in model.items() if v is not None}
                model = MT5CopyLastRatesService.Model(**model)
            self.model: MT5CopyLastRatesService.Model = model
            account = self.model.param.account
            super().__init__(account)

        def _update_args(self, symbol: str = None, timeframe: str = None, count: int = None, debug: bool = None):
            # Update model args only if provided (fall back to existing ones otherwise)
            self.model.args.symbol = symbol if symbol is not None else self.model.args.symbol
            self.model.args.timeframe = timeframe if timeframe is not None else self.model.args.timeframe
            self.model.args.count = count if count is not None else self.model.args.count
            self.model.args.debug = debug if debug is not None else self.model.args.debug

        def __call__(self, symbol: str, timeframe: str, count: int, debug: bool = False):
            super().__call__()
            self._update_args(symbol, timeframe, count, debug)
            # Perform the MT5 action
            res: MT5CopyLastRatesService.Model = MT5Manager().get_singleton().do(self)
            self.model.ret.symbol = symbol
            self.model.ret.timeframe = timeframe
            self.model.ret.count = count
            return res

        def run(self, symbol: str = None, timeframe: str = None, count: int = None, debug: bool = None):
            self._update_args(symbol, timeframe, count, debug)

            if self.model.args.debug:
                # For debugging, return simple mock values
                self.model.ret.rates = None
                self.model.ret.digitsnum = 3  # Mock value for digits
                return self.model

            # Simplified timeframe mapping using getattr with a fallback
            tf = getattr(mt5, f"TIMEFRAME_{self.model.args.timeframe}", mt5.TIMEFRAME_H1)

            # Get symbol's digit info with default value of 3
            digitsnum = self._digitsnum.get(self.model.args.symbol, 3)

            # Retrieve rates using MT5 API
            rates = mt5.copy_rates_from_pos(self.model.args.symbol, tf, self._start_pos, self.model.args.count)

            if rates is None:
                error_code, error_msg = mt5.last_error()
                raise ValueError(f"Failed to retrieve rates: {error_msg} (Error code: {error_code})")

            # Populate the return model with results
            self.model.ret.rates = rates.tolist()
            self.model.ret.digitsnum = digitsnum
            self.model.ret.error = None

            return self.model


# Example usage:
# model_dict = {
#     "param": {"account": {...}, "retry_times_on_error": 3},
# }
# action = MT5CopyLastRatesService.Action(model=model_dict)
# result = action(symbol="USDJPY", timeframe="H4", count=10)
# print(result)
