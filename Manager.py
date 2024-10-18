from threading import Lock
import random
import time
import uuid
from typing import Any, Dict, List
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
        if account_server not in self.terminals:
            self.terminals[account_server] = set()
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
                raise ValueError('This is a exists Order, You can close it.')
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
    sl: float = -1.0
    tp: float = -1.0
    price_open: float = -1.0
    volume: float = -1.0
    magic:int = 901000
    ticket: int = -1
    is_order: bool = False
    is_position: bool = False

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
            return "Failed to get account info"
        else:
            return account_info
        
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
    
class BookAction(MT5Action):
    def __init__(self, account: MT5Account, book: Book, retry_times_on_error=3) -> None:
        if type(account) is dict:
            account = MT5Account(**account)
        if type(book) is dict:
            book = Book(**book)
            
        super().__init__(account, retry_times_on_error)
        self.book = book

    def change_run(self, func_name, kwargs):
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
class MT5Rates(BaseModel):
    pass
class MT5CopyLastRatesAction(MT5Action):

    _start_pos=0
    _digitsnum = {'AUDJPY':3,'CADJPY':3,'CHFJPY':3,'CNHJPY':3,'EURJPY':3,
                    'GBPJPY':3,'USDJPY':3,'NZDJPY':3,'XAUJPY':0,'JPN225':1,'US500':1}

    def __init__(self, account: MT5Account, retry_times_on_error=3) -> None:
        if type(account) is dict:
            account = MT5Account(**account)            
        super().__init__(account, retry_times_on_error)

    def run(self,symbol:str,timeframe:str,count:int,
            debug:bool=False,):
        if debug:
            return '```USDJPY H4 OHLC\n\n142.520\n143.087\n142.382\n142.511\n\n142.509\n142.606\n142.068\n142.266\n\n142.173\n142.954\n142.128\n142.688\n\n142.687\n142.846\n142.080\n142.127\n\n142.127\n142.579\n141.643\n142.534\n\n142.537\n143.004\n142.406\n142.945\n\n142.949\n143.370\n142.746\n143.112\n\n143.112\n143.914\n142.940\n143.624\n\n143.624\n144.125\n143.369\n143.966\n\n143.966\n144.397\n143.661\n144.279\n\n144.277\n144.528\n143.699\n143.807\n\n143.808\n144.069\n143.561\n144.041\n\n144.039\n144.072\n142.972\n143.635\n\n143.634\n143.922\n143.326\n143.553\n\n143.547\n143.881\n143.423\n143.818\n\n143.817\n144.190\n143.561\n143.735\n\n143.733\n144.329\n143.532\n144.328\n\n144.327\n145.446\n144.076\n145.370\n\n145.370\n146.261\n145.298\n146.029\n\n146.030\n146.514\n145.967\n146.454\n\n146.454\n147.054\n146.258\n146.992\n\n146.993\n147.240\n146.676\n146.724\n\n146.723\n146.863\n146.301\n146.749\n\n146.749\n146.993\n146.517\n146.772\n\n146.778\n147.179\n146.470\n146.716\n\n146.716\n146.964\n146.578\n146.922\n\n146.922\n146.932\n146.617\n146.646\n\n146.645\n146.681\n146.152\n146.230\n\n146.230\n146.411\n145.917\n146.341\n\n146.342\n148.061\n146.340\n147.975\n```'
        # symbol=self.symbol
        # timeframe=self.timeframe
        # count=self.count
        digitsnum = mt5.symbol_info(symbol).digits
        tf = {   'M1':mt5.TIMEFRAME_M1,
                        'M2':mt5.TIMEFRAME_M2,
                        'M3':mt5.TIMEFRAME_M3,
                        'M4':mt5.TIMEFRAME_M4,
                        'M5':mt5.TIMEFRAME_M5,
                        'M6':mt5.TIMEFRAME_M6,
                        'M10':mt5.TIMEFRAME_M10,
                        'M12':mt5.TIMEFRAME_M12,
                        'M12':mt5.TIMEFRAME_M12,
                        'M20':mt5.TIMEFRAME_M20,
                        'M30':mt5.TIMEFRAME_M30,
                        'H1':mt5.TIMEFRAME_H1,
                        'H2':mt5.TIMEFRAME_H2,
                        'H3':mt5.TIMEFRAME_H3,
                        'H4':mt5.TIMEFRAME_H4,
                        'H6':mt5.TIMEFRAME_H6,
                        'H8':mt5.TIMEFRAME_H8,
                        'H12':mt5.TIMEFRAME_H12,
                        'D1':mt5.TIMEFRAME_D1,
                        'W1':mt5.TIMEFRAME_W1,
                        'MN1':mt5.TIMEFRAME_MN1,
                    }.get(timeframe,mt5.TIMEFRAME_H1)
        # Retrieve the bar data from MetaTrader 5
        rates = mt5.copy_rates_from_pos(symbol, tf, self._start_pos, count)
        if rates is None:
            return None, mt5.last_error()  # Return error details if retrieval fails
        if digitsnum>0:
            return '\n'.join([f'```{symbol} {count} Open, High, Low, Close (OHLC) data points for the {timeframe} timeframe\n']+[f'{r[1]:.{digitsnum}f}\n{r[2]:.{digitsnum}f}\n{r[3]:.{digitsnum}f}\n{r[4]:.{digitsnum}f}\n' for r in rates]+['```'])
        else:
            return '\n'.join([f'```{symbol} {count} Open, High, Low, Close (OHLC) data points for the {timeframe} timeframe\n']+[f'{int(r[1])}\n{int(r[2])}\n{int(r[3])}\n{int(r[4])}\n' for r in rates]+['```'])
