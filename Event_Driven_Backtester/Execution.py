from datetime import datetime
from queue import Queue

from abc import ABC, abstractmethod

from Event_Driven_Backtester.Event import FillEvent

class ExecutionHandler(ABC):
    '''
    接收OrderEvent
    产生FillEvent
    '''
    @abstractmethod
    def execute_order(self, event):
        raise NotImplementedError('should implement execute_order()')
    
class SimulateExecutionHandler(ExecutionHandler):
    '''
    order->fill 考虑延迟、滑点和成交比例
    '''
    def __init__(self, datahandler, portfolio, events):
        self.datahandler = datahandler
        self.portfolio = portfolio
        self.events = events

    def _match_order(self, order):
        fill_time = datetime.now()
        symbol = order.symbol
        price = order.order_info[1]
        quantity = order.order_info[2] # order_info = tuple([order_type, order_price, order_quantity])
        direction = order.direction
        return tuple([fill_time, symbol, tuple([price, quantity]), direction])

    def execute_order(self, event): # datetime, symbol, order_info, direction, commission=None
        if event.type == 'ORDER':
            fill = self._match_order(event)
            self.events.put(FillEvent(fill[0], fill[1], fill[2], fill[3]))
    
    def update_bar(self):
        pass