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
        fill_time = order.datetime
        symbol = order.symbol
        price = order.order_price
        quantity = order.order_quantity # order_info = tuple([order_type, order_price, order_quantity])
        direction = order.direction
        return FillEvent(fill_time, symbol, 'success', price, quantity, direction)

    def execute_order(self, event): # datetime, symbol, order_info, direction, commission=None
        if event.type == 'ORDER':
            fill = self._match_order(event)
            self.events.put(fill)
    
    def update_bar(self):
        pass