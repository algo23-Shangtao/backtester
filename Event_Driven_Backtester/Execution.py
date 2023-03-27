import datetime
from queue import Queue

from abc import ABC, abstractmethod

from Event_Driven_Backtester.Event import FillEvent, OrderEvent

class ExecutionHandler(ABC):
    '''
    模拟broker的交易接口, 接受OrderEvent, 产生FillEvent
    '''
    @abstractmethod
    def execute_order(self, event):
        raise NotImplementedError('should implement execute_order()')
    
class SimulateExecutionHandler(ExecutionHandler):
    '''
    order->fill 没有考虑延迟、滑点和成交比例等
    '''
    def __init__(self, events):
        self.events = events
    
    def execute_order(self, event):
        if event.type == 'ORDER':
            fill_event = FillEvent(datetime.datetime.now(), event.symbol, 'SHFE', event.quantity, event.direction, None)
            self.events.put(fill_event)