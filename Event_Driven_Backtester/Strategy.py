import numpy as np
import pandas as pd
from queue import Queue
from datetime import datetime
from abc import ABC, abstractmethod

from Event_Driven_Backtester.Event import SignalEvent

class Strategy(ABC):
    '''
    所有strategy的父类
    基于bar产生signal
    '''
    @abstractmethod
    def generate_signals(self):
        '''
        接收MarketEvent
        产生SignalEvent
        '''
        raise NotImplementedError("should implement generate_signals()")

class BuyAndHoldStrategy(Strategy):
    def __init__(self, datahandler, portfolio, events):
        '''
        bars: 提供行情数据的DataHandler, 如HistoryCSVDataHandler
        events: 事件队列
        '''
        self.datahandler = datahandler
        self.portfolio = portfolio
        self.symbol_list = self.datahandler.symbol_list
        self.events = events
        self.all_signals = {} # 记录信号
    
    def _buy_and_hold_strategy(self, event):
        signals = []
        for symbol in self.symbol_list:
            ## 查看当前仓位@TODO
            current_position = self.portfolio.current_positions[symbol]
            if current_position[1] == 0.0:
                signal_time = datetime.now() # DataEvent更新时间 + 处理并产生信号的时间@TODO
                signal_type = 'LONG'
                signal_strength = 1
                signals.append(tuple([symbol, signal_time, signal_type, signal_strength]))
        return signals
    
    def generate_signals(self, event):
        if event.type == 'MARKET':
            signals = self._buy_and_hold_strategy(event)
            self.all_signals[event] = signals 
            for signal in signals:
                self.events.put(SignalEvent(signal[0], signal[1], signal[2], signal[3]))
            
