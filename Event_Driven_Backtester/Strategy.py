import datetime
import numpy as np
import pandas as pd
from queue import Queue

from abc import ABC, abstractmethod

from Event_Driven_Backtester.Event import SignalEvent

class Strategy(ABC):
    '''
    所有strategy的父类
    基于行情数据bar产生信号signal
    '''
    @abstractmethod
    def calculate_signals(self):
        raise NotImplementedError("should implement calculate_signals()")

class BuyAndHoldStrategy(Strategy):
    def __init__(self, bars, events):
        '''
        bars: 提供行情数据的DataHandler, 如HistoryCSVDataHandler
        events: 事件队列
        '''
        self.bars = bars
        self.symbol_list = self.bars.symbol_list
        self.events = events
        self.bought = self._caculate_initial_bought()
    #### 初始化下单列表
    def _caculate_initial_bought(self):
        bought = {}
        for symbol in self.symbol_list:
            bought[symbol] = False
        return bought
    #### buy and hold strategy
    def calculate_signals(self, event):
        if event.type == 'MARKET':
            for symbol in self.symbol_list:
                bar = self.bars.get_latest_bars(symbol, N=1)
                if bar is not None and bar != []:
                    signal = SignalEvent(bar[0][0], bar[0][1], 'LONG')
                    self.events.put(signal)
                    self.bought[symbol] = True
