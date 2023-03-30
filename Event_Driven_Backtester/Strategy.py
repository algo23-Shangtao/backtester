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
        self.events = events
        self._init_params()

    def _init_params(self):
        self.symbol_list = self.datahandler.symbol_list
        self.all_signals = {} # 记录所有时间所有合约的信号 格式: {时间: signals}
        self.current_signals = [] # 记录当前时间所有合约的信号
    
    def _buy_and_hold_strategy(self, event):
        self.current_signals = [] # 清除上一次的信号
        for symbol in self.symbol_list:
            bar_list = self.datahandler.get_latest_bars(symbol)
            new_bar = bar_list[-1]
            ## 查看当前仓位@TODO
            current_position = self.portfolio.current_positions[symbol]
            if current_position.position == 0.0:
                signal_time = new_bar.datetime   # new_bar更新时间 + 处理并产生信号的时间@TODO
                signal_type = 'LONG'
                signal_strength = 1
                signal = SignalEvent(symbol, signal_time, signal_type, signal_strength)
                self.current_signals.append(signal)
    
    def generate_signals(self, event):
        if event.type == 'MARKET':
            self._buy_and_hold_strategy(event)
            self.all_signals[event] = self.current_signals # 更新all_signals
            for signal in self.current_signals:
                self.events.put(signal)
            
