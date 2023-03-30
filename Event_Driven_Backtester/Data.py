import datetime
import os, os.path
import pandas as pd

from abc import ABC, abstractmethod
from Event_Driven_Backtester.Event import MarketEvent

class DataHandler(ABC):
    '''
    所有数据提供场景(历史or实盘)的父类
    为需要的合约提供tick数据
    '''
    @abstractmethod
    def get_latest_bars(self, symbol, N=1):
        '''
        返回指定标的N个bar的数据
        '''
        raise NotImplementedError("should implement get_latest_bars()")
    @abstractmethod
    def publish_bar(self):
        '''
        更新市场行情
        '''
        raise NotImplementedError("should implement publish_bar()")

class HistoryCSVDataHandler(DataHandler):
    '''
    读取历史数据
    '''
    def __init__(self, events, csv_dir='/home/tushetou/python_codes/data', symbol_list=['rb']):
        '''
        params:
        events: 事件队列
        csv_dir: 历史行情数据路径
        symbol_list: 标的ID列表
        '''
        self.events = events
        self.csv_dir = csv_dir
        self.symbol_list = symbol_list
        
        self.symbol_data_iter = {} # symbol_data_iter 格式: {'rb': data_iter}
        self.symbol_data = {} # symbol_data 格式: {'rb': [new_bar1, new_bar2, ...]}
        self.continue_backtest = True

        self._read_data()

    def _read_data(self): 
        for symbol in self.symbol_list:
            self.symbol_data_iter[symbol] = pd.read_csv(os.path.join(self.csv_dir, f"{symbol}.csv")).itertuples()# chunksize
            self.symbol_data[symbol] = []

    def _generate_new_bar(self, symbol): # new_bar 格式: tuple(symbol, datetime, close)
        for new_bar_data in self.symbol_data_iter[symbol]:
            yield(tuple([symbol, getattr(new_bar_data, 'datetime'), getattr(new_bar_data, 'close')])) # tuple is better than list

    def publish_bar(self):
        for symbol in self.symbol_list:
            try:
                new_bar = next(self._generate_new_bar(symbol))
            except StopIteration:
                self.continue_backtest = False
            else:
                if new_bar is not None: # ?
                    self.symbol_data[symbol].append(new_bar) # 一份存在symbol_data
        self.events.put(MarketEvent()) # 一份存在MarketEvent

    def get_latest_bars(self, symbol, N=1):
        try:
            bars_list = self.symbol_data[symbol]
        except:
            print('This symbol is not available in history data set')
        else:
            return bars_list[-N:] #bars 格式 [new_bar1, ..., new_barN] new_bar 格式: tuple(symbol, datetime, close)

