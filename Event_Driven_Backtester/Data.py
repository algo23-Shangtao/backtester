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
    def update_bars(self):
        '''
        更新市场行情
        '''
        raise NotImplementedError("should implement update_bars()")

class HistoryCSVDataHandler(DataHandler):
    '''
    读取历史数据
    '''
    def __init__(self, events, csv_dir, symbol_list):
        '''
        params:
        events: 事件队列
        csv_dir: 历史行情数据路径
        symbol_list: 标的ID列表
        '''
        self.events = events
        self.csv_dir = csv_dir
        self.symbol_list = symbol_list
        
        self.symbol_data = {} # 'rb': data_iter
        self.latest_symbol_data = {}
        self.continue_backtest = True

        self._open_convert_csv_files()

    
    def _open_convert_csv_files(self): ####此函数实现读取数据(数据处理)的逻辑
        for symbol in self.symbol_list:
            self.symbol_data[symbol] = pd.read_csv(os.path.join(self.csv_dir, f"{symbol}.csv"), index_col=0).iterrows()# chunksize
            self.latest_symbol_data[symbol] = []
    def _get_new_bar(self, symbol):
        for bar in self.symbol_data[symbol]:
            yield(tuple([symbol, bar[0], bar[1][0]]))
    
    def get_latest_bars(self, symbol, N=1):
        try:
            bars_list = self.latest_symbol_data[symbol]
        except:
            print('This symbol is not available in history data set')
        else:
            return bars_list[-N:]
    def update_bars(self):
        for symbol in self.symbol_list:
            try:
                bar = next(self._get_new_bar(symbol))
            except StopIteration:
                self.continue_backtest = False
            else:
                if bar is not None:
                    self.latest_symbol_data[symbol].append(bar)
        self.events.put(MarketEvent())

