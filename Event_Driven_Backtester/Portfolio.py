from datetime import datetime
import numpy as np
import pandas as pd
from queue import Queue

from abc import ABC, abstractmethod
from math import floor

from Event_Driven_Backtester.Event import OrderEvent, FillEvent
from Event_Driven_Backtester.Performance import create_drawdowns, create_sharpe_ratio

class Portfolio(ABC):
    '''
    所有Portfolio的父类
    '''
    @abstractmethod
    def generate_order(self, event):
        '''
        接收SignalEvent
        产生OrderEvent
        '''
        raise NotImplementedError('Should implement generate_order()')
    @abstractmethod
    def update_fill(self, event):
        '''
        根据FillEvent更新仓位信息
        '''
        raise NotImplementedError('Should implement update_fill()')
    @abstractmethod
    def update_bar(self):
        '''
        根据MarketEvent更新仓位信息
        '''
        raise NotImplementedError('Should implement update_bar()')
    

class NaivePortfolio(Portfolio):
    '''
    简单的Portfolio class, 以不变的手数下单-即没有risk management & position sizing
    '''
    def __init__(self, datahandler, events, initial_capital=100000):
        '''
        bars: 提供行情数据的DataHandler, 如HistoryCSVDataHandler
        events: 事件队列
        '''
        self.datahandler = datahandler
        self.events = events
        self.symbol_list = datahandler.symbol_list
        self.initial_capital = initial_capital
        self.all_positions = dict() # {datetime : current_positions, ... ,}
        self.current_positions = dict((key, value) for key, value in [(symbol, tuple([0, 0, 0])) for symbol in self.symbol_list]) # {symbol: (direction, position, market_value), ... ,} 
        self.all_capital = dict() # {datetime : current_capital}
        self.current_capital = tuple([0.0, initial_capital]) # tuple([all_market_value, cash])

    def _naive_order_generator(self, signal):
        '''
        此处应该有risk management & position sizing
        '''
        symbol = signal.symbol
        current_position = self.current_positions[symbol]
        new_bar = self.datahandler.get_latest_bars(symbol)[0] # tuple(symbol, datetime, close)
        last_price = new_bar[2] 
        if len(current_position) == 0: # 未持仓
            order_type = 'limit_order'
            order_price = last_price
            order_quantity = floor(100 * signal.strength)
            direction = 1 if signal.signal_type == 'LONG' else -1
            order_info = tuple([order_type, order_price, order_quantity]) # 开仓
        elif current_position[0] == signal.signal_type: # 信号方向与持仓方向一致
            order_type = None
            order_price = last_price
            order_quantity = None
            direction = 0
            order_info = tuple([order_type, order_price, order_quantity])
        else: # 信号方向与持仓方向相反
            order_type = 'limit_order'
            order_price = last_price
            order_quantity = current_position[1] + floor(100 * signal.strength)
            direction = 1 if signal.signal_type == 'LONG' else -1
            order_info = tuple([order_type, order_price, order_quantity]) # 平现仓再开新仓           
        
        order_time = datetime.now() ##TODO
        return tuple([order_time, symbol, order_info, direction])    
    
    def generate_order(self, event):
        if event.type == 'SIGNAL':
            order = self._naive_order_generator(event)
            if order[3] != 0:
                self.events.put(OrderEvent(order[0], order[1], order[2], order[3]))
    

    def _update_position_bar(self):
        for symbol in self.symbol_list: # 更新current_position
            new_bar = self.datahandler.get_latest_bars(symbol)[0] # tuple(symbol, datetime, close)
            last_price = new_bar[2]
            current_position = self.current_positions[symbol]
            direction = current_position[0]
            position = current_position[1]
            market_value = last_price * direction * position
            self.current_positions[symbol] = tuple([direction, position, market_value])
        new_time = self.datahandler.get_latest_bars(self.symbol_list[0])[0][1]
        self.all_positions[new_time] = self.current_positions
    
    def _update_capital_bar(self):
        current_market_value = 0.0
        current_cash = self.current_capital[1]
        for symbol in self.symbol_list:
            current_market_value += self.current_positions[symbol][2]
        self.current_capital = tuple([current_market_value, current_cash])
        new_time = self.datahandler.get_latest_bars(self.symbol_list[0])[0][1]
        self.all_capital[new_time] = self.current_capital
    
    def update_bar(self):
        self._update_position_bar()
        self._update_capital_bar()

    def _update_position_fill(self, fill): # datetime, symbol, order_info(price quantity), direction, commission=None
        fill_time, fill_symbol,  fill_quantity, fill_direction, = fill.datetime, fill.symbol, fill.order_info[1], fill.direction
        # self.current_positions{symbol: (direction, position, market_value), ... ,} 
        current_position = self.current_positions[fill_symbol]
        position = current_position[1] + fill_direction * fill_quantity
        direction = 1 if position > 0 else -1
        position = abs(position)
        bar = self.datahandler.get_latest_bars(fill_symbol)[0] # tuple(symbol, datetime, close)
        last_price = bar[2]
        market_value = last_price * direction * position
        self.current_positions[fill_symbol] = tuple([direction, position, market_value])
        self.all_positions[fill_time] = self.current_positions

    def _update_capital_fill(self, fill):
        fill_time, fill_price, fill_quantity, fill_direction, fill_commission = fill.datetime, fill.order_info[0], fill.order_info[1], fill.direction, fill.calculate_commission()
        fill_cost = fill_price * fill_quantity * fill_direction + fill_commission # 多头为正，空头为负
        current_market_value = 0.0
        current_cash = self.current_capital[1] - fill_cost
        for symbol in self.symbol_list:
            current_market_value += self.current_positions[symbol][2]
        self.current_capital = tuple([current_market_value, current_cash])
        self.all_capital[fill_time] = self.current_capital        
        
    
    def update_fill(self, event):
        if event.type == "FILL":
            self._update_position_fill(event)
            self._update_capital_fill(event)


    def create_equity_curve_dataframe(self):
        capital_df = pd.DataFrame(self.all_capital).T
        capital_df.columns = ['all_market_value', 'cash']
        capital_df['returns'] = (capital_df['all_market_value'] + capital_df['cash']).pct_change().fillna(0)
        capital_df['equity_curve'] = (1 + capital_df['returns']).cumprod()
        self.capital_df = capital_df
    
    def output_summary_stats(self):
        '''
        计算Sharpe ratio & 最大回撤
        '''
        self.create_equity_curve_dataframe()
        total_return = self.capital_df['equity_curve'][-1]
        returns = self.capital_df['returns']
        pnl = self.capital_df['equity_curve']

        sharpe_ratio = create_sharpe_ratio(returns)
        max_dd, dd_duration = create_drawdowns(pnl)
        return (total_return, sharpe_ratio, max_dd, dd_duration)