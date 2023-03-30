import numpy as np
import pandas as pd
from queue import Queue

from abc import ABC, abstractmethod
from math import floor

from Event_Driven_Backtester.Event import OrderEvent, FillEvent
from Event_Driven_Backtester.Performance import create_drawdowns, create_sharpe_ratio
from Event_Driven_Backtester.datastructure import *

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
    def __init__(self, datahandler, events, initial_capital=32410):
        '''
        bars: 提供行情数据的DataHandler, 如HistoryCSVDataHandler
        events: 事件队列
        '''
        self.datahandler = datahandler
        self.events = events
        self.initial_capital = initial_capital
        self._init_params()
    
    def _init_params(self):
        self.symbol_list = self.datahandler.symbol_list
        self.all_positions = {} # {symbol: {datetime: (direction, position), ... ,}, ... ,} # 每个合约一个账户
        self.current_positions = dict((key, value) for key, value in [(symbol, position_info()) for symbol in self.symbol_list]) # {symbol: (direction, position), ... ,} 
        self.all_capital = {} # {symbol: {datetime: market_value, ... ,}, ... ,equity: {datetime: all_market_value}, cash: {datetime: current_cash}}
        self.all_capital['equity'] = {}
        self.all_capital['cash'] = {}
        self.current_capital = {} # {'rb': market_value_rb, 'ag': market_value_ag, ... ,'equity': all_market_value, 'cash': current_cash }
        self.current_capital['equity'] = 0.0
        self.current_capital['cash'] = self.initial_capital
        for symbol in self.symbol_list:
            self.all_positions[symbol] = {}
            self.all_capital[symbol] = {}
            self.current_capital[symbol] = 0.0


    def _naive_order_generator(self, signal):
        '''
        此处应该有risk management & position sizing
        '''
        symbol = signal.symbol
        current_position = self.current_positions[symbol] # position_info()
        new_bar = self.datahandler.get_latest_bars(symbol)[-1] # new_bar()
        datetime = signal.datetime ####TODO 下单时间 = 信号产生时间 + 程序运行时间
        last_price = new_bar.close 
        if current_position.position == 0: # 未持仓
            order_type = 'limit_order'
            order_price = last_price
            order_quantity = floor(10 * signal.strength)
            direction = 1 if signal.signal_type == 'LONG' else -1
            order_cost = order_price * order_quantity * direction
            if self.current_capital['cash'] >= order_cost:
                order = OrderEvent(datetime, symbol, order_type, order_price, order_quantity, direction)
            else:
                order_type = 'cancel_order'
                order = OrderEvent(datetime, symbol, order_type, order_price, order_quantity, direction)
                print('资金不足, 订单取消') ####TODO 学习使用logger模块
        elif current_position.direction == signal.signal_type: # 信号方向与持仓方向一致
            order_type = 'cancel_order'
            order_price = last_price
            order_quantity = None
            direction = 0
            order = OrderEvent(datetime, symbol, order_type, order_price, order_quantity, direction)
        else: # 信号方向与持仓方向相反
            order_type = 'limit_order'
            order_price = last_price
            order_quantity = current_position.position + floor(10 * signal.strength) # 平现仓再开新仓 
            direction = 1 if signal.signal_type == 'LONG' else -1
            order_cost = order_price * order_quantity * direction
            if self.current_capital['cash'] > order_cost:
                order = OrderEvent(datetime, symbol, order_type, order_price, order_quantity, direction)
            else:
                order_type = 'cancel_order'
                order = OrderEvent(datetime, symbol, order_type, order_price, order_quantity, direction)            
        return order   
    
    def generate_order(self, event):
        if event.type == 'SIGNAL':
            order = self._naive_order_generator(event)
            if order.type != 'cancel_order':
                self.events.put(order)
    
    
    def _update_capital_bar(self):
        self.current_capital['equity'] = 0
        for symbol in self.symbol_list:
            new_bar = self.datahandler.get_latest_bars(symbol)[-1]
            datetime = new_bar.datetime
            last_price = new_bar.close
            direction = self.current_positions[symbol].direction
            position = self.current_positions[symbol].position
            self.current_capital[symbol] = direction * last_price * position
            self.current_capital['equity'] += self.current_capital[symbol]
            self.all_capital[symbol][datetime] = self.current_capital[symbol]
            self.all_capital['equity'][datetime] = self.current_capital['equity']
            self.all_capital['cash'][datetime] = self.current_capital['cash']
        
        
    
    def update_bar(self):
        self._update_capital_bar()


    def _update_position_fill(self, fill): # datetime, symbol, order_info(price quantity), direction, commission=None
        # datetime, symbol, fill_type, fill_price, fill_quantity, direction, commission
        datetime, symbol, fill_type, price, quantity, direction,commission = fill.datetime, fill.symbol, fill.fill_type, fill.price, fill.quantity, fill.direction, 0 # fill.calculate_commission()
        # self.current_positions{symbol: (direction, position, market_value), ... ,} 
        if fill_type == 'fail':
            return
        current_position = self.current_positions[symbol]
        new_position = current_position.position + direction * quantity
        new_direction = 1 if new_position > 0 else -1
        new_position = abs(new_position)
        self.current_positions[symbol] = position_info(new_direction, new_position)
        self.all_positions[symbol][datetime] = self.current_positions[symbol]

    def _update_capital_fill(self, fill):
        datetime, symbol, fill_type, price, quantity, direction,commission = fill.datetime, fill.symbol, fill.fill_type, fill.price, fill.quantity, fill.direction, 0 # fill.calculate_commission()
        fill_cost = price * quantity * direction + commission # 多头为正，空头为负
        new_bar = self.datahandler.get_latest_bars(symbol)[-1]
        last_price = new_bar.close
        new_market_value = self.current_positions[symbol].direction * self.current_positions[symbol].position * last_price
        new_cash = self.current_capital['cash'] - fill_cost
        old_market_value = self.current_capital[symbol]
        self.current_capital[symbol] = new_market_value
        self.current_capital['equity'] = self.current_capital['equity'] - old_market_value + new_market_value
        self.current_capital['cash'] = new_cash
        self.all_capital[symbol][datetime] = new_market_value
        self.all_capital['equity'][datetime] = self.current_capital['equity']
        self.all_capital['cash'][datetime] = new_cash
        
    
    def update_fill(self, event):
        if event.type == "FILL":
            self._update_position_fill(event)
            self._update_capital_fill(event)

# all_capital: {symbol: {datetime: market_value, ... ,}, ... ,equity: {datetime: all_market_value}, cash: {datetime: current_cash}}
    def create_equity_curve_dataframe(self):
        equity_df = pd.DataFrame(self.all_capital['equity'].values(), index=self.all_capital['equity'].keys())
        cash_df = pd.DataFrame(self.all_capital['cash'].values(), index=self.all_capital['cash'].keys())
        capital_df = pd.concat([equity_df, cash_df], axis=1)
        capital_df.columns = ['equity', 'cash']
        capital_df['returns'] = (capital_df['equity'] + capital_df['cash']).pct_change().fillna(0)
        capital_df['cum_curve'] = (1 + capital_df['returns']).cumprod()
        self.capital_df = capital_df
    
    def output_summary_stats(self):
        '''
        计算Sharpe ratio & 最大回撤
        '''
        self.create_equity_curve_dataframe()
        total_return = self.capital_df['cum_curve'][-1]
        returns = self.capital_df['returns']
        pnl = self.capital_df['cum_curve']

        sharpe_ratio = create_sharpe_ratio(returns)
        max_dd, dd_duration = create_drawdowns(pnl)
        return (total_return, sharpe_ratio, max_dd, dd_duration)