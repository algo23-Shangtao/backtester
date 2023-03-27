import datetime
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
    def update_signal(self, event):
        '''
        处理SignalEvent
        产生OrderEvent
        '''
        raise NotImplementedError('Should implement update_signal()')
    @abstractmethod
    def update_fill(self, event):
        '''
        根据FillEvent更新当前持仓
        '''
        raise NotImplementedError('Should implement update_fill()')
    

class NaivePortfolio(Portfolio):
    '''
    简单的Portfolio class, 以不变的手数下单-即没有risk management & position sizing
    '''
    def __init__(self, bars, events, start_date, initial_capital=100000):
        '''
        bars: 提供行情数据的DataHandler, 如HistoryCSVDataHandler
        events: 事件队列
        '''
        self.bars = bars
        self.events = events
        self.symbol_list = self.bars.symbol_list
        self.start_date = start_date
        self.initial_capital = initial_capital
        #### all_positions: 历史持仓 current_positions: 当前持仓
        self.all_positions = self.construct_all_positions()
        self.current_positions = self.construct_current_positions()
        #### all_holding: 历史净值 current_holding: 当前净值
        self.all_holdings = self.construct_all_holdings()
        self.current_holdings = self.construct_current_holdings()

    def construct_all_positions(self):
        '''
        all:
        开始时间&持仓列表
        将字典放入一个列表
        '''
        d = dict((k,v) for k, v in [(s, 0) for s in self.symbol_list])
        d['datetime'] = self.start_date
        return [d]
    
    def construct_current_positions(self):
        '''
        current:
        当前持仓列表
        '''
        d = dict((k,v) for k, v in [(s, 0) for s in self.symbol_list])
        return d
    
    def construct_all_holdings(self):
        '''
        all:
        开始时间&净值列表&剩余现金&佣金&总净值
        将字典放入一个列表
        '''
        d = dict((k,v) for k, v in [(s, 0.0) for s in self.symbol_list])
        d['datetime'] = self.start_date
        d['cash'] = self.initial_capital
        d['commission'] = 0.0
        d['total'] = self.initial_capital
        return [d]
    def construct_current_holdings(self):
        '''
        current:
        净值列表&剩余现金&佣金&总净值
        '''
        d = dict((k,v) for k, v in [(s, 0.0) for s in self.symbol_list])
        d['cash'] = self.initial_capital
        d['commission'] = 0.0
        d['total'] = self.initial_capital
        return d
    #### 每次心跳，会传入新行情信息，据此更新Portfolio
    def update_timeindex(self, event):
        bars = {}
        for symbol in self.symbol_list:
            bars[symbol] = self.bars.get_latest_bars(symbol, N=1)
        # update all_positions
        dp = dict((k,v) for k, v in [(s, 0.0) for s in self.symbol_list])
        dp['datetime'] = bars[self.symbol_list[0]][0][1]
        for symbol in self.symbol_list:
            dp[symbol] = self.current_positions[symbol]
        self.all_positions.append(dp)
        # update all_holdings
        dh = dict((k,v) for k, v in [(s, 0.0) for s in self.symbol_list])
        dh['datetime'] = bars[self.symbol_list[0]][0][1]
        dh['cash'] = self.current_holdings['cash']
        dh['commission'] = self.current_holdings['commission']
        dh['total'] = self.current_holdings['cash']
        for symbol in self.symbol_list:
            # 计算当前市值
            market_value = self.current_positions[symbol] * bars[symbol][0][2]
            dh[symbol] = market_value
            dh['total'] += market_value
        self.all_holdings.append(dh)
    
    def update_position_from_fill(self, fill):
        '''
        根据成交信息更新仓位
        fill: FillEvent
        '''
        fill_dir = 0
        if fill.direction == 'LONG':
            fill_dir = 1
        if fill.direction == 'SHORT':
            fill_dir = -1
        self.current_positions[fill.symbol] += fill_dir * fill.quantity
    def update_holdings_from_fill(self, fill):
        '''
        根据成交信息更新净值 
        '''
        fill_dir = 0
        if fill.direction == 'LONG':
            fill_dir = 1
        if fill.direction == 'SHORT':
            fill_dir = -1
        fill_cost = self.bars.get_latest_bars(fill.symbol)[0][2] # fill_cost收盘价
        cost = fill_dir * fill_cost * fill.quantity
        self.current_holdings[fill.symbol] += cost
        self.current_holdings['commission'] += fill.commission
        self.current_holdings['cash'] -= (cost + fill.commission)
        self.current_holdings['total'] -= (cost + fill.commission)

    def update_fill(self, event):
        '''
        FillEvent-执行上面的更新逻辑
        '''
        if event.type == 'FILL':
            self.update_position_from_fill(event)
            self.update_holdings_from_fill(event)

    def generate_order(self, signal):
        '''
        此处应该有risk management & position sizing
        简单的buy and hold
        '''
        order = None
        symbol = signal.symbol
        direction = signal.signal_type
        strength = signal.strength

        mkt_quantity = floor(100 * strength) # 默认下单单位为100--此处可以拓展
        cur_quantity = self.current_positions[symbol]
        order_type = 'MKT'

        if direction == 'LONG' & cur_quantity == 0:
            order = OrderEvent(symbol, order_type, mkt_quantity, 'BUY')
        if direction == 'SHORT' & cur_quantity == 0:
            order = OrderEvent(symbol, order_type, mkt_quantity, 'SELL')
        if direction == 'EXIT' & cur_quantity > 0:
            order = OrderEvent(symbol, order_type, abs(cur_quantity), 'SELL')
        if direction == 'EXIT' & cur_quantity < 0:
            order = OrderEvent(symbol, order_type, abs(cur_quantity), 'BUY')    
        return order    
    
    def update_signal(self, event):
        if event.type == 'SIGNAL':
            order_event = self.generate_order(event)
            self.events.put(order_event)

    def create_equity_curve_dataframe(self):
        curve = pd.DataFrame(self.all_holdings)
        curve.set_index('datetime', inplace=True)
        curve['return'] = curve['total'].pct_change()
        curve['equity_curve'] = (1 + curve['return']).cumprod()
        self.equity_curve = curve
    
    def output_summary_stats(self):
        '''
        计算Sharpe ratio & 最大回撤
        '''
        total_return = self.equity_curve['equity_curve'][-1]
        returns = self.equity_curve['returns']
        pnl = self.equity_curve['equity_curve']

        sharpe_ratio = create_sharpe_ratio(returns)
        max_dd, dd_duration = create_drawdowns(pnl)
        return (total_return, sharpe_ratio, max_dd, dd_duration)