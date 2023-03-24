class Event(object):
    '''
    所有Event的父类
    '''
    pass

class MarketEvent(Event):
    '''
    市场行情更新
    '''
    def __init__(self):
        self.type = 'MARKET'

class SignalEvent(Event):
    '''
    交易信号产生
    '''
    def __init__(self, symbol, datetime, signal_type):
        '''
        params:
        symbol: 标的ID
        datetime: 信号产生时间
        signal_type: 信号类型-Long or Short(Buy or Sell)
        '''
        self.type = 'SIGNAL'
        self.symbol = symbol
        self.datetime = datetime
        self.signal_type = signal_type

class OrderEvent(Event):
    '''
    下单给交易所
    '''
    def __init__(self, symbol, order_type, quantity, direction):
        '''
        params:
        symbol: 标的ID
        order_type: 下什么单-Market order or Limit order
        quantity: 下多少手
        direction: 下单方向-Long or Short(Buy or Sell)
        '''    
        self.type = 'ORDER'
        self.symbol = symbol
        self.order_type = order_type
        self.quantity = quantity
        self.direction = direction
    def print_order(self):
        print(f"symbol: {self.symbol} order_type: {self.order_type} quantity: {self.quantity} direction: {self.direction}")

class FillEvent(Event):
    '''
    交易所给成交结果
    '''
    def __init__(self, timeindex, symbol, exchange, quantity, direction, fill_cost, commission=None):
        '''
        params:
        timeindex: 订单成交时间
        symbol: 标的ID
        exchange: 交易所ID
        quantity: 成交多少手
        direction: 成交方向-Long or Short(Buy or Sell)
        fill_cost: 交易成本???
        commission: broker佣金
        '''
        self.type = 'FILL'
        self.timeindex = timeindex
        self.symbol = symbol
        self.exchange = exchange
        self.quantity = quantity
        self.direction = direction
        self.fill_cost = fill_cost
    def calculate_commission(self):
        '''
        计算佣金
        '''
        full_cost = 1.3
        if self.quantity <= 500:
            full_cost = max(1.3, 0.013 * self.quantity)
        else:
            full_cost = max(1.3, 0.008 * self.quantity)
        full_cost = min(full_cost, 0.5 / 100.0 * self.quantity * self.fill_cost)#????
        return full_cost
