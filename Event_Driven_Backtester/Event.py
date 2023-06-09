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
    def __init__(self, symbol, datetime, signal_type, strength, strategy_id=1):
        '''
        params:
        symbol: 标的ID
        datetime: 信号产生时间
        signal_type: 信号类型-Long or Short
        strength: 信号强度
        strategy_id: 产生信号的策略id
        '''
        self.type = 'SIGNAL'
        self.symbol = symbol
        self.datetime = datetime
        self.signal_type = signal_type
        self.strength = strength
        self.strategy_id = strategy_id

class OrderEvent(Event):
    '''
    下单给交易所
    '''
    def __init__(self, datetime, symbol, order_type, order_price, order_quantity, direction): # order_info = tuple([order_type, order_price, order_quantity])
        '''
        params:
        datetime: 下单时间
        symbol: 标的ID
        order_type price quantity: 下单类型、价格、数量
        direction: 下单方向 - 1(做多), -1(做空), 0(不下单)
        '''
        self.type = 'ORDER'
        self.datetime = datetime    
        self.symbol = symbol
        self.order_type = order_type
        self.order_price = order_price
        self.order_quantity = order_quantity
        self.direction = direction
    def print_order(self):
        print(f"symbol: {self.symbol} datetime: {self.datetime} order_type: {self.order_type} price: {self.price} quantity: {self.quantity} direction: {self.direction}")

class FillEvent(Event):
    '''
    交易所给成交结果
    '''
    def __init__(self, datetime, symbol, fill_type, price, quantity, direction, commission=None):
        '''
        params:
        datetime: 订单成交时间
        symbol: 标的ID
        order_info: 成交价格、成交数量
        direction: 成交方向 - 1(做多), -1(做空), 0(下单失败)
        commission: broker佣金
        '''
        self.type = 'FILL'
        self.datetime = datetime
        self.symbol = symbol
        self.fill_type = fill_type
        self.price = price
        self.quantity = quantity
        self.direction = direction
        self.commission = commission
    
    def calculate_commission(self):
        '''
        计算佣金
        '''
        full_cost = 1.3
        if self.quantity <= 500:
            full_cost = max(1.3, 0.013 * self.quantity)
        else:
            full_cost = max(1.3, 0.008 * self.quantity)
        full_cost = min(full_cost, 0.5 / 100.0 * self.quantity * self.price * self.quantity)#????
        return full_cost
