import pandas as pd


#### 向量式回测
def MA_strategy_df(data):
    # 三个交易日-分钟收盘价数据
    # 此分钟结束时，若MA5>=MA20，则此时平仓后买开1手，下一分钟仓位为1
    # 此分钟结束时，若MA<MA20，则此时平仓后卖开1手，下一分钟仓位为-1
    # rb一手十吨
    capital = 10000
    multiplier = 10
    data = pd.read_csv('/home/tushetou/python_codes/data/rb.csv')
    data['datetime'] = pd.to_datetime(data['datetime'])
    data.set_index('datetime', inplace=True)
    data['MA5'] = data['close'].rolling(5).mean()
    data['MA20'] = data['close'].rolling(20).mean()
    data['MA5>=MA20'] = data.apply(lambda x: float('nan') if pd.isna(x['MA20']) else (True if x['MA5'] >= x['MA20'] else False), axis=1)
    data['this_min_position'] = data['MA5>=MA20'].apply(lambda x: 1 if x == True else(-1 if x == False else float('nan'))).shift(1)
    data['this_min_pnl'] = data['close'].diff() * multiplier * data['this_min_position']
    data['this_min_cum_pnl'] = data['this_min_pnl'].cumsum()
    data['this_min_capital'] = data.apply(lambda x: capital if pd.isna(x['this_min_cum_pnl']) else capital + x['this_min_cum_pnl'], axis=1)
    data['this_min_return'] = data['this_min_pnl'] / data['this_min_capital'].shift(1)
    data['net_return'] = data.apply(lambda x: 1 + x['this_min_cum_pnl'] / capital, axis=1)
    return data.copy()