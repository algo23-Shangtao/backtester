import numpy as np
import pandas as pd

def create_sharpe_ratio(returns, periods=243):
    '''
    params:
    returns: 收益率(pandas.Series)
    periods: 交易期限(243交易日、6.5小时、60分钟、60秒)->用于标准化Sharpe ratio
    '''
    return np.sqrt(periods) * np.mean(returns) / np.std(returns)

def create_drawdowns(equity_curve):
    '''
    计算最大回撤和最大回撤期
    '''
    hwm = [0] # High water mark -- 记录最高位
    eq_idx = equity_curve.index
    drawdowm = pd.Series(index=eq_idx, dtype=float)
    duration = pd.Series(index=eq_idx, dtype=float)
    for t in range(1, len(eq_idx)):
        cur_hwm = max(hwm[t - 1], equity_curve[t])
        hwm.append(cur_hwm)
        drawdowm[t] = hwm[t] - equity_curve[t]
        duration[t] = 0 if drawdowm[t] == 0 else duration[t - 1] + 1
    return drawdowm.max(), duration.max()