import pandas as pd 
import os

#### 功能：1. CTP原始行情数据、tqsdk历史行情数据--->交易时间段内的tick数据 2. tick数据合成分钟数据 3. 获取主力合约转换信息
#### (@TODO 1.数据格式统一--统一字段以及类型 2. 读取日频数据，节约tick数据储存 3. 数据库存储、读取历史行情数据 4. 提高读取历史行情数据效率 5. 测试合成日数据等频率)


def get_trading_time (asset='rb'):
    ###refer to : http://qhsxf.com/%E6%9C%9F%E8%B4%A7%E4%BA%A4%E6%98%93%E6%97%B6%E9%97%B4.html
    CZCE_NIGHT1 = ['fg', 'sa', 'ma', 'sr', 'ta', 'rm', 'oi', 'cf', 'cf', 'cy', 'pf', 'zc']
    CZCE_NIGHT2 = ['sm', 'sf', 'wh', 'jr', 'lr', 'pm', 'ri', 'rs', 'pk', 'ur', 'cj', 'ap']
    DCE_NIGHT1 = ['i', 'j', 'jm', 'a', 'b', 'ma', 'p', 'y', 'c', 'cs', 'pp', 'v', 'eb', 'eg', 'pg', 'rr', 'i']
    DCE_NIGHT2 = ['bb', 'fb' 'Ih', 'jd']
    SHFE_NIGHT1 = ['cu', 'pb', 'al', 'zn', 'sn', 'ni', 'ss']
    SHFE_NIGHT2 = ['fu', 'ru', 'bu', 'sp', 'rb', 'hc']
    SHFE_NIGHT3 = ['au', 'ag']
    SHFE_NIGHT4 = ['wr']
    GFEX_NIGHT = ['si']
    INE_NIGHT1 = ['sc']
    INE_NIGHT2 = ['bc']
    INE_NIGHT3 = ['lu', 'nr']
    CFFEX_DAY1  = ['if', 'ih', 'ic', 'im']
    CFFEX_DAY2 = ['t', 'tf', 'ts']
    if asset in CZCE_NIGHT1 or asset in DCE_NIGHT1 or asset in SHFE_NIGHT2 or asset in INE_NIGHT3: 
        return ("21:00:00.500", "23:00:00.000", "09:00:00.500", "10:15:00.000", "10:30:00.500", "11:30:00.000", "13:30:00.500","15:00:00.000")
    if asset in SHFE_NIGHT1 or asset in INE_NIGHT2:
        return ("21:00:00.500", "01:00:00.000", "09:00:00.500", "10:15:00.000", "10:30:00.500", "11:30:00.000", "13:30:00.500","15:00:00.000")
    if asset in SHFE_NIGHT3 or asset in INE_NIGHT1:
        return ("21:00:00.500", "02:30:00.000", "09:00:00.500", "10:15:00.000", "10:30:00.500", "11:30:00.000", "13:30:00.500","15:00:00.000")
    if asset in CZCE_NIGHT2 or asset in DCE_NIGHT2 or asset in SHFE_NIGHT4 or asset in GFEX_NIGHT:
        return ("99:99:99.500", "99:99:99.000", "09:00:00.500", "10:15:00.000", "10:30:00.500", "11:30:00.000", "13:30:00.500", "15:00:00.000")
    # 暂时不支持中金所
    if asset in CFFEX_DAY1:
        return ("00:00:00.500", "00:00:00.000", "09:30:00.500", "11:30:00.000", "13:00:00.500", "15:00:00.000")
    if asset in CFFEX_DAY2:
        return ("00:00:00.500", "00:00:00.000", "09:15:00.500", "11:30:00.000", "13:00:00.500", "15:15:00.000")

#### 从CTP拿到的最原始的行情数据
def get_raw_df(dataPath):
    raw_df = pd.read_csv(dataPath, index_col=False, encoding='gb2312')
    # 是否添加head视情况而定
    # 部分数据日内不变，可以不纳入tick数据内
    raw_df.columns = ["TradingDay", "InstrumentID", "ExchangeID", "ExchangeInstID", "LastPrice", "PreSettlementPrice",
                "PreClosePrice", "PreOpenInterest", "OpenPrice", "HighestPrice", "LowestPrice", "Volume", "Turnover",
                "OpenInterest", "ClosePrice", "SettlementPrice", "UpperLimitPrice", "LowerLimitPrice", "PreDelta",
                "CurrDelta", "UpdateTime", "UpdateMillisec", "BidPrice1", "BidVolume1", "AskPrice1", "AskVolume1",
                "BidPrice2", "BidVolume2", "AskPrice2", "AskVolume2", "BidPrice3", "BidVolume3", "AskPrice3", "AskVolume3",
                "BidPrice4", "BidVolume4", "AskPrice4", "AskVolume4", "BidPrice5", "BidVolume5", "AskPrice5", "AskVolume5", 
                "AveragePrice", "ActionDay"]
    return raw_df
# 将时间字段合并
def merge_time(raw_df):# 输入为时间字段分开的原始数据
    df = raw_df.copy()
    df['UpdateMillisec'] = df['UpdateMillisec'].apply(lambda x: '000' if x == 0 else str(x))
    Datetime = df['ActionDay'].map(str).apply(lambda x: x[0:4] + '-' + x[4:6] + '-' + x[6:]) + ' ' + df['UpdateTime'] + '.' + df['UpdateMillisec']
    
    # df['DateTime'] = pd.to_datetime (DateTime, format='%Y-%m-%d %H:%M:%S.%f')
    df['Datetime'] = Datetime
    df.drop(['TradingDay', 'UpdateTime', 'UpdateMillisec', 'ActionDay'], axis=1, inplace=True)
    return df
# 剔除日频字段
def split_daily_field(raw_df):
    pass
# 剔除非交易时间段内数据
def split_no_trading_time_df(merged, asset = 'rb'):# 输入为时间字段已经合并完成的数据, 且为字符串
    (s0, e0, s1, e1, s2, e2, s3, e3)= get_trading_time(asset)
    hms = merged['Datetime'].apply(lambda x: float('nan') if pd.isna(x) else x[11:])# hms:时分秒
    if s0 <= e0:
        splited_df = merged.loc[((hms >= s0)&(hms <=e0))|((hms >= s1)&(hms <=e1))|((hms >= s2)&(hms <=e2))|((hms >= s3)&(hms <=e3))].copy()
    else:
        splited_df = merged.loc[((hms >= s0)&(hms <='23:59:59.500'))|((hms >= '00:00:00.000')&(hms <=e1))|((hms >= s1)&(hms <=e1))|((hms >= s2)&(hms <=e2))|((hms >= s3)&(hms <=e3))].copy()
    return splited_df
#### 清洗过的交易时间段内的tick数据---以tqsdk数据为例

#### 合成分钟数据-左开右闭
def resample(tick_df, sample_t='1min'):#could be M,D,H,min,S...
    # nan可以分为非交易时间段和交易时间段内未发生交易，应该有不同的处理逻辑
    df = tick_df.copy()
    df['Datetime_dt'] = pd.to_datetime(df['Datetime'])# 字符串日期转换为时间戳
    df.set_index('Datetime_dt', inplace=True)
    # 每分钟最新价的开盘价收盘价最高价最低价
    price_df = df['LastPrice'].resample('1min', label='right', closed='right').ohlc()
    price_df['Datetime'] = df['Datetime'].resample(sample_t, label='right', closed='right').last()
    price_df = split_no_trading_time_df(price_df)# 剔除非交易时间段
    price_df.fillna(method='ffill', inplace=True)# 交易时间段内未发生交易，则延续上一个tick的数据
    # 每分钟最高价的最高值
    high_df = pd.DataFrame(df['HighestPrice'].resample(sample_t, label='right', closed='right').max())
    high_df['Datetime'] = df['Datetime'].resample(sample_t, label='right', closed='right').last()
    high_df = split_no_trading_time_df(high_df)
    high_df.fillna(method='ffill', inplace=True)
    # 每分钟最低价的最低值
    low_df  = pd.DataFrame(df['LowestPrice'].resample(sample_t, label='right', closed='right').min())
    low_df['Datetime'] = df['Datetime'].resample(sample_t, label='right', closed='right').last()
    low_df = split_no_trading_time_df(low_df)
    low_df.fillna(method='ffill', inplace=True)
    # 判断该分钟内最高价最低价与上一分钟相比，是否发生突破
    high_df['ShiftHighestPrice']= high_df['HighestPrice'].shift(1).fillna(0)# 保证第一个tick用最高价最高值而不是最新价最高值
    low_df['ShiftLowestPrice']= low_df['LowestPrice'].shift(1).fillna(100000)# 保证第一个tick用最低价最低值而不是最新价最低值
    high_df['HighPriceChanged'] = high_df.apply(lambda s : True if s['HighestPrice'] > s['ShiftHighestPrice'] else False, axis=1)
    low_df['LowPriceChanged'] = low_df.apply(lambda s : True if s['LowestPrice'] < s['ShiftLowestPrice'] else False, axis=1)
    # 统一放置到price_df, 准备比较
    price_df['HighPriceChanged'] = high_df['HighPriceChanged']
    price_df['HighestPrice'] = high_df['HighestPrice']
    price_df['LowPriceChanged']  = low_df['LowPriceChanged']
    price_df['LowestPrice']  = low_df['LowestPrice']
    # 产出ohlc
    price_df['NewHigh'] = price_df.apply(lambda s : s['HighestPrice'] if s['HighPriceChanged'] else s['high'], axis=1)
    price_df['NewLow']  = price_df.apply(lambda s : s['LowestPrice']  if s['LowPriceChanged']  else s['low'], axis=1)
    resample_df = price_df[['open','NewHigh','NewLow','close']]
    resample_df.columns= ['open','high','low','close']
    # 该tick内成交量-为累计成交量的差值
    vol_end_df = df[['Volume','Turnover']].resample(sample_t, label='right', closed='right').last()
    vol_end_df['Datetime'] = df['Datetime'].resample(sample_t, label='right', closed='right').last()
    vol_end_df = split_no_trading_time_df(vol_end_df)# 剔除非交易时间段
    vol_end_df.fillna(0, inplace=True)# 交易时间段内未发生交易，则成交量和成交额均为0
    vol_shift_df = vol_end_df.shift(1).fillna(0)
    vol_df = vol_end_df[['Volume','Turnover']] - vol_shift_df[['Volume','Turnover']]
    # 分钟第一个和最后一个盘口数据
    LOB_beg_df = df[['BidPrice1','BidVolume1','AskPrice1', 'AskVolume1']].resample(sample_t, label='right', closed='right').first()
    LOB_beg_df['Datetime'] = df['Datetime'].resample(sample_t, label='right', closed='right').last()
    LOB_beg_df = split_no_trading_time_df(LOB_beg_df)# 剔除非交易时间段
    LOB_beg_df.fillna(0, inplace=True)# 交易时间段内未发生交易，则盘口数据均为0
    LOB_end_df = df[['BidPrice1','BidVolume1','AskPrice1', 'AskVolume1']].resample(sample_t, label='right', closed='right').last()
    LOB_end_df['Datetime'] = df['Datetime'].redatapathsample(sample_t, label='right', closed='right').last()
    LOB_end_df = split_no_trading_time_df(LOB_end_df)# 剔除非交易时间段
    LOB_end_df.fillna(0, inplace=True)# 交易时间段内未发生交易，则盘口数据均为0
    LOB_beg_df.drop(['Datetime'], axis=1, inplace=True)
    LOB_end_df.drop(['Datetime'], axis=1, inplace=True)
    LOB_beg_df.columns = ['open_bid_price1','oen_bid_volume1','open_ask_price1', 'open_ask_volume1']
    LOB_end_df.columns = ['close_bid_price1','close_bid_volume1','close_ask_price1', 'close_ask_volume1']
    result_df = resample_df.merge(vol_df, left_index=True,right_index=True).merge(
                                      LOB_beg_df, left_index=True,right_index=True).merge(
                                            LOB_end_df, left_index=True, right_index=True)
    result_df.reset_index(inplace=True)
    return result_df

#### 获得主力合约
#### 从tqsdk拿到的历史数据------如何优化读取数据的时间？？？？
def get_tqsdk_df(folder_path = '/home/tushetou/python_codes/rb'):# 存放所有合约tqsdk历史数据的文件夹路径
    file_paths = os.listdir(folder_path) # 所有合约tqsdk历史数据csv的文件名
    data_dict = dict()
    for file in file_paths:
        data_iter = pd.read_csv(f"/home/tushetou/python_codes/rb/{file}", chunksize=100000)
        data_dict[file] = data_iter
    return data_dict
# 以下函数输入data_iter中的数据
# 统一columns字段格式
def process_tqsdk_df(tqsdk_df):# 输入为data_iter中的df
    processed = tqsdk_df.copy() # 此处没有深拷贝，节约内存
    processed['datetime'] = processed.datetime.apply(lambda x: x[:-6])
    new_cols = ["Datetime", "DatetimeNano", "LastPrice", "HighestPrice", "LowestPrice", "AveragePrice", "Volume", "Turnover",
                "OpenInterest", "BidPrice1", "BidVolume1", "AskPrice1", "AskVolume1", "BidPrice2", "BidVolume2", "AskPrice2", 
                "AskVolume2", "BidPrice3", "BidVolume3", "AskPrice3", "AskVolume3","BidPrice4", "BidVolume4", "AskPrice4", 
                "AskVolume4", "BidPrice5", "BidVolume5", "AskPrice5", "AskVolume5"]
    processed.columns = new_cols
    return processed
# 获取一部分处理后tqsdk数据中每日最后tick的成交量-----这个函数太慢了！！！！
def get_last_volume(processed):
    processed['Date'] = processed.Datetime.apply(lambda x: x[:10])
    processed['Time'] = processed.Datetime.apply(lambda x: x[11:])
    this_last_volume = processed.loc[processed.Time <= '15:00:00.000'].groupby('Date').last()['Volume']
    return this_last_volume
# 获得所有合约每日最后tick成交量
def get_last_volume_df(data_dict):
    last_volume_df = pd.DataFrame()
    for ID, data_iter in data_dict.items():
        last_volume_ID = pd.Series([], dtype='int64')
        for data in data_iter:
            processed = process_tqsdk_df(data)
            this_last_volume = get_last_volume(processed)
            last_volume_ID = pd.concat([last_volume_ID, this_last_volume])
        last_volume_ID = pd.DataFrame(last_volume_ID, columns=[ID])
        last_volume_df = pd.concat([last_volume_df, last_volume_ID])
    return last_volume_df
# 获得主力合约切换时间和合约ID
def get_main_contract(last_volume_df):
    last_volume_df_cp = last_volume_df.copy()
    last_volume_df_cp.fillna(0, inplace=True)
    last_volume_df_cp['largest_volume_ID'] = last_volume_df.idxmax(1)
    last_volume_df_cp['largest_volume_ID_shift'] = last_volume_df_cp['largest_volume_ID'].shift().fillna(0)
    last_volume_df_cp['changed'] = last_volume_df_cp['largest_volume_ID'] != last_volume_df_cp['largest_volume_ID_shift']
    in_main_contract = []
    def return_main_contract(x):
        if (x['changed'] == True) & (x['largest_volume_ID'] not in in_main_contract):
            in_main_contract.append(x['largest_volume_ID'])
            return x['largest_volume_ID']
        else:
            return x['largest_volume_ID_shift']
    main_contract_series = last_volume_df_cp.apply(return_main_contract, axis=1)
    change_info = main_contract_series[main_contract_series != main_contract_series.shift().fillna(0)]
    main_contract_dict = dict()
    for index, value in change_info.items():
        main_contract_dict[index] = value
        print(f"main contract changed in {index}, to {value}")
    return main_contract_dict




