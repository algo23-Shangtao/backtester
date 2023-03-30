from Event_Driven_Backtester import Data, Execution, Portfolio, Strategy, backtester

start_date = '2021-01-01'
end_date = '2021-05-05'
symbol_list = ['rb']

backtest = backtester.BackTest(start_date, end_date, symbol_list, Data.HistoryCSVDataHandler, Strategy.BuyAndHoldStrategy, Portfolio.NaivePortfolio, Execution.SimulateExecutionHandler)
backtest.run_backtest()
print('over')
print(backtest.portfolio.output_summary_stats())