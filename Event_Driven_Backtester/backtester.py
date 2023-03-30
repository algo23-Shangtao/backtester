import logging
import queue

class BackTest:
    def __init__(self, start_date, end_date, symbol_list, datahandler_cls, strategy_cls, portfolio_cls, execution_cls):
        self.start_date = start_date
        self.end_date = end_date
        self.symbol_list = symbol_list
        self.strategy_cls = strategy_cls
        self.portfolio_cls = portfolio_cls
        self.execution_cls = execution_cls
        self.datahandler_cls = datahandler_cls
        self.events = queue.Queue()
        self._initialize_instances()
    
    def _initialize_instances(self):
        self.datahandler = self.datahandler_cls(self.events)
        self.portfolio = self.portfolio_cls(self.datahandler, self.events)
        self.strategy = self.strategy_cls(self.datahandler, self.portfolio, self.events)
        self.execution = self.execution_cls(self.datahandler, self.portfolio, self.events)
    
    def run_backtest(self):
        while True:
            md_engine = self.datahandler
            if md_engine.continue_backtest:
                md_engine.publish_bar()
                self.portfolio.update_bar()
                self.execution.update_bar()
            else:
                break
            # 处理事件队列
            while True:
                try:
                    event = self.events.get(block=False)
                except queue.Empty:
                    break
                else:
                    if event is not None:
                        if event.type == 'MARKET':
                            self.strategy.generate_signals(event)
                        elif event.type == 'SIGNAL':
                            self.portfolio.generate_order(event)
                        elif event.type == 'ORDER':
                            self.execution.execute_order(event)
                        elif event.type == 'FILL':
                            self.portfolio.update_fill(event)
                    