import datetime
import numpy as np
import pandas as pd
from queue import Queue

from abc import ABC, abstractmethod

from Event_Driven_Backtester import event

class Strategy(ABC):
    '''
    所有strategy的父类
    基于行情数据bar产生信号signal
    '''
    @abstractmethod
    def calculate_signals(self):
        raise NotImplementedError("should implement calculate_signals()")
    