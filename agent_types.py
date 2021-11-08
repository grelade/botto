from enum import Enum

import pandas as pd
import random
import logging
import numpy as np

from funcs import moving_average

#agent decision enum
class dcn(Enum):
    WAIT = 0
    SELL = 1
    BUY = 2

    
class agent_ma:
    
    def __init__(self,
                 init_data,
                 frac_sell = 1.1,
                 data_limit = 10,
                 logger=logging):
        self.init_data = init_data
        self.data_df = pd.DataFrame()
        self.frac_sell = frac_sell
        self.data_limit = data_limit
        self.logger = logger
#         print('init_data',self.init_data)
        
    def load_data(self,data):

        if (self.data_df.shape[0])>=self.data_limit:
            self.data_df = self.data_df[-(self.data_limit):]
        if len(self.data_df.columns)==0:
            self.data_df = pd.DataFrame(data,columns=data.keys(),index=[0])
        else:
            types = self.data_df.dtypes
            self.data_df = self.data_df.append(data,ignore_index=True)
            self.data_df = self.data_df.astype(types)
    
    def decide(self):
#         coin = random.random()
#         print(coin)

#         if coin > 0.9:
#             return dcn.SELL
#         else:
#             return dcn.WAIT
        
        ratio = self.data_df['price'].mean()/self.init_data['init_price']
        self.logger.info(f"data_size = {self.data_df.shape[0]}; profit/loss = {100*(ratio-1):.6}")
        
        if ratio > self.frac_sell:
            return dcn.SELL
        else:
            return dcn.WAIT
        
class agent_trail:
    
    def __init__(self,
                 init_data,
                 stop_loss=5,
                 take_profit=15,
                 ma_window=30,
                 data_limit = 50,
                 logger=logging):
        
        self.ts = np.empty(0)
        self.ps = np.empty(0)
        self.plows = np.empty(0)
        self.phighs = np.empty(0)
        self.decisions = np.empty(0)
        self.i = 0        
        self.n0 = ma_window
        self.exit = False
        
        self.init_data = init_data
        
        self.ts = np.append(self.ts,self.init_data['init_timestamp'])
        self.ps = np.append(self.ps,self.init_data['init_price'])
        self.report_file = 'report.csv'
        
        self.data_df = pd.DataFrame()
        self.data_limit = data_limit
        self.sl = stop_loss/100
        self.tp = take_profit/100
        self.logger = logger
        
    def load_data(self,data):
        if (self.data_df.shape[0])>=self.data_limit:
            self.data_df = self.data_df[-(self.data_limit):]
        if len(self.data_df.columns)==0:
            self.data_df = pd.DataFrame(data,columns=data.keys(),index=[0])
        else:
            types = self.data_df.dtypes
            self.data_df = self.data_df.append(data,ignore_index=True)
            self.data_df = self.data_df.astype(types)
            
        self.ps = np.append(self.ps,data['price'])
        self.ts = np.append(self.ts,data['timestamp'])
        self.i += 1
        
        if len(self.ps)>=self.n0:
            plow = (1 - self.sl)*moving_average(self.ps,n=self.n0)[self.i-self.n0]
            phigh = (1 + self.tp)*moving_average(self.ps,n=self.n0)[self.i-self.n0]
        else:
            plow = (1 - self.sl)*self.ps[0]
            phigh = (1 + self.tp)*self.ps[0]


        self.plows = np.append(self.plows,plow)
        self.phighs = np.append(self.phighs,phigh)

    def save_report(self):
        self.df = pd.DataFrame({'time':self.ts,
                               'price':self.ps,
                               'price_low':self.plows,
                               'price_high':self.phighs,
                               'decision':self.decisions})

        self.df.to_csv(f"reports/{self.report_file}")
        
    def decide(self):
        self.logger.info(f"p={self.ps[-1]:.4f},plow={self.plows[-1]:.4f},phigh={self.phighs[-1]:.4f}")
        if self.plows[-1] >= self.ps[-1]:
#             print('plow alarm')
            self.decisions = np.append(self.decisions,1)
            self.exit = True
        elif self.ps[-1] >= self.phighs[-1]:
#             print('phigh alarm')
            self.decisions = np.append(self.decisions,-1)
            self.exit = True
        else:
#             print('no alarm')
            self.decisions = np.append(self.decisions,0)
            
        if self.exit:
            self.save_report()
            return dcn.SELL
        else:
            return dcn.WAIT
        
agent_dict = {
              'MOVING_AVERAGE': agent_ma,
              'TRAIL': agent_trail
             }
