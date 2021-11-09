import asyncio
import pandas as pd
import time
import zmq
import zmq.asyncio

from enums import *


class socket_in:
    def __init__(self,socket_type = zmq.SUB):
        self.ctx = zmq.asyncio.Context()
        self.port = None
        self.socket_type = socket_type
        
    def __enter__(self):
        self.sock = self.ctx.socket(self.socket_type)
        if self.socket_type == zmq.SUB:
            self.sock.setsockopt_string(zmq.SUBSCRIBE, '')
        self.sock.connect('tcp://localhost:'+str(self.port))
        return self.sock

    def __exit__(self, *exc):
        self.sock.close()

class socket_out:
    def __init__(self,socket_type = zmq.PUB):
        self.ctx = zmq.asyncio.Context()
        self.port = None
        self.socket_type = socket_type
        
    def __enter__(self):
        self.sock = self.ctx.socket(self.socket_type)
        if self.socket_type == zmq.PUB:
            self.sock.setsockopt(zmq.LINGER, 1)
        self.sock.bind('tcp://*:'+str(self.port))
        return self.sock

    def __exit__(self, *exc):
        self.sock.close()        
        


class harvester_socket_rep(socket_in):
    
    def __init__(self):
        super().__init__(socket_type=zmq.REP)
        self.port = HARVESTER_PORT
        
class harvester_socket_req(socket_out):
    
    def __init__(self):
        super().__init__(socket_type=zmq.REQ)
        self.port = HARVESTER_PORT
   

class agent_socket_rep(socket_in):
    
    def __init__(self):
        super().__init__(socket_type=zmq.REP)
        self.port = AGENT_PORT
        
class agent_socket_req(socket_out):
    
    def __init__(self):
        super().__init__(socket_type=zmq.REQ)
        self.port = AGENT_PORT
       
              
class order_socket_rep(socket_in):
    
    def __init__(self):
        super().__init__(socket_type=zmq.REP)
        self.port = TRADER_PORT
        
class order_socket_req(socket_out):
    
    def __init__(self):
        super().__init__(socket_type=zmq.REQ)
        self.port = TRADER_PORT

class crawler_socket_rep(socket_in):
    
    def __init__(self):
        super().__init__(socket_type=zmq.REP)
        self.port = CRAWLER_PORT
        
class crawler_socket_req(socket_out):
    
    def __init__(self):
        super().__init__(socket_type=zmq.REQ)
        self.port = CRAWLER_PORT
        
class data_socket_recv(socket_in):
    
    def __init__(self):
        super().__init__(socket_type=zmq.SUB)
        self.port = DATASTREAM_PORT

class data_socket_send(socket_out):
    
    def __init__(self):
        super().__init__(socket_type=zmq.PUB)
        self.port = DATASTREAM_PORT        
   
class data_aggregated_socket_recv:

    def __init__(self):

        self._running = True
        self.loop = asyncio.get_event_loop()
        # print('test thread:',self.loop._thread_id)
        self.query = asyncio.Queue()
        self.query_aggr = asyncio.Queue()
        self.tasks = []
        self.df = pd.DataFrame(columns=['harvester_id','timestamp','price','quantity'])
        self.dt = 1000 #timedelta in ms

    async def fill_query(self):
        # print('run fill_query()')
        # print('inside thread_id',self.loop,self.loop._thread_id)
        with data_socket_recv() as sock:
            while self._running:
                # print('pop_query')
                data = await sock.recv_json()
                await self.query.put(data)
                #await asyncio.sleep(.1)

    async def fill_query_aggr(self):
        # print('run modify_query()')

        def f(data):
#             return pd.Series({'price':data['price'].mean(),
#                               'quantity':data['quantity'].sum()})
            return pd.Series({'price':(data['price']*data['quantity']).sum()/data['quantity'].sum(),
                              'quantity':data['quantity'].sum()})

        while self._running:
            data = await self.query.get()
            self.df = self.df.append({'harvester_id':data['harvester_id'],
                                      'timestamp':data['timestamp'],
                                      'price':data['price'],
                                      'quantity':data['quantity']},ignore_index=True)

            t0 = int(time.time()*1000)
            mask = self.df['timestamp'] < t0 - self.dt
            group = self.df[mask].groupby(by=['timestamp','harvester_id'],as_index=False)
            self.df = self.df[~mask]

            for i,rec in group.apply(f).iterrows():
                rd = rec.to_dict()
                rd['timestamp'] = int(rd['timestamp'])
                rd['harvester_id'] = int(rd['harvester_id'])
                await self.query_aggr.put(rd)


    async def recv_json(self):
        data = await self.query_aggr.get()
        return data

    def __enter__(self):
        self.tasks+=[self.loop.create_task(self.fill_query())]
        self.tasks+=[self.loop.create_task(self.fill_query_aggr())]
        return self

    def __exit__(self,*exc):
        pass
