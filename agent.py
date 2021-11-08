import asyncio
import sqlite3
import aiosqlite
import yaml
import logging
import pandas as pd

import zmq, zmq.asyncio

from binance.enums import *

from funcs import create_binance_client
from funcs import load_cfg, load_auth
from funcs import aio_pd_read_sql, id_to_symbol
from funcs import create_logger
from funcs import set_argparser

from signal import SIGINT, SIGTERM
from funcs import error_handler

from sockets import order_socket_req #outputs
from sockets import data_aggregated_socket_recv,agent_socket_rep #inputs


from agent_types import agent_ma, agent_trail, agent_dict, dcn

class agent_proc:
    
    def __init__(self,client,args):
        self.client = client
        self.args = args
        self.cfg = load_cfg(args.cfg_file)
        self.db_args = self.cfg['db']
        self.ctx = zmq.asyncio.Context()
        self.orderq = asyncio.Queue()        
        self.agents = pd.DataFrame()
        self.orders = pd.DataFrame()
        self.tasks = dict()
        self.agents_obj = dict()
        self.dbrefresh_time = 5 #in seconds
        self.logger = create_logger(name='agent')  
  
    async def read_db_agents(self):
        async with aiosqlite.connect(**self.db_args) as db:
            self.agents = await aio_pd_read_sql("SELECT * FROM agent", db)
            
    async def read_db_orders(self):
        async with aiosqlite.connect(**self.db_args) as db:
            self.orders = await aio_pd_read_sql('''SELECT * FROM "order"''', db)
            
    async def update_db_agents(self):
        async with aiosqlite.connect(**self.db_args) as db:        
            for i, rec in self.agents.iterrows():
                recl = rec.tolist()
                await db.execute('UPDATE agent SET name=?, init_order_id=?, harvester_id=?, type=?, params=?, active=?, running=? WHERE id=?',recl[1:]+recl[:1])         

    async def insert_db_agent(self,request):
        async with aiosqlite.connect(**self.db_args) as db:
            l = ['name','init_order_id','harvester_id','type','params','active','running']
            request['params'] = str(request['params'])
            rec = [request[key] for key in l]
            
            await db.execute("INSERT INTO agent(name,init_order_id,harvester_id,type,params,active,running) VALUES (?,?,?,?,?,?,?)",rec)
            cursor = await db.execute("SELECT last_insert_rowid()")
            out = await cursor.fetchall()
            await db.commit()
            return {'id':out[0][0]}    
        

        
    def read_init_data(self,
                       init_order_id:int):
        #self.orders[self.orders['agent_id']==agent_id]['']
        mask1 = self.orders['id']==init_order_id
        mask2 = self.orders['status']=='FILLED'
        data = self.orders[(mask1) & (mask2)][['transactTime','price','origQty']]
        #print(data['transactTime'].idxmax())
        data0 = data.loc[data['transactTime'].idxmax()]
        
        return {'init_price': data0['price'],
                'init_timestamp': data0['transactTime'],
                'init_quantity': data0['origQty']}
    
    async def open_agent(self,
                         adict:dict,
                         **kwargs) -> None:
        id = adict['id']
        name = adict['name']
        init_order_id = adict['init_order_id']
        harvester_id = adict['harvester_id']
        type = adict['type']
        agent_params = yaml.load(adict['params'],Loader=yaml.FullLoader)
        
        self.logger.info(f'Starting agent "{name}", type={type}, params={agent_params}')
        
        init_data = self.read_init_data(init_order_id)
        
        #self.agents.loc[self.agents['id']==id,'running'] = 1
        self.agents_obj[id] = agent_dict[type](init_data,**agent_params,logger=self.logger)
        agent = self.agents_obj[id]
        
        with data_aggregated_socket_recv() as sock:
            while True:
                data = await sock.recv_json()
                
                if data['harvester_id']==harvester_id:
                    #self.logger.info(data)
                    
                    agent.load_data(data)
                    decision = agent.decide()
                    
                    if decision == dcn.SELL:

                        rec = {'symbol': await id_to_symbol(self.db_args,harvester_id),
                               'type': ORDER_TYPE_MARKET,
                               'side': SIDE_SELL,
                               'quantity': float(agent.init_data['init_quantity']),
                               'mock': False,
                               'mode': 'new'}
                        
                        await self.orderq.put(rec)
                        self.agents.loc[self.agents['id']==id,'active'] = 0
                        await self.update_db_agents()
                        break
                        
    async def send_order(self):
        with order_socket_req() as sock:
            while True:
                request = await self.orderq.get()
                await sock.send_json(request)
                response = await sock.recv_json()

    async def run_server(self):
        with agent_socket_rep() as sock:
            while True:
                request = await sock.recv_json()
                response = await self.insert_db_agent(request)
                response['resp'] = 200
                await sock.send_json(response)

    async def agent_loop(self):
        self.logger.info('Starting agent_proc()')
        
        while True:
            await self.read_db_agents()
            await self.read_db_orders()
            #open new streams
            for i,agent in self.agents.iterrows():
                
                if agent['active'] and not agent['running']:
                    a = self.open_agent(adict = agent.to_dict())
                    self.tasks[agent['id']] = asyncio.create_task(a)
#                     agent['running'] = True
                    self.agents.loc[self.agents['id']==agent['id'],'running'] = True
                    
                elif not agent['active'] and agent['running']: #stop inactive tasks
                    self.logger.info(f"Stopping {agent['name']}")
                    self.tasks[agent['id']].cancel(msg=None)
#                     agent['running'] = False
                    self.agents.loc[self.agents['id']==agent['id'],'running'] = False
                    
                else:
                    pass

            await self.update_db_agents()
            await asyncio.sleep(self.dbrefresh_time)                
                
    async def close(self):
        print('agent close')
        self.agents['running'] = False #turn off all agents
        await self.update_db_agents()
        await self.client.close_connection()    
        
async def main(args) -> None:
    try:

        auth = load_auth(args.auth_file)

        bclient = await create_binance_client(auth['binance_api'],
                                              auth['binance_secret'])    

        proc = agent_proc(client=bclient,
                          args=args)

        tasks = []
        tasks += [asyncio.create_task(proc.agent_loop())]
        tasks += [asyncio.create_task(proc.send_order())]
        tasks += [asyncio.create_task(proc.run_server())]
        await asyncio.gather(*tasks)
        
    except asyncio.CancelledError:
        await proc.close()
    
if __name__ == "__main__":
    
    args = set_argparser()
    
    loop = asyncio.get_event_loop()
    for sig in (SIGTERM, SIGINT):
        loop.add_signal_handler(sig, error_handler, sig, loop)
    loop.create_task(main(args))
    loop.run_forever()
    tasks = asyncio.all_tasks(loop=loop)
    for t in tasks:
        t.cancel()
    group = asyncio.gather(*tasks, return_exceptions=True)
    loop.run_until_complete(group)
    loop.close()
        
#     asyncio.run(main())
    

