import asyncio
import aiosqlite
import logging
import pandas as pd

import zmq, zmq.asyncio

from binance import BinanceSocketManager

from funcs import create_binance_client
from funcs import create_logger
from funcs import load_cfg, load_auth, load_track

from signal import SIGINT, SIGTERM
from funcs import error_handler
from funcs import set_argparser

from sockets import data_socket_send #outputs
from sockets import harvester_server #inputs
    
class harvester_proc:
    
    def __init__(self,client,args):
        self.client = client
        self.args = args
        self.cfg = load_cfg(args.cfg_file)
        self.db_args = self.cfg['db']
        self.streams = pd.DataFrame(columns=['harvester_id','name','symbol','active','running'])
        self.tasks = dict()
        
        self.ctx = zmq.asyncio.Context()
        self.dataq_raw = asyncio.Queue()
        self.dbrefresh_time = 5
        self.logger = create_logger(name='data_harvester')
        
    async def run(self):
        self.logger.info('Starting harvester_proc()')
        
        while True:
            await self.read_db_streams()
            #open new streams
            for i,stream in self.streams.iterrows():
                
                if stream['active'] and not stream['running']:
                    s = self.open_stream(**stream.to_dict())
                    self.tasks[stream['harvester_id']] = asyncio.create_task(s)
                    stream['running'] = True
        
                elif not stream['active'] and stream['running']: #stop inactive tasks
                    self.logger.info(f"Stopping streaming {stream['name']}")
                    self.tasks[stream['harvester_id']].cancel(msg=None)
                    stream['running'] = False
                    
                else:
                    pass

            await self.update_db_streams()
            await asyncio.sleep(self.dbrefresh_time)            
  
    async def read_db_streams(self):

        streams = []
        async with aiosqlite.connect(**self.db_args) as db:
            async with db.execute("SELECT * FROM data_harvester") as cursor:
                async for row in cursor:
                    streams += [{'harvester_id':row[0],
                           'name':row[1],
                           'symbol':row[2],
                           'active':bool(row[3]),
                           'running': bool(row[4])}]
        
        for s in streams:
            if (self.streams['harvester_id']==s['harvester_id']).sum()==0:
                #new record
                self.streams = self.streams.append(s,ignore_index=True)
            elif (self.streams['harvester_id']==s['harvester_id']).sum()==1:
                #modify record
                self.streams.loc[self.streams['harvester_id']==s['harvester_id'],'active'] = s['active']

    async def insert_db_stream(self,request):
        async with aiosqlite.connect(**self.db_args) as db:
            rec = [request[key] for key in ['name','symbol','active','running']]
            await db.execute("INSERT INTO data_harvester(name,symbol,active,running) VALUES (?,?,?,?)",rec) 
            cursor = await db.execute("SELECT last_insert_rowid()")
            out = await cursor.fetchall()
            await db.commit()
            return {'harvester_id':out[0][0]}
                
    async def update_db_streams(self):
        async with aiosqlite.connect(**self.db_args) as db:        
            for i, rec in self.streams.iterrows():
                recl = rec.tolist()
                await db.execute('UPDATE data_harvester SET name=?, symbol=?, active=?, running=? WHERE id=?',recl[1:]+recl[:1])
    
    async def open_stream(self,
                          harvester_id:int,
                          name:str,
                          symbol:str,
                          **kwargs) -> None:

        self.logger.info(f'Starting harvester "{name}", gathering {symbol}')
    
        binance_socket = BinanceSocketManager(self.client)
        
        async with binance_socket.trade_socket(symbol) as stream, aiosqlite.connect(**self.db_args) as db:
            while True:
                res = await stream.recv()
                rec = dict()
                if res['e']=="trade" and res['s'] == symbol:
                    
                    rec['harvester_id'] = int(harvester_id)
                    rec['trade_id'] = int(res['t'])
                    rec['buyer_id'] = int(res['b'])
                    rec['seller_id'] = int(res['a'])
                    rec['timestamp'] = int(res['E'])
                    rec['price'] = float(res['p'])
                    rec['quantity'] = float(res['q'])
                    
                    rec_list = [rec['harvester_id'],
                               rec['trade_id'],
                               rec['buyer_id'],
                               rec['seller_id'],
                               rec['timestamp'],
                               rec['price'],
                               rec['quantity']]
                    
                    #send outside via tcp-socket
                    await self.dataq_raw.put(rec)
                    
                    #send to db
                    await db.execute('''INSERT INTO data_harvester_data(harvester_id,
                                                                        trade_id,
                                                                        buyer_id,
                                                                        seller_id,
                                                                        timestamp,
                                                                        price,
                                                                        quantity) VALUES (?,?,?,?,?,?,?)''',rec_list)
                    await db.commit()

    async def emit_data_raw(self):
        with data_socket_send() as sock:
            while True:
                rec = await self.dataq_raw.get()
                await sock.send_json(rec)

    async def run_server(self):
        with harvester_server() as sock:
            while True:
                request = await sock.recv_json()
                response = await self.insert_db_stream(request)
                response['resp'] = 200
                await sock.send_json(response)
                
    async def close(self):
        print('harvester close')
        self.streams['running'] = False #turn off all streams
        await self.update_db_streams()
        await self.client.close_connection()     
        
async def main(args) -> None:
    try:

        auth = load_auth(args.auth_file)

        bclient = await create_binance_client(auth['binance_api'],
                                              auth['binance_secret'])    

        proc = harvester_proc(client = bclient,
                              args = args)

        tasks = []
        tasks += [asyncio.create_task(proc.run())]
        tasks += [asyncio.create_task(proc.emit_data_raw())]
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
    
    

