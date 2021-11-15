import asyncio
from datetime import datetime, timedelta
import logging
import pytz

from enums import *
from funcs import create_binance_client
from funcs import load_cfg, load_auth, load_track
from funcs import create_logger, error_handler, set_argparser
from funcs import estimate_price
from sockets import order_socket_req, agent_socket_req, harvester_socket_req, crawler_socket_req #outputs

class cpu_proc:

    def __init__(self,client,args,**kwargs):
        self.client = client
        self.args = args
        self.cfg = load_cfg(args.cfg_file)
        self.pair = self.cfg['general']['pair']
        self.track_cfg = load_track(self.args.track_file)
        self.logger = create_logger(name='cpu')
#         self.orderq = asyncio.Queue()

    async def send_order(self,request):
        with order_socket_req() as sock:
            await sock.send_json(request)
            response = await sock.recv_json()
            return response

#     async def send_order(self):
#         with order_socket_req() as sock:
#             while True:
#                 request = await self.orderq.get()
#                 await sock.send_json(request)
#                 response = await sock.recv_json()

    async def add_harvester(self,request):
        with harvester_socket_req() as sock:
            await sock.send_json(request)
            response = await sock.recv_json()
            return response

    async def add_agent(self,request):
        with agent_socket_req() as sock:
            await sock.send_json(request)
            response = await sock.recv_json()
            return response

    async def add_crawler(self,request):
        with crawler_socket_req() as sock:
            await sock.send_json(request)
            response = await sock.recv_json()
            return response

    async def coin_track(self):
        self.logger.info('init coin track...')

        rec = {'symbol': self.track_cfg['symbol'],
               'side': SIDE_BUY}

        #new order flags
        is_s = 'symbol' in self.track_cfg.keys()
        is_tot = 'trader_order_type' in self.track_cfg.keys()
        is_op = 'order_price' in self.track_cfg.keys()
        is_p = 'price' in self.track_cfg.keys()


        #existing order flags
        is_oi = 'init_order_id' in self.track_cfg.keys()

        #new order
        if is_s and is_tot and is_op:
            rec['mode'] = 'new'
            if self.track_cfg['trader_order_type'] == ORDER_TYPE_MARKET:
                rec['order_price'] = self.track_cfg['order_price']
                rec['type'] = ORDER_TYPE_MARKET

            elif self.track_cfg['trader_order_type'] == ORDER_TYPE_LIMIT and is_p:
                rec['order_price'] = self.track_cfg['order_price']
                rec['type'] = ORDER_TYPE_LIMIT
                rec['price'] = self.track_cfg['price']
            else:
                raise Exception('unknown order / check conf file')

        #existing order
        elif is_s and is_oi:
            rec['mode'] = 'existing'
            rec['orderId'] = self.track_cfg['init_order_id']
        else:
            raise Exception('unknown order / check conf file')

        resp_o = await self.send_order(rec)

        req_h = {'name':self.track_cfg['harvester_name'],
                 'symbol':self.track_cfg['symbol'],
                 'active':True,
                 'running':False}
        resp_h = await self.add_harvester(req_h)
        if resp_h['resp'] != RESPONSE_OK: raise Exception('harvester problem')

        req_a = {'name':self.track_cfg['agent_name'],
                 'init_order_id':resp_o['id'],
                 'harvester_id':resp_h['harvester_id'],
                 'type':self.track_cfg['agent_type'],
                 'params':self.track_cfg['agent_params'],
                 'active':True,
                 'running':False}
        resp_a = await self.add_agent(req_a)
        if resp_a['resp'] != RESPONSE_OK: raise Exception('agent problem')

        self.logger.info('end coin track...')

    async def newcoin_track(self):
        self.logger.info('init newcoin track...')

        #crawl
        req_c = {'mock':self.track_cfg['crawler_mock_msg']}
        resp_c = await self.add_crawler(req_c)

        if resp_c['resp'] != RESPONSE_OK: raise Exception('crawler problem')
        newcoin = resp_c.copy()
        newcoin = await self.order_preparation(newcoin)

        rec = {'symbol': newcoin['symbol'],
               'side': SIDE_BUY,
               'order_price': self.track_cfg['order_price'],
               'mode': 'new',
               'mock': self.track_cfg['trader_mock_orders']}

        if self.track_cfg['trader_order_type'] == ORDER_TYPE_MARKET:
            rec['type']= ORDER_TYPE_MARKET

        elif self.track_cfg['trader_order_type'] == ORDER_TYPE_LIMIT:
            #last_price = await self.get_last_price(newcoin['symbol'])
            rec['type'] = ORDER_TYPE_LIMIT
            rec['price'] = self.track_cfg['trader_price_frac']*newcoin['est_init_price']
        
        success = False
        while not success:
            try:
                await asyncio.sleep(0.001)
                resp_o = await self.send_order(rec)
                success = True
            except BinanceAPIException as e:
                if e.code == BINANCE_WRONG_SYMBOL:
                    self.logger.info('no symbol yet')
                    pass

        req_h = {'name':self.track_cfg['harvester_name'],
                 'symbol':newcoin['symbol'],
                 'active':True,
                 'running':False}
        resp_h = await self.add_harvester(req_h)
        if resp_h['resp'] != RESPONSE_OK: raise Exception('harvester problem')

        req_a = {'name':self.track_cfg['agent_name'],
                 'init_order_id':resp_o['id'],
                 'harvester_id':resp_h['harvester_id'],
                 'type':self.track_cfg['agent_type'],
                 'params':self.track_cfg['agent_params'],
                 'active':True,
                 'running':False}
        resp_a = await self.add_agent(req_a)
        if resp_a['resp'] != RESPONSE_OK: raise Exception('agent problem')

        self.logger.info('end newcoin track...')

    async def backlog_track(self):
        self.logger.info('init backlog track...')

        req_h = {'name':self.track_cfg['harvester_name'],
                 'symbol':newcoin['symbol'],
                 'active':True,
                 'running':False,
                 'backlog_file':self.track_cfg['backlog_file']}
        resp_h = await self.add_harvester(req_h)
        self.logger.info('end backlog track...')

    async def order_preparation(self,newcoin):
        nc = newcoin.copy()
        tz = pytz.timezone(self.cfg['general']['timezone'])

        t_msg = datetime.fromisoformat(newcoin['msg_time'])
        t_start = datetime.fromisoformat(newcoin['start_time'])

        #t_msg = tz.localize(datetime.fromtimestamp(newcoin['msg_time']))
        #t_start = tz.localize(datetime.fromtimestamp(newcoin['start_time']))
        t_price_estimate = t_start - timedelta(minutes=5)
        t_end_preparation = t_start - timedelta(minutes=1)

        self.logger.info(f'init price estimation...')

        #wait with price estimation
        while (t_now:= tz.localize(datetime.now())) < t_price_estimate:
            await asyncio.sleep(.001)

        auth = load_auth(self.args.auth_file)
        prices = await estimate_price(auth['coinapi_apikey'],
                                     base_asset=newcoin['coin_name'],
                                     time_start=t_msg,
                                     time_end=t_price_estimate,
                                     quote_asset=self.pair,
                                     period='5MIN')

        nc['est_init_price'] = prices['price_average']

        while (t_now := tz.localize(datetime.now())) < t_end_preparation:
            await asyncio.sleep(.001)
        self.logger.info(f'... {nc["symbol"]} price = {nc["est_init_price"]}')

        while (t_now := tz.localize(datetime.now())) < t_start:
            await asyncio.sleep(.000001)
        return nc

    async def get_last_price(self,symbol):
        last_price = await self.client.get_symbol_ticker(symbol=symbol)
        last_price = float(last_price['price'])
        return last_price

    async def run_server(self):
        self.logger.info('Starting cpu_proc()')
        if self.track_cfg['track_mode'] == 'coin':
            await self.coin_track()
        elif self.track_cfg['track_mode'] == 'new_coin':
            await self.newcoin_track()
        elif self.track_cfg['track_mode'] == 'backlog':
            await self.backlog_track()
        else:
            self.logger.info('unknown track_mode')

    async def close(self):
        print('cpu close')
        await self.client.close_connection()

async def main(args):
    try:

        auth = load_auth(args.auth_file)

        bclient = await create_binance_client(auth['binance_api'],
                                              auth['binance_secret'])

        proc = cpu_proc(client = bclient,
                        args = args)

        tasks = []
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
