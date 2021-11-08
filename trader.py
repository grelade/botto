import asyncio
import sqlite3
import aiosqlite
import yaml
import logging
import pandas as pd
import random
from datetime import datetime



import zmq, zmq.asyncio

from binance import AsyncClient as BinanceClient
from binance import BinanceSocketManager
from binance.enums import *

from funcs import create_binance_client
from funcs import load_auth, load_cfg

from funcs import aio_pd_read_sql, id_to_symbol, price_average
from funcs import create_logger

from signal import SIGINT, SIGTERM
from funcs import error_handler
from funcs import set_argparser

from sockets import order_server #inputs


class trader_proc:
    
    def __init__(self,client,args):
        self.client = client
        self.args = args
        self.cfg = load_cfg(args.cfg_file)
        self.db_args = self.cfg['db']
        self.ctx = zmq.asyncio.Context()    
        self.update_refresh_time = 5
        self.mock_orders = False
        self.logger = create_logger(name='trader')
        
    async def get_last_price(self,symbol):
        last_price = await self.client.get_symbol_ticker(symbol=symbol)
        last_price = float(last_price['price'])
        return last_price
    
    async def extend_order(self,order):
        if order['type'] == ORDER_TYPE_MARKET:
            if 'price' in order.keys():
                _ = order.pop('price')
                
        elif order['type'] == ORDER_TYPE_LIMIT:
            order['timeInForce'] = TIME_IN_FORCE_GTC
            
        order['newOrderRespType']= ORDER_RESP_TYPE_FULL
        return order
    
    async def verify_order(self,order):
  
        info = await self.client.get_symbol_info(order['symbol'])
        # sensible default values
        applyToMarket = True
        minNotional = 10.0
        minQty = 0.0
        maxQty = 1000.0
        stepSize = 1.0
        minPrice = 0.0001
        maxPrice = 1000.0000
        tickSize = 0.0001
        baseAssetPrecision = 8
        quoteAssetPrecision = 8
        
        if info:
            if 'baseAssetPrecision' in info.keys():
                baseAssetPrecision = info['baseAssetPrecision'] # coin
            if 'quoteAssetPrecision' in info.keys():
                quoteAssetPrecision = info['quoteAssetPrecision'] # USDT

            for f in info['filters']:
                if f['filterType'] == 'MIN_NOTIONAL':
                    applyToMarket = f['applyToMarket']
                    minNotional = float(f['minNotional'])
                if f['filterType'] == 'MARKET_LOT_SIZE':
                    minQty = float(f['minQty'])
                    maxQty = float(f['maxQty'])
                if f['filterType'] == 'LOT_SIZE':
                    #using stepSize from LOT_SIZE because this item in MARKET_LOT_SIZE is somehow 0...
                    stepSize = float(f['stepSize']) 
                if f['filterType'] == 'PRICE_FILTER':
                    minPrice = float(f['minPrice'])
                    maxPrice = float(f['maxPrice'])
                    tickSize = float(f['tickSize'])

        def minNotional_test():
            if applyToMarket:
                return True if minNotional <= order['order_price'] else False
            else:
                return True
            
        def minmaxPrice_test():
            return True if minPrice <= order['price'] and order['price'] <= maxPrice else False
        
        def minmaxQty_test():
            return True if minQty <= order['quantity'] and order['quantity'] <= maxQty else False
            
        def price_trim():
            p = round(order['price']/tickSize)*tickSize
            order['price'] = round(p,quoteAssetPrecision)
    
        def quantity_trim():
            q = round(order['quantity']/stepSize)*stepSize
            order['quantity'] = round(q,8)
            
        # run consistency tests
        tests = dict()
        
        is_op = 'order_price' in order.keys()
        is_p = 'price' in order.keys()
        is_q = 'quantity' in order.keys()
        
        if order['type'] == ORDER_TYPE_LIMIT:

            #order_price + price
            #if 'order_price' in order.keys() and 'quantity' not in order.keys():
            if is_op and is_p:
                tests['minNotional_test'] = minNotional_test()
                price_trim()
                tests['minmaxPrice_test'] = minmaxPrice_test()
                order['quantity'] = order['order_price']/order['price']
                quantity_trim()
                tests['minmaxQty_test'] = minmaxQty_test()
                
            #price + quantity
            elif is_q:
                price_trim()
                tests['minmaxPrice_test'] = minmaxPrice_test()
                quantity_trim()
                tests['minmaxQty_test'] = minmaxQty_test()
                order['order_price'] = order['quantity']*order['price']
                tests['minNotional_test'] = minNotional_test()
                              
        elif order['type'] == ORDER_TYPE_MARKET:
            
            last_price = await self.get_last_price(order['symbol'])
            
            #order_price
            if  is_op:
                tests['minNotional_test'] = minNotional_test()
                order['quantity'] = order['order_price']/last_price
                quantity_trim()
                order['order_price'] = order['quantity']*last_price
                tests['minmaxQty_test'] = minmaxQty_test()
                
            #quantity
            elif is_q:
                quantity_trim()
                tests['minmaxQty_test'] = minmaxQty_test()
                order['order_price'] = order['quantity']*last_price
                tests['minNotional_test'] = minNotional_test()
                
#         _ = order.pop('order_price')   
        return order
    
    async def execute_sell_order(self,order):
        if not self.mock_orders:
            order_crop = order.copy()
            #_ = order_crop.pop('order_price')
            _ = order_crop.pop('mode')
            _ = order_crop.pop('mock')
            out = await self.client.create_order(**order_crop)
            #print(out)
#             
        else:
            last_price = await self.get_last_price(order['symbol'])
            out = {'symbol': order['symbol'],
                   'orderId': random.randint(0,100000),
                   'transactTime': int(datetime.timestamp(datetime.now())),
                   'price': last_price,
                   'origQty': order['quantity'],
                   'executedQty': order['quantity'],
                   'status': 'FILLED', 
                   'side': 'SELL',
                   'fills': list()}
            
        order['origQty'] = float(out['origQty'])
        order['executedQty'] = float(out['executedQty'])
        order['transactTime'] = int(out['transactTime'])
        order['status'] = out['status']
        p = float(out['price'])
        order['price'] = price_average(out['fills']) if p==0 else p
        order['id'] = int(out['orderId'])
        
        return order
    
    async def execute_buy_order(self,order):
        order = await self.extend_order(order)
        order = await self.verify_order(order)

        if not self.mock_orders:
            order_crop = order.copy()
            _ = order_crop.pop('order_price')
            _ = order_crop.pop('mode')
            _ = order_crop.pop('mock')
            out = await self.client.create_order(**order_crop)
        else:
            #last_price = await self.get_last_price(order['symbol'])
            out = {'origQty':order['quantity'],
                   'executedQty':order['quantity'],
                   'transactTime':int(datetime.timestamp(datetime.now())),
                   'status':'FILLED',
                   'side': 'BUY',
                   'price':order['price'],
                   'orderId':random.randint(0,100000),
                   'fills':list()}

        
        order['origQty'] = float(out['origQty'])
        order['executedQty'] = float(out['executedQty'])
        order['transactTime'] = int(out['transactTime'])
        order['status'] = out['status']
        p = float(out['price'])
        order['price'] = price_average(out['fills']) if p==0 else p
        order['id'] = int(out['orderId'])

        return order
        
    async def load_order(self,order):
        norder = order.copy()
        if 'orderId' in norder.keys():
            oid_key = 'orderId'
        elif 'id' in norder.keys():
            oid_key = 'id'
        else:
            oid_key = 'orderId'
            
        out = await self.client.get_order(symbol = order['symbol'], orderId = order[oid_key])
#         print('get_order',out)
        norder['origQty'] = float(out['origQty'])
        norder['executedQty'] = float(out['executedQty'])
        norder['transactTime'] = int(out['time'])
        norder['status'] = out['status']
        p = float(out['price'])

        norder['price'] = float(out['cummulativeQuoteQty'])/float(out['executedQty']) if p==0 else p
        norder['id'] = int(out['orderId'])
        
        #added for compatibility
        
        norder['type'] = out['type']
        if norder['type'] == ORDER_TYPE_LIMIT:
            norder['timeInForce'] = out['timeInForce']
        norder['newOrderRespType'] = ORDER_RESP_TYPE_FULL
        norder['quantity'] = norder['origQty']
        norder['order_price'] = norder['price']*norder['quantity']
        _ = norder.pop(oid_key)
        
        return norder
        
    async def insert_order_db(self,order):
        query = '''INSERT INTO "order"(id,symbol,type,side,transactTime,price,origQty,executedQty,status) VALUES (?,?,?,?,?,?,?,?,?)'''
        rec = [order['id'],order['symbol'],order['type'],
               order['side'],order['transactTime'],order['price'],
               order['origQty'],order['executedQty'],order['status']]

        async with aiosqlite.connect(**self.db_args) as db:
            out = await db.execute(query,rec)
            await db.commit()
        
    async def update_order_db(self):
        query='''SELECT * FROM "order"'''
        async with aiosqlite.connect(**self.db_args) as db:
            async with db.execute(query) as cursor:
                async for row in cursor:
                    
                    symbol = row[1]
                    trasnactTime = row[4]
                    executedQty = row[-2]
                    status = row[-1]
                    orderId = row[0]
                    
                    if status == 'NEW':
                        out = await self.client.get_order(symbol=symbol,orderId=orderId)
                        
                        transactTime_new = int(out['updateTime'])
                        executedQty_new = float(out['executedQty'])
                        status_new = out['status']
                        
                        query = '''UPDATE "order" SET transactTime=?, executedQty=?, status=? WHERE id=?'''
                        rec  = [transactTime_new,executedQty_new,status_new,orderId]
                        await db.execute(query,rec)
                        await db.commit()
            
    async def update_loop(self):
        while True:
            if not self.mock_orders:
                await self.update_order_db()
            await asyncio.sleep(self.update_refresh_time)
#             break

    async def run(self):
            self.logger.info(f'Starting trader_proc()')
            with order_server() as sock:
                while True:
                    order = await sock.recv_json()
                    self.mock_orders = order['mock']
                    mockstr = 'REAL'
                    if self.mock_orders:
                        mockstr = 'MOCK'
                    self.logger.info(f'{mockstr} order execution commences')
                    
                    if order['side'] == SIDE_SELL:
                        
                        order_executed = await self.execute_sell_order(order)
                        if order_executed['status'] == 'FILLED':
                            self.logger.info('...order is FILLED.')
                            await sock.send_json(order_executed)
                        else:
                            raise Exception('problem with SELL order')
                        
                    elif order['side'] == SIDE_BUY:    

                        if order['mode'] == 'new':
                            order_executed = await self.execute_buy_order(order)

                            if self.mock_orders: 
                                order_executed['type'] += '_MOCK'
                            await self.insert_order_db(order_executed)

                        elif order['mode'] == 'existing':
                            order_executed = await self.load_order(order)

                        else:
                            raise Exception('wrong order mode')

                        msg = ''

                        msg += f"{order_executed['id']} | {order_executed['side']} {order_executed['type']} ORDER ({order_executed['status']})"
                        msg += f": {order_executed['symbol']} | "
                        msg += f"quantity={order_executed['origQty']}; "
                        msg += f"price={order_executed['price']}; "
                        msg += f"order_price={order_executed['order_price']}"
                        self.logger.info(msg)

    #                     print('order_executed: ',order_executed)

                        if self.mock_orders: #when mocked, send the order as executed right away
                            self.logger.info('sending order right away')         
                            await sock.send_json(order_executed)
                            continue

                        if order_executed['status'] == 'CANCELED':
                            raise Exception('order was CANCELED')

                        if order_executed['type'] == ORDER_TYPE_MARKET:
                            if order_executed['status'] == 'FILLED':
                                pass
                            else:
                                raise Exception('wrong MARKET order; not FILLED')

                        elif order_executed['type'] == ORDER_TYPE_LIMIT:
                            filled = False if order_executed['status'] == 'NEW' else True
                            while not filled:
                                self.logger.info('LIMIT order not yet filled...')
                                order_executed = await self.load_order(order)
                                #print('order',order_executed)
                                filled = False if order_executed['status'] == 'NEW' else True
                                await asyncio.sleep(self.update_refresh_time)

                        if order_executed['status'] == 'FILLED':
                            self.logger.info('...order is FILLED.')
                            await sock.send_json(order_executed)
                        elif order_executed['status'] == 'CANCELED':
                            self.logger.info('...order was CANCELED.')
                            await sock.send_json(order_executed)

    async def close(self):
        print('trader close')
        await self.client.close_connection()    
            
async def main(args) -> None:
    try:


        auth = load_auth(args.auth_file)

        bclient = await create_binance_client(auth['binance_api'],
                                              auth['binance_secret'])    

        proc = trader_proc(client = bclient,
                           args = args)

        tasks = []
        tasks += [asyncio.create_task(proc.run())]
        tasks += [asyncio.create_task(proc.update_loop())]
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
    

