import aiohttp
import aiosqlite
import argparse
import asyncio
from binance import AsyncClient as BinanceClient
import json
import logging
import numpy as np
import pandas as pd
import re
from telethon import TelegramClient
import yaml

from enums import *

async def create_binance_client(binance_api: str,
                                binance_secret: str) -> BinanceClient:

    client = await BinanceClient.create(binance_api, binance_secret)
    logging.info(f"Binance client {client} connected.")
   
    return client

async def create_telegram_client(api_id: str,
                                 api_hash: str,
                                 phone: str) -> TelegramClient:
        
    client = await TelegramClient('test_session', api_id, api_hash).start(phone=phone)
    logging.info(f"Telegram client {client} connected.")
    return client

def load_auth(file = 'cfg/auth.yml'):
    
    with open(file) as fp:
        auth = yaml.load(fp, Loader=yaml.FullLoader)
    return auth

# async def load_cfg(db_args):
#     cfg = dict()
#     async with aiosqlite.connect(**db_args) as db:
#          async with db.execute("SELECT * FROM config") as cursor:
#                 async for row in cursor:
#                     cfg[row[1]] = json.loads(row[2])
                    
#     return cfg

def load_cfg(file = 'cfg/config.yml'):
    
    with open(file,'r') as fp:
        cfg = yaml.load(fp, Loader=yaml.FullLoader)
    return cfg

def load_track(file = 'tracks/new_coin_limit.yml'):
    
    with open(file,'r') as fp:
        track_cfg = yaml.load(fp, Loader=yaml.FullLoader)
    return track_cfg


async def aio_pd_read_sql(query,db):
    async with db.execute(query) as cursor:
        columns = [col_desc[0] for col_desc in cursor.description]
        result = await cursor.fetchall()
        if not isinstance(result, list):
            result = list(result)
        
    return pd.DataFrame.from_records(result,columns=columns)


async def id_to_symbol(db_args,hid):
    async with aiosqlite.connect(**db_args) as db:
        #async with db.execute("SELECT symbol FROM coin WHERE id=? LIMIT 1",[hid]) as cursor:
        async with db.execute("SELECT symbol FROM data_harvester WHERE id=? LIMIT 1",[hid]) as cursor:
                return (await cursor.fetchall())[0][0]
            
def price_average(fills):
    enum = 0
    denom = 0
    for fill in fills:
        p = float(fill['price'])
        q = float(fill['qty'])
        enum += p*q
        denom += q
        
    return enum/denom

def extract_url(msg):
    extract = re.search('(www|http:|https:)+[^\s]+[\w]',msg)
    url = extract[0] if extract else ''
    
    return url

def moving_average(a, n=3) :
    ret = np.cumsum(a, dtype=float)
    ret[n:] = ret[n:] - ret[:-n]
    return ret[n - 1:] / n


def error_handler(sig,loop):
    loop.stop()
    print(f'Got signal: {sig!s}, shutting down.')
    loop.remove_signal_handler(SIGTERM)
    loop.add_signal_handler(SIGINT, lambda: None)
    
    
def create_logger(name,fmt="%(asctime)s : %(name)s : %(funcName)s() : %(message)s"):
    # create logger
    logger = logging.Logger(name)
    logger.setLevel(logging.INFO)

    # create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)

    # create formatter
    formatter = logging.Formatter(fmt)

    # add formatter to ch
    ch.setFormatter(formatter)

    # add ch to logger
    logger.addHandler(ch)
    
    return logger

async def estimate_price(coinapi_apikey,base_asset,time_start,time_end,quote_asset='USDT',period='5MIN'):
    
    if isinstance(time_start,int):
        time_start = datetime.fromtimestamp(time_start)

    if isinstance(time_end,int):
        time_end = datetime.fromtimestamp(time_end)
    
    parameters = {'time_start': time_start.isoformat(),
              'time_end': time_end.isoformat(),
              'period_id':period}

    headers = {
      'Accepts': 'application/json',
      'X-CoinAPI-Key': coinapi_apikey,
    }
    
    url = 'https://rest.coinapi.io/v1/exchangerate/'
#     base_asset = 'AGLD'
#     quote_asset = 'USDT'
    url = url+base_asset+'/'+quote_asset+'/history'
    
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=parameters, headers=headers) as response:
            out = await response.text()
            data = json.loads(out)
            
    r_av = 0
    rlow_av = 0
    rhigh_av = 0

    for d in data:
        rhigh_av+=d['rate_high']
        rlow_av+=d['rate_low']
    
    rhigh_av = rhigh_av/len(data)
    rlow_av = rlow_av/len(data)
    r_av = (rhigh_av+rlow_av)/2
    return {'price_average':r_av,'price_high_average':rhigh_av,'price_low_average':rlow_av}


def set_argparser():
    
    parser = argparse.ArgumentParser()

    parser.add_argument('--cfg_file', default='cfg/config.yml', type=str)
    parser.add_argument('--auth_file', default='cfg/auth.yml', type=str)
    parser.add_argument('--track_file', default='tracks/new_coin_limit.yml', type=str)
    

    args = parser.parse_args()
    return args