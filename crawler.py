import aiohttp
import asyncio
from datetime import datetime,timezone
import logging
import pytz
import sqlite3
from telethon.errors import FloodWaitError
import re
import yaml

from enums import *
from funcs import create_binance_client, create_telegram_client
from funcs import load_cfg, load_auth, load_track
from funcs import create_logger, error_handler, set_argparser
from funcs import extract_url
from sockets import crawler_socket_rep

class crawler_proc:

    def __init__(self,binance_client,telegram_client,args):
        self.bclient = binance_client
        self.tclient = telegram_client
        self.args = args
        self.cfg = load_cfg(args.cfg_file)
        self.db_args = self.cfg['db']
        self.scan_interval = self.cfg['crawler']['scan_interval']
        self.channel_url = self.cfg['crawler']['channel_url']
        self.init_msg = self.cfg['crawler']['init_telegram_msg']
        self.local_timezone = self.cfg['general']['timezone']
        self.pairing = self.cfg['general']['pair']
        self.logger = create_logger(name='crawler')
        self.mock_msg = True

    # new_coins crawler
    async def is_msg_about_new_coin(self, msg_dict: dict) -> dict:
        msg = msg_dict['message']
        if 'Binance Will List' in msg:

            extract = re.search('\(.+\)',msg)
            coin_name = extract[0][1:-1] if extract else ''
    #         extract = re.search('(www|http:|https:)+[^\s]+[\w]',msg)
    #         news_url = extract[0] if extract else ''
            news_url = extract_url(msg)

            async with aiohttp.ClientSession() as session:
                async with session.request('GET',news_url) as resp:
                    msg_web = await resp.text()

            extract = re.search('[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2} \(.*?\)',msg_web)
            date_format = "%Y-%m-%d %H:%M (%Z)"
            time_str = extract[0] if extract else '1000-1-1 00:00 (UTC)'
            time_dt = datetime.strptime(time_str,date_format)

            extract = re.search('\(.*?\)',time_str)
            tz = pytz.timezone(extract[0][1:-1] if extract else 'UTC') #extract timezone
            tz_local = pytz.timezone(self.local_timezone) #use local timezone

            time_dt_tz = tz.localize(time_dt)

            time_dt_tz_local = time_dt_tz.astimezone(tz_local)

            msg_time_local = msg_dict['date'].astimezone(tz_local)

            return {'coin_name': coin_name,
                    'symbol': coin_name+self.pairing,
                    'start_time': time_dt_tz_local.isoformat(),
                    'msg_time': msg_time_local.isoformat(),
                    #'start_time': int(time_dt_tz_local.timestamp()),
                    #'msg_time': int(msg_time_local.timestamp()),
                   }

    async def crawl_loop(self,sock):

        while True:

            if not self.mock_msg:
                msg_channel = await self.tclient.get_entity(self.channel_url)
                newest_msg = (await self.tclient.get_messages(msg_channel,limit=self.init_msg+1))[-self.init_msg].to_dict()
#                 newest_msg = (await self.tclient.get_messages(msg_channel,limit=50))[29].to_dict()
#                 for i,m in enumerate(await self.tclient.get_messages(msg_channel,limit=50)):
#                     print(i,m.to_dict()['message'])
            else:
                self.new_msg_id = 0
                #mock message
                newest_msg = {'id': 3270,
                              'date': datetime(2021, 10, 5, 2, 39, 36, tzinfo=timezone.utc),
                              'message': 'Binance Will List Adventure Gold (AGLD) in the Innovation Zone\nhttps://www.binance.com/en/support/announcement/4a0b5201a6874cfe98adad4f74b6a75b'}

            if newest_msg['id'] != self.new_msg_id:
                self.new_msg_id = newest_msg['id']
                self.logger.info(f"new msg... {extract_url(newest_msg['message'])}")
                new_coin = await self.is_msg_about_new_coin(newest_msg)
                if new_coin:
                    self.logger.info(f"... on a new coin {new_coin['coin_name']}!")

                    new_coin['resp'] = RESPONSE_OK
                    await sock.send_json(new_coin)
                    break
                else:
                    self.logger.info('... but NOT about a new coin.')
            else:
                self.logger.info('no new msg.')
            await asyncio.sleep(self.scan_interval)

    async def run_server(self):
        self.logger.info('Starting crawler_proc()')
        with crawler_socket_rep() as sock:

                while True:
                    try:
                        self.new_msg_id = 0
                        crawl_request = await sock.recv_json()

                        self.mock_msg = crawl_request['mock']
                        mockstr = 'REAL'
                        if self.mock_msg:
                            mockstr = 'MOCK'
                        self.logger.info(f'{mockstr} crawling commences')
                        await self.crawl_loop(sock)

                    except FloodWaitError as e:
                        self.logger.info(f'FloodWaitError: {str(e)}')
                        # self.logger.info(e)
                        await asyncio.sleep(60)
                        pass



    async def close(self):
        print('crawler close')
        await self.bclient.close_connection()
        await self.tclient.close_connection()

async def main(args) -> None:
    try:

        auth = load_auth(args.auth_file)

        bclient = await create_binance_client(auth['binance_api'],
                                              auth['binance_secret'])

        tclient = await create_telegram_client(auth['telegram_api_id'],
                                               auth['telegram_api_hash'],
                                               auth['telegram_phone'])

        proc = crawler_proc(binance_client = bclient,
                            telegram_client = tclient,
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
