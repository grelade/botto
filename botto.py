import argparse
import sys
import logging
import asyncio
import yaml

from funcs import create_binance_client, create_telegram_client
from funcs import load_cfg, load_auth, load_track

from signal import SIGINT, SIGTERM
from funcs import error_handler
from funcs import set_argparser
from funcs import create_logger

from cpu import cpu_proc
from agent import agent_proc
from crawler import crawler_proc
from trader import trader_proc
from data_harvester import harvester_proc

async def main(args):
    try:

        log_format = "%(asctime)s : %(name)s : %(funcName)s() : %(message)s"
        logging.basicConfig(format=log_format, level=logging.INFO)

        auth = load_auth(file=args.auth_file)    

        bclient = await create_binance_client(auth['binance_api'],
                                              auth['binance_secret'])

        tclient = await create_telegram_client(auth['telegram_api_id'],
                                               auth['telegram_api_hash'],
                                               auth['telegram_phone'])

        cpu = cpu_proc(client = bclient,
                       args = args)

        agent = agent_proc(client = bclient,
                           args = args)

        crawler = crawler_proc(binance_client = bclient,
                               telegram_client = tclient,
                               args = args)

        harvester = harvester_proc(client = bclient,
                                   args = args)

        trader = trader_proc(client = bclient,
                             args = args)

        tasks = []

        tasks += [asyncio.create_task(cpu.run_server())]

        tasks += [asyncio.create_task(crawler.run_server())]
#         tasks += [asyncio.create_task(crawler.emit_newcoin())]

        tasks += [asyncio.create_task(trader.run_server())]
        tasks += [asyncio.create_task(trader.update_loop())]

        tasks += [asyncio.create_task(harvester.stream_loop())]
        tasks += [asyncio.create_task(harvester.emit_data_raw())]
        tasks += [asyncio.create_task(harvester.run_server())]

        tasks += [asyncio.create_task(agent.agent_loop())]
        tasks += [asyncio.create_task(agent.send_order())]
        tasks += [asyncio.create_task(agent.run_server())]

        await asyncio.gather(*tasks)

    except asyncio.CancelledError:

        await agent.close()
        await harvester.close()
        await trader.close()
        await cpu.close()
        await crawler.close()



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
