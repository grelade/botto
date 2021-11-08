import sys
import logging
import asyncio
import yaml

from funcs import create_binance_client, create_telegram_client, load_auth, load_cfg

from signal import SIGINT, SIGTERM
from funcs import error_handler

from cpu import cpu_proc
from agent import agent_proc
from crawler import crawler_proc
from trader import trader_proc
from data_harvester import harvester_proc

async def main(track_cfg):
    try:
        db_args={'database':'cfg/config.db',
                 'isolation_level':None,
                 'check_same_thread':False}    

        cfg = await load_cfg(db_args)

#         logname = 'botto-agent.log'
        log_format = "%(asctime)s : %(name)s : %(funcName)s() : %(message)s"

#         if cfg['general']['logging'] == 'to_file':
#             logging.basicConfig(filename=logname,filemode='a',format=log_format, level=logging.INFO)
#         elif cfg['general']['logging'] == 'to_screen':
        logging.basicConfig(format=log_format, level=logging.INFO)

        auth = load_auth()    

        bclient = await create_binance_client(auth['binance_api'],
                                              auth['binance_secret'])    

        tclient = await create_telegram_client(auth['telegram_api_id'],
                                               auth['telegram_api_hash'],
                                               auth['telegram_phone'])
        
        cpu = cpu_proc(client=bclient,
                       db_args=db_args,
                       track_cfg=track_cfg)
        
        agent = agent_proc(client=bclient,
                           db_args=db_args)
        
        crawler = crawler_proc(binance_client=bclient,
                               telegram_client=tclient,
                               db_args=db_args,
                               cfg=track_cfg)
        
        harvester = harvester_proc(client=bclient,
                                   db_args=db_args)
        
        trader = trader_proc(client=bclient,
                             db_args=db_args,
                             cfg=track_cfg)
        
        tasks = []

        tasks += [asyncio.create_task(cpu.run())]
        
        tasks += [asyncio.create_task(crawler.run())]
#         tasks += [asyncio.create_task(crawler.emit_newcoin())]

        tasks += [asyncio.create_task(trader.run())]
        tasks += [asyncio.create_task(trader.update_loop())]        

        tasks += [asyncio.create_task(harvester.run())]
        tasks += [asyncio.create_task(harvester.emit_data_raw())]
        tasks += [asyncio.create_task(harvester.run_server())]        
                
        tasks += [asyncio.create_task(agent.run())]
        tasks += [asyncio.create_task(agent.emit_order())]
        tasks += [asyncio.create_task(agent.run_server())]

        await asyncio.gather(*tasks)

    except asyncio.CancelledError:

        await agent.close()
        await harvester.close() 
        await trader.close()
        await cpu.close()
        await crawler.close()
       


if __name__ == "__main__":
    
    if len(sys.argv)>=2:
        with open(sys.argv[1],'r') as file:
            track_cfg = yaml.load(file, Loader=yaml.FullLoader)
    else:
        print('need track yaml file')   
        
    loop = asyncio.get_event_loop()
    for sig in (SIGTERM, SIGINT):
        loop.add_signal_handler(sig, error_handler, sig, loop)
    loop.create_task(main(track_cfg))
    loop.run_forever()
    tasks = asyncio.all_tasks(loop=loop)
    for t in tasks:
        t.cancel()
    group = asyncio.gather(*tasks, return_exceptions=True)
    loop.run_until_complete(group)
    loop.close()
    
#     asyncio.run(main())