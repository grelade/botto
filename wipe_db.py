import aiosqlite
import asyncio

db_args={'database':'cfg/main.db',
                 'isolation_level':None,
                 'check_same_thread':False}   

async def cleandb():
    async with aiosqlite.connect(**db_args) as db:
        out = await db.execute("DELETE FROM agent")
        out = await db.execute("DELETE FROM data_harvester")
        await db.commit()
        
        
asyncio.run(cleandb())