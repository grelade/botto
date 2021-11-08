import aiosqlite
import asyncio
import yaml

db_args={'database':'cfg/main.db',
                 'isolation_level':None,
                 'check_same_thread':False}   

async def createdb():
    
    with open('cfg/main.db.sql') as sql_file:
        sql_schema = sql_file.read()
    
    async with aiosqlite.connect(**db_args) as db:
        
        out = await db.executescript(sql_schema)
        await db.commit()
        
        
asyncio.run(createdb())