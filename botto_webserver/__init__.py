import os

from flask import Flask, render_template, request

from flask_socketio import SocketIO, emit

import pandas as pd
import json
import plotly
import plotly.express as px

from datetime import datetime

def create_app(path='/home/grela/local_project/binance/botto-v2'):
    
    from . import db
    
    app = Flask(__name__, instance_path=path,instance_relative_config=True)
    app.config.from_mapping(
        SECRET_KEY='dev',
        DATABASE=os.path.join(app.instance_path,'cfg/main.db'),
    )
    
    #init database
    db.init_app(app)
    
    #socketio
    socketio = SocketIO(app)
    
    @app.route('/')
    def overview():
        return render_template('overview.html')
    
    @app.route('/harvester')
    def harvester():
        return render_template('harvester.html')

    @app.route('/trader')
    def trader():        
        return render_template('trader.html')

    @app.route('/agent')
    def agent():        
        return render_template('agent.html')  

    @socketio.on('get_harvester_list')
    def get_harvester_list():
        dbase = db.get_db()
        cursor = dbase.execute('SELECT * FROM data_harvester')
        dbase.commit()
        
        lcursor = list(cursor)
        
        cursor2 = dbase.execute('SELECT harvester_id, COUNT(*) FROM data_harvester_data GROUP BY harvester_id')
        dbase.commit()
        data_sizes = list(cursor2)

        now = datetime.now()
        current_time = now.strftime("%H:%M:%S")
        
        emit('get_harvester_list', {'list':lcursor,'data_sizes':data_sizes,'time':current_time})
        
    @socketio.on('update_harvester')
    def update_harvester(record):
        dbase = db.get_db()
        cursor = dbase.execute('UPDATE data_harvester SET name=?,symbol=?,active=? WHERE id=?',record[1:]+record[:1])
        dbase.commit()

    @socketio.on('remove_harvester')
    def remove_harvester(record):
        dbase = db.get_db()
        cursor = dbase.execute('DELETE FROM data_harvester WHERE id='+str(record[0]))
        dbase.commit()

    @socketio.on('create_harvester')
    def create_harvester(record):
        dbase = db.get_db()
        cursor = dbase.execute('INSERT INTO data_harvester (name,symbol,active) VALUES (?,?,?)',record)
        dbase.commit()
    
#     def gm(country='United Kingdom'):
#         df = pd.DataFrame(px.data.gapminder())

#         fig = px.line(df[df['country']==country], x="year", y="gdpPercap", title=country)

#         graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
#         return graphJSON
    
    
    
    return socketio, app
