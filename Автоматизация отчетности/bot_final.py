import pandahouse 
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

import io
import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

connection = {'host': 'https://***',
                      'database':'***',
                      'user':'***',
                      'password':'***'
                     }

#дефолтные параметры, котороые прокидываются в таски
default_args = {
    'owner': 'i.vashkovets',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 6, 1),
}

    
#интервал запуска dag (необходимо в 11 утра)
schedule_interval = '0 11 * * *'

my_token = '***'
bot = telegram.Bot(token=my_token)
chat_id = -938659***


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False, tags=['i-vashkovets-19'])

def telegram_bot_go_go():
    @task
    def extract_yesterday(): 
        query = '''
        SELECT time::DATE date,
                       COUNT(DISTINCT user_id) as dau,
                       ROUND(countIf(user_id, action='like') / countIf(user_id, action='view'), 2) AS ctr,
                       countIf(user_id, action='like') as likes,
                       countIf(user_id, action='view') as views
        FROM simulator_20230520.feed_actions
        WHERE time::DATE = yesterday()
        GROUP BY time::DATE
        '''

        df = pandahouse.read_clickhouse(query, connection=connection)
        return df
    
    @task
    def extract_week():
        query2 = """
        SELECT time::DATE AS date, 
                       COUNT(DISTINCT user_id) as dau,
                       countIf(user_id, action='like') as likes,
                       countIf(user_id, action='view') as views,
                       ROUND(countIf(user_id, action='like') / countIf(user_id, action='view'), 2) AS ctr
        FROM simulator_20230520.feed_actions
        WHERE time::DATE BETWEEN yesterday() - 6 AND yesterday()
        GROUP BY time::DATE
        """

        df2 = pandahouse.read_clickhouse(query2, connection=connection)
        return df2
    
    @task
    def message(df, chat_id= None): 
        chat_id = chat_id or 397884***
        yesterday_date = df['date'].astype(str).values[0]
        dau = df.dau.values[0]
        ctr = df.ctr.values[0]
        likes = df.likes.values[0]
        views = df.views.values[0]
        msg = f'Лента новостей:\nDate: {yesterday_date}\nDAU: {dau}\nLikes: {likes}\nViews: {views}\nCTR: {ctr}'
               
        bot.sendMessage(chat_id=chat_id, text=msg)
        
    @task
    def photo(df2, chat_id=None):
        chat_id = chat_id or 397884***
        #dau
        ax = sns.lineplot(x=df2.date.dt.strftime('%m-%d'), y=df2.dau)
        plt.title('Daily Active Users')
        plt.xlabel('')
        plt.ylabel('')
        plt.grid(True)
        plt.xticks(rotation=45)
        
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'photo.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        
        #likes
        ax = sns.lineplot(x=df2.date.dt.strftime('%m-%d'), y=df2.likes)
        plt.title('Likes')
        plt.xlabel('')
        plt.ylabel('')
        plt.grid(True)
        plt.xticks(rotation=45)
        
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'photo.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        
        #Views
        ax = sns.lineplot(x=df2.date.dt.strftime('%m-%d'), y=df2.views)
        plt.title('Views')
        plt.xlabel('')
        plt.ylabel('')
        plt.grid(True)
        plt.xticks(rotation=45)
        
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'photo.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        
        #CTR
        ax = sns.lineplot(x=df2.date.dt.strftime('%m-%d'), y=df2.ctr)
        plt.title('CTR')
        plt.xlabel('')
        plt.ylabel('')
        plt.grid(True)
        plt.xticks(rotation=45)
        
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'photo.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
    
    
    df = extract_yesterday()
    df2 = extract_week()
    message(df, chat_id)
    photo(df2, chat_id)    
        

telegram_bot_go_go = telegram_bot_go_go()        