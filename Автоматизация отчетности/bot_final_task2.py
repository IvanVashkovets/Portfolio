#импорт библиотек
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

#коннект в БД
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

#параметры бота и чата
my_token = '***'
bot = telegram.Bot(token=my_token)
chat_id = -938659***


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False, tags=['i-vashkovets-19'])

def telegram_bot_go_go_go():
    @task
    #извлечение общей статистики(количество пользователей приложения, дата(вчера), процент органиков и рекламных за прошедший день)
    def extract_yesterday(): 
        query = '''
        SELECT COUNT(user_id) app_users,
        date,
        ROUND(countIf(user_id, source = 'organic') / COUNT(user_id) * 100, 2) as organic,
        ROUND(countIf(user_id, source = 'ads') / COUNT(user_id) * 100, 2) as ads
        FROM(SELECT DISTINCT user_id, 
                       time::DATE as date,
                       source
             FROM simulator_20230520.feed_actions as fa
             JOIN simulator_20230520.message_actions as ma
             ON fa.user_id=ma.user_id
             WHERE time::DATE = yesterday())
        GROUP BY date
        '''

        df = pandahouse.read_clickhouse(query, connection=connection)
        return df
    
    @task
    #отправка сообщения со статистиками
    def message(df, chat_id): 
        yesterday_date = df['date'].astype(str).values[0]
        app_users = df.app_users.values[0]
        organic = df.organic.values[0]
        ads = df.ads.values[0]
        msg = f'Приложение:\nDate: {yesterday_date}\nUnique_users: {app_users}\nOrganic: {organic}%\nADS: {ads}%'
               
        bot.sendMessage(chat_id=chat_id, text=msg)
    
    @task
    #запрос DAU
    def extract_week():
        query2 = """
            SELECT time::DATE date, COUNT(DISTINCT user_id) users
            FROM simulator_20230520.feed_actions as fa
            JOIN simulator_20230520.message_actions as ma
            ON fa.user_id=ma.user_id
            WHERE time::DATE BETWEEN yesterday() - 6 AND yesterday()
            GROUP BY time::DATE
            ORDER BY time::DATE
            """

        df2 = pandahouse.read_clickhouse(query2, connection=connection)
        return df2
        
    @task
    #отправка DAU
    def photo(df2, chat_id):    
        #dau       
        ax = sns.lineplot(x=df2.date.dt.strftime('%m-%d'), y=df2.users)
        plt.title('Daily Active Users APP (last week)')
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
        
    @task
    #ретеншн второго дня для ленты
    def extract_retantion_feed():
        query4 = """
          SELECT date, start_date, COUNT(user_id) users

            FROM
            (SELECT user_id, min(time::DATE) start_date
            FROM simulator_20230520.feed_actions
            GROUP BY user_id
            ) t1

            JOIN

            (SELECT DISTINCT(user_id), time::DATE as date
            FROM simulator_20230520.feed_actions 
            ) t2

            using user_id

            GROUP BY date, start_date
            HAVING start_date >= yesterday() - 13
            """

        df4 = pandahouse.read_clickhouse(query4, connection=connection)
        df4['days_since_start'] = (pd.to_datetime(df4['date']) - pd.to_datetime(df4['start_date'])).dt.days
        cohorts = df4[df4['days_since_start'].isin([0,1])]
        cohorts = cohorts.groupby('start_date').agg({'users': 'sum', 'days_since_start': 'min'})
        cohorts.rename(columns={'users': 'total_users', 'days_since_start': 'cohort_day'}, inplace=True)
        cohorts.reset_index(inplace=True)
        retention = pd.merge(cohorts, df4[df4['days_since_start'] == 1][['start_date', 'users']], on='start_date')
        retention.rename(columns={'users': 'users_2'}, inplace=True)
        retention['retention'] = round(retention['users_2'] / retention['total_users'] * 100, 2)
        return retention
    
    @task
    #отправка ретеншена второго дня для ленты
    def photo3(retention, chat_id):    
            
        ax = sns.lineplot(x=retention.start_date.dt.strftime('%m-%d'), y=retention.retention)
        plt.title('Retantion второго дня для ленты, % (last 2 weeks)')
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
        
    @task
    #ретеншн второго дня для сообщений
    def extract_retantion_mess():
        query5 = """
          SELECT date, start_date, COUNT(user_id) users

            FROM
            (SELECT user_id, min(time::DATE) start_date
            FROM simulator_20230520.message_actions
            GROUP BY user_id
            ) t1

            JOIN

            (SELECT DISTINCT(user_id), time::DATE as date
            FROM simulator_20230520.message_actions 
            ) t2

            using user_id

            GROUP BY date, start_date
            HAVING start_date >= yesterday() - 13
            """

        df5 = pandahouse.read_clickhouse(query5, connection=connection)
        df5['days_since_start'] = (pd.to_datetime(df5['date']) - pd.to_datetime(df5['start_date'])).dt.days
        cohorts2 = df5[df5['days_since_start'].isin([0,1])]
        cohorts2 = cohorts2.groupby('start_date').agg({'users': 'sum', 'days_since_start': 'min'})
        cohorts2.rename(columns={'users': 'total_users', 'days_since_start': 'cohort_day'}, inplace=True)
        cohorts2.reset_index(inplace=True)
        retention2 = pd.merge(cohorts2, df5[df5['days_since_start'] == 1][['start_date', 'users']], on='start_date')
        retention2.rename(columns={'users': 'users_2'}, inplace=True)
        retention2['retention'] = round(retention2['users_2'] / retention2['total_users'] * 100, 2)
        return retention2
    
    @task
    #отправка ретеншена второго дня для сообщений
    def photo4(retention2, chat_id):    
             
        ax = sns.lineplot(x=retention2.start_date.dt.strftime('%m-%d'), y=retention2.retention)
        plt.title('Retantion второго дня для мессенджера, % (last 2 weeks)')
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
    message(df, chat_id)
    df2 = extract_week()
    photo(df2, chat_id)
    retention = extract_retantion_feed()  
    photo3(retention, chat_id)
    retention2 = extract_retantion_mess()    
    photo4(retention2, chat_id)
   
        

telegram_bot_go_go_go = telegram_bot_go_go_go()        
