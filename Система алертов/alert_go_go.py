import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import telegram
import pandahouse
from datetime import date
import io
from read_db.CH import Getch

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

connection = {'host': 'https://***',
                      'database':'***',
                      'user':'***',
                      'password':'***'
}

default_args = {
    'owner': 'i.vashkovets',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 6, 1),
}


schedule_interval = '*/15 * * * *'

bot = telegram.Bot(token='***')
chat_id = -938659***

def check_anomaly(df, metric, a=4, n=5):
    df['q25'] = df[metric].shift(1).rolling(n).quantile(0.25)
    df['q75'] = df[metric].shift(1).rolling(n).quantile(0.75)
    df['iqr'] = df['q75'] - df['q25']
    df['up'] = df['q75'] + a*df['iqr']
    df['low'] = df['q25'] - a*df['iqr']
    
    df['up'] = df['up'].rolling(n, center=True, min_periods=1).mean()
    df['low'] = df['low'].rolling(n, center=True, min_periods=1).mean()
    
    if df[metric].iloc[-1] < df['low'].iloc[-1] or df[metric].iloc[-1] > df['up'].iloc[-1]:
        is_alert = 1
    else:
        is_alert = 0
    
    return is_alert, df


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False, tags=['i-vashkovets-19'])
def alert_bot_go_go_go():
    @task
    def get_data():
        chat_id = chat or 558189***


        query = ('''SELECT *
                            FROM
                            (SELECT 
                             toStartOfFifteenMinutes(time) as ts
                            , time::DATE date
                            , formatDateTime(ts, '%R') as hm
                            , uniqExact(user_id) as users_feed
                            , countIf(user_id, action='view') as views
                            , countIf(user_id, action='like') as likes
                            , ROUND(100 * (countIf(user_id, action='like')/ countIf(user_id, action='view')), 2) as ctr

                        FROM simulator_20230520.feed_actions
                        WHERE time >= today() and time < toStartOfFifteenMinutes(now())
                        GROUP BY ts, date, hm
                        ORDER BY ts) t1

                        FULL JOIN

                        (SELECT 
                             toStartOfFifteenMinutes(time) as ts
                            , time::DATE date
                            , formatDateTime(ts, '%R') as hm
                            , uniqExact(user_id) as users_messanger
                            , COUNT(user_id) as messages

                        FROM simulator_20230520.message_actions
                        WHERE time >= today() and time < toStartOfFifteenMinutes(now())
                        GROUP BY ts, date, hm
                        ORDER BY ts)t2

                        using ts, date, hm
                        order by ts                    
                        ''')
        df = pandahouse.read_clickhouse(query, connection=connection)
        return df
    
    @task
    def send_alerts():
        metrics_list = ['users_feed', 'views', 'likes', 'ctr', 'users_messanger', 'messages']
        
        for metric in metrics_list:

        df = data[['ts', 'date', 'hm', metric]].copy()
        is_alert, df = check_anomaly(df, metric)  

        if is_alert == 1:
            msg = '''Метрика {metric}:\n текущее значение {current_val:.2f}\nотклонение от предыдущего значения {last_val_diff:.2%}\nСсылка на дашборд - {url}'''.format(metric=metric,
                                                                                                                                    current_val=df[metric].iloc[-1],
                                                                                                                                    last_val_diff= abs(1 - (df[metric].iloc[-1]/df[metric].iloc[-2])),
                                                                                                                                    url='https://superset.lab.karpov.courses/superset/dashboard/3724/')
        
 
        
        sns.set(rc={'figure.figsize':(16, 10)})
        plt.tight_layout()
        
        ax = sns.lineplot(x=df['ts'], y=df[metric], label='metric') 
        ax = sns.lineplot(x=df['ts'], y=df['up'], label='up')
        ax = sns.lineplot(x=df['ts'], y=df['low'], label='low')
        
        for ind, label in enumerate(ax.get_xticklabels()):
            if ind % 2 == 0:
                label.set_visible(True)
            else:
                label.set_visible(False)
                
        ax.set(xlabel='time')
        ax.set(ylabel=metric)
        
        ax.set_title(metric)
        ax.set(ylim=(0, None))
        
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = '{0}.png'.format(metric)
        plt.close()
            
        bot.sendMessage(chat_id=chat_id, text=msg)
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        
        
    df = get_data()
    send_alerts(df)
    

alert_bot_go_go_go = alert_bot_go_go_go()    
    



