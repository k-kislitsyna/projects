import telegram
import requests
import matplotlib.pyplot as plt
import seaborn as sns
import io
from io import StringIO
import pandas as pd
from airflow.decorators import dag, task
from datetime import timedelta
from datetime import datetime

def ch_get_df(query='', host='', user='', password=''):
    r = requests.post(host, data=query.encode("utf-8"), auth=(user, password), verify=False)
    result = pd.read_csv(StringIO(r.text), sep='\t')
    return result

def check_anomaly(data, metric, a=4, n=5):
    data['q25'] = data[metric].shift(1).rolling(n).quantile(0.25)
    data['q75'] = data[metric].shift(1).rolling(n).quantile(0.75)
    data['iqr'] = data['q75'] - data['q25']
    data['high'] = data['q75'] + a * data['iqr']
    data['low'] = data['q25'] - a * data['iqr']

    data['high'] = data['high'].rolling(n, center=True).mean()
    data['low'] = data['low'].rolling(n, center=True).mean()
    if data[metric].iloc[-1] < data['low'].iloc[-1] or data[metric].iloc[-1] > data['high'].iloc[-1]:
        alert_value = 1
    else:
        alert_value = 0

    return alert_value, data

def check_anomaly_and_send_message(df_message, metrics_list):
    token = ''
    chat_id =
    bot = telegram.Bot(token=token)

    for metric in metrics_list:
        print(metric)
        data = df_message[['ts', 'date', 'hm', metric]].copy()
        is_alert, data = check_anomaly(data, metric)

        if is_alert == 1:
            msg = '''Метрика {metric}:\nтекущее значение {current_val:.2f}\nотклонение от предыдущего значения {last_val_diff:.2%}'''.format(
                metric=metric,
                current_val=data[metric].iloc[-1],
                last_val_diff=1 - (data[metric].iloc[-1] / data[metric].iloc[-2]))
            sns.set(rc={'figure.figsize': (16, 10)})
            plt.tight_layout()

            ax = sns.lineplot(x=data['ts'], y=data[metric], label='metric')
            ax = sns.lineplot(x=data['ts'], y=data['high'], label='high')
            ax = sns.lineplot(x=data['ts'], y=data['low'], label='low')

            for ind, label in enumerate(ax.get_xticklabels()):
                if ind % 30 == 0:
                    label.set_visible(True)
                else:
                    label.set_visible(False)
            ax.set(xlabel='time')
            ax.set(ylabel=metric)

            ax.set_title(metric)
            ax.set(ylim=(0, None))

            plot_object = io.BytesIO()
            ax.figure.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = '{0}.png'.format(metric)
            plt.close()
            bot.sendPhoto(chat_id=chat_id, photo=plot_object, caption=msg)

default_args = {
    'owner': 'k-kislitsyna',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 3, 21),
}

@dag(default_args=default_args, schedule_interval='*/15 * * * *', catchup=False)
def dag_kislitsyna_alerts():
    @task
    def run_alerts_feed():
        query_metrics = """SELECT 
                                toStartOfFifteenMinutes(time) as ts,
                                toDate(time) as date,
                                formatDateTime(ts, '%R') as hm,
                                uniqExact(user_id) as users_feed, 
                                countIf(user_id, action = 'like') likes,
                                countIf(user_id, action = 'view') views,
                                round(likes/views, 3) ctr
                            FROM simulator_20230220.feed_actions
                            WHERE time >= today()-1 and time < toStartOfFifteenMinutes(now())
                            GROUP BY ts, date, hm
                            order by ts
                            format TSVWithNames"""
        df_metrics = ch_get_df(query=query_metrics)

        metrics_list = ['users_feed', 'views', 'likes', 'ctr']
        check_anomaly_and_send_message(df_metrics, metrics_list)

    @task
    def run_alerts_message():
        query_message = """SELECT 
                                    toStartOfFifteenMinutes(time) as ts,
                                    toDate(time) as date,
                                    formatDateTime(ts,'%R') as hm,
                                    uniqExact(user_id) as users_message, 
                                    count(user_id) messages
                                FROM simulator_20230220.message_actions
                                WHERE time >= today()-1 and time < toStartOfFifteenMinutes(now())
                                GROUP BY ts, date, hm
                                order by ts
                                format TSVWithNames"""

        df_message = ch_get_df(query=query_message)
        metrics_list = ['users_message', 'messages']
        check_anomaly_and_send_message(df_message, metrics_list)

    run_alerts_feed()
    run_alerts_message()

dag_kislitsyna_alerts=dag_kislitsyna_alerts()
