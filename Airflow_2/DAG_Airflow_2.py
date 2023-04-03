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

token = ''
chat_id =
bot = telegram.Bot(token=token)

def ch_get_df(query='', host='', user='', password=''):
    r = requests.post(host, data=query.encode("utf-8"), auth=(user, password), verify=False)
    result = pd.read_csv(StringIO(r.text), sep='\t')
    return result

default_args = {
    'owner': 'k-kislitsyna',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 3, 17),
}


@dag(default_args=default_args, schedule_interval='0 11 * * *', catchup=False, )
def dag_kislitsyna_2():
    @task
    def get_feed_yesterday():
        query_feed_yesterday = """SELECT user_id, toDate(time) event_date, 
                countIf(action = 'like') likes,
                countIf(action = 'view') views
                FROM simulator_20230220.feed_actions
                WHERE toDate(time) = today()-1
                GROUP BY event_date, user_id
                format TSVWithNames"""
        feed_yesterday = ch_get_df(query=query_feed_yesterday)
        return feed_yesterday

    @task
    def get_message_yesterday():
        query_message_yesterday = """select count(user_id) messages, toDate(time) day
                    from simulator_20230220.message_actions
                    where day = today() -1
                    group by day
                    format TSVWithNames"""
        message_yesterday = ch_get_df(query=query_message_yesterday)
        return message_yesterday

    @task
    def get_dau_yesterday():
        query_dau_yesterday = """select uniq(user_id) unique_users, day
                    from (
                        select user_id, toDate(time) day
                        from simulator_20230220.feed_actions
                        UNION all
                        select user_id, toDate(time) day
                        from simulator_20230220.message_actions)
                    where day  = today() -1
                    group by day
                    format TSVWithNames"""
        df_dau_yesterday = ch_get_df(query=query_dau_yesterday)
        return df_dau_yesterday

    @task
    def get_likes(feed_yesterday):
        likes = feed_yesterday.likes.sum()
        return likes

    @task
    def get_views(feed_yesterday):
        views = feed_yesterday.views.sum()
        return views

    @task
    def get_ctr(likes, views):
        ctr = (likes / views).round(3)
        return ctr

    @task
    def get_messages(message_yesterday):
        messages = message_yesterday.messages
        return messages

    @task
    def get_dau(df_dau_yesterday):
        dau = df_dau_yesterday.unique_users
        return dau

    @task
    def get_feed_week():
        query_week = """SELECT user_id, toDate(time) date, 
                countIf(action = 'like') likes,
                countIf(action = 'view') views
                FROM simulator_20230220.feed_actions
                WHERE toDate(time) < today() and toDate(time) >= today()-7
                GROUP BY date, user_id
                order by date asc
                format TSVWithNames"""
        feed_week = ch_get_df(query=query_week)
        return feed_week

    @task
    def get_app_retention():
        query_retention = """with full_table as (select user_id, time
                                    from (
                                        select user_id, time
                                        from simulator_20230220.feed_actions
                                        UNION all
                                        select user_id, time
                                        from simulator_20230220.message_actions))
                SELECT start_day,
                       day,
                       count(user_id) AS users 
                FROM
                  (SELECT *
                   FROM
                     (SELECT user_id,
                             min(toDate(time)) AS start_day
                      FROM full_table
                      GROUP BY user_id) t1
                   JOIN
                     (SELECT DISTINCT user_id,
                                      toDate(time) AS day
                      FROM full_table) t2 USING user_id
                   WHERE start_day >= today() - 7 )
                GROUP BY start_day,
                         day
                format TSVWithNames"""
        df_retention = ch_get_df(query=query_retention)
        df_app_retention = df_retention.pivot_table(index='start_day', columns='day', values='users')
        with open('df_app_retention.csv', 'w') as f:
            f.write(df_app_retention.to_csv(index=False))

    @task
    def send_summary(likes, views, ctr, messages, dau, feed_week, df_app_retention):
        print('trying to send message')
        bot.send_message(chat_id, f'''Metrics for yesterday:
Likes: {likes}
Views: {views}
CTR: {ctr}
Messages: {messages}
App DAU: {dau}

Metrics for last 7 days:''')
        print('message was sent')

        media = []

        def append_media(data, y_axis, title):
            plt.figure()
            sns.lineplot(x='date', y=y_axis, data=data)
            plt.xticks(rotation=20)
            plt.title(title)
            plot_object = io.BytesIO()
            plt.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = title + '.png'
            plt.close()
            media.append(telegram.InputMediaPhoto(plot_object))

        query_messages_week = """select count(user_id) messages, toDate(time) day
                    from simulator_20230220.message_actions
                    where day <  today() and day >= today() -7
                    group by day
                    order by day asc
                format TSVWithNames"""
        messages_week = ch_get_df(query=query_messages_week)

        query_all_dau = """select uniq(user_id) unique_users, day, source 
                    from (
                        select user_id, toDate(time) day, source
                        from simulator_20230220.feed_actions
                        UNION all
                        select user_id, toDate(time) day, source
                        from simulator_20230220.message_actions)
                    where day  <  today() and day >= today() -7
                    group by day, source 
                    order by day asc
                format TSVWithNames"""
        df_all_dau = ch_get_df(query=query_all_dau)

        append_media(feed_week.groupby('date').views.sum().reset_index(), 'views', 'Views')
        append_media(feed_week.groupby('date').likes.sum().reset_index(), 'likes', 'Likes')

        likes_views_week = feed_week[['date', 'likes', 'views']].groupby('date').sum().reset_index()
        likes_views_week['ctr'] = (likes_views_week['likes'] / likes_views_week['views']).round(3)
        append_media(likes_views_week, 'ctr', 'CTR')

        append_media(messages_week, 'messages', 'Messages')
        append_media(df_all_dau, 'unique_users', 'App DAU')

        bot.send_media_group(chat_id, media)

        data = df_app_retention
        df_app_retention = io.StringIO()
        data.to_csv(df_app_retention)
        df_app_retention.name = 'app_retention.csv'
        df_app_retention.seek(0)
        bot.sendDocument(chat_id=chat_id, document=df_app_retention)

    feed_yesterday = get_feed_yesterday()
    message_yesterday = get_message_yesterday()
    df_dau_yesterday = get_dau_yesterday()
    likes = get_likes(feed_yesterday)
    views = get_views(feed_yesterday)
    ctr = get_ctr(likes, views)
    messages = get_messages(message_yesterday)
    dau = get_dau(df_dau_yesterday)
    feed_week = get_feed_week()
    df_app_retention = get_app_retention()
    send_summary(likes, views, ctr, messages, dau, feed_week, df_app_retention)


dag_kislitsyna_2 = dag_kislitsyna_2()