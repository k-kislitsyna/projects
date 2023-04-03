from datetime import datetime, timedelta
import pandas as pd
import pandahouse as ph
from io import StringIO
import requests
import numpy as np

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

def ch_get_df(query='', host='', user='', password=''):
    r = requests.post(host, data=query.encode("utf-8"), auth=(user, password), verify=False)
    result = pd.read_csv(StringIO(r.text), sep='\t')
    return result

default_args = {
    'owner': 'k-kislitsyna',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 3, 14),
}

schedule_interval = '0 15 * * *'


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_kislitsyna_sim():
    @task
    def get_extract_feed():
        query_feed = """SELECT user_id, toDate(time) event_date, 
                gender, age, os, 
                countIf(action = 'like') likes,
                countIf(action = 'view') views
                FROM simulator_20230220.feed_actions
                WHERE toDate(time) = today()-1
                GROUP BY event_date, user_id, gender, age, os
                    format TSVWithNames"""
        extract_feed = ch_get_df(query=query_feed)
        return extract_feed

    @task
    def get_extract_message():
        query_message = """SELECT user_id, event_date, gender, age, os, 
                        messages_sent, messages_received, users_sent, users_received 
                    FROM (SELECT user_id, toDate(time) event_date, gender, age, os,
                             count() messages_sent,
                             uniq(reciever_id) users_sent
                            FROM simulator_20230220.message_actions
                            WHERE toDate(time) = today()-1
                            GROUP BY event_date, user_id, gender, age, os) A
                    FULL outer JOIN 
                        (SELECT reciever_id as user_id, toDate(time) event_date,
                              COUNT() messages_received, 
                              uniq(user_id) users_received
                            FROM simulator_20230220.message_actions
                            WHERE toDate(time) = today()-1
                            GROUP BY user_id, event_date) B
                    on A.user_id = B.user_id 
                    WHERE event_date = today()-1
                    format TSVWithNames"""
        extract_message = ch_get_df(query=query_message)
        return extract_message

    @task
    def get_action_comb(extract_feed, extract_message):
        action_comb = extract_feed.merge(extract_message, on=['user_id', 'gender', 'os', 'age', 'event_date'],
                                         how='outer')
        return action_comb

    @task
    def get_dimension_gender(action_comb):
        dimension_gender = action_comb[
            ['event_date', 'gender', 'likes', 'views', 'messages_sent', 'messages_received', 'users_sent',
             'users_received']] \
            .groupby(['event_date', 'gender'], as_index=False).sum().rename(columns={'gender': 'dimension_value'})
        dimension_gender['dimension'] = 'gender'
        with open('dimension_gender.csv', 'w') as f:
            f.write(dimension_gender.to_csv(index=False))

    @task
    def get_dimension_os(action_comb):
        dimension_os = action_comb[
            ['event_date', 'os', 'likes', 'views', 'messages_sent', 'messages_received', 'users_sent',
             'users_received']] \
            .groupby(['event_date', 'os'], as_index=False).sum().rename(columns={'os': 'dimension_value'})
        dimension_os['dimension'] = 'os'
        with open('dimension_os.csv', 'w') as f:
            f.write(dimension_os.to_csv(index=False))

    @task
    def get_dimension_age(action_comb):
        dimension_age = action_comb[
            ['event_date', 'age', 'likes', 'views', 'messages_sent', 'messages_received', 'users_sent',
             'users_received']] \
            .groupby(['event_date', 'age'], as_index=False).sum().rename(columns={'age': 'dimension_value'})
        dimension_age['dimension'] = 'age'
        with open('dimension_age.csv', 'w') as f:
            f.write(dimension_age.to_csv(index=False))

    @task
    def load_test_table(dimension_gender, dimension_os, dimension_age):
        dimension_gender = pd.read_csv('dimension_gender.csv')
        dimension_os = pd.read_csv('dimension_os.csv')
        dimension_age = pd.read_csv('dimension_age.csv')

        test = pd.concat([dimension_gender, dimension_os, dimension_age]).reset_index().drop(columns=['index'])\
                        [['event_date','dimension','dimension_value','views','likes','messages_received','messages_sent','users_received','users_sent']]
        connection_test = {
            'host': '',
            'password': '',
            'user': '',
            'database': ''
        }

        print(test)
        print('creating table...')
        q_create = """CREATE TABLE IF NOT EXISTS test.table_k_kislitsyna 
                      (event_date Date, 
                      dimension String, 
                      dimension_value String, 
                      views Float64, 
                      likes Float64, 
                      messages_received Float64,
                      messages_sent Float64,
                      users_received Float64,
                      users_sent Float64)
                     ENGINE = MergeTree()
                     ORDER BY event_date"""
        ph.execute(connection=connection_test, query=q_create)
        print('table created. Sending to database')
        ph.to_clickhouse(test, 'table_k_kislitsyna', connection=connection_test, index=False)
        print('Database updated.')

    extract_feed = get_extract_feed()
    extract_message = get_extract_message()
    action_comb = get_action_comb(extract_feed, extract_message)
    dimension_gender = get_dimension_gender(action_comb)
    dimension_os = get_dimension_os(action_comb)
    dimension_age = get_dimension_age(action_comb)
    load_test_table(dimension_gender, dimension_os, dimension_age)


dag_kislitsyna_sim = dag_kislitsyna_sim()