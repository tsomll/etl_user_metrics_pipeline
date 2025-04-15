
from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
import requests
import pandahouse as ph

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# подключение к бд
connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'user': 'student',
    'password': 'dpo_python_2020',
    'database': 'simulator_20250220'
}

# подключение к бд test
connection_test = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'user': 'student-rw',
    'password': '656e2b0c9c',
    'database': 'test'
}

# параметры dag
default_args = {
    'owner': 'adel.valishina',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 3, 22),
}

# интервал запуска
schedule_interval = '0 23 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_metrics():
    # запрос в feed_actions
    @task()
    def extract_feed():
        query = """
        SELECT user_id,
               countIf(action = 'view') as views,
               countIf(action = 'like') as likes
        FROM simulator_20250220.feed_actions
        WHERE toDate(time) = yesterday()
        GROUP BY user_id
        """
        return ph.read_clickhouse(query, connection=connection)
    # запрос для отправленных сообщений в message_actions
    @task()
    def extract_messages_sent():
        query = """
        SELECT user_id,
               count() as messages_sent,
               count(DISTINCT receiver_id) as users_sent
        FROM simulator_20250220.message_actions
        WHERE toDate(time) = yesterday()
        GROUP BY user_id
        """
        return ph.read_clickhouse(query, connection=connection)
    # запрос для полученных сообщений в message_actions
    @task()
    def extract_messages_received():
        query = """
        SELECT receiver_id as user_id,
               count() as messages_received,
               count(DISTINCT user_id) as users_received
        FROM simulator_20250220.message_actions
        WHERE toDate(time) = yesterday()
        GROUP BY receiver_id
        """
        return ph.read_clickhouse(query, connection=connection)
    # объединение message_actions и feed_action
    @task()
    def merge_data(feed_df, messages_sent_df, messages_received_df):
        merged_df = feed_df.merge(messages_sent_df, on='user_id', how='outer').fillna(0)
        merged_df = merged_df.merge(messages_received_df, on='user_id', how='outer').fillna(0)

        query = """
        SELECT DISTINCT user_id, os, gender, age
        FROM simulator_20250220.feed_actions
        UNION ALL
        SELECT DISTINCT user_id, os, gender, age
        FROM simulator_20250220.message_actions
        """
        user_info_df = ph.read_clickhouse(query, connection=connection).drop_duplicates(subset=['user_id'])

        df_merged = merged_df.merge(user_info_df, on='user_id', how='left')
        df_merged['event_date'] = datetime.now().date() - timedelta(days=1)
        return df_merged
    # агрегация по полу, возрасту и ос
    @task()
    def transform_os(df_merged):
        df_os = df_merged[['event_date', 'os', 'views', 'likes', 'messages_received',
                           'messages_sent', 'users_received', 'users_sent']].groupby(
            ['event_date', 'os'], as_index=False).sum().rename(columns={'os': 'dimension_value'})
        df_os.insert(1, 'dimension', 'os')
        return df_os

    @task()
    def transform_gender(df_merged):
        df_gender = df_merged[['event_date', 'gender', 'views', 'likes', 'messages_received',
                               'messages_sent', 'users_received', 'users_sent']].groupby(
            ['event_date', 'gender'], as_index=False).sum().rename(columns={'gender': 'dimension_value'})
        df_gender.insert(1, 'dimension', 'gender')
        return df_gender

    @task()
    def transform_age(df_merged):
        df_age = df_merged[['event_date', 'age', 'views', 'likes', 'messages_received',
                            'messages_sent', 'users_received', 'users_sent']].groupby(
            ['event_date', 'age'], as_index=False).sum().rename(columns={'age': 'dimension_value'})
        df_age.insert(1, 'dimension', 'age')
        return df_age
    # финальные данные записываем в таблицу clickhouse
    @task()
    def load_to_ch(df_gender, df_age, df_os):
        df_final = pd.concat([df_gender, df_age, df_os])

        df_final = df_final.astype({
            'views': 'int',
            'likes': 'int',
            'messages_sent': 'int',
            'messages_received': 'int',
            'users_sent': 'int',
            'users_received': 'int'
        })

        create_table_query = """
        CREATE TABLE IF NOT EXISTS test.user_activity_table_valishina (
            event_date Date,
            dimension String,
            dimension_value String,
            views Int64,
            likes Int64,
            messages_sent Int64,
            messages_received Int64,
            users_sent Int64,
            users_received Int64
        ) ENGINE = MergeTree()
        ORDER BY (event_date, dimension, dimension_value)
        """
        ph.execute(create_table_query, connection=connection_test)

        ph.to_clickhouse(df_final, table='user_activity_table_valishina', index=False, connection=connection_test)

    # определение зависимостей
    feed_df = extract_feed()
    messages_sent_df = extract_messages_sent()
    messages_received_df = extract_messages_received()

    df_merged = merge_data(feed_df, messages_sent_df, messages_received_df)

    df_os = transform_os(df_merged)
    df_gender = transform_gender(df_merged)
    df_age = transform_age(df_merged)

    load_to_ch(df_gender, df_age, df_os)

dag_metrics = dag_metrics()
