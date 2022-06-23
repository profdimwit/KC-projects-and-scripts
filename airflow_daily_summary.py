import pandahouse
import pandas as pd
from datetime import datetime, timedelta
from airflow.decorators import dag, task

connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator'
}

connection_load = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': '656e2b0c9c',
    'user': 'student-rw',
    'database': 'test'
}

# Считывание данных из базы
def ch_get_df(query, connection):
    df = pandahouse.read_clickhouse(query = query, connection=connection)
    return df

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'dsokolov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 13),
}

# Интервал запуска DAG
schedule_interval = '0 20 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def etl_daily_summary_dns():
    
    # Запрос данных по базе feed_actions
    @task()
    def extract_feed():
        query = """SELECT user_id,
            toDate(time) as event_date,
            IF(gender=1, 'Male', 'Female') as gender,
            CASE
                 WHEN age < 18 THEN '17 и младше'
                 WHEN age BETWEEN 18 AND 24 THEN '18-24'
                 WHEN age BETWEEN 25 AND 34 THEN '25-34'
                 WHEN age BETWEEN 35 AND 44 THEN '35-44'
                 WHEN age BETWEEN 45 AND 54 THEN '45-55'
                 ELSE '55 и старше'
            END AS age,
            os,
            countIf(user_id, action='view') as views,
            countIf(user_id, action='like') as likes
        FROM simulator_20220520.feed_actions
        WHERE event_date = yesterday()
        GROUP BY user_id, gender, age, os, event_date"""

        df_cube = ch_get_df(connection = connection, query=query)
        return df_cube

    # Запрос данных по базе message_actions
    @task()
    def extract_message():
        query = """SELECT user_id, event_date, gender, age, os, messages_sent, users_sent, messages_recieved, users_recieved FROM
        (SELECT user_id,
            toDate(time) as event_date,
            IF(gender=1, 'Male', 'Female') as gender,
            CASE
                 WHEN age < 18 THEN '17 и младше'
                 WHEN age BETWEEN 18 AND 24 THEN '18-24'
                 WHEN age BETWEEN 25 AND 34 THEN '25-34'
                 WHEN age BETWEEN 35 AND 44 THEN '35-44'
                 WHEN age BETWEEN 45 AND 54 THEN '45-55'
                 ELSE '55 и старше'
            END AS age,
            os,
            COUNT(user_id) AS messages_sent,
            COUNT(DISTINCT reciever_id) AS users_sent
        FROM simulator_20220520.message_actions
        WHERE event_date = yesterday()
        GROUP BY user_id, gender, age, os, event_date) t1
        LEFT JOIN
        (SELECT
            reciever_id,
            IF(gender=1, 'Male', 'Female') as gender,
            CASE
                 WHEN age < 18 THEN '17 и младше'
                 WHEN age BETWEEN 18 AND 24 THEN '18-24'
                 WHEN age BETWEEN 25 AND 34 THEN '25-34'
                 WHEN age BETWEEN 35 AND 44 THEN '35-44'
                 WHEN age BETWEEN 45 AND 54 THEN '45-55'
                 ELSE '55 и старше'
            END AS age,
            os,
            toDate(time) as event_date,
            COUNT(reciever_id) AS messages_recieved,
            COUNT(DISTINCT user_id) AS users_recieved
        FROM simulator_20220520.message_actions
        WHERE event_date = yesterday()
        GROUP BY reciever_id, gender, age, os, event_date) t2
        ON t1.user_id = t2.reciever_id"""

        df_cube = ch_get_df(connection = connection, query=query)
        return df_cube

    # Сливаем датафреймы в один
    @task()
    def merge_data(df1, df2):
        df = df1.merge(df2, on=['user_id', 'event_date', 'gender', 'age', 'os'], how='outer').fillna(0)
        return df

    # Создаем сводные таблицы по трем срезам: gender, age, os
    @task()
    def transfrom_gender(df):
        df_gender = df[['event_date', 'gender', 'views', 'likes', 'messages_sent', 'users_sent', 'messages_recieved', 'users_recieved']].groupby(['event_date', 'gender']).sum().reset_index()
        df_gender['slice'] = 'gender'
        df_gender = df_gender.rename(columns={'gender':'slice_values'})
        return df_gender

    @task()
    def transfrom_age(df):
        df_age = df[['event_date', 'age', 'views', 'likes', 'messages_sent', 'users_sent', 'messages_recieved', 'users_recieved']].groupby(['event_date', 'age']).sum().reset_index()
        df_age['slice'] = 'age'
        df_age = df_age.rename(columns={'age':'slice_values'})
        return df_age

    @task()
    def transfrom_os(df):
        df_os = df[['event_date', 'os', 'views', 'likes', 'messages_sent', 'users_sent', 'messages_recieved', 'users_recieved']].groupby(['event_date', 'os']).sum().reset_index()
        df_os['slice'] = 'os'
        df_os = df_os.rename(columns={'os':'slice_values'})
        return df_os

    # Объединяем результаты срезов и загружаем данные в базу test
    @task()
    def load(df_gender, df_age, df_os):
        # Создаем таблицу в схеме test
        query = '''CREATE TABLE IF NOT EXISTS test.dsokolov1
                (   event_date Date,
                    slice String,
                    slice_values String,
                    views Float64,
                    likes Float64,
                    messages_sent Float64,
                    users_sent Float64,
                    messages_recieved Float64,
                    users_recieved Float64
                ) ENGINE = Log()'''   
        pandahouse.execute(connection=connection_load, query=query)

        # Загружаем получившуюся таблицу в созданную таблицу
        df_summary = pd.concat([df_gender, df_age, df_os], ignore_index=True)
        df_summary = df_summary[['event_date', 'slice', 'slice_values', 'views', 'likes', 'messages_sent', 'users_sent', 'messages_recieved', 'users_recieved']]
        pandahouse.to_clickhouse(df_summary, 'dsokolov1', connection=connection_load, index=False)
    
    # Исполнение всех тасков
    df1 = extract_feed()
    df2 = extract_message()
    df = merge_data(df1, df2)
    df_gender = transfrom_gender(df)
    df_age = transfrom_age(df)
    df_os = transfrom_os(df)
    load(df_gender, df_age, df_os)

etl_daily_summary_dns = etl_daily_summary_dns()
