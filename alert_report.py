import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import telegram
import pandahouse
from datetime import date
import io
from read_db.CH import Getch
import sys
import os
import numpy as np

def check_anomaly(df, metric, n_offsets = 5, n_sigmas = 6):
    # Метод для оценки пороговых значений - стандартные отклонения (сигмы). Лучше всего работает значение 5-6 сигм
    # Применяем shift, чтобы сдвинуть графики границ левее, т.к.видимо rolling их сдвигает
    df['low'] = df[metric].shift(0).rolling(n_offsets, center=True).mean() - n_sigmas * df[metric].shift(1).rolling(n_offsets).std()
    df['up'] = df[metric].shift(0).rolling(n_offsets, center=True).mean() + n_sigmas * df[metric].shift(1).rolling(n_offsets).std()
    
    # Сглаживаем наши границы. Метод ewm возьмет несколько соседних значений и посчитает для каждого среднее. Так графики границ будут более гладкими
    df['low'] = df['low'].ewm(span = 5).mean()
    df['up'] = df['up'].ewm(span = 5).mean()
    
    if df[metric].iloc[-1] > df['up'].iloc[-1] or df[metric].iloc[-1] < df['low'].iloc[-1]:
        is_alert = 1
    else: 
        is_alert = 0

    return is_alert, df

def run_alerts():
    chat_id = 
    bot = telegram.Bot(token='5133536168:AAEeD7to2jwSY8hyjiwpD1eT2RIFFVC8NAM')

    # для удобства построения графиков в запрос можно добавить колонки date, hm
    data = Getch(''' SELECT ts, date, hm, users_feed, views, likes, users_messenger, messages FROM
                    (SELECT
                          toStartOfFifteenMinutes(time) as ts
                        , toDate(ts) as date
                        , formatDateTime(ts, '%R') as hm
                        , uniqExact(user_id) as users_feed
                        , countIf(user_id, action = 'view') as views
                        , countIf(user_id, action = 'like') as likes
                    FROM simulator_20220520.feed_actions
                    WHERE ts >=  today() - 2 and ts < toStartOfFifteenMinutes(now())
                    GROUP BY ts, date, hm) t1
                    JOIN
                    (SELECT
                          toStartOfFifteenMinutes(time) as ts
                        , toDate(ts) as date
                        , formatDateTime(ts, '%R') as hm
                        , uniqExact(user_id) as users_messenger
                        , COUNT(user_id) as messages
                    FROM simulator_20220520.message_actions
                    WHERE ts >=  today() - 2 and ts < toStartOfFifteenMinutes(now())
                    GROUP BY ts, date, hm) t2
                    ON t1.ts = t2.ts
                    ORDER BY ts ''').df

    metrics_list = ['users_feed', 'views', 'likes', 'users_messenger', 'messages']
    for metric in metrics_list:
        print(metric)
        df = data[['ts', 'date', 'hm', metric]].copy()
        is_alert, df = check_anomaly(df, metric) # проверяем метрику на аномальность алгоритмом, описаным внутри функции check_anomaly()
        if is_alert == 1:
            msg = '''Метрика {metric}:\nтекущее значение = {current_value:.2f}\nотклонение от предыдущего значения {last_value:.2%}'''.format(metric=metric,                                                                                                 current_value=df[metric].iloc[-1],                                                                                             last_value=abs(df[metric].iloc[-1]/df[metric].iloc[-2]))

            sns.set(rc={'figure.figsize': (16, 10)}) # задаем размер графика
            plt.tight_layout()

            ax = sns.lineplot(x=df['ts'], y = df[metric], label = 'metric', color = 'red')
            ax = sns.lineplot(x=df['ts'], y = df['low'], label = 'lower limit', color = 'gray')
            ax = sns.lineplot(x=df['ts'], y = df['up'], label = 'upper limit', color = 'gray')
            
            ax.set(xlabel='time') # задаем имя оси Х
            ax.set(ylabel=metric) # задаем имя оси У

            ax.set_title('{}'.format(metric)) # задаем заголовок графика
            ax.set(ylim=(0, None)) # задаем лимит для оси У

            # формируем файловый объект
            plot_object = io.BytesIO()
            ax.figure.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = '{0}.png'.format(metric)
            plt.close()

            # отправляем алерт
            bot.sendMessage(chat_id=chat_id, text=msg)
            bot.sendPhoto(chat_id=chat_id, photo=plot_object)
