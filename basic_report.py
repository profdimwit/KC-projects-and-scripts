import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
from read_db.CH import Getch
import pandas as pd
import pandahouse
import os

def basic_report(chat=None):
    # данные бота и ID пользователя
    chat_id = chat or 223226505
    bot = telegram.Bot(token='5133536168:AAEeD7to2jwSY8hyjiwpD1eT2RIFFVC8NAM')

    # Запрашиваем в базе данные за прошедшую неделю и создаем колонку с короткой датой (понадобится для визуализации)
    query = """SELECT t1.Days, t1.Views, t2.Likes, t3.Users FROM
    (SELECT toDate(time) AS Days, COUNT(user_id) AS Views
    FROM simulator_20220520.feed_actions
    WHERE toDate(time) BETWEEN today()-7 and today()-1 AND action='view'
    GROUP BY toDate(time)) AS t1
    JOIN 
    (SELECT toDate(time) AS Days, COUNT(user_id) AS Likes
    FROM simulator_20220520.feed_actions
    WHERE toDate(time) BETWEEN today()-7 and today()-1 AND action='like'
    GROUP BY toDate(time)) AS t2
    ON t1.Days = t2.Days
    JOIN 
    (SELECT toDate(time) AS Days, COUNT(DISTINCT user_id) AS Users
    FROM simulator_20220520.feed_actions
    WHERE toDate(time) BETWEEN today()-7 and today()-1
    GROUP BY toDate(time)) AS t3
    ON t1.Days = t3.Days"""
    df = Getch(query).df
    df['day'] = df['t1.Days'].dt.strftime('%d/%m')
    df = df.set_index('day')

    # Создаем 4 переменные с ключевыми метриками
    views = pd.Series(data=df['t1.Views'], index=index)
    likes = pd.Series(data=df['t2.Likes'], index=index)
    dau = pd.Series(data=df['t3.Users'], index=index)
    ctr = likes.divide(views)

    # Печатаем данные за прошедшую неделю (исопльзуем ''.join, чтобы в результате был один string)
    msg = ''.join(("Сводная информация со среднедневными метриками за прошлую неделю (c ", min(index), " по ", max(index), "):", 
               "\n", "DAU: ", str(int(dau.mean())), 
               "\n", "Views: ", str(int(views.mean())), 
               "\n", "Likes: ", str(int(likes.mean())), 
               "\n", "CTR: ", str(ctr.mean().round(4))))
    bot.sendMessage(chat_id=chat_id, text=msg)
    
    # Строим 4 графика с ключевыми метриками

    sns.set(rc={'figure.figsize':(16,9)})
    fig, axs = plt.subplots(nrows = 2, ncols=2)

    sns.lineplot(data=views, ax=axs[0, 1], color = 'g').set(ylabel = 'views', xlabel = None)
    sns.lineplot(data=likes, ax=axs[1, 1], color = 'blue').set(ylabel = 'likes', xlabel = None)
    sns.lineplot(data=dau, ax=axs[0, 0], color = 'r').set(ylabel = 'DAU', xlabel = None)
    sns.lineplot(data=ctr, ax=axs[1, 0], color = 'black').set(ylabel = 'CTR', xlabel = None)

    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.seek(0)
    plot_object.name = 'weekly_metrics.png'
    plt.close()
    bot.sendPhoto(chat_id=chat_id, photo=plot_object)

try:
    basic_report()
except Exception as e:
    print(e)
