import io

import pandas as pd
import seaborn as sns
import telegram as tg
import pandahouse as ph

from matplotlib import pyplot as plt
from airflow.decorators import dag, task
from datetime import timedelta, datetime

default_args = {
    'owner': 'a.harchenko-16',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 2, 27),
}

schedule_interval = '0 11 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def daily_report_sender():
    @task(retries=3, retry_delay=timedelta(minutes=10))
    def dau_df_extraction(connection):
        query_dau = """
            SELECT COUNT(DISTINCT user_id) AS users,
            toDate(time) as date
            FROM simulator_20250120.feed_actions
            WHERE toDate(time) BETWEEN yesterday() - 13 AND yesterday()
            GROUP BY toDate(time)
            """
        return ph.read_clickhouse(query=query_dau, connection=connection)
    
    @task(retries=3, retry_delay=timedelta(minutes=10))
    def views_df_extraction(connection):
        query_views = """
            SELECT SUM(action = 'view') AS views,
            toDate(time) AS date
            FROM simulator_20250120.feed_actions
            WHERE toDate(time) BETWEEN yesterday() - 13 AND yesterday()
            GROUP BY toDate(time)
            """
        return ph.read_clickhouse(query=query_views, connection=connection)

    @task(retries=3, retry_delay=timedelta(minutes=10))
    def likes_df_extraction(connection):
        query_likes = """
            SELECT SUM(action = 'like') AS likes,
            toDate(time) AS date
            FROM simulator_20250120.feed_actions
            WHERE toDate(time) BETWEEN yesterday() - 13 AND yesterday()
            GROUP BY toDate(time)
            """
        return ph.read_clickhouse(query=query_likes, connection=connection)

    @task(retries=3, retry_delay=timedelta(minutes=10))
    def ctr_df_extraction(connection):
        query_ctr = """
            SELECT SUM(action = 'like') / SUM(action = 'view') AS ctr,
            toDate(time) AS date
            FROM simulator_20250120.feed_actions
            WHERE toDate(time) BETWEEN yesterday() - 13 AND yesterday()
            GROUP BY toDate(time)
            """
        return ph.read_clickhouse(query=query_ctr, connection=connection)
        
    @task(retries=3, retry_delay=timedelta(minutes=10))
    def message_preparation(dau_df, views_df, likes_df, ctr_df):
        yesterday = dau_df.date.max().strftime('%B %d, %Y')
        dau_yesterday = dau_df.iloc[-1].users
        views_yesterday = views_df.iloc[-1].views
        likes_yesterday = likes_df.iloc[-1].likes
        ctr_yesterday = ctr_df.ctr.mul(100).round(2).iloc[-1]

        message = f"For <b>{yesterday}</b>, the key product metrics were as follows:\n" \
                  f"- Daily Active Users (DAU): <b>{dau_yesterday}</b>\n" \
                  f"- Total views by all active users: <b>{views_yesterday}</b>\n" \
                  f"- Total likes by all active users: <b>{likes_yesterday}</b>\n" \
                  f"- Click-Through Rate (CTR) (views to likes): <b>{ctr_yesterday}%</b>\n\n" \
                  "Above this message, you can see the metrics' trends over the past 7 days " \
                  "compared to the previous 7-day period. Tap the image to see closer.\n"
        
        return message
        
    @task(retries=3, retry_delay=timedelta(minutes=10))
    def plot_preparation(dau_df, views_df, likes_df, ctr_df):
        plot_object = io.BytesIO()

        fig, axes = plt.subplots(2, 2, figsize=(20, 10))
        fig.subplots_adjust(wspace=0.15, hspace=0.25)

        dau_df_values = dau_df.iloc[-7:]
        sns.lineplot(ax=axes[0, 0],
                     x=dau_df_values.date,
                     y=dau_df_values.users,
                     color='r',
                     label='Curent Week',
                     alpha=0.5)
        sns.lineplot(ax=axes[0, 0],
                     x=dau_df_values.date,
                     y=dau_df.users.shift(7),
                     color='grey',
                     alpha=0.5,
                     linestyle='dashed',
                     label='1 Week Offset')
        axes[0, 0].set_title('Daily Active Users', fontsize=14)
        axes[0, 0].set_ylabel('')
        axes[0, 0].set_xlabel('')
        axes[0, 0].set_xticks(dau_df_values.date)
        axes[0, 0].set_xticklabels(dau_df_values.date.dt.strftime('%a').tolist())
        axes[0, 0].tick_params(axis='x', labelsize=12)
        axes[0, 0].tick_params(axis='y', labelsize=10)
        axes[0, 0].grid(True)

        views_df_values = views_df.iloc[-7:]
        sns.lineplot(ax=axes[0, 1],
                     x=views_df_values.date,
                     y=views_df_values.views,
                     color='g',
                     label='Curent Week',
                     alpha=0.5)
        sns.lineplot(ax=axes[0, 1],
                     x=views_df_values.date,
                     y=views_df.views.shift(7),
                     color='grey',
                     alpha=0.5,
                     linestyle='dashed',
                     label='1 Week Offset')
        axes[0, 1].set_title('Total Number of Views', fontsize=14)
        axes[0, 1].set_ylabel('')
        axes[0, 1].set_xlabel('')
        axes[0, 1].set_xticks(views_df_values.date)
        axes[0, 1].set_xticklabels(views_df_values.date.dt.strftime('%a').tolist())
        axes[0, 1].tick_params(axis='x', labelsize=12)
        axes[0, 1].tick_params(axis='y', labelsize=10)
        axes[0, 1].grid(True)

        likes_df_values = likes_df.iloc[-7:]
        sns.lineplot(ax=axes[1, 0],
                     x=likes_df_values.date,
                     y=likes_df_values.likes,
                     color='b',
                     label='Curent Week',
                     alpha=0.5)
        sns.lineplot(ax=axes[1, 0],
                     x=likes_df_values.date,
                     y=likes_df.likes.shift(7),
                     color='grey',
                     alpha=0.5,
                     linestyle='dashed',
                     label='1 Week Offset')
        axes[1, 0].set_title('Total Number of Likes', fontsize=14)
        axes[1, 0].set_ylabel('')
        axes[1, 0].set_xlabel('')
        axes[1, 0].set_xticks(likes_df_values.date)
        axes[1, 0].set_xticklabels(likes_df_values.date.dt.strftime('%a').tolist())
        axes[1, 0].tick_params(axis='x', labelsize=12)
        axes[1, 0].tick_params(axis='y', labelsize=10)
        axes[1, 0].grid(True)

        ctr_df_values = ctr_df.iloc[-7:]
        sns.lineplot(ax=axes[1, 1],
                     x=ctr_df_values.date,
                     y=ctr_df_values.ctr.mul(100).round(2),
                     color='brown',
                     label='Curent Week',
                     alpha=0.5)
        sns.lineplot(ax=axes[1, 1],
                     x=ctr_df_values.date,
                     y=ctr_df.ctr.mul(100).round(2).shift(7),
                     color='grey',
                     alpha=0.5,
                     linestyle='dashed',
                     label='1 Week Offset')
        axes[1, 1].set_title('Click Through Rate, %', fontsize=14)
        axes[1, 1].set_ylabel('')
        axes[1, 1].set_xlabel('')
        axes[1, 1].set_xticks(ctr_df_values.date)
        axes[1, 1].set_xticklabels(ctr_df_values.date.dt.strftime('%a').tolist())
        axes[1, 1].tick_params(axis='x', labelsize=12)
        axes[1, 1].tick_params(axis='y', labelsize=10)
        axes[1, 1].grid(True)

        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = f"product_metrics_{dau_df.date.max().strftime('%Y_%m_%d')}.png"
        plt.close()

        return plot_object
        
    @task(retries=3, retry_delay=timedelta(minutes=10))
    def loading(message, plot_object):
        my_token = "78********:AAGz*******************************"
        bot = tg.Bot(token=my_token)
        chat_id = -*******25
        bot.send_photo(chat_id=chat_id,
                       photo=plot_object,
                       caption=message,
                       parse_mode="HTML"
                      )

    connection = {
            'host': '***********************karpov.courses',
            'password': '**********_2020',
            'user': '*******',
            'database': 'simulator_20250120'
            }
    
    # extraction
    dau_df = dau_df_extraction(connection)
    views_df = views_df_extraction(connection)
    likes_df = likes_df_extraction(connection)
    ctr_df = ctr_df_extraction(connection)
    
    # transformation
    message = message_preparation(dau_df, views_df, likes_df, ctr_df)
    plot_object = plot_preparation(dau_df, views_df, likes_df, ctr_df)
    
    # loading
    loading(message, plot_object)
    
daily_report_sender = daily_report_sender()