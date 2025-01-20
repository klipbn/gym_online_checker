import time
import json
import warnings
from datetime import datetime, timedelta
from io import StringIO, BytesIO
import psycopg2
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from bs4 import BeautifulSoup
from telebot import telebot
from telebot.types import InputMediaPhoto

from ast import literal_eval


warnings.filterwarnings("ignore")


default_args = {
    'owner': 'klip',
    'email': ['klip@klip.com'],
    'start_date': datetime(2024, 9, 12),
    'depends_on_past': False,
    'wait_for_downstream': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

dag_params = {
    'dag_id': 'gym_tg_sender',
    'catchup': False,
    'schedule_interval': '5-59/2 4-20 * * *',
    'default_args': default_args,
    'max_active_runs': 1,
    'concurrency': 0,
    'tags': ['gym', 'tg', 'twsb'],
}


with open(f"main_config.json") as json_file:
    main_config = json.load(json_file)



def get_data_gym():

    def read_df_bd(query, layer):
        """
        Чтение таблицы в DF из БД
        """
        conn = psycopg2.connect(
            dbname=main_config["postgres"]["dbname"],
            user=main_config["postgres"]["user"],
            password=main_config["postgres"]["password"],
            host=main_config["postgres"]["host"],
            port=main_config["postgres"]["port"],
            options=f"-c search_path={layer}",
        )
        cursor = conn.cursor()
    
        query = f"{query}"
        df = pd.read_sql_query(query, conn)
    
        cursor.close()
        conn.close()
    
        return df
    
    # Базы данных
    def load_df_bd(layer, table, df_data):
        """
        Загрузка DF в БД
        """
    
        # Загрузка DataFrame в PostgreSQL
        conn = psycopg2.connect(
            dbname=main_config["postgres"]["dbname"],
            user=main_config["postgres"]["user"],
            password=main_config["postgres"]["password"],
            host=main_config["postgres"]["host"],
            port=main_config["postgres"]["port"],
            options=f"-c search_path={layer}",
        )
        cursor = conn.cursor()
    
        # Сохранение DataFrame в строку в формате CSV
        output = StringIO()
        df_data.to_csv(output, sep="\t", header=False, index=False)
        output.seek(0)
    
        columns = df_data.columns.tolist()
    
        # Открытие транзакции
        conn.autocommit = False
        try:
            # Копирование данных из StringIO в таблицу в PostgreSQL
            cursor.copy_from(output, f"{table}", null="", columns=columns)
            conn.commit()
            print(f"Датафрейм в таблицу {layer}.{table} загружен")
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            conn.rollback()
            print(f"!!!!!Датафрейм в таблицу {layer}.{table} не загружен")
        finally:
            cursor.close()
            conn.close()
    
        # очистим ОЗУ от треша
        output.close()  # Закрыть объект StringIO, освобождая ресурсы, связанные с ним.
        del output  # Удалить ссылку на объект, позволяя сборщику мусора Python освободить память.
    
        cursor.close()
        conn.close()
    
    df_online = read_df_bd("select * from raw.gym_online_checker_hist_regular where ts >= date(now()) - interval '30' day ", "raw")
    df_online["ts"] = pd.to_datetime(df_online["ts"])
    df_online["online"] = df_online["online"].astype("int")
    
    
    df = df_online
    
    # Отфильтровать по залу 'Советская'
    df_sovietskaya = df[df['gym_name'] == 'Советская'].copy()
    
    # Преобразование столбца с датой в datetime формат
    df_sovietskaya['ts'] = pd.to_datetime(df_sovietskaya['ts'])
    
    # Добавление столбца с округлением времени до ближайших 30 минут в меньшую сторону
    df_sovietskaya['time_30min'] = df_sovietskaya['ts'].dt.floor('30T')

    # Добавление столбцов для дня недели и времени (округленное до 30 минут)
    df_sovietskaya['day'] = df_sovietskaya['ts'].dt.strftime('%A')
    df_sovietskaya['time'] = df_sovietskaya['time_30min'].dt.strftime('%H:%M')
    
    # Фильтрация по времени с 07:00 до 23:59
    df_sovietskaya = df_sovietskaya[(df_sovietskaya['ts'].dt.time >= pd.to_datetime('07:00:00').time()) &
                             (df_sovietskaya['ts'].dt.time <= pd.to_datetime('23:59:59').time())]

    # Определение порядка дней недели
    week_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    
    # Создаем сводную таблицу с правильным порядком дней недели
    pivot_table = df_sovietskaya.pivot_table(values='online', index='time', columns='day', aggfunc=np.mean)
    pivot_table = pivot_table[week_order]
    pivot_table.columns = ['ПН', 'ВТ', 'СР', 'ЧТ', 'ПТ', 'СБ', 'ВС']
    
    # Нормализуем данные по каждому столбцу (дню недели)
    # pivot_table_normalized = pivot_table.apply(lambda x: (x - x.min()) / (x.max() - x.min()), axis=0)
    pivot_table_normalized = pivot_table
    
    # Построение тепловой карты с нормализацией по столбцам
    plt.figure(figsize=(6, 10))
    # sns.heatmap(pivot_table_normalized, cmap="RdYlGn_r", annot=pivot_table, fmt=".0f", linewidths=0, cbar=False)
    ax = sns.heatmap(pivot_table_normalized, cmap="RdYlGn_r", annot=pivot_table, fmt=".0f", linewidths=0.1, cbar=False)
    
    plt.title('Загруженность gym за последние 30 дней', pad=30)
    plt.xlabel('')
    plt.ylabel('')

    # Размещаем подписи оси x сверху
    ax.xaxis.set_ticks_position('top')
    plt.xticks(rotation=0)

    # Соответствие дней недели
    day_mapping = {
        'Monday': 'ПН',
        'Tuesday': 'ВТ',
        'Wednesday': 'СР',
        'Thursday': 'ЧТ',
        'Friday': 'ПТ',
        'Saturday': 'СБ',
        'Sunday': 'ВС'
    }
    # Функция для округления времени до ближайших 30 минут в меньшую сторону
    def round_to_nearest_30min(dt):
        return dt - timedelta(minutes=dt.minute % 30, seconds=dt.second, microseconds=dt.microsecond)
    # Получаем текущее время
    current_time = datetime.now() + timedelta(hours=3)
    # Округляем до ближайших 30 минут
    current_time = round_to_nearest_30min(current_time).strftime('%H:%M')
    # current_time = '13:30'
    current_day = (datetime.now()+ timedelta(hours=3)).strftime('%A')
    # Поиск позиции текущего времени и дня недели
    if current_time in pivot_table_normalized.index and day_mapping[current_day] in pivot_table_normalized.columns:
        row_idx = pivot_table_normalized.index.get_loc(current_time)
        col_idx = pivot_table_normalized.columns.get_loc(day_mapping[current_day])
        # Добавляем красный квадрат вокруг ячейки
        ax.add_patch(plt.Rectangle((col_idx, row_idx), 1, 1, fill=False, edgecolor='blue', lw=3))

    # Получение объекта Figure из объекта Axes
    fig = plt.gcf()  # Получаем текущую фигуру (get current figure)
    # Сохраняем график в объект BytesIO вместо файла
    buf_now = BytesIO()
    fig.savefig(buf_now, format="png")
    buf_now.seek(0)  # Перемещаем указатель в начало буфера
    # plt.show()
    plt.close()

    query = """
    select 
        *
    from raw.gym_online_checker_hist_regular
    where 1=1
        and gym_name = 'Советская'
        and (
            ts::date < date '2024-09-16' or ts::date >= date_trunc('week', current_date)
        )
    ;
    """
    df_online = read_df_bd(query, "raw")
    df_online["ts"] = pd.to_datetime(df_online["ts"])
    df_online["online"] = df_online["online"].astype("int")

    df = df_online

    # Отфильтровать по залу 'Советская'
    df_sovietskaya = df[df['gym_name'] == 'Советская'].copy()

    # Преобразование столбца с датой в datetime формат
    df_sovietskaya['ts'] = pd.to_datetime(df_sovietskaya['ts'])

    # Добавление столбца с округлением времени до ближайших 30 минут в меньшую сторону
    df_sovietskaya['time_30min'] = df_sovietskaya['ts'].dt.floor('30T')

    # Добавление столбцов для дня недели и времени (округленное до 30 минут)
    df_sovietskaya['day'] = df_sovietskaya['ts'].dt.strftime('%A')
    df_sovietskaya['time'] = df_sovietskaya['time_30min'].dt.strftime('%H:%M')

    # Фильтрация по времени с 07:00 до 23:59
    df_sovietskaya = df_sovietskaya[(df_sovietskaya['ts'].dt.time >= pd.to_datetime('07:00:00').time()) &
                                (df_sovietskaya['ts'].dt.time <= pd.to_datetime('23:59:59').time())]

    # Определение порядка дней недели
    week_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

    # Создаем сводную таблицу с правильным порядком дней недели
    pivot_table = df_sovietskaya.pivot_table(values='online', index='time', columns='day', aggfunc=np.median)
    pivot_table = pivot_table[week_order]
    pivot_table.columns = ['ПН', 'ВТ', 'СР', 'ЧТ', 'ПТ', 'СБ', 'ВС']

    # Нормализуем данные по каждому столбцу (дню недели)
    # pivot_table_normalized = pivot_table.apply(lambda x: (x - x.min()) / (x.max() - x.min()), axis=0)
    pivot_table_normalized = pivot_table

    # Построение тепловой карты с нормализацией по столбцам
    plt.figure(figsize=(6, 10))
    # sns.heatmap(pivot_table_normalized, cmap="RdYlGn_r", annot=pivot_table, fmt=".0f", linewidths=0, cbar=False)
    ax = sns.heatmap(pivot_table_normalized, cmap="RdYlGn_r", annot=pivot_table, fmt=".0f", linewidths=0.1, cbar=False)

    plt.title('Загруженность gym на текущей неделе', pad=30)
    plt.xlabel('')
    plt.ylabel('')

    # Размещаем подписи оси x сверху
    ax.xaxis.set_ticks_position('top')
    plt.xticks(rotation=0)


    # Соответствие дней недели
    day_mapping = {
        'Monday': 'ПН',
        'Tuesday': 'ВТ',
        'Wednesday': 'СР',
        'Thursday': 'ЧТ',
        'Friday': 'ПТ',
        'Saturday': 'СБ',
        'Sunday': 'ВС'
    }
    # Функция для округления времени до ближайших 30 минут в меньшую сторону
    def round_to_nearest_30min(dt):
        return dt - timedelta(minutes=dt.minute % 30, seconds=dt.second, microseconds=dt.microsecond)
    # Получаем текущее время
    current_time = datetime.now() + timedelta(hours=3)
    # Округляем до ближайших 30 минут
    current_time = round_to_nearest_30min(current_time).strftime('%H:%M')
    # current_time = '13:30'
    current_day = (datetime.now() + timedelta(hours=3)).strftime('%A')
    # Поиск позиции текущего времени и дня недели
    if current_time in pivot_table_normalized.index and day_mapping[current_day] in pivot_table_normalized.columns:
        row_idx = pivot_table_normalized.index.get_loc(current_time)
        col_idx = pivot_table_normalized.columns.get_loc(day_mapping[current_day])
        # Добавляем красный квадрат вокруг ячейки
        ax.add_patch(plt.Rectangle((col_idx, row_idx), 1, 1, fill=False, edgecolor='blue', lw=3))

    # Получение объекта Figure из объекта Axes
    fig = plt.gcf()  # Получаем текущую фигуру (get current figure)
    # Сохраняем график в объект BytesIO вместо файла
    buf_current_week = BytesIO()
    fig.savefig(buf_current_week, format="png")
    buf_current_week.seek(0)  # Перемещаем указатель в начало буфера
    # plt.show()
    plt.close()

    
    bot = telebot.TeleBot(main_config["telegram"]["token"])
    chatid = main_config["telegram"]["notification_tables"]
    

    for kostil_ in range(0, 6):
        msg_id_last = str(
            read_df_bd(
                """
        select 
            chat_message_id 
        from (
            select 
                datetime, chat_message_id,
                row_number () over (order by datetime desc) rwn
            from 
                raw.gym_online_checker_message_id_hist
            where 1=1
            group by
                datetime, chat_message_id
        ) t1
        where 1=1 and rwn = 1
        """,
                "raw",
            )["chat_message_id"][0]
        )
        
        last_msgs = literal_eval(msg_id_last)
        
        try:
            bot.delete_message(last_msgs[0][0], last_msgs[0][1])
            bot.delete_message(last_msgs[1][0], last_msgs[1][1])
            bot.delete_message(last_msgs[2][0], last_msgs[2][1])
            break
        except:
            pass
    
    
    # Функция для отправки графика
    def send_plot(message):
        chat_id = chatid
        buf.seek(0) 
        bot.send_photo(chat_id, photo=buf)
    
    
    text_message = f"{df['ts'].max().strftime('%Y-%m-%d %H:%M')} Онлайн gym [{df_sovietskaya.sort_values(by='ts').tail(1).iloc[0][1]}]:"
    # Отправляем текстовое сообщение
    msg_text_bot = bot.send_message(chatid, text_message)
    msg_text = msg_text_bot.chat.id, msg_text_bot.message_id
    
    buf1 = buf_now  # Первое изображение
    buf2 = buf_current_week # Второе изобрежние
    media = [InputMediaPhoto(buf1), InputMediaPhoto(buf2)]
    msg = bot.send_media_group(chatid, media)
    msg_one = msg[0].chat.id, msg[0].message_id
    msg_two = msg[1].chat.id, msg[1].message_id
    
    
    data_msg = pd.DataFrame(
        {
            "datetime": [datetime.today() + timedelta(hours=3)],
            "chat_message_id": [[msg_text, msg_one, msg_two]],
            "message": [text_message],
        }
    )
    load_df_bd("raw", "gym_online_checker_message_id_hist", data_msg)




with DAG(**dag_params) as dag:

    task_sent_data_gym = PythonOperator(
        task_id='task_sent_data_gym',
        python_callable=get_data_gym,
        execution_timeout=timedelta(minutes=20)
    )

    task_sent_data_gym