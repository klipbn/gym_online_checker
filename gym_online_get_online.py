import json
import time
import warnings
from datetime import datetime
from io import StringIO

import chromedriver_autoinstaller
import numpy as np
import pandas as pd
import psycopg2
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

warnings.filterwarnings("ignore")

with open(f"main_config.json") as json_file:
    main_config = json.load(json_file)

print(f"{datetime.today()} : импорты прошли")

chromedriver_autoinstaller.install()
chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--remote-debugging-port=9222")
driver = webdriver.Chrome(options=chrome_options)
print(f"{datetime.today()} : драйвер окей")

driver.get("https://novokosino.gympro.su/schedule")
time.sleep(10)

def get_online(html_content, gym_name):

    html_content = driver.page_source
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Находим элемент с классом 'online-people_rz'
    online_div = soup.find('div', class_='online-people_rz')
    
    if online_div:
        # Извлекаем текст внутри элемента
        online_text = online_div.text.strip()
        
        # Получаем число посетителей, используя split
        online_count = int(online_text.split()[1])  # Берем второе слово, оно будет числом
    else:
        online_count = np.NaN
    
    df = pd.DataFrame.from_dict(
    {
        "ts": [datetime.today()],
        "online": [online_count],
        "gym_name": [gym_name]
    }
    )

    return df

df_nov = get_online(driver.page_source, "Новокосино")
time.sleep(5)

# Ожидаем появления выпадающего списка
wait = WebDriverWait(driver, 10)
select_box = wait.until(EC.element_to_be_clickable((By.CLASS_NAME, "select")))
time.sleep(5)
# Кликаем на выпадающий список
select_box.click()
# Ожидаем появления пункта "СОВЕТСКАЯ" и кликаем на него
sovetskaya_option = wait.until(EC.element_to_be_clickable((By.XPATH, "//li[text()='СОВЕТСКАЯ']")))
time.sleep(5)
sovetskaya_option.click()
time.sleep(10)

df_sov = get_online(driver.page_source, "Советская")

df_online = pd.concat([df_nov, df_sov])

driver.close()
print(f"{datetime.today()} : датафреймы созданы")

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
    output.close() 
    del output 

    cursor.close()
    conn.close()


load_df_bd("raw", "gym_online_checker_hist_regular", df_online)
print(f"{datetime.today()} : загрузили в БД")