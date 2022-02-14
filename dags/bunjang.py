from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

# crawling deps
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException

from bs4 import BeautifulSoup

import re

def getProducts(**kwargs):
    query=kwargs['keyword']
    min_price=kwargs['min_price']
    max_price=kwargs['max_price']
    filename=kwargs['filename']
    keywords_except=kwargs['keywords_except']

    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')

    browser = webdriver.Chrome(options=options)

    url="https://m.bunjang.co.kr/search/products?q=" + query

    browser.get(url)
    try:
        element = WebDriverWait(browser, 10).until(
            EC.presence_of_element_located((By.ID, "root"))
        )
        soup = BeautifulSoup(element.get_attribute('innerHTML'), features='html.parser')
        # mydivs = soup.find_all("div", {"class": "sc-gmeYpB iqTUpV"})
        # print(mydivs.text)

        productNames=[]
        productPrices=[]
        productLinks=[]
        for e in soup.find_all("div", {"class": "sc-gmeYpB iqTUpV"}):
            productNames.append(e.text)

        for e in soup.find_all("div", {"class": re.compile("sc-kZmsYB*")}):
            if e.text=="연락요망":
                productPrices.append(-1)
            else:
                productPrices.append(e.text.replace(',',''))

        for e in soup.find_all("a", {"class": "sc-kxynE kzhuNn"}):
            productLinks.append(e["data-pid"])

        with open(filename, "w") as text_file:
            for i in range(len(productNames)):
                vlt=0
                for kwd in keywords_except:
                    if kwd in productNames[i]:
                        vlt=1
                        break
                if not vlt and int(productPrices[i]) >= min_price and int(productPrices[i]) <= max_price:
                    text_file.write(productNames[i]+','+productPrices[i]+','+productLinks[i]+'\n')
                # print(productNames[i], productPrices[i], productLinks[i])

    finally:
        browser.quit()


#defining DAG arguments

default_args = {
    'owner': 'jovyan',
    'start_date': days_ago(0),
    'email': ['ramesh@somemail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'bunjang-multiple-items-dag',
    catchup=False,
    default_args=default_args,
    description='test',
    schedule_interval=timedelta(minutes=10),
)

orslow = PythonOperator(
    task_id='orslow',
    provide_context=True,
    python_callable=getProducts,
    op_kwargs={'keyword': 'orslow', 'min_price': 0, 'max_price': 200000, 'filename': 'orslow', 'keywords_except': ['여성용']},
    dag=dag,
)

rrl = PythonOperator(
    task_id='rrl',
    provide_context=True,
    python_callable=getProducts,
    op_kwargs={'keyword': 'rrl', 'min_price': 0, 'max_price': 300000, 'filename': 'rrl', 'keywords_except': []},
    dag=dag,
)

[orslow, rrl]
