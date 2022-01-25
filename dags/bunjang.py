from datetime import timedelta
from airflow import DAG 
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

# crawling related deps
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup
import re

def getProducts(query):

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
        for e in soup.find_all("div", {"class": "sc-gmeYpB iqTUpV"}): # product class name
            productNames.append(e.text)

        for e in soup.find_all("div", {"class": re.compile("sc-kZmsYB*")}): # price class name
            if e.text=="연락요망":
                productPrices.append(-1)
            else:
                productPrices.append(e.text.replace(',',''))

        for e in soup.find_all("a", {"class": "sc-kxynE kzhuNn"}): # product page id
            productLinks.append(e["data-pid"])

        with open("output.txt", "w") as text_file:
            for i in range(len(productNames)):
                text_file.write(productNames[i]+','+productPrices[i]+','+productLinks[i]+'\n')
    finally:
        browser.quit()


#defining DAG arguments

default_args = {
    'owner': 'jueon',
    'start_date': days_ago(0),
    'email': ['jueonpk@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'bunjang-crawling-dag',
    default_args=default_args,
    description='test',
    schedule_interval=timedelta(minutes=10),
)

extract = PythonOperator(
    task_id='extract',
    provide_context=True,
    python_callable=getProducts,
    op_kwargs={'query': 'RRL'},
    dag=dag,
)

extract
