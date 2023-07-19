from datetime import datetime,timedelta
import requests
import xml.etree.ElementTree as ET
import csv
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import XCom
from airflow.models import Variable 

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 7, 19, 23, 0),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

def download_rss_feed(**context):
    url = 'https://timesofindia.indiatimes.com/rssfeedstopstories.cms'
    response = requests.get(url)
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    filename = f'raw_rss_feed_{timestamp}.xml'
    
    with open(filename, 'w') as file:
        file.write(response.text)
    
    context['task_instance'].xcom_push(key='filename', value=filename)

def parse_rss_feed(**context):
    ti = context['task_instance']
    filename = ti.xcom_pull(key='filename')

    tree = ET.parse(filename)
    root = tree.getroot()

    items = []
    for item in root.findall('.//item'):
        title = item.find('title').text
        link = item.find('link').text
        pub_date = item.find('pubDate').text
        items.append((title, link, pub_date))

    curated_filename = f'curated_{datetime.now().strftime("%Y%m%d%H%M%S")}.csv'
    with open(curated_filename, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Title', 'Link', 'Pub Date'])
        writer.writerows(items)

    ti.xcom_push(key='curated_filename', value=curated_filename)

def load_to_database(**context):
    ti = context['task_instance']
    curated_filename = ti.xcom_pull(key='curated_filename')

    # Retrieve PostgreSQL credentials using Airflow's Variable
    postgres_user = Variable.get("POSTGRES_USER")
    postgres_password = Variable.get("POSTGRES_PASSWORD")
    postgres_db = Variable.get("POSTGRES_DB")
    postgres_host = "postgres"  # Use the container service name as the host

    # Connection string for PostgreSQL
    conn_str = f"postgresql+psycopg2://{postgres_user}:{postgres_password}@{postgres_host}/{postgres_db}"


    
    df = pd.read_csv(curated_filename)
    df.to_sql('NEWS_feED', conn_str, if_exists='append', index=False)

dag = DAG('rss_etl_dag', default_args=default_args, schedule_interval='0 23 * * *')

start_task = DummyOperator(task_id='start_task', dag=dag)

download_task = PythonOperator(
    task_id='download_rss_feed',
    python_callable=download_rss_feed,
    provide_context=True,
    dag=dag
)

parse_task = PythonOperator(
    task_id='parse_rss_feed',
    python_callable=parse_rss_feed, 
    provide_context=True,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_to_database',
    python_callable=load_to_database,
    provide_context=True,
    dag=dag
)

end_task = DummyOperator(task_id='end_task', dag=dag)

start_task >> download_task >> parse_task >> load_task >> end_task
