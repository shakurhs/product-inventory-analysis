'''
Product Inventory Analysis

Nama    : Hafizal Shakur

Program ini dibuat untuk mengatur dan menjadwalkan pengerjaan batch processing
menggunakan Airflow dengan proses mengambil data dari PostGreSQL, data cleaning,
dan menyimpan di Elastic Search untuk ditampilkan menggunakan Kibana.
'''


import datetime as dt
from datetime import timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from elasticsearch import Elasticsearch

import pandas as pd
import psycopg2 as db 

conn_string="dbname='airflow' host='postgres' user='airflow' password='airflow'"

def retrieve_sql():
    conn=db.connect(conn_string)
    '''
 
    Fungsi/Node ini bekerja dengan cara mengambil dataset dari PostGreSQL
    yang kemudian data tersebut akan disimpan ke dalam file csv sementara sebelum
    melalui proses data cleaning.
    
    Konfigurasi antara Python dan PostGreSQL menggunakan library
    psycopg2 dengan koneksi database, host, user, dan password disesuaikan
    dengan pengaturan pada file .yaml.

    '''

    try:
        df=pd.read_sql("select * from table_m3", conn)
        df.to_csv('/opt/airflow/dags/supermarket_inventory.csv', index=False)

    finally:
        conn.close()

def clean_data():
    '''
    Fungsi/Node kedua adalah proses data cleaning. Data didapatkan dari folder
    dimana file csv sementara disimpan. Proses data cleaning yang dilakukan
    pertama adalah penyesuian nama column menjadi huruf kecil dan setiap katanya dipisahkan
    underscore dan menghapus karakter lain selain huruf dan angka.

    Proses data cleaning kedua adalah mengatasi missing value dan data duplikat dengan drop.

    Proses selanjutnya adalah membersihkan value dan mengganti data type pada kolom
    "product_id", "supplier_id", "unit_price", "date_received", "last_order_date", dan
    "expiration_date" menjadi tipe data yang sesuai.

    Selanjutnya data yang sudah dibersihkan disimpan dalam file csv
    
    '''
    df = pd.read_csv('/opt/airflow/dags/supermarket_inventory.csv')
    # df.columns=[re.sub('[^a-z0-9]', '', x.strip().lower().replace(' ','_'))
    #             for x in df.columns]
    df.columns = (df.columns.str.strip().str.lower().str.replace(' ','_').str.replace(r'[^a-z0-9_]', '', regex=True))
    df.dropna(inplace=True)
    df.drop_duplicates(inplace=True)
    df['unit_price'] = df['unit_price'].str.replace(r'[^0-9.]', '',regex=True).astype(float)
    df['date_received'] = pd.to_datetime(df['date_received'], format='%m/%d/%Y')
    df['last_order_date'] = pd.to_datetime(df['last_order_date'], format='%m/%d/%Y')
    df['expiration_date'] = pd.to_datetime(df['expiration_date'], format='%m/%d/%Y')
    df.to_csv('/opt/airflow/dags/P2M3_hafizal_shakur_data_clean.csv', index=False)


def to_es():
    '''

    Fungsi/Node terakhir merupakan node untuk mennyimpan atau meng-copy data ke
    Elastic Search. Data yang digunakan adalah data yang sudah melalui proses cleaning dan
    data disimpan ke dalam index "m3".
    
    '''
    es = Elasticsearch("http://elasticsearch:9200") 
    df = pd.read_csv('/opt/airflow/dags/P2M3_hafizal_shakur_data_clean.csv')
    for i,r in df.iterrows():
        doc=r.to_json()
        res=es.index(index="m3v2", doc_type="doc", body=doc, id=i+1)
        print(res)


default_args = {
    'owner': 'shakur',
    'start_date': dt.datetime(2024, 11, 1, 9, 0, 0)-timedelta(hours=7), # menyesuaikan waktu indonesia bagian barat
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
}
with DAG('M3SQLtoKibana',
         default_args=default_args,
         schedule_interval='10-30/10 9 * * 6', # melakukan processing setiap hari sabtu pada pukul 9 pada menit ke 10,20, dan 30
         catchup=False, # batch processing mengabaikan proses dari start_date, hanya menjalankan proses ketika DAG di-run
         max_active_runs=1
         ) as dag:

    retrieveSQL = PythonOperator(task_id='retrieve', python_callable=retrieve_sql)
    cleanData = PythonOperator(task_id='clean', python_callable=clean_data)
    toES = PythonOperator(task_id='copy', python_callable=to_es)


retrieveSQL >> cleanData >> toES # urutan pengerjaan batch processing menggunakan airflow