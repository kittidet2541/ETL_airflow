from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.dates import days_ago
# from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
import pandas as pd
import numpy as np

MYSQL_CONNECTION = "mysql_default"  # ชื่อ connector ใน Airflow ที่ตั้งค่าไว้

# path ทั้งหมดที่จะใช้
movie2020_2009_raw_output_path = "/home/airflow/gcs/data/raw/movie2020_2009.csv"
movie2010_2024_raw_output_path = "/home/airflow/gcs/data/raw/movie2010_2024.csv"
movie2024_raw_output_path = "/home/airflow/gcs/data/raw/movie2024.csv"
movie_merge_output_path = "/home/airflow/gcs/data/cleaned/movie_merge.csv"
final_output_path = "/home/airflow/gcs/data/cleaned/final_output.csv"


def get_data_from_database(movie2020_2009_raw_path, movie2010_2024_raw_path, movie2024_raw_path):
    # เรียกใช้ MySqlHook เพื่อต่อไปยัง MySQL จาก connection ที่สร้างไว้ใน Airflow
    mysqlserver = MySqlHook(MYSQL_CONNECTION)

    # Query จาก database โดยใช้ Hook ที่สร้าง ได้ผลลัพธ์เป็น pandas DataFrame
    df1 = mysqlserver.get_pandas_df(sql="SELECT * FROM sql12727442.TABLE1")
    df2 = mysqlserver.get_pandas_df(sql="SELECT * FROM sql12727442.TABLE2")
    df3 = mysqlserver.get_pandas_df(sql="SELECT * FROM sql12727442.TABLE3")

    # Save เป็น csv
    df1.to_csv(movie2020_2009_raw_path, index=False)
    print(f"Output to {movie2020_2009_raw_path}")

    df2.to_csv(movie2010_2024_raw_path, index=False)
    print(f"Output to {movie2010_2024_raw_path}")

    df3.to_csv(movie2024_raw_path, index=False)
    print(f"Output to {movie2024_raw_path}")


def merge_data(movie2020_2009_raw_path, movie2010_2024_raw_path, movie2024_raw_path, movie_merge_path):
    # อ่านจากไฟล์ สังเกตว่าใช้ path จากที่รับ parameter มา
    df1 = pd.read_csv(movie2020_2009_raw_path)
    df2 = pd.read_csv(movie2010_2024_raw_path)
    df3 = pd.read_csv(movie2024_raw_path)

    data = pd.concat([df1, df2, df3], ignore_index=True)
    data.to_csv(movie_merge_path, index=False)
    print(f"Output to {movie_merge_path}")


def clean_data(movie_merge_path, final_path):
    # อ่านจากไฟล์ สังเกตว่าใช้ path จากที่รับ parameter มา
    data = pd.read_csv(movie_merge_path)
    data.drop(['Rank'], axis=1, inplace=True)
    data.columns = data.columns.str.strip()

    data['Domestic']= data['Domestic'].str.replace(",","").astype(float)
    data['Worldwide']= data['Worldwide'].str.replace(",","").astype(float)
    data['Foreign']= data['Foreign'].str.replace(",","").astype(float)
    data['Domestic_percent'] = data['Domestic_percent'].str.strip('%')
    data['Domestic_percent'] = data['Domestic_percent'].str.replace('<0.1',"0").astype(float)
    data['Foreign_percent'] = data['Foreign_percent'].str.strip('%')
    data['Foreign_percent'] = data['Foreign_percent'].str.replace('<0.1',"0").astype(float)
    data['year'] = data['year'].astype(int)

    # Renaming the Columns for better understanding
    data.rename(columns={"Release Group": "Movie Name"}, inplace=True)

    data.to_csv(final_path, index=False)
    print(f"Output to {final_path}")

# กำหนด DAG
with DAG(
    "movie_data_to_bq",
    start_date=days_ago(1),
    schedule_interval="@once",
    tags=["movie_data"]
) as dag:
    # Task สำหรับดึงข้อมูลจาก database
    t1 = PythonOperator(
        task_id="get_data_from_database",
        python_callable=get_data_from_database,
        op_kwargs={
            "movie2020_2009_raw_path": movie2020_2009_raw_output_path,
            "movie2010_2024_raw_path": movie2010_2024_raw_output_path,
            "movie2024_raw_path": movie2024_raw_output_path
        }
    )
    t2 = PythonOperator(
        task_id="merge_data",
        python_callable=merge_data,
        op_kwargs={
            "movie2020_2009_raw_path": movie2020_2009_raw_output_path,
            "movie2010_2024_raw_path": movie2010_2024_raw_output_path,
            "movie2024_raw_path": movie2024_raw_output_path,
            "movie_merge_path": movie_merge_output_path
        }
    )
    t3 = PythonOperator(
        task_id="clean_data",
        python_callable=clean_data,
        op_kwargs={
            "movie_merge_path": movie_merge_output_path,
            "final_path": final_output_path
        }
    )
    t4 = BashOperator (
        task_id = "load_to_bq",
        bash_command = "bq load \
            --source_format=CSV \
            --autodetect \
            movie.allmovie \
            gs://us-central1-composer3-713a07bc-bucket/data/cleaned/final_output.csv"

    )       
    # กำหนด dependencies ให้แต่ละ tasks
    t1 >> t2 >> t3 >> t4


        # t4 = GCSToBigQueryOperator(
        # task_id='load_to_bq',
        # bucket='us-central1-composer3-713a07bc-bucket',
        # source_objects=['data/cleaned/final_output.csv'],
        # destination_project_dataset_table='dataset.table_name',
        # autodetect=True,write_disposition='WRITE_TRUNCATE',)

    
