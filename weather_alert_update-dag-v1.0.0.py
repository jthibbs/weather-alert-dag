########################################################################
  # Dag file: Get external weather alert data, put into BigQuery Mon-Fri @ 9:30pm
########################################################################
from airflow import models
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from datetime import timedelta
import io
import os
import pandas as pd
import requests
import time
from zipfile import ZipFile

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'catchup_by_default': False,
    'start_date': datetime(2021, 12, 28, 0, 0, 0),
    'email': ['thibbs@gmail.com'],                                                       ### Email address
    'email_on_failure': True,
    'email_on_success': False,
    'provide_context': True
}



def get_csv():
    resp = requests.get('https://mesonet.agron.iastate.edu/pickup/wwa/'+ str(datetime.now().year) +'_all.zip')
    z = ZipFile(io.BytesIO(resp.content))
    for file in z.namelist():
        if file.endswith('.csv'):
            data = pd.read_csv(z.open(file))
            ### If current month is prior to April, also get last year's data:
            if datetime.now().month < 4:
                resp = requests.get('https://mesonet.agron.iastate.edu/pickup/wwa/'+ str(datetime.now().year - 1) +'_all.zip')
                z = ZipFile(io.BytesIO(resp.content))
                for file in z.namelist():
                    if file.endswith('.csv'):
                        add = pd.read_csv(z.open(file))
                        data = pd.concat([data, add], sort=False)
            str_data = io.StringIO()
            data.to_csv(str_data, index=False)
            str_data.seek(0)
            str_data = str_data.read()
            break
    file = f'/home/airflow/gcs/data/weather_alert.csv'                                  ## GCS path to store the data (in airflow's data folder)
    with open(file, 'w') as f:
        f.write(str_data)

def remove_gcs_file():
    file = f'/home/airflow/gcs/data/weather_alert.csv'
    if os.path.isfile(file):
        os.remove(file)

with models.DAG(
    'weather_alert_update-dag-v1.0.0',                                                  ## GCS bucket (use bucket that has airflow)
     catchup=False,
     default_args=default_args,
     schedule_interval='30 21 * * 1-5',
) as dag:
    ### 1. Get & write file to GCS:
    create_file = PythonOperator(task_id='create_file', python_callable = get_csv)
    ### 2. Load (overwrite) written file into BQ table:
    load_data = GoogleCloudStorageToBigQueryOperator(
        task_id = 'load_data',
        bucket = 'us-west1-jarrett-composer-e-7ea1c000-bucket',                         ## GCS bucket (use bucket that has airflow)
        source_objects = ['data/weather_alert.csv'],                                    ## Directory inside bucket plus written file name
        destination_project_dataset_table = 'jarrett-sandbox.sandbox.weather_alerts',   ## BQ project.dataset.table
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        skip_leading_rows = 1
    )
    ### 3. Remove file from GCS:
    remove_file = PythonOperator(task_id='remove_file', python_callable = remove_gcs_file)

create_file >> load_data >> remove_file


