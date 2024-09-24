from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import requests
import datetime
import pendulum
import logging
from os import listdir, makedirs, getcwd, rename
from os.path import isfile, join, dirname, exists
import json

PROCESSED_PATH = "/data_lake/processed/breweries/"
FAILED_PATH = "/data_lake/failed/breweries/"


@dag(
    dag_id="extract_api",
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def ProcessBreweries():
    def move_failed_file(file_path, f):
        new_path = join(getcwd(), FAILED_PATH)
        new_file_path = f"{getcwd()}{FAILED_PATH}{f}"
        logging.info(f"[FAIL] Moving to: {new_file_path}")
        rename(file_path, new_file_path)

    def move_successful_file(file_path, f):
        new_path = join(getcwd(),PROCESSED_PATH)
        new_file_path = f"{getcwd()}{PROCESSED_PATH}{f}"
        logging.info(f"[SUCCESS] Moving to: {new_file_path}")

        rename(file_path, new_file_path)
    
    @task
    def init_env(**kwargs):
        makedirs(join(getcwd(),'data_lake'), exist_ok=True)
        makedirs(join(getcwd(),'data_lake/raw/breweries/'), exist_ok=True)
        makedirs(join(getcwd(),'data_lake/trusted/breweries/'), exist_ok=True)
        makedirs(join(getcwd(),'data_lake/refined/breweries/'), exist_ok=True)
        makedirs(join(getcwd(),'data_lake/processed/breweries/'), exist_ok=True)
        makedirs(join(getcwd(),'data_lake/failed/breweries/'), exist_ok=True)

    @task
    def fetch_data_to_local(**kwargs):
        url = 'https://api.openbrewerydb.org/breweries'
        response = requests.get(url)
        data = response.json()

        now = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        
        ## Define path to save data
        local_path = getcwd() + f"/data_lake/raw/breweries/{now}.json"
        logging.info("Saved:" + local_path)
        
        # Create folder if does not exist
        makedirs(dirname(local_path), mode=0o777, exist_ok=True)
        
        # Save JSON locally
        with open(local_path, 'w') as f:
            json.dump(data, f)

    @task
    def transform_data_to_parquet(**kwargs):
        # Init Spark session
        spark = SparkSession.builder.appName("datalake-brewery").getOrCreate()

        # Load data into local folder
        raw_path = getcwd() + f"/data_lake/raw/breweries/"
        
        files = [f for f in listdir(raw_path) if isfile(join(raw_path, f))]
        for f in files:
            try:
                file_path = join(raw_path, f)
                logging.info("Reading " + file_path)
                df = spark.read.options(mode="PERMISSIVE", columnNameOfCorruptRecord = "_corrupt_record").json(file_path, multiLine=True).cache()
                # Check if file is corrupted
                if df is None :
                    move_failed_file(f)
                    pass # next file

                logging.info("Reading successful")
                df.printSchema()
                logging.info(df.head(5))

                # Create folder if does not exist
                trusted_path = f"{getcwd()}/data_lake/trusted/breweries/"
                #makedirs(refined_path, exist_ok=True)

                logging.info("Writing at: " + trusted_path)

                # Convert to Parquet and partition by brewery location - postal code
                df.write \
                    .partitionBy("postal_code") \
                    .parquet(refined_path, mode="overwrite")

                # Move processed files to folder
                # TODO: implement purge policy

                move_successful_file(file_path, f)
            except Exception as e:
                logging.error(e)

                # Move files to folder
                move_failed_file(file_path, f)

    
    init_env() >> fetch_data_to_local() >> transform_data_to_parquet()
                
dag = ProcessBreweries()

# # Definição do DAG
# default_args = {
#     'owner': 'airflow',
#     'start_date': days_ago(1),
# }

# with DAG('extract_api', default_args=default_args, schedule_interval='@daily') as dag:
    
#     init_env = PythonOperator(
#         task_id='init_env',
#         python_callable=init_env
#     )

#     ingest_data = PythonOperator(
#         task_id='fetch_data_to_local',
#         python_callable=fetch_data_to_local
#     )

#     transform_data = PythonOperator(
#         task_id='transform_data_to_parquet',
#         python_callable=transform_data_to_parquet
#     )

#     init_env >> ingest_data >> transform_data
    
    