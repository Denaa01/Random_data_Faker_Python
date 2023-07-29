import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pandas as pd


def CSVToJson():
    df=pd.read_csv("/home/dina/airflow/dags/mycsv.csv")
    for i, r in df.iterrows():
        print(r["name"])
    
    df.to_json("fromAirFlow.json", orient="records")
#CSVToJson("/home/dina/airflow/dags/mycsv.csv", "name")


default_args={
    "owner": "dina",
    "start_date": dt.datetime(2023,7,27),
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=5)
}

dag =DAG('MyCSVDAG',
          default_args=default_args,
          schedule= timedelta(minutes=5),
          # " 0 * * * * ", 
	)

print_starting = BashOperator(task_id="starting", bash_command='echo "I am Reading the CSV now ..."', dag=dag)
CSVJson= PythonOperator(task_id="Converting_CSV_to_Json", python_callable=CSVToJson, dag=dag)

print_starting >> CSVJson

