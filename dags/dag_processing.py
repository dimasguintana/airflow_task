from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
import pandas as pd
import numpy as np

import logging

def process_data(file_path_1: str, file_path_2: str, output_path: str):
    df1 = pd.read_csv(file_path_1)
    df2 = pd.read_csv(file_path_2)

    df = pd.concat([df1, df2]).drop_duplicates(subset=['number'], keep='last').reset_index(drop=True)
    df["debit"] = np.where(df['client_type'] == 'C', df['amount'], np.nan)
    df["credit"] = np.where(df['client_type'] == 'V', df['amount'], np.nan)

    df_result = df.groupby(["date", "client_code"]).agg(
        stt_count = ("amount", "count"),
        debit = ("debit", "sum"),
        credit = ("credit", "sum")
    ).reset_index()

    df_result.to_csv(output_path, index=False)

def check_data(output_path: str):
    try:
        df_result = pd.read_csv(output_path)
        logging.info("DATA CHECK SUCCESSFUL")
    except Exception as e:
        logging.error("DATA CHECK FAILED")
        logging.error(f"Error details: {e}")
        raise

# Default arguments untuk semua task
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

# Definisi DAG
with DAG(
    dag_id = "processing_data_stt",
    default_args = default_args,
    description = "Processing STT Data",
    schedule  =  None,
    start_date = datetime(2025, 1, 1),
    catchup = False,
    tags = ["stt", "data_processing"],
) as dag:

    t1 = PythonOperator(
        task_id = "data_process_task",
        python_callable = process_data,
        op_kwargs={
            "file_path_1": "{PLACEHOLDER_AIRFLOW_HOME}/data/M+ Software Airflow Assignment - STT1.csv",
            "file_path_2": "{PLACEHOLDER_AIRFLOW_HOME}/data/M+ Software Airflow Assignment - STT2.csv",
            "output_path": "{PLACEHOLDER_AIRFLOW_HOME}/data/processed_transactions.csv"
        }
    )

    t2 = PythonOperator(
        task_id = "data_check_task",
        python_callable = check_data,
        op_kwargs={
            "output_path": "{PLACEHOLDER_AIRFLOW_HOME}/data/processed_transaction.csv"
        }
    )

    t1 >> t2