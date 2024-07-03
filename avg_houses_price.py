import pandas as pd
import os
import shutil
from datetime import datetime
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def calculate_average_and_move_file():
  df = pd.read_csv('./toProcess/houses.csv')

  avg = df['price'].mean()

  result_df = pd.DataFrame({'moyenne': [avg]})

  timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
  filename = f"result/result_{timestamp}.csv"

  os.makedirs(os.path.dirname(filename), exist_ok=True)

  result_df.to_csv(filename, index=False)

  print(f"Résultat écrit dans le fichier : {filename}")

  processed_folder = "already_processed"
  new_path = os.path.join(processed_folder, os.path.basename(filename))

  if os.path.exists(new_path):
      os.remove(new_path) 
      print(f"Ancien fichier remplacé : {new_path}")

  shutil.move(filename, new_path)
  print(f"Fichier déplacé vers : {new_path}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 3),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'avg_houses_price',
    default_args=default_args,
    description='Exécute le script Python et déplace le fichier résultant',
    schedule_interval=timedelta(days=1),
)

run_python_script = PythonOperator(
    task_id='run_python_script',
    python_callable=calculate_average_and_move_file,
    dag=dag,
)

run_python_script