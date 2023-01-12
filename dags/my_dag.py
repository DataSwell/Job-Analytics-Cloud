from airflow import DAG
from datetime import datetime
from random import randint
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator


def _training_model():
    return randint(1, 10)


def choose_best_model(ti):
    accuracies = ti.xcom_pull(task_ids=[     # gets the data created by the following tasks
        'traing_model_A',
        'traing_model_B',
        'traing_model_C'
    ])

    best_accurarcy = max(accuracies)

    if (best_accurarcy > 8):
        return 'accurate'
    return 'inaccurate'


with DAG('my_dag', start_date=datetime(2021, 1, 1),
         schedule_interval='@daily', catchup=False) as dag:

    training_model_A = PythonOperator(
        task_id='traing_model_A',
        python_callable=_training_model,
    )

    training_model_B = PythonOperator(
        task_id='traing_model_B',
        python_callable=_training_model,
    )

    training_model_C = PythonOperator(
        task_id='traing_model_C',
        python_callable=_training_model,
    )

    choose_best_model = BranchPythonOperator(
        task_id="choose_best_model",
        python_callable=choose_best_model,
    )

    accurate = BashOperator(
        task_id='accurate',
        bash_command="echo 'inaccurate'"
    )

    inaccurate = BashOperator(
        task_id='inaccurate',
        bash_command="echo 'inaccurate'"
    )

    [training_model_A, training_model_B, training_model_C] >> choose_best_model >> [accurate, inaccurate]
