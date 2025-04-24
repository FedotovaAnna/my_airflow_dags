
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime
import random

# Функція, яка визначає, яку гілку виконувати
def choose_branch():
    result = random.choice(['branch_a', 'branch_b'])  # Симуляція умови
    print(f"Вибрана гілка: {result}")
    return result

# Функції для демонстрації гілок
def run_branch_a():
    print("Виконується гілка A")

def run_branch_b():
    print("Виконується гілка B")

default_args = {
    'start_date': datetime(2024, 1, 1),
    'catchup': False
}

with DAG(
    dag_id='branching_example_dag',
    schedule_interval=None,
    default_args=default_args,
    description='DAG із умовним розгалуженням',
    tags=['branching', 'example']
) as dag:

    start = DummyOperator(task_id='start')

    branch_decision = BranchPythonOperator(
        task_id='branch_decision',
        python_callable=choose_branch
    )

    branch_a = PythonOperator(
        task_id='branch_a',
        python_callable=run_branch_a
    )

    branch_b = PythonOperator(
        task_id='branch_b',
        python_callable=run_branch_b
    )

    join = DummyOperator(
        task_id='join',
        trigger_rule='none_failed_min_one_success'  # DAG продовжиться навіть якщо одна з гілок не виконалась
    )

    start >> branch_decision
    branch_decision >> [branch_a, branch_b] >> join