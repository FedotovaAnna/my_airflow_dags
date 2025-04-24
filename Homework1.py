from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Задача get_first
def get_first():
    return 42  # приклад значення

# Задача get_second
def get_second():
    return 58  # приклад значення

# Задача add_values
def add_values(ti):
    # Отримуємо значення з XCom
    first_value = ti.xcom_pull(task_ids='get_first')
    second_value = ti.xcom_pull(task_ids='get_second')
    total = first_value + second_value
    print(f"Сума значень: {total}")

# Визначення DAG
with DAG(
    dag_id='example_dag',
    start_date=datetime(2025, 4, 21),
    schedule_interval=None,
    catchup=False,
) as dag:

    # Оператори задач
    task_get_first = PythonOperator(
        task_id='get_first',
        python_callable=get_first,
    )

    task_get_second = PythonOperator(
        task_id='get_second',
        python_callable=get_second,
    )

    task_add_values = PythonOperator(
        task_id='add_values',
        python_callable=add_values,
    )

    # Зв'язки між задачами
    task_get_first >> task_get_second >> task_add_values