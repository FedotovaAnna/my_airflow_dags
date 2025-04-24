from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Задача для повернення списку
def return_list():
    return [1, 2, 3]  # Приклад списку

# Задача для обчислення суми або довжини списку
def process_list(ti):
    # Отримуємо список через XCom
    my_list = ti.xcom_pull(task_ids='return_list')
    total = sum(my_list)  # Обчислення суми списку
    length = len(my_list)  # Довжина списку
    print(f"Сума списку: {total}")
    print(f"Довжина списку: {length}")

# Визначення DAG
with DAG(
    dag_id='list_processing_dag',
    start_date=datetime(2025, 4, 21),
    schedule_interval=None,
    catchup=False,
) as dag:

    # Оператор задачі для повернення списку
    task_return_list = PythonOperator(
        task_id='return_list',
        python_callable=return_list,
    )

    # Оператор задачі для обробки списку
    task_process_list = PythonOperator(
        task_id='process_list',
        python_callable=process_list,
    )

    # Зв'язок задач
    task_return_list >> task_process_list