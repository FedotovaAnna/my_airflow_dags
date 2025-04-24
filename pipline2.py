
from airflow import DAG  # Імпортуємо клас DAG
from airflow.operators.python import PythonOperator  # Оператор для Python-функцій
from datetime import datetime  # Для задання дати старту

# Функція для першої таски: записує повідомлення у XCom
def push_value(**context):
    message = "Привіт із першого таску"
    context['ti'].xcom_push(key='my_message', value=message)  # Зберігаємо значення в XCom

# Функція для другої таски: зчитує повідомлення з XCom
def pull_value(**context):
    message = context['ti'].xcom_pull(key='my_message', task_ids='push_value')  # Читаємо з XCom
    print(f"Отримано з XCom: {message}")

# Базові параметри для DAG
default_args = {
    'start_date': datetime(2024, 1, 1),
    'catchup': False
}

# Оголошення DAG за допомогою контекстного менеджера
with DAG(
    dag_id='xcom_basic_example',  # Назва DAG
    schedule_interval=None,  # Без автоматичного запуску
    default_args=default_args,  # Базові параметри
    description='Приклад передачі даних через XCom',  # Опис DAG
    tags=['xcom', 'example']  # Теги
) as dag:

    # Таска, яка записує дані у XCom
    push = PythonOperator(
        task_id='push_value',
        python_callable=push_value,
        provide_context=True  # Дозволяє передавати execution context
    )

    # Таска, яка зчитує дані з XCom
    pull = PythonOperator(
        task_id='pull_value',
        python_callable=pull_value,
        provide_context=True
    )

    # Встановлення послідовності: push → pull
    push >> pull