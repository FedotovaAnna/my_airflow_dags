
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

# Функція, яка підключається до PostgreSQL і читає таблицю countries
def fetch_countries():
    hook = PostgresHook(postgres_conn_id='ihorbetlei_sql')  # Підключення через Airflow Connection
    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM public.countries")  # SQL-запит
    results = cursor.fetchall()  # Отримання всіх рядків

    for row in results:
        print(row)  # Вивід у лог (або можна використати logging.info)

    cursor.close()
    conn.close()

# Аргументи за замовчуванням
default_args = {
    'start_date': datetime(2024, 1, 1),
    'catchup': False
}

# Оголошення DAG
with DAG(
    dag_id='postgres_hook_example',
    schedule_interval=None,  # Лише ручний запуск
    default_args=default_args,
    description='Простий DAG із PostgresHook і SELECT',
    tags=['postgres', 'example']
) as dag:

    # Таска для запуску функції fetch_countries
    task_fetch = PythonOperator(
        task_id='fetch_countries',
        python_callable=fetch_countries
    )

    task_fetch  # Запуск таски