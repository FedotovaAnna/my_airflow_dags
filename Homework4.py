from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Задача для повернення поточної дати
def get_current_date():
    return datetime.now()  # Повертаємо поточну дату

# Задача для перевірки, чи це вихідний
def check_weekend(ti):
    # Отримуємо дату через XCom
    current_date = ti.xcom_pull(task_ids='get_current_date')
    # Конвертуємо дату в об'єкт datetime, якщо потрібно
    day_of_week = datetime.strptime(current_date, "%Y-%m-%d %H:%M:%S.%f").weekday()
    # День тижня: 5 (субота), 6 (неділя)
    if day_of_week in [5, 6]:
        print("Вихідний")
    else:
        print("Робочий")

# Визначення DAG
with DAG(
    dag_id='check_date_dag',
    start_date=datetime(2025, 4, 21),
    schedule_interval=None,
    catchup=False,
) as dag:

    # Оператор задачі для повернення дати
    task_get_current_date = PythonOperator(
        task_id='get_current_date',
        python_callable=get_current_date,
    )

    # Оператор задачі для перевірки вихідного
    task_check_weekend = PythonOperator(
        task_id='check_weekend',
        python_callable=check_weekend,
    )

    # Зв'язок задач
    task_get_current_date >> task_check_weekend