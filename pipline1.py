
from airflow import DAG  # Основний клас DAG для створення графу задач
from airflow.operators.python import PythonOperator  # Для запуску Python-функцій як тасок
from airflow.operators.bash import BashOperator  # Для запуску Bash-команд
from datetime import datetime  # Для вказання дати початку
import time  # Щоб симулювати затримки у функціях

# Функція для таски: симуляція завантаження даних
def fetch_data():
    print("Завантаження даних...")
    time.sleep(2)  # Затримка 2 секунди
    print("Дані успішно завантажено.")

# Функція для таски: симуляція обробки даних
def process_data():
    print("Обробка даних...")
    time.sleep(3)  # Затримка 3 секунди
    print("Обробка завершена.")

# Аргументи за замовчуванням для DAG
default_args = {
    'start_date': datetime(2024, 1, 1),  # Дата, з якої починається виконання DAG
    'catchup': False  # Не запускати DAG за пропущені дати в минулому
}

# Оголошення DAG за допомогою контекстного менеджера with
with DAG(
    dag_id='mixed_python_bash_dag',  # Назва DAG
    schedule_interval=None,  # Без автоматичного розкладу, лише вручну
    default_args=default_args,  # Базові параметри
    description='DAG із послідовними і паралельними тасками, Python і Bash',  # Короткий опис
    tags=["example", "bash", "python"]  # Теги для пошуку в UI
) as dag:

    # Початкова Bash-таска
    start = BashOperator(
        task_id='start',
        bash_command='echo "Початок виконання DAG-а"'  # Виводить повідомлення
    )

    # Python-таска для завантаження даних
    fetch_data_task = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_data  # Вказуємо функцію, яку потрібно виконати
    )

    # Bash-таска для перевірки диску
    check_disk = BashOperator(
        task_id='check_disk_space',
        bash_command='echo "Перевірка дискового простору" && df -h'  # Вивід дискового простору
    )

    # Python-таска для обробки даних
    process_data_task = PythonOperator(
        task_id='process_data',
        python_callable=process_data
    )

    # Фінальна Bash-таска
    finalize = BashOperator(
        task_id='finalize',
        bash_command='echo "Виконання DAG-а завершено"'  # Завершальний етап
    )

    # Залежності між тасками:
    # Після start → паралельно запускаються fetch_data_task і check_disk
    # Після fetch_data_task → запускається process_data_task
    # Після check_disk і process_data_task → запускається finalize
    start >> [fetch_data_task, check_disk]
    fetch_data_task >> process_data_task
    [check_disk, process_data_task] >> finalize
    