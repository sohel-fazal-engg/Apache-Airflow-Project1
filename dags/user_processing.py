from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk.bases.sensor import PokeReturnValue
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Create table -> Is_api_available -> Extract user -> Process user -> Store user

# def _extract_user(ti):
#     # user = ti.xcom_pull(task_ids = "is_api_available") # xcom is like a box of information which can be shared between aiflow tasks
#     import requests
#     user = requests.get("https://raw.githubusercontent.com/marclamberti/datasets/refs/heads/main/fakeuser.json").json()
#     return {
#         "id" : user["id"],
#         "first_name" : user["personalInfo"]["firstName"],
#         "last_name" : user["personalInfo"]["lastName"],
#         "email" : user["personalInfo"]["email"]
#     }

# Defining a dag with the @dag decorator
@dag
def user_processing():
    #user_processing is the dag

    # create_table task
    create_table = SQLExecuteQueryOperator(
        task_id = "create_table",
        conn_id = "postgres",
        sql = """
                    CREATE TABLE IF NOT EXISTS users(
                        id INT PRIMARY KEY,
                        first_name VARCHAR(255),
                        last_name VARCHAR(255),
                        email VARCHAR(255),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """  
    )

    # is_api_available task, which is of type sensor, which means it waits for an external input before succeeding/executing
    @task.sensor(poke_interval = 30, timeout = 300)
    def is_api_available() -> PokeReturnValue:
        """@task is the decorator that corresponds to a PythonOperator.
        When we want to execute a python code we need to use @task.
        @task.sensor means the python function we decorate is a sensor.
        Its a python operator that waits for the condition to be true to succeed."""

        import requests
        response = requests.get("https://raw.githubusercontent.com/marclamberti/datasets/refs/heads/main/fakeuser.json")
        print(f"Response fetched : Status code = {response.status_code}")

        if response.status_code == 200:
            condition = True
            user = response.json()
        else:
            condition = False
            user = False
        return PokeReturnValue(is_done = condition, xcom_value = user)
        # xcom_values is the data that is shared between tasks
    
    # is_api_available()

    # Extract user task
    # extract_user = PythonOperator(
    #     task_id = "extract_user",
    #     python_callable = _extract_user
    # )

    # Creating tasks the better way, using @task decorator
    @task
    def extract_user_better(user):
        print(f"User = {user}")
        return {
            "id" : user["id"],
            "first_name" : user["personalInfo"]["firstName"],
            "last_name" : user["personalInfo"]["lastName"],
            "email" : user["personalInfo"]["email"]
        }

    # user1 = is_api_available()
    # user_info = extract_user_better(user1)

    @task
    def process_user(user):
        import csv
        from datetime import datetime
        user["created_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open("/tmp/user_info.csv", "w+", newline="") as f:
            writer = csv.DictWriter(f, fieldnames = user.keys())
            writer.writeheader()
            writer.writerow(user)
    
    # processed_user = process_user(user_info)

    # Defining hooks allow us to use functions which are not present in standard operators
    # e.g. Here we have used copy_expert from PostgresHook
    @task
    def store_user():
        hook = PostgresHook(postgres_conn_id = "postgres")
        hook.copy_expert(sql = "COPY users from STDIN WITH CSV HEADER;",
                         filename = "/tmp/user_info.csv")
        
    # store_user()
    # Defining the dependencies
    # This is not the only way to define dependencies, we can also use set_upstream() and set_downstream() functions
    process_user(extract_user_better(create_table >> is_api_available())) >> store_user()

user_processing() # Calling the dag