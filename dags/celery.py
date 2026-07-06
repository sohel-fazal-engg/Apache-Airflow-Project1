from airflow.sdk import dag, task
from time import sleep

@dag
def celery_dag():
    
    @task
    def A():
        sleep(5)

    @task(
        queue = "high_cpu"
    )
    def B():
        sleep(5)

    @task(
        queue = "high_cpu"
    )
    def C():
        sleep(5)

    @task(
        queue = "high_cpu"
    )
    def D():
        sleep(5)

    A() >> [B(), C()] >> D()

celery_dag()