from airflow.sdk import dag, task, Context
from typing import Dict, Any

@dag
def xcom_dag():

    # @task
    # def t1(context: Context):
    #     val = 10042
    #     context['ti'].xcom_push(key = 'my_key', value = val)
    @task
    def t1_better() -> Dict[str, Any]:
        val = 10042
        sent = "Life is beautiful"
        return {
            "my_val" : val,
            "my_sent" : sent
        } # creates two xcoms, one for val, one for sent with key_names as xcom_key

    # @task
    # def t2(context: Context):
    #     val = context['ti'].xcom_pull(task_ids = 't1', key = 'my_key')
    #     print(val)
    @task
    def t2_better(d: Dict[str, Any]): # This is how you communicate between two tasks, the ideal way
        print(f"Value fetched = {d['my_val']}")
        print(f"Sentence fetched = {d['my_sent']}")

    # t1() >> t2()
    val = t1_better()
    t2_better(val)

xcom_dag()