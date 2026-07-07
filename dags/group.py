from airflow.sdk import dag, task, task_group

@dag
def group_dag():

    @task
    def a():
        return 42

    @task_group(default_args={
        "retries" : 2
    })
    def my_group(val: int):
        
        @task
        def b(my_val: int):
            print(my_val + 100)

        @task_group(default_args={
            "retries" : 3
        })
        def nested_group():
        
            @task
            def c():
                print("Task C executed")

            @task
            def d():
                print("Task D executed")

            c() >> d()

        b(val) >> nested_group()

    val = a()
    my_group(val)

group_dag()