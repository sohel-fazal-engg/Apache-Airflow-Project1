from airflow.sdk import dag, task

@dag
def branch():

    @task
    def a():
        return 101
    
    @task.branch
    def b(val: int):
        if val % 2 == 0:
            return "even_func"
        return ["odd_func", "check_prime"]
    
    @task
    def even_func():
        print("Number is even.")

    @task
    def odd_func():
        print("Number is odd.")

    @task
    def check_prime(num):
        for i in range(2, num):
            if num % i == 0:
                print("Number is not prime")
                return 0
        print("Number is prime")
        return 1
    
    num = a()

    b(num) >> [even_func(), odd_func() >> check_prime(num)]

branch()
