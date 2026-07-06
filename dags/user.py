from airflow.sdk import dag, task, Asset

# Defines an asset using logical identifier
user_asset = Asset(uri = "x-api://randomuser.me/api/")

# Defines a dag named user_assets_pipeline, tells it to schedule daily
@dag(
    dag_id = "user_assets_pipeline",
    schedule = "@daily"
)
def user_assets_pipeline():
    """This is the actual function which exectutes in the dag"""
    
    # Creating first upstream task, taking outlets = [user_asset] defined earlier
    @task(outlets = [user_asset])
    def user():
        import requests
        response = requests.get("https://randomuser.me/api/")
        response.raise_for_status()

        data = response.json()
        return data
    
    # Creating downstream task/asset, taking inlets = [user_asset]
    @task(inlets = [user_asset])
    def user_location(ti = None):
        user_data = ti.xcom_pull(task_ids = "user") # pulls the data from the user task using xcom_pull
        """The previous line is possible as the tasks are in the same dag
        The same asset should appear in the producer's outlets and the consumer's inlets,
        if you want Airflow to understand that they are producing and consuming the same dataset."""
        user_location = user_data['results'][0]['location']
        return user_location
    


    @task(inlets = [user_asset])
    def user_login(ti = None):
        user_data = ti.xcom_pull(task_ids = "user")
        user_login = user_data['results'][0]['login']
        return user_login

    fetch_user = user()
    fetch_user_location = user_location()
    fetch_user_login = user_login()
    fetch_user >> fetch_user_location
    fetch_user >> fetch_user_login

user_assets_pipeline()

# For older version of airflow used in Marc Lamberti's course

# from airflow.sdk import asset, Asset, Context

# @asset(
#     schedule = "@daily",
#     uri = "https://randomuser.me/api/"
# )
# def user(self) -> dict[str]:
#     import requests
#     response = requests.get(self.uri)
#     return response.json()

# @asset(
#     schedule = user
# )
# def user_location(user : Asset, context : Context) -> dict[str]:
#     ti = context["ti"]

#     print(f"DAG ID: {ti.dag_id}")
#     print(f"Task ID: {ti.task_id}")
#     print(f"Run ID: {ti.run_id}")

#     user_data = context['ti'].xcom_pull(
#         dag_id = "user",
#         task_ids = "user",
#         include_prior_dates = True
#     )
#     print(f"User fetched : {user_data}")
#     print(context["dag"].dag_id)
#     print(context["run_id"])
#     return user_data['results'][0]['location']