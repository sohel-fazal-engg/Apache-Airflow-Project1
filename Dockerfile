FROM apache/airflow:3.2.2

COPY my-sdk /opt/airflow/my-sdk

RUN pip install -e /opt/airflow/my-sdk

RUN pip show my-sdk
RUN python -c "import my_sdk; print(my_sdk.__file__)"

# FROM apache/airflow:3.2.2

# COPY my-sdk /opt/airflow/my-sdk

# RUN pip install -e /opt/airflow/my-sdk