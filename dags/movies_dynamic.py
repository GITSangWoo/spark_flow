from datetime import datetime, timedelta
from textwrap import dedent
from pprint import pprint

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
    ExternalPythonOperator,
    PythonOperator,
    PythonVirtualenvOperator,
    BranchPythonOperator,
    is_venv_installed,
)

def gen_emp(id,rule="all_success"):
    op = EmptyOperator(task_id = id, trigger_rule=rule)
    return op

def parsing():
    pass

def select():
    pass

REQUIREMENTS = ""

with DAG(
        'movie_dynamic',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='movie Dyanmic Dag',
    schedule="10 2 * * *",
    start_date=datetime(2015, 1, 1),
    end_date=datetime(2015, 1, 2),
    catchup=True,
    tags=['pyspark', 'movie', 'data'],
) as dag:

    start = gen_emp(id="start")
    end = gen_emp(id="end")
    
    get_data = BashOperator(
        task_id = "get.data",
        bash_command="""
            echo "get data"
        """
    )
    
    parsing_parquet = PythonVirtualenvOperator(
        task_id="parsing.parquet",
        python_callable = parsing,
        requirements=REQUIREMENTS,
        system_site_packages = False,
    )
    
    select_parquet = PythonVirtualenvOperator(
        task_id="select.parquet",
        python_callable = select,
        requirements=REQUIREMENTS,
        system_site_packages = False,
    )

    

    

    start >> get_data >> parsing_parquet >> select_parquet >> end



















