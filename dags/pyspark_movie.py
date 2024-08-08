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

REQUIREMENTS = "git+https://github.com/GITSangWoo/spark_flow.git@main"

def gen_emp(id,rule="all_success"):
    op = EmptyOperator(task_id = id, trigger_rule=rule)
    return op

#def gen_vpython(**kw):
#    task = PythonVirtualenvOperator(
#           task_id = kw['id'],
#           python_callable = kw['func'],
#           system_site_pacakges = False,
#           requiremetns = REQUIREMENTS,
#           )
#    return task

def re_partition():
    from spark_flow.transform import re_partition 
    result = re_partition()
    print(result)   

def join_df():
    pass

def agg():
    pass


with DAG(
        'pyspark_movie',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='pyspark Dag',
    schedule="10 2 * * *",
    start_date=datetime(2023, 1, 1),
    end_date=datetime(2023, 5, 1),
    catchup=True,
    tags=['pyspark', 'movie', 'data'],
) as dag:

    re_partition = PythonVirtualenvOperator(
        task_id="re_partition",
        python_callable = re_partition,
        requirements=REQUIREMENTS,
        system_site_packages = False,
    )

    join_df = BashOperator (
        task_id = "join.df",
        bash_command="""
        echo "join_df"       
        """
    )
    
    agg = PythonVirtualenvOperator(
        task_id="agg",
        python_callable = agg     
    )

    task_start = gen_emp('start')
    task_end = gen_emp('end','all_done')
    
    task_start >> re_partition >> join_df >> agg >> task_end

