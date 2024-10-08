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


def repartition(ds_nodash):
    from spark_flow.transform import re_partition 
    print(ds_nodash)
    result = re_partition(load_dt=ds_nodash)
    print(result)   



with DAG(
        'pyspark_movie',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='pyspark Dag',
    schedule="10 2 * * *",
    start_date=datetime(2015, 1, 1),
    end_date=datetime(2016, 1, 1),
    catchup=True,
    tags=['pyspark', 'movie', 'data'],
) as dag:

    re_partition = PythonVirtualenvOperator(
        task_id="repartition",
        python_callable = repartition,
        requirements=REQUIREMENTS,
        system_site_packages = False,
    )

    join_df = BashOperator (
        task_id = "join.df",
        bash_command="""
           $SPARK_HOME/bin/spark-submit /home/centa/code/spark_flow/py/movie_join_df.py {{ds_nodash}} 
        """
    )

    agg = BashOperator (
        task_id = "agg",
        bash_command="""
           $SPARK_HOME/bin/spark-submit /home/centa/code/spark_flow/py/agg.py {{ds_nodash}} 
        """
    )
    
      
    

    task_start = gen_emp('start')
    task_end = gen_emp('end','all_done')
    
    task_start >> re_partition >> join_df >> agg >> task_end

