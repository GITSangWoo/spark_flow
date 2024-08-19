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
)

REQUIREMENTS = "git+https://github.com/GITSangWoo/movdata.git@0.2/movielist"

with DAG(
        'movie_dynamic',
    default_args={
        'depends_on_past': False,
        'retries': 0,
        'retry_delay': timedelta(seconds=3)
    },
    description='movie Dyanmic Dag',
    schedule="@once",
    start_date=datetime(2015, 1, 1),
    end_date=datetime(2015, 1, 2),
    catchup=True,
    tags=['pyspark', 'movie', 'data'],
) as dag:
    
    def gen_emp(id,rule="all_success"):
        op = EmptyOperator(task_id = id, trigger_rule=rule)
        return op

    def getdata(ds_nodash):
        year=ds_nodash[0:4]
        print(year)
        from movdata.ml import save_movies
        save_movies(year)

    start = gen_emp(id="start")
    end = gen_emp(id="end")
    
    get_data = PythonVirtualenvOperator(
        task_id = "get.data",
        python_callable = getdata,
        requirements=REQUIREMENTS,
        system_site_packages = False,
    )
    
    parsing_parquet = BashOperator(
        task_id="parsing.parquet",
        bash_command="""
            YEAR={{ds_nodash[:4]}}
            $SPARK_HOME/bin/spark-submit /home/centa/code/spark_flow/py/flat.py $YEAR
        """
    )
    
    select_parquet = BashOperator(
        task_id="select.parquet",
        bash_command="""
            $SPARK_HOME/bin/spark-submit /home/centa/code/spark_flow/py/my_select.py 
        """
    )


    start >> get_data >> parsing_parquet >> select_parquet >> end



















