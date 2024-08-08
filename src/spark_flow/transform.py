import pandas as pd 
import os 
import shutil

def re_partition(load_dt, from_path='/home/centa/data/movie/extract'):
    home_dir = os.path.expanduser("~")
    read_path = f'{home_dir}/{from_path}/load_dt={load_dt}'
    write_base = f'{home_dir}/data/movie/repartition'
    wirte_path = f'{write_base}/load_dt={load_dt}'
    
    df = pd.read_parquet(read_path)
    df['load_dt'] = load_dt
    rm_dir(write_path)
    df.to_parquet(
        write_base,partition_cols=['movieCd','multiMovieYn','repNationCd']
        )
    return df.size, read_path, f'{write_path}/load_dt={load_dt}'   

def rm_dir(dir_path):
    if os.path.exists(dir_path):
        shutil.rmtree(dir_path)
