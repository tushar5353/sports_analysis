from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime

from sports_analysis.utils import utils

extract_command = "python /opt/airflow/sports_analysis/cricket/etl/run.py --extract"
transform_silver_command = "python /opt/airflow/sports_analysis/cricket/etl/run.py --load_silver"
transform_gold_command = "python /opt/airflow/sports_analysis/cricket/etl/run.py --load_gold"

load_tasks = [
    'T20', 'ODI', 'CPL', 'ODM', 'RLC', 'HND', 'IPO', 'Test', 'WOD', 'MCL', 'CCH', 'WTB',
    'NTB', 'MLC', 'IPT', 'MCT', 'IPL', 'PSL', 'PKS', 'SSH', 'WPL', 'MLT', 'ILT', 'SAT',
    'BPL', 'SSM', 'BBL', 'NPL', 'SMA', 'WBB', 'CTC', 'RHF', 'WCL', 'LPL', 'CEC', 'IT20',
    'FRB', 'SFT', 'BLZ', 'WTC', 'BWT', 'MSL', 'WSL', 'MDM'
]
load_tasks = [load_tasks[i:i + 8] for i in range(0, len(load_tasks), 8)]

gold_tasks = ["match_info", "batting_stats", "bowling_stats", "fielding_stats"]

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 9, 9),
    'retries': 0,
}

with DAG(
    dag_id='cricket-ETL',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['cricket_analysis'],
) as dag:

    # Bronze Layer Task Group
    with TaskGroup("bronze_layer", tooltip="Bronze layer raw data extraction") as bronze_layer:
        extract_raw = BashOperator(
            task_id='extract_raw',
            bash_command=extract_command
        )

    # Silver Layer Task Group
    with TaskGroup("silver_layer", tooltip="Silver layer processing") as silver_layer:
        silver_tasks = []
        for name in load_tasks:
            task = BashOperator(
                task_id=f'load_silver-{"-".join(name)}',
                bash_command=f'{transform_silver_command} {"-".join(name)}'
            )
            silver_tasks.append(task)

    silver_success = BashOperator(
        task_id='SilverTransformation-success',
        bash_command='echo "Silver Transformation Successful"'
    )

    # Gold Layer Task Group
    with TaskGroup("gold_layer", tooltip="Gold layer processing") as gold_layer:
        gold_tasks_list = []
        for task_type in gold_tasks:
            task = BashOperator(
                task_id=f'load_gold-{task_type}',
                bash_command=f'{transform_gold_command} {task_type}'
            )
            gold_tasks_list.append(task)

    gold_success = BashOperator(
        task_id='GoldTransformation-success',
        bash_command='echo "Gold Transformation Successful"'
    )

    # Setting up dependencies across layers
    bronze_layer >> silver_layer >> silver_success >> gold_layer >> gold_success
