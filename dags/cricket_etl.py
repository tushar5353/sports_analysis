from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

from sports_analysis.utils import utils

# Define shell commands to trigger different pipeline stages
extract_command = "python /opt/airflow/sports_analysis/cricket/etl/run.py --extract"
transform_silver_command = "python /opt/airflow/sports_analysis/cricket/etl/run.py --load_silver"
transform_gold_command = "python /opt/airflow/sports_analysis/cricket/etl/run.py --load_gold"

# List of match types to load in silver layer, split into chunks of 8 for parallel tasks
load_tasks = ['T20', 'ODI', 'CPL', 'ODM', 'RLC', 'HND', 'IPO', 'Test', 'WOD', 'MCL', 'CCH', 'WTB',
              'NTB', 'MLC', 'IPT', 'MCT', 'IPL', 'PSL', 'PKS', 'SSH', 'WPL', 'MLT', 'ILT', 'SAT',
              'BPL', 'SSM', 'BBL', 'NPL', 'SMA', 'WBB', 'CTC', 'RHF', 'WCL', 'LPL', 'CEC', 'IT20',
              'FRB', 'SFT', 'BLZ', 'WTC', 'BWT', 'MSL', 'WSL', 'MDM']
load_tasks = [load_tasks[i:i + 8] for i in range(0, len(load_tasks), 8)]

# Types of stats to process in gold layer
gold_tasks = ["match_info", "batting_stats", "bowling_stats", "fielding_stats"]

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 9, 9),
    'retries': 0,  # no retries on failure
}

with DAG(
    dag_id='cricket-ETL',
    default_args=default_args,
    schedule=None,  # manually triggered or external scheduler
    catchup=False,  # no backfill
    tags=['cricket_analysis'],
) as dag:

    # Task to extract raw JSON cricket data files
    extract_raw = BashOperator(
        task_id='extract_raw',
        bash_command=extract_command
    )

    # Dummy Bash task indicating completion of silver transformation
    silver_success = BashOperator(
        task_id='SilverTransformation-success',
        bash_command='echo "Silver Transformation  Successful"'
    )

    # Dummy Bash task indicating completion of gold transformation
    gold_success = BashOperator(
        task_id='GoldTransformation-success',
        bash_command='echo "Gold Transformation  Successful"'
    )

    # Create BashOperator tasks for loading silver layer data in parallel batches
    bash_tasks = []
    for name in load_tasks:
        task = BashOperator(
            task_id=f'load_silver-{"-".join(name)}',  # e.g. load_silver-T20-ODI-CPL-...
            bash_command=f'{transform_silver_command} {"-".join(name)}'
        )
        bash_tasks.append(task)

    # Define task dependencies for silver layer: extraction -> parallel loads -> success marker
    for task in bash_tasks:
        extract_raw >> task >> silver_success

    # Create BashOperator tasks for different gold layer processing types
    bash_tasks = []
    for task_type in gold_tasks:
        task = BashOperator(
            task_id=f'load_gold-{task_type}',
            bash_command=f'{transform_gold_command} {task_type}'
        )
        bash_tasks.append(task)

    # Define task dependencies for gold layer: silver success marker -> each gold task -> gold success marker
    for task in bash_tasks:
        silver_success >> task >> gold_success
