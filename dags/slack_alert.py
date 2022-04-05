from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

import time
from datetime import datetime, timedelta
from airflow.models.dag import DagRun
from airflow.operators.dummy import DummyOperator
from textwrap import dedent

SLACK_CONN_ID = 'slack_oliver'
SLACK_STATUS_EXECUTION_TIMEOUT = ":warning: Task Failed by Execution Timeout."

def task_fail_slack_alert(context):
    task_instance = context.get('task_instance')
    task_id = task_instance.task_id
    dag_id = task_instance.dag_id
    log = task_instance.log_url.replace('localhost','10.0.3.188')
    #date = context.get('execution_date')
    date = context.get('')
    msg = """
            :strawberry: Task Failed.
            *Task*: {task}  
            *Dag*: {dag} 
            *Execution Time*: {exec_date} 
            *Execution Time(KST)*: {time}
            *Log URL: {log_url}
            """.format(
            task=task_id,
            dag=dag_id,
            exec_date=date,
            time=date,#.strftime('%Y%m%d%H'),
            log_url = log)
    failed_alert = SlackWebhookOperator(
        task_id='slack_alert',
        http_conn_id=SLACK_CONN_ID,
        message=msg,
        username='airflow')
    return failed_alert.execute(context=context)


main_dag = DAG(
    dag_id="slack_alert",
    schedule_interval=None,
    start_date=datetime(2022, 4, 4),
    tags=["test"],
)

with main_dag:

    timeout_sleep = BashOperator(
        task_id='fail',
        bash_command= 'exit(1)',
        on_failure_callback=task_fail_slack_alert,
    )