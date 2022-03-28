from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.models.dag import DagRun
from airflow.models import TaskInstance
from airflow.operators.bash import BashOperator
from airflow.utils import trigger_rule
from airflow.sensors.base import BaseSensorOperator
from airflow.operators.python import PythonOperator
import time

SLACK_CONN_ID = 'slack_oliver'
SLACK_STATUS_EXECUTION_TIMEOUT = ":warning: Task Failed by Execution Timeout."

def _on_dag_run_fail(context):
    print("***DAG failed!! do something***")
    print(f"The DAG failed because: {context['reason']}")
    print(context)


def time_sleep(n):
    time.sleep(n)

def send_slack_alert(context):
    task_instance = context.get("task_instance")
    exception = context.get("exception")
    notification = SlackWebhookOperator(
        task_id="slack_notification",
        http_conn_id=SLACK_CONN_ID,
        message=dedent(
        f"""
        {SLACK_STATUS_EXECUTION_TIMEOUT}
        *The DAG failed because: {context.get('reason')}
        *DAG*: {task_instance.dag_id}
        *Task*: {task_instance.task_id}
        *State*: {task_instance.state}
        *Execution Time*: {context.get("execution_date").to_datetime_string()} UTC
        *Execution Timeout*: {task_instance.task.execution_timeout}
        _** Max time allowed for the execution of this task instance_
        *Task Duration*: {timedelta(seconds=round(task_instance.duration))}
        *Task State*: `{task_instance.state}`
        *Exception*: {exception}
        *Log URL*: {task_instance.log_url}
        """
    ),
        username="airflow",
    )
    notification.execute(context)

def dag_active( dag_id):
    """Print the Airflow context and ds variable from the context."""
    dagrun = DagRun.find(dag_id=dag_id)

    message = "test"
    task_instances = dagrun[-1].get_task_instances()
    print(dagrun[-1].state)
    for ti in task_instances:
        # 각 task instance의 id와 state를 확인한다.
        task_id = ti.task_id
        state = ti.current_state()
        message += f"task {task_id} state ::: {state}\n"

    notification = SlackWebhookOperator(
        task_id="slack_notification",
        http_conn_id=SLACK_CONN_ID,
        message=message)

    notification.execute(context=task_instances)

    #print(message)

    return 'Whatever you return gets printed in the logs'


default_args = {
    "owner": "oliver",
}

with DAG(
    dag_id="example",
    start_date=datetime(2022, 3, 25),
    #schedule_interval="*/30 * * * *",
    schedule_interval=None,
    default_args=default_args,
    on_failure_callback=dag_active,
    #execution_timeout=timedelta(seconds=5),
    dagrun_timeout=timedelta(seconds=2),
) as dag:
    delayed1 =PythonOperator(
        task_id="delayed1",
        python_callable=time_sleep,
        op_kwargs={'n': 3},
        on_failure_callback=send_slack_alert

    )
    delayed2 = PythonOperator(
        task_id="delayed2",
        python_callable=time_sleep,
        op_kwargs={'n': 4},
        on_failure_callback=send_slack_alert,

    )
    delayed3 = PythonOperator(
        task_id="delayed3",
        python_callable=time_sleep,
        op_kwargs={'n': 3},
        on_failure_callback=send_slack_alert,
    )
    delayed4 = PythonOperator(
        task_id="delayed4",
        python_callable=time_sleep,
        op_kwargs={'n': 3},
        on_failure_callback=send_slack_alert,
    )

    delayed5 = PythonOperator(
        task_id="delayed5",
        python_callable=time_sleep,
        op_kwargs={'n': 3},
        on_failure_callback=send_slack_alert,
    )
    end1 = BaseSensorOperator(
        task_id='sensor',
        mode='reschedule',
    )

"""
    end = PythonOperator(
        task_id="send_message",
        python_callable=dag_active,
        op_kwargs={'dag_id': 'example'},
        #trigger_rule="one_failed",
        provide_context=True
    )
"""

delayed1 >> delayed2 >> [delayed3, delayed4] >> delayed5
