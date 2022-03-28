from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from textwrap import dedent
from airflow.models.dag import DagRun
from airflow.models import TaskInstance
from airflow.operators.bash import BashOperator
from airflow.utils import trigger_rule
from airflow.sensors.base import BaseSensorOperator



import time
from datetime import datetime, timedelta
from pprint import pprint

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.state import State


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


sensors_dag = DAG(
    "test_launch_sensors",
    schedule_interval=None,
    start_date=datetime(2020, 2, 14, 0, 0, 0),
    dagrun_timeout=timedelta(minutes=150),
    tags=["DEMO"],
)

dummy_dag = DAG(
    dag_id="example_dummy",
    start_date=datetime(2022, 3, 25),
    #schedule_interval="*/30 * * * *",
    schedule_interval=None,
    on_failure_callback=dag_active,
    #execution_timeout=timedelta(seconds=5),
    dagrun_timeout=timedelta(seconds=2),
)

def print_context(ds, **context):
    print(context['conf'])

with dummy_dag:
    starts = DummyOperator(task_id="starts", dag=dummy_dag)

    delayed1 = PythonOperator(
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
    ends = DummyOperator(task_id="ends", dag=dummy_dag)

    delayed1 >> delayed2 >> [delayed3, delayed4] >> delayed5

with sensors_dag:
    trigger = TriggerDagRunOperator(
        task_id=f"trigger_{dummy_dag.dag_id}",
        trigger_dag_id=dummy_dag.dag_id,
        conf={"key": "value"},
        execution_date="{{ execution_date }}",
    )
    sensor = ExternalTaskSensor(
        task_id="wait_for_dag",
        external_dag_id=dummy_dag.dag_id,
        external_task_id="ends",
        failed_states=["failed", "upstream_failed","skipped"],
        poke_interval=5,
        timeout=120,
    )
    trigger >> sensor
