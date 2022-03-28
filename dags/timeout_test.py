import time
from datetime import datetime, timedelta
from textwrap import dedent
from typing import Any, Dict, List, Optional, Tuple

from airflow import AirflowException
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.exceptions import AirflowTaskTimeout
from airflow.hooks.base_hook import BaseHook
from airflow.models import DAG, TaskInstance
from airflow.operators.python import PythonOperator

SLACK_STATUS_TASK_FAILED = ":red_circle: Task Failed"
SLACK_STATUS_EXECUTION_TIMEOUT = ":alert: Task Failed by Execution Timeout."
SLACK_CONN_ID = 'slack_oliver'

def send_slack_alert_sla_miss(
        dag: DAG,
        task_list: str,
        blocking_task_list: str,
        slas: List[Tuple],
        blocking_tis: List[TaskInstance],
) -> None:
    """Send `SLA missed` alert to Slack"""
    task_instance: TaskInstance = blocking_tis[0]
    message = dedent(
        f"""
        :warning: Task SLA missed.
        *DAG*: {dag.dag_id}
        *Task*: {task_instance.task_id}
        *Execution Time*: {task_instance.execution_date.strftime("%Y-%m-%d %H:%M:%S")} UTC
        *SLA Time*: {task_instance.task.sla}
        _* Time by which the job is expected to succeed_
        *Task State*: `{task_instance.state}`
        *Blocking Task List*: {blocking_task_list}
        *Log URL*: {task_instance.log_url}
        """
    )
    send_slack_alert(message=message)


def send_slack_alert_task_failed(context: Dict[str, Any]) -> None:
    """Send `Task Failed` notification to Slack"""
    task_instance: TaskInstance = context.get("task_instance")
    exception: AirflowException = context.get("exception")

    status = SLACK_STATUS_TASK_FAILED
    if isinstance(exception, AirflowTaskTimeout):
        status = SLACK_STATUS_EXECUTION_TIMEOUT

    # Prepare formatted Slack message
    message = dedent(
        f"""
        {status}
        *DAG*: {task_instance.dag_id}
        *Task*: {task_instance.task_id}
        *Execution Time*: {context.get("execution_date").to_datetime_string()} UTC
        *SLA Time*: {task_instance.task.sla}
        _* Time by which the job is expected to succeed_
        *Execution Timeout*: {task_instance.task.execution_timeout}
        _** Max time allowed for the execution of this task instance_
        *Task Duration*: {timedelta(seconds=round(task_instance.duration))}
        *Task State*: `{task_instance.state}`
        *Exception*: {exception}
        *Log URL*: {task_instance.log_url}
        """
    )
    send_slack_alert(
        message=message,
        context=context,
    )


def send_slack_alert(
        message: str,
        context: Optional[Dict[str, Any]] = None,
) -> None:
    """Send prepared message to Slack"""
    notification = SlackWebhookOperator(
        task_id="slack_notification",
        http_conn_id=SLACK_CONN_ID,
        message=message,
        username="airflow",
    )
    notification.execute(context)


# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    "owner": "airflow",
    "email": ["test@test,com"],
    "email_on_failure": True,
    "depends_on_past": False,
    "retry_delay": timedelta(minutes=4),
    "sla": timedelta(minutes=1),  # Default Task SLA time
    "execution_timeout": timedelta(minutes=3),  # Default Task Execution Timeout
    "on_failure_callback": send_slack_alert_task_failed,
}

with DAG(
        dag_id="test_sla",
        schedule_interval="*/30 * * * *",
        start_date=datetime(2021, 1, 11),
        default_args=default_args,
        sla_miss_callback=send_slack_alert_sla_miss,  # Must be set here, not in default_args!
) as dag:
    delay_python_task1 = PythonOperator(
        task_id="delay_five_minutes_python_task1",
        python_callable=lambda: time.sleep(240),
    )
    delay_python_task2=PythonOperator(
        task_id="delay_five_minutes_python_task2",
        python_callable=lambda: time.sleep(240),
    )
    delay_python_task2.set_upstream(delay_python_task1)