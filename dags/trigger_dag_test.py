from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from textwrap import dedent
from airflow.models.dag import DagRun
from airflow.operators.python import BranchPythonOperator
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

SLACK_CONN_ID = 'slack_oliver'
delay_time = 5
options = ['end_sensor','stop']

def time_sleep(n):
    time.sleep(n)

def send_slack(msg, context):
    notification = SlackWebhookOperator(
        task_id="send_slack",
        http_conn_id=SLACK_CONN_ID,
        message=msg,
        username="airflow",
    )
    notification.execute(context=context)

def send_timeout_alert(ti,dag_id, branch_id, **context):
    tasks = ti.xcom_pull(key='task_stopped', task_ids=branch_id)
    sensor_task = context.get("task_instance")
    dagrun = DagRun.find(dag_id=dag_id)

    task_instances = dagrun[-1].get_task_instances()
    message = dedent(
        f"""
        :warning: Task Failed by Execution Timeout.
        *Execution Time*: {context.get("data_interval_start").to_datetime_string()} UTC
        """)
    for tis in task_instances:
        # 각 task instance의 id와 state를 확인한다.
        message += f"task {tis.task_id} : state {tis.current_state()}\n"
    message += f"*Log URL*: {sensor_task.log_url}"
    send_slack(message, context)

def send_fail_alert(context):
    task_instance = context.get("task_instance")
    exception = context.get("exception")
    message = dedent(
        f"""
            :warning: Task Failed by Execution Timeout.
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
            """)
    send_slack(message, context)

def dag_active_xcom(ti,  dag_id):
    """Print the Airflow context and ds variable from the context."""
    dagrun = DagRun.find(dag_id=dag_id)

    message = {}
    task_instances = dagrun[-1].get_task_instances()
    print(dagrun[-1].state)
    for task_i in task_instances:
        # 각 task instance의 id와 state를 확인한다.
        message[task_i.current_state()] = message.get(task_i.current_state(),[]) + [task_i.task_id]
    ti.xcom_push(key='task_info', value=message)

    return None

def branch_func(ti, task_id,options):
    task_info = ti.xcom_pull(key='task_info', task_ids=task_id)
    task_fail_list = []
    for i in task_info.keys():
        if i != 'success':
            task_fail_list += task_info[i]
    ti.xcom_push(key='task_stopped', value=task_fail_list)
    if len(task_info.keys()) > 1:
        return options[1] #'stop'
    else:
        return options[0] #'end_sensor'

sensors_dag = DAG(
    dag_id="sensor_test",
    schedule_interval=None,
    catchup=False,
    start_date=datetime(2022, 4, 5),
    tags=["DEMO"],
)

dummy_dag = DAG(
    dag_id="dummy_test",
    start_date=datetime(2022, 4, 5),
    #schedule_interval="*/30 * * * *",
    schedule_interval=None,
    dagrun_timeout=timedelta(seconds=10),
)
with dummy_dag:

    starts = DummyOperator(task_id="starts", dag=dummy_dag)

    delayed1 = PythonOperator(
        task_id="delayed1",
        python_callable=time_sleep,
        op_kwargs={'n': delay_time},
        on_failure_callback=send_fail_alert,
        dag=dummy_dag,
    )
    always_fail = BashOperator(
        task_id='fail',
        bash_command='exit(1)',
        on_failure_callback=send_fail_alert,
    )
    delayed2 = PythonOperator(
        task_id="delayed2",
        python_callable=time_sleep,
        op_kwargs={'n': delay_time},
        on_failure_callback=send_fail_alert,
        dag=dummy_dag,
    )
    delayed3 = PythonOperator(
        task_id="delayed3",
        python_callable=time_sleep,
        op_kwargs={'n': delay_time},
        on_failure_callback=send_fail_alert,
        dag=dummy_dag,
    )

    delayed5 = PythonOperator(
        task_id="delayed5",
        python_callable=time_sleep,
        op_kwargs={'n': 10},
        on_failure_callback=send_fail_alert,
        dag=dummy_dag,
    )

    ends = DummyOperator(task_id="end_task", dag=dummy_dag)

    starts >> delayed1 >> always_fail >> delayed2 >> delayed3 >> delayed5 >> ends

with sensors_dag:

    trigger = TriggerDagRunOperator(
        task_id=f"trigger_{dummy_dag.dag_id}",
        trigger_dag_id=dummy_dag.dag_id,
        conf={"key": "value"},
        execution_date="{{ execution_date }}",
    )

    timeout_sleep = PythonOperator(
        task_id='time_sleep',
        python_callable=time_sleep,
        op_kwargs={'n':20},
        dag=sensors_dag
    )

    read_dag = PythonOperator(
        task_id='dag_read',
        python_callable=dag_active_xcom,
        op_kwargs={'dag_id':'dummy_test'},
        dag=sensors_dag,
        do_xcom_push=True,
    )
    branching = BranchPythonOperator(
        task_id='branching',
        dag=sensors_dag,
        python_callable=branch_func,
        op_kwargs={'task_id': read_dag.task_id,
                   'options': options},
    )
    stop = PythonOperator(
        task_id=options[1],
        dag=sensors_dag,
        python_callable=send_timeout_alert,
        op_kwargs={'dag_id': dummy_dag.dag_id,
                   'branch_id': branching.task_id},
        provide_context=True,

    )
    end = DummyOperator(task_id=options[0], dag=sensors_dag)



    trigger >> timeout_sleep >> read_dag >> branching
    branching >> stop
    branching >> end
