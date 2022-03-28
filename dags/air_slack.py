from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator

from common import utc_to_kst

SLACK_CONN_ID = 'slack_alert'
def task_fail_slack_alert(context):
    task_instance = context.get('task_instance')
    task_id = task_instance.task_id
    dag_id = task_instance.dag_id
    date = context.get('execution_date')
    log_url = context.get('task_instance').log_url
    log_url = log_url.replace('localhost','0.0.0.0')
    msg = """
            :strawberry: Task Failed.
            <@U022T50D4F5>  
            *Task*: {task}  
            *Dag*: {dag} 
            *Execution Time*: {exec_date} 
            *Execution Time(KST)*: {time}
            *Log URL: {log_url}
            """.format(
            task=task_id,
            dag=dag_id,
            exec_date=date,
            time=utc_to_kst(date.strftime('%Y%m%d%H')),
            log_url = log_url)
    failed_alert = SlackWebhookOperator(
        task_id='slack_alert',
        http_conn_id=SLACK_CONN_ID,
        message=msg,
        username='airflow')
    return failed_alert.execute(context=context)


def fail():
    raise Exception()
    #return {'result': 'ok'}



def dummy(date, ds):
    print('@@@@@@@ this is airflow execution date (input_date UTC) ', date)
    print('@@@@@@@ this is kst time!! ', utc_to_kst(date))
    return {'result': 'fail'}

dag = DAG(
    dag_id='slack11',
    start_date=days_ago(1),
    catchup=False,
    #on_success_callback=task_fail_slack_alert,
    #on_failure_callback=task_fail_slack_alert,
    schedule_interval="@once"
)
print_input_date = PythonOperator(
        task_id="print_input_date1",
        op_kwargs={'date': '{{ execution_date.strftime("%Y%m%d%H") }}',
                   'ds': '{{ ds }}'},
        python_callable=dummy,
        provide_context=True,
        on_failure_callback=task_fail_slack_alert,
        dag=dag
    )
print_input_date1 = PythonOperator(
        task_id="print_input_date21",
        op_kwargs={'date': '{{ execution_date.strftime("%Y%m%d%H") }}',
                   'ds': '{{ ds }}'},
        python_callable=fail,
        provide_context=True,
        on_failure_callback=task_fail_slack_alert,
        dag=dag
    )
print_input_date2 = PythonOperator(
        task_id="print_input_date22",
        op_kwargs={'date': '{{ execution_date.strftime("%Y%m%d%H") }}',
                   'ds': '{{ ds }}'},
        python_callable=fail,
        provide_context=True,
        on_failure_callback=task_fail_slack_alert,
        dag=dag
    )

print_input_date >> [print_input_date1,print_input_date2]
"""
BashOperator(task_id="test",
              bash_command='exit(1)',
              dag=dag)
"""