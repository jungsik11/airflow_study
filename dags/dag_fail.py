import airflow
from airflow.operators.python import PythonOperator
import os
from airflow.models import Variable
import logging
from airflow import configuration as conf
from airflow.models.dag import DagRun
from airflow.models import DagBag, TaskInstance
from airflow import DAG, settings
from airflow.operators.bash import BashOperator

main_dag_id = 'DynamicWorkflow2'

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(1),
    'provide_context': True
}

dag = DAG(
    main_dag_id,
    schedule_interval="@once",
    default_args=args)


def start(*args, **kwargs):
    value = Variable.get("DynamicWorkflow_Group1")
    logging.info("Current DynamicWorkflow_Group1 value is " + str(value))

def resetTasksStatus(task_id, execution_date):
    logging.info("Resetting: " + task_id + " " + execution_date)

    dag_folder = conf.get('core', 'DAGS_FOLDER')
    dagbag = DagBag(dag_folder)
    check_dag = dagbag.dags[main_dag_id]
    print(check_dag)
    session = settings.Session()

    my_task = check_dag.get_task(task_id)
    ti = TaskInstance(my_task, execution_date)
    state = ti.current_state()
    logging.info("Current state of " + task_id + " is " + str(state))
    ti.set_state(None, session)
    state = ti.current_state()
    logging.info("Updated state of " + task_id + " is " + str(state))


def bridge1(*args, **kwargs):
    # You can set this value dynamically e.g., from a database or a calculation
    dynamicValue = 2

    variableValue = Variable.get("DynamicWorkflow_Group2")
    logging.info("Current DynamicWorkflow_Group2 value is " + str(variableValue))

    logging.info("Setting the Airflow Variable DynamicWorkflow_Group2 to " + str(dynamicValue))
    os.system('airflow variables --set DynamicWorkflow_Group2 ' + str(dynamicValue))

    variableValue = Variable.get("DynamicWorkflow_Group2")
    logging.info("Current DynamicWorkflow_Group2 value is " + str(variableValue))

    # Below code prevents this bug: https://issues.apache.org/jira/browse/AIRFLOW-1460
    for i in range(dynamicValue):
        resetTasksStatus('secondGroup_' + str(i), str(kwargs['execution_date']))

def bridge2(*args, **kwargs):

    # You can set this value dynamically e.g., from a database or a calculation
    dynamicValue = 3

    variableValue = Variable.get("DynamicWorkflow_Group3")
    logging.info("Current DynamicWorkflow_Group3 value is " + str(variableValue))

    logging.info("Setting the Airflow Variable DynamicWorkflow_Group3 to " + str(dynamicValue))
    os.system('airflow variables --set DynamicWorkflow_Group3 ' + str(dynamicValue))

    variableValue = Variable.get("DynamicWorkflow_Group3")
    logging.info("Current DynamicWorkflow_Group3 value is " + str(variableValue))

    # Below code prevents this bug: https://issues.apache.org/jira/browse/AIRFLOW-1460
    for i in range(dynamicValue):
        resetTasksStatus('thirdGroup_' + str(i), str(kwargs['execution_date']))


def end(*args, **kwargs):
    logging.info("Ending")


def doSomeWork(name, index, *args, **kwargs):
    # Do whatever work you need to do
    # Here I will just create a new file
    os.system('touch ./dosome_' + str(name) + str(index) + '.txt')


starting_task = PythonOperator(
    task_id='start',
    dag=dag,
    provide_context=True,
    python_callable=start,
    op_args=[])

# Used to connect the stream in the event that the range is zero
bridge1_task = PythonOperator(
    task_id='bridge1',
    dag=dag,
    provide_context=True,
    python_callable=bridge1,
    op_args=[])

DynamicWorkflow_Group1 = Variable.get("DynamicWorkflow_Group1")
logging.info("The current DynamicWorkflow_Group1 value is " + str(DynamicWorkflow_Group1))

for index in range(int(DynamicWorkflow_Group1)):
    dynamicTask = PythonOperator(
        task_id='firstGroup_' + str(index),
        dag=dag,
        provide_context=True,
        python_callable=doSomeWork,
        op_args=['firstGroup', index])

    starting_task.set_downstream(dynamicTask)
    dynamicTask.set_downstream(bridge1_task)

# Used to connect the stream in the event that the range is zero
bridge2_task = PythonOperator(
    task_id='bridge2',
    dag=dag,
    provide_context=True,
    python_callable=bridge2,
    op_args=[])

DynamicWorkflow_Group2 = Variable.get("DynamicWorkflow_Group2")
logging.info("The current DynamicWorkflow value is " + str(DynamicWorkflow_Group2))

for index in range(int(DynamicWorkflow_Group2)):
    dynamicTask = PythonOperator(
        task_id='secondGroup_' + str(index),
        dag=dag,
        provide_context=True,
        python_callable=doSomeWork,
        op_args=['secondGroup', index])

    bridge1_task.set_downstream(dynamicTask)
    dynamicTask.set_downstream(bridge2_task)

ending_task = PythonOperator(
    task_id='end',
    dag=dag,
    provide_context=True,
    python_callable=end,
    op_args=[])

DynamicWorkflow_Group3 = Variable.get("DynamicWorkflow_Group3")
logging.info("The current DynamicWorkflow value is " + str(DynamicWorkflow_Group3))

for index in range(int(DynamicWorkflow_Group3)):

    # You can make this logic anything you'd like
    # I chose to use the PythonOperator for all tasks
    # except the last task will use the BashOperator
    if index < (int(DynamicWorkflow_Group3) - 1):
        dynamicTask = PythonOperator(
            task_id='thirdGroup_' + str(index),
            dag=dag,
            provide_context=True,
            python_callable=doSomeWork,
            op_args=['thirdGroup', index])
    else:
        dynamicTask = BashOperator(
            task_id='thirdGroup_' + str(index),
            bash_command='touch /home/ec2-user/airflow/thirdGroup_' + str(index) + '.txt',
            dag=dag)

    bridge2_task.set_downstream(dynamicTask)
    dynamicTask.set_downstream(ending_task)

# If you do not connect these then in the event that your range is ever zero you will have a disconnection between your stream
# and your tasks will run simultaneously instead of in your desired stream order.
starting_task.set_downstream(bridge1_task)
bridge1_task.set_downstream(bridge2_task)
bridge2_task.set_downstream(ending_task)