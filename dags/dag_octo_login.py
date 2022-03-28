import pendulum
from datetime import datetime
from airflow import DAG
import sys, os
sys.path.insert(0 ,'/Users/yangjungsik/airflow')

from util.common import utc_to_kst
from task_dict import TASK_DICT
from util.run_balancing import split_stocks
from airflow.operators.python import PythonOperator
from run_batch import batch_main
from util.octoapi import Octoparse
from util.octo_logger import logger

from dotenv import load_dotenv
load_dotenv()
import json

def octo_login(ti, file_path):

    octo = Octoparse()

    print(logger)
    try:
        octo.log_in()
        octo.token_entity['datetime'] = octo.token_entity['datetime'].strftime('%Y-%m-%d-%H-%m-%s')
        with open(file_path, 'w') as outfile:
            json.dump(octo.token_entity, outfile)
    except:
        with open(file_path, 'r') as outfile:
            token = json.load(outfile)
        octo._access_token = token['access_token']
        #logger.warn('octoparse login fail {}'.format(sys.exc_info()))
        #print('octoparse login Fail {}'.format(sys.exc_info()))
        print(octo._access_token)

    ti.xcom_push(key='login', value=octo)
    ti.xcom_push(key='login_token', value=octo._access_token)
    return octo

FILE_NAME_FORMAT = '/Users/yangjungsik/airflow/input_data/k100_set_{idx}.csv'
TASK_NUM = 2

with DAG(
        dag_id="login_test",
        schedule_interval="*/30 * * * *",
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=["test"],
) as dag:
    def dummy(date, ds):
        print('@@@@@@@ this is airflow execution date (input_date UTC) ', date)
        print('@@@@@@@ this is kst time!! ', utc_to_kst(date))
        return {'result': 'ok'}


    print_input_date = PythonOperator(
        task_id="print_input_date",
        python_callable=dummy,
        op_kwargs={'date': '{{ execution_date.strftime("%Y%m%d%H") }}',
                   'ds': '{{ ds }}'},
        provide_context=True
    )

    split_stocks = PythonOperator(
        task_id="split_stocks",
        python_callable=split_stocks,
        op_kwargs={'input_date': '{{ execution_date.strftime("%Y%m%d%H") }}',
                   'task_num': TASK_NUM,
                   'stock_file': '/Users/yangjungsik/airflow/input_data/krx100(2).csv',
                   'output_dir': '/Users/yangjungsik/airflow/input_data'},
        provide_context=True
    )
    print_input_date.set_downstream(split_stocks)

    octo_login = PythonOperator(
        task_id="octoparse_login",
        python_callable=octo_login,
        op_kwargs={'file_path': os.path.dirname(os.path.dirname(FILE_NAME_FORMAT)) +'/util/token.json'},
        provide_context=True
    )
    split_stocks.set_downstream(octo_login)
"""
    task_list = []
    retry_task_list = []
    upload_task_list = []
    upload_task_name_list = []
    default_arg = {
        'input_date': '{{ execution_date.strftime("%Y%m%d%H") }}',
        'output-dir': './data',
        'log-dir': './logs',
    }
    for task_type, task_dict_list in TASK_DICT.items():
        if len(task_dict_list) < TASK_NUM:
            print('@@@ [{}] task_dict_list should not be less than TASK_NUM {}!!'.format(task_type, TASK_NUM))
        for idx, task_dict in enumerate(task_dict_list):
            if TASK_NUM < idx + 1:
                break
            task_arg = default_arg.copy()
            task_arg['input_file'] = FILE_NAME_FORMAT.replace('{idx}', str(idx + 1))
            task_arg['task_type'] = task_type
            task_arg.update(task_dict)

            task = PythonOperator(
                task_id='{}_task{}'.format(task_type, idx + 1),
                python_callable=batch_main,
                op_kwargs=task_arg,
                provide_context=True,
                trigger_rule="all_success",
                dag=dag
            )
            task_list.append(task)

            retry_task_arg = task_arg.copy()
            retry_task_arg.update({'retry': 1})
            retry_task = PythonOperator(
                task_id='{}_task{}_retry'.format(task_type, idx + 1),
                python_callable=batch_main,
                op_kwargs=retry_task_arg,
                provide_context=True,
                trigger_rule="all_success",
                dag=dag
            )
            retry_task_list.append(retry_task)
            retry_task.set_upstream(task)

    octo_login.set_downstream(task_list)
"""