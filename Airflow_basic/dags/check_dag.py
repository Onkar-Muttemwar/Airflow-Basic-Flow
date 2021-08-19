from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 1),
}

dag = DAG(
    dag_id = 'check_dag',
    default_args=default_args,
    description='Creating a Simple DAG',
    schedule_interval = None,
    catchup = False
)

#
def Function1(**context):
    print("This is the first function")
    var_value = Variable.get('not default function 1 value', default_var='Function 1 value')
    context['ti'].xcom_push(key='key1', value = var_value)



task1 = PythonOperator(
    task_id='dag1',
    python_callable=Function1,
    provide_context=True,
    dag=dag
)
# #
def Function2(**context):
    print("This is the second function")
    context['ti'].xcom_push(key='key2', value = 'Function 2 value')


task2 = PythonOperator(
    task_id='dag2',
    python_callable=Function2,
    provide_context=True,
    dag=dag
)

def Function3(**context):
    value1 = context['ti'].xcom_pull(key="key1")
    value2 = context['ti'].xcom_pull(key="key2")
    print("The value of function 1 is:{} and function 2 is:{}".format(value1,value2))

#
task3 = PythonOperator(
    task_id='dag3',
    python_callable= Function3,
    provide_context=True,
    dag=dag
)
#
task1 >> task2 >> task3