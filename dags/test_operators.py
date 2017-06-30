#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 29 13:42:05 2017

@author: rjain11
"""

from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
#from airflow.operators.python_operator import PythonOperator
from airflow.operators import MyFirstOperator
from airflow.operators import MyFirstSensor


default_args = {
        'owner':'roopal',
        'depends_on_past' : False,
        'start_date' : datetime.today(),
        'retries' : 1,
        'retry_delay' : timedelta(minutes=1),
        'catch_up' : False,
        'description' : 'Writing dag to test my_first_operator'
        }

dag = DAG('my_test_dag', default_args = default_args)

#dummy operator is used to group the tasks in dag
dummy_operator =  DummyOperator(
                    task_id = 'dummy_task',
                    dag =dag)

''' my_sensor_task = MyFirstSensor(
                task_id = 'sensor_task',
                poke_interval =30,
                dag=dag) '''

operator_task =  MyFirstOperator(
                    my_operator_param = 'This is a test',
                    task_id = 'my_first_operator_task',
                    dag =dag)

dummy_operator  >> operator_task