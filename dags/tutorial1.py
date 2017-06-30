#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 29 09:33:47 2017

@author: rjain11
"""

from airflow import DAG
from airflow.operators import BashOperator  #base class for all operators
from datetime import datetime, timedelta

default_args = {
    'owner' : 'roopal',
    'depends_on_past' : False,
    'start_date' : datetime.today(),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}


dag = DAG('HelloWorld', default_args=default_args)

t1 = BashOperator(
    task_id = 'task_1',
    bash_command = 'echo "task 1" ',
    dag = dag)

t2 = BashOperator(
    task_id = 'task_2',
    bash_command = 'echo "Hello from task 2" ',
    dag = dag)

t3 = BashOperator(
    task_id = 'task_3',
    bash_command = 'echo "Hello from task 3" ',
    dag = dag)

t4 = BashOperator(
    task_id = 'task_4',
    bash_command = 'echo "Hello from task 4" ',
    dag = dag)

t2.set_upstream(t1);
t3.set_upstream(t1);
t4.set_upstream(t2);
t4.set_upstream(t3);