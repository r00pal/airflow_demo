#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 29 13:42:05 2017

@author: rjain11
"""

from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

def print_hello():
    return 'Hello World';

default_args = {
        'owner':'roopal',
        'depends_on_past' : False,
        'start_date' : datetime.today(),
        'retries' : 1,
        'retry_delay' : timedelta(minutes=1),
        'catch_up' : False
        }

dag = DAG('Tutorial2', default_args = default_args)

#dummy operator is used to group the tasks in dag
dummy_operator =  DummyOperator(
                    task_id = 'dummy_task',
                    dag =dag)

hello_operator =  PythonOperator(
                    task_id = 'hello_task',
                    python_callable = print_hello,     #calls a python function
                    dag =dag)

dummy_operator >> hello_operator