#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun 30 09:59:32 2017

@author: rjain11
"""

from airflow import DAG
from airflow.operators import PythonOperator
from datetime import datetime,timedelta
from pyspark import SparkContext
import logging


def run_spark(**kwargs):
    sc = SparkContext()
    df = sc.textFile("data.txt")
    logging.info('Number of lines : {0}'.format(df.count()))
    sc.stop()

default_args = {
        'owner':'roopal',
        'depends_on_past' : False,
        'start_date' : datetime.today(),
        'retries' : 1,
        'retry_delay' : timedelta(minutes=1),
        'catch_up' : False
        }

dag = DAG('Pyspark_tutorial', default_args = default_args)

t_main =  PythonOperator(
        task_id='call_spark',
        dag = dag,
        python_callable =run_spark)
