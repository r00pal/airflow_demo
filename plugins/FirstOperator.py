#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 29 14:05:21 2017

@author: rjain11
"""

import logging

from datetime import datetime
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from IPython import embed
from airflow.operators.sensors import BaseSensorOperator

log = logging.getLogger(__name__)

#user_defined_operator
class MyFirstOperator(BaseOperator):
    
    @apply_defaults
    def __init__(self, my_operator_param, *args, **kwargs):
        self.operator_param =my_operator_param
        super(MyFirstOperator, self).__init__(*args, **kwargs)
    
    def execute(self, context):
        log.info("Hello World")
        embed()
        log.info('operator_param: %s', self.operator_param)

#user_defined_sensor

class MyFirstSensor(BaseSensorOperator):
    
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(MyFirstSensor, self).__init__(*args, *kwargs)
    
    def poke(self, context):
        current_minute = datetime.now().minute
        if current_minute %3!=0:
            log.info("Current minute (%s) is not divisible by 3, sensor will retry", current_minute)
            return False;
        log.info("Current minute (%s) is divisible by 3,sesor finishing", current_minute )
        return True;
        
            
class MyFirstPlugin(AirflowPlugin):
    name = "my_first_plugin"
    operators = [MyFirstOperator,MyFirstSensor]