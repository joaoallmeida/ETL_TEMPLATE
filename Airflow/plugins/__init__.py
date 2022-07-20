from __future__ import division, absolute_import, print_function
from airflow.plugins_manager import AirflowPlugin

from operators.create import runSql
from operators.extract import extractRawData 
from operators.refined import refinedData 
from operators.load import starSchemaModel 

class EtlPlugin(AirflowPlugin):

    name = 'etl_plugin'

    operators = [
        runSql,
        extractRawData,
        refinedData,
        starSchemaModel
    ]
    
    hooks = []
    executors = []
    macros = []
    admin_views	= []
    flask_blueprints = []
    menu_links = []
    appbuilder_views = []
    appbuilder_menu_items =	[]
    global_operator_extra_links = []
    operator_extra_links = []