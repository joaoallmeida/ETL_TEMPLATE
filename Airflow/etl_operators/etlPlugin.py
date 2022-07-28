from __future__ import division, absolute_import, print_function
from airflow.plugins_manager import AirflowPlugin

from etl_operators.create import Create
from etl_operators.extract import Extract 
from etl_operators.refined import Refined 
from etl_operators.load import Load 
from etl_operators.dataQuality import DataQuality

class EtlPlugin(AirflowPlugin):

    name = 'etl_plugin'

    operators = [
        Create,
        Extract,
        Refined,
        Load,
        DataQuality
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