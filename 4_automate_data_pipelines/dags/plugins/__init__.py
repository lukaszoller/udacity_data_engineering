from __future__ import division, absolute_import, print_function
import sys
sys.path.append(' C:/Users/lukas/PycharmProjects/udacity/udacity_data_engineering/4_automate_data_pipelines/dags/')


from airflow.plugins_manager import AirflowPlugin
import operators
import helpers

# Defining the plugin class
class UdacityPlugin(AirflowPlugin):
    name = "udacity_plugin"
    operators = [
        operators.StageToRedshiftOperator,
        operators.LoadFactOperator,
        operators.LoadDimensionOperator,
        operators.DataQualityOperator
    ]
    helpers = [
        helpers.SqlQueries
    ]
