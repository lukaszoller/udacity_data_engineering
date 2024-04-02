import sys
sys.path.append(' C:/Users/lukas/PycharmProjects/udacity/udacity_data_engineering/4_automate_data_pipelines/dags/')

from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from plugins.operators import (StageToRedshiftOperator, LoadFactOperator,
                       LoadDimensionOperator, DataQualityOperator)

default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
    'depends_on_past': False,
    'retries': 3,
    'catchup': False,
    'email_on_retry': False
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)
def final_project_new():

    start_operator = DummyOperator(task_id='Begin_execution')
    end_operator = DummyOperator(task_id='End_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id = 'conn_id',
        table = 'table',
        s3_bucket = 's3_bucket',
        s3_key = 's3_key',
        aws_credentials_id = 'aws_credentials_id',
        json_path = 'json_path'
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id = 'conn_id',
        table = 'table',
        s3_bucket = 's3_bucket',
        s3_key = 's3_key',
        aws_credentials_id = 'aws_credentials_id',
        json_path = 'json_path'
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table'
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table'
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table'
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table'
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table'
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks'
    )

    # Define task dependencies
    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]
    stage_events_to_redshift >> load_songplays_table
    stage_songs_to_redshift >> load_songplays_table
    load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
    run_quality_checks >> end_operator

final_project_new_dag = final_project_new()
