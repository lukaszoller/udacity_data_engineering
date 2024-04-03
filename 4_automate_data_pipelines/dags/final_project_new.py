import sys

sys.path.append(' C:/Users/lukas/PycharmProjects/udacity/udacity_data_engineering/4_automate_data_pipelines/dags/')

import pendulum
from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator

from load_dimension import LoadDimensionOperator
from load_fact import LoadFactOperator
from stage_redshift import StageToRedshiftOperator
from data_quality import DataQualityOperator
from sql_queries import SqlQueries

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
        redshift_conn_id='redshift_conn_id',
        table='staging_events',
        aws_credentials_id='aws_credentials_id',
        s3_bucket="udacity-dend",
        s3_key="log_data",
        json_path="s3://udacity-dend/log_json_path.json"
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id='redshift_conn_id',
        table='staging_songs',
        s3_bucket="udacity-dend",
        s3_key="song_data",
        aws_credentials_id='aws_credentials_id',
        json_path='not required'
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id='redshift_conn_id',
        table_name='songplays',
        query=SqlQueries.songplay_table_insert
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id='redshift_conn_id',
        table_name='users',
        query=SqlQueries.user_table_insert,
        truncate=True
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id='redshift_conn_id',
        table_name='songs',
        query=SqlQueries.song_table_insert,
        truncate=True
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id='redshift_conn_id',
        table_name='artists',
        query=SqlQueries.artist_table_insert,
        truncate=True
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id='redshift_conn_id',
        table_name='time',
        query=SqlQueries.time_table_insert,
        truncate=True
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id='redshift_conn_id',
        tables=['songplays', 'users', 'songs', 'artists', 'time']
    )

    # Define task dependencies
    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]
    stage_events_to_redshift >> load_songplays_table
    stage_songs_to_redshift >> load_songplays_table
    load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table,
                             load_time_dimension_table] >> run_quality_checks
    run_quality_checks >> end_operator


final_project_new_dag = final_project_new()
