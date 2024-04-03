from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 tables,
                 *args, **kwargs):
        """
        :param redshift_conn_id: The connection ID to Redshift.
        :param tables: A list of tables to perform data quality checks on.
        """
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        """
        For each table in the provided list, it checks if there are any records and logs the result.
        """
        try:
            redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
            for table in self.tables:
                records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
                if len(records) < 1 or len(records[0]) < 1:
                    raise ValueError(f"Data quality check failed. {table} returned no results")
                num_records = records[0][0]
                if num_records < 1:
                    raise ValueError(f"Data quality check failed. {table} contained 0 rows")
                self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records")
            self.log.info('DataQualityOperator successfully completed')
        except Exception as e:
            self.log.error(f'DataQualityOperator error: {str(e)}')
