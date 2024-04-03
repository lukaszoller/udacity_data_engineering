from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "redshift_conn_id",
                 table_name = "table_name",
                 query = "query",
                 truncate = False,
                 *args, **kwargs):
        """
        :param conn_id: The connection ID to the database.
        :param table_name: The target dimension table name.
        :param query: The SQL query to select and insert data into the target table.
        :param truncate: If True, the target table will be truncated before inserting new data.
        """

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.query = query
        self.truncate = truncate

    def execute(self, context):
        """
        Runs data transformations to load dimension tables in Redshift.
        """
        self.log.info('LoadDimensionOperator starting')
        try:
            redshift = PostgresHook(postgres_conn_id=redshift_conn_id)
            if self.truncate:
                redshift.run(f"TRUNCATE TABLE {self.table_name}")
            redshift.run(f"INSERT INTO {self.table_name} {self.query}")
            self.log.info('LoadFactOperator successfully run')
        except Exception as e:
            self.log.error(f'LoadDimensionOperator error: {str(e)}')




