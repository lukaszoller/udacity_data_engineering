from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift_conn_id",
                 table_name="table_name",
                 query="query",
                 *args, **kwargs):
        """
        :param conn_id: The connection ID to the database.
        :param table_name: The target fact table name.
        :param query: The SQL query to select and insert data into the target table.
        """
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.query = query

    def execute(self, context):
        """
        Runs data transformations to load fact tables in Redshift.
        """
        self.log.info('LoadFactOperator starting')
        try:
            redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
            redshift.run(f"INSERT INTO {self.table_name} {self.query}")
            self.log.info('LoadFactOperator successfully run')
        except Exception as e:
            self.log.error(f'LoadFactOperator error: {str(e)}')
