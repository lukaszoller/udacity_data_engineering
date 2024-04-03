from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):

    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 aws_credentials_id,
                 table,
                 s3_bucket,
                 s3_key,
                 json_path,
                 *args, **kwargs):
        """
        :param redshift_conn_id: The connection ID to the Redshift database.
        :param aws_credentials_id: The connection ID to AWS credentials.
        :param table: The target Redshift table to copy the data into.
        :param s3_bucket: The S3 bucket where the JSON data resides.
        :param s3_key: The S3 key prefix for the JSON data files.
        :param json_path: The JSON path file for defining the structure of the JSON data.
        """
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        self.json_path = json_path

    def execute(self, context):
        """
        Copies JSON-formatted data from Amazon S3 to Amazon Redshift.
        """
        self.log.info('Copying data from S3 to Redshift')
        try:
            aws_hook = AwsHook(self.aws_credentials_id)
            aws_credentials = aws_hook.get_credentials()
            redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
            s3_path = f"s3://{self.s3_bucket}/{self.s3_key.format(**context)}"
            sql_query = f"""
                COPY {self.table}
                FROM '{s3_path}'
                ACCESS_KEY_ID '{aws_credentials.access_key}'
                SECRET_ACCESS_KEY '{aws_credentials.secret_key}'
                FORMAT AS JSON '{self.json_path}'
            """
            redshift_hook.run(sql_query)
            self.log.info('Data successfully copied to Redshift')
        except Exception as e:
            self.log.error(f'Error copying data to Redshift: {str(e)}')
