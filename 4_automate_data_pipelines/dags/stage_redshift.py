from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    """
    - Load any json-formatted file from s3 to redshift
    - runs sql COPY statement
    - params: source, target table, specific json file, time dependant loads§§1

    """
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

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        self.json_path = json_path

    def execute(self, context):
        self.log.info('Copy data from s3 to redshift')
        aws_hook = AwsHook(self.aws_credentials_id)
        aws_credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        s3_path = f"s3://{self.s3_bucket}/{self.s3_key.format(**context)}"
        redshift_hook.run(f"COPY {self.table} FROM '{s3_path}' ACCESS_KEY_ID '{aws_credentials.access_key}' \
            SECRET_ACCESS_KEY '{aws_credentials.secret_key}' FORMAT AS JSON '{self.json_path}'")




