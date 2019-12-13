from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    copy_sql_code = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        {};
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 eparameters="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.eparameters = eparameters


    def execute(self, context):
        #https://airflow.readthedocs.io/en/stable/_modules/airflow/contrib/hooks/aws_hook.html
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Deleting data from destination dimension Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data in S3 into Redshift tables")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        # Dynamically 
        formatted_sql = StageToRedshiftOperator.copy_sql_code.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.eparameters
        )
        self.log.info(f"Running {formatted_sql} ...")
        redshift.run(formatted_sql)
        
