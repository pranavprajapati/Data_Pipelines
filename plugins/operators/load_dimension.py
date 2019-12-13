from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    insert_sql = """
        INSERT INTO {}
        {};
        """
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table = "",
                 sql_code = "",
                 delete_parameter = "",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_code = sql_code
        self.delete_parameter = delete_parameter

    def execute(self, context):
        self.log.info('LoadDimensionOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.delete_parameter:
            self.log.info("Deleting data from destination dimension Redshift table")
            redshift.run("DELETE FROM {}".format(self.table))

        formatted_sql = LoadDimensionOperator.insert_sql.format(
            self.table,
            self.sql_code
        )
        self.log.info(f"Executing {formatted_sql} ...")
        redshift.run(formatted_sql)
