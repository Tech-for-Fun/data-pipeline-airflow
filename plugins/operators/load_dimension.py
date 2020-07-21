from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define operators params (with defaults)
                 redshift_conn_id="",
                 select_query = "",
                 delete_load = False,
                 table = "",
                 truncate_table=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params
        self.redshift_conn_id = redshift_conn_id
        self.select_query = select_query
        self.table_name = table
        self.delete_load = delete_load

    def execute(self, context):
        redshift = PostgreHook
        if self.truncate_table:
            self.log.info("Will truncate table before inserting new data...")
            redshift.run(LoadDimensionOperator.truncate_stmt.format(
                table=self.table))

        self.log.info(f"Running query to load data into Dimension Table {self.table}")
        redshift.run(LoadDimensionOperator.insert_into_stmt.format(
            table=self.table,
            select_query=self.select_query))        
        self.log.info(f"Dimension Table {self.table} loaded.")

        
        