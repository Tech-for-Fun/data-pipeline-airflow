from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
 
    copy_query = " COPY {} \
                   FROM '{}' \
                   ACCESS_KEY_ID '{}' \
                   SECRET_ACCESS_KEY '{}' \
                   FORMAT AS JSON '{}'; \ "
  

    @apply_defaults
    def __init__(self,
                 # Define operators params (with defaults)
                 redshift_conn_id="",
                 aws_credential_id="",
                 table = "",
                 s3_bucket="",
                 s3_key = "",
                 file_format = "",
                 json_file = "auto",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params
        self.redshift_conn_id = redshift_conn_id
        self.aws_credential_id = aws_credential_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.file_format = file_format
        self.json_file = json_file
        self.execution_date = kwargs.get('execution_date')   
 

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credential_id)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
 
        self.log.info("Copying data from S3 to Redshift")
        s3_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        
        copy_query = self.copy_query.format(self.table, 
                                            s3_path, 
                                            credentials.access_key, 
                                            credentials.secret_key, 
                                            self.json_file)             
        
        self.log.info(f"Running copy query : {copy_query}")
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        redshift_hook.run(copy_query)
        self.log.info(f"Table {self.table} staged successfully!!")





