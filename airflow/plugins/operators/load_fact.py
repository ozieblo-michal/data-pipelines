from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    """
    Loads fact table in Redshift from data in staging tables.
    
    Parameters:
    redshift_conn_id -> Redshift connection ID
    select_sql -> SQL query for getting data to load into target table
    table -> target table in Redshift to load
    """
    
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 select_sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_sql = select_sql

    def execute(self, context):

        self.log.info("Getting credentials")
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Loading data into fact table in Redshift")
        
        table_insert_sql = f"""
            INSERT INTO {self.table}
            {self.select_sql}
        """
        redshift_hook.run(table_insert_sql)