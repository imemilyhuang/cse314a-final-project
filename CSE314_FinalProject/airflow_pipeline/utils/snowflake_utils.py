import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

def upload_dataframe_to_snowflake(df, table_name):
    conn = snowflake.connector.connect(
        user="MALLARD",
        password="L1lysunsk@ter@gmail.com",
        account="IOB55870",
        warehouse="MALLARD_WH",
        database="MALLARD_DB",
        schema="FINAL_PROJECT"
    )
    try:
        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=table_name,
            auto_create_table=True
        )
        if success:
            print(f"Uploaded {nrows} rows to Snowflake table {table_name}.")
        else:
            print(f"Upload to {table_name} failed.")
    finally:
        conn.close()
