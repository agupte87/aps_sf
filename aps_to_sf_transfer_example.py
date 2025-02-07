import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
from snowflake.snowpark import Session
import time
from timeit import default_timer as timer
import sys
from tabulate import tabulate
import numpy as np
import pyarrow.parquet as pq
import pyarrow as pa
import config
from config import * 
import os
from snowflake.connector.pandas_tools import write_pandas
from snowflake.connector.errors import ProgrammingError



import argparse


def createDDLFromAPSToSFTable (env,db_type, schemaID, aps_tableName, table_on_sf_to_load):
    # print(schemaID)
    # print(tableName)
    if ( env == 'dev'):
        sf_db = sf_dev_aps2_mig_ice_sdb


    if ( env == 'uacc'):
        sf_db = sf_uacc_aps2_mig_ice_sdb


    if ( env == 'prod'):
        sf_db = sf_prod_aps2_mig_ice_sdb

    if ( db_type == 'lad'):
        aps_db = config.aps2_laad_2_batch_db
    if ( db_type == 'cnfg'):
        aps_db = config.aps2_laad_cnfg_batch_db
    aps_conn, aps_cur = config.getApsCon(aps_db)

    createTempTable_qry = f"""SELECT 
                                COLUMN_NAME + ' ' +
                                 CASE 
                                    WHEN UPPER(DATA_TYPE) IN ('FLOAT') AND NUMERIC_PRECISION IS NOT NULL 
                                        THEN UPPER(DATA_TYPE) 
                                    WHEN UPPER(DATA_TYPE) IN ('DECIMAL') AND NUMERIC_PRECISION IS NOT NULL 
                                        THEN 'NUMBER'  + '(' + CAST(NUMERIC_PRECISION AS NVARCHAR) + ',' + CAST(NUMERIC_SCALE AS NVARCHAR) +  ')'
                                    WHEN UPPER(DATA_TYPE) IN ( 'NUMERIC') AND NUMERIC_PRECISION IS NOT NULL 
                                        THEN 'NUMBER' 
                                    WHEN UPPER(DATA_TYPE) IN ('BIGINT') AND NUMERIC_PRECISION IS NOT NULL 
                                        THEN 'NUMBER(38,0)'  
                                    WHEN UPPER(DATA_TYPE) IN ('DECIMAL') AND NUMERIC_PRECISION IS NOT NULL 
                                        THEN UPPER(DATA_TYPE) + '(' + CAST(NUMERIC_PRECISION AS NVARCHAR) + ')'
                                    WHEN UPPER(DATA_TYPE) IN ('CHAR', 'VARCHAR') AND CHARACTER_MAXIMUM_LENGTH IS NOT NULL 
                                        THEN UPPER(DATA_TYPE) + '(' + CAST(CHARACTER_MAXIMUM_LENGTH AS NVARCHAR) + ')'
                                    WHEN UPPER(DATA_TYPE) IN ('NCHAR') AND CHARACTER_MAXIMUM_LENGTH IS NOT NULL 
                                        THEN 'CHAR' + '(' + CAST(CHARACTER_MAXIMUM_LENGTH AS NVARCHAR) + ')'
                                    WHEN UPPER(DATA_TYPE) IN ('NVARCHAR') AND CHARACTER_MAXIMUM_LENGTH IS NOT NULL 
                                        THEN 'VARCHAR' + '(' + CAST(CHARACTER_MAXIMUM_LENGTH AS NVARCHAR) + ')'
                                    ELSE UPPER(DATA_TYPE)
                                END +
                                CASE 
                                    WHEN IS_NULLABLE = 'NO' THEN ' NOT NULL'
                                    ELSE ' NULL'
                                END AS  DDL,
                                ORDINAL_POSITION,
                                ROWNUM
                            FROM 
                                (SELECT 
                                                ROW_NUMBER() OVER (ORDER BY ORDINAL_POSITION) AS RowNum,
                                                COLUMN_NAME,
                                                DATA_TYPE,
                                                CHARACTER_MAXIMUM_LENGTH,
                                                NUMERIC_PRECISION,
                                                NUMERIC_SCALE,
                                                IS_NULLABLE,
                                                ORDINAL_POSITION
                                            FROM INFORMATION_SCHEMA.COLUMNS
                                            WHERE TABLE_SCHEMA = '{schemaID}'AND TABLE_NAME = '{aps_tableName}'	) columns
                            ORDER BY 
                                ORDINAL_POSITION;"""
    # print(createTempTable_qry)
    createTempTabledf = pd.read_sql(createTempTable_qry, aps_conn)
    # print(createTempTabledf)
    # print(tabulate(createTempTabledf, headers='keys', tablefmt='psql'))
    ddl = 'CREATE OR REPLACE TABLE ' +  table_on_sf_to_load + ' ( n'  ;
    columnDefinition = ''
    for index, row in createTempTabledf.iterrows():
        columnDefinition = columnDefinition + row['DDL'] + ', n'  ;
        # print(columnDefinition)

    ddl= ddl + '    ' +columnDefinition 
    ddl = ddl[:len(ddl)-3] + 'n);'; 
    return ddl

def direct_table_transfer(env, data):
    db_type = data['db_type']
    create_table_qry = data["create_table_qry"]
    partition_column = data["partition_column"]
    schemaID = data["schemaID"]
    aps_table = data["aps_table"]
    table_on_sf_to_load = data["table_on_sf_to_load"]

    if ( db_type == 'lad'):
        aps_db = config.aps2_laad_2_batch_db
    if ( db_type == 'cnfg'):
        aps_db = config.aps2_laad_cnfg_batch_db

    aps_conn, aps_cur = config.getApsCon(aps_db)
    if ( env == 'dev'):
            sf_db = config.sf_dev_aps2_mig_ice_sdb


    if ( env == 'uacc'):
            sf_db = config.sf_uacc_aps2_mig_ice_sdb


    if ( env == 'prod'):
            sf_db = config.sf_dev_aps2_mig_ice_sdb# get details from SP

    sf_conn, sf_engine, sf_cur , = config.getSfCon_alt(sf_db)

    wrh_qry = f"""USE WAREHOUSE {sf_db["warehouse"]};"""
    
    sf_conn.cursor().execute(wrh_qry)
    # print(create_table_qry)
    sf_conn.cursor().execute(create_table_qry)

    aps_qry_get_partitions_list = f"select distinct {partition_column} from  {aps_table} "
    partitionsdf = pd.read_sql(aps_qry_get_partitions_list, aps_conn)
 
    print("Process start time:", time.strftime("%H:%M:%S"))
    all_start = timer()
    chunksize = 10000  # Adjust chunk size as needed
    for index, row in partitionsdf.iterrows():
        col_name = partition_column
        col = row[col_name] 
        # print(col_name)
        # print(col)
        subquery = f" select * from {aps_table} where {partition_column} = {col} " 
        # print(subquery)
        aps_sub_conn,aps_sub_cur = config.getApsCon(aps_db)   
    
        for chunk in pd.read_sql(subquery, aps_sub_conn, chunksize=chunksize):
            chunk.columns = [col.upper() for col in chunk.columns]
            # print(tabulate(chunk.head(5), headers='keys', tablefmt='psql'))
            start = timer()  
            write_pandas(conn = sf_conn, df = chunk,overwrite=False, auto_create_table=False, table_name= f'{table_on_sf_to_load}',database=f'{sf_db["database"]}',schema=f'{sf_db["schema"]}')    
            end = timer()
            print(f'Time to transfer data for {partition_column} {col} in seconds: {end - start}')

        #closing aps_conn
        aps_sub_conn.commit()
        aps_sub_cur.close()
        aps_sub_conn.close()

    all_end = timer()
    print(f'Total time to transfer data to sf in seconds {all_end - all_start}')

    aps_conn.commit()
    aps_cur.close()
    aps_conn.close()


def parquet_file_creation_and_upload( data):
    db_type = data['db_type']
    partition_number = data["partition_number"]
    partition_column = data["partition_column"]
    schemaID = data["schemaID"]
    aps_table = data["aps_table"]
    table_on_sf_to_load = data["table_on_sf_to_load"]

     # Determine APS database
    if db_type == 'lad':
        aps_db = config.aps2_laad_2_batch_db
    elif db_type == 'cnfg':
        aps_db = config.aps2_laad_cnfg_batch_db

    aps_conn, aps_cur = config.getApsCon(aps_db)

    # Determine Snowflake connection
    if env == 'dev':
        sf_db = config.sf_dev_aps2_mig_ice_sdb
    elif env == 'uacc':
        sf_db = config.sf_uacc_aps2_mig_ice_sdb
    elif env == 'prod':
        sf_db = config.sf_prod_aps2_mig_ice_sdb

    sf_conn, sf_engine, sf_cur = config.getSfCon_alt(sf_db)
      # Use Snowflake stage
    try:
        sf_cur.execute(f"DROP STAGE IF EXISTS {table_on_sf_to_load}")
        sf_cur.execute(f"CREATE STAGE IF NOT EXISTS {table_on_sf_to_load}")
        print("Snowflake stage created (if not already existing).")
    except ProgrammingError as e:
        print(f"Error creating Snowflake stage: {e}")
        return

    aps_conn, aps_cur = config.getApsCon(aps_db)
    # getting modulus range to create a hash value 
    range = np.arange(partition_number)
    all_start = timer()
    print('Process start time ' )
    start_time = time.time()
    
    # Process and upload each chunk
    for modulus_value in range:
        query = f"""
            SELECT * 
            FROM {schemaID}.{aps_table}
            WHERE {partition_column}%{partition_number} = {modulus_value}
        """
        print(f"Processing modulus partition {modulus_value}...")

        # Process chunks for the current partition
        chunk_size = 100000  # Number of rows per chunk
        chunk_iter = pd.read_sql(query, aps_conn, chunksize=chunk_size)
        base_path = config.base_path  # Base directory where files are located
        print(f"Searching for files in path: {base_path} containing schemaID: {schemaID}")
    
        for i, chunk in enumerate(chunk_iter):
            if chunk.empty:
                continue

            # # Convert chunk to PyArrow Table
            table = pa.Table.from_pandas(chunk)

            # # Write to Parquet file (temp path)
            parquet_file_path = f"{base_path}{table_on_sf_to_load}_{modulus_value}_part{i}.parquet"
            pq.write_table(table, parquet_file_path)

            # # Upload the parquet file to Snowflake stage
            try:
                upload_query = f"PUT file://{parquet_file_path} @{table_on_sf_to_load} AUTO_COMPRESS=TRUE;"
                file_path = str(parquet_file_path).replace("//", "//")  # Escape backslashes for Snowflake
                sf_cur.execute(upload_query)
                print(f"Uploaded {parquet_file_path} to Snowflake stage.")
                 # Copy from Snowflake stage into the target table for the current partition
                execute_qry = f"""
                            COPY INTO {table_on_sf_to_load}
                                FROM @{table_on_sf_to_load}/{table_on_sf_to_load}_{modulus_value}_part{i}.parquet
                                FILE_FORMAT = (TYPE = 'PARQUET')
                                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
                        """
                # Data copy try catch
                try:
                    sf_cur.execute(execute_qry)
                    print(f"Data copied from stage to table {table_on_sf_to_load}.")
                except Exception as e:
                    print(f"Error during COPY INTO: {e}")
                
                # Data file deletion try catch
                try:
                    print(f"Deleted copied from parquet file from snowflake to re-appropriate space.")
                    if os.path.exists(parquet_file_path):
                        os.remove(parquet_file_path)
                        print(f"File '{parquet_file_path}' deleted successfully.")
                    else:
                        print(f"File '{parquet_file_path}' does not exist.")
                except Exception as e:
                    print(f"Error during parquet chunk file deletion : {e}")

                        
                    
            except ProgrammingError as e:
                print(f"Error uploading file {parquet_file_path}: {e}")
                continue


    all_end = timer()
    print( ' Time process started at :' )
    print(start_time)
    print(f'Total time to create parquet files and load data for {schemaID}.{table_on_sf_to_load} - {all_end - all_start} seconds')
    sf_cur.execute(f"DROP STAGE IF EXISTS {table_on_sf_to_load}")

    aps_conn.commit()
    aps_cur.close()
    aps_conn.close()
    print('Process end time ' )
    print(time.time())

  
def main(env, db_type , partition_column , schemaID , aps_table):
    # print(db_type)
    if ( db_type == 'lad'):
        aps_db = config.aps2_laad_2_batch_db
    if ( db_type == 'cnfg'):
        aps_db = config.aps2_laad_cnfg_batch_db

    # print(aps_db)
    aps_conn, aps_cur, aps_engine = config.getApsCon_withEngine(aps_db)
    if ( env == 'dev'):
            sf_db = config.sf_dev_aps2_mig_ice_sdb


    if ( env == 'uacc'):
            sf_db = config.sf_uacc_aps2_mig_ice_sdb


    if ( env == 'prod'):
            sf_db = config.sf_dev_aps2_mig_ice_sdb# get details from SP

    sf_conn, sf_engine, sf_cur , = config.getSfCon_alt(sf_db)

    wrh_qry = f"""USE WAREHOUSE {sf_db["warehouse"]};"""
    
    sf_conn.cursor().execute(wrh_qry)
    table_on_sf_to_load = ''
    if aps_table.startswith('LAAD_'):
        tblnm = aps_table[5:len(aps_table)] 
        table_on_sf_to_load = 'LAD' + '_' + schemaID + '_'+ tblnm
    # print(table_on_sf_to_load)
    create_table_ddl = createDDLFromAPSToSFTable(env, db_type, schemaID, aps_table,table_on_sf_to_load)
    # print(create_table_ddl)
    sf_conn.cursor().execute(create_table_ddl)

    getObjectTypeQry = f"""     
                   SELECT  
                        CASE 
                            WHEN O.TYPE = 'U' THEN 'TABLE'
                            WHEN O.TYPE = 'V' THEN 'VIEW'
                            ELSE NULL
                        END as OBJECTTYPE 
                    FROM SYS.OBJECTS O
                    JOIN SYS.SCHEMAS S ON O.SCHEMA_ID = S.SCHEMA_ID
                    WHERE S.NAME =  '{schemaID}' AND O.NAME = '{aps_table}'
            """
    getObjectType_df = pd.read_sql(getObjectTypeQry, aps_conn )
    if getObjectType_df.empty:
        print(f" Table {schemaID}.{aps_table} is not neither table nor view")
        return
    else:
        for index, row in getObjectType_df.iterrows():
            objectType = row["OBJECTTYPE"]
            print('Transferring a '+ objectType)
        
        if objectType == 'TABLE':
            #gathering an approximate rowcount for the patient list market id table for the specific schemaID to 
            # decide the number of parquet files to create
            rowcount_qry = f"""     
                            SELECT SUM(part.rows) AS ROW_COUNT
                                ,t.name
                                ,sch.name
                            FROM sys.objects t
                            JOIN sys.partitions part ON t.object_id = part.object_id
                            JOIN sys.schemas sch ON sch.schema_Id = t.schema_Id
                            WHERE sch.name = '{schemaID}'
                                and t.name like '%{aps_table}'
                            GROUP BY t.name
                                ,sch.name
                    """
            # print(rowcount_qry)
            rowcount_df = pd.read_sql(rowcount_qry, aps_conn )
            if rowcount_df.empty:
                print(f" Some issue with {schemaID}.{aps_table}. Check why the rowcount of the table is not coming up")
                return
            else:
                for index, row in rowcount_df.iterrows():
                    rowcount = row["ROW_COUNT"]
                    print(f'The row count of {schemaID}.{aps_table} is {rowcount}')
        if  objectType == 'VIEW':
            rowcount_qry = f"""
                                select count_big(*) as ROW_COUNT from  {schemaID}.{aps_table}
                            """  
            rowcount_df = pd.read_sql(rowcount_qry, aps_conn )
            if rowcount_df.empty:
                print(f" Some issue with {schemaID}.{aps_table}. Check why the rowcount of the view is not coming up")
                return
            else:
                for index, row in rowcount_df.iterrows():
                    rowcount = row["ROW_COUNT"]  
                    print(f'The row count of {schemaID}.{aps_table} is {rowcount}')


        if rowcount >= 1000000:
            print('Rowcount of table qualifies to create parquet files for data transfer')
            partition_number = round(rowcount /1000000 )  
            #If table is larger than 1million rows then we proceed to create parquet files
            pq_file_creation_data = {
                
                        "db_type":db_type,
                        "partition_number" :partition_number,                
                        "partition_column" :partition_column,                
                        "schemaID" :schemaID,  
                        "aps_table" :aps_table,
                        "table_on_sf_to_load" :table_on_sf_to_load              
            }

            parquet_file_creation_and_upload(pq_file_creation_data)
        else:
            #If table is smaller than 1million rows then we do a direct transfer 
            print('Rowcount of table qualifies to directly transfer data to Snowflake')
            direct_table_transfer(env,db_type,create_table_ddl,partition_column,aps_table,table_on_sf_to_load)
                    
# W15267.LAAD_MX_FACT_PHYS
# W15267.LAAD_RX_FACT_VIEW
##### manual call from python file
# env='dev'
# db_type = 'cnfg'
# partition_column = 'WEEK_ID'
# schemaID='M12206'
# aps_table = f'LAAD_MX_FACT_PHYS_RANK'

# main(env, db_type,partition_column,schemaID,aps_table)


##### call from data_transfer_caller.sh file
if __name__ == "__main__":
    # Get the arguments passed from the command line
    # print('hello')
    arguments = sys.argv[1:]  # Exclude the script name itself (sys.argv[0])

    env = sys.argv[1] #eg:'dev'
    db_type = sys.argv[2] #eg: 'cnfg'
    partition_column = sys.argv[3]  #eg:'WEEK_ID' 
    schemaID = sys.argv[4] #eg:'M12206' 
    aps_table = sys.argv[5]  #eg: f'LAAD_MX_FACT_PHYS_RANK'

    # env = 'uacc'
    # db_type = 'cnfg'
    # partition_column = 'WEEK_ID' 
    # schemaID = 'W15267' 
    # aps_table =f'LAAD_MX_FACT_PHYS'

    main(env, db_type , partition_column , schemaID , aps_table)
      
# W15267.LAAD_MX_FACT_PHYS
# W15267.LAAD_RX_FACT_VIEW
# code call from .sh File
# Use bash terminal
# cd /d/Git/new/com.rxcorp.lad.apssfmdt/python/aps/DataMovementAPSToSF
# ./data_transfer_caller.sh uacc cnfg WEEK_ID W15267 LAAD_MX_FACT_PHYS
# ./data_transfer_caller.sh uacc cnfg WEEK_ID W15267 LAAD_RX_FACT_VIEW}
