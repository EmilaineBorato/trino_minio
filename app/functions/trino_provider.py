
import trino
import unicodedata
import pandas as pd
from .format_sql import FormatSql
from .format_data import FormatData




class TrinoProvider:
    def __init__(self):
        self.trino_conn = trino.dbapi.connect(
            host='localhost',
            port=8080,
            user='admin',
        )

    def test_connection(self):

        try: 
            conn = self.trino_conn
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM system.runtime.nodes')

            return cursor.fetchall()
        except Exception as e:
            return Exception('TrinoProvider.test_connection - ', e)


    
    #
    def encode_anscii(self, word):
        """
        Formata uma string para padrão ANSCII, utilizado pelo trino
        Criado para quando criar uma tabela no trino, todas as colunas estejam nesse formato
        """
        new_word = unicodedata.normalize('NFKD', word).encode('ascii', 'ignore').decode('ascii').lower().replace(" ", "_") 

        return new_word
        
    
    def get_columns_attributes(self, p_sql):
        fsql = FormatSql()
        """
        Retorna o tipagem da tabela trino 

        Parameters
        ----------
        p_sql (str): SQl de execução 

        Returns
        -------
        Array com nome das colunas e tipo
        
        """
        try:
            # new_sql = p_sql
            new_sql = fsql.add_limit(p_sql, 1, 'trino')
            conn = self.trino_conn
            cursor = conn.cursor()
            cursor.execute(new_sql)
       
            metadata_success = ''
            metadata_json = []

        
            for row in cursor.description:
                ## Alterado porque a conversao de json para df e vise versa, perde o formato exato do timezone 
                if row[1] == 'timestamp(3) with time zone':
                    new_column_type  = 'timestamp'
                    metadata_success += f"{row[0]}  { new_column_type}, \n"
                    metadata_json.append({"name": row[0], "type": new_column_type, "comment":""})
                else:
                    metadata_success += f"{row[0]}  {row[1]}, \n"
                    metadata_json.append({"name": row[0], "type": row[1], "comment":""})
                
            
            
            """ 
            Remove os 3 ultimos caracteres ('\n', ',', ' ')
            metadata_success[:-3]
            """
            return { 'success': metadata_success[:-3] , 'error': [], 'columns': metadata_json }
        
        except Exception as e:
            return Exception('TrinoProvider.get_columns_attributes - ', e)
   

        
    def get_columns_dataframe_to_columns_trino(self, array_cols):
        try:
            df_cols = pd.DataFrame(array_cols)
            metadata_success = ''
            metadata_error = ''
            metadata_json = []
            
            #iniciando depara
            for index, row in df_cols.iterrows():                
                row['column_name'] = self.encode_anscii(row['column_name'])
                print("NAME: ", row['column_name'], ' TYPE: ', row['column_type'])

                if row['column_type'] == 'int' or row['column_type'] == 'int64' or row['column_type'] == 'int32': 
                    if row.get('length') and int(row.get('length')) >= 10:
                        row['new_column_type'] = 'bigint'
                    else:
                        row['new_column_type'] = 'integer'
                    metadata_success += f"{row['column_name']}  {row['new_column_type']}, \n"
                    metadata_json.append({"name": row['column_name'], "type": row['new_column_type'], "comment":""})
                    
                elif row['column_type'] == 'datetime' or row['column_type'] == "datetime64[ns]" or row['column_type'] == "timestamp":
                    row['new_column_type']  = 'timestamp'
                    metadata_success += f"{row['column_name']}  {row['new_column_type']}, \n"
                    metadata_json.append({"name": row['column_name'], "type": row['new_column_type'], "comment":""})
                
                elif row['column_type'] == 'Timestamp':
                    row['new_column_type']  = 'date'
                    metadata_success += f"{row['column_name']}  {row['new_column_type']}, \n"
                    metadata_json.append({"name": row['column_name'], "type": row['new_column_type'], "comment":""})

                elif row['column_type'] == 'date':
                    row['new_column_type'] =  'date'        
                    metadata_success += f"{row['column_name']}  {row['new_column_type']}, \n"
                    metadata_json.append({"name": row['column_name'], "type": row['new_column_type'], "comment":""})

                    
                elif row['column_type'] == 'bool':
                    row['new_column_type']  = 'boolean'  
                    metadata_success += f"{row['column_name']}  {row['new_column_type']}, \n"
                    metadata_json.append({"name": row['column_name'], "type": row['new_column_type'], "comment":""})
                    
                    
                elif row['column_type'] == 'float' or row['column_type'] == 'float64':
                    row['new_column_type']  = 'double'
                    metadata_success += f"{row['column_name']}  {row['new_column_type']}, \n"
                    metadata_json.append({"name": row['column_name'], "type": row['new_column_type'], "comment":""})
                    
                    
                elif row['column_type'] == 'str' or row['column_type'] == "text" or row['column_type'] == "object":
                    row['new_column_type'] =  'varchar'
                    metadata_success += f"{row['column_name']}  {row['new_column_type']}, \n"
                    metadata_json.append({"name": row['column_name'], "type": row['new_column_type'], "comment":""})
                    
                    
                else:
                    metadata_error += f"{row['column_name']}  { row['column_type']}, \n"
            
            return { 'success': metadata_success[:-3] , 'error': metadata_error[:-3], 'columns': metadata_json }
        except Exception as e:
             return Exception('TrinoProvider.get_columns_dataframe_to_columns_trino - ', e)
    
    def get_tables(self, p_catalog, p_schema):
        
        try:
            conn = self.trino_conn
            cursor = conn.cursor()

            sql = f"""show tables from  {p_catalog}.{p_schema};"""           

            print('\nExecute SQL:', sql)
            cursor.execute(sql)
            return cursor.fetchall()
            
        except Exception as e:
            return Exception('TrinoProvider.get_tables', e)
        
    def get_schemas(self, p_catalog):
        try:
            conn = self.trino_conn
            cursor = conn.cursor()
           
            sql = f"""show schemas from {p_catalog};""" 

            print('\nExecute SQL:', sql)
            cursor.execute(sql)          
            return cursor.fetchall()
            
        except Exception as e:
            return Exception('TrinoProvider.get_schemas - ', e)
        
    def create_schema(self, p_catalog, p_bucket_name, p_schema):
        """
        Função para criar schema
        
        Parameters:
        p_bucket_name (str): Nome do Bucket da aplicacao (ex: test)
        p_catalog (str): Nome do catalago do Trino (ex: Minio)
        p_schema (str): Nome do schema, também pode ser considerado o owner (ex: dev, hml, prd)
        

        Exemplo da chamada 
        >>> create_schema(p_catalog='minio', p_bucket_name='test',  p_schema='dev')
    
        Returns:
        Result sql
        """
        
        try:
            conn = self.trino_conn
            cursor = conn.cursor()
            
            sql = f"""CREATE SCHEMA IF NOT EXISTS {p_catalog}.{p_schema}
                    WITH (location = 's3a://{p_bucket_name}//{p_schema}');
                """
            print('\nExecute SQL:', sql)
            
            print(f"\nCreating schema {p_catalog}.{p_schema}...")
            cursor.execute(sql)
            conn.commit()
            result = cursor.fetchall()
            
            return result
            
        except Exception as e:
            raise Exception('TrinoProvider.create_schema - ', e)
    
    def create_table(self, p_bucket_name, p_catalog, p_schema, p_table, p_metadata, p_format_file, p_key_s3, p_textfile_field_separator):
        try: 
            """
            CATALOG='minio'
            SCHEMA = 'dev'
            TABLE = 'apr_item'
            KEY_S3='test/dev/subject'
            
            """
            conn = self.trino_conn
            cursor = conn.cursor()
            
            
            
            if p_format_file.lower() == 'csv':
                sql = f"""CREATE TABLE IF NOT EXISTS {p_catalog}.{p_schema}.{p_table} (
                        {p_metadata}
                    )
                    WITH (
                        format = 'TEXTFILE',
                        skip_header_line_count = 1,
                        textfile_field_separator = {p_textfile_field_separator},
                        external_location = 's3a://{p_bucket_name}//{p_key_s3}'
                    )
                    """
                print('\nExecute SQL:', sql)
                cursor.execute(sql)
                conn.commit()
                print(f"Table {p_catalog}.{p_schema}.{p_table} criada, file format {p_format_file} ")
                
            elif p_format_file.lower() == 'parquet':
                sql =  f"""CREATE TABLE IF NOT EXISTS {p_catalog}.{p_schema}.{p_table} (
                        {p_metadata}
                    )
                    WITH (
                        format = 'PARQUET',
                        external_location = 's3a://{p_bucket_name}//{p_key_s3}'
                    );
                    """
                print('\nExecute SQL:', sql)
                cursor.execute(sql)
                conn.commit()
                print(f"\nTable {p_catalog}.{p_schema}.{p_table} criada, file format {p_format_file} ")
            else: 
                print('Undefined file format')
            
            
        except Exception as e:
            conn.rollback()
            return Exception('TrinoProvider', e)
    
   
    def delete_table(self, p_catalog, p_schema, p_table):
        
        try:
            conn = self.trino_conn
            cursor = conn.cursor()

            sql = f"""DROP TABLE IF EXISTS {p_catalog}.{p_schema}.{p_table}"""
            print('\nExecute SQL:', sql)
            cursor.execute(sql)

            print(f"Table {p_catalog}.{p_schema}.{p_table} deletada ")
            conn.commit()
            
        
        except Exception as e:
            conn.rollback()
            return Exception('TrinoProvider.delete_table - ', e)
    

    def check_exist_schema(self, p_catalog, p_schema, p_bucket_name, p_is_create_schema=False):
        "Verifica de schema já existe e se já existe cria"
        try:
            conn = self.trino_conn
            cursor = conn.cursor()

            sql = f"""SHOW SCHEMAS FROM {p_catalog} LIKE '{p_schema}'"""
            print('\nExecute SQL:', sql)
        
        
            cursor.execute(sql)
            data = cursor.fetchall()
            df = pd.DataFrame(data)
            
            if len(df) > 0:
                print(f"Schema {p_schema} exist")
            
            elif len(df) == 0 and p_is_create_schema ==  True:
                self.create_schema(p_catalog, p_bucket_name, p_schema)
                
            return df
                
        
        except Exception as e:
            return Exception('TrinoProvider.check_exist_schema - ', e)
    
    
    def check_exist_table(self, p_bucket_name, p_catalog, p_schema, p_table, p_metadata, p_format_file, p_key_s3, p_is_create_table=False, p_textfile_field_separator="','"):
        "Verifica se a table já existe se não cria"
        try:
            conn = self.trino_conn
            cursor = conn.cursor()

            df = []
            sql = f"""SHOW TABLES FROM {p_catalog}.{p_schema} LIKE '{p_table}'"""
            cursor.execute(sql)
            print('\nExecute SQL:', sql)
            data = cursor.fetchall()
            df = pd.DataFrame(data)
            
            if len(df) > 0:
                print(f"Table {p_catalog}.{p_schema}.{p_table} exist")
                
            
            elif len(df) == 0 and p_is_create_table == True:
                print(f"Creating table {p_catalog}.{p_schema}.{p_table} ...")
                self.create_table(p_bucket_name, p_catalog, p_schema, p_table, p_metadata, p_format_file, p_key_s3, p_textfile_field_separator)

            return df
        
        except Exception as e:
            return Exception('TrinoProvider.check_exist_table - ', e)

    
        
    def get_result_pandas(self, p_sql):
        """
        Execute um script sql que retorna dados em um data frame
        
        Parameters:
        p_sql (str): Sql de execução. Ex: SELECT * FROM owner.table
    
        Returns:
        Data frame [list]
        """
        df=[]
        conn = self.trino_conn
        cursor = conn.cursor()
        try:

            print('\nExecute SQL:', p_sql)
            cursor.execute(p_sql)
            data = cursor.fetchall()
            target_fields = [x[0] for x in cursor.description]
            df = pd.DataFrame(data, columns=target_fields)

            if len(df) == 0:
                print(f'Result empty')
         
            return df
        except Exception as e:
            return 'TrinoProvider.get_result_pandas -', e
    
    def get_result_sql(self, p_sql):
        """
        Execute um script sql que retorna dados
        
        Parameters:
        p_sql (str): Sql de execução. Ex: SELECT * FROM owner.table
    
        Returns:
        Data frame [list]
        """
        try:
            conn = self.trino_conn
            cursor = conn.cursor()
            print('\nExecute SQL:', p_sql)
            cursor.execute(p_sql)
           
            result = cursor.fetchall()
            return result

        except Exception as e:
            return Exception('TrinoProvider.get_result_sql - ', e)

    def conn(self):
        """
        Funçao que retorna o cursor da conexão
        Para executar sqls

        Returns:
        Cursor de conexão
        """
        try:
            conn = self.trino_conn
            cursor = conn.cursor()
            return cursor
        
        except Exception as e:
            return Exception('TrinoProvider', e)


    def create_table_and_metadata(self, df, bucket_name, catalog, schema, table, separator, format_file, key_s3):
        """
        Função que cria a estrutura necessária completa para criar uma tabela
            1 - Busca as colunas do dataframe
            2 - Faz o de-para das colunas do df para colunas do trino
            3 - Verifica se esquema e tabela já existe, se não cria
            4 - Verifica se tabela já existe, se não cria

        """
        try : 
            fmt = FormatData()

            # 1 - Busca as colunas do dataframe
            cols= fmt.get_columns_dataframe(df)

            # 2 - Faz o de-para das colunas do df para colunas do trino
            metadata = self.get_columns_dataframe_to_columns_trino(cols)

            ## 3 - Verifica se esquema e tabela já existe, se não cria
            self.check_exist_schema(p_bucket_name=bucket_name,
                                    p_catalog=catalog,
                                    p_schema=schema,
                                    p_is_create_schema=True
                                    )

            ## 4 - Verifica se tabela já existe, se não cria
    
            return_exist_table = self.check_exist_table(p_bucket_name=bucket_name,
                                                        p_catalog=catalog,
                                                        p_schema=schema,
                                                        p_table=table,
                                                        p_metadata=metadata['success'],
                                                        p_format_file=format_file,
                                                        p_key_s3=key_s3,
                                                        p_textfile_field_separator=f"'{separator}'",
                                                        p_is_create_table=True)
            
            return return_exist_table
        except Exception as e:
           return Exception('TrinoProvider.create_table_and_metadata - ', e)         