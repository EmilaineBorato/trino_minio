
import sqlparse

class FormatSql:

    def __init__(self):
        super(FormatSql, self).__init__()
  
    def add_where(self, p_sql, p_where):
        """
        Description
        -----------
        Verifica se existe WHERE, se tiver inclui WHERE 1 = 1, se não adiciona WHERE logo depois do FROM
        
        Parameters
        --------------
        :param p_sql (str): sql recebido 
        :param p_where (str): where a ser adicionado na instrucao
        
        Examples
        --------
        >>> add_where(p_sql: 'SELECT column FROM table', p_where:'date_trunc('day', data_realizacao) BETWEEN to_date('2023-07-01', 'yyyy-mm-dd') AND to_date('2023-07-31', 'yyyy-mm-dd'))
        """
        
        try:
            sql = sqlparse.format(p_sql, reindent=True, keyword_case='upper') # formata a SQL, deixando em linhas cada comando
            sql_split = sql.split('\n')

            # print('WHERE', p_where)
            i = 0
            for line in sql_split:
                i += 1
                if 'WHERE' in sql:
                    if line == 'WHERE 1 = 1':
                        line = line + f" AND {p_where}"
                        sql_split[i-1] = line
                    else:
                        line = line.replace("WHERE", " WHERE 1 = 1 AND")
                        sql_split[i-1] = line
                elif ('WHERE' not in sql and ' ON ' not in sql) and ('FROM' in line):
                    line = line + f" WHERE {p_where}"
                    sql_split[i-1] = line
                ## case inner join 
                elif 'WHERE' not in sql and ' ON ' in line:
                    print('LINE:', line)
                    line = line + f" WHERE {p_where}"
                    sql_split[i-1] = line

            new_sql = ' '.join(sql_split)
            # print('SQL', new_sql)
            sql = sqlparse.format(new_sql, reindent=True, keyword_case='upper')
            
            return sql  
        
        except Exception as e:
           raise Exception('FormatSql.add_where - ', e)
        
    def add_limit(self, p_sql, p_limit, p_type_connection, extraction_type=''):
        """
        Parameters
        --------------
        :param p_sql (str): sql recebido 
        :param p_limit (int): valor do limit a ser adicionado na instrucao
        :param p_type_connection(str): tipo de conexao (oracle, postgres)
        :param extraction_type (str): tipo de extração/transformacao
        
        Examples
        --------
        >>> add_limit(p_sql: 'SELECT column FROM table', p_limit: 1, p_type_connection: oracle)
        """
        try:
            conn_type = p_type_connection.lower()
            sql = sqlparse.format(p_sql, reindent=True, keyword_case='upper')

            if conn_type == 'postgres' or conn_type == 'trino':
                if 'LIMIT' in sql:
                    limit =  sql.split("LIMIT", 1)
                    new_sql = limit[0] + f"LIMIT {p_limit}"
                else:
                    new_sql = f"{sql} LIMIT {p_limit}"     
                
                
            if conn_type == 'oracle':
                if 'WHERE' in sql:
                    new_sql = f"{sql} AND ROWNUM <= {p_limit}"   
                else:
                    new_sql = f"{sql} WHERE ROWNUM <= {p_limit}"   
                     

            sql = sqlparse.format(new_sql, reindent=True, keyword_case='upper')
            return sql
        except Exception as e:
            raise Exception('FormatSql.add_limit - ' ,e)
