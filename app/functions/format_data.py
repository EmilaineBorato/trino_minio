
import pandas as pd
class FormatData:

    def __init__(self):
        super(FormatData, self).__init__()

    
    def set_timezone_col_dataframe(self, df, set_timezone=False):
        """
        Description
        -----------
        Seta o timezone para a coluna
        OBS: Funcao criada para corrigir o erro de retorno dos dados em df - File "pandas/_libs/tslibs/timezones.pyx", line 266, in pandas._libs.tslibs.timezones.get_dst_info

        Parameters
        --------------
        :param df (df): DataFrame 
        :param set_timezone (boolean): Setagem original do timezone, vinda do dtype

        """
        try:
            for i in df:
                dtype = str(df[i].dtypes)

                ## Verifica se existe alguma coluna com tipo datetime e timezone, se não pass
                if dtype.__contains__('datetime64[ns,') == True:
                    ## Verifica se é para converter para o timezone encontrado
                    if set_timezone == True:
                        split_timezone = dtype.split(',')
                        print('Timezone', split_timezone)

                        timezone = split_timezone[1][1:-1] if len(split_timezone) > 1 else []
                    
                        ## Se encontrar timezone seta, se não deixa como none, ou seja remo
                        if len(timezone) > 0:
                            print(f"Set timezone: {timezone}")
                            df[i]=df[i].dt.tz_convert(timezone)
                            df[i]=df[i].dt.strftime("%Y-%m-%d %H:%M:%S")
                            df[i] = pd.to_datetime(df[i], format="%Y-%m-%d %H:%M:%S")
                        else: 
                            print('Timezone not founf')
                            # df[i]=df[i].dt.tz_convert(None)
                    # else:
                        # df[i]=df[i].dt.tz_convert(None)

            return df
        except Exception as e:
            raise Exception('FormatData.set_timezone_col_dataframe - ',e)
    


    def set_timezone_col_dataframe_serialize(self, df, set_timezone=False):
        """
        Description
        -----------
        Seta o timezone para a coluna
        OBS: Funcao criada para corrigir o erro de retorno dos dados em df - File "pandas/_libs/tslibs/timezones.pyx", line 266, in pandas._libs.tslibs.timezones.get_dst_info

        Parameters
        --------------
        :param df (df): DataFrame 
        :param set_timezone (boolean): Setagem original do timezone, vinda do dtype

        """
        try:
            for i in df:
                dtype = str(df[i].dtypes)

                ## Verifica se existe alguma coluna com tipo datetime e timezone, se não pass
                if dtype.__contains__('datetime64[ns,') == True:
                    ## Verifica se é para converter para o timezone encontrado
                    if set_timezone == True:
                        split_timezone = dtype.split(',')
                        print('Timezone', split_timezone)

                        timezone = split_timezone[1][1:-1] if len(split_timezone) > 1 else []
                    
                        ## Se encontrar timezone seta, se não deixa como none, ou seja remo
                        if len(timezone) > 0:
                            print(f"Set timezone: {timezone}")
                            df[i]=df[i].dt.tz_convert(timezone)
                            df[i]=df[i].dt.strftime("%Y-%m-%d %H:%M:%S")
                            df[i] = pd.to_datetime(df[i], format="%Y-%m-%d %H:%M:%S")
                        else: 
                            print('Timezone not founf')
                            # df[i]=df[i].dt.tz_convert(None)
                    else:
                        #df[i]=df[i].dt.tz_convert(None)
                        df[i] = pd.to_datetime(df[i])

            return df
        except Exception as e:
            raise Exception('FormatData.set_timezone_col_dataframe_serialize - ', e)
    
    def get_columns_dataframe(self, df):
        """
        Description
        -----------
        Busca o nome, tipo e tamanho das colunas no Dataframe
        
        Parameters
        --------------
        :param df (df): DataFrame 


        Returns
        --------------

        Array de json [{'column_name': id, 'column_type': 'int64, 'length': 8}}]

        """
        columns = []

        try:
            if len(df) > 0:
                for k, v in dict(df.dtypes).items():
                    length = ''
                    if v == 'object' or v == 'datetime.date':
                        ## filter nulos de cada k (coluna) e trax o maior valor da coluna
                        aux = df[k]
                        aux = aux[df[k].notnull()].max()
                        length = len(str(aux))
                    else:
                        #Para pegar o maior lenght, pega o max e primeira ocorrencia
                        #Lenght utilizado para saber quando vai ser bigint (> 10)
                        if str(df[k].nlargest(1)) != 'nan': 
                            length=len(str(df[k].nlargest(1)).split()[1])

                    column = {"column_name": k, "column_type": v, "length": length}
                    columns.append(column)
            else:
                print('132 - FormatData.get_columns_dataframe() -> DF received is empty. Unable to search columns ')
            
            return columns
        
        except Exception as e:
            raise Exception('FormatData.get_columns_dataframe() - ', e)
