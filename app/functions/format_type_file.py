from io import BytesIO, StringIO
import logging
import pandas as pd


class FormatTypeFile:

    def __init__(self):
        super(FormatTypeFile, self).__init__()
  
    def df_to_format_file(self, data, type_file):
        """
        Descrition
        ---------
        Converte o DataFrame pra outro formato de arquivo
        
        Parameters
        ----------
        :param data(df)*: Dados em dataframe
        :param type_file(str)*: tipo para conversao, ex: csv, parquet, json

        Returns
        -------
        json
        
        """
        logging.info(f'Format df to {type_file}')
      
        if type_file == 'parquet':
            df = data
            buffer = BytesIO()
            df.to_parquet(buffer, engine='auto', compression='snappy')
            data = buffer.getvalue()
            buffer.seek(0)
            content_type = "application/parquet"
      
        if type_file == 'csv':
            
            bytes = data.to_csv(index=False, header=True).encode('utf-8')
            buffer = BytesIO(bytes)
            data = buffer.getvalue()
            buffer.seek(0)
            content_type = "application/csv"
            
        if type_file == 'json':
                       
            buffer = StringIO()
            data.to_json(path_or_buf= buffer, orient="records", date_format='iso', force_ascii=True, date_unit='ms', default_handler=None)
            data = buffer.getvalue()
            buffer.seek(0)
            content_type = "application/json"
            
        
        return {"data": data, "content_type": content_type, "buffer": buffer}

    def format_file_to_df(self, data, type_file):

        if type_file == 'csv':
            file = StringIO(data)
            df = pd.read_csv(file)

        if type_file == 'parquet':
            pq_file = BytesIO(data)
            df = pd.read_parquet(pq_file)
            
        if type_file == 'json':
            df = pd.read_json(data, orient="records")
         
        return df

