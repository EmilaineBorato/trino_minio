
from io import StringIO, BytesIO, TextIOWrapper
from minio import Minio
from minio.error import S3Error
import pandas as pd
import os
# from decouple import config 
from .format_type_file import FormatTypeFile


ftf = FormatTypeFile()


pd.set_option('display.max_columns', None)

class s3Function:
    def __init__(self):
        self.s3 = Minio("localhost:9000",
                access_key="8PI53SpUutlgZqnN63hZ", 
                secret_key="NXPO7kGfygsKCnbjPfGY6J0FWO8uKvLjbpuTwSk1",
                secure=False
            )
        
    
    
    def create_bucket(self, bucket_name):
        print("Creating new bucket: {0}".format(bucket_name))
        
        self.s3.make_bucket(bucket_name)       
        return "Bucket: {0} created!".format(bucket_name)
    
    def get_buckets(self):
        list = []
        for bucket in self.s3.list_buckets():
            name = str(bucket.name)
            list.append(name)
        return list
    
    def put_bucket(self, bucket_name, key, data, file_format):
           # try:
        if data is None or len(data) == 0:
            print("\nEmpty dataframe, no data to insert")

        else:
            try:
                if len(file_format) > 0:
                    body = ftf.df_to_format_file(data, file_format)
                else:
                    body = data
                
                if self.s3.bucket_exists(bucket_name) == False:
                    print(f"\nBucket {bucket_name} not found")
                    self.create_bucket(bucket_name)
                
                # Insert file
                
                # self.s3.put_object(Bucket=bucket_name, Key=key, Body=body)
                print(body)
                self.s3.put_object(bucket_name, key, body['buffer'], len(body['data']), content_type=body['content_type'])
                print(f"\nForam inseridos {len(data)} linha no arquivos '{key}', no bucket {bucket_name}")
                
            except Exception as err:
                raise Exception(err)


    def load_file_s3(self, path, bucket_name, format):
        """
        Lê o arquivo e retorna os dados em um DataFrame

        Parameters
        --------------
        :param path: nome completo do arquivo desde suas pastas. Ex 2017/05/arquivo_teste
        :param bucket_name: nome do bucket
        :param format: formato que os arquivos estão armazenados (csv, parquet)

        Returns
        -------

        :return: retorna um DataFrame
        """

        try:
            if format == 'csv':
                response = self.s3.get_object(bucket_name, f"{path}.{format}")
                byte = response.read().decode('utf-8')
                file = StringIO(byte)
                df = pd.read_csv(file)

            elif format == 'parquet':
                response = self.s3.get_object(bucket_name, f"{path}.{format}")
                byte = response.read(cache_content=True)
                pq_file = BytesIO(byte)
                df = pd.read_parquet(pq_file)

            return df
                
        except S3Error as exc:
            print("error load_from_s3.", exc)

    def list_objects(self, bucket_name, dir=''):
        """
        Retorna uma lista dos arquivos do bucket_name
        Não está organizado por pastas ou niveis de pastas, retornar o diretorio.formato completo

        Parameters
        ----------
        :bucket_name : nome do bucket
        :dir: diretorio para busca (não obrigatorio)


        Utilite pprint para ver com identação
        """

        list = []
        try:
            objects = self.s3.list_objects(bucket_name, recursive=True, start_after=dir)
            for obj in objects:

                list.append(obj.object_name)

            return list

        except S3Error as exc:
            print("error list_objects.", exc)

    
   
    
    def get_list_objects(self, bucket_name, prefix):
        """
        Retorna uma lista dos arquivos do bucket_name
        Não está organizado por pastas ou niveis de pastas, retornar o diretorio.formato completo

        Parameters
        ----------
        :bucket_name : nome do bucket
        :prefix: diretorio para busca (não obrigatorio)
        """
        list = []
        try: 
            response = self.s3.list_objects(bucket_name, prefix=prefix)

            for file in response:
                list.append(file.object_name)

        except Exception as e:
            print(f"Object '{prefix}' not found {e}")
        else:
            return list
    
    
    def delete_object(self, bucket_name:str, dir: str):
        """
        Deleta objetos no bucket
        Primeiro varre todos os arquivos dentro do diretorio informado e depois os apaga

        Parameters
        ----------
        bucket_name: str, obrigatorio
            Nome do bucket 
        dir: str, obrigatorio
            Diretorio para busca 

        Examples
        --------
        >>> delete_object(bucket_name: 'test', dir:'dev/silver/folder')

        Returns
        -------
        Apenas mensagem de log
        """

        try:
            objects =  self.get_list_objects(bucket_name, dir)
            if len(objects) > 0:
                for obj in objects:
                    self.s3.remove_objects(bucket_name, obj)
                    print(f"Object '{obj}' deleted")
            else:
                print(f'{dir} empty')
        except Exception as e:
            Exception(e)
  
      
        

        