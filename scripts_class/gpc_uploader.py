import os
from google.cloud import storage

class GCPUploader():
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self.client = storage.Client() # Cria o cliente para acessar o bucket
        self.bucket = self.client.bucket('etl_bus_gps') # Referência pro bucket do GCP

    def upload_to_bucket_bronze(self, path_bronze, file_name_bronze): # Faz o upload de todos os arquivos para o bucket
        print('Iniciando upload do arquivo para a Cloud.')
        path_folder_file_bronze = os.path.join(path_bronze, file_name_bronze) # Caminho completo do arquivo a ser enviado

        blob = self.bucket.blob(f'bronze/{file_name_bronze}') # Cria o blob no bucket
        blob.upload_from_filename(path_folder_file_bronze) # Faz o upload do arquivo para o bucket
        print(f'Arquivo "{file_name_bronze}" enviado para a pasta "/bronze" no bucket "{self.bucket.name}"')

    def upload_to_bucket_silver(self, path_folder_silver, file_name_silver): # Função para enviar o arquivo limpo para o bucket do GCP
        print('Iniciando upload do Arquivo para a Cloud.')
        
        path_folder_file_silver = os.path.join(path_folder_silver, file_name_silver) # Caminho completo do arquivo a ser enviado
        
        blob = self.bucket.blob(f'silver/{file_name_silver}') # Cria o blob no bucket
        blob.upload_from_filename(path_folder_file_silver) # Faz o upload do arquivo para o bucket

        print(f'Arquivo "{file_name_silver}" enviado para a pasta "silver/" no bucket "{self.bucket.name}"')