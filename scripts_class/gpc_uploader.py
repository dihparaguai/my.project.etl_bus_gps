import os
from google.cloud import storage

class GCPUploader():
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self.client = storage.Client() # Cria o cliente para acessar o bucket
        self.bucket = self.client.bucket('etl_bus_gps') # Referência pro bucket do GCP
        pass

    def upload_to_bucket_bronze(self, path_bronze, file_bronze): # Faz o upload de todos os arquivos para o bucket
        print('Iniciando upload dos arquivos para a Cloud.')
        file_path = os.path.join(path_bronze, file_bronze)

        blob = self.bucket.blob(f'bronze/{file_bronze}')
        blob.upload_from_filename(file_path)
        print(f'Arquivo "{file_bronze}" enviado para a pasta "/bronze" no bucket "{self.bucket.name}"')