import os
import kagglehub

class KaggleDownloader:
    def __init__(self, kaggle_owner, kaggle_dataset):
        self.kaggle_owner = kaggle_owner
        self.kaggle_dataset = kaggle_dataset

    def download_kaggle_dataset(self, path_bronze):
        path_origin = kagglehub.dataset_download(os.path.join(self.kaggle_owner, self.kaggle_dataset)) # Baixa os arquivos de GPS do Kaggle
        
        os.system(f'mkdir -p {path_bronze}') # Cria a para que conterá os arquivos originais baixados

        # Copia todos os arquivos da pasta do Kaggle para data_raw
        os.system(f'mv {path_origin}/* {path_bronze}/')
        os.system(f'rm -rf {path_origin}')
        
        print(f'Arquivos baixados e movidos para a pasta {path_bronze}.')