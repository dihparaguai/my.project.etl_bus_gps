import os
import pandas as pd

class CSVMananger():
    def __init__(self):
        pass

    def read_csv_bronze(self, path_folder_bronze, file_name_bronze):
        print('-> Iniciciando leitura do CSV no diretório "bronze/".')

        # Devido a limitação de memória, o arquivo CSV é lido em pedaços (chunks)
        df_chunk = pd.read_csv(os.path.join(path_folder_bronze, file_name_bronze), on_bad_lines='skip', chunksize=10000)

        print(f'Arquivo "{file_name_bronze}" lido.')
        return df_chunk

    def save_to_csv_silver(self, df, path_folder_silver, file_name_silver): # Função para salvar o DataFrame limpo no diretório silver
        print('-> Iniciciando salvamento do CSV no diretório "silver/".')

        df.to_csv(os.path.join(path_folder_silver, file_name_silver), index=False) # Salva o DataFrame limpo no diretório silver

        print(f'Arquivo "{file_name_silver}" salvo no diretório "{path_folder_silver}".')
