import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.abspath("./gcp_key.json")
import pandas as pd
from scripts_class.kaggle_downloader import KaggleDownloader
from scripts_class.gpc_uploader import GCPUploader

# ------------------------------------------------------------------------------
path_bronze = os.path.abspath('./data/bronze')
kaggle_owner = 'stoney71'
kaggle_dataset = 'new-york-city-transport-statistics'

kaggle_downloader = KaggleDownloader(kaggle_owner, kaggle_dataset)
kaggle_downloader.download_kaggle_dataset(path_bronze)

# ------------------------------------------------------------------------------
year_month = pd.to_datetime('2017-12-01').strftime('%y%m')

file_bronze = f'mta_{year_month}.csv'

gcp_uploader = GCPUploader('etl_bus_gps')
gcp_uploader.upload_to_bucket_bronze(path_bronze, file_bronze)