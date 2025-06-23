import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "../gcp_key.json"
from scripts_class.kaggle_downloader import KaggleDownloader


path_bronze = '../data/bronze'
kaggle_owner = 'stoney71'
kaggle_dataset = 'new-york-city-transport-statistics'

kaggle_downloader = KaggleDownloader(kaggle_owner, kaggle_dataset)
kaggle_downloader.download_kaggle_dataset(path_bronze)