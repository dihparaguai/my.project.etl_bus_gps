import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.abspath("./gcp_key.json")
import pandas as pd
from scripts_class.kaggle_downloader import KaggleDownloader
from scripts_class.gpc_uploader import GCPUploader
from scripts_class.csv_manager import CSVMananger
from scripts_class.data_transformer import DataTransformer

# ------------------------------------------------------------------------------
folder_path_bronze = os.path.abspath('./data/bronze')
kaggle_owner = 'stoney71'
kaggle_dataset = 'new-york-city-transport-statistics'

kaggle_downloader = KaggleDownloader(kaggle_owner, kaggle_dataset)
kaggle_downloader.download_kaggle_dataset(folder_path_bronze)

# ------------------------------------------------------------------------------
year_month = pd.to_datetime('2017-12-01').strftime('%y%m')

file_name_bronze = f'mta_{year_month}.csv'
gcp_uploader = GCPUploader('etl_bus_gps')
gcp_uploader.upload_to_bucket_bronze(folder_path_bronze, file_name_bronze)

# # ------------------------------------------------------------------------------
csv_manager = CSVMananger()
df = csv_manager.read_csv_bronze(folder_path_bronze, file_name_bronze)

data_transformer = DataTransformer(df)
data_transformer.clear_and_filter_data()
data_transformer.add_RecordedAtDate_and_Time_columns()
data_transformer.add_Recorded_and_ScheduledTimeRange_columns()
data_transformer.add_DiffArrivalMins_column()

df = data_transformer.get_df()

path_folder_silver = os.path.abspath('./data/silver')
file_name_silver = f'mta_{year_month}_cleaned.csv'

csv_manager.save_to_csv_silver(df, path_folder_silver, file_name_silver)
gcp_uploader.upload_to_bucket_silver(path_folder_silver, file_name_silver)

# ------------------------------------------------------------------------------