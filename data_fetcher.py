from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError
from imu_preprocessor import get_row_wise_stat
from static_data import Static
import pandas as pd

def print_dataframe(df):
    print(f"[+] Dataframe shape {df.shape}")
    print(f"[+] Dataframe columns {list(df.columns)}")


class DataFetcher:

    def __init__(self, query):
        self.query = query
        self._MONGO_URI = Static.KAATRU_MONGO_URI
        self._DATABASE = Static.KAATRU_MONGO_DATABASE

    def sensor_query(self, device):
        try:
            client = MongoClient(self._MONGO_URI)
            if client.server_info()['ok'] == 1:
                print("[+] Connection established...")

        except ServerSelectionTimeoutError as err:
            print(f"[-] TimeoutError: {err}")
            return {}
            
        _collections = client[self._DATABASE].list_collection_names()

        collection = [col for col in _collections if (
            device in col) and (col.endswith("senloc"))]

        collection_data = client[self._DATABASE][collection[0]]
        output = list(collection_data.find(self.query))
        df = pd.json_normalize(output)
        print_dataframe(df)
        old_cols = df.columns
        new_cols = {}
        for col in old_cols:
            if col.startswith('value'):
                new_cols[col] = col.replace('value.', '')

        df.rename(columns=new_cols, inplace=True)

        return df

    def imu_query(self, device):
        try:
            client = MongoClient(self._MONGO_URI)
            if client.server_info()['ok'] == 1:
                print("[+] Connection established...")

        except ServerSelectionTimeoutError as err:
            print(f"[-] TimeoutError: {err}")
            return {}

        _collections = client[self._DATABASE].list_collection_names()

        collection = [col for col in _collections if (
            device in col) and (col.endswith("accloc"))]

        collection_data = client[self._DATABASE][collection[0]]
        output = list(collection_data.find(self.query))
        df = pd.json_normalize(output)
        print_dataframe(df)
        old_cols = df.columns
        new_cols = {}
        for col in old_cols:
            if col.startswith('value'):
                new_cols[col] = col.replace('value.', '')
        df.rename(columns=new_cols, inplace=True)

        cols = ['AcX', 'AcY', 'AcZ', 'GcX', 'GcY', 'GcZ', 'Tmp']
        df["LatAcc"] = pd.to_numeric(df["LatAcc"])
        df["LonAcc"] = pd.to_numeric(df["LonAcc"])
        for col in cols:
            col_name = col+"_Mean"
            df[col_name] = get_row_wise_stat(col=col,data=df)
        cols_to_drop = ['name', 'Label', 'SogAcc', 'CogAcc', 'Time', 'B', 'A', 'End_Time', 'value']
        for item in cols_to_drop:
            cols.append(item)
        for cols in cols_to_drop:
            if cols in df.columns:
                df.drop(columns=cols, inplace=True)

        print_dataframe(df)
        return df
