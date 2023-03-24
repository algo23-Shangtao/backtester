from contextlib import closing
from tqsdk import TqApi, TqAuth
import sys
from tqsdk.tools import DataDownloader
from datetime import datetime


api = TqApi(auth=TqAuth('tushetou', 'Tt1234567890'))
contract_list = [
    '2305', '2310'
]
download_tasks = {}
for contract in contract_list:
    download_tasks[contract] = DataDownloader(api, symbol_list=f"SHFE.rb{contract}", dur_sec=0,
                start_dt=datetime(2022, 5, 17, 21,0,0,0), end_dt=datetime(2023, 3, 20, 15,0,0,0), csv_file_name=f"rb{contract}.csv")

with closing(api):
    while not all([v.is_finished() for v in download_tasks.values()]):
        api.wait_update()
        for k,v in download_tasks.items():
            print("progress: ", { k:("%.3f%%" % v.get_progress())})
