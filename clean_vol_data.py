import pandas as pd
import numpy as np
import warnings
import os

warnings.filterwarnings('ignore')
pd.set_option("expand_frame_repr", False)

directory = os.path.dirname(os.path.abspath(__file__))

path = directory + '\\Vol Data\\'
output_path = directory 
file_list = os.listdir(path)

'''
index SXLV1E EURO STOXX Low Risk Weighted 100
'''

Universe_Info = pd.DataFrame()

for file in file_list:
    if file.endswith('.xlsx'):
        file_path = os.path.join(path, file)
        date = file[0:10]
        Universe = pd.read_excel(file_path, sheet_name='Universe', keep_default_na=False, na_values=["", "null", "N/A"], converters={"icb_sectorl1": str})
        Universe = Universe[Universe['index_symbol'] == 'SXLV1E']
        Universe['reportDate'] = pd.to_datetime(date, format='%d-%m-%Y').strftime('%Y-%m-%d')
        # Universe = Universe[['reportDate','dj_id','icb_sectorl1','icb_sectorl3','vol','is_selected']]
        Universe = Universe[['reportDate','dj_id','vol']]

        Universe_Info = pd.concat([Universe_Info,Universe], axis= 0)
        print('finish reading ' + file)


Universe_Info = Universe_Info.sort_values(by = 'reportDate')
print(Universe_Info.head())
Universe_Info.to_csv(directory + '\\vol_data_update.csv')

