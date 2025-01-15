import pandas as pd
import numpy as np
import os
import sys
directory = os.path.dirname(os.path.abspath(__file__))
sys.path.append(directory)

Comp_All = pd.read_csv(directory + '\\IDT_output_comp_update.csv')
backtest_data = Comp_All[Comp_All['exclusion']== False]
iStudio = backtest_data.copy()

Date_list = Comp_All['compositionDate'].unique()


iStudio = iStudio[['stoxxid', 'sedol','isin','compositionDate', 'weight_factor']]
iStudio.rename(columns={'weight_factor':'weightFactor'}, inplace= True)
iStudio['capFactor'] = 1
iStudio.columns = ['STOXXID','SEDOL','ISIN','effectiveDate','weightFactor','capFactor']
iStudio= iStudio[['STOXXID','SEDOL','ISIN','effectiveDate','weightFactor','capFactor']]


# Output.to_csv(output_path + '\\Components_with_weight.csv')
iStudio.to_csv( directory + "\\low_vol100_istudio_update.csv", index=False, lineterminator="\n", sep=";")
# TODO: how to calculate weight factor if the component was added in between the reviews