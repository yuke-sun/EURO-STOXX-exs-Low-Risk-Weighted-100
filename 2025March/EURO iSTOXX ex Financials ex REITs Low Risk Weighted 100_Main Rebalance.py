##data services
from qpds import repo_connector
from qpds import universe
from qpds import calendar
from qpds import history
from qpds import backtest as backtest_data_fetch
#rules and definition
from qidxrules.utils import rule_builder
from qidxdef import index

#analytics
from qit import main
## external libraries
import os
import numpy as np
import pandas as pd
import datetime
import warnings
import time

warnings.filterwarnings('ignore')
pd.set_option("expand_frame_repr", False)

directory = os.path.dirname(os.path.abspath(__file__))

#Configure parameters needed for the get universe call
data_envt = 'PROD'
calc_envt = "PROD"
parent_symbol = "TW1P"
ccy = 'EUR'
cal = 'STOXXCAL'

calendar_name = 'STXSQ1'
calc_input_type = 'file'
fields = []
repo = repo_connector.connect(data_envt)
composition_date = '2025-03-14'

## bring your own data
# vol_data = pd.read_csv(directory + '\\vol_data_update.csv', converters={"icb_sectorl1": str,"icb_sectorl3": str })
vol_data = pd.read_csv(directory + '\\March_Selection.csv')
vol_data['reportDate'] = pd.to_datetime(composition_date)
vol_data['inverse'] = 1
vol_data['inverse_vol'] = 1/vol_data['vol']


##indexDefinition : Create & Add rules to index
##################################
# exclusion rules #
rule_1 = rule_builder.filter_simple(attribute='icb_sectorl1', operator='not in', value = [30] ,dataset_id = 'parent_universe')
rule_2 = rule_builder.filter_simple(attribute='icb_sectorl3', operator='not in', value = [351020] ,dataset_id = 'parent_universe')

rule_4 = rule_builder.filter_top(
    attribute="vol",
    target_count=100,
    sort_order="asc",
    dataset_id="parent_universe",
    previous_dataset_id="",
)

# weighting stage #
rule_6 = rule_builder.math_ops(operator="divide",
    attribute_1= 1.0,
    attribute_2= "vol",
    dataset_id="index_universe",
)

rule_7 = rule_builder.weight_by_attribute(attribute='divide_attribute_1_vol',dataset_id = 'index_universe')
rule_8 = rule_builder.capping(cap_limit = 0.1)
rule_9 = rule_builder.weight_factor(weight_attribute='capped_weight', price_attribute="adjustedOpenPrice", scale_factor_billions=1)


##################################
##Index defintion 
idxdef = index.Index()
selection_stage = idxdef.add_stage(name='selection', calendar_name='STXSQ1',price_date="cutoff_date")
selection_stage.add_rules([rule_1, rule_2, rule_4])
 
weighting_stage = idxdef.add_stage(name='weighting', calendar_name='STXWQ1',price_date="cutoff_date")
weighting_stage.add_rules([rule_6, rule_7, rule_8, rule_9])
 
idxdef.describe()

# running historical index reviews - get universe call & running index deinition in the loop
#################################

index_def_result_all =pd.DataFrame() 

composition_date = pd.to_datetime(composition_date)
result_initial = universe.get(repo, parent_symbol, composition_date, composition_date,
                cal, ccy,
                fields, sid_direct=True)

result_initial = pd.merge(result_initial,vol_data[
            (vol_data['reportDate'].dt.year == composition_date.year) &
            (vol_data['reportDate'].dt.month == composition_date.month)],left_on='stoxxid', right_on='dj_id',how='right')
result_initial.to_csv(directory + "\\universe_output.csv",index=False,lineterminator="\n", sep=";")

#run the selection and weighting rule defined for the index
idxdef_result = idxdef.run(result_initial)


idxdef_result.to_clipboard()
print(composition_date,len(idxdef_result.index),idxdef_result[idxdef_result['exclusion']== False].shape[0])

#consolidate the backtested reviews
index_def_result_all = pd.concat([index_def_result_all,idxdef_result])

#print backtested compositions -- only inclusions
print(index_def_result_all[index_def_result_all['exclusion']== False])
# output = index_def_result_all[index_def_result_all['exclusion']== False]
index_def_result_all = index_def_result_all.drop(columns=['mappedRegions','exclusion_reason'])
index_def_result_all.to_excel( directory + "\\IDT_output_comp_mar.xlsx")
index_def_result_all.to_csv( directory + "\\IDT_output_comp_mar.csv", index=False,lineterminator="\n", sep=";" )


# Create output with required format
Date_list = index_def_result_all['compositionDate'].unique()
Input_4D = index_def_result_all[index_def_result_all['exclusion']== False]
Input_4D = Input_4D[['stoxxid','compositionDate', 'weight_factor']]
Input_4D['weight_factor'] = round(Input_4D['weight_factor'] * 95.391,0).astype(int)
Input_4D.rename(columns={'weight_factor':'weightfactor', 'stoxxid':'dj_id'}, inplace= True)
Input_4D[['capfactor', 'valid_from', 'valid_to', 'index_symbol', 'size','description','not_rep_before']] = [1, '20250324', '99991231', 'SXXFLVE','Y','','']
Selection_4D = Input_4D[['valid_from','valid_to','index_symbol', 'dj_id','size','description','not_rep_before']]
weighting_4D = Input_4D[['valid_from','valid_to','index_symbol', 'dj_id', 'weightfactor','capfactor','description','not_rep_before']]

# # Output.to_csv(output_path + '\\Components_with_weight.csv')
Selection_4D.to_csv( directory + "\\SXXFLVE_Selection_4D.csv", index=False, lineterminator="\n", sep=";")
weighting_4D.to_csv( directory + "\\SXXFLVE_Weighting_4D.csv", index=False, lineterminator="\n", sep=";")