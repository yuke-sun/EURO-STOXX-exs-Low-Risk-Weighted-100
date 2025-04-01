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
parent_symbol = "SXXE"
ccy = 'EUR'
cal = 'STOXXCAL'

calendar_name = 'STXSQ1'
start_at = datetime.date(2015, 2, 28)
end_at = datetime.date(2024, 12, 27)
# end_at = datetime.date.today()

calc_input_type = 'file'
fields = []
repo = repo_connector.connect(data_envt)


## bring your own data
# vol_data = pd.read_csv(directory + '\\vol_data_update.csv', converters={"icb_sectorl1": str,"icb_sectorl3": str })
vol_data = pd.read_csv(directory + '\\vol_data_update_2.csv')
vol_data['reportDate'] = pd.to_datetime(vol_data['reportDate'])
vol_data['inverse'] = 1
vol_data['inverse_vol'] = 1/vol_data['vol']


##indexDefinition : Create & Add rules to index
##################################
# exclusion rules #
rule_1 = rule_builder.filter_simple(attribute='icb5_l1', operator='not in', value = [30] ,dataset_id = 'parent_universe')
rule_2 = rule_builder.filter_simple(attribute='icb5_l3', operator='not in', value = [351020] ,dataset_id = 'parent_universe')

rule_3 = rule_builder.filter_simple(attribute='icb5_l1', operator='not in', value = [83,87,85,86] ,dataset_id = 'parent_universe')

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

## for old ICB
idxdef_old = index.Index()
selection_stage = idxdef_old.add_stage(name='selection', calendar_name='STXSQ1',price_date="cutoff_date")
selection_stage.add_rules([rule_3, rule_4])
 
weighting_stage = idxdef_old.add_stage(name='weighting', calendar_name='STXWQ1',price_date="cutoff_date")
weighting_stage.add_rules([rule_6, rule_7, rule_8, rule_9])
# weighting_stage.add_rules([rule_6])
 
idxdef_old.describe()


#get calendar days
some_days = calendar.get_calendar_days(repo,
                                        calendar_name,
                                        start_at,
                                        end_at)
some_days = some_days[['cuttOffDay','effectiveDay']]
date_pairs = list(zip(some_days['cuttOffDay'],some_days['effectiveDay']))
print(date_pairs)


# running historical index reviews - get universe call & running index deinition in the loop
#################################

index_def_result_all =pd.DataFrame() 

for (securities_data_cutoff_date,composition_date) in date_pairs: # running for every review date in the backtest period
    
    composition_date = pd.to_datetime(composition_date)
    result_initial = universe.get(repo, parent_symbol, composition_date, composition_date,
                    cal, ccy,
                    fields, sid_direct=True)
    
    result_initial = pd.merge(result_initial,vol_data[
                (vol_data['reportDate'].dt.year == composition_date.year) &
                (vol_data['reportDate'].dt.month == composition_date.month)],left_on='stoxxid', right_on='dj_id',how='left')
    result_initial.to_csv(directory + "\\universe_output.csv",index=False,lineterminator="\n", sep=";")

    #run the selection and weighting rule defined for the index
    if pd.isna(result_initial['icb5_l3'][0]):
        idxdef_result = idxdef_old.run(result_initial)
    elif len(str(result_initial['icb5_l3'][0])) == 6:
        idxdef_result = idxdef.run(result_initial)
    else:
        print('please check ICB Codes')
    
    idxdef_result.to_clipboard()
    print(composition_date,len(idxdef_result.index),idxdef_result[idxdef_result['exclusion']== False].shape[0])

    #consolidate the backtested reviews
    index_def_result_all = pd.concat([index_def_result_all,idxdef_result])

#print backtested compositions -- only inclusions
print(index_def_result_all[index_def_result_all['exclusion']== False])
# output = index_def_result_all[index_def_result_all['exclusion']== False]
# index_def_result_all = index_def_result_all.drop(columns=['mappedRegions','exclusion_reason'])
index_def_result_all.to_excel( directory + "\\IDT_output_comp_final.xlsx")
# index_def_result_all.to_csv( directory + "\\IDT_output_comp_final.csv", index=False,lineterminator="\n", sep=";" )

