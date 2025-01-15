##data services
from qpds import repo_connector
from qpds import universe
from qpds import calendar
from qpds import history
from qpds import backtest as backtest_data_fetch
#rules and definition
from qidxrules.utils import rule_builder
from qidxdef import index
#backtest calculation
from qpdcalc import backtest as backtest_calc
from qpdcalc.async_task import create_backtest_job
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
parent_symbol = "SXLV1GT"
ccy = 'EUR'
cal = 'STOXXCAL'
# vendor_items = ['RBICS','Sustainalytics','ISS','EconsightMetaverse'] #'RBICS','Sustainalytics','ISS', RBICS_FOCUS
fields = []
industry = [30, 55, 60] 

calendar_name = 'STXSQ1'
start_at = datetime.date(2019, 9, 1)
end_at = datetime.date.today()

calc_input_type = 'file'
repo = repo_connector.connect(data_envt)






##indexDefinition : Create & Add rules to index
##################################
# exclusion rules #

rule_1 = rule_builder.filter_simple(attribute='icb5_l1', operator='not in', value = [30] ,dataset_id = 'parent_universe')
rule_2 = rule_builder.filter_simple(attribute='icb5_l3', operator='not in', value = [351020] ,dataset_id = 'parent_universe')





# rule_2 = rule_builder.weight_by_attribute(attribute='ffmcap',dataset_id = 'index_universe')
rule_3 = rule_builder.math_ops_divide(operator="divide",
    attribute_1="weights",
    attribute_2="close_usd",
    dataset_id="index_universe",
)

rule_5 = rule_builder.weight_by_attribute(attribute='12-vol',dataset_id = 'parent_universe')
rule_6 = rule_builder.weight_factor(weight_attribute='weight', price_attribute="adjustedOpenPrice", scale_factor_billions=1)
rule_3 = rule_builder.capping(cap_limit = 0.1)


##Index defintion 
idxdef = index.Index()
selection_stage = idxdef.add_stage(name='selection', calendar_name='STXSA3',price_date="cutoff_date")
selection_stage.add_rules([rule_1])
 
weighting_stage = idxdef.add_stage(name='weighting', calendar_name='STXWQ1',price_date="cutoff_date")
weighting_stage.add_rules([rule_6, rule_3])
 
idxdef.describe()


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

index_def_result_all =pd.DataFrame() #creating a df to capture the consolidate the output of the review

for (securities_data_cutoff_date,composition_date) in date_pairs: # running for every review date in the backtest period
    
    #getting the data points needed for the selection and weighting using the data services
    result_initial = universe.get(repo, parent_symbol, securities_data_cutoff_date, composition_date,
                    cal, ccy,
                    fields, sid_direct=True)

    result_initial.to_csv(directory + "\\universe_output.csv",index=False)

    #run the selection and weighting rule defined for the index
    idxdef_result = idxdef.run(result_initial)
    print(composition_date,len(idxdef_result.index),idxdef_result[idxdef_result['exclusion']== False].shape[0])

    #consolidate the backtested reviews
    index_def_result_all = pd.concat([index_def_result_all,idxdef_result])

#print backtested compositions -- only inclusions
print(index_def_result_all[index_def_result_all['exclusion']== False])