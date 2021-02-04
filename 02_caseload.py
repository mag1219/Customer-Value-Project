# -*- coding: utf-8 -*-
"""
Created on Fri Mar 15 00:19:47 2019

@author: manqi.w
"""

# =============================================================================    
# CASELOAD
# =============================================================================
claim_latest_open_score = claim_latest_open.merge(score_app,
                                                  on = ['claim_id','batch_id'],
                                                  how = 'inner')    
df_attributes(claim_latest_open_score,'Open claim with latest batch with score')
'''
Definition of display_complexity:
    1 - Predicted as Level 1
    2 - Predicted as Level 2
    3 - Predicted as Level 3
    4 - Predicted as Level 4
Definition of dispplay_litigation:
    1 - Predicted as HIGH likelihood to get attorney involved
    2 - Predicted as MEDIUM likelihood to get attorney involved
    3 - Predicted as LOW likelihood to get attorney involved
    4 - Already get attorney involved
'''
print('Distribution of display_complexity:')
print(claim_latest_open_score['display_complexity'].value_counts())
print('Distribution of display_litigation:')
print(claim_latest_open_score['display_litigation'].value_counts())

claim_latest_open_score['high_display_complexity'] = np.where(True,
                       claim_latest_open_score['display_complexity'].isin([3,4]), False)
claim_latest_open_score['display_already_litigated'] = np.where(True,
                       claim_latest_open_score['display_litigation'] == 4, False)
claim_latest_open_score['indemnity'] = np.where(True,
                       claim_latest_open_score['claim_type']=='INDEMNITY',False)

## adjuster case load
table = claim_latest_open_score.pivot_table(index = ['adjuster_name'],
                                            values = ['claim_id',
                                                      'total_claim_costs',
                                                      'indemnity',
                                                      'high_display_complexity',
                                                      'display_already_litigated',],
                                            aggfunc = {'claim_id':'nunique',
                                                       'total_claim_costs':'mean',
                                                       'indemnity':'sum',
                                                       'high_display_complexity':'sum',
                                                       'display_already_litigated':'sum'},
                                            margins = True,
                                            fill_value = 0)

table['indemnity'] = table['indemnity'].div(table['claim_id'], axis = 0)
table['high_display_complexity'] = table['high_display_complexity'].div(table['claim_id'], axis=0)
table['display_already_litigated'] = table['display_already_litigated'].div(table['claim_id'], axis=0)

table.rename(columns = {'claim_id':'Number of Claims',
                        'total_claim_costs':'Average Claim Costs',
                        'indemnity':'Percentage of Indemnity Claims',
                        'high_display_complexity':'Percentage of Level 3 or Level 4 Claims',
                        'display_already_litigated':'Percentage of Litigated Claims',
                        #'supervisor_id':'Supervisor Name',
                        'adjuster_name':'Adjuster Name'},
                        inplace = True)


## supervisor - adjuster case load
table2 = claim_latest_open_score.pivot_table(index = ['office','adjuster_name'],
                                            values = ['claim_id',
                                                      'total_claim_costs',
                                                      'indemnity',
                                                      'high_display_complexity',
                                                      'display_already_litigated',],
                                            aggfunc = {'claim_id':'nunique',
                                                       'total_claim_costs':'mean',
                                                       'indemnity':'sum',
                                                       'high_display_complexity':'sum',
                                                       'display_already_litigated':'sum'},
                                            margins = True,
                                            fill_value = 0)

table2['indemnity'] = table2['indemnity'].div(table2['claim_id'], axis = 0)
table2['high_display_complexity'] = table2['high_display_complexity'].div(table2['claim_id'], axis=0)
table2['display_already_litigated'] = table2['display_already_litigated'].div(table2['claim_id'], axis=0)

table2.rename(columns = {'claim_id':'Number of Claims',
                        'total_claim_costs':'Average Claim Costs',
                        'indemnity':'Percentage of Indemnity Claims',
                        'high_display_complexity':'Percentage of Level 3 or Level 4 Claims',
                        'display_already_litigated':'Percentage of Litigated Claims',
                        'office':'Supervisor Name',
                        'adjuster_name':'Adjuster Name'},
                        inplace = True)

## adjuster - supervisor case load
table3 = claim_latest_open_score.pivot_table(index = ['adjuster_name', 'office'],
                                            values = ['claim_id',
                                                      'total_claim_costs',
                                                      'indemnity',
                                                      'high_display_complexity',
                                                      'display_already_litigated',],
                                            aggfunc = {'claim_id':'nunique',
                                                       'total_claim_costs':'mean',
                                                       'indemnity':'sum',
                                                       'high_display_complexity':'sum',
                                                       'display_already_litigated':'sum'},
                                            margins = True,
                                            fill_value = 0)

table3['indemnity'] = table3['indemnity'].div(table3['claim_id'], axis = 0)
table3['high_display_complexity'] = table3['high_display_complexity'].div(table3['claim_id'], axis=0)
table3['display_already_litigated'] = table3['display_already_litigated'].div(table3['claim_id'], axis=0)

table3.rename(columns = {'claim_id':'Number of Claims',
                        'total_claim_costs':'Average Claim Costs',
                        'indemnity':'Percentage of Indemnity Claims',
                        'high_display_complexity':'Percentage of Level 3 or Level 4 Claims',
                        'display_already_litigated':'Percentage of Litigated Claims',
                        'office':'Supervisor Name',
                        'adjuster_name':'Adjuster Name'},
                        inplace = True)


output_dict['caseload'] = {
        'Caseload Adjuster':{'data':table,'info':''},
        'Caseload Supervisor Adjuster':{'data':table2,'info':''},
        'Caseload Adjuster Supervisor':{'data':table3,'info':''}
        }




