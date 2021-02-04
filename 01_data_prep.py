# -*- coding: utf-8 -*-
"""
Created on Sun Mar 24 00:14:37 2019

@author: manqi.w
"""

'''
Load claim tables from SQL to run:
    02_caseload.py
    03_alert_distribution.py
Load claim CIM CSV file to run:
    04_closure_rates.py
    05_01_AI_report.py
    06_claim_cost.py
Load claim snapshot CSV file to run:
    05_02_AI_report.py
    07_indemnity_exposure.py
'''
# =============================================================================
# Read claim data from SQL
# =============================================================================
print('Loading data from SQL...')
conn = pymysql.connect(user = config['db']['db_user'],
                       passwd = config['db']['db_password'],
                       host = config['db']['db_host'],
                       port = int(config['db']['db_port']),
                       db = config['db']['db_name']
                       )
claim_app = pd.read_sql_query("select * from ca_catt_claim",conn).drop_duplicates()
score_app = pd.read_sql_query("select * from ca_catt_score",conn).drop_duplicates()
alert_snap = pd.read_sql_query("select * from ca_catt_alert_snapshot",conn).drop_duplicates()
alert_def = pd.read_sql_query("select * from ca_catt_alert_definition",conn).drop_duplicates()
#print(alert_def['is_configured'])
#map(ord, alert_def['is_configured'][0])
alert_def['is_configured'] = alert_def['is_configured'].apply(lambda x: ord(x))
alert_def['is_active'] = alert_def['is_active'].apply(lambda x: ord(x))
alert_cmpl = pd.read_sql_query("select * from ca_catt_alert_completed_user",conn).drop_duplicates()


## SUBSET OPEN CLAIMS WITH LATEST BATCH ID
claim_app['max_batch_id'] = claim_app.groupby(['claim_id'])['batch_id'].transform(max)
df_attributes(claim_app,'Claim app table')
#test = claim_app[['batch_id','max_batch_id','claim_id']]
claim_latest = claim_app[claim_app['batch_id'] == claim_app['max_batch_id']].drop_duplicates('claim_id')
df_attributes(claim_latest,'Claim with latest batch')
claim_latest_open = claim_latest[claim_latest['claim_status'].isin(['open','OPEN'])] 
df_attributes(claim_latest_open,'Open claim with latest batch')


# =============================================================================
# Read claim CIM csv file
# =============================================================================
print()
print('Loading claim cim data from CSV...')
cols_claims = ['claim_id','claim_type','claim_status','injury_date','carrier_notification_date',
               'closed_date','injured_worker_state','total_claim_paid','total_expense_paid',
               'total_indemnity_paid','total_medical_paid','denied_flag','attorney_involvement']
cols_date_claims = ['injury_date','closed_date', 'carrier_notification_date']

claim = get_cim_data(file_path = cim_files_path + config['data']['claim_file_name'],
                     usecols = cols_claims, 
                     datecols = cols_date_claims)
df_attributes(claim,'Before preprocessing, claim CIM table')

## Common feature engineering
claim['state'] = np.where(claim['injured_worker_state'].isnull(),'Others',claim['injured_worker_state'])
claim['claim_duration'] = (claim['closed_date'] - claim['carrier_notification_date']).dt.days

claim['injury_quarter'] = pd.PeriodIndex(pd.to_datetime(claim.injury_date), freq='Q')
claim['notif_quarter'] = pd.PeriodIndex(pd.to_datetime(claim.carrier_notification_date), freq='Q')
claim['closed_quarter']= pd.PeriodIndex(pd.to_datetime(claim.closed_date), freq='Q')

claim['injury_year'] = pd.to_datetime(claim.injury_date).dt.year
claim['notif_year'] = pd.to_datetime(claim.carrier_notification_date).dt.year
claim['closed_year']= pd.to_datetime(claim.closed_date).dt.year

## Check if there are any negative claim duration
print('There are {} claims with negative claim duration'.format(claim[claim['claim_duration']<0].shape[0]))
negative_claim_duration_list = claim[claim['claim_duration']<0]['claim_id']
#print(negative_claim_duration_list)
claim = claim[~claim['claim_id'].isin(negative_claim_duration_list)]
claim_org = claim.copy()
df_attributes(claim,'After preprocessing, claim CIM table')

## Create a subset of claims  ---  claim filter for closure rates and claim cost analysis
valid_claim_filter = (claim['claim_type'].isin(config['claim_filter']['claim_type_filter'])) &\
                      (claim['denied_flag'].fillna('NO') == 'NO') &\
                      (claim['total_claim_paid'] > 35) &\
                      (claim['injury_date'] > pd.to_datetime(config['claim_filter']['min_claim_date']))
claim_sub = claim.loc[valid_claim_filter]
df_attributes(claim_sub,'After subsetting, claim_sub table')


# =============================================================================
# Read claim snapshot csv file
# =============================================================================
print()
print('Loading claim snapshot data from CSV...')
cols_claims = ['claim_id','claim_type','claim_status','injury_date','carrier_notification_date',
               'closed_date','injured_worker_state','total_claim_paid','total_expense_paid',
               'total_indemnity_paid','total_medical_paid','denied_flag','attorney_involvement','file_date']
cols_date_claims = ['injury_date','closed_date', 'carrier_notification_date','file_date']

claimss = get_snapshot_data(file_path = cim_files_path + config['data']['claim_snapshot_file_name'],
                            usecols = cols_claims, 
                            datecols = cols_date_claims)
df_attributes(claimss,'Claim snapshot table')
print('Check number of NULL values in each column:')
print(claimss.isnull().sum())
print('Range of file date is from {} to {}'.
      format(min(claimss['file_date']),max(claimss['file_date'])))
print('Range of injury date is from {} to {}'.
      format(min(claimss['injury_date']),max(claimss['injury_date'])))

