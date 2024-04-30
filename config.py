sdq_common_config_dict = {
    "db" : "sdq_prod",
    "host" : "prod-sdq.cbvts3gwaard.us-east-1.rds.amazonaws.com",
    "user_name" : "readonly",
    "password" : "ReaD@Onel23$"
}

db_config_dict = {
    "db" : "SDQA8DB",
    "host" : "prod-sdq.cbvts3gwaard.us-east-1.rds.amazonaws.com",
    "user_name" : "readonly",
    "password" : "ReaD@Onel23$"
}

account_id = 8

prod_version = None # 1 if checking prod data otherwise None

dqs_fetch_from = 'MDD' # DB if dqs are reading from database otherwise MDD

mdd_config = {
    'study_name': 'TAS0612_101',
    'file_name': 'Master Data Definition_Taiho_TAS0612_101',
    'sheet_name': 'TAS0612_101_DQ',
    'row_header': 0,
    'format': 'relational_combined',
    'require_cols': ['Study ID', 'DQ Name', 'DQ Description', 'Standard Query text', 'Primary Form Name', 'Primary Visit Name', 'Primary Dataset', 'Primary Dataset Columns','Query Target', 'Library Usage (SAAMA Internal)', 'Relational Form Name', 'Relational Visit Name', 'Relational Dataset', 'Relational Dataset Columns', 'Dynamic Domain', 'Dynamic Columns'],
    'remove_newline_cols': ['Standard Query text', 'Primary Form Name', 'Primary Visit Name', 'Primary Dataset', 'Primary Dataset Columns','query_target', 'Relational Form Name', 'Relational Visit Name', 'Relational Dataset', 'Relational Dataset Columns', 'Dynamic Domain', 'Dynamic Columns']
}

# mdd_config = {
#     'TAS3351_010': {
#     'study_name': 'TAS3351_010',
#     'file_name': 'MDD_Taiho_TAS3351_010',
#     'sheet_name': 'TAS3351_010_DQ',
#     'row_header': 1
#     },
#     'TAS6417_201': {
#     'study_name': 'TAS6417_201',
#     'file_name': 'MDD_Taiho_TAS6417_201',
#     'sheet_name': 'TAS6417_201_DQ',
#     'row_header': 1
#     }
# }


# mdd_config = {
#     'study_name': 'TAS3351_010',
#     'file_name': 'MDD_Taiho_TAS3351_010',
#     'sheet_name': 'TAS3351_010_DQ',
#     'row_header': 1,
#     'format': 'relational_combined',
    # 'require_cols': ['Study ID', 'DQ Name', 'DQ Description', 'Standard Query text', 'Primary Form Name', 'Primary Visit Name', 'Primary Dataset', 'Primary Dataset Columns','Relational Form Name_1', 'Relational Visit Name_1', 'Relational Dataset_1', 'Relational Dataset Columns_1', 'Relational Form Name_2', 'Relational Visit Name_2', 'Relational Dataset_2', 'Relational Dataset Columns_2', 'Relational Form Name_3', 'Relational Visit Name_3', 'Relational Dataset_3', 'Relational Dataset Columns_3', 'Relational Form Name_4', 'Relational Visit Name_4', 'Relational Dataset_4', 'Relational Dataset Columns_4', 'Relational Form Name_5', 'Relational Visit Name_5', 'Relational Dataset_5', 'Relational Dataset Columns_5', 'Primary Dynamic Domain', 'Primary Dynamic Columns', 'Relational Dynamic Domain_1', 'Relational Dynamic Columns_1', 'Relational Dynamic Domain_2', 'Relational Dynamic Columns_2', 'Relational Dynamic Domain_3', 'Relational Dynamic Columns_3', 'Relational Dynamic Domain_4', 'Relational Dynamic Columns_4', 'Relational Dynamic Domain_5', 'Relational Dynamic Columns_5', 'Query Target', 'Library Usage (SAAMA Internal)', 'Origin (If Inherent DQ)'],
    # 'remove_newline_cols': ['Standard Query text', 'Primary Form Name', 'Primary Visit Name', 'Primary Dataset', 'Primary Dataset Columns','Relational Form Name_1', 'Relational Visit Name_1', 'Relational Dataset_1', 'Relational Dataset Columns_1', 'Relational Form Name_2', 'Relational Visit Name_2', 'Relational Dataset_2', 'Relational Dataset Columns_2', 'Relational Form Name_3', 'Relational Visit Name_3', 'Relational Dataset_3', 'Relational Dataset Columns_3', 'Relational Form Name_4', 'Relational Visit Name_4', 'Relational Dataset_4', 'Relational Dataset Columns_4', 'Relational Form Name_5', 'Relational Visit Name_5', 'Relational Dataset_5', 'Relational Dataset Columns_5', 'Primary Dynamic Domain', 'Primary Dynamic Columns', 'Relational Dynamic Domain_1', 'Relational Dynamic Columns_1', 'Relational Dynamic Domain_2', 'Relational Dynamic Columns_2', 'Relational Dynamic Domain_3', 'Relational Dynamic Columns_3', 'Relational Dynamic Domain_4', 'Relational Dynamic Columns_4', 'Relational Dynamic Domain_5', 'Relational Dynamic Columns_5', 'query_target']
# }
