sdq_common_config_dict = {
    "db" : "",
    "host" : "",
    "user_name" : "",
    "password" : ""
}

db_config_dict = {
    "db" : "",
    "host" : "",
    "user_name" : "",
    "password" : ""
}

account_id = 8

prod_version = None # 1 if checking prod data otherwise None

dqs_fetch_from = 'DB' # DB if dqs are reading from database otherwise MDD

mdd_config = {
    'study_name': 'TAS0612_101',
    'file_name': 'Master Data Definition_Taiho_TAS0612_101',
    'sheet_name': 'TAS0612_101_DQ',
    'row_header': 0,
    'format': 'relational_combined',
    'require_cols': ['Study ID', 'DQ Name', 'DQ Description', 'Standard Query text', 'Primary Form Name', 'Primary Visit Name', 'Primary Dataset', 'Primary Dataset Columns','Query Target', 'Library Usage (SAAMA Internal)', 'Relational Form Name', 'Relational Visit Name', 'Relational Dataset', 'Relational Dataset Columns', 'Dynamic Domain', 'Dynamic Columns'],
    'remove_newline_cols': ['Standard Query text', 'Primary Form Name', 'Primary Visit Name', 'Primary Dataset', 'Primary Dataset Columns','query_target', 'Relational Form Name', 'Relational Visit Name', 'Relational Dataset', 'Relational Dataset Columns', 'Dynamic Domain', 'Dynamic Columns']
}
