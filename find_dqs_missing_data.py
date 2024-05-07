import psycopg2
import pandas as pd
import warnings
import datetime

warnings.filterwarnings("ignore")

from config import sdq_common_config_dict, account_id, db_config_dict, prod_version, dqs_fetch_from, mdd_config

class copy_dqs:
    def __init__(self):
        self.sdq_common_conn = psycopg2.connect(
            host=sdq_common_config_dict["host"],
            database=sdq_common_config_dict["db"],
            user=sdq_common_config_dict["user_name"],
            password=sdq_common_config_dict["password"]
        )

        self.sdq_common_cursor = self.sdq_common_conn.cursor()

        self.sdq_dbconn = psycopg2.connect(
            host=db_config_dict["host"],
            database=db_config_dict["db"],
            user=db_config_dict["user_name"],
            password=db_config_dict["password"]
        )

        self.sdq_dbcursor = self.sdq_dbconn.cursor()

    def get_study_id(self, account_id, study_name):
        self.sdq_common_cursor.execute(f"select id from sdq_common.study where account_id = {account_id} and study_id = '{study_name}'")
        study_id = self.sdq_common_cursor.fetchone()
        if study_id:
            return study_id[0]
        return None

    def get_missing_map_data(self, account_id, study_id, table_name, col_name):
        map_data_df = pd.read_sql_query(f"select {col_name} from account_{account_id}_study_{study_id}.{table_name}", self.sdq_dbconn)
        map_list = map_data_df[col_name].tolist()
        return map_list

    def finding_missing_vars(self):
        start_time = datetime.datetime.now()
            
        map_form = {}
        map_visit = {}
        item_dataset_vars = {}

        if prod_version is None:
            prod_version_val = 'null'
            env_name = 'UAT'
        else:
            prod_version_val = 'not null'
            env_name = 'PROD'

        if dqs_fetch_from == 'DB':
            data = pd.read_sql_query(
                f"""select * from (
                select row_number() over (partition by name, sec.study_id order by ec.version desc) as r,
                FIRST_VALUE(inherit_type) over (partition by sec.study_id, name order by ec.version) f_val, ec.id, ec.account_id, ec.name, ec.description, ec.dataset, ec.version, ec.origin, ec.query_target, ec.dynamic_panel_config, ec.inherit_type, sec.study_id, sec.prod_version, s.study_id as study_name
                from sdq_common.edit_check ec inner join sdq_common.study_edit_check sec on sec.ec_id = ec.id inner join sdq_common.study s on s.id = sec.study_id where ec.account_id = {account_id} and ec.is_deleted is false and sec.prod_version is {prod_version_val}) as b where r=1 and f_val = 'copy'""",
                self.sdq_common_conn)

            print('before len', len(data))
            # Remove studies TAS6417_301_SDQ(18)
            data = data[data['study_name'].str.strip() != 'TAS6417_301_SDQ']

            output_file_name = 'missing_variables_studywise_' + env_name

        else:
            data = pd.read_excel(f"{mdd_config['file_name']}.xlsx", sheet_name=f"{mdd_config['sheet_name']}", header=mdd_config['row_header'])
            data = data[mdd_config['require_cols']] 
            data = data.rename(columns={'DQ Name': 'name', 'DQ Description': 'description', 'Query Target': 'query_target', 'Origin (If Inherent DQ)': 'origin', 'Library Usage (SAAMA Internal)': 'inherit_type'})   
            data["Study Name"] = mdd_config['study_name']
            new_line_cols = mdd_config['remove_newline_cols']
            data.fillna('', inplace=True)
            data[new_line_cols] = data[new_line_cols].replace('\n', '', regex=True)

            # get study id for MDD DQs
            study_id = self.get_study_id(account_id, mdd_config['study_name'])
            if not study_id:
                print('Study id is not available for this study')
                return
            
            output_file_name = 'MDD_missing_variables_' + mdd_config['study_name'] + '_' + env_name
    
        preconf_df = pd.read_sql_query(f"select sd.id, sd.study_id, sd.name as domain_name, sdv.study_domain_id, sdv.name as variable_name from sdq_common.study_domain sd inner join sdq_common.study_dataset_variable sdv on sd.id = sdv.study_domain_id where sd.account_id = {account_id}",
                                       self.sdq_common_conn)
        
        final_df = pd.DataFrame()
        for ind in range(len(data)):
            record = data.iloc[[ind]]

            # Construct DQ dataset from MDD
            if dqs_fetch_from == 'MDD':
                dataset = {
                    'primary': {
                        "domain": str(record['Primary Dataset'].values[0]).strip(), 
                        "columns": str(record['Primary Dataset Columns'].values[0]).strip().split(','), 
                        "form_name": str(record['Primary Form Name'].values[0]).strip().split(','), 
                        "visit_name": str(record['Primary Visit Name'].values[0]).strip().split(',')
                        }
                }

                if mdd_config['format'] != 'relational_combined':
                    dynamic_panel_config = []
                    dynamic_panel_config.append({'domain': record['Primary Dynamic Domain'].values[0], 
                                        'columns': str(record['Primary Dynamic Columns'].values[0]).strip().split(',')})

                    relational = []
                    for i in range(5):
                        
                        if pd.isna(record[f'Relational Dataset_{i+1}'].values[0]) or str(record[f'Relational Dataset_{i+1}'].values[0]).strip() == '':
                            break

                        rl_dataset = {
                            "domain": str(record[f'Relational Dataset_{i+1}'].values[0]).strip(), 
                            "columns": str(record[f'Relational Dataset Columns_{i+1}'].values[0]).strip().split(','), 
                            "form_name": str(record[f'Relational Form Name_{i+1}'].values[0]).strip().split(','), 
                            "visit_name": str(record[f'Relational Visit Name_{i+1}'].values[0]).strip().split(',')
                            }
                        relational.append(rl_dataset)

                        if str(record[f'Relational Dynamic Domain_{i+1}'].values[0]).strip() != '' and pd.notna(record[f'Relational Dynamic Domain_{i+1}'].values[0]):
                            dynamic_panel_config.append({'domain': record[f'Relational Dynamic Domain_{i+1}'].values[0], 
                                            'columns': str(record[f'Relational Dynamic Columns_{i+1}'].values[0]).strip().split(',')})
                else:
                    rl_domains = str(record['Relational Dataset'].values[0]).strip().split(',')
                    rl_forms = str(record['Relational Form Name'].values[0]).strip().split(',')
                    rl_visits = str(record['Relational Visit Name'].values[0]).strip().split(',')
                    rl_columns = str(record['Relational Dataset Columns'].values[0]).strip().split(',')

                    relational = []
                    for rl_dom in rl_domains:
                        dom_name = str(rl_dom).strip()
                        rl_dataset = {
                            "domain": str(rl_dom).strip(), 
                            "columns": [x.lstrip(f'{dom_name}').lstrip('.') for x in rl_columns if x.startswith(f'{dom_name}.')], 
                            "form_name": [x.lstrip(f'{dom_name}').lstrip('.') for x in rl_forms if x.startswith(f'{dom_name}.')], 
                            "visit_name": [x.lstrip(f'{dom_name}').lstrip('.') for x in rl_visits if x.startswith(f'{dom_name}.')]
                            }
                        relational.append(rl_dataset)

                    dynamic_panel_config = []
                    dynamic_domains = str(record['Dynamic Domain'].values[0]).strip().split(',')
                    dy_columns = str(record['Dynamic Columns'].values[0]).strip().split(',')
                    for dy_dom in dynamic_domains:
                        dy_dom = str(dy_dom).strip()
                        dynamic_panel_config.append({'domain': dy_dom, 
                                        'columns': [x.lstrip(f'{dy_dom}').lstrip('.') for x in dy_columns if x.startswith(f'{dy_dom}.')]
                                        })

                print('relational', relational, 'dynamic_panel_config', dynamic_panel_config)

                dataset['relational'] = relational
                record['dataset'] = (dataset, )
                record['dynamic_panel_config'] = (dynamic_panel_config, )
                record['study_name'] = mdd_config['study_name']
                record['study_id'] = study_id
                print('study id', record['study_id'].values[0])
            
            dataset = record['dataset'].values[0]
            primary_dataset = dataset['primary']
            relational_dataset = dataset['relational']
            dynamic_panel = record['dynamic_panel_config'].values[0]
            dq_description = str(record['description'].values[0])
            print("DQ name", record['name'].values[0])
            dq_name = record['name'].values[0]
            origin = record['origin'].values[0] if 'origin' in record.columns.tolist() else ''
            inherit_type = record['inherit_type'].values[0] # if 'inherit_type' in record.columns.tolist() else ''
            study_id = record['study_id'].values[0]

            # study_name = self.get_study_name(account_id, study_id)
            study_name = record['study_name'].values[0]
            print('study_name', study_name)
            if not item_dataset_vars.get(study_name):
                item_dataset_vars[study_name] = {}

            if not map_form.get(study_name):
                map_form[study_name] = self.get_missing_map_data(account_id, study_id, 'map_form', 'form_nm')

            form_def_list = ['external', 'External']
            form_values = primary_dataset['form_name']
            prim_form_list = map_form[study_name]
            prim_form_list = [x.lower() for x in prim_form_list]
            prim_form_missing = [x for x in form_values if x.lower().strip() not in prim_form_list and x.lower().strip() not in form_def_list]
            prim_form_missing = ', '.join(prim_form_missing)
    
            if not map_visit.get(study_name):
                map_visit[study_name] = self.get_missing_map_data(account_id, study_id, 'map_visit', 'visit_nm')

            visit_def_list = ['All', 'all']
            visit_values = primary_dataset['visit_name']
            prim_visit_list = map_visit[study_name]
            prim_visit_list = [x.lower() for x in prim_visit_list]
            prim_visit_missing = [x for x in visit_values if x.lower().strip() not in prim_visit_list and x.lower().strip() not in visit_def_list]
            prim_visit_missing = ', '.join(prim_visit_missing)

            stg_pred_base_cols = ['formname', 'itemset_ix', 'subjid', 'siteno', 'visit_nm', 'visit_ix', 'visit_id', 'form_id',
                                'form_ix', 'form_index', 'domain', 'sitemnemonic', 'FORMNAME', 'ITEMSET_IX', 'SUBJID', 'SITENO', 'VISIT_NM', 'VISIT_IX', 'VISIT_ID', 'FORM_ID', 'FORM_IX', 'FORM_INDEX', 'DOMAIN', 'SITEMNEMONIC']

            # Checking Primary dataset
            prim_domain_name = primary_dataset['domain']
            if not prim_domain_name:
                continue
            
            prim_dataset_df = preconf_df[(preconf_df['study_id'] == study_id) & (preconf_df['domain_name'] == prim_domain_name)]
            primary_dataset_variables = prim_dataset_df['variable_name'].tolist()

            for x in dynamic_panel:
                if x['domain'] == prim_domain_name:
                    if isinstance(x['columns'], dict):
                        primary_dataset['columns'] = primary_dataset['columns'] + list(x['columns'].keys())
                    if isinstance(x['columns'], list):
                        primary_dataset['columns'] = primary_dataset['columns'] + x['columns']

            primary_dataset['columns'] = list(set(primary_dataset['columns']))

            primary_dataset['columns'] = [x.strip() for x in primary_dataset['columns']]

            prim_diff_vars = [column for column in primary_dataset['columns'] if
                            column not in primary_dataset_variables and column not in stg_pred_base_cols]

            dataset_sql = f"select distinct js.key from account_{account_id}_study_{study_id}.stg_pred sp, json_each_text(item_dataset::json) as js where domain = '{prim_domain_name}' and iudflag != 'D'"
            key_name = prim_domain_name
            if len(primary_dataset['form_name']) > 0:
                prim_formname = primary_dataset['form_name'][0]
                if 'adverse events' in str(prim_formname).lower():
                    prim_formname = 'Adverse Events'
                    dataset_sql = dataset_sql + f" and formname like '%Adverse Events%'"
                else:
                    dataset_sql = dataset_sql + f" and formname = '{prim_formname}'"

                key_name = key_name + '_' + prim_formname

            print('key_name', key_name)
            if not item_dataset_vars.get(key_name):
                prim_db_keys = pd.read_sql_query(dataset_sql, self.sdq_dbconn)
                prim_db_keys = prim_db_keys['key'].tolist()
                item_dataset_vars[study_name][key_name] = prim_db_keys
                print('item_dataset_vars', item_dataset_vars)
            else:
                prim_db_keys = item_dataset_vars[study_name][key_name]

            prim_diff_vars_db = [column for column in primary_dataset['columns'] if
                                column not in prim_db_keys and column not in stg_pred_base_cols] if len(prim_db_keys) else []
            
            prim_des_vars = [column for column in prim_diff_vars if column in dq_description]
            prim_des_stg_var = [column for column in prim_diff_vars_db if column in dq_description]
            final_prim_des_vars = prim_des_vars + prim_des_stg_var

            query_target = record['query_target'].values[0]
            missing_qt = ''
            if query_target not in primary_dataset_variables or (len(prim_db_keys) and query_target not in prim_db_keys):
                missing_qt = query_target
            
            print('prim_diff_vars', prim_diff_vars, 'prim_diff_vars_db', prim_diff_vars_db)
            
            prim_diff_vars = ', '.join(prim_diff_vars)
            prim_diff_vars_db = ', '.join(prim_diff_vars_db)

            data_dict = {'Study Name': study_name, 'DQ Name': dq_name, 'Origin': origin, 'Inherit Type': inherit_type, 'Description': dq_description, 'Primary Domain': prim_domain_name,
                                'Primary Forms': prim_form_missing, 'Primary Visits': prim_visit_missing,
                                'Primary Preconf Variables': prim_diff_vars,'Primary Dataset Variables': prim_diff_vars_db,
                                'Query Target': missing_qt}

            # Checking Relational dataset
            rl_diff_vars = {}

            data_df = pd.DataFrame()
            if len(relational_dataset) > 0 and relational_dataset[0]['domain'] != '':
                final_rl_des_vars = []
                prim_values_add = True
                for rl_df in relational_dataset:
                    copy_dict = data_dict.copy()

                    rl_domain_name = rl_df['domain']
                    if not rl_domain_name:
                        continue

                    rl_form_values = rl_df['form_name']
                    rl_form_list = map_form[study_name]
                    rl_form_list = [x.lower() for x in rl_form_list]
                    rl_form_missing = [x for x in rl_form_values if x.lower().strip() not in rl_form_list and x.lower().strip() not in form_def_list]
                    rl_form_missing = ', '.join(rl_form_missing)

                    rl_visit_values = rl_df['visit_name']
                    rl_visit_list = map_visit[study_name]
                    rl_visit_list = [x.lower() for x in rl_visit_list]
                    rl_visit_missing = [x for x in rl_visit_values if x.lower().strip() not in rl_visit_list and x.lower().strip() not in visit_def_list]
                    rl_visit_missing = ', '.join(rl_visit_missing)
                   
                    if rl_form_missing or rl_visit_missing:
                        copy_dict['Relational Domain'] = rl_domain_name
                        copy_dict['Relational Forms'] = rl_form_missing
                        copy_dict['Relational Visits'] = rl_visit_missing

                    rl_dataset_df = preconf_df[(preconf_df['study_id'] == study_id) & (preconf_df['domain_name'] == rl_domain_name)]
                    rl_dataset_variables = rl_dataset_df['variable_name'].tolist()

                    for x in dynamic_panel:
                        if x['domain'] == rl_domain_name:
                            if isinstance(x['columns'], dict):
                                rl_df['columns'] = rl_df['columns'] + list(x['columns'].keys())
                            if isinstance(x['columns'], list):
                                rl_df['columns'] = rl_df['columns'] + x['columns']

                    rl_df['columns'] = list(set(rl_df['columns']))
                    rl_df['columns'] = [x.strip() for x in rl_df['columns']]
                    diff_vars = [column for column in rl_df['columns'] if
                                column not in rl_dataset_variables and column not in stg_pred_base_cols]
                    if len(diff_vars) > 0:
                        if not rl_diff_vars.get(rl_domain_name):
                            rl_diff_vars[rl_domain_name] = {}
                        rl_diff_vars[rl_domain_name]['preconf_vars'] = diff_vars

                    dataset_sql = f"select distinct js.key from account_{account_id}_study_{study_id}.stg_pred sp, json_each_text(item_dataset::json) as js where domain = '{rl_domain_name}' and iudflag != 'D'"
                    key_name = rl_domain_name
                    if len(rl_df['form_name']) > 0:
                        rl_formname = rl_df['form_name'][0]
                        if 'adverse events' in str(rl_formname).lower():
                            rl_formname = 'Adverse Events'
                            dataset_sql = dataset_sql + f" and formname like '%Adverse Events%'"
                        else:
                            dataset_sql = dataset_sql + f" and formname = '{rl_formname}'"

                        key_name = key_name + '_' + rl_formname
                    
                    if not item_dataset_vars[study_name].get(key_name):
                        rl_db_keys = pd.read_sql_query(dataset_sql, self.sdq_dbconn)
                        rl_db_keys = rl_db_keys['key'].tolist()
                        item_dataset_vars[study_name][key_name] = rl_db_keys
                    else:
                        rl_db_keys = item_dataset_vars[study_name][key_name]

                    rl_diff_vars_db = [column for column in rl_df['columns'] if
                                    column not in rl_db_keys and column not in stg_pred_base_cols] if len(rl_db_keys) else []
                    if len(rl_diff_vars_db) > 0:
                        if not rl_diff_vars.get(rl_domain_name):
                            rl_diff_vars[rl_domain_name] = {}
                        rl_diff_vars[rl_domain_name]['dataset_vars'] = rl_diff_vars_db

                    rel_des_vars = [column for column in diff_vars if column in dq_description]
                    rel_des_stg_var = [column for column in rl_diff_vars_db if column in dq_description]
                    final_rl_des_vars = final_rl_des_vars + rel_des_vars + rel_des_stg_var
                    
                    rl_missing_vars, rl_stg_pred_vars = '', ''
                    if rl_diff_vars.get(rl_domain_name):
                        copy_dict['Relational Domain'] = rl_domain_name
                        rl_missing_vars = ','.join(rl_diff_vars[rl_domain_name]['preconf_vars']) if rl_diff_vars[rl_domain_name].get('preconf_vars') else ''
                        copy_dict['Relational Preconf Variables'] = rl_missing_vars

                        rl_stg_pred_vars = ','.join(rl_diff_vars[rl_domain_name]['dataset_vars']) if rl_diff_vars[rl_domain_name].get('dataset_vars') else ''
                        copy_dict['Relational Dataset Variables'] = rl_stg_pred_vars

                        copy_dict['Relational Forms'] = rl_form_missing
                        copy_dict['Relational Visits'] = rl_visit_missing

                    print('copy_dict', copy_dict)

                    df1 = pd.DataFrame([copy_dict])

                    if ((prim_diff_vars != '' or prim_diff_vars_db != '' or missing_qt != '' or prim_form_missing != '' or prim_visit_missing != '') and prim_values_add) or \
                        (rl_missing_vars != '' or rl_stg_pred_vars != '' or rl_form_missing != '' or rl_visit_missing != ''):
                        data_df = data_df.append(df1, ignore_index = True)
                        prim_values_add = False

                final_des_vars = final_prim_des_vars + final_rl_des_vars
                final_des_vars = list(set(final_des_vars))
                final_des_vars = ', '.join(final_des_vars)
                data_df['Description Missing Variables'] = final_des_vars
            else:
                if prim_diff_vars != '' or prim_diff_vars_db != '' or missing_qt != '' or prim_form_missing != '' or prim_visit_missing != '':
                    df2 = pd.DataFrame([data_dict])
                    data_df = data_df.append(df2, ignore_index = True)

            if len(data_df) > 0:
                print('data_df', data_df.to_dict(orient='records'))
                final_df = final_df.append(data_df, ignore_index = True)
            
        if len(final_df) > 0:
            final_df = final_df.drop_duplicates()
            final_df.sort_values(["Study Name","DQ Name"], inplace=True)
            final_df1 = final_df.set_index(["Study Name","DQ Name", "Origin", "Inherit Type", "Description", "Primary Domain", "Primary Forms", "Primary Visits", "Primary Preconf Variables", "Primary Dataset Variables", "Query Target", "Description Missing Variables"])
            final_df1.style.set_properties(**{'border': 'black', 'color': 'black'})

            final_df1.to_excel(f'output/{output_file_name}.xlsx', sheet_name="Data missing sheet", merge_cells=True)
            print('DataFrame is written to Excel File successfully.')
            end_time = datetime.datetime.now()
            print('Starting time of script',start_time.strftime("%H:%M:%S"))
            print('Ending time of script',end_time.strftime("%H:%M:%S"))
        else:
            print("There are no missing records found.")

obj = copy_dqs()
obj.finding_missing_vars()