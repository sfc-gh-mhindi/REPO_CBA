from airflow_utils.factories import get_app_config, open_dbt_dag, build_datastage_task
from airflow_utils.error_handlers import create_error_handler_group

default_params = {
    'gdw_user': 'suyentel',
    'gdw_passwd': 'empty',  # Encrypted - using empty string for security
    'gdw_server': 'cbas',
    'gdw_acct_db': 'TDCSAD',
    'gdw_cust_db': 'TDCSCD',
    'util_db': 'PUTIL',
    'base_dir': '/cba_app/CSE14/CSE14DEV',
    'run_stream': 'CSE_COM_BUS_APP_CCL_APP',
    'smtp_server': '10.25.222.123',
    'send_mail': 'Dinesh.viswanathan@cba.com.au',
    'receive_mail': 'Dinesh.viswanathan@cba.com.au',
    'etl_process_dt': '20250807',
    'gdw_amp_max': 16,
    'ctl_database': 'CSE2DEV',
    'ctl_user': 'CSE4_CTL',
    'ctl_passwd': 'empty',  # Encrypted - using empty string for security
    'ctl_schema': 'CSE4_CTL',
    'gdw_acct_vw': 'TDCSAD',
    'default_dt': '11111118',
    'gdw_stg_db': 'TDCSDSTG',
    'app_release': 'CSE',
    'stg_schema': 'CSE4_STG',
    'stg_database': 'CSE2DEV',
    'stg_user': 'CSE4_STG',
    'stg_passwd': 'empty',  # Encrypted - using empty string for security
    'default_null_value': '0x0',
    'gdw_fs_vw': 'TDCSAD',
    'gdw_proc_db': 'SPCSGRD',
    'delim_count': 12,
    'gdw_cust_vw': 'TDCSCD',
    'init_load_flag': 'N',
    'gdw_grd_vw': 'SDCSGRD'
}

app_config = get_app_config('CSEL4')

dag = open_dbt_dag(
    dag_id='sq20dept_apptldtmpdeltaldgdw',
    description='sq20dept_apptldtmpdeltaldgdw',
    default_params=default_params,
    app_config=app_config
)


ldappdeptupd_parameters = {
    'gdw_user': "{{ params.get('gdw_user') }}",
    'gdw_passwd': "{{ params.get('gdw_passwd') }}",
    'base_dir': "{{ params.get('base_dir') }}",
    'run_stream': "{{ params.get('run_stream') }}",
    'gdw_server': "{{ params.get('gdw_server') }}",
    'gdw_acct_db': "{{ params.get('gdw_acct_db') }}",
    'gdw_amp_max': "{{ params.get('gdw_amp_max') }}",
    'gdw_util_db': "{{ params.get('gdw_util_db') }}",
    'etl_process_dt': "{{ params.get('etl_process_dt') }}",
    'cntrl_m': "{{ params.get('cntrl_m') }}",
    'tgt_table': '"dept_appt"',
    'tgt_index': 'dept_i'
}

ldappdeptupd = build_datastage_task(
    task_id='ldappdeptupd',
    model_path='models/cse_dataload/18loadtogdw/appt_dept/ldapptdeptupd',
    vars=ldappdeptupd_parameters,
    app_name='CSEL4',
    dag=dag
)

# gdwloadprocessmetadata_parameters = {
#     'gdw_user': "{{ params.get('gdw_user') }}",
#     'gdw_passwd': "{{ params.get('gdw_passwd') }}",
#     'run_stream': "{{ params.get('run_stream') }}",
#     'gdw_server': "{{ params.get('gdw_server') }}",
#     'gdw_acct_db': "{{ params.get('gdw_acct_db') }}",
#     'etl_process_dt': "{{ params.get('etl_process_dt') }}",
#     'base_dir': "{{ params.get('base_dir') }}",
#     'tgt_table': "{{ params.get('tgt_table') }}",
#     'proc_key': "{{ params.get('proc_key') }}",
#     'smtp_server': "{{ params.get('smtp_server') }}",
#     'send_mail': "{{ params.get('send_mail') }}",
#     'receive_mail': "{{ params.get('receive_mail') }}",
#     'scc_tera_truncate_string_with_null': "{{ params.get('scc_tera_truncate_string_with_null') }}"
# }

# gdwloadprocessmetadata = build_datastage_task(
#     task_id='gdwloadprocessmetadata',
#     model_path='models/cse_dataload/24processmetadata/gdwloadprocessmetadata',
#     vars=gdwloadprocessmetadata_parameters,
#     app_name='CSEL4',
#     dag=dag
# )


ldappdeptins_parameters = {
    'gdw_user': "{{ params.get('gdw_user') }}",
    'gdw_passwd': "{{ params.get('gdw_passwd') }}",
    'base_dir': "{{ params.get('base_dir') }}",
    'run_stream': "{{ params.get('run_stream') }}",
    'gdw_server': "{{ params.get('gdw_server') }}",
    'gdw_acct_db': "{{ params.get('gdw_acct_db') }}",
    'gdw_amp_max': "{{ params.get('gdw_amp_max') }}",
    'gdw_util_db': "{{ params.get('gdw_util_db') }}",
    'etl_process_dt': "{{ params.get('etl_process_dt') }}",
    'tgt_table': '"dept_appt"',
    'cntrl_m': "{{ params.get('cntrl_m') }}",
    'tgt_index': 'dept_i'
}

ldappdeptins = build_datastage_task(
    task_id='ldappdeptins',
    model_path='models/cse_dataload/18loadtogdw/appt_dept/ldapptdeptins',
    vars=ldappdeptins_parameters,
    app_name='CSEL4',
    dag=dag
)

processrunstreamerrorhandler_parameters = {
    'run_stream': "{{ params.get('run_stream') }}",
    'etl_process_dt': "{{ params.get('etl_process_dt') }}",
    'base_dir': "{{ params.get('base_dir') }}",
    'ctl_database': "{{ params.get('ctl_database') }}",
    'ctl_user': "{{ params.get('ctl_user') }}",
    'ctl_passwd': "{{ params.get('ctl_passwd') }}",
    'ctl_schema': "{{ params.get('ctl_schema') }}",
    'err_msg': 'Error in stream',
    'smtp_server': "{{ params.get('smtp_server') }}",
    'send_mail': "{{ params.get('send_mail') }}",
    'receive_mail': "{{ params.get('receive_mail') }}"
}

error_handler_group = create_error_handler_group(
    dag=dag,
    group_id='error_handler',
    error_processor_task_id='sequencefail',
    error_handler_factory=build_datastage_task,
    error_handler_params=processrunstreamerrorhandler_parameters,
    error_handler_task_id='processrunstreamerrorhandler',
    error_handler_kwargs={
        'model_path': 'models/cse_dataload/24processmetadata/processrunstreamerrorhandler',
        'app_name': 'CSEL4'
    },
    terminate_dag_on_error=True
)



# Set task dependencies start
ldappdeptupd >> \
ldappdeptins >> \
error_handler_group
# Set task dependencies end
