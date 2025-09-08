from airflow_utils.factories import get_app_config, open_dbt_dag, build_datastage_task
from airflow_utils.error_handlers import create_error_handler_group


default_params = {
    'gdw_user': 'suyantel',
    'gdw_passwd': 'Empty',
    'gdw_server': 'cba5',
    'gdw_acct_db': 'TDCSAD',
    'gdw_cust_db': 'TDCSCD',
    'util_db': 'PUTIL',
    'base_dir': 'cba_app__CSEL4__CSEL4DEV',
    'run_stream': 'CSE_COM_BUS_APP_CCL_APP',
    'smtp_server': '10.25.222.123',
    'send_mail': 'ruvi.dassanyake@cba.com.au',
    'receive_mail': 'ruvi.dassanyake@cba.com.au',
    'etl_process_dt': '20250807',
    'gdw_amp_max': 16,
    'ctl_database': 'CSE2DEV',
    'ctl_user': 'CSE4_CTL',
    'ctl_passwd': 'Empty',
    'ctl_schema': 'CSE4_CTL',
    'gdw_acct_vw': 'TDCSAD',
    'default_dt': '11111118',
    'gdw_stg_db': 'TDCSDSTG',
    'app_release': 'CSE',
    'stg_schema': 'CSE4_STG',
    'stg_database': 'CSE2DEV',
    'stg_user': 'CSE4_STG',
    'stg_passwd': 'Empty',
    'default_null_val': '0x0',
    'gdw_fs_vw': 'TDCSAD',
    'gdw_proc_db': 'SPCSGRD',
    'delim_count': 12,
    'gdw_cust_vw': 'TDCSCD',
    'init_load_flag': 'N',
    'gdw_grd_vw': 'SDCSGRD'
}

app_config = get_app_config('CSEL4')

dag = open_dbt_dag(
    dag_id='sq20appt_pdctldtmpdeltaldgdw',
    description='-',
    default_params=default_params,
    app_config=app_config
)

ldtmp_appt_pdctfrmxfm_parameters = {
    'gdw_user': "{{ params.get('gdw_user') }}",
    'gdw_passwd': "{{ params.get('gdw_passwd') }}",
    'base_dir': "{{ params.get('base_dir') }}",
    'run_stream': "{{ params.get('run_stream') }}",
    'gdw_server': "{{ params.get('gdw_server') }}",
    'gdw_stg_db': "{{ params.get('gdw_stg_db') }}",
    'gdw_amp_max': "{{ params.get('gdw_amp_max') }}",
    'gdw_util_db': "{{ params.get('util_db') }}",
    'etl_process_dt': "{{ params.get('etl_process_dt') }}",
    'cntrl_m': "{{ params.get('gdw_user') }}DIGITS(Refr_PK,$ReturnValue)",
    'tgt_table': 'TMP_APPT_pdct'
}
ldtmp_appt_pdctfrmxfm = build_datastage_task(
    task_id='ldtmp_appt_pdctfrmxfm',
    model_path='models/appt_pdct/ldtmp_appt_pdctfrmxfm',
    vars=ldtmp_appt_pdctfrmxfm_parameters,
    app_name='CSEL4',
    dag=dag
)

dltappt_pdctfrmtmp_appt_pdct_parameters = {
    'apt_no_sort_insertion': 'True',
    'base_dir': "{{ params.get('base_dir') }}",
    'gdw_user': "{{ params.get('gdw_user') }}",
    'gdw_passwd': "{{ params.get('gdw_passwd') }}",
    'run_stream': "{{ params.get('run_stream') }}",
    'gdw_server': "{{ params.get('gdw_server') }}",
    'gdw_acct_vw': "{{ params.get('gdw_acct_vw') }}",
    'etl_process_dt': "{{ params.get('etl_process_dt') }}",
    'gdw_stg_db': "{{ params.get('gdw_stg_db') }}",
    'gdw_fs_vw': "{{ params.get('gdw_fs_vw') }}",
    'default_null_value': "{{ params.get('default_null_val') }}",
    'refr_pk': "{{ params.get('refr_pk', 'DIGITS(Refr_PK_APPT_pdct,$ReturnValue)') }}",
    'tgt_table': 'APPT_pdct',
    'default_dt': "{{ params.get('default_dt') }}",
    'refr_pk_mirror': "{{ params.get('refr_pk_mirror', 'DIGITS(Refr_PK,$ReturnValue)') }}",
    'cc_tera_truncate_string_wi': 'empty'
}

dltappt_pdctfrmtmp_appt_pdct = build_datastage_task(
    task_id='dltappt_pdctfrmtmp_appt_pdct',
    model_path='models/appt_pdct/dltappt_pdctfrmtmp_appt_pdct',
    vars=dltappt_pdctfrmtmp_appt_pdct_parameters,
    app_name='CSEL4',
    dag=dag
)

ldapptpdctupd_parameters = {
    'gdw_user': "{{ params.get('gdw_user') }}",
    'gdw_passwd': "{{ params.get('gdw_passwd') }}",
    'base_dir': "{{ params.get('base_dir') }}",
    'run_stream': "{{ params.get('run_stream') }}",
    'gdw_server': "{{ params.get('gdw_server') }}",
    'gdw_acct_db': "{{ params.get('gdw_acct_db') }}",
    'gdw_amp_max': "{{ params.get('gdw_amp_max') }}",
    'gdw_util_db': "{{ params.get('util_db') }}",
    'etl_process_dt': "{{ params.get('etl_process_dt') }}",
    'cntrl_m': "{{ params.get('gdw_user') }}DIGITS(Refr_PK,$ReturnValue)",
    'tgt_table': '"appt_pdct"'
}

ldapptpdctupd = build_datastage_task(
    task_id='ldapptpdctupd',
    model_path='models/cse_dataload/18loadtogdw/appt_pdct/ldapptpdctupd',
    vars=ldapptpdctupd_parameters,
    app_name='CSEL4',
    dag=dag
)

ldapptpdctins_parameters = {
    'gdw_user': "{{ params.get('gdw_user') }}",
    'gdw_passwd': "{{ params.get('gdw_passwd') }}",
    'base_dir': "{{ params.get('base_dir') }}",
    'run_stream': "{{ params.get('run_stream') }}",
    'gdw_server': "{{ params.get('gdw_server') }}",
    'gdw_acct_db': "{{ params.get('gdw_acct_db') }}",
    'gdw_amp_max': "{{ params.get('gdw_amp_max') }}",
    'gdw_util_db': "{{ params.get('util_db') }}",
    'etl_process_dt': "{{ params.get('etl_process_dt') }}",
    'tgt_table': '"appt_pdct"',
    'cntrl_m': "{{ params.get('gdw_user') }}DIGITS(Refr_PK,$ReturnValue)",
}

ldapptpdctins = build_datastage_task(
    task_id='ldapptpdctins',
    model_path='models/appt_pdct/ldapptpdctins',
    vars=ldapptpdctins_parameters,
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
#     'tgt_table': 'APPT_pdct',
#     'proc_key': "{{ params.get('proc_key', 'DIGITS(Refr_PK,$ReturnValue)') }}",
#     'smtp_server': "{{ params.get('smtp_server') }}",
#     'send_mail': "{{ params.get('send_mail') }}",
#     'receive_mail': "{{ params.get('receive_mail') }}",
#     'cc_tera_truncate_string_with_null': 'empty'
# }

# gdwloadprocessmetadata = build_datastage_task(
#     task_id='gdwloadprocessmetadata',
#     model_path='models/cse_dataload/24processmetadata/gdwloadprocessmetadata',
#     vars=gdwloadprocessmetadata_parameters,
#     app_name='CSEL4',
#     dag=dag
# )

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
ldtmp_appt_pdctfrmxfm >> \
dltappt_pdctfrmtmp_appt_pdct >> \
ldapptpdctupd >> \
ldapptpdctins >> \
error_handler_group
# Set task dependencies end
