from airflow_utils.factories import get_app_config, open_dbt_dag, build_dbt_task, build_dag_trigger
from airflow_utils.error_handlers import create_error_handler_group

default_params = {
    'gdw_user': 'chanci',
    'gdw_passwd': 'Empty',
    'gdw_server': 'cbaS',
    'gdw_acct_db': 'adhltu',
    'uti_db': 'adhltu',
    'base_dir': '/cba_app/HLT/DEV',
    'run_stream': 'CSE_L4_PRE_PROC',
    'smtp_server': '10.26.32.12',
    'send_mail': 'cindy.chang@cba.co',
    'receive_mail': 'cindy.chang@cba.co',
    'etl_process_dt': '20080624',
    'gdw_amp_max': 16,
    'ctl_database': 'HLTUDEV',
    'ctl_user': 'HLTU_CTL',
    'ctl_passwd': 'Empty',
    'ctl_schema': 'HLTU_CTL',
    'gdw_acct_vw': 'adhltu',
    'default_dt': '11111118',
    'cntrl_m': 'NoMVS',
    'gdw_stg_db': 'adhltu',
    'app_release': 'CSEL4',
    'gdw_proc_db': 'aphltu',
    'stg_user': 'HLTU_STG',
    'stg_database': 'HLTUDEV',
    'stg_schema': 'HLTU_STG',
    'stg_passwd': 'Empty',
    'default_null_value': '0x0',
    'gdw_fs_vw': 'adhltu',
    'gdw_cust_db': 'adhltu',
    'init_load_flag': 'N',
    'delim_count': 8,
    'gdw_cust_vw': 'adhltu',
    'gdw_grd_vw': 'adhltu',
    'gdw_cms_db': 'adhltu'
}

app_config = get_app_config('CSEL4')

dag = open_dbt_dag(
    dag_id='sq00csel4preload',
    description='sq00csel4preload',
    default_params=default_params,
    app_config=app_config
)


processrunstreamstatuscheck_parameters = {
    'base_dir': "{{ params.get('base_dir') }}",
    'etl_process_dt': "{{ params.get('etl_process_dt') }}",
    'ctl_database': "{{ params.get('ctl_database') }}",
    'ctl_user': "{{ params.get('ctl_user') }}",
    'ctl_passwd': "{{ params.get('ctl_passwd') }}",
    'run_stream': "{{ params.get('run_stream') }}",
    'app_release': "{{ params.get('app_release') }}",
    'ctl_schema': "{{ params.get('ctl_schema') }}"
}

processrunstreamstatuscheck = build_dbt_task(
    task_id='processrunstreamstatuscheck',
    select='cse_dataload.24processmetadata.processrunstreamstatuscheck',
    vars=processrunstreamstatuscheck_parameters,
    app_name='CSEL4',
    dag=dag
)

utilprosisacprevloadcheck_parameters = {
    'base_dir': "{{ params.get('base_dir') }}",
    'gdw_server': "{{ params.get('gdw_server') }}",
    'gdw_user': "{{ params.get('gdw_user') }}",
    'gdw_passwd': "{{ params.get('gdw_passwd') }}",
    'gdw_acct_db': "{{ params.get('gdw_acct_db') }}",
    'etl_process_dt': "{{ params.get('etl_process_dt') }}",
    'app_release': "{{ params.get('app_release') }}",
    'send_mail': "{{ params.get('send_mail') }}",
    'smtp_server': "{{ params.get('smtp_server') }}",
    'receive_mail': "{{ params.get('receive_mail') }}",
    'scc_tera_truncate_string_with_null': 'empty'
}

utilprosisacprevloadcheck = build_dbt_task(
    task_id='utilprosisacprevloadcheck',
    select='cse_dataload.24processmetadata.utilprosisacprevloadcheck',
    vars=utilprosisacprevloadcheck_parameters,
    app_name='CSEL4',
    dag=dag
)

sq10initprocesskeysgen_parameters = {
    'gdw_user': "{{ params.get('gdw_user') }}",
    'gdw_passwd': "{{ params.get('gdw_passwd') }}",
    'gdw_server': "{{ params.get('gdw_server') }}",
    'gdw_acct_db': "{{ params.get('gdw_acct_db') }}",
    'base_dir': "{{ params.get('base_dir') }}",
    'run_stream': "{{ params.get('run_stream') }}",
    'smtp_server': "{{ params.get('smtp_server') }}",
    'send_mail': "{{ params.get('send_mail') }}",
    'receive_mail': "{{ params.get('receive_mail') }}",
    'etl_process_dt': "{{ params.get('etl_process_dt') }}",
    'ctl_database': "{{ params.get('ctl_database') }}",
    'ctl_user': "{{ params.get('ctl_user') }}",
    'ctl_passwd': "{{ params.get('ctl_passwd') }}",
    'ctl_schema': "{{ params.get('ctl_schema') }}",
    'app_release': "{{ params.get('app_release') }}",
    'gdw_acct_vw': "{{ params.get('gdw_acct_vw') }}",
    'gdw_proc_db': "{{ params.get('gdw_proc_db') }}",
    'gdw_grd_vw': "{{ params.get('gdw_grd_vw') }}"
}

sq10initprocesskeysgen = build_dag_trigger(
    task_id='sq10initprocesskeysgen',
    target_dag_id='sq10initprocesskeysgen',
    params=sq10initprocesskeysgen_parameters,
    dag=dag
)

sq20initgeneratemaplookups_parameters = {
    'gdw_user': "{{ params.get('gdw_user') }}",
    'gdw_passwd': "{{ params.get('gdw_passwd') }}",
    'gdw_server': "{{ params.get('gdw_server') }}",
    'base_dir': "{{ params.get('base_dir') }}",
    'run_stream': "{{ params.get('run_stream') }}",
    'smtp_server': "{{ params.get('smtp_server') }}",
    'send_mail': "{{ params.get('send_mail') }}",
    'receive_mail': "{{ params.get('receive_mail') }}",
    'etl_process_dt': "{{ params.get('etl_process_dt') }}",
    'gdw_acct_vw': "{{ params.get('gdw_acct_vw') }}",
    'app_release': "{{ params.get('app_release') }}",
    'stg_database': "{{ params.get('stg_database') }}",
    'stg_user': "{{ params.get('stg_user') }}",
    'stg_passwd': "{{ params.get('stg_passwd') }}",
    'stg_schema': "{{ params.get('stg_schema') }}",
    'gdw_grd_vw': "{{ params.get('gdw_grd_vw') }}"
}

sq20initgeneratemaplookups = build_dag_trigger(
    task_id='sq20initgeneratemaplookups',
    target_dag_id='sq20initgeneratemaplookups',
    params=sq20initgeneratemaplookups_parameters,
    dag=dag
)

processrunstreamfinishingpoint_parameters = {
    'run_stream': "{{ params.get('run_stream') }}",
    'etl_process_dt': "{{ params.get('etl_process_dt') }}",
    'base_dir': "{{ params.get('base_dir') }}",
    'ctl_database': "{{ params.get('ctl_database') }}",
    'ctl_user': "{{ params.get('ctl_user') }}",
    'ctl_passwd': "{{ params.get('ctl_passwd') }}",
    'ctl_schema': "{{ params.get('ctl_schema') }}"
}

processrunstreamfinishingpoint = build_dbt_task(
    task_id='processrunstreamfinishingpoint',
    select='cse_dataload.24processmetadata.processrunstreamfinishingpoint',
    vars=processrunstreamfinishingpoint_parameters,
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
    error_handler_factory=build_dbt_task,
    error_handler_params=processrunstreamerrorhandler_parameters,
    error_handler_task_id='processrunstreamerrorhandler',
    error_handler_kwargs={
        'select': 'cse_dataload.24processmetadata.processrunstreamerrorhandler',
        'app_name': 'CSEL4'
    },
    terminate_dag_on_error=True
)


# Set task dependencies start
processrunstreamstatuscheck >> \
utilprosisacprevloadcheck >> \
sq10initprocesskeysgen >> \
sq20initgeneratemaplookups >> \
processrunstreamfinishingpoint >> \
error_handler_group
# Set task dependencies end
