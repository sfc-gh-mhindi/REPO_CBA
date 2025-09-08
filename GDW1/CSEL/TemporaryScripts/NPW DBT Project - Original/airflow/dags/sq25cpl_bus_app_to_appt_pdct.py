from airflow_utils.factories import get_app_config, open_dbt_dag, build_datastage_task, build_dag_trigger
from airflow_utils.error_handlers import create_error_handler_group

default_params = {
    'gdw_user': 'sindih',
    'gdw_passwd': 'Empty',
    'gdw_server': 'cba5',
    'gdw_acct_db': 'TDCSAD',
    'gdw_cust_db': 'TDCSCD',
    'util_db': 'PUTIL',
    'base_dir': 'cba_app__CSEL4__CSEL4DEV',
    'run_stream': 'CSE_CPL_BUS_APP',
    'smtp_server': '10.25.222.123',
    'send_mail': 'halit.sindi@cba.com.au',
    'receive_mail': 'halit.sindi@cba.com.au',
    'etl_process_dt': '20250807',
    'gdw_amp_max': 16,
    'ctl_database': 'CSE2DEV',
    'ctl_user': 'CSE4_CTL',
    'ctl_passwd': 'Empty',
    'ctl_schema': 'CSE4_CTL',
    'gdw_acct_vw': 'TDCSAD',
    'default_dt': '11111118',
    'cntrl_m': 'APPT_DEPT',
    'gdw_stg_db': 'TDCSDSTG',
    'app_release': 'CSE14',
    'stg_schema': 'CSE4_STG',
    'stg_database': 'CSE2DEV',
    'stg_user': 'CSE4_STG',
    'stg_passwd': 'Empty',
    'default_null_value': '0x0',
    'gdw_fs_vw': 'TDCSAD',
    'gdw_proc_db': 'SPCSGRD',
    'delim_count': 5,
    'gdw_cust_vw': 'TDCSCD',
    'init_load_flag': 'N',
    'gdw_grd_vw': 'SDCSGRD',
    'gdw_cms_db': 'empty'
}

app_config = get_app_config('CSEL4')

dag = open_dbt_dag(
    dag_id='sq25cpl_bus_app_to_appt_pdct',
    description='sq25cpl_bus_app_to_appt_pdct',
    default_params=default_params,
    app_config=app_config
)

sq20appt_pdctldtmpdeltaldgdw_parameters = {
    'gdw_user': "{{ params.get('gdw_user') }}",
    'gdw_passwd': "{{ params.get('gdw_passwd') }}",
    'gdw_server': "{{ params.get('gdw_server') }}",
    'gdw_acct_db': "{{ params.get('gdw_acct_db') }}",
    'gdw_cust_db': "{{ params.get('gdw_cust_db') }}",
    'util_db': "{{ params.get('util_db') }}",
    'base_dir': "{{ params.get('base_dir') }}",
    'run_stream': "{{ params.get('run_stream') }}",
    'smtp_server': "{{ params.get('smtp_server') }}",
    'send_mail': "{{ params.get('send_mail') }}",
    'receive_mail': "{{ params.get('receive_mail') }}",
    'etl_process_dt': "{{ params.get('etl_process_dt') }}",
    'gdw_amp_max': "{{ params.get('gdw_amp_max') }}",
    'ctl_database': "{{ params.get('ctl_database') }}",
    'ctl_user': "{{ params.get('ctl_user') }}",
    'ctl_passwd': "{{ params.get('ctl_passwd') }}",
    'ctl_schema': "{{ params.get('ctl_schema') }}",
    'gdw_acct_vw': "{{ params.get('gdw_acct_vw') }}",
    'default_dt': "{{ params.get('default_dt') }}",
    'gdw_stg_db': "{{ params.get('gdw_stg_db') }}",
    'app_release': "{{ params.get('app_release') }}",
    'stg_schema': "{{ params.get('stg_schema') }}",
    'stg_database': "{{ params.get('stg_database') }}",
    'stg_user': "{{ params.get('stg_user') }}",
    'stg_passwd': "{{ params.get('stg_passwd') }}",
    'default_null_value': "{{ params.get('default_null_value') }}",
    'gdw_fs_vw': "{{ params.get('gdw_fs_vw') }}",
    'gdw_proc_db': "{{ params.get('gdw_proc_db') }}",
    'delim_count': "{{ params.get('delim_count') }}",
    'gdw_cust_vw': "{{ params.get('gdw_cust_vw') }}",
    'init_load_flag': "{{ params.get('init_load_flag') }}",
    'gdw_grd_vw': "{{ params.get('gdw_grd_vw') }}"
}

sq20appt_pdctldtmpdeltaldgdw = build_dag_trigger(
    task_id='sq20appt_pdctldtmpdeltaldgdw',
    target_dag_id='sq20appt_pdctldtmpdeltaldgdw',
    params=sq20appt_pdctldtmpdeltaldgdw_parameters,
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
sq20appt_pdctldtmpdeltaldgdw >> \
error_handler_group
# Set task dependencies end
