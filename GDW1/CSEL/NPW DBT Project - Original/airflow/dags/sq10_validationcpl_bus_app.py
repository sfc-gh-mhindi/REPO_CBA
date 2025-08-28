from airflow_utils.factories import get_app_config, open_dbt_dag, build_dbt_task
from airflow_utils.error_handlers import create_error_handler_group


default_params = {
    'base_dir': 'cba_app_csel4_csel4dev', 
    'run_stream': 'CSE_CPL_BUS_APP', 
    'smtp_server': '10.25.222.123', 
    'send_mail': 'halit.sindi@cba.com.au', 
    'receive_mail': 'halit.sindi@cba.com.au', 
    'etl_process_dt': '20061016', 
    'ctl_database': 'CSE2DEV', 
    'ctl_user': 'CSE4_CTL', 
    'ctl_passwd': 'LD:@9IV@<=3M0>EI<IJ<0KH7', 
    'ctl_schema': 'CSE4_CTL', 
    'app_release': 'CSEL4', 
    'ctl_table': 'RUN_STRM_TMPL'
}

app_config = get_app_config('CSEL4')

dag = open_dbt_dag(
    dag_id='sq10_validationcpl_bus_app',
    description='SQ10 Validation CPL BUS APP',
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


# Create an error handling TaskGroup using the new utility function
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
error_handler_group
# Set task dependencies end