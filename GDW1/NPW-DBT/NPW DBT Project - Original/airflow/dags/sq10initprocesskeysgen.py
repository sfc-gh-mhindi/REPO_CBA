from airflow.models import Variable
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow_utils.factories import get_app_config, open_dbt_dag, build_dbt_task
from airflow_utils.error_handlers import create_error_handler_group


default_params = {
    "gdw_user": "suyantel",
    "gdw_passwd": "Empty",
    "gdw_server": "cba5",
    "gdw_acct_db": "SDCSAD",
    "base_dir": "/cbs_app/CSEL4/CSEL4SIT",
    "run_stream": "CSE_L4_PRE_PROCESS",
    "smtp_server": "10.25.222.123",
    "send_mail": "elly.suyanti@cba.com.au",
    "receive_mail": "elly.suyanti@cba.com.au",
    "etl_process_dt": "20070101",
    "ctl_database": "CSE2SIT",
    "ctl_user": "CSE4_CTL",
    "ctl_passwd": "Empty",
    "ctl_schema": "CSE4_CTL",
    "gdw_acct_vw": "SDCSAD",
    "app_release": "CSEL4",
    "gdw_proc_db": "SPCSGRD",
    "gdw_grd_vw": "SDCSGRD",
}

app_config = get_app_config('CSEL4')

dag = open_dbt_dag(
    dag_id="sq10initprocesskeysgen",
    description="SQ10 Init Process Keys Gen",
    default_params=default_params,
    app_config=app_config
)


generatebatchkey = SnowflakeOperator(
    task_id="generatebatchkey",
    snowflake_conn_id=Variable.get(
        "snowflake_conn_id", default_var="snowflake_default"
    ),
    sql="call GDW1.{{ params.gdw_proc_db }}.SP_DS_GET_BTCH_KEY('{{ params.app_release }}', '{{ params.etl_process_dt }}')",
    do_xcom_push=True,
    dag=dag,
)


param_proc_key = SnowflakeOperator(
    task_id="param_proc_key",
    snowflake_conn_id=Variable.get(
        "snowflake_conn_id", default_var="snowflake_default"
    ),
    sql="SELECT * FROM GDW1.{{ params.ctl_schema }}.PARAM_PROS_KEY",
    do_xcom_push=True,
    dag=dag,
)

generateprocesskey = SnowflakeOperator(
    task_id="generateprocesskey",
    snowflake_conn_id=Variable.get(
        "snowflake_conn_id", default_var="snowflake_default"
    ),
    sql="""call GDW1.STAR_CAD_PROD_MACRO.SP_DS_GET_PROS_KEY(
        '{{ task_instance.xcom_pull(task_ids='param_proc_key')[0]['CONV_M'] }}', 
        '{{ task_instance.xcom_pull(task_ids='param_proc_key')[0]['SRCE_SYST_M'] }}', 
        '{{ task_instance.xcom_pull(task_ids='param_proc_key')[0]['SOURCE_M'] }}', 
        '{{ task_instance.xcom_pull(task_ids='param_proc_key')[0]['TARGET_NAME'] }}', 
        '{{ task_instance.xcom_pull(task_ids='generatebatchkey')[0]['SP_DS_GET_BTCH_KEY'].split('=')[1].strip() }}', 
        '{{ params.etl_process_dt }}', 
        '{{ task_instance.xcom_pull(task_ids='param_proc_key')[0]['DATABASE_NAME'] }}'
        )""",
    do_xcom_push=True,
    dag=dag,
)

loadgdwproskeyseq_parameters = {
    "base_dir": "{{ params.get('base_dir') }}",
    "gdw_server": "{{ params.get('gdw_server') }}",
    "gdw_user": "{{ params.get('gdw_user') }}",
    "gdw_passwd": "{{ params.get('gdw_passwd') }}",
    "gdw_acct_db": "{{ params.get('gdw_acct_db') }}",
    "etl_process_dt": "{{ params.get('etl_process_dt') }}",
    "app_release": "{{ params.get('app_release') }}",
    "scc_tera_truncate_string_with_null": "empty",
}

loadgdwproskeyseq = build_dbt_task(
    task_id="loadgdwproskeyseq",
    select="cse_dataload.02ProcessKey.loadgdwproskeyseq",
    vars=loadgdwproskeyseq_parameters,
    app_name="CSEL4",
    dag=dag,
)

processrunstreamerrorhandler_parameters = {
    "run_stream": "{{ params.get('run_stream') }}",
    "etl_process_dt": "{{ params.get('etl_process_dt') }}",
    "base_dir": "{{ params.get('base_dir') }}",
    "ctl_database": "{{ params.get('ctl_database') }}",
    "ctl_user": "{{ params.get('ctl_user') }}",
    "ctl_passwd": "{{ params.get('ctl_passwd') }}",
    "ctl_schema": "{{ params.get('ctl_schema') }}",
    "err_msg": "Error in stream",
    "smtp_server": "{{ params.get('smtp_server') }}",
    "send_mail": "{{ params.get('send_mail') }}",
    "receive_mail": "{{ params.get('receive_mail') }}",
}

error_handler_group = create_error_handler_group(
    dag=dag,
    group_id="error_handler",
    error_processor_task_id="sequencefail",
    error_handler_factory=build_dbt_task,
    error_handler_params=processrunstreamerrorhandler_parameters,
    error_handler_task_id="processrunstreamerrorhandler",
    error_handler_kwargs={
        "select": "cse_dataload.24processmetadata.processrunstreamerrorhandler",
        "app_name": "CSEL4",
    },
    terminate_dag_on_error=True,
)

# Set task dependencies start
(
    generatebatchkey
    >> param_proc_key
    >> generateprocesskey
    >> loadgdwproskeyseq
    >> error_handler_group
)
# Set task dependencies end
