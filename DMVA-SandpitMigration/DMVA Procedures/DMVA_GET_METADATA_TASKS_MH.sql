{\rtf1\ansi\ansicpg1252\cocoartf2822
\cocoatextscaling0\cocoaplatform0{\fonttbl\f0\fswiss\fcharset0 Helvetica;}
{\colortbl;\red255\green255\blue255;}
{\*\expandedcolortbl;;}
\paperw11900\paperh16840\margl1440\margr1440\vieww11520\viewh8400\viewkind0
\pard\tx720\tx1440\tx2160\tx2880\tx3600\tx4320\tx5040\tx5760\tx6480\tx7200\tx7920\tx8640\pardirnatural\partightenfactor0

\f0\fs24 \cf0 CREATE OR REPLACE PROCEDURE NPD_D12_DMN_GDWMIG.MIGRATION_TRACKING.DMVA_GET_METADATA_TASKS_MH("SYSTEM_NAME" VARCHAR(16777216), "FORCE" BOOLEAN DEFAULT FALSE, "IDENTIFIERS" OBJECT DEFAULT null, "TASK_PRIORITY" NUMBER(38,0) DEFAULT null, "TASK_OVERRIDES" OBJECT DEFAULT null)\
RETURNS VARIANT\
LANGUAGE PYTHON\
RUNTIME_VERSION = '3.11'\
PACKAGES = ('snowflake-snowpark-python')\
HANDLER = 'run'\
IMPORTS = ('@NPD_D12_DMN_GDWMIG.MIGRATION_TRACKING.DMVA_PYTHON_CODE/const.py')\
EXECUTE AS CALLER\
AS '\
from snowflake.snowpark.session import Session\
from typing import Dict, List\
import json\
import const\
\
def run(session: Session, system_name: str, force: bool, identifiers: Dict, task_priority: int, task_overrides: Dict) -> Dict:\
    all_schemas_all_objects, metadata_tasks = [ ''%'', ''%'' ], ''dmva_metadata_tasks_to_insert''\
    if task_overrides is None:\
        task_overrides = \{\}\
    \
    # get system info\
    try:\
        system_type, system_identifiers, system_overrides, pool_name = (lambda r: (r[0], json.loads(r[1]), json.loads(r[2]), r[3]))(session.sql(f"""select\
    upper(s.system_details:type),\
    nvl(s.identifiers, object_construct()),\
    nvl(s.overrides, object_construct()),\
    tp.pool_name\
from\
    dmva_systems s\
    inner join dmva_system_task_pool_vw tp\
    on s.system_name = tp.system_name\
    and s.system_name = ''\{system_name\}''\
    and tp.system_name = ''\{system_name\}''\
    and tp.task_type = ''get_metadata''""").collect()[0])\
        if system_type is None:\
            return \{ ''error'': f"''system_details'' must have ''type'' key and value; check dmva_systems and try again" \}\
    except Exception as e:\
        return \{ ''error'': f"failed to get system info for ''\{system_name\}'' (\{str(e)\}); check dmva_systems and dmva_system_task_pool_vw and try again" \}\
    \
    # check defaults\
    defaults = (lambda r: (json.loads(r[0])))(session.sql(f"""select\
    object_agg(param_name, param_value)\
from\
    dmva_defaults""").collect()[0])\
    if defaults is None or len(defaults) == 0:\
        return \{ ''error'': ''no defaults defined; check dmva_defaults and try again'' \}\
    elif not(const.TaskPriority in defaults):\
        return \{ ''error'': f"''\{const.TaskPriority\}'' default not defined; check dmva_defaults and try again" \}\
    \
    # check identifiers\
    identifiers = identifiers or system_identifiers\
    if identifiers is None or len(identifiers) == 0:\
        return \{ ''warning'': ''no identifiers to process; check dmva_systems or try again with identifiers=\{...\}'' \}\
    identifiers = \{ k: [ all_schemas_all_objects ] if not v or all_schemas_all_objects in v else list(\{ tuple(x): x for x in v \}.values()) for k, v in identifiers.items() \}\
    if any(all_schemas_all_objects in v for v in identifiers.values()) and not(force):\
        return \{ ''warning'': ''at least one schema/object scope is unfiltered, resulting in an unrestricted number of metadata results; if intentional, try again with force=true'' \}\
    \
    # create driver table\
    _ = session.sql(f"""create or replace TRANSIENT table \{metadata_tasks\} as\
with task_info as (\
    select\
        ''\{system_type\}'' as system_type,\
        s.system_name,\
        d.key as database_name,\
        d.value as schema_object_names\
    from\
        dmva_systems s,\
        table(flatten(parse_json(''\{json.dumps(identifiers)\}''))) d\
    where\
        s.system_name = ''\{system_name\}''\
), task_checks as (\
    select\
        ti.system_name,\
        ti.database_name,\
        ti.schema_object_names\
    from\
        task_info ti\
    where\
        exists (\
            select 1 from dmva_tasks t\
            where\
                t.task_type = ''get_metadata''\
                and t.system_name = ti.system_name\
                and nvl(t.status_cd, '''') not in (''OK'', ''NOK'')\
                and t.request_payload:scope:system_name = ti.system_name\
                and t.request_payload:scope:database_name = ti.database_name\
                and t.request_payload:scope:schema_object_names = ti.schema_object_names\
        )\
)\
select\
    ti.system_type,\
    ti.system_name,\
    ti.database_name,\
    ti.schema_object_names,\
    tc.system_name is null and tc.database_name is null and tc.schema_object_names is null as task_check_ok,\
    task_check_ok as task_insertable\
from\
    task_info ti\
    left join task_checks tc\
    on ti.system_name = tc.system_name\
    and ti.database_name = tc.database_name\
    and ti.schema_object_names = tc.schema_object_names\
order by\
    ti.system_name,\
    ti.database_name,\
    ti.schema_object_names""").collect()\
\
    # get task counts\
    task_checks_failed, tasks_insertable, tasks_generated = session.sql(f"""select\
    sum(case when task_check_ok then 0 else 1 end),\
    sum(case when task_insertable then 1 else 0 end),\
    count(1)\
from\
    \{metadata_tasks\}""").collect()[0]\
\
    # insert the insertable\
    result = \{ ''TASKS_GENERATED'': tasks_generated \}\
    if tasks_insertable:\
        tasks_inserted = session.sql(f"""insert into dmva_tasks (\
    task_id,\
    task_type,\
    task_priority,\
    queue_ts,\
    request_payload,\
    system_name,\
    pool_name\
)\
with settings as (\
    select\
        parse_json(''\{json.dumps(task_overrides)\}'') as task_overrides,\
        parse_json(''\{json.dumps(system_overrides)\}'') as system_overrides,\
        object_agg(param_name, param_value) as defaults\
    from\
        dmva_defaults\
)\
select\
    dmva_task_id_seq.nextval,\
    ''get_metadata'',\
    coalesce(\
        \{''null'' if task_priority is None else task_priority\},\
        s.task_overrides:task_priority,\
        s.system_overrides:task_priority,\
        s.defaults:task_priority\
    )::number,\
    dmva_current_timestamp_utc(),\
    dmva_get_metadata_request(t.system_type, t.system_name, t.database_name, t.schema_object_names, s.defaults, s.system_overrides, s.task_overrides),\
    t.system_name,\
    ''\{pool_name\}''\
from\
    \{metadata_tasks\} t\
    cross join settings s\
where\
    t.task_insertable""").collect()[0][0]\
        result.update(\{ ''TASKS_INSERTED'': tasks_inserted \})\
    else:\
        result.update(\{ ''TASKS_INSERTED'': 0 \})\
    \
    if task_checks_failed:\
        result.update(\{ ''TASKS_NOT_INSERTED_DUE_TO_EXISTING_UNFINISHED_TASKS'': task_checks_failed \})\
    \
    return result\
';}