CREATE OR REPLACE PROCEDURE NPD_D12_DMN_GDWMIG.MIGRATION_TRACKING.DMVA_GET_CHECKSUM_TASKS_MH("SYSTEM_NAME" VARCHAR, "FORCE" BOOLEAN DEFAULT FALSE, "IDENTIFIERS" OBJECT DEFAULT null, "TASK_PRIORITY" NUMBER(38,0) DEFAULT null, "TASK_OVERRIDES" OBJECT DEFAULT null)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
IMPORTS = ('@NPD_D12_DMN_GDWMIG.MIGRATION_TRACKING.DMVA_PYTHON_CODE/const.py')
EXECUTE AS CALLER
AS '
import const
from snowflake.snowpark.session import Session
from typing import Dict, List
import json
import uuid

def cleanup_temp_table(session: Session, temp_table_with_guid: str) -> dict:
    """Drop the temporary checksum tasks table with GUID suffix"""
    try:
        result = session.sql(f"DROP TABLE IF EXISTS {temp_table_with_guid}").collect()[0]
        return {
            ''TEMP_TABLE_CLEANUP'': {
                ''table_name'': temp_table_with_guid,
                ''dropped'': True,
                ''message'': f"Successfully dropped temporary table {temp_table_with_guid}"
            }
        }
    except Exception as e:
        return {
            ''TEMP_TABLE_CLEANUP'': {
                ''table_name'': temp_table_with_guid,
                ''dropped'': False,
                ''message'': f"Error dropping temporary table: {str(e)}"
            }
        }

def run(session: Session, system_name: str, force: bool, identifiers: Dict, task_priority: int, task_overrides: Dict) -> Dict:
    all_schemas_all_objects, checksum_tasks = [ ''%'', ''%'' ], ''dmva_checksum_tasks_to_insert''
    
    # Generate GUID for unique table name
    table_guid = str(uuid.uuid4()).replace(''-'', '''')
    checksum_tasks_with_guid = f"{checksum_tasks}_{table_guid}"
    temp_table_created = False
    
    try:
        if task_overrides is None:
            task_overrides = {}
        
        # get system info
        try:
            system_type, system_identifiers, system_overrides, pool_name = (lambda r: (r[0], json.loads(r[1]), json.loads(r[2]), r[3]))(session.sql(f"""select
        upper(s.system_details:type),
        nvl(s.identifiers, object_construct()),
        nvl(s.overrides, object_construct()),
        tp.pool_name
    from
        dmva_systems s
        inner join dmva_system_task_pool_vw tp
        on s.system_name = tp.system_name
        and s.system_name = ''{system_name}''
        and s.is_source = true
        and tp.system_name = ''{system_name}''
        and tp.task_type = ''get_checksums''""").collect()[0])
            if system_type is None:
                return { ''error'': f"''system_details'' must have ''type'' key and value; check dmva_systems and try again" }
        except Exception as e:
            return { ''error'': f"failed to get system info for ''{system_name}'' ({str(e)}); check dmva_systems and dmva_system_task_pool_vw and try again" }
        
        # get/check defaults
        defaults = (lambda r: (json.loads(r[0])))(session.sql(f"""select
        object_agg(param_name, param_value)
    from
        dmva_defaults""").collect()[0])
        if defaults is None or len(defaults) == 0:
            return { ''error'': ''no defaults defined; check dmva_defaults and try again'' }
        elif not(const.TaskPriority in defaults and const.MaxPartitionRowCount in defaults):
            return { ''error'': f"''{const.TaskPriority}'' and ''{const.MaxPartitionRowCount}'' defaults not defined; check dmva_defaults and try again" }
        elif not(const.TaskPriority in defaults):
            return { ''error'': f"''{const.TaskPriority}'' default not defined; check dmva_defaults and try again" }
        elif not(const.MaxPartitionRowCount in defaults):
            return { ''error'': f"''{const.MaxPartitionRowCount}'' default not defined; check dmva_defaults and try again" }
        
        # check identifiers
        identifiers = identifiers or system_identifiers
        if identifiers is None or len(identifiers) == 0:
            return { ''warning'': ''no identifiers to process; check dmva_systems or try again with identifiers={...}'' }
        identifiers = { k: [ all_schemas_all_objects ] if not v or all_schemas_all_objects in v else list({ tuple(x): x for x in v }.values()) for k, v in identifiers.items() }
        if any(all_schemas_all_objects in v for v in identifiers.values()) and not(force):
            return { ''warning'': ''at least one schema/object scope is unfiltered, resulting in an unrestricted number of get_checksums tasks being generated; if intentional, try again with force=true'' }
        
        # create driver table with GUID suffix
        _ = session.sql(f"""create or replace TRANSIENT table {checksum_tasks_with_guid} as
    with task_info as (
        select
            ''{system_type}'' as system_type,
            s.system_name,
            d.key as database_name,
            so.value[0]::varchar as schema_name,
            so.value[1]::varchar as object_name
        from
            dmva_systems s,
            table(flatten(parse_json(''{json.dumps(identifiers)}''))) d,
            lateral flatten(d.value) so
        where
            s.system_name = ''{system_name}''
            and s.is_source = true
    ), source_info as (
        select
            coalesce(
                {''null'' if task_priority is None else task_priority},
                {task_overrides.get(const.TaskPriority, ''null'')},
                o.task_priority,
                {system_overrides.get(const.TaskPriority, ''null'')},
                {defaults[const.TaskPriority]}
            )::number as task_priority,
            o.system_type,
            o.system_name,
            o.database_name,
            o.schema_name,
            o.object_name,
            array_agg(case when c.active = true then object_construct(c.*) end) within group (order by c.ordinal_position) as columns,
            coalesce(
                {task_overrides.get(const.ChecksumMethod, ''null'')},
                o.checksum_method,
                {system_overrides.get(const.ChecksumMethod, ''null'')},
                {defaults.get(const.ChecksumMethod, ''null'')}
            ) as checksum_method,
            o.overrides as object_overrides,
            o.object_id,
            coalesce(
                {task_overrides.get(const.MaxPartitionRowCount, ''null'')},
                o.overrides:max_partition_row_count,
                {system_overrides.get(const.MaxPartitionRowCount, ''null'')},
                {defaults[const.MaxPartitionRowCount]}
            )::number as max_partition_row_count,
            o.ignore_max_partition,
            sum(case when c.active = true and c.rollup_type = ''Numeric'' and (c.numeric_precision is null or c.numeric_scale is null) then 1 else 0 end) as column_check
        from
            dmva_object_info o
            inner join task_info t
            on o.system_type = t.system_type
            and o.system_name = t.system_name
            and o.database_name = t.database_name
            and o.schema_name like t.schema_name
            and o.object_name like t.object_name
            inner join dmva_source_to_target_mapping stm
            on o.object_id = stm.source_object_id
            and o.active = stm.active
            and o.active
            inner join dmva_column_info c
            on o.object_id = c.object_id
            and stm.source_object_id = c.object_id
        group by
            o.task_priority,
            o.system_type,
            o.system_name,
            o.database_name,
            o.schema_name,
            o.object_name,
            o.checksum_method,
            o.overrides,
            o.object_id,
            o.ignore_max_partition
    ), partition_checks as (
        select s.object_id from source_info s
        where
            s.ignore_max_partition = false
            and exists (
                select 1 from dmva_checksums p
                where
                    p.source_object_id = s.object_id
                    and p.partition_row_count > s.max_partition_row_count
            )
    ), task_checks as (
        select s.object_id from source_info s
        where
            exists (
                select 1 from dmva_tasks t
                where
                    t.task_type = ''get_checksums''
                    and nvl(t.status_cd, '''') not in (''OK'', ''NOK'')
                    and t.system_name = s.system_name
                    and t.source_object_id = s.object_id
            )
    ), validate_checks as (
        select s.object_id from source_info s
        where
            exists (
                select 1 from dmva_source_to_target_mapping stm
                where
                    stm.validate_only
                    and stm.source_object_id = s.object_id
            )
    )
    select
        s.system_type,
        s.system_name,
        s.database_name,
        s.schema_name,
        s.object_name,
        s.object_overrides,
        s.columns,
        s.checksum_method,
        s.object_id,
        s.task_priority,
        s.column_check = 0 as column_check_ok,
        pc.object_id is null as partition_check_ok,
        tc.object_id is null as task_check_ok,
        vc.object_id is null as validate_check_ok,
        column_check_ok and partition_check_ok and task_check_ok and validate_check_ok as task_insertable
    from
        source_info s
        left join partition_checks pc
        on s.object_id = pc.object_id
        left join task_checks tc
        on s.object_id = tc.object_id
        left join validate_checks vc
        on s.object_id = vc.object_id
    order by
        s.system_name,
        s.database_name,
        s.schema_name,
        s.object_name""").collect()

        temp_table_created = True

        # get task counts
        column_checks_failed, partition_checks_failed, task_checks_failed, validate_checks_failed, tasks_insertable, tasks_generated = session.sql(f"""select
        sum(case when column_check_ok then 0 else 1 end),
        sum(case when partition_check_ok then 0 else 1 end),
        sum(case when task_check_ok then 0 else 1 end),
        sum(case when validate_check_ok then 0 else 1 end),
        sum(case when task_insertable then 1 else 0 end),
        count(1)
    from
        {checksum_tasks_with_guid}""").collect()[0]

        # insert the insertable
        result = { ''TASKS_GENERATED'': tasks_generated, ''TABLE_NAME'': checksum_tasks_with_guid }
        if tasks_insertable:
            tasks_inserted = session.sql(f"""insert into dmva_tasks (
        task_id,
        task_type,
        task_priority,
        queue_ts,
        request_payload,
        system_name,
        pool_name,
        source_object_id
    )
    with settings as (
        select
            parse_json(''{json.dumps(task_overrides)}'') as task_overrides,
            parse_json(''{json.dumps(system_overrides)}'') as system_overrides,
            object_agg(param_name, param_value) as defaults
        from
            dmva_defaults
    )
    select
        dmva_task_id_seq.nextval,
        ''get_checksums'',
        t.task_priority,
        dmva_current_timestamp_utc(),
        dmva_get_checksum_request(t.system_type, t.database_name, t.schema_name, t.object_name, t.columns, t.checksum_method, s.defaults, s.system_overrides, t.object_overrides, s.task_overrides),
        t.system_name,
        ''{pool_name}'',
        t.object_id
    from
        {checksum_tasks_with_guid} t
        cross join settings s
    where
        t.task_insertable""").collect()[0][0]
            result.update({ ''TASKS_INSERTED'': tasks_inserted })
        else:
            result.update({ ''TASKS_INSERTED'': 0 })
        
        if tasks_insertable < tasks_generated:
            result.update({ ''TASKS_NOT_INSERTED_DUE_TO'': { k: v for k, v in zip([ ''UNPROFILED_COLUMNS'', ''MAX_PARTITION_EXCEEDED'', ''EXISTING_TASKS'', ''VALIDATE_ONLY'' ], [ column_checks_failed, partition_checks_failed, task_checks_failed, validate_checks_failed ]) if v > 0 } })
        
        return result
    
    finally:
        # Always cleanup the temporary table if it was created
        if temp_table_created:
            cleanup_result = cleanup_temp_table(session, checksum_tasks_with_guid)
            # If result exists, append cleanup info, otherwise initialize with cleanup info
            try:
                if ''result'' in locals():
                    result.update(cleanup_result)
                else:
                    result = cleanup_result
            except:
                result = cleanup_result
';