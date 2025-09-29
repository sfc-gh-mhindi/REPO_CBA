create or replace procedure dmva_get_compactor_measure_tasks(target_identifiers object default null, task_priority number default null, task_overrides object default null)
returns variant
language python
runtime_version = '3.11'
packages = ('snowflake-snowpark-python')
handler = 'get_compactor_measure_tasks'
imports = ('@DMVA_PYTHON_CODE/const.py')
execute as caller
as
$$
import const
import json
from snowflake.snowpark import Session

def get_compactor_measure_tasks(session: Session, target_identifiers: dict, task_priority: int, task_overrides: dict) -> dict:
    all_schemas_all_objects = [ '%', '%' ]

    if not target_identifiers:
        target_identifiers = { '%': [ all_schemas_all_objects ] }
    else:
        target_identifiers = { k: [ all_schemas_all_objects ] if not v or all_schemas_all_objects in v else list({ tuple(x): x for x in v }.values()) for k, v in target_identifiers.items() }
    if not task_overrides:
        task_overrides = {}
    
    insert_result = session.sql(f"""insert into dmva_tasks (
    task_id,
    task_type,
    task_priority,
    request_payload,
    system_name,
    pool_name,
    source_object_id,
    target_object_id,
    extract_group_id
)
with defaults as (
    select object_agg(param_name, param_value) as defaults from dmva_defaults
), scope as (
    select
        d.key as database_name,
        so.value[0]::varchar as schema_name,
        so.value[1]::varchar as object_name
    from
        table(flatten(parse_json('{json.dumps(target_identifiers)}'))) d,
        lateral flatten(d.value) so
), objects as (
    select
        cc.source_object_id,
        cc.target_object_id,
    from
        scope s
        inner join dmva_compactor_columns cc
        on cc.target_database like s.database_name
        and cc.target_schema like s.schema_name
        and cc.target_object like s.object_name
        and cc.row_count > 0
        inner join dmva_source_to_target_mapping stm
        on cc.source_object_id = stm.source_object_id
        and cc.target_object_id = stm.target_object_id
        and stm.active
        and stm.validate_only
), source_tasks as (
    select
        coalesce(
            {'null' if task_priority is None else task_priority},
            {task_overrides.get(const.TaskPriority, 'null')},
            s.task_priority,
            ss.overrides:task_priority,
            d.defaults:task_priority
        )::number as task_priority,
        dmva_get_measure_request(
            s.system_type,
            s.database_name,
            s.schema_name,
            s.object_name,
            array_agg(case when c.active = True and not(c.rollup_type = 'Other' or c.is_lob_type = 'Y') then object_construct_keep_null(c.*) end) within group (order by c.ordinal_position),
            p.source_filter,
            d.defaults,
            ss.overrides,
            s.overrides
        ) as request_payload,
        s.system_name,
        tp.pool_name,
        o.source_object_id,
        null as target_object_id,
        p.extract_group_id
    from
        objects o
        inner join dmva_object_info s
        on o.source_object_id = s.object_id
        and s.active
        inner join dmva_systems ss
        on s.system_name = ss.system_name
        and s.active = ss.is_source
        inner join dmva_checksums p
        on o.source_object_id = p.source_object_id
        and s.object_id = p.source_object_id
        inner join dmva_column_info c
        on o.source_object_id = c.object_id
        and s.object_id = c.object_id
        and p.source_object_id = c.object_id
        inner join dmva_system_task_pool_vw tp
        on s.system_name = tp.system_name
        and ss.system_name = tp.system_name
        and tp.task_type = 'measure_partition'
        cross join defaults d
    group by
        s.task_priority,
        ss.overrides,
        d.defaults,
        s.system_type,
        s.database_name,
        s.schema_name,
        s.object_name,
        p.source_filter,
        s.overrides,
        s.system_name,
        tp.pool_name,
        o.source_object_id,
        p.extract_group_id
), target_tasks as (
    select
        coalesce(
            {'null' if task_priority is None else task_priority},
            {task_overrides.get(const.TaskPriority, 'null')},
            s.task_priority,
            ss.overrides:task_priority,
            d.defaults:task_priority
        )::number as task_priority,
        dmva_get_measure_request(
            s.system_type,
            s.database_name,
            s.schema_name,
            s.object_name,
            array_agg(case when c.active = True and not(c.rollup_type = 'Other' or c.is_lob_type = 'Y') then object_construct_keep_null(c.*) end) within group (order by c.ordinal_position),
            p.target_filter,
            d.defaults,
            ss.overrides,
            s.overrides
        ) as request_payload,
        s.system_name,
        tp.pool_name,
        o.source_object_id,
        o.target_object_id,
        p.extract_group_id
    from
        objects o
        inner join dmva_object_info s
        on o.target_object_id = s.object_id
        and s.active
        inner join dmva_systems ss
        on s.system_name = ss.system_name
        and ss.is_source = false
        inner join dmva_checksums p
        on o.source_object_id = p.source_object_id
        inner join dmva_column_info c
        on o.target_object_id = c.object_id
        and s.object_id = c.object_id
        inner join dmva_system_task_pool_vw tp
        on s.system_name = tp.system_name
        and ss.system_name = tp.system_name
        and tp.task_type = 'measure_partition'
        cross join defaults d
    group by
        s.task_priority,
        ss.overrides,
        d.defaults,
        s.system_type,
        s.database_name,
        s.schema_name,
        s.object_name,
        p.target_filter,
        s.overrides,
        s.system_name,
        tp.pool_name,
        o.source_object_id,
        o.target_object_id,
        p.extract_group_id
)
select
    dmva_task_id_seq.nextval,
    'measure_partition',
    *
from
    (select * from source_tasks union all select * from target_tasks)""").collect()

    return insert_result[0].as_dict()
$$;
