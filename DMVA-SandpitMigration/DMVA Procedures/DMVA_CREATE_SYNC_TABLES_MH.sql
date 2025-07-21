{\rtf1\ansi\ansicpg1252\cocoartf2822
\cocoatextscaling0\cocoaplatform0{\fonttbl\f0\fswiss\fcharset0 Helvetica;}
{\colortbl;\red255\green255\blue255;}
{\*\expandedcolortbl;;}
\paperw11900\paperh16840\margl1440\margr1440\vieww11520\viewh8400\viewkind0
\pard\tx720\tx1440\tx2160\tx2880\tx3600\tx4320\tx5040\tx5760\tx6480\tx7200\tx7920\tx8640\pardirnatural\partightenfactor0

\f0\fs24 \cf0 CREATE OR REPLACE PROCEDURE NPD_D12_DMN_GDWMIG.MIGRATION_TRACKING.DMVA_CREATE_SYNC_TABLES_MH("CREATE_TABLES" BOOLEAN DEFAULT FALSE, "OR_REPLACE" BOOLEAN DEFAULT FALSE, "SYSTEM_NAME" VARCHAR DEFAULT null, "IDENTIFIERS" OBJECT DEFAULT null, "RESULTS_TABLE" VARCHAR DEFAULT 'dmva_create_sync_tables_results')\
RETURNS VARIANT\
LANGUAGE PYTHON\
RUNTIME_VERSION = '3.11'\
PACKAGES = ('snowflake-snowpark-python','joblib')\
HANDLER = 'run'\
EXECUTE AS CALLER\
AS '\
from joblib import Parallel, delayed\
import json\
from snowflake.snowpark import Session\
\
def create_object(session: Session, object_name: str, object_ddl: str) -> dict:\
    try:\
        result = session.sql(object_ddl).collect()[0]["status"]\
        return \{\
            ''object_name'': object_name,\
            ''object_created'': True if ''successfully created'' in result else False,\
            ''create_message'': result\
        \}\
    except Exception as e:\
        return \{\
            ''object_name'': object_name,\
            ''object_created'': False,\
            ''create_message'': str(e)\
        \}\
\
def upsert_objects(session: Session, results_table: str) -> dict:\
    try:\
        result = session.sql(f"""merge into dmva_object_info t using (\
    select\
        o.object_id,\
        ''SNOWFLAKE'' as system_type,\
        rt.target_system_name as system_name,\
        rt.target_database_name as database_name,\
        rt.target_schema_name as schema_name,\
        rt.target_object_name as object_name,\
        ''TABLE'' as object_type,\
        true as active,\
        false as ignore_max_partition,\
        rt.current_timestamp_utc as created_ts\
    from\
        \{results_table\} rt\
        inner join dmva_systems ss\
        on rt.target_system_name = ss.system_name\
        and ss.is_source = false\
        left join dmva_object_info o\
        on ''SNOWFLAKE'' = o.system_type\
        and rt.target_system_name = o.system_name\
        and rt.target_database_name = o.database_name\
        and rt.target_schema_name = o.schema_name\
        and rt.target_object_name = o.object_name\
\
) s\
on t.object_id = s.object_id\
when matched and not(\
    equal_null(t.object_type, s.object_type)\
    and equal_null(t.active, s.active)\
    and equal_null(t.ignore_max_partition, s.ignore_max_partition)\
) then update set\
    t.object_type = s.object_type,\
    t.active = s.active,\
    t.ignore_max_partition = s.ignore_max_partition,\
    t.created_ts = s.created_ts,\
    t.updated_ts = null\
when not matched then insert (\
    system_type,\
    system_name,\
    database_name,\
    schema_name,\
    object_name,\
    object_type,\
    active,\
    ignore_max_partition,\
    created_ts\
) values (\
    s.system_type,\
    s.system_name,\
    s.database_name,\
    s.schema_name,\
    s.object_name,\
    s.object_type,\
    s.active,\
    s.ignore_max_partition,\
    s.created_ts\
)""").collect()[0].as_dict()\
    except Exception as e:\
        result = \{ type(e).__name__: str(e) \}\
    finally:\
        return \{ ''SYNC_OBJECTS_UPSERTED'': result \}\
\
def upsert_columns(session: Session, results_table: str) -> dict:\
    try:\
        result = session.sql(f"""merge into dmva_column_info t using (\
    with data as (\
        select\
            o.object_id,\
            c.value:COLUMN_NAME::varchar as column_name,\
            c.value:ORDINAL_POSITION::number as ordinal_position,\
            c.value:COLUMN_DEFAULT::varchar as column_default,\
            c.value:IS_NULLABLE::varchar as is_nullable,\
            c.value:DATA_TYPE::varchar as data_type,\
            c.value:CHARACTER_MAXIMUM_LENGTH::number as character_maximum_length,\
            c.value:NUMERIC_PRECISION::number as numeric_precision,\
            c.value:NUMERIC_SCALE::number as numeric_scale,\
            c.value:DATETIME_PRECISION::number as datetime_precision,\
            c.value:CHARACTER_SET_NAME::varchar as character_set_name,\
            c.value:COLLATION_NAME::varchar as collation_name,\
            c.value:ROLLUP_TYPE::varchar as rollup_type,\
            c.value:IS_LOB_TYPE::varchar as is_lob_type,\
            c.value:EXTRA_INFO as extra_info,\
            c.value:RAW_METADATA as raw_metadata,\
            c.value:ACTIVE::boolean as active,\
            rt.current_timestamp_utc as created_ts\
        from\
            \{results_table\} rt,\
            dmva_systems ss,\
            dmva_object_info o,\
            lateral flatten(rt.target_columns) c\
        where\
            rt.target_system_name = ss.system_name\
            and ss.is_source = false\
            and rt.target_system_name = o.system_name\
            and rt.target_database_name = o.database_name\
            and rt.target_schema_name = o.schema_name\
            and rt.target_object_name = o.object_name\
            and o.system_type = ''SNOWFLAKE''\
    )\
    select\
        c.column_id,\
        d.* exclude (character_maximum_length),\
        case\
            when d.data_type = ''TEXT'' and nvl(d.character_maximum_length, 0) = 0 then 16777216\
            when d.data_type = ''TEXT'' then d.character_maximum_length\
            else null\
        end as character_maximum_length\
    from\
        data d\
        left join dmva_column_info c\
        on d.object_id = c.object_id\
        and d.column_name = c.column_name\
) s\
on t.column_id = s.column_id and t.object_id = s.object_id\
when matched and not(\
    equal_null(t.column_name, s.column_name)\
    and equal_null(t.ordinal_position, s.ordinal_position)\
    and equal_null(t.column_default, s.column_default)\
    and equal_null(t.is_nullable, s.is_nullable)\
    and equal_null(t.data_type, s.data_type)\
    and equal_null(t.character_maximum_length, s.character_maximum_length)\
    and equal_null(t.numeric_precision, s.numeric_precision)\
    and equal_null(t.numeric_scale, s.numeric_scale)\
    and equal_null(t.datetime_precision, s.datetime_precision)\
    and equal_null(t.character_set_name, s.character_set_name)\
    and equal_null(t.collation_name, s.collation_name)\
    and equal_null(t.rollup_type, s.rollup_type)\
    and equal_null(t.is_lob_type, s.is_lob_type)\
    and equal_null(t.extra_info, s.extra_info)\
    and equal_null(t.raw_metadata, s.raw_metadata)\
) then update set\
    t.column_name = s.column_name,\
    t.ordinal_position = s.ordinal_position,\
    t.column_default = s.column_default,\
    t.is_nullable = s.is_nullable,\
    t.data_type = s.data_type,\
    t.character_maximum_length = s.character_maximum_length,\
    t.numeric_precision = s.numeric_precision,\
    t.numeric_scale = s.numeric_scale,\
    t.datetime_precision = s.datetime_precision,\
    t.character_set_name = s.character_set_name,\
    t.collation_name = s.collation_name,\
    t.rollup_type = s.rollup_type,\
    t.is_lob_type = s.is_lob_type,\
    t.extra_info = s.extra_info,\
    t.raw_metadata = s.raw_metadata,\
    t.active = s.active,\
    t.created_ts = s.created_ts,\
    t.updated_ts = null\
when not matched then insert (\
    object_id,\
    column_name,\
    ordinal_position,\
    column_default,\
    is_nullable,\
    data_type,\
    character_maximum_length,\
    numeric_precision,\
    numeric_scale,\
    datetime_precision,\
    character_set_name,\
    collation_name,\
    rollup_type,\
    is_lob_type,\
    extra_info,\
    raw_metadata,\
    active,\
    created_ts\
) values (\
    s.object_id,\
    s.column_name,\
    s.ordinal_position,\
    s.column_default,\
    s.is_nullable,\
    s.data_type,\
    s.character_maximum_length,\
    s.numeric_precision,\
    s.numeric_scale,\
    s.datetime_precision,\
    s.character_set_name,\
    s.collation_name,\
    s.rollup_type,\
    s.is_lob_type,\
    s.extra_info,\
    s.raw_metadata,\
    s.active,\
    s.created_ts\
)""").collect()[0].as_dict()\
    except Exception as e:\
        result = \{ type(e).__name__: str(e) \}\
    finally:\
        return \{ ''SYNC_COLUMNS_UPSERTED'': result \}\
\
def upsert_mappings(session: Session, results_table: str) -> dict:\
    try:\
        result = session.sql(f"""merge into dmva_source_to_target_mapping t using (\
    select\
        rt.source_object_id,\
        o.object_id as target_object_id,\
        true as active,\
        rt.current_timestamp_utc as created_ts\
    from\
        \{results_table\} rt\
        inner join dmva_systems ss\
        on rt.target_system_name = ss.system_name\
        and ss.is_source = false\
        inner join dmva_object_info o\
        on rt.target_system_name = o.system_name\
        and rt.target_database_name = o.database_name\
        and rt.target_schema_name = o.schema_name\
        and rt.target_object_name = o.object_name\
        and o.system_type = ''SNOWFLAKE''\
) s\
on t.source_object_id = s.source_object_id\
when matched and not(\
    equal_null(t.target_object_id, s.target_object_id)\
) then update set\
    t.target_object_id = s.target_object_id,\
    t.active = s.active,\
    t.created_ts = s.created_ts,\
    t.updated_ts = null\
when not matched then insert (\
    source_object_id,\
    target_object_id,\
    active,\
    created_ts\
) values (\
    s.source_object_id,\
    s.target_object_id,\
    s.active,\
    s.created_ts\
)""").collect()[0].as_dict()\
    except Exception as e:\
        result = \{ type(e).__name__: str(e) \}\
    finally:\
        return \{ ''SYNC_MAPPINGS_UPSERTED'': result \}\
\
def prune_columns(session: Session, results_table: str) -> dict:\
    try:\
        result = session.sql(f"""merge into dmva_column_info t using (\
    with actuals as (\
        select\
            o.object_id,\
            c.value:COLUMN_NAME::varchar as column_name\
        from\
            \{results_table\} rt,\
            dmva_systems ss,\
            dmva_object_info o,\
            lateral flatten(rt.target_columns) c\
        where\
            rt.target_system_name = ss.system_name\
            and rt.table_created\
            and ss.is_source = false\
            and rt.target_system_name = o.system_name\
            and rt.target_database_name = o.database_name\
            and rt.target_schema_name = o.schema_name\
            and rt.target_object_name = o.object_name\
            and ss.system_name = o.system_name\
            and o.system_type = ''SNOWFLAKE''\
    )\
    select\
        c.object_id,\
        c.column_name\
    from\
        actuals a\
        inner join dmva_column_info c\
        on a.object_id = c.object_id\
        left join actuals a2\
        on c.object_id = a2.object_id\
        and c.column_name = a2.column_name\
    where\
        a2.column_name is null\
) s\
on t.object_id = s.object_id and t.column_name = s.column_name\
when matched then delete""").collect()[0].as_dict()\
    except Exception as e:\
        result = \{ type(e).__name__: str(e) \}\
    finally:\
        return \{ ''SYNC_COLUMNS_PRUNED'': result \}\
\
def run(session: Session, create_tables: bool, or_replace: bool, system_name: str, identifiers: dict, results_table: str) -> list:\
    all_schemas_all_objects = [ ''%'', ''%'' ]\
\
    # handle parameters\
    if not create_tables:\
        create_tables = False\
    if not or_replace:\
        or_replace = False\
    if not system_name:\
        system_name = ''%''\
    if not identifiers:\
        identifiers = \{ ''%'': [ all_schemas_all_objects ] \}\
    else:\
        identifiers = \{ k: [ all_schemas_all_objects ] if not v or all_schemas_all_objects in v else list(\{ tuple(x): x for x in v \}.values()) for k, v in identifiers.items() \}\
    if not results_table:\
        results_table = ''dmva_create_sync_tables_results''\
    \
    # get calling location\
    calling_database_schema = session.sql("select concat_ws(''.'', current_database(), current_schema())").collect()[0][0]\
    \
    # generate driver table\
    create_results = session.sql(f"""create or replace TRANSIENT table \{results_table\} as\
with scope as (\
    select\
        ''\{system_name\}'' as system_name,\
        d.key as database_name,\
        so.value[0]::varchar as schema_name,\
        so.value[1]::varchar as object_name\
    from\
        table(flatten(parse_json(''\{json.dumps(identifiers)\}''))) d,\
        lateral flatten(d.value) so\
), sources as (\
    select\
        o.object_id as source_object_id,\
        o.system_name as source_system_name,\
        o.system_type as source_system_type,\
        o.database_name as source_database_name,\
        o.schema_name as source_schema_name,\
        o.object_name as source_object_name,\
        concat_ws(''.'', o.system_name, o.database_name, o.schema_name, o.object_name) as source_name,\
        array_agg(object_delete(object_insert(object_insert(object_insert(object_construct(c.*),\
            ''COLUMN_NAME'', dmva_transform_identifier(c.column_name, o.system_type), true),\
            ''DATA_TYPE'', nvl(dt.snowflake_data_type, ''VARIANT''), true),\
            ''ROLLUP_TYPE'', nvl(dt.snowflake_rollup_type, ''Other''), true), ''COLUMN_ID'', ''CREATED_TS'', ''UPDATED_TS'')) within group (order by c.ordinal_position)\
        as target_columns\
    from\
        scope s\
        inner join dmva_object_info o\
        on o.system_name like s.system_name\
        and o.database_name like s.database_name\
        and o.schema_name like s.schema_name\
        and o.object_name like s.object_name\
        inner join dmva_systems ss\
        on o.system_name = ss.system_name\
        and o.active = ss.is_source\
        and o.active\
        inner join dmva_column_info c\
        on o.object_id = c.object_id\
        left join dmva_data_types dt\
        on o.system_type = dt.system_type\
        and c.data_type ilike dt.data_type\
    group by\
        all\
), targets as (\
    select\
        s.source_object_id,\
        dmva_transform_identifier(s.source_database_name, s.source_system_type) as transformed_database_name,\
        dmva_transform_identifier(s.source_schema_name, s.source_system_type) as transformed_schema_name,\
        dmva_transform_identifier(s.source_object_name, s.source_system_type) as transformed_object_name,\
        s.target_columns,\
        iff(mr.target_system_name = ''%'', s.source_system_name, mr.target_system_name) as target_system_name,\
        iff(mr.target_database_name = ''%'', transformed_database_name, mr.target_database_name) as target_database_name,\
        iff(mr.target_schema_name = ''%'', transformed_schema_name, mr.target_schema_name) as target_schema_name,\
        iff(mr.target_object_name = ''%'', transformed_object_name, mr.target_object_name) as target_object_name,\
        concat_ws(''.'', s.source_system_name, transformed_database_name, transformed_schema_name, transformed_object_name) as transformed_source_name,\
        replace(concat_ws(''.'', target_system_name, target_database_name, target_schema_name, target_object_name), ''%'', '''') as matching_target_name\
    from\
        sources s\
        inner join dmva_mapping_rules mr\
        on s.source_system_name like mr.source_system_name\
        and s.source_database_name like mr.source_database_name\
        and s.source_schema_name like mr.source_schema_name\
        and s.source_object_name like mr.source_object_name\
    qualify\
        row_number() over (partition by s.source_object_id order by length(matching_target_name) desc, editdistance(transformed_source_name, matching_target_name)) = 1\
)\
select\
    t.target_system_name,\
    t.target_database_name,\
    t.target_schema_name,\
    t.target_object_name,\
    concat_ws(''.'', t.target_system_name, t.target_database_name, t.target_schema_name, t.target_object_name) as table_name,\
    ''create \{''or replace '' if or_replace else ''''\}table \{'''' if or_replace else ''if not exists ''\}"'' || concat_ws(''"."'', t.target_database_name, t.target_schema_name, t.target_object_name) || ''" (\
    '' || array_to_string(array_agg(''"'' || tc.value:COLUMN_NAME || ''" '' || case\
            when tc.value:DATA_TYPE = ''NUMBER'' then tc.value:DATA_TYPE || ''('' || concat_ws('','', tc.value:NUMERIC_PRECISION, tc.value:NUMERIC_SCALE) || '')''\
            when tc.value:DATA_TYPE like ''TIME%'' then tc.value:DATA_TYPE || ''('' || nvl(tc.value:DATETIME_PRECISION, 9) || '')''\
            else tc.value:DATA_TYPE\
        end || iff(upper(tc.value:IS_NULLABLE) in (''TRUE'', ''YES'', ''ON'', ''T'', ''Y'', ''1''), '''', '' NOT NULL'')) within group (order by tc.value:ORDINAL_POSITION), '',\
    '') || ''\
)'' as table_ddl,\
    t.source_object_id,\
    t.target_columns,\
    dmva_current_timestamp_utc() as current_timestamp_utc,\
    false as table_created,\
    null as create_message\
from\
    targets t,\
    lateral flatten(t.target_columns) tc\
group by\
    t.target_system_name,\
    t.target_database_name,\
    t.target_schema_name,\
    t.target_object_name,\
    t.source_object_id,\
    t.target_columns""").collect()\
    results_count = session.sql(f''select count(1) from \{results_table\}'').collect()[0][0]\
    result = [ \{ ''RESULTS_TABLE'': \{ ''CREATE'': create_results[0].as_dict()[''status''], ''RECORD_COUNT'': results_count \} \} ]\
\
    # MH START\
    if results_count > 0:\
        result += [ upsert_objects(session, results_table) ]\
        result += Parallel(n_jobs=-1)(delayed(upsert)(session, results_table) for upsert in [ upsert_columns, upsert_mappings ])\
        #result += [ upsert_mappings(session, results_table) ]\
        #result += Parallel(n_jobs=-1)(delayed(upsert)(session, results_table) for upsert in [ upsert_mappings ])\
    # MH END\
    \
    if results_count > 0 and create_tables:\
        \
        # check database(s)\
        databases = session.sql(f"""select\
    rt.target_database_name,\
    ''create database if not exists "'' || rt.target_database_name || ''"'' as create_ddl,\
    iff(d.database_name is null, 1, 0) as create_database,\
    array_unique_agg(rt.target_schema_name) as target_schema_names\
from\
    \{results_table\} rt\
    left join snowflake.information_schema.databases d\
    on rt.target_database_name = d.database_name\
group by\
    rt.target_database_name,\
    d.database_name""").collect()\
        create_databases = [ d for d in databases if d[2] > 0 ]\
        database_result = \{\
            ''EXPECTED'': len(databases),\
            ''FOUND'': sum(1 for d in databases if d[2] == 0),\
            ''NOT_FOUND'': len(create_databases)\
        \}\
        if create_databases:\
            create_database_results = Parallel(n_jobs=-1)(delayed(create_object)(session, *c[0:2]) for c in create_databases)\
            database_result.update(\{\
                ''CREATED'': sum(1 for r in create_database_results if r[''object_created'']),\
                ''NOT_CREATED'': sum(1 for r in create_database_results if not(r[''object_created'']))\
            \})\
            _ = session.sql(f''use schema \{calling_database_schema\}'').collect()\
        result += [ \{ ''DATABASES'': database_result \} ]\
        \
        # check schema(s)\
        schemas = session.sql(''\\\\nunion all\\\\n''.join([ f"""select\
    s.value::varchar as target_schema_name,\
    ''create schema if not exists "\{d[0]\}"."'' || s.value || ''"'' as create_ddl,\
    iff(s2.schema_name is null, 1, 0) as create_schema\
from\
    table(flatten(parse_json(''\{d[3]\}''))) s\
    left join \{d[0]\}.information_schema.schemata s2\
    on s2.schema_name = s.value""" for d in databases ])).collect()\
        create_schemas = [ s for s in schemas if s[2] > 0 ]\
        schema_result = \{\
            ''EXPECTED'': len(schemas),\
            ''FOUND'': sum(1 for s in schemas if s[2] == 0),\
            ''NOT_FOUND'': len(create_schemas)\
        \}\
        if create_schemas:\
            create_schema_results = Parallel(n_jobs=-1)(delayed(create_object)(session, *c[0:2]) for c in create_schemas)\
            schema_result.update(\{\
                ''CREATED'': sum(1 for r in create_schema_results if r[''object_created'']),\
                ''NOT_CREATED'': sum(1 for r in create_schema_results if not(r[''object_created'']))\
            \})\
            _ = session.sql(f''use schema \{calling_database_schema\}'').collect()\
        result += [ \{ ''SCHEMAS'': schema_result \} ]\
        \
        # create table(s)\
        tables = session.sql(f''select table_name, table_ddl from \{results_table\}'').collect()\
        create_table_results = Parallel(n_jobs=-1)(delayed(create_object)(session, *table) for table in tables)\
        tables_created = sum(1 for r in create_table_results if r[''object_created''])\
        table_result = \{\
            ''EXPECTED'': len(tables),\
            ''CREATED'': tables_created,\
            ''NOT_CREATED'': sum(1 for r in create_table_results if not(r[''object_created'']))\
        \}\
        result += [ \{ ''TABLES'': table_result \} ]\
        \
        # update driver with table creation result(s)\
        results_table_tmp_vw = f''\{results_table\}_tmp_vw''\
        _ = session.create_dataframe(create_table_results).create_or_replace_temp_view(results_table_tmp_vw)\
        _ = session.sql(f''update \{results_table\} t set t.table_created = s.object_created, t.create_message = s.create_message from \{results_table_tmp_vw\} s where t.table_name = s.object_name'').collect()\
\
        # upsert/prune metadata\
        if tables_created > 0:\
            result += [ upsert_objects(session, results_table) ]\
            result += Parallel(n_jobs=-1)(delayed(upsert)(session, results_table) for upsert in [ upsert_columns, upsert_mappings ])\
            result += [ prune_columns(session, results_table) ]\
        \
    return result\
';}