{# 
  ============================================================================
  Custom Iceberg CLD Table Materialization
  ============================================================================
  
  This custom materialization implements table logic for Iceberg tables 
  in catalog-linked databases (CLD). It solves the issue where dbt's standard 
  materialization fails because it tries to create temporary objects in 
  read-only catalog-linked databases.
  
  Key Features:
  - Creates temporary objects in writable database (DCF) while targeting read-only database (CLD)
  - Supports multiple incremental strategies: truncate-load, merge, delete+insert, append
  - Handles Iceberg table limitations (no ALTER operations)
  - Works with pre/post hooks for DCF framework integration
  - Supports configurable temporary relation types (view/table)
  
  Configuration Options:
  - tmp_database: Database for temporary objects (default: DCF database)
  - tmp_schema: Schema for temporary objects (default: DCF schema)
  - tmp_relation_type: Type of temporary relation (default: view)
  - incremental_strategy: Strategy for incremental loads (default: truncate-load)
  
  Usage:
    {{ config(
        materialized='ibrg_cld_table',
        incremental_strategy='truncate-load',
        schema='PDSRCCS',
        tmp_database=var('dcf_database'),
        tmp_schema=var('dcf_schema'),
        tmp_relation_type='view'
    ) }}
  ============================================================================
#}

{% materialization ibrg_cld_table, adapter='snowflake' %}

  {%- set full_refresh_mode = (should_full_refresh()) -%}
  {%- set target_relation = this -%}
  {%- set existing_relation = load_relation(this) -%}
  
  {# Custom configuration for catalog-linked database compatibility #}
  {%- set tmp_database = config.get('tmp_database', var('dcf_database', target.database)) -%}
  {%- set tmp_schema = config.get('tmp_schema', var('dcf_schema', target.schema)) -%}
  {%- set tmp_relation_type = config.get('tmp_relation_type', 'view') -%}
  {%- set incremental_strategy = config.get('incremental_strategy', 'truncate-load') -%}
  
  {# Create custom tmp_relation using specified database/schema #}
  {%- set tmp_relation = api.Relation.create(
      identifier=target_relation.identifier ~ '__dbt_tmp',
      schema=tmp_schema,
      database=tmp_database,
      type=tmp_relation_type
  ) -%}

  {%- set intermediate_relation = api.Relation.create(
      identifier=target_relation.identifier ~ '__dbt_intermediate',
      schema=tmp_schema,
      database=tmp_database,
      type='table'
  ) -%}

  {# Execute pre-hooks before any main logic #}
  {{ run_hooks(pre_hooks) }}

  {%- set build_sql = sql -%}

  {% if existing_relation is none %}
    {# Initial table creation #}
    {% call statement('main') -%}
      {{ create_table_as(False, target_relation, build_sql) }}
    {%- endcall %}

  {% elif full_refresh_mode %}
    {# Full refresh mode - create intermediate table, then swap #}
    {% call statement('create_intermediate') -%}
      {{ create_table_as(False, intermediate_relation, build_sql) }}
    {%- endcall %}
    
    {# Drop the existing table #}
    {% call statement('drop_existing') -%}
      DROP TABLE {{ target_relation }}
    {%- endcall %}
    
    {# Rename intermediate to target #}
    {% call statement('rename_intermediate_to_target') -%}
      ALTER TABLE {{ intermediate_relation }} RENAME TO {{ target_relation.identifier }}
    {%- endcall %}

  {% else %}
    {# Incremental mode - apply strategy-specific logic #}
    
    {# Create temporary relation in DCF database (writable) #}
    {% if tmp_relation_type == 'view' %}
      {% call statement('create_tmp_relation') -%}
        {{ create_view_as(tmp_relation, build_sql) }}
      {%- endcall %}
    {% else %}
      {% call statement('create_tmp_relation') -%}
        {{ create_table_as(False, tmp_relation, build_sql) }}
      {%- endcall %}
    {% endif %}

    {# Get column information #}
    {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
    {% set dest_cols_csv = dest_columns | map(attribute="quoted") | join(', ') %}
    
    {# Apply incremental strategy #}
    {% if incremental_strategy == 'truncate-load' %}
      {% call statement('main') -%}
        {# Truncate-Load Pattern: DELETE ALL + INSERT ALL #}
        
        -- Step 1: Delete all existing data (truncate)
        DELETE FROM {{ target_relation }};
        
        -- Step 2: Insert all new data from temporary relation
        INSERT INTO {{ target_relation }} ({{ dest_cols_csv }})
        SELECT {{ dest_cols_csv }}
        FROM {{ tmp_relation }};
        
      {%- endcall %}
    
    {% elif incremental_strategy == 'append' %}
      {% call statement('main') -%}
        {# Append Pattern: INSERT ALL (no delete) #}
        
        INSERT INTO {{ target_relation }} ({{ dest_cols_csv }})
        SELECT {{ dest_cols_csv }}
        FROM {{ tmp_relation }};
        
      {%- endcall %}
    
    {% elif incremental_strategy == 'scd_type2' %}
      {% call statement('main') -%}
        {# Type 2 SCD Merge Pattern: End-date existing records and insert new versions #}
        
        {%- set unique_key = config.get('unique_key', []) -%}
        {%- if not unique_key -%}
          {{ exceptions.raise_compiler_error('unique_key must be specified for scd_type2 strategy') }}
        {%- endif -%}
        
        {# Build unique key match clause #}
        {%- if unique_key is sequence and unique_key is not string -%}
          {%- set unique_key_match -%}
            {%- for key in unique_key -%}
              DBT_INTERNAL_DEST.{{ key }} = DBT_INTERNAL_SOURCE.{{ key }}
              {%- if not loop.last %} AND {% endif -%}
            {%- endfor -%}
          {%- endset -%}
        {%- else -%}
          {%- set unique_key_match -%}
            DBT_INTERNAL_DEST.{{ unique_key }} = DBT_INTERNAL_SOURCE.{{ unique_key }}
          {%- endset -%}
        {%- endif -%}
        
        -- Type 2 SCD MERGE: End-date existing records and insert new versions
        MERGE INTO {{ target_relation }} AS DBT_INTERNAL_DEST
        USING (
          WITH 
          CTE_SRC AS (
            SELECT * FROM {{ tmp_relation }}
          ),
          CTE_TGT_CURR AS (
            SELECT * FROM {{ target_relation }}
            WHERE EXPY_D = '9999-12-31'
          )
          
          -- Insert for NEW records (not in target)
          SELECT NULL as MERGE_KEY, 
            {% for column in dest_columns -%}
              S1.{{ column.quoted }}
              {%- if not loop.last %},{% endif -%}
            {%- endfor %}
          FROM CTE_SRC S1
          WHERE NOT EXISTS (
            SELECT 1 FROM CTE_TGT_CURR T1 
            WHERE {{ unique_key_match | replace('DBT_INTERNAL_DEST', 'T1') | replace('DBT_INTERNAL_SOURCE', 'S1') }}
          )
          
          UNION ALL
          
          -- Insert NEW VERSION for CHANGED records (data comparison)
          SELECT NULL as MERGE_KEY,
            {% for column in dest_columns -%}
              S2.{{ column.quoted }}
              {%- if not loop.last %},{% endif -%}
            {%- endfor %}
          FROM CTE_SRC S2 
          INNER JOIN CTE_TGT_CURR T2 ON {{ unique_key_match | replace('DBT_INTERNAL_DEST', 'T2') | replace('DBT_INTERNAL_SOURCE', 'S2') }}
          WHERE (
            {%- set comparison_columns = [] -%}
            {%- for column in dest_columns -%}
              {%- if column.name.upper() not in ['EFFT_D', 'EXPY_D', 'PROS_KEY_EFFT_I', 'PROS_KEY_EXPY_I', 'EROR_SEQN_I'] -%}
                {%- do comparison_columns.append(column) -%}
              {%- endif -%}
            {%- endfor -%}
            {%- for column in comparison_columns -%}
              NVL(CAST(T2.{{ column.quoted }} AS STRING), 'NULL') != NVL(CAST(S2.{{ column.quoted }} AS STRING), 'NULL')
              {%- if not loop.last %} OR {% endif -%}
            {%- endfor %}
          )
          
          UNION ALL
          
          -- Update (close) OLD VERSION for CHANGED records
          SELECT 'UPD' as MERGE_KEY,
            {% for column in dest_columns -%}
              T3.{{ column.quoted }}
              {%- if not loop.last %},{% endif -%}
            {%- endfor %}
          FROM CTE_SRC S3
          INNER JOIN CTE_TGT_CURR T3 ON {{ unique_key_match | replace('DBT_INTERNAL_DEST', 'T3') | replace('DBT_INTERNAL_SOURCE', 'S3') }}
          WHERE (
            {%- for column in comparison_columns -%}
              NVL(CAST(T3.{{ column.quoted }} AS STRING), 'NULL') != NVL(CAST(S3.{{ column.quoted }} AS STRING), 'NULL')
              {%- if not loop.last %} OR {% endif -%}
            {%- endfor %}
          )
          
          UNION ALL
          
          -- Update (close) DELETED records (in target but not in source)
          SELECT 'DEL' as MERGE_KEY,
            {% for column in dest_columns -%}
              T4.{{ column.quoted }}
              {%- if not loop.last %},{% endif -%}
            {%- endfor %}
          FROM CTE_TGT_CURR T4
          WHERE NOT EXISTS (
            SELECT 1 FROM CTE_SRC S4 
            WHERE {{ unique_key_match | replace('DBT_INTERNAL_DEST', 'T4') | replace('DBT_INTERNAL_SOURCE', 'S4') }}
          )
          
        ) AS DBT_INTERNAL_SOURCE
        ON {{ unique_key_match }}
           AND DBT_INTERNAL_DEST.EXPY_D = '9999-12-31'
           AND DBT_INTERNAL_SOURCE.MERGE_KEY IN ('UPD', 'DEL')

        -- UPDATE: Close existing records (changed or deleted)
        WHEN MATCHED THEN 
          UPDATE SET 
            EXPY_D = (SELECT BUS_DT FROM {{ var('dcf_database') }}.{{ var('dcf_schema') }}.DCF_T_STRM_BUS_DT WHERE STRM_NAME = '{{ var("run_stream") }}' AND PROCESSING_FLAG = 1 LIMIT 1),
            PROS_KEY_EXPY_I = DBT_INTERNAL_SOURCE.PROS_KEY_EFFT_I

        -- INSERT: New records and new versions of changed records
        WHEN NOT MATCHED THEN
          INSERT ({{ dest_cols_csv }})
          VALUES (
            {% for column in dest_columns -%}
              DBT_INTERNAL_SOURCE.{{ column.quoted }}
              {%- if not loop.last %},{% endif -%}
            {%- endfor %}
          );
        
      {%- endcall %}
    
    {% else %}
      {{ exceptions.raise_compiler_error('Incremental strategy "' ~ incremental_strategy ~ '" is not supported by ibrg_cld_table materialization. Supported strategies: truncate-load, append, scd_type2') }}
    {% endif %}
    
    {# Clean up temporary relation #}
    {% if tmp_relation_type == 'view' %}
      {% call statement('cleanup_tmp_relation') -%}
        DROP VIEW IF EXISTS {{ tmp_relation }}
      {%- endcall %}
    {% else %}
      {% call statement('cleanup_tmp_relation') -%}
        DROP TABLE IF EXISTS {{ tmp_relation }}
      {%- endcall %}
    {% endif %}
    
  {% endif %}

  {# Execute post-hooks #}
  {{ run_hooks(post_hooks) }}

  {{ adapter.commit() }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}