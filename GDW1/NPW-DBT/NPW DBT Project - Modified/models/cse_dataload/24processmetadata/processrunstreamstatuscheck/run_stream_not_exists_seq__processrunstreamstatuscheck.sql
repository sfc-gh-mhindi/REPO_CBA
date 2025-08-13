{{
    config(
        post_hook=[
            'INSERT OVERWRITE INTO ' ~ cvar('intermediate_db') ~ '.'~ cvar("files_schema")~ '.' ~ cvar("base_dir") ~ '__temp__' ~ cvar("run_stream")~ '_runstreamdoesnotexist_err (RUN_STRM_COUNT) SELECT RUN_STRM_COUNT FROM {{ this }} limit 1',
            "{{ run_stream_check('transformer_58__processrunstreamstatuscheck') }}"
        ]
    )
}}

with run_stream_not_exists_seq as (
    select run_strm_count
    from {{ ref("transformer_58__processrunstreamstatuscheck") }}
    where run_strm_count = -1
)

select *
from run_stream_not_exists_seq
