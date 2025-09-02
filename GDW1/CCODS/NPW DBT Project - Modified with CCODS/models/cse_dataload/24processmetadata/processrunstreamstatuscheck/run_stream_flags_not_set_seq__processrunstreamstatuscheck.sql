{{
    config(
        post_hook=[
            'INSERT OVERWRITE INTO '~cvar("intermediate_db")~'.'~ cvar("files_schema") ~ '.'~ cvar("base_dir")~ '__temp__'~ cvar("run_stream")~ '_processmd02_err (ERR_COL) SELECT ERR_COL FROM {{ this}} limit 1',
            "{{ run_stream_check('transformer_7__processrunstreamstatuscheck') }}",
        ]
    )
}}

with run_stream_flags_not_set_seq as (
    select
        case
            when streamaborted = 'Y'
            then 'Run stream has aborted flag set to Y in RUN_STRM_TMPL.'
            when streamactive = 'Y'
            then 'Run stream has active flag set to Y in RUN_STRM_TMPL.'
            else ''
        end as err_col
    from {{ ref("transformer_7__processrunstreamstatuscheck") }}
    where streamaborted = 'Y' or streamactive = 'Y'
)

select err_col
from run_stream_flags_not_set_seq
