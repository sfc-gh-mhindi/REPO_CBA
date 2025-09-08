{{
    config(
        post_hook=[
            "INSERT OVERWRITE INTO "~cvar("intermediate_db")~'.'~ cvar("files_schema")~ "."~ cvar("base_dir")~ "__lookupset__run_stream_name_check_" ~ cvar("run_stream") ~ "_hash (RUN_STRM_COUNT) SELECT RUN_STRM_COUNT FROM {{ this }}"
        ]
    )
}}

with hashed_file_61 as (
    select run_strm_count
    from {{ ref("transformer_58__processrunstreamstatuscheck") }}
    where run_strm_count = 0
)

select *
from hashed_file_61
