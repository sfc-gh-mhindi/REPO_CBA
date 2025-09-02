{{
    config(
        post_hook=[
            'INSERT OVERWRITE INTO '~ cvar("intermediate_db")~'.'~ cvar("files_schema")~ '.'~ cvar("base_dir")~ '__lookupset__finish_rs_etl_d_'~ cvar("run_stream") ~ '_hash (ETL_D) SELECT ETL_D FROM {{ this }}'
        ]
    )
}}


select ETL_D
from {{ ref("src_run_strm_etl_d_tbl__processrunstreamfinishingpoint") }}
