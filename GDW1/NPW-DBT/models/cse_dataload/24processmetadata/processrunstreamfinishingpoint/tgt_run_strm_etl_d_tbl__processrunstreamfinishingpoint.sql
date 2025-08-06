{{
    config(
        post_hook=[
            "UPDATE "~ cvar("stg_ctl_db")~"."~ cvar("ctl_schema") ~ ".run_strm_etl_d tgt SET tgt.ETL_D = src.ETL_D FROM {{ this }} src WHERE src.RS_M = tgt.RS_M"
        ],
    )
}}

select 
    '{{ cvar("run_stream") }}' as RS_M,
    DATEADD(DAY, 1, TO_DATE(ETL_D, 'YYYY-MM-DD')) AS ETL_D

from {{ ref("xfm_rs_finish__processrunstreamfinishingpoint") }}

