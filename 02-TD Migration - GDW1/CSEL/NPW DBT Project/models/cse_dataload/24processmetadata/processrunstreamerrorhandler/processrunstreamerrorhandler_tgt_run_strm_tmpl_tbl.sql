{{
    config(
        post_hook=[
            "UPDATE "~ cvar("stg_ctl_db") ~"."~ cvar("ctl_schema") ~ ".run_strm_tmpl tgt SET tgt.RUN_STRM_ABRT_F = src.RUN_STRM_ABRT_F, tgt.RUN_STRM_ACTV_F = src.RUN_STRM_ACTV_F, tgt.RECD_CRAT_S = src.RECD_CRAT_S FROM {{ this }} src WHERE src.RUN_STRM_C = tgt.RUN_STRM_C"
        ],
    )
}}



SELECT
	'{{ cvar("run_stream") }}' as run_strm_c,
	'Y' as run_strm_abrt_f,
	'I' as run_strm_actv_f,
	current_timestamp as recd_crat_s 
FROM {{ ref('processrunstreamerrorhandler_xfm_err_handling') }}