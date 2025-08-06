{{
  config(
    post_hook=[
      "INSERT INTO "~ cvar("stg_ctl_db") ~"."~ cvar("gdw_stag_db") ~ "."~ cvar("tgt_table") ~ " SELECT * FROM {{ this }}"
    ]
  )
}}


SELECT
	APPT_I,
	DEPT_ROLE_C,
	EFFT_D,
	DEPT_I,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I,
	RUN_STRM 
FROM {{ ref('srcapptdeptds__ldtmp_appt_deptrmxfm') }}