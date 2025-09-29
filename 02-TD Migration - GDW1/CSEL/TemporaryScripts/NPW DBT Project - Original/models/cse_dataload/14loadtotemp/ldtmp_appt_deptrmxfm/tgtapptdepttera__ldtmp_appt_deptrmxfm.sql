{{
  config(
    post_hook=[
	'MERGE INTO '~cvar("stg_ctl_db")~'.'~cvar("gdw_stag_db")~'.'~cvar("tgt_table")~' AS tgt
USING {{ this }} AS src
ON tgt.APPT_I = src.APPT_I
WHEN NOT MATCHED THEN
    INSERT (
        APPT_I,
        DEPT_ROLE_C,
        EFFT_D,
        DEPT_I,
        EXPY_D,
        PROS_KEY_EFFT_I,
        PROS_KEY_EXPY_I,
        EROR_SEQN_I,
        RUN_STRM
    )
    VALUES (
        src.APPT_I,
        src.DEPT_ROLE_C,
        src.EFFT_D,
        src.DEPT_I,
        src.EXPY_D,
        src.PROS_KEY_EFFT_I,
        src.PROS_KEY_EXPY_I,
        src.EROR_SEQN_I,
        src.RUN_STRM
    );'
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