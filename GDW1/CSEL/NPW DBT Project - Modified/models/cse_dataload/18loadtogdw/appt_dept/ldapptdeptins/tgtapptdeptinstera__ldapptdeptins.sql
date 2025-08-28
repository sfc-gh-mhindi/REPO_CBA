{{
  config(
    post_hook=[
	'MERGE INTO '~cvar("mart_db")~'.'~cvar("gdw_acct_db")~'.'~cvar("tgt_table")~' AS tgt
USING {{ this }} AS src
ON ' ~ cmerge_condition(cvar("tgt_table")) ~
' WHEN NOT MATCHED THEN
    INSERT (
        appt_i,
        dept_role_c,
        efft_d,
        dept_i,
        expy_d,
        pros_key_efft_i,
        pros_key_expy_i,
        eror_seqn_i
    )
    VALUES (
        src.APPT_I,
        src.DEPT_ROLE_C,
        src.EFFT_D,
        src.DEPT_I,
        src.EXPY_D,
        src.PROS_KEY_EFFT_I,
        src.PROS_KEY_EXPY_I,
        src.EROR_SEQN_I
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
	EROR_SEQN_I

FROM {{ ref('tgtapptdeptinsertds__ldapptdeptins') }}