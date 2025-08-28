{{
    config(
        post_hook=[
            'UPDATE ' ~ cvar("mart_db") ~'.'~ cvar("gdw_acct_db") ~ '.' ~ cvar("tgt_table") ~' tgt SET tgt.expy_d = src.EXPY_D, tgt.pros_key_expy_i = src.PROS_KEY_EXPY_I FROM {{ this }} src WHERE src.DEPT_I = tgt.dept_i and src.APPT_I = tgt.appt_i and src.DEPT_ROLE_C = tgt.dept_role_c and src.EFFT_D = tgt.efft_d'
        ]
    )
}}

SELECT
	DEPT_I,
	APPT_I,
	DEPT_ROLE_C,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EXPY_I 
FROM {{ ref('tgtapptdeptupdateds__ldapptdeptupd') }}
