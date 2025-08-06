{{
  config(
    post_hook=[
      'INSERT INTO ' ~ cvar("mart_db") ~'.'~ cvar("gdw_acct_db") ~ '.' ~ cvar("tgt_table") ~' ("appt_i", "dept_role_c", "efft_d", "dept_i", "expy_d", "pros_key_efft_i", "pros_key_expy_i", "eror_seqn_i") SELECT * FROM {{ this}}'
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