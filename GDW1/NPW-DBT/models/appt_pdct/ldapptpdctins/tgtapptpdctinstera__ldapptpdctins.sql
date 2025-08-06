{{
  config(
          post_hook=[
            'INSERT INTO ' ~ cvar("mart_db") ~'.'~ cvar("gdw_acct_db") ~ '.' ~ cvar("tgt_table") ~ ' SELECT * FROM {{ this}}'
            ]
  )
}}

SELECT
appt_pdct_i,
appt_qlfy_c,
acqr_type_c,
acqr_adhc_x,
acqr_srce_c,
pdct_n,
appt_i,
srce_syst_c,
srce_syst_appt_pdct_i,
loan_fndd_meth_c,
new_acct_f,
brok_paty_i,
copy_from_othr_appt_f,
efft_d,
expy_d,
pros_key_efft_i,
pros_key_expy_i,
eror_seqn_i,
job_comm_catg_c,
debt_abn_x,
debt_busn_m,
smpl_appt_f,
appt_pdct_catg_c,
appt_pdct_durt_c,
ases_d

FROM {{ ref("tgtapptpdctinsertds__ldapptpdctins") }}
    