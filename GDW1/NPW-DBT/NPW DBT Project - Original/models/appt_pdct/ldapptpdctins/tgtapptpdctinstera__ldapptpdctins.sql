{{
  config(
          post_hook=[
	'MERGE INTO '~cvar("mart_db")~'.'~cvar("gdw_acct_db")~'.'~cvar("tgt_table")~' AS tgt
USING {{ this }} AS src
ON tgt."appt_i" = src.appt_i
   WHEN NOT MATCHED THEN
    INSERT (
          "appt_pdct_i",
          "appt_qlfy_c",
          "acqr_type_c",
          "acqr_adhc_x",
          "acqr_srce_c",
          "pdct_n",
          "appt_i",
          "srce_syst_c",
          "srce_syst_appt_pdct_i",
          "loan_fndd_meth_c",
          "new_acct_f",
          "brok_paty_i",
          "copy_from_othr_appt_f",
          "efft_d",
          "expy_d",
          "pros_key_efft_i",
          "pros_key_expy_i",
          "eror_seqn_i",
          "job_comm_catg_c",
          "debt_abn_x",
          "debt_busn_m",
          "smpl_appt_f",
          "appt_pdct_catg_c",
          "appt_pdct_durt_c",
          "ases_d"
    )
    VALUES (
          src.appt_pdct_i,
          src.appt_qlfy_c,
          src.acqr_type_c,
          src.acqr_adhc_x,
          src.acqr_srce_c,
          src.pdct_n,
          src.appt_i,
          src.srce_syst_c,
          src.srce_syst_appt_pdct_i,
          src.loan_fndd_meth_c,
          src.new_acct_f,
          src.brok_paty_i,
          src.copy_from_othr_appt_f,
          src.efft_d,
          src.expy_d,
          src.pros_key_efft_i,
          src.pros_key_expy_i,
          src.eror_seqn_i,
          src.job_comm_catg_c,
          src.debt_abn_x,
          src.debt_busn_m,
          src.smpl_appt_f,
          src.appt_pdct_catg_c,
          src.appt_pdct_durt_c,
          src.ases_d
    );'
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
    