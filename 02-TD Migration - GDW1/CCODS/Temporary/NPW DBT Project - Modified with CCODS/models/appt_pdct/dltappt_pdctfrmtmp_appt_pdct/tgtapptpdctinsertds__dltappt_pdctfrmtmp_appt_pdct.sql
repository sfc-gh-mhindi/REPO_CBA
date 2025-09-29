{{
  config(
    post_hook=[
      "INSERT OVERWRITE INTO " ~ cvar("datasets_schema") ~ "." ~ cvar("base_dir") ~ "__dataset__"~ cvar("tgt_table") ~ "_I_"~ cvar("run_stream") ~ "_"~ cvar("etl_process_dt_tbl") ~ "__DS SELECT * FROM {{ this }}"
    ]
  )
}}

WITH XfmCheckDeltaAction__OutTgtApptPdctInsertDS AS (
	SELECT
		COALESCE(trans.NEW_APPT_PDCT_I,'') AS APPT_PDCT_I,
		trans.NEW_DEBT_ABN_X AS DEBT_ABN_X,
		trans.NEW_DEBT_BUSN_M AS DEBT_BUSN_M,
		trans.NEW_SMPL_APPT_F AS SMPL_APPT_F,
		trans.NEW_JOB_COMM_CATG_C AS JOB_COMM_CATG_C,
		trans.NEW_APPT_QLFY_C AS APPT_QLFY_C,
		trans.NEW_ACQR_TYPE_C AS ACQR_TYPE_C,
		trans.NEW_ACQR_ADHC_X AS ACQR_ADHC_X,
		trans.NEW_ACQR_SRCE_C AS ACQR_SRCE_C,
		COALESCE(NEW_PDCT_N,'') AS PDCT_N,
		COALESCE(NEW_APPT_I,'') as APPT_I,
		COALESCE(NEW_SRCE_SYST_C,'') as SRCE_SYST_C,
		COALESCE(NEW_SRCE_SYST_APPT_PDCT_I,'') as SRCE_SYST_APPT_PDCT_I,
		trans.NEW_LOAN_FNDD_METH_C AS LOAN_FNDD_METH_C,
		trans.NEW_NEW_ACCT_F AS NEW_ACCT_F,
		trans.NEW_BROK_PATY_I AS BROK_PATY_I,
		trans.NEW_COPY_FROM_OTHR_APPT_F AS COPY_FROM_OTHR_APPT_F,
		to_date(OLD_APPT_I) AS EFFT_D,
		to_date(OLD_EFFT_D) AS EXPY_D,
		'{{ cvar("refr_pk") }}' AS PROS_KEY_EFFT_I,
		null AS PROS_KEY_EXPY_I,
		null AS EROR_SEQN_I,
		trans.NEW_APPT_PDCT_CATG_C AS APPT_PDCT_CATG_C,
		trans.NEW_APPT_PDCT_DURT_C AS APPT_PDCT_DURT_C,
		trans.NEW_ASES_D AS ASES_D
	FROM {{ ref('xfmcheckdeltaaction__dltappt_pdctfrmtmp_appt_pdct') }} trans
	WHERE trans.change_code = trans."INSERT"
        OR trans.change_code = trans."UPDATE"
)

SELECT
	APPT_PDCT_I,
	DEBT_ABN_X,
	DEBT_BUSN_M,
	SMPL_APPT_F,
	JOB_COMM_CATG_C,
	APPT_QLFY_C,
	ACQR_TYPE_C,
	ACQR_ADHC_X,
	ACQR_SRCE_C,
	PDCT_N,
	APPT_I,
	SRCE_SYST_C,
	SRCE_SYST_APPT_PDCT_I,
	LOAN_FNDD_METH_C,
	NEW_ACCT_F,
	BROK_PATY_I,
	COPY_FROM_OTHR_APPT_F,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I,
	APPT_PDCT_CATG_C,
	APPT_PDCT_DURT_C,
	ASES_D 
FROM XfmCheckDeltaAction__OutTgtApptPdctInsertDS