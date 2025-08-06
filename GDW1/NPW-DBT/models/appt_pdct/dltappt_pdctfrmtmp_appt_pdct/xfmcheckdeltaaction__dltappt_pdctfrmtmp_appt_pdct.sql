WITH xfmcheckdeltaaction AS (
		SELECT
		NEW_APPT_PDCT_I,
		NEW_DEBT_ABN_X,
		NEW_DEBT_BUSN_M,
		NEW_SMPL_APPT_F,
		NEW_JOB_COMM_CATG_C,
		NEW_APPT_QLFY_C,
		NEW_ACQR_TYPE_C,
		NEW_ACQR_ADHC_X,
		NEW_ACQR_SRCE_C,
		NEW_PDCT_N,
		NEW_APPT_I,
		NEW_SRCE_SYST_C,
		NEW_SRCE_SYST_APPT_PDCT_I,
		NEW_LOAN_FNDD_METH_C,
		NEW_NEW_ACCT_F,
		NEW_BROK_PATY_I,
		NEW_COPY_FROM_OTHR_APPT_F,
		NEW_APPT_PDCT_CATG_C,
		NEW_APPT_PDCT_DURT_C,
		NEW_ASES_D,
		OLD_APPT_I,
		OLD_EFFT_D,
		change_code,
		1 AS "INSERT",
		3 AS "UPDATE",
        DATEADD(day, -1, TO_DATE( TO_VARCHAR('{{ cvar("etl_process_dt") }}'), 'YYYYMMDD')) as expirydate
		FROM {{ ref('joinall__dltappt_pdctfrmtmp_appt_pdct') }} 
)
select * from xfmcheckdeltaaction