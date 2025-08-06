
WITH JoinAll AS (
	SELECT
		ChangeCapture.NEW_APPT_PDCT_I,
		ChangeCapture.NEW_DEBT_ABN_X,
		ChangeCapture.NEW_DEBT_BUSN_M,
		ChangeCapture.NEW_SMPL_APPT_F,
		ChangeCapture.NEW_JOB_COMM_CATG_C,
		ChangeCapture.NEW_APPT_QLFY_C,
		ChangeCapture.NEW_ACQR_TYPE_C,
		ChangeCapture.NEW_ACQR_ADHC_X,
		ChangeCapture.NEW_ACQR_SRCE_C,
		ChangeCapture.NEW_PDCT_N,
		ChangeCapture.NEW_APPT_I,
		ChangeCapture.NEW_SRCE_SYST_C,
		ChangeCapture.NEW_SRCE_SYST_APPT_PDCT_I,
		ChangeCapture.NEW_LOAN_FNDD_METH_C,
		ChangeCapture.NEW_NEW_ACCT_F,
		ChangeCapture.NEW_BROK_PATY_I,
		ChangeCapture.NEW_COPY_FROM_OTHR_APPT_F,
		ChangeCapture.NEW_APPT_PDCT_CATG_C,
		ChangeCapture.NEW_APPT_PDCT_DURT_C,
		ChangeCapture.NEW_ASES_D,
		CpyApptPdct.OLD_APPT_I,
		CpyApptPdct.OLD_EFFT_D,
		ChangeCapture.change_code
	FROM {{ ref('changecapture__dltappt_pdctfrmtmp_appt_pdct') }} ChangeCapture
	INNER JOIN {{ ref('cpyapptpdct__dltappt_pdctfrmtmp_appt_pdct') }} CpyApptPdct 
    ON ChangeCapture.NEW_APPT_PDCT_I = CpyApptPdct.NEW_APPT_PDCT_I
)

SELECT * FROM JoinAll