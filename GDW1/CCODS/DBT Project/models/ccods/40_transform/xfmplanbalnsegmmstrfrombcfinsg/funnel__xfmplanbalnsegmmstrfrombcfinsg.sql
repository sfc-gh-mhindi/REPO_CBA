with BcfDtSpcTrmsEd as (

	SELECT 
		CONCAT(TRIM(BCF_CORP), TRIM(BCF_ACCOUNT_NO1), TRIM(BCF_PLAN_ID)) AS SRCE_KEY_I,
		'{{ cvar("pods_load_user") }}' AS CONV_M,
		'Invalid record due to date' AS CONV_MAP_RULE_M,
		'PLAN_BALN_SEGM_MSTR' AS TRSF_TABL_M,
		TO_DATE('{{ cvar("prun_strm_pros_d") }}', 'YYYYMMDD') AS SRCE_EFFT_D,
		TRIM(BCF_DT_SPEC_TERMS_END) AS VALU_CHNG_BFOR_X,
		'1111-11-11' AS VALU_CHNG_AFTR_X,
		'xfmplanbalnsegmmstrfrombcfinsg' AS TRSF_X,
		'BCF_DT_SPEC_TERMS_END' AS TRSF_COLM_M,
		NULL AS EROR_SEQN_I,
		'BCFINSG' AS SRCE_FILE_M,
		'{{ cvar("pods_pros_id") }}' AS PROS_KEY_EFFT_I,
		CONCAT(TRIM(BCF_CORP), 'CCSCC', TRIM(BCF_ACCOUNT_NO1), TRIM(BCF_PLAN_ID)) AS TRSF_KEY_I
		
	FROM {{ ref('xfmtotable__xfmplanbalnsegmmstrfrombcfinsg') }}
	WHERE svIsValidBcfDtSpecTermsEnd = 'N' AND svIsValidRecord = 'N'

),
BcfDtIntDefr AS (

	SELECT
		CONCAT(TRIM(BCF_CORP), TRIM(BCF_ACCOUNT_NO1), TRIM(BCF_PLAN_ID)) AS SRCE_KEY_I,
		'{{ cvar("pods_load_user") }}' AS CONV_M,
		'Invalid record due to date' AS CONV_MAP_RULE_M,
		'PLAN_BALN_SEGM_MSTR' AS TRSF_TABL_M,
		TO_DATE('{{ cvar("prun_strm_pros_d") }}', 'YYYYMMDD') AS SRCE_EFFT_D,
		TRIM(BCF_DT_INTEREST_DEFER) AS VALU_CHNG_BFOR_X,
		'1111-11-11' AS VALU_CHNG_AFTR_X,
		'xfmplanbalnsegmmstrfrombcfinsg' AS TRSF_X,
		'BCF_DT_INTEREST_DEFER' AS TRSF_COLM_M,
		NULL AS EROR_SEQN_I,
	   'BCFINSG' AS SRCE_FILE_M,
		'{{ cvar("pods_pros_id") }}' AS PROS_KEY_EFFT_I,
		CONCAT(TRIM(BCF_CORP), 'CCSCC', TRIM(BCF_ACCOUNT_NO1), TRIM(BCF_PLAN_ID)) AS TRSF_KEY_I
		
	FROM {{ ref('xfmtotable__xfmplanbalnsegmmstrfrombcfinsg') }}
	WHERE svIsValidBcfDtInterestDefer = 'N' AND svIsValidRecord = 'N'

),
BcfDtPymtDefr AS (

	SELECT
		CONCAT(TRIM(BCF_CORP), TRIM(BCF_ACCOUNT_NO1), TRIM(BCF_PLAN_ID)) AS SRCE_KEY_I,
		'{{ cvar("pods_load_user") }}' AS CONV_M,
		'Invalid record due to date' AS CONV_MAP_RULE_M,
		'PLAN_BALN_SEGM_MSTR' AS TRSF_TABL_M,
		TO_DATE('{{ cvar("prun_strm_pros_d") }}', 'YYYYMMDD') AS SRCE_EFFT_D,
		TRIM(BCF_DT_PAYMENT_DEFER) AS VALU_CHNG_BFOR_X,
		'1111-11-11' AS VALU_CHNG_AFTR_X,
		'xfmplanbalnsegmmstrfrombcfinsg' AS TRSF_X,
		'BCF_DT_PAYMENT_DEFER' AS TRSF_COLM_M,
		NULL AS EROR_SEQN_I,
		'BCFINSG' AS SRCE_FILE_M,
		'{{ cvar("pods_pros_id") }}' AS PROS_KEY_EFFT_I, 
		CONCAT(TRIM(BCF_CORP), 'CCSCC', TRIM(BCF_ACCOUNT_NO1),TRIM(BCF_PLAN_ID)) AS TRSF_KEY_I
		
	FROM {{ ref('xfmtotable__xfmplanbalnsegmmstrfrombcfinsg') }}
	WHERE svIsValidBcfDtPaymentDefer = 'N' AND svIsValidRecord = 'N'

),
BcfDtFrstTrns AS (

	SELECT
		CONCAT(TRIM(BCF_CORP), TRIM(BCF_ACCOUNT_NO1), TRIM(BCF_PLAN_ID)) AS SRCE_KEY_I,
		'{{ cvar("pods_load_user") }}' AS CONV_M,
		'Invalid record due to date' AS CONV_MAP_RULE_M,
		'PLAN_BALN_SEGM_MSTR' AS TRSF_TABL_M,
		TO_DATE('{{ cvar("prun_strm_pros_d") }}', 'YYYYMMDD')  AS SRCE_EFFT_D,
		TRIM(BCF_DT_FIRST_TRANS) AS VALU_CHNG_BFOR_X,
		'1111-11-11' AS VALU_CHNG_AFTR_X,
		'xfmplanbalnsegmmstrfrombcfinsg' AS TRSF_X,
		'BCF_DT_FIRST_TRANS' AS TRSF_COLM_M,
		NULL AS EROR_SEQN_I,
		'BCFINSG' AS SRCE_FILE_M,
		'{{ cvar("pods_pros_id") }}' AS PROS_KEY_EFFT_I,
		CONCAT(TRIM(BCF_CORP), 'CCSCC', TRIM(BCF_ACCOUNT_NO1), TRIM(BCF_PLAN_ID)) AS TRSF_KEY_I
		
	FROM {{ ref('xfmtotable__xfmplanbalnsegmmstrfrombcfinsg') }}
	WHERE svIsValidBcfDtFirstTrans = 'N' AND svIsValidRecord = 'N'

),
BcfDtPaidOff AS (

	SELECT
		CONCAT(TRIM(BCF_CORP), TRIM(BCF_ACCOUNT_NO1), TRIM(BCF_PLAN_ID)) AS SRCE_KEY_I,
		'{{ cvar("pods_load_user") }}' AS CONV_M,
		'Invalid record due to date' AS CONV_MAP_RULE_M,
		'PLAN_BALN_SEGM_MSTR' AS TRSF_TABL_M,
		TO_DATE('{{ cvar("prun_strm_pros_d") }}', 'YYYYMMDD')  AS SRCE_EFFT_D,
		TRIM(BCF_DT_PAID_OFF) AS VALU_CHNG_BFOR_X,
		'1111-11-11' AS VALU_CHNG_AFTR_X,
		'xfmplanbalnsegmmstrfrombcfinsg' AS TRSF_X,
		'BCF_DT_PAID_OFF' AS TRSF_COLM_M,
		NULL AS EROR_SEQN_I,
		'BCFINSG' AS SRCE_FILE_M,
		'{{ cvar("pods_pros_id") }}' AS PROS_KEY_EFFT_I,
		CONCAT(TRIM(BCF_CORP), 'CCSCC', TRIM(BCF_ACCOUNT_NO1), TRIM(BCF_PLAN_ID)) AS TRSF_KEY_I
		
	FROM {{ ref('xfmtotable__xfmplanbalnsegmmstrfrombcfinsg') }}
	WHERE svIsValidBcfDtPaidOff = 'N' AND svIsValidRecord = 'N'

),
BcfDtLstPymt AS (

	SELECT
		CONCAT(TRIM(BCF_CORP), TRIM(BCF_ACCOUNT_NO1), TRIM(BCF_PLAN_ID)) AS SRCE_KEY_I,
		'{{ cvar("pods_load_user") }}'	 AS CONV_M,
		'Invalid record due to date' AS CONV_MAP_RULE_M,
		'PLAN_BALN_SEGM_MSTR' AS TRSF_TABL_M,
		TO_DATE('{{ cvar("prun_strm_pros_d") }}', 'YYYYMMDD') AS SRCE_EFFT_D,
		TRIM(BCF_DT_LAST_PAYMENT) AS VALU_CHNG_BFOR_X,
		'1111-11-11' AS VALU_CHNG_AFTR_X,
		'xfmplanbalnsegmmstrfrombcfinsg' AS TRSF_X,
		'BCF_DT_LAST_PAYMENT' AS TRSF_COLM_M,
		NULL AS EROR_SEQN_I,
		'BCFINSG' AS SRCE_FILE_M,
		'{{ cvar("pods_pros_id") }}' AS PROS_KEY_EFFT_I,
		CONCAT(TRIM(BCF_CORP), 'CCSCC', TRIM(BCF_ACCOUNT_NO1), TRIM(BCF_PLAN_ID)) AS TRSF_KEY_I
		
	FROM {{ ref('xfmtotable__xfmplanbalnsegmmstrfrombcfinsg') }}
	WHERE svIsValidBcfDtLastPayt = 'N' AND svIsValidRecord = 'N'

),
BcfDtLstMaint AS (

	SELECT
		CONCAT(TRIM(BCF_CORP), BCF_ACCOUNT_NO1, TRIM(BCF_PLAN_ID)) AS SRCE_KEY_I,
		'{{ cvar("pods_load_user") }}' AS CONV_M,
		'Invalid record due to date' AS CONV_MAP_RULE_M,
		'PLAN_BALN_SEGM_MSTR' AS TRSF_TABL_M,
		TO_DATE('{{ cvar("prun_strm_pros_d") }}', 'YYYYMMDD') AS SRCE_EFFT_D,
		TRIM(BCF_DT_LAST_MAINT) AS VALU_CHNG_BFOR_X,
		'1111-11-11' AS VALU_CHNG_AFTR_X,
		'xfmplanbalnsegmmstrfrombcfinsg' AS TRSF_X,
		'BCF_DT_LAST_MAINT' AS TRSF_COLM_M,
		NULL AS EROR_SEQN_I,
		'BCFINSG' AS SRCE_FILE_M,
		'{{ cvar("pods_pros_id") }}' AS PROS_KEY_EFFT_I,
		CONCAT(TRIM(BCF_CORP), 'CCSCC', BCF_ACCOUNT_NO1, TRIM(BCF_PLAN_ID)) AS TRSF_KEY_I
		
	FROM {{ ref('xfmtotable__xfmplanbalnsegmmstrfrombcfinsg') }}
	WHERE svIsValidBcfDtLastMntn = 'N' AND svIsValidRecord = 'N'

),
BcfPlanDueDt AS (

	SELECT
		CONCAT(TRIM(BCF_CORP), BCF_ACCOUNT_NO1, TRIM(BCF_PLAN_ID)) AS SRCE_KEY_I,
		'{{ cvar("pods_load_user") }}' AS CONV_M,
		'Invalid record due to date' AS CONV_MAP_RULE_M,
		'PLAN_BALN_SEGM_MSTR' AS TRSF_TABL_M,
		TO_DATE('{{ cvar("prun_strm_pros_d") }}', 'YYYYMMDD') AS SRCE_EFFT_D,
		TRIM(BCF_PLAN_DUE_DATE) AS VALU_CHNG_BFOR_X,
		'1111-11-11' AS VALU_CHNG_AFTR_X,
		'xfmplanbalnsegmmstrfrombcfinsg' AS TRSF_X,
		'BCF_PLAN_DUE_DATE' AS TRSF_COLM_M,
		NULL AS EROR_SEQN_I,
		'BCFINSG' AS SRCE_FILE_M,
		'{{ cvar("pods_pros_id") }}' AS PROS_KEY_EFFT_I,
		CONCAT(TRIM(BCF_CORP), 'CCSCC', BCF_ACCOUNT_NO1, TRIM(BCF_PLAN_ID)) AS TRSF_KEY_I
		
	FROM {{ ref('xfmtotable__xfmplanbalnsegmmstrfrombcfinsg') }}
	WHERE svIsValidBcfPlanDueDt = 'N' AND svIsValidRecord = 'N'

),
BcfDtEndIntrstFree AS (

	SELECT
		CONCAT(TRIM(BCF_CORP), BCF_ACCOUNT_NO1, TRIM(BCF_PLAN_ID)) AS SRCE_KEY_I,
		'{{ cvar("pods_load_user") }}' AS CONV_M,
		'Invalid record due to date' AS CONV_MAP_RULE_M,
		'PLAN_BALN_SEGM_MSTR' AS TRSF_TABL_M,
		TO_DATE('{{ cvar("prun_strm_pros_d") }}', 'YYYYMMDD') AS SRCE_EFFT_D,
		TRIM(BCF_DATE_END_INTEREST_FREE) AS VALU_CHNG_BFOR_X,
		'1111-11-11' AS VALU_CHNG_AFTR_X,
		'xfmplanbalnsegmmstrfrombcfinsg' AS TRSF_X,
		'BCF_DATE_END_INTEREST_FREE' AS TRSF_COLM_M,
		NULL AS EROR_SEQN_I,
		'BCFINSG'AS SRCE_FILE_M,
		'{{ cvar("pods_pros_id") }}' AS PROS_KEY_EFFT_I,
		CONCAT(TRIM(BCF_CORP), 'CCSCC', BCF_ACCOUNT_NO1, TRIM(BCF_PLAN_ID)) AS TRSF_KEY_I
		
	FROM {{ ref('xfmtotable__xfmplanbalnsegmmstrfrombcfinsg') }}
	WHERE svIsValidBcfDtEndInterestFree = 'N' AND svIsValidRecord = 'N'

),
BcfDtMigrate AS (

	SELECT
		CONCAT(TRIM(BCF_CORP), BCF_ACCOUNT_NO1, TRIM(BCF_PLAN_ID)) AS SRCE_KEY_I,
		'{{ cvar("pods_load_user") }}' AS CONV_M,
		'Invalid record due to date' AS CONV_MAP_RULE_M,
		'PLAN_BALN_SEGM_MSTR' AS TRSF_TABL_M,
		TO_DATE('{{ cvar("prun_strm_pros_d") }}', 'YYYYMMDD') AS SRCE_EFFT_D,
		TRIM(BCF_DT_MIGRATE) AS VALU_CHNG_BFOR_X,
		'1111-11-11' AS VALU_CHNG_AFTR_X,
		'xfmplanbalnsegmmstrfrombcfinsg' AS TRSF_X,
		'BCF_DT_MIGRATE' AS TRSF_COLM_M,
		NULL AS EROR_SEQN_I,
		'BCFINSG' AS SRCE_FILE_M,
		'{{ cvar("pods_pros_id") }}' AS PROS_KEY_EFFT_I,
		CONCAT(TRIM(BCF_CORP), 'CCSCC', BCF_ACCOUNT_NO1, TRIM(BCF_PLAN_ID)) AS TRSF_KEY_I

	FROM {{ ref('xfmtotable__xfmplanbalnsegmmstrfrombcfinsg') }}
	WHERE svIsValidBcfDtMigrate = 'N' AND svIsValidRecord = 'N'

),
BcfFlLstInstallmntDt AS (

	SELECT
		CONCAT(TRIM(BCF_CORP), BCF_ACCOUNT_NO1, TRIM(BCF_PLAN_ID)) AS SRCE_KEY_I,
		'{{ cvar("pods_load_user") }}' AS CONV_M,
		'Invalid record due to date' AS CONV_MAP_RULE_M,
		'PLAN_BALN_SEGM_MSTR' AS TRSF_TABL_M,
		TO_DATE('{{ cvar("prun_strm_pros_d") }}', 'YYYYMMDD') AS SRCE_EFFT_D,
		TRIM(BCF_FL_LAST_INSTALLMENT_DT) AS VALU_CHNG_BFOR_X,
		'1111-11-11' AS VALU_CHNG_AFTR_X,
		'xfmplanbalnsegmmstrfrombcfinsg' AS TRSF_X,
		'BCF_FL_LAST_INSTALLMENT_DT' AS TRSF_COLM_M,
		NULL AS EROR_SEQN_I,
		'BCFINSG' AS SRCE_FILE_M,
		'{{ cvar("pods_pros_id") }}' AS PROS_KEY_EFFT_I,
		CONCAT(TRIM(BCF_CORP), 'CCSCC', BCF_ACCOUNT_NO1, TRIM(BCF_PLAN_ID)) AS TRSF_KEY_I

	FROM {{ ref('xfmtotable__xfmplanbalnsegmmstrfrombcfinsg') }}
	WHERE svIsValidBcfFlLastInstallmentsDt = 'N' AND svIsValidRecord = 'N'

),
BcfSchedPayoffDt AS (

	SELECT
		CONCAT(TRIM(BCF_CORP), BCF_ACCOUNT_NO1, TRIM(BCF_PLAN_ID)) AS SRCE_KEY_I,
		'{{ cvar("pods_load_user") }}' AS CONV_M,
		'Invalid record due to date' AS CONV_MAP_RULE_M,
		'PLAN_BALN_SEGM_MSTR' AS TRSF_TABL_M,
		TO_DATE('{{ cvar("prun_strm_pros_d") }}', 'YYYYMMDD') AS SRCE_EFFT_D,
		TRIM(BCF_SCHED_PAYOFF_DT) AS VALU_CHNG_BFOR_X,
		'1111-11-11' AS VALU_CHNG_AFTR_X,
		'xfmplanbalnsegmmstrfrombcfinsg' AS TRSF_X,
		'BCF_SCHED_PAYOFF_DT' AS TRSF_COLM_M,
		NULL AS EROR_SEQN_I,
		'BCFINSG' AS SRCE_FILE_M,
		'{{ cvar("pods_pros_id") }}' AS PROS_KEY_EFFT_I,
		CONCAT(TRIM(BCF_CORP), 'CCSCC', BCF_ACCOUNT_NO1, TRIM(BCF_PLAN_ID)) AS TRSF_KEY_I

	FROM {{ ref('xfmtotable__xfmplanbalnsegmmstrfrombcfinsg') }}
	WHERE svIsValidBcfSchedPayoffDt = 'N' AND svIsValidRecord = 'N'

),
BcfActualPayoffDt AS (

	SELECT
		CONCAT(TRIM(BCF_CORP), BCF_ACCOUNT_NO1, TRIM(BCF_PLAN_ID)) AS SRCE_KEY_I,
		'{{ cvar("pods_load_user") }}' AS CONV_M,
		'Invalid record due to date' AS CONV_MAP_RULE_M,
		'PLAN_BALN_SEGM_MSTR' AS TRSF_TABL_M,
		TO_DATE('{{ cvar("prun_strm_pros_d") }}', 'YYYYMMDD') AS SRCE_EFFT_D,
		TRIM(BCF_ACTUAL_PAYOFF_DT) AS VALU_CHNG_BFOR_X, 
		'1111-11-11' AS VALU_CHNG_AFTR_X,
		'xfmplanbalnsegmmstrfrombcfinsg' AS TRSF_X,
		'BCF_ACTUAL_PAYOFF_DT' AS TRSF_COLM_M,
		NULL AS EROR_SEQN_I,
		'BCFINSG' AS SRCE_FILE_M,
		'{{ cvar("pods_pros_id") }}' AS PROS_KEY_EFFT_I,
		CONCAT(TRIM(BCF_CORP), 'CCSCC', BCF_ACCOUNT_NO1, TRIM(BCF_PLAN_ID)) AS TRSF_KEY_I

	FROM {{ ref('xfmtotable__xfmplanbalnsegmmstrfrombcfinsg') }}
	WHERE svIsValidBcfActualPayoffDt = 'N' AND svIsValidRecord = 'N'

),
BcfDtInstallTermChg AS (

	SELECT
		CONCAT(TRIM(BCF_CORP), BCF_ACCOUNT_NO1, TRIM(BCF_PLAN_ID)) AS SRCE_KEY_I,
		'{{ cvar("pods_load_user") }}' AS CONV_M,
		'Invalid record due to date' AS CONV_MAP_RULE_M,
		'PLAN_BALN_SEGM_MSTR' AS TRSF_TABL_M,
		TO_DATE('{{ cvar("prun_strm_pros_d") }}', 'YYYYMMDD') AS SRCE_EFFT_D,
		TRIM(BCF_DT_INSTALL_TERM_CHG) AS VALU_CHNG_BFOR_X,
		'1111-11-11' AS VALU_CHNG_AFTR_X,
		'xfmplanbalnsegmmstrfrombcfinsg' AS TRSF_X,
		'BCF_DT_INSTALL_TERM_CHG' AS TRSF_COLM_M,
		NULL AS EROR_SEQN_I,
		'BCFINSG' AS SRCE_FILE_M,
		'{{ cvar("pods_pros_id") }}' AS PROS_KEY_EFFT_I,
		CONCAT(TRIM(BCF_CORP), 'CCSCC', BCF_ACCOUNT_NO1, TRIM(BCF_PLAN_ID)) AS TRSF_KEY_I
	FROM {{ ref('xfmtotable__xfmplanbalnsegmmstrfrombcfinsg') }}
	WHERE svIsValidBcfDtInstallTermChg = 'N' AND svIsValidRecord = 'N'

),
BcfDtInstallPaid AS (

	SELECT
		CONCAT(TRIM(BCF_CORP), BCF_ACCOUNT_NO1, TRIM(BCF_PLAN_ID)) AS SRCE_KEY_I,
		'{{ cvar("pods_load_user") }}' AS CONV_M,
		'Invalid record due to date' AS CONV_MAP_RULE_M,
		'PLAN_BALN_SEGM_MSTR' AS TRSF_TABL_M,
		TO_DATE('{{ cvar("prun_strm_pros_d") }}', 'YYYYMMDD') AS SRCE_EFFT_D,
		TRIM(BCF_DT_INSTALL_PAID) AS VALU_CHNG_BFOR_X,
		'1111-11-11' AS VALU_CHNG_AFTR_X,
		'xfmplanbalnsegmmstrfrombcfinsg' AS TRSF_X,
		'BCF_DT_INSTALL_PAID' AS TRSF_COLM_M,
		null AS EROR_SEQN_I,
		'BCFINSG' AS SRCE_FILE_M,
		'{{ cvar("pods_pros_id") }}' AS PROS_KEY_EFFT_I,
		CONCAT(TRIM(BCF_CORP), 'CCSCC', BCF_ACCOUNT_NO1, TRIM(BCF_PLAN_ID)) AS TRSF_KEY_I
	FROM {{ ref('xfmtotable__xfmplanbalnsegmmstrfrombcfinsg') }}
	WHERE svIsValidBcfDtInstallPaid = 'N' AND svIsValidRecord = 'N'

),
BcfProjectedPayoffDt AS (

	SELECT
		CONCAT(TRIM(BCF_CORP), BCF_ACCOUNT_NO1, TRIM(BCF_PLAN_ID)) AS SRCE_KEY_I,
		'{{ cvar("pods_load_user") }}' AS CONV_M,
		'Invalid record due to date' AS CONV_MAP_RULE_M,
		'PLAN_BALN_SEGM_MSTR' AS TRSF_TABL_M,
		TO_DATE('{{ cvar("prun_strm_pros_d") }}', 'YYYYMMDD') AS SRCE_EFFT_D,
		TRIM(BCF_PROJECTED_PAYOFF_DT) AS VALU_CHNG_BFOR_X,
		'1111-11-11' AS VALU_CHNG_AFTR_X,
		'xfmplanbalnsegmmstrfrombcfinsg' AS TRSF_X,
		'BCF_PROJECTED_PAYOFF_DT' AS TRSF_COLM_M,
		NULL AS EROR_SEQN_I,
		'BCFINSG' AS SRCE_FILE_M,
		'{{ cvar("pods_pros_id") }}' AS PROS_KEY_EFFT_I,
		CONCAT(TRIM(BCF_CORP), 'CCSCC', BCF_ACCOUNT_NO1, TRIM(BCF_PLAN_ID)) AS TRSF_KEY_I
	FROM {{ ref('xfmtotable__xfmplanbalnsegmmstrfrombcfinsg') }}
	WHERE svIsValidBcfProjectedPayoffDt = 'N' AND svIsValidRecord = 'N'

),
BcfDisputeOldDt AS (

	SELECT
		CONCAT(TRIM(BCF_CORP), BCF_ACCOUNT_NO1, TRIM(BCF_PLAN_ID)) AS SRCE_KEY_I,
		'{{ cvar("pods_load_user") }}' AS CONV_M,
		'Invalid record due to date' AS CONV_MAP_RULE_M,
		'PLAN_BALN_SEGM_MSTR' AS TRSF_TABL_M,
		TO_DATE('{{ cvar("prun_strm_pros_d") }}', 'YYYYMMDD') AS SRCE_EFFT_D,
		TRIM(BCF_DISPUTE_OLD_DATE) AS VALU_CHNG_BFOR_X,
		'1111-11-11' AS VALU_CHNG_AFTR_X,
		'xfmplanbalnsegmmstrfrombcfinsg' AS TRSF_X,
		'BCF_DISPUTE_OLD_DATE' AS TRSF_COLM_M,
		NULL AS EROR_SEQN_I,
		'BCFINSG' AS SRCE_FILE_M,
		'{{ cvar("pods_pros_id") }}' AS PROS_KEY_EFFT_I,
		CONCAT(TRIM(BCF_CORP), 'CCSCC', BCF_ACCOUNT_NO1, TRIM(BCF_PLAN_ID)) AS TRSF_KEY_I

	FROM {{ ref('xfmtotable__xfmplanbalnsegmmstrfrombcfinsg') }}
	WHERE svIsValidBcfDisputeOldDt = 'N' AND svIsValidRecord = 'N'

),
FUNNEL AS (
	SELECT
		SRCE_KEY_I as SRCE_KEY_I,
		CONV_M as CONV_M,
		CONV_MAP_RULE_M as CONV_MAP_RULE_M,
		TRSF_TABL_M as TRSF_TABL_M,
		SRCE_EFFT_D as SRCE_EFFT_D,
		VALU_CHNG_BFOR_X as VALU_CHNG_BFOR_X,
		VALU_CHNG_AFTR_X as VALU_CHNG_AFTR_X,
		TRSF_X as TRSF_X,
		TRSF_COLM_M as TRSF_COLM_M,
		EROR_SEQN_I as EROR_SEQN_I,
		SRCE_FILE_M as SRCE_FILE_M,
		PROS_KEY_EFFT_I as PROS_KEY_EFFT_I,
		TRSF_KEY_I as TRSF_KEY_I
	FROM BcfDtSpcTrmsEd
	UNION ALL
	SELECT
		SRCE_KEY_I,
		CONV_M,
		CONV_MAP_RULE_M,
		TRSF_TABL_M,
		SRCE_EFFT_D,
		VALU_CHNG_BFOR_X,
		VALU_CHNG_AFTR_X,
		TRSF_X,
		TRSF_COLM_M,
		EROR_SEQN_I,
		SRCE_FILE_M,
		PROS_KEY_EFFT_I,
		TRSF_KEY_I
	FROM BcfDtIntDefr
	UNION ALL
	SELECT
		SRCE_KEY_I,
		CONV_M,
		CONV_MAP_RULE_M,
		TRSF_TABL_M,
		SRCE_EFFT_D,
		VALU_CHNG_BFOR_X,
		VALU_CHNG_AFTR_X,
		TRSF_X,
		TRSF_COLM_M,
		EROR_SEQN_I,
		SRCE_FILE_M,
		PROS_KEY_EFFT_I,
		TRSF_KEY_I
	FROM BcfDtPymtDefr
	UNION ALL
	SELECT
		SRCE_KEY_I,
		CONV_M,
		CONV_MAP_RULE_M,
		TRSF_TABL_M,
		SRCE_EFFT_D,
		VALU_CHNG_BFOR_X,
		VALU_CHNG_AFTR_X,
		TRSF_X,
		TRSF_COLM_M,
		EROR_SEQN_I,
		SRCE_FILE_M,
		PROS_KEY_EFFT_I,
		TRSF_KEY_I
	FROM BcfDtFrstTrns
	UNION ALL
	SELECT
		SRCE_KEY_I,
		CONV_M,
		CONV_MAP_RULE_M,
		TRSF_TABL_M,
		SRCE_EFFT_D,
		VALU_CHNG_BFOR_X,
		VALU_CHNG_AFTR_X,
		TRSF_X,
		TRSF_COLM_M,
		EROR_SEQN_I,
		SRCE_FILE_M,
		PROS_KEY_EFFT_I,
		TRSF_KEY_I
	FROM BcfDtPaidOff
	UNION ALL
	SELECT
		SRCE_KEY_I,
		CONV_M,
		CONV_MAP_RULE_M,
		TRSF_TABL_M,
		SRCE_EFFT_D,
		VALU_CHNG_BFOR_X,
		VALU_CHNG_AFTR_X,
		TRSF_X,
		TRSF_COLM_M,
		EROR_SEQN_I,
		SRCE_FILE_M,
		PROS_KEY_EFFT_I,
		TRSF_KEY_I
	FROM BcfDtLstPymt
	UNION ALL
	SELECT
		SRCE_KEY_I,
		CONV_M,
		CONV_MAP_RULE_M,
		TRSF_TABL_M,
		SRCE_EFFT_D,
		VALU_CHNG_BFOR_X,
		VALU_CHNG_AFTR_X,
		TRSF_X,
		TRSF_COLM_M,
		EROR_SEQN_I,
		SRCE_FILE_M,
		PROS_KEY_EFFT_I,
		TRSF_KEY_I
	FROM BcfDtLstMaint
	UNION ALL
	SELECT
		SRCE_KEY_I,
		CONV_M,
		CONV_MAP_RULE_M,
		TRSF_TABL_M,
		SRCE_EFFT_D,
		VALU_CHNG_BFOR_X,
		VALU_CHNG_AFTR_X,
		TRSF_X,
		TRSF_COLM_M,
		EROR_SEQN_I,
		SRCE_FILE_M,
		PROS_KEY_EFFT_I,
		TRSF_KEY_I
	FROM BcfPlanDueDt
	UNION ALL
	SELECT
		SRCE_KEY_I,
		CONV_M,
		CONV_MAP_RULE_M,
		TRSF_TABL_M,
		SRCE_EFFT_D,
		VALU_CHNG_BFOR_X,
		VALU_CHNG_AFTR_X,
		TRSF_X,
		TRSF_COLM_M,
		EROR_SEQN_I,
		SRCE_FILE_M,
		PROS_KEY_EFFT_I,
		TRSF_KEY_I
	FROM BcfDtEndIntrstFree
	UNION ALL
	SELECT
		SRCE_KEY_I,
		CONV_M,
		CONV_MAP_RULE_M,
		TRSF_TABL_M,
		SRCE_EFFT_D,
		VALU_CHNG_BFOR_X,
		VALU_CHNG_AFTR_X,
		TRSF_X,
		TRSF_COLM_M,
		EROR_SEQN_I,
		SRCE_FILE_M,
		PROS_KEY_EFFT_I,
		TRSF_KEY_I
	FROM BcfDtMigrate
	UNION ALL
	SELECT
		SRCE_KEY_I,
		CONV_M,
		CONV_MAP_RULE_M,
		TRSF_TABL_M,
		SRCE_EFFT_D,
		VALU_CHNG_BFOR_X,
		VALU_CHNG_AFTR_X,
		TRSF_X,
		TRSF_COLM_M,
		EROR_SEQN_I,
		SRCE_FILE_M,
		PROS_KEY_EFFT_I,
		TRSF_KEY_I
	FROM BcfFlLstInstallmntDt
	UNION ALL
	SELECT
		SRCE_KEY_I,
		CONV_M,
		CONV_MAP_RULE_M,
		TRSF_TABL_M,
		SRCE_EFFT_D,
		VALU_CHNG_BFOR_X,
		VALU_CHNG_AFTR_X,
		TRSF_X,
		TRSF_COLM_M,
		EROR_SEQN_I,
		SRCE_FILE_M,
		PROS_KEY_EFFT_I,
		TRSF_KEY_I
	FROM BcfSchedPayoffDt
	UNION ALL
	SELECT
		SRCE_KEY_I,
		CONV_M,
		CONV_MAP_RULE_M,
		TRSF_TABL_M,
		SRCE_EFFT_D,
		VALU_CHNG_BFOR_X,
		VALU_CHNG_AFTR_X,
		TRSF_X,
		TRSF_COLM_M,
		EROR_SEQN_I,
		SRCE_FILE_M,
		PROS_KEY_EFFT_I,
		TRSF_KEY_I
	FROM BcfActualPayoffDt
	UNION ALL
	SELECT
		SRCE_KEY_I,
		CONV_M,
		CONV_MAP_RULE_M,
		TRSF_TABL_M,
		SRCE_EFFT_D,
		VALU_CHNG_BFOR_X,
		VALU_CHNG_AFTR_X,
		TRSF_X,
		TRSF_COLM_M,
		EROR_SEQN_I,
		SRCE_FILE_M,
		PROS_KEY_EFFT_I,
		TRSF_KEY_I
	FROM BcfDtInstallTermChg
	UNION ALL
	SELECT
		SRCE_KEY_I,
		CONV_M,
		CONV_MAP_RULE_M,
		TRSF_TABL_M,
		SRCE_EFFT_D,
		VALU_CHNG_BFOR_X,
		VALU_CHNG_AFTR_X,
		TRSF_X,
		TRSF_COLM_M,
		EROR_SEQN_I,
		SRCE_FILE_M,
		PROS_KEY_EFFT_I,
		TRSF_KEY_I
	FROM BcfDtInstallPaid
	UNION ALL
	SELECT
		SRCE_KEY_I,
		CONV_M,
		CONV_MAP_RULE_M,
		TRSF_TABL_M,
		SRCE_EFFT_D,
		VALU_CHNG_BFOR_X,
		VALU_CHNG_AFTR_X,
		TRSF_X,
		TRSF_COLM_M,
		EROR_SEQN_I,
		SRCE_FILE_M,
		PROS_KEY_EFFT_I,
		TRSF_KEY_I
	FROM BcfProjectedPayoffDt
	UNION ALL
	SELECT
		SRCE_KEY_I,
		CONV_M,
		CONV_MAP_RULE_M,
		TRSF_TABL_M,
		SRCE_EFFT_D,
		VALU_CHNG_BFOR_X,
		VALU_CHNG_AFTR_X,
		TRSF_X,
		TRSF_COLM_M,
		EROR_SEQN_I,
		SRCE_FILE_M,
		PROS_KEY_EFFT_I,
		TRSF_KEY_I
	FROM BcfDisputeOldDt
)

SELECT * FROM FUNNEL