{{ config(materialized='view', tags=['DltAPPT_PDCTFrmTMP_APPT_PDCT']) }}

WITH XfmCheckDeltaAction__OutTgtApptPdctInsertDS AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS ExpiryDate,
		1 AS INSERT,
		3 AS UPDATE,
		-- *SRC*: ( IF IsNotNull((JoinAllApptPdct.NEW_APPT_PDCT_I)) THEN (JoinAllApptPdct.NEW_APPT_PDCT_I) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_APPT_PDCT_I IS NOT NULL, {{ ref('JoinAll') }}.NEW_APPT_PDCT_I, '') AS APPT_PDCT_I,
		{{ ref('JoinAll') }}.NEW_DEBT_ABN_X AS DEBT_ABN_X,
		{{ ref('JoinAll') }}.NEW_DEBT_BUSN_M AS DEBT_BUSN_M,
		{{ ref('JoinAll') }}.NEW_SMPL_APPT_F AS SMPL_APPT_F,
		{{ ref('JoinAll') }}.NEW_JOB_COMM_CATG_C AS JOB_COMM_CATG_C,
		{{ ref('JoinAll') }}.NEW_APPT_QLFY_C AS APPT_QLFY_C,
		{{ ref('JoinAll') }}.NEW_ACQR_TYPE_C AS ACQR_TYPE_C,
		{{ ref('JoinAll') }}.NEW_ACQR_ADHC_X AS ACQR_ADHC_X,
		{{ ref('JoinAll') }}.NEW_ACQR_SRCE_C AS ACQR_SRCE_C,
		-- *SRC*: ( IF IsNotNull((JoinAllApptPdct.NEW_PDCT_N)) THEN (JoinAllApptPdct.NEW_PDCT_N) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_PDCT_N IS NOT NULL, {{ ref('JoinAll') }}.NEW_PDCT_N, '') AS PDCT_N,
		-- *SRC*: ( IF IsNotNull((JoinAllApptPdct.NEW_APPT_I)) THEN (JoinAllApptPdct.NEW_APPT_I) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_APPT_I IS NOT NULL, {{ ref('JoinAll') }}.NEW_APPT_I, '') AS APPT_I,
		-- *SRC*: ( IF IsNotNull((JoinAllApptPdct.NEW_SRCE_SYST_C)) THEN (JoinAllApptPdct.NEW_SRCE_SYST_C) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_SRCE_SYST_C IS NOT NULL, {{ ref('JoinAll') }}.NEW_SRCE_SYST_C, '') AS SRCE_SYST_C,
		-- *SRC*: ( IF IsNotNull((JoinAllApptPdct.NEW_SRCE_SYST_APPT_PDCT_I)) THEN (JoinAllApptPdct.NEW_SRCE_SYST_APPT_PDCT_I) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_SRCE_SYST_APPT_PDCT_I IS NOT NULL, {{ ref('JoinAll') }}.NEW_SRCE_SYST_APPT_PDCT_I, '') AS SRCE_SYST_APPT_PDCT_I,
		{{ ref('JoinAll') }}.NEW_LOAN_FNDD_METH_C AS LOAN_FNDD_METH_C,
		{{ ref('JoinAll') }}.NEW_NEW_ACCT_F AS NEW_ACCT_F,
		{{ ref('JoinAll') }}.NEW_BROK_PATY_I AS BROK_PATY_I,
		{{ ref('JoinAll') }}.NEW_COPY_FROM_OTHR_APPT_F AS COPY_FROM_OTHR_APPT_F,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd'),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS EFFT_D,
		-- *SRC*: StringToDate('9999-12-31', '%yyyy-%mm-%dd'),
		STRINGTODATE('9999-12-31', '%yyyy-%mm-%dd') AS EXPY_D,
		REFR_PK AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I,
		{{ ref('JoinAll') }}.NEW_APPT_PDCT_CATG_C AS APPT_PDCT_CATG_C,
		{{ ref('JoinAll') }}.NEW_APPT_PDCT_DURT_C AS APPT_PDCT_DURT_C,
		{{ ref('JoinAll') }}.NEW_ASES_D AS ASES_D
	FROM {{ ref('JoinAll') }}
	WHERE {{ ref('JoinAll') }}.change_code = INSERT OR {{ ref('JoinAll') }}.change_code = UPDATE
)

SELECT * FROM XfmCheckDeltaAction__OutTgtApptPdctInsertDS