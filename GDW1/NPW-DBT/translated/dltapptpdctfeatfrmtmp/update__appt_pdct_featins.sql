{{ config(materialized='view', tags=['DltApptPdctFeatFrmTMP']) }}

WITH Update__Appt_Pdct_FeatIns AS (
	SELECT
		APPT_PDCT_I,
		FEAT_I,
		SRCE_SYST_APPT_FEAT_I,
		-- *SRC*: StringToDate(Trim(ETL_PROCESS_DT[1, 4]) : '-' : Trim(ETL_PROCESS_DT[5, 2]) : '-' : Trim(ETL_PROCESS_DT[7, 2]), "%yyyy-%mm-%dd"),
		STRINGTODATE(CONCAT(CONCAT(CONCAT(CONCAT(TRIM(SUBSTRING(ETL_PROCESS_DT, 1, 4)), '-'), TRIM(SUBSTRING(ETL_PROCESS_DT, 5, 2))), '-'), TRIM(SUBSTRING(ETL_PROCESS_DT, 7, 2))), '%yyyy-%mm-%dd') AS EFFT_D,
		SRCE_SYST_C,
		SRCE_SYST_APPT_OVRD_I,
		OVRD_FEAT_I,
		SRCE_SYST_STND_VALU_Q,
		SRCE_SYST_STND_VALU_R,
		SRCE_SYST_STND_VALU_A,
		CNCY_C,
		ACTL_VALU_Q,
		ACTL_VALU_R,
		ACTL_VALU_A,
		FEAT_SEQN_N,
		FEAT_STRT_D,
		FEE_CHRG_D,
		OVRD_REAS_C,
		FEE_ADD_TO_TOTL_F,
		FEE_CAPL_F,
		-- *SRC*: StringToDate('9999-12-31', "%yyyy-%mm-%dd"),
		STRINGTODATE('9999-12-31', '%yyyy-%mm-%dd') AS EXPY_D,
		REFR_PK AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		EROR_SEQN_I
	FROM {{ ref('Join') }}
	WHERE {{ ref('Join') }}.change_code = 1 OR {{ ref('Join') }}.change_code = 3
)

SELECT * FROM Update__Appt_Pdct_FeatIns