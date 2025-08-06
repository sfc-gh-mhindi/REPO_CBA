{{ config(materialized='view', tags=['DltAppt_Pdct_RpayFrmTMP']) }}

WITH Update__Appt_Pdct_Rpay_Ins AS (
	SELECT
		APPT_PDCT_I,
		RPAY_TYPE_C,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, "%yyyy%mm%dd"),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS EFFT_D,
		PAYT_FREQ_C,
		STRT_RPAY_D,
		-- *SRC*: StringToDate('9999-12-31', "%yyyy-%mm-%dd"),
		STRINGTODATE('9999-12-31', '%yyyy-%mm-%dd') AS EXPY_D,
		REFR_PK AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I,
		SRCE_SYST_C,
		RPAY_SRCE_C,
		RPAY_SRCE_OTHR_X
	FROM {{ ref('Join') }}
	WHERE {{ ref('Join') }}.change_code = 1 OR {{ ref('Join') }}.change_code = 3
)

SELECT * FROM Update__Appt_Pdct_Rpay_Ins