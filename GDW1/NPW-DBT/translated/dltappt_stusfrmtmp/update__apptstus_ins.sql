{{ config(materialized='view', tags=['DltAppt_StusFrmTMP']) }}

WITH Update__ApptStus_Ins AS (
	SELECT
		APPT_I,
		STUS_C,
		STRT_S,
		STRT_D,
		STRT_T,
		END_D,
		END_T,
		END_S,
		EMPL_I,
		EFFT_D,
		-- *SRC*: \(20)If ToTrans.change_code = 6 Then trim(ToTrans.EFFT_D) Else StringToDate("99991231", "%yyyy%mm%dd"),
		IFF({{ ref('FunlTransAndJoin') }}.change_code = 6, TRIM({{ ref('FunlTransAndJoin') }}.EFFT_D), STRINGTODATE('99991231', '%yyyy%mm%dd')) AS EXPY_D,
		REFR_PK AS PROS_KEY_EFFT_I,
		-- *SRC*: \(20)If ToTrans.change_code = 6 Then REFR_PK Else SetNull(),
		IFF({{ ref('FunlTransAndJoin') }}.change_code = 6, REFR_PK, SETNULL()) AS PROS_KEY_EXPY_I,
		EROR_SEQN_I
	FROM {{ ref('FunlTransAndJoin') }}
	WHERE {{ ref('FunlTransAndJoin') }}.change_code = 1 OR {{ ref('FunlTransAndJoin') }}.change_code = 3 OR {{ ref('FunlTransAndJoin') }}.change_code = 6
)

SELECT * FROM Update__ApptStus_Ins