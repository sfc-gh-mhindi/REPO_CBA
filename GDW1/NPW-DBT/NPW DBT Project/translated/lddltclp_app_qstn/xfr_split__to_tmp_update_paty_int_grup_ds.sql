{{ config(materialized='view', tags=['LdDltClp_App_Qstn']) }}

WITH XFR_Split__To_Tmp_Update_Paty_Int_Grup_DS AS (
	SELECT
		-- *SRC*: \(20)If IsNull(To_Split_XFR.APPT_I) OR IsNull(To_Split_XFR.QSTN_C) Then "F" Else "T",
		IFF({{ ref('CC_Identify_Deltas') }}.APPT_I IS NULL OR {{ ref('CC_Identify_Deltas') }}.QSTN_C IS NULL, 'F', 'T') AS ActionRequired,
		APPT_I,
		-- *SRC*: \(20)If IsNull(To_Split_XFR.PATY_I) Then '0x0' Else To_Split_XFR.PATY_I,
		IFF({{ ref('CC_Identify_Deltas') }}.PATY_I IS NULL, '0x0', {{ ref('CC_Identify_Deltas') }}.PATY_I) AS PATY_I,
		QSTN_C,
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS EXPY_D,
		REFR_PK AS PROS_KEY_EXPY_I
	FROM {{ ref('CC_Identify_Deltas') }}
	WHERE ActionRequired = 'T' AND {{ ref('CC_Identify_Deltas') }}.change_code = 2 OR {{ ref('CC_Identify_Deltas') }}.change_code = 3
)

SELECT * FROM XFR_Split__To_Tmp_Update_Paty_Int_Grup_DS