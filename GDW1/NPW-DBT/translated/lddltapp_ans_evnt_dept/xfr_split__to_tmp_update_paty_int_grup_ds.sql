{{ config(materialized='view', tags=['LdDltAPP_ANS_EVNT_DEPT']) }}

WITH XFR_Split__To_Tmp_Update_Paty_Int_Grup_DS AS (
	SELECT
		-- *SRC*: \(20)If IsNull(To_Split_XFR.DEPT_I) Then "F" Else "T",
		IFF({{ ref('CC_Identify_Deltas') }}.DEPT_I IS NULL, 'F', 'T') AS ActionRequired,
		EVNT_I,
		DEPT_ROLE_C,
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS EXPY_D,
		REFR_PK AS PROS_KEY_EXPY_I
	FROM {{ ref('CC_Identify_Deltas') }}
	WHERE ActionRequired = 'T' AND {{ ref('CC_Identify_Deltas') }}.change_code = 2 OR {{ ref('CC_Identify_Deltas') }}.change_code = 3
)

SELECT * FROM XFR_Split__To_Tmp_Update_Paty_Int_Grup_DS