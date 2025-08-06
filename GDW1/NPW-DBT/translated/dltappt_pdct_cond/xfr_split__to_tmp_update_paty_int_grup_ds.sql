{{ config(materialized='view', tags=['DltAPPT_PDCT_COND']) }}

WITH XFR_Split__To_Tmp_Update_Paty_Int_Grup_DS AS (
	SELECT
		-- *SRC*: \(20)If IsNull(To_Split_XFR.APPT_PDCT_I) Or IsNull(To_Split_XFR.COND_C) Then "F" Else "T",
		IFF({{ ref('CC_Identify_Deltas') }}.APPT_PDCT_I IS NULL OR {{ ref('CC_Identify_Deltas') }}.COND_C IS NULL, 'F', 'T') AS ActionRequired,
		APPT_PDCT_I,
		COND_C,
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS EXPY_D,
		-- *SRC*: AsInteger(REFR_PK),
		ASINTEGER(REFR_PK) AS PROS_KEY_EXPY_I
	FROM {{ ref('CC_Identify_Deltas') }}
	WHERE ActionRequired = 'T' AND {{ ref('CC_Identify_Deltas') }}.change_code = 2 OR {{ ref('CC_Identify_Deltas') }}.change_code = 3
)

SELECT * FROM XFR_Split__To_Tmp_Update_Paty_Int_Grup_DS