{{ config(materialized='view', tags=['LdDltClp_App_Pdct_Feat']) }}

WITH XFR_Split__To_Tmp_Update_Paty_Int_Grup_DS AS (
	SELECT
		-- *SRC*: \(20)If IsNull(To_Split_XFR.APPT_PDCT_I) OR IsNull(To_Split_XFR.FEAT_I) Then "F" Else "T",
		IFF({{ ref('CC_Identify_Deltas') }}.APPT_PDCT_I IS NULL OR {{ ref('CC_Identify_Deltas') }}.FEAT_I IS NULL, 'F', 'T') AS ActionRequired,
		APPT_PDCT_I,
		FEAT_I,
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS EXPY_D,
		REFR_PK AS PROS_KEY_EXPY_I
	FROM {{ ref('CC_Identify_Deltas') }}
	WHERE ActionRequired = 'T' AND {{ ref('CC_Identify_Deltas') }}.change_code = 2 OR {{ ref('CC_Identify_Deltas') }}.change_code = 3
)

SELECT * FROM XFR_Split__To_Tmp_Update_Paty_Int_Grup_DS