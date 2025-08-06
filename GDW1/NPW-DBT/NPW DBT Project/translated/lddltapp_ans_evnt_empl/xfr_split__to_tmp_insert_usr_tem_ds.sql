{{ config(materialized='view', tags=['LdDltAPP_ANS_EVNT_EMPL']) }}

WITH XFR_Split__To_Tmp_Insert_USR_TEM_DS AS (
	SELECT
		-- *SRC*: \(20)If IsNull(To_Split_XFR.EMPL_I) Then "F" Else "T",
		IFF({{ ref('CC_Identify_Deltas') }}.EMPL_I IS NULL, 'F', 'T') AS ActionRequired,
		EVNT_I,
		EMPL_I,
		EVNT_PATY_ROLE_TYPE_C,
		-- *SRC*: StringToDate(LEFT(ETL_PROCESS_DT, 4) : '-' : LEFT(RIGHT(ETL_PROCESS_DT, 4), 2) : '-' : RIGHT(ETL_PROCESS_DT, 2), "%yyyy-%mm-%dd"),
		STRINGTODATE(CONCAT(CONCAT(CONCAT(CONCAT(LEFT(ETL_PROCESS_DT, 4), '-'), LEFT(RIGHT(ETL_PROCESS_DT, 4), 2)), '-'), RIGHT(ETL_PROCESS_DT, 2)), '%yyyy-%mm-%dd') AS EFFT_D,
		-- *SRC*: StringToDate('9999-12-31', "%yyyy-%mm-%dd"),
		STRINGTODATE('9999-12-31', '%yyyy-%mm-%dd') AS EXPY_D,
		REFR_PK AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I,
		'0' AS ROW_SECU_ACCS_C
	FROM {{ ref('CC_Identify_Deltas') }}
	WHERE ActionRequired = 'T' AND {{ ref('CC_Identify_Deltas') }}.change_code = 1 OR {{ ref('CC_Identify_Deltas') }}.change_code = 3
)

SELECT * FROM XFR_Split__To_Tmp_Insert_USR_TEM_DS