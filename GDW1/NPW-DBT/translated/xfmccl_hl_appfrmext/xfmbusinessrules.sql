{{ config(materialized='view', tags=['XfmCCL_HL_APPFrmExt']) }}

WITH XfmBusinessRules AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((OutApptRelPremapDS.HL_APP_ID)) THEN (OutApptRelPremapDS.HL_APP_ID) ELSE ""))) = 0) Then 'N' Else 'Y',
		IFF(LEN(TRIM(IFF({{ ref('SrcApptRelPremapDS') }}.HL_APP_ID IS NOT NULL, {{ ref('SrcApptRelPremapDS') }}.HL_APP_ID, ''))) = 0, 'N', 'Y') AS svLoadApptRel,
		-- *SRC*: "CSE" : "CL" : OutApptRelPremapDS.CCL_APP_ID,
		CONCAT(CONCAT('CSE', 'CL'), {{ ref('SrcApptRelPremapDS') }}.CCL_APP_ID) AS APPT_I,
		-- *SRC*: "CSE" : "HL" : OutApptRelPremapDS.HL_APP_ID,
		CONCAT(CONCAT('CSE', 'HL'), {{ ref('SrcApptRelPremapDS') }}.HL_APP_ID) AS RELD_APPT_I,
		'CLHL' AS REL_TYPE_C,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd'),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS EFFT_D,
		-- *SRC*: StringToDate('99991231', '%yyyy%mm%dd'),
		STRINGTODATE('99991231', '%yyyy%mm%dd') AS EXPY_D,
		0 AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I,
		RUN_STREAM AS RUN_STRM
	FROM {{ ref('SrcApptRelPremapDS') }}
	WHERE svLoadApptRel = 'Y'
)

SELECT * FROM XfmBusinessRules