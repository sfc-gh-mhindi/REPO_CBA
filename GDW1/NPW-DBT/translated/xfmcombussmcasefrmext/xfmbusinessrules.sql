{{ config(materialized='view', tags=['XfmComBusSmCaseFrmExt']) }}

WITH XfmBusinessRules AS (
	SELECT
		-- *SRC*: \(20)if (InXfmBusinessRules.TARG_SUBJ = 'APPT' and InXfmBusinessRules.TARG_I <> '9999') then 'Y' else 'N',
		IFF({{ ref('ModNullHandling') }}.TARG_SUBJ = 'APPT' AND {{ ref('ModNullHandling') }}.TARG_I <> '9999', 'Y', 'N') AS svLoadFlag,
		-- *SRC*: \(20)If Len(Trim(( IF IsNotNull((InXfmBusinessRules.WIM_PROCESS_ID)) THEN (InXfmBusinessRules.WIM_PROCESS_ID) ELSE ""))) = 0 Then 'Y' Else 'N',
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.WIM_PROCESS_ID IS NOT NULL, {{ ref('ModNullHandling') }}.WIM_PROCESS_ID, ''))) = 0, 'Y', 'N') AS svWimProcIdIsNull,
		{{ ref('ModNullHandling') }}.TARG_I AS APPT_I,
		-- *SRC*: "CSE" : "PR" : ( IF IsNotNull((InXfmBusinessRules.WIM_PROCESS_ID)) THEN (InXfmBusinessRules.WIM_PROCESS_ID) ELSE ""),
		CONCAT(CONCAT('CSE', 'PR'), IFF({{ ref('ModNullHandling') }}.WIM_PROCESS_ID IS NOT NULL, {{ ref('ModNullHandling') }}.WIM_PROCESS_ID, '')) AS EVNT_GRUP_I,
		-- *SRC*: StringToDate(InXfmBusinessRules.ORIG_ETL_D, "%yyyy%mm%dd"),
		STRINGTODATE({{ ref('ModNullHandling') }}.ORIG_ETL_D, '%yyyy%mm%dd') AS EFFT_D,
		-- *SRC*: StringToDate("99991231", "%yyyy%mm%dd"),
		STRINGTODATE('99991231', '%yyyy%mm%dd') AS EXPY_D,
		0 AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I,
		RUN_STREAM AS RUN_STRM
	FROM {{ ref('ModNullHandling') }}
	WHERE svLoadFlag = 'Y' AND svWimProcIdIsNull = 'N'
)

SELECT * FROM XfmBusinessRules