{{ config(materialized='view', tags=['XfmPL_APPFrmExt']) }}

WITH XfmBusinessRules__OutTmpApptDeptDS AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((InXfmBusinessRules.PL_PACKAGE_CAT_ID)) THEN (InXfmBusinessRules.PL_PACKAGE_CAT_ID) ELSE ""))) = 0) Then DEFAULT_NULL_VALUE ELSE InXfmBusinessRules.PDCT_N,
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.PL_PACKAGE_CAT_ID IS NOT NULL, {{ ref('ModNullHandling') }}.PL_PACKAGE_CAT_ID, ''))) = 0, DEFAULT_NULL_VALUE, {{ ref('ModNullHandling') }}.PDCT_N) AS svPdctN,
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((InXfmBusinessRules.NOMINATED_BRANCH_ID)) THEN (InXfmBusinessRules.NOMINATED_BRANCH_ID) ELSE ""))) = 0) Then 'N' Else 'Y',
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.NOMINATED_BRANCH_ID IS NOT NULL, {{ ref('ModNullHandling') }}.NOMINATED_BRANCH_ID, ''))) = 0, 'N', 'Y') AS svLoadApptDept,
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((InXfmBusinessRules.PL_PACKAGE_CAT_ID)) THEN (InXfmBusinessRules.PL_PACKAGE_CAT_ID) ELSE ""))) = 0) Then 'N' Else 'Y',
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.PL_PACKAGE_CAT_ID IS NOT NULL, {{ ref('ModNullHandling') }}.PL_PACKAGE_CAT_ID, ''))) = 0, 'N', 'Y') AS svLoadApptPdct,
		-- *SRC*: \(20)If svPdctN = '800999' then 'RPR2104' else '',
		IFF(svPdctN = '800999', 'RPR2104', '') AS svErrorCode,
		-- *SRC*: \(20)If svErrorCode <> "" Then @TRUE Else @FALSE,
		IFF(svErrorCode <> '', @TRUE, @FALSE) AS svRejectFlag,
		-- *SRC*: "CSE" : "PL" : InXfmBusinessRules.PL_APP_ID,
		CONCAT(CONCAT('CSE', 'PL'), {{ ref('ModNullHandling') }}.PL_APP_ID) AS APPT_I,
		'NOMN' AS DEPT_ROLE_C,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd'),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS EFFT_D,
		{{ ref('ModNullHandling') }}.NOMINATED_BRANCH_ID AS DEPT_I,
		-- *SRC*: StringToDate('99991231', '%yyyy%mm%dd'),
		STRINGTODATE('99991231', '%yyyy%mm%dd') AS EXPY_D,
		0 AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I,
		RUN_STREAM AS RUN_STRM
	FROM {{ ref('ModNullHandling') }}
	WHERE svLoadApptDept = 'Y'
)

SELECT * FROM XfmBusinessRules__OutTmpApptDeptDS