{{ config(materialized='view', tags=['XfmPL_APPFrmExt']) }}

WITH XfmBusinessRules__OutErrorMapPdctN AS (
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
		{{ ref('ModNullHandling') }}.PL_APP_ID AS SRCE_KEY_I,
		'PL_PACKAGE_CAT_ID' AS CONV_M,
		'LOOKUP' AS CONV_MAP_RULE_M,
		'MAP_CSE_PACK_PDCT_PL' AS TRSF_TABL_M,
		{{ ref('ModNullHandling') }}.PDCT_N AS VALU_CHNG_BFOR_X,
		svPdctN AS VALU_CHNG_AFTR_X,
		-- *SRC*: 'Job Name = ' : DSJobName,
		CONCAT('Job Name = ', DSJobName) AS TRSF_X,
		'PDCT_N' AS TRSF_COLM_M
	FROM {{ ref('ModNullHandling') }}
	WHERE svErrorCode = 'RPR2104' AND svLoadApptPdct = 'Y'
)

SELECT * FROM XfmBusinessRules__OutErrorMapPdctN