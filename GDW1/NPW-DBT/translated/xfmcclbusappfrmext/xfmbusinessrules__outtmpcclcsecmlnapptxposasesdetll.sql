{{ config(materialized='view', tags=['XfmCclBusAppFrmExt']) }}

WITH XfmBusinessRules__OutTmpCclCseCmlnApptXposAsesDetll AS (
	SELECT
		-- *SRC*: Len(Trim(( IF IsNotNull((InXfmBusinessRules.TOPUP_APP_ID)) THEN (InXfmBusinessRules.TOPUP_APP_ID) ELSE ""))) = 0,
		LEN(TRIM(IFF({{ ref('CpyNoOp') }}.TOPUP_APP_ID IS NOT NULL, {{ ref('CpyNoOp') }}.TOPUP_APP_ID, ''))) = 0 AS svRejectApptRel,
		-- *SRC*: Len(Trim(( IF IsNotNull((InXfmBusinessRules.AD_TUC_AMT)) THEN (InXfmBusinessRules.AD_TUC_AMT) ELSE ""))) = 0 OR InXfmBusinessRules.AD_TUC_AMT = 0,
		LEN(TRIM(IFF({{ ref('CpyNoOp') }}.AD_TUC_AMT IS NOT NULL, {{ ref('CpyNoOp') }}.AD_TUC_AMT, ''))) = 0 OR {{ ref('CpyNoOp') }}.AD_TUC_AMT = 0 AS svRejectApptAsesDetl,
		-- *SRC*: "CSE" : "CL" : InXfmBusinessRules.CCL_APP_ID,
		CONCAT(CONCAT('CSE', 'CL'), {{ ref('CpyNoOp') }}.CCL_APP_ID) AS APPT_I,
		{{ ref('CpyNoOp') }}.CARNELL_EXPOSURE_AMT AS XPOS_A,
		-- *SRC*: StringToTimestamp(InXfmBusinessRules.CARNELL_EXPOSURE_AMT_DATE, "%yyyy%mm%dd%hh%nn%ss"),
		STRINGTOTIMESTAMP({{ ref('CpyNoOp') }}.CARNELL_EXPOSURE_AMT_DATE, '%yyyy%mm%dd%hh%nn%ss') AS XPOS_AMT_D,
		{{ ref('CpyNoOp') }}.CARNELL_OVERRIDE_COV_ASSESSMNT AS OVRD_COVTS_ASES_F,
		{{ ref('CpyNoOp') }}.CARNELL_OVERRIDE_REASON_CAT_ID AS CSE_CMLN_OVRD_REAS_CATG_C,
		-- *SRC*: \(20)If IsNull(InXfmBusinessRules.CARNELL_SHORT_DEFAULT_OVERRIDE) Or Trim(( IF IsNotNull((InXfmBusinessRules.CARNELL_SHORT_DEFAULT_OVERRIDE)) THEN (InXfmBusinessRules.CARNELL_SHORT_DEFAULT_OVERRIDE) ELSE "")) = '' Then SetNull() Else InXfmBusinessRules.CARNELL_SHORT_DEFAULT_OVERRIDE,
		IFF({{ ref('CpyNoOp') }}.CARNELL_SHORT_DEFAULT_OVERRIDE IS NULL OR TRIM(IFF({{ ref('CpyNoOp') }}.CARNELL_SHORT_DEFAULT_OVERRIDE IS NOT NULL, {{ ref('CpyNoOp') }}.CARNELL_SHORT_DEFAULT_OVERRIDE, '')) = '', SETNULL(), {{ ref('CpyNoOp') }}.CARNELL_SHORT_DEFAULT_OVERRIDE) AS SHRT_DFLT_OVRD_F,
		-- *SRC*: StringToDate(InXfmBusinessRules.ORIG_ETL_D, "%yyyy%mm%dd"),
		STRINGTODATE({{ ref('CpyNoOp') }}.ORIG_ETL_D, '%yyyy%mm%dd') AS EFFT_D,
		-- *SRC*: StringToDate('9999-12-31', "%yyyy-%mm-%dd"),
		STRINGTODATE('9999-12-31', '%yyyy-%mm-%dd') AS EXPY_D
	FROM {{ ref('CpyNoOp') }}
	WHERE {{ ref('CpyNoOp') }}.SRCE_REC = 'Y'
)

SELECT * FROM XfmBusinessRules__OutTmpCclCseCmlnApptXposAsesDetll