{{ config(materialized='view', tags=['XfmCclBusAppFrmExt']) }}

WITH XfmBusinessRules__OutTmpCclApptAsesDetl AS (
	SELECT
		-- *SRC*: Len(Trim(( IF IsNotNull((InXfmBusinessRules.TOPUP_APP_ID)) THEN (InXfmBusinessRules.TOPUP_APP_ID) ELSE ""))) = 0,
		LEN(TRIM(IFF({{ ref('CpyNoOp') }}.TOPUP_APP_ID IS NOT NULL, {{ ref('CpyNoOp') }}.TOPUP_APP_ID, ''))) = 0 AS svRejectApptRel,
		-- *SRC*: Len(Trim(( IF IsNotNull((InXfmBusinessRules.AD_TUC_AMT)) THEN (InXfmBusinessRules.AD_TUC_AMT) ELSE ""))) = 0 OR InXfmBusinessRules.AD_TUC_AMT = 0,
		LEN(TRIM(IFF({{ ref('CpyNoOp') }}.AD_TUC_AMT IS NOT NULL, {{ ref('CpyNoOp') }}.AD_TUC_AMT, ''))) = 0 OR {{ ref('CpyNoOp') }}.AD_TUC_AMT = 0 AS svRejectApptAsesDetl,
		-- *SRC*: "CSE" : "CL" : InXfmBusinessRules.CCL_APP_ID,
		CONCAT(CONCAT('CSE', 'CL'), {{ ref('CpyNoOp') }}.CCL_APP_ID) AS APPT_I,
		'TUCA' AS AMT_TYPE_C,
		-- *SRC*: StringToDate(InXfmBusinessRules.ORIG_ETL_D, "%yyyy%mm%dd"),
		STRINGTODATE({{ ref('CpyNoOp') }}.ORIG_ETL_D, '%yyyy%mm%dd') AS EFFT_D,
		-- *SRC*: StringToDate("9999-12-31", "%yyyy-%mm-%dd"),
		STRINGTODATE('9999-12-31', '%yyyy-%mm-%dd') AS EXPY_D,
		'AUD' AS CNCY_C,
		{{ ref('CpyNoOp') }}.AD_TUC_AMT AS APPT_ASES_A,
		0 AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I,
		RUN_STREAM AS RUN_STRM
	FROM {{ ref('CpyNoOp') }}
	WHERE Not svRejectApptAsesDetl
)

SELECT * FROM XfmBusinessRules__OutTmpCclApptAsesDetl