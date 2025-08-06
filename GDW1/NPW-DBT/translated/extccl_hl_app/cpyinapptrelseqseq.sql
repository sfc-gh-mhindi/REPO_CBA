{{ config(materialized='view', tags=['ExtCCL_HL_APP']) }}

WITH CpyInApptRelSeqSeq AS (
	SELECT
		CCL_HL_APP_ID,
		CCL_APP_ID,
		HL_APP_ID,
		LMI_AMT,
		HL_PACKAGE_CAT_ID
	FROM {{ ref('SrcInApptRelSeqSeq') }}
)

SELECT * FROM CpyInApptRelSeqSeq