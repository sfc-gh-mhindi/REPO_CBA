{{ config(materialized='view', tags=['XfmHL_APP_PROD_PURPOSEFrmExt']) }}

WITH LkpReferences AS (
	SELECT
		{{ ref('CpyRename') }}.HL_APP_PROD_PURPOSE_ID,
		{{ ref('CpyRename') }}.HL_APP_PROD_ID,
		{{ ref('CpyRename') }}.HL_LOAN_PURPOSE_CAT_ID,
		{{ ref('CpyRename') }}.AMOUNT,
		{{ ref('CpyRename') }}.MAIN_PURPOSE,
		{{ ref('CpyRename') }}.ORIG_ETL_D,
		{{ ref('SrcMAP_CSE_APPT_PURP_HLLks') }}.PURP_TYPE_C
	FROM {{ ref('CpyRename') }}
	LEFT JOIN {{ ref('SrcMAP_CSE_APPT_PURP_HLLks') }} ON 
)

SELECT * FROM LkpReferences