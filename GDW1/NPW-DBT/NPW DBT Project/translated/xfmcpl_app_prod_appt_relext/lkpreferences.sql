{{ config(materialized='view', tags=['XfmCPL_APP_PROD_Appt_RelExt']) }}

WITH LkpReferences AS (
	SELECT
		{{ ref('CpyRename') }}.APPT_I,
		{{ ref('CpyRename') }}.RELD_APPT_I,
		{{ ref('SrcMAP_CSE_APPT_QLFYLks') }}.APPT_QLFY_C,
		{{ ref('SrcMAP_CSE_LOAN_APPT_QLFYLks') }}.LOAN_APPT_QLFY_C,
		{{ ref('CpyRename') }}.ORIG_ETL_D
	FROM {{ ref('CpyRename') }}
	LEFT JOIN {{ ref('SrcMAP_CSE_APPT_QLFYLks') }} ON 
	LEFT JOIN {{ ref('SrcMAP_CSE_LOAN_APPT_QLFYLks') }} ON 
)

SELECT * FROM LkpReferences