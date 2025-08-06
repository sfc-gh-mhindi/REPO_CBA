{{ config(materialized='view', tags=['XfmPL_APP_PROD_PURPFrmExt']) }}

WITH LkpReferences AS (
	SELECT
		{{ ref('CpyRename') }}.PL_APP_PROD_PURP_ID,
		{{ ref('CpyRename') }}.PL_APP_PROD_PURP_CAT_ID,
		{{ ref('CpyRename') }}.AMT,
		{{ ref('CpyRename') }}.PL_APP_PROD_ID,
		{{ ref('CpyRename') }}.ORIG_ETL_D,
		{{ ref('SrcMAP_CSE_APPT_PURP_PLLks') }}.PURP_TYPE_C
	FROM {{ ref('CpyRename') }}
	LEFT JOIN {{ ref('SrcMAP_CSE_APPT_PURP_PLLks') }} ON 
)

SELECT * FROM LkpReferences