{{ config(materialized='view', tags=['XfmPL_APPFrmExt']) }}

WITH LkpReferences AS (
	SELECT
		{{ ref('CpyRename') }}.PL_APP_ID,
		{{ ref('CpyRename') }}.NOMINATED_BRANCH_ID,
		{{ ref('CpyRename') }}.PL_PACKAGE_CAT_ID,
		{{ ref('CpyRename') }}.ORIG_ETL_D,
		{{ ref('SrcMAP_CSE_PACK_PDCT_PLLks') }}.PDCT_N
	FROM {{ ref('CpyRename') }}
	LEFT JOIN {{ ref('SrcMAP_CSE_PACK_PDCT_PLLks') }} ON 
)

SELECT * FROM LkpReferences