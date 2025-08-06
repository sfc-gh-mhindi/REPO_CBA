{{ config(materialized='view', tags=['XfmComBusAppProdClientRoleFrmExt']) }}

WITH LkpReferences AS (
	SELECT
		{{ ref('CpyRename') }}.APP_PROD_CLIENT_ROLE_ID,
		{{ ref('CpyRename') }}.ROLE_CAT_ID,
		{{ ref('CpyRename') }}.CIF_CODE,
		{{ ref('CpyRename') }}.APP_PROD_ID,
		{{ ref('CpyRename') }}.SBTY_CODE AS SUBTYPE_CODE,
		{{ ref('CpyRename') }}.ORIG_ETL_D,
		{{ ref('SrcMAP_CSE_APPT_PDCT_PATY_ROLELks') }}.PATY_ROLE_C,
		{{ ref('SrcMAP_CSE_APPT_QLFYLks') }}.APPT_QLFY_C
	FROM {{ ref('CpyRename') }}
	LEFT JOIN {{ ref('SrcMAP_CSE_APPT_PDCT_PATY_ROLELks') }} ON 
	LEFT JOIN {{ ref('SrcMAP_CSE_APPT_QLFYLks') }} ON 
)

SELECT * FROM LkpReferences