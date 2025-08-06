{{ config(materialized='view', tags=['MergeTMP_DELETED']) }}

WITH LkpReferences AS (
	SELECT
		{{ ref('SrcLdMAP_CSE_APPT_PDCT_PATY_ROLELks') }}.PATY_ROLE_C,
		{{ ref('CpyRename') }}.DELETED_TABLE_NAME,
		{{ ref('CpyRename') }}.DELETED_KEY_1,
		{{ ref('CpyRename') }}.DELETED_KEY_1_VALUE,
		{{ ref('CpyRename') }}.DELETED_KEY_2,
		{{ ref('CpyRename') }}.DELETED_KEY_2_VALUE,
		{{ ref('CpyRename') }}.DELETED_KEY_3,
		{{ ref('CpyRename') }}.ROLE_CAT_ID AS DELETED_KEY_3_VALUE,
		{{ ref('CpyRename') }}.DELETED_KEY_4,
		{{ ref('CpyRename') }}.SBTY_CODE AS DELETED_KEY_4_VALUE,
		{{ ref('CpyRename') }}.DELETED_KEY_5,
		{{ ref('CpyRename') }}.DELETED_KEY_5_VALUE,
		{{ ref('SrcMAP_CSE_APPT_QLFYLks') }}.APPT_QLFY_C
	FROM {{ ref('CpyRename') }}
	LEFT JOIN {{ ref('SrcLdMAP_CSE_APPT_PDCT_PATY_ROLELks') }} ON 
	LEFT JOIN {{ ref('SrcMAP_CSE_APPT_QLFYLks') }} ON 
)

SELECT * FROM LkpReferences