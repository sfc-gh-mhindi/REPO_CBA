{{ config(materialized='view', tags=['XfmAppt_Pdct_Purp']) }}

WITH LkpPurpTypeC AS (
	SELECT
		{{ ref('Transformer') }}.APP_PROD_ID,
		{{ ref('Transformer') }}.PL_PROD_PURP_CAT_ID,
		{{ ref('MAP_CSE_APPT_PDCT_PURP_PO') }}.PURP_TYPE_C
	FROM {{ ref('Transformer') }}
	LEFT JOIN {{ ref('MAP_CSE_APPT_PDCT_PURP_PO') }} ON {{ ref('Transformer') }}.PL_PROD_PURP_CAT_ID = {{ ref('MAP_CSE_APPT_PDCT_PURP_PO') }}.PL_PROD_PURP_CAT_ID
)

SELECT * FROM LkpPurpTypeC