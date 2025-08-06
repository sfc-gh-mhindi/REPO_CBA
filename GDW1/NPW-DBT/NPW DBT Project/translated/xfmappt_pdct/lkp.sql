{{ config(materialized='view', tags=['XfmAppt_Pdct']) }}

WITH Lkp AS (
	SELECT
		{{ ref('FiltrNullAndNotNull') }}.APP_PROD_ID,
		{{ ref('FiltrNullAndNotNull') }}.APP_ID,
		{{ ref('FiltrNullAndNotNull') }}.PDCT_N,
		{{ ref('FiltrNullAndNotNull') }}.PO_OVERDRAFT_CAT_ID,
		{{ ref('MAP_CSE_APPT_PDCT') }}.APPT_PDCT_CATG_C,
		{{ ref('MAP_CSE_APPT_PDCT') }}.APPT_PDCT_DURT_C
	FROM {{ ref('FiltrNullAndNotNull') }}
	LEFT JOIN {{ ref('MAP_CSE_APPT_PDCT') }} ON {{ ref('FiltrNullAndNotNull') }}.PO_OVERDRAFT_CAT_ID = {{ ref('MAP_CSE_APPT_PDCT') }}.PO_OVERDRAFT_CAT_ID
	WHERE PO_OVERDRAFT_CAT_ID_CHK = 'N'
)

SELECT * FROM Lkp