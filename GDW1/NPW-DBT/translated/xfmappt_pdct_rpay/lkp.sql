{{ config(materialized='view', tags=['XfmAppt_Pdct_Rpay']) }}

WITH Lkp AS (
	SELECT
		{{ ref('XfmNull') }}.APP_PROD_ID,
		{{ ref('XfmNull') }}.PO_REPAYMENT_SOURCE_CAT_ID,
		{{ ref('XfmNull') }}.PO_REPAYMENT_SOURCE_OTHER,
		{{ ref('MAP_CSE_APPT_PDCT_RPAY') }}.RPAY_SRCE_C
	FROM {{ ref('XfmNull') }}
	LEFT JOIN {{ ref('MAP_CSE_APPT_PDCT_RPAY') }} ON {{ ref('XfmNull') }}.PO_REPAYMENT_SOURCE_CAT_ID = {{ ref('MAP_CSE_APPT_PDCT_RPAY') }}.PO_REPAYMENT_SOURCE_CAT_ID
)

SELECT * FROM Lkp