{{ config(materialized='view', tags=['LdMAP_CSE_APPT_ACQR_SRCELkp']) }}

WITH XfmConversions AS (
	SELECT
		-- *SRC*: trim(InMAP_CSE_APPT_ACQR_SRCETera.PL_MRKT_SRCE_CAT_ID),
		TRIM({{ ref('SrcMAP_CSE_APPT_ACQR_SRCETera') }}.PL_MRKT_SRCE_CAT_ID) AS PL_MRKT_SRCE_CAT_ID,
		ACQR_SRCE_C
	FROM {{ ref('SrcMAP_CSE_APPT_ACQR_SRCETera') }}
	WHERE 
)

SELECT * FROM XfmConversions