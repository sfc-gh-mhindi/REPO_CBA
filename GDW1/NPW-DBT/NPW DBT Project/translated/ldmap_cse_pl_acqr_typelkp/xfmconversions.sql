{{ config(materialized='view', tags=['LdMAP_CSE_PL_ACQR_TYPELkp']) }}

WITH XfmConversions AS (
	SELECT
		-- *SRC*: trim(InMAP_CSE_PL_ACQR_TYPETera.PL_CMPN_CAT_ID),
		TRIM({{ ref('SrcMAP_CSE_PL_ACQR_TYPETera') }}.PL_CMPN_CAT_ID) AS PL_CMPN_CAT_ID,
		-- *SRC*: trim(InMAP_CSE_PL_ACQR_TYPETera.ACQR_TYPE_C),
		TRIM({{ ref('SrcMAP_CSE_PL_ACQR_TYPETera') }}.ACQR_TYPE_C) AS ACQR_TYPE_C
	FROM {{ ref('SrcMAP_CSE_PL_ACQR_TYPETera') }}
	WHERE 
)

SELECT * FROM XfmConversions