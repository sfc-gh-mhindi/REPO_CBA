{{ config(materialized='view', tags=['LdMAP_CSE_APPT_PDCT_FEAT_CAMPkp']) }}

WITH XfmConversions AS (
	SELECT
		CAMPAIGN_CAT_ID,
		FEAT_I,
		{{ ref('SrcMAP_CSE_APPT_QLFYTera') }}.ACTL_VALU_R AS ACTL_VAL_R
	FROM {{ ref('SrcMAP_CSE_APPT_QLFYTera') }}
	WHERE 
)

SELECT * FROM XfmConversions