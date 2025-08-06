{{ config(materialized='view', tags=['LdMAP_CSE_APPT_CMPELkp']) }}

WITH XfmConversions AS (
	SELECT
		-- *SRC*: trim(InMAP_CSE_APPT_CMPETera.BAL_XFER_INSN_CAT_ID),
		TRIM({{ ref('SrcMAP_CSE_APPT_CMPETera') }}.BAL_XFER_INSN_CAT_ID) AS BAL_XFER_INSTITUTION_CAT_ID,
		CMPE_I
	FROM {{ ref('SrcMAP_CSE_APPT_CMPETera') }}
	WHERE 
)

SELECT * FROM XfmConversions