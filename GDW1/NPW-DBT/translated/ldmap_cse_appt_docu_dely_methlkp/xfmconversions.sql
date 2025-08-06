{{ config(materialized='view', tags=['LdMAP_CSE_APPT_DOCU_DELY_METHLkp']) }}

WITH XfmConversions AS (
	SELECT
		{{ ref('SrcMAP_CSE_DOCU_DELY_METHTera') }}.DOCU_COLL_CAT_ID AS TU_DOCCOLLECT_METHOD_CAT_ID,
		DOCU_DELY_METH_C
	FROM {{ ref('SrcMAP_CSE_DOCU_DELY_METHTera') }}
	WHERE 
)

SELECT * FROM XfmConversions