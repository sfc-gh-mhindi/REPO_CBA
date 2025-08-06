{{ config(materialized='view', tags=['LdMAP_CSE_CNTY_CODELkp']) }}

WITH XfmConversions AS (
	SELECT
		{{ ref('SrcMAP_CSE_CNTY_CODETera') }}.DOCU_COLL_CNTY_ID AS CNTY_ID,
		ISO_CNTY_C
	FROM {{ ref('SrcMAP_CSE_CNTY_CODETera') }}
	WHERE 
)

SELECT * FROM XfmConversions