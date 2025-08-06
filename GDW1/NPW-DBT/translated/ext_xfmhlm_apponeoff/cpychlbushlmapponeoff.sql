{{ config(materialized='view', tags=['Ext_XfmHLM_APPONEOFF']) }}

WITH CpyChlbushlmapponeoff AS (
	SELECT
		HLM_APP_PROD_ID,
		PEXA_FLAG
	FROM {{ ref('SrcChlBusHlmApp') }}
)

SELECT * FROM CpyChlbushlmapponeoff