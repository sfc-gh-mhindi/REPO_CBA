{{ config(materialized='view', tags=['Ext_XfmHLM_APPONEOFF']) }}

WITH LkpMapCseApptPDctFeatValueC AS (
	SELECT
		{{ ref('CpyChlbushlmapponeoff') }}.HLM_APP_PROD_ID,
		{{ ref('CpyChlbushlmapponeoff') }}.PEXA_FLAG,
		{{ ref('MAP_CSE_APPT_PDCT_FEAT') }}.FEAT_VALU_C
	FROM {{ ref('CpyChlbushlmapponeoff') }}
	LEFT JOIN {{ ref('MAP_CSE_APPT_PDCT_FEAT') }} ON {{ ref('CpyChlbushlmapponeoff') }}.PEXA_FLAG = {{ ref('MAP_CSE_APPT_PDCT_FEAT') }}.PEXA_FLAG
)

SELECT * FROM LkpMapCseApptPDctFeatValueC