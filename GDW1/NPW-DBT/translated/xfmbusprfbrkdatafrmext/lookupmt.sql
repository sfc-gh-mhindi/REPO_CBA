{{ config(materialized='view', tags=['XfmBusPrfBrkDataFrmExt']) }}

WITH LookUpMT AS (
	SELECT
		{{ ref('IgnrNulls__ToLkp') }}.UNID_PATY_I,
		{{ ref('IgnrNulls__ToLkp') }}.ALIAS_ID,
		{{ ref('IgnrNulls__ToLkp') }}.PRINT_ANYWHERE_FLAG,
		{{ ref('IgnrNulls__ToLkp') }}.GRADE,
		{{ ref('IgnrNulls__ToLkp') }}.SUBGRADE,
		{{ ref('MAP_CSE_UNID_PATY_PRFL_GR') }}.GRDE_C,
		{{ ref('MAP_CSE_UNID_PATY_PRFL_SG') }}.SUB_GRDE_C
	FROM {{ ref('IgnrNulls__ToLkp') }}
	LEFT JOIN {{ ref('MAP_CSE_UNID_PATY_PRFL_GR') }} ON {{ ref('IgnrNulls__ToLkp') }}.GRADE = {{ ref('MAP_CSE_UNID_PATY_PRFL_GR') }}.GRDE
	LEFT JOIN {{ ref('MAP_CSE_UNID_PATY_PRFL_SG') }} ON {{ ref('IgnrNulls__ToLkp') }}.SUBGRADE = {{ ref('MAP_CSE_UNID_PATY_PRFL_SG') }}.SUB_GRDE
)

SELECT * FROM LookUpMT