{{ config(materialized='view', tags=['XfmAppt_Pdct_Dept']) }}

WITH LookUp_Nom AS (
	SELECT
		{{ ref('NullRec_Nom') }}.APP_PROD_ID,
		{{ ref('NullRec_Nom') }}.NOMINATED_BSB,
		{{ ref('GRD_OUN_MAP_NOM') }}.DEPT_I
	FROM {{ ref('NullRec_Nom') }}
	LEFT JOIN {{ ref('GRD_OUN_MAP_NOM') }} ON {{ ref('NullRec_Nom') }}.NOMINATED_BSB = {{ ref('GRD_OUN_MAP_NOM') }}.BSB_BRCH_N
)

SELECT * FROM LookUp_Nom