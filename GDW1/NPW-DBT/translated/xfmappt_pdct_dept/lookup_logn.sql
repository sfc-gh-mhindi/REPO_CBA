{{ config(materialized='view', tags=['XfmAppt_Pdct_Dept']) }}

WITH LookUp_Logn AS (
	SELECT
		{{ ref('NullRec_Logn') }}.APP_PROD_ID,
		{{ ref('NullRec_Logn') }}.LODGEMENT_BRANCH_BSB,
		{{ ref('GRD_OUN_MAP_LOGN') }}.DEPT_I
	FROM {{ ref('NullRec_Logn') }}
	LEFT JOIN {{ ref('GRD_OUN_MAP_LOGN') }} ON {{ ref('NullRec_Logn') }}.LODGEMENT_BRANCH_BSB = {{ ref('GRD_OUN_MAP_LOGN') }}.BSB_BRCH_N
)

SELECT * FROM LookUp_Logn