{{ config(materialized='view', tags=['XfmAppt_Stus']) }}

WITH Join_Sm_Case_Id AS (
	SELECT
		{{ ref('Transfrm') }}.APP_ID,
		{{ ref('CSE_COM_BUS_SM_CASE_STATE') }}.SM_STATE_CAT_ID,
		{{ ref('CSE_COM_BUS_SM_CASE_STATE') }}.START_DATE AS START_D,
		{{ ref('CSE_COM_BUS_SM_CASE_STATE') }}.END_DATE AS END_D,
		{{ ref('CSE_COM_BUS_SM_CASE_STATE') }}.CREATED_BY_STAFF_NUMBER,
		{{ ref('Transfrm') }}.SUBTYPE_CODE
	FROM {{ ref('Transfrm') }}
	OUTER JOIN {{ ref('CSE_COM_BUS_SM_CASE_STATE') }} ON {{ ref('Transfrm') }}.SM_CASE_ID = {{ ref('CSE_COM_BUS_SM_CASE_STATE') }}.SM_CASE_ID
)

SELECT * FROM Join_Sm_Case_Id