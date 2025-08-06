{{ config(materialized='view', tags=['LdTMP_APPT_ORIGFrmXfm']) }}

WITH Lkp AS (
	SELECT
		{{ ref('SrcComBusApp') }}.APP_RECORD_TYPE,
		{{ ref('SrcComBusApp') }}.APP_MOD_TIMESTAMP,
		{{ ref('SrcComBusApp') }}.APP_APP_ID,
		{{ ref('SrcComBusApp') }}.APP_SUBTYPE_CODE,
		{{ ref('SrcComBusApp') }}.APP_APP_NO,
		{{ ref('SrcComBusApp') }}.APP_CREATED_DATE,
		{{ ref('SrcComBusApp') }}.APP_CREATED_BY_STAFF_NUMBER,
		{{ ref('SrcComBusApp') }}.APP_OWNED_BY_STAFF_NUMBER,
		{{ ref('SrcComBusApp') }}.APP_CHANNEL_CAT_ID,
		{{ ref('SrcComBusApp') }}.APP_LODGEMENT_BRANCH_ID,
		{{ ref('SrcComBusApp') }}.APP_SM_CASE_ID,
		{{ ref('SrcComBusApp') }}.APP_ENTRY_POINT,
		{{ ref('SrcComBusApp') }}.APP_CREATED_CHANNEL_CAT_ID,
		{{ ref('SrcComBusApp') }}.APP_SUBMITTED_CHANNEL_CAT_ID,
		{{ ref('ds_MapCseApptQlfy') }}.APPT_QLFY_C,
		{{ ref('CC') }}.APPT_ORIG_C AS CHNL_APPT_ORIG_C,
		{{ ref('CC') }}.APPT_ORIG_C AS SUB_APPT_ORIG_C,
		{{ ref('CC') }}.APPT_ORIG_C AS CREATED_APPT_ORIG_C
	FROM {{ ref('SrcComBusApp') }}
	LEFT JOIN {{ ref('ds_MapCseApptQlfy') }} ON {{ ref('SrcComBusApp') }}.APP_SUBTYPE_CODE = {{ ref('ds_MapCseApptQlfy') }}.SBTY_CODE
	LEFT JOIN {{ ref('CC') }} ON {{ ref('SrcComBusApp') }}.APP_CHANNEL_CAT_ID = {{ ref('CC') }}.CHNL_CAT_ID
	LEFT JOIN {{ ref('CC') }} ON {{ ref('SrcComBusApp') }}.APP_CHANNEL_CAT_ID = {{ ref('CC') }}.CHNL_CAT_ID
	LEFT JOIN {{ ref('CC') }} ON {{ ref('SrcComBusApp') }}.APP_CHANNEL_CAT_ID = {{ ref('CC') }}.CHNL_CAT_ID
)

SELECT * FROM Lkp