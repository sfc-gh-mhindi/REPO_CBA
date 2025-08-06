{{ config(materialized='view', tags=['XfmHL_APPFrmExt1']) }}

WITH LkpFileandMap AS (
	SELECT
		{{ ref('IgnrNulls__ToLkp') }}.HL_APP_ID,
		{{ ref('IgnrNulls__ToLkp') }}.CHL_APP_HL_APP_ID,
		{{ ref('IgnrNulls__ToLkp') }}.EXEC_DOCU_RECV_TYPE,
		{{ ref('MAP_CSE_APPT_DOCU_DELY') }}.DOCU_DELY_RECV_C
	FROM {{ ref('IgnrNulls__ToLkp') }}
	LEFT JOIN {{ ref('MAP_CSE_APPT_DOCU_DELY') }} ON {{ ref('IgnrNulls__ToLkp') }}.EXEC_DOCU_RECV_TYPE = {{ ref('MAP_CSE_APPT_DOCU_DELY') }}.EXEC_DOCU_RECV_TYPE
)

SELECT * FROM LkpFileandMap