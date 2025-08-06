{{ config(materialized='view', tags=['XfmEvntRel']) }}

WITH LookUp AS (
	SELECT
		{{ ref('DropNullRec') }}.MOD_TIMESTAMP,
		{{ ref('DropNullRec') }}.OL_CLIENT_RM_RATING_ID,
		{{ ref('DropNullRec') }}.CLIENT_ID,
		{{ ref('DropNullRec') }}.CIF_CODE,
		{{ ref('DropNullRec') }}.OU_ID,
		{{ ref('DropNullRec') }}.CS_USER_ID,
		{{ ref('DropNullRec') }}.RATING,
		{{ ref('DropNullRec') }}.WIM_PROCESS_ID,
		{{ ref('BUSN_EVNT') }}.EVNT_I
	FROM {{ ref('DropNullRec') }}
	LEFT JOIN {{ ref('BUSN_EVNT') }} ON {{ ref('DropNullRec') }}.WIM_PROCESS_ID = {{ ref('BUSN_EVNT') }}.SRCE_SYST_EVNT_I
)

SELECT * FROM LookUp