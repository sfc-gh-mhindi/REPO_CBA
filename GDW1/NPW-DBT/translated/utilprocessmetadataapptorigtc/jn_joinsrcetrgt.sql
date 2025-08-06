{{ config(materialized='view', tags=['UtilProcessMetaDataApptOrigTC']) }}

WITH jn_JoinSrceTrgt AS (
	SELECT
		{{ ref('ag_CntTypes') }}.SyncId,
		{{ ref('ag_CntTypes') }}.InsertCount,
		{{ ref('ag_CntTypes') }}.UpdateCount,
		{{ ref('ag_CntTypes') }}.DeleteCount,
		{{ ref('tc_SyncTbl') }}.INS_CNT,
		{{ ref('tc_SyncTbl') }}.UPD_CNT,
		{{ ref('tc_SyncTbl') }}.DEL_CNT,
		{{ ref('tc_SyncTbl') }}.REJ_CNT,
		{{ ref('tc_SyncTbl') }}.MIS_ROW_CNT,
		{{ ref('tc_SyncTbl') }}.ERR_CNT1,
		{{ ref('tc_SyncTbl') }}.ERR_CNT2,
		{{ ref('tc_SyncTbl') }}.DUP_KEY_CNT,
		{{ ref('tc_SyncTbl') }}.START_T,
		{{ ref('tc_SyncTbl') }}.END_T
	FROM {{ ref('ag_CntTypes') }}
	LEFT JOIN {{ ref('tc_SyncTbl') }} ON {{ ref('ag_CntTypes') }}.SyncId = {{ ref('tc_SyncTbl') }}.SyncId
)

SELECT * FROM jn_JoinSrceTrgt