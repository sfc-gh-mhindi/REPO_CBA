{{ config(materialized='view', tags=['GDWUtilProcessMetaDataFL']) }}

WITH Update_Util_Pros__Update_Util_Proc_Table AS (
	SELECT
		-- *SRC*: Update_Link.InputRows = Update_Link.RowsWritten and Update_Link.TotalRowsRejected = 0,
		{{ ref('Lookup') }}.InputRows = {{ ref('Lookup') }}.RowsWritten AND {{ ref('Lookup') }}.TotalRowsRejected = 0 AS svLoadOK,
		pODS_PROS_ID AS PROS_KEY_I,
		'Y' AS COMT_F,
		{{ ref('Lookup') }}.RowsWritten AS SYST_INS_Q,
		DSJobStartTimestamp AS STUS_CHNG_S,
		DSJobStartTimestamp AS COMT_S,
		-- *SRC*: \(20)If svLoadOK then 'COMT' else 'FAIL',
		IFF(svLoadOK, 'COMT', 'FAIL') AS STUS_C,
		pODS_LOAD_USER AS CONV_M,
		'FL' AS CONV_TYPE_M,
		pRUN_STRM_C AS SRCE_SYST_M,
		pcFILE_NAME AS SRCE_M,
		TRGT_M,
		{{ ref('Lookup') }}.Timestamp AS SYST_S,
		BTCH_KEY_I,
		{{ ref('Lookup') }}.SRCE_LOAD_CNT_SUM AS SRCE_BTCH_LOAD_CNT
	FROM {{ ref('Lookup') }}
	WHERE 
)

SELECT * FROM Update_Util_Pros__Update_Util_Proc_Table