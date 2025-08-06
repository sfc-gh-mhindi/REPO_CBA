{{ config(materialized='view', tags=['XfmCLP_ANS_Appt_QstnExt']) }}

WITH 
_cba__app_hlt_sit_lookupset_map__cse__appt__qstn AS (
	SELECT
	*
	FROM {{ source("","_cba__app_hlt_sit_lookupset_map__cse__appt__qstn")  }})
SrcMAP_CSE_APPT_QSTNks AS (
	SELECT QSTN_ID,
		ROW_S
	FROM _cba__app_hlt_sit_lookupset_map__cse__appt__qstn 
)

SELECT * FROM SrcMAP_CSE_APPT_QSTNks