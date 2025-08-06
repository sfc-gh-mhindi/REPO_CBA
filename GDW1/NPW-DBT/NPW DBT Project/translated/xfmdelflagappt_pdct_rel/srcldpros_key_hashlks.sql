{{ config(materialized='view', tags=['XfmDelFlagAPPT_PDCT_REL']) }}

WITH 
_cba__app_csel4_csel4dev_lookupset_pros__key__hash__conv__m AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_lookupset_pros__key__hash__conv__m")  }})
SrcLdPROS_KEY_HASHLks AS (
	SELECT CONV_M,
		PROS_KEY_I
	FROM _cba__app_csel4_csel4dev_lookupset_pros__key__hash__conv__m
)

SELECT * FROM SrcLdPROS_KEY_HASHLks