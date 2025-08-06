{{ config(materialized='view', tags=['LdProsKeyHashLkp']) }}

WITH 
_cba__app_csel4_csel4dev_temp_proskeyseq AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_temp_proskeyseq")  }})
SeqProsKey AS (
	SELECT CONV_M,
		PROS_KEY_I
	FROM _cba__app_csel4_csel4dev_temp_proskeyseq
)

SELECT * FROM SeqProsKey