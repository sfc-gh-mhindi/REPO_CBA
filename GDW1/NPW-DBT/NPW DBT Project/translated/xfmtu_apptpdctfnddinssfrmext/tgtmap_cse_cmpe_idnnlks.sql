{{ config(materialized='view', tags=['XfmTu_ApptPdctFnddInssFrmExt']) }}

WITH 
_cba__app_hlt_dev_lookupset_map__cse__smpe__indd AS (
	SELECT
	*
	FROM {{ source("","_cba__app_hlt_dev_lookupset_map__cse__smpe__indd")  }})
TgtMAP_CSE_CMPE_IDNNLks AS (
	SELECT INSN_ID,
		CMPE_I
	FROM _cba__app_hlt_dev_lookupset_map__cse__smpe__indd
)

SELECT * FROM TgtMAP_CSE_CMPE_IDNNLks