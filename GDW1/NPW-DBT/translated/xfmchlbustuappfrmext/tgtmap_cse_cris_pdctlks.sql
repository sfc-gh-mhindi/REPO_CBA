{{ config(materialized='view', tags=['XfmChlBusTuAPPFrmExt']) }}

WITH 
_cba__app_hlt_dev_lookupset_map__cse__cris__pdct AS (
	SELECT
	*
	FROM {{ source("","_cba__app_hlt_dev_lookupset_map__cse__cris__pdct")  }})
TgtMAP_CSE_CRIS_PDCTLks AS (
	SELECT CRIS_PDCT_C,
		ACCT_QLFY_C,
		SRCE_SYST_C
	FROM _cba__app_hlt_dev_lookupset_map__cse__cris__pdct
)

SELECT * FROM TgtMAP_CSE_CRIS_PDCTLks