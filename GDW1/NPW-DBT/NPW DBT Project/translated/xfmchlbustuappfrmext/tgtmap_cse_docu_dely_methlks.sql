{{ config(materialized='view', tags=['XfmChlBusTuAPPFrmExt']) }}

WITH 
_cba__app_hlt_dev_lookupset_map__docu__dely__meth AS (
	SELECT
	*
	FROM {{ source("","_cba__app_hlt_dev_lookupset_map__docu__dely__meth")  }})
TgtMAP_CSE_DOCU_DELY_METHLks AS (
	SELECT TU_DOCCOLLECT_METHOD_CAT_ID,
		DOCU_DELY_METH_C
	FROM _cba__app_hlt_dev_lookupset_map__docu__dely__meth
)

SELECT * FROM TgtMAP_CSE_DOCU_DELY_METHLks