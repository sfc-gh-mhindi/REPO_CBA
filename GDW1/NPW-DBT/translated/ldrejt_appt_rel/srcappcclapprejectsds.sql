{{ config(materialized='view', tags=['LdREJT_APPT_REL']) }}

WITH 
_cba__app_lpxs_lpxs__dev_dataset_cse__clp__bus__appt__rel__mapping__rejects__20080212 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_lpxs_lpxs__dev_dataset_cse__clp__bus__appt__rel__mapping__rejects__20080212")  }})
SrcAppCclAppRejectsDS AS (
	SELECT APPT_I,
		RELD_APPT_I,
		REL_TYPE_C,
		LOAN_SUBTYPE_CODE,
		ETL_D,
		ORIG_ETL_D,
		EROR_C
	FROM _cba__app_lpxs_lpxs__dev_dataset_cse__clp__bus__appt__rel__mapping__rejects__20080212
)

SELECT * FROM SrcAppCclAppRejectsDS